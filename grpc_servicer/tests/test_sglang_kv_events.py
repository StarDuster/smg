"""Exercise the SGLang bridge over real ZMQ and gRPC without loading an engine."""

import ast
import asyncio
import importlib.util
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytest.importorskip("smg_grpc_proto")
grpc = pytest.importorskip("grpc")
zmq = pytest.importorskip("zmq")
import zmq.asyncio  # noqa: E402, F811
from smg_grpc_proto.generated import common_pb2  # noqa: E402

_PATH = Path(__file__).parents[1] / "smg_grpc_servicer" / "sglang" / "kv_events.py"
_spec = importlib.util.spec_from_file_location("sglang_kv_transport", _PATH)
transport = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(transport)


def decode(payload):
    return int(payload)


def convert(value, seq):
    return common_pb2.KvEventBatch(sequence_number=seq, timestamp=value)


def subscribe_method():
    # Load the real RPC method without importing SGLang/torch. Its transport
    # and gRPC context are real; only engine batch decoding is substituted.
    path = _PATH.with_name("servicer.py")
    tree = ast.parse(path.read_text())
    method = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "SubscribeKvEvents"
    )
    namespace = {
        "AsyncIterator": AsyncIterator,
        "common_pb2": common_pb2,
        "grpc": grpc,
        "asyncio": asyncio,
        "zmq": zmq,
        "logger": logging.getLogger(__name__),
        "KVEventBatch": object,
        "msgspec": SimpleNamespace(
            msgpack=SimpleNamespace(Decoder=lambda _: SimpleNamespace(decode=decode))
        ),
        "ZmqEventPublisher": SimpleNamespace(offset_endpoint_port=transport.endpoint_for_rank),
        "subscribe_kv_events": transport.subscribe_kv_events,
    }
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["SubscribeKvEvents"]


@pytest_asyncio.fixture
async def bridge():
    ctx = zmq.asyncio.Context()
    pub = ctx.socket(zmq.XPUB)
    pub.setsockopt(zmq.XPUB_VERBOSE, 1)
    port = pub.bind_to_random_port("tcp://127.0.0.1")
    config = SimpleNamespace(endpoint=f"tcp://127.0.0.1:{port}", topic="kv")
    cursors = []
    method = subscribe_method()
    servicer = SimpleNamespace(_kv_events_config=config, _convert_kv_event_batch=convert)

    async def handler(request, context):
        cursors.append(request.start_sequence_number)
        async for batch in method(servicer, request, context):
            yield batch

    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "test.KvEvents",
                {
                    "Subscribe": grpc.unary_stream_rpc_method_handler(
                        handler,
                        request_deserializer=common_pb2.SubscribeKvEventsRequest.FromString,
                        response_serializer=common_pb2.KvEventBatch.SerializeToString,
                    )
                },
            ),
        )
    )
    grpc_port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{grpc_port}")
    rpc = channel.unary_stream(
        "/test.KvEvents/Subscribe",
        request_serializer=common_pb2.SubscribeKvEventsRequest.SerializeToString,
        response_deserializer=common_pb2.KvEventBatch.FromString,
    )

    def subscribe(cursor=0):
        return rpc(common_pb2.SubscribeKvEventsRequest(start_sequence_number=cursor))

    async def subscribed():
        # XPUB acknowledges the actual subscription; no timing sleeps needed.
        while await asyncio.wait_for(pub.recv(), 3) != b"\x01kv":
            pass

    async def publish(seq, payload=None):
        await pub.send_multipart([b"kv", seq.to_bytes(8, "big"), payload or str(seq).encode()])

    try:
        yield SimpleNamespace(
            subscribe=subscribe,
            subscribed=subscribed,
            publish=publish,
            config=config,
            cursors=cursors,
            pub=pub,
            ctx=ctx,
        )
    finally:
        await channel.close()
        await server.stop(None)
        pub.close(linger=0)
        ctx.term()


async def read(call):
    return await asyncio.wait_for(call.read(), 3)


@pytest.mark.asyncio
async def test_gap_resume_is_rejected_then_live_cursor_advances(bridge):
    call = bridge.subscribe()
    await bridge.subscribed()
    await bridge.publish(100)
    assert (await read(call)).sequence_number == 100
    await bridge.publish(103)
    assert (await read(call)).sequence_number == 103
    call.cancel()

    # The router retries from its last contiguous batch. Do not silently
    # reconnect to live seq=110 while it still expects seq=101.
    retry = bridge.subscribe(100)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(retry)
    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE

    # OUT_OF_RANGE triggers the gateway's existing per-worker clear/reset.
    fresh = bridge.subscribe()
    await bridge.subscribed()
    for seq in (110, 111):
        await bridge.publish(seq)
        assert (await read(fresh)).sequence_number == seq
    assert bridge.cursors == [0, 100, 0]
    fresh.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [1, 2**64 - 1])
async def test_replay_rejected_before_opening_live_subscription(bridge, cursor):
    call = bridge.subscribe(cursor)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(call)
    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE
    assert not await bridge.pub.poll(timeout=50)


@pytest.mark.asyncio
async def test_idle_poll_preserves_later_events_and_cancellation(bridge):
    call = bridge.subscribe()
    await bridge.subscribed()
    await asyncio.wait_for(call.initial_metadata(), 3)
    await asyncio.sleep(1.1)  # exercise the idle poll timeout
    await bridge.publish(0)
    assert (await read(call)).sequence_number == 0
    call.cancel()
    assert await asyncio.wait_for(bridge.pub.recv(), 3) == b"\x00kv"


@pytest.mark.asyncio
async def test_bad_payload_does_not_hide_native_sequence_gap(bridge):
    call = bridge.subscribe()
    await bridge.subscribed()
    await bridge.publish(10)
    assert (await read(call)).sequence_number == 10
    await bridge.pub.send_multipart([b"kv", b"short"])
    await bridge.publish(11, b"undecodable")
    await bridge.publish(12)
    assert (await read(call)).sequence_number == 12
    call.cancel()


@pytest_asyncio.fixture
async def replay(bridge):
    socket = bridge.ctx.socket(zmq.ROUTER)
    port = socket.bind_to_random_port("tcp://127.0.0.1")
    bridge.config.replay_endpoint = f"tcp://127.0.0.1:{port}"

    async def request(cursor):
        frames = await asyncio.wait_for(socket.recv_multipart(), 3)
        assert frames[1:] == [b"", (cursor + 1).to_bytes(8, "big")]
        return frames[0]

    async def send(identity, seq, payload=None):
        wire_seq = transport._END_SEQ if seq == -1 else seq.to_bytes(8, "big")
        await socket.send_multipart(
            [identity, b"", wire_seq, str(seq).encode() if payload is None else payload]
        )

    try:
        yield SimpleNamespace(request=request, send=send, socket=socket)
    finally:
        socket.close(linger=0)


@pytest.mark.asyncio
async def test_replay_handoff_keeps_order_and_deduplicates_live_overlap(bridge, replay):
    call = bridge.subscribe(100)
    identity = await replay.request(100)
    await bridge.subscribed()
    # Queue both overlap and new live traffic while historical replay runs.
    for seq in (101, 102, 103):
        await bridge.publish(seq)
    for seq in (101, 102):
        await replay.send(identity, seq)
        batch = await read(call)
        assert (batch.sequence_number, batch.timestamp) == (seq, seq)
    await replay.send(identity, -1)
    assert (await read(call)).sequence_number == 103
    await bridge.publish(104)
    assert (await read(call)).sequence_number == 104
    assert bridge.cursors == [100]
    call.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("first", [-1, 90, 103])
async def test_empty_or_expired_history_requires_fresh_subscription(bridge, replay, first):
    call = bridge.subscribe(100)
    identity = await replay.request(100)
    await replay.send(identity, first)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(call)
    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE
    fresh = bridge.subscribe()
    await bridge.subscribed()
    # The failed call also subscribed; drain its subscribe/unsubscribe so the
    # new live subscriber is established before sending.
    await bridge.subscribed()
    await bridge.publish(120)
    assert (await read(fresh)).sequence_number == 120
    fresh.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["gap", "decode", "timeout", "malformed"])
async def test_partial_replay_failure_signals_data_loss(bridge, replay, monkeypatch, fault):
    monkeypatch.setattr(transport, "_REPLAY_TIMEOUT_MS", 100)
    call = bridge.subscribe(100)
    identity = await replay.request(100)
    await replay.send(identity, 101)
    assert (await read(call)).sequence_number == 101
    if fault == "gap":
        await replay.send(identity, 103)
    elif fault == "decode":
        await replay.send(identity, 102, b"bad")
    elif fault == "malformed":
        await replay.socket.send_multipart([identity, b"", b"bad", b""])
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(call)
    assert error.value.code() == grpc.StatusCode.DATA_LOSS


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["timeout", "malformed", "decode"])
async def test_replay_failure_before_headers_signals_out_of_range(
    bridge, replay, monkeypatch, fault
):
    monkeypatch.setattr(transport, "_REPLAY_TIMEOUT_MS", 100)
    call = bridge.subscribe(100)
    identity = await replay.request(100)
    if fault == "malformed":
        await replay.socket.send_multipart([identity, b"", b"bad", b""])
    elif fault == "decode":
        await replay.send(identity, 101, b"bad")
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(call)
    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE


@pytest.mark.asyncio
async def test_zero_cursor_does_not_replay_old_history(bridge, replay):
    call = bridge.subscribe()
    await bridge.subscribed()
    await bridge.publish(7)
    assert (await read(call)).sequence_number == 7
    assert not await replay.socket.poll(timeout=50)
    call.cancel()


@pytest.mark.asyncio
async def test_cancellation_during_replay_releases_live_subscription(bridge, replay):
    call = bridge.subscribe(100)
    await replay.request(100)
    await bridge.subscribed()
    call.cancel()
    assert await asyncio.wait_for(bridge.pub.recv(), 3) == b"\x00kv"


@pytest.mark.asyncio
async def test_live_gap_after_replay_remains_visible_for_next_recovery(bridge, replay):
    call = bridge.subscribe(100)
    identity = await replay.request(100)
    await bridge.subscribed()
    await replay.send(identity, 101)
    await replay.send(identity, -1)
    assert (await read(call)).sequence_number == 101
    await bridge.publish(103)
    assert (await read(call)).sequence_number == 103
    call.cancel()


@pytest.mark.asyncio
async def test_invalid_replay_endpoint_falls_back_without_retaining_cursor(bridge):
    bridge.config.replay_endpoint = "invalid://endpoint"
    call = bridge.subscribe(100)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(call)
    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE


@pytest.mark.asyncio
async def test_decode_failure_closes_both_sockets(bridge, replay, monkeypatch):
    ctx = zmq.asyncio.Context.instance()
    sockets = []

    def socket(kind):
        result = ctx.socket(kind)
        sockets.append(result)
        return result

    monkeypatch.setattr(zmq.asyncio.Context, "instance", lambda: SimpleNamespace(socket=socket))
    call = bridge.subscribe(100)
    identity = await replay.request(100)
    await replay.send(identity, 101, b"bad")
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await read(call)
    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE
    assert len(sockets) == 2
    assert all(socket.closed for socket in sockets)


@pytest.mark.asyncio
@pytest.mark.parametrize("hwm", [None, 4096, 0])
async def test_live_backlog_survives_replay_with_publisher_hwm(bridge, replay, monkeypatch, hwm):
    if hwm is not None:
        bridge.config.hwm = hwm
    # In-process transport makes queue capacity deterministic, without TCP
    # kernel buffers masking a too-small SUB HWM. Limit the sender's share.
    bridge.pub.setsockopt(zmq.SNDHWM, 1)
    bridge.config.endpoint = "inproc://kv-replay-backlog"
    bridge.pub.bind(bridge.config.endpoint)
    monkeypatch.setattr(zmq.asyncio.Context, "instance", lambda: bridge.ctx)

    call = bridge.subscribe(100)
    identity = await replay.request(100)
    await bridge.subscribed()
    await replay.send(identity, 101)
    assert (await read(call)).sequence_number == 101
    # Keep replay open while more than the default 1000 live batches queue.
    for seq in range(102, 2150):
        await bridge.publish(seq)
    await replay.send(identity, -1)
    for seq in range(102, 2150):
        assert (await read(call)).sequence_number == seq
    call.cancel()
