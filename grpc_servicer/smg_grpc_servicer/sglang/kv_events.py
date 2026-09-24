"""SGLang KV-event transport, independent of engine imports."""

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import aclosing

import grpc
import zmq
import zmq.asyncio
from smg_grpc_proto.generated import common_pb2

from smg_grpc_servicer.kv_events import endpoint_for_rank

logger = logging.getLogger(__name__)
_REPLAY_TIMEOUT_MS = 5000
_END_SEQ = (-1).to_bytes(8, "big", signed=True)


class ReplayUnavailable(Exception):
    """The publisher cannot provide a contiguous, decodable replay."""


async def replay_frames(endpoint: str, cursor: int) -> AsyncIterator[tuple[int, bytes]]:
    """Read SGLang's ROUTER replay protocol through a DEALER socket.

    The publisher accepts an inclusive start sequence and ends with -1.
    Request the first missing batch, verifying continuity before forwarding.
    """
    replay = zmq.asyncio.Context.instance().socket(zmq.DEALER)
    try:
        replay.connect(endpoint_for_rank(endpoint, 0))
        await replay.send_multipart([b"", (cursor + 1).to_bytes(8, "big")])
        expected = cursor + 1
        received_any = False
        while True:
            if not await replay.poll(timeout=_REPLAY_TIMEOUT_MS):
                raise ReplayUnavailable("KV event replay timed out")
            frames = await replay.recv_multipart()
            if len(frames) != 3 or frames[0] != b"" or len(frames[1]) != 8:
                raise ReplayUnavailable("Malformed KV event replay response")
            if frames[1] == _END_SEQ:
                # Empty replay cannot distinguish an idle publisher from a
                # restarted publisher whose counter is behind our cursor.
                if not received_any:
                    raise ReplayUnavailable("KV event replay history is unavailable")
                return
            seq = int.from_bytes(frames[1], "big")
            if seq != expected:
                raise ReplayUnavailable(
                    f"KV event replay is incomplete: expected {expected}, received {seq}"
                )
            yield seq, frames[2]
            received_any = True
            expected += 1
    except zmq.ZMQError as exc:
        raise ReplayUnavailable("KV event replay transport failed") from exc
    finally:
        replay.close(linger=0)


async def subscribe_kv_events(
    config: object,
    start_sequence_number: int,
    context: grpc.aio.ServicerContext,
    decode: Callable[[bytes], object],
    convert: Callable[[object, int], common_pb2.KvEventBatch],
) -> AsyncIterator[common_pb2.KvEventBatch]:
    """Replay a resume cursor when configured, then forward live events.

    Unrecoverable replay clears stale gateway mappings via OUT_OF_RANGE
    before headers, or DATA_LOSS once streaming. Zero starts live rebuilding,
    not a full cache snapshot.
    """
    replay_endpoint = getattr(config, "replay_endpoint", None)
    if start_sequence_number and (not replay_endpoint or start_sequence_number == 2**64 - 1):
        await context.abort(
            grpc.StatusCode.OUT_OF_RANGE,
            "KV event replay is unavailable; resubscribe with zero for live events",
        )
        return

    # Each DP rank has independent sequence numbers. Keep the existing rank-0
    # subscription until the gateway supports per-rank indexes.
    endpoint = endpoint_for_rank(config.endpoint, 0)
    sub = zmq.asyncio.Context.instance().socket(zmq.SUB)
    sent_headers = False
    replayed_seq = None
    try:
        sub.setsockopt(zmq.RCVHWM, getattr(config, "hwm", 100_000))
        sub.subscribe(config.topic.encode("utf-8"))
        sub.connect(endpoint)
        logger.info("SubscribeKvEvents: connected to ZMQ endpoint %s", endpoint)
        # Subscribe before replay so live events can queue during recovery.
        # A PUB/SUB join race still surfaces as a native gap for another replay.
        if start_sequence_number:
            async with aclosing(replay_frames(replay_endpoint, start_sequence_number)) as frames:
                async for seq, payload in frames:
                    try:
                        batch = convert(decode(payload), seq)
                    except Exception as exc:
                        raise ReplayUnavailable("Failed to decode KV event replay") from exc
                    if not sent_headers:
                        await context.send_initial_metadata(())
                        sent_headers = True
                    yield batch
                    replayed_seq = seq
        if not sent_headers:
            await context.send_initial_metadata(())
            sent_headers = True
        while not context.cancelled():
            # Cancelling recv_multipart on an idle timeout can lose a message.
            if not await sub.poll(timeout=1000):
                continue
            frames = await sub.recv_multipart()
            if len(frames) < 3:
                continue
            seq = int.from_bytes(frames[1], "big")
            if replayed_seq is not None and seq <= replayed_seq:
                continue
            try:
                batch = decode(frames[2])
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to decode KV event batch: %s", exc)
                continue
            yield convert(batch, seq)
    except ReplayUnavailable as exc:
        await context.abort(
            grpc.StatusCode.DATA_LOSS if sent_headers else grpc.StatusCode.OUT_OF_RANGE,
            str(exc),
        )
    finally:
        sub.close(linger=0)
        logger.info("SubscribeKvEvents: stream closed")
