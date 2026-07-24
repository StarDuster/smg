//! Prefill/Decode (PD) routing integration tests
//!
//! Tests for prefill-decode disaggregation routing mode.

use axum::{
    body::{to_bytes, Body},
    extract::Request,
    http::{header::CONTENT_TYPE, StatusCode},
};
use openai_protocol::{
    model_card::ModelCard,
    models::ListModelsResponse,
    worker::{WorkerInfo, WorkerSpec, WorkerStatus, WorkerType as ProtocolWorkerType},
};
use serde_json::json;
use smg::config::RouterConfig;
use tower::ServiceExt;

use crate::common::{
    mock_worker::{HealthStatus, MockWorkerConfig, WorkerType},
    AppTestContext, TestWorkerConfig,
};

#[cfg(test)]
mod pd_routing_tests {
    use super::*;

    const CANONICAL_MODEL: &str = "GLM-5.2";
    const MODEL_ALIAS: &str = "GLM-5.2-Coding";

    fn worker_spec(url: &str, worker_type: ProtocolWorkerType) -> WorkerSpec {
        let mut spec = WorkerSpec::new(url);
        spec.models = vec![ModelCard::new(CANONICAL_MODEL).with_alias(MODEL_ALIAS)].into();
        spec.worker_type = worker_type;
        spec.health.disable_health_check = Some(true);
        spec
    }

    async fn put_worker(app: &axum::Router, worker_id: &str, spec: &WorkerSpec) {
        let req = Request::builder()
            .method("PUT")
            .uri(format!("/workers/{worker_id}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(spec).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "PUT /workers/{{id}} should enqueue the full worker replacement"
        );
    }

    async fn get_worker(app: &axum::Router, worker_id: &str) -> WorkerInfo {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/workers/{worker_id}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn wait_for_worker_alias(app: &axum::Router, worker_id: &str) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);

        loop {
            let info = get_worker(app, worker_id).await;
            let has_alias = info
                .spec
                .models
                .find(CANONICAL_MODEL)
                .is_some_and(|card| card.aliases.iter().any(|alias| alias == MODEL_ALIAS));
            if info.status == Some(WorkerStatus::Ready) && has_alias {
                return;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "worker {worker_id} did not become Ready with alias {MODEL_ALIAS}; last response: {info:?}"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    /// Test basic PD mode routing with prefill and decode workers
    #[tokio::test]
    async fn test_pd_mode_basic_routing() {
        let mut config = RouterConfig::builder()
            .prefill_decode_mode(
                vec![
                    ("http://127.0.0.1:19800".to_string(), None),
                    ("http://127.0.0.1:19801".to_string(), None),
                ],
                vec![
                    "http://127.0.0.1:19802".to_string(),
                    "http://127.0.0.1:19803".to_string(),
                ],
            )
            .power_of_two_policy(1)
            .host("127.0.0.1")
            .port(3800)
            .max_payload_size(256 * 1024 * 1024)
            .request_timeout_secs(600)
            .worker_startup_timeout_secs(5)
            .worker_startup_check_interval_secs(1)
            .max_concurrent_requests(64)
            .queue_timeout_secs(60)
            .build_unchecked();
        config.health_check.disable_health_check = true;

        // Note: For PD mode tests, we need to start prefill and decode workers separately
        // The test context will need to handle this specially
        let ctx = AppTestContext::new_with_config(
            config,
            vec![
                // Prefill workers
                TestWorkerConfig::prefill(19800),
                TestWorkerConfig::prefill(19801),
                // Decode workers
                TestWorkerConfig::decode(19802),
                TestWorkerConfig::decode(19803),
            ],
        )
        .await;

        let app = ctx.create_app();

        // Send requests and verify they succeed
        for i in 0..10 {
            let payload = json!({
                "text": format!("PD mode request {}", i),
                "stream": false
            });

            let req = Request::builder()
                .method("POST")
                .uri("/generate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&payload).unwrap()))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "PD mode request should succeed"
            );
        }

        ctx.shutdown().await;
    }

    /// Test PD mode with round robin policy
    #[tokio::test]
    async fn test_pd_mode_round_robin() {
        let mut config = RouterConfig::builder()
            .prefill_decode_mode(
                vec![("http://127.0.0.1:19810".to_string(), None)],
                vec![
                    "http://127.0.0.1:19811".to_string(),
                    "http://127.0.0.1:19812".to_string(),
                ],
            )
            .round_robin_policy()
            .host("127.0.0.1")
            .port(3801)
            .max_payload_size(256 * 1024 * 1024)
            .request_timeout_secs(600)
            .worker_startup_timeout_secs(5)
            .worker_startup_check_interval_secs(1)
            .max_concurrent_requests(64)
            .queue_timeout_secs(60)
            .build_unchecked();
        config.health_check.disable_health_check = true;

        let ctx = AppTestContext::new_with_config(
            config,
            vec![
                TestWorkerConfig::prefill(19810),
                TestWorkerConfig::decode(19811),
                TestWorkerConfig::decode(19812),
            ],
        )
        .await;

        let app = ctx.create_app();
        let mut success_count = 0;

        for i in 0..20 {
            let payload = json!({
                "text": format!("PD round robin {}", i),
                "stream": false
            });

            let req = Request::builder()
                .method("POST")
                .uri("/generate")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_string(&payload).unwrap()))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            if resp.status() == StatusCode::OK {
                success_count += 1;
            }
        }

        assert_eq!(
            success_count, 20,
            "All requests should succeed in PD mode with round robin"
        );

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn test_pd_model_alias_via_worker_put() {
        let prefill_url = "http://127.0.0.1:19840".to_string();
        let decode_url = "http://127.0.0.1:19841".to_string();

        let mut config = RouterConfig::builder()
            .prefill_decode_mode(vec![(prefill_url.clone(), None)], vec![decode_url.clone()])
            .round_robin_policy()
            .host("127.0.0.1")
            .port(3804)
            .max_payload_size(256 * 1024 * 1024)
            .request_timeout_secs(600)
            .worker_startup_timeout_secs(5)
            .worker_startup_check_interval_secs(1)
            .max_concurrent_requests(64)
            .queue_timeout_secs(60)
            .build_unchecked();
        config.health_check.disable_health_check = true;

        let ctx = AppTestContext::new_with_config(
            config,
            vec![
                TestWorkerConfig::prefill(19840),
                TestWorkerConfig::decode(19841),
            ],
        )
        .await;
        let app = ctx.create_app();

        let prefill_id = ctx
            .app_context
            .worker_registry
            .get_id_by_url(&prefill_url)
            .unwrap();
        let decode_id = ctx
            .app_context
            .worker_registry
            .get_id_by_url(&decode_url)
            .unwrap();

        let prefill_before = get_worker(&app, prefill_id.as_str()).await;
        let decode_before = get_worker(&app, decode_id.as_str()).await;
        assert_eq!(prefill_before.status, Some(WorkerStatus::Ready));
        assert_eq!(decode_before.status, Some(WorkerStatus::Ready));
        assert!(prefill_before.spec.models.find(MODEL_ALIAS).is_none());
        assert!(decode_before.spec.models.find(MODEL_ALIAS).is_none());

        put_worker(
            &app,
            prefill_id.as_str(),
            &worker_spec(&prefill_url, ProtocolWorkerType::Prefill),
        )
        .await;
        put_worker(
            &app,
            decode_id.as_str(),
            &worker_spec(&decode_url, ProtocolWorkerType::Decode),
        )
        .await;

        wait_for_worker_alias(&app, prefill_id.as_str()).await;
        wait_for_worker_alias(&app, decode_id.as_str()).await;

        let mut conflicting_spec = worker_spec(&prefill_url, ProtocolWorkerType::Prefill);
        conflicting_spec.models =
            vec![ModelCard::new("replacement-model").with_alias("replacement-alias")].into();
        let duplicate_post = Request::builder()
            .method("POST")
            .uri("/workers")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&conflicting_spec).unwrap()))
            .unwrap();
        let duplicate_response = app.clone().oneshot(duplicate_post).await.unwrap();
        assert_eq!(duplicate_response.status(), StatusCode::CONFLICT);

        let prefill_after_conflict = get_worker(&app, prefill_id.as_str()).await;
        assert!(prefill_after_conflict
            .spec
            .models
            .find(CANONICAL_MODEL)
            .is_some());
        assert!(prefill_after_conflict
            .spec
            .models
            .find("replacement-model")
            .is_none());

        let alias_request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "model": MODEL_ALIAS,
                    "messages": [{"role": "user", "content": "Hello"}],
                    "stream": false
                })
                .to_string(),
            ))
            .unwrap();
        let alias_response = app.clone().oneshot(alias_request).await.unwrap();
        assert_eq!(alias_response.status(), StatusCode::OK);

        let unknown_request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "model": "GLM-5.2-Unknown",
                    "messages": [{"role": "user", "content": "Hello"}],
                    "stream": false
                })
                .to_string(),
            ))
            .unwrap();
        let unknown_response = app.clone().oneshot(unknown_request).await.unwrap();
        assert!(
            !unknown_response.status().is_success(),
            "an unknown model must not be routed"
        );

        let models_request = Request::builder()
            .method("GET")
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap();
        let models_response = app.clone().oneshot(models_request).await.unwrap();
        assert_eq!(models_response.status(), StatusCode::OK);
        let models_body = to_bytes(models_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let models: ListModelsResponse = serde_json::from_slice(&models_body).unwrap();
        assert_eq!(models.data.len(), 1);
        assert_eq!(models.data[0].id, CANONICAL_MODEL);

        ctx.shutdown().await;
    }

    /// A non-streaming PD request must emit the SMG-only PD metrics, including
    /// the honest `smg_pd_ttft_seconds`. Runs on a current-thread runtime so the
    /// thread-local Prometheus recorder captures emissions from the request path.
    #[test]
    fn test_pd_metrics_emitted_on_request() {
        use metrics_exporter_prometheus::PrometheusBuilder;

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut config = RouterConfig::builder()
                    .prefill_decode_mode(
                        vec![("http://127.0.0.1:19830".to_string(), None)],
                        vec!["http://127.0.0.1:19831".to_string()],
                    )
                    .round_robin_policy()
                    .host("127.0.0.1")
                    .port(3803)
                    .max_payload_size(256 * 1024 * 1024)
                    .request_timeout_secs(600)
                    .worker_startup_timeout_secs(5)
                    .worker_startup_check_interval_secs(1)
                    .max_concurrent_requests(64)
                    .queue_timeout_secs(60)
                    .build_unchecked();
                config.health_check.disable_health_check = true;

                let ctx = AppTestContext::new_with_config(
                    config,
                    vec![
                        TestWorkerConfig::prefill(19830),
                        TestWorkerConfig::decode(19831),
                    ],
                )
                .await;

                let app = ctx.create_app();
                let payload = json!({ "text": "PD metrics request", "stream": false });
                let req = Request::builder()
                    .method("POST")
                    .uri("/generate")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_string(&payload).unwrap()))
                    .unwrap();

                let resp = app.oneshot(req).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK, "PD request should succeed");

                ctx.shutdown().await;
            });
        });

        let rendered = handle.render();
        assert!(
            rendered.contains("smg_pd_prefill_duration_seconds_count"),
            "smg_pd_prefill_duration_seconds not emitted; rendered:\n{rendered}"
        );
        assert!(
            rendered.contains("smg_pd_ttft_seconds_count"),
            "smg_pd_ttft_seconds not emitted; rendered:\n{rendered}"
        );
    }

    /// Test PD mode handles worker failures gracefully
    #[tokio::test]
    async fn test_pd_mode_with_failing_decode_worker() {
        use smg::config::RetryConfig;

        let mut config = RouterConfig::builder()
            .prefill_decode_mode(
                vec![("http://127.0.0.1:19820".to_string(), None)],
                vec![
                    "http://127.0.0.1:19821".to_string(),
                    "http://127.0.0.1:19822".to_string(),
                ],
            )
            .round_robin_policy()
            .host("127.0.0.1")
            .port(3802)
            .max_payload_size(256 * 1024 * 1024)
            .request_timeout_secs(600)
            .worker_startup_timeout_secs(5)
            .worker_startup_check_interval_secs(1)
            .max_concurrent_requests(64)
            .queue_timeout_secs(60)
            .retry_config(RetryConfig {
                max_retries: 3,
                initial_backoff_ms: 10,
                max_backoff_ms: 50,
                ..Default::default()
            })
            .build_unchecked();
        config.health_check.disable_health_check = true;

        let ctx = AppTestContext::new_with_config(
            config,
            vec![
                TestWorkerConfig::prefill(19820),
                MockWorkerConfig {
                    port: 19821,
                    worker_type: WorkerType::Decode,
                    health_status: HealthStatus::Healthy,
                    response_delay_ms: 0,
                    fail_rate: 1.0, // Failing decode worker
                },
                TestWorkerConfig::decode(19822), // Healthy decode worker
            ],
        )
        .await;

        let app = ctx.create_app();

        // Request should succeed via retry to healthy decode worker
        let payload = json!({
            "text": "Test with failing decode worker",
            "stream": false
        });

        let req = Request::builder()
            .method("POST")
            .uri("/generate")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Request should succeed via retry to healthy decode worker"
        );

        ctx.shutdown().await;
    }
}
