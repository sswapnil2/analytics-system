//! Two control planes on one metadata DB must deliver each alert once (scalability review R-05).
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::routing::post;
use axum::Router;
use pulse_control::{build_control_router, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_storage::{build_from_url, EncryptedObjectStore, StorageConfig, StorageEngine};
use serde_json::json;
use tower::ServiceExt;

const ROOT: &str = "sk_root_test_alert_claims_0123456789";
const INGEST_KEY: &str = "pk_test_alert_claims_ingest_0123456789";

/// One node: its own storage, the metadata DB in `dir` shared with its peers.
async fn node(dir: &std::path::Path) -> Arc<ControlPlaneService> {
    let enc_store = Arc::new(EncryptedObjectStore::new(
        build_from_url("memory://").unwrap(),
    ));
    let id_router = Arc::new(IdentityRouter::new(enc_store.as_object_store()));
    let storage = StorageEngine::new_with_encrypted(
        StorageConfig {
            num_buckets: 2,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        enc_store,
        id_router,
    );
    let qengine = QueryEngine::new(storage, 4 * 1024 * 1024, None, 0, CatalogRegistry::new())
        .await
        .unwrap();
    let meta = MetaStore::connect_auto(dir).await.unwrap();
    ControlPlaneService::new_with_admin(meta, qengine, "proj_claims", INGEST_KEY, Some(ROOT.into()))
        .await
        .unwrap()
}

async fn hook(State(hits): State<Arc<AtomicUsize>>) {
    hits.fetch_add(1, Ordering::SeqCst);
}

#[tokio::test]
async fn two_nodes_deliver_one_webhook_per_alert_trigger() {
    std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1");
    std::env::set_var("PULSE_ALLOW_EPHEMERAL_KEK", "1");
    let dir = tempfile::tempdir().unwrap();
    let a = node(dir.path()).await;
    let b = node(dir.path()).await;

    let hits = Arc::new(AtomicUsize::new(0));
    let hook_app = Router::new()
        .route("/hook", post(hook))
        .with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, hook_app).await.unwrap() });

    // No events exist, so "fewer than 1" is true on both nodes.
    let app = build_control_router(a.clone());
    for (method, uri, body) in [
        (
            "PUT",
            "/v1/projects/proj_claims/byok",
            json!({ "category": "webhook", "provider": "custom_webhook", "secret": url,
                    "config_json": { "signing_secret": "claims_signing_secret_0123" } }),
        ),
        (
            "POST",
            "/v1/projects/proj_claims/alerts",
            json!({ "name": "No signups", "event_name": "Signed_Up", "operator": "lt", "threshold": 1.0 }),
        ),
    ] {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {ROOT}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(status, 200, "{uri}: {}", String::from_utf8_lossy(&body));
    }

    tokio::join!(a.evaluate_alerts(), b.evaluate_alerts());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "both nodes saw the condition; exactly one may deliver"
    );
}
