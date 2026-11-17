use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use chrono::Utc;
use object_store::ObjectStore;
use pulse_control::{
    build_control_router, sign_license_ed25519, CommercialFeature, ControlPlaneService,
    EditionTier, LicenseClaims, MetaStore,
};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, IngestService};
use pulse_storage::{build_from_url, EncryptedObjectStore, StorageConfig, StorageEngine};
use pulse_wire::{InteractionKind, ReplayChunk, WireframeOp};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tower::ServiceExt;

const ROOT: &str = "sk_root_test_byok_license_0123456789";
const INGEST_KEY: &str = "pk_test_byok_license_key_0123456789";

struct TestVendorKeys {
    pkcs8: Vec<u8>,
}

impl TestVendorKeys {
    fn generate() -> Self {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let pubkey_hex = hex::encode(key_pair.public_key().as_ref());
        std::env::set_var("PULSE_LICENSE_PUBKEY_HEX", pubkey_hex);
        Self {
            pkcs8: pkcs8.as_ref().to_vec(),
        }
    }

    fn mint(&self, org: &str, expires_at_ms: i64) -> String {
        let claims = LicenseClaims {
            license_id: format!("lic_test_{}", Utc::now().timestamp_millis()),
            customer_org: org.to_string(),
            edition: EditionTier::CommercialByok,
            features: CommercialFeature::all_names(),
            issued_at_ms: Utc::now().timestamp_millis() - 1000,
            expires_at_ms,
        };
        sign_license_ed25519(&self.pkcs8, &claims).unwrap()
    }
}

async fn setup_full(
    tmp: &std::path::Path,
    admin_token: &str,
    raw_store_opt: Option<Arc<dyn object_store::ObjectStore>>,
) -> (
    Router,
    Router,
    Arc<ControlPlaneService>,
    Arc<StorageEngine>,
    Arc<EncryptedObjectStore>,
) {
    std::env::set_var("PULSE_ADMIN_TOKEN", admin_token);
    std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1");
    std::env::set_var("PULSE_ALLOW_EPHEMERAL_KEK", "1");

    let raw_store = raw_store_opt.unwrap_or_else(|| build_from_url("memory://").unwrap());
    let enc_store = Arc::new(EncryptedObjectStore::new(raw_store));
    let id_router = Arc::new(IdentityRouter::new(enc_store.as_object_store()));
    let storage = StorageEngine::new_with_encrypted(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        enc_store.clone(),
        id_router,
    );
    let qengine = QueryEngine::new(
        storage.clone(),
        16 * 1024 * 1024,
        None,
        0,
        CatalogRegistry::new(),
    )
    .await
    .unwrap();
    let meta = MetaStore::connect_auto(tmp).await.unwrap();
    let ctrl = ControlPlaneService::new_with_admin(
        meta,
        qengine,
        "proj_byok",
        INGEST_KEY,
        Some(admin_token.to_string()),
    )
    .await
    .unwrap();
    let ingest = IngestService::new(storage.clone(), ctrl.clone());
    ctrl.attach_ingest(ingest.clone()).await;

    (
        build_control_router(ctrl.clone()),
        build_ingest_router(ingest),
        ctrl,
        storage,
        enc_store,
    )
}

async fn send_admin_with_headers(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    token: &str,
    extra_headers: &[(&str, &str)],
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let body_bytes = if let Some(val) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(val.to_string())
    } else {
        Body::empty()
    };
    let resp = app
        .clone()
        .oneshot(builder.body(body_bytes).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 4 * 1024 * 1024).await.unwrap();
    (status, bytes.to_vec())
}

async fn send_admin_json(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let (status, raw) = send_admin_with_headers(app, method, uri, body, ROOT, &[]).await;
    let json = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    (status, json)
}

async fn send_ingest(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("x-pulse-key", INGEST_KEY)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 2 * 1024 * 1024).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ---------------- Local Mock Webhook Receiver ----------------
#[derive(Clone)]
struct MockWebhookState {
    received_count: Arc<AtomicUsize>,
    last_sig: Arc<parking_lot::Mutex<Option<String>>>,
}

async fn webhook_receiver(
    State(state): State<MockWebhookState>,
    headers: HeaderMap,
    _body: axum::body::Bytes,
) -> impl IntoResponse {
    let sig = headers
        .get("x-pulse-signature-256")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    *state.last_sig.lock() = sig;
    state.received_count.fetch_add(1, Ordering::Relaxed);
    StatusCode::OK
}

// ---------------- Local Mock LLM Provider Server ----------------
async fn mock_llm_handler(axum::Json(req): axum::Json<Value>) -> impl IntoResponse {
    let q = req["messages"][0]["content"]
        .as_str()
        .or_else(|| req["prompt"].as_str())
        .unwrap_or("dropoff analysis");
    axum::Json(json!({
        "id": "chatcmpl_mock_123",
        "choices": [{
            "message": {
                "role": "assistant",
                "content": format!("Analyzed via BYOK LLM: Drop-off detected at App_Open -> KYC_PAN_Submitted. Query: {q}")
            }
        }]
    }))
}

#[tokio::test]
async fn test_full_oss_and_commercial_suite() {
    let vendor = TestVendorKeys::generate();
    let tmp = tempfile::tempdir().unwrap();
    let (ctrl_app, ingest_app, _ctrl, storage, enc_store) =
        setup_full(tmp.path(), ROOT, None).await;

    // =========================================================================
    // 1. OPEN-SOURCE ENCRYPTION AT REST (Available in Community without license)
    // =========================================================================
    let (st, enc_status) = send_admin_json(&ctrl_app, "GET", "/v1/system/encryption", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(enc_status["status"]["enabled"], false);
    assert_eq!(enc_status["has_durable_kek"], true);

    // Rotate encryption master key K1 using Argon2id passphrase
    let k1_passphrase = "mumbai-vault-passphrase-alpha-2026";
    let (st, rot_res) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/system/encryption/rotate",
        Some(json!({ "secret": k1_passphrase })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rot_res["status"]["enabled"], true);
    let k1_id = rot_res["status"]["active_key_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(rot_res["status"]["keyring_size"], 1);

    // Ingest events under K1 and flush to columnar `.pvtx` segment
    let now_ms = Utc::now().timestamp_millis();
    let events_batch_1 = json!({
        "events": [
            {
                "event_name": "App_Open",
                "distinct_id": "u_k1_1",
                "session_id": "sess_k1_01",
                "ts_ms": now_ms - 10_000,
                "properties": { "plan": "pro" }
            },
            {
                "event_name": "KYC_PAN_Submitted",
                "distinct_id": "u_k1_1",
                "session_id": "sess_k1_01",
                "ts_ms": now_ms - 8_000,
                "properties": { "plan": "pro" }
            }
        ]
    });
    let (st, ing1) = send_ingest(&ingest_app, "/v1/ingest", events_batch_1).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ing1["accepted"], 2);

    let replay_k1 = ReplayChunk {
        project_id: "proj_byok".to_string(),
        session_id: "sess_k1_01".to_string(),
        chunk_start_ts_ms: now_ms - 10_000,
        ops: vec![WireframeOp::PointerInteraction {
            ts_offset_ms: 500,
            x: 120,
            y: 340,
            kind: InteractionKind::RageTap { count: 3 },
        }],
    };
    let rep_res1 = storage.append_replay_chunk(&replay_k1).await.unwrap();
    storage.flush_all_projects().await.unwrap();

    // Verify raw bytes in object store are ENCRYPTED with AES-256-GCM envelope magic (PLSEAGM1)
    let raw_store = enc_store.raw_inner();
    let rep_path: object_store::path::Path = rep_res1.object_path.as_str().into();
    let raw_rep_head = raw_store.get_range(&rep_path, 0..8).await.unwrap();
    assert_eq!(raw_rep_head.as_ref(), b"PLSEAGM1");

    // Bit-flip tamper check: modifying ciphertext in raw store causes AES-256-GCM authentication failure!
    let tampered_raw = {
        let mut full = raw_store
            .get_range(&rep_path, 0..200)
            .await
            .unwrap()
            .to_vec();
        full[110] ^= 0xFF; // flip bit in first chunk
        full
    };
    let tampered_path: object_store::path::Path = "tamper_test.plsr".into();
    raw_store
        .put(
            &tampered_path,
            object_store::PutPayload::from_bytes(bytes::Bytes::from(tampered_raw)),
        )
        .await
        .unwrap();
    let tamper_read = enc_store.get_range(&tampered_path, 0..16).await;
    assert!(
        tamper_read.is_err(),
        "tampered ciphertext chunk must fail AES-256-GCM authentication"
    );

    // =========================================================================
    // 2. KEY ROTATION (K1 -> K2) & MULTI-KEY TRANSPARENT READ
    // =========================================================================
    let k2_passphrase = "mumbai-vault-passphrase-beta-2027";
    let (st, rot_res2) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/system/encryption/rotate",
        Some(json!({ "secret": k2_passphrase })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let k2_id = rot_res2["status"]["active_key_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(k1_id, k2_id);
    assert_eq!(rot_res2["status"]["keyring_size"], 2);

    // Ingest events under K2
    let events_batch_2 = json!({
        "events": [
            {
                "event_name": "App_Open",
                "distinct_id": "u_k2_1",
                "session_id": "sess_k2_01",
                "ts_ms": now_ms - 2_000,
                "properties": { "plan": "enterprise" }
            },
            {
                "event_name": "KYC_PAN_Submitted",
                "distinct_id": "u_k2_1",
                "session_id": "sess_k2_01",
                "ts_ms": now_ms - 1_000,
                "properties": { "plan": "enterprise" }
            }
        ]
    });
    let (st, ing2) = send_ingest(&ingest_app, "/v1/ingest", events_batch_2).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ing2["accepted"], 2);
    storage.flush_all_projects().await.unwrap();

    // Query across both K1 and K2 segments: seamlessly decrypts both!
    let (st, funnel) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/query/funnel",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 30_000, "end_ts_ms": now_ms + 30_000 },
            "steps": [
                { "event_name": "App_Open" },
                { "event_name": "KYC_PAN_Submitted" }
            ],
            "conversion_window_ms": 86_400_000
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(funnel["overall_steps"][0]["entered_users"], 2); // u_k1_1 + u_k2_1
    assert_eq!(funnel["overall_steps"][1]["entered_users"], 2);

    // =========================================================================
    // 3. OFFLINE ED25519 LICENSE GATE & 14-DAY GRACE PERIOD VERIFICATION
    // =========================================================================
    // Self-serve trial minting is REJECTED
    let (st, trial_err) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/license",
        Some(json!({ "trial": true })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(trial_err["message"]
        .as_str()
        .unwrap()
        .contains("trial key minting is disabled"));

    // Tampered Ed25519 signature is REJECTED
    let valid_lic_key = vendor.mint("Acme Fintech VPC", now_ms + 30 * 86_400_000);
    let mut tampered_key = valid_lic_key.clone();
    tampered_key.push('A');
    let (st, _) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/license",
        Some(json!({ "license_key": tampered_key })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Expired license outside 14-day grace period is REJECTED
    let ancient_key = vendor.mint("Acme Expired", now_ms - 20 * 86_400_000);
    let (st, _) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/license",
        Some(json!({ "license_key": ancient_key })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Expired license WITHIN 14-day grace period is ACCEPTED with GracePeriod health
    let grace_key = vendor.mint("Acme Grace Corp", now_ms - 2 * 86_400_000);
    let (st, grace_res) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/license",
        Some(json!({ "license_key": grace_key })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(grace_res["health"], "grace_period");
    assert_eq!(grace_res["in_grace_period"], true);
    assert_eq!(grace_res["is_commercial"], true);

    // Activate fresh valid license
    let (st, lic_ok) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/license",
        Some(json!({ "license_key": valid_lic_key })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(lic_ok["health"], "active");
    assert_eq!(lic_ok["in_grace_period"], false);

    // =========================================================================
    // 4. BYOK VAULT (SSRF Protection & Config Sanitization)
    // =========================================================================
    // file:// URLs are rejected by SSRF guard
    let (st, ssrf_err) = send_admin_json(
        &ctrl_app,
        "PUT",
        "/v1/projects/proj_byok/byok",
        Some(json!({
            "category": "storage",
            "provider": "s3_compatible",
            "secret": "s3secret12345678",
            "config_json": { "url": "file:///etc/passwd" }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(ssrf_err["message"].as_str().unwrap().contains("SSRF"));

    // Save S3 BYOC storage credential with memory:// test bucket
    let (st, cred_res) = send_admin_json(
        &ctrl_app,
        "PUT",
        "/v1/projects/proj_byok/byok",
        Some(json!({
            "category": "storage",
            "provider": "s3_compatible",
            "label": "Acme Warehouse Bucket",
            "secret": "s3secret12345678",
            "config_json": {
                "url": "memory://acme-warehouse",
                "access_key": "AKIA_VERY_SECRET_KEY_NEVER_ECHO",
                "region": "ap-south-1"
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    // access_key is sanitized and NEVER echoed in response
    assert!(!cred_res["config_json"]
        .to_string()
        .contains("AKIA_VERY_SECRET_KEY_NEVER_ECHO"));
    assert_eq!(cred_res["config_json"]["region"], "ap-south-1");

    // =========================================================================
    // 5. SIGNED WEBHOOK VERIFY WITH REAL LOCAL HTTP RECEIVER
    // =========================================================================
    let webhook_state = MockWebhookState {
        received_count: Arc::new(AtomicUsize::new(0)),
        last_sig: Arc::new(parking_lot::Mutex::new(None)),
    };
    let webhook_app = Router::new()
        .route("/alert/probe", post(webhook_receiver))
        .with_state(webhook_state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, webhook_app).await.unwrap();
    });

    let webhook_url = format!("http://127.0.0.1:{local_port}/alert/probe");
    let (st, _) = send_admin_json(
        &ctrl_app,
        "PUT",
        "/v1/projects/proj_byok/byok",
        Some(json!({
            "category": "webhook",
            "provider": "custom_webhook",
            "secret": webhook_url,
            "config_json": { "signing_secret": "webhook_hmac_secret_4455" }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, verify_wh) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/byok-verify",
        Some(json!({ "category": "webhook" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(verify_wh["verified"], true);
    assert_eq!(webhook_state.received_count.load(Ordering::Relaxed), 1);
    assert!(webhook_state
        .last_sig
        .lock()
        .as_ref()
        .unwrap()
        .starts_with("sha256="));

    // =========================================================================
    // 6. REAL CLOUD WAREHOUSE EXPORT (NDJSON)
    // =========================================================================
    let (st, wh_res) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/export/warehouse",
        Some(json!({ "limit": 100 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(wh_res["status"], "ok");
    assert_eq!(wh_res["events_exported"], 4); // 2 from K1 + 2 from K2
    assert!(wh_res["bytes_written"].as_u64().unwrap() > 0);

    // =========================================================================
    // 7. REAL BYOK AI COPILOT WITH MOCK LLM SERVER & 502 ON FAILURE
    // =========================================================================
    let llm_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm_port = llm_listener.local_addr().unwrap().port();
    let llm_app = Router::new().route("/v1/chat/completions", post(mock_llm_handler));
    tokio::spawn(async move {
        axum::serve(llm_listener, llm_app).await.unwrap();
    });

    let mock_llm_endpoint = format!("http://127.0.0.1:{llm_port}/v1/chat/completions");
    let (st, _) = send_admin_json(
        &ctrl_app,
        "PUT",
        "/v1/projects/proj_byok/byok",
        Some(json!({
            "category": "llm",
            "provider": "openai",
            "secret": "sk-proj-test-mock-api-key-9988",
            "config_json": {
                "endpoint": mock_llm_endpoint,
                "model": "gpt-4o-mini"
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, copilot_res) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/ai/copilot",
        Some(json!({ "prompt": "Diagnose drop-off in KYC funnel" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(copilot_res["provider"], "openai");
    assert!(copilot_res["answer_markdown"]
        .as_str()
        .unwrap()
        .contains("Analyzed via BYOK LLM"));
    let tools = copilot_res["tools_executed"].as_array().unwrap();
    assert!(tools.iter().any(|t| t.as_str() == Some("query_funnel")));

    // When upstream LLM server is dead / unreachable, returns 502 Bad Gateway
    let (st, _) = send_admin_json(
        &ctrl_app,
        "PUT",
        "/v1/projects/proj_byok/byok",
        Some(json!({
            "category": "llm",
            "provider": "openai",
            "secret": "sk-proj-test-mock-api-key-9988",
            "config_json": {
                "endpoint": "http://127.0.0.1:1/v1/chat/completions", // dead port
                "model": "gpt-4o-mini"
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _copilot_err) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/ai/copilot",
        Some(json!({ "prompt": "Should fail with 502" })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_GATEWAY);

    // =========================================================================
    // 8. ENTERPRISE AUDIT LOGS
    // =========================================================================
    let (st, audit_logs) = send_admin_json(&ctrl_app, "GET", "/v1/audit-logs", None).await;
    assert_eq!(st, StatusCode::OK);
    let logs_arr = audit_logs.as_array().unwrap();
    assert!(logs_arr
        .iter()
        .any(|l| l["action"] == "encryption.rotate_key"));
    assert!(logs_arr.iter().any(|l| l["action"] == "warehouse.export"));
    assert!(logs_arr.iter().any(|l| l["action"] == "license.activate"));

    // =========================================================================
    // 9. DEACTIVATION & NODE RESTART WITH DIFFERENT ADMIN TOKEN (Zero Data Lockout)
    // =========================================================================
    // Deactivating license clears commercial features
    let (st, deact_res) = send_admin_json(&ctrl_app, "DELETE", "/v1/license", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(deact_res["is_commercial"], false);

    // Paid feature is now 402 Payment Required
    let (st, _) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/export/warehouse",
        Some(json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::PAYMENT_REQUIRED);

    // But query and decryption of all historical data continues to work flawlessly!
    let (st, funnel_after) = send_admin_json(
        &ctrl_app,
        "POST",
        "/v1/projects/proj_byok/query/funnel",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 30_000, "end_ts_ms": now_ms + 30_000 },
            "steps": [
                { "event_name": "App_Open" },
                { "event_name": "KYC_PAN_Submitted" }
            ],
            "conversion_window_ms": 86_400_000
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(funnel_after["overall_steps"][0]["entered_users"], 2);

    // =========================================================================
    // 10. RESTART NODE WITH DIFFERENT PULSE_ADMIN_TOKEN (C3 verification)
    // =========================================================================
    let new_admin_token = "sk_root_restarted_node_new_admin_token_0123456789";
    std::env::set_var("PULSE_ADMIN_TOKEN", new_admin_token);
    let (new_ctrl_app, _, _new_ctrl, _new_storage, _) = setup_full(
        tmp.path(),
        new_admin_token,
        Some(enc_store.raw_inner().clone()),
    )
    .await;

    let (st, funnel_restarted) = send_admin_with_headers(
        &new_ctrl_app,
        "POST",
        "/v1/projects/proj_byok/query/funnel",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 30_000, "end_ts_ms": now_ms + 30_000 },
            "steps": [
                { "event_name": "App_Open" },
                { "event_name": "KYC_PAN_Submitted" }
            ],
            "conversion_window_ms": 86_400_000
        })),
        new_admin_token,
        &[],
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let funnel_json: Value = serde_json::from_slice(&funnel_restarted).unwrap();
    assert_eq!(funnel_json["overall_steps"][0]["entered_users"], 2);
}
