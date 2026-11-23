use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use pulse_control::{build_control_router, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, IngestService};
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use pulse_wire::{InteractionKind, ReplayChunk, WireframeOp};
use serde_json::{json, Value};
use tower::ServiceExt;

const ROOT: &str = "sk_root_test_profiles_secret_012345";
const INGEST_KEY: &str = "pk_test_profiles_key_0123456789abcdef";

async fn setup_full(
    tmp: &std::path::Path,
) -> (Router, Router, Arc<ControlPlaneService>, Arc<IngestService>) {
    std::env::set_var("PULSE_ADMIN_TOKEN", ROOT);
    let store = build_from_url("memory://").unwrap();
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
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
    let ctrl = ControlPlaneService::new(meta, qengine, "proj_alpha", INGEST_KEY)
        .await
        .unwrap();
    let ingest = IngestService::new(storage, ctrl.clone());
    ctrl.attach_ingest(ingest.clone()).await;

    (
        build_control_router(ctrl.clone()),
        build_ingest_router(ingest.clone()),
        ctrl,
        ingest,
    )
}

async fn send_admin(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<Body>,
    content_type: &str,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {ROOT}"));
    if body.is_some() {
        builder = builder.header("content-type", content_type);
    }
    let resp = app
        .clone()
        .oneshot(builder.body(body.unwrap_or_else(Body::empty)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 2 * 1024 * 1024).await.unwrap();
    (status, bytes.to_vec())
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
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn profiles_user_props_groups_dpdp_erasure_and_import_export() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();

    // 1. Ingest events for 2 users (one will be erased via DPDP later)
    let (s, ing) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                {
                    "event": "App_Open",
                    "distinct_id": "anon_a1",
                    "session_id": "sess_a1",
                    "timestamp": now_ms - 5_000,
                    "properties": { "city": "Bengaluru", "amount": 500 }
                },
                {
                    "event": "First_Disbursal_Success",
                    "distinct_id": "+919845000111",
                    "session_id": "sess_a1",
                    "timestamp": now_ms - 2_000,
                    "properties": { "amount": 1500 }
                },
                {
                    "event": "App_Open",
                    "distinct_id": "usr_keep_2",
                    "session_id": "sess_keep_2",
                    "timestamp": now_ms - 3_000,
                    "properties": { "city": "Mumbai" }
                }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{ing}");

    // 2. Identify + user_properties merge anon_a1 -> +919845000111 & Engage + Groups
    let (s, id_res) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({
            "anon_id": "anon_a1",
            "user_id": "+919845000111",
            "user_properties": { "plan": "credit_pro", "utm_source": "google_ads" }
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{id_res}");
    assert_eq!(id_res["status"], "ok");
    let identified_uid = id_res["canonical_uid"]
        .as_u64()
        .expect("identify must return canonical_uid");

    let (s, eng_res) = send_ingest(
        &ingest_app,
        "/v1/engage",
        json!([
            {
                "$distinct_id": "+919845000111",
                "$setOnce": { "plan": "free", "first_utm": "meta" },
                "$add": { "logins": 2 },
                "$track_charge": { "$amount": 1500.0 }
            },
            {
                "$distinct_id": "usr_keep_2",
                "$set": { "plan": "upi_plus", "utm_source": "organic" },
                "$track_charge": { "$amount": 500.0 }
            }
        ]),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{eng_res}");
    assert_eq!(eng_res["status"], "ok");
    assert_eq!(eng_res["updated"], 2);
    assert!(eng_res.get("canonical_uid").is_none());
    let eng_uids = eng_res["canonical_uids"]
        .as_array()
        .expect("engage must return canonical_uids");
    assert_eq!(eng_uids.len(), 2);
    assert_eq!(eng_uids[0].as_u64(), Some(identified_uid));

    // Immediate read-back on the very next request must reflect both profiles.
    let (s, detail_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/users/%2B919845000111",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let detail_json: Value = serde_json::from_slice(&detail_bytes).unwrap();
    assert_eq!(detail_json["canonical_uid"].as_u64(), Some(identified_uid));
    assert_eq!(detail_json["profile"]["props"]["plan"], "credit_pro");
    assert_eq!(detail_json["profile"]["props"]["first_utm"], "meta");
    assert_eq!(detail_json["profile"]["props"]["logins"], 2);

    let (s, grp_res) = send_ingest(
        &ingest_app,
        "/v1/groups",
        json!({
            "group_type": "merchant",
            "group_id": "m_zepto",
            "$set": { "tier": "enterprise", "gmv_band": "10Cr+" }
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{grp_res}");

    // Seed a replay chunk for sess_a1 so DPDP erasure proves replay deletion too
    ctrl.qengine()
        .storage()
        .append_replay_chunk(&ReplayChunk {
            project_id: "proj_alpha".to_string(),
            session_id: "sess_a1".to_string(),
            chunk_start_ts_ms: now_ms - 5_000,
            ops: vec![WireframeOp::PointerInteraction {
                ts_offset_ms: 100,
                x: 10,
                y: 20,
                kind: InteractionKind::Tap,
            }],
        })
        .await
        .unwrap();

    // 3. Query Insights broken down by `user.plan` (must attribute pre-login `anon_a1` event to `credit_pro`!)
    let (s, qbytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_alpha/query/insights",
        Some(Body::from(
            serde_json::to_vec(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "App Opens", "event_name": "App_Open", "metric": "uniques" }],
                "breakdown_by": ["user.plan"]
            }))
            .unwrap(),
        )),
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&qbytes));
    let qjson: Value = serde_json::from_slice(&qbytes).unwrap();
    let keys: Vec<String> = qjson["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["breakdown_key"].as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains(&"credit_pro".to_string()), "keys={keys:?}");
    assert!(keys.contains(&"upi_plus".to_string()), "keys={keys:?}");

    // 4. Create cohort and test CSV export + Revenue + Import/Export + Alerts
    let (s, cbytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_alpha/cohorts",
        Some(Body::from(
            serde_json::to_vec(&json!({
                "name": "All Opener Cohort",
                "description": "Users who opened app",
                "definition": { "kind": "performed_event", "event_name": "App_Open", "min_count": 1 }
            }))
            .unwrap(),
        )),
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&cbytes));
    let cohort_id = serde_json::from_slice::<Value>(&cbytes).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (s, exp_csv) = send_admin(
        &admin_app,
        "GET",
        &format!("/v1/projects/proj_alpha/cohorts/{cohort_id}/export?format=csv"),
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let exp_csv_str = String::from_utf8(exp_csv).unwrap();
    assert!(
        exp_csv_str.contains("canonical_uid,distinct_id,plan"),
        "{exp_csv_str}"
    );
    assert!(exp_csv_str.contains("credit_pro"), "{exp_csv_str}");

    // 5. Batch Import (CSV) + Export (NDJSON) + Revenue Summary + Alerts CRUD
    let csv_import = "event,distinct_id,timestamp,city\nCSV_Event,usr_keep_2,,Delhi_NCR\n";
    let (s, imp_bytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_alpha/import",
        Some(Body::from(csv_import)),
        "text/csv",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&imp_bytes));
    assert_eq!(
        serde_json::from_slice::<Value>(&imp_bytes).unwrap()["accepted"].as_u64(),
        Some(1)
    );

    let (s, ev_ndjson) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/export?format=ndjson",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(String::from_utf8(ev_ndjson).unwrap().contains("CSV_Event"));

    let (s, rev_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/revenue",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let rev_json: Value = serde_json::from_slice(&rev_bytes).unwrap();
    assert_eq!(rev_json["total_revenue"].as_f64(), Some(2000.0));
    assert_eq!(rev_json["paying_users"].as_u64(), Some(2));
    assert_eq!(rev_json["revenue_event"].as_str(), Some("$revenue"));

    let (s, rev_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/revenue?event=Order_Paid",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let rev_json: Value = serde_json::from_slice(&rev_bytes).unwrap();
    assert_eq!(rev_json["revenue_event"].as_str(), Some("Order_Paid"));

    let (s, alr_bytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_alpha/alerts",
        Some(Body::from(
            serde_json::to_vec(&json!({
                "name": "Open Spike",
                "event_name": "App_Open",
                "operator": "gt",
                "threshold": 0.5
            }))
            .unwrap(),
        )),
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&alr_bytes));
    let alert_id = serde_json::from_slice::<Value>(&alr_bytes).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (s, alr_list) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/alerts",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let alr_arr: Vec<Value> = serde_json::from_slice(&alr_list).unwrap();
    assert_eq!(alr_arr[0]["triggered"].as_bool(), Some(true));
    let (s, _) = send_admin(
        &admin_app,
        "DELETE",
        &format!("/v1/projects/proj_alpha/alerts/{alert_id}"),
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // 6. Execute DPDP Erasure on `+919845000111`: verifies profile, L0/L1 events, replay deleted & audit recorded!
    let (s, del_bytes) = send_admin(
        &admin_app,
        "DELETE",
        "/v1/projects/proj_alpha/users/%2B919845000111",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&del_bytes));
    let del_json: Value = serde_json::from_slice(&del_bytes).unwrap();
    assert_eq!(del_json["distinct_id_masked"], "+91 •••• 111");
    assert_eq!(del_json["replays_purged"].as_u64(), Some(1));

    let (s, audit_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/deletions",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let audit_arr: Vec<Value> = serde_json::from_slice(&audit_bytes).unwrap();
    assert_eq!(audit_arr.len(), 1);

    // After erasure, `credit_pro` must NO LONGER appear in Insights breakdowns!
    let (s, qbytes_after) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_alpha/query/insights",
        Some(Body::from(
            serde_json::to_vec(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "App Opens", "event_name": "App_Open", "metric": "uniques" }],
                "breakdown_by": ["user.plan"]
            }))
            .unwrap(),
        )),
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let qjson_after: Value = serde_json::from_slice(&qbytes_after).unwrap();
    let keys_after: Vec<String> = qjson_after["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["breakdown_key"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !keys_after.contains(&"credit_pro".to_string()),
        "erased user must be excluded from queries: {keys_after:?}"
    );
    assert!(keys_after.contains(&"upi_plus".to_string()));
}

#[tokio::test]
async fn w2_single_engage_returns_canonical_uid_and_propagates_metastore_and_conflict_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _ingest) = setup_full(tmp.path()).await;

    // 1) Single-user /v1/engage returns `canonical_uid` and `canonical_uids: [uid]` and reads back immediately.
    let (s, eng) = send_ingest(
        &ingest_app,
        "/v1/engage",
        json!({
            "distinct_id": "single_w2_user",
            "$set": { "plan": "enterprise" }
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{eng}");
    let uid = eng["canonical_uid"]
        .as_u64()
        .expect("single /v1/engage must include canonical_uid");
    assert_eq!(eng["canonical_uids"], json!([uid]));

    let (s, detail_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_alpha/users/single_w2_user",
        None,
        "application/json",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let detail: Value = serde_json::from_slice(&detail_bytes).unwrap();
    assert_eq!(detail["canonical_uid"].as_u64(), Some(uid));
    assert_eq!(detail["profile"]["props"]["plan"], "enterprise");

    // 2) Merge conflict on two already-identified users returns 409 on both /v1/identify and mixed /v1/ingest.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({"anon_id": "a_c1", "user_id": "u_c1"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({"anon_id": "a_c2", "user_id": "u_c2"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, conflict_id) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({"anon_id": "u_c1", "user_id": "u_c2"}),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{conflict_id}");

    let (s, conflict_mix) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "batch": [
                {"type": "track", "userId": "u_c1", "event": "Checkout", "messageId": "m_c1"},
                {"type": "identify", "userId": "u_c2", "anonymousId": "u_c1"}
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{conflict_mix}");

    // 3) Closing MetaStore causes /v1/groups and /v1/engage (erased_uids read) to return 503, NEVER 200.
    match ctrl.meta() {
        MetaStore::Sqlite(pool) => pool.close().await,
        MetaStore::Postgres(pool) => pool.close().await,
    }
    let (s, grp_err) = send_ingest(
        &ingest_app,
        "/v1/groups",
        json!({
            "group_type": "company",
            "group_id": "acme",
            "$set": { "seats": 10 }
        }),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::SERVICE_UNAVAILABLE,
        "MetaStore failure on /v1/groups must propagate 503: {grp_err}"
    );
    let (s, eng_err) = send_ingest(
        &ingest_app,
        "/v1/engage",
        json!({"distinct_id": "single_w2_user", "$set": {"plan": "broken"}}),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::SERVICE_UNAVAILABLE,
        "MetaStore failure during /v1/engage must propagate 503: {eng_err}"
    );
}
