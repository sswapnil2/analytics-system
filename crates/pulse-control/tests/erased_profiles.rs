use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use pulse_control::copilot::{run_byok_copilot, CopilotRequest};
use pulse_control::{
    build_control_router, copilot_revenue_summary, handle_mcp_request, ByokCredentialRecord,
    ControlPlaneService, JsonRpcRequest, MetaStore,
};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, IngestService};
use pulse_storage::{build_from_url, StorageConfig, StorageEngine, StoredUserProfile};
use pulse_wire::PropValue;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tower::ServiceExt;

const ROOT: &str = "sk_root_erased_profiles_test_secret_012345";
const INGEST_KEY: &str = "pk_erased_profiles_key_0123456789abcdef";

async fn setup_full(
    tmp: &std::path::Path,
) -> (
    Router,
    Router,
    Arc<ControlPlaneService>,
    Arc<IngestService>,
    MetaStore,
    Arc<QueryEngine>,
) {
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
    let ctrl = ControlPlaneService::new(meta.clone(), qengine.clone(), "proj_erased", INGEST_KEY)
        .await
        .unwrap();
    let ingest = IngestService::new(storage, ctrl.clone());
    ctrl.attach_ingest(ingest.clone()).await;

    (
        build_control_router(ctrl.clone()),
        build_ingest_router(ingest.clone()),
        ctrl,
        ingest,
        meta,
        qengine,
    )
}

async fn send_admin(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {ROOT}"));
    let req_body = match body {
        Some(v) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&v).unwrap())
        }
        None => Body::empty(),
    };
    let resp = app
        .clone()
        .oneshot(builder.body(req_body).unwrap())
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
async fn all_profile_read_endpoints_and_catalog_filter_erased_uids() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();

    // Ingest events so UID 0 ("uid0_user"), UID 1/2 ("anon_1" -> "user_1"), UID 3 ("user_2"),
    // UID 4 ("user_3"), and UID 5 ("user_4") exist in the identity graph and L0.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Page_View", "distinct_id": "uid0_user", "timestamp": now_ms - 60_000 },
                { "event": "Page_View", "distinct_id": "anon_1", "timestamp": now_ms - 50_000 },
                { "event": "Page_View", "distinct_id": "user_1", "timestamp": now_ms - 40_000 },
                { "event": "Page_View", "distinct_id": "user_2", "timestamp": now_ms - 30_000 },
                { "event": "Page_View", "distinct_id": "user_3", "timestamp": now_ms - 20_000 },
                { "event": "Page_View", "distinct_id": "user_4", "timestamp": now_ms - 10_000 }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({ "anon_id": "anon_1", "user_id": "user_1" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let uid0 = id_graph.lookup_canonical("uid0_user").unwrap();
    assert_eq!(uid0, 0, "first user must be UID 0");
    let raw_anon1 = id_graph.lookup_raw("anon_1").unwrap();
    let canon_u1 = id_graph.lookup_canonical("user_1").unwrap();
    let uid2 = id_graph.lookup_canonical("user_2").unwrap();
    let uid3 = id_graph.lookup_canonical("user_3").unwrap();
    let uid4 = id_graph.lookup_canonical("user_4").unwrap();

    // Write profile rows directly into pstore with explicit timestamps so we can test unpurged
    // snapshot rows + post-erasure re-signup without deleting from pstore.
    let pstore = qengine
        .storage()
        .profile_store("proj_erased", true)
        .await
        .unwrap();
    let t_old = now_ms - 100_000;
    let t_erase = now_ms - 25_000;
    let t_new = now_ms - 10_000;

    for (did, uid, plan, rev, ts) in [
        ("uid0_user", uid0, "plan_uid0", 100.0, t_old),
        ("anon_1", canon_u1, "plan_anon1", 50.0, t_old),
        ("user_1", canon_u1, "plan_u1", 75.0, t_old + 1_000),
        ("user_2", uid2, "plan_u2", 200.0, t_old + 2_000),
        ("user_3", uid3, "plan_u3", 30.0, t_new), // re-signup AFTER erasure
        ("user_4", uid4, "plan_u4", 40.0, t_new + 1_000),
    ] {
        pstore
            .upsert(&StoredUserProfile {
                project_id: "proj_erased".into(),
                distinct_id: did.into(),
                canonical_uid: uid,
                props: BTreeMap::from([("plan".into(), json!(plan))]),
                total_revenue: rev,
                updated_at_ms: ts,
            })
            .await
            .unwrap();
    }

    // Before erasure, UID 0 detail must resolve `list_for_canonical(0)` cleanly (proves UID 0 fix).
    let (s, d0_pre) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/users/uid0_user?uid=0",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let d0_pre_json: Value = serde_json::from_slice(&d0_pre).unwrap();
    assert_eq!(d0_pre_json["canonical_uid"], 0);
    assert_eq!(d0_pre_json["profile"]["props"]["plan"], "plan_uid0");

    // Create two saved cohorts: one PerformedEvent, one static BehavioralCohortDef::Users.
    let (s, c1) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({
            "name": "Viewers",
            "description": "all viewers",
            "definition": { "kind": "performed_event", "event_name": "Page_View", "min_count": 1 }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let coh_event_id = serde_json::from_slice::<Value>(&c1).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (s, c2) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({
            "name": "Static Users",
            "description": "explicit UIDs",
            "definition": { "kind": "users", "canonical_uids": [uid0, canon_u1, uid2, uid3, uid4] }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let coh_users_id = serde_json::from_slice::<Value>(&c2).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Load initial catalog BEFORE erasures so user_4 is in ColumnarProfileIndex.
    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    // Now insert `erased_uids` directly in MetaStore WITHOUT touching `pstore`:
    // - uid0 (UID 0) erased at t_erase
    // - raw_anon1 (only raw UID of merged anon_1/user_1) erased at t_erase
    // - uid2 (user_2) erased at t_erase
    // - uid3 (user_3) erased at t_erase (but user_3 has updated_at_ms = t_new > t_erase, so survives!)
    meta.insert_erased_uids("proj_erased", &[uid0, raw_anon1, uid2, uid3], t_erase)
        .await
        .unwrap();

    // 1. handle_list_user_profiles: pagination & search must skip erased profiles BEFORE offset/limit.
    let (s, page0) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/users?limit=1&offset=0",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let page0_arr: Vec<Value> = serde_json::from_slice(&page0).unwrap();
    assert_eq!(page0_arr.len(), 1);
    assert_eq!(page0_arr[0]["distinct_id"], "user_4");

    let (s, page1) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/users?limit=1&offset=1",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let page1_arr: Vec<Value> = serde_json::from_slice(&page1).unwrap();
    assert_eq!(page1_arr.len(), 1);
    assert_eq!(page1_arr[0]["distinct_id"], "user_3");

    let (s, search_res) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/users?search=plan",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let search_arr: Vec<Value> = serde_json::from_slice(&search_res).unwrap();
    let search_ids: Vec<&str> = search_arr
        .iter()
        .map(|v| v["distinct_id"].as_str().unwrap())
        .collect();
    assert_eq!(search_ids, vec!["user_4", "user_3"]);

    // 2. handle_get_user_detail: hides UID 0, anon_1, user_1; keeps user_3 & user_4.
    for hidden_did in ["uid0_user", "anon_1", "user_1", "user_2"] {
        let (s, body) = send_admin(
            &admin_app,
            "GET",
            &format!("/v1/projects/proj_erased/users/{hidden_did}"),
            None,
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let j: Value = serde_json::from_slice(&body).unwrap();
        assert!(j["profile"].is_null(), "{hidden_did}: {j}");
        assert_eq!(
            j["events"].as_array().unwrap().len(),
            0,
            "{hidden_did}: {j}"
        );
    }
    let (s, u3_body) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/users/user_3",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let u3_json: Value = serde_json::from_slice(&u3_body).unwrap();
    assert_eq!(u3_json["profile"]["props"]["plan"], "plan_u3");

    // 3. handle_export_cohort: excludes erased UIDs & props from BOTH event cohort and Users cohort.
    for cid in [&coh_event_id, &coh_users_id] {
        let (s, body) = send_admin(
            &admin_app,
            "GET",
            &format!("/v1/projects/proj_erased/cohorts/{cid}/export?format=json"),
            None,
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let j: Value = serde_json::from_slice(&body).unwrap();
        let uids: Vec<u64> = j["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["canonical_uid"].as_u64().unwrap())
            .collect();
        assert!(!uids.contains(&(uid0 as u64)), "{cid}: {j}");
        assert!(!uids.contains(&(canon_u1 as u64)), "{cid}: {j}");
        assert!(!uids.contains(&(uid2 as u64)), "{cid}: {j}");
        assert!(uids.contains(&(uid4 as u64)), "{cid}: {j}");
    }

    // 4. handle_get_revenue_summary: excludes uid0 (100), anon_1+user_1 (125), user_2 (200);
    // keeps user_3 (30) + user_4 (40) = 70.0 across 2 profiles.
    let (s, rev_bytes) =
        send_admin(&admin_app, "GET", "/v1/projects/proj_erased/revenue", None).await;
    assert_eq!(s, StatusCode::OK);
    let rev_json: Value = serde_json::from_slice(&rev_bytes).unwrap();
    assert_eq!(rev_json["total_profiles"].as_u64(), Some(2));
    assert_eq!(rev_json["paying_users"].as_u64(), Some(2));
    assert!((rev_json["total_revenue"].as_f64().unwrap() - 70.0).abs() < 1e-6);

    // 5. copilot revenue summary helper: matches filtered revenue.
    let (cp_total, cp_paying, cp_rev) =
        copilot_revenue_summary(&meta, qengine.storage(), "proj_erased").await;
    assert_eq!((cp_total, cp_paying), (2, 2));
    assert!((cp_rev - 70.0).abs() < 1e-6);

    // 6. mcp "get_user": hides erased UID 0 & user_1, shows user_4.
    let mcp_u0 = handle_mcp_request(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(1),
            method: "tools/call".into(),
            params: json!({ "name": "get_user", "arguments": { "distinct_id": "uid0_user" } }),
        },
        "proj_erased",
        &meta,
        &qengine,
    )
    .await;
    let sc0 = &mcp_u0.result.unwrap()["structuredContent"];
    assert!(sc0["properties"].is_null(), "{sc0}");
    assert!(sc0["total_revenue"].is_null(), "{sc0}");
    assert_eq!(sc0["recent_events"].as_array().unwrap().len(), 0);

    let mcp_u4 = handle_mcp_request(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(2),
            method: "tools/call".into(),
            params: json!({ "name": "get_user", "arguments": { "distinct_id": "user_4" } }),
        },
        "proj_erased",
        &meta,
        &qengine,
    )
    .await;
    let sc4 = &mcp_u4.result.unwrap()["structuredContent"];
    assert_eq!(sc4["properties"]["plan"], "plan_u4");

    // 7. Cross-node erasure eviction on incremental `refresh_project_virtual_catalog`:
    // Erase `user_4` in MetaStore ONLY (no pstore write) and refresh incrementally!
    meta.insert_erased_uids("proj_erased", &[uid4], t_new + 10_000)
        .await
        .unwrap();
    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    let (s, qbytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/query/insights",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 120_000, "end_ts_ms": now_ms + 60_000 },
            "interval": "day",
            "series": [{ "label": "Views", "event_name": "Page_View", "metric": "uniques" }],
            "breakdown_by": ["user.plan"]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&qbytes));
    let qjson: Value = serde_json::from_slice(&qbytes).unwrap();
    let keys: Vec<&str> = qjson["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["breakdown_key"].as_str().unwrap())
        .collect();
    assert!(!keys.contains(&"plan_uid0"), "{keys:?}");
    assert!(!keys.contains(&"plan_u1"), "{keys:?}");
    assert!(!keys.contains(&"plan_u2"), "{keys:?}");
    assert!(
        !keys.contains(&"plan_u4"),
        "user_4 must be evicted: {keys:?}"
    );
    assert!(
        keys.contains(&"plan_u3"),
        "re-signup user_3 must survive: {keys:?}"
    );
}

#[tokio::test]
async fn apply_engage_and_on_identify_isolate_post_erasure_resignup_from_stale_snapshot_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();

    // Pre-erasure user in pstore & identity graph.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Signup", "distinct_id": "resign_u", "timestamp": now_ms - 5_000 }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let uid = id_graph.lookup_canonical("resign_u").unwrap();
    let pstore = qengine
        .storage()
        .profile_store("proj_erased", true)
        .await
        .unwrap();
    let t_erase = now_ms - 1_000;
    pstore
        .upsert(&StoredUserProfile {
            project_id: "proj_erased".into(),
            distinct_id: "resign_u".into(),
            canonical_uid: uid,
            props: BTreeMap::from([
                ("first_utm".into(), json!("old_meta")),
                ("stale_secret".into(), json!("must_not_leak")),
            ]),
            total_revenue: 500.0,
            updated_at_ms: t_erase - 2_000,
        })
        .await
        .unwrap();
    meta.insert_erased_uids("proj_erased", &[uid], t_erase)
        .await
        .unwrap();
    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    // User re-signs up AFTER erasure (`now_ms > t_erase`) and calls /v1/engage + /v1/identify.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/engage",
        json!({
            "distinct_id": "resign_u",
            "$setOnce": { "first_utm": "new_google" },
            "$track_charge": { "$amount": 25.0 }
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({ "anon_id": "fresh_anon", "user_id": "resign_u" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, detail_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/users/resign_u",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let detail: Value = serde_json::from_slice(&detail_bytes).unwrap();
    assert_eq!(detail["profile"]["props"]["first_utm"], "new_google");
    assert!(
        detail["profile"]["props"].get("stale_secret").is_none(),
        "stale pre-erasure props must not leak: {detail}"
    );
    assert!(
        (detail["profile"]["total_revenue"].as_f64().unwrap() - 25.0).abs() < 1e-6,
        "{detail}"
    );
}

#[tokio::test]
async fn copilot_revenue_summary_does_not_clobber_expanded_erased_uids_for_merged_users() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();

    // Create merged user: anon_0 (raw UID 0) -> user_1 (canonical UID 1), plus non-erased user_2.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Landing", "distinct_id": "anon_0", "timestamp": now_ms - 40_000 },
                { "event": "SecretEvent", "distinct_id": "user_1", "timestamp": now_ms - 30_000 },
                { "event": "PublicEvent", "distinct_id": "user_2", "timestamp": now_ms - 20_000 }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({ "anon_id": "anon_0", "user_id": "user_1" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let raw_anon0 = id_graph.lookup_raw("anon_0").unwrap();
    let canon_u1 = id_graph.lookup_canonical("user_1").unwrap();
    assert_eq!(raw_anon0, 0);
    assert_ne!(raw_anon0, canon_u1);

    // Record ONLY the raw UID (`anon_0` = 0) in `erased_uids`, then sync catalog/storage.
    let t_erase = now_ms - 10_000;
    meta.insert_erased_uids("proj_erased", &[raw_anon0], t_erase)
        .await
        .unwrap();
    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    // Calling copilot_revenue_summary must preserve expanded erased UIDs on StorageEngine.
    let (cp_total, cp_paying, cp_rev) =
        copilot_revenue_summary(&meta, qengine.storage(), "proj_erased").await;
    assert_eq!((cp_total, cp_paying), (0, 0));
    assert!(cp_rev.abs() < 1e-6);

    // Raw event export must return 0 events for merged canonical user_1 (only user_2's PublicEvent).
    let (s, export_bytes) = send_admin(
        &admin_app,
        "GET",
        "/v1/projects/proj_erased/export?format=json&limit=100",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let export_json: Value = serde_json::from_slice(&export_bytes).unwrap();
    assert_eq!(export_json["count"].as_u64(), Some(1), "{export_json}");
    let events = export_json["events"].as_array().unwrap();
    assert_eq!(events.len(), 1, "expected only user_2 event: {export_json}");
    assert_eq!(events[0]["event_name"], "PublicEvent");

    // Insights query for SecretEvent must return 0.
    let (s, qbytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/query/insights",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 120_000, "end_ts_ms": now_ms + 60_000 },
            "interval": "day",
            "series": [{ "label": "Secret", "event_name": "SecretEvent", "metric": "totals" }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&qbytes));
    let qjson: Value = serde_json::from_slice(&qbytes).unwrap();
    let total = qjson["series"][0]["overall_value"].as_f64().unwrap_or(0.0);
    assert!(
        total.abs() < 1e-6,
        "erased merged user's SecretEvent leaked into insights: {qjson}"
    );
}

#[tokio::test]
async fn export_cohort_preserves_post_erasure_event_only_resignup_and_filters_static_users() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let t_pre = now_ms - 60_000;
    let t_erase = now_ms - 30_000;
    let t_post = now_ms - 10_000;

    // Pre-erasure events for `ev_resign` and `never_erased` (no /v1/engage -> no ProfileStore rows).
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Signup", "distinct_id": "ev_resign", "timestamp": t_pre },
                { "event": "Signup", "distinct_id": "never_erased", "timestamp": t_pre },
                { "event": "Signup", "distinct_id": "permanently_erased", "timestamp": t_pre }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let ev_uid = id_graph.lookup_canonical("ev_resign").unwrap();
    let never_uid = id_graph.lookup_canonical("never_erased").unwrap();
    let perm_uid = id_graph.lookup_canonical("permanently_erased").unwrap();

    // Erase both `ev_resign` and `permanently_erased` at `t_erase`.
    meta.insert_erased_uids("proj_erased", &[ev_uid, perm_uid], t_erase)
        .await
        .unwrap();

    // `ev_resign` re-signs up with an event ONLY (`t_post > t_erase`) and NO /v1/engage call.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Signup", "distinct_id": "ev_resign", "timestamp": t_post }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Confirm `ev_resign` genuinely has no ProfileStore row.
    let pstore = qengine
        .storage()
        .profile_store("proj_erased", false)
        .await
        .unwrap();
    assert!(
        pstore
            .get_by_distinct_id("ev_resign")
            .await
            .unwrap()
            .is_none(),
        "test requires event-only re-signup with no ProfileStore row"
    );

    // Create:
    // 1. Event-based cohort (`PerformedEvent`)
    // 2. Static `Users` cohort
    // 3. Nested `And` cohort combining `PerformedEvent` and `Users`
    let (s, c_ev) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({
            "name": "Signups",
            "definition": { "kind": "performed_event", "event_name": "Signup", "min_count": 1 }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let event_cid = serde_json::from_slice::<Value>(&c_ev).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (s, c_users) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({
            "name": "Static List",
            "definition": { "kind": "users", "canonical_uids": [ev_uid, never_uid, perm_uid] }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let users_cid = serde_json::from_slice::<Value>(&c_users).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (s, c_and) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({
            "name": "Nested And",
            "definition": {
                "kind": "and",
                "clauses": [
                    { "kind": "performed_event", "event_name": "Signup", "min_count": 1 },
                    { "kind": "users", "canonical_uids": [ev_uid, never_uid, perm_uid] }
                ]
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let and_cid = serde_json::from_slice::<Value>(&c_and).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    // Event-based cohort MUST include post-erasure event-only re-signup (`ev_uid`) and `never_uid`,
    // while excluding `perm_uid`.
    let (s, ev_export) = send_admin(
        &admin_app,
        "GET",
        &format!("/v1/projects/proj_erased/cohorts/{event_cid}/export?format=json"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let ev_json: Value = serde_json::from_slice(&ev_export).unwrap();
    let ev_uids: Vec<u64> = ev_json["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["canonical_uid"].as_u64().unwrap())
        .collect();
    assert_eq!(ev_json["count"].as_u64(), Some(2), "{ev_json}");
    assert!(ev_uids.contains(&(ev_uid as u64)), "{ev_json}");
    assert!(ev_uids.contains(&(never_uid as u64)), "{ev_json}");
    assert!(!ev_uids.contains(&(perm_uid as u64)), "{ev_json}");

    // Static `Users` cohort and nested `And` containing `Users` must exclude erased UIDs.
    for cid in [&users_cid, &and_cid] {
        let (s, body) = send_admin(
            &admin_app,
            "GET",
            &format!("/v1/projects/proj_erased/cohorts/{cid}/export?format=json"),
            None,
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let j: Value = serde_json::from_slice(&body).unwrap();
        let uids: Vec<u64> = j["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| u["canonical_uid"].as_u64().unwrap())
            .collect();
        assert_eq!(j["count"].as_u64(), Some(1), "{cid}: {j}");
        assert_eq!(uids, vec![never_uid as u64], "{cid}: {j}");
    }
}

#[tokio::test]
async fn preview_cohort_and_mcp_list_cohorts_sync_and_expand_erased_uids_before_catalog_refresh() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let t_ev = now_ms - 30_000;
    let t_erase = now_ms - 10_000;

    // Create merged user `anon_a` -> `user_a` and surviving user `user_b`.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Checkout", "distinct_id": "anon_a", "timestamp": t_ev },
                { "event": "Checkout", "distinct_id": "user_b", "timestamp": t_ev }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({ "anon_id": "anon_a", "user_id": "user_a" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let raw_anon_a = id_graph.lookup_raw("anon_a").unwrap();
    let canon_a = id_graph.lookup_canonical("user_a").unwrap();
    let canon_b = id_graph.lookup_canonical("user_b").unwrap();

    // Save both an event-based cohort and a static `Users` cohort.
    let ev_def = json!({ "kind": "performed_event", "event_name": "Checkout", "min_count": 1 });
    let users_def = json!({ "kind": "users", "canonical_uids": [canon_a, canon_b] });

    let (s, c_ev) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({ "name": "Buyers", "definition": ev_def })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let ev_cid = serde_json::from_slice::<Value>(&c_ev).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (s, c_users) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({ "name": "Static Buyers", "definition": users_def })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let users_cid = serde_json::from_slice::<Value>(&c_users).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Refresh catalog once BEFORE erasure (simulates node state prior to a cross-node erasure).
    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    // Pre-erasure baseline: all endpoints agree on count = 2.
    for def in [&ev_def, &users_def] {
        let (s, prev) = send_admin(
            &admin_app,
            "POST",
            "/v1/projects/proj_erased/cohorts/preview",
            Some(json!({ "definition": def })),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let prev_json: Value = serde_json::from_slice(&prev).unwrap();
        assert_eq!(prev_json["user_count"].as_u64(), Some(2), "{prev_json}");
    }

    // Insert ONLY the raw alias UID directly into MetaStore (cross-node erasure, no
    // `erase_user_dpdp` and NO `refresh_project_virtual_catalog` on this node).
    meta.insert_erased_uids("proj_erased", &[raw_anon_a], t_erase)
        .await
        .unwrap();

    // 1. POST /cohorts/preview must sync & expand `erased_uids` and report 1 (not 2).
    for def in [&ev_def, &users_def] {
        let (s, prev) = send_admin(
            &admin_app,
            "POST",
            "/v1/projects/proj_erased/cohorts/preview",
            Some(json!({ "definition": def })),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let prev_json: Value = serde_json::from_slice(&prev).unwrap();
        assert_eq!(
            prev_json["user_count"].as_u64(),
            Some(1),
            "preview_cohort leaked erased user: {prev_json}"
        );
    }

    // 2. handle_mcp_request "list_cohorts" must agree on count = 1.
    let mcp_resp = handle_mcp_request(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(10),
            method: "tools/call".into(),
            params: json!({ "name": "list_cohorts", "arguments": {} }),
        },
        "proj_erased",
        &meta,
        &qengine,
    )
    .await;
    let cohorts = mcp_resp.result.unwrap()["structuredContent"]["cohorts"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(cohorts.len(), 2, "{cohorts:?}");
    for c in &cohorts {
        assert_eq!(
            c["users"].as_u64(),
            Some(1),
            "mcp list_cohorts leaked erased user: {c}"
        );
    }

    // 3. GET /cohorts/:cid/export must agree on count = 1 (only canon_b).
    for cid in [&ev_cid, &users_cid] {
        let (s, body) = send_admin(
            &admin_app,
            "GET",
            &format!("/v1/projects/proj_erased/cohorts/{cid}/export?format=json"),
            None,
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let exp_json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(exp_json["count"].as_u64(), Some(1), "{cid}: {exp_json}");
        assert_eq!(
            exp_json["users"][0]["canonical_uid"].as_u64(),
            Some(canon_b as u64),
            "{cid}: {exp_json}"
        );
    }

    // 4. Sibling query handler (POST /query/insights) must also reflect cross-node erasure.
    let (s, qbytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/query/insights",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 120_000, "end_ts_ms": now_ms + 60_000 },
            "interval": "day",
            "series": [{ "label": "Checkouts", "event_name": "Checkout", "metric": "uniques" }]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let qjson: Value = serde_json::from_slice(&qbytes).unwrap();
    assert!(
        (qjson["series"][0]["overall_value"].as_f64().unwrap_or(0.0) - 1.0).abs() < 1e-6,
        "insights query leaked erased user: {qjson}"
    );

    // 5. Standalone handle_mcp_request "list_cohorts" on a fresh StorageEngine/QueryEngine BEFORE
    // any catalog refresh must load & expand `erased_uids` and populate cohort defs, returning 1.
    qengine.storage().flush_all_projects().await.unwrap();
    let fresh_storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        qengine.storage().object_store().clone(),
        qengine.storage().identity_router().clone(),
    );
    let fresh_qengine = QueryEngine::new(
        fresh_storage,
        16 * 1024 * 1024,
        None,
        0,
        CatalogRegistry::new(),
    )
    .await
    .unwrap();

    let standalone_mcp = handle_mcp_request(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(11),
            method: "tools/call".into(),
            params: json!({ "name": "list_cohorts", "arguments": {} }),
        },
        "proj_erased",
        &meta,
        &fresh_qengine,
    )
    .await;
    let standalone_cohorts = standalone_mcp.result.unwrap()["structuredContent"]["cohorts"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(standalone_cohorts.len(), 2, "{standalone_cohorts:?}");
    for c in &standalone_cohorts {
        assert_eq!(
            c["users"].as_u64(),
            Some(1),
            "standalone mcp list_cohorts before refresh failed: {c}"
        );
    }
}

#[tokio::test]
async fn cross_node_erasure_evicts_columnar_profile_index_before_catalog_refresh() {
    let tmp = tempfile::tempdir().unwrap();
    let (admin_app, ingest_app, ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let t_pre = now_ms - 60_000;
    let t_erase = now_ms - 30_000;
    let t_resign = now_ms - 10_000;

    // 1. Ingest pre-erasure events for merged `anon_a` -> `user_a`, `user_c`, and surviving `user_b`.
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Checkout", "distinct_id": "anon_a", "timestamp": t_pre },
                { "event": "Checkout", "distinct_id": "user_b", "timestamp": t_pre },
                { "event": "Checkout", "distinct_id": "user_c", "timestamp": t_pre }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({ "anon_id": "anon_a", "user_id": "user_a" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Set PII profile properties on all users via /v1/engage.
    for (did, pii) in [
        ("user_a", "alice_secret_42"),
        ("user_b", "bob_public"),
        ("user_c", "charlie_secret_99"),
    ] {
        let (s, _) = send_ingest(
            &ingest_app,
            "/v1/engage",
            json!({
                "distinct_id": did,
                "$set": { "secret_pii": pii }
            }),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }

    // Ensure profile timestamps in SlateDB precede `t_erase` so pre-erasure profiles are hidden
    // after erasure while `t_resign > t_erase` is treated as a post-erasure re-signup.
    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let raw_anon_a = id_graph.lookup_raw("anon_a").unwrap();
    let canon_a = id_graph.lookup_canonical("user_a").unwrap();
    let canon_b = id_graph.lookup_canonical("user_b").unwrap();
    let canon_c = id_graph.lookup_canonical("user_c").unwrap();

    let pstore = qengine
        .storage()
        .profile_store("proj_erased", true)
        .await
        .unwrap();
    for (did, uid, pii) in [
        ("user_a", canon_a, "alice_secret_42"),
        ("user_b", canon_b, "bob_public"),
        ("user_c", canon_c, "charlie_secret_99"),
    ] {
        pstore
            .upsert(&StoredUserProfile {
                project_id: "proj_erased".into(),
                distinct_id: did.into(),
                canonical_uid: uid,
                props: BTreeMap::from([("secret_pii".into(), json!(pii))]),
                total_revenue: 0.0,
                updated_at_ms: t_pre,
            })
            .await
            .unwrap();
    }

    // Save a cohort so standalone MCP `query_insights` with `global_filter.cohort` is also verified.
    let (s, c_resp) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts",
        Some(json!({
            "name": "Checkouts Cohort",
            "definition": { "kind": "performed_event", "event_name": "Checkout", "min_count": 1 }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let cohort_id = serde_json::from_slice::<Value>(&c_resp).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Refresh catalog BEFORE erasure so `ColumnarProfileIndex` (`cat.profiles`) caches both profiles.
    ctrl.refresh_project_virtual_catalog("proj_erased")
        .await
        .unwrap();

    // Baseline check: `alice_secret_42` is present before erasure.
    let (s, base_prev) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts/preview",
        Some(json!({
            "definition": {
                "kind": "property_match",
                "predicate": { "prop_name": "user.secret_pii", "filter": { "op": "eq", "val": "alice_secret_42" } }
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&base_prev).unwrap()["user_count"].as_u64(),
        Some(1)
    );

    // 2. Simulate cross-node erasure on another node (NO `refresh_project_virtual_catalog` here).
    meta.insert_erased_uids("proj_erased", &[raw_anon_a], t_erase)
        .await
        .unwrap();
    pstore.delete_for_ids(&["anon_a", "user_a"]).await.unwrap();

    // 3. User re-signs up with ONLY an event at `t_resign > t_erase` (no `/v1/engage`).
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Checkout", "distinct_id": "user_a", "timestamp": t_resign }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // 4a. POST /query/insights breakdown_by user.secret_pii must NOT leak `alice_secret_42` before refresh.
    let (s, qbytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/query/insights",
        Some(json!({
            "time_range": { "start_ts_ms": now_ms - 120_000, "end_ts_ms": now_ms + 60_000 },
            "interval": "day",
            "series": [{ "label": "Checkouts", "event_name": "Checkout", "metric": "uniques" }],
            "breakdown_by": ["user.secret_pii"]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&qbytes));
    let qjson: Value = serde_json::from_slice(&qbytes).unwrap();
    let breakdown_keys: Vec<&str> = qjson["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["breakdown_key"].as_str().unwrap())
        .collect();
    assert!(
        !breakdown_keys.contains(&"alice_secret_42"),
        "ColumnarProfileIndex leaked erased user.secret_pii into insights: {qjson}"
    );
    assert!(
        breakdown_keys.contains(&"bob_public"),
        "non-erased user_b missing: {qjson}"
    );

    // 4b. POST /cohorts/preview PropertyMatch on user.secret_pii == "alice_secret_42" must return 0.
    let (s, prev_after) = send_admin(
        &admin_app,
        "POST",
        "/v1/projects/proj_erased/cohorts/preview",
        Some(json!({
            "definition": {
                "kind": "property_match",
                "predicate": { "prop_name": "user.secret_pii", "filter": { "op": "eq", "val": "alice_secret_42" } }
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let prev_after_json: Value = serde_json::from_slice(&prev_after).unwrap();
    assert_eq!(
        prev_after_json["user_count"].as_u64(),
        Some(0),
        "cohorts/preview leaked erased profile in ColumnarProfileIndex: {prev_after_json}"
    );

    // 4c. Now erase `user_c` (`charlie_secret_99`, still cached in `cat.profiles` & `profile_catalog_ts`)
    // and re-ingest an event-only re-signup, then hit POST /v1/mcp FIRST before any other route.
    meta.insert_erased_uids("proj_erased", &[canon_c], t_erase)
        .await
        .unwrap();
    pstore.delete_for_ids(&["user_c"]).await.unwrap();
    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Checkout", "distinct_id": "user_c", "timestamp": t_resign }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, mcp_bytes) = send_admin(
        &admin_app,
        "POST",
        "/v1/mcp",
        Some(json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/call",
            "params": {
                "name": "list_property_values",
                "arguments": {
                    "property": "user.secret_pii",
                    "event_name": "Checkout",
                    "start_ts_ms": now_ms - 120_000,
                    "end_ts_ms": now_ms + 60_000
                }
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{}", String::from_utf8_lossy(&mcp_bytes));
    let mcp_json: Value = serde_json::from_slice(&mcp_bytes).unwrap();
    let mcp_vals: Vec<&str> = mcp_json["result"]["structuredContent"]["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap())
        .collect();
    assert!(
        !mcp_vals.contains(&"alice_secret_42") && !mcp_vals.contains(&"charlie_secret_99"),
        "HTTP /v1/mcp leaked erased user.secret_pii: {mcp_json}"
    );
    assert!(
        mcp_vals.contains(&"bob_public"),
        "HTTP /v1/mcp missing bob_public: {mcp_json}"
    );

    // 4d. Standalone QueryEngine with pre-populated `cat.profiles` (without ControlPlaneService)
    // calling `handle_mcp_request` (`query_insights` with `global_filter.cohort` + `user.secret_pii`
    // breakdown) must evict `alice_secret_42` AND sync `cat.saved_cohorts`.
    qengine.storage().flush_all_projects().await.unwrap();
    let standalone_storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        qengine.storage().object_store().clone(),
        qengine.storage().identity_router().clone(),
    );
    let standalone_qengine = QueryEngine::new(
        standalone_storage,
        16 * 1024 * 1024,
        None,
        0,
        CatalogRegistry::new(),
    )
    .await
    .unwrap();
    standalone_qengine
        .catalogs()
        .for_project("proj_erased")
        .set_user_profiles(HashMap::from([
            (
                canon_a,
                HashMap::from([(
                    "secret_pii".into(),
                    PropValue::Str("alice_secret_42".into()),
                )]),
            ),
            (
                canon_b,
                HashMap::from([("secret_pii".into(), PropValue::Str("bob_public".into()))]),
            ),
        ]));

    let standalone_resp = handle_mcp_request(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(22),
            method: "tools/call".into(),
            params: json!({
                "name": "query_insights",
                "arguments": {
                    "time_range": { "start_ts_ms": now_ms - 120_000, "end_ts_ms": now_ms + 60_000 },
                    "interval": "day",
                    "series": [{ "label": "Checkouts", "event_name": "Checkout", "metric": "uniques" }],
                    "breakdown_by": ["user.secret_pii"],
                    "global_filter": {
                        "cohort": { "kind": "cohort_ref", "cohort_id": cohort_id }
                    }
                }
            }),
        },
        "proj_erased",
        &meta,
        &standalone_qengine,
    )
    .await;
    let sc = &standalone_resp.result.as_ref().unwrap()["structuredContent"];
    let standalone_keys: Vec<&str> = sc["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["breakdown_key"].as_str().unwrap())
        .collect();
    assert!(
        !standalone_keys.contains(&"alice_secret_42"),
        "standalone MCP leaked erased ColumnarProfileIndex entry: {sc}"
    );
    assert!(
        standalone_keys.contains(&"bob_public"),
        "standalone MCP failed to evaluate saved cohort or bob_public: {sc}"
    );
}

struct PrivateEndpointEnvGuard {
    prev: Option<String>,
}

impl PrivateEndpointEnvGuard {
    fn allow() -> Self {
        let prev = std::env::var("PULSE_ALLOW_PRIVATE_ENDPOINTS").ok();
        std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1");
        Self { prev }
    }
}

impl Drop for PrivateEndpointEnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", v),
            None => std::env::remove_var("PULSE_ALLOW_PRIVATE_ENDPOINTS"),
        }
    }
}

async fn mock_copilot_llm_handler() -> impl IntoResponse {
    axum::Json(json!({
        "id": "chatcmpl_erasure_test",
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "Mock copilot answer"
            }
        }]
    }))
}

#[tokio::test]
async fn direct_run_byok_copilot_syncs_erased_uids_before_querying() {
    let _allow_priv = PrivateEndpointEnvGuard::allow();
    let tmp = tempfile::tempdir().unwrap();
    let (_admin_app, ingest_app, _ctrl, _, meta, qengine) = setup_full(tmp.path()).await;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let t1 = now_ms - 50_000;
    let t2 = now_ms - 40_000;
    let t_erase = now_ms - 10_000;

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Step1", "distinct_id": "anon_erased", "timestamp": t1 },
                { "event": "Step1", "distinct_id": "user_live", "timestamp": t1 }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/ingest",
        json!({
            "events": [
                { "event": "Step2", "distinct_id": "user_erased", "timestamp": t2 },
                { "event": "Step2", "distinct_id": "user_live", "timestamp": t2 }
            ]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = send_ingest(
        &ingest_app,
        "/v1/identify",
        json!({ "anon_id": "anon_erased", "user_id": "user_erased" }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let id_graph = qengine
        .storage()
        .identity_graph("proj_erased", false)
        .await
        .unwrap();
    let raw_anon_erased = id_graph.lookup_raw("anon_erased").unwrap();
    let canon_erased = id_graph.lookup_canonical("user_erased").unwrap();
    let canon_live = id_graph.lookup_canonical("user_live").unwrap();
    assert_ne!(raw_anon_erased, canon_erased);

    let pstore = qengine
        .storage()
        .profile_store("proj_erased", true)
        .await
        .unwrap();
    for (did, uid, amount) in [
        ("user_erased", canon_erased, 250.0),
        ("user_live", canon_live, 100.0),
    ] {
        pstore
            .upsert(&StoredUserProfile {
                project_id: "proj_erased".into(),
                distinct_id: did.into(),
                canonical_uid: uid,
                props: BTreeMap::new(),
                total_revenue: amount,
                updated_at_ms: t1,
            })
            .await
            .unwrap();
    }

    qengine.storage().flush_all_projects().await.unwrap();

    // Erase ONLY raw alias UID in MetaStore (simulating cross-node erasure before catalog refresh).
    meta.insert_erased_uids("proj_erased", &[raw_anon_erased], t_erase)
        .await
        .unwrap();

    // Fresh pre-refresh QueryEngine (empty `erased_snapshot` and empty `CatalogRegistry`).
    let fresh_storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        qengine.storage().object_store().clone(),
        qengine.storage().identity_router().clone(),
    );
    let fresh_qengine = QueryEngine::new(
        fresh_storage,
        16 * 1024 * 1024,
        None,
        0,
        CatalogRegistry::new(),
    )
    .await
    .unwrap();

    let llm_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm_port = llm_listener.local_addr().unwrap().port();
    let llm_app = Router::new().route("/v1/chat/completions", post(mock_copilot_llm_handler));
    let _llm_srv = tokio::spawn(async move {
        let _ = axum::serve(llm_listener, llm_app).await;
    });

    let llm_cred = ByokCredentialRecord {
        id: "byok_test_llm".into(),
        project_id: "proj_erased".into(),
        category: "llm".into(),
        provider: "openai".into(),
        label: "test-llm".into(),
        key_prefix_masked: "sk-t...".into(),
        sealed_secret_hex: String::new(),
        config_json: json!({
            "endpoint": format!("http://127.0.0.1:{llm_port}/v1/chat/completions"),
            "model": "gpt-4o-mini"
        }),
        status: "verified".into(),
        last_verified_at_ms: Some(now_ms),
        updated_at_ms: now_ms,
    };

    let resp = run_byok_copilot(
        &fresh_qengine,
        &meta,
        "proj_erased",
        &llm_cred,
        "sk-test-key",
        &CopilotRequest {
            prompt: "Analyze Step1 Step2 funnel drop-off and revenue".into(),
            start_ts_ms: Some(now_ms - 120_000),
            end_ts_ms: Some(now_ms + 60_000),
        },
    )
    .await
    .unwrap();

    let insights = resp.metrics_summary["insights"].as_array().unwrap();
    assert!(!insights.is_empty());
    for entry in insights {
        assert!(
            (entry["uniques"].as_f64().unwrap() - 1.0).abs() < 1e-6,
            "direct run_byok_copilot leaked erased user in insights: {}",
            resp.metrics_summary
        );
    }

    let funnel_steps = resp.metrics_summary["funnel_steps"].as_array().unwrap();
    assert_eq!(funnel_steps.len(), 2, "{}", resp.metrics_summary);
    for step in funnel_steps {
        assert_eq!(
            step["entered_users"].as_u64(),
            Some(1),
            "direct run_byok_copilot funnel mismatch: {} (full={})",
            step,
            resp.metrics_summary
        );
    }

    assert_eq!(
        resp.metrics_summary["revenue"]["paying_users"].as_u64(),
        Some(1)
    );
    assert!(
        (resp.metrics_summary["revenue"]["total_revenue"]
            .as_f64()
            .unwrap()
            - 100.0)
            .abs()
            < 1e-6,
        "{}",
        resp.metrics_summary
    );
}
