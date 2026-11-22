use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use pulse_control::{
    build_control_router, copilot_revenue_summary, handle_mcp_request, ControlPlaneService,
    JsonRpcRequest, MetaStore,
};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, IngestService};
use pulse_storage::{build_from_url, StorageConfig, StorageEngine, StoredUserProfile};
use serde_json::{json, Value};
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
