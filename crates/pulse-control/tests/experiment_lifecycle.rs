//! Experiment lifecycle: drafts start later, concluding records a decision and acts on the flag.
//! Holdouts keep a share of users off every attached flag and measure the flags' combined
//! effect; layers split users between experiments so no one is in two of them.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use pulse_control::{build_control_router, ControlPlaneService, MetaStore};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::IngestService;
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use pulse_wire::{PropValue, WireBatch, WireEvent};
use serde_json::{json, Value};
use tower::ServiceExt;

const BASE: i64 = 1_762_000_000_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

async fn call(
    app: &Router,
    token: &str,
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(if method == "GET" {
                    Body::empty()
                } else {
                    Body::from(body.to_string())
                })
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn ev(seq: u64, who: &str, name: &str, ts: i64, props: Vec<(&str, PropValue)>) -> WireEvent {
    WireEvent {
        event_uuid: WireEvent::deterministic_uuid(who, "s", name, ts, seq),
        routing_seed: who.to_string(),
        distinct_id: who.to_string(),
        session_id: format!("s_{who}"),
        event_name: name.to_string(),
        client_event_ts_ms: ts,
        client_sent_ts_ms: BASE + 10 * DAY,
        props: props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        replay_offset_kb: 0,
    }
}

async fn setup() -> (Router, Arc<ControlPlaneService>, String, Arc<IngestService>) {
    let store = build_from_url("memory://").unwrap();
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 100_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router,
    );
    let qengine = QueryEngine::new(storage.clone(), 8 << 20, None, 0, CatalogRegistry::new())
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
    std::mem::forget(tmp);
    let ctrl = ControlPlaneService::new(meta, qengine, "proj_h", "pk_live_holdout_test_key")
        .await
        .unwrap();
    let token = ctrl
        .take_bootstrap_credentials()
        .generated_admin_token
        .unwrap_or_else(|| {
            std::env::var("PULSE_ADMIN_TOKEN")
                .unwrap_or_else(|_| "sk_root_test_admin_secret_0123456789".to_string())
        });
    let ingest = IngestService::new(storage, ctrl.clone());
    ctrl.attach_ingest(ingest.clone()).await;
    let app = build_control_router(ctrl.clone());
    for key in ["checkout", "a", "b", "c"] {
        let (status, _) = call(
            &app,
            &token,
            "POST",
            "/v1/projects/proj_h/flags",
            json!({
                "key": key, "description": "", "enabled": true, "rollout_pct": 100,
            }),
        )
        .await;
        assert!(status.is_success());
    }
    (app, ctrl, token, ingest)
}

fn flag<'a>(flags: &'a Value, key: &str) -> &'a Value {
    flags
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["key"] == key)
        .unwrap()
}

#[tokio::test]
async fn holdout_holds_users_back_and_measures_the_combined_effect() {
    let (app, _ctrl, token, ingest) = setup().await;
    let (status, _) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_h/holdouts",
        json!({"name": "Too big", "holdout_pct": 60}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, h) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_h/holdouts",
        json!({"name": "Q4 holdout", "holdout_pct": 10}),
    )
    .await;
    assert!(status.is_success(), "{h}");
    assert_eq!(h["bps"], 1000);
    let hid = h["id"].as_str().unwrap().to_string();

    let (status, f) = call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/checkout",
        json!({"holdout_id": hid}),
    )
    .await;
    assert!(status.is_success(), "{f}");
    assert_eq!(f["holdout"], json!({"id": hid, "bps": 1000}));

    // The public bundle carries the gate, and held-out users get no variant.
    let (_, bundle) = call(
        &app,
        &token,
        "GET",
        "/v1/flags/pk_live_holdout_test_key",
        json!(null),
    )
    .await;
    assert_eq!(bundle["schema_version"], 2);
    let rule: pulse_control::models::EdgeFlagRule =
        serde_json::from_value(flag(&bundle["flags"], "checkout").clone()).unwrap();
    let mut held = 0;
    for u in 0..2000 {
        let id = format!("u_{u}");
        let in_holdout = pulse_wire::holdout_slot(&hid, &id) < 1000;
        assert_eq!(
            pulse_control::evaluate_edge_flag(&rule, &id).is_none(),
            in_holdout,
            "{id}"
        );
        held += usize::from(in_holdout);
    }
    assert!((150..250).contains(&held), "10% of 2000 held back: {held}");

    // Exposed users buy 40% of the time, held-back users 20%; 20,000 users so the ~2,000 held
    // back give a clear answer whichever users the random holdout id picks.
    let mut events = Vec::new();
    for u in 0..20_000u64 {
        let who = format!("u_{u}");
        let in_holdout = pulse_wire::holdout_slot(&hid, &who) < 1000;
        let ts = BASE + (u as i64 % 5) * DAY;
        events.push(ev(
            2 * u,
            &who,
            "$holdout_assigned",
            ts,
            vec![
                ("holdout_id", PropValue::Str(hid.clone())),
                (
                    "group",
                    PropValue::Str(if in_holdout { "holdout" } else { "exposed" }.into()),
                ),
            ],
        ));
        if u % 10 < if in_holdout { 2 } else { 4 } {
            events.push(ev(2 * u + 1, &who, "Purchased", ts + HOUR, vec![]));
        }
    }
    ingest
        .ingest_for_project("proj_h", WireBatch::new(events), BASE + 10 * DAY)
        .await
        .unwrap();

    let uri = format!("/v1/projects/proj_h/holdouts/{hid}/results");
    let (status, res) = call(
        &app,
        &token,
        "POST",
        &uri,
        json!({
            "metric": {"name": "Purchases", "event_name": "Purchased"},
            "start_ts_ms": BASE, "end_ts_ms": BASE + 6 * DAY,
        }),
    )
    .await;
    assert!(status.is_success(), "{res}");
    assert_eq!(res["sample_ratio_mismatch"], false, "{res}");
    let rows = res["variants"].as_array().unwrap();
    assert_eq!(rows[0]["variant_key"], "holdout");
    assert_eq!(rows[0]["is_control"], true);
    assert_eq!(rows[1]["variant_key"], "exposed");
    let lift = rows[1]["relative_lift_pct"].as_f64().unwrap();
    assert!(
        (70.0..130.0).contains(&lift),
        "exposed vs holdout lift {lift}"
    );
    assert_eq!(rows[1]["statistically_significant"], true, "{res}");

    let del = format!("/v1/projects/proj_h/holdouts/{hid}");
    let (status, _) = call(&app, &token, "DELETE", &del, json!(null)).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "attached holdouts can't be deleted"
    );
    let (_, f) = call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/checkout",
        json!({"holdout_id": ""}),
    )
    .await;
    assert!(f.get("holdout").is_none());
    let (status, _) = call(&app, &token, "DELETE", &del, json!(null)).await;
    assert!(status.is_success());
}

fn exp_in_layer(flag: &str, layer: &str, pct: f64) -> Value {
    json!({
        "name": format!("{flag} test"), "flag_key": flag, "hypothesis": "",
        "primary_metric": {"name": "Purchases", "event_name": "Purchased"},
        "layer": {"layer_id": layer, "traffic_pct": pct},
    })
}

#[tokio::test]
async fn layer_experiments_get_disjoint_slices() {
    let (app, _ctrl, token, _ingest) = setup().await;
    let (_, l) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_h/layers",
        json!({"name": "Checkout page"}),
    )
    .await;
    let lid = l["id"].as_str().unwrap().to_string();
    let exps = "/v1/projects/proj_h/experiments";

    let (status, body) = call(&app, &token, "POST", exps, exp_in_layer("a", &lid, 60.0)).await;
    assert!(status.is_success(), "{body}");
    let (status, body) = call(&app, &token, "POST", exps, exp_in_layer("b", &lid, 50.0)).await;
    assert_eq!(status, StatusCode::CONFLICT, "only 40% is left: {body}");
    let (status, _) = call(&app, &token, "POST", exps, exp_in_layer("b", &lid, 40.0)).await;
    assert!(status.is_success());

    let (_, flags) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_h/flags",
        json!(null),
    )
    .await;
    assert_eq!(
        flag(&flags, "a")["layer"],
        json!({"id": lid, "start_bps": 0, "end_bps": 6000})
    );
    assert_eq!(
        flag(&flags, "b")["layer"],
        json!({"id": lid, "start_bps": 6000, "end_bps": 10000})
    );
    let (_, layers) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_h/layers",
        json!(null),
    )
    .await;
    assert_eq!(layers[0]["free_bps"], 0);
    assert_eq!(layers[0]["flags"].as_array().unwrap().len(), 2);

    // A turned-off flag serves no one, so its slice is free again; turning it back on would
    // overlap the experiment that took the slice.
    call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/a",
        json!({"enabled": false}),
    )
    .await;
    let (status, _) = call(&app, &token, "POST", exps, exp_in_layer("c", &lid, 30.0)).await;
    assert!(status.is_success());
    let (_, flags) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_h/flags",
        json!(null),
    )
    .await;
    assert_eq!(flag(&flags, "c")["layer"]["start_bps"], 0);
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/a",
        json!({"enabled": true}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Shipping leaves the layer so the winner reaches everyone; joining needs an experiment.
    let (status, f) = call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/b",
        json!({"layer_id": ""}),
    )
    .await;
    assert!(status.is_success() && f.get("layer").is_none());
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/b",
        json!({"layer_id": lid}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

fn draft_or_running(flag: &str, status: &str) -> Value {
    json!({
        "name": format!("{flag} {status}"), "flag_key": flag, "hypothesis": "",
        "status": status, "start_ts_ms": BASE, "end_ts_ms": BASE + 14 * DAY,
        "primary_metric": {"name": "Purchases", "event_name": "Purchased"},
    })
}

#[tokio::test]
async fn drafts_start_later_and_conclusions_record_a_decision() {
    let (app, _ctrl, token, _ingest) = setup().await;
    let exps = "/v1/projects/proj_h/experiments";
    let flags_uri = "/v1/projects/proj_h/flags";
    call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_h/flags/a",
        json!({"enabled": false}),
    )
    .await;

    // A draft leaves the flag alone and doesn't hold it.
    let (status, draft) = call(&app, &token, "POST", exps, draft_or_running("a", "draft")).await;
    assert!(status.is_success(), "{draft}");
    let draft_uri = format!("{exps}/{}", draft["id"].as_str().unwrap());
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(flag(&flags, "a")["enabled"], false);
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &draft_uri,
        json!({"status": "paused"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "drafts can't be paused");

    // A running experiment turns its flag on and blocks starting the draft.
    let (status, live) = call(&app, &token, "POST", exps, draft_or_running("a", "running")).await;
    assert!(status.is_success(), "{live}");
    let live_uri = format!("{exps}/{}", live["id"].as_str().unwrap());
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(flag(&flags, "a")["enabled"], true);
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &draft_uri,
        json!({"status": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Concluding records the decision and rolls the flag back.
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &live_uri,
        json!({"decision": {"outcome": "inconclusive", "flag_action": "rollback"}}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a decision needs status concluded"
    );
    let (status, done) = call(&app, &token, "PATCH", &live_uri, json!({
        "status": "concluded",
        "decision": {"outcome": "inconclusive", "reason": "No difference after 3 weeks", "flag_action": "rollback"},
    }))
    .await;
    assert!(status.is_success(), "{done}");
    assert_eq!(done["decision"]["outcome"], "inconclusive");
    assert_eq!(done["decision"]["reason"], "No difference after 3 weeks");
    assert!(done["decision"]["decided_at_ms"].as_i64().unwrap() > 0);
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(flag(&flags, "a")["enabled"], false);

    // Starting the draft moves its clock to now, keeps its length, and turns the flag on.
    let (status, started) = call(
        &app,
        &token,
        "PATCH",
        &draft_uri,
        json!({"status": "running"}),
    )
    .await;
    assert!(status.is_success(), "{started}");
    let start = started["start_ts_ms"].as_i64().unwrap();
    assert!(start > BASE + 100 * DAY, "start moved to now");
    assert_eq!(started["end_ts_ms"].as_i64().unwrap() - start, 14 * DAY);
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(flag(&flags, "a")["enabled"], true);

    // Shipping rolls the winner out to everyone.
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &draft_uri,
        json!({
            "status": "concluded",
            "decision": {"outcome": "ship", "variant": "nope", "flag_action": "rollout"},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, shipped) = call(&app, &token, "PATCH", &draft_uri, json!({
        "status": "concluded",
        "decision": {"outcome": "ship", "variant": "treatment", "reason": "+8 pts", "flag_action": "rollout"},
    }))
    .await;
    assert!(status.is_success(), "{shipped}");
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    let a = flag(&flags, "a");
    assert_eq!(a["rollout_pct"], 100.0);
    assert_eq!(
        a["variants"][1],
        json!({"key": "treatment", "weight_bps": 10000, "payload": null})
    );
}
