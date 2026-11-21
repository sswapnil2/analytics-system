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
    // The impact metric is remembered on the holdout.
    let (_, hs) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_h/holdouts",
        json!(null),
    )
    .await;
    assert_eq!(hs[0]["metric_event"], "Purchased", "{hs}");

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

    let (status, exp_a) = call(&app, &token, "POST", exps, exp_in_layer("a", &lid, 60.0)).await;
    assert!(status.is_success(), "{exp_a}");
    let a_uri = format!("{exps}/{}", exp_a["id"].as_str().unwrap());
    let (status, body) = call(&app, &token, "POST", exps, exp_in_layer("b", &lid, 50.0)).await;
    assert_eq!(status, StatusCode::CONFLICT, "only 40% is left: {body}");
    let (status, exp_b) = call(&app, &token, "POST", exps, exp_in_layer("b", &lid, 40.0)).await;
    assert!(status.is_success());
    let b_uri = format!("{exps}/{}", exp_b["id"].as_str().unwrap());

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

    // Pausing turns the flag off, which serves no one, so its slice is free again; resuming
    // would overlap the experiment that took the slice.
    let (status, _) = call(&app, &token, "PATCH", &a_uri, json!({"status": "paused"})).await;
    assert!(status.is_success());
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
    let (status, _) = call(&app, &token, "PATCH", &a_uri, json!({"status": "running"})).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "the layer has no free slice to resume into"
    );

    // Shipping leaves the layer so the winner reaches everyone; joining needs an experiment.
    let (status, done) = call(&app, &token, "PATCH", &b_uri, json!({
        "status": "concluded",
        "decision": {"outcome": "ship", "variant": "treatment", "reason": "won", "flag_action": "rollout"},
    }))
    .await;
    assert!(status.is_success(), "{done}");
    let (_, flags) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_h/flags",
        json!(null),
    )
    .await;
    assert!(flag(&flags, "b").get("layer").is_none());
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
    // Problem and owner default empty, notifications on; all three can be changed later.
    assert_eq!(draft["problem"], "");
    assert_eq!(draft["notify"], true);
    let (status, edited) = call(
        &app,
        &token,
        "PATCH",
        &draft_uri,
        json!({
            "problem": "  Users quit at PAN entry  ", "owner": "Asha (growth)", "notify": false,
        }),
    )
    .await;
    assert!(status.is_success(), "{edited}");
    assert_eq!(edited["problem"], "Users quit at PAN entry");
    assert_eq!(edited["owner"], "Asha (growth)");
    assert_eq!(edited["notify"], false);
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &draft_uri,
        json!({"owner": "x".repeat(201)}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
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

    // While it's live, the flag's serving is locked; its description isn't.
    let flag_uri = format!("{flags_uri}/{}", flag(&flags, "a")["id"].as_str().unwrap());
    for body in [
        json!({"rollout_pct": 10.0}),
        json!({"enabled": false}),
        json!({"targeting_cohort_id": ""}),
    ] {
        let (status, err) = call(&app, &token, "PATCH", &flag_uri, body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{err}");
    }
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &flag_uri,
        json!({"description": "1-tap"}),
    )
    .await;
    assert!(status.is_success());

    // Pause turns the flag off and keeps why; resume turns it back on and clears it.
    let (status, paused) = call(
        &app,
        &token,
        "PATCH",
        &live_uri,
        json!({"status": "paused", "reason": " Payment errors spiked "}),
    )
    .await;
    assert!(status.is_success(), "{paused}");
    assert_eq!(paused["pause_reason"], "Payment errors spiked");
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(flag(&flags, "a")["enabled"], false);
    let (status, err) = call(&app, &token, "PATCH", &flag_uri, json!({"enabled": true})).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "still locked while paused: {err}"
    );
    let (status, resumed) = call(
        &app,
        &token,
        "PATCH",
        &live_uri,
        json!({"status": "running"}),
    )
    .await;
    assert!(status.is_success(), "{resumed}");
    assert!(resumed["pause_reason"].is_null());
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(flag(&flags, "a")["enabled"], true);

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
        "decision": {"outcome": "inconclusive", "reason": "No difference after 3 weeks", "flag_action": "rollback", "acknowledged": ["Only 40% of the planned sample."]},
    }))
    .await;
    assert!(status.is_success(), "{done}");
    assert_eq!(done["decision"]["outcome"], "inconclusive");
    assert_eq!(done["decision"]["reason"], "No difference after 3 weeks");
    assert_eq!(
        done["decision"]["acknowledged"],
        json!(["Only 40% of the planned sample."])
    );
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

#[tokio::test]
async fn secondary_metrics_measure_step_through_and_time_between() {
    let (app, _ctrl, token, ingest) = setup().await;
    let exps = "/v1/projects/proj_h/experiments";
    let body = json!({
        "name": "Checkout copy", "flag_key": "checkout", "hypothesis": "",
        "start_ts_ms": BASE, "end_ts_ms": BASE + 14 * DAY,
        "primary_metric": {"name": "Purchases", "event_name": "Purchased"},
        "secondary_metrics": [
            {"name": "Visit to purchase", "event_name": "Purchased", "metric_type": "step_through",
             "denominator": {"event_name": "Visited"}},
            {"name": "Time to purchase", "event_name": "Purchased", "metric_type": "time_between",
             "denominator": {"event_name": "Visited"}, "direction": "increase_is_bad"},
        ],
    });
    let (status, exp) = call(&app, &token, "POST", exps, body.clone()).await;
    assert!(status.is_success(), "{exp}");
    assert_eq!(exp["created_by"], "admin token");
    // Names are unique per project, ignoring case.
    let mut dup = body.clone();
    dup["name"] = json!("checkout COPY");
    dup["flag_key"] = json!("a");
    let (status, _) = call(&app, &token, "POST", exps, dup).await;
    assert_eq!(status, StatusCode::CONFLICT);
    // A two-step metric needs its first step.
    let mut bad = body.clone();
    bad["name"] = json!("Other");
    bad["flag_key"] = json!("b");
    bad["secondary_metrics"] =
        json!([{"name": "x", "event_name": "Purchased", "metric_type": "time_between"}]);
    let (status, _) = call(&app, &token, "POST", exps, bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Everyone visits an hour after exposure. Treatment: 60 of 100 buy 2h after visiting.
    // Control: 30 of 100 buy 4h after visiting; 10 more bought before visiting, which doesn't
    // count as stepping through.
    let mut events = Vec::new();
    let mut seq = 0;
    let mut push = |who: &str, name: &str, ts: i64, props: Vec<(&str, PropValue)>| {
        seq += 1;
        events.push(ev(seq, who, name, ts, props));
    };
    for u in 0..200 {
        let who = format!("u_{u}");
        let treat = u >= 100;
        let i = u % 100;
        let t0 = BASE + HOUR;
        push(
            &who,
            "$experiment_started",
            t0,
            vec![
                ("flag_key", PropValue::Str("checkout".into())),
                (
                    "variant",
                    PropValue::Str(if treat { "treatment" } else { "control" }.into()),
                ),
            ],
        );
        push(&who, "Visited", t0 + HOUR, vec![]);
        if treat && i < 60 {
            push(&who, "Purchased", t0 + 3 * HOUR, vec![]);
        } else if !treat && i < 30 {
            push(&who, "Purchased", t0 + 5 * HOUR, vec![]);
        } else if !treat && i < 40 {
            push(&who, "Purchased", t0 + HOUR / 2, vec![]);
        }
    }
    ingest
        .ingest_for_project("proj_h", WireBatch::new(events), BASE + 10 * DAY)
        .await
        .unwrap();
    let uri = format!("{exps}/{}/results", exp["id"].as_str().unwrap());
    let (status, res) = call(&app, &token, "GET", &uri, json!(null)).await;
    assert!(status.is_success(), "{res}");
    let sec = res["secondary"].as_array().unwrap();
    assert_eq!(sec.len(), 2, "{res}");
    let mean = |m: &Value, v: &str| {
        m["variants"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["variant_key"] == v)
            .unwrap()["mean_value"]
            .as_f64()
            .unwrap()
    };
    assert_eq!(sec[0]["metric_type"], "step_through");
    assert!((mean(&sec[0], "control") - 0.3).abs() < 1e-9, "{}", sec[0]);
    assert!(
        (mean(&sec[0], "treatment") - 0.6).abs() < 1e-9,
        "{}",
        sec[0]
    );
    assert_eq!(sec[1]["metric_type"], "time_between");
    assert!(
        (mean(&sec[1], "control") - 4.0 * 3600.0).abs() < 1e-6,
        "{}",
        sec[1]
    );
    assert!(
        (mean(&sec[1], "treatment") - 2.0 * 3600.0).abs() < 1e-6,
        "{}",
        sec[1]
    );
    // Secondaries never flag regressions.
    assert_eq!(sec[1]["regressed_variants"], json!([]));
}

#[tokio::test]
async fn saved_metrics_are_named_once_and_reused() {
    let (app, _ctrl, token, _ingest) = setup().await;
    let uri = "/v1/projects/proj_h/saved-metrics";
    let kyc = json!({
        "name": " KYC failure ", "event_name": "KYC_Verified", "metric_type": "conversion",
        "conversion_window_ms": 7 * DAY, "direction": "increase_is_bad", "auto_stop": true,
        "filters": [{"prop_name": "status", "filter": {"op": "neq", "val": "success"}}],
    });
    let (status, saved) = call(&app, &token, "POST", uri, kyc.clone()).await;
    assert!(status.is_success(), "{saved}");
    assert_eq!(saved["name"], "KYC failure");
    assert_eq!(saved["spec"]["name"], "KYC failure");
    assert_eq!(
        saved["spec"]["auto_stop"], false,
        "auto-stop belongs to an experiment"
    );
    let mut dup = kyc.clone();
    dup["name"] = json!("kyc FAILURE");
    let (status, _) = call(&app, &token, "POST", uri, dup).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let mut bad = kyc.clone();
    bad["metric_type"] = json!("nonsense");
    let (status, _) = call(&app, &token, "POST", uri, bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (_, list) = call(&app, &token, "GET", uri, json!(null)).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["spec"]["filters"][0]["prop_name"], "status");
    let id = saved["id"].as_str().unwrap();
    let (status, _) = call(&app, &token, "DELETE", &format!("{uri}/{id}"), json!(null)).await;
    assert!(status.is_success());
    let (_, list) = call(&app, &token, "GET", uri, json!(null)).await;
    assert!(list.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn staged_ship_raises_the_rollout_until_someone_changes_the_flag() {
    let (app, ctrl, token, _ingest) = setup().await;
    let exps = "/v1/projects/proj_h/experiments";
    let flags_uri = "/v1/projects/proj_h/flags";
    let rollout = |flags: &Value, key: &str| flag(flags, key)["rollout_pct"].as_f64().unwrap();

    let ship = |every_days: u32| {
        json!({
            "status": "concluded",
            "decision": {"outcome": "ship", "variant": "treatment", "reason": "won", "flag_action": "staged", "every_days": every_days},
        })
    };
    let (_, live) = call(&app, &token, "POST", exps, draft_or_running("b", "running")).await;
    assert!(live["id"].is_string(), "{live}");
    let live_uri = format!("{exps}/{}", live["id"].as_str().unwrap());
    let (status, _) = call(&app, &token, "PATCH", &live_uri, ship(15)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "every_days is 1 to 14");
    let (status, done) = call(&app, &token, "PATCH", &live_uri, ship(2)).await;
    assert!(status.is_success(), "{done}");
    let staged = &done["decision"]["staged"];
    assert_eq!(staged["next_step"], 1);
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(rollout(&flags, "b"), 25.0);
    assert_eq!(flag(&flags, "b")["enabled"], true);

    // Not due yet, then due: 25% → 50%.
    let next = staged["next_at_ms"].as_i64().unwrap();
    ctrl.advance_staged_rollouts_at(next - 1).await;
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(rollout(&flags, "b"), 25.0);
    ctrl.advance_staged_rollouts_at(next).await;
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(rollout(&flags, "b"), 50.0);

    // A hand edit halts the plan and is left alone.
    let flag_uri = format!("{flags_uri}/{}", flag(&flags, "b")["id"].as_str().unwrap());
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &flag_uri,
        json!({"rollout_pct": 30.0}),
    )
    .await;
    assert!(
        status.is_success(),
        "a concluded experiment doesn't lock its flag"
    );
    ctrl.advance_staged_rollouts_at(next + 2 * DAY).await;
    let (_, flags) = call(&app, &token, "GET", flags_uri, json!(null)).await;
    assert_eq!(rollout(&flags, "b"), 30.0);
    let (_, all) = call(&app, &token, "GET", exps, json!(null)).await;
    let exp = all
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["id"] == live["id"])
        .unwrap();
    assert!(exp["decision"]["staged"]["halted"].is_string(), "{exp}");
}
