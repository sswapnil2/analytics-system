//! Guardrail auto-stop: a treatment that makes an `auto_stop` guardrail significantly worse gets
//! rolled back to control, the experiment is concluded with a reason, and it is audited.
//! Bandits: the daily update moves flag weights to the best arm and records the change.

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

/// 1,000 users; every 3rd control user and 2 in 3 treatment users hit an error after exposure.
async fn setup() -> (Router, Arc<ControlPlaneService>, String) {
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
    let ctrl = ControlPlaneService::new(meta, qengine, "proj_g", "pk_live_guardrail_test_key")
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

    let mut events = Vec::new();
    let mut seq = 0u64;
    for u in 0..1000u64 {
        let who = format!("u_{u}");
        let treat = u % 2 == 1;
        let exposed = BASE + (u as i64 % 3) * DAY;
        seq += 1;
        events.push(ev(
            seq,
            &who,
            "$experiment_started",
            exposed,
            vec![
                ("flag_key", PropValue::Str("risky".into())),
                (
                    "variant",
                    PropValue::Str(if treat { "treatment" } else { "control" }.into()),
                ),
            ],
        ));
        if (u / 2) % 3 < if treat { 2 } else { 1 } {
            seq += 1;
            events.push(ev(seq, &who, "Error_Shown", exposed + HOUR, vec![]));
        }
    }
    let (status, _) = call(&app, &token, "POST", "/v1/projects/proj_g/flags", json!({
        "key": "risky", "description": "", "enabled": true, "rollout_pct": 50,
        "variants": [{"key": "control", "weight_bps": 5000}, {"key": "treatment", "weight_bps": 5000}]
    }))
    .await;
    assert!(status.is_success());
    ingest
        .ingest_for_project("proj_g", WireBatch::new(events), BASE + 10 * DAY)
        .await
        .unwrap();
    (app, ctrl, token)
}

fn experiment(auto_stop: bool, sequential: bool) -> Value {
    json!({
        "name": "Risky", "flag_key": "risky", "hypothesis": "",
        "start_ts_ms": BASE, "end_ts_ms": BASE + 5 * DAY,
        "primary_metric": {"name": "Errors", "event_name": "Error_Shown"},
        "guardrail_metrics": [{"name": "Errors", "event_name": "Error_Shown", "direction": "increase_is_bad", "auto_stop": auto_stop}],
        "analysis": {"sequential": sequential},
    })
}

async fn flag_weights(app: &Router, token: &str) -> Vec<(String, u64)> {
    let (_, flags) = call(app, token, "GET", "/v1/projects/proj_g/flags", json!(null)).await;
    flags[0]["variants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["key"].as_str().unwrap().to_string(),
                v["weight_bps"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn regressed_auto_stop_guardrail_rolls_back_to_control() {
    let (app, ctrl, token) = setup().await;

    let (status, body) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        experiment(true, false),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "auto_stop needs sequential: {body}"
    );

    // Without auto_stop, a regression is reported but nothing changes.
    let (status, watch) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        experiment(false, true),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    ctrl.evaluate_experiment_guardrails().await;
    let (_, list) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_g/experiments",
        json!(null),
    )
    .await;
    assert_eq!(list[0]["status"], "running");
    assert_eq!(flag_weights(&app, &token).await[1].1, 5000);
    let (_, res) = call(
        &app,
        &token,
        "GET",
        &format!(
            "/v1/projects/proj_g/experiments/{}/results",
            watch["id"].as_str().unwrap()
        ),
        json!(null),
    )
    .await;
    assert_eq!(
        res["guardrails"][0]["regressed_variants"],
        json!(["treatment"])
    );

    // One live experiment per flag: a second is refused until the first stops.
    let (status, _) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        experiment(true, true),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let watch_uri = format!(
        "/v1/projects/proj_g/experiments/{}",
        watch["id"].as_str().unwrap()
    );
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &watch_uri,
        json!({"status": "concluded"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Names are unique per project, so the rerun gets its own.
    let mut rerun = experiment(true, true);
    rerun["name"] = json!("Checkout guardrail rerun");
    let (_, exp) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        rerun,
    )
    .await;
    let id = exp["id"].as_str().unwrap();
    ctrl.evaluate_experiment_guardrails().await;

    let (_, list) = call(
        &app,
        &token,
        "GET",
        "/v1/projects/proj_g/experiments",
        json!(null),
    )
    .await;
    let stopped = list
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["id"] == id)
        .unwrap();
    assert_eq!(stopped["status"], "concluded");
    let reason = stopped["stop_reason"].as_str().unwrap();
    assert!(
        reason.contains("Errors") && reason.contains("treatment"),
        "{reason}"
    );
    assert_eq!(
        flag_weights(&app, &token).await,
        vec![
            ("control".to_string(), 10_000),
            ("treatment".to_string(), 0)
        ]
    );

    let audit = ctrl.meta().list_enterprise_audit(50).await.unwrap();
    let row = audit
        .iter()
        .find(|a| a.action == "experiment.auto_stopped")
        .expect("audited");
    assert_eq!(row.actor, "system:guardrail");
    assert_eq!(row.details_json["experiment_id"], id);
    assert!(audit.iter().any(|a| a.action == "flag.saved"));
    assert!(audit.iter().any(|a| a.action == "experiment.created"));

    // Resuming clears the reason.
    let (status, resumed) = call(
        &app,
        &token,
        "PATCH",
        &format!("/v1/projects/proj_g/experiments/{id}"),
        json!({"status": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(resumed["stop_reason"].is_null());
    // The flag is busy again, so the older experiment can't be resumed alongside it.
    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        &watch_uri,
        json!({"status": "running"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

/// The edge bundle is cached, but a flag change shows up on the next poll.
#[tokio::test]
async fn cached_flag_bundle_follows_flag_changes() {
    let (app, _ctrl, token) = setup().await;
    let bundle = |app: Router| async move {
        call(
            &app,
            "",
            "GET",
            "/v1/flags/pk_live_guardrail_test_key",
            json!(null),
        )
        .await
    };
    let (status, first) = bundle(app.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let (_, again) = bundle(app.clone()).await;
    assert_eq!(first["etag"], again["etag"]);
    assert_eq!(first["flags"][0]["rollout_bps"], 5000);

    let (status, _) = call(
        &app,
        &token,
        "PATCH",
        "/v1/projects/proj_g/flags/risky",
        json!({"rollout_pct": 20}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, after) = bundle(app.clone()).await;
    assert_eq!(after["flags"][0]["rollout_bps"], 2000);
    assert_ne!(first["etag"], after["etag"]);
}

/// A bandit on the same data: errors are bad, control errs half as often, so the daily update
/// moves traffic to control and records it; a second run the same day changes nothing.
#[tokio::test]
async fn bandit_moves_traffic_to_the_best_arm_once_a_day() {
    let (app, ctrl, token) = setup().await;
    let mut body = json!({
        "name": "Risky bandit", "flag_key": "risky", "hypothesis": "", "kind": "bandit",
        "start_ts_ms": BASE, "end_ts_ms": BASE + 5 * DAY,
        "primary_metric": {"name": "Errors", "event_name": "Error_Shown", "direction": "increase_is_bad"},
        "guardrail_metrics": [{"name": "Errors", "event_name": "Error_Shown"}],
    });
    let (status, _) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "no guardrails on bandits");
    body["guardrail_metrics"] = json!([]);
    body["flag_key"] = json!("missing");
    let (status, _) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "flag must exist");
    body["flag_key"] = json!("risky");
    let (status, exp) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{exp}");
    assert_eq!(exp["kind"], "bandit");
    assert_eq!(exp["bandit"]["min_weight_bps"], 100);
    let (status, _) = call(
        &app,
        &token,
        "POST",
        "/v1/projects/proj_g/experiments",
        experiment(false, true),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "a bandit owns its flag");
    let results_uri = format!(
        "/v1/projects/proj_g/experiments/{}/results",
        exp["id"].as_str().unwrap()
    );

    let (_, res) = call(&app, &token, "GET", &results_uri, json!(null)).await;
    let b = &res["bandit"];
    assert_eq!(res["kind"], "bandit");
    assert_eq!(res["variants"], json!([]));
    assert_eq!(b["best_variant"], "control");
    assert_eq!(b["burn_in_complete"], true);
    assert_eq!(b["arms"][0]["users"], 500);
    assert!(b["arms"][0]["prob_best"].as_f64().unwrap() > 0.99, "{b}");
    assert!((b["arms"][0]["mean_value"].as_f64().unwrap() - 1.0 / 3.0).abs() < 0.01);
    assert_eq!(b["weight_history"].as_array().unwrap().len(), 1);
    assert_eq!(res["sample_ratio_mismatch"], false);

    // Weights only move in the first hour of a day (UTC here).
    let midnight = chrono::Utc::now().timestamp_millis() / DAY * DAY;
    ctrl.evaluate_bandits_at(midnight + 2 * HOUR).await;
    assert_eq!(
        flag_weights(&app, &token).await[1].1,
        5000,
        "mid-day: no change"
    );
    ctrl.evaluate_bandits_at(midnight + 5 * 60_000).await;
    assert_eq!(
        flag_weights(&app, &token).await,
        vec![
            ("control".to_string(), 9_900),
            ("treatment".to_string(), 100)
        ]
    );
    ctrl.evaluate_bandits_at(midnight + 30 * 60_000).await;
    let (_, res) = call(&app, &token, "GET", &results_uri, json!(null)).await;
    let history = res["bandit"]["weight_history"].as_array().unwrap();
    assert_eq!(history.len(), 2, "one update per day");
    assert_eq!(history[1]["weights"]["control"], 9_900);
    assert_eq!(res["bandit"]["arms"][0]["weight_bps"], 9_900);
    // Users so far were assigned 50/50; checking them against today's weights would be an SRM.
    assert_eq!(res["sample_ratio_mismatch"], false);

    let audit = ctrl.meta().list_enterprise_audit(50).await.unwrap();
    assert_eq!(
        audit
            .iter()
            .filter(|a| a.action == "experiment.bandit_reweighted")
            .count(),
        1
    );
}
