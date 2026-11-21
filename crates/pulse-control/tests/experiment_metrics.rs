//! End to end through the HTTP API: sum, ratio and legacy `continuous` metrics, CUPED, segments,
//! the lift time series, and metric validation.

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
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};
use tower::ServiceExt;

const BASE: i64 = 1_762_000_000_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;
const USERS: usize = 600;

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

struct Truth {
    /// Per variant: (users, Σ post amount, Σ visits).
    control: (f64, f64, f64),
    treatment: (f64, f64, f64),
}

/// Each user has a spend level that drives both a pre-exposure purchase (5 days before) and the
/// post-exposure purchase; treatment adds 8. Every user also visits 1-4 times after exposure.
async fn setup() -> (Router, String, Truth) {
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
    // Keep the temp dir alive for the test process.
    std::mem::forget(tmp);
    let ctrl = ControlPlaneService::new(meta, qengine, "proj_m", "pk_live_metrics_test_key")
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
    let app = build_control_router(ctrl);

    let mut rng = StdRng::seed_from_u64(11);
    let mut events = Vec::new();
    let mut truth = Truth {
        control: (0.0, 0.0, 0.0),
        treatment: (0.0, 0.0, 0.0),
    };
    let mut seq = 0u64;
    let mut next = || {
        seq += 1;
        seq
    };
    for u in 0..USERS {
        let who = format!("buyer_{u}");
        let treat = u % 2 == 1;
        let spend: f64 = rng.gen_range(20.0..100.0);
        let exposed = BASE + (u as i64 % 3) * DAY + HOUR;
        let pre = (spend + rng.gen_range(-5.0..5.0)).round();
        let post = (spend + if treat { 8.0 } else { 0.0 } + rng.gen_range(-5.0..5.0)).round();
        let visits = rng.gen_range(1..=4);
        events.push(ev(
            next(),
            &who,
            "Purchase",
            exposed - 5 * DAY,
            vec![("amount", PropValue::Int(pre as i64))],
        ));
        events.push(ev(
            next(),
            &who,
            "$experiment_started",
            exposed,
            vec![
                ("flag_key", PropValue::Str("bigger_cart".into())),
                (
                    "variant",
                    PropValue::Str(if treat { "treatment" } else { "control" }.into()),
                ),
                (
                    "country",
                    PropValue::Str(if u % 4 < 2 { "DE" } else { "US" }.into()),
                ),
            ],
        ));
        events.push(ev(
            next(),
            &who,
            "Purchase",
            exposed + HOUR,
            vec![("amount", PropValue::Int(post as i64))],
        ));
        for v in 0..visits {
            events.push(ev(
                next(),
                &who,
                "Visit",
                exposed + 2 * HOUR + v as i64 * 60_000,
                vec![],
            ));
        }
        let t = if treat {
            &mut truth.treatment
        } else {
            &mut truth.control
        };
        t.0 += 1.0;
        t.1 += post;
        t.2 += f64::from(visits);
    }
    let (status, _) = call(&app, &token, "POST", "/v1/projects/proj_m/flags", json!({
        "key": "bigger_cart", "description": "", "enabled": true, "rollout_pct": 100,
        "variants": [{"key": "control", "weight_bps": 5000}, {"key": "treatment", "weight_bps": 5000}]
    }))
    .await;
    assert!(status.is_success(), "flag create {status}");
    ingest
        .ingest_for_project("proj_m", WireBatch::new(events), BASE + 10 * DAY)
        .await
        .unwrap();
    (app, token, truth)
}

fn experiment(metric: Value, analysis: Value) -> Value {
    // Names are unique per project, so each analysis gets its own.
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    json!({
        // Concluded, so several analyses of the same flag can coexist (one live experiment per flag).
        "name": format!("Bigger cart {n}"), "flag_key": "bigger_cart", "hypothesis": "", "status": "concluded",
        "start_ts_ms": BASE, "end_ts_ms": BASE + 5 * DAY,
        "primary_metric": metric, "guardrail_metrics": [], "analysis": analysis,
    })
}

async fn results(app: &Router, token: &str, metric: Value, analysis: Value, query: &str) -> Value {
    let (status, exp) = call(
        app,
        token,
        "POST",
        "/v1/projects/proj_m/experiments",
        experiment(metric, analysis),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{exp}");
    let id = exp["id"].as_str().unwrap();
    let (status, res) = call(
        app,
        token,
        "GET",
        &format!("/v1/projects/proj_m/experiments/{id}/results{query}"),
        json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{res}");
    res
}

fn variant<'a>(res: &'a Value, key: &str) -> &'a Value {
    res["variants"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["variant_key"] == key)
        .unwrap()
}

#[tokio::test]
async fn sum_metric_with_cuped_segments_and_timeseries() {
    let (app, token, truth) = setup().await;
    let metric = json!({"name": "Spend", "event_name": "Purchase", "metric_type": "sum", "numeric_prop": "amount", "conversion_window_ms": DAY});

    let plain = results(&app, &token, metric.clone(), json!({"cuped": false}), "").await;
    assert_eq!(plain["metric_type"], "sum");
    assert_eq!(plain["cuped_applied"], false);
    let c = variant(&plain, "control");
    assert_eq!(c["sample_size"].as_u64(), Some(truth.control.0 as u64));
    let want = truth.control.1 / truth.control.0;
    assert!(
        (c["mean_value"].as_f64().unwrap() - want).abs() < 1e-9,
        "{c} vs {want}"
    );

    let cuped = results(
        &app,
        &token,
        metric,
        json!({"cuped": true}),
        "?segment=country",
    )
    .await;
    assert_eq!(cuped["cuped_applied"], true);
    let reduction = cuped["variance_reduction_pct"].as_f64().unwrap();
    assert!(
        reduction > 80.0,
        "pre-period spend explains most variance, got {reduction}%"
    );
    let (pt, ct) = (variant(&plain, "treatment"), variant(&cuped, "treatment"));
    let width = |v: &Value| v["abs_ci_high"].as_f64().unwrap() - v["abs_ci_low"].as_f64().unwrap();
    assert!(
        width(ct) < width(pt) / 2.0,
        "CUPED must narrow the interval"
    );
    let diff = ct["abs_diff"].as_f64().unwrap();
    assert!(
        (diff - 8.0).abs() < 1.5,
        "CUPED estimate {diff} should be near the true +8"
    );
    assert_eq!(ct["statistically_significant"], true);

    let segs = cuped["segments"].as_array().unwrap();
    assert_eq!(cuped["segment_by"], "country");
    assert_eq!(segs.len(), 2);
    let seg_users: u64 = segs
        .iter()
        .flat_map(|s| s["variants"].as_array().unwrap())
        .map(|v| v["sample_size"].as_u64().unwrap())
        .sum();
    assert_eq!(seg_users, USERS as u64);

    let series = cuped["timeseries"].as_array().unwrap();
    assert_eq!(series.len(), 3, "exposures land on three days");
    let last_users: u64 = series.last().unwrap()["variants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["users"].as_u64().unwrap())
        .sum();
    assert_eq!(last_users, USERS as u64);
}

#[tokio::test]
async fn ratio_and_legacy_continuous_metrics() {
    let (app, token, truth) = setup().await;
    let ratio = results(&app, &token, json!({
        "name": "Spend per visit", "event_name": "Purchase", "metric_type": "ratio", "numeric_prop": "amount",
        "denominator": {"event_name": "Visit"}, "conversion_window_ms": DAY
    }), json!({"cuped": true}), "").await;
    assert_eq!(ratio["metric_type"], "ratio");
    assert_eq!(
        ratio["cuped_applied"], false,
        "CUPED doesn't apply to ratio metrics"
    );
    let t = variant(&ratio, "treatment");
    let want = truth.treatment.1 / truth.treatment.2;
    assert!((t["mean_value"].as_f64().unwrap() - want).abs() < 1e-9);
    assert!(t["abs_ci_low"].as_f64().unwrap() < t["abs_diff"].as_f64().unwrap());

    let legacy = results(&app, &token, json!({
        "name": "Spend", "event_name": "Purchase", "metric_type": "continuous", "numeric_prop": "amount",
        "conversion_window_ms": DAY
    }), json!({}), "").await;
    assert_eq!(legacy["metric_type"], "sum");
}

#[tokio::test]
async fn incomplete_metric_definitions_are_rejected() {
    let (app, token, _) = setup().await;
    for metric in [
        json!({"name": "x", "event_name": "Purchase", "metric_type": "sum", "conversion_window_ms": DAY}),
        json!({"name": "x", "event_name": "Purchase", "metric_type": "ratio", "conversion_window_ms": DAY}),
        json!({"name": "x", "event_name": "Purchase", "metric_type": "median", "conversion_window_ms": DAY}),
    ] {
        let (status, body) = call(
            &app,
            &token,
            "POST",
            "/v1/projects/proj_m/experiments",
            experiment(metric, json!({})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
}

#[tokio::test]
async fn metric_filters_count_only_matching_events() {
    let (app, token, _) = setup().await;
    let metric = |filters: Value| {
        json!({"name": "Big purchase", "event_name": "Purchase", "metric_type": "conversion",
               "conversion_window_ms": 2 * DAY, "filters": filters})
    };
    // Every post-exposure purchase is 15..=113, so `> 1000` matches nothing and `> 0` everything.
    let none = results(
        &app,
        &token,
        metric(json!([{"prop_name": "amount", "filter": {"op": "gt", "val": 1000}}])),
        json!({}),
        "",
    )
    .await;
    let all = results(
        &app,
        &token,
        metric(json!([{"prop_name": "amount", "filter": {"op": "gt", "val": 0}}])),
        json!({}),
        "",
    )
    .await;
    for key in ["control", "treatment"] {
        assert_eq!(variant(&none, key)["conversions"], 0, "{none}");
        assert_eq!(
            variant(&all, key)["conversions"],
            variant(&all, key)["sample_size"],
            "{all}"
        );
    }
}
