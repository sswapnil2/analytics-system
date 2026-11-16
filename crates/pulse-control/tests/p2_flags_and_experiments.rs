use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use pulse_control::{
    build_control_router, compute_sample_ratio_mismatch, compute_statistical_lift_table,
    evaluate_edge_flag, ControlPlaneService, EdgeFlagBundle, MetaStore,
};
use pulse_exec::{BehavioralCohortDef, CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::IngestService;
use pulse_storage::{build_from_url, StorageConfig, StorageEngine};
use pulse_wire::{PropValue, WireBatch, WireEvent};
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn p2_02_and_p2_09f_flag_cohort_targeting_and_sub_24h_experiment_window() {
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
    let catalogs = CatalogRegistry::new();
    let qengine = QueryEngine::new(storage.clone(), 8 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
    let ingest_key = "pk_live_p2_cohort_test_key".to_string();
    let ctrl = ControlPlaneService::new(meta, qengine, "proj_p2", &ingest_key)
        .await
        .unwrap();
    let admin_token = ctrl
        .take_bootstrap_credentials()
        .generated_admin_token
        .unwrap_or_else(|| {
            std::env::var("PULSE_ADMIN_TOKEN")
                .unwrap_or_else(|_| "sk_root_test_admin_secret_0123456789".to_string())
        });

    let ingest = IngestService::new(storage, ctrl.clone());
    ctrl.attach_ingest(ingest.clone()).await;

    let app = build_control_router(ctrl);

    let base_ts = 1_762_000_000_000i64;
    // Ingest events:
    // - `beta_tester` performs `Beta_Opt_In`, `$experiment_started` (t=0), and converts at +30m (inside 1h window!)
    // - `regular_user` performs `$experiment_started` (t=0), and converts at +2h (OUTSIDE 1h window, inside 24h!)
    let events = vec![
        WireEvent {
            event_uuid: [1; 16],
            routing_seed: "s1".into(),
            distinct_id: "beta_tester".into(),
            session_id: "sess_beta".into(),
            event_name: "Beta_Opt_In".into(),
            client_event_ts_ms: base_ts,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        },
        WireEvent {
            event_uuid: [2; 16],
            routing_seed: "s1".into(),
            distinct_id: "beta_tester".into(),
            session_id: "sess_beta".into(),
            event_name: "$experiment_started".into(),
            client_event_ts_ms: base_ts + 1_000,
            client_sent_ts_ms: 0,
            props: vec![
                ("flag_key".into(), PropValue::Str("fast_checkout".into())),
                ("variant".into(), PropValue::Str("treatment".into())),
            ],
            replay_offset_kb: 0,
        },
        WireEvent {
            event_uuid: [3; 16],
            routing_seed: "s1".into(),
            distinct_id: "beta_tester".into(),
            session_id: "sess_beta".into(),
            event_name: "Purchase".into(),
            client_event_ts_ms: base_ts + 30 * 60 * 1_000, // +30m
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        },
        WireEvent {
            event_uuid: [4; 16],
            routing_seed: "s2".into(),
            distinct_id: "regular_user".into(),
            session_id: "sess_reg".into(),
            event_name: "$experiment_started".into(),
            client_event_ts_ms: base_ts + 1_000,
            client_sent_ts_ms: 0,
            props: vec![
                ("flag_key".into(), PropValue::Str("fast_checkout".into())),
                ("variant".into(), PropValue::Str("control".into())),
            ],
            replay_offset_kb: 0,
        },
        WireEvent {
            event_uuid: [5; 16],
            routing_seed: "s2".into(),
            distinct_id: "regular_user".into(),
            session_id: "sess_reg".into(),
            event_name: "Purchase".into(),
            client_event_ts_ms: base_ts + 2 * 3600 * 1_000, // +2h (outside 1h window!)
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        },
    ];
    ingest
        .ingest_for_project("proj_p2", WireBatch::new(events), base_ts + 5_000)
        .await
        .unwrap();

    // 1. Save behavioral cohort of users who performed `Beta_Opt_In`
    let coh_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/proj_p2/cohorts")
                .header("authorization", format!("Bearer {admin_token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "name": "Beta Testers",
                        "definition": BehavioralCohortDef::PerformedEvent {
                            event_name: "Beta_Opt_In".into(),
                            min_count: 1,
                            time_range: None,
                            where_clauses: vec![],
                        }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(coh_resp.status(), StatusCode::OK);
    let coh_json: Value =
        serde_json::from_slice(&to_bytes(coh_resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let cohort_id = coh_json["id"].as_str().unwrap().to_string();

    // 2. Create flag targeted at `cohort_id` at 100% rollout (`P2-02`)
    let flag_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/proj_p2/flags")
                .header("authorization", format!("Bearer {admin_token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "key": "fast_checkout",
                        "enabled": true,
                        "rollout_pct": 100.0,
                        "targeting_cohort_id": cohort_id,
                        "variants": [
                            { "key": "control", "weight_bps": 5000 },
                            { "key": "treatment", "weight_bps": 5000 }
                        ]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(flag_resp.status(), StatusCode::OK);

    // Fetch public flag bundle -> `beta_tester` MUST match, `regular_user` MUST get `None` (`P2-02`)!
    let bundle_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/flags/{ingest_key}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bundle_resp.status(), StatusCode::OK);
    let bundle: EdgeFlagBundle =
        serde_json::from_slice(&to_bytes(bundle_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let rule = bundle
        .flags
        .iter()
        .find(|r| r.key == "fast_checkout")
        .unwrap();
    assert!(
        evaluate_edge_flag(rule, "beta_tester").is_some(),
        "cohort member must match targeted flag"
    );
    assert_eq!(
        evaluate_edge_flag(rule, "regular_user"),
        None,
        "non-cohort user must NOT match cohort-targeted flag!"
    );

    // 3. Create experiment with 1-hour conversion window (`3_600_000` ms) (`P2-09f`)
    let exp_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/proj_p2/experiments")
                .header("authorization", format!("Bearer {admin_token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "name": "1-Hour Window Test",
                        "flag_key": "fast_checkout",
                        "hypothesis": "Sub-24h window respected",
                        "start_ts_ms": base_ts - 10_000,
                        "end_ts_ms": base_ts + 86_400_000,
                        "primary_metric": {
                            "name": "1h Purchase",
                            "event_name": "Purchase",
                            "conversion_window_ms": 3_600_000
                        }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exp_resp.status(), StatusCode::OK);
    let exp_json: Value =
        serde_json::from_slice(&to_bytes(exp_resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let exp_id = exp_json["id"].as_str().unwrap();

    let res_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/projects/proj_p2/experiments/{exp_id}/results"))
                .header("authorization", format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res_resp.status(), StatusCode::OK);
    let res_json: Value =
        serde_json::from_slice(&to_bytes(res_resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let variants = res_json["variants"].as_array().unwrap();
    let ctrl_row = variants
        .iter()
        .find(|v| v["variant_key"] == "control")
        .unwrap();
    let treat_row = variants
        .iter()
        .find(|v| v["variant_key"] == "treatment")
        .unwrap();
    assert_eq!(
        ctrl_row["conversions"].as_u64(),
        Some(0),
        "2h conversion must be excluded by 1h conversion_window_ms!"
    );
    assert_eq!(treat_row["conversions"].as_u64(), Some(1));
}

#[test]
fn p2_10_srm_detection_holm_bonferroni_and_fieller_ci() {
    let mut skewed = BTreeMap::new();
    skewed.insert("control".to_string(), (650u64, 65u64));
    skewed.insert("treatment".to_string(), (350u64, 55u64));
    let mut weights = BTreeMap::new();
    weights.insert("control".to_string(), 5000u32);
    weights.insert("treatment".to_string(), 5000u32);

    let (srm_p, srm_flagged) = compute_sample_ratio_mismatch(&skewed, &weights);
    assert!(srm_p < 1e-6, "expected tiny SRM p-value, got {srm_p}");
    assert!(srm_flagged, "650/350 split against 50/50 must flag SRM");

    let mut balanced = BTreeMap::new();
    balanced.insert("control".to_string(), (502u64, 50u64));
    balanced.insert("treatment".to_string(), (498u64, 75u64));
    let (ok_p, ok_flagged) = compute_sample_ratio_mismatch(&balanced, &weights);
    assert!(ok_p > 0.5 && !ok_flagged);

    let mut multi = BTreeMap::new();
    multi.insert("control".to_string(), (1000, 100));
    multi.insert("treat_a".to_string(), (1000, 131)); // raw p ~ 0.030
    multi.insert("treat_b".to_string(), (1000, 102));
    multi.insert("treat_c".to_string(), (1000, 99));
    multi.insert("treat_d".to_string(), (1000, 101));

    let table = compute_statistical_lift_table(&multi);
    let treat_a = table.iter().find(|r| r.variant_key == "treat_a").unwrap();
    assert!(
        treat_a.p_value > 0.05 && !treat_a.statistically_significant,
        "Holm-Bonferroni across 4 treatments must adjust raw p~0.030 above 0.05, got p={}",
        treat_a.p_value
    );
    let naive_se = ((0.1 * 0.9 / 1000.0_f64) + (0.131 * 0.869 / 1000.0_f64)).sqrt();
    let naive_width = 2.0 * (1.959_964 * naive_se / 0.1) * 100.0;
    let fieller_width = treat_a.ci95_high_pct - treat_a.ci95_low_pct;
    assert!(
        fieller_width > naive_width,
        "Fieller CI ({fieller_width:.2}%) must account for p_c uncertainty (> naive {naive_width:.2}%)"
    );
}
