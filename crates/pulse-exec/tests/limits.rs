use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use pulse_exec::{
    eval_math_expr, validate_cohort_acyclic, BehavioralCohortDef, CatalogRegistry,
    CustomPropertyDef, ExecError, FlowDirection, FlowsQuery, FunnelExclusion, FunnelOrderMode,
    FunnelQuery, FunnelStepSpec, GlobalFilter, InsightsMetric, InsightsQuery, InsightsSeriesSpec,
    LookupTableDef, PropFilterOp, PropPredicate, QueryEngine, RetentionMode, RetentionQuery,
    SessionSearchQuery, TimeInterval, TimeRange,
};
use pulse_identity::IdentityRouter;
use pulse_ingest::{InMemoryGovernance, IngestService};
use pulse_storage::{StorageConfig, StorageEngine};
use pulse_wire::{PropValue, WireBatch, WireEvent};

fn ev(
    seq: u64,
    uid: &str,
    sess: &str,
    name: &str,
    ts_ms: i64,
    props: Vec<(&str, PropValue)>,
) -> WireEvent {
    WireEvent {
        event_uuid: WireEvent::deterministic_uuid(uid, sess, name, ts_ms, seq),
        routing_seed: uid.to_string(),
        distinct_id: uid.to_string(),
        session_id: sess.to_string(),
        event_name: name.to_string(),
        client_event_ts_ms: ts_ms,
        client_sent_ts_ms: 0,
        props: props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        replay_offset_kb: 0,
    }
}

#[test]
fn formula_unknown_var_and_trailing_junk_are_rejected() {
    let mut vars = HashMap::new();
    vars.insert("A".to_string(), 10.0);
    vars.insert("B".to_string(), 2.0);

    // Unknown variable must error, not silently return 0
    assert!(
        eval_math_expr("A + UNKNOWN_VAR", &vars).is_err(),
        "unknown variable should return Err"
    );

    // Trailing junk must error
    assert!(
        eval_math_expr("A B", &vars).is_err(),
        "trailing identifier should return Err"
    );
    assert!(
        eval_math_expr("(A)))", &vars).is_err(),
        "extra closing parens should return Err"
    );

    // Division by zero must return Ok(None), not 0.0
    assert_eq!(eval_math_expr("A / 0", &vars).unwrap(), None);
    assert_eq!(eval_math_expr("(A / (B - 2)) + 5", &vars).unwrap(), None);
    assert_eq!(eval_math_expr("(A / B) * 3", &vars).unwrap(), Some(15.0));
}

#[test]
fn formula_deep_unary_and_nested_parens_do_not_stack_overflow() {
    let mut vars = HashMap::new();
    vars.insert("A".to_string(), 42.0);

    // 2,000,000 unary minuses previously aborted the process via stack overflow
    let huge_unary = format!("{}A", "-".repeat(2_000_000));
    assert!(eval_math_expr(&huge_unary, &vars).is_err());

    // 200 nested parentheses (> depth 64) must error cleanly without recursing unboundedly
    let deep_parens = format!("{}A{}", "(".repeat(200), ")".repeat(200));
    assert!(eval_math_expr(&deep_parens, &vars).is_err());
}

#[tokio::test]
async fn cyclic_cohort_references_return_error_instead_of_infinite_recursion() {
    let store = Arc::new(InMemory::new());
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
    let cat = catalogs.for_project("proj_cycle");
    // Mutual cycle: coh_a -> coh_b -> coh_a
    cat.saved_cohorts.insert(
        "coh_a".to_string(),
        BehavioralCohortDef::CohortRef {
            cohort_id: "coh_b".to_string(),
        },
    );
    cat.saved_cohorts.insert(
        "coh_b".to_string(),
        BehavioralCohortDef::CohortRef {
            cohort_id: "coh_a".to_string(),
        },
    );

    let qengine = QueryEngine::new(storage, 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let res = qengine
        .preview_cohort_count(
            "proj_cycle",
            &BehavioralCohortDef::CohortRef {
                cohort_id: "coh_a".to_string(),
            },
            Some(TimeRange {
                start_ts_ms: 0,
                end_ts_ms: 10_000,
            }),
        )
        .await;
    assert!(
        res.is_err(),
        "cyclic cohort reference must fail with Err instead of recursing forever"
    );

    // Save-time cycle validator must also reject self-cycles and mutual cycles synchronously
    let cat_ref = qengine.catalogs().for_project("proj_cycle");
    assert!(validate_cohort_acyclic(
        Some("coh_self"),
        &BehavioralCohortDef::CohortRef {
            cohort_id: "coh_self".to_string()
        },
        &cat_ref
    )
    .is_err());
    assert!(validate_cohort_acyclic(
        Some("coh_c"),
        &BehavioralCohortDef::CohortRef {
            cohort_id: "coh_a".to_string()
        },
        &cat_ref
    )
    .is_err());
}

#[test]
fn previous_period_is_exact_inclusive_duration() {
    // [1_000, 1_999] is an inclusive 1,000 ms window; previous period must be [0, 999], not [1, 999]!
    let tr = TimeRange {
        start_ts_ms: 1_000,
        end_ts_ms: 1_999,
    };
    assert_eq!(tr.duration_ms(), 1_000);
    assert_eq!(
        tr.previous_period(),
        TimeRange {
            start_ts_ms: 0,
            end_ts_ms: 999,
        }
    );
}

#[tokio::test]
async fn custom_property_preserves_camel_case_identifiers() {
    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let cat = catalogs.for_project("proj_camel");
    cat.custom_properties.insert(
        "effectiveFee".to_string(),
        CustomPropertyDef {
            name: "effectiveFee".to_string(),
            sql_expr: "case when userPlan = 'Pro' then coalesce(baseFee, 0) * 2 else 10 end"
                .to_string(),
            referenced_props: Vec::new(),
        },
    );

    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let ts = 1_762_000_000_000i64;
    ingest
        .ingest_for_project(
            "proj_camel",
            WireBatch::new(vec![
                ev(
                    1,
                    "u1",
                    "s1",
                    "Pay",
                    ts,
                    vec![
                        ("userPlan", PropValue::Str("Pro".into())),
                        ("baseFee", PropValue::Int(25)),
                    ],
                ),
                ev(
                    2,
                    "u2",
                    "s2",
                    "Pay",
                    ts + 100,
                    vec![
                        ("userPlan", PropValue::Str("Free".into())),
                        ("baseFee", PropValue::Int(25)),
                    ],
                ),
            ]),
            ts + 1_000,
        )
        .await
        .unwrap();

    let res = qengine
        .query_insights(
            "proj_camel",
            &InsightsQuery {
                time_range: TimeRange {
                    start_ts_ms: ts - 1_000,
                    end_ts_ms: ts + 5_000,
                },
                interval: TimeInterval::Day,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "Pay".into(),
                    metric: InsightsMetric::PropSum {
                        prop: "effectiveFee".into(),
                    },
                    filters: Vec::new(),
                }],
                breakdown_by: Vec::new(),
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await
        .expect("mixed-case custom property SQL must evaluate without lowercasing identifiers");
    assert_eq!(res.series[0].overall_value, Some(60.0));

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn inverted_index_row_mask_matches_unmasked_on_mixed_types_and_high_cardinality() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let ts = 1_762_000_000_000i64;
    let mut batch = vec![
        // (a) Int(2) + Str("2") in the same segment
        ev(
            1,
            "u_int",
            "s1",
            "Evt",
            ts + 10,
            vec![("mixed_num", PropValue::Int(2))],
        ),
        ev(
            2,
            "u_str",
            "s2",
            "Evt",
            ts + 20,
            vec![("mixed_num", PropValue::Str("2".into()))],
        ),
        // (b) Bool(true) + Str("true") in the same segment
        ev(
            3,
            "u_b",
            "s3",
            "Evt",
            ts + 30,
            vec![("mixed_bool", PropValue::Bool(true))],
        ),
        ev(
            4,
            "u_bs",
            "s4",
            "Evt",
            ts + 40,
            vec![("mixed_bool", PropValue::Str("true".into()))],
        ),
    ];
    // (c) Bool(false) + > 1,024 distinct Str values in `hi_card` so Str exceeds MAX_INVERTED_INDEX_CARDINALITY while Bool remains!
    batch.push(ev(
        5,
        "u_hc_bool",
        "s5",
        "Evt",
        ts + 50,
        vec![("hi_card", PropValue::Bool(false))],
    ));
    for i in 0..1_100u64 {
        batch.push(ev(
            100 + i,
            &format!("u_hc_{i}"),
            &format!("s_hc_{i}"),
            "Evt",
            ts + 100 + i as i64,
            vec![("hi_card", PropValue::Str(format!("val_{i}")))],
        ));
    }

    ingest
        .ingest_for_project("proj_p201", WireBatch::new(batch), ts + 10_000)
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    let range = TimeRange {
        start_ts_ms: ts,
        end_ts_ms: ts + 5_000,
    };

    for (prop, target, expected_users) in [
        ("mixed_num", PropValue::Int(2), 2u64),
        ("mixed_num", PropValue::Str("2".into()), 2u64),
        ("mixed_bool", PropValue::Bool(true), 2u64),
        ("hi_card", PropValue::Str("val_777".into()), 1u64),
    ] {
        let pred = PropPredicate {
            prop_name: prop.to_string(),
            filter: PropFilterOp::Eq(target),
        };
        let ins = qengine
            .query_insights(
                "proj_p201",
                &InsightsQuery {
                    time_range: range,
                    interval: TimeInterval::Day,
                    series: vec![InsightsSeriesSpec {
                        label: "A".into(),
                        event_name: "Evt".into(),
                        metric: InsightsMetric::Uniques,
                        filters: vec![pred.clone()],
                    }],
                    breakdown_by: Vec::new(),
                    global_filter: GlobalFilter::default(),
                    compare_previous_period: false,
                    formula: None,
                    sampling: Default::default(),
                },
            )
            .await
            .unwrap();
        let fun = qengine
            .query_funnel(
                "proj_p201",
                &FunnelQuery {
                    time_range: range,
                    steps: vec![FunnelStepSpec {
                        event_name: "Evt".into(),
                        filters: vec![pred],
                        first_time_ever: false,
                    }],
                    order_mode: FunnelOrderMode::LooseOrder,
                    conversion_window_ms: 86_400_000,
                    exclusions: Vec::new(),
                    hold_constant_props: Vec::new(),
                    breakdown_by: None,
                    global_filter: GlobalFilter::default(),
                    sampling: Default::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            ins.series[0].overall_value,
            Some(expected_users as f64),
            "Insights masked query wrong for {prop}"
        );
        assert_eq!(
            fun.overall_steps[0].entered_users, expected_users,
            "Insights masked vs Funnel unmasked mismatch for {prop}"
        );
    }

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn semantics_and_governance_regression_suite() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let cat = catalogs.for_project("proj_sem");
    cat.set_timezone("Asia/Kolkata");
    cat.insert_lookup_table(
        LookupTableDef::from_csv_bytes(
            "merchants",
            "merchant_id",
            b"merchant_id,category\nm1,LookupCat\n",
        )
        .unwrap(),
    );

    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    // 2026-02-01 00:30:00 IST == 2026-01-31 19:00:00 UTC!
    // In UTC this is Jan 2026; in Asia/Kolkata this is Feb 1, 2026!
    let ist_feb1_0030 = chrono::DateTime::parse_from_rfc3339("2026-02-01T00:30:00+05:30")
        .unwrap()
        .timestamp_millis();
    let ist_mar1_0030 = chrono::DateTime::parse_from_rfc3339("2026-03-01T00:30:00+05:30")
        .unwrap()
        .timestamp_millis();

    ingest
        .ingest_for_project(
            "proj_sem",
            WireBatch::new(vec![
                ev(
                    1,
                    "u_neg1",
                    "s1",
                    "PnL",
                    ist_feb1_0030,
                    vec![
                        ("delta", PropValue::Float(-100.0)),
                        ("category", PropValue::Str("PhysicalWins".into())),
                        ("merchant_id", PropValue::Str("m1".into())),
                        ("nullable_col", PropValue::Null),
                    ],
                ),
                ev(
                    2,
                    "u_neg2",
                    "s2",
                    "PnL",
                    ist_feb1_0030 + 1_000,
                    vec![
                        ("delta", PropValue::Float(-50.0)),
                        ("merchant_id", PropValue::Str("m1".into())),
                    ],
                ),
                ev(
                    3,
                    "u_neg3",
                    "s3",
                    "PnL",
                    ist_mar1_0030,
                    vec![("delta", PropValue::Float(-10.0))],
                ),
            ]),
            ist_mar1_0030 + 5_000,
        )
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    // P2-09l: Explicit PropValue::Null must survive flush to Vortex segment (distinct from missing)
    let ps = storage.get_or_open_project("proj_sem").await.unwrap();
    let nullable_pk = ps
        .dictionary()
        .lookup_prop_key("nullable_col")
        .expect("nullable_col interned");
    let snap = ps.snapshot();
    let mut saw_explicit_null = false;
    for seg in snap.manifest.active_segments.values().flatten() {
        for r in storage.segment_reader().read_all_rows(seg).await.unwrap() {
            if r.event_ts_ms == ist_feb1_0030 {
                assert!(
                    r.props
                        .iter()
                        .any(|(k, v)| *k == nullable_pk && *v == PropValue::Null),
                    "explicit PropValue::Null was lost on segment flush: {:?}",
                    r.props
                );
                saw_explicit_null = true;
            }
        }
    }
    assert!(saw_explicit_null);

    let range = TimeRange {
        start_ts_ms: ist_feb1_0030 - 60_000,
        end_ts_ms: ist_mar1_0030 + 60_000,
    };

    // P2-09a & P2-09d: Negative percentiles must be negative (~ -50.0, NOT clamped to 0.0!)
    // and Month buckets in Asia/Kolkata must align to Feb 1 IST and Mar 1 IST!
    let ins_neg = qengine
        .query_insights(
            "proj_sem",
            &InsightsQuery {
                time_range: range,
                interval: TimeInterval::Month,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "PnL".into(),
                    metric: InsightsMetric::PropPercentile {
                        prop: "delta".into(),
                        quantile: 0.50,
                    },
                    filters: Vec::new(),
                }],
                breakdown_by: Vec::new(),
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();
    let p50 = ins_neg.series[0].overall_value.expect("p50 value");
    assert!(
        p50 < -40.0 && p50 > -60.0,
        "expected negative p50 around -50.0, got {p50}"
    );
    let ist_feb1_midnight = chrono::DateTime::parse_from_rfc3339("2026-02-01T00:00:00+05:30")
        .unwrap()
        .timestamp_millis();
    let ist_mar1_midnight = chrono::DateTime::parse_from_rfc3339("2026-03-01T00:00:00+05:30")
        .unwrap()
        .timestamp_millis();
    assert_eq!(
        ins_neg.buckets_ts_ms,
        vec![ist_feb1_midnight, ist_mar1_midnight],
        "Month buckets must align to 1st of month in Asia/Kolkata"
    );

    // P2-09b: Dau at Week interval must return InvalidQuery; Wau at Week interval anchors at end of bucket!
    let bad_dau = qengine
        .query_insights(
            "proj_sem",
            &InsightsQuery {
                time_range: range,
                interval: TimeInterval::Week,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "PnL".into(),
                    metric: InsightsMetric::Dau,
                    filters: Vec::new(),
                }],
                breakdown_by: Vec::new(),
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await;
    assert!(matches!(bad_dau, Err(ExecError::InvalidQuery(_))));

    let wau_week = qengine
        .query_insights(
            "proj_sem",
            &InsightsQuery {
                time_range: TimeRange {
                    start_ts_ms: ist_feb1_0030,
                    end_ts_ms: ist_feb1_0030 + 3_600_000,
                },
                interval: TimeInterval::Week,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "PnL".into(),
                    metric: InsightsMetric::Wau,
                    filters: Vec::new(),
                }],
                breakdown_by: Vec::new(),
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();
    // Both u_neg1 and u_neg2 acted inside that week bucket -> end-anchored WAU must see both (2.0), not lag!
    assert_eq!(wau_week.series[0].points[0].value, Some(2.0));

    // P2-09j: Physical `category` property wins over lookup, while qualified `merchants.category` resolves lookup!
    let phys_vs_lookup = qengine
        .query_insights(
            "proj_sem",
            &InsightsQuery {
                time_range: range,
                interval: TimeInterval::Month,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "PnL".into(),
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                }],
                breakdown_by: vec!["category".into(), "merchants.category".into()],
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();
    assert!(
        phys_vs_lookup
            .series
            .iter()
            .any(|s| s.breakdown_key == "PhysicalWins | LookupCat" && s.overall_value == Some(1.0)),
        "expected physical `category` to win and `merchants.category` to join: {:?}",
        phys_vs_lookup.series
    );

    // P2-09k: > 2 breakdown dimensions must error instead of silently dropping the 3rd
    let three_bdowns = qengine
        .query_insights(
            "proj_sem",
            &InsightsQuery {
                time_range: range,
                interval: TimeInterval::Day,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "PnL".into(),
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                }],
                breakdown_by: vec!["a".into(), "b".into(), "c".into()],
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await;
    assert!(matches!(three_bdowns, Err(ExecError::InvalidQuery(_))));

    // P2-09g: Out-of-range FunnelExclusion `between_steps` must return InvalidQuery (no shift panic!)
    let bad_excl = qengine
        .query_funnel(
            "proj_sem",
            &FunnelQuery {
                time_range: range,
                steps: vec![
                    FunnelStepSpec {
                        event_name: "PnL".into(),
                        filters: Vec::new(),
                        first_time_ever: false,
                    },
                    FunnelStepSpec {
                        event_name: "PnL".into(),
                        filters: Vec::new(),
                        first_time_ever: false,
                    },
                ],
                order_mode: FunnelOrderMode::LooseOrder,
                conversion_window_ms: 60_000,
                exclusions: vec![FunnelExclusion {
                    event_name: "Err".into(),
                    between_steps: (0, 99),
                }],
                hold_constant_props: Vec::new(),
                breakdown_by: None,
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await;
    assert!(matches!(bad_excl, Err(ExecError::InvalidQuery(_))));

    // P2-09d: Month Retention from Feb 1 IST must match Mar 1 IST return (28-day Feb calendar month, NOT 30 fixed days!)
    ingest
        .ingest_for_project(
            "proj_sem",
            WireBatch::new(vec![ev(
                99,
                "u_neg1",
                "s99",
                "PnL",
                ist_mar1_0030 + 500,
                Vec::new(),
            )]),
            ist_mar1_0030 + 5_000,
        )
        .await
        .unwrap();
    let ret_month = qengine
        .query_retention(
            "proj_sem",
            &RetentionQuery {
                time_range: TimeRange {
                    start_ts_ms: ist_feb1_midnight,
                    end_ts_ms: ist_feb1_midnight + 86_400_000,
                },
                start_event: "PnL".into(),
                return_event: "PnL".into(),
                mode: RetentionMode::NDay,
                unit: TimeInterval::Month,
                num_periods: 1,
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(ret_month.cohorts.len(), 1);
    assert_eq!(ret_month.cohorts[0].retained_counts, vec![2, 1]);

    // P3-02: Bucket cap (> 10,000 hourly buckets) must fail fast with InvalidQuery
    let huge_buckets = qengine
        .query_insights(
            "proj_sem",
            &InsightsQuery {
                time_range: TimeRange {
                    start_ts_ms: 0,
                    end_ts_ms: 20_000 * 3_600_000,
                },
                interval: TimeInterval::Hour,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "PnL".into(),
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                }],
                breakdown_by: Vec::new(),
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await;
    assert!(matches!(huge_buckets, Err(ExecError::InvalidQuery(_))));

    // Section 6 & P3-02: Flows `top_n_per_step` folds lower-ranked step events into `$Other`
    ingest
        .ingest_for_project(
            "proj_sem",
            WireBatch::new(vec![
                ev(201, "uf1", "sf1", "Anchor", ist_feb1_0030, Vec::new()),
                ev(202, "uf1", "sf1", "TopA", ist_feb1_0030 + 100, Vec::new()),
                ev(203, "uf2", "sf2", "Anchor", ist_feb1_0030, Vec::new()),
                ev(204, "uf2", "sf2", "TopA", ist_feb1_0030 + 100, Vec::new()),
                ev(205, "uf3", "sf3", "Anchor", ist_feb1_0030, Vec::new()),
                ev(206, "uf3", "sf3", "RareB", ist_feb1_0030 + 100, Vec::new()),
            ]),
            ist_feb1_0030 + 5_000,
        )
        .await
        .unwrap();
    let flows_top1 = qengine
        .query_flows(
            "proj_sem",
            &FlowsQuery {
                time_range: range,
                anchor_event: "Anchor".into(),
                direction: FlowDirection::Forward,
                steps: 1,
                top_n_per_step: Some(1),
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();
    assert!(flows_top1
        .nodes
        .iter()
        .any(|n| n.step_offset == 1 && n.event_name == "TopA" && n.user_count == 2));
    assert!(flows_top1
        .nodes
        .iter()
        .any(|n| n.step_offset == 1 && n.event_name == "$Other" && n.user_count == 1));

    // P3-02: Session search `limit` pushdown returns exact top-`limit` newest sessions
    let sess_res = qengine
        .search_sessions(
            "proj_sem",
            &SessionSearchQuery {
                time_range: Some(range),
                event_name: None,
                only_rage_tap: false,
                only_dead_tap: false,
                only_errors: false,
                only_with_replay: false,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
    assert_eq!(sess_res.sessions.len(), 2);
    assert!(sess_res.sessions[0].end_ts_ms >= sess_res.sessions[1].end_ts_ms);

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn per_query_deadline_enforces_timeout() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    let inner = InMemory::new();
    let throttled = Arc::new(ThrottledStore::new(
        inner,
        ThrottleConfig {
            wait_get_per_call: Duration::from_millis(80),
            ..Default::default()
        },
    ));
    let store: Arc<dyn ObjectStore> = throttled.clone();
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let ts = 1_762_000_000_000i64;
    ingest
        .ingest_for_project(
            "proj_timeout",
            WireBatch::new(vec![ev(1, "u1", "s1", "SlowEvt", ts, Vec::new())]),
            ts + 1_000,
        )
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    qengine.set_query_timeout(Duration::from_millis(10));
    let res = qengine
        .query_insights(
            "proj_timeout",
            &InsightsQuery {
                time_range: TimeRange {
                    start_ts_ms: ts - 1_000,
                    end_ts_ms: ts + 1_000,
                },
                interval: TimeInterval::Day,
                series: vec![InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "SlowEvt".into(),
                    metric: InsightsMetric::Uniques,
                    filters: Vec::new(),
                }],
                breakdown_by: Vec::new(),
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            },
        )
        .await;
    assert!(
        matches!(res, Err(ExecError::QueryTimeout(10))),
        "expected ExecError::QueryTimeout(10), got {res:?}"
    );

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn pr1_funnel_drop_off_prioritizes_recorded_sessions() {
    use pulse_wire::{InteractionKind, NodeKind, ReplayChunk, WireNode, WireframeOp};

    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let ts = 1_762_000_000_000i64;
    // Record session for only the 26th dropped user (`u_025`), after 25 unrecorded dropped users (`u_000..u_024`)
    storage
        .append_replay_chunk(&ReplayChunk {
            project_id: "proj_pr1".to_string(),
            session_id: "sess_rec_025".to_string(),
            chunk_start_ts_ms: ts,
            ops: vec![
                WireframeOp::FullSnapshot {
                    ts_offset_ms: 0,
                    width: 390,
                    height: 844,
                    nodes: vec![WireNode {
                        node_id: 1,
                        parent_id: 0,
                        kind: NodeKind::Screen,
                        x: 0,
                        y: 0,
                        w: 390,
                        h: 844,
                        bg_rgba: 0x090D_16FF,
                        masked_label: "Checkout".to_string(),
                    }],
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 400,
                    x: 100,
                    y: 200,
                    kind: InteractionKind::RageTap { count: 3 },
                },
            ],
        })
        .await
        .unwrap();

    let mut batch = Vec::new();
    for u in 0..30u64 {
        let uid = format!("u_{u:03}");
        let sid = if u == 25 {
            "sess_rec_025".to_string()
        } else {
            format!("sess_unrec_{u:03}")
        };
        batch.push(ev(
            u + 1,
            &uid,
            &sid,
            "Step_A",
            ts + (u as i64) * 100,
            Vec::new(),
        ));
    }
    ingest
        .ingest_for_project("proj_pr1", WireBatch::new(batch), ts + 10_000)
        .await
        .unwrap();

    let res = qengine
        .query_funnel(
            "proj_pr1",
            &FunnelQuery {
                time_range: TimeRange {
                    start_ts_ms: ts - 1_000,
                    end_ts_ms: ts + 10_000,
                },
                steps: vec![
                    FunnelStepSpec {
                        event_name: "Step_A".into(),
                        filters: Vec::new(),
                        first_time_ever: false,
                    },
                    FunnelStepSpec {
                        event_name: "Step_B".into(),
                        filters: Vec::new(),
                        first_time_ever: false,
                    },
                ],
                order_mode: FunnelOrderMode::LooseOrder,
                conversion_window_ms: 60_000,
                exclusions: Vec::new(),
                hold_constant_props: Vec::new(),
                breakdown_by: None,
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();

    let step1_samples: Vec<_> = res
        .dropped_sample_sessions
        .iter()
        .filter(|d| d.dropped_before_step == 1)
        .collect();
    assert!(!step1_samples.is_empty());
    assert!(
        step1_samples[0].has_replay && step1_samples[0].session_id == "sess_rec_025",
        "recorded session must appear first in dropped samples even after 25 unrecorded drops: {step1_samples:?}"
    );

    id_router.close_all().await.unwrap();
}

#[test]
fn pr8_en4_flexible_query_enums_and_readable_errors() {
    let insights_json = serde_json::json!({
        "time_range": { "start_ts_ms": 1_760_000_000_000i64, "end_ts_ms": 1_760_086_400_000i64 },
        "interval": "Hour",
        "series": [
            { "label": "Total", "event_name": "App_Open", "metric": "total_events" },
            { "label": "Unique", "event_name": "App_Open", "metric": "Unique_Users" },
            { "label": "Revenue", "event_name": "Paid", "metric": { "type": "Prop_Sum", "prop": "amount" } }
        ]
    });
    let iq: InsightsQuery =
        serde_json::from_value(insights_json).expect("flexible InsightsQuery deserialization");
    assert_eq!(iq.interval, TimeInterval::Hour);
    assert_eq!(iq.series[0].metric, InsightsMetric::Totals);
    assert_eq!(iq.series[1].metric, InsightsMetric::Uniques);
    assert_eq!(
        iq.series[2].metric,
        InsightsMetric::PropSum {
            prop: "amount".into()
        }
    );

    let funnel_json = serde_json::json!({
        "time_range": { "start_ts_ms": 1_760_000_000_000i64, "end_ts_ms": 1_760_086_400_000i64 },
        "steps": [{ "event_name": "Step_A" }, { "event_name": "Step_B" }],
        "order_mode": "Strict_Order",
        "conversion_window_ms": 60_000
    });
    let fq: FunnelQuery =
        serde_json::from_value(funnel_json).expect("flexible FunnelQuery deserialization");
    assert_eq!(fq.order_mode, FunnelOrderMode::StrictOrder);

    let ret_json = serde_json::json!({
        "time_range": { "start_ts_ms": 1_760_000_000_000i64, "end_ts_ms": 1_760_086_400_000i64 },
        "start_event": "App_Open",
        "return_event": "App_Open",
        "mode": "stickiness",
        "unit": "Weekly",
        "num_periods": 7
    });
    let rq: RetentionQuery =
        serde_json::from_value(ret_json).expect("flexible RetentionQuery deserialization");
    assert_eq!(rq.mode, RetentionMode::FrequencyStickiness);
    assert_eq!(rq.unit, TimeInterval::Week);

    let flows_json = serde_json::json!({
        "time_range": { "start_ts_ms": 1_760_000_000_000i64, "end_ts_ms": 1_760_086_400_000i64 },
        "anchor_event": "Checkout",
        "direction": "backward",
        "steps": 3,
        "top_n_per_step": 5
    });
    let flq: FlowsQuery =
        serde_json::from_value(flows_json).expect("flexible FlowsQuery deserialization");
    assert_eq!(flq.direction, FlowDirection::Reverse);

    let bad_interval = serde_json::json!({
        "time_range": { "start_ts_ms": 1, "end_ts_ms": 2 },
        "interval": "Year",
        "series": []
    });
    let err = serde_json::from_value::<InsightsQuery>(bad_interval).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("invalid interval 'Year': expected hour, day, week, or month"),
        "unexpected error message: {msg}"
    );
    assert!(!msg.contains("TimeInterval"), "leaked Rust enum: {msg}");
}

#[tokio::test]
async fn pr10_flows_hides_system_events_unless_anchored() {
    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = CatalogRegistry::new();
    let qengine = QueryEngine::new(storage.clone(), 16 * 1024 * 1024, None, 0, catalogs)
        .await
        .unwrap();

    let ts = 1_762_000_000_000i64;
    let mut batch = Vec::new();
    for u in 0..5u64 {
        let uid = format!("flow_u_{u}");
        let sid = format!("flow_s_{u}");
        batch.push(ev(
            u * 3 + 1,
            &uid,
            &sid,
            "App_Open",
            ts + 1_000,
            Vec::new(),
        ));
        batch.push(ev(
            u * 3 + 2,
            &uid,
            &sid,
            "$experiment_started",
            ts + 2_000,
            Vec::new(),
        ));
        batch.push(ev(
            u * 3 + 3,
            &uid,
            &sid,
            "Checkout",
            ts + 3_000,
            Vec::new(),
        ));
    }
    ingest
        .ingest_for_project("proj_pr10", WireBatch::new(batch), ts + 10_000)
        .await
        .unwrap();

    let range = TimeRange {
        start_ts_ms: ts,
        end_ts_ms: ts + 10_000,
    };

    let product_flows = qengine
        .query_flows(
            "proj_pr10",
            &FlowsQuery {
                time_range: range,
                anchor_event: "App_Open".to_string(),
                direction: FlowDirection::Forward,
                steps: 3,
                top_n_per_step: Some(10),
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();

    assert!(
        !product_flows
            .nodes
            .iter()
            .any(|n| n.event_name.starts_with('$') && n.event_name != "$Drop_Off"),
        "system $-prefixed events must not clutter user journeys anchored on App_Open: {:?}",
        product_flows.nodes
    );
    assert!(
        product_flows
            .nodes
            .iter()
            .any(|n| n.event_name == "Checkout" && n.step_offset == 1),
        "Checkout should advance directly to step +1 once system event is excluded: {:?}",
        product_flows.nodes
    );

    let exp_flows = qengine
        .query_flows(
            "proj_pr10",
            &FlowsQuery {
                time_range: range,
                anchor_event: "$experiment_started".to_string(),
                direction: FlowDirection::Forward,
                steps: 2,
                top_n_per_step: Some(10),
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();

    assert!(
        exp_flows
            .nodes
            .iter()
            .any(|n| n.event_name == "$experiment_started" && n.step_offset == 0),
        "anchoring directly on a $ system event must still include it: {:?}",
        exp_flows.nodes
    );

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn any_event_counts_a_return_through_any_app_event_but_not_system_events() {
    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let ingest = IngestService::new(storage.clone(), InMemoryGovernance::new());
    let qengine = QueryEngine::new(
        storage.clone(),
        16 * 1024 * 1024,
        None,
        0,
        CatalogRegistry::new(),
    )
    .await
    .unwrap();

    let day = 86_400_000i64;
    let ts = 1_762_000_000_000i64 - (1_762_000_000_000i64 % day);
    // a: back on day 1 with a different event; b: back on day 1 only via a system event; c: never back.
    let batch = vec![
        ev(1, "a", "s1", "Signup", ts + 1_000, Vec::new()),
        ev(2, "a", "s2", "Search", ts + day + 1_000, Vec::new()),
        ev(3, "b", "s3", "Signup", ts + 2_000, Vec::new()),
        ev(
            4,
            "b",
            "s4",
            "$experiment_started",
            ts + day + 2_000,
            Vec::new(),
        ),
        ev(5, "c", "s5", "Signup", ts + 3_000, Vec::new()),
    ];
    ingest
        .ingest_for_project("proj_any", WireBatch::new(batch), ts + 2 * day)
        .await
        .unwrap();

    let res = qengine
        .query_retention(
            "proj_any",
            &RetentionQuery {
                time_range: TimeRange {
                    start_ts_ms: ts,
                    end_ts_ms: ts + day,
                },
                start_event: "Signup".into(),
                return_event: pulse_exec::ANY_EVENT.into(),
                mode: RetentionMode::NDay,
                unit: TimeInterval::Day,
                num_periods: 2,
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            },
        )
        .await
        .unwrap();
    let row = &res.cohorts[0];
    assert_eq!(row.cohort_size, 3);
    assert_eq!(
        row.retained_counts[1], 1,
        "only `a` came back through an app event"
    );
}

async fn budget_engine() -> (
    Arc<StorageEngine>,
    Arc<IngestService>,
    Arc<QueryEngine>,
    Arc<IdentityRouter>,
) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 500_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let ingest = IngestService::new(storage.clone(), InMemoryGovernance::new());
    let qengine = QueryEngine::new(
        storage.clone(),
        16 * 1024 * 1024,
        None,
        0,
        CatalogRegistry::new(),
    )
    .await
    .unwrap();
    (storage, ingest, qengine, id_router)
}

fn two_step_funnel(range: TimeRange) -> FunnelQuery {
    let step = |name: &str| FunnelStepSpec {
        event_name: name.into(),
        filters: Vec::new(),
        first_time_ever: false,
    };
    FunnelQuery {
        time_range: range,
        steps: vec![step("A"), step("B")],
        order_mode: FunnelOrderMode::LooseOrder,
        conversion_window_ms: 7 * 86_400_000,
        exclusions: Vec::new(),
        hold_constant_props: Vec::new(),
        breakdown_by: None,
        global_filter: GlobalFilter::default(),
        sampling: Default::default(),
    }
}

/// The deadline used to be a `tokio::time::timeout`, which can't fire while rayon work runs inside
/// `block_in_place`; a funnel over one bot-like user (O(n²)) ran to completion whatever the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_deadline_interrupts_cpu_bound_funnel() {
    let (_storage, ingest, qengine, id_router) = budget_engine().await;
    let ts = 1_762_000_000_000i64;
    const N: i64 = 120_000;
    // One "B" before every "A", so B exists but never follows: each A scans to the end of the window.
    let mut events = vec![ev(0, "bot", "s1", "B", ts - 10, Vec::new())];
    events.extend((0..N).map(|i| ev(i as u64 + 1, "bot", "s1", "A", ts + i, Vec::new())));
    for (n, chunk) in events.chunks(20_000).enumerate() {
        ingest
            .ingest_for_project(
                "proj_cpu",
                WireBatch::new(chunk.to_vec()),
                ts + N + n as i64,
            )
            .await
            .unwrap();
    }
    let range = TimeRange {
        start_ts_ms: ts - 1_000,
        end_ts_ms: ts + N + 1_000,
    };

    qengine.set_query_timeout(Duration::from_millis(150));
    let started = std::time::Instant::now();
    let res = qengine
        .query_funnel("proj_cpu", &two_step_funnel(range))
        .await;
    let took = started.elapsed();
    assert!(
        matches!(res, Err(ExecError::QueryTimeout(150))),
        "expected QueryTimeout(150), got {res:?}"
    );
    assert!(
        took < Duration::from_secs(2),
        "query ran {took:?} past its deadline"
    );

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn budget_row_limit_rejects_oversized_scan() {
    let (storage, ingest, qengine, id_router) = budget_engine().await;
    let ts = 1_762_000_000_000i64;
    let events: Vec<_> = (0..500)
        .map(|i| ev(i, &format!("u{i}"), "s", "A", ts + i as i64, Vec::new()))
        .collect();
    ingest
        .ingest_for_project("proj_rows", WireBatch::new(events), ts + 1_000)
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();
    let range = TimeRange {
        start_ts_ms: ts - 1_000,
        end_ts_ms: ts + 1_000,
    };

    assert!(qengine
        .query_funnel("proj_rows", &two_step_funnel(range))
        .await
        .is_ok());
    qengine.set_max_rows(100);
    let res = qengine
        .query_funnel("proj_rows", &two_step_funnel(range))
        .await;
    assert!(
        matches!(
            res,
            Err(ExecError::QueryTooLarge {
                rows: 500,
                limit: 100
            })
        ),
        "expected QueryTooLarge, got {res:?}"
    );

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn budget_memory_limit_rejects_high_cardinality_breakdown() {
    let (storage, ingest, qengine, id_router) = budget_engine().await;
    let ts = 1_762_000_000_000i64;
    let events: Vec<_> = (0..500)
        .map(|i| {
            ev(
                i,
                &format!("u{i}"),
                "s",
                "page_view",
                ts + i as i64 * 1_000,
                vec![("url", PropValue::Str(format!("/item/{i}")))],
            )
        })
        .collect();
    ingest
        .ingest_for_project("proj_mem", WireBatch::new(events), ts + 1_000_000)
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    let query = InsightsQuery {
        time_range: TimeRange {
            start_ts_ms: ts - 1_000,
            end_ts_ms: ts + 1_000_000,
        },
        interval: TimeInterval::Hour,
        series: vec![InsightsSeriesSpec {
            label: "A".into(),
            event_name: "page_view".into(),
            metric: InsightsMetric::Totals,
            filters: Vec::new(),
        }],
        breakdown_by: vec!["url".into()],
        global_filter: GlobalFilter::default(),
        formula: None,
        compare_previous_period: false,
        sampling: Default::default(),
    };

    // Normal budget succeeds.
    assert!(qengine.query_insights("proj_mem", &query).await.is_ok());

    // Tight working-memory budget rejects cleanly with QueryMemoryExceeded (HTTP 422).
    qengine.set_max_memory_bytes(8 * 1024);
    let res = qengine.query_insights("proj_mem", &query).await;
    assert!(
        matches!(res, Err(ExecError::QueryMemoryExceeded { .. })),
        "expected QueryMemoryExceeded, got {res:?}"
    );

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn insights_unfiltered_totals_uses_zero_io_footer_fast_path() {
    let (storage, ingest, qengine, id_router) = budget_engine().await;
    // Align cleanly inside a single UTC day (2025-11-02T12:00:00Z).
    let ts = 1_762_084_800_000i64;
    let events: Vec<_> = (0..300)
        .map(|i| {
            let name = if i % 3 == 0 { "checkout" } else { "view" };
            ev(
                i,
                &format!("u{i}"),
                "s",
                name,
                ts + i as i64 * 1_000,
                Vec::new(),
            )
        })
        .collect();
    ingest
        .ingest_for_project("proj_footer", WireBatch::new(events), ts + 500_000)
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    let reads_before = storage
        .segment_reader()
        .tier_ab_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    let query = InsightsQuery {
        time_range: TimeRange {
            start_ts_ms: ts - 3_600_000,
            end_ts_ms: ts + 3_600_000,
        },
        interval: TimeInterval::Day,
        series: vec![InsightsSeriesSpec {
            label: "A".into(),
            event_name: "checkout".into(),
            metric: InsightsMetric::Totals,
            filters: Vec::new(),
        }],
        breakdown_by: Vec::new(),
        global_filter: GlobalFilter::default(),
        formula: None,
        compare_previous_period: false,
        sampling: Default::default(),
    };

    let res = qengine.query_insights("proj_footer", &query).await.unwrap();
    let reads_after = storage
        .segment_reader()
        .tier_ab_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(res.series.len(), 1);
    assert_eq!(res.series[0].overall_value, Some(100.0));
    assert_eq!(
        reads_after, reads_before,
        "unfiltered bucket-aligned Totals should answer from SegmentFooterMeta with 0 Vortex reads"
    );

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn insights_user_prop_filter_short_circuits_with_zero_vortex_reads() {
    let (storage, ingest, qengine, id_router) = budget_engine().await;
    let ts = 1_762_084_800_000i64;
    let events: Vec<_> = (0..200)
        .map(|i| {
            ev(
                i,
                &format!("u{i}"),
                "s",
                "purchase",
                ts + i as i64 * 1_000,
                Vec::new(),
            )
        })
        .collect();
    ingest
        .ingest_for_project("proj_uprop", WireBatch::new(events), ts + 500_000)
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    let id_graph = id_router.get_or_open("proj_uprop").await.unwrap();
    let cat = qengine.catalogs().for_project("proj_uprop");
    for i in 0..200 {
        let uid = id_graph.lookup_canonical(&format!("u{i}")).unwrap();
        let mut props = std::collections::HashMap::new();
        props.insert(
            "plan".to_string(),
            PropValue::Str(if i < 25 { "Pro" } else { "Free" }.to_string()),
        );
        cat.upsert_user_profile(uid, props);
    }

    // 1. User property filter with 0 matching users short-circuits with 0 Vortex reads.
    let reads_before = storage
        .segment_reader()
        .tier_ab_reads
        .load(std::sync::atomic::Ordering::Relaxed);
    let empty_q = InsightsQuery {
        time_range: TimeRange {
            start_ts_ms: ts - 3_600_000,
            end_ts_ms: ts + 3_600_000,
        },
        interval: TimeInterval::Day,
        series: vec![InsightsSeriesSpec {
            label: "A".into(),
            event_name: "purchase".into(),
            metric: InsightsMetric::Totals,
            filters: Vec::new(),
        }],
        breakdown_by: Vec::new(),
        global_filter: GlobalFilter {
            predicates: vec![PropPredicate::eq_str("user.plan", "Enterprise")],
            cohort: None,
        },
        formula: None,
        compare_previous_period: false,
        sampling: Default::default(),
    };
    let res_empty = qengine
        .query_insights("proj_uprop", &empty_q)
        .await
        .unwrap();
    assert_eq!(res_empty.series[0].overall_value, Some(0.0));
    assert_eq!(
        storage
            .segment_reader()
            .tier_ab_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        reads_before,
        "empty user.* bitmap must skip all Vortex reads"
    );

    // 2. Selective user property filter returns exact count (25 Pro users).
    let pro_q = InsightsQuery {
        global_filter: GlobalFilter {
            predicates: vec![PropPredicate::eq_str("user.plan", "Pro")],
            cohort: None,
        },
        ..empty_q
    };
    let res_pro = qengine.query_insights("proj_uprop", &pro_q).await.unwrap();
    assert_eq!(res_pro.series[0].overall_value, Some(25.0));

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn deterministic_entity_bucket_sampling_scales_counts_preserves_ratios_and_cuts_io() {
    use pulse_exec::SamplingMode;
    use std::sync::atomic::Ordering;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 500_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let ingest = IngestService::new(storage.clone(), InMemoryGovernance::new());
    let qengine = QueryEngine::new(
        storage.clone(),
        16 * 1024 * 1024,
        None,
        16 * 1024 * 1024,
        Arc::new(CatalogRegistry::default()),
    )
    .await
    .unwrap();
    let day0 = 1_790_000_000_000i64 - (1_790_000_000_000i64 % 86_400_000);

    // 1,600 distinct users spread across 16 deterministic entity buckets (~100 users/bucket).
    // Every user does "signup" on day 0 (amount = 100.0) and half do "purchase" on day 0 + 1h and day 1.
    let mut events = Vec::with_capacity(3_200);
    for u in 0..1_600u32 {
        let did = format!("user_{u:05}");
        events.push(ev(
            u as u64,
            &did,
            &format!("s0_{u}"),
            "signup",
            day0 + 1_000,
            vec![("amount", PropValue::Float(100.0))],
        ));
        if u % 2 == 0 {
            events.push(ev(
                u as u64 + 10_000,
                &did,
                &format!("s0_{u}"),
                "purchase",
                day0 + 3_600_000,
                vec![("amount", PropValue::Float(50.0))],
            ));
            events.push(ev(
                u as u64 + 20_000,
                &did,
                &format!("s1_{u}"),
                "purchase",
                day0 + 86_400_000 + 1_000,
                vec![("amount", PropValue::Float(50.0))],
            ));
        }
    }
    ingest
        .ingest_for_project("proj_samp", WireBatch::new(events), day0 + 2 * 86_400_000)
        .await
        .unwrap();
    storage.flush_all_projects().await.unwrap();

    let tr = TimeRange {
        start_ts_ms: day0,
        end_ts_ms: day0 + 2 * 86_400_000 - 1,
    };

    // 1. Auto mode on small dataset stays 100% exact.
    let auto_q = InsightsQuery {
        time_range: tr,
        interval: TimeInterval::Day,
        series: vec![
            InsightsSeriesSpec {
                label: "U".into(),
                event_name: "signup".into(),
                metric: InsightsMetric::Uniques,
                filters: vec![],
            },
            InsightsSeriesSpec {
                label: "Avg".into(),
                event_name: "signup".into(),
                metric: InsightsMetric::PropAvg {
                    prop: "amount".into(),
                },
                filters: vec![],
            },
            InsightsSeriesSpec {
                label: "Sum".into(),
                event_name: "signup".into(),
                metric: InsightsMetric::PropSum {
                    prop: "amount".into(),
                },
                filters: vec![],
            },
        ],
        breakdown_by: vec![],
        global_filter: GlobalFilter::default(),
        compare_previous_period: false,
        formula: None,
        sampling: SamplingMode::Auto,
    };
    let exact_res = qengine.query_insights("proj_samp", &auto_q).await.unwrap();
    assert!(!exact_res.sampling.as_ref().unwrap().is_sampled);
    assert_eq!(exact_res.series[0].overall_value, Some(1_600.0));
    assert_eq!(exact_res.series[1].overall_value, Some(100.0));
    assert_eq!(exact_res.series[2].overall_value, Some(160_000.0));

    // 2. Explicit 25% sampling (4 of 16 buckets) cuts Vortex reads by ~4x, scales Uniques & PropSum,
    //    and leaves PropAvg unscaled at exactly 100.0.
    let qengine_cold = QueryEngine::new(
        storage.clone(),
        16 * 1024 * 1024,
        None,
        16 * 1024 * 1024,
        Arc::new(CatalogRegistry::default()),
    )
    .await
    .unwrap();
    let reads_before = storage
        .segment_reader()
        .tier_ab_reads
        .load(Ordering::Relaxed);
    let mut samp_q = auto_q.clone();
    samp_q.sampling = SamplingMode::Rate(0.25);
    let samp_res = qengine_cold
        .query_insights("proj_samp", &samp_q)
        .await
        .unwrap();
    let reads_sampled = storage
        .segment_reader()
        .tier_ab_reads
        .load(Ordering::Relaxed)
        - reads_before;
    let sinfo = samp_res.sampling.as_ref().unwrap();
    assert!(sinfo.is_sampled);
    assert_eq!(sinfo.sampled_buckets, 4);
    assert_eq!(sinfo.total_buckets, 16);
    assert_eq!(sinfo.scale_factor, 4.0);
    assert!(sinfo.relative_error_pct.is_some());
    assert!(
        reads_sampled <= 8,
        "25% bucket sampling should read <= 8 segment Vortex blocks, read {reads_sampled}"
    );
    let est_uniques = samp_res.series[0].overall_value.unwrap();
    assert!(
        (est_uniques - 1_600.0).abs() < 400.0,
        "sampled uniques {est_uniques} should be close to 1600"
    );
    assert_eq!(
        samp_res.series[1].overall_value,
        Some(100.0),
        "PropAvg must NOT be scaled by sample factor"
    );
    let est_sum = samp_res.series[2].overall_value.unwrap();
    assert!((est_sum - est_uniques * 100.0).abs() < 1.0);

    // 3. Funnel with 25% sampling preserves ~50% conversion rate and scales entered_users.
    let fq = FunnelQuery {
        time_range: tr,
        steps: vec![
            FunnelStepSpec {
                event_name: "signup".into(),
                filters: vec![],
                first_time_ever: false,
            },
            FunnelStepSpec {
                event_name: "purchase".into(),
                filters: vec![],
                first_time_ever: false,
            },
        ],
        order_mode: FunnelOrderMode::LooseOrder,
        conversion_window_ms: 86_400_000,
        exclusions: vec![],
        hold_constant_props: vec![],
        breakdown_by: None,
        global_filter: GlobalFilter::default(),
        sampling: SamplingMode::Rate(0.25),
    };
    let fres = qengine.query_funnel("proj_samp", &fq).await.unwrap();
    assert!(fres.sampling.as_ref().unwrap().is_sampled);
    assert!(
        (fres.overall_steps[1].conversion_from_start_pct - 50.0).abs() < 12.0,
        "sampled funnel conversion {}% should be near 50%",
        fres.overall_steps[1].conversion_from_start_pct
    );

    // 4. Retention with 25% sampling preserves ~50% day-1 retention and scales cohort_size.
    let rq = RetentionQuery {
        time_range: tr,
        start_event: "signup".into(),
        return_event: "purchase".into(),
        mode: RetentionMode::NDay,
        unit: TimeInterval::Day,
        num_periods: 1,
        global_filter: GlobalFilter::default(),
        sampling: SamplingMode::Rate(0.25),
    };
    let rres = qengine.query_retention("proj_samp", &rq).await.unwrap();
    assert!(rres.sampling.as_ref().unwrap().is_sampled);
    assert!(
        (rres.cohorts[0].retained_pcts[1] - 50.0).abs() < 12.0,
        "sampled retention {}% should be near 50%",
        rres.cohorts[0].retained_pcts[1]
    );

    // 5. Flows with 25% sampling reports sampling metadata and scales anchor node counts.
    let flq = FlowsQuery {
        time_range: tr,
        anchor_event: "signup".into(),
        direction: FlowDirection::Forward,
        steps: 2,
        top_n_per_step: Some(10),
        global_filter: GlobalFilter::default(),
        sampling: SamplingMode::Rate(0.25),
    };
    let flres = qengine.query_flows("proj_samp", &flq).await.unwrap();
    assert!(flres.sampling.as_ref().unwrap().is_sampled);
    assert!(!flres.nodes.is_empty());

    id_router.close_all().await.unwrap();
}
