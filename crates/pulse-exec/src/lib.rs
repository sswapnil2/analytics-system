pub mod ast;
pub mod budget;
pub mod cache;
pub mod catalog;
pub mod engine;
pub mod flows;
pub mod funnel;
pub mod insights;
pub mod retention;
pub mod scanner;

pub use ast::*;
pub use cache::{SegmentBlockCache, TierAColumns};
pub use catalog::{
    arrow_array_to_prop_values, evaluate_custom_property, extract_sql_identifiers, CatalogRegistry,
    CustomEventDef, CustomPropertyDef, LookupTableDef, VirtualCatalog,
};
pub use engine::{ExportedEventRow, QueryEngine};
pub use flows::FlowsEngine;
pub use funnel::FunnelEngine;
pub use insights::{eval_math_expr, InsightsEngine};
pub use retention::RetentionEngine;
pub use scanner::{validate_cohort_acyclic, ExecError, Result, ANY_EVENT, MAX_COHORT_DEPTH};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use pulse_identity::IdentityRouter;
    use pulse_ingest::{IdentifyRequest, InMemoryGovernance, IngestService};
    use pulse_storage::{StorageConfig, StorageEngine};
    use pulse_wire::{PropValue, WireBatch, WireEvent};

    use super::*;

    fn make_ev(
        seq: u64,
        routing_seed: &str,
        distinct_id: &str,
        session_id: &str,
        event_name: &str,
        ts_ms: i64,
        props: Vec<(&str, PropValue)>,
    ) -> WireEvent {
        WireEvent {
            event_uuid: WireEvent::deterministic_uuid(
                distinct_id,
                session_id,
                event_name,
                ts_ms,
                seq,
            ),
            routing_seed: routing_seed.to_string(),
            distinct_id: distinct_id.to_string(),
            session_id: session_id.to_string(),
            event_name: event_name.to_string(),
            client_event_ts_ms: ts_ms,
            client_sent_ts_ms: ts_ms + 50,
            props: props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            replay_offset_kb: (seq % 64) as u32,
        }
    }

    #[tokio::test]
    async fn gate3_virtual_schema_cross_bucket_and_formula_execution() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        let id_router = Arc::new(IdentityRouter::new(store.clone()));

        let cfg = StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 50_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        };
        let storage = StorageEngine::new(cfg, store, id_router.clone());
        let gov = InMemoryGovernance::new();
        gov.register_api_key("pk_diff", "proj_diff");
        let ingest = IngestService::new(storage.clone(), gov);

        let catalogs = CatalogRegistry::new();
        let qengine = QueryEngine::new(
            storage.clone(),
            32 * 1024 * 1024,
            Some(&tmp_dir.path().join("foyer_cache")),
            64 * 1024 * 1024,
            catalogs.clone(),
        )
        .await
        .expect("open QueryEngine");

        let day0 = TimeInterval::Day.align_floor(1_762_000_000_000);
        let server_now = day0 + 5 * 86_400_000;
        let mut raw_events: Vec<WireEvent> = Vec::new();
        let mut seq = 1u64;

        // Same-bucket anonymous-to-identified users with merchant lookup keys
        for u in 0..20 {
            let seed = format!("seed_same_{u}");
            let anon = format!("anon_same_{u}");
            let user = format!("user_same_{u}");
            let sess = format!("sess_same_{u}");
            let t = day0 + 10_000 + u * 1_000;

            raw_events.push(make_ev(
                seq,
                &seed,
                &anon,
                &sess,
                "Step_0",
                t,
                vec![
                    ("payment_method", PropValue::Str("upi".into())),
                    ("amount", PropValue::Int(1500)),
                    ("merchant_id", PropValue::Str("m_swiggy".into())),
                ],
            ));
            seq += 1;

            ingest
                .identify_for_project(
                    "proj_diff",
                    IdentifyRequest {
                        anon_id: anon,
                        user_id: user.clone(),
                        routing_seed: Some(seed.clone()),
                        anon_routing_seed: Some(seed.clone()),
                        user_properties: None,
                        operations: None,
                    },
                )
                .await
                .unwrap();

            raw_events.push(make_ev(
                seq,
                &seed,
                &user,
                &sess,
                "Step_3",
                t + 20_000,
                vec![
                    ("payment_method", PropValue::Str("upi".into())),
                    ("amount", PropValue::Int(1500)),
                    ("merchant_id", PropValue::Str("m_swiggy".into())),
                ],
            ));
            seq += 1;
        }

        // Cross-bucket users (iOS seed -> Web seed)
        for u in 0..15 {
            let seed_a = format!("seed_ios_device_{u}_alpha");
            let seed_b = format!("seed_web_laptop_{u}_omega");
            let anon_a = format!("anon_ios_{u}");
            let user = format!("user_cross_{u}");
            let t = day0 + 50_000 + u * 1_000;

            raw_events.push(make_ev(
                seq,
                &seed_a,
                &anon_a,
                &format!("s_ios_{u}"),
                "Step_0",
                t,
                vec![
                    ("payment_method", PropValue::Str("card".into())),
                    ("amount", PropValue::Int(500)),
                    ("merchant_id", PropValue::Str("m_zepto".into())),
                ],
            ));
            seq += 1;

            ingest
                .identify_for_project(
                    "proj_diff",
                    IdentifyRequest {
                        anon_id: anon_a,
                        user_id: user.clone(),
                        routing_seed: Some(seed_b.clone()),
                        anon_routing_seed: Some(seed_a.clone()),
                        user_properties: None,
                        operations: None,
                    },
                )
                .await
                .unwrap();

            raw_events.push(make_ev(
                seq,
                &seed_b,
                &user,
                &format!("s_web_{u}"),
                "Step_3",
                t + 15_000,
                vec![
                    ("payment_method", PropValue::Str("card".into())),
                    ("amount", PropValue::Int(500)),
                    ("merchant_id", PropValue::Str("m_zepto".into())),
                ],
            ));
            seq += 1;
        }

        for ev in &mut raw_events {
            ev.client_sent_ts_ms = server_now;
        }

        let split_at = (raw_events.len() * 7) / 10;
        ingest
            .ingest_for_project(
                "proj_diff",
                WireBatch::new(raw_events[..split_at].to_vec()),
                server_now,
            )
            .await
            .unwrap();
        storage.flush_all_projects().await.unwrap();
        ingest
            .ingest_for_project(
                "proj_diff",
                WireBatch::new(raw_events[split_at..].to_vec()),
                server_now,
            )
            .await
            .unwrap();

        let cat = catalogs.for_project("proj_diff");
        cat.custom_properties.insert(
            "value_tier".to_string(),
            CustomPropertyDef {
                name: "value_tier".to_string(),
                sql_expr: "CASE WHEN amount > 1000 THEN 'High' ELSE 'Standard' END".to_string(),
                referenced_props: vec!["amount".to_string()],
            },
        );
        let csv_lookup = b"merchant_id,category\nm_swiggy,Food_Delivery\nm_zepto,Quick_Commerce\n";
        cat.insert_lookup_table(
            LookupTableDef::from_csv_bytes("merchants", "merchant_id", csv_lookup).unwrap(),
        );

        let full_window = TimeRange {
            start_ts_ms: day0,
            end_ts_ms: day0 + 86_400_000 - 1,
        };
        let ins_query = InsightsQuery {
            time_range: full_window,
            interval: TimeInterval::Day,
            series: vec![
                InsightsSeriesSpec {
                    label: "A".to_string(),
                    event_name: "Step_0".to_string(),
                    metric: InsightsMetric::Uniques,
                    filters: Vec::new(),
                },
                InsightsSeriesSpec {
                    label: "B".to_string(),
                    event_name: "Step_3".to_string(),
                    metric: InsightsMetric::Uniques,
                    filters: Vec::new(),
                },
            ],
            breakdown_by: vec!["value_tier".to_string(), "merchants.category".to_string()],
            global_filter: GlobalFilter::default(),
            compare_previous_period: true,
            formula: Some("(B / A) * 100".to_string()),
        };
        let ins_res = qengine
            .query_insights("proj_diff", &ins_query)
            .await
            .expect("insights");
        assert!(ins_res
            .series
            .iter()
            .any(|s| s.breakdown_key == "High | Food_Delivery" && s.overall_value == Some(20.0)));
        assert!(ins_res.series.iter().any(
            |s| s.breakdown_key == "Standard | Quick_Commerce" && s.overall_value == Some(15.0)
        ));
        assert!(!ins_res.formula_series.is_empty());

        id_router.close_all().await.unwrap();
    }

    #[tokio::test]
    async fn gate3_benchmark_1m_events_4step_funnel_under_50ms() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        let id_router = Arc::new(IdentityRouter::new(store.clone()));

        let cfg = StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 250_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 8,
        };
        let storage = StorageEngine::new(cfg, store, id_router.clone());
        let gov = InMemoryGovernance::new();
        let ingest = IngestService::new(storage.clone(), gov);
        let catalogs = CatalogRegistry::new();
        let qengine = QueryEngine::new(
            storage.clone(),
            128 * 1024 * 1024,
            None,
            64 * 1024 * 1024,
            catalogs,
        )
        .await
        .unwrap();

        let base_ts = TimeInterval::Day.align_floor(1_763_000_000_000);
        let step_names = [
            "App_Open",
            "KYC_PAN_Submitted",
            "UPI_Autopay_Mandate_Approved",
            "First_Disbursal_Success",
            "Feed_Scroll",
            "View_Offer",
        ];

        // Ingest 1,000,000 events across 25,000 users in 20 batches of 50,000 events
        let batch_len = 50_000usize;
        for b_idx in 0..20usize {
            let mut batch_events = Vec::with_capacity(batch_len);
            let offset_base = b_idx * batch_len;
            for j in 0..batch_len {
                let i = offset_base + j;
                let uid = i % 25_000;
                let stage = (i / 25_000) % 6;
                let ev_name = if stage == 1 && uid % 10 >= 7 {
                    "Feed_Scroll"
                } else if stage == 2 && uid % 10 >= 5 {
                    "View_Offer"
                } else if stage == 3 && uid % 10 >= 3 {
                    "Feed_Scroll"
                } else {
                    step_names[stage]
                };

                let ts = base_ts + ((i as i64) * 40);
                let distinct_id = format!("u_{uid:05}");
                batch_events.push(WireEvent {
                    event_uuid: [
                        (i & 0xff) as u8,
                        ((i >> 8) & 0xff) as u8,
                        ((i >> 16) & 0xff) as u8,
                        ((i >> 24) & 0xff) as u8,
                        b_idx as u8,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                    ],
                    routing_seed: distinct_id.clone(),
                    distinct_id,
                    session_id: format!("s_{uid:05}"),
                    event_name: ev_name.to_string(),
                    client_event_ts_ms: ts,
                    client_sent_ts_ms: ts + 10,
                    props: Vec::new(),
                    replay_offset_kb: 0,
                });
            }
            ingest
                .ingest_for_project(
                    "proj_bench_1m",
                    WireBatch::new(batch_events),
                    base_ts + 50_000_000,
                )
                .await
                .unwrap();
        }
        storage.flush_all_projects().await.unwrap();

        let ps = storage.get_or_open_project("proj_bench_1m").await.unwrap();
        assert_eq!(ps.snapshot().manifest.total_l1_rows(), 1_000_000);

        let query = FunnelQuery {
            time_range: TimeRange {
                start_ts_ms: base_ts,
                end_ts_ms: base_ts + 86_400_000,
            },
            steps: [
                "App_Open",
                "KYC_PAN_Submitted",
                "UPI_Autopay_Mandate_Approved",
                "First_Disbursal_Success",
            ]
            .into_iter()
            .map(|s| FunnelStepSpec {
                event_name: s.to_string(),
                filters: Vec::new(),
                first_time_ever: false,
            })
            .collect(),
            order_mode: FunnelOrderMode::LooseOrder,
            conversion_window_ms: 86_400_000,
            exclusions: Vec::new(),
            hold_constant_props: Vec::new(),
            breakdown_by: None,
            global_filter: GlobalFilter::default(),
        };

        let warmup = qengine.query_funnel("proj_bench_1m", &query).await.unwrap();
        assert_eq!(warmup.overall_steps[0].entered_users, 25_000);
        assert_eq!(warmup.overall_steps[1].entered_users, 17_500);
        assert_eq!(warmup.overall_steps[2].entered_users, 12_500);
        assert_eq!(warmup.overall_steps[3].entered_users, 7_500);

        let t0 = Instant::now();
        let res = qengine.query_funnel("proj_bench_1m", &query).await.unwrap();
        let elapsed = t0.elapsed();
        println!(
            "[BENCHMARK] 1,000,000 stored events 4-step Funnel latency: {:?} (cold={:.2}ms)",
            elapsed,
            warmup.exec_time_us as f64 / 1000.0
        );

        #[cfg(not(debug_assertions))]
        {
            assert!(
                Duration::from_micros(warmup.exec_time_us) < Duration::from_millis(250),
                "expected < 250ms cold scan for 1,000,000-event 4-step Funnel in --release, took {:.2}ms",
                warmup.exec_time_us as f64 / 1000.0
            );
            assert!(
                elapsed < Duration::from_millis(50),
                "expected < 50ms for 1,000,000-event 4-step Funnel in --release, took {elapsed:?}"
            );
        }
        assert_eq!(res.overall_steps[3].entered_users, 7_500);

        id_router.close_all().await.unwrap();
    }

    #[tokio::test]
    async fn search_sessions_merges_l0_and_multiple_l1_segments_and_applies_rage_tap_once() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let id_router = Arc::new(IdentityRouter::new(store.clone()));
        let storage = StorageEngine::new(StorageConfig::default(), store, id_router.clone());
        let gov = InMemoryGovernance::new();
        let ingest = IngestService::new(storage.clone(), gov);
        let catalogs = CatalogRegistry::new();
        let qengine = QueryEngine::new(
            storage.clone(),
            64 * 1024 * 1024,
            None,
            64 * 1024 * 1024,
            catalogs,
        )
        .await
        .unwrap();

        let base_ts: i64 = 1_760_000_000_000;
        let mk_ev = |seq: u8, uid: &str, sid: &str, name: &str, ts: i64| WireEvent {
            event_uuid: [seq, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0, 0, 0, 0, seq],
            routing_seed: uid.to_string(),
            distinct_id: uid.to_string(),
            session_id: sid.to_string(),
            event_name: name.to_string(),
            client_event_ts_ms: ts,
            client_sent_ts_ms: ts + 5,
            props: Vec::new(),
            replay_offset_kb: 0,
        };

        // Flush segment 1 with early events for both sessions
        ingest
            .ingest_for_project(
                "proj_sess_merge",
                WireBatch::new(vec![
                    mk_ev(1, "user_a", "sess_multi", "App_Open", base_ts),
                    mk_ev(2, "user_b", "sess_calm", "App_Open", base_ts + 100),
                ]),
                base_ts + 500,
            )
            .await
            .unwrap();
        storage.flush_all_projects().await.unwrap();

        // Flush segment 2 with mid-session event for sess_multi
        ingest
            .ingest_for_project(
                "proj_sess_merge",
                WireBatch::new(vec![mk_ev(
                    3,
                    "user_a",
                    "sess_multi",
                    "Checkout_Tap",
                    base_ts + 2_000,
                )]),
                base_ts + 2_500,
            )
            .await
            .unwrap();
        storage.flush_all_projects().await.unwrap();

        // Leave a 3rd event in hot L0 for sess_multi
        ingest
            .ingest_for_project(
                "proj_sess_merge",
                WireBatch::new(vec![mk_ev(
                    4,
                    "user_a",
                    "sess_multi",
                    "Checkout_Done",
                    base_ts + 5_000,
                )]),
                base_ts + 5_500,
            )
            .await
            .unwrap();

        // Record replay chunks: sess_multi has a rage tap; sess_calm does not.
        storage
            .append_replay_chunk(&pulse_wire::ReplayChunk {
                project_id: "proj_sess_merge".into(),
                session_id: "sess_multi".into(),
                chunk_start_ts_ms: base_ts,
                ops: vec![pulse_wire::WireframeOp::PointerInteraction {
                    ts_offset_ms: 2_100,
                    x: 120,
                    y: 420,
                    kind: pulse_wire::InteractionKind::RageTap { count: 4 },
                }],
            })
            .await
            .unwrap();
        storage
            .append_replay_chunk(&pulse_wire::ReplayChunk {
                project_id: "proj_sess_merge".into(),
                session_id: "sess_calm".into(),
                chunk_start_ts_ms: base_ts + 100,
                ops: vec![pulse_wire::WireframeOp::PointerInteraction {
                    ts_offset_ms: 500,
                    x: 80,
                    y: 200,
                    kind: pulse_wire::InteractionKind::Tap,
                }],
            })
            .await
            .unwrap();

        let all = qengine
            .search_sessions(
                "proj_sess_merge",
                &SessionSearchQuery {
                    time_range: None,
                    event_name: None,
                    only_rage_tap: false,
                    only_dead_tap: false,
                    only_errors: false,
                    only_with_replay: true,
                    limit: Some(20),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            all.sessions.len(),
            2,
            "each session must appear exactly once, never split across L0/L1 segments"
        );
        let multi = all
            .sessions
            .iter()
            .find(|s| s.session_id == "sess_multi")
            .unwrap();
        assert_eq!(multi.event_count, 3);
        assert_eq!(multi.start_ts_ms, base_ts);
        assert_eq!(multi.end_ts_ms, base_ts + 5_000);
        assert_eq!(multi.duration_ms, 5_000);
        assert!(multi.has_rage_tap);
        assert!(multi.has_replay);

        let calm = all
            .sessions
            .iter()
            .find(|s| s.session_id == "sess_calm")
            .unwrap();
        assert_eq!(calm.event_count, 1);
        assert!(!calm.has_rage_tap);
        assert!(calm.has_replay);

        let rage_only = qengine
            .search_sessions(
                "proj_sess_merge",
                &SessionSearchQuery {
                    time_range: None,
                    event_name: None,
                    only_rage_tap: true,
                    only_dead_tap: false,
                    only_errors: false,
                    only_with_replay: true,
                    limit: Some(20),
                },
            )
            .await
            .unwrap();
        assert_eq!(rage_only.sessions.len(), 1);
        assert_eq!(rage_only.sessions[0].session_id, "sess_multi");

        id_router.close_all().await.unwrap();
    }
}
