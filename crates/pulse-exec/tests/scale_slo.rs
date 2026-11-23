use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use pulse_exec::{
    FlowDirection, FlowsQuery, FunnelOrderMode, FunnelQuery, FunnelStepSpec, GlobalFilter,
    InsightsMetric, InsightsQuery, InsightsSeriesSpec, QueryEngine, RetentionMode, RetentionQuery,
    TimeInterval, TimeRange,
};
use pulse_identity::IdentityRouter;
use pulse_ingest::{InMemoryGovernance, IngestService};
use pulse_storage::{route_bucket, StorageConfig, StorageEngine};
use pulse_wire::{PropValue, WireBatch, WireEvent};

struct ScaleConfig {
    label: String,
    total_events: usize,
    num_users: usize,
    batch_size: usize,
    cold_iters: usize,
    warm_iters: usize,
    flows_iters: usize,
    concurrent_waves: usize,
}

impl ScaleConfig {
    fn from_env() -> Self {
        if let Ok(raw_n) = env::var("PULSE_BENCH_EVENTS") {
            if let Ok(n) = raw_n.trim().parse::<usize>() {
                let n = n.max(10_000);
                let batch_size = (n / 20).clamp(5_000, 50_000);
                let num_users = (n / 40).clamp(1_000, 250_000);
                return Self {
                    label: format!("{n}_events"),
                    total_events: n,
                    num_users,
                    batch_size,
                    cold_iters: if n >= 5_000_000 { 3 } else { 5 },
                    warm_iters: if n >= 5_000_000 { 10 } else { 20 },
                    flows_iters: if n >= 5_000_000 { 8 } else { 15 },
                    concurrent_waves: 2,
                };
            }
        }

        let profile = env::var("PULSE_BENCH_SCALE")
            .unwrap_or_else(|_| "100k".to_string())
            .to_ascii_lowercase();
        match profile.as_str() {
            "10m" => Self {
                label: "10M".to_string(),
                total_events: 10_000_000,
                num_users: 200_000,
                batch_size: 50_000,
                cold_iters: 3,
                warm_iters: 10,
                flows_iters: 8,
                concurrent_waves: 2,
            },
            "5m" => Self {
                label: "5M".to_string(),
                total_events: 5_000_000,
                num_users: 100_000,
                batch_size: 50_000,
                cold_iters: 3,
                warm_iters: 12,
                flows_iters: 10,
                concurrent_waves: 2,
            },
            "1m" => Self {
                label: "1M".to_string(),
                total_events: 1_000_000,
                num_users: 25_000,
                batch_size: 50_000,
                cold_iters: 5,
                warm_iters: 20,
                flows_iters: 15,
                concurrent_waves: 2,
            },
            _ => Self {
                label: "100K".to_string(),
                total_events: 100_000,
                num_users: 5_000,
                batch_size: 10_000,
                cold_iters: 5,
                warm_iters: 25,
                flows_iters: 15,
                concurrent_waves: 2,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LatencyStats {
    samples: usize,
    min_ms: f64,
    p50_ms: f64,
    p75_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct SloBudget {
    p50_ms: f64,
    p75_ms: f64,
    p99_ms: f64,
}

fn compute_stats(samples_us: &[u64]) -> LatencyStats {
    assert!(!samples_us.is_empty(), "need at least 1 latency sample");
    let mut sorted = samples_us.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let pick = |pct: f64| -> f64 {
        let idx = ((pct * ((n - 1) as f64)).ceil() as usize).min(n - 1);
        sorted[idx] as f64 / 1000.0
    };
    LatencyStats {
        samples: n,
        min_ms: sorted[0] as f64 / 1000.0,
        p50_ms: pick(0.50),
        p75_ms: pick(0.75),
        p95_ms: pick(0.95),
        p99_ms: pick(0.99),
        max_ms: sorted[n - 1] as f64 / 1000.0,
    }
}

fn record_row(
    rows: &mut Vec<(&'static str, LatencyStats, SloBudget, bool)>,
    failures: &mut Vec<String>,
    name: &'static str,
    samples_us: &[u64],
    slo: SloBudget,
) {
    let stats = compute_stats(samples_us);
    let pass =
        stats.p50_ms <= slo.p50_ms && stats.p75_ms <= slo.p75_ms && stats.p99_ms <= slo.p99_ms;
    if !pass {
        failures.push(format!(
            "{name}: p50={:.2}ms (<= {:.2}ms), p75={:.2}ms (<= {:.2}ms), p99={:.2}ms (<= {:.2}ms)",
            stats.p50_ms, slo.p50_ms, stats.p75_ms, slo.p75_ms, stats.p99_ms, slo.p99_ms
        ));
    }
    rows.push((name, stats, slo, pass));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scale_latency_percentiles_and_slo_harness() {
    let cfg = ScaleConfig::from_env();
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
    let id_router = Arc::new(IdentityRouter::new(store.clone()));

    let storage_cfg = StorageConfig {
        num_buckets: 16,
        l0_flush_row_threshold: 250_000,
        l0_flush_max_age: Duration::from_secs(600),
        compaction_min_segments: 8,
    };
    let storage = StorageEngine::new(storage_cfg, store, id_router.clone());
    let gov = InMemoryGovernance::new();
    let ingest = IngestService::new(storage.clone(), gov);
    let catalogs = pulse_exec::CatalogRegistry::new();

    let project_id = "proj_scale_bench";
    let base_ts = TimeInterval::Day.align_floor(1_763_000_000_000);
    let span_days = 14i64;
    let server_now = base_ts + span_days * 86_400_000 + 60_000;

    let plans = ["basic", "upi_plus", "credit_pro"];
    let cities = ["Bengaluru", "Mumbai", "Delhi_NCR", "Hyderabad", "Pune"];
    let pay_methods = ["upi_autopay", "upi_intent", "card", "netbanking"];
    let merchants = ["m_swiggy", "m_zepto", "m_zomato", "m_cred"];

    let mut ingest_samples_us = Vec::new();
    let seed_t0 = Instant::now();
    let num_batches = cfg.total_events.div_ceil(cfg.batch_size);

    for b_idx in 0..num_batches {
        let offset_base = b_idx * cfg.batch_size;
        let len = (cfg.total_events - offset_base).min(cfg.batch_size);
        let mut batch_events = Vec::with_capacity(len);

        for j in 0..len {
            let i = offset_base + j;
            let uid = i % cfg.num_users;
            let visit_idx = i / cfg.num_users;
            let stage = visit_idx % 6;
            let day_idx = ((visit_idx + uid) % (span_days as usize)) as i64;
            let ts = base_ts
                + day_idx * 86_400_000
                + ((visit_idx as i64) * 37_000)
                + ((uid as i64) % 1000);

            let ev_name = if i % 20 == 0 {
                "$session_start"
            } else {
                match stage {
                    0 => "App_Open",
                    1 if uid % 10 < 7 => "KYC_PAN_Submitted",
                    1 => "Quick_Grocery_Browse",
                    2 if uid % 10 < 5 => "UPI_Autopay_Mandate_Approved",
                    2 => "Quick_Grocery_Browse",
                    3 if uid % 10 < 3 => "First_Disbursal_Success",
                    3 => "Cart_Checkout",
                    4 => "Quick_Grocery_Browse",
                    _ => "Cart_Checkout",
                }
            };

            let is_cross_anon = uid % 10 == 0 && stage == 0;
            let distinct_id = if is_cross_anon {
                format!("anon_ios_{uid:06}")
            } else {
                format!("u_{uid:06}")
            };
            let routing_seed = if is_cross_anon {
                format!("seed_ios_{uid:06}")
            } else {
                format!("seed_web_{uid:06}")
            };

            let amount = 500 + (((uid * 131 + visit_idx * 17) % 9500) as i64);
            let uuid = [
                (i & 0xff) as u8,
                ((i >> 8) & 0xff) as u8,
                ((i >> 16) & 0xff) as u8,
                ((i >> 24) & 0xff) as u8,
                (b_idx & 0xff) as u8,
                ((b_idx >> 8) & 0xff) as u8,
                7,
                9,
                11,
                13,
                17,
                19,
                23,
                29,
                31,
                37,
            ];

            batch_events.push(WireEvent {
                event_uuid: uuid,
                routing_seed,
                distinct_id,
                session_id: format!("s_{uid:06}_{day_idx}"),
                event_name: ev_name.to_string(),
                client_event_ts_ms: ts,
                client_sent_ts_ms: 0,
                props: vec![
                    (
                        "plan".to_string(),
                        PropValue::Str(plans[uid % plans.len()].into()),
                    ),
                    (
                        "city".to_string(),
                        PropValue::Str(cities[(uid / 3) % cities.len()].into()),
                    ),
                    (
                        "payment_method".to_string(),
                        PropValue::Str(pay_methods[(uid / 5) % pay_methods.len()].into()),
                    ),
                    (
                        "merchant_id".to_string(),
                        PropValue::Str(merchants[(uid + visit_idx) % merchants.len()].into()),
                    ),
                    ("amount".to_string(), PropValue::Int(amount)),
                ],
                replay_offset_kb: 0,
            });
        }

        let b_t0 = Instant::now();
        ingest
            .ingest_for_project(project_id, WireBatch::new(batch_events), server_now)
            .await
            .expect("ingest batch");
        ingest_samples_us.push(b_t0.elapsed().as_micros() as u64);
    }

    // Batch-alias 10% of users across mobile & web seeds (`uid % 10 == 0`) with 1 durable flush.
    let id_graph = id_router.get_or_open(project_id).await.expect("id_graph");
    for uid in (0..cfg.num_users).step_by(10) {
        let anon = format!("anon_ios_{uid:06}");
        let user = format!("u_{uid:06}");
        let anon_seed = format!("seed_ios_{uid:06}");
        let user_seed = format!("seed_web_{uid:06}");
        id_graph
            .alias(
                &anon,
                &user,
                route_bucket(&anon_seed, 16),
                route_bucket(&user_seed, 16),
            )
            .expect("alias cross-bucket user");
    }
    id_graph.flush().await.expect("flush identity graph");

    let flush_t0 = Instant::now();
    storage.flush_all_projects().await.expect("flush storage");
    let flush_elapsed = flush_t0.elapsed();
    let total_ingest_elapsed = seed_t0.elapsed();

    let ps = storage
        .get_or_open_project(project_id)
        .await
        .expect("project");
    let stored_l1_rows = ps.snapshot().manifest.total_l1_rows();
    assert_eq!(stored_l1_rows as usize, cfg.total_events);
    let ingest_eps = cfg.total_events as f64 / total_ingest_elapsed.as_secs_f64();

    let full_range = TimeRange {
        start_ts_ms: base_ts,
        end_ts_ms: base_ts + span_days * 86_400_000 - 1,
    };
    let flows_days = ((1_500_000usize * span_days as usize) / cfg.total_events)
        .clamp(1, span_days as usize) as i64;
    let flows_range = TimeRange {
        start_ts_ms: base_ts,
        end_ts_ms: base_ts + flows_days * 86_400_000 - 1,
    };

    let funnel_query = FunnelQuery {
        time_range: full_range,
        steps: [
            "App_Open",
            "KYC_PAN_Submitted",
            "UPI_Autopay_Mandate_Approved",
            "First_Disbursal_Success",
            "Cart_Checkout",
        ]
        .into_iter()
        .map(|s| FunnelStepSpec {
            event_name: s.to_string(),
            filters: Vec::new(),
            first_time_ever: false,
        })
        .collect(),
        order_mode: FunnelOrderMode::LooseOrder,
        conversion_window_ms: 14 * 86_400_000,
        exclusions: Vec::new(),
        hold_constant_props: vec!["city".to_string()],
        breakdown_by: Some("payment_method".to_string()),
        global_filter: GlobalFilter::default(),
        sampling: Default::default(),
    };

    let insights_query = InsightsQuery {
        time_range: full_range,
        interval: TimeInterval::Day,
        series: vec![
            InsightsSeriesSpec {
                label: "A".to_string(),
                event_name: "App_Open".to_string(),
                metric: InsightsMetric::Uniques,
                filters: Vec::new(),
            },
            InsightsSeriesSpec {
                label: "B".to_string(),
                event_name: "First_Disbursal_Success".to_string(),
                metric: InsightsMetric::PropPercentile {
                    prop: "amount".to_string(),
                    quantile: 0.95,
                },
                filters: Vec::new(),
            },
        ],
        breakdown_by: vec!["plan".to_string()],
        global_filter: GlobalFilter::default(),
        compare_previous_period: false,
        formula: Some("(B / A) * 100".to_string()),
        sampling: Default::default(),
    };

    let retention_query = RetentionQuery {
        time_range: full_range,
        start_event: "App_Open".to_string(),
        return_event: "First_Disbursal_Success".to_string(),
        mode: RetentionMode::NDay,
        unit: TimeInterval::Day,
        num_periods: 14,
        global_filter: GlobalFilter::default(),
        sampling: Default::default(),
    };

    let flows_query = FlowsQuery {
        time_range: flows_range,
        anchor_event: "App_Open".to_string(),
        direction: FlowDirection::Forward,
        steps: 5,
        top_n_per_step: Some(10),
        global_filter: GlobalFilter::default(),
        sampling: Default::default(),
    };

    // 1. Cold 5-step Funnel (fresh QueryEngine with empty cache per iteration)
    let mut funnel_cold_us = Vec::with_capacity(cfg.cold_iters);
    for _ in 0..cfg.cold_iters {
        let cold_engine = QueryEngine::new(
            storage.clone(),
            256 * 1024 * 1024,
            None,
            64 * 1024 * 1024,
            catalogs.clone(),
        )
        .await
        .expect("cold QueryEngine");
        let t0 = Instant::now();
        let res = cold_engine
            .query_funnel(project_id, &funnel_query)
            .await
            .expect("cold funnel");
        funnel_cold_us.push(t0.elapsed().as_micros() as u64);
        assert_eq!(res.overall_steps.len(), 5);
        assert!(res.overall_steps[0].entered_users > 0);
        cold_engine.cache().close().await;
    }

    // 2. Warm QueryEngine across all analytics modes
    let qengine = QueryEngine::new(
        storage.clone(),
        256 * 1024 * 1024,
        None,
        64 * 1024 * 1024,
        catalogs.clone(),
    )
    .await
    .expect("warm QueryEngine");
    // Prime L1 decoded columns & inverted indexes before measuring warm percentiles
    let _ = qengine
        .query_funnel(project_id, &funnel_query)
        .await
        .unwrap();
    let _ = qengine
        .query_insights(project_id, &insights_query)
        .await
        .unwrap();
    let _ = qengine
        .query_retention(project_id, &retention_query)
        .await
        .unwrap();
    let _ = qengine.query_flows(project_id, &flows_query).await.unwrap();

    let mut funnel_warm_us = Vec::with_capacity(cfg.warm_iters);
    for _ in 0..cfg.warm_iters {
        let t0 = Instant::now();
        let res = qengine
            .query_funnel(project_id, &funnel_query)
            .await
            .expect("warm funnel");
        funnel_warm_us.push(t0.elapsed().as_micros() as u64);
        assert!(!res.breakdowns.is_empty());
    }

    let mut insights_warm_us = Vec::with_capacity(cfg.warm_iters);
    for _ in 0..cfg.warm_iters {
        let t0 = Instant::now();
        let res = qengine
            .query_insights(project_id, &insights_query)
            .await
            .expect("warm insights");
        insights_warm_us.push(t0.elapsed().as_micros() as u64);
        assert!(!res.series.is_empty());
        assert!(!res.formula_series.is_empty());
    }

    let mut retention_warm_us = Vec::with_capacity(cfg.warm_iters);
    for _ in 0..cfg.warm_iters {
        let t0 = Instant::now();
        let res = qengine
            .query_retention(project_id, &retention_query)
            .await
            .expect("warm retention");
        retention_warm_us.push(t0.elapsed().as_micros() as u64);
        assert!(!res.cohorts.is_empty());
    }

    let mut flows_warm_us = Vec::with_capacity(cfg.flows_iters);
    for _ in 0..cfg.flows_iters {
        let t0 = Instant::now();
        let res = qengine
            .query_flows(project_id, &flows_query)
            .await
            .expect("warm flows");
        flows_warm_us.push(t0.elapsed().as_micros() as u64);
        assert!(!res.nodes.is_empty());
    }

    // 3. 16x concurrent mixed queries
    let mut concurrent_us = Vec::with_capacity(cfg.concurrent_waves * 16);
    for _wave in 0..cfg.concurrent_waves {
        let mut handles = Vec::with_capacity(16);
        for worker in 0..16 {
            let qe = qengine.clone();
            let fq = funnel_query.clone();
            let iq = insights_query.clone();
            let rq = retention_query.clone();
            let flq = flows_query.clone();
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                match worker % 4 {
                    0 => {
                        qe.query_funnel(project_id, &fq).await.unwrap();
                    }
                    1 => {
                        qe.query_insights(project_id, &iq).await.unwrap();
                    }
                    2 => {
                        qe.query_retention(project_id, &rq).await.unwrap();
                    }
                    _ => {
                        qe.query_flows(project_id, &flq).await.unwrap();
                    }
                }
                t0.elapsed().as_micros() as u64
            }));
        }
        for h in handles {
            concurrent_us.push(h.await.expect("concurrent worker join"));
        }
    }

    // 4. Simultaneous Live Ingest + Background L0->L1 Segment Flushes + 16x Concurrent Mixed Queries
    let ingest_load_task = {
        let ing = ingest.clone();
        let ps_bg = ps.clone();
        let num_users = cfg.num_users;
        tokio::spawn(async move {
            let mut samples = Vec::with_capacity(10);
            for wave in 0..10usize {
                let mut evs = Vec::with_capacity(1_000);
                for j in 0..1_000usize {
                    let uid = (wave * 1_000 + j) % num_users;
                    let ts = base_ts + ((wave % 14) as i64) * 86_400_000 + (j as i64) * 100;
                    evs.push(WireEvent {
                        event_uuid: [
                            (j & 0xff) as u8,
                            ((j >> 8) & 0xff) as u8,
                            (wave & 0xff) as u8,
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
                            12,
                            99,
                        ],
                        routing_seed: format!("seed_web_{uid:06}"),
                        distinct_id: format!("u_{uid:06}"),
                        session_id: format!("s_live_{uid:06}_{wave}"),
                        event_name: match j % 4 {
                            0 => "App_Open",
                            1 => "KYC_PAN_Submitted",
                            2 => "UPI_Autopay_Mandate_Approved",
                            _ => "First_Disbursal_Success",
                        }
                        .to_string(),
                        client_event_ts_ms: ts,
                        client_sent_ts_ms: 0,
                        props: vec![
                            (
                                "plan".to_string(),
                                PropValue::Str(plans[uid % plans.len()].into()),
                            ),
                            (
                                "city".to_string(),
                                PropValue::Str(cities[(uid / 3) % cities.len()].into()),
                            ),
                            (
                                "payment_method".to_string(),
                                PropValue::Str(pay_methods[(uid / 5) % pay_methods.len()].into()),
                            ),
                            ("amount".to_string(), PropValue::Int(1_250)),
                        ],
                        replay_offset_kb: 0,
                    });
                }
                let t0 = Instant::now();
                let resp = ing
                    .ingest_for_project(project_id, WireBatch::new(evs), server_now + 5_000)
                    .await
                    .expect("live ingest under query load");
                assert_eq!(resp.accepted, 1_000);
                samples.push(t0.elapsed().as_micros() as u64);
                ps_bg.schedule_background_flush((wave % 16) as u8);
            }
            samples
        })
    };

    let query_load_task = {
        let qe = qengine.clone();
        let fq = funnel_query.clone();
        let iq = insights_query.clone();
        let rq = retention_query.clone();
        let flq = flows_query.clone();
        let waves = cfg.concurrent_waves;
        tokio::spawn(async move {
            let mut samples = Vec::with_capacity(waves * 16);
            for _wave in 0..waves {
                let mut handles = Vec::with_capacity(16);
                for worker in 0..16 {
                    let qe = qe.clone();
                    let fq = fq.clone();
                    let iq = iq.clone();
                    let rq = rq.clone();
                    let flq = flq.clone();
                    handles.push(tokio::spawn(async move {
                        let t0 = Instant::now();
                        match worker % 4 {
                            0 => {
                                qe.query_funnel(project_id, &fq).await.unwrap();
                            }
                            1 => {
                                qe.query_insights(project_id, &iq).await.unwrap();
                            }
                            2 => {
                                qe.query_retention(project_id, &rq).await.unwrap();
                            }
                            _ => {
                                qe.query_flows(project_id, &flq).await.unwrap();
                            }
                        }
                        t0.elapsed().as_micros() as u64
                    }));
                }
                for h in handles {
                    samples.push(h.await.expect("query_under_ingest worker join"));
                }
            }
            samples
        })
    };

    let (ingest_under_load_res, query_under_ingest_res) =
        tokio::join!(ingest_load_task, query_load_task);
    let ingest_under_load_us = ingest_under_load_res.expect("ingest_load_task join");
    let query_under_ingest_us = query_under_ingest_res.expect("query_load_task join");
    storage.flush_all_projects().await.expect("final flush");

    let sf = (cfg.total_events as f64 / 1_000_000.0).max(0.5);
    let flows_sf = (sf * (flows_days as f64 / span_days as f64)).max(0.5);

    let mut rows = Vec::new();
    let mut failures = Vec::new();

    record_row(
        &mut rows,
        &mut failures,
        "ingest_batch",
        &ingest_samples_us,
        SloBudget {
            p50_ms: 450.0,
            p75_ms: 600.0,
            p99_ms: 1200.0,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "funnel_5step_cold",
        &funnel_cold_us,
        SloBudget {
            p50_ms: 300.0 * sf,
            p75_ms: 380.0 * sf,
            p99_ms: 600.0 * sf,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "funnel_5step_warm",
        &funnel_warm_us,
        SloBudget {
            p50_ms: 35.0 * sf,
            p75_ms: 50.0 * sf,
            p99_ms: 100.0 * sf,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "insights_multi_warm",
        &insights_warm_us,
        SloBudget {
            p50_ms: 45.0 * sf,
            p75_ms: 65.0 * sf,
            p99_ms: 120.0 * sf,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "retention_14d_warm",
        &retention_warm_us,
        SloBudget {
            p50_ms: 40.0 * sf,
            p75_ms: 55.0 * sf,
            p99_ms: 110.0 * sf,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "flows_5step_warm",
        &flows_warm_us,
        SloBudget {
            p50_ms: 160.0 * flows_sf,
            p75_ms: 220.0 * flows_sf,
            p99_ms: 350.0 * flows_sf,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "concurrent_16x_mixed",
        &concurrent_us,
        SloBudget {
            p50_ms: 240.0 * sf,
            p75_ms: 360.0 * sf,
            p99_ms: 650.0 * sf,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "ingest_under_load",
        &ingest_under_load_us,
        SloBudget {
            p50_ms: 80.0,
            p75_ms: 120.0,
            p99_ms: 250.0,
        },
    );
    record_row(
        &mut rows,
        &mut failures,
        "query_under_ingest",
        &query_under_ingest_us,
        SloBudget {
            p50_ms: 260.0 * sf,
            p75_ms: 400.0 * sf,
            p99_ms: 750.0 * sf,
        },
    );

    println!();
    println!(
        "=== Pulse Analytics Scale & Latency SLO Report [scale={}, events={}, users={}, batch={}, ingest_throughput={:.0} ev/s, l1_flush={:.1}ms, flows_window={}d] ===",
        cfg.label,
        cfg.total_events,
        cfg.num_users,
        cfg.batch_size,
        ingest_eps,
        flush_elapsed.as_secs_f64() * 1000.0,
        flows_days
    );
    println!(
        "| Workload             |  N |  min(ms) |  p50(ms) |  p75(ms) |  p95(ms) |  p99(ms) |  max(ms) | SLO p50/p75/p99 (ms)      | Status |"
    );
    println!(
        "|----------------------|---:|---------:|---------:|---------:|---------:|---------:|---------:|---------------------------|--------|"
    );
    for (name, st, slo, pass) in &rows {
        println!(
            "| {:<20} | {:>2} | {:>8.2} | {:>8.2} | {:>8.2} | {:>8.2} | {:>8.2} | {:>8.2} | {:>6.0} / {:>6.0} / {:>6.0}    | {:<6} |",
            name,
            st.samples,
            st.min_ms,
            st.p50_ms,
            st.p75_ms,
            st.p95_ms,
            st.p99_ms,
            st.max_ms,
            slo.p50_ms,
            slo.p75_ms,
            slo.p99_ms,
            if *pass { "PASS" } else { "BREACH" }
        );
    }
    println!();

    qengine.cache().close().await;
    drop(qengine);
    id_router.close_all().await.unwrap();

    #[cfg(not(debug_assertions))]
    {
        if env::var("PULSE_BENCH_REPORT_ONLY").as_deref() != Ok("1") {
            assert!(
                failures.is_empty(),
                "SLO latency budget exceeded:\n{}",
                failures.join("\n")
            );
        }
    }
}

fn current_rss_mb() -> f64 {
    let pid = std::process::id().to_string();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

async fn run_large_scale_milestone(total_events: usize, distinct_users: u32) {
    use chrono::NaiveDate;
    use futures::stream::{self, StreamExt};
    use pulse_exec::{CatalogRegistry, ExecError, PropPredicate};
    use pulse_storage::{
        write_vortex_segment, Manifest, ManifestStore, ProjectDictionary, StoredRow,
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("object_store");
    std::fs::create_dir_all(&data_dir).unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&data_dir).unwrap());
    let project_id = "proj_large_scale";

    let dict = ProjectDictionary::new();
    let ev_names = [
        "page_view",
        "product_viewed",
        "add_to_cart",
        "checkout_started",
        "purchase",
    ];
    let ev_ids: Vec<u16> = ev_names
        .iter()
        .map(|n| dict.get_or_intern_event(n))
        .collect();
    let pk_country = dict.get_or_intern_prop_key("country");
    let pk_platform = dict.get_or_intern_prop_key("platform");
    let pk_amount = dict.get_or_intern_prop_key("amount");
    let countries = ["IN", "US", "GB", "DE", "SG", "JP", "BR", "CA"];
    let platforms = ["web", "ios", "android", "desktop"];

    let num_days = 30usize;
    let num_buckets = 16usize;
    let total_segs = num_days * num_buckets;
    let rows_per_seg = total_events / total_segs;
    let base_date = NaiveDate::from_ymd_opt(2026, 10, 21).unwrap();
    let base_ts_ms = base_date
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    println!(
        "\n================================================================================\n\
         LARGE-SCALE BENCHMARK: {}M events ({} segments x {} rows, {} distinct users)\n\
         ================================================================================",
        total_events / 1_000_000,
        total_segs,
        rows_per_seg,
        distinct_users
    );

    let t_build = Instant::now();
    let tasks: Vec<(usize, usize)> = (0..num_days)
        .flat_map(|d| (0..num_buckets).map(move |b| (d, b)))
        .collect();

    let seg_metas: Vec<(NaiveDate, u8, pulse_storage::SegmentMeta)> = stream::iter(tasks)
        .map(|(day, bucket)| {
            let store = store.clone();
            let ev_ids = ev_ids.clone();
            async move {
                let date = base_date + chrono::Duration::days(day as i64);
                let day_start_ms = base_ts_ms + (day as i64) * 86_400_000;
                let mut rows = Vec::with_capacity(rows_per_seg);
                let base_idx = ((day * num_buckets + bucket) * rows_per_seg) as u64;
                for i in 0..rows_per_seg {
                    let g = base_idx + i as u64;
                    // Partition users deterministically into this bucket
                    let u_in_bucket =
                        ((g * 2654435761) as u32) % (distinct_users / num_buckets as u32).max(1);
                    let uid = u_in_bucket * num_buckets as u32 + bucket as u32;
                    let eid = ev_ids[(g as usize) % ev_ids.len()];
                    let ts = day_start_ms + ((g * 104729) % 86_400_000) as i64;
                    let c = countries[(uid as usize) % countries.len()];
                    let p = platforms[((uid >> 3) as usize) % platforms.len()];
                    let amt = 10 + (g % 500) as i64;
                    rows.push(StoredRow {
                        l0_seq: g,
                        wal_seq: g,
                        uid_internal: uid,
                        session_hash: (uid as u64) * 31 + (day as u64),
                        session_id: format!("s_{uid}_{day}"),
                        event_uuid_fp: g,
                        event_id: eid,
                        event_ts_ms: ts,
                        replay_offset_kb: 0,
                        session_flags: 0,
                        props: vec![
                            (pk_country, PropValue::Str(c.to_string())),
                            (pk_platform, PropValue::Str(p.to_string())),
                            (pk_amount, PropValue::Int(amt)),
                        ],
                    });
                }
                let seg_id = format!("seg_d{day:02}_b{bucket:02}");
                let meta =
                    write_vortex_segment(&store, project_id, date, bucket as u8, &seg_id, rows)
                        .await
                        .expect("write_vortex_segment")
                        .expect("non-empty segment");
                (date, bucket as u8, meta)
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;

    let mut manifest = Manifest {
        epoch: 0,
        fenced_wal_epoch: 0,
        writer_generation: 0,
        last_published_seq: None,
        pending_s3_deletes: Vec::new(),
        s3_partition_last_upload_ms: std::collections::HashMap::new(),
        active_segments: std::collections::HashMap::new(),
        committed_wal_seq: std::collections::BTreeMap::new(),
        committed_l0_seq: std::collections::BTreeMap::new(),
        dict_snapshot: dict.snapshot(),
    };
    let mut total_seg_bytes = 0u64;
    for (date, bucket, meta) in seg_metas {
        total_seg_bytes += meta.file_size;
        manifest
            .active_segments
            .entry((date, bucket))
            .or_default()
            .push(meta);
    }
    let mstore = ManifestStore::new(project_id, store.clone());
    mstore.commit_next(manifest).await.expect("commit manifest");
    println!(
        "Built {}M events ({} Vortex segments, {:.1} MB on disk) in {:.2}s | RSS {:.0} MB",
        total_events / 1_000_000,
        total_segs,
        total_seg_bytes as f64 / (1024.0 * 1024.0),
        t_build.elapsed().as_secs_f64(),
        current_rss_mb()
    );

    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: num_buckets as u16,
            l0_flush_row_threshold: 250_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store.clone(),
        id_router.clone(),
    );
    let catalogs = CatalogRegistry::new();
    let foyer_dir = tmp.path().join("foyer_cache");
    let qengine = QueryEngine::new(
        storage.clone(),
        128 * 1024 * 1024,
        Some(&foyer_dir),
        512 * 1024 * 1024,
        catalogs.clone(),
    )
    .await
    .expect("query engine");
    qengine.set_max_rows(250_000_000);

    // Populate ColumnarProfileIndex with 1,000,000 user profiles to test user.* bitmap pushdown!
    let t_prof = Instant::now();
    let rss_before_prof = current_rss_mb();
    let cat = catalogs.for_project(project_id);
    let mut prof_map = std::collections::HashMap::with_capacity(1_000_000);
    for u in 0..1_000_000u32 {
        let mut m = std::collections::HashMap::with_capacity(2);
        let plan = if u % 100 == 0 {
            "enterprise"
        } else if u % 10 == 0 {
            "pro"
        } else {
            "free"
        };
        m.insert("plan".to_string(), PropValue::Str(plan.to_string()));
        m.insert("is_paid".to_string(), PropValue::Bool(u % 10 == 0));
        prof_map.insert(u, m);
    }
    cat.set_user_profiles(prof_map);
    println!(
        "Loaded 1,000,000 user profiles into ColumnarProfileIndex in {:.2}s | RSS {:.0} -> {:.0} MB",
        t_prof.elapsed().as_secs_f64(),
        rss_before_prof,
        current_rss_mb()
    );

    let tr30 = TimeRange {
        start_ts_ms: base_ts_ms,
        end_ts_ms: base_ts_ms + 30 * 86_400_000,
    };
    let tr7 = TimeRange {
        start_ts_ms: base_ts_ms + 23 * 86_400_000,
        end_ts_ms: base_ts_ms + 30 * 86_400_000,
    };

    let run_ins =
        |name: &str, q: InsightsQuery| {
            let qe = qengine.clone();
            let name = name.to_string();
            async move {
                let t1 = Instant::now();
                let r1 = qe.query_insights(project_id, &q).await;
                let d1 = t1.elapsed().as_secs_f64() * 1000.0;
                let rss1 = current_rss_mb();
                let t2 = Instant::now();
                let r2 = qe.query_insights(project_id, &q).await;
                let d2 = t2.elapsed().as_secs_f64() * 1000.0;
                let rss2 = current_rss_mb();
                println!(
                "  {:<46} | run1={:7.1}ms ({:?}, RSS {:.0}MB) -> run2={:7.1}ms ({:?}, RSS {:.0}MB)",
                name,
                d1,
                r1.as_ref().map(|x| x.series.first().and_then(|s| s.overall_value)),
                rss1,
                d2,
                r2.as_ref().map(|x| x.series.first().and_then(|s| s.overall_value)),
                rss2
            );
            }
        };

    run_ins(
        "totals, 1 event, 30d by day (footer fast path)",
        InsightsQuery {
            time_range: tr30,
            interval: TimeInterval::Day,
            series: vec![InsightsSeriesSpec {
                label: "A".into(),
                event_name: "purchase".into(),
                metric: InsightsMetric::Totals,
                filters: Vec::new(),
            }],
            breakdown_by: Vec::new(),
            global_filter: GlobalFilter::default(),
            formula: None,
            compare_previous_period: false,
            sampling: Default::default(),
        },
    )
    .await;

    run_ins(
        "uniques, 1 event, 30d by day",
        InsightsQuery {
            time_range: tr30,
            interval: TimeInterval::Day,
            series: vec![InsightsSeriesSpec {
                label: "A".into(),
                event_name: "purchase".into(),
                metric: InsightsMetric::Uniques,
                filters: Vec::new(),
            }],
            breakdown_by: Vec::new(),
            global_filter: GlobalFilter::default(),
            formula: None,
            compare_previous_period: false,
            sampling: Default::default(),
        },
    )
    .await;

    run_ins(
        "filtered totals (country==IN), 30d",
        InsightsQuery {
            time_range: tr30,
            interval: TimeInterval::Day,
            series: vec![InsightsSeriesSpec {
                label: "A".into(),
                event_name: "purchase".into(),
                metric: InsightsMetric::Totals,
                filters: Vec::new(),
            }],
            breakdown_by: Vec::new(),
            global_filter: GlobalFilter {
                predicates: vec![PropPredicate::eq_str("country", "IN")],
                cohort: None,
            },
            formula: None,
            compare_previous_period: false,
            sampling: Default::default(),
        },
    )
    .await;

    run_ins(
        "user.* filtered totals (user.plan==enterprise), 30d",
        InsightsQuery {
            time_range: tr30,
            interval: TimeInterval::Day,
            series: vec![InsightsSeriesSpec {
                label: "A".into(),
                event_name: "purchase".into(),
                metric: InsightsMetric::Totals,
                filters: Vec::new(),
            }],
            breakdown_by: Vec::new(),
            global_filter: GlobalFilter {
                predicates: vec![PropPredicate::eq_str("user.plan", "enterprise")],
                cohort: None,
            },
            formula: None,
            compare_previous_period: false,
            sampling: Default::default(),
        },
    )
    .await;

    run_ins(
        "breakdown by country, 7d",
        InsightsQuery {
            time_range: tr7,
            interval: TimeInterval::Day,
            series: vec![InsightsSeriesSpec {
                label: "A".into(),
                event_name: "purchase".into(),
                metric: InsightsMetric::Totals,
                filters: Vec::new(),
            }],
            breakdown_by: vec!["country".into()],
            global_filter: GlobalFilter::default(),
            formula: None,
            compare_previous_period: false,
            sampling: Default::default(),
        },
    )
    .await;

    run_ins(
        "3-series hourly breakdown by platform, 7d",
        InsightsQuery {
            time_range: tr7,
            interval: TimeInterval::Hour,
            series: vec![
                InsightsSeriesSpec {
                    label: "A".into(),
                    event_name: "page_view".into(),
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                },
                InsightsSeriesSpec {
                    label: "B".into(),
                    event_name: "add_to_cart".into(),
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                },
                InsightsSeriesSpec {
                    label: "C".into(),
                    event_name: "purchase".into(),
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                },
            ],
            breakdown_by: vec!["platform".into()],
            global_filter: GlobalFilter::default(),
            formula: None,
            compare_previous_period: false,
            sampling: Default::default(),
        },
    )
    .await;

    // 4-step funnel, 7d
    let fq = FunnelQuery {
        time_range: tr7,
        steps: vec![
            FunnelStepSpec {
                event_name: "page_view".into(),
                filters: Vec::new(),
                first_time_ever: false,
            },
            FunnelStepSpec {
                event_name: "product_viewed".into(),
                filters: Vec::new(),
                first_time_ever: false,
            },
            FunnelStepSpec {
                event_name: "add_to_cart".into(),
                filters: Vec::new(),
                first_time_ever: false,
            },
            FunnelStepSpec {
                event_name: "purchase".into(),
                filters: Vec::new(),
                first_time_ever: false,
            },
        ],
        conversion_window_ms: 7 * 86_400_000,
        order_mode: FunnelOrderMode::LooseOrder,
        exclusions: Vec::new(),
        hold_constant_props: Vec::new(),
        breakdown_by: None,
        global_filter: GlobalFilter::default(),
        sampling: Default::default(),
    };
    let tf1 = Instant::now();
    let rf1 = qengine.query_funnel(project_id, &fq).await;
    let df1 = tf1.elapsed().as_secs_f64() * 1000.0;
    let rss_f1 = current_rss_mb();
    let tf2 = Instant::now();
    let rf2 = qengine.query_funnel(project_id, &fq).await;
    let df2 = tf2.elapsed().as_secs_f64() * 1000.0;
    let rss_f2 = current_rss_mb();
    println!(
        "  {:<46} | run1={:7.1}ms (ok={}, RSS {:.0}MB) -> run2={:7.1}ms (ok={}, RSS {:.0}MB)",
        "4-step funnel, 7d",
        df1,
        rf1.is_ok(),
        rss_f1,
        df2,
        rf2.is_ok(),
        rss_f2
    );

    // Deadline enforcement test (#61): 150ms deadline on heavy 30d 3-series hourly breakdown
    qengine.set_query_timeout(Duration::from_millis(150));
    let td = Instant::now();
    let rd = qengine
        .query_insights(
            project_id,
            &InsightsQuery {
                time_range: tr30,
                interval: TimeInterval::Hour,
                series: vec![
                    InsightsSeriesSpec {
                        label: "A".into(),
                        event_name: "page_view".into(),
                        metric: InsightsMetric::Uniques,
                        filters: Vec::new(),
                    },
                    InsightsSeriesSpec {
                        label: "B".into(),
                        event_name: "add_to_cart".into(),
                        metric: InsightsMetric::Uniques,
                        filters: Vec::new(),
                    },
                    InsightsSeriesSpec {
                        label: "C".into(),
                        event_name: "purchase".into(),
                        metric: InsightsMetric::Uniques,
                        filters: Vec::new(),
                    },
                ],
                breakdown_by: vec!["country".into()],
                global_filter: GlobalFilter::default(),
                formula: None,
                compare_previous_period: false,
                sampling: Default::default(),
            },
        )
        .await;
    let dd = td.elapsed().as_secs_f64() * 1000.0;
    println!(
        "  {:<46} | took={:7.1}ms | result={:?}",
        "deadline check (150ms on 30d 3-series uniques)",
        dd,
        rd.as_ref().err()
    );
    assert!(
        matches!(rd, Err(ExecError::QueryTimeout(150))),
        "expected QueryTimeout(150), got {rd:?}"
    );
    assert!(dd < 2000.0, "deadline enforcement took too long: {dd:.1}ms");

    qengine.cache().close().await;
    drop(qengine);
    id_router.close_all().await.unwrap();
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn bench_50m_and_100m_events() {
    run_large_scale_milestone(50_000_000, 2_000_000).await;
    run_large_scale_milestone(100_000_000, 2_000_000).await;
}
