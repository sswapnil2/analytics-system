//! `query_experiment_metric` against a naive per-user oracle, on random worlds with identity
//! merges across storage buckets, multi-variant users, missing variants and split flushes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use proptest::prelude::*;
use pulse_exec::{
    CatalogRegistry, ExperimentMetricEvent, ExperimentMetricQuery, MetricMoments, MetricValue,
    PropFilterOp, PropPredicate, QueryEngine, TimeRange,
};
use pulse_identity::IdentityRouter;
use pulse_ingest::{IdentifyRequest, InMemoryGovernance, IngestService};
use pulse_storage::{StorageConfig, StorageEngine};
use pulse_wire::{PropValue, WireBatch, WireEvent};

const BASE: i64 = 1_762_041_600_000;
const HOUR: i64 = 3_600_000;
const PROJECT: &str = "proj_exp";

#[derive(Debug, Clone)]
struct Ev {
    who: String,
    name: &'static str,
    ts: i64,
    variant: Option<&'static str>,
    country: Option<&'static str>,
    amount: Option<i64>,
}

#[derive(Debug, Clone)]
struct World {
    events: Vec<Ev>,
    /// `(anon, user)`; odd indices route the user to another bucket.
    merges: Vec<(String, String)>,
    flush_split_pct: usize,
}

fn arb_event() -> impl Strategy<Value = Ev> {
    (
        0usize..12,
        prop_oneof![
            Just("$experiment_started"),
            Just("Purchase"),
            Just("Visit"),
            Just("Other")
        ],
        -60i64..140,
        prop_oneof![
            3 => Just(Some("control")),
            3 => Just(Some("treatment")),
            1 => Just(None),
        ],
        prop_oneof![Just(Some("DE")), Just(Some("US")), Just(None)],
        prop_oneof![Just(None), (0i64..500).prop_map(Some)],
    )
        .prop_map(|(u, name, h, variant, country, amount)| Ev {
            who: if u % 3 == 0 {
                format!("anon_{u}")
            } else {
                format!("user_{u}")
            },
            name,
            ts: BASE + h * HOUR,
            variant,
            country,
            amount,
        })
}

fn arb_world() -> impl Strategy<Value = World> {
    (
        prop::collection::vec(arb_event(), 0..120),
        prop::collection::vec((0usize..12, 0usize..12), 0..4),
        0usize..=100,
    )
        .prop_map(|(events, pairs, flush_split_pct)| World {
            events,
            // Each anon merges into at most one user and each user takes at most one anon.
            merges: {
                let mut seen = std::collections::HashSet::new();
                pairs
                    .into_iter()
                    .map(|(a, u)| (format!("anon_{}", a * 3 % 12), format!("user_{}", u | 1)))
                    .filter(|(a, u)| seen.insert(a.clone()) && seen.insert(u.clone()))
                    .collect()
            },
            flush_split_pct,
        })
}

fn query(value: MetricValue, ratio: bool, segment: bool) -> ExperimentMetricQuery {
    ExperimentMetricQuery {
        time_range: TimeRange {
            start_ts_ms: BASE,
            end_ts_ms: BASE + 72 * HOUR,
        },
        exposure_event: "$experiment_started".into(),
        exposure_filters: vec![PropPredicate {
            prop_name: "flag_key".into(),
            filter: PropFilterOp::Eq(PropValue::Str("f".into())),
        }],
        variant_prop: "variant".into(),
        segment_by: segment.then(|| "country".to_string()),
        metric: ExperimentMetricEvent {
            event_name: "Purchase".into(),
            filters: vec![],
            value,
        },
        denominator: ratio.then(|| ExperimentMetricEvent {
            event_name: "Visit".into(),
            filters: vec![],
            value: MetricValue::Count,
        }),
        window_ms: 24 * HOUR,
        pre_window_ms: 48 * HOUR,
    }
}

// ---------- naive oracle: shares no code with the engine ----------

fn canonical(world: &World) -> HashMap<String, String> {
    let mut parent: HashMap<String, String> = HashMap::new();
    fn find(p: &HashMap<String, String>, mut k: String) -> String {
        while let Some(n) = p.get(&k) {
            if *n == k {
                break;
            }
            k = n.clone();
        }
        k
    }
    for (a, u) in &world.merges {
        let (ra, ru) = (find(&parent, a.clone()), find(&parent, u.clone()));
        parent.entry(ru.clone()).or_insert(ru.clone());
        if ra != ru {
            parent.insert(ra, ru);
        }
    }
    world
        .events
        .iter()
        .map(|e| (e.who.clone(), find(&parent, e.who.clone())))
        .collect()
}

#[derive(Default)]
struct Expected {
    variants: BTreeMap<String, MetricMoments>,
    segments: BTreeMap<(String, String), MetricMoments>,
    multi: u64,
}

fn add(m: &mut MetricMoments, y: f64, x: f64, d: f64) {
    m.n += 1;
    m.sum_y += y;
    m.sum_y2 += y * y;
    m.sum_x += x;
    m.sum_x2 += x * x;
    m.sum_xy += x * y;
    m.sum_d += d;
    m.sum_d2 += d * d;
    m.sum_yd += y * d;
}

fn oracle(world: &World, q: &ExperimentMetricQuery) -> Expected {
    let cmap = canonical(world);
    let mut by_user: BTreeMap<String, Vec<&Ev>> = BTreeMap::new();
    for e in &world.events {
        by_user.entry(cmap[&e.who].clone()).or_default().push(e);
    }
    let mut out = Expected::default();
    let in_range = |ts: i64| ts >= q.time_range.start_ts_ms && ts <= q.time_range.end_ts_ms;
    let val = |v: &MetricValue, e: &Ev| match v {
        MetricValue::Sum(_) => e.amount.unwrap_or(0) as f64,
        _ => 1.0,
    };
    let fin = |v: &MetricValue, hits: &[&Ev]| match v {
        MetricValue::Binary => f64::from(u8::from(!hits.is_empty())),
        _ => hits.iter().map(|e| val(v, e)).sum(),
    };
    for evs in by_user.values() {
        let mut exps: Vec<&&Ev> = evs
            .iter()
            .filter(|e| e.name == "$experiment_started" && e.variant.is_some() && in_range(e.ts))
            .collect();
        if exps.is_empty() {
            continue;
        }
        exps.sort_by_key(|e| e.ts);
        let anchor = exps[0];
        if exps.iter().any(|e| e.variant != anchor.variant) {
            out.multi += 1;
            continue;
        }
        let post = |name: &str| -> Vec<&Ev> {
            evs.iter()
                .copied()
                .filter(|e| e.name == name && e.ts >= anchor.ts && e.ts <= anchor.ts + q.window_ms)
                .collect()
        };
        let pre: Vec<&Ev> = evs
            .iter()
            .copied()
            .filter(|e| {
                e.name == "Purchase" && e.ts < anchor.ts && e.ts >= anchor.ts - q.pre_window_ms
            })
            .collect();
        let y = fin(&q.metric.value, &post("Purchase"));
        let x = fin(&q.metric.value, &pre);
        let d = if q.denominator.is_some() {
            post("Visit").len() as f64
        } else {
            0.0
        };
        let v = anchor.variant.unwrap().to_string();
        add(out.variants.entry(v.clone()).or_default(), y, x, d);
        if q.segment_by.is_some() {
            let s = anchor.country.unwrap_or("(none)").to_string();
            add(out.segments.entry((v, s)).or_default(), y, x, d);
        }
    }
    out
}

fn close(a: &MetricMoments, b: &MetricMoments) -> bool {
    let f = |x: f64, y: f64| (x - y).abs() <= 1e-6 * (1.0 + x.abs().max(y.abs()));
    a.n == b.n
        && f(a.sum_y, b.sum_y)
        && f(a.sum_y2, b.sum_y2)
        && f(a.sum_x, b.sum_x)
        && f(a.sum_x2, b.sum_x2)
        && f(a.sum_xy, b.sum_xy)
        && f(a.sum_d, b.sum_d)
        && f(a.sum_d2, b.sum_d2)
        && f(a.sum_yd, b.sum_yd)
}

// ---------- engine side ----------

fn seed(who: &str) -> String {
    if who.starts_with("anon_") {
        format!("seed_a_{who}")
    } else {
        format!("seed_b_{who}")
    }
}

async fn load(world: &World) -> (Arc<QueryEngine>, Arc<IdentityRouter>) {
    let store = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 8,
            l0_flush_row_threshold: 100_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let ingest = IngestService::new(storage.clone(), InMemoryGovernance::new());
    let qengine = QueryEngine::new(storage.clone(), 16 << 20, None, 0, CatalogRegistry::new())
        .await
        .unwrap();
    for (i, (anon, user)) in world.merges.iter().enumerate() {
        ingest
            .identify_for_project(
                PROJECT,
                IdentifyRequest {
                    anon_id: anon.clone(),
                    user_id: user.clone(),
                    routing_seed: Some(if i % 2 == 0 { seed(anon) } else { seed(user) }),
                    anon_routing_seed: Some(seed(anon)),
                    user_properties: None,
                    operations: None,
                },
            )
            .await
            .unwrap();
    }
    let now = BASE + 10 * 24 * HOUR;
    let wire: Vec<WireEvent> = world
        .events
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let mut props = BTreeMap::new();
            if e.name == "$experiment_started" {
                props.insert("flag_key".to_string(), PropValue::Str("f".into()));
                if let Some(v) = e.variant {
                    props.insert("variant".to_string(), PropValue::Str(v.into()));
                }
                if let Some(c) = e.country {
                    props.insert("country".to_string(), PropValue::Str(c.into()));
                }
            }
            if let Some(a) = e.amount {
                props.insert("amount".to_string(), PropValue::Int(a));
            }
            WireEvent {
                event_uuid: WireEvent::deterministic_uuid(&e.who, "s", e.name, e.ts, i as u64 + 1),
                routing_seed: seed(&e.who),
                distinct_id: e.who.clone(),
                session_id: format!("s_{}", e.who),
                event_name: e.name.to_string(),
                client_event_ts_ms: e.ts,
                client_sent_ts_ms: now,
                props: props.into_iter().collect(),
                replay_offset_kb: 0,
            }
        })
        .collect();
    let split = wire.len() * world.flush_split_pct / 100;
    for (part, flush) in [(&wire[..split], true), (&wire[split..], false)] {
        if part.is_empty() {
            continue;
        }
        ingest
            .ingest_for_project(PROJECT, WireBatch::new(part.to_vec()), now)
            .await
            .unwrap();
        if flush {
            storage.flush_all_projects().await.unwrap();
        }
    }
    (qengine, id_router)
}

async fn check(world: World) {
    let (qengine, id_router) = load(&world).await;
    for (value, ratio, segment) in [
        (MetricValue::Binary, false, true),
        (MetricValue::Count, true, false),
        (MetricValue::Sum("amount".into()), false, true),
    ] {
        let q = query(value, ratio, segment);
        let got = qengine.query_experiment_metric(PROJECT, &q).await.unwrap();
        let want = oracle(&world, &q);
        assert_eq!(
            got.multi_variant_users, want.multi,
            "multi-variant users for {q:?}"
        );
        assert_eq!(
            got.variants.keys().collect::<Vec<_>>(),
            want.variants.keys().collect::<Vec<_>>()
        );
        for (v, m) in &want.variants {
            assert!(
                close(&got.variants[v], m),
                "{v}: got {:?} want {m:?}",
                got.variants[v]
            );
        }
        let got_segs: BTreeMap<(String, String), MetricMoments> = got
            .segments
            .iter()
            .flat_map(|g| {
                g.variants
                    .iter()
                    .map(|(v, m)| ((v.clone(), g.segment.clone()), *m))
            })
            .collect();
        assert_eq!(got_segs.len(), want.segments.len());
        for (k, m) in &want.segments {
            assert!(close(&got_segs[k], m), "{k:?}");
        }
        // The last cumulative day equals the overall totals.
        if let Some(last) = got.daily.last() {
            for (v, m) in &got.variants {
                assert!(close(&last.cumulative[v], m));
                // Per-day groups add up to the totals too.
                let mut summed = MetricMoments::default();
                for p in &got.daily {
                    if let Some(d) = p.day.get(v) {
                        summed.merge(d);
                    }
                }
                assert!(close(&summed, m));
            }
        }
    }
    id_router.close_all().await.unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn experiment_metric_matches_naive_oracle(world in arb_world()) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(check(world));
    }
}

#[tokio::test]
async fn first_exposure_anchors_and_pre_period_is_separate() {
    // One user: purchase 10h before exposure (pre), exposure, purchase 2h after (post),
    // purchase 30h after (outside the 24h window), and a later exposure that doesn't re-anchor.
    let world = World {
        events: vec![
            Ev {
                who: "user_1".into(),
                name: "Purchase",
                ts: BASE + 10 * HOUR,
                variant: None,
                country: None,
                amount: Some(7),
            },
            Ev {
                who: "user_1".into(),
                name: "$experiment_started",
                ts: BASE + 20 * HOUR,
                variant: Some("treatment"),
                country: Some("DE"),
                amount: None,
            },
            Ev {
                who: "user_1".into(),
                name: "Purchase",
                ts: BASE + 22 * HOUR,
                variant: None,
                country: None,
                amount: Some(40),
            },
            Ev {
                who: "user_1".into(),
                name: "$experiment_started",
                ts: BASE + 40 * HOUR,
                variant: Some("treatment"),
                country: Some("US"),
                amount: None,
            },
            Ev {
                who: "user_1".into(),
                name: "Purchase",
                ts: BASE + 50 * HOUR,
                variant: None,
                country: None,
                amount: Some(500),
            },
        ],
        merges: vec![],
        flush_split_pct: 50,
    };
    let (qengine, id_router) = load(&world).await;
    let res = qengine
        .query_experiment_metric(
            PROJECT,
            &query(MetricValue::Sum("amount".into()), false, true),
        )
        .await
        .unwrap();
    let m = res.variants["treatment"];
    assert_eq!((m.n, m.sum_y, m.sum_x), (1, 40.0, 7.0));
    assert_eq!(res.segments.len(), 1);
    assert_eq!(res.segments[0].segment, "DE");
    id_router.close_all().await.unwrap();
}
