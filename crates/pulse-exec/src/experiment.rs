//! Per-user experiment metrics: each exposed user's metric value after their first exposure
//! (and, for CUPED, before it), summed into per-variant moments.
//!
//! Separate from the funnel because experiments anchor on the *first* exposure in range (the
//! funnel keeps the anchor with the shortest time to convert) and need a pre-exposure window.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use pulse_identity::IdentitySnapshot;
use pulse_storage::{ProjectStorage, StorageSnapshot};
use pulse_wire::PropValue;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::ast::{PropPredicate, TimeInterval, TimeRange};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    load_project_chunks_sampled, resolve_event_target, row_matches_predicates, DataChunk,
    ExecError, ResolvedEventTarget, Result,
};

/// Segment values past this many share `$other`.
pub const MAX_EXPERIMENT_SEGMENTS: usize = 50;

/// What a metric event contributes per user.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "prop", rename_all = "snake_case")]
pub enum MetricValue {
    /// `1` if the user did the event in the window, else `0`.
    Binary,
    /// Number of times the user did the event in the window.
    Count,
    /// Sum of a numeric property over the user's events in the window (missing counts as `0`).
    Sum(String),
}

/// Funnel-style metrics between two events: the "from" event is the query's `denominator`,
/// the "to" event its `metric`, both inside the post-exposure window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricSequence {
    /// `y` = reached "to" after first reaching "from", `d` = reached "from" (both 0/1), so the
    /// ratio is the step-through rate among users who got to the first step.
    StepThrough,
    /// `y` = seconds from the first "from" to the first "to" after it, `d` = 1 if both
    /// happened, so the ratio is the mean time among users who completed.
    TimeBetween,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentMetricEvent {
    pub event_name: String,
    #[serde(default)]
    pub filters: Vec<PropPredicate>,
    pub value: MetricValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentMetricQuery {
    /// Exposures count only inside this range.
    pub time_range: TimeRange,
    pub exposure_event: String,
    #[serde(default)]
    pub exposure_filters: Vec<PropPredicate>,
    /// Exposure property holding the variant key.
    pub variant_prop: String,
    /// Optional exposure-event or `user.*` property to split results by.
    #[serde(default)]
    pub segment_by: Option<String>,
    pub metric: ExperimentMetricEvent,
    /// Ratio metrics: per-user denominator, over the same post-exposure window.
    #[serde(default)]
    pub denominator: Option<ExperimentMetricEvent>,
    /// Metric events count in `[exposure, exposure + window_ms]`.
    pub window_ms: i64,
    /// CUPED covariate: the same metric in `[exposure - pre_window_ms, exposure)`. `0` = none.
    #[serde(default)]
    pub pre_window_ms: i64,
    /// Ordered two-event metric; needs `denominator` (the first step). No CUPED.
    #[serde(default)]
    pub sequence: Option<MetricSequence>,
}

/// Sufficient statistics over users: `y` post metric, `x` pre-period metric, `d` denominator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricMoments {
    pub n: u64,
    pub sum_y: f64,
    pub sum_y2: f64,
    pub sum_x: f64,
    pub sum_x2: f64,
    pub sum_xy: f64,
    pub sum_d: f64,
    pub sum_d2: f64,
    pub sum_yd: f64,
}

impl MetricMoments {
    fn add(&mut self, y: f64, x: f64, d: f64) {
        self.n += 1;
        self.sum_y += y;
        self.sum_y2 += y * y;
        self.sum_x += x;
        self.sum_x2 += x * x;
        self.sum_xy += x * y;
        self.sum_d += d;
        self.sum_d2 += d * d;
        self.sum_yd += y * d;
    }

    pub fn merge(&mut self, o: &MetricMoments) {
        self.n += o.n;
        self.sum_y += o.sum_y;
        self.sum_y2 += o.sum_y2;
        self.sum_x += o.sum_x;
        self.sum_x2 += o.sum_x2;
        self.sum_xy += o.sum_xy;
        self.sum_d += o.sum_d;
        self.sum_d2 += o.sum_d2;
        self.sum_yd += o.sum_yd;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentSegmentGroup {
    pub segment: String,
    pub variants: BTreeMap<String, MetricMoments>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentDailyPoint {
    /// Start of the exposure day in the project's timezone.
    pub day_start_ms: i64,
    /// Users first exposed on or before this day.
    pub cumulative: BTreeMap<String, MetricMoments>,
    /// Users first exposed on this day only.
    #[serde(default)]
    pub day: BTreeMap<String, MetricMoments>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentMetricResult {
    pub exec_time_us: u64,
    pub variants: BTreeMap<String, MetricMoments>,
    pub segments: Vec<ExperimentSegmentGroup>,
    pub daily: Vec<ExperimentDailyPoint>,
    /// Users exposed to more than one variant, left out of every group.
    pub multi_variant_users: u64,
}

const ROW_CHARGE_BLOCK: usize = 1024;
const ROW_CHARGE_BLOCK_BYTES: u64 = (ROW_CHARGE_BLOCK * std::mem::size_of::<Row>()) as u64;

const KIND_EXPOSURE: u8 = 1;
const KIND_METRIC: u8 = 2;
const KIND_DENOM: u8 = 4;

#[derive(Debug, Clone, Copy)]
struct Row {
    uid: u32,
    ts: i64,
    kinds: u8,
    /// Value of this row towards the metric / denominator (`1` for binary and count).
    m: f64,
    d: f64,
    /// Interned variant / segment (exposure rows only; `0` = missing).
    variant: u32,
    segment: u32,
}

/// Per-shard result keyed by that shard's interned ids.
#[derive(Default)]
struct Agg {
    variants: HashMap<u32, MetricMoments>,
    segments: HashMap<(u32, u32), MetricMoments>,
    daily: HashMap<(i64, u32), MetricMoments>,
    multi: u64,
}

struct Interner {
    ids: HashMap<String, u32>,
    names: Vec<String>,
}

impl Interner {
    fn new() -> Self {
        Self {
            ids: HashMap::from([(String::new(), 0)]),
            names: vec![String::new()],
        }
    }

    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.ids.get(s) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(s.to_string());
        self.ids.insert(s.to_string(), id);
        id
    }

    fn intern_prop(&mut self, chunk: &DataChunk, r: usize, prop: &str) -> u32 {
        match chunk.prop_cols.get(prop).and_then(|c| c.get(r)) {
            None | Some(PropValue::Null) => 0,
            Some(pv) => self.intern(&pv.as_str_cow()),
        }
    }
}

struct Plan<'a> {
    q: &'a ExperimentMetricQuery,
    exposure: ResolvedEventTarget,
    metric: ResolvedEventTarget,
    denom: Option<ResolvedEventTarget>,
    window_ms: i64,
    pre_window_ms: i64,
    tz: chrono_tz::Tz,
}

impl Plan<'_> {
    fn event_matches(
        tgt: &ResolvedEventTarget,
        filters: &[PropPredicate],
        chunk: &DataChunk,
        r: usize,
        eid: u16,
    ) -> bool {
        tgt.matches_id(eid)
            && tgt
                .extra_filter
                .as_ref()
                .is_none_or(|ef| row_matches_predicates(chunk, r, std::slice::from_ref(ef)))
            && row_matches_predicates(chunk, r, filters)
    }

    fn row_value(value: &MetricValue, chunk: &DataChunk, r: usize) -> f64 {
        match value {
            MetricValue::Binary | MetricValue::Count => 1.0,
            MetricValue::Sum(p) => chunk
                .prop_cols
                .get(p)
                .and_then(|c| c.get(r))
                .and_then(PropValue::as_f64)
                .filter(|v| v.is_finite())
                .unwrap_or(0.0),
        }
    }

    /// Per-user metric from the rows in a window: binary metrics cap at `1`.
    fn finish(value: &MetricValue, total: f64, any: bool) -> f64 {
        match value {
            MetricValue::Binary => f64::from(u8::from(any)),
            _ => total,
        }
    }

    /// Evaluates every user in `rows` (sorted here by uid, ts) into `agg`.
    fn evaluate(&self, rows: &mut [Row], agg: &mut Agg, stop: &crate::budget::Stop) {
        rows.sort_unstable_by_key(|r| (r.uid, r.ts));
        let range = self.q.time_range;
        let mut i = 0;
        while i < rows.len() {
            if !stop.live_every(i) {
                return;
            }
            let uid = rows[i].uid;
            let mut j = i;
            while j < rows.len() && rows[j].uid == uid {
                j += 1;
            }
            let user = &rows[i..j];
            i = j;

            let mut exposures = user
                .iter()
                .filter(|r| r.kinds & KIND_EXPOSURE != 0 && r.variant != 0 && range.contains(r.ts));
            let Some(anchor) = exposures.next().copied() else {
                continue;
            };
            if exposures.any(|r| r.variant != anchor.variant) {
                agg.multi += 1;
                continue;
            }

            let post_end = anchor.ts.saturating_add(self.window_ms);
            if let Some(seq) = self.q.sequence {
                let in_window = |r: &&Row, kind: u8| {
                    r.kinds & kind != 0 && r.ts >= anchor.ts && r.ts <= post_end
                };
                let from = user.iter().find(|r| in_window(r, KIND_DENOM)).map(|r| r.ts);
                let to = from.and_then(|a| {
                    user.iter()
                        .find(|r| in_window(r, KIND_METRIC) && r.ts >= a)
                        .map(|r| r.ts)
                });
                let flag = |b: bool| f64::from(u8::from(b));
                let (y, d) = match seq {
                    MetricSequence::StepThrough => (flag(to.is_some()), flag(from.is_some())),
                    MetricSequence::TimeBetween => (
                        from.zip(to).map_or(0.0, |(a, b)| (b - a) as f64 / 1000.0),
                        flag(to.is_some()),
                    ),
                };
                self.record(agg, &anchor, y, 0.0, d);
                continue;
            }
            let pre_start = anchor.ts.saturating_sub(self.pre_window_ms);
            let (mut y, mut y_any, mut x, mut x_any, mut d, mut d_any) =
                (0.0, false, 0.0, false, 0.0, false);
            for r in user {
                if r.ts >= anchor.ts && r.ts <= post_end {
                    if r.kinds & KIND_METRIC != 0 {
                        y += r.m;
                        y_any = true;
                    }
                    if r.kinds & KIND_DENOM != 0 {
                        d += r.d;
                        d_any = true;
                    }
                } else if self.pre_window_ms > 0
                    && r.ts >= pre_start
                    && r.ts < anchor.ts
                    && r.kinds & KIND_METRIC != 0
                {
                    x += r.m;
                    x_any = true;
                }
            }
            let mv = &self.q.metric.value;
            let y = Self::finish(mv, y, y_any);
            let x = Self::finish(mv, x, x_any);
            let d = self
                .q
                .denominator
                .as_ref()
                .map_or(0.0, |dn| Self::finish(&dn.value, d, d_any));

            self.record(agg, &anchor, y, x, d);
        }
    }

    fn record(&self, agg: &mut Agg, anchor: &Row, y: f64, x: f64, d: f64) {
        agg.variants.entry(anchor.variant).or_default().add(y, x, d);
        if self.q.segment_by.is_some() {
            agg.segments
                .entry((anchor.variant, anchor.segment))
                .or_default()
                .add(y, x, d);
        }
        let day = TimeInterval::Day.align_floor_tz(anchor.ts, self.tz);
        agg.daily
            .entry((day, anchor.variant))
            .or_default()
            .add(y, x, d);
    }
}

pub struct ExperimentEngine;

impl ExperimentEngine {
    pub async fn execute(
        q: &ExperimentMetricQuery,
        ps: &ProjectStorage,
        snap: &StorageSnapshot,
        id_snap: &IdentitySnapshot,
        cache: &SegmentBlockCache,
        catalog: &VirtualCatalog,
    ) -> Result<ExperimentMetricResult> {
        let t0 = Instant::now();
        if q.time_range.start_ts_ms > q.time_range.end_ts_ms {
            return Err(ExecError::InvalidQuery(
                "invalid time_range: start after end".into(),
            ));
        }
        if q.sequence.is_some() && q.denominator.is_none() {
            return Err(ExecError::InvalidQuery(
                "a sequence metric needs a denominator (its first step)".into(),
            ));
        }
        if q.window_ms <= 0 || q.pre_window_ms < 0 {
            return Err(ExecError::InvalidQuery(
                "window_ms must be positive and pre_window_ms not negative".into(),
            ));
        }

        let dict = ps.dictionary();
        let plan = Plan {
            q,
            exposure: resolve_event_target(&q.exposure_event, dict, catalog),
            metric: resolve_event_target(&q.metric.event_name, dict, catalog),
            denom: q
                .denominator
                .as_ref()
                .map(|d| resolve_event_target(&d.event_name, dict, catalog)),
            window_ms: q.window_ms,
            pre_window_ms: q.pre_window_ms,
            tz: catalog.timezone(),
        };

        let mut props: HashSet<String> = HashSet::new();
        props.insert(q.variant_prop.clone());
        props.extend(q.segment_by.clone());
        let events = std::iter::once((&plan.exposure, &q.exposure_filters, None))
            .chain(std::iter::once((
                &plan.metric,
                &q.metric.filters,
                Some(&q.metric.value),
            )))
            .chain(
                plan.denom
                    .as_ref()
                    .zip(q.denominator.as_ref())
                    .map(|(t, d)| (t, &d.filters, Some(&d.value))),
            );
        let mut bloom = 0u64;
        for (tgt, filters, value) in events {
            bloom |= tgt.bloom_mask_64;
            props.extend(tgt.required_props());
            props.extend(filters.iter().map(|f| f.prop_name.clone()));
            if let Some(MetricValue::Sum(p)) = value {
                props.insert(p.clone());
            }
        }

        let scan_range = TimeRange {
            start_ts_ms: q.time_range.start_ts_ms.saturating_sub(q.pre_window_ms),
            end_ts_ms: q.time_range.end_ts_ms.saturating_add(q.window_ms),
        };
        let props: Vec<String> = props.into_iter().collect();
        let chunks = load_project_chunks_sampled(
            ps,
            snap,
            cache,
            catalog,
            Some(scan_range),
            &props,
            (bloom != 0).then_some(bloom),
            Some(id_snap),
            None,
        )
        .await?;

        let mut by_bucket: BTreeMap<u8, Vec<&DataChunk>> = BTreeMap::new();
        for c in &chunks {
            by_bucket.entry(c.bucket_id).or_default().push(c);
        }
        let groups: Vec<Vec<&DataChunk>> = by_bucket.into_values().collect();

        let shard_outputs: Vec<(Agg, Interner, Interner, Vec<Row>)> =
            crate::scanner::run_rayon_compute(|stop| {
                groups
                    .par_iter()
                    .map(|b_chunks| {
                        let mut variants = Interner::new();
                        let mut segments = Interner::new();
                        let (mut local, mut cross) = (Vec::new(), Vec::new());
                        for chunk in b_chunks {
                            let ta = &chunk.tier_a;
                            for r in 0..ta.len() {
                                if !stop.live_every(r) {
                                    break;
                                }
                                let ts = ta.event_ts_ms[r];
                                if !scan_range.contains(ts) {
                                    continue;
                                }
                                let eid = ta.event_id[r];
                                let mut row = Row {
                                    uid: 0,
                                    ts,
                                    kinds: 0,
                                    m: 0.0,
                                    d: 0.0,
                                    variant: 0,
                                    segment: 0,
                                };
                                if Plan::event_matches(
                                    &plan.exposure,
                                    &q.exposure_filters,
                                    chunk,
                                    r,
                                    eid,
                                ) {
                                    row.kinds |= KIND_EXPOSURE;
                                    row.variant = variants.intern_prop(chunk, r, &q.variant_prop);
                                    if let Some(sp) = &q.segment_by {
                                        row.segment = segments.intern_prop(chunk, r, sp);
                                    }
                                }
                                if Plan::event_matches(
                                    &plan.metric,
                                    &q.metric.filters,
                                    chunk,
                                    r,
                                    eid,
                                ) {
                                    row.kinds |= KIND_METRIC;
                                    row.m = Plan::row_value(&q.metric.value, chunk, r);
                                }
                                if let (Some(t), Some(dn)) = (&plan.denom, &q.denominator) {
                                    if Plan::event_matches(t, &dn.filters, chunk, r, eid) {
                                        row.kinds |= KIND_DENOM;
                                        row.d = Plan::row_value(&dn.value, chunk, r);
                                    }
                                }
                                if row.kinds == 0 {
                                    continue;
                                }
                                row.uid = id_snap.resolve(ta.uid_internal[r]);
                                if id_snap.is_cross_bucket(row.uid) {
                                    cross.push(row);
                                } else {
                                    local.push(row);
                                }
                                // Charge kept rows to the query's memory budget, a block at a time.
                                if (local.len() + cross.len()) % ROW_CHARGE_BLOCK == 0
                                    && !stop.charge_memory(ROW_CHARGE_BLOCK_BYTES)
                                {
                                    break;
                                }
                            }
                        }
                        let mut agg = Agg::default();
                        plan.evaluate(&mut local, &mut agg, stop);
                        (agg, variants, segments, cross)
                    })
                    .collect()
            });

        // Re-key every shard onto names; cross-bucket users are evaluated once, here.
        crate::budget::check_current()?;
        let mut variants: BTreeMap<String, MetricMoments> = BTreeMap::new();
        let mut segs: HashMap<(String, String), MetricMoments> = HashMap::new();
        let mut daily: BTreeMap<i64, BTreeMap<String, MetricMoments>> = BTreeMap::new();
        let mut multi = 0u64;
        let mut coord_v = Interner::new();
        let mut coord_s = Interner::new();
        let mut all_cross = Vec::new();

        let mut fold = |agg: Agg, vn: &[String], sn: &[String]| {
            multi += agg.multi;
            for (v, m) in agg.variants {
                variants
                    .entry(vn[v as usize].clone())
                    .or_default()
                    .merge(&m);
            }
            for ((v, s), m) in agg.segments {
                let seg = if s == 0 { "(none)" } else { &sn[s as usize] };
                segs.entry((vn[v as usize].clone(), seg.to_string()))
                    .or_default()
                    .merge(&m);
            }
            for ((day, v), m) in agg.daily {
                daily
                    .entry(day)
                    .or_default()
                    .entry(vn[v as usize].clone())
                    .or_default()
                    .merge(&m);
            }
        };

        for (agg, v, s, cross) in shard_outputs {
            fold(agg, &v.names, &s.names);
            all_cross.extend(cross.into_iter().map(|mut r| {
                r.variant = coord_v.intern(&v.names[r.variant as usize]);
                r.segment = coord_s.intern(&s.names[r.segment as usize]);
                r
            }));
        }
        if !all_cross.is_empty() {
            let mut agg = Agg::default();
            plan.evaluate(&mut all_cross, &mut agg, &crate::budget::Stop::current());
            fold(agg, &coord_v.names, &coord_s.names);
        }

        Ok(ExperimentMetricResult {
            exec_time_us: t0.elapsed().as_micros() as u64,
            variants,
            segments: cap_segments(segs),
            daily: cumulate(daily),
            multi_variant_users: multi,
        })
    }
}

/// Largest segments (by users across variants) first; the tail past the cap folds into `$other`.
fn cap_segments(segs: HashMap<(String, String), MetricMoments>) -> Vec<ExperimentSegmentGroup> {
    let mut by_seg: HashMap<String, BTreeMap<String, MetricMoments>> = HashMap::new();
    for ((v, s), m) in segs {
        by_seg.entry(s).or_default().insert(v, m);
    }
    let size = |g: &BTreeMap<String, MetricMoments>| g.values().map(|m| m.n).sum::<u64>();
    let mut groups: Vec<(String, BTreeMap<String, MetricMoments>)> = by_seg.into_iter().collect();
    groups.sort_by(|a, b| size(&b.1).cmp(&size(&a.1)).then_with(|| a.0.cmp(&b.0)));
    if groups.len() > MAX_EXPERIMENT_SEGMENTS {
        let tail = groups.split_off(MAX_EXPERIMENT_SEGMENTS);
        let mut other: BTreeMap<String, MetricMoments> = BTreeMap::new();
        for (_, g) in tail {
            for (v, m) in g {
                other.entry(v).or_default().merge(&m);
            }
        }
        groups.push(("$other".to_string(), other));
    }
    groups
        .into_iter()
        .map(|(segment, variants)| ExperimentSegmentGroup { segment, variants })
        .collect()
}

fn cumulate(daily: BTreeMap<i64, BTreeMap<String, MetricMoments>>) -> Vec<ExperimentDailyPoint> {
    let mut running: BTreeMap<String, MetricMoments> = BTreeMap::new();
    daily
        .into_iter()
        .map(|(day_start_ms, day)| {
            for (v, m) in &day {
                running.entry(v.clone()).or_default().merge(m);
            }
            ExperimentDailyPoint {
                day_start_ms,
                cumulative: running.clone(),
                day,
            }
        })
        .collect()
}
