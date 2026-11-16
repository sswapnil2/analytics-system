use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use chrono_tz::Tz;
use pulse_identity::IdentitySnapshot;
use pulse_storage::{ProjectStorage, StorageSnapshot};
use pulse_wire::PropValue;
use roaring::RoaringBitmap;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};

use crate::ast::{
    InsightsMetric, InsightsQuery, InsightsResult, InsightsSeriesSpec, SeriesOutput, TimeInterval,
    TimePoint, TimeRange,
};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    evaluate_cohort, load_project_chunks, precompute_segment_row_mask, resolve_event_target,
    resolve_global_cohort_bitmap, row_matches_predicates, ExecError, Result,
};

pub const MAX_TIME_BUCKETS: usize = 10_000;
pub const MAX_BREAKDOWN_GROUPS: usize = 250;

pub struct InsightsEngine;

impl InsightsEngine {
    pub async fn execute(
        query: &InsightsQuery,
        ps: &ProjectStorage,
        snap: &StorageSnapshot,
        id_snap: &IdentitySnapshot,
        cache: &SegmentBlockCache,
        catalog: &VirtualCatalog,
    ) -> Result<InsightsResult> {
        let t0 = Instant::now();
        validate_insights_query(query)?;

        let tz = catalog.timezone();
        let buckets = build_time_buckets_tz(query.time_range, query.interval, tz)?;

        let series = evaluate_window(
            query,
            query.time_range,
            &buckets,
            tz,
            ps,
            snap,
            id_snap,
            cache,
            catalog,
        )
        .await?;

        let previous_series = if query.compare_previous_period {
            let prev_range = query.time_range.previous_period();
            let prev_buckets = build_time_buckets_tz(prev_range, query.interval, tz)?;
            evaluate_window(
                query,
                prev_range,
                &prev_buckets,
                tz,
                ps,
                snap,
                id_snap,
                cache,
                catalog,
            )
            .await?
        } else {
            Vec::new()
        };

        let formula_series = if let Some(formula_str) = &query.formula {
            if !formula_str.trim().is_empty() {
                evaluate_formulas(formula_str, &buckets, &series)?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        Ok(InsightsResult {
            snapshot_epoch: snap.epoch(),
            exec_time_us: t0.elapsed().as_micros() as u64,
            buckets_ts_ms: buckets,
            series,
            previous_series,
            formula_series,
        })
    }
}

fn validate_insights_query(query: &InsightsQuery) -> Result<()> {
    if query.time_range.start_ts_ms > query.time_range.end_ts_ms {
        return Err(ExecError::InvalidQuery(
            "time_range.start_ts_ms must be <= end_ts_ms".into(),
        ));
    }
    if query.breakdown_by.len() > 2 {
        return Err(ExecError::InvalidQuery(format!(
            "insights supports at most 2 breakdown dimensions, got {}",
            query.breakdown_by.len()
        )));
    }
    for spec in &query.series {
        match spec.metric {
            InsightsMetric::Dau if !matches!(query.interval, TimeInterval::Day) => {
                return Err(ExecError::InvalidQuery(format!(
                    "series '{}': DAU metric is only valid with Day interval",
                    spec.label
                )));
            }
            InsightsMetric::Wau if matches!(query.interval, TimeInterval::Hour) => {
                return Err(ExecError::InvalidQuery(format!(
                    "series '{}': WAU metric is not valid with Hour interval",
                    spec.label
                )));
            }
            InsightsMetric::Mau if matches!(query.interval, TimeInterval::Hour) => {
                return Err(ExecError::InvalidQuery(format!(
                    "series '{}': MAU metric is not valid with Hour interval",
                    spec.label
                )));
            }
            InsightsMetric::PropPercentile { quantile, .. } if !(0.0..=1.0).contains(&quantile) => {
                return Err(ExecError::InvalidQuery(format!(
                    "series '{}': quantile {quantile} must be in [0.0, 1.0]",
                    spec.label
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

fn build_time_buckets_tz(range: TimeRange, interval: TimeInterval, tz: Tz) -> Result<Vec<i64>> {
    let mut cur = interval.align_floor_tz(range.start_ts_ms, tz);
    let end = interval.align_floor_tz(range.end_ts_ms, tz);
    let mut out = Vec::new();
    while cur <= end {
        if out.len() >= MAX_TIME_BUCKETS {
            return Err(ExecError::InvalidQuery(format!(
                "time range produces more than {MAX_TIME_BUCKETS} interval buckets"
            )));
        }
        out.push(cur);
        let Some(next) = interval.next_bucket_tz(cur, tz) else {
            break;
        };
        if next <= cur {
            break;
        }
        cur = next;
    }
    if out.is_empty() {
        out.push(range.start_ts_ms);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_window(
    query: &InsightsQuery,
    range: TimeRange,
    buckets: &[i64],
    tz: Tz,
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
) -> Result<Vec<SeriesOutput>> {
    let max_lookback_ms = query
        .series
        .iter()
        .map(|s| match s.metric {
            InsightsMetric::Mau => 31 * 86_400_000,
            InsightsMetric::Wau => 8 * 86_400_000,
            _ => 0,
        })
        .max()
        .unwrap_or(0);

    let scan_range = TimeRange {
        start_ts_ms: range.start_ts_ms.saturating_sub(max_lookback_ms),
        end_ts_ms: range.end_ts_ms,
    };

    let eligible_cohort = resolve_global_cohort_bitmap(
        &query.global_filter,
        range,
        ps,
        snap,
        id_snap,
        cache,
        catalog,
    )
    .await?;

    let mut breakdown_cohort_bitmaps: HashMap<String, RoaringBitmap> = HashMap::new();
    let breakdown_dims: Vec<String> = query.breakdown_by.clone();
    for bdim in &breakdown_dims {
        if let Some(cid) = bdim.strip_prefix("$cohort:") {
            if let Some(cdef) = catalog.saved_cohorts.get(cid) {
                let cdef_cloned = cdef.clone();
                drop(cdef);
                let bm =
                    evaluate_cohort(&cdef_cloned, Some(range), ps, snap, id_snap, cache, catalog)
                        .await?;
                breakdown_cohort_bitmaps.insert(bdim.clone(), bm);
            }
        }
    }

    let mut props_needed: HashSet<String> = HashSet::new();
    for p in &query.global_filter.predicates {
        props_needed.insert(p.prop_name.clone());
    }
    for b in &breakdown_dims {
        if !b.starts_with("$cohort:") {
            props_needed.insert(b.clone());
        }
    }
    let mut combined_bloom = 0u64;
    let mut resolved_targets = Vec::with_capacity(query.series.len());
    for spec in &query.series {
        let tgt = resolve_event_target(&spec.event_name, ps.dictionary(), catalog);
        combined_bloom |= tgt.bloom_mask_64;
        for rp in tgt.required_props() {
            props_needed.insert(rp);
        }
        for fp in &spec.filters {
            props_needed.insert(fp.prop_name.clone());
        }
        match &spec.metric {
            InsightsMetric::PropSum { prop }
            | InsightsMetric::PropAvg { prop }
            | InsightsMetric::PropMin { prop }
            | InsightsMetric::PropMax { prop }
            | InsightsMetric::PropPercentile { prop, .. } => {
                props_needed.insert(prop.clone());
            }
            _ => {}
        }
        resolved_targets.push(tgt);
    }

    let props_vec: Vec<String> = props_needed.into_iter().collect();
    let chunks = load_project_chunks(
        ps,
        snap,
        cache,
        catalog,
        Some(scan_range),
        &props_vec,
        (combined_bloom != 0).then_some(combined_bloom),
    )
    .await?;

    let mut all_outputs = Vec::new();
    for (s_idx, spec) in query.series.iter().enumerate() {
        let tgt = &resolved_targets[s_idx];
        let mut series_out = evaluate_single_series(
            spec,
            tgt,
            range,
            query.interval,
            buckets,
            tz,
            &breakdown_dims,
            &breakdown_cohort_bitmaps,
            &query.global_filter.predicates,
            eligible_cohort.as_ref(),
            &chunks,
            ps,
            id_snap,
            cache,
            catalog,
        )
        .await?;
        all_outputs.append(&mut series_out);
    }

    Ok(all_outputs)
}

struct BucketAccum {
    users: RoaringBitmap,
    sessions: HashSet<u64>,
    totals: u64,
    sum: f64,
    count_num: u64,
    min: f64,
    max: f64,
    sketch: DDSketch,
}

impl BucketAccum {
    fn new() -> Self {
        Self {
            users: RoaringBitmap::new(),
            sessions: HashSet::new(),
            totals: 0,
            sum: 0.0,
            count_num: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sketch: DDSketch::new(DDSketchConfig::defaults()),
        }
    }

    fn observe(&mut self, canon_uid: u32, session_hash: u64, num_val: Option<f64>) {
        self.users.insert(canon_uid);
        self.sessions.insert(session_hash);
        self.totals += 1;
        if let Some(v) = num_val {
            if v.is_finite() {
                self.sum += v;
                self.count_num += 1;
                self.min = self.min.min(v);
                self.max = self.max.max(v);
                // Native signed DDSketch (`negative_store` + `zero_count` + `store`) — never clamp negatives (`P2-09a`)!
                self.sketch.add(v);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.users |= other.users;
        self.sessions.extend(other.sessions);
        self.totals += other.totals;
        self.sum += other.sum;
        self.count_num += other.count_num;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        let _ = self.sketch.merge(&other.sketch);
    }

    fn finish(&self, metric: &InsightsMetric) -> Option<f64> {
        match metric {
            InsightsMetric::Uniques | InsightsMetric::Dau => Some(self.users.len() as f64),
            InsightsMetric::Totals => Some(self.totals as f64),
            InsightsMetric::Sessions => Some(self.sessions.len() as f64),
            InsightsMetric::PropSum { .. } => Some(self.sum),
            InsightsMetric::PropAvg { .. } => {
                (self.count_num > 0).then(|| self.sum / (self.count_num as f64))
            }
            InsightsMetric::PropMin { .. } => (self.count_num > 0).then_some(self.min),
            InsightsMetric::PropMax { .. } => (self.count_num > 0).then_some(self.max),
            InsightsMetric::PropPercentile { quantile, .. } => {
                if self.count_num == 0 {
                    None
                } else {
                    self.sketch
                        .quantile(quantile.clamp(0.0, 1.0))
                        .ok()
                        .flatten()
                }
            }
            InsightsMetric::Wau | InsightsMetric::Mau => Some(self.users.len() as f64),
        }
    }
}

fn empty_metric_value(metric: &InsightsMetric) -> Option<f64> {
    match metric {
        InsightsMetric::PropAvg { .. }
        | InsightsMetric::PropMin { .. }
        | InsightsMetric::PropMax { .. }
        | InsightsMetric::PropPercentile { .. } => None,
        InsightsMetric::Uniques
        | InsightsMetric::Totals
        | InsightsMetric::Sessions
        | InsightsMetric::Dau
        | InsightsMetric::Wau
        | InsightsMetric::Mau
        | InsightsMetric::PropSum { .. } => Some(0.0),
    }
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_single_series(
    spec: &InsightsSeriesSpec,
    tgt: &crate::scanner::ResolvedEventTarget,
    range: TimeRange,
    interval: TimeInterval,
    buckets: &[i64],
    tz: Tz,
    breakdown_dims: &[String],
    breakdown_cohorts: &HashMap<String, RoaringBitmap>,
    global_preds: &[crate::ast::PropPredicate],
    eligible_cohort: Option<&RoaringBitmap>,
    chunks: &[crate::scanner::DataChunk],
    ps: &ProjectStorage,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
) -> Result<Vec<SeriesOutput>> {
    let is_rolling = matches!(spec.metric, InsightsMetric::Wau | InsightsMetric::Mau);
    let metric_prop = match &spec.metric {
        InsightsMetric::PropSum { prop }
        | InsightsMetric::PropAvg { prop }
        | InsightsMetric::PropMin { prop }
        | InsightsMetric::PropMax { prop }
        | InsightsMetric::PropPercentile { prop, .. } => Some(prop.as_str()),
        _ => None,
    };

    let mut groups: BTreeMap<String, (BucketAccum, BTreeMap<i64, BucketAccum>)> = BTreeMap::new();
    if breakdown_dims.is_empty() {
        groups.insert("overall".to_string(), (BucketAccum::new(), BTreeMap::new()));
    }

    let mut all_preds = Vec::with_capacity(global_preds.len() + spec.filters.len() + 1);
    all_preds.extend_from_slice(global_preds);
    all_preds.extend_from_slice(&spec.filters);
    if let Some(ef) = &tgt.extra_filter {
        all_preds.push(ef.clone());
    }

    for chunk in chunks {
        if tgt.event_ids.is_empty() {
            break;
        }
        let row_mask =
            precompute_segment_row_mask(chunk, &all_preds, ps.dictionary(), catalog, cache).await?;

        for r in 0..chunk.len() {
            if let Some(rm) = &row_mask {
                if !rm.contains(r as u32) {
                    continue;
                }
            }
            if !tgt.matches_id(chunk.tier_a.event_id[r]) {
                continue;
            }
            let ts = chunk.tier_a.event_ts_ms[r];
            if !is_rolling && !range.contains(ts) {
                continue;
            }
            if is_rolling && ts > range.end_ts_ms {
                continue;
            }
            let canon_uid = id_snap.resolve(chunk.tier_a.uid_internal[r]);
            if let Some(ec) = eligible_cohort {
                if !ec.contains(canon_uid) {
                    continue;
                }
            }
            if !row_matches_predicates(chunk, r, &all_preds) {
                continue;
            }

            let bkey = if breakdown_dims.is_empty() {
                "overall".to_string()
            } else {
                let mut parts = Vec::with_capacity(breakdown_dims.len());
                for dim in breakdown_dims {
                    if let Some(cbm) = breakdown_cohorts.get(dim) {
                        parts.push(if cbm.contains(canon_uid) {
                            "in_cohort".to_string()
                        } else {
                            "not_in_cohort".to_string()
                        });
                    } else {
                        let pv = chunk
                            .prop_cols
                            .get(dim)
                            .and_then(|c| c.get(r))
                            .unwrap_or(&PropValue::Null);
                        parts.push(if matches!(pv, PropValue::Null) {
                            "(none)".to_string()
                        } else {
                            pv.as_str_cow().into_owned()
                        });
                    }
                }
                parts.join(" | ")
            };

            let num_val = metric_prop
                .and_then(|mp| chunk.prop_cols.get(mp))
                .and_then(|c| c.get(r))
                .and_then(|pv| pv.as_f64());

            let entry = groups
                .entry(bkey)
                .or_insert_with(|| (BucketAccum::new(), BTreeMap::new()));

            if range.contains(ts) {
                entry
                    .0
                    .observe(canon_uid, chunk.tier_a.session_hash[r], num_val);
            }

            let align_unit = if is_rolling {
                TimeInterval::Day
            } else {
                interval
            };
            let b_ts = align_unit.align_floor_tz(ts, tz);
            entry
                .1
                .entry(b_ts)
                .or_insert_with(BucketAccum::new)
                .observe(canon_uid, chunk.tier_a.session_hash[r], num_val);
        }
    }

    // Cap breakdown cardinality to `MAX_BREAKDOWN_GROUPS`, folding tail groups into `"$other"` (`P3-02`).
    type BreakdownGroup = (BucketAccum, BTreeMap<i64, BucketAccum>);
    let mut group_vec: Vec<(String, BreakdownGroup)> = groups.into_iter().collect();
    if group_vec.len() > MAX_BREAKDOWN_GROUPS {
        group_vec.sort_by(|a, b| {
            b.1 .0
                .totals
                .cmp(&a.1 .0.totals)
                .then_with(|| a.0.cmp(&b.0))
        });
        let tail = group_vec.split_off(MAX_BREAKDOWN_GROUPS);
        let mut other_overall = BucketAccum::new();
        let mut other_per_bucket: BTreeMap<i64, BucketAccum> = BTreeMap::new();
        for (_k, (ov, pb)) in tail {
            other_overall.merge(ov);
            for (bts, acc) in pb {
                other_per_bucket
                    .entry(bts)
                    .or_insert_with(BucketAccum::new)
                    .merge(acc);
            }
        }
        group_vec.push(("$other".to_string(), (other_overall, other_per_bucket)));
    }

    let mut out = Vec::with_capacity(group_vec.len());
    for (bkey, (overall_acc, per_bucket)) in group_vec {
        let mut points = Vec::with_capacity(buckets.len());
        for &bts in buckets {
            let val = match spec.metric {
                InsightsMetric::Wau | InsightsMetric::Mau => {
                    // Anchor rolling WAU (7d) and MAU (30d) at the END of the interval bucket (`P2-09b`)!
                    let window_days = if matches!(spec.metric, InsightsMetric::Wau) {
                        7i64
                    } else {
                        30i64
                    };
                    let bucket_end = interval
                        .bucket_end_inclusive_tz(bts, tz)
                        .min(range.end_ts_ms);
                    let end_day = TimeInterval::Day.align_floor_tz(bucket_end, tz);
                    let mut union_bm = RoaringBitmap::new();
                    for d in 0..window_days {
                        if let Some(day_ts) = TimeInterval::prev_days_tz(end_day, d, tz) {
                            if let Some(acc) = per_bucket.get(&day_ts) {
                                union_bm |= &acc.users;
                            }
                        }
                    }
                    Some(union_bm.len() as f64)
                }
                _ => per_bucket
                    .get(&bts)
                    .map(|acc| acc.finish(&spec.metric))
                    .unwrap_or_else(|| empty_metric_value(&spec.metric)),
            };
            points.push(TimePoint {
                bucket_ts_ms: bts,
                value: val,
            });
        }
        let overall_value = if is_rolling {
            points.last().and_then(|p| p.value).or(Some(0.0))
        } else {
            overall_acc.finish(&spec.metric)
        };

        out.push(SeriesOutput {
            label: spec.label.clone(),
            breakdown_key: bkey,
            overall_value,
            points,
        });
    }

    Ok(out)
}

pub const MAX_FORMULA_LEN: usize = 1_024;
pub const MAX_FORMULA_DEPTH: usize = 64;

/// Evaluates multi-series mathematical formulas like `(B / A) * 100` per breakdown group & time bucket.
fn evaluate_formulas(
    formula_str: &str,
    buckets: &[i64],
    series: &[SeriesOutput],
) -> Result<Vec<SeriesOutput>> {
    if series.is_empty() {
        let empty = HashMap::new();
        let _ = eval_math_expr_opt(formula_str, &empty)?;
        return Ok(Vec::new());
    }

    let mut by_breakdown: BTreeMap<String, HashMap<String, &SeriesOutput>> = BTreeMap::new();
    for s in series {
        by_breakdown
            .entry(s.breakdown_key.clone())
            .or_default()
            .insert(s.label.clone(), s);
    }

    let mut out = Vec::with_capacity(by_breakdown.len());
    for (bkey, map) in by_breakdown {
        let mut overall_vars: HashMap<String, Option<f64>> = HashMap::new();
        for (lbl, s) in &map {
            overall_vars.insert(lbl.clone(), s.overall_value);
        }
        let overall_value = eval_math_expr_opt(formula_str, &overall_vars)?;

        let mut points = Vec::with_capacity(buckets.len());
        for (idx, &bts) in buckets.iter().enumerate() {
            let mut pt_vars: HashMap<String, Option<f64>> = HashMap::new();
            for (lbl, s) in &map {
                let v = s.points.get(idx).and_then(|p| p.value);
                pt_vars.insert(lbl.clone(), v);
            }
            let val = eval_math_expr_opt(formula_str, &pt_vars)?;
            points.push(TimePoint {
                bucket_ts_ms: bts,
                value: val,
            });
        }

        out.push(SeriesOutput {
            label: format!("Formula({formula_str})"),
            breakdown_key: bkey,
            overall_value,
            points,
        });
    }

    Ok(out)
}

/// Recursive-descent arithmetic expression evaluator (`+`, `-`, `*`, `/`, unary `+/-`, parens, numbers, and series variables).
///
/// Enforces length (`<= 1024` bytes) and recursion depth (`<= 64`), rejects unknown variables and trailing tokens,
/// and returns `Ok(None)` on division by zero or non-finite values (`P0-08`).
pub fn eval_math_expr(expr: &str, vars: &HashMap<String, f64>) -> Result<Option<f64>> {
    let opt_vars: HashMap<String, Option<f64>> =
        vars.iter().map(|(k, &v)| (k.clone(), Some(v))).collect();
    eval_math_expr_opt(expr, &opt_vars)
}

pub fn eval_math_expr_opt(expr: &str, vars: &HashMap<String, Option<f64>>) -> Result<Option<f64>> {
    if expr.len() > MAX_FORMULA_LEN {
        return Err(ExecError::Expression(format!(
            "formula length {} exceeds maximum of {MAX_FORMULA_LEN} bytes",
            expr.len()
        )));
    }

    struct Parser<'a> {
        bytes: &'a [u8],
        pos: usize,
        vars: &'a HashMap<String, Option<f64>>,
    }

    #[inline]
    fn combine(
        lhs: Option<f64>,
        rhs: Option<f64>,
        f: impl FnOnce(f64, f64) -> Option<f64>,
    ) -> Option<f64> {
        match (lhs, rhs) {
            (Some(a), Some(b)) => f(a, b).filter(|v| v.is_finite()),
            _ => None,
        }
    }

    impl<'a> Parser<'a> {
        fn check_depth(depth: usize) -> Result<()> {
            if depth > MAX_FORMULA_DEPTH {
                return Err(ExecError::Expression(format!(
                    "formula nesting depth exceeds maximum of {MAX_FORMULA_DEPTH}"
                )));
            }
            Ok(())
        }

        fn skip_ws(&mut self) {
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
        }

        fn parse_expr(&mut self, depth: usize) -> Result<Option<f64>> {
            Self::check_depth(depth)?;
            let mut val = self.parse_term(depth + 1)?;
            loop {
                self.skip_ws();
                if self.pos >= self.bytes.len() {
                    break;
                }
                match self.bytes[self.pos] {
                    b'+' => {
                        self.pos += 1;
                        let rhs = self.parse_term(depth + 1)?;
                        val = combine(val, rhs, |a, b| Some(a + b));
                    }
                    b'-' => {
                        self.pos += 1;
                        let rhs = self.parse_term(depth + 1)?;
                        val = combine(val, rhs, |a, b| Some(a - b));
                    }
                    _ => break,
                }
            }
            Ok(val)
        }

        fn parse_term(&mut self, depth: usize) -> Result<Option<f64>> {
            Self::check_depth(depth)?;
            let mut val = self.parse_factor(depth + 1)?;
            loop {
                self.skip_ws();
                if self.pos >= self.bytes.len() {
                    break;
                }
                match self.bytes[self.pos] {
                    b'*' => {
                        self.pos += 1;
                        let rhs = self.parse_factor(depth + 1)?;
                        val = combine(val, rhs, |a, b| Some(a * b));
                    }
                    b'/' => {
                        self.pos += 1;
                        let rhs = self.parse_factor(depth + 1)?;
                        val = combine(val, rhs, |a, b| {
                            if b == 0.0 || b.abs() < 1e-15 {
                                None
                            } else {
                                Some(a / b)
                            }
                        });
                    }
                    _ => break,
                }
            }
            Ok(val)
        }

        fn parse_factor(&mut self, depth: usize) -> Result<Option<f64>> {
            Self::check_depth(depth)?;
            self.skip_ws();
            if self.pos >= self.bytes.len() {
                return Err(ExecError::Expression("unexpected end of formula".into()));
            }
            let b = self.bytes[self.pos];
            if b == b'-' {
                self.pos += 1;
                let inner = self.parse_factor(depth + 1)?;
                return Ok(inner.map(|v| -v).filter(|v| v.is_finite()));
            }
            if b == b'+' {
                self.pos += 1;
                return self.parse_factor(depth + 1);
            }
            if b == b'(' {
                self.pos += 1;
                let inner = self.parse_expr(depth + 1)?;
                self.skip_ws();
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b')' {
                    self.pos += 1;
                    return Ok(inner);
                }
                return Err(ExecError::Expression(
                    "missing closing ')' in formula".into(),
                ));
            }
            if b.is_ascii_digit() || b == b'.' {
                let start = self.pos;
                while self.pos < self.bytes.len()
                    && (self.bytes[self.pos].is_ascii_digit() || self.bytes[self.pos] == b'.')
                {
                    self.pos += 1;
                }
                let s = std::str::from_utf8(&self.bytes[start..self.pos]).unwrap_or("");
                let parsed = s
                    .parse::<f64>()
                    .map_err(|e| ExecError::Expression(format!("invalid formula number: {e}")))?;
                return Ok(if parsed.is_finite() {
                    Some(parsed)
                } else {
                    None
                });
            }
            if b.is_ascii_alphabetic() || b == b'_' {
                let start = self.pos;
                while self.pos < self.bytes.len()
                    && (self.bytes[self.pos].is_ascii_alphanumeric()
                        || self.bytes[self.pos] == b'_')
                {
                    self.pos += 1;
                }
                let name = std::str::from_utf8(&self.bytes[start..self.pos]).unwrap_or("");
                let Some(&val) = self.vars.get(name) else {
                    return Err(ExecError::Expression(format!(
                        "unknown formula variable '{name}'"
                    )));
                };
                return Ok(val.filter(|v| v.is_finite()));
            }
            Err(ExecError::Expression(format!(
                "unexpected token '{}' in formula",
                b as char
            )))
        }
    }

    let mut parser = Parser {
        bytes: expr.as_bytes(),
        pos: 0,
        vars,
    };
    let res = parser.parse_expr(0)?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err(ExecError::Expression(format!(
            "unexpected trailing token at byte {} in formula",
            parser.pos
        )));
    }
    Ok(res.filter(|v| v.is_finite()))
}
