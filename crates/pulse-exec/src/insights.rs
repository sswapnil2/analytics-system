use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

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
        let buckets = build_time_buckets(query.time_range, query.interval);

        let series = evaluate_window(
            query,
            query.time_range,
            &buckets,
            ps,
            snap,
            id_snap,
            cache,
            catalog,
        )
        .await?;

        let previous_series = if query.compare_previous_period {
            let prev_range = query.time_range.previous_period();
            let prev_buckets = build_time_buckets(prev_range, query.interval);
            evaluate_window(
                query,
                prev_range,
                &prev_buckets,
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

fn build_time_buckets(range: TimeRange, interval: TimeInterval) -> Vec<i64> {
    let step = interval.step_ms();
    let mut cur = interval.align_floor(range.start_ts_ms);
    let end = interval.align_floor(range.end_ts_ms);
    let mut out = Vec::new();
    while cur <= end {
        out.push(cur);
        cur += step;
    }
    if out.is_empty() {
        out.push(range.start_ts_ms);
    }
    out
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_window(
    query: &InsightsQuery,
    range: TimeRange,
    buckets: &[i64],
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
) -> Result<Vec<SeriesOutput>> {
    // Expand query scan window backward if DAU/WAU/MAU rolling lookbacks are requested
    let max_lookback_ms = query
        .series
        .iter()
        .map(|s| match s.metric {
            InsightsMetric::Mau => 29 * 86_400_000,
            InsightsMetric::Wau => 6 * 86_400_000,
            _ => 0,
        })
        .max()
        .unwrap_or(0);

    let scan_range = TimeRange {
        start_ts_ms: range.start_ts_ms - max_lookback_ms,
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

    // Resolve any cohort-based breakdowns (`$cohort:<id>`)
    let mut breakdown_cohort_bitmaps: HashMap<String, RoaringBitmap> = HashMap::new();
    let breakdown_dims: Vec<String> = query.breakdown_by.iter().take(2).cloned().collect();
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

    // Collect required property names across global filter, breakdown, and all series
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
            &breakdown_dims,
            &breakdown_cohort_bitmaps,
            &query.global_filter.predicates,
            eligible_cohort.as_ref(),
            &chunks,
            ps,
            id_snap,
            cache,
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
                // DDSketch supports non-negative values; clamp negative values to 0.0 or abs for quantile estimation
                self.sketch.add(v.max(0.0));
            }
        }
    }

    fn finish(&self, metric: &InsightsMetric) -> f64 {
        match metric {
            InsightsMetric::Uniques | InsightsMetric::Dau => self.users.len() as f64,
            InsightsMetric::Totals => self.totals as f64,
            InsightsMetric::Sessions => self.sessions.len() as f64,
            InsightsMetric::PropSum { .. } => self.sum,
            InsightsMetric::PropAvg { .. } => {
                if self.count_num == 0 {
                    0.0
                } else {
                    self.sum / (self.count_num as f64)
                }
            }
            InsightsMetric::PropMin { .. } => {
                if self.count_num == 0 {
                    0.0
                } else {
                    self.min
                }
            }
            InsightsMetric::PropMax { .. } => {
                if self.count_num == 0 {
                    0.0
                } else {
                    self.max
                }
            }
            InsightsMetric::PropPercentile { quantile, .. } => self
                .sketch
                .quantile(quantile.clamp(0.0, 1.0))
                .ok()
                .flatten()
                .unwrap_or(0.0),
            InsightsMetric::Wau | InsightsMetric::Mau => self.users.len() as f64,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_single_series(
    spec: &InsightsSeriesSpec,
    tgt: &crate::scanner::ResolvedEventTarget,
    range: TimeRange,
    interval: TimeInterval,
    buckets: &[i64],
    breakdown_dims: &[String],
    breakdown_cohorts: &HashMap<String, RoaringBitmap>,
    global_preds: &[crate::ast::PropPredicate],
    eligible_cohort: Option<&RoaringBitmap>,
    chunks: &[crate::scanner::DataChunk],
    ps: &ProjectStorage,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
) -> Result<Vec<SeriesOutput>> {
    // Breakdown key -> (overall accum, per-day-or-bucket accums)
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
            precompute_segment_row_mask(chunk, &all_preds, ps.dictionary(), cache).await?;

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
            let b_ts = align_unit.align_floor(ts);
            entry
                .1
                .entry(b_ts)
                .or_insert_with(BucketAccum::new)
                .observe(canon_uid, chunk.tier_a.session_hash[r], num_val);
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for (bkey, (overall_acc, per_bucket)) in groups {
        let mut points = Vec::with_capacity(buckets.len());
        for &bts in buckets {
            let val = match spec.metric {
                InsightsMetric::Wau | InsightsMetric::Mau => {
                    let window_days = if matches!(spec.metric, InsightsMetric::Wau) {
                        7
                    } else {
                        30
                    };
                    let end_day = TimeInterval::Day.align_floor(bts);
                    let mut union_bm = RoaringBitmap::new();
                    for d in 0..window_days {
                        let day_ts = end_day - d * 86_400_000;
                        if let Some(acc) = per_bucket.get(&day_ts) {
                            union_bm |= &acc.users;
                        }
                    }
                    union_bm.len() as f64
                }
                _ => per_bucket
                    .get(&bts)
                    .map(|acc| acc.finish(&spec.metric))
                    .unwrap_or(0.0),
            };
            points.push(TimePoint {
                bucket_ts_ms: bts,
                value: val,
            });
        }
        let overall_value = if is_rolling {
            points.last().map(|p| p.value).unwrap_or(0.0)
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

/// Evaluates multi-series mathematical formulas like `(B / A) * 100` per breakdown group & time bucket.
fn evaluate_formulas(
    formula_str: &str,
    buckets: &[i64],
    series: &[SeriesOutput],
) -> Result<Vec<SeriesOutput>> {
    let mut by_breakdown: BTreeMap<String, HashMap<String, &SeriesOutput>> = BTreeMap::new();
    for s in series {
        by_breakdown
            .entry(s.breakdown_key.clone())
            .or_default()
            .insert(s.label.clone(), s);
    }

    let mut out = Vec::with_capacity(by_breakdown.len());
    for (bkey, map) in by_breakdown {
        let mut overall_vars = HashMap::new();
        for (lbl, s) in &map {
            overall_vars.insert(lbl.clone(), s.overall_value);
        }
        let overall_value = eval_math_expr(formula_str, &overall_vars)?;

        let mut points = Vec::with_capacity(buckets.len());
        for (idx, &bts) in buckets.iter().enumerate() {
            let mut pt_vars = HashMap::new();
            for (lbl, s) in &map {
                let v = s.points.get(idx).map(|p| p.value).unwrap_or(0.0);
                pt_vars.insert(lbl.clone(), v);
            }
            let val = eval_math_expr(formula_str, &pt_vars)?;
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

/// Recursive-descent arithmetic expression evaluator (`+`, `-`, `*`, `/`, unary `-`, parens, numbers, and series variables `A`, `B`, ...).
pub fn eval_math_expr(expr: &str, vars: &HashMap<String, f64>) -> Result<f64> {
    struct Parser<'a> {
        bytes: &'a [u8],
        pos: usize,
        vars: &'a HashMap<String, f64>,
    }

    impl<'a> Parser<'a> {
        fn skip_ws(&mut self) {
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
        }

        fn parse_expr(&mut self) -> Result<f64> {
            let mut val = self.parse_term()?;
            loop {
                self.skip_ws();
                if self.pos >= self.bytes.len() {
                    break;
                }
                match self.bytes[self.pos] {
                    b'+' => {
                        self.pos += 1;
                        val += self.parse_term()?;
                    }
                    b'-' => {
                        self.pos += 1;
                        val -= self.parse_term()?;
                    }
                    _ => break,
                }
            }
            Ok(val)
        }

        fn parse_term(&mut self) -> Result<f64> {
            let mut val = self.parse_factor()?;
            loop {
                self.skip_ws();
                if self.pos >= self.bytes.len() {
                    break;
                }
                match self.bytes[self.pos] {
                    b'*' => {
                        self.pos += 1;
                        val *= self.parse_factor()?;
                    }
                    b'/' => {
                        self.pos += 1;
                        let denom = self.parse_factor()?;
                        val = if denom.abs() < 1e-12 {
                            0.0
                        } else {
                            val / denom
                        };
                    }
                    _ => break,
                }
            }
            Ok(val)
        }

        fn parse_factor(&mut self) -> Result<f64> {
            self.skip_ws();
            if self.pos >= self.bytes.len() {
                return Err(ExecError::Expression("unexpected end of formula".into()));
            }
            let b = self.bytes[self.pos];
            if b == b'-' {
                self.pos += 1;
                return Ok(-self.parse_factor()?);
            }
            if b == b'+' {
                self.pos += 1;
                return self.parse_factor();
            }
            if b == b'(' {
                self.pos += 1;
                let inner = self.parse_expr()?;
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
                let s = std::str::from_utf8(&self.bytes[start..self.pos]).unwrap_or("0");
                return s
                    .parse::<f64>()
                    .map_err(|e| ExecError::Expression(format!("invalid formula number: {e}")));
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
                return Ok(self.vars.get(name).copied().unwrap_or(0.0));
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
    let res = parser.parse_expr()?;
    Ok(if res.is_finite() { res } else { 0.0 })
}
