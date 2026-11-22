use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use chrono_tz::Tz;
use futures::stream::{self, StreamExt};
use pulse_identity::IdentitySnapshot;
use pulse_storage::{HllSketch, ProjectStorage, SegmentMeta, StorageSnapshot, StoredRow};
use pulse_wire::PropValue;
use roaring::RoaringBitmap;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};

use crate::ast::{
    InsightsMetric, InsightsQuery, InsightsResult, InsightsSeriesSpec, PropPredicate, SamplingInfo,
    SamplingMode, SeriesOutput, TimeInterval, TimePoint, TimeRange,
};
use crate::cache::SegmentBlockCache;
use crate::catalog::{evaluate_custom_property, UserFilterMask, VirtualCatalog};
use crate::scanner::{
    align_floor_tz_cached, evaluate_cohort, parallel_segments,
    precompute_segment_row_mask_for_meta, resolve_event_target, resolve_global_cohort_bitmap,
    segment_can_match_preds, ts_to_date_floor, ExecError, ResolvedEventTarget, Result,
    SamplingPlan,
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
        let sampling_plan = SamplingPlan::resolve(query.sampling, snap, ps, query.time_range);

        let (series, sampling_info) = evaluate_window(
            query,
            query.time_range,
            &buckets,
            tz,
            sampling_plan,
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
            let (prev_out, _) = evaluate_window(
                query,
                prev_range,
                &prev_buckets,
                tz,
                sampling_plan,
                ps,
                snap,
                id_snap,
                cache,
                catalog,
            )
            .await?;
            prev_out
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
            sampling: Some(sampling_info),
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

type BreakdownGroup = (BucketAccum, BTreeMap<i64, BucketAccum>);

/// Approximate working memory charged per newly created `(breakdown_key, time_bucket)` slot.
const ACCUM_SLOT_EST_BYTES: u64 = 192;

struct SeriesPlan<'a> {
    spec: &'a InsightsSeriesSpec,
    tgt: ResolvedEventTarget,
    is_rolling: bool,
    align_unit: TimeInterval,
    metric_mask: MetricMask,
    metric_prop: Option<&'a str>,
    /// Non-`user.*` series-specific predicates (series filters + custom event extra_filter).
    event_preds: Vec<PropPredicate>,
    /// Pre-evaluated `user.*` series-specific predicates (if any).
    user_mask: Option<UserFilterMask>,
}

fn split_user_and_event_preds(preds: &[PropPredicate]) -> (Vec<PropPredicate>, Vec<PropPredicate>) {
    let mut user_preds = Vec::new();
    let mut event_preds = Vec::new();
    for p in preds {
        if p.prop_name.starts_with("user.") {
            user_preds.push(p.clone());
        } else {
            event_preds.push(p.clone());
        }
    }
    (user_preds, event_preds)
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_window(
    query: &InsightsQuery,
    range: TimeRange,
    buckets: &[i64],
    tz: Tz,
    sampling_plan: SamplingPlan,
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
) -> Result<(Vec<SeriesOutput>, SamplingInfo)> {
    crate::budget::check_current()?;

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
    crate::budget::check_current()?;

    // Pre-evaluate global `user.*` predicates once against `ColumnarProfileIndex` (0 segment I/O).
    let (global_user_preds, global_event_preds) =
        split_user_and_event_preds(&query.global_filter.predicates);
    let mut global_user_mask = catalog.evaluate_user_predicates(&global_user_preds);
    if let Some(cohort) = eligible_cohort {
        global_user_mask = Some(match global_user_mask {
            Some(m) => m.intersect_cohort(&cohort),
            None => UserFilterMask::Include(cohort),
        });
    }

    let breakdown_dims: Vec<String> = query.breakdown_by.clone();
    let mut breakdown_cohort_bitmaps: HashMap<String, RoaringBitmap> = HashMap::new();
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
    crate::budget::check_current()?;

    // Build per-series execution plans.
    let dict = ps.dictionary();
    let mut plans: Vec<SeriesPlan<'_>> = Vec::with_capacity(query.series.len());
    for spec in &query.series {
        let tgt = resolve_event_target(&spec.event_name, dict, catalog);
        let is_rolling = matches!(spec.metric, InsightsMetric::Wau | InsightsMetric::Mau);
        let align_unit = if is_rolling {
            TimeInterval::Day
        } else {
            query.interval
        };
        let metric_mask = MetricMask::from_metric(&spec.metric);
        let metric_prop = match &spec.metric {
            InsightsMetric::PropSum { prop }
            | InsightsMetric::PropAvg { prop }
            | InsightsMetric::PropMin { prop }
            | InsightsMetric::PropMax { prop }
            | InsightsMetric::PropPercentile { prop, .. } => Some(prop.as_str()),
            _ => None,
        };
        let mut raw_series_preds = spec.filters.clone();
        if let Some(ef) = &tgt.extra_filter {
            raw_series_preds.push(ef.clone());
        }
        let (s_user_preds, s_event_preds) = split_user_and_event_preds(&raw_series_preds);
        let user_mask = catalog.evaluate_user_predicates(&s_user_preds);
        plans.push(SeriesPlan {
            spec,
            tgt,
            is_rolling,
            align_unit,
            metric_mask,
            metric_prop,
            event_preds: s_event_preds,
            user_mask,
        });
    }

    let mut series_groups: Vec<BTreeMap<String, BreakdownGroup>> =
        (0..plans.len()).map(|_| BTreeMap::new()).collect();

    // Short-circuit immediately with 0 segment I/O if the global user/cohort filter is empty.
    if global_user_mask
        .as_ref()
        .is_some_and(UserFilterMask::is_definitely_empty)
    {
        let exact_plan = SamplingPlan::exact(
            sampling_plan.total_buckets,
            sampling_plan.estimated_total_rows,
        );
        return finalize_series_outputs(
            &plans,
            series_groups,
            range,
            query.interval,
            buckets,
            tz,
            &breakdown_dims,
            exact_plan,
        );
    }

    let erased = ps.erased_snapshot();
    let has_any_non_empty_target = plans.iter().any(|p| !p.tgt.event_ids.is_empty());
    if !has_any_non_empty_target {
        let exact_plan = SamplingPlan::exact(
            sampling_plan.total_buckets,
            sampling_plan.estimated_total_rows,
        );
        return finalize_series_outputs(
            &plans,
            series_groups,
            range,
            query.interval,
            buckets,
            tz,
            &breakdown_dims,
            exact_plan,
        );
    }

    // Determine which Tier A columns and non-`user.*` properties are needed when scanning.
    let need_uid = !erased.is_empty()
        || global_user_mask.is_some()
        || !breakdown_cohort_bitmaps.is_empty()
        || breakdown_dims
            .iter()
            .any(|b| b.starts_with("user.") || catalog.has_user_prop(b))
        || plans.iter().any(|p| {
            p.metric_mask.users
                || p.user_mask.is_some()
                || p.metric_prop
                    .is_some_and(|mp| mp.starts_with("user.") || catalog.has_user_prop(mp))
                || p.event_preds
                    .iter()
                    .any(|pr| catalog.has_user_prop(&pr.prop_name))
        })
        || global_event_preds
            .iter()
            .any(|pr| catalog.has_user_prop(&pr.prop_name));
    let need_session = plans.iter().any(|p| p.metric_mask.sessions);

    let mut non_user_props_needed: HashSet<String> = HashSet::new();
    for p in &global_event_preds {
        non_user_props_needed.insert(p.prop_name.clone());
    }
    for b in &breakdown_dims {
        if !b.starts_with("$cohort:") && !b.starts_with("user.") {
            non_user_props_needed.insert(b.clone());
        }
    }
    for plan in &plans {
        for p in &plan.event_preds {
            non_user_props_needed.insert(p.prop_name.clone());
        }
        if let Some(mp) = plan.metric_prop {
            if !mp.starts_with("user.") {
                non_user_props_needed.insert(mp.to_string());
            }
        }
    }

    // 1. Collect & prune L1 segments using footer metadata (0 I/O).
    let start_date = ts_to_date_floor(scan_range.start_ts_ms);
    let end_date = ts_to_date_floor(scan_range.end_ts_ms);
    let all_segments = snap.segments_for_range(start_date, end_date, None);

    let all_series_footer_totals_eligible = erased.is_empty()
        && breakdown_dims.is_empty()
        && global_event_preds.is_empty()
        && global_user_mask.is_none()
        && plans.iter().all(|p| {
            matches!(p.spec.metric, InsightsMetric::Totals)
                && p.event_preds.is_empty()
                && p.user_mask.is_none()
                && !p.is_rolling
        });

    // Option 3: Zero-I/O footer HyperLogLog fast path for unfiltered `Uniques` / `DAU` / `WAU` / `MAU`
    // when `SamplingMode::Auto` engages on large ranges (`sampling_plan.is_sampled()`).
    let all_series_footer_hll_eligible = matches!(query.sampling, SamplingMode::Auto)
        && sampling_plan.is_sampled()
        && erased.is_empty()
        && breakdown_dims.is_empty()
        && global_event_preds.is_empty()
        && global_user_mask.is_none()
        && plans.iter().all(|p| {
            matches!(
                p.spec.metric,
                InsightsMetric::Uniques
                    | InsightsMetric::Dau
                    | InsightsMetric::Wau
                    | InsightsMetric::Mau
            ) && p.event_preds.is_empty()
                && p.user_mask.is_none()
        });

    if all_series_footer_hll_eligible {
        if let Some(hll_out) = try_footer_hll_fast_path(
            &all_segments,
            &plans,
            range,
            scan_range,
            query.interval,
            buckets,
            tz,
            ps,
            snap,
            sampling_plan,
        )? {
            return Ok(hll_out);
        }
    }

    // Option 1: Unfiltered `Totals` served by the 0-I/O footer fast path is always sub-10ms exact,
    // so never auto-sample it (explicit `SamplingMode::Rate(_)` still honors the caller's request).
    let effective_plan =
        if all_series_footer_totals_eligible && matches!(query.sampling, SamplingMode::Auto) {
            SamplingPlan::exact(
                sampling_plan.total_buckets,
                sampling_plan.estimated_total_rows,
            )
        } else {
            sampling_plan
        };
    let bucket_limit = effective_plan.bucket_limit();

    let mut scan_segments: Vec<SegmentMeta> = Vec::new();
    for seg in all_segments {
        if let Some(lim) = bucket_limit {
            if u16::from(seg.bucket_id) >= lim {
                continue;
            }
        }
        if seg.footer.max_ts_ms < scan_range.start_ts_ms
            || seg.footer.min_ts_ms > scan_range.end_ts_ms
        {
            continue;
        }
        let any_series_event = plans.iter().any(|p| {
            !p.tgt.event_ids.is_empty()
                && seg
                    .footer
                    .may_contain_any_event_ids(&p.tgt.event_ids, p.tgt.bloom_mask_64)
        });
        if !any_series_event {
            continue;
        }
        if !segment_can_match_preds(&seg, &global_event_preds, dict, catalog) {
            continue;
        }

        // Zero-I/O footer fast path: unfiltered `Totals` where every row in `seg` lands in the
        // exact same time bucket inside `range` and `seg.footer.event_counts` is populated.
        if all_series_footer_totals_eligible
            && seg.footer.min_ts_ms >= range.start_ts_ms
            && seg.footer.max_ts_ms <= range.end_ts_ms
        {
            let b_min = query.interval.align_floor_tz(seg.footer.min_ts_ms, tz);
            let b_max = query.interval.align_floor_tz(seg.footer.max_ts_ms, tz);
            if b_min == b_max {
                let mut exact_counts: Option<Vec<u64>> = Some(Vec::with_capacity(plans.len()));
                for p in &plans {
                    let mut sum = 0u64;
                    for &eid in &p.tgt.event_ids {
                        if let Some(c) = seg.footer.exact_event_count(eid) {
                            sum += u64::from(c);
                        } else {
                            exact_counts = None;
                            break;
                        }
                    }
                    if let Some(ref mut v) = exact_counts {
                        v.push(sum);
                    } else {
                        break;
                    }
                }
                if let Some(counts) = exact_counts {
                    for (s_idx, cnt) in counts.into_iter().enumerate() {
                        if cnt == 0 {
                            continue;
                        }
                        let entry = series_groups[s_idx]
                            .entry("overall".to_string())
                            .or_insert_with(|| (BucketAccum::new(), BTreeMap::new()));
                        entry.0.totals += cnt;
                        entry.1.entry(b_min).or_insert_with(BucketAccum::new).totals += cnt;
                    }
                    continue;
                }
            }
        }

        scan_segments.push(seg);
    }

    // Charge row budget only for segments we actually need to scan.
    if let Some(ctx) = crate::budget::current() {
        ctx.check()?;
        let rows_to_scan: u64 = scan_segments
            .iter()
            .map(|s| u64::from(s.footer.row_count))
            .sum();
        ctx.charge_rows(rows_to_scan)?;
    }

    // 2. Streaming bounded-concurrency single-pass execution over L1 segments.
    if !scan_segments.is_empty() {
        let est_bpr = 10
            + usize::from(need_uid) * 4
            + usize::from(need_session) * 8
            + non_user_props_needed.len() * 48;
        let concurrency = parallel_segments(&scan_segments, est_bpr);
        let plans_ref = &plans;
        let global_event_preds_ref = &global_event_preds;
        let global_user_mask_ref = &global_user_mask;
        let breakdown_dims_ref = &breakdown_dims;
        let breakdown_cohorts_ref = &breakdown_cohort_bitmaps;
        let non_user_props_ref = &non_user_props_needed;
        let erased_ref = &erased;

        let mut seg_stream = stream::iter(scan_segments.into_iter())
            .map(|seg| async move {
                process_l1_segment_streaming(
                    &seg,
                    plans_ref,
                    range,
                    tz,
                    global_event_preds_ref,
                    global_user_mask_ref.as_ref(),
                    breakdown_dims_ref,
                    breakdown_cohorts_ref,
                    non_user_props_ref,
                    need_uid,
                    need_session,
                    dict,
                    erased_ref,
                    id_snap,
                    cache,
                    catalog,
                )
                .await
            })
            .buffer_unordered(concurrency);

        while let Some(seg_res) = seg_stream.next().await {
            crate::budget::check_current()?;
            let per_series_local = seg_res?;
            for (s_idx, local_map) in per_series_local.into_iter().enumerate() {
                merge_breakdown_maps(&mut series_groups[s_idx], local_map)?;
            }
        }
    }

    // 3. Single-pass scan over hot L0 `MemBucket`s.
    let props_vec: Vec<String> = non_user_props_needed.iter().cloned().collect();
    let max_bucket = bucket_limit
        .unwrap_or(ps.config().num_buckets)
        .min(ps.config().num_buckets);
    for b in 0..max_bucket {
        crate::budget::check_current()?;
        let bucket_id = b as u8;
        let l0_rows: Vec<StoredRow> = ps
            .snapshot_l0_bucket(snap, bucket_id)
            .into_iter()
            .filter(|r| {
                scan_range.contains(r.event_ts_ms)
                    && (erased.is_empty()
                        || !(erased.hides(r.uid_internal, r.event_ts_ms)
                            || erased.hides(id_snap.resolve(r.uid_internal), r.event_ts_ms)))
            })
            .collect();
        if l0_rows.is_empty() {
            continue;
        }
        let n = l0_rows.len();
        let mut uid_internal = Vec::with_capacity(n);
        let mut session_hash = Vec::with_capacity(n);
        let mut event_id = Vec::with_capacity(n);
        let mut event_ts_ms = Vec::with_capacity(n);
        for r in &l0_rows {
            uid_internal.push(r.uid_internal);
            session_hash.push(r.session_hash);
            event_id.push(r.event_id);
            event_ts_ms.push(r.event_ts_ms);
        }

        let mut base_map: HashMap<String, Arc<Vec<PropValue>>> = HashMap::new();
        for p in &props_vec {
            if let Some(pk) = dict.lookup_prop_key(p) {
                let mut col = Vec::with_capacity(n);
                for r in &l0_rows {
                    let v = r
                        .props
                        .iter()
                        .find(|(k, _)| *k == pk)
                        .map(|(_, pv)| pv.clone())
                        .unwrap_or(PropValue::Null);
                    col.push(v);
                }
                base_map.insert(p.clone(), Arc::new(col));
            } else if let Some(cp) = catalog.custom_properties.get(p) {
                for dep in cp.referenced_columns() {
                    if !dep.starts_with("user.") && !base_map.contains_key(&dep) {
                        if let Some(pk) = dict.lookup_prop_key(&dep) {
                            let mut col = Vec::with_capacity(n);
                            for r in &l0_rows {
                                let v = r
                                    .props
                                    .iter()
                                    .find(|(k, _)| *k == pk)
                                    .map(|(_, pv)| pv.clone())
                                    .unwrap_or(PropValue::Null);
                                col.push(v);
                            }
                            base_map.insert(dep, Arc::new(col));
                        }
                    }
                }
            } else if let Some((lt, _)) = catalog.resolve_lookup_column(p) {
                if !base_map.contains_key(&lt.join_prop) {
                    if let Some(pk) = dict.lookup_prop_key(&lt.join_prop) {
                        let mut col = Vec::with_capacity(n);
                        for r in &l0_rows {
                            let v = r
                                .props
                                .iter()
                                .find(|(k, _)| *k == pk)
                                .map(|(_, pv)| pv.clone())
                                .unwrap_or(PropValue::Null);
                            col.push(v);
                        }
                        base_map.insert(lt.join_prop.clone(), Arc::new(col));
                    }
                }
            }
        }
        let prop_cols = materialize_props_for_rows(
            n,
            Some(&uid_internal),
            &base_map,
            &props_vec,
            catalog,
            id_snap,
        )?;

        let per_series_local = aggregate_rows_multi_series(
            n,
            Some(&uid_internal),
            Some(&session_hash),
            &event_id,
            &event_ts_ms,
            &prop_cols,
            &global_event_preds,
            global_user_mask.as_ref(),
            &plans,
            range,
            tz,
            &breakdown_dims,
            &breakdown_cohort_bitmaps,
            id_snap,
            catalog,
            &crate::budget::Stop::current(),
        )?;
        for (s_idx, local_map) in per_series_local.into_iter().enumerate() {
            merge_breakdown_maps(&mut series_groups[s_idx], local_map)?;
        }
    }

    finalize_series_outputs(
        &plans,
        series_groups,
        range,
        query.interval,
        buckets,
        tz,
        &breakdown_dims,
        effective_plan,
    )
}

#[allow(clippy::too_many_arguments)]
async fn process_l1_segment_streaming(
    seg: &SegmentMeta,
    plans: &[SeriesPlan<'_>],
    range: TimeRange,
    tz: Tz,
    global_event_preds: &[PropPredicate],
    global_user_mask: Option<&UserFilterMask>,
    breakdown_dims: &[String],
    breakdown_cohorts: &HashMap<String, RoaringBitmap>,
    non_user_props_needed: &HashSet<String>,
    need_uid: bool,
    need_session: bool,
    dict: &pulse_storage::ProjectDictionary,
    erased: &pulse_storage::ErasedUids,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
) -> Result<Vec<BTreeMap<String, BreakdownGroup>>> {
    crate::budget::check_current()?;
    let empty_res = || {
        (0..plans.len())
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>()
    };

    // Step 1: Inverted-index pruning *before* touching Vortex bytes.
    let (inv_mask, satisfied_global_idx) =
        precompute_segment_row_mask_for_meta(seg, global_event_preds, dict, catalog, cache).await?;
    if inv_mask.as_ref().is_some_and(RoaringBitmap::is_empty) {
        return Ok(empty_res());
    }

    let mut remaining_global_preds: Vec<PropPredicate> = Vec::new();
    for (idx, p) in global_event_preds.iter().enumerate() {
        if !satisfied_global_idx.contains(&idx) {
            remaining_global_preds.push(p.clone());
        }
    }

    // Build initial surviving row indices if inverted index pruned rows.
    let mut selected_rows: Option<Vec<u64>> = inv_mask.map(|bm| bm.iter().map(u64::from).collect());

    // Step 2: Late materialization for `user.*` / cohort filters: decode ONLY `uid_internal` first!
    let mut preloaded_uid: Option<Vec<u32>> = None;
    if let Some(umask) = global_user_mask {
        crate::budget::check_current()?;
        let uid_sub = cache
            .get_tier_a_subset(seg, true, false, false, false, selected_rows.as_deref())
            .await?;
        let uids = uid_sub.uid_internal.unwrap_or_default();
        let mut next_indices = Vec::new();
        let mut next_uids = Vec::new();
        match &selected_rows {
            Some(prev) => {
                for (&orig_row, &raw_uid) in prev.iter().zip(uids.iter()) {
                    if umask.contains(id_snap.resolve(raw_uid)) {
                        next_indices.push(orig_row);
                        next_uids.push(raw_uid);
                    }
                }
            }
            None => {
                for (row_idx, &raw_uid) in uids.iter().enumerate() {
                    if umask.contains(id_snap.resolve(raw_uid)) {
                        next_indices.push(row_idx as u64);
                        next_uids.push(raw_uid);
                    }
                }
            }
        }
        if next_indices.is_empty() {
            return Ok(empty_res());
        }
        selected_rows = Some(next_indices);
        preloaded_uid = Some(next_uids);
    }

    // Step 3: Project ONLY the needed Tier A columns (skipping already-loaded `uid_internal` and unused `session_hash`/`replay_offset_kb`).
    crate::budget::check_current()?;
    let fetch_uid = need_uid && preloaded_uid.is_none();
    let tier_a = cache
        .get_tier_a_subset(
            seg,
            fetch_uid,
            need_session,
            true,
            true,
            selected_rows.as_deref(),
        )
        .await?;
    let mut uid_vec = preloaded_uid.or(tier_a.uid_internal);
    let mut shash_vec = tier_a.session_hash;
    let mut ev_vec = tier_a.event_id.unwrap_or_default();
    let mut ts_vec = tier_a.event_ts_ms.unwrap_or_default();
    let n_before_erasure = ev_vec.len();
    if n_before_erasure == 0 {
        return Ok(empty_res());
    }

    // Step 4: Determine which non-`user.*` properties still need to be decoded (skipping properties
    // only used by predicates already 100% satisfied by the inverted index!).
    let mut needed_final_props: HashSet<String> = HashSet::new();
    for p in &remaining_global_preds {
        needed_final_props.insert(p.prop_name.clone());
    }
    for b in breakdown_dims {
        if !b.starts_with("$cohort:") && !b.starts_with("user.") {
            needed_final_props.insert(b.clone());
        }
    }
    for plan in plans {
        for p in &plan.event_preds {
            needed_final_props.insert(p.prop_name.clone());
        }
        if let Some(mp) = plan.metric_prop {
            if !mp.starts_with("user.") {
                needed_final_props.insert(mp.to_string());
            }
        }
    }
    let _ = non_user_props_needed;

    let mut base_props: HashSet<String> = HashSet::new();
    for p in &needed_final_props {
        if dict.lookup_prop_key(p).is_some() {
            base_props.insert(p.clone());
        } else if let Some(cp) = catalog.custom_properties.get(p) {
            for dep in cp.referenced_columns() {
                if !dep.starts_with("user.") {
                    base_props.insert(dep);
                }
            }
        } else if let Some((lt, _)) = catalog.resolve_lookup_column(p) {
            base_props.insert(lt.join_prop.clone());
        } else if !catalog.has_user_prop(p) {
            base_props.insert(p.clone());
        }
    }

    let mut base_map: HashMap<String, Arc<Vec<PropValue>>> =
        HashMap::with_capacity(base_props.len());
    for bp in &base_props {
        crate::budget::check_current()?;
        let col = if let Some(pk) = dict.lookup_prop_key(bp) {
            match &selected_rows {
                Some(indices) => {
                    Arc::new(cache.get_property_column_selected(seg, pk, indices).await?)
                }
                None => cache.get_property_column(seg, pk).await?,
            }
        } else {
            Arc::new(vec![PropValue::Null; n_before_erasure])
        };
        base_map.insert(bp.clone(), col);
    }

    // Step 5: Apply DPDP erasure filtering if any erased users exist.
    if !erased.is_empty() {
        if let Some(ref uids) = uid_vec {
            let has_hidden = (0..n_before_erasure).any(|r| {
                let u = uids[r];
                let ts = ts_vec[r];
                erased.hides(u, ts) || erased.hides(id_snap.resolve(u), ts)
            });
            if has_hidden {
                let keep: Vec<usize> = (0..n_before_erasure)
                    .filter(|&r| {
                        let u = uids[r];
                        let ts = ts_vec[r];
                        !(erased.hides(u, ts) || erased.hides(id_snap.resolve(u), ts))
                    })
                    .collect();
                if keep.is_empty() {
                    return Ok(empty_res());
                }
                uid_vec = Some(keep.iter().map(|&i| uids[i]).collect());
                if let Some(ref sh) = shash_vec {
                    shash_vec = Some(keep.iter().map(|&i| sh[i]).collect());
                }
                ev_vec = keep.iter().map(|&i| ev_vec[i]).collect();
                ts_vec = keep.iter().map(|&i| ts_vec[i]).collect();
                for col in base_map.values_mut() {
                    let compacted: Vec<PropValue> = keep.iter().map(|&i| col[i].clone()).collect();
                    *col = Arc::new(compacted);
                }
            }
        }
    }

    let final_props_vec: Vec<String> = needed_final_props.into_iter().collect();
    let prop_cols = materialize_props_for_rows(
        ev_vec.len(),
        uid_vec.as_deref(),
        &base_map,
        &final_props_vec,
        catalog,
        id_snap,
    )?;

    let stop = crate::budget::Stop::current();
    // Global user mask was already applied in Step 2, so pass `None` to avoid re-checking it.
    aggregate_rows_multi_series(
        ev_vec.len(),
        uid_vec.as_deref(),
        shash_vec.as_deref(),
        &ev_vec,
        &ts_vec,
        &prop_cols,
        &remaining_global_preds,
        None,
        plans,
        range,
        tz,
        breakdown_dims,
        breakdown_cohorts,
        id_snap,
        catalog,
        &stop,
    )
}

fn materialize_props_for_rows(
    num_rows: usize,
    uid_internal: Option<&[u32]>,
    base_map: &HashMap<String, Arc<Vec<PropValue>>>,
    final_props: &[String],
    catalog: &VirtualCatalog,
    id_snap: &IdentitySnapshot,
) -> Result<HashMap<String, Arc<Vec<PropValue>>>> {
    let mut out: HashMap<String, Arc<Vec<PropValue>>> = HashMap::with_capacity(final_props.len());
    for p in final_props {
        if let Some(col) = base_map.get(p) {
            if catalog.has_user_prop(p)
                && uid_internal.is_some()
                && col.iter().any(|v| matches!(v, PropValue::Null))
            {
                let uids = uid_internal.unwrap();
                let user_col = catalog.resolve_user_prop_column(uids, Some(id_snap), p);
                let merged: Vec<PropValue> = col
                    .iter()
                    .zip(user_col)
                    .map(|(ev_v, u_v)| {
                        if matches!(ev_v, PropValue::Null) {
                            u_v
                        } else {
                            ev_v.clone()
                        }
                    })
                    .collect();
                out.insert(p.clone(), Arc::new(merged));
            } else {
                out.insert(p.clone(), col.clone());
            }
        } else if let Some(cp) = catalog.custom_properties.get(p) {
            let computed =
                evaluate_custom_property(&cp, num_rows, base_map).map_err(ExecError::Expression)?;
            out.insert(p.clone(), Arc::new(computed));
        } else if let Some((lt, attr_col)) = catalog.resolve_lookup_column(p) {
            let join_vals = base_map.get(&lt.join_prop);
            let mut joined = Vec::with_capacity(num_rows);
            for r in 0..num_rows {
                let val = join_vals
                    .and_then(|v| v.get(r))
                    .and_then(|pv| {
                        if matches!(pv, PropValue::Null) {
                            None
                        } else {
                            lt.lookup_attr(&pv.as_str_cow(), &attr_col).cloned()
                        }
                    })
                    .unwrap_or(PropValue::Null);
                joined.push(val);
            }
            out.insert(p.clone(), Arc::new(joined));
        } else if catalog.has_user_prop(p) {
            if let Some(uids) = uid_internal {
                let vals = catalog.resolve_user_prop_column(uids, Some(id_snap), p);
                out.insert(p.clone(), Arc::new(vals));
            } else {
                out.insert(p.clone(), Arc::new(vec![PropValue::Null; num_rows]));
            }
        } else {
            out.insert(p.clone(), Arc::new(vec![PropValue::Null; num_rows]));
        }
    }
    Ok(out)
}

#[inline]
fn row_matches_props_map(
    prop_cols: &HashMap<String, Arc<Vec<PropValue>>>,
    row_idx: usize,
    preds: &[PropPredicate],
) -> bool {
    for pred in preds {
        let val = prop_cols.get(&pred.prop_name).and_then(|c| c.get(row_idx));
        if !pred.matches(val) {
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn aggregate_rows_multi_series(
    n: usize,
    uid_internal: Option<&[u32]>,
    session_hash: Option<&[u64]>,
    event_id: &[u16],
    event_ts_ms: &[i64],
    prop_cols: &HashMap<String, Arc<Vec<PropValue>>>,
    global_event_preds: &[PropPredicate],
    global_user_mask: Option<&UserFilterMask>,
    plans: &[SeriesPlan<'_>],
    range: TimeRange,
    tz: Tz,
    breakdown_dims: &[String],
    breakdown_cohorts: &HashMap<String, RoaringBitmap>,
    id_snap: &IdentitySnapshot,
    catalog: &VirtualCatalog,
    stop: &crate::budget::Stop,
) -> Result<Vec<BTreeMap<String, BreakdownGroup>>> {
    let mut per_series: Vec<BTreeMap<String, BreakdownGroup>> =
        (0..plans.len()).map(|_| BTreeMap::new()).collect();
    let mut tz_caches: Vec<Option<(i64, i64)>> = vec![None; plans.len()];

    for r in 0..n {
        if !stop.live_every(r) {
            break;
        }
        let raw_uid = uid_internal.map_or(0, |u| u[r]);
        let canon_uid = if uid_internal.is_some() {
            id_snap.resolve(raw_uid)
        } else {
            0
        };
        if let Some(umask) = global_user_mask {
            if !umask.contains(canon_uid) {
                continue;
            }
        }
        if !global_event_preds.is_empty()
            && !row_matches_props_map(prop_cols, r, global_event_preds)
        {
            continue;
        }

        let eid = event_id[r];
        let ts = event_ts_ms[r];
        let shash = session_hash.map_or(0, |s| s[r]);

        for (s_idx, plan) in plans.iter().enumerate() {
            if !plan.tgt.matches_id(eid) {
                continue;
            }
            if !plan.is_rolling && !range.contains(ts) {
                continue;
            }
            if plan.is_rolling && ts > range.end_ts_ms {
                continue;
            }
            if let Some(ref smask) = plan.user_mask {
                if !smask.contains(canon_uid) {
                    continue;
                }
            }
            if !plan.event_preds.is_empty()
                && !row_matches_props_map(prop_cols, r, &plan.event_preds)
            {
                continue;
            }

            let num_val = plan.metric_prop.and_then(|mp| {
                if mp.starts_with("user.") {
                    catalog.get_user_prop_f64(canon_uid, raw_uid, mp)
                } else {
                    prop_cols
                        .get(mp)
                        .and_then(|c| c.get(r))
                        .and_then(|pv| pv.as_f64())
                }
            });

            let local_groups = &mut per_series[s_idx];
            let entry = if breakdown_dims.is_empty() {
                if !local_groups.contains_key("overall") {
                    if !stop.charge_memory(ACCUM_SLOT_EST_BYTES + 7) {
                        break;
                    }
                    local_groups
                        .insert("overall".to_string(), (BucketAccum::new(), BTreeMap::new()));
                }
                local_groups
                    .get_mut("overall")
                    .expect("overall group inserted above")
            } else if breakdown_dims.len() == 1
                && !breakdown_cohorts.contains_key(&breakdown_dims[0])
            {
                let dim = &breakdown_dims[0];
                let key_cow: Cow<'_, str> = if dim.starts_with("user.") {
                    if let Some(arc_s) = catalog.get_user_prop_str_arc(canon_uid, raw_uid, dim) {
                        Cow::Owned(arc_s.to_string())
                    } else {
                        let pv = catalog.get_user_prop(canon_uid, raw_uid, dim);
                        if matches!(pv, PropValue::Null) {
                            Cow::Borrowed("(none)")
                        } else {
                            Cow::Owned(pv.as_str_cow().into_owned())
                        }
                    }
                } else {
                    let pv = prop_cols
                        .get(dim)
                        .and_then(|c| c.get(r))
                        .unwrap_or(&PropValue::Null);
                    if matches!(pv, PropValue::Null) {
                        Cow::Borrowed("(none)")
                    } else {
                        pv.as_str_cow()
                    }
                };
                if !local_groups.contains_key(key_cow.as_ref()) {
                    let k_len = key_cow.len() as u64;
                    if !stop.charge_memory(ACCUM_SLOT_EST_BYTES + k_len) {
                        break;
                    }
                    let owned = key_cow.into_owned();
                    local_groups.insert(owned.clone(), (BucketAccum::new(), BTreeMap::new()));
                    local_groups
                        .get_mut(&owned)
                        .expect("breakdown group inserted above")
                } else {
                    local_groups
                        .get_mut(key_cow.as_ref())
                        .expect("breakdown group checked above")
                }
            } else {
                let mut parts = Vec::with_capacity(breakdown_dims.len());
                for dim in breakdown_dims {
                    if let Some(cbm) = breakdown_cohorts.get(dim) {
                        parts.push(if cbm.contains(canon_uid) {
                            "in_cohort".to_string()
                        } else {
                            "not_in_cohort".to_string()
                        });
                    } else if dim.starts_with("user.") {
                        let pv = catalog.get_user_prop(canon_uid, raw_uid, dim);
                        parts.push(if matches!(pv, PropValue::Null) {
                            "(none)".to_string()
                        } else {
                            pv.as_str_cow().into_owned()
                        });
                    } else {
                        let pv = prop_cols
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
                let bkey = parts.join(" | ");
                if !local_groups.contains_key(&bkey) {
                    if !stop.charge_memory(ACCUM_SLOT_EST_BYTES + bkey.len() as u64) {
                        break;
                    }
                    local_groups.insert(bkey.clone(), (BucketAccum::new(), BTreeMap::new()));
                }
                local_groups
                    .get_mut(&bkey)
                    .expect("breakdown group inserted above")
            };

            if range.contains(ts) {
                entry.0.observe(canon_uid, shash, num_val, plan.metric_mask);
            }

            let b_ts = align_floor_tz_cached(ts, plan.align_unit, tz, &mut tz_caches[s_idx]);
            if let std::collections::btree_map::Entry::Vacant(e) = entry.1.entry(b_ts) {
                if !stop.charge_memory(ACCUM_SLOT_EST_BYTES) {
                    break;
                }
                e.insert(BucketAccum::new());
            }
            entry
                .1
                .get_mut(&b_ts)
                .expect("bucket inserted above")
                .observe(canon_uid, shash, num_val, plan.metric_mask);
        }
    }

    crate::budget::check_current()?;
    Ok(per_series)
}

fn merge_breakdown_maps(
    acc: &mut BTreeMap<String, BreakdownGroup>,
    map: BTreeMap<String, BreakdownGroup>,
) -> Result<()> {
    for (idx, (bkey, (ov, pb))) in map.into_iter().enumerate() {
        if idx & 255 == 0 {
            crate::budget::check_current()?;
        }
        let dst = acc
            .entry(bkey)
            .or_insert_with(|| (BucketAccum::new(), BTreeMap::new()));
        dst.0.merge(ov);
        for (b_idx, (bts, bucket_acc)) in pb.into_iter().enumerate() {
            if b_idx & 255 == 0 {
                crate::budget::check_current()?;
            }
            dst.1
                .entry(bts)
                .or_insert_with(BucketAccum::new)
                .merge(bucket_acc);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn try_footer_hll_fast_path(
    all_segments: &[SegmentMeta],
    plans: &[SeriesPlan<'_>],
    range: TimeRange,
    scan_range: TimeRange,
    interval: TimeInterval,
    buckets: &[i64],
    tz: Tz,
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    sampling_plan: SamplingPlan,
) -> Result<Option<(Vec<SeriesOutput>, SamplingInfo)>> {
    let mut per_series_overall: Vec<HllSketch> =
        (0..plans.len()).map(|_| HllSketch::new()).collect();
    let mut per_series_buckets: Vec<BTreeMap<i64, HllSketch>> =
        (0..plans.len()).map(|_| BTreeMap::new()).collect();

    for seg in all_segments {
        if seg.footer.max_ts_ms < scan_range.start_ts_ms
            || seg.footer.min_ts_ms > scan_range.end_ts_ms
        {
            continue;
        }
        // Every overlapping segment must lie completely inside `scan_range` and inside a single
        // alignment bucket for each series so we never misattribute boundary rows.
        if seg.footer.min_ts_ms < scan_range.start_ts_ms
            || seg.footer.max_ts_ms > scan_range.end_ts_ms
        {
            return Ok(None);
        }
        let in_main_range =
            seg.footer.min_ts_ms >= range.start_ts_ms && seg.footer.max_ts_ms <= range.end_ts_ms;
        let overlaps_main =
            seg.footer.max_ts_ms >= range.start_ts_ms && seg.footer.min_ts_ms <= range.end_ts_ms;
        if overlaps_main && !in_main_range {
            return Ok(None);
        }

        for (s_idx, plan) in plans.iter().enumerate() {
            if !plan.is_rolling && !in_main_range {
                continue;
            }
            let b_min = plan.align_unit.align_floor_tz(seg.footer.min_ts_ms, tz);
            let b_max = plan.align_unit.align_floor_tz(seg.footer.max_ts_ms, tz);
            if b_min != b_max {
                return Ok(None);
            }
            for &eid in &plan.tgt.event_ids {
                match seg.footer.event_hll(eid) {
                    Some(Some(compact)) => {
                        per_series_buckets[s_idx]
                            .entry(b_min)
                            .or_default()
                            .merge_compact(compact);
                        if in_main_range {
                            per_series_overall[s_idx].merge_compact(compact);
                        }
                    }
                    Some(None) => {}
                    None => return Ok(None),
                }
            }
        }
    }

    // Fold any hot L0 rows directly into the HLL sketches (zero Vortex I/O).
    for b in 0..ps.config().num_buckets {
        let l0_rows = ps.snapshot_l0_bucket(snap, b as u8);
        for r in &l0_rows {
            if !scan_range.contains(r.event_ts_ms) {
                continue;
            }
            let in_main = range.contains(r.event_ts_ms);
            for (s_idx, plan) in plans.iter().enumerate() {
                if !plan.is_rolling && !in_main {
                    continue;
                }
                if plan.tgt.matches_id(r.event_id) {
                    let b_ts = plan.align_unit.align_floor_tz(r.event_ts_ms, tz);
                    per_series_buckets[s_idx]
                        .entry(b_ts)
                        .or_default()
                        .observe_uid(r.uid_internal);
                    if in_main {
                        per_series_overall[s_idx].observe_uid(r.uid_internal);
                    }
                }
            }
        }
    }

    let mut outputs = Vec::with_capacity(plans.len());
    for (s_idx, plan) in plans.iter().enumerate() {
        let per_bucket = &per_series_buckets[s_idx];
        let mut points = Vec::with_capacity(buckets.len());
        for &bts in buckets {
            let val = match plan.spec.metric {
                InsightsMetric::Wau | InsightsMetric::Mau => {
                    let window_days = if matches!(plan.spec.metric, InsightsMetric::Wau) {
                        7i64
                    } else {
                        30i64
                    };
                    let bucket_end = interval
                        .bucket_end_inclusive_tz(bts, tz)
                        .min(range.end_ts_ms);
                    let end_day = TimeInterval::Day.align_floor_tz(bucket_end, tz);
                    let mut union_hll = HllSketch::new();
                    for d in 0..window_days {
                        if let Some(day_ts) = TimeInterval::prev_days_tz(end_day, d, tz) {
                            if let Some(sk) = per_bucket.get(&day_ts) {
                                union_hll.merge_sketch(sk);
                            }
                        }
                    }
                    Some(union_hll.estimate())
                }
                _ => Some(per_bucket.get(&bts).map_or(0.0, HllSketch::estimate)),
            };
            points.push(TimePoint {
                bucket_ts_ms: bts,
                value: val,
            });
        }
        let overall_value = if plan.is_rolling {
            points.last().and_then(|p| p.value).or(Some(0.0))
        } else {
            Some(per_series_overall[s_idx].estimate())
        };
        outputs.push(SeriesOutput {
            label: plan.spec.label.clone(),
            breakdown_key: "overall".to_string(),
            overall_value,
            points,
        });
    }

    Ok(Some((outputs, sampling_plan.to_hll_info())))
}

#[inline]
fn scale_metric_value(
    metric: &InsightsMetric,
    val: Option<f64>,
    plan: SamplingPlan,
) -> Option<f64> {
    if !plan.is_sampled() {
        return val;
    }
    let v = val?;
    match metric {
        InsightsMetric::Uniques
        | InsightsMetric::Totals
        | InsightsMetric::Sessions
        | InsightsMetric::Dau
        | InsightsMetric::Wau
        | InsightsMetric::Mau => Some((v * plan.scale_factor()).round()),
        InsightsMetric::PropSum { .. } => Some(v * plan.scale_factor()),
        InsightsMetric::PropAvg { .. }
        | InsightsMetric::PropMin { .. }
        | InsightsMetric::PropMax { .. }
        | InsightsMetric::PropPercentile { .. } => Some(v),
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_series_outputs(
    plans: &[SeriesPlan<'_>],
    series_groups: Vec<BTreeMap<String, BreakdownGroup>>,
    range: TimeRange,
    interval: TimeInterval,
    buckets: &[i64],
    tz: Tz,
    breakdown_dims: &[String],
    sampling_plan: SamplingPlan,
) -> Result<(Vec<SeriesOutput>, SamplingInfo)> {
    let mut all_outputs = Vec::new();
    let mut sample_count_basis = 0u64;
    for (plan, mut groups) in plans.iter().zip(series_groups.into_iter()) {
        crate::budget::check_current()?;
        if breakdown_dims.is_empty() {
            groups
                .entry("overall".to_string())
                .or_insert_with(|| (BucketAccum::new(), BTreeMap::new()));
        }

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
            for (idx, (_k, (ov, pb))) in tail.into_iter().enumerate() {
                if idx & 255 == 0 {
                    crate::budget::check_current()?;
                }
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

        for (bkey, (overall_acc, per_bucket)) in group_vec {
            crate::budget::check_current()?;
            sample_count_basis = sample_count_basis.saturating_add(if plan.metric_mask.users {
                overall_acc.users.len()
            } else {
                overall_acc.totals
            });
            let mut points = Vec::with_capacity(buckets.len());
            for &bts in buckets {
                let raw_val = match plan.spec.metric {
                    InsightsMetric::Wau | InsightsMetric::Mau => {
                        let window_days = if matches!(plan.spec.metric, InsightsMetric::Wau) {
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
                        .map(|acc| acc.finish(&plan.spec.metric))
                        .unwrap_or_else(|| empty_metric_value(&plan.spec.metric)),
                };
                points.push(TimePoint {
                    bucket_ts_ms: bts,
                    value: scale_metric_value(&plan.spec.metric, raw_val, sampling_plan),
                });
            }
            let overall_value = if plan.is_rolling {
                points.last().and_then(|p| p.value).or(Some(0.0))
            } else {
                scale_metric_value(
                    &plan.spec.metric,
                    overall_acc.finish(&plan.spec.metric),
                    sampling_plan,
                )
            };

            all_outputs.push(SeriesOutput {
                label: plan.spec.label.clone(),
                breakdown_key: bkey,
                overall_value,
                points,
            });
        }
    }
    Ok((all_outputs, sampling_plan.to_info(sample_count_basis)))
}

#[derive(Clone, Copy)]
struct MetricMask {
    users: bool,
    sessions: bool,
    numeric: bool,
    sketch: bool,
}

impl MetricMask {
    fn from_metric(metric: &InsightsMetric) -> Self {
        match metric {
            InsightsMetric::Uniques
            | InsightsMetric::Dau
            | InsightsMetric::Wau
            | InsightsMetric::Mau => Self {
                users: true,
                sessions: false,
                numeric: false,
                sketch: false,
            },
            InsightsMetric::Totals => Self {
                users: false,
                sessions: false,
                numeric: false,
                sketch: false,
            },
            InsightsMetric::Sessions => Self {
                users: false,
                sessions: true,
                numeric: false,
                sketch: false,
            },
            InsightsMetric::PropSum { .. }
            | InsightsMetric::PropAvg { .. }
            | InsightsMetric::PropMin { .. }
            | InsightsMetric::PropMax { .. } => Self {
                users: false,
                sessions: false,
                numeric: true,
                sketch: false,
            },
            InsightsMetric::PropPercentile { .. } => Self {
                users: false,
                sessions: false,
                numeric: true,
                sketch: true,
            },
        }
    }
}

struct BucketAccum {
    users: RoaringBitmap,
    sessions: HashSet<u64>,
    totals: u64,
    sum: f64,
    count_num: u64,
    min: f64,
    max: f64,
    sketch: Option<DDSketch>,
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
            sketch: None,
        }
    }

    #[inline]
    fn observe(
        &mut self,
        canon_uid: u32,
        session_hash: u64,
        num_val: Option<f64>,
        mask: MetricMask,
    ) {
        if mask.users {
            self.users.insert(canon_uid);
        }
        if mask.sessions {
            self.sessions.insert(session_hash);
        }
        self.totals += 1;
        if mask.numeric {
            if let Some(v) = num_val {
                if v.is_finite() {
                    self.sum += v;
                    self.count_num += 1;
                    self.min = self.min.min(v);
                    self.max = self.max.max(v);
                    if mask.sketch {
                        // Native signed DDSketch (`negative_store` + `zero_count` + `store`) — never clamp negatives (`P2-09a`)!
                        self.sketch
                            .get_or_insert_with(|| DDSketch::new(DDSketchConfig::defaults()))
                            .add(v);
                    }
                }
            }
        }
    }

    fn merge(&mut self, other: Self) {
        if !other.users.is_empty() {
            self.users |= other.users;
        }
        if !other.sessions.is_empty() {
            self.sessions.extend(other.sessions);
        }
        self.totals += other.totals;
        self.sum += other.sum;
        self.count_num += other.count_num;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        if let Some(other_sketch) = other.sketch {
            match &mut self.sketch {
                Some(self_sketch) => {
                    let _ = self_sketch.merge(&other_sketch);
                }
                None => {
                    self.sketch = Some(other_sketch);
                }
            }
        }
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
                        .as_ref()
                        .and_then(|s| s.quantile(quantile.clamp(0.0, 1.0)).ok().flatten())
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
/// and returns `Ok(None)` on division by zero or non-finite values.
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

#[cfg(test)]
mod hll_fast_path_tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use pulse_identity::IdentityRouter;
    use pulse_storage::{StorageConfig, StorageEngine};
    use pulse_wire::WireEvent;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[tokio::test]
    async fn footer_hll_fast_path_answers_uniques_and_wau_with_zero_vortex_reads() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id_router = Arc::new(IdentityRouter::new(store.clone()));
        let storage = StorageEngine::new(
            StorageConfig {
                num_buckets: 8,
                l0_flush_row_threshold: 100_000,
                l0_flush_max_age: Duration::from_secs(3600),
                compaction_min_segments: 4,
            },
            store,
            id_router.clone(),
        );
        let ps = storage.get_or_open_project("proj_hll").await.unwrap();
        let day0 = 1_790_000_000_000i64 - (1_790_000_000_000i64 % 86_400_000);

        // 2,000 distinct users on day 0, 1,000 overlapping + 500 new on day 1 -> 2,500 total uniques.
        let mut by_bucket: BTreeMap<u8, Vec<(WireEvent, u32)>> = BTreeMap::new();
        for u in 1..=2_000u32 {
            let did = format!("u_{u}");
            let b = pulse_storage::route_bucket(&did, 8);
            by_bucket.entry(b).or_default().push((
                WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        &did,
                        "s",
                        "open",
                        day0 + 1_000,
                        u as u64,
                    ),
                    routing_seed: did.clone(),
                    distinct_id: did,
                    session_id: "s".into(),
                    event_name: "open".into(),
                    client_event_ts_ms: day0 + 1_000,
                    client_sent_ts_ms: 0,
                    props: vec![],
                    replay_offset_kb: 0,
                },
                u,
            ));
        }
        for u in 1_001..=2_500u32 {
            let did = format!("u_{u}");
            let b = pulse_storage::route_bucket(&did, 8);
            by_bucket.entry(b).or_default().push((
                WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        &did,
                        "s2",
                        "open",
                        day0 + 86_400_000 + 1_000,
                        u as u64 + 10_000,
                    ),
                    routing_seed: did.clone(),
                    distinct_id: did,
                    session_id: "s2".into(),
                    event_name: "open".into(),
                    client_event_ts_ms: day0 + 86_400_000 + 1_000,
                    client_sent_ts_ms: 0,
                    props: vec![],
                    replay_offset_kb: 0,
                },
                u,
            ));
        }
        for (b, evs) in by_bucket {
            let g = ps.lock_bucket_append(b).await;
            ps.append_bucket_events_locked(&g, b, &evs).await.unwrap();
        }
        ps.flush_all().await.unwrap();

        let snap = ps.snapshot();
        let range = TimeRange {
            start_ts_ms: day0,
            end_ts_ms: day0 + 2 * 86_400_000 - 1,
        };
        let tz = chrono_tz::UTC;
        let buckets = build_time_buckets_tz(range, TimeInterval::Day, tz).unwrap();
        let dict = ps.dictionary();
        let catalog = VirtualCatalog::default();
        let spec_uniques = InsightsSeriesSpec {
            label: "U".into(),
            event_name: "open".into(),
            metric: InsightsMetric::Uniques,
            filters: vec![],
        };
        let spec_wau = InsightsSeriesSpec {
            label: "W".into(),
            event_name: "open".into(),
            metric: InsightsMetric::Wau,
            filters: vec![],
        };
        let plans = vec![
            SeriesPlan {
                spec: &spec_uniques,
                tgt: resolve_event_target("open", dict, &catalog),
                is_rolling: false,
                align_unit: TimeInterval::Day,
                metric_mask: MetricMask::from_metric(&spec_uniques.metric),
                metric_prop: None,
                event_preds: vec![],
                user_mask: None,
            },
            SeriesPlan {
                spec: &spec_wau,
                tgt: resolve_event_target("open", dict, &catalog),
                is_rolling: true,
                align_unit: TimeInterval::Day,
                metric_mask: MetricMask::from_metric(&spec_wau.metric),
                metric_prop: None,
                event_preds: vec![],
                user_mask: None,
            },
        ];

        let all_segments = snap.segments_for_range(
            ts_to_date_floor(range.start_ts_ms),
            ts_to_date_floor(range.end_ts_ms),
            None,
        );
        let reads_before = storage
            .segment_reader()
            .tier_ab_reads
            .load(Ordering::Relaxed);
        let plan = SamplingPlan {
            total_buckets: 8,
            sampled_buckets: 2,
            estimated_total_rows: 3_500,
        };
        let (outputs, info) = try_footer_hll_fast_path(
            &all_segments,
            &plans,
            range,
            range,
            TimeInterval::Day,
            &buckets,
            tz,
            &ps,
            &snap,
            plan,
        )
        .unwrap()
        .expect("HLL fast path should succeed on flushed segments");
        let reads_after = storage
            .segment_reader()
            .tier_ab_reads
            .load(Ordering::Relaxed);

        assert_eq!(
            reads_after, reads_before,
            "HLL fast path must perform 0 Vortex reads"
        );
        assert_eq!(info.method.as_deref(), Some("hll_sketch"));
        assert!(info.is_sampled);
        assert_eq!(info.sample_rate, 1.0);

        let est_overall = outputs[0].overall_value.unwrap();
        assert!(
            (est_overall - 2_500.0).abs() / 2_500.0 < 0.08,
            "HLL overall uniques {est_overall} should be within 8% of 2500"
        );
        let est_wau_day1 = outputs[1].points[1].value.unwrap();
        assert!(
            (est_wau_day1 - 2_500.0).abs() / 2_500.0 < 0.08,
            "HLL WAU day1 {est_wau_day1} should be within 8% of 2500"
        );

        id_router.close_all().await.unwrap();
    }
}
