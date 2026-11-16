use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use parking_lot::Mutex;
use pulse_identity::{IdentitySnapshot, ProjectIdentityGraph};
use pulse_storage::{ProjectStorage, StorageSnapshot};
use pulse_wire::PropValue;
use rayon::prelude::*;
use sketches_ddsketch::{Config as DDSketchConfig, DDSketch};

use crate::ast::{
    DroppedSessionSample, FunnelBreakdownGroup, FunnelOrderMode, FunnelQuery, FunnelResult,
    FunnelStepCount, TimeRange, TimeToConvertStats,
};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    load_project_chunks, resolve_event_target, resolve_global_cohort_bitmap,
    row_matches_predicates, DataChunk, ExecError, Result,
};

const HOLD_MISSING: u32 = u32::MAX;
const MAX_DROPPED_SAMPLES_PER_STEP: usize = 15;

#[derive(Debug, Clone, Copy)]
struct UserEvent {
    canon_uid: u32,
    ts_ms: i64,
    step_mask: u16,
    excl_mask: u16,
    hold_id: u32,
    breakdown_id: u32,
    chunk_idx: u16,
    row_idx: u32,
}

#[derive(Debug, Clone, Copy)]
struct UserFunnelOutcome {
    max_step: usize,
    breakdown_id: u32,
    ttc_ms: Option<i64>,
    last_ev: UserEvent,
}

struct ShardSummary {
    step_counts: Vec<u64>,
    breakdown_counts: HashMap<u32, Vec<u64>>,
    ttc_values: Vec<f64>,
    dropped_samples: Vec<(usize, UserEvent)>,
}

impl ShardSummary {
    fn new(k: usize) -> Self {
        Self {
            step_counts: vec![0u64; k],
            breakdown_counts: HashMap::new(),
            ttc_values: Vec::new(),
            dropped_samples: Vec::new(),
        }
    }

    fn record(&mut self, out: UserFunnelOutcome, k: usize, track_breakdown: bool) {
        for s in 0..=out.max_step {
            self.step_counts[s] += 1;
        }
        if track_breakdown {
            let bvec = self
                .breakdown_counts
                .entry(out.breakdown_id)
                .or_insert_with(|| vec![0u64; k]);
            for v in bvec.iter_mut().take(out.max_step + 1) {
                *v += 1;
            }
        }
        if out.max_step == k - 1 {
            if let Some(ttc) = out.ttc_ms {
                self.ttc_values.push((ttc.max(0)) as f64);
            }
        } else {
            let dropped_before = out.max_step + 1;
            let cur_count = self
                .dropped_samples
                .iter()
                .filter(|(st, _)| *st == dropped_before)
                .count();
            if cur_count < MAX_DROPPED_SAMPLES_PER_STEP {
                self.dropped_samples.push((dropped_before, out.last_ev));
            }
        }
    }

    fn merge(&mut self, mut other: ShardSummary, k: usize) {
        for s in 0..k {
            self.step_counts[s] += other.step_counts[s];
        }
        for (bid, counts) in other.breakdown_counts {
            let dst = self
                .breakdown_counts
                .entry(bid)
                .or_insert_with(|| vec![0u64; k]);
            for s in 0..k {
                dst[s] += counts[s];
            }
        }
        self.ttc_values.append(&mut other.ttc_values);
        for (step_idx, ev) in other.dropped_samples {
            let cur = self
                .dropped_samples
                .iter()
                .filter(|(st, _)| *st == step_idx)
                .count();
            if cur < MAX_DROPPED_SAMPLES_PER_STEP {
                self.dropped_samples.push((step_idx, ev));
            }
        }
    }
}

pub struct FunnelEngine;

impl FunnelEngine {
    pub async fn execute(
        query: &FunnelQuery,
        ps: &ProjectStorage,
        snap: &StorageSnapshot,
        id_graph: &ProjectIdentityGraph,
        id_snap: &IdentitySnapshot,
        cache: &SegmentBlockCache,
        catalog: &VirtualCatalog,
    ) -> Result<FunnelResult> {
        let t0 = Instant::now();
        let k = query.steps.len();
        if k == 0 || k > 16 {
            return Err(ExecError::InvalidQuery(
                "funnel must contain between 1 and 16 steps".into(),
            ));
        }

        let window_ms = if query.conversion_window_ms <= 0 {
            7 * 86_400_000
        } else {
            query.conversion_window_ms
        };

        let any_first_time = query.steps.iter().any(|s| s.first_time_ever);
        // Expand scan range to `[start, end + window_ms]` (or full history if `first_time_ever` is enabled!)
        let scan_range = if any_first_time {
            None
        } else {
            Some(TimeRange {
                start_ts_ms: query.time_range.start_ts_ms,
                end_ts_ms: query.time_range.end_ts_ms.saturating_add(window_ms),
            })
        };

        let eligible_cohort = resolve_global_cohort_bitmap(
            &query.global_filter,
            query.time_range,
            ps,
            snap,
            id_snap,
            cache,
            catalog,
        )
        .await?;

        // Resolve step & exclusion targets + collect needed properties
        let dict = ps.dictionary();
        let mut step_targets = Vec::with_capacity(k);
        let mut props_needed: HashSet<String> = HashSet::new();
        let mut bloom_union = 0u64;

        for p in &query.global_filter.predicates {
            props_needed.insert(p.prop_name.clone());
        }
        for hp in &query.hold_constant_props {
            props_needed.insert(hp.clone());
        }
        if let Some(bp) = &query.breakdown_by {
            props_needed.insert(bp.clone());
        }

        for step in &query.steps {
            let tgt = resolve_event_target(&step.event_name, dict, catalog);
            bloom_union |= tgt.bloom_mask_64;
            for rp in tgt.required_props() {
                props_needed.insert(rp);
            }
            for fp in &step.filters {
                props_needed.insert(fp.prop_name.clone());
            }
            step_targets.push(tgt);
        }

        let mut excl_targets = Vec::with_capacity(query.exclusions.len());
        for ex in &query.exclusions {
            let tgt = resolve_event_target(&ex.event_name, dict, catalog);
            bloom_union |= tgt.bloom_mask_64;
            for rp in tgt.required_props() {
                props_needed.insert(rp);
            }
            excl_targets.push((tgt, ex.between_steps));
        }

        let is_strict = query.order_mode == FunnelOrderMode::StrictOrder;
        let bloom_hint = if is_strict {
            None
        } else if bloom_union != 0 {
            Some(bloom_union)
        } else {
            None
        };

        let props_vec: Vec<String> = props_needed.into_iter().collect();
        let chunks =
            load_project_chunks(ps, snap, cache, catalog, scan_range, &props_vec, bloom_hint)
                .await?;

        // Fast event_id -> (base_step_mask, base_excl_mask) lookup table for pure event-ID checks
        let max_ev_id = dict.all_events().len().max(64);
        let mut ev_step_bits = vec![0u16; max_ev_id + 1];
        let mut ev_excl_bits = vec![0u16; max_ev_id + 1];
        for (s_idx, tgt) in step_targets.iter().enumerate() {
            for &eid in &tgt.event_ids {
                if (eid as usize) < ev_step_bits.len() {
                    ev_step_bits[eid as usize] |= 1u16 << s_idx;
                }
            }
        }
        for (e_idx, (tgt, _)) in excl_targets.iter().enumerate().take(16) {
            for &eid in &tgt.event_ids {
                if (eid as usize) < ev_excl_bits.len() {
                    ev_excl_bits[eid as usize] |= 1u16 << e_idx;
                }
            }
        }

        let has_step_filters = query.steps.iter().any(|s| !s.filters.is_empty())
            || step_targets.iter().any(|t| t.extra_filter.is_some())
            || excl_targets.iter().any(|(t, _)| t.extra_filter.is_some());
        let has_global_preds = !query.global_filter.predicates.is_empty();
        let has_hold = !query.hold_constant_props.is_empty();
        let has_breakdown = query.breakdown_by.is_some();

        // Thread-safe string-tuple interners for `hold_constant` and `breakdown_by`
        let hold_dict: Mutex<(HashMap<String, u32>, Vec<String>)> =
            Mutex::new((HashMap::new(), vec![String::new()]));
        let bdown_dict: Mutex<(HashMap<String, u32>, Vec<String>)> =
            Mutex::new((HashMap::new(), vec!["(none)".to_string()]));

        // Group chunks by physical `bucket_id` so single-bucket users execute 100% shard-locally in parallel!
        let mut chunks_by_bucket: BTreeMap<u8, Vec<(u16, &DataChunk)>> = BTreeMap::new();
        for (c_idx, chunk) in chunks.iter().enumerate() {
            chunks_by_bucket
                .entry(chunk.bucket_id)
                .or_default()
                .push((c_idx as u16, chunk));
        }
        let bucket_groups: Vec<(u8, Vec<(u16, &DataChunk)>)> =
            chunks_by_bucket.into_iter().collect();

        let first_time_mask: u16 = query.steps.iter().enumerate().fold(0u16, |acc, (i, s)| {
            if s.first_time_ever {
                acc | (1u16 << i)
            } else {
                acc
            }
        });

        let excl_ranges: Vec<(usize, usize)> = excl_targets.iter().map(|(_, r)| *r).collect();

        let effective_max_ts = query.time_range.end_ts_ms.saturating_add(window_ms);
        let effective_min_ts = if any_first_time {
            i64::MIN
        } else {
            query.time_range.start_ts_ms
        };

        // Parallel shard execution via Rayon!
        // Single-bucket users (~98%) finish completely inside their shard;
        // Cross-bucket users (~2%) emit compact `UserEvent`s to be stitched at the coordinator.
        let shard_outputs: Vec<(ShardSummary, Vec<UserEvent>)> = bucket_groups
            .par_iter()
            .map(|(_bucket_id, b_chunks)| {
                let mut local_events: Vec<UserEvent> = Vec::new();
                let mut cross_events: Vec<UserEvent> = Vec::new();

                for &(chunk_idx, chunk) in b_chunks {
                    let ta = &chunk.tier_a;
                    let n = ta.len();

                    for r in 0..n {
                        let ts = ta.event_ts_ms[r];
                        if ts < effective_min_ts || ts > effective_max_ts {
                            continue;
                        }

                        let eid = ta.event_id[r] as usize;
                        let mut s_mask = ev_step_bits.get(eid).copied().unwrap_or(0);
                        let mut x_mask = ev_excl_bits.get(eid).copied().unwrap_or(0);

                        if !is_strict && s_mask == 0 && x_mask == 0 {
                            continue;
                        }

                        let canon_uid = id_snap.resolve(ta.uid_internal[r]);
                        if let Some(ec) = &eligible_cohort {
                            if !ec.contains(canon_uid) {
                                continue;
                            }
                        }

                        if has_global_preds
                            && !row_matches_predicates(chunk, r, &query.global_filter.predicates)
                        {
                            continue;
                        }

                        if has_step_filters {
                            if s_mask != 0 {
                                for (s_idx, step_tgt) in step_targets.iter().enumerate().take(k) {
                                    if (s_mask & (1u16 << s_idx)) != 0 {
                                        let pass_extra =
                                            step_tgt.extra_filter.as_ref().is_none_or(|ef| {
                                                row_matches_predicates(
                                                    chunk,
                                                    r,
                                                    std::slice::from_ref(ef),
                                                )
                                            });
                                        let pass_step = pass_extra
                                            && row_matches_predicates(
                                                chunk,
                                                r,
                                                &query.steps[s_idx].filters,
                                            );
                                        if !pass_step {
                                            s_mask &= !(1u16 << s_idx);
                                        }
                                    }
                                }
                            }
                            if x_mask != 0 {
                                for (e_idx, (tgt, _)) in excl_targets.iter().enumerate() {
                                    if (x_mask & (1u16 << e_idx)) != 0
                                        && tgt.extra_filter.as_ref().is_some_and(|ef| {
                                            !row_matches_predicates(
                                                chunk,
                                                r,
                                                std::slice::from_ref(ef),
                                            )
                                        })
                                    {
                                        x_mask &= !(1u16 << e_idx);
                                    }
                                }
                            }
                            if !is_strict && s_mask == 0 && x_mask == 0 {
                                continue;
                            }
                        }

                        let hold_id = if has_hold {
                            intern_hold_tuple(chunk, r, &query.hold_constant_props, &hold_dict)
                        } else {
                            0
                        };

                        let breakdown_id = if let Some(bprop) = &query.breakdown_by {
                            if (s_mask & 1) != 0 || query.order_mode == FunnelOrderMode::AnyOrder {
                                intern_single_prop(chunk, r, bprop, &bdown_dict)
                            } else {
                                0
                            }
                        } else {
                            0
                        };

                        let uev = UserEvent {
                            canon_uid,
                            ts_ms: ts,
                            step_mask: s_mask,
                            excl_mask: x_mask,
                            hold_id,
                            breakdown_id,
                            chunk_idx,
                            row_idx: r as u32,
                        };

                        if id_snap.is_cross_bucket(canon_uid) {
                            cross_events.push(uev);
                        } else {
                            local_events.push(uev);
                        }
                    }
                }

                let summary = evaluate_sorted_user_stream(
                    &mut local_events,
                    query.time_range,
                    k,
                    query.order_mode,
                    window_ms,
                    &excl_ranges,
                    first_time_mask,
                    has_hold,
                    has_breakdown,
                );
                (summary, cross_events)
            })
            .collect();

        // Merge shard summaries + execute coordinator Cross-Bucket Exact Stitch on `cross_bucket_users`!
        let mut final_summary = ShardSummary::new(k);
        let mut all_cross_events: Vec<UserEvent> = Vec::new();
        for (shard_sum, mut cross) in shard_outputs {
            final_summary.merge(shard_sum, k);
            all_cross_events.append(&mut cross);
        }

        if !all_cross_events.is_empty() {
            let cross_summary = evaluate_sorted_user_stream(
                &mut all_cross_events,
                query.time_range,
                k,
                query.order_mode,
                window_ms,
                &excl_ranges,
                first_time_mask,
                has_hold,
                has_breakdown,
            );
            final_summary.merge(cross_summary, k);
        }

        let overall_steps = build_step_counts(&query.steps, &final_summary.step_counts);

        let mut breakdowns = Vec::new();
        if has_breakdown {
            let bdown_names = bdown_dict.into_inner().1;
            let mut entries: Vec<(String, Vec<FunnelStepCount>)> = final_summary
                .breakdown_counts
                .into_iter()
                .map(|(bid, counts)| {
                    let name = bdown_names
                        .get(bid as usize)
                        .cloned()
                        .unwrap_or_else(|| "(none)".to_string());
                    (name, build_step_counts(&query.steps, &counts))
                })
                .collect();
            entries.sort_by(|a, b| {
                b.1[0]
                    .entered_users
                    .cmp(&a.1[0].entered_users)
                    .then_with(|| a.0.cmp(&b.0))
            });
            for (breakdown_value, steps) in entries {
                breakdowns.push(FunnelBreakdownGroup {
                    breakdown_value,
                    steps,
                });
            }
        }

        let time_to_convert = build_ttc_stats(&final_summary.ttc_values);

        // Materialize dropped sample sessions with `session_id`, `distinct_id`, and `replay_offset_kb`
        let mut dropped_sample_sessions = Vec::with_capacity(final_summary.dropped_samples.len());
        for (dropped_before_step, ev) in final_summary.dropped_samples {
            let chunk = &chunks[ev.chunk_idx as usize];
            let r_idx = ev.row_idx as usize;
            let session_id = chunk.session_id_for_row(r_idx);
            let replay_offset_kb = chunk.tier_a.replay_offset_kb[r_idx];
            let distinct_id = id_graph
                .external_id(ev.canon_uid)
                .unwrap_or_else(|| format!("uid_{}", ev.canon_uid));

            dropped_sample_sessions.push(DroppedSessionSample {
                dropped_before_step,
                session_id,
                canonical_uid: ev.canon_uid,
                distinct_id,
                drop_ts_ms: ev.ts_ms,
                replay_offset_kb,
            });
        }

        Ok(FunnelResult {
            snapshot_epoch: snap.epoch(),
            exec_time_us: t0.elapsed().as_micros() as u64,
            overall_steps,
            breakdowns,
            time_to_convert,
            dropped_sample_sessions,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_sorted_user_stream(
    events: &mut [UserEvent],
    query_range: TimeRange,
    k: usize,
    mode: FunnelOrderMode,
    window_ms: i64,
    exclusions: &[(usize, usize)],
    first_time_mask: u16,
    has_hold: bool,
    has_breakdown: bool,
) -> ShardSummary {
    let mut summary = ShardSummary::new(k);
    if events.is_empty() {
        return summary;
    }

    // Sort contiguously by `(canon_uid, ts_ms, lowest_step_bit_tiebreak, chunk_idx, row_idx)`
    events.sort_unstable_by(|a, b| {
        a.canon_uid
            .cmp(&b.canon_uid)
            .then_with(|| a.ts_ms.cmp(&b.ts_ms))
            .then_with(|| {
                a.step_mask
                    .trailing_zeros()
                    .cmp(&b.step_mask.trailing_zeros())
            })
            .then_with(|| a.chunk_idx.cmp(&b.chunk_idx))
            .then_with(|| a.row_idx.cmp(&b.row_idx))
    });

    // Precompute exclusion bitmask active at `cur_step`: `lo <= cur_step && cur_step < hi`
    let mut active_excl_at_step = [0u16; 16];
    for (e_idx, &(lo, hi)) in exclusions.iter().enumerate().take(16) {
        for (s, mask_slot) in active_excl_at_step
            .iter_mut()
            .enumerate()
            .take(hi.min(16))
            .skip(lo)
        {
            let _ = s;
            *mask_slot |= 1u16 << e_idx;
        }
    }

    let mut start = 0usize;
    while start < events.len() {
        let uid = events[start].canon_uid;
        let mut end = start + 1;
        while end < events.len() && events[end].canon_uid == uid {
            end += 1;
        }

        let user_slice = &mut events[start..end];
        if first_time_mask != 0 {
            let mut seen_first: u16 = 0;
            for ev in user_slice.iter_mut() {
                let restricted = ev.step_mask & first_time_mask;
                if restricted != 0 {
                    let already_seen = restricted & seen_first;
                    ev.step_mask &= !already_seen;
                    seen_first |= restricted;
                }
            }
        }

        if let Some(outcome) = evaluate_single_user(
            user_slice,
            query_range,
            k,
            mode,
            window_ms,
            &active_excl_at_step,
            exclusions,
            has_hold,
        ) {
            summary.record(outcome, k, has_breakdown);
        }

        start = end;
    }

    summary
}

#[allow(clippy::too_many_arguments)]
fn evaluate_single_user(
    events: &[UserEvent],
    query_range: TimeRange,
    k: usize,
    mode: FunnelOrderMode,
    window_ms: i64,
    active_excl_at_step: &[u16; 16],
    exclusions: &[(usize, usize)],
    has_hold: bool,
) -> Option<UserFunnelOutcome> {
    let mut best: Option<UserFunnelOutcome> = None;

    match mode {
        FunnelOrderMode::LooseOrder | FunnelOrderMode::StrictOrder => {
            let is_strict = mode == FunnelOrderMode::StrictOrder;
            for i in 0..events.len() {
                let e0 = events[i];
                if (e0.step_mask & 1) == 0 || !query_range.contains(e0.ts_ms) {
                    continue;
                }
                if has_hold && e0.hold_id == HOLD_MISSING {
                    continue;
                }

                let mut cur_step = 0usize;
                let mut last_ev = e0;

                if k > 1 {
                    for ej in &events[(i + 1)..] {
                        if ej.ts_ms - e0.ts_ms > window_ms {
                            break;
                        }
                        if (ej.excl_mask & active_excl_at_step[cur_step]) != 0 {
                            break;
                        }
                        let next_bit = 1u16 << (cur_step + 1);
                        let step_ok = (ej.step_mask & next_bit) != 0
                            && (!has_hold || ej.hold_id == e0.hold_id);

                        if step_ok {
                            cur_step += 1;
                            last_ev = *ej;
                            if cur_step == k - 1 {
                                break;
                            }
                        } else if is_strict {
                            break;
                        }
                    }
                }

                let ttc_ms = (cur_step == k - 1).then_some(last_ev.ts_ms - e0.ts_ms);
                let cand = UserFunnelOutcome {
                    max_step: cur_step,
                    breakdown_id: e0.breakdown_id,
                    ttc_ms,
                    last_ev,
                };
                best = Some(pick_better(best, cand));
                if cur_step == k - 1 && !has_hold {
                    // Already converted all steps; keep scanning only if an even faster TTC exists
                }
            }
        }
        FunnelOrderMode::AnyOrder => {
            let full_mask: u16 = ((1u32 << k) - 1) as u16;
            for i in 0..events.len() {
                let e0 = events[i];
                if (e0.step_mask & full_mask) == 0 || !query_range.contains(e0.ts_ms) {
                    continue;
                }
                if has_hold && e0.hold_id == HOLD_MISSING {
                    continue;
                }

                let mut matched_mask = e0.step_mask & full_mask;
                let mut last_ev = e0;

                for ej in &events[(i + 1)..] {
                    if ej.ts_ms - e0.ts_ms > window_ms {
                        break;
                    }
                    // Check if any exclusion `(lo, hi)` triggers when `lo` is matched and `hi` is not yet matched
                    let mut excluded = false;
                    if ej.excl_mask != 0 {
                        for (e_idx, &(lo, hi)) in exclusions.iter().enumerate().take(16) {
                            if (ej.excl_mask & (1u16 << e_idx)) != 0
                                && (matched_mask & (1u16 << lo)) != 0
                                && (matched_mask & (1u16 << hi)) == 0
                            {
                                excluded = true;
                                break;
                            }
                        }
                    }
                    if excluded {
                        break;
                    }
                    if !has_hold || ej.hold_id == e0.hold_id {
                        let new_bits = (ej.step_mask & full_mask) & !matched_mask;
                        if new_bits != 0 {
                            matched_mask |= new_bits;
                            last_ev = *ej;
                            if matched_mask == full_mask {
                                break;
                            }
                        }
                    }
                }

                let completed_count = matched_mask.count_ones() as usize;
                if completed_count == 0 {
                    continue;
                }
                let max_step = completed_count - 1;
                let ttc_ms = (max_step == k - 1).then_some(last_ev.ts_ms - e0.ts_ms);
                let cand = UserFunnelOutcome {
                    max_step,
                    breakdown_id: e0.breakdown_id,
                    ttc_ms,
                    last_ev,
                };
                best = Some(pick_better(best, cand));
            }
        }
    }

    best
}

#[inline]
fn pick_better(prev: Option<UserFunnelOutcome>, cand: UserFunnelOutcome) -> UserFunnelOutcome {
    let Some(cur) = prev else {
        return cand;
    };
    if cand.max_step > cur.max_step {
        return cand;
    }
    if cand.max_step == cur.max_step {
        if let (Some(c_ttc), Some(p_ttc)) = (cand.ttc_ms, cur.ttc_ms) {
            if c_ttc < p_ttc {
                return cand;
            }
        }
    }
    cur
}

fn intern_hold_tuple(
    chunk: &DataChunk,
    row_idx: usize,
    hold_props: &[String],
    dict: &Mutex<(HashMap<String, u32>, Vec<String>)>,
) -> u32 {
    let mut key = String::new();
    for (idx, hp) in hold_props.iter().enumerate() {
        let Some(pv) = chunk.prop_cols.get(hp).and_then(|c| c.get(row_idx)) else {
            return HOLD_MISSING;
        };
        if matches!(pv, PropValue::Null) {
            return HOLD_MISSING;
        }
        if idx > 0 {
            key.push('\x1f');
        }
        key.push_str(&pv.as_str_cow());
    }
    let mut guard = dict.lock();
    if let Some(&id) = guard.0.get(&key) {
        return id;
    }
    let id = guard.1.len() as u32;
    guard.1.push(key.clone());
    guard.0.insert(key, id);
    id
}

fn intern_single_prop(
    chunk: &DataChunk,
    row_idx: usize,
    prop_name: &str,
    dict: &Mutex<(HashMap<String, u32>, Vec<String>)>,
) -> u32 {
    let Some(pv) = chunk.prop_cols.get(prop_name).and_then(|c| c.get(row_idx)) else {
        return 0;
    };
    if matches!(pv, PropValue::Null) {
        return 0;
    }
    let s = pv.as_str_cow();
    let mut guard = dict.lock();
    if let Some(&id) = guard.0.get(s.as_ref()) {
        return id;
    }
    let id = guard.1.len() as u32;
    let owned = s.into_owned();
    guard.1.push(owned.clone());
    guard.0.insert(owned, id);
    id
}

fn build_step_counts(specs: &[crate::ast::FunnelStepSpec], counts: &[u64]) -> Vec<FunnelStepCount> {
    let step0 = counts.first().copied().unwrap_or(0);
    let mut out = Vec::with_capacity(specs.len());
    for (idx, spec) in specs.iter().enumerate() {
        let entered = counts.get(idx).copied().unwrap_or(0);
        let next_entered = counts.get(idx + 1).copied().unwrap_or(entered);
        let dropped = if idx + 1 < specs.len() {
            entered.saturating_sub(next_entered)
        } else {
            0
        };
        let prev = if idx == 0 {
            entered
        } else {
            counts.get(idx - 1).copied().unwrap_or(0)
        };
        let from_start = if step0 == 0 {
            0.0
        } else {
            (entered as f64 / step0 as f64) * 100.0
        };
        let from_prev = if prev == 0 {
            0.0
        } else {
            (entered as f64 / prev as f64) * 100.0
        };
        out.push(FunnelStepCount {
            step_index: idx,
            event_name: spec.event_name.clone(),
            entered_users: entered,
            dropped_users: dropped,
            conversion_from_start_pct: from_start,
            conversion_from_prev_pct: from_prev,
        });
    }
    out
}

fn build_ttc_stats(ttc_values: &[f64]) -> TimeToConvertStats {
    if ttc_values.is_empty() {
        return TimeToConvertStats {
            converted_count: 0,
            avg_ms: 0.0,
            p50_ms: 0.0,
            p90_ms: 0.0,
            p99_ms: 0.0,
            histogram_bins: Vec::new(),
        };
    }

    let mut sketch = DDSketch::new(DDSketchConfig::defaults());
    let mut sum = 0.0;
    let mut min_v = f64::INFINITY;
    let mut max_v = 0.0f64;

    for &v in ttc_values {
        sketch.add(v);
        sum += v;
        min_v = min_v.min(v);
        max_v = max_v.max(v);
    }

    let p50 = sketch.quantile(0.50).ok().flatten().unwrap_or(0.0);
    let p90 = sketch.quantile(0.90).ok().flatten().unwrap_or(0.0);
    let p99 = sketch.quantile(0.99).ok().flatten().unwrap_or(0.0);
    let avg = sum / (ttc_values.len() as f64);

    let bin_count = 8usize;
    let span = (max_v - min_v).max(1.0);
    let step = span / (bin_count as f64);
    let mut bins = vec![0u64; bin_count];
    for &v in ttc_values {
        let idx = (((v - min_v) / step).floor() as usize).min(bin_count - 1);
        bins[idx] += 1;
    }
    let histogram_bins = (0..bin_count)
        .map(|i| {
            let lo = min_v + (i as f64) * step;
            let hi = lo + step;
            (lo, hi, bins[i])
        })
        .collect();

    TimeToConvertStats {
        converted_count: ttc_values.len() as u64,
        avg_ms: avg,
        p50_ms: p50,
        p90_ms: p90,
        p99_ms: p99,
        histogram_bins,
    }
}
