use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use chrono::DateTime;
use pulse_identity::IdentitySnapshot;
use pulse_storage::{ProjectStorage, StorageSnapshot};
use roaring::RoaringBitmap;

use crate::ast::{RetentionCohortRow, RetentionMode, RetentionQuery, RetentionResult, TimeRange};
use crate::cache::SegmentBlockCache;
use crate::catalog::VirtualCatalog;
use crate::scanner::{
    load_project_chunks, resolve_event_target, resolve_global_cohort_bitmap,
    row_matches_predicates, Result,
};

pub struct RetentionEngine;

impl RetentionEngine {
    pub async fn execute(
        query: &RetentionQuery,
        ps: &ProjectStorage,
        snap: &StorageSnapshot,
        id_snap: &IdentitySnapshot,
        cache: &SegmentBlockCache,
        catalog: &VirtualCatalog,
    ) -> Result<RetentionResult> {
        let t0 = Instant::now();
        let num_periods = query.num_periods.clamp(1, 60);
        let step_ms = query.unit.step_ms();

        let scan_range = TimeRange {
            start_ts_ms: query.time_range.start_ts_ms,
            end_ts_ms: query
                .time_range
                .end_ts_ms
                .saturating_add((num_periods as i64 + 1) * step_ms),
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

        let dict = ps.dictionary();
        let start_tgt = resolve_event_target(&query.start_event, dict, catalog);
        let return_tgt = resolve_event_target(&query.return_event, dict, catalog);

        let mut props_needed: HashSet<String> = query
            .global_filter
            .predicates
            .iter()
            .map(|p| p.prop_name.clone())
            .collect();
        for p in start_tgt.required_props() {
            props_needed.insert(p);
        }
        for p in return_tgt.required_props() {
            props_needed.insert(p);
        }
        let props_vec: Vec<String> = props_needed.into_iter().collect();

        let bloom = start_tgt.bloom_mask_64 | return_tgt.bloom_mask_64;
        let chunks = load_project_chunks(
            ps,
            snap,
            cache,
            catalog,
            Some(scan_range),
            &props_vec,
            (bloom != 0).then_some(bloom),
        )
        .await?;

        // 1. Track each canonical user's earliest qualifying `start_event` inside `query.time_range`
        // 2. Track exact `RoaringBitmap` of users performing `return_event` in every aligned interval bucket
        let mut first_start_ts: HashMap<u32, i64> = HashMap::new();
        let mut return_by_bucket: BTreeMap<i64, RoaringBitmap> = BTreeMap::new();

        for chunk in &chunks {
            let ta = &chunk.tier_a;
            for r in 0..chunk.len() {
                let ts = ta.event_ts_ms[r];
                if !scan_range.contains(ts) {
                    continue;
                }
                let eid = ta.event_id[r];
                let is_start = start_tgt.matches_id(eid);
                let is_ret = return_tgt.matches_id(eid);
                if !is_start && !is_ret {
                    continue;
                }

                let canon_uid = id_snap.resolve(ta.uid_internal[r]);
                if let Some(ec) = &eligible_cohort {
                    if !ec.contains(canon_uid) {
                        continue;
                    }
                }
                if !row_matches_predicates(chunk, r, &query.global_filter.predicates) {
                    continue;
                }

                if is_start
                    && query.time_range.contains(ts)
                    && start_tgt
                        .extra_filter
                        .as_ref()
                        .is_none_or(|ef| row_matches_predicates(chunk, r, std::slice::from_ref(ef)))
                {
                    let entry = first_start_ts.entry(canon_uid).or_insert(ts);
                    if ts < *entry {
                        *entry = ts;
                    }
                }

                if is_ret
                    && return_tgt
                        .extra_filter
                        .as_ref()
                        .is_none_or(|ef| row_matches_predicates(chunk, r, std::slice::from_ref(ef)))
                {
                    let b_ts = query.unit.align_floor(ts);
                    return_by_bucket.entry(b_ts).or_default().insert(canon_uid);
                }
            }
        }

        // Group users into cohorts by `unit.align_floor(first_start_ts)`
        let mut cohorts_map: BTreeMap<i64, RoaringBitmap> = BTreeMap::new();
        for (uid, start_ts) in first_start_ts {
            let c_ts = query.unit.align_floor(start_ts);
            cohorts_map.entry(c_ts).or_default().insert(uid);
        }

        let mut rows = Vec::with_capacity(cohorts_map.len());
        for (cohort_ts, cohort_bm) in cohorts_map {
            let cohort_size = cohort_bm.len();
            let label = DateTime::from_timestamp_millis(cohort_ts)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| cohort_ts.to_string());

            let retained_counts: Vec<u64> = match query.mode {
                RetentionMode::NDay => {
                    let mut counts = Vec::with_capacity(num_periods + 1);
                    counts.push(cohort_size);
                    for k in 1..=num_periods {
                        let target_ts = cohort_ts + (k as i64) * step_ms;
                        let cnt = if let Some(ret_bm) = return_by_bucket.get(&target_ts) {
                            (&cohort_bm & ret_bm).len()
                        } else {
                            0
                        };
                        counts.push(cnt);
                    }
                    counts
                }
                RetentionMode::Unbounded => {
                    let mut counts = vec![0u64; num_periods + 1];
                    counts[0] = cohort_size;
                    let mut suffix_union = RoaringBitmap::new();
                    for k in (1..=num_periods).rev() {
                        let target_ts = cohort_ts + (k as i64) * step_ms;
                        if let Some(ret_bm) = return_by_bucket.get(&target_ts) {
                            suffix_union |= ret_bm;
                        }
                        counts[k] = (&cohort_bm & &suffix_union).len();
                    }
                    counts
                }
                RetentionMode::FrequencyStickiness => {
                    // Count distinct active periods in `[0..=num_periods]` per user in `cohort_bm`
                    let mut freq_per_user: HashMap<u32, usize> = HashMap::new();
                    for k in 0..=num_periods {
                        let target_ts = cohort_ts + (k as i64) * step_ms;
                        if let Some(ret_bm) = return_by_bucket.get(&target_ts) {
                            let active = &cohort_bm & ret_bm;
                            for uid in active.iter() {
                                *freq_per_user.entry(uid).or_insert(0) += 1;
                            }
                        }
                    }
                    let mut counts = Vec::with_capacity(num_periods + 1);
                    for min_intervals in 1..=(num_periods + 1) {
                        let c = freq_per_user
                            .values()
                            .filter(|&&f| f >= min_intervals)
                            .count() as u64;
                        counts.push(c);
                    }
                    counts
                }
            };

            let retained_pcts: Vec<f64> = retained_counts
                .iter()
                .map(|&c| {
                    if cohort_size == 0 {
                        0.0
                    } else {
                        (c as f64 / cohort_size as f64) * 100.0
                    }
                })
                .collect();

            rows.push(RetentionCohortRow {
                cohort_start_ts_ms: cohort_ts,
                cohort_label: label,
                cohort_size,
                retained_counts,
                retained_pcts,
            });
        }

        Ok(RetentionResult {
            snapshot_epoch: snap.epoch(),
            exec_time_us: t0.elapsed().as_micros() as u64,
            mode: query.mode,
            cohorts: rows,
        })
    }
}
