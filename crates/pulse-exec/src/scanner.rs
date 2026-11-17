use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use futures::future::try_join_all;
use pulse_identity::IdentitySnapshot;
use pulse_storage::{
    ProjectDictionary, ProjectStorage, SegmentMeta, StorageError, StorageSnapshot, StoredRow,
};
use pulse_wire::PropValue;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use thiserror::Error;

use crate::ast::{
    BehavioralCohortDef, GlobalFilter, PropFilterOp, PropPredicate, TimeInterval, TimeRange,
};
use crate::cache::{SegmentBlockCache, TierAColumns};
use crate::catalog::{evaluate_custom_property, VirtualCatalog};

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("identity error: {0}")]
    Identity(#[from] pulse_identity::IdentityError),

    #[error("expression evaluation error: {0}")]
    Expression(String),

    #[error("invalid query parameter: {0}")]
    InvalidQuery(String),

    #[error("query execution exceeded deadline of {0}ms")]
    QueryTimeout(u64),
}

pub type Result<T> = std::result::Result<T, ExecError>;

/// Runs CPU-intensive Rayon parallel reductions off the async cooperative scheduler
/// when executing on a multi-threaded Tokio runtime, preventing ingest I/O starvation.
#[inline]
pub fn run_rayon_compute<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

/// Uniform columnar batch representing either an L1 Vortex segment or an L0 `MemBucket` snapshot.
pub struct DataChunk {
    pub bucket_id: u8,
    pub tier_a: Arc<TierAColumns>,
    pub prop_cols: HashMap<String, Arc<Vec<PropValue>>>,
    /// Optional L0 raw rows (None for L1 segments).
    l0_rows: Option<Vec<StoredRow>>,
    segment_meta: Option<SegmentMeta>,
}

impl DataChunk {
    #[inline]
    pub fn len(&self) -> usize {
        self.tier_a.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tier_a.is_empty()
    }

    pub fn session_id_for_row(&self, row_idx: usize) -> String {
        if let Some(rows) = &self.l0_rows {
            return rows
                .get(row_idx)
                .map(|r| r.session_id.clone())
                .unwrap_or_default();
        }
        // Binary search or scan SessionIndex prefix ranges
        let sidx = &self.tier_a.session_index;
        let shash = self.tier_a.session_hash[row_idx];
        let raw_uid = self.tier_a.uid_internal[row_idx];
        for hdr in &sidx.headers {
            if hdr.raw_uid == raw_uid && hdr.session_hash == shash {
                return sidx.session_id(hdr).to_string();
            }
        }
        format!("sess_{shash:016x}")
    }
}

/// Resolves an event name (either physical or `CustomEventDef` virtual union) into physical `(event_id, optional_predicate)` pairs.
#[derive(Debug, Clone)]
pub struct ResolvedEventTarget {
    pub event_ids: Vec<u16>,
    pub bloom_mask_64: u64,
    pub extra_filter: Option<PropPredicate>,
}

impl ResolvedEventTarget {
    #[inline]
    pub fn matches_id(&self, event_id: u16) -> bool {
        self.event_ids.contains(&event_id)
    }

    pub fn required_props(&self) -> Vec<String> {
        match &self.extra_filter {
            Some(p) => vec![p.prop_name.clone()],
            None => Vec::new(),
        }
    }
}

pub fn resolve_event_target(
    name: &str,
    dict: &ProjectDictionary,
    catalog: &VirtualCatalog,
) -> ResolvedEventTarget {
    if let Some(ce) = catalog.custom_events.get(name) {
        let mut ids = Vec::new();
        let mut mask = 0u64;
        for src in &ce.source_events {
            if let Some(id) = dict.lookup_event(src) {
                ids.push(id);
                mask |= pulse_storage::SessionHeader::event_bit(id);
            }
        }
        return ResolvedEventTarget {
            event_ids: ids,
            bloom_mask_64: mask,
            extra_filter: ce.filter.clone(),
        };
    }

    if let Some(id) = dict.lookup_event(name) {
        ResolvedEventTarget {
            event_ids: vec![id],
            bloom_mask_64: pulse_storage::SessionHeader::event_bit(id),
            extra_filter: None,
        }
    } else {
        ResolvedEventTarget {
            event_ids: Vec::new(),
            bloom_mask_64: 0,
            extra_filter: None,
        }
    }
}

pub fn ts_to_date_floor(ts_ms: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
}

/// Loads all L1 + L0 `DataChunk`s intersecting `time_range` (or full history when `None`),
/// materializing ONLY the requested property columns (including Custom Properties via DataFusion and Lookup Tables!).
pub async fn load_project_chunks(
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
    time_range: Option<TimeRange>,
    requested_props: &[String],
    event_bloom_hint: Option<u64>,
) -> Result<Vec<DataChunk>> {
    load_project_chunks_with_id(
        ps,
        snap,
        cache,
        catalog,
        time_range,
        requested_props,
        event_bloom_hint,
        None,
    )
    .await
}

/// Variant of `load_project_chunks` carrying `id_snap` so user profile properties (`user.*`)
/// and DPDP erasure tombstones resolve consistently across pre-login and post-login events (`Gaps 1, 4`).
#[allow(clippy::too_many_arguments)]
pub async fn load_project_chunks_with_id(
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
    time_range: Option<TimeRange>,
    requested_props: &[String],
    event_bloom_hint: Option<u64>,
    id_snap: Option<&IdentitySnapshot>,
) -> Result<Vec<DataChunk>> {
    let dict = ps.dictionary();
    let deleted_uids = catalog.deleted_uids_snapshot();

    // Expand requested properties to include dependencies of Custom Properties & Lookup Tables
    let mut base_props_needed: HashSet<String> = HashSet::new();
    let mut final_props_set: HashSet<String> = HashSet::new();

    for p in requested_props {
        final_props_set.insert(p.clone());
        if p.starts_with("user.") || p.starts_with("$cohort:") {
            continue;
        }
        // Physical properties ALWAYS take precedence over virtual/lookup columns (`P2-09j`)!
        if dict.lookup_prop_key(p).is_some() {
            base_props_needed.insert(p.clone());
        } else if let Some(cp) = catalog.custom_properties.get(p) {
            for dep in cp.referenced_columns() {
                if !dep.starts_with("user.") {
                    base_props_needed.insert(dep);
                }
            }
        } else if let Some((lt, _)) = catalog.resolve_lookup_column(p) {
            base_props_needed.insert(lt.join_prop.clone());
        } else if !catalog.has_user_prop(p) {
            base_props_needed.insert(p.clone());
        }
    }

    let (start_date, end_date) = match time_range {
        Some(tr) => (
            ts_to_date_floor(tr.start_ts_ms),
            ts_to_date_floor(tr.end_ts_ms),
        ),
        None => (
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2100, 1, 1).unwrap(),
        ),
    };

    let segments = snap.segments_for_range(start_date, end_date, None);
    let filtered_segments: Vec<SegmentMeta> = segments
        .into_iter()
        .filter(|seg| {
            if let Some(tr) = time_range {
                if seg.footer.max_ts_ms < tr.start_ts_ms || seg.footer.min_ts_ms > tr.end_ts_ms {
                    return false;
                }
            }
            if let Some(mask) = event_bloom_hint {
                if mask != 0 && (seg.footer.segment_event_bloom_64 & mask) == 0 {
                    return false;
                }
            }
            true
        })
        .collect();

    let base_props_vec: Vec<String> = base_props_needed.into_iter().collect();
    let final_props_vec: Vec<String> = final_props_set.into_iter().collect();

    // Parallel async fetch of all matching L1 Vortex segments through `foyer` SegmentBlockCache
    let futs = filtered_segments.iter().map(|seg| {
        let base_props_ref = &base_props_vec;
        let final_props_ref = &final_props_vec;
        let deleted_ref = &deleted_uids;
        async move {
            let mut tier_a = cache.get_tier_a(seg).await?;
            let orig_n = tier_a.len();
            let mut base_map: HashMap<String, Arc<Vec<PropValue>>> = HashMap::new();

            for bp in base_props_ref {
                let col = if let Some(pk) = dict.lookup_prop_key(bp) {
                    cache.get_property_column(seg, pk).await?
                } else {
                    Arc::new(vec![PropValue::Null; orig_n])
                };
                base_map.insert(bp.clone(), col);
            }

            let mut seg_meta_opt = Some(seg.clone());
            if !deleted_ref.is_empty()
                && tier_a.uid_internal.iter().any(|&u| {
                    deleted_ref.contains(u)
                        || id_snap.is_some_and(|s| deleted_ref.contains(s.resolve(u)))
                })
            {
                let keep_idx: Vec<usize> = (0..orig_n)
                    .filter(|&r| {
                        let u = tier_a.uid_internal[r];
                        !(deleted_ref.contains(u)
                            || id_snap.is_some_and(|s| deleted_ref.contains(s.resolve(u))))
                    })
                    .collect();
                let mut f_uid = Vec::with_capacity(keep_idx.len());
                let mut f_sh = Vec::with_capacity(keep_idx.len());
                let mut f_ev = Vec::with_capacity(keep_idx.len());
                let mut f_ts = Vec::with_capacity(keep_idx.len());
                let mut f_rkb = Vec::with_capacity(keep_idx.len());
                for &idx in &keep_idx {
                    f_uid.push(tier_a.uid_internal[idx]);
                    f_sh.push(tier_a.session_hash[idx]);
                    f_ev.push(tier_a.event_id[idx]);
                    f_ts.push(tier_a.event_ts_ms[idx]);
                    f_rkb.push(tier_a.replay_offset_kb[idx]);
                }
                tier_a = Arc::new(TierAColumns {
                    uid_internal: f_uid,
                    session_hash: f_sh,
                    event_id: f_ev,
                    event_ts_ms: f_ts,
                    replay_offset_kb: f_rkb,
                    session_index: tier_a.session_index.clone(),
                });
                for col in base_map.values_mut() {
                    let compacted: Vec<PropValue> =
                        keep_idx.iter().map(|&i| col[i].clone()).collect();
                    *col = Arc::new(compacted);
                }
                seg_meta_opt = None;
            }

            let n = tier_a.len();
            let prop_cols = materialize_virtual_columns(
                n,
                &tier_a.uid_internal,
                &base_map,
                final_props_ref,
                catalog,
                id_snap,
            )?;

            Ok::<DataChunk, ExecError>(DataChunk {
                bucket_id: seg.bucket_id,
                tier_a,
                prop_cols,
                l0_rows: None,
                segment_meta: seg_meta_opt,
            })
        }
    });

    let mut chunks = try_join_all(futs).await?;

    // Append L0 hot `MemBucket` rows per bucket within MVCC snapshot watermark
    for b in 0..ps.config().num_buckets {
        let bucket_id = b as u8;
        let l0_rows: Vec<StoredRow> = ps
            .snapshot_l0_bucket(snap, bucket_id)
            .into_iter()
            .filter(|r| {
                time_range.is_none_or(|tr| tr.contains(r.event_ts_ms))
                    && (deleted_uids.is_empty()
                        || !(deleted_uids.contains(r.uid_internal)
                            || id_snap
                                .is_some_and(|s| deleted_uids.contains(s.resolve(r.uid_internal)))))
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
        let mut replay_offset_kb = Vec::with_capacity(n);

        for r in &l0_rows {
            uid_internal.push(r.uid_internal);
            session_hash.push(r.session_hash);
            event_id.push(r.event_id);
            event_ts_ms.push(r.event_ts_ms);
            replay_offset_kb.push(r.replay_offset_kb);
        }

        let mut base_map: HashMap<String, Arc<Vec<PropValue>>> = HashMap::new();
        for bp in &base_props_vec {
            if let Some(pk) = dict.lookup_prop_key(bp) {
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
                base_map.insert(bp.clone(), Arc::new(col));
            } else {
                base_map.insert(bp.clone(), Arc::new(vec![PropValue::Null; n]));
            }
        }

        let prop_cols = materialize_virtual_columns(
            n,
            &uid_internal,
            &base_map,
            &final_props_vec,
            catalog,
            id_snap,
        )?;
        let empty_sidx = Arc::new(pulse_storage::SegmentSessionIndex {
            headers: Vec::new(),
            session_ids: Vec::new(),
        });

        chunks.push(DataChunk {
            bucket_id,
            tier_a: Arc::new(TierAColumns {
                uid_internal,
                session_hash,
                event_id,
                event_ts_ms,
                replay_offset_kb,
                session_index: empty_sidx,
            }),
            prop_cols,
            l0_rows: Some(l0_rows),
            segment_meta: None,
        });
    }

    Ok(chunks)
}

fn materialize_virtual_columns(
    num_rows: usize,
    uid_internal: &[u32],
    base_map: &HashMap<String, Arc<Vec<PropValue>>>,
    final_props: &[String],
    catalog: &VirtualCatalog,
    id_snap: Option<&IdentitySnapshot>,
) -> Result<HashMap<String, Arc<Vec<PropValue>>>> {
    let mut out: HashMap<String, Arc<Vec<PropValue>>> = HashMap::with_capacity(final_props.len());
    for p in final_props {
        if p.starts_with("user.") {
            let vals = catalog.resolve_user_prop_column(uid_internal, id_snap, p);
            out.insert(p.clone(), Arc::new(vals));
        // Physical property in `base_map` ALWAYS wins over custom/lookup columns (`P2-09j`)!
        } else if let Some(col) = base_map.get(p) {
            if catalog.has_user_prop(p) && col.iter().any(|v| matches!(v, PropValue::Null)) {
                let user_col = catalog.resolve_user_prop_column(uid_internal, id_snap, p);
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
            let vals = catalog.resolve_user_prop_column(uid_internal, id_snap, p);
            out.insert(p.clone(), Arc::new(vals));
        } else {
            out.insert(p.clone(), Arc::new(vec![PropValue::Null; num_rows]));
        }
    }
    Ok(out)
}

/// Evaluates a row-level `PropPredicate` slice against `chunk` at `row_idx`, using Tier-B Inverted `RoaringBitmap`
/// fast path when precomputed bitmaps are provided.
#[inline]
pub fn row_matches_predicates(chunk: &DataChunk, row_idx: usize, preds: &[PropPredicate]) -> bool {
    for pred in preds {
        let val = chunk
            .prop_cols
            .get(&pred.prop_name)
            .and_then(|c| c.get(row_idx));
        if !pred.matches(val) {
            return false;
        }
    }
    true
}

/// Computes segment-level `RoaringBitmap` row mask ONLY when the Tier B inverted index is strictly
/// complete AND unmixed for the predicate's exact value type (`P1-15`, `P2-01`).
pub async fn precompute_segment_row_mask(
    chunk: &DataChunk,
    preds: &[PropPredicate],
    dict: &ProjectDictionary,
    catalog: &VirtualCatalog,
    cache: &SegmentBlockCache,
) -> Result<Option<RoaringBitmap>> {
    let Some(meta) = &chunk.segment_meta else {
        return Ok(None);
    };
    if preds.is_empty() {
        return Ok(None);
    }

    let mut combined: Option<RoaringBitmap> = None;
    for pred in preds {
        let PropFilterOp::Eq(target) = &pred.filter else {
            continue;
        };
        // Skip physical index if property is not physical
        let Some(pk) = dict.lookup_prop_key(&pred.prop_name) else {
            let _ = catalog;
            continue;
        };
        let Some(entry) = meta
            .footer
            .tier_b_columns
            .iter()
            .find(|c| c.prop_key_id == pk)
        else {
            continue;
        };

        let bm = match target {
            PropValue::Bool(b) => {
                if !entry.bool_indexed || entry.has_str || entry.has_i64 || entry.has_f64 {
                    continue;
                }
                let inv = cache.get_inverted_index(meta).await?;
                inv.get_bool(pk, *b).cloned().unwrap_or_default()
            }
            PropValue::Str(s) => {
                if !entry.str_indexed_complete || entry.has_i64 || entry.has_f64 || entry.has_bool {
                    continue;
                }
                let inv = cache.get_inverted_index(meta).await?;
                inv.get_str(pk, s).cloned().unwrap_or_default()
            }
            _ => continue,
        };

        combined = Some(match combined {
            Some(mut prev) => {
                prev &= &bm;
                prev
            }
            None => bm,
        });
    }
    Ok(combined)
}

pub const MAX_COHORT_DEPTH: usize = 32;

/// Cached fast path for aligning millisecond timestamps to timezone-aware `TimeInterval` boundaries.
#[inline]
pub(crate) fn align_floor_tz_cached(
    ts: i64,
    unit: TimeInterval,
    tz: chrono_tz::Tz,
    cache: &mut Option<(i64, i64)>,
) -> i64 {
    if let Some((b_start, b_next)) = *cache {
        if ts >= b_start && ts < b_next {
            return b_start;
        }
    }
    let b_start = unit.align_floor_tz(ts, tz);
    let b_next = unit.next_bucket_tz(b_start, tz).unwrap_or(i64::MAX);
    *cache = Some((b_start, b_next));
    b_start
}

/// Validates synchronously at save/preview time that a `BehavioralCohortDef` contains no reference cycles
/// and does not exceed `MAX_COHORT_DEPTH` (`P0-09`).
pub fn validate_cohort_acyclic(
    self_id: Option<&str>,
    def: &BehavioralCohortDef,
    catalog: &VirtualCatalog,
) -> Result<()> {
    fn walk(
        node: &BehavioralCohortDef,
        catalog: &VirtualCatalog,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_COHORT_DEPTH {
            return Err(ExecError::InvalidQuery(format!(
                "cohort nesting depth exceeds maximum of {MAX_COHORT_DEPTH}"
            )));
        }
        match node {
            BehavioralCohortDef::PerformedEvent { .. }
            | BehavioralCohortDef::NotPerformedEvent { .. }
            | BehavioralCohortDef::PropertyMatch { .. } => Ok(()),
            BehavioralCohortDef::CohortRef { cohort_id } => {
                if !visited.insert(cohort_id.clone()) {
                    return Err(ExecError::InvalidQuery(format!(
                        "cyclic cohort reference detected involving '{cohort_id}'"
                    )));
                }
                if let Some(target) = catalog.saved_cohorts.get(cohort_id) {
                    let target_def = target.clone();
                    drop(target);
                    walk(&target_def, catalog, visited, depth + 1)?;
                }
                visited.remove(cohort_id);
                Ok(())
            }
            BehavioralCohortDef::And { clauses } | BehavioralCohortDef::Or { clauses } => {
                for c in clauses {
                    walk(c, catalog, visited, depth + 1)?;
                }
                Ok(())
            }
        }
    }

    let mut visited = HashSet::new();
    if let Some(id) = self_id {
        visited.insert(id.to_string());
    }
    walk(def, catalog, &mut visited, 0)
}

/// Evaluates recursive `BehavioralCohortDef` into an exact `RoaringBitmap` of canonical `u32` user IDs!
pub fn evaluate_cohort<'a>(
    def: &'a BehavioralCohortDef,
    default_range: Option<TimeRange>,
    ps: &'a ProjectStorage,
    snap: &'a StorageSnapshot,
    id_snap: &'a IdentitySnapshot,
    cache: &'a SegmentBlockCache,
    catalog: &'a VirtualCatalog,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RoaringBitmap>> + Send + 'a>> {
    evaluate_cohort_bounded(
        def,
        default_range,
        ps,
        snap,
        id_snap,
        cache,
        catalog,
        HashSet::new(),
        0,
    )
}

#[allow(clippy::too_many_arguments)]
fn evaluate_cohort_bounded<'a>(
    def: &'a BehavioralCohortDef,
    default_range: Option<TimeRange>,
    ps: &'a ProjectStorage,
    snap: &'a StorageSnapshot,
    id_snap: &'a IdentitySnapshot,
    cache: &'a SegmentBlockCache,
    catalog: &'a VirtualCatalog,
    visited: HashSet<String>,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RoaringBitmap>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_COHORT_DEPTH {
            return Err(ExecError::InvalidQuery(format!(
                "cohort nesting depth exceeds maximum of {MAX_COHORT_DEPTH}"
            )));
        }
        match def {
            BehavioralCohortDef::PerformedEvent {
                event_name,
                min_count,
                time_range,
                where_clauses,
            } => {
                let target = resolve_event_target(event_name, ps.dictionary(), catalog);
                if target.event_ids.is_empty() {
                    return Ok(RoaringBitmap::new());
                }
                let eff_range = time_range.or(default_range);
                let mut props: Vec<String> =
                    where_clauses.iter().map(|p| p.prop_name.clone()).collect();
                props.extend(target.required_props());

                let chunks = load_project_chunks_with_id(
                    ps,
                    snap,
                    cache,
                    catalog,
                    eff_range,
                    &props,
                    Some(target.bloom_mask_64),
                    Some(id_snap),
                )
                .await?;

                let row_masks = try_join_all(chunks.iter().map(|chunk| {
                    precompute_segment_row_mask(
                        chunk,
                        where_clauses,
                        ps.dictionary(),
                        catalog,
                        cache,
                    )
                }))
                .await?;

                let counts: HashMap<u32, u32> = run_rayon_compute(|| {
                    chunks
                        .par_iter()
                        .zip(row_masks.par_iter())
                        .map(|(chunk, row_mask)| {
                            let mut local: HashMap<u32, u32> = HashMap::new();
                            for i in 0..chunk.len() {
                                if let Some(rm) = row_mask {
                                    if !rm.contains(i as u32) {
                                        continue;
                                    }
                                }
                                if let Some(tr) = eff_range {
                                    if !tr.contains(chunk.tier_a.event_ts_ms[i]) {
                                        continue;
                                    }
                                }
                                if !target.matches_id(chunk.tier_a.event_id[i]) {
                                    continue;
                                }
                                if let Some(ef) = &target.extra_filter {
                                    if !row_matches_predicates(chunk, i, std::slice::from_ref(ef)) {
                                        continue;
                                    }
                                }
                                if !row_matches_predicates(chunk, i, where_clauses) {
                                    continue;
                                }
                                let canon = id_snap.resolve(chunk.tier_a.uid_internal[i]);
                                *local.entry(canon).or_insert(0) += 1;
                            }
                            local
                        })
                        .reduce(HashMap::new, |mut acc, map| {
                            for (uid, cnt) in map {
                                *acc.entry(uid).or_insert(0) += cnt;
                            }
                            acc
                        })
                });

                let needed = (*min_count).max(1);
                let mut out = RoaringBitmap::new();
                for (uid, cnt) in counts {
                    if cnt >= needed {
                        out.insert(uid);
                    }
                }
                Ok(out)
            }
            BehavioralCohortDef::NotPerformedEvent {
                event_name,
                time_range,
            } => {
                let eff_range = time_range.or(default_range);
                // Universe of active users in window minus those who performed `event_name` >= 1 time
                let all_chunks = load_project_chunks_with_id(
                    ps,
                    snap,
                    cache,
                    catalog,
                    eff_range,
                    &[],
                    None,
                    Some(id_snap),
                )
                .await?;
                let mut universe = run_rayon_compute(|| {
                    all_chunks
                        .par_iter()
                        .map(|chunk| {
                            let mut local = RoaringBitmap::new();
                            for i in 0..chunk.len() {
                                if eff_range
                                    .is_none_or(|tr| tr.contains(chunk.tier_a.event_ts_ms[i]))
                                {
                                    local.insert(id_snap.resolve(chunk.tier_a.uid_internal[i]));
                                }
                            }
                            local
                        })
                        .reduce(RoaringBitmap::new, |mut a, b| {
                            a |= b;
                            a
                        })
                });
                let performed = evaluate_cohort_bounded(
                    &BehavioralCohortDef::PerformedEvent {
                        event_name: event_name.clone(),
                        min_count: 1,
                        time_range: eff_range,
                        where_clauses: Vec::new(),
                    },
                    eff_range,
                    ps,
                    snap,
                    id_snap,
                    cache,
                    catalog,
                    visited,
                    depth + 1,
                )
                .await?;
                universe -= performed;
                Ok(universe)
            }
            BehavioralCohortDef::PropertyMatch {
                predicate,
                time_range,
            } => {
                let eff_range = time_range.or(default_range);
                let chunks = load_project_chunks_with_id(
                    ps,
                    snap,
                    cache,
                    catalog,
                    eff_range,
                    std::slice::from_ref(&predicate.prop_name),
                    None,
                    Some(id_snap),
                )
                .await?;
                let out = run_rayon_compute(|| {
                    chunks
                        .par_iter()
                        .map(|chunk| {
                            let mut local = RoaringBitmap::new();
                            for i in 0..chunk.len() {
                                if eff_range
                                    .is_none_or(|tr| tr.contains(chunk.tier_a.event_ts_ms[i]))
                                    && row_matches_predicates(
                                        chunk,
                                        i,
                                        std::slice::from_ref(predicate),
                                    )
                                {
                                    local.insert(id_snap.resolve(chunk.tier_a.uid_internal[i]));
                                }
                            }
                            local
                        })
                        .reduce(RoaringBitmap::new, |mut a, b| {
                            a |= b;
                            a
                        })
                });
                Ok(out)
            }
            BehavioralCohortDef::CohortRef { cohort_id } => {
                let mut next_visited = visited;
                if !next_visited.insert(cohort_id.clone()) {
                    return Err(ExecError::InvalidQuery(format!(
                        "cyclic cohort reference detected involving '{cohort_id}'"
                    )));
                }
                let Some(saved) = catalog.saved_cohorts.get(cohort_id) else {
                    return Err(ExecError::InvalidQuery(format!(
                        "unknown saved cohort_id: {cohort_id}"
                    )));
                };
                let cloned = saved.clone();
                drop(saved);
                evaluate_cohort_bounded(
                    &cloned,
                    default_range,
                    ps,
                    snap,
                    id_snap,
                    cache,
                    catalog,
                    next_visited,
                    depth + 1,
                )
                .await
            }
            BehavioralCohortDef::And { clauses } => {
                let mut acc: Option<RoaringBitmap> = None;
                for c in clauses {
                    let bm = evaluate_cohort_bounded(
                        c,
                        default_range,
                        ps,
                        snap,
                        id_snap,
                        cache,
                        catalog,
                        visited.clone(),
                        depth + 1,
                    )
                    .await?;
                    acc = Some(match acc {
                        Some(mut prev) => {
                            prev &= bm;
                            prev
                        }
                        None => bm,
                    });
                }
                Ok(acc.unwrap_or_default())
            }
            BehavioralCohortDef::Or { clauses } => {
                let mut acc = RoaringBitmap::new();
                for c in clauses {
                    let bm = evaluate_cohort_bounded(
                        c,
                        default_range,
                        ps,
                        snap,
                        id_snap,
                        cache,
                        catalog,
                        visited.clone(),
                        depth + 1,
                    )
                    .await?;
                    acc |= bm;
                }
                Ok(acc)
            }
        }
    })
}

pub async fn resolve_global_cohort_bitmap(
    global_filter: &GlobalFilter,
    default_range: TimeRange,
    ps: &ProjectStorage,
    snap: &StorageSnapshot,
    id_snap: &IdentitySnapshot,
    cache: &SegmentBlockCache,
    catalog: &VirtualCatalog,
) -> Result<Option<RoaringBitmap>> {
    if let Some(cohort_def) = &global_filter.cohort {
        let bm = evaluate_cohort(
            cohort_def,
            Some(default_range),
            ps,
            snap,
            id_snap,
            cache,
            catalog,
        )
        .await?;
        Ok(Some(bm))
    } else {
        Ok(None)
    }
}
