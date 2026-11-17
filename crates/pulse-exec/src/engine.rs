use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::NaiveDate;
use pulse_storage::{
    SessionHeader, StorageEngine, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};
use pulse_wire::{mask_user_identifier, PropValue};
use serde::Serialize;

use crate::ast::{
    BehavioralCohortDef, FlowsQuery, FlowsResult, FunnelQuery, FunnelResult, InsightsQuery,
    InsightsResult, RetentionQuery, RetentionResult, SessionSearchQuery, SessionSearchResult,
    SessionSummaryItem, TimeRange,
};
use crate::cache::SegmentBlockCache;
use crate::catalog::CatalogRegistry;
use crate::flows::FlowsEngine;
use crate::funnel::FunnelEngine;
use crate::insights::InsightsEngine;
use crate::retention::RetentionEngine;
use crate::scanner::{evaluate_cohort, resolve_event_target, ts_to_date_floor, ExecError, Result};

#[derive(Debug, Clone, Serialize)]
pub struct ExportedEventRow {
    pub event_ts_ms: i64,
    pub event_name: String,
    pub distinct_id: String,
    pub canonical_uid: u32,
    pub session_id: String,
    pub props: std::collections::BTreeMap<String, PropValue>,
}

const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;

struct RankedSession(SessionSummaryItem);

impl PartialEq for RankedSession {
    fn eq(&self, other: &Self) -> bool {
        self.0.end_ts_ms == other.0.end_ts_ms && self.0.session_id == other.0.session_id
    }
}

impl Eq for RankedSession {}

impl PartialOrd for RankedSession {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedSession {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .end_ts_ms
            .cmp(&other.0.end_ts_ms)
            .then_with(|| other.0.session_id.cmp(&self.0.session_id))
    }
}

fn push_bounded_session(
    heap: &mut BinaryHeap<Reverse<RankedSession>>,
    item: SessionSummaryItem,
    limit: usize,
) {
    if limit == 0 {
        return;
    }
    if heap.len() < limit {
        heap.push(Reverse(RankedSession(item)));
    } else if let Some(min_top) = heap.peek() {
        let candidate = RankedSession(item);
        if candidate > min_top.0 {
            heap.pop();
            heap.push(Reverse(candidate));
        }
    }
}

/// Unified analytical query engine backed by `foyer` hybrid block cache, Vortex L1 segments, and L0 `MemBucket`s.
pub struct QueryEngine {
    storage: Arc<StorageEngine>,
    cache: Arc<SegmentBlockCache>,
    catalogs: Arc<CatalogRegistry>,
    query_timeout_ms: AtomicU64,
    last_query_latency_us: AtomicU64,
    queries_executed: AtomicU64,
}

impl QueryEngine {
    pub async fn new(
        storage: Arc<StorageEngine>,
        memory_cache_bytes: usize,
        disk_cache_dir: Option<&Path>,
        disk_cache_bytes: usize,
        catalogs: Arc<CatalogRegistry>,
    ) -> Result<Arc<Self>> {
        let cache = SegmentBlockCache::open_with_store(
            storage.segment_reader().clone(),
            Some(storage.encrypted_store().clone()),
            memory_cache_bytes,
            disk_cache_dir,
            disk_cache_bytes,
        )
        .await
        .map_err(ExecError::Expression)?;

        let timeout_ms = std::env::var("PULSE_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .unwrap_or(DEFAULT_QUERY_TIMEOUT_MS);

        Ok(Arc::new(Self {
            storage,
            cache,
            catalogs,
            query_timeout_ms: AtomicU64::new(timeout_ms),
            last_query_latency_us: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
        }))
    }

    pub fn set_query_timeout(&self, timeout: Duration) {
        let ms = (timeout.as_millis() as u64).max(1);
        self.query_timeout_ms.store(ms, Ordering::Relaxed);
    }

    pub fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.query_timeout_ms.load(Ordering::Relaxed))
    }

    async fn with_deadline<T, F>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let timeout_ms = self.query_timeout_ms.load(Ordering::Relaxed);
        tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
            .await
            .map_err(|_| ExecError::QueryTimeout(timeout_ms))?
    }

    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    pub fn cache(&self) -> &Arc<SegmentBlockCache> {
        &self.cache
    }

    pub fn catalogs(&self) -> &Arc<CatalogRegistry> {
        &self.catalogs
    }

    pub fn last_query_latency_us(&self) -> u64 {
        self.last_query_latency_us.load(Ordering::Relaxed)
    }

    pub fn queries_executed(&self) -> u64 {
        self.queries_executed.load(Ordering::Relaxed)
    }

    /// Pre-loads segment headers and commonly queried property columns into the cache (`EN-3`).
    pub async fn warm_project_cache(&self, project_id: &str) -> Result<usize> {
        let ps = self.storage.get_or_open_project(project_id).await?;
        let snap = ps.snapshot();
        let cat = self.catalogs.for_project(project_id);
        let warm_props: Vec<String> = ["city", "platform", "variant", "flag_key", "amount"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let chunks = crate::scanner::load_project_chunks(
            &ps,
            &snap,
            &self.cache,
            &cat,
            None,
            &warm_props,
            None,
        )
        .await?;
        Ok(chunks.len())
    }

    fn record_latency(&self, us: u64) {
        self.last_query_latency_us.store(us, Ordering::Relaxed);
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn query_insights(
        &self,
        project_id: &str,
        query: &InsightsQuery,
    ) -> Result<InsightsResult> {
        self.with_deadline(async {
            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);

            let res =
                InsightsEngine::execute(query, &ps, &snap, &id_snap, &self.cache, &cat).await?;
            self.record_latency(res.exec_time_us);
            Ok(res)
        })
        .await
    }

    pub async fn query_funnel(
        &self,
        project_id: &str,
        query: &FunnelQuery,
    ) -> Result<FunnelResult> {
        self.with_deadline(async {
            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);

            let res = FunnelEngine::execute(
                query,
                &ps,
                &snap,
                &id_graph,
                &id_snap,
                &self.cache,
                &cat,
                self.storage.replay_store(),
            )
            .await?;
            self.record_latency(res.exec_time_us);
            Ok(res)
        })
        .await
    }

    pub async fn query_retention(
        &self,
        project_id: &str,
        query: &RetentionQuery,
    ) -> Result<RetentionResult> {
        self.with_deadline(async {
            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);

            let res =
                RetentionEngine::execute(query, &ps, &snap, &id_snap, &self.cache, &cat).await?;
            self.record_latency(res.exec_time_us);
            Ok(res)
        })
        .await
    }

    pub async fn query_flows(&self, project_id: &str, query: &FlowsQuery) -> Result<FlowsResult> {
        self.with_deadline(async {
            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);

            let res = FlowsEngine::execute(query, &ps, &snap, &id_snap, &self.cache, &cat).await?;
            self.record_latency(res.exec_time_us);
            Ok(res)
        })
        .await
    }

    pub async fn preview_cohort_count(
        &self,
        project_id: &str,
        def: &BehavioralCohortDef,
        default_range: Option<TimeRange>,
    ) -> Result<(u64, u64)> {
        self.with_deadline(async {
            let t0 = Instant::now();
            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);

            let bm = evaluate_cohort(def, default_range, &ps, &snap, &id_snap, &self.cache, &cat)
                .await?;
            let us = t0.elapsed().as_micros() as u64;
            self.record_latency(us);
            Ok((bm.len(), us))
        })
        .await
    }

    /// Evaluates a saved cohort (`cohort_id`) and returns the sorted list of all external `distinct_id`s
    /// belonging to its canonical `u32` user roots (`P2-02` Edge Flag cohort targeting).
    pub async fn evaluate_saved_cohort_external_ids(
        &self,
        project_id: &str,
        cohort_id: &str,
    ) -> Result<Vec<String>> {
        self.with_deadline(async {
            let cat = self.catalogs.for_project(project_id);
            let Some(def_ref) = cat.saved_cohorts.get(cohort_id) else {
                return Ok(Vec::new());
            };
            let def = def_ref.clone();
            drop(def_ref);

            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();

            let bm = evaluate_cohort(&def, None, &ps, &snap, &id_snap, &self.cache, &cat).await?;
            Ok(id_graph.external_ids_for_canonical_roots(&bm))
        })
        .await
    }

    /// Evaluates a saved cohort (`cohort_id`) and returns `(canonical_uid, masked_distinct_id)` pairs (`Gap 15`).
    pub async fn evaluate_saved_cohort_uids(
        &self,
        project_id: &str,
        cohort_id: &str,
    ) -> Result<Vec<(u32, String)>> {
        self.with_deadline(async {
            let cat = self.catalogs.for_project(project_id);
            let Some(def_ref) = cat.saved_cohorts.get(cohort_id) else {
                return Ok(Vec::new());
            };
            let def = def_ref.clone();
            drop(def_ref);

            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();

            let bm = evaluate_cohort(&def, None, &ps, &snap, &id_snap, &self.cache, &cat).await?;
            let deleted = cat.deleted_uids_snapshot();
            let mut out = Vec::new();
            for uid in bm.iter() {
                if deleted.contains(uid) {
                    continue;
                }
                let ext = id_graph
                    .external_id(uid)
                    .map(|raw| mask_user_identifier(&raw))
                    .unwrap_or_else(|| format!("uid_{uid}"));
                out.push((uid, ext));
            }
            Ok(out)
        })
        .await
    }

    /// Exports raw event rows (honoring DPDP erasure tombstones and PII masking) filtered optionally
    /// by `time_range` and/or `filter_canonical_uid` (`Gaps 1, 16`).
    pub async fn export_events(
        &self,
        project_id: &str,
        time_range: Option<TimeRange>,
        filter_canonical_uid: Option<u32>,
        limit: usize,
        mask_ids: bool,
    ) -> Result<Vec<ExportedEventRow>> {
        self.with_deadline(async {
            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);
            let dict = ps.dictionary();
            let id_to_name: HashMap<u16, String> = dict.all_events().into_iter().collect();

            let prop_names: Vec<String> = dict
                .all_prop_keys()
                .into_iter()
                .map(|(_, k)| k)
                .take(64)
                .collect();

            let chunks = crate::scanner::load_project_chunks_with_id(
                &ps,
                &snap,
                &self.cache,
                &cat,
                time_range,
                &prop_names,
                None,
                Some(&id_snap),
            )
            .await?;

            let cap = limit.clamp(1, 50_000);
            let mut rows = Vec::new();
            for chunk in &chunks {
                let ta = &chunk.tier_a;
                for r in 0..chunk.len() {
                    let ts = ta.event_ts_ms[r];
                    if let Some(tr) = time_range {
                        if !tr.contains(ts) {
                            continue;
                        }
                    }
                    let canon = id_snap.resolve(ta.uid_internal[r]);
                    if let Some(target_uid) = filter_canonical_uid {
                        if canon != target_uid {
                            continue;
                        }
                    }
                    let raw_did = id_graph
                        .external_id(canon)
                        .unwrap_or_else(|| format!("uid_{canon}"));
                    let distinct_id = if mask_ids {
                        mask_user_identifier(&raw_did)
                    } else {
                        raw_did
                    };
                    let event_name = id_to_name
                        .get(&ta.event_id[r])
                        .cloned()
                        .unwrap_or_else(|| format!("event_{}", ta.event_id[r]));
                    let session_id = chunk.session_id_for_row(r);

                    let mut props = std::collections::BTreeMap::new();
                    for (pk, col) in &chunk.prop_cols {
                        if let Some(pv) = col.get(r) {
                            if !matches!(pv, PropValue::Null) {
                                props.insert(pk.clone(), pv.clone());
                            }
                        }
                    }
                    rows.push(ExportedEventRow {
                        event_ts_ms: ts,
                        event_name,
                        distinct_id,
                        canonical_uid: canon,
                        session_id,
                        props,
                    });
                    if rows.len() >= cap && filter_canonical_uid.is_none() {
                        break;
                    }
                }
                if rows.len() >= cap && filter_canonical_uid.is_none() {
                    break;
                }
            }
            rows.sort_by(|a, b| b.event_ts_ms.cmp(&a.event_ts_ms));
            rows.truncate(cap);
            Ok(rows)
        })
        .await
    }

    /// Searches sessions leveraging `SessionIndex` 64-bit event Bloom bitmasks, frustration flags,
    /// and `limit` pushdown (`P3-02`) BEFORE reading any Tier A/B arrays on pruned segments!
    pub async fn search_sessions(
        &self,
        project_id: &str,
        query: &SessionSearchQuery,
    ) -> Result<SessionSearchResult> {
        self.with_deadline(async {
            let t0 = Instant::now();
            if let Some(tr) = query.time_range {
                if tr.start_ts_ms > tr.end_ts_ms {
                    return Err(ExecError::InvalidQuery(format!(
                        "invalid time_range: start_ts_ms ({}) > end_ts_ms ({})",
                        tr.start_ts_ms, tr.end_ts_ms
                    )));
                }
            }

            let ps = self.storage.get_or_open_project(project_id).await?;
            let snap = ps.snapshot();
            let id_graph = self
                .storage
                .identity_router()
                .get_or_open(project_id)
                .await?;
            let id_snap = id_graph.snapshot();
            let cat = self.catalogs.for_project(project_id);
            let dict = ps.dictionary();

            let id_to_name: HashMap<u16, String> = dict.all_events().into_iter().collect();
            let limit = query.limit.unwrap_or(50).clamp(1, 500);

            let mut required_flags = 0u16;
            if query.only_rage_tap {
                required_flags |= SESSION_FLAG_RAGE_TAP;
            }
            if query.only_dead_tap {
                required_flags |= SESSION_FLAG_DEAD_TAP;
            }
            if query.only_errors {
                required_flags |= SESSION_FLAG_ERROR;
            }
            // Recorded sessions by session-id hash, the same check funnels use for drop-off replays.
            let recorded = if query.only_with_replay {
                Some(self.storage.replay_store().recorded_hashes_for_project(project_id).await)
            } else {
                None
            };
            let is_recorded = |sid: &str| {
                recorded
                    .as_ref()
                    .is_none_or(|h| h.contains(&xxhash_rust::xxh3::xxh3_64(sid.as_bytes())))
            };

            let target = query
                .event_name
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|name| resolve_event_target(name, dict, &cat));

            let (start_date, end_date) = match query.time_range {
                Some(tr) => (
                    ts_to_date_floor(tr.start_ts_ms),
                    ts_to_date_floor(tr.end_ts_ms),
                ),
                None => (
                    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2100, 1, 1).unwrap(),
                ),
            };

            let mut total_sessions_inspected = 0usize;
            let mut sessions_pruned_by_bloom = 0usize;
            let mut top_heap: BinaryHeap<Reverse<RankedSession>> = BinaryHeap::with_capacity(limit);
            let deleted_uids = cat.deleted_uids_snapshot();

            // 1. Scan hot L0 `MemBucket` sessions first to establish an immediate recency floor for `limit` pushdown (`P3-02`)
            type L0SessionAcc = (u16, Vec<(i64, u16, u32)>);
            let mut l0_grouped: HashMap<(u32, String), L0SessionAcc> = HashMap::new();
            for b in 0..ps.config().num_buckets {
                let rows = ps.snapshot_l0_bucket(&snap, b as u8);
                for r in rows.iter() {
                    let canon_uid = id_snap.resolve(r.uid_internal);
                    if !deleted_uids.is_empty()
                        && (deleted_uids.contains(r.uid_internal)
                            || deleted_uids.contains(canon_uid))
                    {
                        continue;
                    }
                    let entry = l0_grouped
                        .entry((canon_uid, r.session_id.clone()))
                        .or_insert_with(|| (0, Vec::new()));
                    entry.0 |= r.session_flags;
                    entry
                        .1
                        .push((r.event_ts_ms, r.event_id, r.replay_offset_kb));
                }
            }

            for ((canon_uid, session_id), (flags, mut evs)) in l0_grouped {
                total_sessions_inspected += 1;
                let mut bloom = 0u64;
                for &(_, eid, _) in &evs {
                    bloom |= SessionHeader::event_bit(eid);
                }
                if (flags & required_flags) != required_flags || !is_recorded(&session_id) {
                    sessions_pruned_by_bloom += 1;
                    continue;
                }
                if let Some(tgt) = &target {
                    if tgt.event_ids.is_empty() || (bloom & tgt.bloom_mask_64) == 0 {
                        sessions_pruned_by_bloom += 1;
                        continue;
                    }
                    if !evs.iter().any(|&(_, eid, _)| tgt.matches_id(eid)) {
                        continue;
                    }
                }
                evs.sort_by_key(|(ts, _, _)| *ts);
                let start_ts = evs.first().map(|x| x.0).unwrap_or(0);
                let end_ts = evs.last().map(|x| x.0).unwrap_or(0);
                if let Some(tr) = query.time_range {
                    if end_ts < tr.start_ts_ms || start_ts > tr.end_ts_ms {
                        sessions_pruned_by_bloom += 1;
                        continue;
                    }
                }
                if top_heap.len() >= limit {
                    if let Some(min_top) = top_heap.peek() {
                        if end_ts < min_top.0 .0.end_ts_ms {
                            continue;
                        }
                    }
                }

                let raw_did = id_graph
                    .external_id(canon_uid)
                    .unwrap_or_else(|| format!("uid_{canon_uid}"));
                let distinct_id = pulse_wire::mask_user_identifier(&raw_did);
                let event_count = evs.len() as u32;
                let mapped_evs = evs
                    .into_iter()
                    .map(|(ts, eid, rkb)| {
                        let ename = id_to_name
                            .get(&eid)
                            .cloned()
                            .unwrap_or_else(|| format!("event_{eid}"));
                        (ts, ename, rkb)
                    })
                    .collect();

                push_bounded_session(
                    &mut top_heap,
                    SessionSummaryItem {
                        session_id,
                        canonical_uid: canon_uid,
                        distinct_id,
                        start_ts_ms: start_ts,
                        end_ts_ms: end_ts,
                        duration_ms: (end_ts - start_ts).max(0),
                        event_count,
                        has_rage_tap: (flags & SESSION_FLAG_RAGE_TAP) != 0,
                        has_dead_tap: (flags & SESSION_FLAG_DEAD_TAP) != 0,
                        has_error: (flags & SESSION_FLAG_ERROR) != 0,
                        has_replay: false,
                        events: mapped_evs,
                    },
                    limit,
                );
            }

            // 2. Scan L1 Vortex segments newest-first (`footer.max_ts_ms DESC`), pushing `limit` down (`P3-02`)
            let mut segs = snap.segments_for_range(start_date, end_date, None);
            segs.sort_by_key(|s| Reverse(s.footer.max_ts_ms));

            for seg in &segs {
                if top_heap.len() >= limit {
                    if let Some(min_top) = top_heap.peek() {
                        if seg.footer.max_ts_ms < min_top.0 .0.end_ts_ms {
                            // Segment's newest event is strictly older than our current k-th best session!
                            continue;
                        }
                    }
                }

                let sidx = self.cache.get_session_index(seg).await?;
                total_sessions_inspected += sidx.headers.len();

                let min_end_cutoff = if top_heap.len() >= limit {
                    top_heap.peek().map(|m| m.0 .0.end_ts_ms)
                } else {
                    None
                };

                let mut matched_ranges = sidx.matching_session_ranges(|hdr: &SessionHeader| {
                    if !deleted_uids.is_empty()
                        && (deleted_uids.contains(hdr.raw_uid)
                            || deleted_uids.contains(id_snap.resolve(hdr.raw_uid)))
                    {
                        return false;
                    }
                    if (hdr.flags & required_flags) != required_flags {
                        return false;
                    }
                    if let Some(tr) = query.time_range {
                        if hdr.end_ts_ms < tr.start_ts_ms || hdr.start_ts_ms > tr.end_ts_ms {
                            return false;
                        }
                    }
                    if let Some(cutoff) = min_end_cutoff {
                        if hdr.end_ts_ms < cutoff {
                            return false;
                        }
                    }
                    if let Some(tgt) = &target {
                        if tgt.event_ids.is_empty() || !hdr.may_contain_any_event(tgt.bloom_mask_64)
                        {
                            return false;
                        }
                    }
                    true
                });

                sessions_pruned_by_bloom += sidx.headers.len().saturating_sub(matched_ranges.len());
                if matched_ranges.is_empty() {
                    continue;
                }

                // Order matched sessions within segment newest-first so the cutoff tightens immediately
                matched_ranges.sort_by_key(|(_, _, hdr)| Reverse(hdr.end_ts_ms));

                // Read Tier A ONLY for segments that survived SessionIndex + limit pruning!
                let ta = self.cache.get_tier_a(seg).await?;
                for (r_start, r_end, hdr) in matched_ranges {
                    if top_heap.len() >= limit {
                        if let Some(min_top) = top_heap.peek() {
                            if hdr.end_ts_ms < min_top.0 .0.end_ts_ms {
                                break;
                            }
                        }
                    }
                    if !is_recorded(sidx.session_id(&hdr)) {
                        continue;
                    }
                    if let Some(tgt) = &target {
                        let actually_has = (r_start..r_end).any(|r| tgt.matches_id(ta.event_id[r]));
                        if !actually_has {
                            continue;
                        }
                    }

                    let canon_uid = id_snap.resolve(hdr.raw_uid);
                    let raw_did = id_graph
                        .external_id(canon_uid)
                        .unwrap_or_else(|| format!("uid_{canon_uid}"));
                    let distinct_id = pulse_wire::mask_user_identifier(&raw_did);
                    let ev_list: Vec<(i64, String, u32)> = (r_start..r_end)
                        .map(|r| {
                            let ename = id_to_name
                                .get(&ta.event_id[r])
                                .cloned()
                                .unwrap_or_else(|| format!("event_{}", ta.event_id[r]));
                            (ta.event_ts_ms[r], ename, ta.replay_offset_kb[r])
                        })
                        .collect();

                    push_bounded_session(
                        &mut top_heap,
                        SessionSummaryItem {
                            session_id: sidx.session_id(&hdr).to_string(),
                            canonical_uid: canon_uid,
                            distinct_id,
                            start_ts_ms: hdr.start_ts_ms,
                            end_ts_ms: hdr.end_ts_ms,
                            duration_ms: (hdr.end_ts_ms - hdr.start_ts_ms).max(0),
                            event_count: hdr.event_count,
                            has_rage_tap: (hdr.flags & SESSION_FLAG_RAGE_TAP) != 0,
                            has_dead_tap: (hdr.flags & SESSION_FLAG_DEAD_TAP) != 0,
                            has_error: (hdr.flags & SESSION_FLAG_ERROR) != 0,
                            has_replay: false,
                            events: ev_list,
                        },
                        limit,
                    );
                }
            }

            let mut sessions: Vec<SessionSummaryItem> =
                top_heap.into_iter().map(|Reverse(r)| r.0).collect();
            sessions.sort_by(|a, b| {
                b.end_ts_ms
                    .cmp(&a.end_ts_ms)
                    .then_with(|| a.session_id.cmp(&b.session_id))
            });
            let rstore = self.storage.replay_store();
            for s in &mut sessions {
                s.has_replay = rstore.has_session(project_id, &s.session_id).await;
            }

            let exec_time_us = t0.elapsed().as_micros() as u64;
            self.record_latency(exec_time_us);

            Ok(SessionSearchResult {
                snapshot_epoch: snap.epoch(),
                exec_time_us,
                total_sessions_inspected,
                sessions_pruned_by_bloom,
                sessions,
            })
        })
        .await
    }
}
