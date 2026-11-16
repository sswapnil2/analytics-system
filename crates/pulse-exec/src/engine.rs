use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::NaiveDate;
use pulse_storage::{
    SessionHeader, StorageEngine, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};

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

/// Unified analytical query engine backed by `foyer` hybrid block cache, Vortex L1 segments, and L0 `MemBucket`s.
pub struct QueryEngine {
    storage: Arc<StorageEngine>,
    cache: Arc<SegmentBlockCache>,
    catalogs: Arc<CatalogRegistry>,
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
        let cache = SegmentBlockCache::open(
            storage.segment_reader().clone(),
            memory_cache_bytes,
            disk_cache_dir,
            disk_cache_bytes,
        )
        .await
        .map_err(ExecError::Expression)?;

        Ok(Arc::new(Self {
            storage,
            cache,
            catalogs,
            last_query_latency_us: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
        }))
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

    fn record_latency(&self, us: u64) {
        self.last_query_latency_us.store(us, Ordering::Relaxed);
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn query_insights(
        &self,
        project_id: &str,
        query: &InsightsQuery,
    ) -> Result<InsightsResult> {
        let ps = self.storage.get_or_open_project(project_id).await?;
        let snap = ps.snapshot();
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;
        let id_snap = id_graph.snapshot();
        let cat = self.catalogs.for_project(project_id);

        let res = InsightsEngine::execute(query, &ps, &snap, &id_snap, &self.cache, &cat).await?;
        self.record_latency(res.exec_time_us);
        Ok(res)
    }

    pub async fn query_funnel(
        &self,
        project_id: &str,
        query: &FunnelQuery,
    ) -> Result<FunnelResult> {
        let ps = self.storage.get_or_open_project(project_id).await?;
        let snap = ps.snapshot();
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;
        let id_snap = id_graph.snapshot();
        let cat = self.catalogs.for_project(project_id);

        let res = FunnelEngine::execute(query, &ps, &snap, &id_graph, &id_snap, &self.cache, &cat)
            .await?;
        self.record_latency(res.exec_time_us);
        Ok(res)
    }

    pub async fn query_retention(
        &self,
        project_id: &str,
        query: &RetentionQuery,
    ) -> Result<RetentionResult> {
        let ps = self.storage.get_or_open_project(project_id).await?;
        let snap = ps.snapshot();
        let id_graph = self
            .storage
            .identity_router()
            .get_or_open(project_id)
            .await?;
        let id_snap = id_graph.snapshot();
        let cat = self.catalogs.for_project(project_id);

        let res = RetentionEngine::execute(query, &ps, &snap, &id_snap, &self.cache, &cat).await?;
        self.record_latency(res.exec_time_us);
        Ok(res)
    }

    pub async fn query_flows(&self, project_id: &str, query: &FlowsQuery) -> Result<FlowsResult> {
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
    }

    pub async fn preview_cohort_count(
        &self,
        project_id: &str,
        def: &BehavioralCohortDef,
        default_range: Option<TimeRange>,
    ) -> Result<(u64, u64)> {
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

        let bm =
            evaluate_cohort(def, default_range, &ps, &snap, &id_snap, &self.cache, &cat).await?;
        let us = t0.elapsed().as_micros() as u64;
        self.record_latency(us);
        Ok((bm.len(), us))
    }

    /// Evaluates a saved cohort (`cohort_id`) and returns the sorted list of all external `distinct_id`s
    /// belonging to its canonical `u32` user roots (`P2-02` Edge Flag cohort targeting).
    pub async fn evaluate_saved_cohort_external_ids(
        &self,
        project_id: &str,
        cohort_id: &str,
    ) -> Result<Vec<String>> {
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
    }

    /// Searches sessions leveraging `SessionIndex` 64-bit event Bloom bitmasks and frustration flags
    /// BEFORE reading any Tier A/B arrays on pruned segments!
    pub async fn search_sessions(
        &self,
        project_id: &str,
        query: &SessionSearchQuery,
    ) -> Result<SessionSearchResult> {
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
        let dict = ps.dictionary();

        let id_to_name: HashMap<u16, String> = dict.all_events().into_iter().collect();
        let limit = query.limit.unwrap_or(50).min(500);

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
        let mut sessions: Vec<SessionSummaryItem> = Vec::new();

        // 1. Scan L1 Vortex segments via SessionIndex footer byte-ranges
        let segs = snap.segments_for_range(start_date, end_date, None);
        for seg in &segs {
            let sidx = self.cache.get_session_index(seg).await?;
            total_sessions_inspected += sidx.headers.len();

            let matched_ranges = sidx.matching_session_ranges(|hdr: &SessionHeader| {
                if (hdr.flags & required_flags) != required_flags {
                    return false;
                }
                if let Some(tr) = query.time_range {
                    if hdr.end_ts_ms < tr.start_ts_ms || hdr.start_ts_ms > tr.end_ts_ms {
                        return false;
                    }
                }
                if let Some(tgt) = &target {
                    if tgt.event_ids.is_empty() || !hdr.may_contain_any_event(tgt.bloom_mask_64) {
                        return false;
                    }
                }
                true
            });

            sessions_pruned_by_bloom += sidx.headers.len().saturating_sub(matched_ranges.len());
            if matched_ranges.is_empty() {
                continue;
            }

            // Read Tier A ONLY for segments that survived SessionIndex pruning!
            let ta = self.cache.get_tier_a(seg).await?;
            for (r_start, r_end, hdr) in matched_ranges {
                if let Some(tgt) = &target {
                    let actually_has = (r_start..r_end).any(|r| tgt.matches_id(ta.event_id[r]));
                    if !actually_has {
                        continue;
                    }
                }

                let canon_uid = id_snap.resolve(hdr.raw_uid);
                let distinct_id = id_graph
                    .external_id(canon_uid)
                    .unwrap_or_else(|| format!("uid_{canon_uid}"));
                let ev_list: Vec<(i64, String, u32)> = (r_start..r_end)
                    .map(|r| {
                        let ename = id_to_name
                            .get(&ta.event_id[r])
                            .cloned()
                            .unwrap_or_else(|| format!("event_{}", ta.event_id[r]));
                        (ta.event_ts_ms[r], ename, ta.replay_offset_kb[r])
                    })
                    .collect();

                sessions.push(SessionSummaryItem {
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
                    events: ev_list,
                });
            }
        }

        // 2. Also include hot L0 `MemBucket` sessions
        type L0SessionAcc = (u16, Vec<(i64, u16, u32)>);
        let mut l0_grouped: HashMap<(u32, String), L0SessionAcc> = HashMap::new();
        for b in 0..ps.config().num_buckets {
            for r in ps.snapshot_l0_bucket(&snap, b as u8) {
                let canon_uid = id_snap.resolve(r.uid_internal);
                let entry = l0_grouped
                    .entry((canon_uid, r.session_id))
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
            if (flags & required_flags) != required_flags {
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
            let distinct_id = id_graph
                .external_id(canon_uid)
                .unwrap_or_else(|| format!("uid_{canon_uid}"));
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

            sessions.push(SessionSummaryItem {
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
                events: mapped_evs,
            });
        }

        sessions.sort_by(|a, b| b.end_ts_ms.cmp(&a.end_ts_ms));
        sessions.truncate(limit);

        let exec_time_us = t0.elapsed().as_micros() as u64;
        self.record_latency(exec_time_us);

        Ok(SessionSearchResult {
            snapshot_epoch: snap.epoch(),
            exec_time_us,
            total_sessions_inspected,
            sessions_pruned_by_bloom,
            sessions,
        })
    }
}
