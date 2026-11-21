pub mod bloom;
pub mod cache;
pub mod compactor;
pub mod error;
pub mod manifest;
pub mod memtable;
pub mod query;
pub mod segment;
pub mod wal;

pub use error::{StoreError, StoreResult};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDate;
use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use pulse_storage::EncryptedObjectStore;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::model::{
    AgentAnalyticsOverview, AgentErasureTombstone, AgentEvalRollup, AgentSpan, AgentTrace,
    AnalyticsFilter, CompareFilter, CompareResponse, EvalTargetType, SessionDetail,
    SessionListFilter, SessionListResponse, TraceDetail, TraceIngestBatch, TraceIngestResponse,
    TraceListFilter, TraceListResponse,
};
use crate::pricing::{sync_span_cost, sync_trace_cost};
use crate::store::cache::AgentSegmentCache;
use crate::store::error::validate_identifier;
use crate::store::manifest::{AgentManifest, ManifestStore, ReaderPinTracker};
use crate::store::memtable::{
    ShardedMemTable, SpanFieldRanks, StagedShardFlush, TraceCrdtMeta, DEFAULT_AGENT_SHARDS,
};
use crate::store::segment::{PlssReader, SegmentWriter, MAX_PLSS_SEGMENT_BYTES};
use crate::store::wal::{
    AgentWalStore, AgentWalWriter, WalConfig, DEFAULT_WAL_GROUP_COMMIT_MS,
    DEFAULT_WAL_MAX_GROUP_BYTES, DEFAULT_WAL_MAX_GROUP_ITEMS, MAX_WAL_GROUP_COMMIT_MS,
    MIN_WAL_GROUP_COMMIT_MS,
};

#[derive(Debug, Clone)]
pub struct AgentTraceStoreConfig {
    pub cache_dir: Option<PathBuf>,
    pub writer_id: Uuid,
    pub wal_group_commit_ms: u64,
    pub l0_flush_age_secs: u64,
    pub l0_shards: usize,
    pub max_segment_bytes: usize,
    pub gc_grace_secs: i64,
    pub read_only: bool,
    pub cache_memory_bytes: usize,
    pub cache_disk_bytes: usize,
    pub l0_flush_max_traces: usize,
    pub l0_flush_max_bytes: usize,
    pub wal_max_group_items: usize,
    pub wal_max_group_bytes: usize,
}

impl Default for AgentTraceStoreConfig {
    fn default() -> Self {
        Self {
            cache_dir: None,
            writer_id: Uuid::now_v7(),
            wal_group_commit_ms: DEFAULT_WAL_GROUP_COMMIT_MS,
            l0_flush_age_secs: 4,
            l0_shards: DEFAULT_AGENT_SHARDS,
            max_segment_bytes: MAX_PLSS_SEGMENT_BYTES,
            gc_grace_secs: 900,
            read_only: false,
            cache_memory_bytes: 64 * 1024 * 1024,
            cache_disk_bytes: 256 * 1024 * 1024,
            l0_flush_max_traces: 4_096,
            l0_flush_max_bytes: 32 * 1024 * 1024,
            wal_max_group_items: DEFAULT_WAL_MAX_GROUP_ITEMS,
            wal_max_group_bytes: DEFAULT_WAL_MAX_GROUP_BYTES,
        }
    }
}

/// RAII MVCC snapshot pinning active `.plss` segment paths until dropped.
pub struct AgentStorageSnapshot {
    pub manifest: Arc<AgentManifest>,
    pub l0_entries: Vec<(AgentTrace, Vec<AgentSpan>)>,
    pub(crate) l0_meta: HashMap<String, TraceCrdtMeta>,
    pub(crate) l0_span_ranks: HashMap<(String, String), SpanFieldRanks>,
    pinned_paths: Vec<String>,
    pin_tracker: Arc<ReaderPinTracker>,
}

impl Drop for AgentStorageSnapshot {
    fn drop(&mut self) {
        if !self.pinned_paths.is_empty() {
            self.pin_tracker.unpin_all(&self.pinned_paths);
        }
    }
}

#[allow(dead_code)]
pub(crate) struct ProjectAgentState {
    pub project_id: String,
    pub manifest_store: Arc<ManifestStore>,
    pub wal_store: Arc<AgentWalStore>,
    pub memtable: Arc<ShardedMemTable>,
    pub wal_writer: Option<Arc<AgentWalWriter>>,
    pub applied_wal_seq: AsyncMutex<HashMap<String, u64>>,
    pub flush_lock: AsyncMutex<()>,
}

/// Binary storage engine for AI Agent traces and spans (`PTWL` WAL + Sharded L0 + `PLSS` 7-tag segments + MVCC Manifest).
pub struct AgentTraceStore {
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<AgentSegmentCache>,
    pin_tracker: Arc<ReaderPinTracker>,
    config: AgentTraceStoreConfig,
    writer_id_str: String,
    projects: DashMap<String, Arc<ProjectAgentState>>,
    project_init_lock: AsyncMutex<()>,
    closed: AtomicBool,
}

impl AgentTraceStore {
    pub async fn open(
        object_store: Arc<dyn ObjectStore>,
        config: AgentTraceStoreConfig,
    ) -> Result<Self, StoreError> {
        Self::open_with_encrypted(object_store, None, config).await
    }

    pub async fn open_with_encrypted(
        object_store: Arc<dyn ObjectStore>,
        encrypted_store: Option<Arc<EncryptedObjectStore>>,
        mut config: AgentTraceStoreConfig,
    ) -> Result<Self, StoreError> {
        config.wal_group_commit_ms = config
            .wal_group_commit_ms
            .clamp(MIN_WAL_GROUP_COMMIT_MS, MAX_WAL_GROUP_COMMIT_MS);
        config.l0_shards = config.l0_shards.clamp(1, memtable::MAX_AGENT_SHARDS);
        config.max_segment_bytes = config.max_segment_bytes.clamp(1024, MAX_PLSS_SEGMENT_BYTES);
        config.gc_grace_secs = config.gc_grace_secs.max(0);

        let writer_id_str = config.writer_id.to_string();
        validate_identifier("writer_id", &writer_id_str)?;

        let cache = AgentSegmentCache::open(
            encrypted_store,
            config.cache_memory_bytes,
            config.cache_dir.as_deref(),
            config.cache_disk_bytes,
        )
        .await?;

        let store = Self {
            object_store,
            cache,
            pin_tracker: Arc::new(ReaderPinTracker::default()),
            config,
            writer_id_str,
            projects: DashMap::new(),
            project_init_lock: AsyncMutex::new(()),
            closed: AtomicBool::new(false),
        };

        // Discover and recover any existing projects with manifests or uncommitted WALs
        let existing = store.list_known_projects().await?;
        for pid in existing {
            store.get_or_open_project(&pid).await?;
        }

        Ok(store)
    }

    pub async fn ingest_batch(
        &self,
        project_id: &str,
        mut batch: TraceIngestBatch,
    ) -> Result<TraceIngestResponse, StoreError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StoreError::Shutdown);
        }
        if self.config.read_only {
            return Err(StoreError::InvalidIdentifier(
                "cannot ingest into read-only AgentTraceStore".to_string(),
            ));
        }
        validate_identifier("project_id", project_id)?;

        let accepted_traces = batch.traces.len();
        let accepted_spans = batch.spans.len();
        if accepted_traces == 0 && accepted_spans == 0 {
            return Ok(TraceIngestResponse {
                accepted_traces: 0,
                accepted_spans: 0,
                truncated_payloads: 0,
                redacted_fields: 0,
                dropped_spans: 0,
                warnings: Vec::new(),
            });
        }

        for tr in &mut batch.traces {
            if tr.project_id.is_empty() {
                tr.project_id = project_id.to_string();
            }
            sync_trace_cost(tr);
        }
        for sp in &mut batch.spans {
            if sp.project_id.is_empty() {
                sp.project_id = project_id.to_string();
            }
            sync_span_cost(sp);
        }

        let state = self.get_or_open_project(project_id).await?;
        let writer = state.wal_writer.as_ref().ok_or_else(|| {
            StoreError::Corrupted("missing WAL writer on writable project state".to_string())
        })?;
        writer.write(batch).await?;

        Ok(TraceIngestResponse {
            accepted_traces,
            accepted_spans,
            truncated_payloads: 0,
            redacted_fields: 0,
            dropped_spans: 0,
            warnings: Vec::new(),
        })
    }

    pub async fn flush_l0(&self) -> Result<(), StoreError> {
        self.flush_l0_inner(false).await
    }

    pub async fn flush_all_l0(&self) -> Result<(), StoreError> {
        self.flush_l0_inner(true).await
    }

    pub async fn compact_and_gc(
        &self,
        project_id: Option<&str>,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<(), StoreError> {
        compactor::compact_and_gc_impl(self, project_id, tombstones).await
    }

    pub async fn prune_expired_segments(
        &self,
        project_id: &str,
        cutoff_date: NaiveDate,
    ) -> Result<usize, StoreError> {
        compactor::prune_expired_segments_impl(self, project_id, cutoff_date).await
    }

    pub async fn list_sessions(
        &self,
        project_id: &str,
        filter: &SessionListFilter,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<SessionListResponse, StoreError> {
        query::list_sessions_impl(self, project_id, filter, tombstones).await
    }

    pub async fn get_session_detail(
        &self,
        project_id: &str,
        session_id: &str,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<Option<SessionDetail>, StoreError> {
        query::get_session_detail_impl(self, project_id, session_id, tombstones).await
    }

    pub async fn list_traces(
        &self,
        project_id: &str,
        filter: &TraceListFilter,
        rollups: &HashMap<(EvalTargetType, String), AgentEvalRollup>,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<TraceListResponse, StoreError> {
        query::list_traces_impl(self, project_id, filter, rollups, tombstones).await
    }

    pub async fn get_trace_detail(
        &self,
        project_id: &str,
        trace_id: &str,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<Option<TraceDetail>, StoreError> {
        query::get_trace_detail_impl(self, project_id, trace_id, tombstones).await
    }

    pub async fn query_analytics(
        &self,
        project_id: &str,
        filter: &AnalyticsFilter,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<AgentAnalyticsOverview, StoreError> {
        query::query_analytics_impl(self, project_id, filter, tombstones, &HashMap::new()).await
    }

    /// Like [`Self::query_analytics`], with trace eval rollups applied so every breakdown has eval scores.
    pub async fn query_analytics_with_rollups(
        &self,
        project_id: &str,
        filter: &AnalyticsFilter,
        tombstones: &[AgentErasureTombstone],
        rollups: &HashMap<(EvalTargetType, String), AgentEvalRollup>,
    ) -> Result<AgentAnalyticsOverview, StoreError> {
        query::query_analytics_impl(self, project_id, filter, tombstones, rollups).await
    }

    pub async fn compare_groups(
        &self,
        project_id: &str,
        filter: &CompareFilter,
        tombstones: &[AgentErasureTombstone],
    ) -> Result<CompareResponse, StoreError> {
        query::compare_groups_impl(self, project_id, filter, tombstones).await
    }

    async fn flush_l0_inner(&self, force_all: bool) -> Result<(), StoreError> {
        if self.config.read_only {
            return Ok(());
        }
        let states: Vec<Arc<ProjectAgentState>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        // Every project gets its turn: one failing project must not leave the others'
        // acknowledged spans in memory. The first error is still reported.
        let mut first_err = None;
        for state in states {
            if let Err(e) = self.flush_project_l0(&state, force_all).await {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    async fn flush_project_l0(
        &self,
        state: &Arc<ProjectAgentState>,
        force_all: bool,
    ) -> StoreResult<()> {
        let _guard = state.flush_lock.lock().await;
        for _attempt in 0..manifest::MAX_MANIFEST_CAS_RETRIES {
            if state.memtable.total_traces() == 0 && !state.memtable.has_purged_wal() {
                return Ok(());
            }

            let pre_manifest = state.manifest_store.load_latest().await?;
            state
                .memtable
                .evict_committed_wal(&pre_manifest.committed_wal_seq);
            if !pre_manifest.erased_traces.is_empty() {
                state.memtable.purge_erased_ids(&pre_manifest.erased_traces);
            }

            let eligible = state.memtable.eligible_shards(
                Duration::from_secs(self.config.l0_flush_age_secs),
                self.config.l0_flush_max_traces,
                self.config.l0_flush_max_bytes,
                force_all,
            );
            let mut staged_list: Vec<StagedShardFlush> = Vec::with_capacity(eligible.len());
            for (date, shard_id) in eligible {
                if let Some(staged) = state.memtable.stage_shard(date, shard_id) {
                    staged_list.push(staged);
                }
            }
            if staged_list.is_empty() {
                if force_all || state.memtable.has_purged_wal() {
                    let committed_manifest = state
                        .manifest_store
                        .commit_with_cas(|m| {
                            let safe_wal = state.memtable.compute_safe_committed_wal_seq(
                                &m.committed_wal_seq,
                                &[],
                                &self.writer_id_str,
                            );
                            let Some(&self_safe) = safe_wal.get(&self.writer_id_str) else {
                                return Ok(false);
                            };
                            let prev_self = m
                                .committed_wal_seq
                                .get(&self.writer_id_str)
                                .copied()
                                .unwrap_or(0);
                            if self_safe <= prev_self {
                                return Ok(false);
                            }
                            m.committed_wal_seq
                                .insert(self.writer_id_str.clone(), self_safe);
                            m.updated_at_ms = chrono::Utc::now().timestamp_millis();
                            Ok(true)
                        })
                        .await?;
                    if let Some(&seq) = committed_manifest
                        .committed_wal_seq
                        .get(&self.writer_id_str)
                    {
                        if seq > 0 {
                            let _ = state
                                .wal_store
                                .purge_up_to(&self.writer_id_str, seq)
                                .await?;
                        }
                    }
                    state.memtable.clear_purged_wal_flag();
                }
                return Ok(());
            }

            let mut new_segments = Vec::new();
            for sf in &staged_list {
                let date_str = sf.date.format("%Y-%m-%d").to_string();
                match SegmentWriter::write_segments_split_ranked(
                    &state.project_id,
                    &date_str,
                    sf.shard_id,
                    0,
                    sf.entries.clone(),
                    self.config.max_segment_bytes,
                    &self.object_store,
                )
                .await
                {
                    Ok(segs) => new_segments.extend(segs),
                    Err(e) => {
                        for seg in &new_segments {
                            let loc = ObjectPath::from(seg.object_path.as_str());
                            let _ = self.object_store.delete(&loc).await;
                        }
                        return Err(e);
                    }
                }
            }

            let mut retry_needed = false;
            let mut advanced_wids: Vec<(String, u64)> = Vec::new();
            let commit_res = state
                .manifest_store
                .commit_with_cas(|m| {
                    retry_needed = false;
                    advanced_wids.clear();
                    if m.epoch != pre_manifest.epoch {
                        let newly_erased_staged = staged_list.iter().any(|sf| {
                            sf.entries.iter().any(|(tr, _, _)| {
                                m.erased_traces.contains_key(&tr.trace_id)
                                    && !pre_manifest.erased_traces.contains_key(&tr.trace_id)
                            })
                        });
                        let committed_while_staging = staged_list.iter().any(|sf| {
                            sf.entries.iter().any(|(tr, _, _)| {
                                sf.staged_wal_seqs.get(&tr.trace_id).is_some_and(|wmap| {
                                    !wmap.is_empty()
                                        && wmap.iter().all(|(wid, seqs)| {
                                            let c =
                                                m.committed_wal_seq.get(wid).copied().unwrap_or(0);
                                            seqs.iter().all(|&s| s <= c)
                                        })
                                })
                            })
                        });
                        if newly_erased_staged || committed_while_staging {
                            retry_needed = true;
                            return Ok(false);
                        }
                    }

                    let safe_wal = state.memtable.compute_safe_committed_wal_seq(
                        &m.committed_wal_seq,
                        &staged_list,
                        &self.writer_id_str,
                    );
                    m.segments.extend(new_segments.clone());
                    for (wid, seq) in safe_wal {
                        let cur = m.committed_wal_seq.get(&wid).copied().unwrap_or(0);
                        if seq > cur {
                            m.committed_wal_seq.insert(wid.clone(), seq);
                            advanced_wids.push((wid, seq));
                        }
                    }
                    m.updated_at_ms = chrono::Utc::now().timestamp_millis();
                    Ok(true)
                })
                .await;

            if retry_needed || commit_res.is_err() {
                for seg in &new_segments {
                    let loc = ObjectPath::from(seg.object_path.as_str());
                    let _ = self.object_store.delete(&loc).await;
                }
                if retry_needed {
                    continue;
                }
                commit_res?;
                unreachable!();
            }

            let committed_manifest = commit_res?;
            for sf in &staged_list {
                state.memtable.complete_shard_flush(sf);
            }

            for (wid, seq) in &advanced_wids {
                if *seq > 0 {
                    let _ = state.wal_store.purge_up_to(wid, *seq).await?;
                }
            }
            if let Some(&self_seq) = committed_manifest
                .committed_wal_seq
                .get(&self.writer_id_str)
            {
                if self_seq > 0 && !advanced_wids.iter().any(|(w, _)| w == &self.writer_id_str) {
                    let _ = state
                        .wal_store
                        .purge_up_to(&self.writer_id_str, self_seq)
                        .await?;
                }
            }
            state.memtable.clear_purged_wal_flag();
            return Ok(());
        }
        Err(StoreError::ManifestConflict)
    }

    pub async fn close(&self) -> Result<(), StoreError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let states: Vec<Arc<ProjectAgentState>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        for state in &states {
            if let Some(writer) = &state.wal_writer {
                writer.shutdown().await;
            }
        }
        if !self.config.read_only {
            for state in &states {
                self.flush_project_l0(state, true).await?;
            }
        }

        self.cache.close().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // pub(crate) hooks for M1c (`compactor.rs` & `query.rs`)
    // -----------------------------------------------------------------------

    pub(crate) async fn get_or_open_project(
        &self,
        project_id: &str,
    ) -> StoreResult<Arc<ProjectAgentState>> {
        validate_identifier("project_id", project_id)?;
        if let Some(existing) = self.projects.get(project_id) {
            return Ok(existing.value().clone());
        }

        let _init_guard = self.project_init_lock.lock().await;
        if let Some(existing) = self.projects.get(project_id) {
            return Ok(existing.value().clone());
        }

        let manifest_store = Arc::new(ManifestStore::new(
            project_id,
            self.object_store.clone(),
            self.gc_grace_ms(),
        )?);
        let wal_store = Arc::new(AgentWalStore::new(project_id, self.object_store.clone())?);
        let memtable = Arc::new(ShardedMemTable::new(
            project_id,
            self.config.l0_shards as u16,
        ));

        let manifest = manifest_store.load_latest().await?;
        let mut applied = HashMap::new();
        let replayed = wal_store
            .replay_uncommitted(
                &manifest.committed_wal_seq,
                &mut applied,
                !self.config.read_only,
            )
            .await?;
        for (wid, seq, mut batch) in replayed {
            let had_erased = filter_batch_erased(&mut batch, &manifest.erased_traces);
            if had_erased && wid != self.writer_id_str {
                memtable.record_blocked_foreign_wal_seq(&wid, seq);
            }
            if batch.traces.is_empty() && batch.spans.is_empty() {
                if wid == self.writer_id_str {
                    memtable.record_accounted_wal_seq(&wid, seq);
                }
            } else {
                memtable.insert_batch(&wid, seq, batch)?;
            }
        }

        let wal_writer = if self.config.read_only {
            None
        } else {
            let committed_for_me = manifest
                .committed_wal_seq
                .get(&self.writer_id_str)
                .copied()
                .unwrap_or(0);
            let existing_max = wal_store
                .max_existing_seq_for_writer(&self.writer_id_str)
                .await?;
            let initial_next_seq = committed_for_me.max(existing_max).saturating_add(1);
            let writer = AgentWalWriter::spawn(
                project_id,
                &self.writer_id_str,
                initial_next_seq,
                WalConfig {
                    group_commit_ms: self.config.wal_group_commit_ms,
                    max_group_items: self.config.wal_max_group_items,
                    max_group_bytes: self.config.wal_max_group_bytes,
                },
                self.object_store.clone(),
                memtable.clone(),
            )?;
            Some(Arc::new(writer))
        };

        let state = Arc::new(ProjectAgentState {
            project_id: project_id.to_string(),
            manifest_store,
            wal_store,
            memtable,
            wal_writer,
            applied_wal_seq: AsyncMutex::new(applied),
            flush_lock: AsyncMutex::new(()),
        });
        self.projects.insert(project_id.to_string(), state.clone());
        Ok(state)
    }

    pub(crate) async fn refresh_from_wal(&self, project_id: &str) -> StoreResult<AgentManifest> {
        let state = self.get_or_open_project(project_id).await?;
        let manifest = state.manifest_store.load_latest().await?;
        let mut applied = state.applied_wal_seq.lock().await;
        state
            .memtable
            .evict_committed_wal(&manifest.committed_wal_seq);
        if !manifest.erased_traces.is_empty() {
            state.memtable.purge_erased_ids(&manifest.erased_traces);
        }
        let replayed = state
            .wal_store
            .replay_uncommitted(
                &manifest.committed_wal_seq,
                &mut applied,
                !self.config.read_only,
            )
            .await?;
        for (wid, seq, mut batch) in replayed {
            let had_erased = filter_batch_erased(&mut batch, &manifest.erased_traces);
            if had_erased && wid != self.writer_id_str {
                state.memtable.record_blocked_foreign_wal_seq(&wid, seq);
            }
            if batch.traces.is_empty() && batch.spans.is_empty() {
                if wid == self.writer_id_str {
                    state.memtable.record_accounted_wal_seq(&wid, seq);
                }
            } else {
                state.memtable.insert_batch(&wid, seq, batch)?;
            }
        }
        Ok(manifest)
    }

    pub(crate) async fn acquire_snapshot(
        &self,
        project_id: &str,
    ) -> StoreResult<AgentStorageSnapshot> {
        let state = self.get_or_open_project(project_id).await?;
        let manifest = if self.config.read_only {
            Arc::new(self.refresh_from_wal(project_id).await?)
        } else {
            let m = Arc::new(state.manifest_store.load_latest().await?);
            state.memtable.evict_committed_wal(&m.committed_wal_seq);
            m
        };
        let pinned_paths: Vec<String> = manifest
            .segments
            .iter()
            .map(|s| s.object_path.clone())
            .collect();
        self.pin_tracker.pin_all(&pinned_paths);
        let ranked = state.memtable.snapshot_all_ranked();
        let mut l0_entries = ranked.entries;
        let mut l0_meta = ranked.trace_meta;
        let mut l0_span_ranks = ranked.span_ranks;
        if !manifest.erased_traces.is_empty() {
            l0_entries.retain(|(tr, _)| !manifest.erased_traces.contains_key(&tr.trace_id));
            l0_meta.retain(|tid, _| !manifest.erased_traces.contains_key(tid));
            l0_span_ranks.retain(|(tid, _), _| !manifest.erased_traces.contains_key(tid));
        }
        Ok(AgentStorageSnapshot {
            manifest,
            l0_entries,
            l0_meta,
            l0_span_ranks,
            pinned_paths,
            pin_tracker: self.pin_tracker.clone(),
        })
    }

    pub(crate) async fn list_known_projects(&self) -> StoreResult<Vec<String>> {
        let mut projects: BTreeSet<String> =
            self.projects.iter().map(|e| e.key().clone()).collect();
        for (prefix_str, strip) in [
            ("agent_manifests/", "agent_manifests/"),
            ("agent_wal/", "agent_wal/"),
        ] {
            let prefix = ObjectPath::from(prefix_str);
            let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
            for m in metas {
                if let Some(rest) = m.location.as_ref().strip_prefix(strip) {
                    if let Some((pid, _)) = rest.split_once('/') {
                        if validate_identifier("project_id", pid).is_ok() {
                            projects.insert(pid.to_string());
                        }
                    }
                }
            }
        }
        Ok(projects.into_iter().collect())
    }

    #[allow(dead_code)]
    pub(crate) async fn manifest_store(&self, project_id: &str) -> StoreResult<Arc<ManifestStore>> {
        Ok(self
            .get_or_open_project(project_id)
            .await?
            .manifest_store
            .clone())
    }

    #[allow(dead_code)]
    pub(crate) async fn memtable(&self, project_id: &str) -> StoreResult<Arc<ShardedMemTable>> {
        Ok(self.get_or_open_project(project_id).await?.memtable.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn cache(&self) -> &Arc<AgentSegmentCache> {
        &self.cache
    }

    #[allow(dead_code)]
    pub(crate) fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    #[allow(dead_code)]
    pub(crate) fn max_segment_bytes(&self) -> usize {
        self.config.max_segment_bytes
    }

    pub(crate) fn gc_grace_ms(&self) -> i64 {
        self.config.gc_grace_secs.saturating_mul(1000)
    }

    #[allow(dead_code)]
    pub(crate) fn pin_tracker(&self) -> &Arc<ReaderPinTracker> {
        &self.pin_tracker
    }

    #[allow(dead_code)]
    pub(crate) fn plss_reader(&self) -> PlssReader {
        PlssReader::new(self.object_store.clone(), self.cache.clone())
    }
}

fn filter_batch_erased(
    batch: &mut TraceIngestBatch,
    erased_traces: &BTreeMap<String, i64>,
) -> bool {
    if erased_traces.is_empty() {
        return false;
    }
    let before_traces = batch.traces.len();
    let before_spans = batch.spans.len();
    batch
        .traces
        .retain(|t| !erased_traces.contains_key(&t.trace_id));
    batch
        .spans
        .retain(|s| !erased_traces.contains_key(&s.trace_id));
    batch.traces.len() != before_traces || batch.spans.len() != before_spans
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{SpanPhase, SpanType, TraceStatus};
    use object_store::memory::InMemory;
    use serde_json::json;

    fn sample_batch(trace_id: &str, ts: i64) -> TraceIngestBatch {
        TraceIngestBatch {
            traces: vec![AgentTrace {
                trace_id: trace_id.to_string(),
                project_id: "proj_e2e".to_string(),
                agent_name: "planner".to_string(),
                agent_version: "1.0.0".to_string(),
                prompt_version: None,
                model_config_version: None,
                tool_config_version: None,
                app_release: None,
                environment: "production".to_string(),
                session_id: Some("sess_1".to_string()),
                user_id: Some("user_1".to_string()),
                workflow_id: None,
                parent_trace_id: None,
                status: TraceStatus::Ok,
                start_ts_ms: ts,
                end_ts_ms: Some(ts + 120),
                duration_ms: 120,
                input: json!({"prompt": "hi"}),
                output: json!({"reply": "hello"}),
                error_type: None,
                error_message: None,
                failure_source: None,
                metadata: serde_json::Value::Null,
                tags: vec!["tag1".to_string()],
                span_count: 1,
                llm_call_count: 1,
                tool_call_count: 0,
                error_span_count: 0,
                input_tokens: 10,
                output_tokens: 20,
                cached_tokens: 0,
                total_tokens: 30,
                total_cost_nanos: 1_500_000,
                total_cost_usd: 0.0015,
                primary_model: Some("gpt-4o".to_string()),
                primary_provider: Some("openai".to_string()),
                avg_eval_score: None,
                eval_pass_rate: None,
                has_loop_warning: false,
            }],
            spans: vec![AgentSpan {
                span_id: format!("sp_{trace_id}"),
                trace_id: trace_id.to_string(),
                project_id: "proj_e2e".to_string(),
                parent_span_id: None,
                linked_span_ids: vec![],
                span_phase: SpanPhase::Final,
                depth: 0,
                name: "llm_call".to_string(),
                span_type: SpanType::Llm,
                status: TraceStatus::Ok,
                start_ts_ms: ts,
                end_ts_ms: Some(ts + 100),
                duration_ms: 100,
                provider: Some("openai".to_string()),
                model: Some("gpt-4o".to_string()),
                model_id: None,
                model_version: None,
                prompt_template_id: None,
                prompt_template_version: None,
                input_tokens: 10,
                output_tokens: 20,
                cached_tokens: 0,
                total_tokens: 30,
                cost_nanos: 1_500_000,
                cost_usd: 0.0015,
                temperature: None,
                finish_reason: Some("stop".to_string()),
                model_params: serde_json::Value::Null,
                tool_name: None,
                tool_version: None,
                tool_retry_count: 0,
                input: json!({"msg": "hi"}),
                output: json!({"msg": "hello"}),
                attributes: serde_json::Value::Null,
                events: vec![],
                metadata: serde_json::Value::Null,
                error_type: None,
                error_message: None,
                error_stack: None,
            }],
        }
    }

    /// By-model rows count runs (so a failed run shows up as a failure) and fall back to
    /// input + output tokens when a sender leaves total_tokens unset. Checked in L0 and after flush.
    #[tokio::test]
    async fn by_model_counts_runs_and_derives_tokens() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = AgentTraceStore::open(os, AgentTraceStoreConfig::default())
            .await
            .unwrap();
        let ts = 1_700_000_000_000;
        for (i, failed) in [false, true].into_iter().enumerate() {
            let mut b = sample_batch(&format!("tr_bm_{i}"), ts + i as i64);
            b.spans[0].total_tokens = 0;
            if failed {
                b.traces[0].status = TraceStatus::Error;
            }
            store.ingest_batch("proj_e2e", b).await.unwrap();
        }
        let rollups = HashMap::from([(
            (EvalTargetType::Trace, "tr_bm_0".to_string()),
            AgentEvalRollup {
                project_id: "proj_e2e".into(),
                target_type: EvalTargetType::Trace,
                target_id: "tr_bm_0".into(),
                avg_score: 0.5,
                pass_rate: Some(1.0),
                eval_count: 1,
                updated_at: ts,
            },
        )]);
        for stage in ["l0", "plss"] {
            let qa = store
                .query_analytics_with_rollups(
                    "proj_e2e",
                    &AnalyticsFilter::default(),
                    &[],
                    &rollups,
                )
                .await
                .unwrap();
            let row = qa.by_model.iter().find(|r| r.key == "gpt-4o").unwrap();
            assert_eq!(row.executions, 2, "{stage}");
            assert_eq!(row.failures, 1, "{stage}");
            assert_eq!(row.total_tokens, 60, "{stage}");
            assert_eq!(row.avg_eval_score, Some(0.5), "{stage}");
            store.flush_all_l0().await.unwrap();
        }
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn query_node_refresh_from_wal_gives_near_realtime_visibility() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = AgentTraceStore::open(
            os.clone(),
            AgentTraceStoreConfig {
                writer_id: Uuid::now_v7(),
                wal_group_commit_ms: 5,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let query_node = AgentTraceStore::open(
            os.clone(),
            AgentTraceStoreConfig {
                writer_id: Uuid::now_v7(),
                read_only: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Writer ingests to WAL + L0 (without flushing L0 to PLSS yet)
        writer
            .ingest_batch("proj_e2e", sample_batch("tr_rt_1", 1_700_000_000_000))
            .await
            .unwrap();

        // Query node refreshes from uncommitted WAL and sees the trace immediately
        let snap1 = query_node.acquire_snapshot("proj_e2e").await.unwrap();
        assert!(snap1.manifest.segments.is_empty());
        assert_eq!(snap1.l0_entries.len(), 1);
        assert_eq!(snap1.l0_entries[0].0.trace_id, "tr_rt_1");
        drop(snap1);

        // Writer flushes L0 -> PLSS + commits manifest -> query node sees segment & evicts L0
        writer.flush_all_l0().await.unwrap();
        let snap2 = query_node.acquire_snapshot("proj_e2e").await.unwrap();
        assert_eq!(snap2.manifest.segments.len(), 1);
        assert!(snap2.l0_entries.is_empty());
        drop(snap2);

        query_node.close().await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn store_open_ingest_flush_close_roundtrip_with_foyer_disk_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let in_mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let enc = Arc::new(EncryptedObjectStore::new(in_mem));
        enc.set_master_key(Some([0x11u8; 32]));
        let os = enc.as_object_store();

        let cache_dir = tmp.path().join("agent_foyer_cache");
        let store = AgentTraceStore::open_with_encrypted(
            os.clone(),
            Some(enc.clone()),
            AgentTraceStoreConfig {
                cache_dir: Some(cache_dir.clone()),
                cache_memory_bytes: 8 * 1024 * 1024,
                cache_disk_bytes: 32 * 1024 * 1024,
                wal_group_commit_ms: 5,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let resp = store
            .ingest_batch("proj_e2e", sample_batch("tr_disk_1", 1_700_000_000_000))
            .await
            .unwrap();
        assert_eq!(resp.accepted_traces, 1);
        assert_eq!(resp.accepted_spans, 1);

        store.flush_all_l0().await.unwrap();

        let snap = store.acquire_snapshot("proj_e2e").await.unwrap();
        assert_eq!(snap.manifest.segments.len(), 1);
        let seg = &snap.manifest.segments[0];
        let reader = store.plss_reader();
        let tail = reader.read_eof_tail(seg).await.unwrap();
        let hydrated = reader
            .read_trace_ranges(seg, &tail, "tr_disk_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hydrated.0.trace_id, "tr_disk_1");
        assert_eq!(hydrated.1.len(), 1);

        let all = reader.read_all_traces_and_spans(seg).await.unwrap();
        assert_eq!(all.len(), 1);
        let span_metrics = reader.read_span_metrics(seg, &tail).await.unwrap();
        assert_eq!(span_metrics.len(), 1);
        let sess_rollups = reader.read_session_rollups(seg, &tail).await.unwrap();
        assert_eq!(sess_rollups.len(), 1);
        assert_eq!(sess_rollups[0].trace_count, 1);
        drop(snap);

        store.close().await.unwrap();
        assert!(cache_dir.exists());
    }

    #[tokio::test]
    async fn startup_recovers_uncommitted_wal_after_simulated_crash() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal = AgentWalStore::new("proj_e2e", os.clone()).unwrap();
        let crashed_wid = Uuid::now_v7().to_string();
        let frame = crate::store::wal::encode_ptwl_frame(
            1,
            1_700_000_000_000,
            &sample_batch("tr_crash_1", 1_700_000_000_000),
        )
        .unwrap();
        os.put(
            &crate::store::wal::wal_object_path("proj_e2e", &crashed_wid, 1).unwrap(),
            object_store::PutPayload::from_bytes(bytes::Bytes::from(frame)),
        )
        .await
        .unwrap();

        let store = AgentTraceStore::open(os.clone(), AgentTraceStoreConfig::default())
            .await
            .unwrap();
        let snap = store.acquire_snapshot("proj_e2e").await.unwrap();
        assert_eq!(snap.l0_entries.len(), 1);
        assert_eq!(snap.l0_entries[0].0.trace_id, "tr_crash_1");
        drop(snap);

        store.flush_all_l0().await.unwrap();
        assert!(os
            .head(&crate::store::wal::wal_object_path("proj_e2e", &crashed_wid, 1).unwrap())
            .await
            .is_err());
        let _ = wal;
        store.close().await.unwrap();
    }
}
