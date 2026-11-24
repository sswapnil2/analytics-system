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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDate;
use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode};
use pulse_storage::{EncryptedObjectStore, OwnerCheck, ProjectOwnerChecker, StorageError};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

use crate::model::{
    AgentAnalyticsOverview, AgentErasureTombstone, AgentEvalRollup, AgentSpan, AgentTrace,
    AnalyticsFilter, CompareFilter, CompareResponse, ErasureLevel, EvalTargetType, SessionDetail,
    SessionListFilter, SessionListResponse, TraceDetail, TraceIngestBatch, TraceIngestResponse,
    TraceListFilter, TraceListResponse,
};
use crate::pricing::{sync_span_cost, sync_trace_cost};
use crate::store::cache::AgentSegmentCache;
use crate::store::error::validate_identifier;
use crate::store::manifest::{
    compute_urgent_grace_ms, partition_upload_key, AgentManifest, AgentUploadIntent,
    AgentUploadIntentBatch, AgentUploadIntentStore, ManifestStore, PendingAgentS3Delete,
    PublishedAgentManifest, PublishedAgentManifestStore, ReaderPinTracker, SegmentDescriptor,
};
use crate::store::memtable::{
    merge_spans_crdt_with_ranks, merge_trace_headers_with_meta,
    recompute_trace_from_spans_with_explicit, RankedTraceEntry, ShardedMemTable, SpanFieldRanks,
    StagedShardFlush, TraceCrdtMeta, DEFAULT_AGENT_SHARDS,
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
    /// Unflushed bytes one project may hold before ingest is refused (`PULSE_AGENT_L0_PROJECT_MAX_MB`).
    pub l0_max_bytes_project: usize,
    /// Unflushed bytes all projects together may hold (`PULSE_AGENT_L0_MAX_MB`).
    pub l0_max_bytes_node: usize,
    /// Span records one trace may accumulate across batches; further spans are dropped.
    pub max_trace_spans: usize,
    /// Estimated bytes one trace may accumulate across batches.
    pub max_trace_bytes: usize,
}

fn env_mb(name: &str, default_mb: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default_mb)
        .saturating_mul(1024 * 1024)
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
            l0_max_bytes_project: env_mb("PULSE_AGENT_L0_PROJECT_MAX_MB", 256),
            l0_max_bytes_node: env_mb("PULSE_AGENT_L0_MAX_MB", 512),
            max_trace_spans: 10_000,
            max_trace_bytes: 64 * 1024 * 1024,
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

/// Outcome of a physical agent erasure pass over one project.
#[derive(Debug, Clone, Default)]
pub struct ProjectErasureOutcome {
    pub purged_tombstone_ids: Vec<String>,
    pub next_urgent_gc_deadline_ms: Option<i64>,
    pub pass_started_at_ms: i64,
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
    pub publish_lock: AsyncMutex<()>,
    pub fenced: Arc<AtomicBool>,
    pub last_published_seq: parking_lot::Mutex<Option<u64>>,
    pub last_published_s3_segments: parking_lot::Mutex<Option<Vec<String>>>,
    pub last_published_erased_traces: parking_lot::Mutex<Option<BTreeMap<String, i64>>>,
    pub has_pending_upload_intent: AtomicBool,
    pub pending_local_deletes: parking_lot::Mutex<HashSet<String>>,
    pub closed_date_pending_since_ms: parking_lot::Mutex<HashMap<String, i64>>,
    pub last_ingest_ms: AtomicI64,
    pub dirty_since_compact: AtomicBool,
    pub query_cached_publish: parking_lot::Mutex<Option<(u64, Arc<AgentManifest>)>>,
    /// Unflushed memtable bytes, refreshed after every append and flush. Backpressure reads
    /// this instead of locking every project's memtable.
    pub l0_bytes: AtomicUsize,
    /// Span records and bytes accepted so far per trace, kept across flushes so a looping
    /// agent cannot grow one trace without limit.
    // ponytail: in memory, so a restart resets the counts; entries idle for an hour or beyond
    // 100k traces are forgotten. Persist in the manifest if a hard cap across restarts matters.
    pub trace_usage: moka::sync::Cache<String, (usize, usize)>,
}

/// Binary storage engine for AI Agent traces and spans (`PTWL` WAL + Sharded L0 + `PLSS` 7-tag segments + MVCC Manifest).
pub struct AgentTraceStore {
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<AgentSegmentCache>,
    pin_tracker: Arc<ReaderPinTracker>,
    config: AgentTraceStoreConfig,
    writer_id_str: String,
    local_hot_tier: AtomicBool,
    upload_interval_ms: AtomicU64,
    closed_date_upload_ms: AtomicU64,
    closed_date_max_bytes: AtomicU64,
    upload_merge_max_input_bytes: AtomicU64,
    cold_merge_idle_ms: AtomicU64,
    projects: DashMap<String, Arc<ProjectAgentState>>,
    project_init_lock: AsyncMutex<()>,
    owner_checker: Option<Arc<dyn ProjectOwnerChecker>>,
    closed: AtomicBool,
    flush_notify: Notify,
    /// Set when ingest was refused for memory: the next flush takes every shard, not only the
    /// ones past their own threshold.
    flush_all_next: AtomicBool,
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
        config: AgentTraceStoreConfig,
    ) -> Result<Self, StoreError> {
        Self::open_with_encrypted_and_owner(object_store, encrypted_store, config, None).await
    }

    pub async fn open_with_encrypted_and_owner(
        object_store: Arc<dyn ObjectStore>,
        encrypted_store: Option<Arc<EncryptedObjectStore>>,
        mut config: AgentTraceStoreConfig,
        owner_checker: Option<Arc<dyn ProjectOwnerChecker>>,
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

        let is_local_hot = pulse_storage::is_hot_routed_store(object_store.as_ref());
        let upload_interval_ms = pulse_storage::upload_interval_from_env().as_millis() as u64;
        let closed_date_upload_ms =
            pulse_storage::closed_date_upload_interval_from_env().as_millis() as u64;
        let closed_date_max_bytes = pulse_storage::closed_date_max_bytes_from_env();
        let upload_merge_max_input_bytes = pulse_storage::upload_merge_max_input_bytes_from_env();
        let cold_merge_idle_ms = pulse_storage::cold_merge_idle_from_env().as_millis() as u64;

        let store = Self {
            object_store,
            cache,
            pin_tracker: Arc::new(ReaderPinTracker::default()),
            config,
            writer_id_str,
            local_hot_tier: AtomicBool::new(is_local_hot),
            upload_interval_ms: AtomicU64::new(upload_interval_ms),
            closed_date_upload_ms: AtomicU64::new(closed_date_upload_ms),
            closed_date_max_bytes: AtomicU64::new(closed_date_max_bytes),
            upload_merge_max_input_bytes: AtomicU64::new(upload_merge_max_input_bytes),
            cold_merge_idle_ms: AtomicU64::new(cold_merge_idle_ms),
            projects: DashMap::new(),
            project_init_lock: AsyncMutex::new(()),
            owner_checker,
            closed: AtomicBool::new(false),
            flush_notify: Notify::new(),
            flush_all_next: AtomicBool::new(false),
        };

        // Discover and recover any existing projects with manifests or uncommitted WALs.
        // In local hot tier + read_only (`NodeRole::Query`), skip eager S3 scanning at boot and
        // load `agent_published/` on demand.
        if !(is_local_hot && store.config.read_only) {
            let existing = store.list_known_projects().await?;
            for pid in existing {
                match store.get_or_open_project(&pid).await {
                    Ok(_) => {}
                    Err(StoreError::NotOwner { .. }) => {
                        tracing::debug!(project_id = %pid, "skipping foreign agent project on boot");
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(store)
    }

    pub(crate) async fn ensure_writable_owner(&self, project_id: &str) -> StoreResult<()> {
        if self.config.read_only {
            return Ok(());
        }
        let Some(ref checker) = self.owner_checker else {
            return Ok(());
        };
        match checker.check_owner(project_id).await {
            Ok(OwnerCheck::Owned) => Ok(()),
            Ok(OwnerCheck::Unassigned) => Err(StoreError::NotOwner {
                project_id: project_id.to_string(),
                owner_node: None,
                owner_address: None,
            }),
            Ok(OwnerCheck::NotOwner(info)) => Err(StoreError::NotOwner {
                project_id: project_id.to_string(),
                owner_node: Some(info.node_name),
                owner_address: Some(info.address),
            }),
            Err(e) => Err(StoreError::Storage(StorageError::Unavailable(format!(
                "project owner check failed: {e}"
            )))),
        }
    }

    pub(crate) async fn is_owned_locally(&self, project_id: &str) -> bool {
        if self.config.read_only {
            return false;
        }
        match &self.owner_checker {
            Some(checker) => checker.is_owned_locally(project_id).await,
            None => true,
        }
    }

    /// Fences and evicts any open writable project whose ownership moved away in `project_owner`,
    /// shutting down its WAL writer without flushing L0 or committing manifests.
    pub async fn reconcile_ownership(&self) {
        if self.config.read_only {
            return;
        }
        let Some(ref checker) = self.owner_checker else {
            return;
        };
        let open_states: Vec<Arc<ProjectAgentState>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        for state in open_states {
            match checker.check_owner(&state.project_id).await {
                Ok(OwnerCheck::Owned) => {}
                Ok(OwnerCheck::NotOwner(_) | OwnerCheck::Unassigned) => {
                    tracing::error!(
                        project_id = %state.project_id,
                        "agent project ownership lost to another node; fencing writer"
                    );
                    state.fenced.store(true, Ordering::Release);
                    self.projects
                        .remove_if(&state.project_id, |_, cur| Arc::ptr_eq(cur, &state));
                    if let Some(ref writer) = state.wal_writer {
                        writer.shutdown().await;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        project_id = %state.project_id,
                        error = %e,
                        "agent project ownership check failed during background reconcile"
                    );
                }
            }
        }
    }

    pub fn is_local_hot_tier(&self) -> bool {
        self.local_hot_tier.load(Ordering::Acquire)
    }

    pub fn is_read_only(&self) -> bool {
        self.config.read_only
    }

    pub fn upload_interval(&self) -> Duration {
        Duration::from_millis(self.upload_interval_ms.load(Ordering::Acquire))
    }

    pub fn set_upload_interval(&self, interval: Duration) {
        self.upload_interval_ms
            .store(interval.as_millis() as u64, Ordering::Release);
    }

    pub fn set_closed_date_upload_interval(&self, interval: Duration) {
        self.closed_date_upload_ms
            .store(interval.as_millis() as u64, Ordering::Release);
    }

    pub fn set_closed_date_max_bytes(&self, bytes: u64) {
        self.closed_date_max_bytes
            .store(bytes.max(1), Ordering::Release);
    }

    pub fn set_upload_merge_max_input_bytes(&self, bytes: u64) {
        self.upload_merge_max_input_bytes
            .store(bytes.max(1), Ordering::Release);
    }

    pub fn set_cold_merge_idle(&self, idle: Duration) {
        self.cold_merge_idle_ms
            .store(idle.as_millis() as u64, Ordering::Release);
    }

    pub fn last_ingest_ms(&self, project_id: &str) -> Option<i64> {
        self.projects
            .get(project_id)
            .map(|p| p.last_ingest_ms.load(Ordering::Acquire))
            .filter(|&ts| ts > 0)
    }

    pub async fn known_project_ids(&self) -> StoreResult<Vec<String>> {
        self.list_known_projects().await
    }

    pub async fn purge_erased_in_l0(
        &self,
        project_id: &str,
        tombstones: &[AgentErasureTombstone],
    ) -> StoreResult<usize> {
        if self.closed.load(Ordering::Acquire) || self.config.read_only || tombstones.is_empty() {
            return Ok(0);
        }
        let Some(state) = self.projects.get(project_id).map(|e| e.value().clone()) else {
            return Ok(0);
        };
        if state.fenced.load(Ordering::Acquire) || !self.is_owned_locally(project_id).await {
            return Ok(0);
        }
        let effective: Vec<AgentErasureTombstone> = tombstones
            .iter()
            .cloned()
            .map(|mut tb| {
                tb.expires_at = None;
                tb
            })
            .collect();
        let removed = state.memtable.purge_erased(&effective);
        state
            .l0_bytes
            .store(state.memtable.byte_stats().0, Ordering::Release);
        Ok(removed)
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
        if state.fenced.load(Ordering::Acquire) {
            return Err(StoreError::NotWriter {
                project_id: state.project_id.clone(),
            });
        }
        self.check_l0_backpressure(&state)?;
        let dropped_spans = self.enforce_trace_limits(&state, &mut batch);
        if batch.traces.is_empty() && batch.spans.is_empty() {
            return Ok(TraceIngestResponse {
                accepted_traces: 0,
                accepted_spans: 0,
                truncated_payloads: 0,
                redacted_fields: 0,
                dropped_spans,
                warnings: Vec::new(),
            });
        }
        let writer = state.wal_writer.as_ref().ok_or_else(|| {
            StoreError::Corrupted("missing WAL writer on writable project state".to_string())
        })?;
        writer.write(batch).await?;

        state
            .last_ingest_ms
            .store(chrono::Utc::now().timestamp_millis(), Ordering::Release);
        state.dirty_since_compact.store(true, Ordering::Release);

        // Flush on size from the append path; the periodic tick alone lets a burst pile up.
        let (total, largest_shard) = state.memtable.byte_stats();
        state.l0_bytes.store(total, Ordering::Release);
        if largest_shard >= self.config.l0_flush_max_bytes {
            self.flush_notify.notify_one();
        }

        Ok(TraceIngestResponse {
            accepted_traces,
            accepted_spans: accepted_spans - dropped_spans,
            truncated_payloads: 0,
            redacted_fields: 0,
            dropped_spans,
            warnings: Vec::new(),
        })
    }

    /// Drops a trace's spans from `batch` once that trace has reached its span or byte cap.
    /// Returns how many spans were dropped. The trace header is kept: it merges, it does not grow.
    // ponytail: read-then-insert, so concurrent batches for one trace can overshoot by a batch.
    fn enforce_trace_limits(
        &self,
        state: &ProjectAgentState,
        batch: &mut TraceIngestBatch,
    ) -> usize {
        let mut incoming: HashMap<&str, (usize, usize)> = HashMap::new();
        for sp in &batch.spans {
            let e = incoming.entry(sp.trace_id.as_str()).or_default();
            e.0 += 1;
            e.1 += memtable::approx_span_bytes(sp);
        }
        let mut over: HashSet<String> = HashSet::new();
        for (tid, (spans, bytes)) in incoming {
            let (used_spans, used_bytes) = state.trace_usage.get(tid).unwrap_or_default();
            let total = (used_spans + spans, used_bytes + bytes);
            if total.0 > self.config.max_trace_spans || total.1 > self.config.max_trace_bytes {
                over.insert(tid.to_string());
            } else {
                state.trace_usage.insert(tid.to_string(), total);
            }
        }
        if over.is_empty() {
            return 0;
        }
        let before = batch.spans.len();
        batch.spans.retain(|sp| !over.contains(&sp.trace_id));
        before - batch.spans.len()
    }

    /// Refuses ingest while this project, or the node as a whole, holds more unflushed bytes
    /// than its limit, and asks for a flush so the condition clears.
    // ponytail: requests already queued for the WAL are not counted, so the overshoot is up to
    // one in-flight request per connection. Reserve bytes at admission if that ever matters.
    fn check_l0_backpressure(&self, state: &ProjectAgentState) -> StoreResult<()> {
        let project = state.l0_bytes.load(Ordering::Acquire);
        let (bytes, max_bytes) = if project >= self.config.l0_max_bytes_project {
            (project, self.config.l0_max_bytes_project)
        } else {
            let node: usize = self
                .projects
                .iter()
                .map(|e| e.value().l0_bytes.load(Ordering::Acquire))
                .sum();
            if node < self.config.l0_max_bytes_node {
                return Ok(());
            }
            (node, self.config.l0_max_bytes_node)
        };
        self.flush_all_next.store(true, Ordering::Release);
        self.flush_notify.notify_one();
        Err(StoreError::L0Backpressure {
            project_id: state.project_id.clone(),
            bytes,
            max_bytes,
        })
    }

    /// Resolves when the append path wants a flush before the next periodic tick. The node's
    /// flush task waits on this alongside its timer.
    pub async fn flush_requested(&self) {
        self.flush_notify.notified().await;
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
        let force_all = force_all || self.flush_all_next.swap(false, Ordering::AcqRel);
        let states: Vec<Arc<ProjectAgentState>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        // Every project gets its turn: one failing project must not leave the others'
        // acknowledged spans in memory. The first error is still reported.
        let mut first_err = None;
        let eager_upload = self.is_local_hot_tier() && self.upload_interval().is_zero();
        for state in states {
            if state.fenced.load(Ordering::Acquire)
                || !self.is_owned_locally(&state.project_id).await
            {
                continue;
            }
            match self.flush_project_l0(&state, force_all).await {
                Ok(committed) => {
                    if committed && eager_upload {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        match self
                            .upload_project_at(&state.project_id, now_ms, false)
                            .await
                        {
                            Ok(_) => {}
                            Err(StoreError::NotOwner { .. } | StoreError::NotWriter { .. }) => {}
                            Err(e) => {
                                first_err.get_or_insert(e);
                            }
                        }
                    }
                }
                Err(StoreError::NotOwner { .. } | StoreError::NotWriter { .. }) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    async fn flush_project_l0(
        &self,
        state: &Arc<ProjectAgentState>,
        force_all: bool,
    ) -> StoreResult<bool> {
        let res = self.flush_project_l0_inner(state, force_all).await;
        state
            .l0_bytes
            .store(state.memtable.byte_stats().0, Ordering::Release);
        res
    }

    async fn flush_project_l0_inner(
        &self,
        state: &Arc<ProjectAgentState>,
        force_all: bool,
    ) -> StoreResult<bool> {
        let _guard = state.flush_lock.lock().await;
        if state.fenced.load(Ordering::Acquire) {
            return Err(StoreError::NotWriter {
                project_id: state.project_id.clone(),
            });
        }
        let seg_prefix = if self.is_local_hot_tier() {
            "hot/agent_segments"
        } else {
            "agent_segments"
        };
        for _attempt in 0..manifest::MAX_MANIFEST_CAS_RETRIES {
            if state.memtable.total_traces() == 0 && !state.memtable.has_purged_wal() {
                return Ok(false);
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
            // Stage and write one shard at a time, handing the staged rows to the writer, so a
            // flush holds one extra copy of one shard rather than two copies of all of them.
            let mut staged_list: Vec<StagedShardFlush> = Vec::with_capacity(eligible.len());
            let mut staged_trace_ids: Vec<Vec<String>> = Vec::with_capacity(eligible.len());
            let mut new_segments = Vec::new();
            for (date, shard_id) in eligible {
                let Some(mut sf) = state.memtable.stage_shard(date, shard_id) else {
                    continue;
                };
                let entries = std::mem::take(&mut sf.entries);
                staged_trace_ids.push(
                    entries
                        .iter()
                        .map(|(tr, _, _)| tr.trace_id.clone())
                        .collect(),
                );
                let date_str = sf.date.format("%Y-%m-%d").to_string();
                match SegmentWriter::write_segments_split_ranked_with_prefix_and_mode(
                    seg_prefix,
                    &state.project_id,
                    &date_str,
                    sf.shard_id,
                    0,
                    entries,
                    self.config.max_segment_bytes,
                    &self.object_store,
                    PutMode::Overwrite,
                    None,
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
                staged_list.push(sf);
            }
            if staged_list.is_empty() {
                let mut any_wal_purged = false;
                if force_all || state.memtable.has_purged_wal() {
                    let mut advanced_wids: Vec<(String, u64)> = Vec::new();
                    let committed_manifest = state
                        .manifest_store
                        .commit_with_cas(|m| {
                            advanced_wids.clear();
                            let safe_wal = state.memtable.compute_safe_committed_wal_seq(
                                &m.committed_wal_seq,
                                &[],
                                &self.writer_id_str,
                            );
                            let mut changed = false;
                            for (wid, seq) in safe_wal {
                                let prev = m.committed_wal_seq.get(&wid).copied().unwrap_or(0);
                                if seq > prev {
                                    m.committed_wal_seq.insert(wid.clone(), seq);
                                    advanced_wids.push((wid, seq));
                                    changed = true;
                                }
                            }
                            if changed {
                                m.updated_at_ms = chrono::Utc::now().timestamp_millis();
                            }
                            Ok(changed)
                        })
                        .await?;
                    for (wid, seq) in &advanced_wids {
                        if *seq > 0 {
                            let _ = state.wal_store.purge_up_to(wid, *seq).await?;
                            any_wal_purged = true;
                        }
                    }
                    if let Some(&seq) = committed_manifest
                        .committed_wal_seq
                        .get(&self.writer_id_str)
                    {
                        if seq > 0 && !advanced_wids.iter().any(|(w, _)| w == &self.writer_id_str) {
                            let _ = state
                                .wal_store
                                .purge_up_to(&self.writer_id_str, seq)
                                .await?;
                        }
                    }
                    state.memtable.clear_purged_wal_flag();
                }
                return Ok(any_wal_purged);
            }

            let mut retry_needed = false;
            let mut advanced_wids: Vec<(String, u64)> = Vec::new();
            let commit_res = state
                .manifest_store
                .commit_with_cas(|m| {
                    retry_needed = false;
                    advanced_wids.clear();
                    if m.epoch != pre_manifest.epoch {
                        let newly_erased_staged = staged_trace_ids.iter().flatten().any(|tid| {
                            m.erased_traces.contains_key(tid)
                                && !pre_manifest.erased_traces.contains_key(tid)
                        });
                        let committed_while_staging =
                            staged_list.iter().zip(&staged_trace_ids).any(|(sf, ids)| {
                                ids.iter().any(|tid| {
                                    sf.staged_wal_seqs.get(tid).is_some_and(|wmap| {
                                        !wmap.is_empty()
                                            && wmap.iter().all(|(wid, seqs)| {
                                                let c = m
                                                    .committed_wal_seq
                                                    .get(wid)
                                                    .copied()
                                                    .unwrap_or(0);
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
            state.dirty_since_compact.store(true, Ordering::Release);
            return Ok(true);
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
            let now_ms = chrono::Utc::now().timestamp_millis();
            for state in &states {
                if state.fenced.load(Ordering::Acquire)
                    || !self.is_owned_locally(&state.project_id).await
                {
                    continue;
                }
                match self.flush_project_l0(state, true).await {
                    Ok(_) => {}
                    Err(
                        StoreError::NotWriter { .. }
                        | StoreError::NotOwner { .. }
                        | StoreError::StaleWriter { .. },
                    ) => continue,
                    Err(e) => return Err(e),
                }
                if self.is_local_hot_tier() && !state.fenced.load(Ordering::Acquire) {
                    self.upload_project_state(state, now_ms, true, &[], None)
                        .await?;
                }
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
            let st = existing.value().clone();
            if !self.config.read_only && st.fenced.load(Ordering::Acquire) {
                return Err(StoreError::NotWriter {
                    project_id: st.project_id.clone(),
                });
            }
            return Ok(st);
        }

        let _init_guard = self.project_init_lock.lock().await;
        if let Some(existing) = self.projects.get(project_id) {
            let st = existing.value().clone();
            if !self.config.read_only && st.fenced.load(Ordering::Acquire) {
                return Err(StoreError::NotWriter {
                    project_id: st.project_id.clone(),
                });
            }
            return Ok(st);
        }

        if !self.config.read_only {
            self.ensure_writable_owner(project_id).await?;
        }

        let is_local_hot = self.is_local_hot_tier();
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

        if is_local_hot && self.config.read_only {
            // `NodeRole::Query` reads S3 `agent_published/{project_id}/` on demand; no local WAL replay.
            let state = Arc::new(ProjectAgentState {
                project_id: project_id.to_string(),
                manifest_store,
                wal_store,
                memtable,
                wal_writer: None,
                applied_wal_seq: AsyncMutex::new(HashMap::new()),
                flush_lock: AsyncMutex::new(()),
                publish_lock: AsyncMutex::new(()),
                fenced: Arc::new(AtomicBool::new(false)),
                last_published_seq: parking_lot::Mutex::new(None),
                last_published_s3_segments: parking_lot::Mutex::new(None),
                last_published_erased_traces: parking_lot::Mutex::new(None),
                has_pending_upload_intent: AtomicBool::new(false),
                pending_local_deletes: parking_lot::Mutex::new(HashSet::new()),
                closed_date_pending_since_ms: parking_lot::Mutex::new(HashMap::new()),
                last_ingest_ms: AtomicI64::new(0),
                dirty_since_compact: AtomicBool::new(false),
                query_cached_publish: parking_lot::Mutex::new(None),
                l0_bytes: AtomicUsize::new(0),
                trace_usage: moka::sync::Cache::builder()
                    .max_capacity(100_000)
                    .time_to_idle(Duration::from_secs(3600))
                    .build(),
            });
            self.projects.insert(project_id.to_string(), state.clone());
            return Ok(state);
        }

        let mut manifest = manifest_store.load_latest().await?;
        let mut latest_published: Option<PublishedAgentManifest> = None;

        if is_local_hot && !self.config.read_only && manifest.fenced {
            return Err(StoreError::StaleWriter {
                project_id: project_id.to_string(),
                pub_seq: manifest.last_published_seq.unwrap_or(0),
                pub_gen: manifest.writer_generation,
                local_last_pub_seq: manifest.last_published_seq,
                local_gen: manifest.writer_generation,
            });
        }

        if is_local_hot
            && !self.config.read_only
            && (manifest.epoch > 0 || manifest.last_published_seq.is_some())
        {
            let published_store =
                PublishedAgentManifestStore::new(project_id, self.object_store.clone())?;
            latest_published = published_store.load_latest().await?;
            let grace_ms =
                (self.upload_interval_ms.load(Ordering::Acquire) as i64).saturating_mul(2);

            if let Some(ref pub_m) = latest_published {
                let local_gen = manifest.writer_generation;
                let local_epoch = manifest.epoch;
                let local_last_pub_seq = manifest.last_published_seq;

                if pub_m.writer_generation > local_gen {
                    return Err(StoreError::StaleWriter {
                        project_id: project_id.to_string(),
                        pub_seq: pub_m.publish_seq,
                        pub_gen: pub_m.writer_generation,
                        local_last_pub_seq,
                        local_gen,
                    });
                }
                let seq_ahead = local_last_pub_seq.is_none_or(|s| pub_m.publish_seq > s);
                if seq_ahead {
                    let expected_next = local_last_pub_seq.map_or(0, |s| s.saturating_add(1));
                    let local_intent =
                        AgentUploadIntentStore::new(project_id, self.object_store.clone())?
                            .load()
                            .await?;
                    let intent_ok = match pub_m.intent_id.as_deref() {
                        Some(pub_intent_id) => local_intent
                            .as_ref()
                            .is_some_and(|i| i.intent_id == pub_intent_id),
                        None => local_last_pub_seq.is_some(),
                    };
                    let is_step6 = pub_m.publish_seq == expected_next
                        && pub_m.writer_generation == local_gen
                        && pub_m.source_local_epoch <= local_epoch
                        && local_epoch > 0
                        && intent_ok;
                    if !is_step6 {
                        return Err(StoreError::StaleWriter {
                            project_id: project_id.to_string(),
                            pub_seq: pub_m.publish_seq,
                            pub_gen: pub_m.writer_generation,
                            local_last_pub_seq,
                            local_gen,
                        });
                    }

                    let mut step6_prev_pub: Option<PublishedAgentManifest> = None;
                    if pub_m.publish_seq > 0 {
                        match published_store.load_seq(pub_m.publish_seq - 1).await {
                            Ok(prev) => step6_prev_pub = Some(prev),
                            Err(StoreError::ObjectStore(object_store::Error::NotFound {
                                ..
                            })) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    apply_step6_to_agent_manifest(
                        &mut manifest,
                        pub_m,
                        step6_prev_pub.as_ref(),
                        grace_ms,
                    );
                    manifest = manifest_store.commit_next(manifest).await?;
                }
            }

            let _ = reconcile_agent_upload_intent(
                project_id,
                &self.object_store,
                &manifest,
                latest_published.as_ref(),
            )
            .await?;
            sweep_local_hot_agent_segments(
                project_id,
                &self.object_store,
                &manifest,
                &self.pin_tracker,
            )
            .await?;
        }

        let mut applied = HashMap::new();
        let replayed = wal_store
            .replay_uncommitted(
                &manifest.committed_wal_seq,
                &mut applied,
                !self.config.read_only,
            )
            .await?;
        let mut replayed_any = false;
        for (wid, seq, mut batch) in replayed {
            replayed_any = true;
            let had_erased = filter_batch_erased(&mut batch, &manifest.erased_traces);
            if wid != self.writer_id_str {
                if is_local_hot && !self.config.read_only {
                    memtable.record_recovered_foreign_wal_seq(&wid, seq, had_erased);
                } else if had_erased {
                    memtable.record_blocked_foreign_wal_seq(&wid, seq);
                }
            }
            if batch.traces.is_empty() && batch.spans.is_empty() {
                if wid == self.writer_id_str {
                    memtable.record_accounted_wal_seq(&wid, seq);
                }
            } else {
                memtable.insert_batch(&wid, seq, batch)?;
            }
        }

        let memtable_bytes = memtable.byte_stats().0;
        let fenced = Arc::new(AtomicBool::new(false));
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
            let writer = AgentWalWriter::spawn_with_fence(
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
                fenced.clone(),
            )?;
            Some(Arc::new(writer))
        };

        let initial_pub_seq = manifest.last_published_seq;
        let initial_pub_s3 = latest_published
            .as_ref()
            .map(|p| sorted_s3_segment_paths(&p.segments));
        let initial_pub_erased = latest_published.as_ref().map(|p| p.erased_traces.clone());
        let dirty_init = replayed_any || memtable.total_traces() > 0 || memtable.has_purged_wal();

        let state = Arc::new(ProjectAgentState {
            project_id: project_id.to_string(),
            manifest_store,
            wal_store,
            memtable,
            wal_writer,
            applied_wal_seq: AsyncMutex::new(applied),
            flush_lock: AsyncMutex::new(()),
            publish_lock: AsyncMutex::new(()),
            fenced,
            last_published_seq: parking_lot::Mutex::new(initial_pub_seq),
            last_published_s3_segments: parking_lot::Mutex::new(initial_pub_s3),
            last_published_erased_traces: parking_lot::Mutex::new(initial_pub_erased),
            has_pending_upload_intent: AtomicBool::new(false),
            pending_local_deletes: parking_lot::Mutex::new(HashSet::new()),
            closed_date_pending_since_ms: parking_lot::Mutex::new(HashMap::new()),
            last_ingest_ms: AtomicI64::new(if replayed_any {
                chrono::Utc::now().timestamp_millis()
            } else {
                0
            }),
            dirty_since_compact: AtomicBool::new(dirty_init),
            query_cached_publish: parking_lot::Mutex::new(None),
            l0_bytes: AtomicUsize::new(memtable_bytes),
            trace_usage: moka::sync::Cache::builder()
                .max_capacity(100_000)
                .time_to_idle(Duration::from_secs(3600))
                .build(),
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

    pub(crate) fn invalidate_query_publish_cache(&self, project_id: &str) {
        if let Some(state) = self.projects.get(project_id) {
            *state.query_cached_publish.lock() = None;
        }
    }

    pub(crate) async fn acquire_snapshot(
        &self,
        project_id: &str,
    ) -> StoreResult<AgentStorageSnapshot> {
        let state = self.get_or_open_project(project_id).await?;
        let manifest = if self.config.read_only {
            if self.is_local_hot_tier() {
                let pub_store =
                    PublishedAgentManifestStore::new(project_id, self.object_store.clone())?;
                match pub_store.latest_pub_seq().await? {
                    Some((seq, meta)) => {
                        let cached = {
                            let guard = state.query_cached_publish.lock();
                            guard
                                .as_ref()
                                .filter(|(cseq, _)| *cseq == seq)
                                .map(|(_, mf)| mf.clone())
                        };
                        if let Some(mf) = cached {
                            mf
                        } else {
                            let pub_m = pub_store.load_from_meta(&meta).await?;
                            let mf =
                                Arc::new(pub_m.to_local_manifest(seq, pub_m.writer_generation));
                            *state.query_cached_publish.lock() = Some((seq, mf.clone()));
                            mf
                        }
                    }
                    None => {
                        *state.query_cached_publish.lock() = None;
                        Arc::new(AgentManifest::default())
                    }
                }
            } else {
                Arc::new(self.refresh_from_wal(project_id).await?)
            }
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
        if self.is_local_hot_tier() && self.config.read_only {
            let prefix = ObjectPath::from("agent_published/");
            let res = self.object_store.list_with_delimiter(Some(&prefix)).await?;
            for cp in res.common_prefixes {
                if let Some(pid) = cp.parts().nth(1).map(|p| p.as_ref().to_string()) {
                    if validate_identifier("project_id", &pid).is_ok() {
                        projects.insert(pid);
                    }
                }
            }
            return Ok(projects.into_iter().collect());
        }
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

    pub(crate) async fn local_project_has_manifest_or_wal(
        &self,
        project_id: &str,
    ) -> StoreResult<bool> {
        if self.projects.contains_key(project_id) {
            return Ok(true);
        }
        for p in [
            ObjectPath::from(format!("agent_manifests/{project_id}/")),
            ObjectPath::from(format!("agent_wal/{project_id}/")),
        ] {
            let mut stream = self.object_store.list(Some(&p));
            if futures::StreamExt::next(&mut stream)
                .await
                .transpose()?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
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

    /// Uploads eligible local `hot/agent_segments/` and publishes dense `PublishedAgentManifest`
    /// objects to `agent_published/` across all known writable projects. Makes **0 S3 calls**
    /// for idle projects or projects holding only deferred closed-date segments.
    pub async fn publish_all_projects(&self) -> StoreResult<usize> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StoreError::Shutdown);
        }
        if self.config.read_only || !self.is_local_hot_tier() {
            return Ok(0);
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        let pids = self.list_known_projects().await?;
        let mut published_count = 0usize;
        let mut first_err: Option<StoreError> = None;
        for pid in pids {
            if !self.is_owned_locally(&pid).await {
                continue;
            }
            match self.upload_project_at(&pid, now_ms, false).await {
                Ok(Some(_)) => published_count += 1,
                Ok(None) => {}
                Err(StoreError::NotOwner { .. }) => {}
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(published_count)
    }

    pub async fn upload_project_at(
        &self,
        project_id: &str,
        now_ms: i64,
        force_closed: bool,
    ) -> StoreResult<Option<PublishedAgentManifest>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StoreError::Shutdown);
        }
        if self.config.read_only || !self.is_local_hot_tier() {
            return Ok(None);
        }
        let state = self.get_or_open_project(project_id).await?;
        self.upload_project_state(&state, now_ms, force_closed, &[], None)
            .await
    }

    pub(crate) async fn upload_project_state(
        &self,
        state: &Arc<ProjectAgentState>,
        now_ms: i64,
        force_closed: bool,
        extra_tombstones: &[AgentErasureTombstone],
        urgent_deadlines: Option<&HashMap<String, Option<i64>>>,
    ) -> StoreResult<Option<PublishedAgentManifest>> {
        let _pub_guard = state.publish_lock.lock().await;
        self.upload_project_locked(
            state,
            now_ms,
            force_closed,
            extra_tombstones,
            urgent_deadlines,
        )
        .await
    }

    pub(crate) async fn upload_project_locked(
        &self,
        state: &Arc<ProjectAgentState>,
        now_ms: i64,
        force_closed: bool,
        extra_tombstones: &[AgentErasureTombstone],
        urgent_deadlines: Option<&HashMap<String, Option<i64>>>,
    ) -> StoreResult<Option<PublishedAgentManifest>> {
        if self.config.read_only || !self.is_local_hot_tier() {
            return Ok(None);
        }
        if state.fenced.load(Ordering::Acquire) {
            return Err(StoreError::NotWriter {
                project_id: state.project_id.clone(),
            });
        }

        self.drain_pending_local_deletes(state).await;

        if state.has_pending_upload_intent.load(Ordering::Acquire) {
            let cur_mf = state.manifest_store.load_latest().await?;
            let stale_local =
                reconcile_agent_upload_intent(&state.project_id, &self.object_store, &cur_mf, None)
                    .await?;
            if !stale_local.is_empty() {
                state.pending_local_deletes.lock().extend(stale_local);
            }
            self.drain_pending_local_deletes(state).await;
            state
                .has_pending_upload_intent
                .store(false, Ordering::Release);
        }

        let cur_mf = state.manifest_store.load_latest().await?;
        let frozen_groups =
            self.select_eligible_local_segments(state, &cur_mf, now_ms, force_closed);
        let cur_s3_segs = sorted_s3_segment_paths(&cur_mf.segments);

        let s3_state_dirty = match &*state.last_published_s3_segments.lock() {
            Some(prev) => *prev != cur_s3_segs,
            None => !cur_s3_segs.is_empty(),
        };
        let erased_dirty = match &*state.last_published_erased_traces.lock() {
            Some(prev) => *prev != cur_mf.erased_traces,
            None => !cur_mf.erased_traces.is_empty() && !cur_s3_segs.is_empty(),
        };
        let has_unstamped_s3_deletes = cur_mf
            .pending_s3_deletes
            .iter()
            .any(|d| d.eligible_after_ms.is_none());

        if frozen_groups.is_empty() && !s3_state_dirty && !erased_dirty && !has_unstamped_s3_deletes
        {
            let has_ready_s3_deletes = cur_mf.pending_s3_deletes.iter().any(|d| {
                d.eligible_after_ms.is_some_and(|t| now_ms >= t)
                    && !self.pin_tracker.is_pinned(&d.object_path)
            });
            if has_ready_s3_deletes {
                let _ = state
                    .manifest_store
                    .run_gc(now_ms, &self.pin_tracker)
                    .await?;
            }
            return Ok(None);
        }

        let publish_seq = state
            .last_published_seq
            .lock()
            .map_or(0, |s| s.saturating_add(1));

        struct PlannedBatch {
            date: String,
            shard_id: u16,
            target_level: u8,
            sources: Vec<SegmentDescriptor>,
            target_segment_prefix: String,
        }

        let max_input_bytes = self
            .upload_merge_max_input_bytes
            .load(Ordering::Acquire)
            .min(self.config.max_segment_bytes as u64)
            .max(1);
        let mut planned_batches: Vec<PlannedBatch> = Vec::new();
        for ((date, shard_id), mut segs) in frozen_groups {
            segs.sort_by(|a, b| a.segment_id.cmp(&b.segment_id));
            let mut chunk: Vec<SegmentDescriptor> = Vec::new();
            let mut chunk_bytes: u64 = 0;
            for seg in segs {
                if !chunk.is_empty() && chunk_bytes.saturating_add(seg.file_size) > max_input_bytes
                {
                    let target_level = chunk.iter().map(|s| s.level).max().unwrap_or(0);
                    planned_batches.push(PlannedBatch {
                        date: date.clone(),
                        shard_id,
                        target_level,
                        sources: std::mem::take(&mut chunk),
                        target_segment_prefix: format!("seg_up_{}", Uuid::new_v4().simple()),
                    });
                    chunk_bytes = 0;
                }
                chunk_bytes = chunk_bytes.saturating_add(seg.file_size);
                chunk.push(seg);
            }
            if !chunk.is_empty() {
                let target_level = chunk.iter().map(|s| s.level).max().unwrap_or(0);
                planned_batches.push(PlannedBatch {
                    date,
                    shard_id,
                    target_level,
                    sources: chunk,
                    target_segment_prefix: format!("seg_up_{}", Uuid::new_v4().simple()),
                });
            }
        }

        let intent_store =
            AgentUploadIntentStore::new(&state.project_id, self.object_store.clone())?;
        let intent = AgentUploadIntent {
            intent_id: format!("intent_{}", Uuid::new_v4().simple()),
            writer_generation: cur_mf.writer_generation,
            base_local_epoch: cur_mf.epoch,
            target_publish_seq: publish_seq,
            created_at_ms: now_ms,
            batches: planned_batches
                .iter()
                .map(|b| AgentUploadIntentBatch {
                    date: b.date.clone(),
                    shard_id: b.shard_id,
                    source_segment_ids: b.sources.iter().map(|s| s.segment_id.clone()).collect(),
                    source_object_paths: b.sources.iter().map(|s| s.object_path.clone()).collect(),
                    target_segment_prefix: b.target_segment_prefix.clone(),
                })
                .collect(),
        };
        state
            .has_pending_upload_intent
            .store(true, Ordering::Release);
        intent_store.save(&intent).await?;

        // Step 2: Merge frozen local segments per `(date, shard_id)` batch and write create-only to S3.
        let mut uploaded_outputs: Vec<Vec<SegmentDescriptor>> =
            Vec::with_capacity(planned_batches.len());
        if !planned_batches.is_empty() {
            let active_tombs: Vec<&AgentErasureTombstone> = extra_tombstones
                .iter()
                .filter(|tb| {
                    !tb.is_expired(now_ms)
                        && (tb.project_id.is_empty() || tb.project_id == state.project_id)
                })
                .collect();
            let reader = self.plss_reader();

            struct MergedAcc {
                trace: AgentTrace,
                meta: TraceCrdtMeta,
                spans: HashMap<String, (AgentSpan, SpanFieldRanks)>,
            }

            for batch in &planned_batches {
                let mut merged: HashMap<String, MergedAcc> = HashMap::new();
                for seg in &batch.sources {
                    let entries = reader.read_all_traces_and_spans_ranked(seg).await?;
                    for (tr, tr_meta, spans) in entries {
                        if cur_mf.erased_traces.contains_key(&tr.trace_id)
                            || (!active_tombs.is_empty()
                                && compactor::trace_matches_tombstones(&tr, &active_tombs))
                        {
                            continue;
                        }
                        let surviving_spans: Vec<(AgentSpan, SpanFieldRanks)> =
                            if cur_mf.erased_traces.is_empty() && active_tombs.is_empty() {
                                spans
                            } else {
                                spans
                                    .into_iter()
                                    .filter(|(sp, _)| {
                                        !cur_mf.erased_traces.contains_key(&sp.trace_id)
                                            && !compactor::span_matches_trace_tombstones(
                                                sp,
                                                &active_tombs,
                                            )
                                    })
                                    .collect()
                            };
                        match merged.get_mut(&tr.trace_id) {
                            Some(acc) => {
                                merge_trace_headers_with_meta(
                                    &mut acc.trace,
                                    &mut acc.meta,
                                    tr,
                                    tr_meta,
                                );
                                merge_spans_crdt_with_ranks(&mut acc.spans, surviving_spans);
                            }
                            None => {
                                let tid = tr.trace_id.clone();
                                let mut dst_spans = HashMap::with_capacity(surviving_spans.len());
                                merge_spans_crdt_with_ranks(&mut dst_spans, surviving_spans);
                                merged.insert(
                                    tid,
                                    MergedAcc {
                                        trace: tr,
                                        meta: tr_meta,
                                        spans: dst_spans,
                                    },
                                );
                            }
                        }
                    }
                }

                let mut merged_entries: Vec<RankedTraceEntry> = Vec::with_capacity(merged.len());
                for (_tid, acc) in merged {
                    let mut tr = acc.trace;
                    let mut ranked_spans: Vec<(AgentSpan, SpanFieldRanks)> =
                        acc.spans.into_values().collect();
                    ranked_spans.sort_by(|a, b| {
                        a.0.start_ts_ms
                            .cmp(&b.0.start_ts_ms)
                            .then_with(|| a.0.span_id.cmp(&b.0.span_id))
                    });
                    let sp_vec: Vec<AgentSpan> =
                        ranked_spans.iter().map(|(s, _)| s.clone()).collect();
                    recompute_trace_from_spans_with_explicit(
                        &mut tr,
                        acc.meta.explicit_status,
                        acc.meta.explicit_failure_source,
                        acc.meta.explicit_has_loop_warning,
                        acc.meta.explicit_primary_model.clone(),
                        acc.meta.explicit_primary_provider.clone(),
                        &sp_vec,
                    );
                    if cur_mf.erased_traces.contains_key(&tr.trace_id)
                        || (!active_tombs.is_empty()
                            && compactor::trace_matches_tombstones(&tr, &active_tombs))
                    {
                        continue;
                    }
                    merged_entries.push((tr, acc.meta, ranked_spans));
                }

                let written = if merged_entries.is_empty() {
                    Vec::new()
                } else {
                    SegmentWriter::write_segments_split_ranked_with_prefix_and_mode(
                        "agent_segments",
                        &state.project_id,
                        &batch.date,
                        batch.shard_id,
                        batch.target_level,
                        merged_entries,
                        self.config.max_segment_bytes,
                        &self.object_store,
                        PutMode::Create,
                        Some(&batch.target_segment_prefix),
                    )
                    .await?
                };
                uploaded_outputs.push(written);
            }
        }

        // Step 3: Commit local AgentManifest (frozen local segments out, new S3 segments in).
        let mut committed_mf = cur_mf.clone();
        if !planned_batches.is_empty() {
            #[derive(Default)]
            struct PartRepl {
                source_ids: HashSet<String>,
                source_paths: Vec<String>,
                new_segs: Vec<SegmentDescriptor>,
            }
            let mut by_part: BTreeMap<(String, u16), PartRepl> = BTreeMap::new();
            for (batch, new_segs) in planned_batches.iter().zip(uploaded_outputs.into_iter()) {
                let part = by_part
                    .entry((batch.date.clone(), batch.shard_id))
                    .or_default();
                for src in &batch.sources {
                    part.source_ids.insert(src.segment_id.clone());
                    part.source_paths.push(src.object_path.clone());
                }
                part.new_segs.extend(new_segs);
            }

            let mut rejected_s3_paths: Vec<String> = Vec::new();
            let mut accepted_local_paths: Vec<String> = Vec::new();
            let mut any_committed = false;

            committed_mf = state
                .manifest_store
                .commit_with_cas(|m| {
                    rejected_s3_paths.clear();
                    accepted_local_paths.clear();
                    any_committed = false;
                    let active_ids: HashSet<&str> =
                        m.segments.iter().map(|s| s.segment_id.as_str()).collect();
                    let mut remove_ids: HashSet<String> = HashSet::new();
                    let mut add_segs: Vec<SegmentDescriptor> = Vec::new();

                    for ((date, shard_id), repl) in &by_part {
                        let all_present = repl
                            .source_ids
                            .iter()
                            .all(|sid| active_ids.contains(sid.as_str()));
                        if all_present {
                            remove_ids.extend(repl.source_ids.iter().cloned());
                            add_segs.extend(repl.new_segs.iter().cloned());
                            accepted_local_paths.extend(repl.source_paths.iter().cloned());
                            if !repl.new_segs.is_empty() {
                                m.s3_partition_last_upload_ms
                                    .insert(partition_upload_key(date, *shard_id), now_ms);
                            }
                            any_committed = true;
                        } else {
                            rejected_s3_paths
                                .extend(repl.new_segs.iter().map(|s| s.object_path.clone()));
                        }
                    }
                    if !any_committed {
                        return Ok(false);
                    }
                    m.segments.retain(|s| !remove_ids.contains(&s.segment_id));
                    m.segments.extend(add_segs);
                    m.updated_at_ms = now_ms;
                    Ok(true)
                })
                .await?;

            if any_committed {
                for src_id in by_part.values().flat_map(|r| r.source_ids.iter()) {
                    self.cache.invalidate_segment(src_id);
                }
                state
                    .pending_local_deletes
                    .lock()
                    .extend(accepted_local_paths);
            }
            for rej in rejected_s3_paths {
                let _ = self
                    .object_store
                    .delete(&ObjectPath::from(rej.as_str()))
                    .await;
            }
        }

        // Step 4: Publish `PublishedAgentManifest` (S3 segments only, create-only + head(seq-1) guard).
        let s3_segments: Vec<SegmentDescriptor> = committed_mf
            .segments
            .iter()
            .filter(|s| !s.object_path.starts_with("hot/agent_segments/"))
            .cloned()
            .collect();
        let pub_ts_ms = chrono::Utc::now().timestamp_millis();
        let pub_manifest = PublishedAgentManifest {
            publish_seq,
            writer_generation: committed_mf.writer_generation,
            source_local_epoch: committed_mf.epoch.max(1),
            published_at_ms: pub_ts_ms,
            intent_id: Some(intent.intent_id.clone()),
            segments: s3_segments.clone(),
            committed_wal_seq: committed_mf.committed_wal_seq.clone(),
            erased_traces: committed_mf.erased_traces.clone(),
        };

        let pub_store =
            PublishedAgentManifestStore::new(&state.project_id, self.object_store.clone())?;
        let published = match pub_store.publish_next(pub_manifest).await {
            Ok(p) => p,
            Err(StoreError::ManifestConflict) => {
                state.fenced.store(true, Ordering::Release);
                for path in committed_mf
                    .segments
                    .iter()
                    .filter(|s| !cur_mf.segments.iter().any(|c| c.segment_id == s.segment_id))
                    .map(|s| s.object_path.as_str())
                {
                    let _ = self.object_store.delete(&ObjectPath::from(path)).await;
                }
                let _ = intent_store.clear().await;
                state
                    .has_pending_upload_intent
                    .store(false, Ordering::Release);
                let _ = state
                    .manifest_store
                    .commit_with_cas(|m| {
                        m.fenced = true;
                        m.updated_at_ms = chrono::Utc::now().timestamp_millis();
                        Ok(true)
                    })
                    .await;
                return Err(StoreError::NotWriter {
                    project_id: state.project_id.clone(),
                });
            }
            Err(e) => return Err(e),
        };

        // Step 5: Record `last_published_seq` and stamp `pending_s3_deletes`.
        let normal_grace_ms =
            (self.upload_interval_ms.load(Ordering::Acquire) as i64).saturating_mul(2);
        let post_step6 = state
            .manifest_store
            .commit_with_cas(|m| {
                m.last_published_seq = Some(
                    m.last_published_seq
                        .map_or(publish_seq, |s| s.max(publish_seq)),
                );
                for entry in &mut m.pending_s3_deletes {
                    if let Some(deadlines) = urgent_deadlines {
                        for tb_id in &entry.erasure_tombstone_ids {
                            if let Some(Some(dl)) = deadlines.get(tb_id) {
                                entry.urgent_erasure_deadline_ms = Some(
                                    entry
                                        .urgent_erasure_deadline_ms
                                        .map_or(*dl, |cur| cur.min(*dl)),
                                );
                            }
                        }
                    }
                    if entry.superseding_pub_seq <= publish_seq && entry.eligible_after_ms.is_none()
                    {
                        entry.superseded_at_ms = Some(published.published_at_ms);
                        let grace = match entry.urgent_erasure_deadline_ms {
                            Some(dl) => compute_urgent_grace_ms(
                                published.published_at_ms,
                                dl,
                                normal_grace_ms,
                            ),
                            None => normal_grace_ms,
                        };
                        entry.eligible_after_ms =
                            Some(published.published_at_ms.saturating_add(grace));
                    }
                }
                m.updated_at_ms = published.published_at_ms;
                Ok(true)
            })
            .await?;

        *state.last_published_seq.lock() = Some(publish_seq);
        *state.last_published_s3_segments.lock() = Some(sorted_s3_segment_paths(&s3_segments));
        *state.last_published_erased_traces.lock() = Some(post_step6.erased_traces.clone());

        {
            let active_hot_dates: HashSet<String> = post_step6
                .segments
                .iter()
                .filter(|s| s.object_path.starts_with("hot/agent_segments/"))
                .map(|s| s.date.clone())
                .collect();
            state
                .closed_date_pending_since_ms
                .lock()
                .retain(|d, _| active_hot_dates.contains(d));
        }

        // Step 6: Run S3/local GC, drain unpinned local segments, and clear UploadIntent.
        if !post_step6.pending_gc.is_empty() || !post_step6.pending_s3_deletes.is_empty() {
            let gc_now_ms = chrono::Utc::now().timestamp_millis();
            state
                .manifest_store
                .run_gc(gc_now_ms, &self.pin_tracker)
                .await?;
        }
        self.drain_pending_local_deletes(state).await;
        intent_store.clear().await?;
        state
            .has_pending_upload_intent
            .store(false, Ordering::Release);

        Ok(Some(published))
    }

    fn select_eligible_local_segments(
        &self,
        state: &ProjectAgentState,
        manifest: &AgentManifest,
        now_ms: i64,
        force_closed: bool,
    ) -> BTreeMap<(String, u16), Vec<SegmentDescriptor>> {
        let today_utc = chrono::DateTime::from_timestamp_millis(now_ms)
            .unwrap_or_default()
            .date_naive();
        let upload_eager = self.upload_interval().is_zero();
        let closed_interval_ms = self.closed_date_upload_ms.load(Ordering::Acquire) as i64;
        let closed_max_bytes = self.closed_date_max_bytes.load(Ordering::Acquire);

        let mut by_date: BTreeMap<String, Vec<SegmentDescriptor>> = BTreeMap::new();
        for seg in &manifest.segments {
            if seg.object_path.starts_with("hot/agent_segments/") {
                by_date
                    .entry(seg.date.clone())
                    .or_default()
                    .push(seg.clone());
            }
        }

        let mut out: BTreeMap<(String, u16), Vec<SegmentDescriptor>> = BTreeMap::new();
        for (date, segs) in by_date {
            let parsed_date = NaiveDate::parse_from_str(&date, "%Y-%m-%d").ok();
            let is_active_or_future = parsed_date.is_none_or(|d| d >= today_utc);
            let eligible = if is_active_or_future || upload_eager || force_closed {
                true
            } else {
                let date_bytes: u64 = segs.iter().map(|s| s.file_size).sum();
                let first_seen = *state
                    .closed_date_pending_since_ms
                    .lock()
                    .entry(date.clone())
                    .or_insert(now_ms);
                let age_ms = now_ms.saturating_sub(first_seen);
                date_bytes > closed_max_bytes || age_ms >= closed_interval_ms
            };
            if eligible {
                for seg in segs {
                    out.entry((date.clone(), seg.shard_id))
                        .or_default()
                        .push(seg);
                }
            }
        }
        out
    }

    pub(crate) async fn drain_pending_local_deletes(
        &self,
        state: &Arc<ProjectAgentState>,
    ) -> usize {
        let candidates: Vec<String> = state.pending_local_deletes.lock().iter().cloned().collect();
        if candidates.is_empty() {
            return 0;
        }
        let mut deleted = 0usize;
        for path in candidates {
            if self.pin_tracker.is_pinned(&path) {
                continue;
            }
            let loc = ObjectPath::from(path.as_str());
            match self.object_store.delete(&loc).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    state.pending_local_deletes.lock().remove(&path);
                    deleted += 1;
                }
                Err(_) => {}
            }
        }
        deleted
    }

    pub async fn next_urgent_gc_deadline_ms(&self) -> Option<i64> {
        let mut min_deadline: Option<i64> = None;
        let states: Vec<Arc<ProjectAgentState>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        for state in states {
            let Ok(mf) = state.manifest_store.load_latest().await else {
                continue;
            };
            for entry in &mf.pending_gc {
                if !entry.erasure_tombstone_ids.is_empty()
                    || entry.urgent_erasure_deadline_ms.is_some()
                {
                    min_deadline = Some(
                        min_deadline
                            .map_or(entry.eligible_after_ms, |m| m.min(entry.eligible_after_ms)),
                    );
                }
            }
            for entry in &mf.pending_s3_deletes {
                if !entry.erasure_tombstone_ids.is_empty()
                    || entry.urgent_erasure_deadline_ms.is_some()
                {
                    if let Some(elig) = entry.eligible_after_ms {
                        min_deadline = Some(min_deadline.map_or(elig, |m| m.min(elig)));
                    }
                }
            }
        }
        min_deadline
    }

    pub fn has_dirty_projects_for_compact(&self) -> bool {
        self.projects.iter().any(|e| {
            let st = e.value();
            st.dirty_since_compact.load(Ordering::Acquire)
                || st.memtable.has_purged_wal()
                || !st.pending_local_deletes.lock().is_empty()
        })
    }

    /// Physically purges all traces/spans matching `tombstones` in `project_id` across L0, WAL,
    /// local hot segments, S3 segments, `AgentSegmentCache`, `agent_published/`, and pending GC.
    ///
    /// Returns the IDs of tombstones confirmed 100% physically absent across all tiers (including
    /// pending GC deletion) plus the earliest urgent GC deadline if any S3 deletion is still waiting
    /// on its pre-expiry grace window.
    pub async fn purge_erased_in_project(
        &self,
        project_id: &str,
        tombstones: &[AgentErasureTombstone],
    ) -> StoreResult<ProjectErasureOutcome> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StoreError::Shutdown);
        }
        if self.config.read_only || tombstones.is_empty() {
            return Ok(ProjectErasureOutcome::default());
        }
        validate_identifier("project_id", project_id)?;
        self.ensure_writable_owner(project_id).await?;

        let pass_started_at_ms = chrono::Utc::now().timestamp_millis();

        let is_local_hot = self.is_local_hot_tier();

        // Fast-path when `project_id` has no open state and no agent manifests/WALs anywhere:
        // zero traces exist, so tombstones created <= pass_started_at_ms are already physically absent.
        // If `agent_published/` exists in S3 without local `agent_manifests/` or `agent_wal/`,
        // refuse with `NotWriter` rather than opening an empty local manifest and falsely purging.
        if !self.projects.contains_key(project_id)
            && !self.local_project_has_manifest_or_wal(project_id).await?
        {
            let has_pub = if is_local_hot {
                PublishedAgentManifestStore::new(project_id, self.object_store.clone())?
                    .latest_pub_seq()
                    .await?
                    .is_some()
            } else {
                false
            };
            if !has_pub {
                let purged = tombstones
                    .iter()
                    .filter(|tb| tb.created_at <= pass_started_at_ms)
                    .map(|tb| tb.id.clone())
                    .collect();
                return Ok(ProjectErasureOutcome {
                    purged_tombstone_ids: purged,
                    next_urgent_gc_deadline_ms: None,
                    pass_started_at_ms,
                });
            }
            return Err(StoreError::NotWriter {
                project_id: project_id.to_string(),
            });
        }

        let state = self.get_or_open_project(project_id).await?;
        let _pub_guard = state.publish_lock.lock().await;

        let mut original_deadlines: HashMap<String, Option<i64>> =
            HashMap::with_capacity(tombstones.len());
        let mut effective_tombstones: Vec<AgentErasureTombstone> =
            Vec::with_capacity(tombstones.len());
        for tb in tombstones {
            original_deadlines.insert(tb.id.clone(), tb.expires_at);
            let mut eff = tb.clone();
            eff.expires_at = None;
            effective_tombstones.push(eff);
        }

        // 1. Purge L0 and force-flush surviving L0 so shared `.ptwl` WALs advance & delete.
        state.memtable.purge_erased_with_ids(&effective_tombstones);
        let _ = self.flush_project_l0(&state, true).await?;
        state.memtable.purge_erased_with_ids(&effective_tombstones);

        // 2. Check uncommitted WALs for any trace/span still matching `effective_tombstones`
        //    (e.g., concurrent live writer in `object` mode).
        let pre_mf = state.manifest_store.load_latest().await?;
        let mut dummy_applied = HashMap::new();
        let wal_batches = state
            .wal_store
            .replay_uncommitted(&pre_mf.committed_wal_seq, &mut dummy_applied, false)
            .await?;
        let eff_refs: Vec<&AgentErasureTombstone> = effective_tombstones.iter().collect();
        let mut wal_dirty_tbs: HashSet<String> = HashSet::new();
        let mut extra_erased_trace_ids: HashSet<String> = HashSet::new();
        for (_wid, _seq, batch) in wal_batches {
            for tr in &batch.traces {
                for tb in &effective_tombstones {
                    if tb.matches_trace(tr) {
                        wal_dirty_tbs.insert(tb.id.clone());
                        extra_erased_trace_ids.insert(tr.trace_id.clone());
                    }
                }
            }
            for sp in &batch.spans {
                for tb in &effective_tombstones {
                    if tb.level == ErasureLevel::Trace && tb.target_id == sp.trace_id {
                        wal_dirty_tbs.insert(tb.id.clone());
                        extra_erased_trace_ids.insert(sp.trace_id.clone());
                    }
                }
            }
        }

        // 3. Compact & strip matching local/S3 `.plss` segments, invalidate cache, tighten GC.
        compactor::compact_project_locked(
            self,
            &state,
            &effective_tombstones,
            Some(&original_deadlines),
            Some(&extra_erased_trace_ids),
            pass_started_at_ms,
        )
        .await?;

        // 3.5. Defense-in-depth for `restore --from-published` or crash leftovers in local hot tier:
        //      reconcile any unreferenced S3 `.plss` objects under `agent_segments/{project_id}/`
        //      into `pending_s3_deletes` with finite-TTL urgent deadlines before publishing/GC.
        if is_local_hot {
            self.reconcile_s3_orphans_for_erasure(&state, tombstones, pass_started_at_ms)
                .await?;
        }

        // 4. In local hot tier, immediately publish any S3 manifest/segment/deletion changes so
        //    `pending_s3_deletes` gets stamped and `agent_published/` drops old S3 segments, then run GC.
        let now_ms = chrono::Utc::now().timestamp_millis();
        if is_local_hot {
            let _ = self
                .upload_project_locked(
                    &state,
                    now_ms,
                    false,
                    &effective_tombstones,
                    Some(&original_deadlines),
                )
                .await?;
        }
        let post_mf = state.manifest_store.load_latest().await?;
        if !post_mf.pending_gc.is_empty() || !post_mf.pending_s3_deletes.is_empty() {
            let gc_now_ms = chrono::Utc::now().timestamp_millis();
            state
                .manifest_store
                .run_gc(gc_now_ms, &self.pin_tracker)
                .await?;
        }
        self.drain_pending_local_deletes(&state).await;

        // 5. Independently verify 0 matches across L0, WAL, active segments, S3 publish, and pending GC.
        let final_mf = state.manifest_store.load_latest().await?;
        let unreferenced_s3_paths: HashSet<String> = if is_local_hot {
            let intent_prefixes = self.active_intent_s3_prefixes(project_id).await?;
            self.list_unreferenced_s3_agent_segments(project_id, &final_mf, &intent_prefixes)
                .await?
                .into_iter()
                .collect()
        } else {
            HashSet::new()
        };
        let tracked_pending_s3_paths: HashSet<&str> = final_mf
            .pending_s3_deletes
            .iter()
            .map(|d| d.object_path.as_str())
            .collect();
        let l0_snap = state.memtable.snapshot_all();
        let reader = self.plss_reader();

        // Precompute segment tails + summaries only if any segment's Bloom hits.
        let mut seg_tails = Vec::with_capacity(final_mf.segments.len());
        for seg in &final_mf.segments {
            seg_tails.push((seg, reader.read_eof_tail(seg).await?));
        }

        let pub_up_to_date = if is_local_hot {
            let cur_s3 = sorted_s3_segment_paths(&final_mf.segments);
            let pub_s3 = state.last_published_s3_segments.lock().clone();
            let s3_dirty = match pub_s3 {
                Some(prev) => prev != cur_s3,
                None => !cur_s3.is_empty(),
            };
            !s3_dirty
                && !final_mf
                    .pending_s3_deletes
                    .iter()
                    .any(|d| d.eligible_after_ms.is_none())
        } else {
            true
        };

        let pending_local_still_present = !state.pending_local_deletes.lock().is_empty();
        let mut next_urgent_gc_deadline_ms: Option<i64> = None;
        for entry in &final_mf.pending_gc {
            if !entry.erasure_tombstone_ids.is_empty() || entry.urgent_erasure_deadline_ms.is_some()
            {
                next_urgent_gc_deadline_ms = Some(
                    next_urgent_gc_deadline_ms
                        .map_or(entry.eligible_after_ms, |m| m.min(entry.eligible_after_ms)),
                );
            }
        }
        for entry in &final_mf.pending_s3_deletes {
            if !entry.erasure_tombstone_ids.is_empty() || entry.urgent_erasure_deadline_ms.is_some()
            {
                if let Some(elig) = entry.eligible_after_ms {
                    next_urgent_gc_deadline_ms =
                        Some(next_urgent_gc_deadline_ms.map_or(elig, |m| m.min(elig)));
                }
            }
        }

        let mut purged_tombstone_ids = Vec::new();
        for tb in tombstones {
            if tb.created_at > pass_started_at_ms {
                continue;
            }
            if wal_dirty_tbs.contains(&tb.id) {
                continue;
            }
            if !pub_up_to_date || pending_local_still_present {
                continue;
            }
            if l0_snap.iter().any(|(tr, _)| tb.matches_trace(tr)) {
                continue;
            }
            let untracked_s3_orphan = unreferenced_s3_paths
                .iter()
                .any(|p| !tracked_pending_s3_paths.contains(p.as_str()));
            let finite_s3_pending = tb.expires_at.is_some()
                && (!final_mf.pending_s3_deletes.is_empty() || !unreferenced_s3_paths.is_empty());
            let pending_gc_blocks = untracked_s3_orphan
                || finite_s3_pending
                || final_mf.pending_gc.iter().any(|e| {
                    e.erasure_tombstone_ids.contains(&tb.id)
                        || (tb.expires_at.is_some() && e.superseded_at_ms <= tb.created_at)
                })
                || final_mf.pending_s3_deletes.iter().any(|e| {
                    e.erasure_tombstone_ids.contains(&tb.id)
                        || (tb.expires_at.is_some()
                            && (e.superseded_at_ms.is_none_or(|s| s <= tb.created_at)
                                || e.eligible_after_ms.is_none()))
                });
            if pending_gc_blocks {
                continue;
            }

            let mut seg_hit = false;
            for (seg, tail) in &seg_tails {
                let bloom_may = match tb.level {
                    ErasureLevel::Trace => tail.index.may_contain_trace(&tb.target_id),
                    ErasureLevel::Session => tail.index.may_contain_session(&tb.target_id),
                    ErasureLevel::User => tail.index.may_contain_user(&tb.target_id),
                };
                if !bloom_may {
                    continue;
                }
                let strings = reader.read_string_table(seg, tail).await?;
                let summaries = reader.read_trace_summaries(seg, tail).await?;
                for row in summaries.iter() {
                    let tid = strings.get_required(row.trace_id_str, "trace_id")?;
                    let sid = strings
                        .get_opt(row.session_id_str, "session_id")?
                        .filter(|s| !s.is_empty());
                    let uid = strings
                        .get_opt(row.user_id_str, "user_id")?
                        .filter(|s| !s.is_empty());
                    let matched = match tb.level {
                        ErasureLevel::Trace => tid == tb.target_id,
                        ErasureLevel::Session => sid == Some(tb.target_id.as_str()),
                        ErasureLevel::User => uid == Some(tb.target_id.as_str()),
                    };
                    if matched {
                        seg_hit = true;
                        break;
                    }
                }
                if seg_hit {
                    break;
                }
            }
            if !seg_hit {
                purged_tombstone_ids.push(tb.id.clone());
            }
        }
        let _ = eff_refs;

        Ok(ProjectErasureOutcome {
            purged_tombstone_ids,
            next_urgent_gc_deadline_ms,
            pass_started_at_ms,
        })
    }

    async fn active_intent_s3_prefixes(&self, project_id: &str) -> StoreResult<Vec<String>> {
        let intent_store = AgentUploadIntentStore::new(project_id, self.object_store.clone())?;
        let Some(intent) = intent_store.load().await? else {
            return Ok(Vec::new());
        };
        Ok(intent
            .batches
            .into_iter()
            .map(|b| {
                format!(
                    "agent_segments/{project_id}/{}/shard_{:02}/{}",
                    b.date, b.shard_id, b.target_segment_prefix
                )
            })
            .collect())
    }

    async fn list_unreferenced_s3_agent_segments(
        &self,
        project_id: &str,
        manifest: &AgentManifest,
        intent_prefixes: &[String],
    ) -> StoreResult<Vec<String>> {
        let active_paths: HashSet<&str> = manifest
            .segments
            .iter()
            .map(|s| s.object_path.as_str())
            .collect();
        let prefix = ObjectPath::from(format!("agent_segments/{project_id}/"));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut out = Vec::new();
        for m in metas {
            let p = m.location.as_ref();
            if !p.ends_with(".plss") {
                continue;
            }
            if active_paths.contains(p) {
                continue;
            }
            if intent_prefixes.iter().any(|pref| p.starts_with(pref)) {
                continue;
            }
            out.push(p.to_string());
        }
        Ok(out)
    }

    async fn reconcile_s3_orphans_for_erasure(
        &self,
        state: &Arc<ProjectAgentState>,
        tombstones: &[AgentErasureTombstone],
        now_ms: i64,
    ) -> StoreResult<()> {
        let cur_mf = state.manifest_store.load_latest().await?;
        let intent_prefixes = self.active_intent_s3_prefixes(&state.project_id).await?;
        let unreferenced = self
            .list_unreferenced_s3_agent_segments(&state.project_id, &cur_mf, &intent_prefixes)
            .await?;
        if unreferenced.is_empty() && cur_mf.pending_s3_deletes.is_empty() {
            return Ok(());
        }

        let unreferenced_set: HashSet<&str> = unreferenced.iter().map(String::as_str).collect();
        let normal_s3_grace_ms =
            (self.upload_interval_ms.load(Ordering::Acquire) as i64).saturating_mul(2);
        let finite_tombstones: Vec<(&AgentErasureTombstone, i64)> = tombstones
            .iter()
            .filter_map(|tb| tb.expires_at.map(|dl| (tb, dl)))
            .collect();
        let min_finite_dl = finite_tombstones.iter().map(|(_, dl)| *dl).min();
        let mut all_tb_ids: Vec<String> = tombstones.iter().map(|tb| tb.id.clone()).collect();
        all_tb_ids.sort();
        all_tb_ids.dedup();
        let max_tb_created_at = tombstones.iter().map(|tb| tb.created_at).max();

        state
            .manifest_store
            .commit_with_cas(|m| {
                let mut changed = false;
                let mut existing_paths: HashSet<String> = m
                    .pending_s3_deletes
                    .iter()
                    .map(|d| d.object_path.clone())
                    .collect();
                let next_pub_seq = m.last_published_seq.unwrap_or(0);

                for path in &unreferenced {
                    if existing_paths.insert(path.clone()) {
                        let seg_id = path
                            .rsplit('/')
                            .next()
                            .and_then(|f| f.strip_suffix(".plss"))
                            .unwrap_or(path.as_str())
                            .to_string();
                        m.pending_s3_deletes.push(PendingAgentS3Delete {
                            object_path: path.clone(),
                            segment_id: seg_id,
                            superseding_pub_seq: next_pub_seq,
                            superseded_at_ms: None,
                            eligible_after_ms: None,
                            urgent_erasure_deadline_ms: min_finite_dl,
                            erasure_tombstone_ids: all_tb_ids.clone(),
                        });
                        changed = true;
                    }
                }

                for entry in &mut m.pending_s3_deletes {
                    let still_in_s3 = unreferenced_set.contains(entry.object_path.as_str());
                    if !still_in_s3 {
                        continue;
                    }
                    let attach_all = entry.eligible_after_ms.is_none()
                        || entry.erasure_tombstone_ids.is_empty()
                        || max_tb_created_at
                            .is_some_and(|mc| entry.superseded_at_ms.is_none_or(|s| s <= mc));
                    if attach_all {
                        for id in &all_tb_ids {
                            if !entry.erasure_tombstone_ids.contains(id) {
                                entry.erasure_tombstone_ids.push(id.clone());
                                changed = true;
                            }
                        }
                    }
                    for &(tb, dl) in &finite_tombstones {
                        if entry.superseded_at_ms.is_none_or(|s| s <= tb.created_at)
                            || entry.eligible_after_ms.is_none()
                            || entry.erasure_tombstone_ids.contains(&tb.id)
                            || still_in_s3
                        {
                            if !entry.erasure_tombstone_ids.contains(&tb.id) {
                                entry.erasure_tombstone_ids.push(tb.id.clone());
                                changed = true;
                            }
                            let next_dl = entry
                                .urgent_erasure_deadline_ms
                                .map_or(dl, |cur| cur.min(dl));
                            if entry.urgent_erasure_deadline_ms != Some(next_dl) {
                                entry.urgent_erasure_deadline_ms = Some(next_dl);
                                changed = true;
                            }
                            if let (Some(sup_at), Some(cur_elig)) =
                                (entry.superseded_at_ms, entry.eligible_after_ms)
                            {
                                let eff_grace =
                                    compute_urgent_grace_ms(sup_at, next_dl, normal_s3_grace_ms);
                                let capped_elig = sup_at.saturating_add(eff_grace);
                                if capped_elig < cur_elig {
                                    entry.eligible_after_ms = Some(capped_elig);
                                    changed = true;
                                }
                            }
                        }
                    }
                }

                if changed {
                    m.updated_at_ms = now_ms;
                }
                Ok(changed)
            })
            .await?;

        Ok(())
    }
}

fn sorted_s3_segment_paths(segments: &[SegmentDescriptor]) -> Vec<String> {
    let mut paths: Vec<String> = segments
        .iter()
        .filter(|s| !s.object_path.starts_with("hot/agent_segments/"))
        .map(|s| s.object_path.clone())
        .collect();
    paths.sort();
    paths
}

fn apply_step6_to_agent_manifest(
    manifest: &mut AgentManifest,
    pub_m: &PublishedAgentManifest,
    prev_pub: Option<&PublishedAgentManifest>,
    normal_grace_ms: i64,
) {
    manifest.last_published_seq = Some(
        manifest
            .last_published_seq
            .map_or(pub_m.publish_seq, |s| s.max(pub_m.publish_seq)),
    );
    if let Some(prev) = prev_pub {
        let cur_s3: HashSet<&str> = pub_m
            .segments
            .iter()
            .map(|s| s.object_path.as_str())
            .collect();
        let mut pending_paths: HashSet<String> = manifest
            .pending_s3_deletes
            .iter()
            .map(|d| d.object_path.clone())
            .collect();
        for old_seg in &prev.segments {
            if !cur_s3.contains(old_seg.object_path.as_str())
                && pending_paths.insert(old_seg.object_path.clone())
            {
                manifest.pending_s3_deletes.push(PendingAgentS3Delete {
                    object_path: old_seg.object_path.clone(),
                    segment_id: old_seg.segment_id.clone(),
                    superseding_pub_seq: pub_m.publish_seq,
                    superseded_at_ms: Some(pub_m.published_at_ms),
                    eligible_after_ms: Some(pub_m.published_at_ms.saturating_add(normal_grace_ms)),
                    urgent_erasure_deadline_ms: None,
                    erasure_tombstone_ids: Vec::new(),
                });
            }
        }
    }
    for entry in &mut manifest.pending_s3_deletes {
        if entry.superseding_pub_seq <= pub_m.publish_seq && entry.eligible_after_ms.is_none() {
            entry.superseded_at_ms = Some(pub_m.published_at_ms);
            let grace = match entry.urgent_erasure_deadline_ms {
                Some(dl) => compute_urgent_grace_ms(pub_m.published_at_ms, dl, normal_grace_ms),
                None => normal_grace_ms,
            };
            entry.eligible_after_ms = Some(pub_m.published_at_ms.saturating_add(grace));
        }
    }
    manifest.updated_at_ms = pub_m.published_at_ms;
}

async fn reconcile_agent_upload_intent(
    project_id: &str,
    object_store: &Arc<dyn ObjectStore>,
    manifest: &AgentManifest,
    latest_published: Option<&PublishedAgentManifest>,
) -> StoreResult<Vec<String>> {
    let intent_store = AgentUploadIntentStore::new(project_id, object_store.clone())?;
    let Some(intent) = intent_store.load().await? else {
        return Ok(Vec::new());
    };

    let active_paths: HashSet<&str> = manifest
        .segments
        .iter()
        .map(|s| s.object_path.as_str())
        .collect();
    let mut stale_local_sources = Vec::new();
    for batch in &intent.batches {
        for src_path in &batch.source_object_paths {
            if src_path.starts_with("hot/agent_segments/")
                && !active_paths.contains(src_path.as_str())
            {
                stale_local_sources.push(src_path.clone());
            }
        }
    }

    let mut referenced_s3: HashSet<&str> = active_paths;
    if let Some(pub_m) = latest_published {
        for seg in &pub_m.segments {
            referenced_s3.insert(seg.object_path.as_str());
        }
    }
    for pending in &manifest.pending_s3_deletes {
        referenced_s3.insert(pending.object_path.as_str());
    }

    for batch in &intent.batches {
        let part_prefix = ObjectPath::from(format!(
            "agent_segments/{project_id}/{}/shard_{:02}/",
            batch.date, batch.shard_id
        ));
        let metas: Vec<_> = object_store.list(Some(&part_prefix)).try_collect().await?;
        for m in metas {
            let fname = m.location.as_ref().rsplit('/').next().unwrap_or("");
            if fname.starts_with(&batch.target_segment_prefix)
                && !referenced_s3.contains(m.location.as_ref())
            {
                match object_store.delete(&m.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(StoreError::ObjectStore(e)),
                }
            }
        }
    }

    intent_store.clear().await?;
    Ok(stale_local_sources)
}

async fn sweep_local_hot_agent_segments(
    project_id: &str,
    object_store: &Arc<dyn ObjectStore>,
    manifest: &AgentManifest,
    pin_tracker: &ReaderPinTracker,
) -> StoreResult<()> {
    let active_paths: HashSet<&str> = manifest
        .segments
        .iter()
        .map(|s| s.object_path.as_str())
        .collect();
    let prefix = ObjectPath::from(format!("hot/agent_segments/{project_id}/"));
    let metas: Vec<_> = object_store.list(Some(&prefix)).try_collect().await?;
    for m in metas {
        let p = m.location.as_ref();
        if !active_paths.contains(p) && !pin_tracker.is_pinned(p) {
            match object_store.delete(&m.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(StoreError::ObjectStore(e)),
            }
        }
    }
    Ok(())
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

    #[derive(Debug)]
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        calls: AtomicU64,
    }

    impl CountingObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                calls: AtomicU64::new(0),
            })
        }

        fn take_calls(&self) -> u64 {
            self.calls.swap(0, Ordering::AcqRel)
        }
    }

    impl std::fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingObjectStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<bytes::Bytes>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(
            &self,
            location: &ObjectPath,
        ) -> object_store::Result<object_store::ObjectMeta> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.head(location).await
        }

        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
        ) -> object_store::Result<()> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn local_hot_tier_flush_writes_only_local_cycle_uploads_fences_and_idles_with_zero_s3_calls(
    ) {
        let writer_dir = tempfile::tempdir().unwrap();
        let query_dir = tempfile::tempdir().unwrap();
        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s3 = CountingObjectStore::new(s3_raw.clone());

        let routed_writer =
            pulse_storage::with_hot_store_for_role(s3.clone(), writer_dir.path(), false)
                .await
                .unwrap();
        let enc_writer = Arc::new(EncryptedObjectStore::new(routed_writer));
        enc_writer.set_master_key(Some([0x22u8; 32]));

        let store = AgentTraceStore::open_with_encrypted(
            enc_writer.as_object_store(),
            Some(enc_writer.clone()),
            AgentTraceStoreConfig::default(),
        )
        .await
        .unwrap();
        assert!(store.is_local_hot_tier());
        store.set_upload_interval(Duration::from_secs(300));
        let _boot_calls = s3.take_calls();

        let now_ms = chrono::Utc::now().timestamp_millis();
        store
            .ingest_batch("proj_local", sample_batch("tr_loc_1", now_ms))
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        assert_eq!(
            s3.take_calls(),
            0,
            "local flush must make zero S3 calls when upload_interval > 0"
        );
        let s3_keys: Vec<_> = s3_raw.list(None).try_collect().await.unwrap();
        assert!(
            s3_keys.is_empty(),
            "S3 bucket must remain empty after local L0 flush: {s3_keys:?}"
        );

        // Idle compaction & prune before upload must make 0 S3 calls.
        store.compact_and_gc(None, &[]).await.unwrap();
        assert_eq!(
            s3.take_calls(),
            0,
            "idle compact_and_gc must make 0 S3 calls"
        );
        let old_cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(30);
        let _ = store
            .prune_expired_segments("proj_local", old_cutoff)
            .await
            .unwrap();
        assert_eq!(
            s3.take_calls(),
            0,
            "prune_expired_segments with no S3 segments must make 0 S3 calls"
        );

        // Explicit publish cycle uploads active today partition to S3 and deletes local hot segment.
        store.publish_all_projects().await.unwrap();
        assert!(s3.take_calls() > 0);
        let pub_store =
            PublishedAgentManifestStore::new("proj_local", enc_writer.as_object_store()).unwrap();
        let latest = pub_store.load_latest().await.unwrap().unwrap();
        assert_eq!(latest.publish_seq, 0);
        assert_eq!(latest.writer_generation, 0);
        assert_eq!(latest.segments.len(), 1);
        assert!(latest.segments[0]
            .object_path
            .starts_with("agent_segments/proj_local/"));
        let local_hot_objs: Vec<_> = enc_writer
            .as_object_store()
            .list(Some(&ObjectPath::from("hot/agent_segments/proj_local")))
            .try_collect()
            .await
            .unwrap();
        assert!(
            local_hot_objs.is_empty(),
            "local hot segments must be deleted after S3 upload: {local_hot_objs:?}"
        );

        // Clean publish + compact + prune after publish must make 0 S3 calls.
        let _ = s3.take_calls();
        store.publish_all_projects().await.unwrap();
        assert_eq!(s3.take_calls(), 0, "clean publish must make 0 S3 calls");
        store.compact_and_gc(None, &[]).await.unwrap();
        assert_eq!(
            s3.take_calls(),
            0,
            "idle compact_and_gc after publish must make 0 S3 calls"
        );

        // Query node reads on demand from S3 agent_published/.
        let routed_query =
            pulse_storage::with_hot_store_for_role(s3.clone(), query_dir.path(), true)
                .await
                .unwrap();
        let enc_query = Arc::new(EncryptedObjectStore::new(routed_query));
        enc_query.set_master_key(Some([0x22u8; 32]));
        let query_store = AgentTraceStore::open_with_encrypted(
            enc_query.as_object_store(),
            Some(enc_query),
            AgentTraceStoreConfig {
                read_only: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let listed = query_store
            .list_traces(
                "proj_local",
                &TraceListFilter::default(),
                &HashMap::new(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(listed.traces.len(), 1);
        assert_eq!(listed.traces[0].trace_id, "tr_loc_1");

        // Fenced writer test: simulate newer writer publishing pub_000000000001 with writer_generation = 1.
        let mut hijacked = latest.clone();
        hijacked.publish_seq = 1;
        hijacked.writer_generation = 1;
        PublishedAgentManifestStore::new("proj_local", enc_writer.as_object_store())
            .unwrap()
            .publish_next(hijacked.clone())
            .await
            .unwrap();
        store
            .ingest_batch("proj_local", sample_batch("tr_loc_2", now_ms + 1000))
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        let fence_err = store.publish_all_projects().await.unwrap_err();
        assert!(
            matches!(
                fence_err,
                StoreError::NotWriter { .. } | StoreError::StaleWriter { .. }
            ),
            "expected NotWriter/StaleWriter, got {fence_err:?}"
        );
        store.close().await.ok();

        // Reopening stale writer with local writer_generation = 0 < S3 writer_generation = 1 fails with StaleWriter.
        let stale_open = AgentTraceStore::open_with_encrypted(
            enc_writer.as_object_store(),
            Some(enc_writer.clone()),
            AgentTraceStoreConfig::default(),
        )
        .await;
        assert!(
            matches!(stale_open, Err(StoreError::StaleWriter { .. })),
            "reopening fenced writer must fail with StaleWriter"
        );

        // Self-crash recovery at T2 step 6: local writer_generation == 1, last_published_seq == Some(1),
        // and S3 already has pub_000000000002 with writer_generation == 1.
        let mut self_pub = hijacked.clone();
        self_pub.publish_seq = 2;
        self_pub.writer_generation = 1;
        self_pub.intent_id = None;
        PublishedAgentManifestStore::new("proj_local", enc_writer.as_object_store())
            .unwrap()
            .publish_next(self_pub)
            .await
            .unwrap();
        let local_m_store = ManifestStore::new(
            "proj_local",
            enc_writer.as_object_store(),
            crate::store::manifest::AGENT_GC_GRACE_MS,
        )
        .unwrap();
        let mut local_m = local_m_store.load_latest().await.unwrap();
        local_m.writer_generation = 1;
        local_m.last_published_seq = Some(1);
        local_m.fenced = false;
        local_m_store.commit_next(local_m).await.unwrap();

        let reopened = AgentTraceStore::open_with_encrypted(
            enc_writer.as_object_store(),
            Some(enc_writer.clone()),
            AgentTraceStoreConfig::default(),
        )
        .await
        .unwrap();
        let recovered_snap = reopened.acquire_snapshot("proj_local").await.unwrap();
        assert_eq!(recovered_snap.manifest.writer_generation, 1);
        assert_eq!(recovered_snap.manifest.last_published_seq, Some(2));
        drop(recovered_snap);
        query_store.close().await.unwrap();
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_hot_tier_eager_upload_closed_date_cold_merge_and_s3_delete_grace() {
        let writer_dir = tempfile::tempdir().unwrap();
        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed =
            pulse_storage::with_hot_store_for_role(s3_raw.clone(), writer_dir.path(), false)
                .await
                .unwrap();
        let store = AgentTraceStore::open(routed.clone(), AgentTraceStoreConfig::default())
            .await
            .unwrap();

        // 1. Eager upload when upload_interval == 0.
        store.set_upload_interval(Duration::ZERO);
        let old_ts_1 = 1_700_000_000_000i64; // 2023-11-14 (closed date)
        store
            .ingest_batch("proj_eager", sample_batch("tr_eager_1", old_ts_1))
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        let pub_store = PublishedAgentManifestStore::new("proj_eager", routed.clone()).unwrap();
        let p1 = pub_store.load_latest().await.unwrap().unwrap();
        assert_eq!(p1.publish_seq, 0);
        assert_eq!(p1.segments.len(), 1);
        let first_s3_seg = p1.segments[0].object_path.clone();

        // 2. Upload interval > 0: closed date stays local until closed_date_upload_interval or max_bytes triggers.
        store.set_upload_interval(Duration::from_millis(50));
        store.set_closed_date_upload_interval(Duration::from_millis(80));
        store.set_closed_date_max_bytes(1024 * 1024);
        store
            .ingest_batch("proj_eager", sample_batch("tr_closed_2", old_ts_1 + 1000))
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();
        store.publish_all_projects().await.unwrap();
        let p_still = pub_store.load_latest().await.unwrap().unwrap();
        assert_eq!(
            p_still.publish_seq, 0,
            "small recent closed-date partition must stay local until closed_date_upload_interval or max_bytes"
        );

        // Wait past closed_date_upload_interval -> uploads second S3 segment for same closed date.
        tokio::time::sleep(Duration::from_millis(95)).await;
        store.publish_all_projects().await.unwrap();
        let p2 = pub_store.load_latest().await.unwrap().unwrap();
        assert_eq!(p2.publish_seq, 1);
        assert_eq!(p2.segments.len(), 2);
        let second_s3_seg = p2
            .segments
            .iter()
            .find(|s| s.object_path != first_s3_seg)
            .unwrap()
            .object_path
            .clone();

        // 3. Cold S3 merge + 2-publish-interval pending_s3_deletes grace.
        store.set_cold_merge_idle(Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        store.compact_and_gc(Some("proj_eager"), &[]).await.unwrap();
        store.publish_all_projects().await.unwrap();
        let p3 = pub_store.load_latest().await.unwrap().unwrap();
        assert_eq!(p3.publish_seq, 2);
        assert_eq!(
            p3.segments.len(),
            1,
            "cold closed-date S3 segments should merge into 1 segment"
        );
        // Old S3 segments must still exist right after merge (2 * upload_interval grace = 100ms).
        assert!(s3_raw
            .head(&ObjectPath::from(first_s3_seg.as_str()))
            .await
            .is_ok());
        assert!(s3_raw
            .head(&ObjectPath::from(second_s3_seg.as_str()))
            .await
            .is_ok());

        // Wait past 2 * upload_interval grace (100ms) and run publish/GC -> deletes old S3 segments.
        tokio::time::sleep(Duration::from_millis(120)).await;
        store.publish_all_projects().await.unwrap();
        assert!(
            s3_raw
                .head(&ObjectPath::from(first_s3_seg.as_str()))
                .await
                .is_err(),
            "old merged S3 segment 1 should be deleted after 2 publish intervals"
        );
        assert!(
            s3_raw
                .head(&ObjectPath::from(second_s3_seg.as_str()))
                .await
                .is_err(),
            "old merged S3 segment 2 should be deleted after 2 publish intervals"
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_hot_tier_purges_dead_recovered_foreign_wal_when_100pct_erased() {
        let writer_dir = tempfile::tempdir().unwrap();
        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed =
            pulse_storage::with_hot_store_for_role(s3_raw.clone(), writer_dir.path(), false)
                .await
                .unwrap();
        let crashed_wid = Uuid::now_v7().to_string();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let frame =
            crate::store::wal::encode_ptwl_frame(1, now_ms, &sample_batch("tr_dead_wal", now_ms))
                .unwrap();
        let wal_path =
            crate::store::wal::wal_object_path("proj_wal_erase", &crashed_wid, 1).unwrap();
        routed
            .put(
                &wal_path,
                object_store::PutPayload::from_bytes(bytes::Bytes::from(frame)),
            )
            .await
            .unwrap();

        let store = AgentTraceStore::open(routed.clone(), AgentTraceStoreConfig::default())
            .await
            .unwrap();
        let tb = AgentErasureTombstone {
            id: "tb_wal_1".to_string(),
            project_id: "proj_wal_erase".to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_dead_wal".to_string(),
            created_at: now_ms,
            expires_at: Some(now_ms + 5_000),
        };
        let outcome = store
            .purge_erased_in_project("proj_wal_erase", &[tb])
            .await
            .unwrap();
        assert_eq!(outcome.purged_tombstone_ids, vec!["tb_wal_1".to_string()]);
        assert!(
            routed.head(&wal_path).await.is_err(),
            "dead recovered foreign WAL must be purged even when 100% erased"
        );
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_hot_tier_epoch_zero_step6_does_not_bypass_fencing_and_enforces_fenced_ingest() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let dir_c = tempfile::tempdir().unwrap();
        let dir_self6 = tempfile::tempdir().unwrap();
        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // 1. Writer A publishes pub_0 for `proj_e0` (sets intent_id = Some(...)).
        let routed_a = pulse_storage::with_hot_store_for_role(s3_raw.clone(), dir_a.path(), false)
            .await
            .unwrap();
        let store_a = AgentTraceStore::open(routed_a, AgentTraceStoreConfig::default())
            .await
            .unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis();
        store_a
            .ingest_batch("proj_e0", sample_batch("tr_writer_a", now_ms))
            .await
            .unwrap();
        store_a.flush_all_l0().await.unwrap();
        assert_eq!(store_a.publish_all_projects().await.unwrap(), 1);
        store_a.close().await.unwrap();

        // 2. Writer B starts on an initialized local `<data_dir>` (with `hot_store_mode` + `hot/identity/proj_other/`)
        //    and NO local `agent_manifests/proj_e0/`. First ingest + flush must still make 0 S3 calls.
        let _init_b =
            pulse_storage::with_hot_store_for_role(Arc::new(InMemory::new()), dir_b.path(), false)
                .await
                .unwrap();
        std::fs::create_dir_all(dir_b.path().join("hot/identity/proj_other")).unwrap();
        std::fs::write(dir_b.path().join("hot/identity/proj_other/marker"), b"1").unwrap();
        let s3_b = CountingObjectStore::new(s3_raw.clone());
        let routed_b = pulse_storage::with_hot_store_for_role(s3_b.clone(), dir_b.path(), false)
            .await
            .unwrap();
        let store_b = AgentTraceStore::open(routed_b.clone(), AgentTraceStoreConfig::default())
            .await
            .unwrap();
        s3_b.take_calls();
        store_b
            .ingest_batch("proj_e0", sample_batch("tr_writer_b", now_ms + 100))
            .await
            .unwrap();
        store_b.flush_all_l0().await.unwrap();
        assert_eq!(
            s3_b.take_calls(),
            0,
            "first ingest+flush on epoch=0 project must make 0 S3 calls"
        );

        // Writer B's first publish collides at pub_0 -> NotWriter, persists fenced=true, and
        // live `ingest_batch` must immediately return NotWriter too.
        let pub_err = store_b.publish_all_projects().await.unwrap_err();
        assert!(
            matches!(pub_err, StoreError::NotWriter { .. }),
            "{pub_err:?}"
        );
        let live_ingest_err = store_b
            .ingest_batch("proj_e0", sample_batch("tr_after_fence", now_ms + 200))
            .await
            .unwrap_err();
        assert!(
            matches!(live_ingest_err, StoreError::NotWriter { .. }),
            "live fenced writer must reject ingest_batch with NotWriter, got {live_ingest_err:?}"
        );
        store_b.close().await.ok();

        // Reopening Writer B MUST return StaleWriter, never hijacking pub_0 via is_step6.
        let reopen_b = AgentTraceStore::open(routed_b, AgentTraceStoreConfig::default()).await;
        assert!(
            matches!(reopen_b, Err(StoreError::StaleWriter { .. })),
            "fenced epoch=0 writer reopen must return StaleWriter"
        );

        // 3. Writer C only flushes L0 (local_epoch = 1, never attempted publish -> no upload_intent)
        //    and restarts against both intent-bearing pub_0 and legacy intent_id=None pub_0 -> StaleWriter!
        let _init_c =
            pulse_storage::with_hot_store_for_role(Arc::new(InMemory::new()), dir_c.path(), false)
                .await
                .unwrap();
        std::fs::create_dir_all(dir_c.path().join("hot/identity/proj_other")).unwrap();
        std::fs::write(dir_c.path().join("hot/identity/proj_other/marker"), b"1").unwrap();
        let routed_c = pulse_storage::with_hot_store_for_role(s3_raw.clone(), dir_c.path(), false)
            .await
            .unwrap();
        let store_c = AgentTraceStore::open(routed_c.clone(), AgentTraceStoreConfig::default())
            .await
            .unwrap();
        store_c
            .ingest_batch("proj_e0", sample_batch("tr_writer_c", now_ms + 300))
            .await
            .unwrap();
        store_c.flush_all_l0().await.unwrap();
        drop(store_c);
        let reopen_c =
            AgentTraceStore::open(routed_c.clone(), AgentTraceStoreConfig::default()).await;
        assert!(
            matches!(reopen_c, Err(StoreError::StaleWriter { .. })),
            "un-published local_epoch=1 writer must fail reopen with StaleWriter when pub_0 has foreign intent_id"
        );
        // Strip intent_id from S3 pub_0 to simulate legacy pub_0 -> local_last_pub_seq.is_none() MUST STILL refuse!
        let pub0_path = PublishedAgentManifestStore::new("proj_e0", s3_raw.clone())
            .unwrap()
            .pub_path(0);
        let mut legacy_pub0: PublishedAgentManifest =
            serde_json::from_slice(&s3_raw.get(&pub0_path).await.unwrap().bytes().await.unwrap())
                .unwrap();
        legacy_pub0.intent_id = None;
        s3_raw
            .put(
                &pub0_path,
                object_store::PutPayload::from_bytes(bytes::Bytes::from(
                    serde_json::to_vec(&legacy_pub0).unwrap(),
                )),
            )
            .await
            .unwrap();
        let reopen_c_legacy =
            AgentTraceStore::open(routed_c, AgentTraceStoreConfig::default()).await;
        assert!(
            matches!(reopen_c_legacy, Err(StoreError::StaleWriter { .. })),
            "is_step6 must NEVER match intent_id=None when local_last_pub_seq is None"
        );

        // 4. Genuine step-6 self-crash at pub_0 (matching upload_intent.json on disk) recovers cleanly.
        let s3_self6: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed_s6 =
            pulse_storage::with_hot_store_for_role(s3_self6.clone(), dir_self6.path(), false)
                .await
                .unwrap();
        let ms_s6 = ManifestStore::new(
            "proj_s6_p0",
            routed_s6.clone(),
            crate::store::manifest::AGENT_GC_GRACE_MS,
        )
        .unwrap();
        ms_s6
            .commit_next(AgentManifest {
                updated_at_ms: now_ms,
                ..Default::default()
            })
            .await
            .unwrap();
        AgentUploadIntentStore::new("proj_s6_p0", routed_s6.clone())
            .unwrap()
            .save(&AgentUploadIntent {
                intent_id: "intent_self6_p0".to_string(),
                writer_generation: 0,
                base_local_epoch: 1,
                target_publish_seq: 0,
                created_at_ms: now_ms,
                batches: Vec::new(),
            })
            .await
            .unwrap();
        PublishedAgentManifestStore::new("proj_s6_p0", routed_s6.clone())
            .unwrap()
            .publish_next(PublishedAgentManifest {
                publish_seq: 0,
                writer_generation: 0,
                source_local_epoch: 1,
                published_at_ms: now_ms,
                intent_id: Some("intent_self6_p0".to_string()),
                segments: Vec::new(),
                committed_wal_seq: BTreeMap::new(),
                erased_traces: BTreeMap::new(),
            })
            .await
            .unwrap();
        let recovered_s6 = AgentTraceStore::open(routed_s6, AgentTraceStoreConfig::default())
            .await
            .expect("genuine step-6 self-crash at pub_0 with matching intent_id must recover");
        let snap_s6 = recovered_s6.acquire_snapshot("proj_s6_p0").await.unwrap();
        assert_eq!(snap_s6.manifest.last_published_seq, Some(0));
        drop(snap_s6);
        recovered_s6.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_hot_tier_purge_erased_refuses_when_s3_published_exists_without_local_manifest() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let routed_a = pulse_storage::with_hot_store_for_role(s3_raw.clone(), dir_a.path(), false)
            .await
            .unwrap();
        let store_a = AgentTraceStore::open(routed_a, AgentTraceStoreConfig::default())
            .await
            .unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis();
        store_a
            .ingest_batch("proj_remote_only", sample_batch("tr_remote_1", now_ms))
            .await
            .unwrap();
        store_a.flush_all_l0().await.unwrap();
        assert_eq!(store_a.publish_all_projects().await.unwrap(), 1);
        store_a.close().await.unwrap();

        // Node B has initialized local hot store (for another project) and NO local manifest/WAL for `proj_remote_only`.
        let _init_b =
            pulse_storage::with_hot_store_for_role(Arc::new(InMemory::new()), dir_b.path(), false)
                .await
                .unwrap();
        std::fs::create_dir_all(dir_b.path().join("hot/identity/proj_other")).unwrap();
        std::fs::write(dir_b.path().join("hot/identity/proj_other/marker"), b"1").unwrap();
        let routed_b = pulse_storage::with_hot_store_for_role(s3_raw.clone(), dir_b.path(), false)
            .await
            .unwrap();
        let store_b = AgentTraceStore::open(routed_b, AgentTraceStoreConfig::default())
            .await
            .unwrap();
        let tb = AgentErasureTombstone {
            id: "tb_remote_1".to_string(),
            project_id: "proj_remote_only".to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_remote_1".to_string(),
            created_at: now_ms,
            expires_at: Some(now_ms + 5_000),
        };
        let err = store_b
            .purge_erased_in_project("proj_remote_only", &[tb])
            .await
            .unwrap_err();
        assert!(
            matches!(err, StoreError::NotWriter { .. }),
            "expected NotWriter when S3 agent_published exists without local manifest/WAL, got {err:?}"
        );
        store_b.close().await.unwrap();
    }

    struct MapAgentOwnerChecker {
        self_node: String,
        owners: parking_lot::RwLock<HashMap<String, pulse_storage::ProjectOwnerInfo>>,
    }

    #[async_trait::async_trait]
    impl ProjectOwnerChecker for MapAgentOwnerChecker {
        async fn check_owner(&self, project_id: &str) -> std::result::Result<OwnerCheck, String> {
            let guard = self.owners.read();
            match guard.get(project_id) {
                Some(info) if info.node_name == self.self_node => Ok(OwnerCheck::Owned),
                Some(info) => Ok(OwnerCheck::NotOwner(info.clone())),
                None => Ok(OwnerCheck::Unassigned),
            }
        }
    }

    #[tokio::test]
    async fn owner_checker_guards_boot_ingest_purge_prune_and_reconcile() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let checker_a = Arc::new(MapAgentOwnerChecker {
            self_node: "node-a".into(),
            owners: parking_lot::RwLock::new(HashMap::from([(
                "proj_a".into(),
                pulse_storage::ProjectOwnerInfo {
                    project_id: "proj_a".into(),
                    node_name: "node-a".into(),
                    address: "http://node-a:8080".into(),
                    updated_at_ms: 1000,
                },
            )])),
        });
        let checker_b = Arc::new(MapAgentOwnerChecker {
            self_node: "node-b".into(),
            owners: parking_lot::RwLock::new(HashMap::from([
                (
                    "proj_a".into(),
                    pulse_storage::ProjectOwnerInfo {
                        project_id: "proj_a".into(),
                        node_name: "node-a".into(),
                        address: "http://node-a:8080".into(),
                        updated_at_ms: 1000,
                    },
                ),
                (
                    "proj_b".into(),
                    pulse_storage::ProjectOwnerInfo {
                        project_id: "proj_b".into(),
                        node_name: "node-b".into(),
                        address: "http://node-b:8080".into(),
                        updated_at_ms: 1000,
                    },
                ),
            ])),
        });

        let routed_a = pulse_storage::with_hot_store_for_role(s3_raw.clone(), dir_a.path(), false)
            .await
            .unwrap();
        let store_a = AgentTraceStore::open_with_encrypted_and_owner(
            routed_a,
            None,
            AgentTraceStoreConfig::default(),
            Some(checker_a.clone()),
        )
        .await
        .unwrap();

        let now_ms = chrono::Utc::now().timestamp_millis();
        store_a
            .ingest_batch("proj_a", sample_batch("tr_a1", now_ms))
            .await
            .unwrap();
        store_a.flush_all_l0().await.unwrap();
        assert_eq!(store_a.publish_all_projects().await.unwrap(), 1);

        // Node B initializes its local hot store (for proj_b), while shared S3 has proj_a published.
        let _init_b =
            pulse_storage::with_hot_store_for_role(Arc::new(InMemory::new()), dir_b.path(), false)
                .await
                .unwrap();
        std::fs::create_dir_all(dir_b.path().join("hot/identity/proj_b")).unwrap();
        std::fs::write(dir_b.path().join("hot/identity/proj_b/marker"), b"1").unwrap();
        let routed_b = pulse_storage::with_hot_store_for_role(s3_raw.clone(), dir_b.path(), false)
            .await
            .unwrap();
        let store_b = AgentTraceStore::open_with_encrypted_and_owner(
            routed_b,
            None,
            AgentTraceStoreConfig::default(),
            Some(checker_b.clone()),
        )
        .await
        .unwrap();

        // Non-owner ingest, purge_erased_in_project (even before any local files exist!), and prune_expired_segments refuse with NotOwner.
        assert!(matches!(
            store_b.ingest_batch("proj_a", sample_batch("tr_bad", now_ms)).await,
            Err(StoreError::NotOwner { owner_node: Some(ref n), .. }) if n == "node-a"
        ));
        let tb = AgentErasureTombstone {
            id: "tb_1".into(),
            project_id: "proj_a".into(),
            level: ErasureLevel::Trace,
            target_id: "tr_a1".into(),
            created_at: now_ms,
            expires_at: Some(now_ms + 5_000),
        };
        assert!(matches!(
            store_b.purge_erased_in_project("proj_a", &[tb]).await,
            Err(StoreError::NotOwner { owner_node: Some(ref n), .. }) if n == "node-a"
        ));
        let cutoff = chrono::Utc::now().date_naive();
        assert!(matches!(
            store_b.prune_expired_segments("proj_a", cutoff).await,
            Err(StoreError::NotOwner { owner_node: Some(ref n), .. }) if n == "node-a"
        ));

        // Unowned project in owner-enforced mode also returns NotOwner(None).
        assert!(matches!(
            store_b
                .ingest_batch("proj_unassigned", sample_batch("tr_un", now_ms))
                .await,
            Err(StoreError::NotOwner {
                owner_node: None,
                owner_address: None,
                ..
            })
        ));

        // Background ops (publish_all_projects, compact_and_gc(None)) skip foreign projects and succeed for owned proj_b.
        store_b
            .ingest_batch("proj_b", sample_batch("tr_b1", now_ms))
            .await
            .unwrap();
        store_b.flush_all_l0().await.unwrap();
        assert_eq!(store_b.publish_all_projects().await.unwrap(), 1);
        store_b.compact_and_gc(None, &[]).await.unwrap();

        // Ownership transfer away from node-a -> reconcile_ownership fences and evicts proj_a on node-a.
        checker_a.owners.write().insert(
            "proj_a".into(),
            pulse_storage::ProjectOwnerInfo {
                project_id: "proj_a".into(),
                node_name: "node-b".into(),
                address: "http://node-b:8080".into(),
                updated_at_ms: 2000,
            },
        );
        store_a.reconcile_ownership().await;
        assert!(!store_a.projects.contains_key("proj_a"));
        assert!(matches!(
            store_a.ingest_batch("proj_a", sample_batch("tr_after_move", now_ms)).await,
            Err(StoreError::NotOwner { owner_node: Some(ref n), .. }) if n == "node-b"
        ));

        store_a.close().await.unwrap();
        store_b.close().await.unwrap();
    }
}
