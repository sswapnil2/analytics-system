use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::Mutex;
use pulse_storage::{event_uuid_fingerprint, route_bucket, StorageEngine, StorageError};
use pulse_wire::{
    mask_input_value, DictionaryRegistry, PropValue, ReplayChunk, WireBatch, WireError, WireEvent,
    WireframeOp,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dedup::{normalize_event_timestamp, DedupRegistry};
use crate::governance::{DiscoveredEventSchema, GovernanceProvider, ProjectLexiconRules};

/// Per-batch and per-project schema cardinality caps preventing dictionary exhaustion.
pub const MAX_NEW_EVENTS_PER_BATCH: usize = 128;
pub const MAX_NEW_PROP_KEYS_PER_BATCH: usize = 256;
pub const MAX_EVENTS_PER_PROJECT: usize = 8_192;
pub const MAX_PROP_KEYS_PER_PROJECT: usize = 16_384;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("missing or invalid x-pulse-key API key")]
    Unauthorized,

    #[error("wire decode error: {0}")]
    Wire(#[from] WireError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("identity error: {0}")]
    Identity(#[from] pulse_identity::IdentityError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("schema governance limit exceeded: {0}")]
    SchemaGovernanceLimit(String),

    #[error("quic transport error: {0}")]
    Quic(String),

    #[error("ingest rate limit exceeded for this project; retry in {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },
}

pub type Result<T> = std::result::Result<T, IngestError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    pub status: String,
    pub accepted: usize,
    pub deduped: usize,
    pub dropped: usize,
    pub server_ts_ms: i64,
    /// Events of the request that were refused, by index; the rest were processed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<pulse_wire::RejectedEvent>,
    /// Segment `identify` / `alias` calls in the batch that were applied to profiles.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub profiles_updated: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentifyRequest {
    pub anon_id: String,
    pub user_id: String,
    #[serde(default)]
    pub routing_seed: Option<String>,
    #[serde(default)]
    pub anon_routing_seed: Option<String>,
    #[serde(default)]
    pub user_properties: Option<serde_json::Value>,
    #[serde(default)]
    pub operations: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentifyResponse {
    pub status: String,
    pub canonical_uid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayIngestResponse {
    pub status: String,
    pub object_path: String,
    pub byte_offset: u64,
    pub replay_offset_kb: u32,
    pub chunk_bytes: usize,
}

#[derive(Default)]
pub struct IngestMetrics {
    pub accepted_total: AtomicU64,
    pub deduped_total: AtomicU64,
    pub dropped_total: AtomicU64,
    pub batches_total: AtomicU64,
    eps_buckets: Mutex<[(i64, u64); 10]>,
}

impl IngestMetrics {
    fn record(&self, accepted: u64, deduped: u64, dropped: u64, now_ms: i64) {
        self.record_accepted_and_deduped(accepted, deduped, now_ms);
        self.dropped_total.fetch_add(dropped, Ordering::Relaxed);
        self.batches_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_receipt(&self, dropped: u64) {
        self.dropped_total.fetch_add(dropped, Ordering::Relaxed);
        self.batches_total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_accepted_and_deduped(&self, accepted: u64, deduped: u64, now_ms: i64) {
        self.accepted_total.fetch_add(accepted, Ordering::Relaxed);
        self.deduped_total.fetch_add(deduped, Ordering::Relaxed);

        if accepted > 0 {
            let sec = now_ms / 1_000;
            let idx = (sec.rem_euclid(10)) as usize;
            let mut guard = self.eps_buckets.lock();
            if guard[idx].0 == sec {
                guard[idx].1 += accepted;
            } else {
                guard[idx] = (sec, accepted);
            }
        }
    }

    pub fn current_eps(&self) -> u64 {
        let now_sec = Utc::now().timestamp();
        let guard = self.eps_buckets.lock();
        let mut sum = 0u64;
        let mut active_secs = 0u64;
        for &(sec, count) in guard.iter() {
            if sec >= now_sec - 5 && sec <= now_sec {
                sum += count;
                active_secs += 1;
            }
        }
        if active_secs == 0 {
            0
        } else {
            sum / active_secs.max(1)
        }
    }
}

/// One WAL record holds at most about this many bytes of events. Requests beyond it wait for
/// the next record, so a single PUT stays bounded however many requests are queued.
const WAL_GROUP_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Bounded per-bucket processor queue capacity to cap unapplied heap growth.
const PROCESSOR_CHANNEL_CAP: usize = 256;

const DEFAULT_WRITER_FLUSH_MS: u64 = 20;
const DEFAULT_WRITER_FLUSH_BYTES: usize = 1_048_576;
const DEFAULT_INGEST_BUFFER_PROJECT_MAX_MB: usize = 64;
const DEFAULT_INGEST_BUFFER_MAX_MB: usize = 256;

/// Snapshot of W1 writer memory buffer state and bounds for `/metrics`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BufferStats {
    pub flush_ms: u64,
    pub flush_bytes: usize,
    pub buffer_bytes: usize,
    pub capacity_bytes: usize,
    pub buffer_events: usize,
    pub buffer_batches: usize,
    pub flushes_timer: u64,
    pub flushes_bytes: u64,
    pub flushes_drain: u64,
}

/// A routed, interned event waiting for its bucket's WAL on the sync path: `(event, uid, uuid fingerprint)`.
type SyncCandidate = (WireEvent, u32, u64);
/// What one sync request got from a bucket: `(accepted, deduped, the accepted events)`.
type SyncBucketOutcome = (usize, usize, Vec<WireEvent>);

/// A routed event waiting for its bucket's WAL on the W1 async path: `(event, uuid fingerprint)`.
type Candidate = (WireEvent, u64);

/// One durable WAL record on its way from the appender (the request path) to the project's
/// processor task, which turns it into L0 rows.
struct LoggedRecord {
    bucket_id: u8,
    wal_seq: u64,
    events: Vec<WireEvent>,
    fps: Vec<u64>,
    bytes: usize,
}

type RecordSenders = Arc<Vec<tokio::sync::mpsc::Sender<LoggedRecord>>>;

/// The processor of one open project: one task per bucket, each consuming its own queue in
/// that bucket's WAL order. Tied to one `ProjectStorage` instance (a project re-opened after a
/// writer-lease takeover gets a new one).
struct Processor {
    ps: Arc<pulse_storage::ProjectStorage>,
    txs: RecordSenders,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// One sync request's events for one bucket, queued until a WAL record that includes them is
/// durable. `Err` hands the events back after a failed write so the request retries itself.
struct GroupWaiter {
    candidates: Vec<SyncCandidate>,
    bytes: usize,
    tx: tokio::sync::oneshot::Sender<std::result::Result<SyncBucketOutcome, Vec<SyncCandidate>>>,
}

type BucketQueues = Arc<Vec<Mutex<VecDeque<GroupWaiter>>>>;

fn approx_event_bytes(ev: &WireEvent) -> usize {
    128 + ev.event_name.len()
        + ev.distinct_id.len()
        + ev.session_id.len()
        + ev.routing_seed.len()
        + ev.props
            .iter()
            .map(|(k, v)| {
                48 + k.len()
                    + match v {
                        PropValue::Str(s) => s.len(),
                        _ => 0,
                    }
            })
            .sum::<usize>()
}

#[derive(Clone, Debug)]
enum StoredWalFailure {
    NotWriter {
        project_id: String,
        fenced: bool,
    },
    StaleWriter {
        project_id: String,
        pub_seq: u64,
        pub_gen: u64,
        local_last_pub_seq: Option<u64>,
        local_gen: u64,
    },
    Io(String),
}

impl StoredWalFailure {
    fn from_storage_err(err: &StorageError, fenced: bool) -> Self {
        match err {
            StorageError::NotWriter { project_id } => Self::NotWriter {
                project_id: project_id.clone(),
                fenced,
            },
            StorageError::StaleWriter {
                project_id,
                pub_seq,
                pub_gen,
                local_last_pub_seq,
                local_gen,
            } => Self::StaleWriter {
                project_id: project_id.clone(),
                pub_seq: *pub_seq,
                pub_gen: *pub_gen,
                local_last_pub_seq: *local_last_pub_seq,
                local_gen: *local_gen,
            },
            other => Self::Io(other.to_string()),
        }
    }

    fn is_fenced(&self) -> bool {
        match self {
            Self::NotWriter { fenced, .. } => *fenced,
            Self::StaleWriter { .. } => true,
            Self::Io(_) => false,
        }
    }

    fn to_ingest_error(&self) -> IngestError {
        match self {
            Self::NotWriter { project_id, .. } => IngestError::Storage(StorageError::NotWriter {
                project_id: project_id.clone(),
            }),
            Self::StaleWriter {
                project_id,
                pub_seq,
                pub_gen,
                local_last_pub_seq,
                local_gen,
            } => IngestError::Storage(StorageError::StaleWriter {
                project_id: project_id.clone(),
                pub_seq: *pub_seq,
                pub_gen: *pub_gen,
                local_last_pub_seq: *local_last_pub_seq,
                local_gen: *local_gen,
            }),
            Self::Io(msg) => {
                IngestError::Storage(StorageError::Io(std::io::Error::other(msg.clone())))
            }
        }
    }
}

struct BufferedBatch {
    ps: Arc<pulse_storage::ProjectStorage>,
    processor: RecordSenders,
    server_now_ms: i64,
    events: Vec<Candidate>,
    bytes: usize,
}

struct ProjectBufferState {
    buckets: Vec<VecDeque<BufferedBatch>>,
    reserved_bytes: usize,
    buffered_bytes: usize,
    buffered_events: usize,
    buffered_batches: usize,
    oldest_queued_at: Option<tokio::time::Instant>,
    explicit_flush: bool,
    flush_seq: u64,
    wal_error: Option<StoredWalFailure>,
    retry_backoff_ms: u64,
    next_retry_at: tokio::time::Instant,
    shutting_down: bool,
}

struct ProjectBuffer {
    project_id: String,
    state: Mutex<ProjectBufferState>,
    wake_flusher: tokio::sync::Notify,
    flush_done: tokio::sync::Notify,
    flusher_started: AtomicBool,
    flusher_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ProjectBuffer {
    fn new(project_id: String, num_buckets: usize) -> Arc<Self> {
        let now = tokio::time::Instant::now();
        Arc::new(Self {
            project_id,
            state: Mutex::new(ProjectBufferState {
                buckets: (0..num_buckets).map(|_| VecDeque::new()).collect(),
                reserved_bytes: 0,
                buffered_bytes: 0,
                buffered_events: 0,
                buffered_batches: 0,
                oldest_queued_at: None,
                explicit_flush: false,
                flush_seq: 0,
                wal_error: None,
                retry_backoff_ms: 0,
                next_retry_at: now,
                shutting_down: false,
            }),
            wake_flusher: tokio::sync::Notify::new(),
            flush_done: tokio::sync::Notify::new(),
            flusher_started: AtomicBool::new(false),
            flusher_handle: Mutex::new(None),
        })
    }
}

struct IngestBuffer {
    flush_ms: AtomicU64,
    flush_bytes: AtomicUsize,
    project_max_bytes: AtomicUsize,
    node_max_bytes: AtomicUsize,
    node_bytes: Arc<AtomicUsize>,
    node_events: Arc<AtomicUsize>,
    node_batches: Arc<AtomicUsize>,
    flushes_timer: AtomicU64,
    flushes_bytes: AtomicU64,
    flushes_drain: AtomicU64,
    projects: DashMap<String, Arc<ProjectBuffer>>,
}

impl IngestBuffer {
    fn from_env() -> Self {
        let flush_ms = std::env::var("PULSE_WRITER_FLUSH_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_WRITER_FLUSH_MS);
        let flush_bytes = std::env::var("PULSE_WRITER_FLUSH_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&b| b > 0)
            .unwrap_or(DEFAULT_WRITER_FLUSH_BYTES);
        let project_max_bytes = std::env::var("PULSE_INGEST_BUFFER_PROJECT_MAX_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&mb| mb > 0)
            .unwrap_or(DEFAULT_INGEST_BUFFER_PROJECT_MAX_MB)
            .saturating_mul(1024 * 1024);
        let node_max_bytes = std::env::var("PULSE_INGEST_BUFFER_MAX_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&mb| mb > 0)
            .unwrap_or(DEFAULT_INGEST_BUFFER_MAX_MB)
            .saturating_mul(1024 * 1024);
        Self {
            flush_ms: AtomicU64::new(flush_ms),
            flush_bytes: AtomicUsize::new(flush_bytes),
            project_max_bytes: AtomicUsize::new(project_max_bytes),
            node_max_bytes: AtomicUsize::new(node_max_bytes),
            node_bytes: Arc::new(AtomicUsize::new(0)),
            node_events: Arc::new(AtomicUsize::new(0)),
            node_batches: Arc::new(AtomicUsize::new(0)),
            flushes_timer: AtomicU64::new(0),
            flushes_bytes: AtomicU64::new(0),
            flushes_drain: AtomicU64::new(0),
            projects: DashMap::new(),
        }
    }

    fn project(&self, project_id: &str, num_buckets: usize) -> Arc<ProjectBuffer> {
        if let Some(p) = self.projects.get(project_id) {
            return p.clone();
        }
        self.projects
            .entry(project_id.to_string())
            .or_insert_with(|| ProjectBuffer::new(project_id.to_string(), num_buckets))
            .clone()
    }
}

struct BufferReservation {
    proj_buf: Arc<ProjectBuffer>,
    node_bytes: Arc<AtomicUsize>,
    node_events: Arc<AtomicUsize>,
    node_batches: Arc<AtomicUsize>,
    flush_ms: u64,
    flush_bytes: usize,
    reserved_bytes: usize,
    committed: bool,
}

impl Drop for BufferReservation {
    fn drop(&mut self) {
        if !self.committed && self.reserved_bytes > 0 {
            self.node_bytes
                .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
            let mut st = self.proj_buf.state.lock();
            st.reserved_bytes = st.reserved_bytes.saturating_sub(self.reserved_bytes);
        }
    }
}

impl BufferReservation {
    fn commit(
        mut self,
        by_bucket: BTreeMap<u8, (Vec<Candidate>, usize)>,
        total_events: usize,
        ps: &Arc<pulse_storage::ProjectStorage>,
        processor: &RecordSenders,
        server_now_ms: i64,
    ) {
        self.committed = true;
        let batch_count = by_bucket.len();
        if batch_count == 0 {
            if self.reserved_bytes > 0 {
                self.node_bytes
                    .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
                let mut st = self.proj_buf.state.lock();
                st.reserved_bytes = st.reserved_bytes.saturating_sub(self.reserved_bytes);
            }
            return;
        }
        let num_buckets = ps.config().num_buckets as usize;
        let mut should_wake = false;
        {
            let mut st = self.proj_buf.state.lock();
            if st.buckets.len() < num_buckets {
                st.buckets.resize_with(num_buckets, VecDeque::new);
            }
            st.reserved_bytes = st.reserved_bytes.saturating_sub(self.reserved_bytes);
            for (bucket_id, (events, bytes)) in by_bucket {
                st.buckets[bucket_id as usize].push_back(BufferedBatch {
                    ps: ps.clone(),
                    processor: processor.clone(),
                    server_now_ms,
                    events,
                    bytes,
                });
            }
            st.buffered_bytes += self.reserved_bytes;
            st.buffered_events += total_events;
            st.buffered_batches += batch_count;
            if st.oldest_queued_at.is_none() {
                st.oldest_queued_at = Some(tokio::time::Instant::now());
                should_wake = true;
            }
            if st.buffered_bytes >= self.flush_bytes || self.flush_ms == 0 {
                should_wake = true;
            }
        }
        self.node_events.fetch_add(total_events, Ordering::AcqRel);
        self.node_batches.fetch_add(batch_count, Ordering::AcqRel);
        if should_wake {
            self.proj_buf.wake_flusher.notify_one();
        }
    }
}

#[derive(Clone, Copy)]
enum FlushReason {
    Timer,
    Bytes,
    Drain,
}

fn spawn_processor(
    ps: Arc<pulse_storage::ProjectStorage>,
    id_graph: Arc<pulse_identity::ProjectIdentityGraph>,
    governance: Arc<dyn GovernanceProvider>,
) -> Processor {
    let mut txs = Vec::with_capacity(ps.config().num_buckets as usize);
    let mut handles = Vec::with_capacity(ps.config().num_buckets as usize);
    for _ in 0..ps.config().num_buckets {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LoggedRecord>(PROCESSOR_CHANNEL_CAP);
        let (ps, id_graph, governance) = (ps.clone(), id_graph.clone(), governance.clone());
        let handle = tokio::spawn(async move {
            while let Some(rec) = rx.recv().await {
                // Nothing below is lost by repeating it, and the record must not be skipped:
                // it was acknowledged. The only failure is the object store refusing a
                // dead-letter write.
                while let Err(e) = apply_record(&ps, &id_graph, &rec).await {
                    tracing::error!(
                        project_id = %ps.project_id(),
                        bucket_id = rec.bucket_id,
                        wal_seq = rec.wal_seq,
                        error = %e,
                        "applying a logged record failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                record_discovery(governance.as_ref(), ps.project_id(), &rec.events);
                if ps.should_flush_bucket(rec.bucket_id) {
                    ps.schedule_background_flush(rec.bucket_id);
                }
            }
        });
        txs.push(tx);
        handles.push(handle);
    }
    Processor {
        ps,
        txs: Arc::new(txs),
        handles: Mutex::new(handles),
    }
}

/// Interns ids and turns one logged record into L0 rows. Events over the project's schema caps
/// go to the dead-letter path instead of growing the dictionary.
async fn apply_record(
    ps: &Arc<pulse_storage::ProjectStorage>,
    id_graph: &pulse_identity::ProjectIdentityGraph,
    rec: &LoggedRecord,
) -> std::result::Result<(), StorageError> {
    let dict = ps.dictionary();
    let mut new_events: HashSet<&str> = HashSet::new();
    let mut new_keys: HashSet<&str> = HashSet::new();
    let mut events = Vec::with_capacity(rec.events.len());
    for (ev, &fp) in rec.events.iter().zip(&rec.fps) {
        let event_is_new = dict.lookup_event(&ev.event_name).is_none()
            && !new_events.contains(ev.event_name.as_str());
        let keys_new: Vec<&str> = ev
            .props
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| dict.lookup_prop_key(k).is_none() && !new_keys.contains(k))
            .collect();
        if (event_is_new && dict.event_count() + new_events.len() >= MAX_EVENTS_PER_PROJECT)
            || (!keys_new.is_empty()
                && dict.prop_key_count() + new_keys.len() + keys_new.len()
                    > MAX_PROP_KEYS_PER_PROJECT)
        {
            ps.dead_letter(
                rec.bucket_id,
                rec.wal_seq,
                ev.clone(),
                fp,
                &"project schema cap reached (event names or property keys)",
            )
            .await?;
            continue;
        }
        if event_is_new {
            new_events.insert(&ev.event_name);
        }
        new_keys.extend(keys_new);
        let uid = id_graph.get_or_intern(&ev.distinct_id, rec.bucket_id);
        events.push((ev, uid, fp));
    }
    ps.apply_logged(rec.bucket_id, rec.wal_seq, &events, rec.bytes)
        .await?;
    Ok(())
}

fn record_discovery(governance: &dyn GovernanceProvider, project_id: &str, events: &[WireEvent]) {
    type Seen = (u64, Vec<(String, &'static str)>);
    let mut discovery_map: HashMap<&str, Seen> = HashMap::new();
    for ev in events {
        let disc = discovery_map.entry(&ev.event_name).or_insert_with(|| {
            let ptypes = ev
                .props
                .iter()
                .map(|(k, v)| {
                    let t = match v {
                        PropValue::Null => "null",
                        PropValue::Bool(_) => "bool",
                        PropValue::Int(_) => "int",
                        PropValue::Float(_) => "float",
                        PropValue::Str(_) => "string",
                    };
                    (k.clone(), t)
                })
                .collect();
            (0, ptypes)
        });
        disc.0 += 1;
    }
    if !discovery_map.is_empty() {
        governance.record_discovery(
            discovery_map
                .into_iter()
                .map(|(event_name, (count, props))| DiscoveredEventSchema {
                    project_id: project_id.to_string(),
                    event_name: event_name.to_string(),
                    count,
                    props,
                })
                .collect(),
        );
    }
}

/// Core transport-agnostic Ingestion Pipeline shared by HTTP/1.1, HTTP/2, QUIC, and Seeder.
pub struct IngestService {
    storage: Arc<StorageEngine>,
    governance: Arc<dyn GovernanceProvider>,
    dedup: Arc<DedupRegistry>,
    processors: DashMap<String, Processor>,
    buffer: IngestBuffer,
    dict_registry: Arc<DictionaryRegistry>,
    metrics: IngestMetrics,
    rate_limit: crate::rate_limit::RateLimiter,
    /// Per project, per bucket: sync callers waiting to share the next WAL record.
    wal_groups: DashMap<String, BucketQueues>,
    self_weak: std::sync::OnceLock<std::sync::Weak<Self>>,
}

impl IngestService {
    pub fn new(storage: Arc<StorageEngine>, governance: Arc<dyn GovernanceProvider>) -> Arc<Self> {
        Self::with_rate_limit(
            storage,
            governance,
            crate::rate_limit::RateLimiter::from_env(),
        )
    }

    pub fn with_rate_limit(
        storage: Arc<StorageEngine>,
        governance: Arc<dyn GovernanceProvider>,
        rate_limit: crate::rate_limit::RateLimiter,
    ) -> Arc<Self> {
        let svc = Arc::new(Self {
            storage,
            governance,
            dedup: Arc::new(DedupRegistry::new()),
            processors: DashMap::new(),
            buffer: IngestBuffer::from_env(),
            dict_registry: Arc::new(DictionaryRegistry::default()),
            metrics: IngestMetrics::default(),
            rate_limit,
            wal_groups: DashMap::new(),
            self_weak: std::sync::OnceLock::new(),
        });
        let _ = svc.self_weak.set(Arc::downgrade(&svc));
        svc
    }

    /// Charges a request from the public SDK endpoints against its project's rate limit.
    fn charge(&self, project_id: &str, units: u64) -> Result<()> {
        self.rate_limit
            .charge(project_id, units)
            .map_err(|retry_after_secs| IngestError::RateLimited { retry_after_secs })
    }

    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    pub fn governance(&self) -> &Arc<dyn GovernanceProvider> {
        &self.governance
    }

    pub fn dict_registry(&self) -> &Arc<DictionaryRegistry> {
        &self.dict_registry
    }

    pub fn metrics(&self) -> &IngestMetrics {
        &self.metrics
    }

    pub fn buffer_stats(&self) -> BufferStats {
        BufferStats {
            flush_ms: self.buffer.flush_ms.load(Ordering::Relaxed),
            flush_bytes: self.buffer.flush_bytes.load(Ordering::Relaxed),
            buffer_bytes: self.buffer.node_bytes.load(Ordering::Relaxed),
            capacity_bytes: self.buffer.node_max_bytes.load(Ordering::Relaxed),
            buffer_events: self.buffer.node_events.load(Ordering::Relaxed),
            buffer_batches: self.buffer.node_batches.load(Ordering::Relaxed),
            flushes_timer: self.buffer.flushes_timer.load(Ordering::Relaxed),
            flushes_bytes: self.buffer.flushes_bytes.load(Ordering::Relaxed),
            flushes_drain: self.buffer.flushes_drain.load(Ordering::Relaxed),
        }
    }

    #[doc(hidden)]
    pub fn set_buffer_limits_for_test(
        &self,
        project_max_bytes: usize,
        node_max_bytes: usize,
        flush_ms: u64,
        flush_bytes: usize,
    ) {
        self.buffer
            .project_max_bytes
            .store(project_max_bytes.max(1), Ordering::Relaxed);
        self.buffer
            .node_max_bytes
            .store(node_max_bytes.max(1), Ordering::Relaxed);
        self.buffer.flush_ms.store(flush_ms, Ordering::Relaxed);
        self.buffer
            .flush_bytes
            .store(flush_bytes.max(1), Ordering::Relaxed);
    }

    async fn ensure_project_ready(
        &self,
        project_id: &str,
        now_ms: i64,
    ) -> Result<Arc<pulse_storage::ProjectStorage>> {
        let ps = self.storage.get_or_open_project(project_id).await?;
        // Empty except right after an open, including a re-open after a writer-lease takeover.
        let recovered = ps.take_recovered_uuid_fps();
        if !recovered.is_empty() {
            self.dedup.seed_recovered(project_id, &recovered, now_ms);
        }
        Ok(ps)
    }

    /// The processor for `ps`, started on first use and restarted when the project was re-opened.
    async fn processor_for(
        &self,
        project_id: &str,
        ps: &Arc<pulse_storage::ProjectStorage>,
    ) -> Result<RecordSenders> {
        if let Some(p) = self.processors.get(project_id) {
            if Arc::ptr_eq(&p.ps, ps) {
                return Ok(p.txs.clone());
            }
        }
        let id_graph = self.storage.identity_graph(project_id, true).await?;
        let mut entry = self
            .processors
            .entry(project_id.to_string())
            .or_insert_with(|| {
                spawn_processor(ps.clone(), id_graph.clone(), self.governance.clone())
            });
        if !Arc::ptr_eq(&entry.ps, ps) {
            // Dropping the old sender ends the old task once its queue is drained.
            *entry = spawn_processor(ps.clone(), id_graph, self.governance.clone());
        }
        Ok(entry.txs.clone())
    }

    fn ensure_flusher_started(&self, proj_buf: &Arc<ProjectBuffer>) {
        if proj_buf
            .flusher_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if let Some(weak) = self.self_weak.get().cloned() {
                let pb = proj_buf.clone();
                let handle = tokio::spawn(project_flusher_loop(weak, pb));
                *proj_buf.flusher_handle.lock() = Some(handle);
            } else {
                proj_buf.flusher_started.store(false, Ordering::Release);
            }
        }
    }

    fn reserve_buffer(
        &self,
        project_id: &str,
        num_buckets: usize,
        bytes: usize,
    ) -> Result<BufferReservation> {
        let proj_buf = self.buffer.project(project_id, num_buckets);
        self.ensure_flusher_started(&proj_buf);

        let mut st = proj_buf.state.lock();
        if st.shutting_down {
            return Err(IngestError::Storage(StorageError::Io(
                std::io::Error::other("ingest service shutting down"),
            )));
        }
        // Check sticky WAL failure BEFORE `ingest_buffer` 429 backpressure so disk/WAL errors
        // surface as 503 Service Unavailable even if the buffer backed up while WAL was failing.
        if let Some(ref err) = st.wal_error {
            return Err(err.to_ingest_error());
        }
        let proj_max = self.buffer.project_max_bytes.load(Ordering::Relaxed);
        let node_max = self.buffer.node_max_bytes.load(Ordering::Relaxed);
        let proj_used = st.reserved_bytes + st.buffered_bytes;
        if bytes > 0 && proj_used + bytes > proj_max {
            return Err(IngestError::Storage(StorageError::Backpressure {
                project_id: project_id.to_string(),
                what: "ingest_buffer",
                bytes: proj_used + bytes,
                max_bytes: proj_max,
            }));
        }
        if bytes > 0 {
            let mut cur = self.buffer.node_bytes.load(Ordering::Acquire);
            loop {
                if cur + bytes > node_max {
                    return Err(IngestError::Storage(StorageError::Backpressure {
                        project_id: project_id.to_string(),
                        what: "ingest_buffer",
                        bytes: cur + bytes,
                        max_bytes: node_max,
                    }));
                }
                match self.buffer.node_bytes.compare_exchange_weak(
                    cur,
                    cur + bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
            st.reserved_bytes += bytes;
        }
        drop(st);

        Ok(BufferReservation {
            proj_buf,
            node_bytes: self.buffer.node_bytes.clone(),
            node_events: self.buffer.node_events.clone(),
            node_batches: self.buffer.node_batches.clone(),
            flush_ms: self.buffer.flush_ms.load(Ordering::Relaxed),
            flush_bytes: self.buffer.flush_bytes.load(Ordering::Relaxed),
            reserved_bytes: bytes,
            committed: false,
        })
    }

    /// Flushes any queued W1 buffer batches for `project_id` to the durable WAL.
    pub async fn flush_project_buffer(&self, project_id: &str) -> Result<()> {
        let Some(proj_buf) = self.buffer.projects.get(project_id).map(|e| e.clone()) else {
            return Ok(());
        };
        let mut target_seq = {
            let mut st = proj_buf.state.lock();
            if st.buffered_batches == 0 {
                if let Some(ref err) = st.wal_error {
                    return Err(err.to_ingest_error());
                }
                return Ok(());
            }
            st.explicit_flush = true;
            st.flush_seq + 1
        };
        self.ensure_flusher_started(&proj_buf);
        loop {
            let notified = proj_buf.flush_done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut st = proj_buf.state.lock();
                if st.flush_seq >= target_seq {
                    if let Some(ref err) = st.wal_error {
                        return Err(err.to_ingest_error());
                    }
                    if st.buffered_batches == 0 {
                        return Ok(());
                    }
                    st.explicit_flush = true;
                    target_seq = st.flush_seq + 1;
                } else if st.buffered_batches == 0 {
                    if let Some(ref err) = st.wal_error {
                        return Err(err.to_ingest_error());
                    }
                    return Ok(());
                } else {
                    st.explicit_flush = true;
                }
            }
            proj_buf.wake_flusher.notify_one();
            notified.await;
        }
    }

    /// Flushes buffered W1 events for `project_id` to WAL and waits for the processor to apply them.
    pub async fn flush_project(&self, project_id: &str) -> Result<()> {
        self.flush_project_buffer(project_id).await?;
        if let Some(ps) = self.storage.get_project(project_id) {
            ps.wait_applied(Duration::from_secs(10)).await;
        }
        Ok(())
    }

    /// Flushes all projects' W1 buffers to WAL and waits for their processors to apply them.
    pub async fn flush_all(&self) -> Result<()> {
        let pids: Vec<String> = self
            .buffer
            .projects
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for pid in pids {
            self.flush_project_buffer(&pid).await?;
        }
        for ps in self.storage.open_projects() {
            ps.wait_applied(Duration::from_secs(10)).await;
        }
        Ok(())
    }

    /// Drains all W1 buffers to WAL and cleanly stops all flusher & processor tasks before shutdown.
    pub async fn shutdown(&self) {
        let proj_bufs: Vec<Arc<ProjectBuffer>> = self
            .buffer
            .projects
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for pb in &proj_bufs {
            if let Err(e) = self.flush_project_buffer(&pb.project_id).await {
                tracing::error!(
                    project_id = %pb.project_id,
                    error = %e,
                    "draining ingest buffer on shutdown failed"
                );
            }
            {
                let mut st = pb.state.lock();
                st.shutting_down = true;
            }
            pb.wake_flusher.notify_one();
            let handle = pb.flusher_handle.lock().take();
            if let Some(h) = handle {
                let _ = h.await;
            }
        }
        let procs: Vec<(String, Processor)> = self
            .processors
            .iter()
            .map(|e| {
                (
                    e.key().clone(),
                    Processor {
                        ps: e.value().ps.clone(),
                        txs: e.value().txs.clone(),
                        handles: Mutex::new(std::mem::take(&mut *e.value().handles.lock())),
                    },
                )
            })
            .collect();
        self.processors.clear();
        for (_pid, proc_item) in procs {
            drop(proc_item.txs);
            let handles = std::mem::take(&mut *proc_item.handles.lock());
            for h in handles {
                let _ = h.await;
            }
        }
    }

    pub async fn ingest_raw(
        &self,
        api_key: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<IngestResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        let batch = WireBatch::decode_auto(body, content_type, &self.dict_registry)?;
        self.charge(&project_id, batch.events.len() as u64)?;
        self.ingest_async_for_project(&project_id, batch, Utc::now().timestamp_millis())
            .await
    }

    /// W1 single-node async event ingest:
    /// - Validates lexicon rules, routes to buckets, checks `wal_error` (`503`), hot-segment & L0/log
    ///   backpressure (`429`), and reserves space in `IngestBuffer` (`429` if buffer full).
    /// - Drops per-request `422` schema caps (per-project caps dead-letter in the processor).
    /// - For mixed batches (`identify`/`alias` + events): flushes prior buffer + `wait_applied(1s)`,
    ///   runs `apply_segment_profile_calls` synchronously, and drops the reservation on error so
    ///   zero events are queued.
    /// - Commits events to `IngestBuffer` (`20 ms` / `1 MiB` WAL group commit) and returns `200`
    ///   immediately with `deduped: 0`.
    pub async fn ingest_async_for_project(
        &self,
        project_id: &str,
        mut batch: WireBatch,
        server_now_ms: i64,
    ) -> Result<IngestResponse> {
        let ps = self.ensure_project_ready(project_id, server_now_ms).await?;
        let rules = self.governance.lexicon_rules(project_id);
        let num_buckets = ps.config().num_buckets;

        let mut accepted = 0usize;
        let mut dropped = 0usize;
        let rejected = std::mem::take(&mut batch.rejected);
        let profile_calls = std::mem::take(&mut batch.profile_calls);

        let mut by_bucket: BTreeMap<u8, (Vec<Candidate>, usize)> = BTreeMap::new();
        let mut total_bytes = 0usize;

        for mut ev in batch.events {
            if !rules.apply(&mut ev) {
                dropped += 1;
                continue;
            }
            let seed = if ev.routing_seed.is_empty() {
                ev.distinct_id.as_str()
            } else {
                ev.routing_seed.as_str()
            };
            let bucket_id = route_bucket(seed, num_buckets);
            let fp = event_uuid_fingerprint(&ev.event_uuid);
            ev.client_event_ts_ms = normalize_event_timestamp(
                ev.client_event_ts_ms,
                ev.client_sent_ts_ms,
                server_now_ms,
            );
            let ev_bytes = approx_event_bytes(&ev);
            total_bytes += ev_bytes;
            accepted += 1;
            let entry = by_bucket
                .entry(bucket_id)
                .or_insert_with(|| (Vec::new(), 0));
            entry.0.push((ev, fp));
            entry.1 += ev_bytes;
        }

        // Check sticky WAL failure BEFORE backpressure checks so WAL outages return 503.
        if let Some(pb) = self.buffer.projects.get(project_id) {
            if let Some(ref err) = pb.state.lock().wal_error {
                return Err(err.to_ingest_error());
            }
        }

        if !by_bucket.is_empty() {
            ps.check_hot_segment_backpressure()?;
        }
        for &bucket_id in by_bucket.keys() {
            ps.check_l0_backpressure(bucket_id)?;
        }

        let processor = self.processor_for(project_id, &ps).await?;
        let reservation = self.reserve_buffer(project_id, num_buckets as usize, total_bytes)?;

        let profiles_updated = if !profile_calls.is_empty() {
            self.flush_project_buffer(project_id).await?;
            ps.wait_applied(Duration::from_secs(1)).await;
            self.apply_segment_profile_calls(project_id, profile_calls)
                .await?
        } else {
            0
        };

        reservation.commit(by_bucket, accepted, &ps, &processor, server_now_ms);
        self.metrics.record_receipt(dropped as u64);

        Ok(IngestResponse {
            status: "ok".to_string(),
            accepted,
            deduped: 0,
            dropped,
            server_ts_ms: server_now_ms,
            rejected,
            profiles_updated,
        })
    }

    /// Synchronous event ingestion used by bulk import (`/v1/projects/:id/import`), the demo
    /// seeder, and synchronous tests. Flushes any buffered W1 events for `project_id` first to
    /// preserve FIFO ordering, then runs synchronous WAL group commit + L0 insertion.
    pub async fn ingest_for_project(
        &self,
        project_id: &str,
        mut batch: WireBatch,
        server_now_ms: i64,
    ) -> Result<IngestResponse> {
        self.flush_project_buffer(project_id).await?;
        let ps = self.ensure_project_ready(project_id, server_now_ms).await?;
        let id_graph = self.storage.identity_graph(project_id, true).await?;
        let rules = self.governance.lexicon_rules(project_id);
        let num_buckets = ps.config().num_buckets;
        let dict = ps.dictionary();

        let mut accepted = 0usize;
        let mut deduped = 0usize;
        let mut dropped = 0usize;
        let rejected = std::mem::take(&mut batch.rejected);
        let profile_calls = std::mem::take(&mut batch.profile_calls);

        type PropTypeList = Vec<(String, &'static str)>;
        let mut by_bucket: BTreeMap<u8, Vec<(WireEvent, u32, u64)>> = BTreeMap::new();
        let mut intra_batch_seen: HashSet<(u8, u64)> = HashSet::with_capacity(batch.len());
        let mut new_events_in_batch: HashSet<String> = HashSet::new();
        let mut new_prop_keys_in_batch: HashSet<String> = HashSet::new();

        for mut ev in batch.events {
            // 1. Lexicon Governance Check
            if !rules.apply(&mut ev) {
                dropped += 1;
                continue;
            }

            // 2. Deterministic Bucket Routing & Non-Mutating Dedup Pre-Check
            let seed = if ev.routing_seed.is_empty() {
                ev.distinct_id.as_str()
            } else {
                ev.routing_seed.as_str()
            };
            let bucket_id = route_bucket(seed, num_buckets);
            let fp = event_uuid_fingerprint(&ev.event_uuid);

            if !intra_batch_seen.insert((bucket_id, fp))
                || self
                    .dedup
                    .contains(project_id, bucket_id, fp, server_now_ms)
            {
                deduped += 1;
                continue;
            }

            // 3. Enforce per-batch and per-project schema cardinality governance
            if dict.lookup_event(&ev.event_name).is_none() {
                new_events_in_batch.insert(ev.event_name.clone());
                if new_events_in_batch.len() > MAX_NEW_EVENTS_PER_BATCH
                    || dict.event_count() + new_events_in_batch.len() > MAX_EVENTS_PER_PROJECT
                {
                    return Err(IngestError::SchemaGovernanceLimit(format!(
                        "batch exceeds maximum new or total event names ({MAX_NEW_EVENTS_PER_BATCH} new/batch, {MAX_EVENTS_PER_PROJECT}/project)"
                    )));
                }
            }
            for (k, _) in &ev.props {
                if dict.lookup_prop_key(k).is_none() {
                    new_prop_keys_in_batch.insert(k.clone());
                    if new_prop_keys_in_batch.len() > MAX_NEW_PROP_KEYS_PER_BATCH
                        || dict.prop_key_count() + new_prop_keys_in_batch.len()
                            > MAX_PROP_KEYS_PER_PROJECT
                    {
                        return Err(IngestError::SchemaGovernanceLimit(format!(
                            "batch exceeds maximum new or total property keys ({MAX_NEW_PROP_KEYS_PER_BATCH} new/batch, {MAX_PROP_KEYS_PER_PROJECT}/project)"
                        )));
                    }
                }
            }

            // 4. Mobile Clock-Skew Normalization
            ev.client_event_ts_ms = normalize_event_timestamp(
                ev.client_event_ts_ms,
                ev.client_sent_ts_ms,
                server_now_ms,
            );

            // 5. Dense u32 Identity Interning
            let uid = id_graph.get_or_intern(&ev.distinct_id, bucket_id);
            by_bucket.entry(bucket_id).or_default().push((ev, uid, fp));
        }

        // 6. Enforce bounded local hot-segment & L0 high-water backpressure BEFORE flushing identities or appending WALs.
        if !by_bucket.is_empty() {
            ps.check_hot_segment_backpressure()?;
        }
        for &bucket_id in by_bucket.keys() {
            ps.check_l0_backpressure(bucket_id)?;
        }

        // 7. Flush interned u32 identities durably BEFORE writing WAL or scheduling L1 flushes!
        if !by_bucket.is_empty() && id_graph.has_pending_mutations() {
            id_graph.flush().await?;
        }

        // 8. Group commit per bucket (`P0-2`). Each request queues its events, then takes the bucket
        //    append lock. Whoever holds the lock writes ONE WAL record for everything queued (up to
        //    `WAL_GROUP_MAX_BYTES`), so there is one PUT in flight per bucket log and requests that
        //    arrive during it share the next one. Under the lock, as before (`P1-01`, `P1-02`):
        //    re-check dedup -> durable WAL append -> L0 append -> commit dedup; a request is
        //    answered only after the record holding its events is durable.
        //    L0 -> L1 segment flushes are scheduled asynchronously so ingestion never blocks on storage segment creation!
        let queues = self
            .wal_groups
            .entry(project_id.to_string())
            .or_insert_with(|| {
                Arc::new(
                    (0..num_buckets)
                        .map(|_| Mutex::new(VecDeque::new()))
                        .collect(),
                )
            })
            .clone();
        let bucket_results =
            futures::future::try_join_all(by_bucket.into_iter().map(|(bucket_id, candidates)| {
                let ps = ps.clone();
                let queues = queues.clone();
                let dedup = self.dedup.clone();
                let project_id = project_id.to_string();
                // Spawned so the lock holder, who writes for every request queued behind it,
                // cannot be cancelled by its own client going away.
                let task = tokio::spawn(async move {
                    let project_id = project_id.as_str();
                    let queue = &queues[bucket_id as usize];
                    let enqueue = |candidates: Vec<SyncCandidate>| {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let bytes = candidates
                            .iter()
                            .map(|(ev, _, _)| approx_event_bytes(ev))
                            .sum();
                        queue.lock().push_back(GroupWaiter {
                            candidates,
                            bytes,
                            tx,
                        });
                        rx
                    };
                    let mut rx = enqueue(candidates);
                    let (outcome, should_flush) = {
                        let append_guard = ps.lock_bucket_append(bucket_id).await;
                        let outcome = loop {
                            use tokio::sync::oneshot::error::TryRecvError;
                            match rx.try_recv() {
                                // An earlier lock holder wrote our events.
                                Ok(Ok(outcome)) => break outcome,
                                // The record carrying our events failed: write them ourselves.
                                Ok(Err(candidates)) => rx = enqueue(candidates),
                                Err(TryRecvError::Empty) => {}
                                Err(TryRecvError::Closed) => return Err(IngestError::Storage(
                                    StorageError::Io(std::io::Error::other(
                                        "WAL group write was abandoned before completing; retry",
                                    )),
                                )),
                            }

                            let mut group = Vec::new();
                            let mut group_bytes = 0usize;
                            {
                                let mut q = queue.lock();
                                while let Some(w) = q.front() {
                                    if !group.is_empty()
                                        && group_bytes + w.bytes > WAL_GROUP_MAX_BYTES
                                    {
                                        break;
                                    }
                                    group_bytes += w.bytes;
                                    group.extend(q.pop_front());
                                }
                            }

                            let mut seen_in_group: HashSet<u64> = HashSet::new();
                            let mut to_append = Vec::new();
                            let mut committed_fps = Vec::new();
                            // Per waiter: where its events start in `to_append`, and its dedup count.
                            let mut spans = Vec::with_capacity(group.len());
                            for w in &group {
                                let start = to_append.len();
                                let mut waiter_deduped = 0usize;
                                for (ev, uid, fp) in &w.candidates {
                                    if dedup.contains(project_id, bucket_id, *fp, server_now_ms)
                                        || !seen_in_group.insert(*fp)
                                    {
                                        waiter_deduped += 1;
                                        continue;
                                    }
                                    committed_fps.push(*fp);
                                    to_append.push((ev.clone(), *uid));
                                }
                                spans.push((start, waiter_deduped));
                            }

                            let written = if to_append.is_empty() {
                                Ok(0)
                            } else {
                                ps.append_bucket_events_locked(&append_guard, bucket_id, &to_append)
                                    .await
                            };
                            // The task runs to completion even if this request is dropped, so the
                            // group is always answered.
                            match written {
                                Ok(_) => {
                                    dedup.commit_batch(
                                        project_id,
                                        bucket_id,
                                        &committed_fps,
                                        server_now_ms,
                                    );
                                    // Hand each waiter its own events back, moved, not cloned.
                                    let total = to_append.len();
                                    let mut appended = to_append.into_iter().map(|(ev, _)| ev);
                                    for (i, w) in group.into_iter().enumerate() {
                                        let (start, waiter_deduped) = spans[i];
                                        let end = spans.get(i + 1).map_or(total, |s| s.0);
                                        let events: Vec<_> =
                                            appended.by_ref().take(end - start).collect();
                                        let _ =
                                            w.tx.send(Ok((end - start, waiter_deduped, events)));
                                    }
                                }
                                Err(e) => {
                                    for w in group {
                                        let _ = w.tx.send(Err(w.candidates));
                                    }
                                    // Ours was in the failed record: report the failure. Otherwise
                                    // it is still queued; keep going.
                                    if matches!(rx.try_recv(), Ok(Err(_))) {
                                        return Err(IngestError::Storage(e));
                                    }
                                }
                            }
                        };
                        (outcome, ps.should_flush_bucket(bucket_id))
                    };

                    if should_flush {
                        ps.schedule_background_flush(bucket_id);
                    }

                    Ok::<_, IngestError>(outcome)
                });
                async move {
                    task.await.map_err(|e| {
                        IngestError::Storage(StorageError::Io(std::io::Error::other(format!(
                            "WAL group write task failed: {e}"
                        ))))
                    })?
                }
            }))
            .await?;

        let mut discovery_map: HashMap<String, (u64, PropTypeList)> = HashMap::new();
        for (local_accepted, local_deduped, appended_events) in bucket_results {
            accepted += local_accepted;
            deduped += local_deduped;
            for ev in &appended_events {
                let disc = discovery_map
                    .entry(ev.event_name.clone())
                    .or_insert_with(|| {
                        let ptypes = ev
                            .props
                            .iter()
                            .map(|(k, v)| {
                                let t = match v {
                                    PropValue::Null => "null",
                                    PropValue::Bool(_) => "bool",
                                    PropValue::Int(_) => "int",
                                    PropValue::Float(_) => "float",
                                    PropValue::Str(_) => "string",
                                };
                                (k.clone(), t)
                            })
                            .collect();
                        (0, ptypes)
                    });
                disc.0 += 1;
            }
        }

        if !discovery_map.is_empty() {
            let items = discovery_map
                .into_iter()
                .map(|(event_name, (count, props))| DiscoveredEventSchema {
                    project_id: project_id.to_string(),
                    event_name,
                    count,
                    props,
                })
                .collect();
            self.governance.record_discovery(items);
        }

        self.metrics.record(
            accepted as u64,
            deduped as u64,
            dropped as u64,
            server_now_ms,
        );

        Ok(IngestResponse {
            status: "ok".to_string(),
            accepted,
            deduped,
            dropped,
            server_ts_ms: server_now_ms,
            rejected,
            profiles_updated: self
                .apply_segment_profile_calls(project_id, profile_calls)
                .await?,
        })
    }

    /// Applies Segment calls that travel in event batches:
    /// - `identify` with `userId` + `anonymousId` links the two ids and sets `traits` on the user;
    ///   with only one id it sets `traits` on that id.
    /// - `alias` links `previousId` to `userId`.
    async fn apply_segment_profile_calls(
        &self,
        project_id: &str,
        calls: Vec<serde_json::Value>,
    ) -> Result<usize> {
        let s = |v: &serde_json::Value, keys: &[&str]| {
            keys.iter()
                .find_map(|k| v.get(*k).and_then(|x| x.as_str()))
                .map(str::trim)
                .filter(|x| !x.is_empty())
                .map(str::to_string)
        };
        let mut applied = 0usize;
        for call in calls {
            let kind = s(&call, &["type"]).unwrap_or_default().to_ascii_lowercase();
            let user = s(&call, &["userId", "user_id"]);
            let traits = call
                .get("traits")
                .or_else(|| call.get("context").and_then(|c| c.get("traits")))
                .filter(|t| t.as_object().is_some_and(|o| !o.is_empty()))
                .cloned();
            let other = if kind == "alias" {
                s(&call, &["previousId", "previous_id"])
            } else {
                s(&call, &["anonymousId", "anonymous_id"])
            };
            match (user, other) {
                (Some(user_id), Some(anon_id)) if user_id != anon_id => {
                    self.identify_for_project(
                        project_id,
                        IdentifyRequest {
                            anon_id,
                            user_id,
                            user_properties: traits,
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                (Some(id), _) | (None, Some(id)) => {
                    let Some(t) = traits else { continue };
                    self.engage_for_project(
                        project_id,
                        serde_json::json!({ "distinct_id": id, "$set": t }),
                    )
                    .await?;
                }
                (None, None) => continue,
            }
            applied += 1;
        }
        Ok(applied)
    }

    pub async fn identify(&self, api_key: &str, req: IdentifyRequest) -> Result<IdentifyResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
        self.identify_for_project(&project_id, req).await
    }

    pub async fn identify_for_project(
        &self,
        project_id: &str,
        req: IdentifyRequest,
    ) -> Result<IdentifyResponse> {
        self.flush_project_buffer(project_id).await?;
        let ps = self
            .ensure_project_ready(project_id, Utc::now().timestamp_millis())
            .await?;
        // Keep the old order: events acknowledged before this call get their ids first.
        ps.wait_applied(Duration::from_secs(1)).await;
        let num_buckets = ps.config().num_buckets;
        let id_graph = self.storage.identity_graph(project_id, true).await?;

        let anon_seed = req
            .anon_routing_seed
            .as_deref()
            .or(req.routing_seed.as_deref())
            .unwrap_or(&req.anon_id);
        let user_seed = req.routing_seed.as_deref().unwrap_or(&req.user_id);

        let anon_bucket = route_bucket(anon_seed, num_buckets);
        let user_bucket = route_bucket(user_seed, num_buckets);

        let canonical_uid = id_graph
            .alias_durable(&req.anon_id, &req.user_id, anon_bucket, user_bucket)
            .await?;
        self.governance
            .on_identify(project_id, &req.anon_id, &req.user_id, canonical_uid)
            .await;

        if let Some(up) = req.user_properties {
            let payload = serde_json::json!({ "$set": up });
            self.governance
                .apply_engage(project_id, &req.user_id, canonical_uid, &payload)
                .await;
        }
        if let Some(ops) = req.operations {
            self.governance
                .apply_engage(project_id, &req.user_id, canonical_uid, &ops)
                .await;
        }

        Ok(IdentifyResponse {
            status: "ok".to_string(),
            canonical_uid,
        })
    }

    /// Processes `/engage` or Identify user profile mutations (`Gap 1`).
    pub async fn engage(&self, api_key: &str, body: &[u8]) -> Result<serde_json::Value> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
        let val: serde_json::Value = serde_json::from_slice(body)?;
        let updated = self.engage_for_project(&project_id, val).await?;
        Ok(serde_json::json!({
            "status": "ok",
            "updated": updated,
        }))
    }

    pub async fn engage_for_project(
        &self,
        project_id: &str,
        payload: serde_json::Value,
    ) -> Result<usize> {
        let ps = self
            .ensure_project_ready(project_id, Utc::now().timestamp_millis())
            .await?;
        let id_graph = self.storage.identity_graph(project_id, true).await?;
        let num_buckets = ps.config().num_buckets;

        let items: Vec<serde_json::Value> = match payload {
            serde_json::Value::Array(arr) => arr,
            serde_json::Value::Object(mut obj) => {
                if let Some(serde_json::Value::Array(arr)) =
                    obj.remove("profiles").or_else(|| obj.remove("batch"))
                {
                    arr
                } else {
                    vec![serde_json::Value::Object(obj)]
                }
            }
            _ => {
                return Err(IngestError::Wire(WireError::InvalidPayload(
                    "expected a JSON profile object or array".to_string(),
                )));
            }
        };

        let mut updated = 0usize;
        let mut need_flush = false;
        for item in &items {
            let distinct_id = [
                "distinct_id",
                "$distinct_id",
                "user_id",
                "$user_id",
                "userId",
            ]
            .iter()
            .find_map(|k| item.get(*k).and_then(|v| v.as_str()))
            .map(str::trim)
            .filter(|s| !s.is_empty());
            let Some(did) = distinct_id else {
                continue;
            };
            let seed = item
                .get("routing_seed")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(did);
            let bucket_id = route_bucket(seed, num_buckets);
            let raw_uid = id_graph.get_or_intern(did, bucket_id);
            let canon_uid = id_graph.lookup_canonical(did).unwrap_or(raw_uid);
            need_flush = true;
            self.governance
                .apply_engage(project_id, did, canon_uid, item)
                .await;
            updated += 1;
        }
        if need_flush {
            id_graph.flush().await?;
        }
        Ok(updated)
    }

    /// Processes Group Analytics `/groups` property updates (`Gap 8`).
    pub async fn group_engage(&self, api_key: &str, body: &[u8]) -> Result<serde_json::Value> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
        let val: serde_json::Value = serde_json::from_slice(body)?;
        let updated = self.group_engage_for_project(&project_id, val).await?;
        Ok(serde_json::json!({
            "status": "ok",
            "updated": updated,
        }))
    }

    pub async fn group_engage_for_project(
        &self,
        project_id: &str,
        payload: serde_json::Value,
    ) -> Result<usize> {
        let items: Vec<serde_json::Value> = match payload {
            serde_json::Value::Array(arr) => arr,
            serde_json::Value::Object(_) => vec![payload],
            _ => {
                return Err(IngestError::Wire(WireError::InvalidPayload(
                    "expected a JSON group object or array".to_string(),
                )));
            }
        };
        let mut updated = 0usize;
        for item in &items {
            let group_type = ["group_type", "$group_key", "type"]
                .iter()
                .find_map(|k| item.get(*k).and_then(|v| v.as_str()))
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let group_id = ["group_id", "$group_id", "id"]
                .iter()
                .find_map(|k| item.get(*k).and_then(|v| v.as_str()))
                .map(str::trim)
                .filter(|s| !s.is_empty());
            if let (Some(gt), Some(gid)) = (group_type, group_id) {
                self.governance
                    .apply_group_engage(project_id, gt, gid, item)
                    .await;
                updated += 1;
            }
        }
        Ok(updated)
    }

    pub async fn ingest_replay(
        &self,
        api_key: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ReplayIngestResponse> {
        let project_id = self
            .governance
            .authenticate_api_key(api_key)
            .await
            .ok_or(IngestError::Unauthorized)?;
        self.charge(&project_id, 1)?;
        self.ingest_replay_for_project(&project_id, content_type, body)
            .await
    }

    pub async fn ingest_replay_for_project(
        &self,
        project_id: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ReplayIngestResponse> {
        let mut chunk = ReplayChunk::decode_auto(body, content_type)?;
        chunk.project_id = project_id.to_string();
        let rules = self.governance.lexicon_rules(project_id);
        apply_lexicon_to_replay_chunk(&mut chunk, &rules);
        let res = self.storage.append_replay_chunk(&chunk).await?;
        Ok(ReplayIngestResponse {
            status: "ok".to_string(),
            object_path: res.object_path,
            byte_offset: res.byte_offset,
            replay_offset_kb: res.replay_offset_kb,
            chunk_bytes: res.chunk_bytes,
        })
    }
}

/// Applies project Lexicon PII rules and input masking to wireframe replay labels on ingest.
fn apply_lexicon_to_replay_chunk(chunk: &mut ReplayChunk, rules: &ProjectLexiconRules) {
    let mask_label = |label: &mut String, kind: pulse_wire::NodeKind| {
        if label.is_empty() || label == "████" {
            return;
        }
        if kind == pulse_wire::NodeKind::MaskedInput {
            *label = mask_input_value(label);
            return;
        }
        let lower = label.to_ascii_lowercase();
        for prop in &rules.pii_masked_properties {
            if !prop.is_empty() && lower.contains(&prop.to_ascii_lowercase()) {
                *label = mask_input_value(label);
                return;
            }
        }
        for dropped in &rules.dropped_properties {
            if !dropped.is_empty() && lower.contains(&dropped.to_ascii_lowercase()) {
                label.clear();
                return;
            }
        }
    };

    for op in &mut chunk.ops {
        match op {
            WireframeOp::FullSnapshot { nodes, .. } => {
                for node in nodes {
                    mask_label(&mut node.masked_label, node.kind);
                }
            }
            WireframeOp::NodeUpsert { node, .. } => {
                mask_label(&mut node.masked_label, node.kind);
            }
            _ => {}
        }
    }
}

async fn project_flusher_loop(
    weak_svc: std::sync::Weak<IngestService>,
    proj_buf: Arc<ProjectBuffer>,
) {
    loop {
        let Some(svc) = weak_svc.upgrade() else {
            return;
        };
        let flush_ms = svc.buffer.flush_ms.load(Ordering::Relaxed);
        let flush_bytes = svc.buffer.flush_bytes.load(Ordering::Relaxed);

        enum Action {
            WaitIndefinite,
            WaitUntil(tokio::time::Instant),
            Flush(FlushReason),
            Exit,
        }

        let action = {
            let mut st = proj_buf.state.lock();
            if st.shutting_down && (st.buffered_batches == 0 || st.wal_error.is_some()) {
                if st.buffered_batches > 0 {
                    let dropped_batches = st.buffered_batches;
                    let dropped_events = st.buffered_events;
                    let dropped_bytes = st.buffered_bytes;
                    tracing::error!(
                        project_id = %proj_buf.project_id,
                        dropped_batches,
                        dropped_events,
                        dropped_bytes,
                        error = ?st.wal_error,
                        "exiting project flusher on shutdown with un-WALed batches after persistent WAL/writer error"
                    );
                    for q in &mut st.buckets {
                        q.clear();
                        q.shrink_to(0);
                    }
                    st.buffered_bytes = 0;
                    st.buffered_events = 0;
                    st.buffered_batches = 0;
                    st.oldest_queued_at = None;
                    st.explicit_flush = false;
                    svc.buffer
                        .node_bytes
                        .fetch_sub(dropped_bytes, Ordering::AcqRel);
                    svc.buffer
                        .node_events
                        .fetch_sub(dropped_events, Ordering::AcqRel);
                    svc.buffer
                        .node_batches
                        .fetch_sub(dropped_batches, Ordering::AcqRel);
                }
                Action::Exit
            } else if st.buffered_batches == 0 {
                st.explicit_flush = false;
                Action::WaitIndefinite
            } else if st.wal_error.as_ref().is_some_and(|e| e.is_fenced())
                && !st.explicit_flush
                && !st.shutting_down
            {
                Action::WaitIndefinite
            } else if st.wal_error.is_some()
                && !st.explicit_flush
                && !st.shutting_down
                && st.next_retry_at > tokio::time::Instant::now()
            {
                Action::WaitUntil(st.next_retry_at)
            } else if st.explicit_flush || st.shutting_down {
                st.explicit_flush = false;
                Action::Flush(FlushReason::Drain)
            } else if st.buffered_bytes >= flush_bytes || flush_ms == 0 {
                Action::Flush(FlushReason::Bytes)
            } else if let Some(oldest) = st.oldest_queued_at {
                let deadline = oldest + Duration::from_millis(flush_ms);
                if tokio::time::Instant::now() >= deadline {
                    Action::Flush(FlushReason::Timer)
                } else {
                    Action::WaitUntil(deadline)
                }
            } else {
                Action::Flush(FlushReason::Timer)
            }
        };

        match action {
            Action::Exit => {
                proj_buf.flush_done.notify_waiters();
                return;
            }
            Action::WaitIndefinite => {
                drop(svc);
                proj_buf.wake_flusher.notified().await;
            }
            Action::WaitUntil(deadline) => {
                drop(svc);
                let notified = proj_buf.wake_flusher.notified();
                tokio::pin!(notified);
                tokio::select! {
                    _ = &mut notified => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            Action::Flush(reason) => {
                drain_project_buffer_once(&svc, &proj_buf, reason).await;
            }
        }
    }
}

async fn drain_project_buffer_once(
    svc: &IngestService,
    proj_buf: &Arc<ProjectBuffer>,
    reason: FlushReason,
) {
    match reason {
        FlushReason::Timer => svc.buffer.flushes_timer.fetch_add(1, Ordering::Relaxed),
        FlushReason::Bytes => svc.buffer.flushes_bytes.fetch_add(1, Ordering::Relaxed),
        FlushReason::Drain => svc.buffer.flushes_drain.fetch_add(1, Ordering::Relaxed),
    };

    let num_buckets = { proj_buf.state.lock().buckets.len() };
    for b_idx in 0..num_buckets {
        loop {
            let slice: Vec<BufferedBatch> = {
                let mut st = proj_buf.state.lock();
                let Some(q) = st.buckets.get_mut(b_idx) else {
                    break;
                };
                if q.is_empty() {
                    break;
                }
                let mut out = Vec::new();
                let mut slice_bytes = 0usize;
                while let Some(front) = q.front() {
                    if let Some(first) = out.first() as Option<&BufferedBatch> {
                        if !Arc::ptr_eq(&first.ps, &front.ps) {
                            break;
                        }
                        if slice_bytes + front.bytes > WAL_GROUP_MAX_BYTES {
                            break;
                        }
                    }
                    slice_bytes += front.bytes;
                    if let Some(item) = q.pop_front() {
                        out.push(item);
                    }
                }
                out
            };

            if slice.is_empty() {
                break;
            }

            if let Err((err, slice)) =
                flush_bucket_slice(svc, &proj_buf.project_id, b_idx as u8, slice).await
            {
                let fenced = slice.first().is_some_and(|b| b.ps.is_fenced());
                let mut st = proj_buf.state.lock();
                for item in slice.into_iter().rev() {
                    st.buckets[b_idx].push_front(item);
                }
                st.wal_error = Some(StoredWalFailure::from_storage_err(&err, fenced));
                let next_ms = if st.retry_backoff_ms == 0 {
                    20
                } else {
                    (st.retry_backoff_ms * 2).min(1_000)
                };
                st.retry_backoff_ms = next_ms;
                st.next_retry_at = tokio::time::Instant::now() + Duration::from_millis(next_ms);
                st.flush_seq += 1;
                proj_buf.flush_done.notify_waiters();
                return;
            }
        }
    }

    let mut st = proj_buf.state.lock();
    st.wal_error = None;
    st.retry_backoff_ms = 0;
    if st.buffered_batches == 0 {
        st.oldest_queued_at = None;
        for q in &mut st.buckets {
            if q.is_empty() {
                q.shrink_to(0);
            }
        }
    }
    st.flush_seq += 1;
    proj_buf.flush_done.notify_waiters();
}

async fn flush_bucket_slice(
    svc: &IngestService,
    project_id: &str,
    bucket_id: u8,
    slice: Vec<BufferedBatch>,
) -> std::result::Result<(), (StorageError, Vec<BufferedBatch>)> {
    let ps = slice[0].ps.clone();
    let processor = slice[0].processor.clone();
    let append_guard = ps.lock_bucket_append(bucket_id).await;

    let mut seen_in_group: HashSet<u64> = HashSet::new();
    let mut to_append = Vec::new();
    let mut committed_fps = Vec::new();
    let mut deduped_count = 0usize;
    let mut slice_bytes = 0usize;
    let mut slice_events = 0usize;
    let mut max_now_ms = i64::MIN;

    for b in &slice {
        slice_bytes += b.bytes;
        slice_events += b.events.len();
        max_now_ms = max_now_ms.max(b.server_now_ms);
        for (ev, fp) in &b.events {
            if svc
                .dedup
                .contains(project_id, bucket_id, *fp, b.server_now_ms)
                || !seen_in_group.insert(*fp)
            {
                deduped_count += 1;
                continue;
            }
            committed_fps.push(*fp);
            to_append.push(ev.clone());
        }
    }

    let appended = to_append.len();
    if !to_append.is_empty() {
        let batch = WireBatch::new(to_append);
        let wal_seq = match ps
            .log_events_locked(&append_guard, bucket_id, &batch, slice_bytes)
            .await
        {
            Ok(seq) => seq,
            Err(e) => {
                drop(append_guard);
                return Err((e, slice));
            }
        };
        svc.dedup
            .commit_batch(project_id, bucket_id, &committed_fps, max_now_ms);
        let _ = processor[bucket_id as usize]
            .send(LoggedRecord {
                bucket_id,
                wal_seq,
                events: batch.events,
                fps: committed_fps,
                bytes: slice_bytes,
            })
            .await;
    }
    drop(append_guard);

    svc.metrics
        .record_accepted_and_deduped(appended as u64, deduped_count as u64, max_now_ms);

    let batch_len = slice.len();
    if let Some(proj_buf) = svc.buffer.projects.get(project_id) {
        let mut st = proj_buf.state.lock();
        st.buffered_bytes = st.buffered_bytes.saturating_sub(slice_bytes);
        st.buffered_events = st.buffered_events.saturating_sub(slice_events);
        st.buffered_batches = st.buffered_batches.saturating_sub(batch_len);
    }
    svc.buffer
        .node_bytes
        .fetch_sub(slice_bytes, Ordering::AcqRel);
    svc.buffer
        .node_events
        .fetch_sub(slice_events, Ordering::AcqRel);
    svc.buffer
        .node_batches
        .fetch_sub(batch_len, Ordering::AcqRel);

    Ok(())
}
