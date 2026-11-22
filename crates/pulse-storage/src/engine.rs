use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Utc};
use dashmap::DashMap;
use futures::stream::{self, StreamExt};
use moka::sync::Cache;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use parking_lot::{Mutex, RwLock};
use pulse_identity::{IdentityRouter, ProjectIdentityGraph};
use pulse_wire::{PropValue, ReplayChunk, WireBatch, WireEvent};
use roaring::RoaringBitmap;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

use crate::dictionary::ProjectDictionary;
use crate::encrypted_store::{ByokEncryptionStatus, EncryptedObjectStore};
use crate::erased::ErasedUids;
use crate::error::{Result, StorageError};
use crate::manifest::{Manifest, ManifestStore, ReaderPinTracker, StorageSnapshot};
use crate::mem_bucket::{
    MemBucket, StoredRow, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};
use crate::replay_store::{ReplayAppendResult, ReplayStore};
use crate::segment::{write_vortex_segment, SegmentMeta, VortexSegmentReader};
use crate::wal::BucketWal;

pub const DEFAULT_NUM_BUCKETS: u16 = 16;
/// Only seed post-restart dedup from L1 segments whose `max_ts_ms` falls within the 30-min two-window lookback.
const DEDUP_SEED_LOOKBACK_MS: i64 = 30 * 60 * 1_000;
const MAX_SESSION_FLAGS_CACHE_ENTRIES: u64 = 262_144;

/// Deterministic entity-centric bucket router (`xxh3_64(routing_seed) % num_buckets`).
#[inline]
pub fn route_bucket(routing_seed: &str, num_buckets: u16) -> u8 {
    debug_assert!((1..=256).contains(&num_buckets));
    (xxh3_64(routing_seed.as_bytes()) % (num_buckets as u64)) as u8
}

#[inline]
pub fn event_uuid_fingerprint(uuid: &[u8; 16]) -> u64 {
    let fp = xxh3_64(uuid);
    if fp == 0 {
        1
    } else {
        fp
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub num_buckets: u16,
    pub l0_flush_row_threshold: usize,
    pub l0_flush_max_age: Duration,
    pub compaction_min_segments: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            num_buckets: DEFAULT_NUM_BUCKETS,
            l0_flush_row_threshold: 250_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        }
    }
}

/// Events that could not be turned into rows and were written to `wal_dead/` instead.
pub static DEAD_LETTERED_EVENTS: AtomicU64 = AtomicU64::new(0);

fn env_mb(name: &str, default_mb: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&mb| mb > 0)
        .unwrap_or(default_mb)
        .saturating_mul(1024 * 1024)
}

/// Byte bounds on the event rows a node holds in memory, and the node-wide running total.
pub struct NodeLoad {
    l0_bytes: Arc<AtomicUsize>,
    /// `PULSE_L0_PROJECT_MAX_MB` (512) / `PULSE_L0_MAX_MB` (1024): rows not yet in a segment.
    l0_max_project: AtomicUsize,
    l0_max_node: AtomicUsize,
}

impl NodeLoad {
    fn from_env() -> Arc<Self> {
        Arc::new(Self {
            l0_bytes: Arc::new(AtomicUsize::new(0)),
            l0_max_project: AtomicUsize::new(env_mb("PULSE_L0_PROJECT_MAX_MB", 512)),
            l0_max_node: AtomicUsize::new(env_mb("PULSE_L0_MAX_MB", 1024)),
        })
    }
}

/// Grants one node at a time the right to write a project's event store (`P0-1`).
#[async_trait::async_trait]
pub trait WriterLeases: Send + Sync {
    /// Takes or renews this node's lease on `project_id`. `Ok(Some(generation))` when this node
    /// holds it; the generation grows every time the holder changes. `Ok(None)` when another
    /// node holds an unexpired lease.
    async fn acquire(&self, project_id: &str) -> std::result::Result<Option<u64>, String>;
    /// How long a lease stays valid without renewal.
    fn ttl(&self) -> Duration;
}

/// How long after the start of a successful `acquire` this node may keep acknowledging
/// writes. Stops a third of the TTL early so a slow clock here cannot overlap a takeover.
fn local_lease_validity(ttl: Duration) -> Duration {
    ttl * 2 / 3
}

/// This node's hold on a project it opened for writing under a lease.
struct WriterFence {
    generation: u64,
    valid_until: Mutex<Instant>,
    fenced: AtomicBool,
}

/// Per-project storage state managing WALs, L0 `MemBucket`s, MVCC `Manifest`, and Compaction.
pub struct ProjectStorage {
    project_id: String,
    config: StorageConfig,
    object_store: Arc<dyn ObjectStore>,
    id_graph: Arc<ProjectIdentityGraph>,
    /// `None` when no lease is configured (embedded use, tests) or the project is read-only here.
    writer: Option<WriterFence>,
    manifest_store: ManifestStore,
    manifest: RwLock<Arc<Manifest>>,
    manifest_commit_lock: AsyncMutex<()>,
    dictionary: Arc<ProjectDictionary>,
    wals: Vec<BucketWal>,
    mem_buckets: Vec<MemBucket>,
    /// Per-bucket append mutex serializing `wal_seq` allocation, durable `wal.append`, `l0_seq` assignment,
    /// `MemBucket` insertion, and `DedupRegistry` commit (`P1-01`, `P1-02`).
    node_load: Arc<NodeLoad>,
    l0_bytes: Arc<AtomicUsize>,
    bucket_append_locks: Vec<AsyncMutex<()>>,
    bucket_flush_locks: Vec<AsyncMutex<()>>,
    compaction_locks: DashMap<(NaiveDate, u8), Arc<AsyncMutex<()>>>,
    l0_seq: AtomicU64,
    max_l0_rows_per_bucket: AtomicUsize,
    flush_in_flight: Vec<AtomicBool>,
    bg_flush_handles: Mutex<Vec<tokio::task::AbortHandle>>,
    pin_tracker: Arc<ReaderPinTracker>,
    pending_gc: Mutex<Vec<String>>,
    session_flags: Cache<u64, u16>,
    recovered_uuid_fps: Mutex<Vec<(u8, u64)>>,
    erased: RwLock<Arc<ErasedUids>>,
}

impl Drop for ProjectStorage {
    fn drop(&mut self) {
        for handle in self.bg_flush_handles.get_mut().drain(..) {
            handle.abort();
        }
    }
}

struct FlushInFlightResetGuard {
    ps: std::sync::Weak<ProjectStorage>,
    b_idx: usize,
}

impl Drop for FlushInFlightResetGuard {
    fn drop(&mut self) {
        if let Some(ps) = self.ps.upgrade() {
            if let Some(flag) = ps.flush_in_flight.get(self.b_idx) {
                flag.store(false, Ordering::Release);
            }
        }
    }
}

fn default_max_l0_rows_per_bucket(config: &StorageConfig) -> usize {
    std::env::var("PULSE_L0_MAX_ROWS_PER_BUCKET")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| config.l0_flush_row_threshold.saturating_mul(4).max(1_000))
}

impl ProjectStorage {
    pub async fn open_and_recover(
        project_id: impl Into<String>,
        config: StorageConfig,
        object_store: Arc<dyn ObjectStore>,
        identity_router: &IdentityRouter,
    ) -> Result<Arc<Self>> {
        Self::open_inner(
            project_id.into(),
            config,
            object_store,
            identity_router,
            None,
            NodeLoad::from_env(),
        )
        .await
    }

    async fn open_inner(
        project_id: String,
        config: StorageConfig,
        object_store: Arc<dyn ObjectStore>,
        identity_router: &IdentityRouter,
        lease: Option<(u64, Instant)>,
        node_load: Arc<NodeLoad>,
    ) -> Result<Arc<Self>> {
        let manifest_store = ManifestStore::new(&project_id, object_store.clone());
        let mut manifest = manifest_store.load_latest().await?;
        if let Some((generation, _)) = lease {
            // Publish our generation BEFORE replaying the WAL: from this commit on, the previous
            // holder's manifest commits (and so its WAL purges) abort, so nothing we are about
            // to replay can be flushed or deleted behind our back.
            while manifest.writer_generation != generation {
                if manifest.writer_generation > generation {
                    return Err(StorageError::NotWriter { project_id });
                }
                let mut next = manifest.clone();
                next.writer_generation = generation;
                manifest = match manifest_store.commit_next(next).await {
                    Ok(committed) => committed,
                    Err(StorageError::ManifestConflict) => manifest_store.load_latest().await?,
                    Err(e) => return Err(e),
                };
            }
        }
        let dictionary = Arc::new(ProjectDictionary::from_snapshot(
            manifest.dict_snapshot.clone(),
        ));
        let id_graph = identity_router.get_or_open(&project_id).await?;

        let max_committed_l0 = manifest
            .committed_l0_seq
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        let l0_seq = AtomicU64::new(max_committed_l0);
        let max_l0_rows_per_bucket = AtomicUsize::new(default_max_l0_rows_per_bucket(&config));

        let l0_bytes = Arc::new(AtomicUsize::new(0));
        let mut wals = Vec::with_capacity(config.num_buckets as usize);
        let mut mem_buckets = Vec::with_capacity(config.num_buckets as usize);
        let mut bucket_append_locks = Vec::with_capacity(config.num_buckets as usize);
        let mut bucket_flush_locks = Vec::with_capacity(config.num_buckets as usize);
        let mut flush_in_flight = Vec::with_capacity(config.num_buckets as usize);

        for b in 0..config.num_buckets {
            let bucket_id = b as u8;
            let committed_wal = manifest
                .committed_wal_seq
                .get(&bucket_id)
                .copied()
                .unwrap_or(0);
            let wal = BucketWal::new(
                &project_id,
                bucket_id,
                manifest.epoch,
                committed_wal,
                object_store.clone(),
            );
            wal.set_min_valid_epoch(manifest.fenced_wal_epoch);
            wals.push(wal);
            mem_buckets.push(MemBucket::with_totals(
                bucket_id,
                vec![l0_bytes.clone(), node_load.l0_bytes.clone()],
            ));
            bucket_append_locks.push(AsyncMutex::new(()));
            bucket_flush_locks.push(AsyncMutex::new(()));
            flush_in_flight.push(AtomicBool::new(false));
        }

        let ps = Arc::new(Self {
            project_id: project_id.clone(),
            config: config.clone(),
            object_store: object_store.clone(),
            id_graph,
            writer: lease.map(|(generation, valid_until)| WriterFence {
                generation,
                valid_until: Mutex::new(valid_until),
                fenced: AtomicBool::new(false),
            }),
            manifest_store,
            manifest: RwLock::new(Arc::new(manifest)),
            manifest_commit_lock: AsyncMutex::new(()),
            dictionary,
            wals,
            mem_buckets,
            node_load,
            l0_bytes,
            bucket_append_locks,
            bucket_flush_locks,
            compaction_locks: DashMap::new(),
            l0_seq,
            max_l0_rows_per_bucket,
            flush_in_flight,
            bg_flush_handles: Mutex::new(Vec::new()),
            pin_tracker: Arc::new(ReaderPinTracker::default()),
            pending_gc: Mutex::new(Vec::new()),
            session_flags: Cache::builder()
                .max_capacity(MAX_SESSION_FLAGS_CACHE_ENTRIES)
                .time_to_idle(Duration::from_secs(3600))
                .build(),
            recovered_uuid_fps: Mutex::new(Vec::new()),
            erased: RwLock::new(Arc::new(ErasedUids::default())),
        });

        if !identity_router.is_read_only() {
            // Replay uncommitted WAL records across all buckets (RPO = 0)
            let mut seen_replay_fps: HashSet<u64> = HashSet::new();

            // Seed dedup ONLY from recent L1 segments (`max_ts_ms >= now - 30 min`) so startup cost is O(recent),
            // not O(total history).
            let cutoff_ts_ms = Utc::now()
                .timestamp_millis()
                .saturating_sub(DEDUP_SEED_LOOKBACK_MS);
            let reader = VortexSegmentReader::new(object_store);
            let recent_segs: Vec<SegmentMeta> = ps
                .manifest
                .read()
                .active_segments
                .values()
                .flat_map(|v| v.iter())
                .filter(|s| s.footer.max_ts_ms >= cutoff_ts_ms)
                .cloned()
                .collect();
            for seg in &recent_segs {
                if let Ok(batch) = reader.project_columns(seg, &["event_uuid_fp"]).await {
                    if let Some(col) = batch.column_by_name("event_uuid_fp") {
                        use arrow_array::cast::AsArray;
                        use arrow_array::types::UInt64Type;
                        let arr = col.as_primitive::<UInt64Type>();
                        let mut rec_guard = ps.recovered_uuid_fps.lock();
                        for i in 0..arr.len() {
                            let fp = arr.value(i);
                            seen_replay_fps.insert(fp);
                            rec_guard.push((seg.bucket_id, fp));
                        }
                    }
                }
            }

            for b in 0..config.num_buckets {
                let bucket_id = b as u8;
                let committed_wal = ps
                    .manifest
                    .read()
                    .committed_wal_seq
                    .get(&bucket_id)
                    .copied()
                    .unwrap_or(0);
                let records = ps.wals[b as usize].recover(committed_wal).await?;
                for (wal_seq, batch) in records {
                    let mut events = Vec::with_capacity(batch.len());
                    for ev in &batch.events {
                        let fp = event_uuid_fingerprint(&ev.event_uuid);
                        if !seen_replay_fps.insert(fp) {
                            continue;
                        }
                        ps.recovered_uuid_fps.lock().push((bucket_id, fp));
                        let uid = ps.id_graph.get_or_intern(&ev.distinct_id, bucket_id);
                        events.push((ev, uid, fp));
                    }
                    let rows = ps.rows_or_dead_letter(bucket_id, wal_seq, &events).await?;
                    ps.mem_buckets[b as usize].append_batch(rows);
                }
            }
            ps.id_graph.flush().await?;
        }

        Ok(ps)
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    pub fn dictionary(&self) -> &Arc<ProjectDictionary> {
        &self.dictionary
    }

    /// Drains recovered `(bucket_id, event_uuid_fp)` pairs so `pulse-ingest` can pre-seed its dedup rings.
    pub fn take_recovered_uuid_fps(&self) -> Vec<(u8, u64)> {
        std::mem::take(&mut *self.recovered_uuid_fps.lock())
    }

    /// Acquires a strict read-after-write MVCC `StorageSnapshot` inside a single `manifest.read()` critical section (`P1-05`, `P1-07`).
    /// Pins active segments AND captures `Arc` L0 bucket slices atomically so concurrent flushes or GC can never
    /// delete a pinned segment or evict unflushed rows before the reader scans them!
    pub fn snapshot(&self) -> StorageSnapshot {
        let mf_guard = self.manifest.read();
        let manifest = mf_guard.clone();
        let watermark = self.l0_seq.load(Ordering::Acquire);
        let l0_buckets: Vec<Arc<[Arc<StoredRow>]>> = self
            .mem_buckets
            .iter()
            .map(|mb| mb.arc_snapshot())
            .collect();
        StorageSnapshot::new(manifest, watermark, l0_buckets, self.pin_tracker.clone())
    }

    /// Reads L0 rows for `bucket_id` strictly from the snapshot's captured MVCC state.
    pub fn snapshot_l0_bucket(&self, snap: &StorageSnapshot, bucket_id: u8) -> Vec<StoredRow> {
        snap.l0_bucket_rows(bucket_id)
    }

    pub fn total_l0_rows(&self) -> usize {
        self.mem_buckets.iter().map(|b| b.len()).sum()
    }

    fn wire_event_to_row(
        &self,
        ev: &WireEvent,
        uid_internal: u32,
        wal_seq: u64,
        event_uuid_fp: u64,
    ) -> Result<StoredRow> {
        let session_hash = xxh3_64(ev.session_id.as_bytes());
        let event_id = self.dictionary.try_get_or_intern_event(&ev.event_name)?;

        let mut flags = self.session_flags.get(&session_hash).unwrap_or(0);
        if is_error_event(&ev.event_name, &ev.props) {
            flags |= SESSION_FLAG_ERROR;
            self.session_flags.insert(session_hash, flags);
        }

        let mut props = Vec::with_capacity(ev.props.len());
        for (k, v) in &ev.props {
            let pk = self.dictionary.try_get_or_intern_prop_key(k)?;
            props.push((pk, v.clone()));
        }

        let l0_seq = self.l0_seq.fetch_add(1, Ordering::AcqRel) + 1;

        Ok(StoredRow {
            l0_seq,
            wal_seq,
            event_uuid_fp,
            uid_internal,
            session_hash,
            session_id: ev.session_id.clone(),
            event_id,
            event_ts_ms: ev.client_event_ts_ms,
            replay_offset_kb: ev.replay_offset_kb,
            session_flags: flags,
            props,
        })
    }

    pub fn record_session_flags(&self, session_id: &str, flags: u16) {
        if flags == 0 {
            return;
        }
        let session_hash = xxh3_64(session_id.as_bytes());
        let prev = self.session_flags.get(&session_hash).unwrap_or(0);
        self.session_flags.insert(session_hash, prev | flags);
        for mb in &self.mem_buckets {
            mb.add_session_flags(session_hash, flags);
        }
    }

    pub fn session_flags_for(&self, session_id: &str) -> u16 {
        let session_hash = xxh3_64(session_id.as_bytes());
        self.session_flags.get(&session_hash).unwrap_or(0)
    }

    /// Acquires the per-bucket append mutex (`P1-01`, `P1-02`).
    /// Fails unless this node may still acknowledge writes: no lease configured, or the lease
    /// is held and was renewed recently enough.
    pub fn check_writer(&self) -> Result<()> {
        match &self.writer {
            Some(w)
                if w.fenced.load(Ordering::Acquire) || Instant::now() >= *w.valid_until.lock() =>
            {
                Err(self.not_writer())
            }
            _ => Ok(()),
        }
    }

    fn not_writer(&self) -> StorageError {
        StorageError::NotWriter {
            project_id: self.project_id.clone(),
        }
    }

    fn fence(&self) {
        if let Some(w) = &self.writer {
            w.fenced.store(true, Ordering::Release);
        }
    }

    pub async fn lock_bucket_append(&self, bucket_id: u8) -> AsyncMutexGuard<'_, ()> {
        self.bucket_append_locks[bucket_id as usize].lock().await
    }

    /// Appends events while the caller holds `lock_bucket_append(bucket_id)` (`P1-01`, `P1-02`).
    /// Guarantees `wal_seq` allocation, durable `BucketWal::append`, `l0_seq` allocation, and
    /// `MemBucket` insertion execute in strict total order for `bucket_id`. Events that cannot
    /// become rows are dead-lettered, so the count can be below `events.len()`.
    pub async fn append_bucket_events_locked(
        &self,
        _append_guard: &AsyncMutexGuard<'_, ()>,
        bucket_id: u8,
        events: &[(WireEvent, u32)],
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let batch = WireBatch::new(events.iter().map(|(e, _)| e.clone()).collect());
        self.check_writer()?;
        // 1. Durable WAL write BEFORE allocating l0_seq or mutating L0
        let wal_seq = match self.wals[bucket_id as usize].append(&batch).await {
            Err(e @ StorageError::NotWriter { .. }) => {
                self.fence();
                return Err(e);
            }
            other => other?,
        };
        // Checked again AFTER the write: if the lease ran out while the PUT was in flight, a new
        // holder may already have listed this log without the record, so it must not be acked.
        self.check_writer()?;

        // 2. Rows into L0 while still holding the bucket append lock. The record is durable, so
        //    an event that cannot become a row is dead-lettered rather than failing the request.
        let events: Vec<_> = events
            .iter()
            .map(|(ev, uid)| (ev, *uid, event_uuid_fingerprint(&ev.event_uuid)))
            .collect();
        let rows = self
            .rows_or_dead_letter(bucket_id, wal_seq, &events)
            .await?;
        let count = rows.len();
        self.mem_buckets[bucket_id as usize].append_batch(rows);
        Ok(count)
    }

    /// Converts events to rows. An event that cannot be converted (today: the project dictionary
    /// is full) is written to `wal_dead/` and skipped, so one bad record never blocks the
    /// project, live or during recovery.
    async fn rows_or_dead_letter(
        &self,
        bucket_id: u8,
        wal_seq: u64,
        events: &[(&WireEvent, u32, u64)],
    ) -> Result<Vec<StoredRow>> {
        let mut rows = Vec::with_capacity(events.len());
        for &(ev, uid, fp) in events {
            match self.wire_event_to_row(ev, uid, wal_seq, fp) {
                Ok(row) => rows.push(row),
                Err(reason) => {
                    self.dead_letter(bucket_id, wal_seq, ev.clone(), fp, &reason)
                        .await?
                }
            }
        }
        Ok(rows)
    }

    /// Stores one event that will not be applied under
    /// `wal_dead/{project}/bucket_{NNN}/seq_{wal_seq}_{fingerprint}.plsb` (a one-event `PLSB`
    /// batch). The name is fixed by the record, so applying it again after a crash writes nothing
    /// new. Fails only if the object store does; the caller must then retry, not skip.
    pub async fn dead_letter(
        &self,
        bucket_id: u8,
        wal_seq: u64,
        ev: WireEvent,
        fp: u64,
        reason: &(dyn std::fmt::Display + Sync),
    ) -> Result<()> {
        let path = ObjectPath::from(format!(
            "wal_dead/{}/bucket_{:03}/seq_{:012}_{:016x}.plsb",
            self.project_id, bucket_id, wal_seq, fp
        ));
        let payload = WireBatch::new(vec![ev]).encode_binary(1)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .object_store
            .put_opts(&path, PutPayload::from(payload), opts)
            .await
        {
            Ok(_) => {
                DEAD_LETTERED_EVENTS.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    project_id = %self.project_id,
                    bucket_id,
                    dead_letter_path = %path,
                    %reason,
                    "event could not be applied; moved to the dead-letter path"
                );
                Ok(())
            }
            Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn should_flush_bucket(&self, bucket_id: u8) -> bool {
        self.mem_buckets.get(bucket_id as usize).is_some_and(|mb| {
            mb.should_flush(
                self.config.l0_flush_row_threshold,
                // Flush early enough that all buckets together sit near half the byte limit.
                self.node_load.l0_max_project.load(Ordering::Relaxed)
                    / (2 * self.config.num_buckets.max(1) as usize),
                self.config.l0_flush_max_age,
            )
        })
    }

    pub fn bucket_l0_rows(&self, bucket_id: u8) -> usize {
        self.mem_buckets
            .get(bucket_id as usize)
            .map_or(0, |mb| mb.len())
    }

    pub fn max_l0_rows_per_bucket(&self) -> usize {
        self.max_l0_rows_per_bucket.load(Ordering::Acquire)
    }

    pub fn set_max_l0_rows_per_bucket(&self, max_rows: usize) {
        self.max_l0_rows_per_bucket
            .store(max_rows.max(1), Ordering::Release);
    }

    /// Rejects writes with `StorageError::L0Backpressure` if a bucket's unflushed L0 buffer hits
    /// the high-water mark, scheduling an asynchronous background flush so ingestion recovers automatically.
    pub fn check_l0_backpressure(self: &Arc<Self>, bucket_id: u8) -> Result<()> {
        let current_rows = self.bucket_l0_rows(bucket_id);
        let max_rows = self.max_l0_rows_per_bucket();
        if current_rows >= max_rows {
            self.schedule_background_flush(bucket_id);
            return Err(StorageError::L0Backpressure {
                project_id: self.project_id.clone(),
                bucket_id,
                current_rows,
                max_rows,
            });
        }
        let load = &self.node_load;
        for (bytes, max_bytes) in [
            (
                self.l0_bytes.load(Ordering::Acquire),
                load.l0_max_project.load(Ordering::Relaxed),
            ),
            (
                load.l0_bytes.load(Ordering::Acquire),
                load.l0_max_node.load(Ordering::Relaxed),
            ),
        ] {
            if bytes >= max_bytes {
                self.schedule_background_flush(bucket_id);
                return Err(StorageError::Backpressure {
                    project_id: self.project_id.clone(),
                    bytes,
                    max_bytes,
                });
            }
        }
        Ok(())
    }

    /// Schedules an asynchronous L0 -> L1 flush for `bucket_id`, deduplicated per bucket via `flush_in_flight`.
    /// Returns `true` if a new background flush task was spawned.
    pub fn schedule_background_flush(self: &Arc<Self>, bucket_id: u8) -> bool {
        let b_idx = bucket_id as usize;
        let Some(flag) = self.flush_in_flight.get(b_idx) else {
            return false;
        };
        if flag
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let ps_weak = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            let _reset_guard = FlushInFlightResetGuard {
                ps: ps_weak.clone(),
                b_idx,
            };
            if let Some(ps) = ps_weak.upgrade() {
                let _ = ps.flush_bucket(bucket_id).await;
            }
        });
        let mut guard = self.bg_flush_handles.lock();
        guard.retain(|h| !h.is_finished());
        guard.push(handle.abort_handle());
        true
    }

    /// Appends a pre-filtered bucket batch durably to `BucketWal` and `MemBucket` under the per-bucket append lock.
    pub async fn append_bucket_events(
        &self,
        bucket_id: u8,
        events: Vec<(WireEvent, u32)>,
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let count = {
            let guard = self.lock_bucket_append(bucket_id).await;
            self.append_bucket_events_locked(&guard, bucket_id, &events)
                .await?
        };
        if self.should_flush_bucket(bucket_id) {
            self.flush_bucket(bucket_id).await?;
        }
        Ok(count)
    }

    /// Flushes a single bucket's L0 `MemBucket` into immutable Vortex L1 segment(s) and commits a new MVCC Manifest epoch.
    pub async fn flush_bucket(&self, bucket_id: u8) -> Result<Vec<SegmentMeta>> {
        let b_idx = bucket_id as usize;
        let Some(flush_lock) = self.bucket_flush_locks.get(b_idx) else {
            return Ok(Vec::new());
        };
        let _bucket_guard = flush_lock.lock().await;

        // Guarantee interned u32 identities are durable in SlateDB BEFORE publishing any L1 segment.
        self.id_graph.flush().await?;

        let floor_l0 = self
            .manifest
            .read()
            .committed_l0_seq
            .get(&bucket_id)
            .copied()
            .unwrap_or(0);

        let Some((max_l0, max_wal, staged_rows)) = self.mem_buckets[b_idx].stage_flush(floor_l0)
        else {
            return Ok(Vec::new());
        };

        let mut by_date: BTreeMap<NaiveDate, Vec<StoredRow>> = BTreeMap::new();
        for row in staged_rows {
            by_date.entry(row.event_date()).or_default().push(row);
        }

        let mut written_segments = Vec::with_capacity(by_date.len());
        for (date, rows) in by_date {
            let seg_id = format!("seg_{}", Uuid::new_v4().simple());
            if let Some(meta) = write_vortex_segment(
                &self.object_store,
                &self.project_id,
                date,
                bucket_id,
                &seg_id,
                rows,
            )
            .await?
            {
                written_segments.push(meta);
            }
        }

        // Atomically commit new MVCC Manifest epoch (with CAS reload-and-merge retries) BEFORE evicting from L0 MemBucket!
        if let Some(committed) = self
            .commit_manifest_cas(|next_manifest| {
                for meta in &written_segments {
                    next_manifest
                        .active_segments
                        .entry((meta.date, bucket_id))
                        .or_default()
                        .push(meta.clone());
                }
                next_manifest
                    .committed_l0_seq
                    .entry(bucket_id)
                    .and_modify(|v| *v = (*v).max(max_l0))
                    .or_insert(max_l0);
                next_manifest
                    .committed_wal_seq
                    .entry(bucket_id)
                    .and_modify(|v| *v = (*v).max(max_wal))
                    .or_insert(max_wal);
                next_manifest.dict_snapshot = self.dictionary.snapshot();
                true
            })
            .await?
        {
            self.wals[b_idx].advance_epoch(committed.epoch);
        }

        self.mem_buckets[b_idx].complete_flush(max_l0);
        let _ = self.wals[b_idx].purge_up_to(max_wal).await;

        Ok(written_segments)
    }

    async fn commit_manifest_cas<F>(&self, mut apply: F) -> Result<Option<Arc<Manifest>>>
    where
        F: FnMut(&mut Manifest) -> bool,
    {
        let _commit_guard = self.manifest_commit_lock.lock().await;
        for attempt in 0..8u8 {
            let mut candidate = (**self.manifest.read()).clone();
            if let Some(w) = &self.writer {
                // A newer generation means another node took the lease and is replaying our WAL.
                // Merging on top of it (as a compactor may) would flush rows it also replays and
                // let us purge WAL objects it still needs: stop for good instead.
                if w.fenced.load(Ordering::Acquire) || candidate.writer_generation > w.generation {
                    self.fence();
                    return Err(self.not_writer());
                }
            }
            if !apply(&mut candidate) {
                return Ok(None);
            }
            match self.manifest_store.commit_next(candidate).await {
                Ok(committed) => {
                    let arc = Arc::new(committed);
                    *self.manifest.write() = arc.clone();
                    // Throttle manifest GC to every 16 commits so bursty flushes/compactions do
                    // not issue an object-store LIST on every single epoch write.
                    // Best effort: a failed GC only leaves extra epoch files behind.
                    if arc.epoch % 16 == 0 {
                        let _ = self.manifest_store.gc_old_epochs().await;
                    }
                    return Ok(Some(arc));
                }
                Err(StorageError::ManifestConflict) if attempt < 7 => {
                    let latest = self.manifest_store.load_latest().await?;
                    self.dictionary.merge_snapshot(&latest.dict_snapshot);
                    *self.manifest.write() = Arc::new(latest);
                }
                Err(e) => return Err(e),
            }
        }
        Err(StorageError::ManifestConflict)
    }

    /// Flushes the specified `bucket_ids` concurrently (bounded to 16 parallel segment writes)
    /// and publishes all resulting L1 segments in a single MVCC Manifest commit.
    pub async fn flush_buckets(&self, bucket_ids: &[u8]) -> Result<usize> {
        let mut sorted: Vec<u8> = bucket_ids
            .iter()
            .copied()
            .filter(|&b| (b as u16) < self.config.num_buckets)
            .collect();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.is_empty() {
            return Ok(0);
        }

        self.id_graph.flush().await?;
        let mut guards = Vec::with_capacity(sorted.len());
        for &b in &sorted {
            guards.push(self.bucket_flush_locks[b as usize].lock().await);
        }

        struct StagedBucket {
            bucket_id: u8,
            b_idx: usize,
            max_l0: u64,
            max_wal: u64,
            by_date: BTreeMap<NaiveDate, Vec<StoredRow>>,
        }

        let mut staged_buckets = Vec::new();
        {
            let mf = self.manifest.read();
            for &bucket_id in &sorted {
                let b_idx = bucket_id as usize;
                let floor_l0 = mf.committed_l0_seq.get(&bucket_id).copied().unwrap_or(0);
                if let Some((max_l0, max_wal, staged_rows)) =
                    self.mem_buckets[b_idx].stage_flush(floor_l0)
                {
                    let mut by_date: BTreeMap<NaiveDate, Vec<StoredRow>> = BTreeMap::new();
                    for row in staged_rows {
                        by_date.entry(row.event_date()).or_default().push(row);
                    }
                    staged_buckets.push(StagedBucket {
                        bucket_id,
                        b_idx,
                        max_l0,
                        max_wal,
                        by_date,
                    });
                }
            }
        }

        if staged_buckets.is_empty() {
            return Ok(0);
        }

        let sem = Arc::new(tokio::sync::Semaphore::new(16));
        let mut join_set = tokio::task::JoinSet::new();
        let mut bucket_watermarks = Vec::with_capacity(staged_buckets.len());
        for sb in staged_buckets {
            bucket_watermarks.push((sb.bucket_id, sb.b_idx, sb.max_l0, sb.max_wal));
            for (date, rows) in sb.by_date {
                let sem_c = sem.clone();
                let store = self.object_store.clone();
                let pid = self.project_id.clone();
                let bucket_id = sb.bucket_id;
                join_set.spawn(async move {
                    let _permit = sem_c.acquire_owned().await.unwrap();
                    let seg_id = format!("seg_{}", Uuid::new_v4().simple());
                    write_vortex_segment(&store, &pid, date, bucket_id, &seg_id, rows).await
                });
            }
        }

        let mut written_segments = Vec::new();
        while let Some(res) = join_set.join_next().await {
            let seg_opt =
                res.map_err(|e| crate::error::StorageError::Corrupted(e.to_string()))??;
            if let Some(meta) = seg_opt {
                written_segments.push(meta);
            }
        }

        if let Some(committed) = self
            .commit_manifest_cas(|next_manifest| {
                for meta in &written_segments {
                    next_manifest
                        .active_segments
                        .entry((meta.date, meta.bucket_id))
                        .or_default()
                        .push(meta.clone());
                }
                for &(bucket_id, _, max_l0, max_wal) in &bucket_watermarks {
                    next_manifest
                        .committed_l0_seq
                        .entry(bucket_id)
                        .and_modify(|v| *v = (*v).max(max_l0))
                        .or_insert(max_l0);
                    next_manifest
                        .committed_wal_seq
                        .entry(bucket_id)
                        .and_modify(|v| *v = (*v).max(max_wal))
                        .or_insert(max_wal);
                }
                next_manifest.dict_snapshot = self.dictionary.snapshot();
                true
            })
            .await?
        {
            for &(_, b_idx, _, _) in &bucket_watermarks {
                self.wals[b_idx].advance_epoch(committed.epoch);
            }
        }

        for &(_, b_idx, max_l0, max_wal) in &bucket_watermarks {
            self.mem_buckets[b_idx].complete_flush(max_l0);
            let _ = self.wals[b_idx].purge_up_to(max_wal).await;
        }

        drop(guards);
        Ok(written_segments.len())
    }

    /// Flushes all buckets that have crossed their row or age threshold in a single manifest commit.
    pub async fn flush_due_buckets(&self) -> Result<usize> {
        let due: Vec<u8> = (0..self.config.num_buckets)
            .map(|b| b as u8)
            .filter(|&bid| self.should_flush_bucket(bid))
            .collect();
        if due.is_empty() {
            return Ok(0);
        }
        self.flush_buckets(&due).await
    }

    pub async fn flush_all(&self) -> Result<usize> {
        let all: Vec<u8> = (0..self.config.num_buckets).map(|b| b as u8).collect();
        self.flush_buckets(&all).await
    }

    /// Background/manual Late-Data Compactor: merges any `(date, bucket)` partition with `>= min_segments`
    /// overlapping L1 segments into a single sorted Vortex segment under a per-partition lock.
    pub async fn compact_overdue(&self, reader: &VortexSegmentReader) -> Result<usize> {
        self.compact_min_segments(reader, self.config.compaction_min_segments)
            .await
    }

    /// Merges any `(date, bucket)` partition with `>= min_segments` overlapping L1 segments into a
    /// single sorted Vortex segment (`EN-3`), compacting up to 32 partitions per manifest commit
    /// with bounded concurrency to avoid O(P * M) manifest write amplification.
    pub async fn compact_min_segments(
        &self,
        reader: &VortexSegmentReader,
        min_segments: usize,
    ) -> Result<usize> {
        const COMPACTION_COMMIT_BATCH: usize = 32;
        const COMPACTION_CONCURRENCY: usize = 8;

        let min_segments = min_segments.max(2);
        let mut candidates = self
            .manifest
            .read()
            .partitions_needing_compaction(min_segments);
        candidates.sort_unstable();

        struct PreparedCompaction {
            date: NaiveDate,
            bucket_id: u8,
            source_ids: HashSet<String>,
            superseded_paths: Vec<String>,
            new_meta: SegmentMeta,
        }

        let mut compacted_partitions = 0usize;
        for chunk in candidates.chunks(COMPACTION_COMMIT_BATCH) {
            // Acquire per-partition locks in strictly ascending `(date, bucket_id)` order.
            let mut part_guards = Vec::with_capacity(chunk.len());
            for &(date, bucket_id) in chunk {
                let part_lock = self
                    .compaction_locks
                    .entry((date, bucket_id))
                    .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                    .clone();
                part_guards.push(part_lock.lock_owned().await);
            }

            let work_items: Vec<(NaiveDate, u8, Vec<SegmentMeta>)> = {
                let mf = self.manifest.read();
                chunk
                    .iter()
                    .filter_map(|&(date, bucket_id)| {
                        mf.active_segments
                            .get(&(date, bucket_id))
                            .filter(|segs| segs.len() >= min_segments)
                            .map(|segs| (date, bucket_id, segs.clone()))
                    })
                    .collect()
            };
            if work_items.is_empty() {
                continue;
            }

            let erased = self.erased_snapshot();
            let results: Vec<Result<Option<PreparedCompaction>>> = stream::iter(work_items)
                .map(|(date, bucket_id, source_segments)| {
                    let erased = erased.clone();
                    async move {
                        let source_ids: HashSet<String> = source_segments
                            .iter()
                            .map(|s| s.segment_id.clone())
                            .collect();
                        let superseded_paths: Vec<String> = source_segments
                            .iter()
                            .map(|s| s.object_path.clone())
                            .collect();

                        let mut merged_rows = Vec::new();
                        for seg in &source_segments {
                            let mut rows = reader.read_all_rows(seg).await?;
                            merged_rows.append(&mut rows);
                        }
                        // Erased users' rows are dropped for good here, not just hidden at query time.
                        if !erased.is_empty() {
                            merged_rows.retain(|r| !erased.hides(r.uid_internal, r.event_ts_ms));
                        }

                        let compacted_id = format!("seg_cmp_{}", Uuid::new_v4().simple());
                        let Some(new_meta) = write_vortex_segment(
                            &self.object_store,
                            &self.project_id,
                            date,
                            bucket_id,
                            &compacted_id,
                            merged_rows,
                        )
                        .await?
                        else {
                            return Ok(None);
                        };

                        Ok(Some(PreparedCompaction {
                            date,
                            bucket_id,
                            source_ids,
                            superseded_paths,
                            new_meta,
                        }))
                    }
                })
                .buffer_unordered(COMPACTION_CONCURRENCY)
                .collect()
                .await;

            let mut prepared = Vec::new();
            let mut first_err: Option<StorageError> = None;
            for res in results {
                match res {
                    Ok(Some(item)) => prepared.push(item),
                    Ok(None) => {}
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }

            if !prepared.is_empty() {
                let mut accepted_flags = vec![false; prepared.len()];
                let committed_ok = self
                    .commit_manifest_cas(|next_manifest| {
                        let mut any_accepted = false;
                        for (idx, item) in prepared.iter().enumerate() {
                            let entry = next_manifest
                                .active_segments
                                .entry((item.date, item.bucket_id))
                                .or_default();
                            // Verify ALL `source_ids` for this partition are still active before replacing them.
                            let active_ids: HashSet<&str> =
                                entry.iter().map(|s| s.segment_id.as_str()).collect();
                            if item
                                .source_ids
                                .iter()
                                .all(|sid| active_ids.contains(sid.as_str()))
                            {
                                entry.retain(|s| !item.source_ids.contains(&s.segment_id));
                                entry.push(item.new_meta.clone());
                                accepted_flags[idx] = true;
                                any_accepted = true;
                            } else {
                                accepted_flags[idx] = false;
                            }
                        }
                        if !any_accepted {
                            return false;
                        }
                        next_manifest.dict_snapshot = self.dictionary.snapshot();
                        true
                    })
                    .await?
                    .is_some();

                if committed_ok {
                    let mut gc_batch = Vec::new();
                    for (item, accepted) in prepared.into_iter().zip(accepted_flags) {
                        if accepted {
                            gc_batch.extend(item.superseded_paths);
                            compacted_partitions += 1;
                        } else {
                            let orphan = ObjectPath::from(item.new_meta.object_path.as_str());
                            let _ = self.object_store.delete(&orphan).await;
                        }
                    }
                    if !gc_batch.is_empty() {
                        self.pending_gc.lock().extend(gc_batch);
                    }
                } else {
                    for item in prepared {
                        let orphan = ObjectPath::from(item.new_meta.object_path.as_str());
                        let _ = self.object_store.delete(&orphan).await;
                    }
                }
            }

            drop(part_guards);
            if let Some(err) = first_err {
                return Err(err);
            }
        }

        self.run_gc().await?;
        Ok(compacted_partitions)
    }

    /// Deletes superseded segment files whose active reader pin count is 0, re-checked under `manifest.read()`.
    pub async fn run_gc(&self) -> Result<usize> {
        let candidates = {
            let mf_guard = self.manifest.read();
            let mut guard = self.pending_gc.lock();
            if guard.is_empty() {
                return Ok(0);
            }
            let active_paths: HashSet<&str> = mf_guard
                .active_segments
                .values()
                .flat_map(|v| v.iter().map(|s| s.object_path.as_str()))
                .collect();
            let mut ready = Vec::new();
            let mut still_pinned = Vec::new();
            for path in guard.drain(..) {
                if active_paths.contains(path.as_str()) || self.pin_tracker.is_pinned(&path) {
                    still_pinned.push(path);
                } else {
                    ready.push(path);
                }
            }
            *guard = still_pinned;
            ready
        };

        let mut deleted = 0usize;
        for rel_path in candidates {
            let obj_path = ObjectPath::from(rel_path.as_str());
            if self.object_store.delete(&obj_path).await.is_ok() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Erasure tombstones used by queries and compaction.
    pub fn erased_snapshot(&self) -> Arc<ErasedUids> {
        self.erased.read().clone()
    }

    pub fn set_erased(&self, erased: Arc<ErasedUids>) {
        *self.erased.write() = erased;
    }

    /// Removes in-memory L0 events belonging to erased UIDs, returning `(purged_rows, session_ids)` (`Gap 4`).
    pub fn purge_user_l0(&self, uids: &RoaringBitmap) -> (u64, Vec<String>) {
        let mut total_purged = 0u64;
        let mut sessions = Vec::new();
        for bucket in &self.mem_buckets {
            let (purged, mut sids) = bucket.purge_uids(uids);
            total_purged += purged;
            sessions.append(&mut sids);
        }
        sessions.sort_unstable();
        sessions.dedup();
        (total_purged, sessions)
    }

    /// Refreshes the in-memory `ProjectIdentityGraph`, `Manifest`, and `ProjectDictionary` from `ObjectStore`.
    pub async fn refresh_manifest(&self) -> Result<bool> {
        let id_refreshed = self.id_graph.refresh().await?;
        let latest = self.manifest_store.load_latest().await?;
        let current_epoch = self.manifest.read().epoch;
        if latest.epoch <= current_epoch {
            return Ok(id_refreshed);
        }
        self.dictionary.merge_snapshot(&latest.dict_snapshot);
        for (&bucket_id, &committed_l0) in &latest.committed_l0_seq {
            if let Some(mb) = self.mem_buckets.get(bucket_id as usize) {
                mb.complete_flush(committed_l0);
            }
        }
        *self.manifest.write() = Arc::new(latest);
        Ok(true)
    }
}

fn is_error_event(event_name: &str, props: &[(String, PropValue)]) -> bool {
    let lower = event_name.to_ascii_lowercase();
    if lower.contains("error")
        || lower.contains("timeout")
        || lower.contains("504")
        || lower.contains("500")
        || lower.contains("failed")
    {
        return true;
    }
    for (k, v) in props {
        if k == "status_code" || k == "http_status" {
            if let Some(code) = v.as_f64() {
                if code >= 500.0 {
                    return true;
                }
            }
        }
    }
    false
}

/// Top-level multi-project storage coordinator managing `ProjectStorage`, `ReplayStore`, `VortexSegmentReader`, and `IdentityRouter`.
/// Background work that failed (flushes, compactions, ...). Each failure is also logged; the
/// counts are exported on the node's `/metrics`.
#[derive(Debug, Default)]
pub struct MaintenanceErrors {
    pub identity_flush: AtomicU64,
    pub flush: AtomicU64,
    pub compaction: AtomicU64,
    pub manifest_refresh: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub enum MaintenanceKind {
    IdentityFlush,
    Flush,
    Compaction,
    ManifestRefresh,
}

impl MaintenanceErrors {
    /// Counts and logs `r` if it failed, instead of dropping it with `let _ =`.
    pub fn check<T, E: std::fmt::Display>(
        &self,
        kind: MaintenanceKind,
        r: std::result::Result<T, E>,
    ) {
        if let Err(e) = r {
            let (counter, what) = match kind {
                MaintenanceKind::IdentityFlush => (&self.identity_flush, "identity flush"),
                MaintenanceKind::Flush => (&self.flush, "L0 flush"),
                MaintenanceKind::Compaction => (&self.compaction, "compaction"),
                MaintenanceKind::ManifestRefresh => (&self.manifest_refresh, "manifest refresh"),
            };
            counter.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %e, "background {what} failed");
        }
    }
}

pub struct StorageEngine {
    maintenance_errors: MaintenanceErrors,
    config: StorageConfig,
    encrypted_store: Arc<EncryptedObjectStore>,
    object_store: Arc<dyn ObjectStore>,
    identity_router: Arc<IdentityRouter>,
    profile_router: Arc<crate::ProfileRouter>,
    segment_reader: Arc<VortexSegmentReader>,
    replay_store: Arc<ReplayStore>,
    projects: DashMap<String, Arc<ProjectStorage>>,
    erased: DashMap<String, Arc<ErasedUids>>,
    open_lock: AsyncMutex<()>,
    writer_leases: OnceLock<Arc<dyn WriterLeases>>,
    node_load: Arc<NodeLoad>,
}

impl StorageEngine {
    pub fn new(
        config: StorageConfig,
        object_store: Arc<dyn ObjectStore>,
        identity_router: Arc<IdentityRouter>,
    ) -> Arc<Self> {
        let encrypted_store = Arc::new(EncryptedObjectStore::new(object_store));
        Self::new_with_encrypted(config, encrypted_store, identity_router)
    }

    pub fn new_with_encrypted(
        config: StorageConfig,
        encrypted_store: Arc<EncryptedObjectStore>,
        identity_router: Arc<IdentityRouter>,
    ) -> Arc<Self> {
        let wrapped_store: Arc<dyn ObjectStore> = encrypted_store.clone();
        let segment_reader = Arc::new(VortexSegmentReader::new(wrapped_store.clone()));
        let replay_store = Arc::new(ReplayStore::new(wrapped_store.clone()));
        let profile_router = Arc::new(crate::ProfileRouter::new(
            wrapped_store.clone(),
            identity_router.is_read_only(),
        ));
        Arc::new(Self {
            maintenance_errors: MaintenanceErrors::default(),
            config,
            encrypted_store,
            object_store: wrapped_store,
            identity_router,
            profile_router,
            segment_reader,
            replay_store,
            projects: DashMap::new(),
            erased: DashMap::new(),
            open_lock: AsyncMutex::new(()),
            writer_leases: OnceLock::new(),
            node_load: NodeLoad::from_env(),
        })
    }

    pub fn profile_router(&self) -> &Arc<crate::ProfileRouter> {
        &self.profile_router
    }

    pub fn maintenance_errors(&self) -> &MaintenanceErrors {
        &self.maintenance_errors
    }

    pub fn encrypted_store(&self) -> &Arc<EncryptedObjectStore> {
        &self.encrypted_store
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    pub fn raw_object_store(&self) -> &Arc<dyn ObjectStore> {
        self.encrypted_store.raw_inner()
    }

    pub fn register_byok_master_key(
        &self,
        key_id: &str,
        key: [u8; 32],
        make_active: bool,
    ) -> Result<()> {
        self.encrypted_store
            .register_master_key(key_id, key, make_active)
    }

    pub fn set_byok_master_key(&self, key: Option<[u8; 32]>) {
        self.encrypted_store.set_master_key(key);
    }

    pub fn disable_new_encryption(&self) {
        self.encrypted_store.disable_new_encryption();
    }

    pub fn byok_encryption_status(&self) -> ByokEncryptionStatus {
        self.encrypted_store.status()
    }

    pub fn identity_router(&self) -> &Arc<IdentityRouter> {
        &self.identity_router
    }

    pub fn segment_reader(&self) -> &Arc<VortexSegmentReader> {
        &self.segment_reader
    }

    pub fn replay_store(&self) -> &Arc<ReplayStore> {
        &self.replay_store
    }

    pub async fn get_or_open_project(&self, project_id: &str) -> Result<Arc<ProjectStorage>> {
        if let Some(p) = self.projects.get(project_id) {
            return Ok(p.clone());
        }
        let _guard = self.open_lock.lock().await;
        if let Some(p) = self.projects.get(project_id) {
            return Ok(p.clone());
        }
        let lease = match self.writer_leases.get() {
            Some(leases) if !self.identity_router.is_read_only() => {
                let started = Instant::now();
                match leases.acquire(project_id).await {
                    Ok(Some(generation)) => {
                        Some((generation, started + local_lease_validity(leases.ttl())))
                    }
                    Ok(None) => {
                        return Err(StorageError::NotWriter {
                            project_id: project_id.to_string(),
                        })
                    }
                    Err(e) => {
                        tracing::warn!(project_id, error = %e, "writer lease store unavailable");
                        return Err(StorageError::NotWriter {
                            project_id: project_id.to_string(),
                        });
                    }
                }
            }
            _ => None,
        };
        let ps = ProjectStorage::open_inner(
            project_id.to_string(),
            self.config.clone(),
            self.object_store.clone(),
            &self.identity_router,
            lease,
            self.node_load.clone(),
        )
        .await?;
        self.projects.insert(project_id.to_string(), ps.clone());
        // After the insert, so a concurrent `set_erased` is seen by one side or the other.
        if let Some(e) = self.erased.get(project_id) {
            ps.set_erased(e.clone());
        }
        Ok(ps)
    }

    /// Overrides the L0 byte bounds read from the environment: `(per project, per node)`.
    pub fn set_l0_byte_limits(&self, project: usize, node: usize) {
        self.node_load
            .l0_max_project
            .store(project, Ordering::Relaxed);
        self.node_load.l0_max_node.store(node, Ordering::Relaxed);
    }

    /// Bytes of event rows this node holds in L0.
    pub fn l0_bytes(&self) -> usize {
        self.node_load.l0_bytes.load(Ordering::Relaxed)
    }

    /// Sets a project's erasure tombstones without opening its storage; applied on open.
    pub fn set_erased(&self, project_id: &str, erased: ErasedUids) {
        let erased = Arc::new(erased);
        self.erased.insert(project_id.to_string(), erased.clone());
        if let Some(p) = self.projects.get(project_id) {
            p.set_erased(erased);
        }
    }

    pub fn erased_snapshot(&self, project_id: &str) -> Arc<ErasedUids> {
        self.erased
            .get(project_id)
            .map(|e| e.clone())
            .unwrap_or_default()
    }

    pub async fn append_replay_chunk(&self, chunk: &ReplayChunk) -> Result<ReplayAppendResult> {
        let res = self.replay_store.append_chunk(chunk).await?;
        let mut flags = 0u16;
        if chunk.has_rage_tap() {
            flags |= SESSION_FLAG_RAGE_TAP;
        }
        if chunk.has_dead_tap() {
            flags |= SESSION_FLAG_DEAD_TAP;
        }
        if chunk.has_error() {
            flags |= SESSION_FLAG_ERROR;
        }
        if flags != 0 {
            let ps = self.get_or_open_project(&chunk.project_id).await?;
            ps.record_session_flags(&chunk.session_id, flags);
        }
        Ok(res)
    }

    /// Makes this engine take a writer lease before opening a project for writing. Read-only
    /// engines (query and compactor nodes) never take one.
    pub fn set_writer_leases(&self, leases: Arc<dyn WriterLeases>) {
        let _ = self.writer_leases.set(leases);
    }

    /// Renews the lease of every project open for writing; call a few times per TTL. A project
    /// whose lease went to another node is fenced and dropped, so the next request re-opens it
    /// from storage once this node holds the lease again.
    pub async fn renew_writer_leases(&self) {
        let Some(leases) = self.writer_leases.get() else {
            return;
        };
        for ps in self.open_projects() {
            let Some(w) = &ps.writer else { continue };
            if w.fenced.load(Ordering::Acquire) {
                self.drop_fenced(&ps);
                continue;
            }
            let started = Instant::now();
            match leases.acquire(&ps.project_id).await {
                Ok(Some(generation)) if generation == w.generation => {
                    *w.valid_until.lock() = started + local_lease_validity(leases.ttl());
                }
                Ok(_) => {
                    tracing::error!(project_id = %ps.project_id, "writer lease lost to another node; refusing writes");
                    ps.fence();
                    self.drop_fenced(&ps);
                }
                // Not extended: writes stop once the last renewal runs out, and resume on the
                // next successful one.
                Err(e) => {
                    tracing::warn!(project_id = %ps.project_id, error = %e, "writer lease renewal failed");
                }
            }
        }
    }

    fn drop_fenced(&self, ps: &Arc<ProjectStorage>) {
        self.projects
            .remove_if(&ps.project_id, |_, cur| Arc::ptr_eq(cur, ps));
        // Their in-memory state is stale once another node has written the project.
        self.identity_router.evict(&ps.project_id);
        self.profile_router.evict(&ps.project_id);
    }

    pub fn open_projects(&self) -> Vec<Arc<ProjectStorage>> {
        self.projects.iter().map(|e| e.value().clone()).collect()
    }

    pub async fn flush_all_projects(&self) -> Result<usize> {
        self.identity_router
            .flush_all()
            .await
            .map_err(StorageError::Identity)?;
        let mut total = 0usize;
        for p in self.open_projects() {
            match p.flush_all().await {
                // Its rows are in the WAL the new writer replays; do not hold up the others.
                Err(StorageError::NotWriter { .. }) => {}
                other => total += other?,
            }
        }
        Ok(total)
    }

    pub async fn compact_all_projects(&self) -> Result<usize> {
        let mut total = 0usize;
        for p in self.open_projects() {
            total += p.compact_overdue(&self.segment_reader).await?;
        }
        Ok(total)
    }

    pub async fn compact_all_with_min_segments(&self, min_segments: usize) -> Result<usize> {
        let mut total = 0usize;
        for p in self.open_projects() {
            total += p
                .compact_min_segments(&self.segment_reader, min_segments)
                .await?;
        }
        Ok(total)
    }

    /// Refreshes manifests for all opened projects from `ObjectStore`, returning how many projects updated.
    pub async fn refresh_all_manifests(&self) -> Result<usize> {
        let mut refreshed = 0usize;
        for p in self.open_projects() {
            if p.refresh_manifest().await? {
                refreshed += 1;
            }
        }
        Ok(refreshed)
    }

    /// Spawns background L0 age-based flusher and late-data compactor loop.
    pub fn spawn_background_maintenance(
        self: &Arc<Self>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let errs = &engine.maintenance_errors;
                        errs.check(MaintenanceKind::IdentityFlush, engine.identity_router.flush_all().await);
                        let projects: Vec<Arc<ProjectStorage>> =
                            engine.projects.iter().map(|e| e.value().clone()).collect();
                        for p in projects {
                            errs.check(MaintenanceKind::Flush, p.flush_due_buckets().await);
                            errs.check(
                                MaintenanceKind::Compaction,
                                p.compact_overdue(&engine.segment_reader).await,
                            );
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod manifest_retention_tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::time::Duration;

    #[tokio::test]
    async fn recovery_and_reads_survive_manifest_gc() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let router = IdentityRouter::new(store.clone());
        let cfg = StorageConfig {
            num_buckets: 1,
            ..StorageConfig::default()
        };
        let ps = ProjectStorage::open_and_recover("p", cfg.clone(), store.clone(), &router)
            .await
            .unwrap();
        for i in 0..20i64 {
            let ts = 1_790_000_000_000 + i * 1_000;
            let ev = WireEvent {
                event_uuid: WireEvent::deterministic_uuid("u", "s", "e", ts, i as u64),
                routing_seed: "u".into(),
                distinct_id: "u".into(),
                session_id: "s".into(),
                event_name: "e".into(),
                client_event_ts_ms: ts,
                client_sent_ts_ms: 0,
                props: vec![],
                replay_offset_kb: 0,
            };
            ps.append_bucket_events(0, vec![(ev, 1)]).await.unwrap();
            ps.flush_bucket(0).await.unwrap();
        }
        let epoch = ps.snapshot().epoch();
        assert!(epoch >= 20);
        drop(ps);

        let ms = ManifestStore::new("p", store.clone()).with_retention(2, Duration::ZERO);
        assert!(ms.gc_old_epochs().await.unwrap() as u64 >= epoch - 2);

        // Crash recovery reads only the newest epoch and sees every flushed row.
        let reopened = ProjectStorage::open_and_recover("p", cfg, store, &router)
            .await
            .unwrap();
        let snap = reopened.snapshot();
        assert_eq!(snap.epoch(), epoch);
        assert_eq!(snap.manifest.total_l1_rows(), 20);
        let reader = VortexSegmentReader::new(reopened.object_store.clone());
        let mut rows = 0;
        for seg in snap.manifest.active_segments.values().flatten() {
            rows += reader.read_all_rows(seg).await.unwrap().len();
        }
        assert_eq!(rows, 20);
    }

    /// An event that cannot become a row (here: the dictionary is full) used to fail the request
    /// after its WAL record was durable, and then fail every later open of the project.
    #[tokio::test]
    async fn unappliable_event_is_dead_lettered_and_never_blocks_the_project() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let router = IdentityRouter::new(store.clone());
        let cfg = StorageConfig {
            num_buckets: 1,
            ..StorageConfig::default()
        };
        let ev = |name: &str, i: i64| WireEvent {
            event_uuid: WireEvent::deterministic_uuid("u", "s", name, i, i as u64),
            routing_seed: "u".into(),
            distinct_id: "u".into(),
            session_id: "s".into(),
            event_name: name.into(),
            client_event_ts_ms: 1_790_000_000_000 + i,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        };
        let dead_letters = || async {
            use futures::TryStreamExt;
            let prefix = ObjectPath::from("wal_dead/p");
            let found: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
            found.len()
        };

        let ps = ProjectStorage::open_and_recover("p", cfg.clone(), store.clone(), &router)
            .await
            .unwrap();
        ps.append_bucket_events(0, vec![(ev("known", 0), 1)])
            .await
            .unwrap();
        let mut n = 0;
        while ps
            .dictionary()
            .try_get_or_intern_event(&format!("filler_{n}"))
            .is_ok()
        {
            n += 1;
        }
        // Persist the full dictionary so recovery starts from it too.
        ps.flush_bucket(0).await.unwrap();

        let applied = ps
            .append_bucket_events(0, vec![(ev("one_too_many", 1), 1), (ev("known", 2), 1)])
            .await
            .expect("a record with an unappliable event is still accepted");
        assert_eq!(applied, 1);
        assert_eq!(ps.total_l0_rows(), 1);
        assert_eq!(dead_letters().await, 1);
        drop(ps);

        // Crash before the flush: recovery meets the same record and must not stop on it.
        let reopened = ProjectStorage::open_and_recover("p", cfg, store.clone(), &router)
            .await
            .expect("project opens despite the unappliable event in its WAL");
        assert_eq!(reopened.total_l0_rows(), 1);
        assert_eq!(
            dead_letters().await,
            1,
            "replay dead-lettered the event twice"
        );
    }

    /// Rows in L0 are bounded in bytes, per project and per node, until a flush moves them to a
    /// segment.
    #[tokio::test]
    async fn l0_bytes_push_back_until_a_flush() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = StorageEngine::new(
            StorageConfig {
                num_buckets: 1,
                ..StorageConfig::default()
            },
            store.clone(),
            Arc::new(IdentityRouter::new(store)),
        );
        let ps = engine.get_or_open_project("p").await.unwrap();
        let ev = WireEvent {
            event_uuid: WireEvent::deterministic_uuid("u", "s", "e", 1, 1),
            routing_seed: "u".into(),
            distinct_id: "u".into(),
            session_id: "s".into(),
            event_name: "e".into(),
            client_event_ts_ms: 1_790_000_000_000,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        };
        ps.append_bucket_events(0, vec![(ev, 1)]).await.unwrap();
        assert!(engine.l0_bytes() > 0);
        ps.check_l0_backpressure(0).unwrap();
        for (project, node) in [(1, usize::MAX), (usize::MAX, 1)] {
            engine.set_l0_byte_limits(project, node);
            assert!(matches!(
                ps.check_l0_backpressure(0),
                Err(StorageError::Backpressure { .. })
            ));
        }
        ps.flush_all().await.unwrap();
        ps.check_l0_backpressure(0).unwrap();
        assert_eq!(engine.l0_bytes(), 0);
    }

    struct FakeLeases {
        me: &'static str,
        holder: Arc<Mutex<(&'static str, u64)>>,
    }

    #[async_trait::async_trait]
    impl WriterLeases for FakeLeases {
        async fn acquire(&self, _project_id: &str) -> std::result::Result<Option<u64>, String> {
            let (holder, generation) = *self.holder.lock();
            Ok((holder == self.me).then_some(generation))
        }
        fn ttl(&self) -> Duration {
            Duration::from_secs(60)
        }
    }

    /// A standby that takes the lease replays the previous writer's WAL and loses nothing it
    /// acknowledged; the previous writer, still alive, can no longer commit, purge or ack.
    #[tokio::test]
    async fn takeover_replays_the_old_writers_wal_and_fences_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let holder = Arc::new(Mutex::new(("a", 1u64)));
        let engine = |me| {
            let e = StorageEngine::new(
                StorageConfig {
                    num_buckets: 1,
                    ..StorageConfig::default()
                },
                store.clone(),
                Arc::new(IdentityRouter::new(store.clone())),
            );
            e.set_writer_leases(Arc::new(FakeLeases {
                me,
                holder: holder.clone(),
            }));
            e
        };
        let ev = |i: i64| {
            let ts = 1_790_000_000_000 + i * 1_000;
            WireEvent {
                event_uuid: WireEvent::deterministic_uuid("u", "s", "e", ts, i as u64),
                routing_seed: "u".into(),
                distinct_id: "u".into(),
                session_id: "s".into(),
                event_name: "e".into(),
                client_event_ts_ms: ts,
                client_sent_ts_ms: 0,
                props: vec![],
                replay_offset_kb: 0,
            }
        };
        let is_not_writer = |r: Result<usize>| matches!(r, Err(StorageError::NotWriter { .. }));

        let a = engine("a");
        let ps_a = a.get_or_open_project("p").await.unwrap();
        for i in 0..3 {
            ps_a.append_bucket_events(0, vec![(ev(i), 1)])
                .await
                .unwrap();
        }
        ps_a.flush_bucket(0).await.unwrap();
        // Acknowledged but only in the WAL when the writer "dies".
        for i in 3..5 {
            ps_a.append_bucket_events(0, vec![(ev(i), 1)])
                .await
                .unwrap();
        }

        let b = engine("b");
        assert!(matches!(
            b.get_or_open_project("p").await,
            Err(StorageError::NotWriter { .. })
        ));

        *holder.lock() = ("b", 2);
        let ps_b = b.get_or_open_project("p").await.unwrap();
        assert_eq!(ps_b.snapshot().manifest.total_l1_rows(), 3);
        assert_eq!(
            ps_b.total_l0_rows(),
            2,
            "acknowledged WAL records were not replayed"
        );

        // The old writer is still running. Its flush must not publish the two rows a second
        // time nor purge the WAL records the new writer has not flushed yet.
        assert!(is_not_writer(ps_a.flush_bucket(0).await.map(|v| v.len())));
        assert!(is_not_writer(
            ps_a.append_bucket_events(0, vec![(ev(9), 1)]).await
        ));
        a.renew_writer_leases().await;
        assert!(a.open_projects().is_empty());

        // A third node taking over before `b` flushed still finds every acknowledged record.
        *holder.lock() = ("c", 3);
        let ps_c = engine("c").get_or_open_project("p").await.unwrap();
        assert_eq!(ps_c.snapshot().manifest.total_l1_rows(), 3);
        assert_eq!(ps_c.total_l0_rows(), 2);
        ps_c.flush_bucket(0).await.unwrap();
        assert_eq!(ps_c.snapshot().manifest.total_l1_rows(), 5);
        assert!(is_not_writer(ps_b.flush_bucket(0).await.map(|v| v.len())));
    }

    #[tokio::test]
    async fn flush_due_buckets_and_batched_compaction_coalesce_manifest_epochs() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let router = IdentityRouter::new(store.clone());
        let cfg = StorageConfig {
            num_buckets: 10,
            l0_flush_row_threshold: 100,
            l0_flush_max_age: Duration::ZERO,
            compaction_min_segments: 2,
        };
        let ps = ProjectStorage::open_and_recover("p_batch", cfg, store.clone(), &router)
            .await
            .unwrap();

        let make_ev = |b: u8, wave: i64| WireEvent {
            event_uuid: WireEvent::deterministic_uuid(
                &format!("u_{b}"),
                &format!("s_{b}"),
                "click",
                1_790_000_000_000 + wave * 1_000,
                (b as u64) * 100 + wave as u64,
            ),
            routing_seed: format!("u_{b}"),
            distinct_id: format!("u_{b}"),
            session_id: format!("s_{b}"),
            event_name: "click".into(),
            client_event_ts_ms: 1_790_000_000_000 + wave * 1_000,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        };

        // Wave 1: append 1 event to each of 10 buckets, flush all due buckets in ONE epoch.
        for b in 0..10u8 {
            let guard = ps.lock_bucket_append(b).await;
            ps.append_bucket_events_locked(&guard, b, &[(make_ev(b, 1), b as u32 + 1)])
                .await
                .unwrap();
        }
        let ep0 = ps.snapshot().epoch();
        let flushed1 = ps.flush_due_buckets().await.unwrap();
        assert_eq!(flushed1, 10);
        let ep1 = ps.snapshot().epoch();
        assert_eq!(
            ep1,
            ep0 + 1,
            "10 due buckets should commit in 1 manifest epoch"
        );

        // Wave 2: append another event to each of 10 buckets, flush all due buckets in ONE epoch.
        for b in 0..10u8 {
            let guard = ps.lock_bucket_append(b).await;
            ps.append_bucket_events_locked(&guard, b, &[(make_ev(b, 2), b as u32 + 1)])
                .await
                .unwrap();
        }
        let flushed2 = ps.flush_due_buckets().await.unwrap();
        assert_eq!(flushed2, 10);
        let ep2 = ps.snapshot().epoch();
        assert_eq!(ep2, ep1 + 1);
        assert_eq!(ps.snapshot().manifest.total_l1_rows(), 20);

        // Compaction: all 10 partitions now have 2 segments -> compact all 10 in ONE manifest epoch.
        let reader = VortexSegmentReader::new(store.clone());
        let compacted = ps.compact_min_segments(&reader, 2).await.unwrap();
        assert_eq!(compacted, 10);
        let ep3 = ps.snapshot().epoch();
        assert_eq!(
            ep3,
            ep2 + 1,
            "10 compacted partitions (< 32 batch size) should commit in 1 manifest epoch"
        );
        let snap = ps.snapshot();
        assert_eq!(snap.manifest.total_l1_rows(), 20);
        assert!(snap
            .manifest
            .active_segments
            .values()
            .all(|segs| segs.len() == 1));
    }
}
