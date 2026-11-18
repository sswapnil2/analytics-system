use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDate, Utc};
use dashmap::DashMap;
use moka::sync::Cache;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
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
/// Only seed post-restart dedup from L1 segments whose `max_ts_ms` falls within the 30-min two-window lookback (`P3-04`).
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

/// Per-project storage state managing WALs, L0 `MemBucket`s, MVCC `Manifest`, and Compaction.
pub struct ProjectStorage {
    project_id: String,
    config: StorageConfig,
    object_store: Arc<dyn ObjectStore>,
    id_graph: Arc<ProjectIdentityGraph>,
    manifest_store: ManifestStore,
    manifest: RwLock<Arc<Manifest>>,
    manifest_commit_lock: AsyncMutex<()>,
    dictionary: Arc<ProjectDictionary>,
    wals: Vec<BucketWal>,
    mem_buckets: Vec<MemBucket>,
    /// Per-bucket append mutex serializing `wal_seq` allocation, durable `wal.append`, `l0_seq` assignment,
    /// `MemBucket` insertion, and `DedupRegistry` commit (`P1-01`, `P1-02`).
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
        let project_id = project_id.into();
        let manifest_store = ManifestStore::new(&project_id, object_store.clone());
        let manifest = manifest_store.load_latest().await?;
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
            mem_buckets.push(MemBucket::new(bucket_id));
            bucket_append_locks.push(AsyncMutex::new(()));
            bucket_flush_locks.push(AsyncMutex::new(()));
            flush_in_flight.push(AtomicBool::new(false));
        }

        let ps = Arc::new(Self {
            project_id: project_id.clone(),
            config: config.clone(),
            object_store: object_store.clone(),
            id_graph,
            manifest_store,
            manifest: RwLock::new(Arc::new(manifest)),
            manifest_commit_lock: AsyncMutex::new(()),
            dictionary,
            wals,
            mem_buckets,
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
            // not O(total history) (`P3-04`).
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
                    let mut rows = Vec::with_capacity(batch.len());
                    for ev in &batch.events {
                        let fp = event_uuid_fingerprint(&ev.event_uuid);
                        if !seen_replay_fps.insert(fp) {
                            continue;
                        }
                        ps.recovered_uuid_fps.lock().push((bucket_id, fp));
                        let uid = ps.id_graph.get_or_intern(&ev.distinct_id, bucket_id);
                        rows.push(ps.wire_event_to_row(ev, uid, wal_seq, fp)?);
                    }
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

    /// Reads L0 rows for `bucket_id` strictly from the snapshot's captured MVCC state (`P1-07`).
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

    /// Acquires the per-bucket append mutex (`P1-01`, `P1-02`).
    pub async fn lock_bucket_append(&self, bucket_id: u8) -> AsyncMutexGuard<'_, ()> {
        self.bucket_append_locks[bucket_id as usize].lock().await
    }

    /// Appends events while the caller holds `lock_bucket_append(bucket_id)` (`P1-01`, `P1-02`).
    /// Guarantees `wal_seq` allocation, durable `BucketWal::append`, `l0_seq` allocation, and `MemBucket` insertion
    /// execute in strict total order for `bucket_id`.
    pub async fn append_bucket_events_locked(
        &self,
        _append_guard: &AsyncMutexGuard<'_, ()>,
        bucket_id: u8,
        events: Vec<(WireEvent, u32)>,
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let b_idx = bucket_id as usize;
        let wire_events: Vec<WireEvent> = events.iter().map(|(e, _)| e.clone()).collect();
        let batch = WireBatch::new(wire_events);

        // 1. Durable WAL write BEFORE allocating l0_seq or mutating L0
        let wal_seq = self.wals[b_idx].append(&batch).await?;

        // 2. Allocate monotonic `l0_seq`s and append into L0 MemBucket while still holding `bucket_append_locks[b_idx]`
        let mut rows = Vec::with_capacity(events.len());
        for (ev, uid) in &events {
            let fp = event_uuid_fingerprint(&ev.event_uuid);
            rows.push(self.wire_event_to_row(ev, *uid, wal_seq, fp)?);
        }
        let count = rows.len();
        self.mem_buckets[b_idx].append_batch(rows);
        Ok(count)
    }

    pub fn should_flush_bucket(&self, bucket_id: u8) -> bool {
        self.mem_buckets.get(bucket_id as usize).is_some_and(|mb| {
            mb.should_flush(
                self.config.l0_flush_row_threshold,
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
            self.append_bucket_events_locked(&guard, bucket_id, events)
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

        // Guarantee interned u32 identities are durable in SlateDB BEFORE publishing any L1 segment (`P1-10`).
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
            if !apply(&mut candidate) {
                return Ok(None);
            }
            match self.manifest_store.commit_next(candidate).await {
                Ok(committed) => {
                    let arc = Arc::new(committed);
                    *self.manifest.write() = arc.clone();
                    // ponytail: one LIST per commit; batch every N commits if LIST cost shows up.
                    // Best effort: a failed GC only leaves extra epoch files behind.
                    let _ = self.manifest_store.gc_old_epochs().await;
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

    pub async fn flush_all(&self) -> Result<usize> {
        self.id_graph.flush().await?;
        let mut guards = Vec::with_capacity(self.bucket_flush_locks.len());
        for lock in &self.bucket_flush_locks {
            guards.push(lock.lock().await);
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
            for b in 0..self.config.num_buckets {
                let bucket_id = b as u8;
                let b_idx = b as usize;
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

    /// Background/manual Late-Data Compactor: merges any `(date, bucket)` partition with `>= min_segments`
    /// overlapping L1 segments into a single sorted Vortex segment under a per-partition lock (`P1-06`).
    pub async fn compact_overdue(&self, reader: &VortexSegmentReader) -> Result<usize> {
        self.compact_min_segments(reader, self.config.compaction_min_segments)
            .await
    }

    /// Merges any `(date, bucket)` partition with `>= min_segments` overlapping L1 segments into a
    /// single sorted Vortex segment (`EN-3`).
    pub async fn compact_min_segments(
        &self,
        reader: &VortexSegmentReader,
        min_segments: usize,
    ) -> Result<usize> {
        let min_segments = min_segments.max(2);
        let candidates = self
            .manifest
            .read()
            .partitions_needing_compaction(min_segments);

        let mut compacted_partitions = 0usize;
        for (date, bucket_id) in candidates {
            let part_lock = self
                .compaction_locks
                .entry((date, bucket_id))
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone();
            let _part_guard = part_lock.lock().await;

            let source_segments = {
                let mf = self.manifest.read();
                match mf.active_segments.get(&(date, bucket_id)) {
                    Some(segs) if segs.len() >= min_segments => segs.clone(),
                    _ => continue,
                }
            };

            let source_ids: HashSet<String> = source_segments
                .iter()
                .map(|s| s.segment_id.clone())
                .collect();

            let mut merged_rows = Vec::new();
            for seg in &source_segments {
                let mut rows = reader.read_all_rows(seg).await?;
                merged_rows.append(&mut rows);
            }
            // Erased users' rows are dropped for good here, not just hidden at query time.
            let erased = self.erased_snapshot();
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
                continue;
            };

            let superseded_paths: Vec<String> = source_segments
                .iter()
                .map(|s| s.object_path.clone())
                .collect();

            let committed_ok = self
                .commit_manifest_cas(|next_manifest| {
                    let entry = next_manifest
                        .active_segments
                        .entry((date, bucket_id))
                        .or_default();

                    // Verify ALL `source_ids` are still active before replacing them (`P1-06`).
                    let active_ids: HashSet<&str> =
                        entry.iter().map(|s| s.segment_id.as_str()).collect();
                    if !source_ids
                        .iter()
                        .all(|sid| active_ids.contains(sid.as_str()))
                    {
                        return false;
                    }
                    entry.retain(|s| !source_ids.contains(&s.segment_id));
                    entry.push(new_meta.clone());
                    next_manifest.dict_snapshot = self.dictionary.snapshot();
                    true
                })
                .await?
                .is_some();

            if !committed_ok {
                let orphan = ObjectPath::from(new_meta.object_path.as_str());
                let _ = self.object_store.delete(&orphan).await;
                continue;
            }

            self.pending_gc.lock().extend(superseded_paths);
            compacted_partitions += 1;
        }

        self.run_gc().await?;
        Ok(compacted_partitions)
    }

    /// Deletes superseded segment files whose active reader pin count is 0, re-checked under `manifest.read()` (`P1-05`).
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
pub struct StorageEngine {
    config: StorageConfig,
    encrypted_store: Arc<EncryptedObjectStore>,
    object_store: Arc<dyn ObjectStore>,
    identity_router: Arc<IdentityRouter>,
    segment_reader: Arc<VortexSegmentReader>,
    replay_store: Arc<ReplayStore>,
    projects: DashMap<String, Arc<ProjectStorage>>,
    erased: DashMap<String, Arc<ErasedUids>>,
    open_lock: AsyncMutex<()>,
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
        Arc::new(Self {
            config,
            encrypted_store,
            object_store: wrapped_store,
            identity_router,
            segment_reader,
            replay_store,
            projects: DashMap::new(),
            erased: DashMap::new(),
            open_lock: AsyncMutex::new(()),
        })
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
        let ps = ProjectStorage::open_and_recover(
            project_id,
            self.config.clone(),
            self.object_store.clone(),
            &self.identity_router,
        )
        .await?;
        self.projects.insert(project_id.to_string(), ps.clone());
        // After the insert, so a concurrent `set_erased` is seen by one side or the other.
        if let Some(e) = self.erased.get(project_id) {
            ps.set_erased(e.clone());
        }
        Ok(ps)
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
            total += p.flush_all().await?;
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
                        let _ = engine.identity_router.flush_all().await;
                        let projects: Vec<Arc<ProjectStorage>> =
                            engine.projects.iter().map(|e| e.value().clone()).collect();
                        for p in projects {
                            for b in 0..p.config().num_buckets {
                                let bid = b as u8;
                                if p.should_flush_bucket(bid) {
                                    let _ = p.flush_bucket(bid).await;
                                }
                            }
                            let _ = p.compact_overdue(&engine.segment_reader).await;
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
}
