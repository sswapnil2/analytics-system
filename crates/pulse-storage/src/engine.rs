use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDate;
use dashmap::DashMap;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use pulse_identity::IdentityRouter;
use pulse_wire::{PropValue, ReplayChunk, WireBatch, WireEvent};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

use crate::dictionary::ProjectDictionary;
use crate::error::{Result, StorageError};
use crate::manifest::{Manifest, ManifestStore, ReaderPinTracker, StorageSnapshot};
use crate::mem_bucket::{
    MemBucket, StoredRow, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};
use crate::replay_store::{ReplayAppendResult, ReplayStore};
use crate::segment::{write_vortex_segment, SegmentMeta, VortexSegmentReader};
use crate::wal::BucketWal;

pub const DEFAULT_NUM_BUCKETS: u16 = 256;

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
    manifest_store: ManifestStore,
    manifest: RwLock<Arc<Manifest>>,
    manifest_commit_lock: AsyncMutex<()>,
    dictionary: Arc<ProjectDictionary>,
    wals: Vec<BucketWal>,
    mem_buckets: Vec<MemBucket>,
    bucket_flush_locks: Vec<AsyncMutex<()>>,
    l0_seq: AtomicU64,
    pin_tracker: Arc<ReaderPinTracker>,
    pending_gc: Mutex<Vec<String>>,
    session_flags: DashMap<u64, u16>,
    recovered_uuid_fps: Mutex<Vec<(u8, u64)>>,
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

        let max_committed_l0 = manifest
            .committed_l0_seq
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        let l0_seq = AtomicU64::new(max_committed_l0);

        let mut wals = Vec::with_capacity(config.num_buckets as usize);
        let mut mem_buckets = Vec::with_capacity(config.num_buckets as usize);
        let mut bucket_flush_locks = Vec::with_capacity(config.num_buckets as usize);

        for b in 0..config.num_buckets {
            let bucket_id = b as u8;
            let committed_wal = manifest
                .committed_wal_seq
                .get(&bucket_id)
                .copied()
                .unwrap_or(0);
            wals.push(BucketWal::new(
                &project_id,
                bucket_id,
                manifest.epoch,
                committed_wal,
                object_store.clone(),
            ));
            mem_buckets.push(MemBucket::new(bucket_id));
            bucket_flush_locks.push(AsyncMutex::new(()));
        }

        let ps = Arc::new(Self {
            project_id: project_id.clone(),
            config: config.clone(),
            object_store: object_store.clone(),
            manifest_store,
            manifest: RwLock::new(Arc::new(manifest)),
            manifest_commit_lock: AsyncMutex::new(()),
            dictionary,
            wals,
            mem_buckets,
            bucket_flush_locks,
            l0_seq,
            pin_tracker: Arc::new(ReaderPinTracker::default()),
            pending_gc: Mutex::new(Vec::new()),
            session_flags: DashMap::new(),
            recovered_uuid_fps: Mutex::new(Vec::new()),
        });

        // Replay uncommitted WAL records across all buckets (RPO = 0)
        let id_graph = identity_router.get_or_open(&project_id).await?;
        let mut seen_replay_fps: HashSet<u64> = HashSet::new();

        // Also seed seen_replay_fps from existing L1 segments so duplicate retries of already-flushed
        // batches after an abrupt restart are also idempotently suppressed!
        let reader = VortexSegmentReader::new(object_store);
        let active_segs: Vec<SegmentMeta> = ps
            .manifest
            .read()
            .active_segments
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect();
        for seg in &active_segs {
            if let Ok(batch) = reader.project_columns(seg, &["event_uuid_fp"]).await {
                if let Some(col) = batch.column_by_name("event_uuid_fp") {
                    use arrow_array::cast::AsArray;
                    use arrow_array::types::UInt64Type;
                    let arr = col.as_primitive::<UInt64Type>();
                    for i in 0..arr.len() {
                        let fp = arr.value(i);
                        seen_replay_fps.insert(fp);
                        ps.recovered_uuid_fps.lock().push((seg.bucket_id, fp));
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
                    let uid = id_graph.get_or_intern(&ev.distinct_id, bucket_id);
                    rows.push(ps.wire_event_to_row(ev, uid, wal_seq, fp));
                }
                ps.mem_buckets[b as usize].append_batch(rows);
            }
        }
        id_graph.flush().await?;

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

    /// Acquires a strict read-after-write MVCC `StorageSnapshot`.
    pub fn snapshot(&self) -> StorageSnapshot {
        let manifest = self.manifest.read().clone();
        let watermark = self.l0_seq.load(Ordering::Acquire);
        StorageSnapshot::new(manifest, watermark, self.pin_tracker.clone())
    }

    /// Reads L0 rows for `bucket_id` strictly within the MVCC snapshot window.
    pub fn snapshot_l0_bucket(&self, snap: &StorageSnapshot, bucket_id: u8) -> Vec<StoredRow> {
        let Some(mb) = self.mem_buckets.get(bucket_id as usize) else {
            return Vec::new();
        };
        let floor = snap.bucket_l0_floor(bucket_id);
        mb.snapshot_rows(floor, snap.l0_watermark)
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
    ) -> StoredRow {
        let l0_seq = self.l0_seq.fetch_add(1, Ordering::AcqRel) + 1;
        let session_hash = xxh3_64(ev.session_id.as_bytes());
        let event_id = self.dictionary.get_or_intern_event(&ev.event_name);

        let mut flags = self
            .session_flags
            .get(&session_hash)
            .map(|f| *f)
            .unwrap_or(0);
        if is_error_event(&ev.event_name, &ev.props) {
            flags |= SESSION_FLAG_ERROR;
            self.session_flags
                .entry(session_hash)
                .and_modify(|f| *f |= SESSION_FLAG_ERROR)
                .or_insert(SESSION_FLAG_ERROR);
        }

        let mut props = Vec::with_capacity(ev.props.len());
        for (k, v) in &ev.props {
            let pk = self.dictionary.get_or_intern_prop_key(k);
            props.push((pk, v.clone()));
        }

        StoredRow {
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
        }
    }

    pub fn record_session_flags(&self, session_id: &str, flags: u16) {
        if flags == 0 {
            return;
        }
        let session_hash = xxh3_64(session_id.as_bytes());
        self.session_flags
            .entry(session_hash)
            .and_modify(|f| *f |= flags)
            .or_insert(flags);
        for mb in &self.mem_buckets {
            mb.add_session_flags(session_hash, flags);
        }
    }

    /// Appends a pre-filtered, deduplicated bucket batch durably to `BucketWal` and `MemBucket`.
    pub async fn append_bucket_events(
        &self,
        bucket_id: u8,
        events: Vec<(WireEvent, u32)>,
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let b_idx = bucket_id as usize;
        let wire_events: Vec<WireEvent> = events.iter().map(|(e, _)| e.clone()).collect();
        let batch = WireBatch::new(wire_events);

        // 1. Durable WAL write BEFORE mutating L0 or returning 200 OK
        let wal_seq = self.wals[b_idx].append(&batch).await?;

        // 2. Append into L0 MemBucket
        let mut rows = Vec::with_capacity(events.len());
        for (ev, uid) in &events {
            let fp = event_uuid_fingerprint(&ev.event_uuid);
            rows.push(self.wire_event_to_row(ev, *uid, wal_seq, fp));
        }
        let count = rows.len();
        self.mem_buckets[b_idx].append_batch(rows);

        if self.mem_buckets[b_idx].should_flush(
            self.config.l0_flush_row_threshold,
            self.config.l0_flush_max_age,
        ) {
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

        // Group staged rows by event calendar date (`yyyymmdd`)
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

        // Atomically commit new MVCC Manifest epoch BEFORE evicting from L0 MemBucket!
        {
            let _commit_guard = self.manifest_commit_lock.lock().await;
            let mut next_manifest = (**self.manifest.read()).clone();
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

            let committed = self.manifest_store.commit_next(next_manifest).await?;
            self.wals[b_idx].advance_epoch(committed.epoch);
            *self.manifest.write() = Arc::new(committed);
        }

        // Safe to prune L0 rows and committed WAL records now that MVCC Manifest includes the L1 segments.
        self.mem_buckets[b_idx].complete_flush(max_l0);
        let _ = self.wals[b_idx].purge_up_to(max_wal).await;

        Ok(written_segments)
    }

    pub async fn flush_all(&self) -> Result<usize> {
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

        // Single atomic MVCC Manifest commit across all flushed buckets!
        {
            let _commit_guard = self.manifest_commit_lock.lock().await;
            let mut next_manifest = (**self.manifest.read()).clone();
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

            let committed = self.manifest_store.commit_next(next_manifest).await?;
            for &(_, b_idx, _, _) in &bucket_watermarks {
                self.wals[b_idx].advance_epoch(committed.epoch);
            }
            *self.manifest.write() = Arc::new(committed);
        }

        for &(_, b_idx, max_l0, max_wal) in &bucket_watermarks {
            self.mem_buckets[b_idx].complete_flush(max_l0);
            let _ = self.wals[b_idx].purge_up_to(max_wal).await;
        }

        drop(guards);
        Ok(written_segments.len())
    }

    /// Background/manual Late-Data Compactor: merges any `(date, bucket)` partition with `>= min_segments`
    /// overlapping L1 segments into a single sorted Vortex segment and bumps the MVCC Manifest epoch.
    pub async fn compact_overdue(&self, reader: &VortexSegmentReader) -> Result<usize> {
        let candidates = self
            .manifest
            .read()
            .partitions_needing_compaction(self.config.compaction_min_segments);

        let mut compacted_partitions = 0usize;
        for (date, bucket_id) in candidates {
            let source_segments = {
                let mf = self.manifest.read();
                match mf.active_segments.get(&(date, bucket_id)) {
                    Some(segs) if segs.len() >= self.config.compaction_min_segments => segs.clone(),
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

            {
                let _commit_guard = self.manifest_commit_lock.lock().await;
                let mut next_manifest = (**self.manifest.read()).clone();
                let entry = next_manifest
                    .active_segments
                    .entry((date, bucket_id))
                    .or_default();
                entry.retain(|s| !source_ids.contains(&s.segment_id));
                entry.push(new_meta);
                next_manifest.dict_snapshot = self.dictionary.snapshot();

                let committed = self.manifest_store.commit_next(next_manifest).await?;
                *self.manifest.write() = Arc::new(committed);
            }

            self.pending_gc.lock().extend(superseded_paths);
            compacted_partitions += 1;
        }

        self.run_gc().await?;
        Ok(compacted_partitions)
    }

    /// Deletes superseded segment files whose active reader pin count is 0.
    pub async fn run_gc(&self) -> Result<usize> {
        let candidates = {
            let mut guard = self.pending_gc.lock();
            if guard.is_empty() {
                return Ok(0);
            }
            let mut ready = Vec::new();
            let mut still_pinned = Vec::new();
            for path in guard.drain(..) {
                if self.pin_tracker.is_pinned(&path) {
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
    object_store: Arc<dyn ObjectStore>,
    identity_router: Arc<IdentityRouter>,
    segment_reader: Arc<VortexSegmentReader>,
    replay_store: Arc<ReplayStore>,
    projects: DashMap<String, Arc<ProjectStorage>>,
    open_lock: AsyncMutex<()>,
}

impl StorageEngine {
    pub fn new(
        config: StorageConfig,
        object_store: Arc<dyn ObjectStore>,
        identity_router: Arc<IdentityRouter>,
    ) -> Arc<Self> {
        let segment_reader = Arc::new(VortexSegmentReader::new(object_store.clone()));
        let replay_store = Arc::new(ReplayStore::new(object_store.clone()));
        Arc::new(Self {
            config,
            object_store,
            identity_router,
            segment_reader,
            replay_store,
            projects: DashMap::new(),
            open_lock: AsyncMutex::new(()),
        })
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
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
        Ok(ps)
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

    pub async fn flush_all_projects(&self) -> Result<usize> {
        let list: Vec<Arc<ProjectStorage>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        let mut total = 0usize;
        for p in list {
            total += p.flush_all().await?;
        }
        self.identity_router
            .flush_all()
            .await
            .map_err(StorageError::Identity)?;
        Ok(total)
    }

    pub async fn compact_all_projects(&self) -> Result<usize> {
        let list: Vec<Arc<ProjectStorage>> =
            self.projects.iter().map(|e| e.value().clone()).collect();
        let mut total = 0usize;
        for p in list {
            total += p.compact_overdue(&self.segment_reader).await?;
        }
        Ok(total)
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
                        let projects: Vec<Arc<ProjectStorage>> =
                            engine.projects.iter().map(|e| e.value().clone()).collect();
                        for p in projects {
                            for b in 0..p.config().num_buckets {
                                let bid = b as u8;
                                if p.mem_buckets[b as usize].should_flush(
                                    p.config().l0_flush_row_threshold,
                                    p.config().l0_flush_max_age,
                                ) {
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
