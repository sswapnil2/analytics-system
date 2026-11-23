use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Utc};
use dashmap::DashMap;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
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
use crate::manifest::{
    IdentitySnapshotRef, Manifest, ManifestStore, PartitionEntry, PendingS3Delete,
    PendingS3DeleteKind, ProfileSnapshotRef, PublishedManifest, PublishedManifestStore,
    ReaderPinTracker, SnapshotRef, StorageSnapshot, UploadIntent, UploadIntentBatch,
    UploadIntentStore,
};
use crate::mem_bucket::{
    MemBucket, StoredRow, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};
use crate::profile_store::{ProfileRouter, ProjectProfileStore};
use crate::replay_store::{ReplayAppendResult, ReplayStore};
use crate::segment::{
    write_vortex_segment, write_vortex_segment_opts, SegmentMeta, VortexSegmentReader,
};
use crate::snapshot::{
    delete_objects_with_prefix, restore_identity_from_snapshot_read_only,
    restore_profiles_from_snapshot_read_only,
};
use crate::store_factory::is_hot_routed_store;
use crate::wal::BucketWal;

pub const DEFAULT_NUM_BUCKETS: u16 = 16;
/// Default interval (`PULSE_UPLOAD_INTERVAL_SECS`) between S3 `published/` cycles (300 s; `0` = eager).
pub const DEFAULT_UPLOAD_INTERVAL_SECS: u64 = 300;
/// Default closed-date upload timer (`PULSE_CLOSED_DATE_UPLOAD_SECS`, 1 hour).
pub const DEFAULT_CLOSED_DATE_UPLOAD_SECS: u64 = 3600;
/// Default closed-date local byte threshold (`PULSE_CLOSED_DATE_MAX_BYTES`, 4 MiB).
pub const DEFAULT_CLOSED_DATE_MAX_BYTES: u64 = 4 * 1024 * 1024;
/// Default input-byte cap per merged S3 output segment (`PULSE_UPLOAD_MERGE_MAX_INPUT_BYTES`, 64 MiB).
pub const DEFAULT_UPLOAD_MERGE_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;
/// Default per-project `hot/segments/` `1x` limit in MiB (`PULSE_HOT_SEGMENTS_PROJECT_MAX_MB`).
pub const DEFAULT_HOT_SEGMENTS_PROJECT_MAX_MB: u64 = 512;
/// Default per-node `hot/segments/` `1x` limit in MiB (`PULSE_HOT_SEGMENTS_MAX_MB`).
pub const DEFAULT_HOT_SEGMENTS_MAX_MB: u64 = 2048;
/// Default shared flush concurrency (`PULSE_FLUSH_CONCURRENCY`, 4 concurrent segment builds/writes).
pub const DEFAULT_FLUSH_CONCURRENCY: usize = 4;
/// Default input-byte cap per cold S3 compaction merge pass (`64 MiB`).
const DEFAULT_COMPACTION_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;
/// Minimum cooldown between upload retries triggered by `1x`/`2x` pressure after an S3 upload failure.
const UPLOAD_FAILURE_COOLDOWN: Duration = Duration::from_secs(2);
/// Only seed post-restart dedup from L1 segments whose `max_ts_ms` falls within the 30-min two-window lookback.
const DEDUP_SEED_LOOKBACK_MS: i64 = 30 * 60 * 1_000;
const MAX_SESSION_FLAGS_CACHE_ENTRIES: u64 = 262_144;

/// Reads `PULSE_UPLOAD_INTERVAL_SECS` (default `300`, `0` = eager publish).
pub fn upload_interval_from_env() -> Duration {
    let secs = std::env::var("PULSE_UPLOAD_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_UPLOAD_INTERVAL_SECS);
    Duration::from_secs(secs)
}

/// Reads `PULSE_CLOSED_DATE_UPLOAD_SECS` (default `3600`).
pub fn closed_date_upload_interval_from_env() -> Duration {
    let secs = std::env::var("PULSE_CLOSED_DATE_UPLOAD_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CLOSED_DATE_UPLOAD_SECS);
    Duration::from_secs(secs)
}

/// Reads `PULSE_CLOSED_DATE_MAX_BYTES` (default `4 MiB`).
pub fn closed_date_max_bytes_from_env() -> u64 {
    std::env::var("PULSE_CLOSED_DATE_MAX_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&b| b > 0)
        .unwrap_or(DEFAULT_CLOSED_DATE_MAX_BYTES)
}

/// Reads `PULSE_UPLOAD_MERGE_MAX_INPUT_BYTES` (default `64 MiB`).
pub fn upload_merge_max_input_bytes_from_env() -> u64 {
    std::env::var("PULSE_UPLOAD_MERGE_MAX_INPUT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&b| b > 0)
        .unwrap_or(DEFAULT_UPLOAD_MERGE_MAX_INPUT_BYTES)
}

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

fn env_mb_u64(name: &str, default_mb: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&mb| mb > 0)
        .unwrap_or(default_mb)
        .saturating_mul(1024 * 1024)
}

fn env_usize(name: &str, default_val: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default_val)
}

/// Byte bounds on what a node holds in memory for events and local `hot/segments/` awaiting upload.
pub struct NodeLoad {
    l0_bytes: Arc<AtomicUsize>,
    unapplied_bytes: AtomicUsize,
    /// `PULSE_L0_PROJECT_MAX_MB` (512) / `PULSE_L0_MAX_MB` (1024): rows not yet in a segment.
    l0_max_project: AtomicUsize,
    l0_max_node: AtomicUsize,
    /// `PULSE_LOG_LAG_PROJECT_MAX_MB` (256) / `PULSE_LOG_LAG_MAX_MB` (512): events in the WAL
    /// that the processor has not applied yet.
    log_max_project: AtomicUsize,
    log_max_node: AtomicUsize,
    hot_segment_bytes: Arc<AtomicU64>,
    /// `PULSE_HOT_SEGMENTS_PROJECT_MAX_MB` (512) / `PULSE_HOT_SEGMENTS_MAX_MB` (2048): `1x` bounds.
    hot_seg_max_project: AtomicU64,
    hot_seg_max_node: AtomicU64,
    wal_bytes: Arc<AtomicU64>,
    flush_semaphore: Arc<tokio::sync::Semaphore>,
}

impl NodeLoad {
    fn from_env() -> Arc<Self> {
        Arc::new(Self {
            l0_bytes: Arc::new(AtomicUsize::new(0)),
            unapplied_bytes: AtomicUsize::new(0),
            l0_max_project: AtomicUsize::new(env_mb("PULSE_L0_PROJECT_MAX_MB", 512)),
            l0_max_node: AtomicUsize::new(env_mb("PULSE_L0_MAX_MB", 1024)),
            log_max_project: AtomicUsize::new(env_mb("PULSE_LOG_LAG_PROJECT_MAX_MB", 256)),
            log_max_node: AtomicUsize::new(env_mb("PULSE_LOG_LAG_MAX_MB", 512)),
            hot_segment_bytes: Arc::new(AtomicU64::new(0)),
            hot_seg_max_project: AtomicU64::new(env_mb_u64(
                "PULSE_HOT_SEGMENTS_PROJECT_MAX_MB",
                DEFAULT_HOT_SEGMENTS_PROJECT_MAX_MB,
            )),
            hot_seg_max_node: AtomicU64::new(env_mb_u64(
                "PULSE_HOT_SEGMENTS_MAX_MB",
                DEFAULT_HOT_SEGMENTS_MAX_MB,
            )),
            wal_bytes: Arc::new(AtomicU64::new(0)),
            flush_semaphore: Arc::new(tokio::sync::Semaphore::new(env_usize(
                "PULSE_FLUSH_CONCURRENCY",
                DEFAULT_FLUSH_CONCURRENCY,
            ))),
        })
    }
}

/// Coalesced upload wakeup signal with failure cooldown so `1x`/`2x` pressure cannot busy-spin S3.
#[derive(Default)]
struct UploadSignal {
    notify: tokio::sync::Notify,
    in_progress: AtomicBool,
    last_failure: Mutex<Option<Instant>>,
}

impl UploadSignal {
    fn request_upload_if_due(&self) {
        if self.in_progress.load(Ordering::Acquire) {
            return;
        }
        if let Some(last_fail) = *self.last_failure.lock() {
            if last_fail.elapsed() < UPLOAD_FAILURE_COOLDOWN {
                return;
            }
        }
        self.notify.notify_one();
    }

    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    fn mark_success(&self) {
        *self.last_failure.lock() = None;
    }

    fn mark_failure(&self) {
        *self.last_failure.lock() = Some(Instant::now());
    }
}

struct UploadInProgressGuard<'a>(&'a UploadSignal);

impl Drop for UploadInProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.in_progress.store(false, Ordering::Release);
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

#[derive(Default)]
struct LastSnapshotState {
    last_snapshot_ref: Option<SnapshotRef>,
    exported_id_epoch: Option<u64>,
    exported_prof_dirty_seq: Option<u64>,
}

/// Per-project storage state managing WALs, L0 `MemBucket`s, MVCC `Manifest`, and Compaction.
pub struct ProjectStorage {
    project_id: String,
    config: StorageConfig,
    object_store: Arc<dyn ObjectStore>,
    id_graph: RwLock<Arc<ProjectIdentityGraph>>,
    identity_router: Option<Arc<IdentityRouter>>,
    profile_router: Arc<ProfileRouter>,
    query_snapshot_store: Option<Arc<dyn ObjectStore>>,
    /// `None` when no lease is configured (embedded use, tests) or the project is read-only here.
    writer: Option<WriterFence>,
    manifest_store: ManifestStore,
    published_store: PublishedManifestStore,
    manifest: RwLock<Arc<Manifest>>,
    manifest_commit_lock: AsyncMutex<()>,
    publish_lock: AsyncMutex<()>,
    refresh_lock: AsyncMutex<()>,
    gc_lock: AsyncMutex<()>,
    publish_enabled: AtomicBool,
    local_segment_tier: Arc<AtomicBool>,
    upload_interval_ms: Arc<AtomicU64>,
    closed_date_upload_ms: Arc<AtomicU64>,
    closed_date_max_bytes: Arc<AtomicU64>,
    upload_merge_max_input_bytes: Arc<AtomicU64>,
    upload_signal: Arc<UploadSignal>,
    last_published_seq: Mutex<Option<u64>>,
    last_published_epoch: Mutex<Option<u64>>,
    last_published_s3_partitions: Mutex<Option<Vec<PartitionEntry>>>,
    last_snapshot_state: Mutex<LastSnapshotState>,
    restored_snapshot_paths: Mutex<(Option<String>, Option<String>)>,
    closed_date_pending_since_ms: Mutex<HashMap<NaiveDate, i64>>,
    tracked_hot_segments: Mutex<HashMap<String, u64>>,
    hot_segment_bytes: AtomicU64,
    pending_local_deletes: Mutex<HashMap<String, u64>>,
    has_pending_upload_intent: AtomicBool,
    retired_profile_stores: Mutex<Vec<Arc<ProjectProfileStore>>>,
    dictionary: Arc<ProjectDictionary>,
    wals: Vec<BucketWal>,
    mem_buckets: Vec<MemBucket>,
    /// Per-bucket append mutex serializing `wal_seq` allocation, durable `wal.append`, `l0_seq` assignment,
    /// `MemBucket` insertion, and `DedupRegistry` commit (`P1-01`, `P1-02`).
    node_load: Arc<NodeLoad>,
    l0_bytes: Arc<AtomicUsize>,
    /// WAL records / events written vs applied to L0, and the bytes/ages in between (`log_events_locked`,
    /// `apply_logged`). Per bucket: records are applied in order within a bucket, not across buckets.
    logged_records: Vec<AtomicU64>,
    applied_records: Vec<AtomicU64>,
    logged_events: Vec<AtomicU64>,
    applied_events: Vec<AtomicU64>,
    unapplied_record_times: Vec<Mutex<VecDeque<(u64, i64, u64)>>>,
    oldest_unapplied_ms: Vec<AtomicI64>,
    unapplied_bytes: AtomicUsize,
    applied_notify: tokio::sync::Notify,
    bucket_append_locks: Vec<AsyncMutex<()>>,
    bucket_flush_locks: Vec<AsyncMutex<()>>,
    compaction_locks: DashMap<(NaiveDate, u8), Arc<AsyncMutex<()>>>,
    l0_seq: AtomicU64,
    max_l0_rows_per_bucket: AtomicUsize,
    flush_in_flight: Vec<AtomicBool>,
    bg_flush_handles: Mutex<Vec<tokio::task::AbortHandle>>,
    pin_tracker: Arc<ReaderPinTracker>,
    session_flags: Cache<u64, u16>,
    recovered_uuid_fps: Mutex<Vec<(u8, u64)>>,
    /// Test hook: runs right before a flush stages L0 rows.
    #[cfg(test)]
    before_stage: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    erased: RwLock<Arc<ErasedUids>>,
}

impl Drop for ProjectStorage {
    fn drop(&mut self) {
        for handle in self.bg_flush_handles.get_mut().drain(..) {
            handle.abort();
        }
        let rem = *self.hot_segment_bytes.get_mut();
        if rem > 0 {
            self.node_load
                .hot_segment_bytes
                .fetch_sub(rem, Ordering::Relaxed);
            *self.hot_segment_bytes.get_mut() = 0;
        }
        let rem_unapplied = *self.unapplied_bytes.get_mut();
        if rem_unapplied > 0 {
            self.node_load
                .unapplied_bytes
                .fetch_sub(rem_unapplied, Ordering::Relaxed);
            *self.unapplied_bytes.get_mut() = 0;
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

fn date_from_ms(ts_ms: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .unwrap_or_else(Utc::now)
        .date_naive()
}

fn s3_partitions(m: &Manifest) -> Vec<PartitionEntry> {
    let mut partitions: Vec<PartitionEntry> = m
        .active_segments
        .iter()
        .filter_map(|(&(date, bucket_id), segs)| {
            let s3_segs: Vec<SegmentMeta> = segs
                .iter()
                .filter(|s| !s.object_path.starts_with("hot/"))
                .cloned()
                .collect();
            if s3_segs.is_empty() {
                None
            } else {
                Some(PartitionEntry {
                    date,
                    bucket_id,
                    segments: s3_segs,
                })
            }
        })
        .collect();
    partitions.sort_by_key(|p| (p.date, p.bucket_id));
    partitions
}

async fn reconcile_upload_intent_with_stores(
    project_id: &str,
    object_store: &Arc<dyn ObjectStore>,
    manifest: &Manifest,
    latest_published: Option<&PublishedManifest>,
    last_snapshot_ref: Option<&SnapshotRef>,
) -> Result<Vec<String>> {
    let intent_store = UploadIntentStore::new(project_id, object_store.clone());
    let Some(intent) = intent_store.load().await? else {
        return Ok(Vec::new());
    };

    let active_manifest_segments: HashSet<&str> = manifest
        .active_segments
        .values()
        .flat_map(|v| v.iter().map(|s| s.object_path.as_str()))
        .collect();
    let mut unreferenced_local_sources: Vec<String> = Vec::new();
    for batch in &intent.batches {
        for src_path in &batch.source_object_paths {
            if src_path.starts_with("hot/segments/")
                && !active_manifest_segments.contains(src_path.as_str())
            {
                unreferenced_local_sources.push(src_path.clone());
            }
        }
    }

    let mut referenced_segments: HashSet<&str> = active_manifest_segments;
    if let Some(pub_m) = latest_published {
        for part in &pub_m.partitions {
            for seg in &part.segments {
                referenced_segments.insert(seg.object_path.as_str());
            }
        }
    }
    for pending in &manifest.pending_s3_deletes {
        if pending.kind == PendingS3DeleteKind::Segment {
            referenced_segments.insert(pending.object_path.as_str());
        }
    }

    let mut referenced_snapshots: HashSet<&str> = HashSet::new();
    for sr in latest_published
        .map(|p| &p.snapshot_ref)
        .into_iter()
        .chain(last_snapshot_ref)
    {
        referenced_snapshots.insert(sr.identity.object_path.as_str());
        if let Some(ref pr) = sr.profiles {
            referenced_snapshots.insert(pr.object_path.as_str());
        }
    }
    for pending in &manifest.pending_s3_deletes {
        if pending.kind != PendingS3DeleteKind::Segment {
            referenced_snapshots.insert(pending.object_path.as_str());
        }
    }

    for batch in &intent.batches {
        if !referenced_segments.contains(batch.target_object_path.as_str()) {
            let obj_path = ObjectPath::from(batch.target_object_path.as_str());
            match object_store.delete(&obj_path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(StorageError::ObjectStore(e)),
            }
        }
    }

    for snap_path in [
        intent.new_identity_snapshot_path.as_deref(),
        intent.new_profile_snapshot_path.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !referenced_snapshots.contains(snap_path) {
            let obj_path = ObjectPath::from(snap_path);
            match object_store.delete(&obj_path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(StorageError::ObjectStore(e)),
            }
        }
    }

    intent_store.clear().await?;
    Ok(unreferenced_local_sources)
}

async fn sweep_local_hot_segments_for_project(
    project_id: &str,
    object_store: &Arc<dyn ObjectStore>,
    manifest: &Manifest,
) -> Result<usize> {
    let active_paths: HashSet<&str> = manifest
        .active_segments
        .values()
        .flat_map(|v| v.iter().map(|s| s.object_path.as_str()))
        .collect();
    let prefix = ObjectPath::from(format!("hot/segments/{project_id}"));
    let metas: Vec<_> = object_store.list(Some(&prefix)).try_collect().await?;
    let mut swept = 0usize;
    for meta in metas {
        if !active_paths.contains(meta.location.as_ref()) {
            match object_store.delete(&meta.location).await {
                Ok(()) => swept += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(StorageError::ObjectStore(e)),
            }
        }
    }
    Ok(swept)
}

fn manifest_matches_published(m: &Manifest, p: &PublishedManifest) -> bool {
    m.committed_wal_seq == p.committed_wal_seq
        && m.committed_l0_seq == p.committed_l0_seq
        && m.dict_snapshot == p.dict_snapshot
        && m.to_sorted_partitions() == p.partitions
}

fn push_superseded_snapshots_to_pending(
    pending: &mut Vec<PendingS3Delete>,
    prev_snapshot: &SnapshotRef,
    new_snapshot: &SnapshotRef,
    superseding_pub_seq: u64,
    pub_ts_ms: i64,
    grace_ms: i64,
) {
    let mut existing: HashSet<String> = pending.iter().map(|e| e.object_path.clone()).collect();
    if prev_snapshot.identity.object_path != new_snapshot.identity.object_path
        && existing.insert(prev_snapshot.identity.object_path.clone())
    {
        pending.push(PendingS3Delete {
            object_path: prev_snapshot.identity.object_path.clone(),
            kind: PendingS3DeleteKind::IdentitySnapshot,
            superseding_pub_seq,
            superseded_at_ms: Some(pub_ts_ms),
            eligible_after_ms: Some(pub_ts_ms.saturating_add(grace_ms)),
        });
    }
    if let Some(ref prev_prof) = prev_snapshot.profiles {
        if new_snapshot
            .profiles
            .as_ref()
            .is_none_or(|cur| cur.object_path != prev_prof.object_path)
            && existing.insert(prev_prof.object_path.clone())
        {
            pending.push(PendingS3Delete {
                object_path: prev_prof.object_path.clone(),
                kind: PendingS3DeleteKind::ProfileSnapshot,
                superseding_pub_seq,
                superseded_at_ms: Some(pub_ts_ms),
                eligible_after_ms: Some(pub_ts_ms.saturating_add(grace_ms)),
            });
        }
    }
}

fn apply_step6_to_manifest(
    manifest: &mut Manifest,
    pub_m: &PublishedManifest,
    prev_pub: Option<&PublishedManifest>,
    grace_ms: i64,
) {
    let pub_seq = pub_m.publish_seq;
    let pub_ts_ms = pub_m.published_at_ms;
    manifest.last_published_seq = Some(
        manifest
            .last_published_seq
            .map_or(pub_seq, |s| s.max(pub_seq)),
    );
    for entry in &mut manifest.pending_s3_deletes {
        if entry.superseding_pub_seq <= pub_seq && entry.eligible_after_ms.is_none() {
            entry.superseded_at_ms = Some(pub_ts_ms);
            entry.eligible_after_ms = Some(pub_ts_ms.saturating_add(grace_ms));
        }
    }
    if let Some(prev) = prev_pub {
        push_superseded_snapshots_to_pending(
            &mut manifest.pending_s3_deletes,
            &prev.snapshot_ref,
            &pub_m.snapshot_ref,
            pub_seq,
            pub_ts_ms,
            grace_ms,
        );
    }
}

struct RestorePublishedCtx<'a> {
    project_id: &'a str,
    published_store: &'a PublishedManifestStore,
    s3_store: &'a Arc<dyn ObjectStore>,
    qsnap_store: &'a Arc<dyn ObjectStore>,
    identity_router: Option<&'a Arc<IdentityRouter>>,
    profile_router: &'a Arc<ProfileRouter>,
}

/// Staged published manifest and restored Candidate B snapshot handles prepared off-lock
/// before being atomically installed via [`ProjectStorage::apply_prepared_refresh`].
pub struct PreparedPublishedRefresh {
    pub_m: PublishedManifest,
    id_graph: Option<Arc<ProjectIdentityGraph>>,
    prof_store: Option<Arc<ProjectProfileStore>>,
}

impl PreparedPublishedRefresh {
    pub fn publish_seq(&self) -> u64 {
        self.pub_m.publish_seq
    }

    pub fn id_graph(&self) -> Option<&Arc<ProjectIdentityGraph>> {
        self.id_graph.as_ref()
    }

    pub fn prof_store(&self) -> Option<&Arc<ProjectProfileStore>> {
        self.prof_store.as_ref()
    }
}

async fn restore_published_snapshots_with_retry(
    ctx: RestorePublishedCtx<'_>,
    initial_pub: PublishedManifest,
    prev_restored_paths: Option<(Option<&str>, Option<&str>)>,
    install_in_routers: bool,
) -> Result<(
    PublishedManifest,
    Option<Arc<ProjectIdentityGraph>>,
    Option<Arc<ProjectProfileStore>>,
    Option<Arc<ProjectProfileStore>>,
)> {
    let RestorePublishedCtx {
        project_id,
        published_store,
        s3_store,
        qsnap_store,
        identity_router,
        profile_router,
    } = ctx;
    let mut pub_m = initial_pub;
    let mut cached_id: Option<(String, Arc<ProjectIdentityGraph>)> = None;
    for attempt in 0..2 {
        let (restore_identity, restore_profiles) = match prev_restored_paths {
            None => (true, true),
            Some((prev_id, prev_prof)) => (
                prev_id != Some(pub_m.snapshot_ref.identity.object_path.as_str()),
                prev_prof
                    != pub_m
                        .snapshot_ref
                        .profiles
                        .as_ref()
                        .map(|p| p.object_path.as_str()),
            ),
        };

        let id_res = if restore_identity {
            if let Some((ref cached_path, ref graph)) = cached_id {
                if cached_path == &pub_m.snapshot_ref.identity.object_path {
                    Ok(Some(graph.clone()))
                } else {
                    match restore_identity_from_snapshot_read_only(
                        project_id,
                        s3_store,
                        qsnap_store.clone(),
                        &pub_m.snapshot_ref.identity,
                        pub_m.high_water_mark,
                    )
                    .await
                    {
                        Ok((g, _)) => {
                            cached_id =
                                Some((pub_m.snapshot_ref.identity.object_path.clone(), g.clone()));
                            Ok(Some(g))
                        }
                        Err(e) => Err(e),
                    }
                }
            } else {
                match restore_identity_from_snapshot_read_only(
                    project_id,
                    s3_store,
                    qsnap_store.clone(),
                    &pub_m.snapshot_ref.identity,
                    pub_m.high_water_mark,
                )
                .await
                {
                    Ok((g, _)) => {
                        cached_id =
                            Some((pub_m.snapshot_ref.identity.object_path.clone(), g.clone()));
                        Ok(Some(g))
                    }
                    Err(e) => Err(e),
                }
            }
        } else {
            Ok(None)
        };

        let id_graph = match id_res {
            Ok(g) => g,
            Err(StorageError::ObjectStore(object_store::Error::NotFound { .. }))
                if attempt == 0 =>
            {
                if let Some(newer) = published_store.load_latest().await? {
                    pub_m = newer;
                    continue;
                }
                return Err(StorageError::ObjectStore(object_store::Error::NotFound {
                    path: pub_m.snapshot_ref.identity.object_path.clone(),
                    source: "published snapshot missing".into(),
                }));
            }
            Err(e) => return Err(e),
        };

        let prof_res = if restore_profiles {
            restore_profiles_from_snapshot_read_only(
                project_id,
                pub_m.publish_seq,
                s3_store,
                qsnap_store.clone(),
                pub_m.snapshot_ref.profiles.as_ref(),
            )
            .await
            .map(Some)
        } else {
            Ok(None)
        };

        let prof_store = match prof_res {
            Ok(p) => p,
            Err(StorageError::ObjectStore(object_store::Error::NotFound { .. }))
                if attempt == 0 =>
            {
                if let Some(newer) = published_store.load_latest().await? {
                    pub_m = newer;
                    continue;
                }
                return Err(StorageError::ObjectStore(object_store::Error::NotFound {
                    path: pub_m
                        .snapshot_ref
                        .profiles
                        .as_ref()
                        .map_or_else(String::new, |p| p.object_path.clone()),
                    source: "published profile snapshot missing".into(),
                }));
            }
            Err(e) => return Err(e),
        };

        let prev_prof_store = if install_in_routers {
            if let Some(ref g) = id_graph {
                if let Some(router) = identity_router {
                    router.replace_read_only(project_id, g.clone());
                }
            }
            if let Some(ref p) = prof_store {
                profile_router.replace_read_only(project_id, p.clone())
            } else {
                None
            }
        } else {
            None
        };
        return Ok((pub_m, id_graph, prof_store, prev_prof_store));
    }
    unreachable!("loop returns on attempt 1")
}

impl ProjectStorage {
    pub async fn open_and_recover(
        project_id: impl Into<String>,
        config: StorageConfig,
        object_store: Arc<dyn ObjectStore>,
        identity_router: &IdentityRouter,
    ) -> Result<Arc<Self>> {
        let profile_router = Arc::new(ProfileRouter::new(
            object_store.clone(),
            identity_router.is_read_only(),
        ));
        let local_segment_tier =
            Arc::new(AtomicBool::new(is_hot_routed_store(object_store.as_ref())));
        let upload_interval_ms =
            Arc::new(AtomicU64::new(upload_interval_from_env().as_millis() as u64));
        let closed_date_upload_ms = Arc::new(AtomicU64::new(
            closed_date_upload_interval_from_env().as_millis() as u64,
        ));
        let closed_date_max_bytes = Arc::new(AtomicU64::new(closed_date_max_bytes_from_env()));
        let upload_merge_max_input_bytes =
            Arc::new(AtomicU64::new(upload_merge_max_input_bytes_from_env()));
        let upload_signal = Arc::new(UploadSignal::default());
        Self::open_inner(
            project_id.into(),
            config,
            object_store,
            identity_router,
            None,
            profile_router,
            None,
            None,
            NodeLoad::from_env(),
            local_segment_tier,
            upload_interval_ms,
            closed_date_upload_ms,
            closed_date_max_bytes,
            upload_merge_max_input_bytes,
            upload_signal,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_inner(
        project_id: String,
        config: StorageConfig,
        object_store: Arc<dyn ObjectStore>,
        identity_router: &IdentityRouter,
        identity_router_arc: Option<Arc<IdentityRouter>>,
        profile_router: Arc<ProfileRouter>,
        query_snapshot_store: Option<Arc<dyn ObjectStore>>,
        lease: Option<(u64, Instant)>,
        node_load: Arc<NodeLoad>,
        local_segment_tier: Arc<AtomicBool>,
        upload_interval_ms: Arc<AtomicU64>,
        closed_date_upload_ms: Arc<AtomicU64>,
        closed_date_max_bytes: Arc<AtomicU64>,
        upload_merge_max_input_bytes: Arc<AtomicU64>,
        upload_signal: Arc<UploadSignal>,
    ) -> Result<Arc<Self>> {
        let manifest_store = ManifestStore::new(&project_id, object_store.clone());
        let published_store = PublishedManifestStore::new(&project_id, object_store.clone());
        let mut latest_published = published_store.load_latest().await?;

        let mut manifest;
        let id_graph;
        let initial_last_published_seq;
        let mut initial_last_published_epoch = None;
        let mut initial_restored_paths = (None, None);
        let mut last_snapshot_ref = None;

        if identity_router.is_read_only() {
            if let Some(ref qsnap_store) = query_snapshot_store {
                if let Some(pub_m) = latest_published.take() {
                    let _ =
                        delete_objects_with_prefix(qsnap_store, &format!("profiles/{project_id}"))
                            .await;
                    let (resolved_pub, restored_id_opt, _, _) =
                        restore_published_snapshots_with_retry(
                            RestorePublishedCtx {
                                project_id: &project_id,
                                published_store: &published_store,
                                s3_store: &object_store,
                                qsnap_store,
                                identity_router: identity_router_arc.as_ref(),
                                profile_router: &profile_router,
                            },
                            pub_m,
                            None,
                            true,
                        )
                        .await?;
                    id_graph = match restored_id_opt {
                        Some(g) => g,
                        None => identity_router.get_or_open(&project_id).await?,
                    };
                    manifest =
                        resolved_pub.to_local_manifest(resolved_pub.publish_seq.saturating_add(1));
                    initial_last_published_seq = Some(resolved_pub.publish_seq);
                    initial_restored_paths = (
                        Some(resolved_pub.snapshot_ref.identity.object_path.clone()),
                        resolved_pub
                            .snapshot_ref
                            .profiles
                            .as_ref()
                            .map(|p| p.object_path.clone()),
                    );
                    last_snapshot_ref = Some(resolved_pub.snapshot_ref.clone());
                    latest_published = Some(resolved_pub);
                } else {
                    manifest = manifest_store.load_latest().await?;
                    id_graph = identity_router.get_or_open(&project_id).await?;
                    initial_last_published_seq = manifest.last_published_seq;
                }
            } else {
                manifest = manifest_store.load_latest().await?;
                id_graph = identity_router.get_or_open(&project_id).await?;
                initial_last_published_seq = latest_published
                    .as_ref()
                    .map(|p| p.publish_seq)
                    .max(manifest.last_published_seq);
                manifest.last_published_seq = initial_last_published_seq;
                last_snapshot_ref = latest_published.as_ref().map(|p| p.snapshot_ref.clone());
            }
        } else {
            manifest = manifest_store.load_latest().await?;
            let grace_ms = (upload_interval_ms.load(Ordering::Acquire) as i64).saturating_mul(2);
            let mut step6_recovered = false;
            let mut step6_prev_pub: Option<PublishedManifest> = None;

            if let Some(ref pub_m) = latest_published {
                let local_gen = manifest.writer_generation;
                let local_epoch = manifest.epoch;
                let local_last_pub_seq = manifest.last_published_seq;

                if let Some((generation, _)) = lease {
                    if pub_m.writer_generation > generation {
                        return Err(StorageError::NotWriter { project_id });
                    }
                }
                if pub_m.writer_generation > local_gen {
                    return Err(StorageError::StaleWriter {
                        project_id,
                        pub_seq: pub_m.publish_seq,
                        pub_gen: pub_m.writer_generation,
                        local_last_pub_seq,
                        local_gen,
                    });
                }
                let seq_ahead = local_last_pub_seq.is_none_or(|s| pub_m.publish_seq > s);
                if seq_ahead {
                    let expected_next = local_last_pub_seq.map_or(0, |s| s.saturating_add(1));
                    let is_step6 = pub_m.publish_seq == expected_next
                        && pub_m.writer_generation == local_gen
                        && pub_m.source_local_epoch <= local_epoch
                        && local_epoch > 0;
                    if !is_step6 {
                        return Err(StorageError::StaleWriter {
                            project_id,
                            pub_seq: pub_m.publish_seq,
                            pub_gen: pub_m.writer_generation,
                            local_last_pub_seq,
                            local_gen,
                        });
                    }
                    step6_recovered = true;
                    if pub_m.publish_seq > 0 {
                        match published_store.load_seq(pub_m.publish_seq - 1).await {
                            Ok(prev) => step6_prev_pub = Some(prev),
                            Err(StorageError::ObjectStore(object_store::Error::NotFound {
                                ..
                            })) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    apply_step6_to_manifest(
                        &mut manifest,
                        pub_m,
                        step6_prev_pub.as_ref(),
                        grace_ms,
                    );
                }
            }

            let mut committed_in_open = false;
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
                    if step6_recovered {
                        if let Some(ref pub_m) = latest_published {
                            apply_step6_to_manifest(
                                &mut next,
                                pub_m,
                                step6_prev_pub.as_ref(),
                                grace_ms,
                            );
                        }
                    }
                    manifest = match manifest_store.commit_next(next).await {
                        Ok(committed) => {
                            committed_in_open = true;
                            committed
                        }
                        Err(StorageError::ManifestConflict) => manifest_store.load_latest().await?,
                        Err(e) => return Err(e),
                    };
                }
            }
            if step6_recovered && !committed_in_open {
                manifest = manifest_store.commit_next(manifest).await?;
            }

            initial_last_published_seq = manifest.last_published_seq;
            if let Some(ref pub_m) = latest_published {
                if manifest_matches_published(&manifest, pub_m) {
                    initial_last_published_epoch = Some(manifest.epoch);
                }
                last_snapshot_ref = Some(pub_m.snapshot_ref.clone());
            }

            // Replay/discard any leftover UploadIntent (AFTER step-6 recovery, BEFORE dedup seeding)
            // and sweep unreferenced local `hot/segments/{project_id}/` files.
            let _ = reconcile_upload_intent_with_stores(
                &project_id,
                &object_store,
                &manifest,
                latest_published.as_ref(),
                last_snapshot_ref.as_ref(),
            )
            .await?;
            sweep_local_hot_segments_for_project(&project_id, &object_store, &manifest).await?;

            id_graph = identity_router.get_or_open(&project_id).await?;
        }

        let initial_last_published_s3_partitions =
            latest_published.as_ref().map(|p| p.partitions.clone());

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
            )
            .with_wal_bytes_counter(node_load.wal_bytes.clone());
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

        let num_buckets = config.num_buckets as usize;
        let ps = Arc::new(Self {
            project_id: project_id.clone(),
            config: config.clone(),
            object_store: object_store.clone(),
            id_graph: RwLock::new(id_graph),
            identity_router: identity_router_arc,
            profile_router,
            query_snapshot_store,
            writer: lease.map(|(generation, valid_until)| WriterFence {
                generation,
                valid_until: Mutex::new(valid_until),
                fenced: AtomicBool::new(false),
            }),
            manifest_store,
            published_store,
            manifest: RwLock::new(Arc::new(manifest)),
            manifest_commit_lock: AsyncMutex::new(()),
            publish_lock: AsyncMutex::new(()),
            refresh_lock: AsyncMutex::new(()),
            gc_lock: AsyncMutex::new(()),
            publish_enabled: AtomicBool::new(true),
            local_segment_tier,
            upload_interval_ms,
            closed_date_upload_ms,
            closed_date_max_bytes,
            upload_merge_max_input_bytes,
            upload_signal,
            last_published_seq: Mutex::new(initial_last_published_seq),
            last_published_epoch: Mutex::new(initial_last_published_epoch),
            last_published_s3_partitions: Mutex::new(initial_last_published_s3_partitions),
            last_snapshot_state: Mutex::new(LastSnapshotState {
                last_snapshot_ref,
                exported_id_epoch: None,
                exported_prof_dirty_seq: None,
            }),
            restored_snapshot_paths: Mutex::new(initial_restored_paths),
            closed_date_pending_since_ms: Mutex::new(HashMap::new()),
            tracked_hot_segments: Mutex::new(HashMap::new()),
            hot_segment_bytes: AtomicU64::new(0),
            pending_local_deletes: Mutex::new(HashMap::new()),
            has_pending_upload_intent: AtomicBool::new(false),
            retired_profile_stores: Mutex::new(Vec::new()),
            dictionary,
            wals,
            mem_buckets,
            node_load,
            l0_bytes,
            logged_records: (0..num_buckets).map(|_| AtomicU64::new(0)).collect(),
            applied_records: (0..num_buckets).map(|_| AtomicU64::new(0)).collect(),
            logged_events: (0..num_buckets).map(|_| AtomicU64::new(0)).collect(),
            applied_events: (0..num_buckets).map(|_| AtomicU64::new(0)).collect(),
            unapplied_record_times: (0..num_buckets)
                .map(|_| Mutex::new(VecDeque::new()))
                .collect(),
            oldest_unapplied_ms: (0..num_buckets).map(|_| AtomicI64::new(0)).collect(),
            unapplied_bytes: AtomicUsize::new(0),
            applied_notify: tokio::sync::Notify::new(),
            bucket_append_locks,
            bucket_flush_locks,
            compaction_locks: DashMap::new(),
            l0_seq,
            max_l0_rows_per_bucket,
            flush_in_flight,
            bg_flush_handles: Mutex::new(Vec::new()),
            pin_tracker: Arc::new(ReaderPinTracker::default()),
            session_flags: Cache::builder()
                .max_capacity(MAX_SESSION_FLAGS_CACHE_ENTRIES)
                .time_to_idle(Duration::from_secs(3600))
                .build(),
            recovered_uuid_fps: Mutex::new(Vec::new()),
            #[cfg(test)]
            before_stage: Mutex::new(None),
            erased: RwLock::new(Arc::new(ErasedUids::default())),
        });

        if !identity_router.is_read_only() {
            // Seed live `hot/segments/` byte tracking and closed-date 1h timers from local state (0 S3 calls).
            let now_ms = Utc::now().timestamp_millis();
            let today_utc = date_from_ms(now_ms);
            let local_hot_segs: Vec<SegmentMeta> = ps
                .manifest
                .read()
                .active_segments
                .values()
                .flat_map(|v| v.iter())
                .filter(|s| s.object_path.starts_with("hot/segments/"))
                .cloned()
                .collect();
            for seg in &local_hot_segs {
                ps.record_hot_segment(&seg.object_path, seg.file_size);
                if seg.date < today_utc {
                    let ts_ms = match ps
                        .object_store
                        .head(&ObjectPath::from(seg.object_path.as_str()))
                        .await
                    {
                        Ok(meta) => meta.last_modified.timestamp_millis(),
                        Err(_) => now_ms,
                    };
                    let mut guard = ps.closed_date_pending_since_ms.lock();
                    guard
                        .entry(seg.date)
                        .and_modify(|cur| *cur = (*cur).min(ts_ms))
                        .or_insert(ts_ms);
                }
            }

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
                let batch = reader.project_columns(seg, &["event_uuid_fp"]).await?;
                let col = batch.column_by_name("event_uuid_fp").ok_or_else(|| {
                    StorageError::Corrupted(format!(
                        "segment `{}` missing `event_uuid_fp` column during dedup seeding",
                        seg.object_path
                    ))
                })?;
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

            let id_g = ps.id_graph();
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
                        let uid = id_g.get_or_intern(&ev.distinct_id, bucket_id);
                        events.push((ev, uid, fp));
                    }
                    let rows = ps.rows_or_dead_letter(bucket_id, wal_seq, &events).await?;
                    ps.mem_buckets[b as usize].append_batch(rows);
                }
            }
            id_g.flush().await?;
            if let Some(ref pub_m) = latest_published {
                if id_g.mutation_epoch() == 0 && pub_m.high_water_mark >= id_g.user_count() as u64 {
                    ps.last_snapshot_state.lock().exported_id_epoch = Some(0);
                }
            }
        }

        Ok(ps)
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn id_graph(&self) -> Arc<ProjectIdentityGraph> {
        self.id_graph.read().clone()
    }

    pub fn upload_interval(&self) -> Duration {
        Duration::from_millis(self.upload_interval_ms.load(Ordering::Acquire))
    }

    pub fn set_upload_interval(&self, interval: Duration) {
        self.upload_interval_ms
            .store(interval.as_millis() as u64, Ordering::Release);
    }

    fn upload_interval_ms(&self) -> u64 {
        self.upload_interval_ms.load(Ordering::Acquire)
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

    pub fn is_fenced(&self) -> bool {
        self.writer
            .as_ref()
            .is_some_and(|w| w.fenced.load(Ordering::Acquire))
    }

    fn fence(&self) {
        if let Some(w) = &self.writer {
            w.fenced.store(true, Ordering::Release);
        }
    }

    pub async fn lock_bucket_append(&self, bucket_id: u8) -> AsyncMutexGuard<'_, ()> {
        self.bucket_append_locks[bucket_id as usize].lock().await
    }

    /// Logs and applies in one call, for callers that do not split the two (tests, embedded use).
    /// Events that cannot become rows are dead-lettered, so the count can be below `events.len()`.
    pub async fn append_bucket_events_locked(
        &self,
        append_guard: &AsyncMutexGuard<'_, ()>,
        bucket_id: u8,
        events: &[(WireEvent, u32)],
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let batch = WireBatch::new(events.iter().map(|(e, _)| e.clone()).collect());
        let wal_seq = self
            .log_events_locked(append_guard, bucket_id, &batch, 0)
            .await?;
        let events: Vec<_> = events
            .iter()
            .map(|(ev, uid)| (ev, *uid, event_uuid_fingerprint(&ev.event_uuid)))
            .collect();
        self.apply_logged(bucket_id, wal_seq, &events, 0).await
    }

    /// The appender half: one durable WAL record for `batch`, written while the caller holds
    /// `lock_bucket_append(bucket_id)`, so there is one PUT in flight per bucket log and `wal_seq`
    /// follows lock order. Every logged record must later go through `apply_logged`, in order.
    pub async fn log_events_locked(
        &self,
        _append_guard: &AsyncMutexGuard<'_, ()>,
        bucket_id: u8,
        batch: &WireBatch,
        approx_bytes: usize,
    ) -> Result<u64> {
        self.check_writer()?;
        let wal_seq = match self.wals[bucket_id as usize].append(batch).await {
            Err(e @ StorageError::NotWriter { .. }) => {
                self.fence();
                return Err(e);
            }
            other => other?,
        };
        // Checked again AFTER the write: if the lease ran out while the PUT was in flight, a new
        // holder may already have listed this log without the record, so it must not be acked.
        self.check_writer()?;
        let b_idx = bucket_id as usize;
        let now_ms = Utc::now().timestamp_millis();
        let ev_count = batch.len() as u64;
        {
            let mut q = self.unapplied_record_times[b_idx].lock();
            if q.is_empty() {
                self.oldest_unapplied_ms[b_idx].store(now_ms, Ordering::Release);
            }
            q.push_back((wal_seq, now_ms, ev_count));
        }
        self.logged_events[b_idx].fetch_add(ev_count, Ordering::AcqRel);
        self.logged_records[b_idx].fetch_add(1, Ordering::AcqRel);
        self.unapplied_bytes
            .fetch_add(approx_bytes, Ordering::AcqRel);
        self.node_load
            .unapplied_bytes
            .fetch_add(approx_bytes, Ordering::AcqRel);
        Ok(wal_seq)
    }

    /// The processor half: turns a logged record's events (already deduplicated and interned by
    /// the caller) into L0 rows. Call once per logged record, in `wal_seq` order per bucket,
    /// with the same `approx_bytes`; an `Err` means nothing was applied and the call can be
    /// repeated.
    pub async fn apply_logged(
        &self,
        bucket_id: u8,
        wal_seq: u64,
        events: &[(&WireEvent, u32, u64)],
        approx_bytes: usize,
    ) -> Result<usize> {
        let rows = self.rows_or_dead_letter(bucket_id, wal_seq, events).await?;
        let count = rows.len();
        let b_idx = bucket_id as usize;
        self.mem_buckets[b_idx].append_batch(rows);
        self.unapplied_bytes
            .fetch_sub(approx_bytes, Ordering::AcqRel);
        self.node_load
            .unapplied_bytes
            .fetch_sub(approx_bytes, Ordering::AcqRel);
        let applied_ev_count = {
            let mut q = self.unapplied_record_times[b_idx].lock();
            let mut ev_sum = 0u64;
            while let Some(&(seq, _, ev_c)) = q.front() {
                if seq <= wal_seq {
                    q.pop_front();
                    ev_sum += ev_c;
                } else {
                    break;
                }
            }
            let next_oldest = q.front().map_or(0, |&(_, ts, _)| ts);
            self.oldest_unapplied_ms[b_idx].store(next_oldest, Ordering::Release);
            ev_sum
        };
        self.applied_events[b_idx].fetch_add(applied_ev_count, Ordering::AcqRel);
        self.applied_records[b_idx].fetch_add(1, Ordering::AcqRel);
        self.applied_notify.notify_waiters();
        Ok(count)
    }

    /// Waits, up to `timeout`, until every record logged before this call has been applied, so
    /// a read or flush right after an acknowledged write sees it.
    pub async fn wait_applied(&self, timeout: Duration) {
        let targets: Vec<u64> = self
            .logged_records
            .iter()
            .map(|n| n.load(Ordering::Acquire))
            .collect();
        let caught_up = || {
            self.applied_records
                .iter()
                .zip(&targets)
                .all(|(n, &t)| n.load(Ordering::Acquire) >= t)
        };
        if caught_up() {
            return;
        }
        let _ = tokio::time::timeout(timeout, async {
            loop {
                let notified = self.applied_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if caught_up() {
                    return;
                }
                notified.await;
            }
        })
        .await;
    }

    /// Returns `(unapplied_records, unapplied_events)` queued across this project's buckets.
    pub fn apply_queue_depth(&self) -> (u64, u64) {
        let records: u64 = self
            .logged_records
            .iter()
            .zip(&self.applied_records)
            .map(|(l, a)| {
                l.load(Ordering::Acquire)
                    .saturating_sub(a.load(Ordering::Acquire))
            })
            .sum();
        let events: u64 = self
            .logged_events
            .iter()
            .zip(&self.applied_events)
            .map(|(l, a)| {
                l.load(Ordering::Acquire)
                    .saturating_sub(a.load(Ordering::Acquire))
            })
            .sum();
        (records, events)
    }

    /// Returns the age in milliseconds of the oldest unapplied WAL record across this project's
    /// buckets (`0` when all buckets are caught up).
    pub fn max_apply_lag_ms(&self, now_ms: i64) -> i64 {
        self.oldest_unapplied_ms
            .iter()
            .map(|ts| {
                let oldest = ts.load(Ordering::Acquire);
                if oldest > 0 {
                    now_ms.saturating_sub(oldest).max(0)
                } else {
                    0
                }
            })
            .max()
            .unwrap_or(0)
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
    /// `wal_dead/{project}/bucket_{NNN}/seq_{wal_seq}_{fingerprint}.plsd` (`PLSD` v1 envelope
    /// containing `recorded_at_ms`, the UTF-8 refusal reason, and the single-event `PLSB` batch).
    /// The name is fixed by the record, so applying it again after a crash writes nothing new.
    /// Fails only if the object store does; the caller must then retry, not skip.
    pub async fn dead_letter(
        &self,
        bucket_id: u8,
        wal_seq: u64,
        ev: WireEvent,
        fp: u64,
        reason: &(dyn std::fmt::Display + Sync),
    ) -> Result<()> {
        let path = ObjectPath::from(format!(
            "wal_dead/{}/bucket_{:03}/seq_{:012}_{:016x}.plsd",
            self.project_id, bucket_id, wal_seq, fp
        ));
        let reason_str = reason.to_string();
        let now_ms = Utc::now().timestamp_millis();
        let payload = encode_dead_letter(now_ms, &reason_str, &ev)?;
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
                    reason = %reason_str,
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
        let checks = [
            (
                "l0",
                self.l0_bytes.load(Ordering::Acquire),
                load.l0_max_project.load(Ordering::Relaxed),
            ),
            (
                "l0",
                load.l0_bytes.load(Ordering::Acquire),
                load.l0_max_node.load(Ordering::Relaxed),
            ),
            (
                "log",
                self.unapplied_bytes.load(Ordering::Acquire),
                load.log_max_project.load(Ordering::Relaxed),
            ),
            (
                "log",
                load.unapplied_bytes.load(Ordering::Acquire),
                load.log_max_node.load(Ordering::Relaxed),
            ),
        ];
        for (what, bytes, max_bytes) in checks {
            if bytes >= max_bytes {
                if what == "l0" {
                    self.schedule_background_flush(bucket_id);
                }
                return Err(StorageError::Backpressure {
                    project_id: self.project_id.clone(),
                    what,
                    bytes,
                    max_bytes,
                });
            }
        }
        Ok(())
    }

    /// Schedules an asynchronous L0 -> L1 flush for `bucket_id`, deduplicated per bucket via `flush_in_flight`
    /// and bounded across the node by `NodeLoad::flush_semaphore`.
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
        let sem = self.node_load.flush_semaphore.clone();
        let handle = tokio::spawn(async move {
            let _reset_guard = FlushInFlightResetGuard {
                ps: ps_weak.clone(),
                b_idx,
            };
            let Ok(_permit) = sem.acquire_owned().await else {
                return;
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

    pub fn is_local_segment_tier(&self) -> bool {
        self.local_segment_tier.load(Ordering::Acquire)
    }

    pub fn hot_segment_bytes(&self) -> u64 {
        self.hot_segment_bytes.load(Ordering::Acquire)
    }

    fn record_hot_segment(&self, path: &str, bytes: u64) {
        if !path.starts_with("hot/segments/") {
            return;
        }
        let mut guard = self.tracked_hot_segments.lock();
        let prev = guard.insert(path.to_string(), bytes);
        let prev_bytes = prev.unwrap_or(0);
        if bytes >= prev_bytes {
            let diff = bytes - prev_bytes;
            self.hot_segment_bytes.fetch_add(diff, Ordering::Relaxed);
            self.node_load
                .hot_segment_bytes
                .fetch_add(diff, Ordering::Relaxed);
        } else {
            let diff = prev_bytes - bytes;
            self.hot_segment_bytes.fetch_sub(diff, Ordering::Relaxed);
            self.node_load
                .hot_segment_bytes
                .fetch_sub(diff, Ordering::Relaxed);
        }
    }

    fn unrecord_hot_segment(&self, path: &str) {
        let mut guard = self.tracked_hot_segments.lock();
        if let Some(prev) = guard.remove(path) {
            self.hot_segment_bytes.fetch_sub(prev, Ordering::Relaxed);
            self.node_load
                .hot_segment_bytes
                .fetch_sub(prev, Ordering::Relaxed);
        }
    }

    fn after_flush_local_segments(&self, written_segments: &[SegmentMeta], now_ms: i64) {
        let today_utc = date_from_ms(now_ms);
        let mut touched_closed_dates: HashSet<NaiveDate> = HashSet::new();
        for meta in written_segments {
            if meta.object_path.starts_with("hot/segments/") {
                self.record_hot_segment(&meta.object_path, meta.file_size);
                if meta.date < today_utc {
                    self.closed_date_pending_since_ms
                        .lock()
                        .entry(meta.date)
                        .or_insert(now_ms);
                    touched_closed_dates.insert(meta.date);
                }
            }
        }
        if !self.is_local_segment_tier() || self.upload_interval().is_zero() {
            return;
        }
        let proj_bytes = self.hot_segment_bytes();
        let proj_1x = self.node_load.hot_seg_max_project.load(Ordering::Relaxed);
        let node_bytes = self.node_load.hot_segment_bytes.load(Ordering::Relaxed);
        let node_1x = self.node_load.hot_seg_max_node.load(Ordering::Relaxed);
        let closed_max = self.closed_date_max_bytes.load(Ordering::Acquire);
        let closed_over = if touched_closed_dates.is_empty() {
            false
        } else {
            let mf = self.manifest.read();
            touched_closed_dates.into_iter().any(|date| {
                let sum: u64 = mf
                    .active_segments
                    .iter()
                    .filter(|(&(d, _), _)| d == date)
                    .flat_map(|(_, segs)| segs.iter())
                    .filter(|s| s.object_path.starts_with("hot/segments/"))
                    .map(|s| s.file_size)
                    .sum();
                sum > closed_max
            })
        };
        if proj_bytes >= proj_1x || node_bytes >= node_1x || closed_over {
            self.upload_signal.request_upload_if_due();
        }
    }

    /// Enforces `2x` `hot/segments/` byte bounds (returning `StorageError::HotSegmentBackpressure`)
    /// and triggers an early upload wake (with failure cooldown) at `>= 1x`.
    pub fn check_hot_segment_backpressure(&self) -> Result<()> {
        if !self.is_local_segment_tier() {
            return Ok(());
        }
        let proj_bytes = self.hot_segment_bytes();
        let proj_1x = self.node_load.hot_seg_max_project.load(Ordering::Relaxed);
        let node_bytes = self.node_load.hot_segment_bytes.load(Ordering::Acquire);
        let node_1x = self.node_load.hot_seg_max_node.load(Ordering::Relaxed);

        if proj_bytes >= proj_1x || node_bytes >= node_1x {
            self.upload_signal.request_upload_if_due();
        }
        let proj_2x = proj_1x.saturating_mul(2);
        if proj_bytes >= proj_2x {
            return Err(StorageError::HotSegmentBackpressure {
                project_id: self.project_id.clone(),
                bytes: proj_bytes,
                max_bytes: proj_2x,
            });
        }
        let node_2x = node_1x.saturating_mul(2);
        if node_bytes >= node_2x {
            return Err(StorageError::HotSegmentBackpressure {
                project_id: self.project_id.clone(),
                bytes: node_bytes,
                max_bytes: node_2x,
            });
        }
        Ok(())
    }

    /// Deletes any deferred local `hot/segments/` paths that are no longer pinned by a `StorageSnapshot`.
    pub async fn drain_pending_local_deletes(&self) -> usize {
        let ready: Vec<String> = {
            let guard = self.pending_local_deletes.lock();
            if guard.is_empty() {
                return 0;
            }
            guard
                .keys()
                .filter(|p| !self.pin_tracker.is_pinned(p.as_str()))
                .cloned()
                .collect()
        };
        let mut deleted = 0usize;
        for path in ready {
            match self
                .object_store
                .delete(&ObjectPath::from(path.as_str()))
                .await
            {
                Ok(()) => {
                    self.pending_local_deletes.lock().remove(&path);
                    self.unrecord_hot_segment(&path);
                    deleted += 1;
                }
                Err(object_store::Error::NotFound { .. }) => {
                    self.pending_local_deletes.lock().remove(&path);
                    self.unrecord_hot_segment(&path);
                }
                Err(e) => {
                    tracing::warn!(
                        project_id = %self.project_id,
                        path = %path,
                        error = %e,
                        "deferred local segment delete failed; will retry"
                    );
                }
            }
        }
        deleted
    }

    /// Flushes a single bucket's L0 `MemBucket` into immutable Vortex L1 segment(s) and commits a new MVCC Manifest epoch.
    pub async fn flush_bucket(&self, bucket_id: u8) -> Result<Vec<SegmentMeta>> {
        let b_idx = bucket_id as usize;
        let Some(flush_lock) = self.bucket_flush_locks.get(b_idx) else {
            return Ok(Vec::new());
        };
        let bucket_guard = flush_lock.lock().await;

        let floor_l0 = self
            .manifest
            .read()
            .committed_l0_seq
            .get(&bucket_id)
            .copied()
            .unwrap_or(0);

        #[cfg(test)]
        if let Some(hook) = self.before_stage.lock().take() {
            hook();
        }
        let Some((max_l0, max_wal, staged_rows)) = self.mem_buckets[b_idx].stage_flush(floor_l0)
        else {
            return Ok(Vec::new());
        };
        // AFTER staging: every uid in the staged rows was interned before this point, so this
        // flush makes all of them durable before any segment that holds them is published. The
        // processor interns without flushing, so flushing first would miss ids interned between.
        self.id_graph().flush().await?;

        let mut by_date: BTreeMap<NaiveDate, Vec<StoredRow>> = BTreeMap::new();
        for row in staged_rows {
            by_date.entry(row.event_date()).or_default().push(row);
        }

        let prefix_root = if self.is_local_segment_tier() {
            "hot/segments"
        } else {
            "segments"
        };
        let mut written_segments = Vec::with_capacity(by_date.len());
        for (date, rows) in by_date {
            let seg_id = format!("seg_{}", Uuid::new_v4().simple());
            if let Some(meta) = write_vortex_segment_opts(
                &self.object_store,
                prefix_root,
                &self.project_id,
                date,
                bucket_id,
                &seg_id,
                rows,
                PutMode::Create,
            )
            .await?
            {
                written_segments.push(meta);
            }
        }

        // Atomically commit new MVCC Manifest epoch (without holding `publish_lock`) BEFORE evicting from L0 MemBucket!
        let committed_res = self
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
            .await;

        let committed_opt = match committed_res {
            Ok(opt) => opt,
            Err(e) => {
                for meta in &written_segments {
                    let _ = self
                        .object_store
                        .delete(&ObjectPath::from(meta.object_path.as_str()))
                        .await;
                }
                return Err(e);
            }
        };

        if let Some(ref committed) = committed_opt {
            self.wals[b_idx].advance_epoch(committed.epoch);
        }

        self.mem_buckets[b_idx].complete_flush(max_l0);
        let _ = self.wals[b_idx].purge_up_to(max_wal).await;

        let now_ms = Utc::now().timestamp_millis();
        self.after_flush_local_segments(&written_segments, now_ms);
        drop(bucket_guard);

        if committed_opt.is_some()
            && !self.id_graph().is_read_only()
            && self.publish_enabled.load(Ordering::Acquire)
            && self.upload_interval().is_zero()
        {
            self.upload_if_dirty_at(now_ms, false).await?;
        }

        Ok(written_segments)
    }

    #[doc(hidden)]
    pub fn set_publish_enabled(&self, enabled: bool) {
        self.publish_enabled.store(enabled, Ordering::Release);
    }

    pub fn last_published_seq(&self) -> Option<u64> {
        *self.last_published_seq.lock()
    }

    pub async fn latest_published_manifest(&self) -> Result<Option<PublishedManifest>> {
        self.published_store.load_latest().await
    }

    fn select_eligible_local_segments(
        &self,
        manifest: &Manifest,
        now_ms: i64,
        force_closed: bool,
    ) -> BTreeMap<(NaiveDate, u8), Vec<SegmentMeta>> {
        let today_utc = date_from_ms(now_ms);
        let upload_eager = self.upload_interval().is_zero();
        let closed_interval_ms = self.closed_date_upload_ms.load(Ordering::Acquire) as i64;
        let closed_max_bytes = self.closed_date_max_bytes.load(Ordering::Acquire);

        let mut by_date: BTreeMap<NaiveDate, Vec<(u8, SegmentMeta)>> = BTreeMap::new();
        for (&(date, bucket_id), segs) in &manifest.active_segments {
            for seg in segs {
                if seg.object_path.starts_with("hot/segments/") {
                    by_date
                        .entry(date)
                        .or_default()
                        .push((bucket_id, seg.clone()));
                }
            }
        }

        let mut out: BTreeMap<(NaiveDate, u8), Vec<SegmentMeta>> = BTreeMap::new();
        for (date, entries) in by_date {
            let eligible = if date >= today_utc || upload_eager || force_closed {
                true
            } else {
                let date_bytes: u64 = entries.iter().map(|(_, s)| s.file_size).sum();
                let first_seen = *self
                    .closed_date_pending_since_ms
                    .lock()
                    .entry(date)
                    .or_insert(now_ms);
                let age_ms = now_ms.saturating_sub(first_seen);
                date_bytes > closed_max_bytes || age_ms >= closed_interval_ms
            };
            if eligible {
                for (bucket_id, seg) in entries {
                    out.entry((date, bucket_id)).or_default().push(seg);
                }
            }
        }
        out
    }

    /// Runs the 7-step local-to-S3 upload & publish cycle if any eligible local segments, S3
    /// segment changes, identity mutations, or profile mutations exist. Performs **0 S3 calls**
    /// when idle or when only deferred closed-date `hot/segments/` are present.
    pub async fn publish_if_dirty(&self) -> Result<Option<PublishedManifest>> {
        self.upload_if_dirty_at(Utc::now().timestamp_millis(), false)
            .await
    }

    /// Time-parameterized 7-step upload & publish cycle for deterministic timer/crash testing.
    pub async fn upload_if_dirty_at(
        &self,
        now_ms: i64,
        force_closed: bool,
    ) -> Result<Option<PublishedManifest>> {
        let _publish_guard = self.publish_lock.lock().await;
        let id_graph = self.id_graph();
        if id_graph.is_read_only() || !self.publish_enabled.load(Ordering::Acquire) {
            return Ok(None);
        }
        if self.is_fenced() {
            return Err(self.not_writer());
        }

        self.drain_pending_local_deletes().await;

        // Reconcile any leftover local intent from an earlier failed Step 4/5/6 (without restart).
        if self.has_pending_upload_intent.load(Ordering::Acquire) {
            let cur_mf = (**self.manifest.read()).clone();
            let last_snap = self.last_snapshot_state.lock().last_snapshot_ref.clone();
            let stale_local_sources = reconcile_upload_intent_with_stores(
                &self.project_id,
                &self.object_store,
                &cur_mf,
                None,
                last_snap.as_ref(),
            )
            .await?;
            if !stale_local_sources.is_empty() {
                let tracked = self.tracked_hot_segments.lock();
                let mut pending = self.pending_local_deletes.lock();
                for local_path in stale_local_sources {
                    let sz = tracked.get(&local_path).copied().unwrap_or(0);
                    pending.insert(local_path, sz);
                }
            }
            self.drain_pending_local_deletes().await;
            self.has_pending_upload_intent
                .store(false, Ordering::Release);
        }

        let mut cur_mf = (**self.manifest.read()).clone();
        let frozen_groups = self.select_eligible_local_segments(&cur_mf, now_ms, force_closed);
        let cur_s3_parts = s3_partitions(&cur_mf);
        let s3_state_dirty = match &*self.last_published_s3_partitions.lock() {
            Some(published_parts) => *published_parts != cur_s3_parts,
            None => !cur_s3_parts.is_empty(),
        };

        let id_dirty = id_graph.user_count() > 0
            && self.last_snapshot_state.lock().exported_id_epoch != Some(id_graph.mutation_epoch());

        let prof_dirty = self
            .profile_router
            .get_if_open(&self.project_id)
            .is_some_and(|ps| {
                ps.dirty_seq() > 0
                    && self.last_snapshot_state.lock().exported_prof_dirty_seq
                        != Some(ps.dirty_seq())
            });

        if frozen_groups.is_empty() && !s3_state_dirty && !id_dirty && !prof_dirty {
            return Ok(None);
        }

        self.check_writer()?;
        if cur_mf.epoch == 0 {
            if let Some(committed) = self.commit_manifest_cas(|_| true).await? {
                cur_mf = (*committed).clone();
            }
        }

        let publish_seq = self
            .last_published_seq
            .lock()
            .map_or(0, |seq| seq.saturating_add(1));

        // ------------------------------------------------------------------
        // Step 1 — Freeze eligible local segments, chunk <= 64 MiB/batch per (date, bucket),
        //          pre-assign target S3 segment & snapshot paths, and persist UploadIntent locally.
        // ------------------------------------------------------------------
        struct PlannedBatch {
            date: NaiveDate,
            bucket_id: u8,
            sources: Vec<SegmentMeta>,
            target_segment_id: String,
            target_object_path: String,
        }

        let max_input_bytes = self
            .upload_merge_max_input_bytes
            .load(Ordering::Acquire)
            .max(1);
        let mut planned_batches: Vec<PlannedBatch> = Vec::new();
        for ((date, bucket_id), mut segs) in frozen_groups {
            segs.sort_by(|a, b| a.segment_id.cmp(&b.segment_id));
            let mut chunk: Vec<SegmentMeta> = Vec::new();
            let mut chunk_bytes: u64 = 0;
            for seg in segs {
                if !chunk.is_empty() && chunk_bytes.saturating_add(seg.file_size) > max_input_bytes
                {
                    let target_segment_id = format!("seg_up_{}", Uuid::new_v4().simple());
                    let yyyymmdd = date.format("%Y%m%d").to_string();
                    let target_object_path = format!(
                        "segments/{}/{yyyymmdd}/bucket_{bucket_id:03}/{target_segment_id}.pvtx",
                        self.project_id
                    );
                    planned_batches.push(PlannedBatch {
                        date,
                        bucket_id,
                        sources: std::mem::take(&mut chunk),
                        target_segment_id,
                        target_object_path,
                    });
                    chunk_bytes = 0;
                }
                chunk_bytes = chunk_bytes.saturating_add(seg.file_size);
                chunk.push(seg);
            }
            if !chunk.is_empty() {
                let target_segment_id = format!("seg_up_{}", Uuid::new_v4().simple());
                let yyyymmdd = date.format("%Y%m%d").to_string();
                let target_object_path = format!(
                    "segments/{}/{yyyymmdd}/bucket_{bucket_id:03}/{target_segment_id}.pvtx",
                    self.project_id
                );
                planned_batches.push(PlannedBatch {
                    date,
                    bucket_id,
                    sources: chunk,
                    target_segment_id,
                    target_object_path,
                });
            }
        }

        let prof_store = if let Some(ps) = self.profile_router.get_if_open(&self.project_id) {
            Some(ps)
        } else {
            let prof_prefix = ObjectPath::from(format!("profiles/{}", self.project_id));
            let mut list = self.object_store.list(Some(&prof_prefix));
            if futures::StreamExt::next(&mut list)
                .await
                .transpose()?
                .is_some()
            {
                Some(self.profile_router.get_or_open(&self.project_id).await?)
            } else {
                None
            }
        };

        let (prev_snapshot_ref, prev_id_epoch, prev_prof_dirty) = {
            let st = self.last_snapshot_state.lock();
            (
                st.last_snapshot_ref.clone(),
                st.exported_id_epoch,
                st.exported_prof_dirty_seq,
            )
        };

        let reuse_prof = prof_store.as_ref().and_then(|ps| {
            let cur_dirty = ps.dirty_seq();
            if prev_prof_dirty == Some(cur_dirty) {
                prev_snapshot_ref
                    .as_ref()
                    .and_then(|s| s.profiles.clone())
                    .map(|p| (p, cur_dirty))
            } else {
                None
            }
        });
        let preassigned_prof_path = if prof_store.is_some() && reuse_prof.is_none() {
            Some(format!(
                "published/{}/snapshots/profiles_{:012}_{}.pzst",
                self.project_id,
                publish_seq,
                Uuid::new_v4().simple()
            ))
        } else {
            None
        };

        let prelim_id_epoch = id_graph.mutation_epoch();
        let prelim_flushed_epoch = id_graph.flushed_mutation_epoch();
        let can_reuse_id_prelim = prev_id_epoch == Some(prelim_id_epoch)
            && prelim_flushed_epoch == prelim_id_epoch
            && !id_graph.has_pending_mutations()
            && prev_snapshot_ref.is_some();
        let preassigned_id_path = if can_reuse_id_prelim {
            None
        } else {
            Some(format!(
                "published/{}/snapshots/identity_{:012}_{}.pzst",
                self.project_id,
                publish_seq,
                Uuid::new_v4().simple()
            ))
        };

        let intent_store = UploadIntentStore::new(&self.project_id, self.object_store.clone());
        let mut intent = UploadIntent {
            intent_id: format!("intent_{}", Uuid::new_v4().simple()),
            writer_generation: cur_mf.writer_generation,
            base_local_epoch: cur_mf.epoch,
            target_publish_seq: publish_seq,
            created_at_ms: now_ms,
            batches: planned_batches
                .iter()
                .map(|b| UploadIntentBatch {
                    date: b.date,
                    bucket_id: b.bucket_id,
                    source_segment_ids: b.sources.iter().map(|s| s.segment_id.clone()).collect(),
                    source_object_paths: b.sources.iter().map(|s| s.object_path.clone()).collect(),
                    target_segment_id: b.target_segment_id.clone(),
                    target_object_path: b.target_object_path.clone(),
                })
                .collect(),
            new_identity_snapshot_path: preassigned_id_path.clone(),
            new_profile_snapshot_path: preassigned_prof_path.clone(),
        };
        self.has_pending_upload_intent
            .store(true, Ordering::Release);
        intent_store.save(&intent).await?;

        // ------------------------------------------------------------------
        // Step 2 — Snapshot profiles (`T_prof >= T_freeze`) to S3 (`PutMode::Create`).
        // ------------------------------------------------------------------
        let (prof_ref, new_prof_dirty, prof_uploaded_path) = match prof_store {
            Some(ps) => {
                if let Some((prev_prof, cur_dirty)) = reuse_prof {
                    (Some(prev_prof), Some(cur_dirty), None)
                } else {
                    let cur_dirty = ps.dirty_seq();
                    let bytes = ps.export_snapshot_bytes().await?;
                    let path_str = preassigned_prof_path.clone().unwrap_or_else(|| {
                        format!(
                            "published/{}/snapshots/profiles_{:012}_{}.pzst",
                            self.project_id,
                            publish_seq,
                            Uuid::new_v4().simple()
                        )
                    });
                    if intent.new_profile_snapshot_path.as_deref() != Some(path_str.as_str()) {
                        intent.new_profile_snapshot_path = Some(path_str.clone());
                        self.has_pending_upload_intent
                            .store(true, Ordering::Release);
                        intent_store.save(&intent).await?;
                    }
                    self.object_store
                        .put_opts(
                            &ObjectPath::from(path_str.as_str()),
                            PutPayload::from_bytes(bytes::Bytes::from(bytes)),
                            PutOptions {
                                mode: PutMode::Create,
                                ..PutOptions::default()
                            },
                        )
                        .await?;
                    (
                        Some(ProfileSnapshotRef {
                            object_path: path_str.clone(),
                            dirty_seq: cur_dirty,
                        }),
                        Some(cur_dirty),
                        Some(path_str),
                    )
                }
            }
            None => (None, None, None),
        };

        // ------------------------------------------------------------------
        // Step 3 — Flush & snapshot identity (`T_id >= T_prof >= T_freeze`) + HWM assertion.
        // ------------------------------------------------------------------
        id_graph.flush().await?;
        let flushed_hwm = id_graph.flushed_user_count();
        let cur_id_epoch = id_graph.mutation_epoch();
        let cur_flushed_epoch = id_graph.flushed_mutation_epoch();

        let reuse_id = if prev_id_epoch == Some(cur_id_epoch) && cur_flushed_epoch == cur_id_epoch {
            prev_snapshot_ref.as_ref().map(|s| s.identity.clone())
        } else {
            None
        };

        let (id_ref, new_id_epoch, id_uploaded_path) = if let Some(prev_id) = reuse_id {
            if prev_id.user_count < flushed_hwm {
                return Err(StorageError::HighWaterMarkViolation {
                    project_id: self.project_id.clone(),
                    identity_user_count: prev_id.user_count,
                    high_water_mark: flushed_hwm,
                });
            }
            (prev_id, cur_id_epoch, None)
        } else {
            let exp = id_graph.export_snapshot().await?;
            if exp.user_count < flushed_hwm {
                return Err(StorageError::HighWaterMarkViolation {
                    project_id: self.project_id.clone(),
                    identity_user_count: exp.user_count,
                    high_water_mark: flushed_hwm,
                });
            }
            let path_str = preassigned_id_path.clone().unwrap_or_else(|| {
                format!(
                    "published/{}/snapshots/identity_{:012}_{}.pzst",
                    self.project_id,
                    publish_seq,
                    Uuid::new_v4().simple()
                )
            });
            if intent.new_identity_snapshot_path.as_deref() != Some(path_str.as_str()) {
                intent.new_identity_snapshot_path = Some(path_str.clone());
                self.has_pending_upload_intent
                    .store(true, Ordering::Release);
                intent_store.save(&intent).await?;
            }
            self.object_store
                .put_opts(
                    &ObjectPath::from(path_str.as_str()),
                    PutPayload::from_bytes(bytes::Bytes::from(exp.payload)),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await?;
            (
                IdentitySnapshotRef {
                    object_path: path_str.clone(),
                    user_count: exp.user_count,
                    mutation_epoch: exp.mutation_epoch,
                },
                exp.mutation_epoch,
                Some(path_str),
            )
        };

        // ------------------------------------------------------------------
        // Step 4 — Merge frozen local segments sequentially one `(date, bucket_id)` partition
        //          at a time (`<= 64 MiB` input cap, create-only to S3 `segments/`).
        // ------------------------------------------------------------------
        let mut uploaded_outputs: Vec<Option<SegmentMeta>> =
            Vec::with_capacity(planned_batches.len());
        if !planned_batches.is_empty() {
            let erased = self.erased_snapshot();
            let reader = VortexSegmentReader::new(self.object_store.clone());
            for batch in &planned_batches {
                let mut merged_rows = Vec::new();
                for seg in &batch.sources {
                    let mut rows = reader.read_all_rows(seg).await?;
                    merged_rows.append(&mut rows);
                }
                if !erased.is_empty() {
                    merged_rows.retain(|r| !erased.hides(r.uid_internal, r.event_ts_ms));
                }
                let meta_opt = write_vortex_segment_opts(
                    &self.object_store,
                    "segments",
                    &self.project_id,
                    batch.date,
                    batch.bucket_id,
                    &batch.target_segment_id,
                    merged_rows,
                    PutMode::Create,
                )
                .await?;
                uploaded_outputs.push(meta_opt);
            }
        }

        // ------------------------------------------------------------------
        // Step 5 — Commit local manifest: frozen local segments out, new S3 segments in.
        // ------------------------------------------------------------------
        let mut committed_mf = cur_mf.clone();
        if !planned_batches.is_empty() {
            #[derive(Default)]
            struct PartitionReplace {
                source_ids: HashSet<String>,
                source_paths: Vec<String>,
                new_metas: Vec<SegmentMeta>,
            }
            let mut by_part: BTreeMap<(NaiveDate, u8), PartitionReplace> = BTreeMap::new();
            for (batch, new_meta_opt) in planned_batches.iter().zip(uploaded_outputs.iter()) {
                let part = by_part.entry((batch.date, batch.bucket_id)).or_default();
                for src in &batch.sources {
                    part.source_ids.insert(src.segment_id.clone());
                    part.source_paths.push(src.object_path.clone());
                }
                if let Some(ref meta) = new_meta_opt {
                    part.new_metas.push(meta.clone());
                }
            }

            let mut rejected_s3_paths: Vec<String> = Vec::new();
            let mut maybe_accepted_paths: Vec<String> = Vec::new();
            if let Some(committed) = self
                .commit_manifest_cas(|next_manifest| {
                    rejected_s3_paths.clear();
                    maybe_accepted_paths.clear();
                    let mut any_accepted = false;
                    for (&(date, bucket_id), repl) in &by_part {
                        let active_ok = next_manifest
                            .active_segments
                            .get(&(date, bucket_id))
                            .is_some_and(|entry| {
                                let active_ids: HashSet<&str> =
                                    entry.iter().map(|s| s.segment_id.as_str()).collect();
                                repl.source_ids
                                    .iter()
                                    .all(|sid| active_ids.contains(sid.as_str()))
                            });
                        if active_ok {
                            if let Some(entry) =
                                next_manifest.active_segments.get_mut(&(date, bucket_id))
                            {
                                entry.retain(|s| !repl.source_ids.contains(&s.segment_id));
                                entry.extend(repl.new_metas.iter().cloned());
                                if entry.is_empty() {
                                    next_manifest.active_segments.remove(&(date, bucket_id));
                                }
                            }
                            maybe_accepted_paths.extend(repl.source_paths.iter().cloned());
                            any_accepted = true;
                        } else {
                            rejected_s3_paths
                                .extend(repl.new_metas.iter().map(|m| m.object_path.clone()));
                        }
                    }
                    if !any_accepted {
                        return false;
                    }
                    next_manifest.dict_snapshot = self.dictionary.snapshot();
                    true
                })
                .await?
            {
                committed_mf = (*committed).clone();
                let tracked = self.tracked_hot_segments.lock();
                let mut pending = self.pending_local_deletes.lock();
                for local_path in maybe_accepted_paths {
                    let sz = tracked.get(&local_path).copied().unwrap_or(0);
                    pending.insert(local_path, sz);
                }
            }
            for rej_path in rejected_s3_paths {
                let _ = self
                    .object_store
                    .delete(&ObjectPath::from(rej_path.as_str()))
                    .await;
            }
        }

        // ------------------------------------------------------------------
        // Step 6 — Publish `PublishedManifest` (S3 segments only, create-only + head(seq-1) guard),
        //          update local `last_published_seq`, stamp grace, and run S3 GC.
        // ------------------------------------------------------------------
        let high_water_mark = id_ref.user_count;
        let snapshot_ref = SnapshotRef {
            identity: id_ref,
            profiles: prof_ref,
        };
        let pub_partitions = s3_partitions(&committed_mf);
        let pub_manifest = PublishedManifest {
            publish_seq,
            writer_generation: committed_mf.writer_generation,
            source_local_epoch: committed_mf.epoch.max(1),
            high_water_mark,
            published_at_ms: Utc::now().timestamp_millis(),
            partitions: pub_partitions.clone(),
            dict_snapshot: committed_mf.dict_snapshot.clone(),
            committed_wal_seq: committed_mf.committed_wal_seq.clone(),
            committed_l0_seq: committed_mf.committed_l0_seq.clone(),
            snapshot_ref: snapshot_ref.clone(),
        };

        let published = match self.published_store.publish_next(pub_manifest).await {
            Ok(p) => p,
            Err(StorageError::ManifestConflict) => {
                if let Some(p) = &id_uploaded_path {
                    let _ = self
                        .object_store
                        .delete(&ObjectPath::from(p.as_str()))
                        .await;
                }
                if let Some(p) = &prof_uploaded_path {
                    let _ = self
                        .object_store
                        .delete(&ObjectPath::from(p.as_str()))
                        .await;
                }
                if self.writer.is_some() {
                    self.fence();
                    return Err(self.not_writer());
                }
                return Err(StorageError::ManifestConflict);
            }
            Err(e) => return Err(e),
        };

        *self.last_published_seq.lock() = Some(publish_seq);
        *self.last_published_s3_partitions.lock() = Some(pub_partitions);
        {
            let mut st = self.last_snapshot_state.lock();
            st.last_snapshot_ref = Some(snapshot_ref.clone());
            st.exported_id_epoch = Some(new_id_epoch);
            st.exported_prof_dirty_seq = new_prof_dirty;
        }

        let pub_ts_ms = published.published_at_ms;
        let grace_ms = (self.upload_interval_ms() as i64).saturating_mul(2);
        let post_step6 = self
            .commit_manifest_cas(|next_manifest| {
                next_manifest.last_published_seq = Some(
                    next_manifest
                        .last_published_seq
                        .map_or(publish_seq, |s| s.max(publish_seq)),
                );
                for entry in &mut next_manifest.pending_s3_deletes {
                    if entry.superseding_pub_seq <= publish_seq && entry.eligible_after_ms.is_none()
                    {
                        entry.superseded_at_ms = Some(pub_ts_ms);
                        entry.eligible_after_ms = Some(pub_ts_ms.saturating_add(grace_ms));
                    }
                }
                if let Some(ref prev) = prev_snapshot_ref {
                    push_superseded_snapshots_to_pending(
                        &mut next_manifest.pending_s3_deletes,
                        prev,
                        &published.snapshot_ref,
                        publish_seq,
                        pub_ts_ms,
                        grace_ms,
                    );
                }
                true
            })
            .await?;
        if let Some(ref mf_arc) = post_step6 {
            *self.last_published_epoch.lock() = Some(mf_arc.epoch);
        }

        self.run_gc_at(now_ms).await?;

        // ------------------------------------------------------------------
        // Step 7 — Delete uploaded local segments (if unpinned, else queue) & clear UploadIntent.
        // ------------------------------------------------------------------
        {
            let active_hot_dates: HashSet<NaiveDate> = self
                .manifest
                .read()
                .active_segments
                .iter()
                .filter(|(_, segs)| {
                    segs.iter()
                        .any(|s| s.object_path.starts_with("hot/segments/"))
                })
                .map(|(&(d, _), _)| d)
                .collect();
            self.closed_date_pending_since_ms
                .lock()
                .retain(|d, _| active_hot_dates.contains(d));
        }

        self.drain_pending_local_deletes().await;

        intent_store.clear().await?;
        self.has_pending_upload_intent
            .store(false, Ordering::Release);
        Ok(Some(published))
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
                    let mut latest = self.manifest_store.load_latest().await?;
                    let mem_pub_seq = *self.last_published_seq.lock();
                    latest.last_published_seq = latest.last_published_seq.max(mem_pub_seq);
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
        // After staging, for the same reason as in `flush_bucket`.
        self.id_graph().flush().await?;

        let prefix_root = if self.is_local_segment_tier() {
            "hot/segments"
        } else {
            "segments"
        };
        let sem = self.node_load.flush_semaphore.clone();
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
                    write_vortex_segment_opts(
                        &store,
                        prefix_root,
                        &pid,
                        date,
                        bucket_id,
                        &seg_id,
                        rows,
                        PutMode::Create,
                    )
                    .await
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

        let committed_res = self
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
            .await;

        let committed_opt = match committed_res {
            Ok(opt) => opt,
            Err(e) => {
                for meta in &written_segments {
                    let _ = self
                        .object_store
                        .delete(&ObjectPath::from(meta.object_path.as_str()))
                        .await;
                }
                return Err(e);
            }
        };

        if let Some(ref committed) = committed_opt {
            for &(_, b_idx, _, _) in &bucket_watermarks {
                self.wals[b_idx].advance_epoch(committed.epoch);
            }
        }

        for &(_, b_idx, max_l0, max_wal) in &bucket_watermarks {
            self.mem_buckets[b_idx].complete_flush(max_l0);
            let _ = self.wals[b_idx].purge_up_to(max_wal).await;
        }

        let now_ms = Utc::now().timestamp_millis();
        self.after_flush_local_segments(&written_segments, now_ms);
        drop(guards);

        if committed_opt.is_some()
            && !self.id_graph().is_read_only()
            && self.publish_enabled.load(Ordering::Acquire)
            && self.upload_interval().is_zero()
        {
            self.upload_if_dirty_at(now_ms, false).await?;
        }

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
        // Acknowledged records still on their way into L0 belong in this flush.
        self.wait_applied(Duration::from_secs(10)).await;
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
    /// with bounded concurrency (`2`) and `64 MiB` per-partition input cap to bound RSS.
    pub async fn compact_min_segments(
        &self,
        reader: &VortexSegmentReader,
        min_segments: usize,
    ) -> Result<usize> {
        const COMPACTION_COMMIT_BATCH: usize = 32;
        const COMPACTION_CONCURRENCY: usize = 2;

        if self.is_local_segment_tier() && !self.upload_interval().is_zero() {
            return Ok(0);
        }

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
                            .map(|segs| {
                                let mut by_size = segs.clone();
                                by_size.sort_by_key(|s| s.file_size);
                                let mut selected = Vec::with_capacity(by_size.len());
                                let mut bytes = 0u64;
                                for seg in by_size {
                                    if selected.len() >= 2
                                        && bytes.saturating_add(seg.file_size)
                                            > DEFAULT_COMPACTION_MAX_INPUT_BYTES
                                    {
                                        break;
                                    }
                                    bytes = bytes.saturating_add(seg.file_size);
                                    selected.push(seg);
                                }
                                (date, bucket_id, selected)
                            })
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
                let mut superseded_local_paths: Vec<String> = Vec::new();
                let committed = {
                    let _publish_guard = self.publish_lock.lock().await;
                    let mut queued_superseded: Vec<String> = Vec::new();
                    let committed_opt = self
                        .commit_manifest_cas(|next_manifest| {
                            let mut any_accepted = false;
                            queued_superseded.clear();
                            superseded_local_paths.clear();
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
                                    queued_superseded.extend(item.superseded_paths.iter().cloned());
                                } else {
                                    accepted_flags[idx] = false;
                                }
                            }
                            if !any_accepted {
                                return false;
                            }
                            let cur_pub = next_manifest
                                .last_published_seq
                                .max(*self.last_published_seq.lock());
                            let superseding_pub_seq = cur_pub.map_or(0, |s| s.saturating_add(1));
                            let mut seen_pending: HashSet<String> = next_manifest
                                .pending_s3_deletes
                                .iter()
                                .map(|e| e.object_path.clone())
                                .collect();
                            for path in &queued_superseded {
                                if path.starts_with("hot/") {
                                    superseded_local_paths.push(path.clone());
                                } else if seen_pending.insert(path.clone()) {
                                    next_manifest.pending_s3_deletes.push(PendingS3Delete {
                                        object_path: path.clone(),
                                        kind: PendingS3DeleteKind::Segment,
                                        superseding_pub_seq,
                                        superseded_at_ms: None,
                                        eligible_after_ms: None,
                                    });
                                }
                            }
                            next_manifest.dict_snapshot = self.dictionary.snapshot();
                            true
                        })
                        .await?;
                    committed_opt
                };

                for local_path in superseded_local_paths {
                    if self.pin_tracker.is_pinned(&local_path) {
                        let sz = self
                            .tracked_hot_segments
                            .lock()
                            .get(&local_path)
                            .copied()
                            .unwrap_or(0);
                        self.pending_local_deletes.lock().insert(local_path, sz);
                    } else {
                        match self
                            .object_store
                            .delete(&ObjectPath::from(local_path.as_str()))
                            .await
                        {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                                self.unrecord_hot_segment(&local_path);
                            }
                            Err(_) => {
                                let sz = self
                                    .tracked_hot_segments
                                    .lock()
                                    .get(&local_path)
                                    .copied()
                                    .unwrap_or(0);
                                self.pending_local_deletes.lock().insert(local_path, sz);
                            }
                        }
                    }
                }

                if committed.is_some() {
                    if !self.id_graph().is_read_only()
                        && self.publish_enabled.load(Ordering::Acquire)
                        && self.upload_interval().is_zero()
                    {
                        self.upload_if_dirty_at(Utc::now().timestamp_millis(), false)
                            .await?;
                    }
                    for (item, accepted) in prepared.into_iter().zip(accepted_flags) {
                        if accepted {
                            compacted_partitions += 1;
                        } else {
                            let orphan = ObjectPath::from(item.new_meta.object_path.as_str());
                            let _ = self.object_store.delete(&orphan).await;
                        }
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

    /// Deletes superseded S3 segments and snapshot blobs whose superseding `PublishedManifest` has
    /// landed, whose 2-publish-interval grace window has elapsed, and which are unpinned.
    pub async fn run_gc(&self) -> Result<usize> {
        self.run_gc_at(Utc::now().timestamp_millis()).await
    }

    /// Time-parameterized variant of [`Self::run_gc`] for deterministic grace-window testing.
    pub async fn run_gc_at(&self, now_ms: i64) -> Result<usize> {
        if self.query_snapshot_store.is_some() {
            return Ok(0);
        }
        let _gc_guard = self.gc_lock.lock().await;

        let ready_paths: Vec<String> = {
            let mf = self.manifest.read();
            if mf.pending_s3_deletes.is_empty() {
                return Ok(0);
            }
            let active_paths: HashSet<&str> = mf
                .active_segments
                .values()
                .flat_map(|v| v.iter().map(|s| s.object_path.as_str()))
                .collect();
            let snap_guard = self.last_snapshot_state.lock();
            let mut active_snap_paths: HashSet<&str> = HashSet::new();
            if let Some(ref sr) = snap_guard.last_snapshot_ref {
                active_snap_paths.insert(sr.identity.object_path.as_str());
                if let Some(ref pr) = sr.profiles {
                    active_snap_paths.insert(pr.object_path.as_str());
                }
            }
            let last_pub = mf.last_published_seq.max(*self.last_published_seq.lock());
            let interval_ms = self.upload_interval_ms();

            let mut ready = Vec::new();
            for entry in &mf.pending_s3_deletes {
                let published_past = last_pub.is_some_and(|p| p >= entry.superseding_pub_seq);
                let grace_elapsed = entry.eligible_after_ms.is_some_and(|t| now_ms >= t);
                let zero_interval_guard = interval_ms > 0
                    || last_pub.is_some_and(|p| p >= entry.superseding_pub_seq.saturating_add(2));
                if published_past
                    && grace_elapsed
                    && zero_interval_guard
                    && !active_paths.contains(entry.object_path.as_str())
                    && !active_snap_paths.contains(entry.object_path.as_str())
                    && !self.pin_tracker.is_pinned(&entry.object_path)
                {
                    ready.push(entry.object_path.clone());
                }
            }
            ready
        };

        if ready_paths.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0usize;
        let mut resolved_paths = HashSet::new();
        for rel_path in ready_paths {
            let obj_path = ObjectPath::from(rel_path.as_str());
            match self.object_store.delete(&obj_path).await {
                Ok(()) => {
                    resolved_paths.insert(rel_path);
                    deleted_count += 1;
                }
                Err(object_store::Error::NotFound { .. }) => {
                    resolved_paths.insert(rel_path);
                }
                Err(e) => {
                    tracing::warn!(
                        project_id = %self.project_id,
                        path = %rel_path,
                        error = %e,
                        "deferred S3 delete failed; will retry"
                    );
                }
            }
        }

        if !resolved_paths.is_empty() {
            let mut base_epoch = 0u64;
            if let Some(committed) = self
                .commit_manifest_cas(|m| {
                    base_epoch = m.epoch;
                    let before = m.pending_s3_deletes.len();
                    m.pending_s3_deletes
                        .retain(|e| !resolved_paths.contains(&e.object_path));
                    m.pending_s3_deletes.len() != before
                })
                .await?
            {
                let mut lpe = self.last_published_epoch.lock();
                if committed.epoch == base_epoch.saturating_add(1) && *lpe == Some(base_epoch) {
                    *lpe = Some(committed.epoch);
                }
            }
        }

        Ok(deleted_count)
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

    async fn cleanup_retired_profile_stores(&self, qsnap_store: &Arc<dyn ObjectStore>) {
        let versioned_prefix = format!("profiles/{}/pub_", self.project_id);
        let active_prefix = self
            .profile_router
            .get_if_open(&self.project_id)
            .map(|s| s.db_prefix().to_string());
        let ready: Vec<Arc<ProjectProfileStore>> = {
            let mut guard = self.retired_profile_stores.lock();
            if guard.is_empty() {
                return;
            }
            let mut keep = Vec::new();
            let mut drain = Vec::new();
            for store in guard.drain(..) {
                if Arc::strong_count(&store) == 1
                    && store.db_prefix().starts_with(&versioned_prefix)
                    && active_prefix.as_deref() != Some(store.db_prefix())
                {
                    drain.push(store);
                } else {
                    keep.push(store);
                }
            }
            *guard = keep;
            drain
        };
        for store in ready {
            let prefix = store.db_prefix().to_string();
            let _ = store.close().await;
            drop(store);
            let _ = delete_objects_with_prefix(qsnap_store, &prefix).await;
        }
    }

    pub async fn lock_refresh(&self) -> AsyncMutexGuard<'_, ()> {
        self.refresh_lock.lock().await
    }

    /// Prepares a published-manifest refresh off-lock for `NodeRole::Query`: downloads any new
    /// published manifest and restores its Candidate B snapshots into `query_snapshot_store`
    /// without mutating `self.manifest`, `self.id_graph`, or the active routers.
    pub async fn prepare_published_refresh(&self) -> Result<Option<PreparedPublishedRefresh>> {
        let Some(ref qsnap_store) = self.query_snapshot_store else {
            return Ok(None);
        };
        let Some((seq, meta)) = self.published_store.latest_pub_seq().await? else {
            return Ok(None);
        };
        let prev_seq = *self.last_published_seq.lock();
        if prev_seq.is_some_and(|p| seq <= p) {
            return Ok(None);
        }
        let initial_pub = match self.published_store.load_from_meta(&meta).await {
            Ok(p) => p,
            Err(StorageError::ObjectStore(object_store::Error::NotFound { .. })) => {
                match self.published_store.load_latest().await? {
                    Some(newer) => {
                        if prev_seq.is_some_and(|p| newer.publish_seq <= p) {
                            return Ok(None);
                        }
                        newer
                    }
                    None => return Ok(None),
                }
            }
            Err(e) => return Err(e),
        };

        let (prev_id_path, prev_prof_path) = self.restored_snapshot_paths.lock().clone();
        let (pub_m, id_graph, prof_store, _) = restore_published_snapshots_with_retry(
            RestorePublishedCtx {
                project_id: &self.project_id,
                published_store: &self.published_store,
                s3_store: &self.object_store,
                qsnap_store,
                identity_router: self.identity_router.as_ref(),
                profile_router: &self.profile_router,
            },
            initial_pub,
            Some((prev_id_path.as_deref(), prev_prof_path.as_deref())),
            false,
        )
        .await?;

        if prev_seq.is_some_and(|p| pub_m.publish_seq <= p) {
            return Ok(None);
        }

        Ok(Some(PreparedPublishedRefresh {
            pub_m,
            id_graph,
            prof_store,
        }))
    }

    /// Synchronously installs a [`PreparedPublishedRefresh`] produced by
    /// [`Self::prepare_published_refresh`], returning `true` if applied or `false` if a newer or
    /// equal `publish_seq` was already active.
    pub fn apply_prepared_refresh(&self, prepared: PreparedPublishedRefresh) -> bool {
        let prev_seq = *self.last_published_seq.lock();
        if prev_seq.is_some_and(|p| prepared.pub_m.publish_seq <= p) {
            return false;
        }
        if let Some(ref g) = prepared.id_graph {
            if let Some(ref router) = self.identity_router {
                router.replace_read_only(&self.project_id, g.clone());
            }
            *self.id_graph.write() = g.clone();
        }
        if let Some(ref p) = prepared.prof_store {
            if let Some(old_prof) = self
                .profile_router
                .replace_read_only(&self.project_id, p.clone())
            {
                let versioned_prefix = format!("profiles/{}/pub_", self.project_id);
                if old_prof.db_prefix().starts_with(&versioned_prefix) {
                    self.retired_profile_stores.lock().push(old_prof);
                }
            }
        }
        self.dictionary
            .merge_snapshot(&prepared.pub_m.dict_snapshot);
        let next_mf = prepared
            .pub_m
            .to_local_manifest(prepared.pub_m.publish_seq.saturating_add(1));
        for (&bucket_id, &committed_l0) in &next_mf.committed_l0_seq {
            if let Some(mb) = self.mem_buckets.get(bucket_id as usize) {
                mb.complete_flush(committed_l0);
            }
        }
        *self.manifest.write() = Arc::new(next_mf);
        *self.restored_snapshot_paths.lock() = (
            Some(prepared.pub_m.snapshot_ref.identity.object_path.clone()),
            prepared
                .pub_m
                .snapshot_ref
                .profiles
                .as_ref()
                .map(|p| p.object_path.clone()),
        );
        *self.last_snapshot_state.lock() = LastSnapshotState {
            last_snapshot_ref: Some(prepared.pub_m.snapshot_ref),
            exported_id_epoch: None,
            exported_prof_dirty_seq: None,
        };
        *self.last_published_seq.lock() = Some(prepared.pub_m.publish_seq);
        true
    }

    /// Closes and deletes any retired versioned `query_hot` profile stores that no reader still pins.
    pub async fn cleanup_retired_stores(&self) {
        if let Some(ref qsnap_store) = self.query_snapshot_store {
            self.cleanup_retired_profile_stores(qsnap_store).await;
        }
    }

    /// Refreshes the in-memory `ProjectIdentityGraph`, `Manifest`, and `ProjectDictionary` from `ObjectStore`
    /// (or from S3 `published/` + local Candidate B snapshot restores on remote `NodeRole::Query` nodes).
    pub async fn refresh_manifest(&self) -> Result<bool> {
        let _refresh_guard = self.refresh_lock.lock().await;
        if self.query_snapshot_store.is_some() {
            if let Some(prepared) = self.prepare_published_refresh().await? {
                let applied = self.apply_prepared_refresh(prepared);
                self.cleanup_retired_stores().await;
                return Ok(applied);
            }
            self.cleanup_retired_stores().await;
            if self.last_published_seq().is_some() {
                return Ok(false);
            }
        }

        let id_refreshed = self.id_graph().refresh().await?;
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
        *self.last_published_seq.lock() = latest.last_published_seq;
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
    pub publish: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub enum MaintenanceKind {
    IdentityFlush,
    Flush,
    Compaction,
    ManifestRefresh,
    Publish,
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
                MaintenanceKind::Publish => (&self.publish, "publish"),
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
    query_snapshot_store: OnceLock<Arc<dyn ObjectStore>>,
    segment_reader: Arc<VortexSegmentReader>,
    replay_store: Arc<ReplayStore>,
    projects: DashMap<String, Arc<ProjectStorage>>,
    erased: DashMap<String, Arc<ErasedUids>>,
    open_lock: AsyncMutex<()>,
    writer_leases: OnceLock<Arc<dyn WriterLeases>>,
    node_load: Arc<NodeLoad>,
    local_segment_tier: Arc<AtomicBool>,
    upload_interval_ms: Arc<AtomicU64>,
    closed_date_upload_ms: Arc<AtomicU64>,
    closed_date_max_bytes: Arc<AtomicU64>,
    upload_merge_max_input_bytes: Arc<AtomicU64>,
    upload_signal: Arc<UploadSignal>,
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
        let local_segment_tier = Arc::new(AtomicBool::new(is_hot_routed_store(
            encrypted_store.raw_inner().as_ref(),
        )));
        let upload_interval_ms =
            Arc::new(AtomicU64::new(upload_interval_from_env().as_millis() as u64));
        let closed_date_upload_ms = Arc::new(AtomicU64::new(
            closed_date_upload_interval_from_env().as_millis() as u64,
        ));
        let closed_date_max_bytes = Arc::new(AtomicU64::new(closed_date_max_bytes_from_env()));
        let upload_merge_max_input_bytes =
            Arc::new(AtomicU64::new(upload_merge_max_input_bytes_from_env()));
        let upload_signal = Arc::new(UploadSignal::default());
        Arc::new(Self {
            maintenance_errors: MaintenanceErrors::default(),
            config,
            encrypted_store,
            object_store: wrapped_store,
            identity_router,
            profile_router,
            query_snapshot_store: OnceLock::new(),
            segment_reader,
            replay_store,
            projects: DashMap::new(),
            erased: DashMap::new(),
            open_lock: AsyncMutex::new(()),
            writer_leases: OnceLock::new(),
            node_load: NodeLoad::from_env(),
            local_segment_tier,
            upload_interval_ms,
            closed_date_upload_ms,
            closed_date_max_bytes,
            upload_merge_max_input_bytes,
            upload_signal,
        })
    }

    /// Configures this read-only engine (`NodeRole::Query`) to discover S3 `published/`
    /// manifests and restore Candidate B snapshots into `snapshot_store`.
    pub fn enable_published_reader(&self, snapshot_store: Arc<dyn ObjectStore>) {
        let _ = self.query_snapshot_store.set(snapshot_store);
    }

    pub fn is_published_reader(&self) -> bool {
        self.query_snapshot_store.get().is_some()
    }

    /// Returns `(publish_seq, profiles_snapshot_object_path)` if a `PublishedManifest` has been
    /// loaded for `project_id` on this `NodeRole::Query` engine.
    pub fn published_profile_snapshot_key(
        &self,
        project_id: &str,
    ) -> Option<(u64, Option<String>)> {
        if !self.is_published_reader() {
            return None;
        }
        let ps = self.projects.get(project_id)?;
        let pub_seq = ps.last_published_seq()?;
        let prof_path = ps.restored_snapshot_paths.lock().1.clone();
        Some((pub_seq, prof_path))
    }

    pub fn upload_interval(&self) -> Duration {
        Duration::from_millis(self.upload_interval_ms.load(Ordering::Acquire))
    }

    pub fn set_upload_interval(&self, interval: Duration) {
        self.upload_interval_ms
            .store(interval.as_millis() as u64, Ordering::Release);
    }

    pub fn is_local_segment_tier(&self) -> bool {
        self.local_segment_tier.load(Ordering::Acquire)
    }

    /// Overrides whether L0 flushes write to local `hot/segments/` (defaults to `true` when `HotRouted` is active).
    pub fn set_local_segment_tier(&self, enabled: bool) {
        self.local_segment_tier.store(enabled, Ordering::Release);
    }

    /// Overrides closed-date upload policy (`PULSE_CLOSED_DATE_UPLOAD_SECS`, `PULSE_CLOSED_DATE_MAX_BYTES`).
    pub fn set_closed_date_upload_policy(&self, interval: Duration, max_bytes: u64) {
        self.closed_date_upload_ms
            .store(interval.as_millis() as u64, Ordering::Release);
        self.closed_date_max_bytes
            .store(max_bytes.max(1), Ordering::Release);
    }

    /// Overrides the per-partition input-byte cap for Step 4 upload merging (`PULSE_UPLOAD_MERGE_MAX_INPUT_BYTES`).
    pub fn set_upload_merge_max_input_bytes(&self, max_bytes: u64) {
        self.upload_merge_max_input_bytes
            .store(max_bytes.max(1), Ordering::Release);
    }

    /// Returns a future woken when `1x` `hot/segments/` bounds or closed-date `> 4 MiB` requests an early upload.
    pub fn upload_requested(&self) -> tokio::sync::futures::Notified<'_> {
        self.upload_signal.notified()
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
            Some(self.identity_router.clone()),
            self.profile_router.clone(),
            self.query_snapshot_store.get().cloned(),
            lease,
            self.node_load.clone(),
            self.local_segment_tier.clone(),
            self.upload_interval_ms.clone(),
            self.closed_date_upload_ms.clone(),
            self.closed_date_max_bytes.clone(),
            self.upload_merge_max_input_bytes.clone(),
            self.upload_signal.clone(),
        )
        .await?;
        self.projects.insert(project_id.to_string(), ps.clone());
        // This node writes the project now: readers for it would only go stale.
        self.identity_router.evict_reader(project_id);
        self.profile_router.evict_reader(project_id);
        // After the insert, so a concurrent `set_erased` is seen by one side or the other.
        if let Some(e) = self.erased.get(project_id) {
            ps.set_erased(e.clone());
        }
        Ok(ps)
    }

    /// Overrides the byte bounds read from the environment: rows held in L0 and logged events not
    /// yet applied, each per project and per node.
    pub fn set_byte_limits(&self, l0: (usize, usize), log_lag: (usize, usize)) {
        let load = &self.node_load;
        load.l0_max_project.store(l0.0, Ordering::Relaxed);
        load.l0_max_node.store(l0.1, Ordering::Relaxed);
        load.log_max_project.store(log_lag.0, Ordering::Relaxed);
        load.log_max_node.store(log_lag.1, Ordering::Relaxed);
    }

    /// Bytes of L0 rows and of logged-but-unapplied events held by this node.
    pub fn memory_load(&self) -> (usize, usize) {
        (
            self.node_load.l0_bytes.load(Ordering::Relaxed),
            self.node_load.unapplied_bytes.load(Ordering::Relaxed),
        )
    }

    /// Total bytes of active WAL files across open projects on this node.
    pub fn wal_bytes(&self) -> u64 {
        self.node_load.wal_bytes.load(Ordering::Relaxed)
    }

    /// Configured node-wide byte limit for unapplied WAL records (`PULSE_LOG_LAG_MAX_MB`).
    pub fn log_lag_limit_bytes(&self) -> usize {
        self.node_load.log_max_node.load(Ordering::Relaxed)
    }

    /// Sum of `(unapplied_records, unapplied_events)` queued across open projects on this node.
    pub fn apply_queue_depth(&self) -> (u64, u64) {
        let mut recs = 0u64;
        let mut evs = 0u64;
        for p in self.open_projects() {
            let (r, e) = p.apply_queue_depth();
            recs = recs.saturating_add(r);
            evs = evs.saturating_add(e);
        }
        (recs, evs)
    }

    /// Age in seconds of the oldest unapplied WAL record across open projects (`0.0` when caught up).
    pub fn max_apply_lag_seconds(&self) -> f64 {
        let now_ms = Utc::now().timestamp_millis();
        let max_ms = self
            .open_projects()
            .into_iter()
            .map(|p| p.max_apply_lag_ms(now_ms))
            .max()
            .unwrap_or(0);
        max_ms as f64 / 1_000.0
    }

    /// Opens a project for a query: like `get_or_open_project`, then waits briefly for events
    /// this node has acknowledged but not yet applied, so a read right after a write sees it.
    pub async fn open_for_read(&self, project_id: &str) -> Result<Arc<ProjectStorage>> {
        let ps = self.get_or_open_project(project_id).await?;
        ps.wait_applied(Duration::from_millis(250)).await;
        Ok(ps)
    }

    /// Overrides the `1x` `hot/segments/` byte bounds read from the environment: `(per project, per node)`.
    /// Reaching `1x` triggers an immediate upload cycle; reaching `2x` returns `HotSegmentBackpressure` (`429`).
    pub fn set_hot_segment_byte_limits(&self, project_1x: u64, node_1x: u64) {
        self.node_load
            .hot_seg_max_project
            .store(project_1x, Ordering::Relaxed);
        self.node_load
            .hot_seg_max_node
            .store(node_1x, Ordering::Relaxed);
    }

    /// Bytes of local `hot/segments/` files across all open projects on this node.
    pub fn hot_segment_bytes(&self) -> u64 {
        self.node_load.hot_segment_bytes.load(Ordering::Relaxed)
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

    /// Whether this node may write `project_id`'s identity and profile stores: always without
    /// writer leases; with them, only while it holds the project open as its writer.
    fn writes_project(&self, project_id: &str) -> bool {
        if self.writer_leases.get().is_none() {
            return true;
        }
        self.projects
            .get(project_id)
            .is_some_and(|p| !p.is_fenced())
    }

    /// The project's user-profile store. `write` takes the writer lease first (opening the
    /// project), so a node that does not hold it gets `NotWriter` instead of taking the store
    /// from the writer. A read on such a node gets a read-only store.
    pub async fn profile_store(
        &self,
        project_id: &str,
        write: bool,
    ) -> Result<Arc<crate::ProjectProfileStore>> {
        if self.profile_router.is_read_only() {
            if self.is_published_reader() && !self.projects.contains_key(project_id) {
                self.get_or_open_project(project_id).await?;
            }
            return self.profile_router.get_or_open(project_id).await;
        }
        if write {
            self.get_or_open_project(project_id).await?;
        } else if !self.writes_project(project_id) {
            return Ok(self.profile_router.open_reader(project_id));
        }
        self.profile_router.get_or_open(project_id).await
    }

    /// The project's identity graph, on the same terms as `profile_store`.
    pub async fn identity_graph(
        &self,
        project_id: &str,
        write: bool,
    ) -> Result<Arc<ProjectIdentityGraph>> {
        let router = &self.identity_router;
        if router.is_read_only() {
            if self.is_published_reader() && !self.projects.contains_key(project_id) {
                self.get_or_open_project(project_id).await?;
            }
        } else if write {
            self.get_or_open_project(project_id).await?;
        } else if !self.writes_project(project_id) {
            return router
                .open_reader(project_id)
                .await
                .map_err(StorageError::Identity);
        }
        router
            .get_or_open(project_id)
            .await
            .map_err(StorageError::Identity)
    }

    fn drop_fenced(&self, ps: &Arc<ProjectStorage>) {
        self.projects
            .remove_if(&ps.project_id, |_, cur| Arc::ptr_eq(cur, ps));
        // Their in-memory state is stale once another node has written the project.
        self.identity_router.evict(&ps.project_id);
        self.profile_router.evict(&ps.project_id);
    }

    pub fn get_project(&self, project_id: &str) -> Option<Arc<ProjectStorage>> {
        self.projects.get(project_id).map(|e| e.value().clone())
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

    /// Uploads eligible local segments and publishes all open dirty projects to S3 `published/`
    /// (0 S3 calls for idle projects or projects holding only deferred closed-date segments).
    pub async fn publish_all_projects(&self) -> Result<usize> {
        self.upload_all_projects_at(Utc::now().timestamp_millis())
            .await
    }

    /// Time-parameterized upload + publish across all open projects.
    pub async fn upload_all_projects_at(&self, now_ms: i64) -> Result<usize> {
        self.upload_signal
            .in_progress
            .store(true, Ordering::Release);
        let _guard = UploadInProgressGuard(&self.upload_signal);

        let node_bytes = self.hot_segment_bytes();
        let node_1x = self.node_load.hot_seg_max_node.load(Ordering::Relaxed);
        let proj_1x = self.node_load.hot_seg_max_project.load(Ordering::Relaxed);
        let node_over_1x = node_bytes >= node_1x;

        let mut published = 0usize;
        let mut first_err: Option<StorageError> = None;
        for p in self.open_projects() {
            let force_closed = node_over_1x || p.hot_segment_bytes() >= proj_1x;
            match p.upload_if_dirty_at(now_ms, force_closed).await {
                Ok(Some(_)) => published += 1,
                Ok(None) => {}
                Err(StorageError::NotWriter { .. }) => {}
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(err) = first_err {
            self.upload_signal.mark_failure();
            return Err(err);
        }
        self.upload_signal.mark_success();
        Ok(published)
    }

    /// Runs deferred S3 deletion grace cleanup and unpinned local segment drains across all open projects.
    pub async fn run_gc_all_projects(&self) -> Result<usize> {
        let mut deleted = 0usize;
        for p in self.open_projects() {
            deleted += p.drain_pending_local_deletes().await;
            deleted += p.run_gc().await?;
        }
        Ok(deleted)
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

    /// Spawns background L0 age-based flusher, late-data compactor, GC drain, and periodic upload/publish loop.
    pub fn spawn_background_maintenance(
        self: &Arc<Self>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let engine = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            let interval = engine.upload_interval();
            let mut publish_ticker = (!interval.is_zero()).then(|| {
                let mut t =
                    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
                t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                t
            });
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
                            p.drain_pending_local_deletes().await;
                            errs.check(MaintenanceKind::Compaction, p.run_gc().await);
                        }
                    }
                    _ = async {
                        match publish_ticker.as_mut() {
                            Some(t) => t.tick().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        engine
                            .maintenance_errors
                            .check(MaintenanceKind::Publish, engine.publish_all_projects().await);
                    }
                    _ = engine.upload_requested() => {
                        engine
                            .maintenance_errors
                            .check(MaintenanceKind::Publish, engine.publish_all_projects().await);
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

/// Magic bytes for dead-lettered event envelopes (`PLSD` v1).
pub const DEAD_LETTER_MAGIC: [u8; 4] = *b"PLSD";
pub const DEAD_LETTER_VERSION: u8 = 1;
const MAX_DEAD_LETTER_REASON_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct DeadLetterEntry {
    pub object_path: String,
    pub bucket_id: u8,
    pub wal_seq: u64,
    pub recorded_at_ms: i64,
    pub reason: Option<String>,
    pub event: WireEvent,
}

fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Encodes a dead-lettered `WireEvent` alongside its timestamp and refusal reason into `PLSD` v1.
pub fn encode_dead_letter(recorded_at_ms: i64, reason: &str, ev: &WireEvent) -> Result<Vec<u8>> {
    let reason_bytes = truncate_utf8(reason, MAX_DEAD_LETTER_REASON_BYTES).as_bytes();
    let plsb_bytes = WireBatch::new(vec![ev.clone()]).encode_binary(1)?;
    let reason_len = u32::try_from(reason_bytes.len())
        .map_err(|_| StorageError::Corrupted("dead-letter reason exceeds u32".into()))?;
    let plsb_len = u32::try_from(plsb_bytes.len())
        .map_err(|_| StorageError::Corrupted("dead-letter payload exceeds u32".into()))?;

    let mut out = Vec::with_capacity(4 + 1 + 8 + 4 + reason_bytes.len() + 4 + plsb_bytes.len());
    out.extend_from_slice(&DEAD_LETTER_MAGIC);
    out.push(DEAD_LETTER_VERSION);
    out.extend_from_slice(&recorded_at_ms.to_le_bytes());
    out.extend_from_slice(&reason_len.to_le_bytes());
    out.extend_from_slice(reason_bytes);
    out.extend_from_slice(&plsb_len.to_le_bytes());
    out.extend_from_slice(&plsb_bytes);
    Ok(out)
}

fn parse_dead_letter_path(object_path: &str) -> (u8, u64) {
    let mut bucket_id = 0u8;
    let mut wal_seq = 0u64;
    for part in object_path.split('/') {
        if let Some(b_str) = part.strip_prefix("bucket_") {
            if let Ok(b) = b_str.parse::<u8>() {
                bucket_id = b;
            }
        } else if let Some(rest) = part.strip_prefix("seq_") {
            if let Some((seq_str, _)) = rest.split_once('_') {
                if let Ok(s) = seq_str.parse::<u64>() {
                    wal_seq = s;
                }
            }
        }
    }
    (bucket_id, wal_seq)
}

/// Decodes either a `PLSD` v1 dead-letter envelope or a legacy `PLSB` single-event file.
pub fn decode_dead_letter(
    bytes: &[u8],
    object_path: &str,
    fallback_ts_ms: i64,
) -> Result<DeadLetterEntry> {
    let (bucket_id, wal_seq) = parse_dead_letter_path(object_path);
    if bytes.starts_with(&DEAD_LETTER_MAGIC) {
        if bytes.len() < 21 {
            return Err(StorageError::Corrupted(format!(
                "dead-letter `{object_path}` shorter than 21-byte PLSD header"
            )));
        }
        let version = bytes[4];
        if version != DEAD_LETTER_VERSION {
            return Err(StorageError::Corrupted(format!(
                "unsupported PLSD dead-letter version {version} at `{object_path}`"
            )));
        }
        let recorded_at_ms = i64::from_le_bytes(bytes[5..13].try_into().unwrap());
        let reason_len = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
        if reason_len > MAX_DEAD_LETTER_REASON_BYTES || 17 + reason_len + 4 > bytes.len() {
            return Err(StorageError::Corrupted(format!(
                "invalid PLSD reason length {reason_len} at `{object_path}`"
            )));
        }
        let reason_raw = std::str::from_utf8(&bytes[17..17 + reason_len]).map_err(|_| {
            StorageError::Corrupted(format!("non-UTF-8 PLSD reason at `{object_path}`"))
        })?;
        let reason = if reason_raw.is_empty() {
            None
        } else {
            Some(reason_raw.to_string())
        };
        let plsb_off = 17 + reason_len;
        let plsb_len =
            u32::from_le_bytes(bytes[plsb_off..plsb_off + 4].try_into().unwrap()) as usize;
        let payload_off = plsb_off + 4;
        if payload_off.checked_add(plsb_len) != Some(bytes.len()) {
            return Err(StorageError::Corrupted(format!(
                "PLSD payload length mismatch at `{object_path}`"
            )));
        }
        let batch = WireBatch::decode_binary(&bytes[payload_off..])?;
        if batch.events.len() != 1 {
            return Err(StorageError::Corrupted(format!(
                "PLSD `{object_path}` expected 1 event, found {}",
                batch.events.len()
            )));
        }
        Ok(DeadLetterEntry {
            object_path: object_path.to_string(),
            bucket_id,
            wal_seq,
            recorded_at_ms,
            reason,
            event: batch.events.into_iter().next().unwrap(),
        })
    } else if bytes.starts_with(b"PLSB") {
        let batch = WireBatch::decode_binary(bytes)?;
        if batch.events.len() != 1 {
            return Err(StorageError::Corrupted(format!(
                "legacy PLSB dead-letter `{object_path}` expected 1 event, found {}",
                batch.events.len()
            )));
        }
        Ok(DeadLetterEntry {
            object_path: object_path.to_string(),
            bucket_id,
            wal_seq,
            recorded_at_ms: fallback_ts_ms,
            reason: None,
            event: batch.events.into_iter().next().unwrap(),
        })
    } else {
        Err(StorageError::Corrupted(format!(
            "unrecognized dead-letter magic at `{object_path}`"
        )))
    }
}

/// Lists and decodes dead-lettered events under `wal_dead/{project_id}/` (optionally scoped to one bucket).
pub async fn list_dead_letters(
    object_store: &Arc<dyn ObjectStore>,
    project_id: &str,
    bucket_filter: Option<u8>,
) -> Result<Vec<DeadLetterEntry>> {
    crate::replay_store::validate_replay_identifier("project_id", project_id)?;
    let prefix = match bucket_filter {
        Some(b) => ObjectPath::from(format!("wal_dead/{project_id}/bucket_{b:03}/")),
        None => ObjectPath::from(format!("wal_dead/{project_id}/")),
    };
    let metas: Vec<_> = object_store.list(Some(&prefix)).try_collect().await?;
    let mut parsed: Vec<(u8, u64, String, i64, ObjectPath)> = metas
        .into_iter()
        .map(|meta| {
            let loc_str = meta.location.to_string();
            let (b, seq) = parse_dead_letter_path(&loc_str);
            (
                b,
                seq,
                loc_str,
                meta.last_modified.timestamp_millis(),
                meta.location,
            )
        })
        .collect();
    parsed.sort_by(|a, b| (a.0, a.1, &a.2).cmp(&(b.0, b.1, &b.2)));

    let mut out = Vec::with_capacity(parsed.len());
    for (_, _, loc_str, last_mod_ms, loc) in parsed {
        let bytes = object_store.get(&loc).await?.bytes().await?;
        out.push(decode_dead_letter(&bytes, &loc_str, last_mod_ms)?);
    }
    Ok(out)
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

    /// A uid interned after the flush decided what is durable, but before it staged the rows,
    /// must still be durable before the segment holding it is published. Otherwise a crash
    /// there can hand the uid to a different distinct_id after restart.
    #[tokio::test]
    async fn ids_interned_just_before_staging_are_durable_when_the_segment_is_published() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cfg = StorageConfig {
            num_buckets: 1,
            ..StorageConfig::default()
        };
        let router = IdentityRouter::new(store.clone());
        let ps = ProjectStorage::open_and_recover("p", cfg, store.clone(), &router)
            .await
            .unwrap();
        let ev = WireEvent {
            event_uuid: WireEvent::deterministic_uuid("late_user", "s", "e", 1, 1),
            routing_seed: "late_user".into(),
            distinct_id: "late_user".into(),
            session_id: "s".into(),
            event_name: "e".into(),
            client_event_ts_ms: 1_790_000_000_000,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        };
        // What the processor does: intern and insert, with no identity flush of its own.
        let uid = Arc::new(AtomicU64::new(0));
        let (hook_ps, hook_uid) = (ps.clone(), uid.clone());
        *ps.before_stage.lock() = Some(Box::new(move || {
            let id = hook_ps.id_graph().get_or_intern(&ev.distinct_id, 0);
            hook_uid.store(u64::from(id), Ordering::SeqCst);
            let fp = event_uuid_fingerprint(&ev.event_uuid);
            let row = hook_ps.wire_event_to_row(&ev, id, 1, fp).unwrap();
            hook_ps.mem_buckets[0].append_batch(vec![row]);
        }));
        let published = ps.flush_bucket(0).await.unwrap();
        assert!(!published.is_empty(), "the row was not flushed");

        // Crash here, then restart: read the identity store as it is in object storage (no
        // close, which would flush).
        let fresh = IdentityRouter::new_read_only(store.clone());
        let graph = fresh.get_or_open("p").await.unwrap();
        let uid = uid.load(Ordering::SeqCst) as u32;
        assert_eq!(
            graph.external_id(uid).as_deref(),
            Some("late_user"),
            "a published segment holds uid {uid}, which is not durable"
        );
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
        let entries = list_dead_letters(&store, "p", None).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event.event_name, "one_too_many");
        assert!(
            entries[0]
                .reason
                .as_deref()
                .is_some_and(|r| r.contains("dictionary capacity exceeded")),
            "expected persisted reason, got {:?}",
            entries[0].reason
        );
    }

    #[tokio::test]
    async fn list_dead_letters_supports_plsd_v1_and_legacy_plsb() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ev1 = WireEvent {
            event_uuid: WireEvent::deterministic_uuid("u1", "s1", "e1", 100, 1),
            routing_seed: "u1".into(),
            distinct_id: "u1".into(),
            session_id: "s1".into(),
            event_name: "e1".into(),
            client_event_ts_ms: 1_790_000_000_001,
            client_sent_ts_ms: 0,
            props: vec![("k".into(), PropValue::Str("v".into()))],
            replay_offset_kb: 0,
        };
        let ev2 = WireEvent {
            event_uuid: WireEvent::deterministic_uuid("u2", "s2", "e2", 200, 2),
            routing_seed: "u2".into(),
            distinct_id: "u2".into(),
            session_id: "s2".into(),
            event_name: "e2".into(),
            client_event_ts_ms: 1_790_000_000_002,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        };
        let plsd =
            encode_dead_letter(1_790_000_111_000, "project schema cap reached", &ev1).unwrap();
        let plsb = WireBatch::new(vec![ev2.clone()]).encode_binary(1).unwrap();
        store
            .put(
                &ObjectPath::from(
                    "wal_dead/proj/bucket_000/seq_000000000001_0000000000000001.plsd",
                ),
                plsd.into(),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from(
                    "wal_dead/proj/bucket_002/seq_000000000003_0000000000000002.plsb",
                ),
                plsb.into(),
            )
            .await
            .unwrap();

        let all = list_dead_letters(&store, "proj", None).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].bucket_id, 0);
        assert_eq!(all[0].wal_seq, 1);
        assert_eq!(all[0].recorded_at_ms, 1_790_000_111_000);
        assert_eq!(all[0].reason.as_deref(), Some("project schema cap reached"));
        assert_eq!(all[0].event.distinct_id, "u1");
        assert_eq!(all[0].event.client_event_ts_ms, 1_790_000_000_001);

        assert_eq!(all[1].bucket_id, 2);
        assert_eq!(all[1].wal_seq, 3);
        assert_eq!(all[1].reason, None);
        assert_eq!(all[1].event.distinct_id, "u2");

        let only_b2 = list_dead_letters(&store, "proj", Some(2)).await.unwrap();
        assert_eq!(only_b2.len(), 1);
        assert_eq!(only_b2[0].event.event_name, "e2");
    }

    /// Events written to the WAL and not yet applied are bounded in bytes: the appender is
    /// refused until the processor catches up.
    #[tokio::test]
    async fn unapplied_log_bytes_push_back_until_the_record_is_applied() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = StorageEngine::new(
            StorageConfig {
                num_buckets: 1,
                ..StorageConfig::default()
            },
            store.clone(),
            Arc::new(IdentityRouter::new(store)),
        );
        engine.set_byte_limits((usize::MAX, usize::MAX), (1_000, usize::MAX));
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
        ps.check_l0_backpressure(0).unwrap();
        let batch = WireBatch::new(vec![ev.clone()]);
        let wal_seq = {
            let guard = ps.lock_bucket_append(0).await;
            ps.log_events_locked(&guard, 0, &batch, 1_000)
                .await
                .unwrap()
        };
        assert!(matches!(
            ps.check_l0_backpressure(0),
            Err(StorageError::Backpressure { what: "log", .. })
        ));
        assert_eq!(engine.memory_load().1, 1_000);
        assert_eq!(engine.apply_queue_depth(), (1, 1));
        assert!(engine.wal_bytes() > 0);
        ps.apply_logged(0, wal_seq, &[(&ev, 1, 7)], 1_000)
            .await
            .unwrap();
        ps.check_l0_backpressure(0).unwrap();
        assert_eq!(engine.memory_load().1, 0);
        assert_eq!(engine.apply_queue_depth(), (0, 0));
        assert!(engine.memory_load().0 > 0);

        // Rows in L0 are bounded in bytes too, until a flush moves them to a segment.
        engine.set_byte_limits((1, usize::MAX), (usize::MAX, usize::MAX));
        assert!(matches!(
            ps.check_l0_backpressure(0),
            Err(StorageError::Backpressure { what: "l0", .. })
        ));
        ps.flush_all().await.unwrap();
        ps.check_l0_backpressure(0).unwrap();
        assert_eq!(engine.memory_load().0, 0);
        assert_eq!(engine.wal_bytes(), 0);
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

    #[tokio::test]
    async fn publish_sequence_compaction_before_gc_and_step6_recovery() {
        use crate::snapshot::restore_project_from_published;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = StorageEngine::new(
            StorageConfig {
                num_buckets: 2,
                l0_flush_row_threshold: 100,
                l0_flush_max_age: Duration::ZERO,
                compaction_min_segments: 2,
            },
            store.clone(),
            Arc::new(IdentityRouter::new(store.clone())),
        );
        engine.set_upload_interval(Duration::ZERO);
        let ps = engine.get_or_open_project("p_pub").await.unwrap();
        let id_graph = engine.identity_graph("p_pub", true).await.unwrap();
        let pstore = engine.profile_store("p_pub", true).await.unwrap();

        let make_ev = |did: &str, i: i64| {
            let ts = 1_790_000_000_000 + i * 1_000;
            WireEvent {
                event_uuid: WireEvent::deterministic_uuid(did, "s", "click", ts, i as u64),
                routing_seed: did.into(),
                distinct_id: did.into(),
                session_id: "s".into(),
                event_name: "click".into(),
                client_event_ts_ms: ts,
                client_sent_ts_ms: 0,
                props: vec![],
                replay_offset_kb: 0,
            }
        };

        // Flush 1: 2 users, 1 profile -> pub_0
        let u0 = id_graph.get_or_intern("u0", 0);
        let u1 = id_graph.get_or_intern("u1", 0);
        pstore
            .upsert(&crate::StoredUserProfile {
                project_id: "p_pub".into(),
                distinct_id: "u0".into(),
                canonical_uid: u0,
                props: BTreeMap::from([("plan".into(), serde_json::json!("pro"))]),
                total_revenue: 10.0,
                updated_at_ms: 1000,
            })
            .await
            .unwrap();
        ps.append_bucket_events(0, vec![(make_ev("u0", 1), u0), (make_ev("u1", 2), u1)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();
        assert_eq!(ps.last_published_seq(), Some(0));

        let pub0 = ps.latest_published_manifest().await.unwrap().unwrap();
        assert_eq!(pub0.publish_seq, 0);
        assert_eq!(pub0.high_water_mark, 2);
        assert!(pub0.snapshot_ref.profiles.is_some());

        // Flush 2: 1 more user -> pub_1
        let u2 = id_graph.get_or_intern("u2", 0);
        ps.append_bucket_events(0, vec![(make_ev("u2", 3), u2)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();
        assert_eq!(ps.last_published_seq(), Some(1));

        // Compaction merges the 2 segments in bucket 0 -> publishes pub_2 BEFORE run_gc() deletes old segments.
        let compacted = ps
            .compact_min_segments(engine.segment_reader(), 2)
            .await
            .unwrap();
        assert_eq!(compacted, 1);
        assert_eq!(ps.last_published_seq(), Some(2));
        let pub2 = ps.latest_published_manifest().await.unwrap().unwrap();
        assert_eq!(pub2.publish_seq, 2);
        assert_eq!(pub2.high_water_mark, 3);
        assert_eq!(pub2.partitions.len(), 1);
        assert_eq!(pub2.partitions[0].segments.len(), 1);
        let compacted_seg_path = pub2.partitions[0].segments[0].object_path.clone();
        assert!(store
            .head(&ObjectPath::from(compacted_seg_path.as_str()))
            .await
            .is_ok());

        // Simulate step-6 crash: pub_3 exists in S3 while local manifest has last_published_seq = Some(2).
        let mut crashed_pub3 = pub2.clone();
        crashed_pub3.publish_seq = 3;
        PublishedManifestStore::new("p_pub", store.clone())
            .publish_next(crashed_pub3)
            .await
            .unwrap();
        drop(ps);
        drop(engine);

        // Reopen: open_inner reconciles last_published_seq to Some(3), so next flush writes pub_4.
        let engine2 = StorageEngine::new(
            StorageConfig {
                num_buckets: 2,
                l0_flush_row_threshold: 100,
                l0_flush_max_age: Duration::ZERO,
                compaction_min_segments: 2,
            },
            store.clone(),
            Arc::new(IdentityRouter::new(store.clone())),
        );
        engine2.set_upload_interval(Duration::ZERO);
        let ps2 = engine2.get_or_open_project("p_pub").await.unwrap();
        assert_eq!(ps2.last_published_seq(), Some(3));
        let id2 = engine2.identity_graph("p_pub", true).await.unwrap();
        let u3 = id2.get_or_intern("u3", 0);
        ps2.append_bucket_events(0, vec![(make_ev("u3", 4), u3)])
            .await
            .unwrap();
        ps2.flush_bucket(0).await.unwrap();
        assert_eq!(ps2.last_published_seq(), Some(4));

        // Verify HWM violation error on restore if published HWM > identity_user_count.
        let latest_pub = ps2.latest_published_manifest().await.unwrap().unwrap();
        let mut bad_pub = latest_pub.clone();
        bad_pub.high_water_mark = 999;
        let restore_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Copy published/ into restore_store.
        {
            use futures::TryStreamExt;
            let metas: Vec<_> = store
                .list(Some(&ObjectPath::from("published/p_pub")))
                .try_collect()
                .await
                .unwrap();
            for m in metas {
                let b = store.get(&m.location).await.unwrap().bytes().await.unwrap();
                restore_store.put(&m.location, b.into()).await.unwrap();
            }
        }
        let err = restore_project_from_published("p_pub", &bad_pub, restore_store)
            .await
            .err()
            .expect("must fail on HighWaterMarkViolation");
        assert!(matches!(err, StorageError::HighWaterMarkViolation { .. }));
    }

    #[tokio::test]
    async fn run_gc_at_concurrent_with_flush_does_not_skip_publishing_flushed_segments() {
        use futures::TryStreamExt;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = StorageEngine::new(
            StorageConfig {
                num_buckets: 1,
                l0_flush_row_threshold: 100,
                l0_flush_max_age: Duration::from_secs(60),
                compaction_min_segments: 2,
            },
            store.clone(),
            Arc::new(IdentityRouter::new(store.clone())),
        );
        engine.set_upload_interval(Duration::from_secs(300));
        let ps = engine.get_or_open_project("p_gc_race").await.unwrap();
        let id_graph = engine.identity_graph("p_gc_race", true).await.unwrap();
        let u0 = id_graph.get_or_intern("u0", 0);

        let make_ev = |i: i64| {
            let ts = 1_790_000_000_000 + i * 1_000;
            WireEvent {
                event_uuid: WireEvent::deterministic_uuid("u0", "s", "click", ts, i as u64),
                routing_seed: "u0".into(),
                distinct_id: "u0".into(),
                session_id: "s".into(),
                event_name: "click".into(),
                client_event_ts_ms: ts,
                client_sent_ts_ms: 0,
                props: vec![],
                replay_offset_kb: 0,
            }
        };

        // Two flushes + publishes (pub_0, pub_1), then compact + publish (pub_2) so pending_s3_deletes is populated.
        ps.append_bucket_events(0, vec![(make_ev(1), u0)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();
        assert_eq!(ps.publish_if_dirty().await.unwrap().unwrap().publish_seq, 0);

        ps.append_bucket_events(0, vec![(make_ev(2), u0)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();
        assert_eq!(ps.publish_if_dirty().await.unwrap().unwrap().publish_seq, 1);

        assert_eq!(
            ps.compact_min_segments(engine.segment_reader(), 2)
                .await
                .unwrap(),
            1
        );
        let pub2 = ps.publish_if_dirty().await.unwrap().unwrap();
        assert_eq!(pub2.publish_seq, 2);

        let epoch_before = ps.snapshot().epoch();
        assert_eq!(*ps.last_published_epoch.lock(), Some(epoch_before));
        assert!(ps.publish_if_dirty().await.unwrap().is_none());

        let pending_paths: Vec<String> = ps
            .snapshot()
            .manifest
            .pending_s3_deletes
            .iter()
            .filter(|e| e.kind == PendingS3DeleteKind::Segment)
            .map(|e| e.object_path.clone())
            .collect();
        assert_eq!(pending_paths.len(), 2);

        // Append event 3 for the *existing* user u0 (identity & profiles unchanged: id_dirty == false, prof_dirty == false).
        ps.append_bucket_events(0, vec![(make_ev(3), u0)])
            .await
            .unwrap();

        let seg_count = || async {
            let metas: Vec<_> = store
                .list(Some(&ObjectPath::from("segments/p_gc_race")))
                .try_collect()
                .await
                .unwrap();
            metas.len()
        };
        let segs_before = seg_count().await;

        // Hold `manifest_commit_lock` to force the exact interleaving:
        // 1. `flush_bucket(0)` writes its new segment and parks first on `manifest_commit_lock`.
        // 2. `run_gc_at(far_future)` deletes superseded S3 segments and parks second on `manifest_commit_lock`.
        // 3. Releasing `manifest_commit_lock` lets `flush_bucket` commit E+1, then `run_gc_at` commit E+2.
        let mc_guard = ps.manifest_commit_lock.lock().await;

        let ps_flush = ps.clone();
        let flush_handle = tokio::spawn(async move { ps_flush.flush_bucket(0).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while seg_count().await <= segs_before {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("flush_bucket must write its segment before committing");
        tokio::time::sleep(Duration::from_millis(20)).await;

        let ps_gc = ps.clone();
        let far_future = Utc::now().timestamp_millis() + 86_400_000;
        let gc_handle = tokio::spawn(async move { ps_gc.run_gc_at(far_future).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut all_deleted = true;
                for p in &pending_paths {
                    if store.head(&ObjectPath::from(p.as_str())).await.is_ok() {
                        all_deleted = false;
                        break;
                    }
                }
                if all_deleted {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("run_gc_at must delete superseded S3 segments before committing");
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(mc_guard);
        assert_eq!(flush_handle.await.unwrap().unwrap().len(), 1);
        assert_eq!(gc_handle.await.unwrap().unwrap(), 2);

        assert_eq!(ps.snapshot().epoch(), epoch_before + 2);
        assert!(ps.snapshot().manifest.pending_s3_deletes.is_empty());
        assert_eq!(
            *ps.last_published_epoch.lock(),
            Some(epoch_before),
            "run_gc_at must NOT advance last_published_epoch over concurrent flush epoch E+1"
        );

        let pub3 = ps
            .publish_if_dirty()
            .await
            .unwrap()
            .expect("publish_if_dirty must still publish segments flushed in E+1");
        assert_eq!(pub3.publish_seq, 3);
        let published_rows: u64 = pub3
            .partitions
            .iter()
            .flat_map(|p| p.segments.iter())
            .map(|s| s.footer.row_count as u64)
            .sum();
        assert_eq!(published_rows, 3);
    }

    #[tokio::test]
    async fn restore_published_snapshots_retry_recomputes_both_directions() {
        use bytes::Bytes;
        use futures::stream::BoxStream;
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
            PutResult, Result as OsResult,
        };
        use std::fmt::{Display, Formatter};
        use std::ops::Range;

        #[derive(Debug)]
        struct GcRaceStore {
            inner: Arc<InMemory>,
            hook: Mutex<Option<(String, String, Vec<u8>)>>,
        }

        impl Display for GcRaceStore {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "GcRaceStore")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for GcRaceStore {
            async fn put_opts(
                &self,
                l: &ObjectPath,
                p: PutPayload,
                o: PutOptions,
            ) -> OsResult<PutResult> {
                self.inner.put_opts(l, p, o).await
            }
            async fn put_multipart_opts(
                &self,
                l: &ObjectPath,
                o: PutMultipartOptions,
            ) -> OsResult<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(l, o).await
            }
            async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
                let triggered = {
                    let mut g = self.hook.lock();
                    if g.as_ref()
                        .is_some_and(|(target, _, _)| target == l.as_ref())
                    {
                        g.take()
                    } else {
                        None
                    }
                };
                if let Some((target, pub2_path, pub2_bytes)) = triggered {
                    self.inner
                        .put(&ObjectPath::from(pub2_path), PutPayload::from(pub2_bytes))
                        .await?;
                    return Err(object_store::Error::NotFound {
                        path: target,
                        source: "simulated S3 GC".into(),
                    });
                }
                self.inner.get_opts(l, o).await
            }
            async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
                self.inner.delete(l).await
            }
            fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
                self.inner.list(p)
            }
            async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
                self.inner.list_with_delimiter(p).await
            }
            async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
                self.inner.copy(from, to).await
            }
            async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
                self.inner.copy_if_not_exists(from, to).await
            }
            async fn get_range(&self, l: &ObjectPath, r: Range<u64>) -> OsResult<Bytes> {
                self.inner.get_range(l, r).await
            }
        }

        let make_ev = |seq: u64, did: &str| WireEvent {
            event_uuid: WireEvent::deterministic_uuid(did, "s", "ev", 1_790_000_000_000, seq),
            routing_seed: did.into(),
            distinct_id: did.into(),
            session_id: "s".into(),
            event_name: "ev".into(),
            client_event_ts_ms: 1_790_000_000_000 + seq as i64 * 1000,
            client_sent_ts_ms: 1_790_000_000_000,
            props: vec![],
            replay_offset_kb: 0,
        };
        let cfg = StorageConfig {
            num_buckets: 1,
            ..StorageConfig::default()
        };

        // Direction A: initial_pub (pub_1) has `profiles: None` matching Query's pub_0,
        // `id_1` hits NotFound on attempt 0 -> retry loads `pub_2` which introduces `profiles: Some(prof_2)`.
        {
            let s3_inner = Arc::new(InMemory::new());
            let s3_hooked = Arc::new(GcRaceStore {
                inner: s3_inner.clone(),
                hook: Mutex::new(None),
            });
            let s3_dyn: Arc<dyn ObjectStore> = s3_hooked.clone();

            let writer_engine = StorageEngine::new(
                cfg.clone(),
                s3_dyn.clone(),
                Arc::new(IdentityRouter::new(s3_dyn.clone())),
            );
            let w_ps = writer_engine.get_or_open_project("p_dir_a").await.unwrap();
            let u0 = w_ps.id_graph().get_or_intern("u0", 0);
            w_ps.append_bucket_events(0, vec![(make_ev(1, "u0"), u0)])
                .await
                .unwrap();
            w_ps.flush_all().await.unwrap();
            let pub0 = w_ps.publish_if_dirty().await.unwrap().unwrap();
            assert!(pub0.snapshot_ref.profiles.is_none());

            // Query opens on pub_0 (profiles: None).
            let q_snap: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let q_engine = StorageEngine::new(
                cfg.clone(),
                s3_dyn.clone(),
                Arc::new(IdentityRouter::new_read_only(s3_dyn.clone())),
            );
            q_engine.enable_published_reader(q_snap);
            let q_ps = q_engine.get_or_open_project("p_dir_a").await.unwrap();
            assert_eq!(q_ps.last_published_seq(), Some(0));

            // Writer publishes pub_1 (new identity u1, still profiles: None).
            let u1 = w_ps.id_graph().get_or_intern("u1", 0);
            w_ps.append_bucket_events(0, vec![(make_ev(2, "u1"), u1)])
                .await
                .unwrap();
            w_ps.flush_all().await.unwrap();
            let pub1 = w_ps.publish_if_dirty().await.unwrap().unwrap();
            assert!(pub1.snapshot_ref.profiles.is_none());

            // Writer publishes pub_2 (profiles: Some(prof_2) with "u_retry"), then hide pub_2's
            // manifest temporarily until pub_1's identity snapshot is requested.
            let pstore = writer_engine.profile_store("p_dir_a", true).await.unwrap();
            pstore
                .upsert(&crate::StoredUserProfile {
                    project_id: "p_dir_a".into(),
                    distinct_id: "u_retry".into(),
                    canonical_uid: u1,
                    props: BTreeMap::from([("plan".into(), serde_json::json!("gold"))]),
                    total_revenue: 10.0,
                    updated_at_ms: 1_790_000_005_000,
                })
                .await
                .unwrap();
            let _pub2 = w_ps.publish_if_dirty().await.unwrap().unwrap();
            let pub2_path = "published/p_dir_a/pub_000000000002.json".to_string();
            let pub2_bytes = s3_inner
                .get(&ObjectPath::from(pub2_path.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            s3_inner
                .delete(&ObjectPath::from(pub2_path.as_str()))
                .await
                .unwrap();

            *s3_hooked.hook.lock() = Some((
                pub1.snapshot_ref.identity.object_path.clone(),
                pub2_path,
                pub2_bytes,
            ));

            assert!(q_ps.refresh_manifest().await.unwrap());
            assert_eq!(q_ps.last_published_seq(), Some(2));
            let q_pstore = q_engine.profile_store("p_dir_a", false).await.unwrap();
            let loaded = q_pstore.get_by_distinct_id("u_retry").await.unwrap();
            assert!(
                loaded.is_some(),
                "retry against pub_2 must recompute restore_profiles=true and load u_retry"
            );
        }

        // Direction B (symmetric): initial_pub (pub_1) reuses pub_0's identity snapshot
        // (`restore_identity=false`), `prof_1` hits NotFound on attempt 0 -> retry loads `pub_2`
        // which updates identity with `"u_new_id"`.
        {
            let s3_inner = Arc::new(InMemory::new());
            let s3_hooked = Arc::new(GcRaceStore {
                inner: s3_inner.clone(),
                hook: Mutex::new(None),
            });
            let s3_dyn: Arc<dyn ObjectStore> = s3_hooked.clone();

            let writer_engine = StorageEngine::new(
                cfg.clone(),
                s3_dyn.clone(),
                Arc::new(IdentityRouter::new(s3_dyn.clone())),
            );
            let w_ps = writer_engine.get_or_open_project("p_dir_b").await.unwrap();
            let u0 = w_ps.id_graph().get_or_intern("u0", 0);
            let w_pstore = writer_engine.profile_store("p_dir_b", true).await.unwrap();
            w_pstore
                .upsert(&crate::StoredUserProfile {
                    project_id: "p_dir_b".into(),
                    distinct_id: "u0".into(),
                    canonical_uid: u0,
                    props: BTreeMap::from([("tier".into(), serde_json::json!("p0"))]),
                    total_revenue: 1.0,
                    updated_at_ms: 1_790_000_001_000,
                })
                .await
                .unwrap();
            w_ps.append_bucket_events(0, vec![(make_ev(1, "u0"), u0)])
                .await
                .unwrap();
            w_ps.flush_all().await.unwrap();
            let pub0 = w_ps.publish_if_dirty().await.unwrap().unwrap();

            // Query opens on pub_0.
            let q_snap: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let q_engine = StorageEngine::new(
                cfg.clone(),
                s3_dyn.clone(),
                Arc::new(IdentityRouter::new_read_only(s3_dyn.clone())),
            );
            q_engine.enable_published_reader(q_snap);
            let q_ps = q_engine.get_or_open_project("p_dir_b").await.unwrap();
            assert_eq!(q_ps.last_published_seq(), Some(0));

            // Writer publishes pub_1 updating ONLY profile (same identity snapshot as pub_0!).
            w_pstore
                .upsert(&crate::StoredUserProfile {
                    project_id: "p_dir_b".into(),
                    distinct_id: "u0".into(),
                    canonical_uid: u0,
                    props: BTreeMap::from([("tier".into(), serde_json::json!("p1"))]),
                    total_revenue: 2.0,
                    updated_at_ms: 1_790_000_002_000,
                })
                .await
                .unwrap();
            let pub1 = w_ps.publish_if_dirty().await.unwrap().unwrap();
            assert_eq!(
                pub1.snapshot_ref.identity.object_path,
                pub0.snapshot_ref.identity.object_path
            );

            // Writer publishes pub_2 with a brand-new user "u_new_id" in identity + profile update.
            let u_new = w_ps.id_graph().get_or_intern("u_new_id", 0);
            w_pstore
                .upsert(&crate::StoredUserProfile {
                    project_id: "p_dir_b".into(),
                    distinct_id: "u_new_id".into(),
                    canonical_uid: u_new,
                    props: BTreeMap::from([("tier".into(), serde_json::json!("p2"))]),
                    total_revenue: 3.0,
                    updated_at_ms: 1_790_000_003_000,
                })
                .await
                .unwrap();
            w_ps.append_bucket_events(0, vec![(make_ev(2, "u_new_id"), u_new)])
                .await
                .unwrap();
            w_ps.flush_all().await.unwrap();
            let _pub2 = w_ps.publish_if_dirty().await.unwrap().unwrap();
            let pub2_path = "published/p_dir_b/pub_000000000002.json".to_string();
            let pub2_bytes = s3_inner
                .get(&ObjectPath::from(pub2_path.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec();
            s3_inner
                .delete(&ObjectPath::from(pub2_path.as_str()))
                .await
                .unwrap();

            *s3_hooked.hook.lock() = Some((
                pub1.snapshot_ref
                    .profiles
                    .as_ref()
                    .unwrap()
                    .object_path
                    .clone(),
                pub2_path,
                pub2_bytes,
            ));

            assert!(q_ps.refresh_manifest().await.unwrap());
            assert_eq!(q_ps.last_published_seq(), Some(2));
            assert!(
                q_ps.id_graph().lookup_raw("u_new_id").is_some(),
                "retry against pub_2 must recompute restore_identity=true and load u_new_id"
            );
        }
    }
}

#[cfg(test)]
mod t3_tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutResult, Result as OsResult,
    };
    use std::fmt::{Display, Formatter};
    use std::ops::Range;

    #[derive(Debug, Default)]
    struct CountingMemStore {
        inner: InMemory,
        calls: Mutex<BTreeMap<(String, &'static str), u64>>,
        fail_get_prefix: Mutex<Option<String>>,
        fail_put_substring: Mutex<Option<String>>,
    }

    impl CountingMemStore {
        fn count(&self, op: &'static str, p: Option<&ObjectPath>) {
            let prefix = p
                .and_then(|p| p.parts().next())
                .map_or("(root)".to_string(), |s| s.as_ref().to_string());
            *self.calls.lock().entry((prefix, op)).or_default() += 1;
        }

        fn take(&self) -> BTreeMap<(String, &'static str), u64> {
            std::mem::take(&mut *self.calls.lock())
        }

        fn sum_prefix(
            calls: &BTreeMap<(String, &'static str), u64>,
            prefix: &str,
            op: &str,
        ) -> u64 {
            calls
                .iter()
                .filter(|((p, o), _)| p == prefix && *o == op)
                .map(|(_, n)| *n)
                .sum()
        }
    }

    impl Display for CountingMemStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingMemStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingMemStore {
        async fn put_opts(
            &self,
            l: &ObjectPath,
            p: PutPayload,
            o: PutOptions,
        ) -> OsResult<PutResult> {
            self.count("PUT", Some(l));
            if let Some(ref sub) = *self.fail_put_substring.lock() {
                if l.as_ref().contains(sub.as_str()) {
                    return Err(object_store::Error::Generic {
                        store: "CountingMemStore",
                        source: Box::new(std::io::Error::other(format!(
                            "injected PUT failure on {l}"
                        ))),
                    });
                }
            }
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjectPath,
            o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.count("PUT", Some(l));
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
            self.count("GET", Some(l));
            if let Some(ref pref) = *self.fail_get_prefix.lock() {
                if l.as_ref().starts_with(pref.as_str()) {
                    return Err(object_store::Error::Generic {
                        store: "CountingMemStore",
                        source: Box::new(std::io::Error::other("injected S3 read failure")),
                    });
                }
            }
            self.inner.get_opts(l, o).await
        }
        async fn get_range(&self, l: &ObjectPath, r: Range<u64>) -> OsResult<Bytes> {
            self.count("GET", Some(l));
            if let Some(ref pref) = *self.fail_get_prefix.lock() {
                if l.as_ref().starts_with(pref.as_str()) {
                    return Err(object_store::Error::Generic {
                        store: "CountingMemStore",
                        source: Box::new(std::io::Error::other("injected S3 get_range failure")),
                    });
                }
            }
            self.inner.get_range(l, r).await
        }
        async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
            self.count("DELETE", Some(l));
            self.inner.delete(l).await
        }
        fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.count("LIST", p);
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
            self.count("LIST", p);
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.count("PUT", Some(to));
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.count("PUT", Some(to));
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn mk_event(did: &str, ts_ms: i64, seq: u64) -> WireEvent {
        WireEvent {
            event_uuid: WireEvent::deterministic_uuid(did, "s1", "click", ts_ms, seq),
            routing_seed: did.into(),
            distinct_id: did.into(),
            session_id: "s1".into(),
            event_name: "click".into(),
            client_event_ts_ms: ts_ms,
            client_sent_ts_ms: 0,
            props: vec![("k".into(), PropValue::Str(format!("val_{seq}")))],
            replay_offset_kb: 0,
        }
    }

    #[tokio::test]
    async fn upload_64mib_merge_cap_splits_batches_and_allows_single_oversize_segment() {
        assert_eq!(DEFAULT_UPLOAD_MERGE_MAX_INPUT_BYTES, 64 * 1024 * 1024);
        let store = Arc::new(CountingMemStore::default());
        let dyn_store: Arc<dyn ObjectStore> = store.clone();
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };
        let engine = StorageEngine::new(
            cfg,
            dyn_store.clone(),
            Arc::new(IdentityRouter::new(dyn_store)),
        );
        engine.set_local_segment_tier(true);
        engine.set_upload_interval(Duration::from_secs(300));

        let ps = engine.get_or_open_project("p_cap").await.unwrap();
        let ig = engine.identity_graph("p_cap", true).await.unwrap();
        let uid = ig.get_or_intern("u0", 0);

        let now_ms = Utc::now().timestamp_millis();
        let mut sizes = Vec::new();
        for w in 0..3u64 {
            let mut batch = Vec::new();
            for i in 0..40u64 {
                batch.push((
                    mk_event("u0", now_ms + (w * 100 + i) as i64, w * 100 + i),
                    uid,
                ));
            }
            ps.append_bucket_events(0, batch).await.unwrap();
            let segs = ps.flush_bucket(0).await.unwrap();
            assert_eq!(segs.len(), 1);
            assert!(segs[0].object_path.starts_with("hot/segments/"));
            sizes.push(segs[0].file_size);
        }

        // Cap just above one segment's size so 3 segments split into at least 2 S3 output segments.
        let cap = sizes.iter().copied().max().unwrap() + 1;
        engine.set_upload_merge_max_input_bytes(cap);

        store.take();
        let pub0 = ps
            .upload_if_dirty_at(now_ms, false)
            .await
            .unwrap()
            .expect("must upload today's segments");
        let calls = store.take();
        assert_eq!(
            CountingMemStore::sum_prefix(&calls, "segments", "GET"),
            0,
            "Step 4 must never read existing S3 segments/"
        );
        assert_eq!(CountingMemStore::sum_prefix(&calls, "segments", "LIST"), 0);
        let s3_segs: Vec<_> = pub0
            .partitions
            .iter()
            .flat_map(|p| p.segments.iter())
            .collect();
        assert!(
            s3_segs.len() >= 2,
            "expected >=2 split S3 segments under cap={cap}, got {} (sizes={sizes:?})",
            s3_segs.len()
        );
        assert_eq!(ps.snapshot().manifest.total_l1_rows(), 120);
        assert_eq!(ps.hot_segment_bytes(), 0);

        // Single local segment larger than cap must still make forward progress as 1 S3 segment.
        let mut big_batch = Vec::new();
        for i in 0..60u64 {
            big_batch.push((mk_event("u0", now_ms + 10_000 + i as i64, 10_000 + i), uid));
        }
        ps.append_bucket_events(0, big_batch).await.unwrap();
        let big_seg = ps.flush_bucket(0).await.unwrap().pop().unwrap();
        assert!(big_seg.file_size > 256);
        engine.set_upload_merge_max_input_bytes(256);
        let pub1 = ps
            .upload_if_dirty_at(now_ms + 1_000, false)
            .await
            .unwrap()
            .expect("oversize single segment must upload");
        assert_eq!(pub1.publish_seq, 1);
        assert_eq!(ps.snapshot().manifest.total_l1_rows(), 180);
    }

    #[tokio::test]
    async fn closed_date_1h_timer_4mib_threshold_empty_erasure_and_zero_idle_s3() {
        let store = Arc::new(CountingMemStore::default());
        let dyn_store: Arc<dyn ObjectStore> = store.clone();
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };
        let engine = StorageEngine::new(
            cfg,
            dyn_store.clone(),
            Arc::new(IdentityRouter::new(dyn_store)),
        );
        engine.set_local_segment_tier(true);
        engine.set_upload_interval(Duration::from_secs(300));

        let ps = engine.get_or_open_project("p_closed").await.unwrap();
        let ig = engine.identity_graph("p_closed", true).await.unwrap();
        let uid = ig.get_or_intern("u0", 0);

        // Publish initial pub_0 with today's event so identity snapshot is already clean.
        let now_ms = Utc::now().timestamp_millis();
        ps.append_bucket_events(0, vec![(mk_event("u0", now_ms, 1), uid)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();
        let pub0 = ps.upload_if_dirty_at(now_ms, false).await.unwrap().unwrap();
        assert_eq!(pub0.publish_seq, 0);

        // Idle upload makes 0 S3 calls.
        store.take();
        assert!(ps
            .upload_if_dirty_at(now_ms + 10_000, false)
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.take().values().sum::<u64>(), 0);

        // Flush a late event for a closed date (`now_ms - 2 days`) for existing user `u0`.
        let old_ts = now_ms - 2 * 86_400_000;
        ps.append_bucket_events(0, vec![(mk_event("u0", old_ts, 2), uid)])
            .await
            .unwrap();
        let closed_seg = ps.flush_bucket(0).await.unwrap().pop().unwrap();
        assert!(closed_seg.object_path.starts_with("hot/segments/"));
        let t_flush = Utc::now().timestamp_millis();

        // Before 1h (and <= 4 MiB): deferred closed-date segment MUST make 0 S3 calls and NOT dirty S3 state!
        store.take();
        assert!(ps
            .upload_if_dirty_at(t_flush + 3_599_000, false)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            store.take().values().sum::<u64>(),
            0,
            "deferred closed-date segment must not trigger S3 calls before 1h"
        );

        // At +1h (3_600_000 ms): uploads cleanly.
        let pub1 = ps
            .upload_if_dirty_at(t_flush + 3_600_000, false)
            .await
            .unwrap()
            .expect("1h timer must upload closed-date segment");
        assert_eq!(pub1.publish_seq, 1);
        assert_eq!(ps.hot_segment_bytes(), 0);

        // Now test > closed_date_max_bytes threshold AND empty-after-erasure batch handling.
        engine.set_closed_date_upload_policy(Duration::from_secs(3600), 100);
        ps.append_bucket_events(0, vec![(mk_event("u0", old_ts + 1_000, 3), uid)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();

        // Erase uid up to old_ts + 2000 so the frozen batch produces 0 rows!
        let mut erased = ErasedUids::default();
        erased.insert(uid, old_ts + 2_000);
        engine.set_erased("p_closed", erased);

        let pub2 = ps
            .upload_if_dirty_at(now_ms + 3_601_000, false)
            .await
            .unwrap()
            .expect("closed date > max_bytes must upload immediately");
        assert_eq!(pub2.publish_seq, 2);
        assert_eq!(ps.hot_segment_bytes(), 0);
        // The fully-erased closed-date segment was dropped without writing an empty S3 segment.
        assert_eq!(
            ps.snapshot()
                .manifest
                .active_segments
                .get(&(date_from_ms(old_ts), 0))
                .map_or(0, |v| v.len()),
            1
        );
    }

    #[tokio::test]
    async fn pinned_local_segment_defers_delete_and_dedup_seeding_propagates_s3_errors() {
        let store = Arc::new(CountingMemStore::default());
        let dyn_store: Arc<dyn ObjectStore> = store.clone();
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };
        let engine = StorageEngine::new(
            cfg.clone(),
            dyn_store.clone(),
            Arc::new(IdentityRouter::new(dyn_store.clone())),
        );
        engine.set_local_segment_tier(true);
        engine.set_upload_interval(Duration::from_secs(300));

        let ps = engine.get_or_open_project("p_pin").await.unwrap();
        let ig = engine.identity_graph("p_pin", true).await.unwrap();
        let uid = ig.get_or_intern("u0", 0);

        let now_ms = Utc::now().timestamp_millis();
        let ev1 = mk_event("u0", now_ms, 10);
        let fp1 = event_uuid_fingerprint(&ev1.event_uuid);
        ps.append_bucket_events(0, vec![(ev1, uid)]).await.unwrap();
        let seg1 = ps.flush_bucket(0).await.unwrap().pop().unwrap();
        assert!(seg1.object_path.starts_with("hot/segments/"));

        // Hold StorageSnapshot pinning seg1 across upload cycle.
        let pinned_snap = ps.snapshot();
        let pub0 = ps.upload_if_dirty_at(now_ms, false).await.unwrap().unwrap();
        assert!(
            pub0.partitions
                .iter()
                .flat_map(|p| p.segments.iter())
                .all(|s| !s.object_path.starts_with("hot/")),
            "PublishedManifest must never include hot/ paths"
        );
        assert!(
            dyn_store
                .head(&ObjectPath::from(seg1.object_path.as_str()))
                .await
                .is_ok(),
            "pinned local segment must stay on disk while StorageSnapshot is held"
        );
        assert!(ps.hot_segment_bytes() > 0);

        // Drop StorageSnapshot -> drain deletes local segment and zeroes hot_segment_bytes.
        drop(pinned_snap);
        assert_eq!(ps.drain_pending_local_deletes().await, 1);
        assert!(dyn_store
            .head(&ObjectPath::from(seg1.object_path.as_str()))
            .await
            .is_err());
        assert_eq!(ps.hot_segment_bytes(), 0);
        assert_eq!(engine.hot_segment_bytes(), 0);

        ig.close().await.unwrap();
        drop(ps);
        drop(engine);

        // Inject S3 read failure on `segments/` during reopen dedup seeding (`engine.rs:815` fix).
        *store.fail_get_prefix.lock() = Some("segments/".to_string());
        let engine_fail = StorageEngine::new(
            cfg.clone(),
            dyn_store.clone(),
            Arc::new(IdentityRouter::new(dyn_store.clone())),
        );
        let err = match engine_fail.get_or_open_project("p_pin").await {
            Ok(_) => panic!("dedup seeding must propagate S3 segment read error"),
            Err(e) => e,
        };
        assert!(matches!(err, StorageError::ObjectStore(_)), "{err}");
        assert!(
            engine_fail.get_project("p_pin").is_none(),
            "failed open must not cache project with empty dedup"
        );

        // Clear fault -> reopen succeeds and recovers fp1.
        *store.fail_get_prefix.lock() = None;
        let ps_ok = engine_fail.get_or_open_project("p_pin").await.unwrap();
        let recovered = ps_ok.take_recovered_uuid_fps();
        assert!(
            recovered.contains(&(0, fp1)),
            "reopen after S3 recovery must seed fp1 into dedup"
        );
    }

    #[tokio::test]
    async fn hot_segment_byte_bounds_backpressure_and_drop_accounting() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };
        let engine = StorageEngine::new(cfg, store.clone(), Arc::new(IdentityRouter::new(store)));
        engine.set_local_segment_tier(true);
        engine.set_upload_interval(Duration::from_secs(300));
        engine.set_hot_segment_byte_limits(100, 10_000_000);

        let ps_a = engine.get_or_open_project("p_a").await.unwrap();
        let ps_b = engine.get_or_open_project("p_b").await.unwrap();
        let ig_a = engine.identity_graph("p_a", true).await.unwrap();
        let ua = ig_a.get_or_intern("ua", 0);
        let now_ms = Utc::now().timestamp_millis();

        ps_a.append_bucket_events(0, vec![(mk_event("ua", now_ms, 1), ua)])
            .await
            .unwrap();
        let seg_a = ps_a.flush_bucket(0).await.unwrap().pop().unwrap();
        assert_eq!(ps_a.hot_segment_bytes(), seg_a.file_size);
        assert_eq!(engine.hot_segment_bytes(), seg_a.file_size);
        assert!(seg_a.file_size >= 200);

        // Project A exceeds 2x (200 bytes) -> HotSegmentBackpressure; Project B stays Ok.
        assert!(matches!(
            ps_a.check_hot_segment_backpressure(),
            Err(StorageError::HotSegmentBackpressure { .. })
        ));
        assert!(ps_b.check_hot_segment_backpressure().is_ok());

        // Dropping engine / ProjectStorage subtracts remaining bytes cleanly.
        let bytes_before = engine.hot_segment_bytes();
        assert!(bytes_before > 0);
        engine.projects.remove("p_a");
        drop(ps_a);
        assert_eq!(engine.hot_segment_bytes(), 0);
    }

    #[tokio::test]
    async fn live_step5_to_step6_transient_failure_recovery_without_restart() {
        let store = Arc::new(CountingMemStore::default());
        let dyn_store: Arc<dyn ObjectStore> = store.clone();
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };
        let engine = StorageEngine::new(
            cfg,
            dyn_store.clone(),
            Arc::new(IdentityRouter::new(dyn_store.clone())),
        );
        engine.set_local_segment_tier(true);
        engine.set_upload_interval(Duration::from_secs(300));
        engine.set_hot_segment_byte_limits(100, 100_000_000);

        let pid = "p_live_step6";
        let ps = engine.get_or_open_project(pid).await.unwrap();
        let ig = engine.identity_graph(pid, true).await.unwrap();
        let uid = ig.get_or_intern("u0", 0);
        let now_ms = Utc::now().timestamp_millis();

        ps.append_bucket_events(0, vec![(mk_event("u0", now_ms, 1), uid)])
            .await
            .unwrap();
        let seg = ps.flush_bucket(0).await.unwrap().pop().unwrap();
        assert!(seg.object_path.starts_with("hot/segments/"));
        assert!(seg.file_size >= 200);
        assert!(matches!(
            ps.check_hot_segment_backpressure(),
            Err(StorageError::HotSegmentBackpressure { .. })
        ));

        // Fail Step 6 (`published/{pid}/pub_000000000000.json` PUT) after Step 5 local manifest commit.
        *store.fail_put_substring.lock() = Some("/pub_000000000000.json".to_string());
        let err = ps
            .upload_if_dirty_at(now_ms, false)
            .await
            .expect_err("Step 6 PUT failure must surface error");
        assert!(matches!(err, StorageError::ObjectStore(_)), "{err}");
        assert_eq!(ps.last_published_seq(), None);

        // Recover S3 WITHOUT restarting ProjectStorage / StorageEngine and retry upload_if_dirty_at.
        *store.fail_put_substring.lock() = None;
        let pub0 = ps
            .upload_if_dirty_at(now_ms + 5_000, false)
            .await
            .unwrap()
            .expect("retry after S3 recovery must publish pub_0");
        assert_eq!(pub0.publish_seq, 0);
        assert_eq!(ps.last_published_seq(), Some(0));
        assert_eq!(ps.hot_segment_bytes(), 0);
        assert_eq!(engine.hot_segment_bytes(), 0);
        assert!(ps.check_hot_segment_backpressure().is_ok());
        let leftovers: Vec<_> = dyn_store
            .list(Some(&ObjectPath::from(format!("hot/segments/{pid}"))))
            .try_collect()
            .await
            .unwrap();
        assert!(leftovers.is_empty(), "leftover hot/segments: {leftovers:?}");

        // Also test post-Step-6 local manifest commit failure followed by live retry (no restart).
        ps.append_bucket_events(0, vec![(mk_event("u0", now_ms + 6_000, 2), uid)])
            .await
            .unwrap();
        let seg2 = ps.flush_bucket(0).await.unwrap().pop().unwrap();
        assert!(seg2.file_size >= 200);

        let cur_epoch = ps.snapshot().manifest.epoch;
        let post_step6_epoch = cur_epoch + 2; // Step 5 commits cur_epoch+1; post_step6 commits cur_epoch+2.
        *store.fail_put_substring.lock() = Some(format!("epoch_{post_step6_epoch:012}.json"));
        let err2 = ps
            .upload_if_dirty_at(now_ms + 10_000, false)
            .await
            .expect_err("post-Step-6 manifest commit failure must surface error");
        assert!(matches!(err2, StorageError::ObjectStore(_)), "{err2}");
        assert_eq!(ps.last_published_seq(), Some(1));

        *store.fail_put_substring.lock() = None;
        let _ = ps.upload_if_dirty_at(now_ms + 15_000, false).await.unwrap();
        assert_eq!(ps.hot_segment_bytes(), 0);
        assert_eq!(engine.hot_segment_bytes(), 0);
        assert!(ps.check_hot_segment_backpressure().is_ok());
        let leftovers2: Vec<_> = dyn_store
            .list(Some(&ObjectPath::from(format!("hot/segments/{pid}"))))
            .try_collect()
            .await
            .unwrap();
        assert!(
            leftovers2.is_empty(),
            "leftover hot/segments: {leftovers2:?}"
        );
    }
}
