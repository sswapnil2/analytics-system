use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::NaiveDate;
use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectMeta, ObjectStore, PutMode, PutOptions, PutPayload};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::dictionary::ProjectDictSnapshot;
use crate::error::{Result, StorageError};
use crate::mem_bucket::StoredRow;
use crate::segment::SegmentMeta;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionEntry {
    pub date: NaiveDate,
    pub bucket_id: u8,
    pub segments: Vec<SegmentMeta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingS3DeleteKind {
    Segment,
    IdentitySnapshot,
    ProfileSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingS3Delete {
    pub object_path: String,
    pub kind: PendingS3DeleteKind,
    /// First `publish_seq` that no longer references `object_path`.
    pub superseding_pub_seq: u64,
    /// UTC ms (`PublishedManifest.published_at_ms`) when `superseding_pub_seq` landed in S3.
    #[serde(default)]
    pub superseded_at_ms: Option<i64>,
    /// `superseded_at_ms + 2 * publish_interval_ms` (stamped once `superseding_pub_seq` is published).
    #[serde(default)]
    pub eligible_after_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestWire {
    epoch: u64,
    #[serde(default)]
    fenced_wal_epoch: u64,
    #[serde(default)]
    writer_generation: u64,
    #[serde(default)]
    last_published_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pending_s3_deletes: Vec<PendingS3Delete>,
    partitions: Vec<PartitionEntry>,
    committed_wal_seq: BTreeMap<u8, u64>,
    committed_l0_seq: BTreeMap<u8, u64>,
    dict_snapshot: ProjectDictSnapshot,
}

/// Durable MVCC Manifest for a project's immutable L1 Vortex segments, WAL checkpoints, and dictionaries.
#[derive(Debug, Clone, Default)]
pub struct Manifest {
    pub epoch: u64,
    pub fenced_wal_epoch: u64,
    /// Generation of the writer lease held by the node that last opened this project for
    /// writing (0 = never leased). A writer whose own generation is older must stop committing.
    pub writer_generation: u64,
    /// Dense `publish_seq` of the newest `PublishedManifest` emitted for this project (if any).
    pub last_published_seq: Option<u64>,
    /// Superseded S3 segments and snapshot blobs awaiting the 2-publish-interval grace period.
    pub pending_s3_deletes: Vec<PendingS3Delete>,
    pub active_segments: HashMap<(NaiveDate, u8), Vec<SegmentMeta>>,
    pub committed_wal_seq: BTreeMap<u8, u64>,
    pub committed_l0_seq: BTreeMap<u8, u64>,
    pub dict_snapshot: ProjectDictSnapshot,
}

impl Manifest {
    pub fn total_segments(&self) -> usize {
        self.active_segments.values().map(|v| v.len()).sum()
    }

    pub fn total_l1_rows(&self) -> u64 {
        self.active_segments
            .values()
            .flat_map(|v| v.iter())
            .map(|m| m.footer.row_count as u64)
            .sum()
    }

    pub fn partitions_needing_compaction(&self, min_segments: usize) -> Vec<(NaiveDate, u8)> {
        let mut out: Vec<(NaiveDate, u8)> = self
            .active_segments
            .iter()
            .filter(|(_, segs)| segs.len() >= min_segments)
            .map(|(&k, _)| k)
            .collect();
        out.sort_unstable();
        out
    }

    pub fn to_sorted_partitions(&self) -> Vec<PartitionEntry> {
        let mut partitions: Vec<PartitionEntry> = self
            .active_segments
            .iter()
            .filter(|(_, segs)| !segs.is_empty())
            .map(|(&(date, bucket_id), segs)| PartitionEntry {
                date,
                bucket_id,
                segments: segs.clone(),
            })
            .collect();
        partitions.sort_by_key(|p| (p.date, p.bucket_id));
        partitions
    }

    fn to_wire(&self) -> ManifestWire {
        ManifestWire {
            epoch: self.epoch,
            fenced_wal_epoch: self.fenced_wal_epoch,
            writer_generation: self.writer_generation,
            last_published_seq: self.last_published_seq,
            pending_s3_deletes: self.pending_s3_deletes.clone(),
            partitions: self.to_sorted_partitions(),
            committed_wal_seq: self.committed_wal_seq.clone(),
            committed_l0_seq: self.committed_l0_seq.clone(),
            dict_snapshot: self.dict_snapshot.clone(),
        }
    }

    fn from_wire(w: ManifestWire) -> Self {
        let mut active_segments = HashMap::with_capacity(w.partitions.len());
        for p in w.partitions {
            active_segments.insert((p.date, p.bucket_id), p.segments);
        }
        Self {
            epoch: w.epoch,
            fenced_wal_epoch: w.fenced_wal_epoch,
            writer_generation: w.writer_generation,
            last_published_seq: w.last_published_seq,
            pending_s3_deletes: w.pending_s3_deletes,
            active_segments,
            committed_wal_seq: w.committed_wal_seq,
            committed_l0_seq: w.committed_l0_seq,
            dict_snapshot: w.dict_snapshot,
        }
    }
}

/// Number of newest manifest epochs always kept on disk.
pub const MANIFEST_RETAIN_EPOCHS: u64 = 16;
/// Minimum age before an epoch outside the retained window may be deleted, covering a reader that
/// listed the prefix and is about to fetch what was then the newest epoch.
pub const MANIFEST_GC_GRACE: Duration = Duration::from_secs(300);

/// Atomic MVCC Manifest store backed by conditional `PutMode::Create` writes on `ObjectStore`.
///
/// Old epoch files are never read back: `load_latest` (startup recovery, CAS retry, query-node refresh)
/// only fetches the newest epoch, and in-flight `StorageSnapshot`s hold their `Manifest` in memory.
/// So everything outside the newest `retain_epochs` (and older than `gc_grace`) is garbage.
pub struct ManifestStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
    retain_epochs: u64,
    gc_grace: Duration,
}

impl ManifestStore {
    pub fn new(project_id: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            project_id: project_id.into(),
            object_store,
            retain_epochs: MANIFEST_RETAIN_EPOCHS,
            gc_grace: MANIFEST_GC_GRACE,
        }
    }

    pub fn with_retention(mut self, retain_epochs: u64, gc_grace: Duration) -> Self {
        self.retain_epochs = retain_epochs.max(1);
        self.gc_grace = gc_grace;
        self
    }

    fn prefix(&self) -> ObjectPath {
        ObjectPath::from(format!("manifests/{}/", self.project_id))
    }

    /// Lists `(epoch, meta)` for every manifest file, oldest first.
    async fn list_epochs(&self) -> Result<Vec<(u64, ObjectMeta)>> {
        let metas: Vec<_> = self
            .object_store
            .list(Some(&self.prefix()))
            .try_collect()
            .await?;
        let mut out: Vec<(u64, ObjectMeta)> = metas
            .into_iter()
            .filter_map(|m| parse_epoch_from_path(m.location.as_ref()).map(|ep| (ep, m)))
            .collect();
        out.sort_unstable_by_key(|(ep, _)| *ep);
        Ok(out)
    }

    /// Deletes manifest epochs outside the newest `retain_epochs` once older than `gc_grace`.
    ///
    /// Deletes oldest-first and stops at the first epoch it keeps or fails to delete, so the deleted
    /// epochs always form a prefix; `commit_next` relies on that to detect stale writers.
    pub async fn gc_old_epochs(&self) -> Result<usize> {
        let epochs = self.list_epochs().await?;
        let Some(latest) = epochs.last().map(|(ep, _)| *ep) else {
            return Ok(0);
        };
        let grace = chrono::Duration::from_std(self.gc_grace).unwrap_or(chrono::Duration::MAX);
        let now = chrono::Utc::now();
        let mut deleted = 0usize;
        for (ep, meta) in epochs {
            if ep + self.retain_epochs > latest || now - meta.last_modified < grace {
                break;
            }
            self.object_store.delete(&meta.location).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    fn epoch_path(&self, epoch: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "manifests/{}/epoch_{:012}.json",
            self.project_id, epoch
        ))
    }

    pub async fn load_latest(&self) -> Result<Manifest> {
        let Some((_, meta)) = self.list_epochs().await?.pop() else {
            return Ok(Manifest::default());
        };
        let bytes = self.object_store.get(&meta.location).await?.bytes().await?;
        let wire = decode_manifest_payload(&bytes)?;
        Ok(Manifest::from_wire(wire))
    }

    /// Commits `next_manifest` at `expected_prev_epoch + 1` using conditional `PutMode::Create`.
    pub async fn commit_next(&self, mut next: Manifest) -> Result<Manifest> {
        next.epoch += 1;
        let path = self.epoch_path(next.epoch);
        let payload = encode_manifest_payload(&next.to_wire())?;

        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(payload)), opts)
            .await
        {
            Ok(_) => {
                // A writer more than `retain_epochs` behind would find its next epoch slot already
                // garbage-collected and `Create` would succeed, hiding its commit under newer epochs.
                // Its predecessor is then gone too (GC deletes a prefix), so treat that as a conflict.
                if next.epoch > 1 {
                    if let Err(OsError::NotFound { .. }) = self
                        .object_store
                        .head(&self.epoch_path(next.epoch - 1))
                        .await
                    {
                        let _ = self.object_store.delete(&path).await;
                        return Err(StorageError::ManifestConflict);
                    }
                }
                Ok(next)
            }
            Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                Err(StorageError::ManifestConflict)
            }
            Err(other) => Err(StorageError::ObjectStore(other)),
        }
    }
}

const MANIFEST_ZSTD_MAGIC: &[u8; 4] = b"PZST";

fn encode_json_zstd<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(value)?;
    let compressed = zstd::stream::encode_all(json.as_slice(), 3)
        .map_err(|e| StorageError::Corrupted(format!("manifest zstd compress: {e}")))?;
    let mut out = Vec::with_capacity(MANIFEST_ZSTD_MAGIC.len() + compressed.len());
    out.extend_from_slice(MANIFEST_ZSTD_MAGIC);
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn decode_json_zstd<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if let Some(rest) = bytes.strip_prefix(MANIFEST_ZSTD_MAGIC) {
        let json = zstd::stream::decode_all(rest)
            .map_err(|e| StorageError::Corrupted(format!("manifest zstd decompress: {e}")))?;
        Ok(serde_json::from_slice(&json)?)
    } else {
        Ok(serde_json::from_slice(bytes)?)
    }
}

fn encode_manifest_payload(wire: &ManifestWire) -> Result<Vec<u8>> {
    encode_json_zstd(wire)
}

fn decode_manifest_payload(bytes: &[u8]) -> Result<ManifestWire> {
    decode_json_zstd(bytes)
}

fn parse_epoch_from_path(path: &str) -> Option<u64> {
    let fname = path.rsplit('/').next()?;
    let body = fname.strip_prefix("epoch_")?.strip_suffix(".json")?;
    body.parse::<u64>().ok()
}

/// Reference to a project's S3 identity snapshot under `published/{project_id}/snapshots/...`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdentitySnapshotRef {
    pub object_path: String,
    pub user_count: u64,
    pub mutation_epoch: u64,
}

/// Reference to a project's S3 profile snapshot under `published/{project_id}/snapshots/...`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileSnapshotRef {
    pub object_path: String,
    pub dirty_seq: u64,
}

/// Snapshot pointers bundled in a [`PublishedManifest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef {
    pub identity: IdentitySnapshotRef,
    #[serde(default)]
    pub profiles: Option<ProfileSnapshotRef>,
}

/// Dense-sequence published S3 manifest (`published/{project_id}/pub_{publish_seq:012}.json`).
///
/// Written create-only after `T_id >= T_prof >= T_freeze` snapshots complete, carrying the
/// identity `high_water_mark` and fencing metadata (`publish_seq`, `writer_generation`,
/// `source_local_epoch`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedManifest {
    pub publish_seq: u64,
    pub writer_generation: u64,
    pub source_local_epoch: u64,
    pub high_water_mark: u64,
    pub published_at_ms: i64,
    pub partitions: Vec<PartitionEntry>,
    pub dict_snapshot: ProjectDictSnapshot,
    pub committed_wal_seq: BTreeMap<u8, u64>,
    pub committed_l0_seq: BTreeMap<u8, u64>,
    pub snapshot_ref: SnapshotRef,
}

impl PublishedManifest {
    /// Materializes a local [`Manifest`] at `target_epoch` for `restore --from-published`.
    pub fn to_local_manifest(&self, target_epoch: u64) -> Manifest {
        let mut active_segments = HashMap::with_capacity(self.partitions.len());
        for p in &self.partitions {
            active_segments.insert((p.date, p.bucket_id), p.segments.clone());
        }
        Manifest {
            epoch: target_epoch,
            fenced_wal_epoch: target_epoch,
            writer_generation: self.writer_generation,
            last_published_seq: Some(self.publish_seq),
            pending_s3_deletes: Vec::new(),
            active_segments,
            committed_wal_seq: self.committed_wal_seq.clone(),
            committed_l0_seq: self.committed_l0_seq.clone(),
            dict_snapshot: self.dict_snapshot.clone(),
        }
    }
}

/// Conditional-create store for `published/{project_id}/pub_{publish_seq:012}.json`.
pub struct PublishedManifestStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl PublishedManifestStore {
    pub fn new(project_id: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            project_id: project_id.into(),
            object_store,
        }
    }

    fn prefix(&self) -> ObjectPath {
        ObjectPath::from(format!("published/{}", self.project_id))
    }

    pub fn pub_path(&self, publish_seq: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "published/{}/pub_{:012}.json",
            self.project_id, publish_seq
        ))
    }

    async fn list_pub_seqs(&self) -> Result<Vec<(u64, ObjectMeta)>> {
        let prefix = self.prefix();
        let res = self.object_store.list_with_delimiter(Some(&prefix)).await?;
        let expected_dir = format!("published/{}/", self.project_id);
        let mut out: Vec<(u64, ObjectMeta)> = res
            .objects
            .into_iter()
            .filter_map(|m| {
                let rel = m.location.as_ref().strip_prefix(&expected_dir)?;
                if rel.contains('/') {
                    return None;
                }
                parse_pub_seq_from_filename(rel).map(|seq| (seq, m))
            })
            .collect();
        out.sort_unstable_by_key(|(seq, _)| *seq);
        Ok(out)
    }

    pub async fn latest_pub_seq(&self) -> Result<Option<(u64, ObjectMeta)>> {
        Ok(self.list_pub_seqs().await?.pop())
    }

    pub async fn load_from_meta(&self, meta: &ObjectMeta) -> Result<PublishedManifest> {
        let bytes = self.object_store.get(&meta.location).await?.bytes().await?;
        decode_json_zstd(&bytes)
    }

    pub async fn load_latest(&self) -> Result<Option<PublishedManifest>> {
        let Some((_, meta)) = self.latest_pub_seq().await? else {
            return Ok(None);
        };
        Ok(Some(self.load_from_meta(&meta).await?))
    }

    pub async fn load_seq(&self, publish_seq: u64) -> Result<PublishedManifest> {
        let path = self.pub_path(publish_seq);
        let bytes = self.object_store.get(&path).await?.bytes().await?;
        decode_json_zstd(&bytes)
    }

    /// Writes `pub_manifest` create-only at `pub_{publish_seq:012}.json`, verifying
    /// `head(publish_seq - 1)` when `publish_seq > 0` so a stale writer behind GC cannot slip past.
    pub async fn publish_next(&self, pub_manifest: PublishedManifest) -> Result<PublishedManifest> {
        let seq = pub_manifest.publish_seq;
        let path = self.pub_path(seq);
        let payload = encode_json_zstd(&pub_manifest)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(payload)), opts)
            .await
        {
            Ok(_) => {
                if seq > 0 {
                    if let Err(OsError::NotFound { .. }) =
                        self.object_store.head(&self.pub_path(seq - 1)).await
                    {
                        let _ = self.object_store.delete(&path).await;
                        return Err(StorageError::ManifestConflict);
                    }
                }
                Ok(pub_manifest)
            }
            Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                Err(StorageError::ManifestConflict)
            }
            Err(other) => Err(StorageError::ObjectStore(other)),
        }
    }
}

fn parse_pub_seq_from_filename(fname: &str) -> Option<u64> {
    let body = fname.strip_prefix("pub_")?.strip_suffix(".json")?;
    body.parse::<u64>().ok()
}

/// Installs `pub_m` into `manifests/{project_id}/` for `restore --from-published`.
///
/// When `manifests/{project_id}/` already contains epochs (e.g., T1 with S3 `manifests/`), commits
/// at `latest.epoch + 1` via CAS; when empty (e.g., T2 after local volume loss), writes
/// `pub_m.source_local_epoch.max(1)` create-only so subsequent commits maintain monotonic epochs.
pub async fn install_restored_manifest(
    project_id: &str,
    store: Arc<dyn ObjectStore>,
    pub_m: &PublishedManifest,
) -> Result<Manifest> {
    let ms = ManifestStore::new(project_id, store.clone());
    let latest = ms.load_latest().await?;
    if latest.epoch > 0 {
        let next = pub_m.to_local_manifest(latest.epoch);
        return ms.commit_next(next).await;
    }

    let target_epoch = pub_m.source_local_epoch.max(1);
    let restored = pub_m.to_local_manifest(target_epoch);
    let path = ms.epoch_path(target_epoch);
    let payload = encode_manifest_payload(&restored.to_wire())?;
    let opts = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store
        .put_opts(&path, PutPayload::from_bytes(Bytes::from(payload)), opts)
        .await
    {
        Ok(_) => Ok(restored),
        Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
            let latest = ms.load_latest().await?;
            ms.commit_next(pub_m.to_local_manifest(latest.epoch)).await
        }
        Err(other) => Err(StorageError::ObjectStore(other)),
    }
}

/// Pin tracker ensuring compacted/superseded L1 segments are deleted ONLY once all active readers finish.
#[derive(Default)]
pub struct ReaderPinTracker {
    segment_pins: DashMap<String, usize>,
}

impl ReaderPinTracker {
    pub fn pin_all(&self, segment_paths: &[String]) {
        for p in segment_paths {
            *self.segment_pins.entry(p.clone()).or_insert(0) += 1;
        }
    }

    pub fn unpin_all(&self, segment_paths: &[String]) {
        for p in segment_paths {
            if let Some(mut entry) = self.segment_pins.get_mut(p) {
                *entry = entry.saturating_sub(1);
            }
            // Atomic shard-locked conditional removal so a concurrent `0 -> 1` pin is NEVER erased!
            self.segment_pins.remove_if(p, |_, v| *v == 0);
        }
    }

    pub fn is_pinned(&self, object_path: &str) -> bool {
        self.segment_pins.get(object_path).is_some_and(|v| *v > 0)
    }
}

/// Consistent read-after-write MVCC snapshot acquired atomically at the start of every query (`P1-05`, `P1-07`).
pub struct StorageSnapshot {
    pub manifest: Arc<Manifest>,
    pub l0_watermark: u64,
    l0_buckets: Vec<Arc<[Arc<StoredRow>]>>,
    pinned_paths: Vec<String>,
    tracker: Arc<ReaderPinTracker>,
}

impl StorageSnapshot {
    pub fn new(
        manifest: Arc<Manifest>,
        l0_watermark: u64,
        l0_buckets: Vec<Arc<[Arc<StoredRow>]>>,
        tracker: Arc<ReaderPinTracker>,
    ) -> Self {
        let pinned_paths: Vec<String> = manifest
            .active_segments
            .values()
            .flat_map(|v| v.iter().map(|s| s.object_path.clone()))
            .collect();
        tracker.pin_all(&pinned_paths);
        Self {
            manifest,
            l0_watermark,
            l0_buckets,
            pinned_paths,
            tracker,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    /// Returns captured L0 rows for `bucket_id` strictly inside `(bucket_l0_floor, l0_watermark]`.
    pub fn l0_bucket_rows(&self, bucket_id: u8) -> Vec<StoredRow> {
        let Some(slice) = self.l0_buckets.get(bucket_id as usize) else {
            return Vec::new();
        };
        let floor = self.bucket_l0_floor(bucket_id);
        slice
            .iter()
            .filter(|r| r.l0_seq > floor && r.l0_seq <= self.l0_watermark)
            .map(|r| (**r).clone())
            .collect()
    }

    pub fn segments_for_range(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
        bucket_filter: Option<u8>,
    ) -> Vec<SegmentMeta> {
        let mut out = Vec::new();
        for (&(date, bucket), segs) in &self.manifest.active_segments {
            if date >= start_date && date <= end_date && bucket_filter.is_none_or(|b| b == bucket) {
                out.extend(segs.iter().cloned());
            }
        }
        out.sort_by(|a, b| {
            a.bucket_id
                .cmp(&b.bucket_id)
                .then_with(|| a.date.cmp(&b.date))
                .then_with(|| a.segment_id.cmp(&b.segment_id))
        });
        out
    }

    pub fn bucket_l0_floor(&self, bucket_id: u8) -> u64 {
        self.manifest
            .committed_l0_seq
            .get(&bucket_id)
            .copied()
            .unwrap_or(0)
    }
}

impl Drop for StorageSnapshot {
    fn drop(&mut self) {
        self.tracker.unpin_all(&self.pinned_paths);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn epoch_files(store: &Arc<dyn ObjectStore>) -> Vec<u64> {
        let metas: Vec<_> = store.list(None).try_collect().await.unwrap();
        let mut eps: Vec<u64> = metas
            .iter()
            .filter_map(|m| parse_epoch_from_path(m.location.as_ref()))
            .collect();
        eps.sort_unstable();
        eps
    }

    #[tokio::test]
    async fn old_epochs_are_garbage_collected_and_latest_still_loads() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ms = ManifestStore::new("p", store.clone()).with_retention(4, Duration::ZERO);

        let mut cur = Manifest::default();
        for i in 0..40u64 {
            cur.committed_wal_seq.insert(0, i);
            cur = ms.commit_next(cur).await.unwrap();
        }
        assert_eq!(epoch_files(&store).await.len(), 40);

        // Grace period protects young files even outside the retained window.
        let guarded =
            ManifestStore::new("p", store.clone()).with_retention(4, Duration::from_secs(3600));
        assert_eq!(guarded.gc_old_epochs().await.unwrap(), 0);

        assert_eq!(ms.gc_old_epochs().await.unwrap(), 36);
        assert_eq!(epoch_files(&store).await, vec![37, 38, 39, 40]);

        let latest = ms.load_latest().await.unwrap();
        assert_eq!(latest.epoch, 40);
        assert_eq!(latest.committed_wal_seq.get(&0), Some(&39));

        // A stale writer whose next slot was collected must conflict, not silently "commit".
        let stale = Manifest {
            epoch: 9,
            ..Manifest::default()
        };
        assert!(matches!(
            ms.commit_next(stale).await,
            Err(StorageError::ManifestConflict)
        ));
        assert_eq!(epoch_files(&store).await, vec![37, 38, 39, 40]);

        // The live writer keeps committing normally.
        assert_eq!(ms.commit_next(latest).await.unwrap().epoch, 41);
    }

    #[tokio::test]
    async fn compressed_manifest_roundtrip_and_legacy_json_compat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ms = ManifestStore::new("p", store.clone());

        // 1. Write a legacy uncompressed JSON epoch directly to object storage.
        let mut legacy = Manifest {
            epoch: 1,
            ..Manifest::default()
        };
        legacy.committed_wal_seq.insert(7, 42);
        let raw_json = serde_json::to_vec(&legacy.to_wire()).unwrap();
        store
            .put(
                &ObjectPath::from("manifests/p/epoch_000000000001.json"),
                PutPayload::from_bytes(Bytes::from(raw_json)),
            )
            .await
            .unwrap();

        let loaded = ms.load_latest().await.unwrap();
        assert_eq!(loaded.epoch, 1);
        assert_eq!(loaded.committed_wal_seq.get(&7), Some(&42));

        // 2. Commit a large manifest with 512 segments and verify PZST compression + round-trip.
        let mut big = loaded;
        let date = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
        for b in 0..256u16 {
            for s in 0..2u8 {
                let seg = SegmentMeta {
                    segment_id: format!("seg_test_{b}_{s}"),
                    date,
                    bucket_id: b as u8,
                    object_path: format!("segments/p/{date}/bucket_{b}/seg_test_{b}_{s}.vtx"),
                    file_size: 128_000,
                    vortex_len: 120_000,
                    inverted_idx_offset: 120_000,
                    session_idx_offset: 124_000,
                    footer: crate::segment::SegmentFooterMeta {
                        row_count: 10_000,
                        session_count: 2_500,
                        min_ts_ms: 1_790_000_000_000,
                        max_ts_ms: 1_790_086_400_000,
                        segment_event_bloom_64: 0xFF,
                        tier_c_bloom_64: 0x0F,
                        has_tier_c_column: true,
                        tier_b_columns: vec![],
                        event_counts: vec![(1, 5_000), (2, 5_000)],
                        event_hlls: vec![],
                    },
                };
                big.active_segments
                    .entry((date, b as u8))
                    .or_default()
                    .push(seg);
            }
        }
        let raw_big_len = serde_json::to_vec(&big.to_wire()).unwrap().len();
        let committed = ms.commit_next(big).await.unwrap();
        assert_eq!(committed.epoch, 2);

        let stored_bytes = store
            .get(&ObjectPath::from("manifests/p/epoch_000000000002.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(stored_bytes.starts_with(MANIFEST_ZSTD_MAGIC));
        assert!(
            stored_bytes.len() * 5 < raw_big_len,
            "expected >=5x compression, got {} vs {}",
            stored_bytes.len(),
            raw_big_len
        );

        let reloaded = ms.load_latest().await.unwrap();
        assert_eq!(reloaded.epoch, 2);
        assert_eq!(reloaded.active_segments.len(), 256);
        assert_eq!(reloaded.total_l1_rows(), 512 * 10_000);
    }

    #[tokio::test]
    async fn published_manifest_dense_seq_conflict_and_gc_guard() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pms = PublishedManifestStore::new("p", store.clone());
        assert!(pms.load_latest().await.unwrap().is_none());

        // Snapshot objects under published/p/snapshots/ must not confuse load_latest.
        store
            .put(
                &ObjectPath::from("published/p/snapshots/identity_000000000000.pzst"),
                PutPayload::from_static(b"snap"),
            )
            .await
            .unwrap();
        assert!(pms.load_latest().await.unwrap().is_none());

        let make_pub = |seq: u64, gen: u64, epoch: u64, hwm: u64| PublishedManifest {
            publish_seq: seq,
            writer_generation: gen,
            source_local_epoch: epoch,
            high_water_mark: hwm,
            published_at_ms: 1_790_000_000_000 + seq as i64,
            partitions: vec![],
            dict_snapshot: ProjectDictSnapshot::default(),
            committed_wal_seq: BTreeMap::from([(0, seq + 1)]),
            committed_l0_seq: BTreeMap::from([(0, seq + 1)]),
            snapshot_ref: SnapshotRef {
                identity: IdentitySnapshotRef {
                    object_path: format!("published/p/snapshots/identity_{seq:012}.pzst"),
                    user_count: hwm,
                    mutation_epoch: seq,
                },
                profiles: None,
            },
        };

        // Publish seq 0 and 1.
        let p0 = pms.publish_next(make_pub(0, 1, 1, 10)).await.unwrap();
        assert_eq!(p0.publish_seq, 0);
        let p1 = pms.publish_next(make_pub(1, 1, 2, 25)).await.unwrap();
        assert_eq!(p1.publish_seq, 1);

        // Duplicate PutMode::Create at seq 1 conflicts.
        assert!(matches!(
            pms.publish_next(make_pub(1, 2, 3, 30)).await,
            Err(StorageError::ManifestConflict)
        ));

        // Verify PZST compression and load_latest.
        let raw_p1 = store
            .get(&pms.pub_path(1))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(raw_p1.starts_with(MANIFEST_ZSTD_MAGIC));
        let latest = pms.load_latest().await.unwrap().unwrap();
        assert_eq!(latest.publish_seq, 1);
        assert_eq!(latest.high_water_mark, 25);

        // GC guard: if predecessor (seq 1) is missing, publish_next(2) deletes seq 2 and returns Conflict.
        store.delete(&pms.pub_path(1)).await.unwrap();
        assert!(matches!(
            pms.publish_next(make_pub(2, 1, 3, 40)).await,
            Err(StorageError::ManifestConflict)
        ));
        assert!(store.head(&pms.pub_path(2)).await.is_err());

        // Plain-JSON fallback on PublishedManifestStore + install_restored_manifest (empty & non-empty).
        let p2_plain = make_pub(2, 2, 5, 42);
        store
            .put(
                &pms.pub_path(2),
                PutPayload::from(serde_json::to_vec(&p2_plain).unwrap()),
            )
            .await
            .unwrap();
        let loaded_p2 = pms.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded_p2.publish_seq, 2);

        // Install into empty manifests/ -> epoch = source_local_epoch (5).
        let installed1 = install_restored_manifest("p", store.clone(), &loaded_p2)
            .await
            .unwrap();
        assert_eq!(installed1.epoch, 5);
        assert_eq!(installed1.last_published_seq, Some(2));
        assert_eq!(installed1.writer_generation, 2);

        // Install again when manifests/ already has epoch 5 -> commits epoch 6.
        let installed2 = install_restored_manifest("p", store.clone(), &loaded_p2)
            .await
            .unwrap();
        assert_eq!(installed2.epoch, 6);
        assert_eq!(installed2.last_published_seq, Some(2));
    }

    #[tokio::test]
    async fn pending_s3_deletes_roundtrip_and_legacy_wire_compat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ms = ManifestStore::new("p_gc", store.clone());

        // Legacy ManifestWire JSON without `pending_s3_deletes` deserializes cleanly.
        let legacy_json = br#"{
            "epoch": 1,
            "fenced_wal_epoch": 1,
            "writer_generation": 2,
            "last_published_seq": 0,
            "partitions": [],
            "committed_wal_seq": {},
            "committed_l0_seq": {},
            "dict_snapshot": { "events": [], "prop_keys": [] }
        }"#;
        store
            .put(
                &ObjectPath::from("manifests/p_gc/epoch_000000000001.json"),
                PutPayload::from_static(legacy_json),
            )
            .await
            .unwrap();
        let loaded = ms.load_latest().await.unwrap();
        assert_eq!(loaded.epoch, 1);
        assert!(loaded.pending_s3_deletes.is_empty());

        // Round-trip all PendingS3DeleteKind variants through PZST ManifestStore.
        let mut next = loaded;
        next.pending_s3_deletes = vec![
            PendingS3Delete {
                object_path: "segments/p_gc/2026-11-22/bucket_000/seg_1.pvtx".into(),
                kind: PendingS3DeleteKind::Segment,
                superseding_pub_seq: 1,
                superseded_at_ms: Some(1_790_000_000_000),
                eligible_after_ms: Some(1_790_000_600_000),
            },
            PendingS3Delete {
                object_path: "published/p_gc/snapshots/identity_000000000000_a.pzst".into(),
                kind: PendingS3DeleteKind::IdentitySnapshot,
                superseding_pub_seq: 1,
                superseded_at_ms: None,
                eligible_after_ms: None,
            },
            PendingS3Delete {
                object_path: "published/p_gc/snapshots/profiles_000000000000_b.pzst".into(),
                kind: PendingS3DeleteKind::ProfileSnapshot,
                superseding_pub_seq: 2,
                superseded_at_ms: Some(1_790_000_100_000),
                eligible_after_ms: Some(1_790_000_700_000),
            },
        ];
        let committed = ms.commit_next(next).await.unwrap();
        assert_eq!(committed.epoch, 2);

        let reloaded = ms.load_latest().await.unwrap();
        assert_eq!(reloaded.pending_s3_deletes.len(), 3);
        assert_eq!(
            reloaded.pending_s3_deletes[0].kind,
            PendingS3DeleteKind::Segment
        );
        assert_eq!(
            reloaded.pending_s3_deletes[1].kind,
            PendingS3DeleteKind::IdentitySnapshot
        );
        assert_eq!(
            reloaded.pending_s3_deletes[2].kind,
            PendingS3DeleteKind::ProfileSnapshot
        );
        assert_eq!(
            reloaded.pending_s3_deletes[2].eligible_after_ms,
            Some(1_790_000_700_000)
        );
    }
}
