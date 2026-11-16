use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use chrono::NaiveDate;
use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};

use crate::dictionary::ProjectDictSnapshot;
use crate::error::{Result, StorageError};
use crate::mem_bucket::StoredRow;
use crate::segment::SegmentMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartitionEntry {
    date: NaiveDate,
    bucket_id: u8,
    segments: Vec<SegmentMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestWire {
    epoch: u64,
    #[serde(default)]
    fenced_wal_epoch: u64,
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

    fn to_wire(&self) -> ManifestWire {
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
        ManifestWire {
            epoch: self.epoch,
            fenced_wal_epoch: self.fenced_wal_epoch,
            partitions,
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
            active_segments,
            committed_wal_seq: w.committed_wal_seq,
            committed_l0_seq: w.committed_l0_seq,
            dict_snapshot: w.dict_snapshot,
        }
    }
}

/// Atomic MVCC Manifest store backed by conditional `PutMode::Create` writes on `ObjectStore`.
pub struct ManifestStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl ManifestStore {
    pub fn new(project_id: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            project_id: project_id.into(),
            object_store,
        }
    }

    fn epoch_path(&self, epoch: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "manifests/{}/epoch_{:012}.json",
            self.project_id, epoch
        ))
    }

    pub async fn load_latest(&self) -> Result<Manifest> {
        let prefix = ObjectPath::from(format!("manifests/{}/", self.project_id));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut max_epoch: Option<(u64, ObjectPath)> = None;
        for m in metas {
            if let Some(ep) = parse_epoch_from_path(m.location.as_ref()) {
                if max_epoch.as_ref().is_none_or(|(cur, _)| ep > *cur) {
                    max_epoch = Some((ep, m.location));
                }
            }
        }

        let Some((_, path)) = max_epoch else {
            return Ok(Manifest::default());
        };
        let bytes = self.object_store.get(&path).await?.bytes().await?;
        let wire: ManifestWire = serde_json::from_slice(&bytes)?;
        Ok(Manifest::from_wire(wire))
    }

    /// Commits `next_manifest` at `expected_prev_epoch + 1` using conditional `PutMode::Create`.
    pub async fn commit_next(&self, mut next: Manifest) -> Result<Manifest> {
        next.epoch += 1;
        let path = self.epoch_path(next.epoch);
        let payload = serde_json::to_vec(&next.to_wire())?;

        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(payload)), opts)
            .await
        {
            Ok(_) => Ok(next),
            Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                Err(StorageError::ManifestConflict)
            }
            Err(other) => Err(StorageError::ObjectStore(other)),
        }
    }
}

fn parse_epoch_from_path(path: &str) -> Option<u64> {
    let fname = path.rsplit('/').next()?;
    let body = fname.strip_prefix("epoch_")?.strip_suffix(".json")?;
    body.parse::<u64>().ok()
}

/// Pin tracker ensuring compacted/superseded L1 segments are deleted ONLY once all active readers finish (`P1-05`).
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
            // Atomic shard-locked conditional removal so a concurrent `0 -> 1` pin is NEVER erased (`P1-05`)!
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

    /// Returns captured L0 rows for `bucket_id` strictly inside `(bucket_l0_floor, l0_watermark]` (`P1-07`).
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
