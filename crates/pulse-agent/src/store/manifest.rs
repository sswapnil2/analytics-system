use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectMeta, ObjectStore, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};

use crate::store::bloom::SegmentZoneMap;
use crate::store::error::{validate_identifier, StoreError, StoreResult};

pub const AGENT_GC_GRACE_MS: i64 = 15 * 60 * 1000; // 15 minutes
pub const AGENT_MANIFEST_RETAIN_EPOCHS: u64 = 16;
pub const MAX_MANIFEST_CAS_RETRIES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub segment_id: String,
    pub object_path: String,
    pub date: String,
    pub shard_id: u16,
    pub level: u8,
    pub file_size: u64,
    pub tail_offset: u64,
    pub zone_map: SegmentZoneMap,
    pub trace_count: u32,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingGcEntry {
    pub object_path: String,
    pub segment_id: String,
    pub superseded_at_ms: i64,
    pub eligible_after_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgent_erasure_deadline_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub erasure_tombstone_ids: Vec<String>,
}

/// Deferred S3 `.plss` deletion persisted in the local [`AgentManifest`].
///
/// An S3 segment may only be deleted after a superseding `PublishedAgentManifest` has been
/// written to `agent_published/`, plus 2 publish intervals (or an accelerated pre-expiry
/// deadline when urgent agent erasure tombstones are waiting), and once unpinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingAgentS3Delete {
    pub object_path: String,
    pub segment_id: String,
    pub superseding_pub_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eligible_after_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgent_erasure_deadline_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub erasure_tombstone_ids: Vec<String>,
}

/// Computes the accelerated deletion grace window when an urgent finite-TTL agent erasure
/// tombstone (`deadline_ms`) requires physical removal from S3 before expiry.
pub fn compute_urgent_grace_ms(pub_ts_ms: i64, deadline_ms: i64, normal_grace_ms: i64) -> i64 {
    let remaining = deadline_ms.saturating_sub(pub_ts_ms);
    if remaining >= normal_grace_ms.saturating_add(30_000) {
        normal_grace_ms
    } else if remaining <= 0 {
        0
    } else {
        normal_grace_ms.min((remaining / 3).max(10_000.min(remaining / 2)))
    }
}

pub(crate) fn partition_upload_key(date: &str, shard_id: u16) -> String {
    format!("{date}/{shard_id}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AgentManifest {
    pub epoch: u64,
    pub updated_at_ms: i64,
    #[serde(default)]
    pub writer_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_published_seq: Option<u64>,
    #[serde(default)]
    pub fenced: bool,
    #[serde(default)]
    pub segments: Vec<SegmentDescriptor>,
    #[serde(default)]
    pub committed_wal_seq: BTreeMap<String, u64>,
    #[serde(default)]
    pub pending_gc: Vec<PendingGcEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_s3_deletes: Vec<PendingAgentS3Delete>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub s3_partition_last_upload_ms: BTreeMap<String, i64>,
    #[serde(default)]
    pub erased_traces: BTreeMap<String, i64>,
}

/// Pin tracker ensuring superseded/compacted `.plss` segments are never deleted while
/// live readers hold an active snapshot.
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
            self.segment_pins.remove_if(p, |_, v| *v == 0);
        }
    }

    pub fn is_pinned(&self, object_path: &str) -> bool {
        self.segment_pins.get(object_path).is_some_and(|v| *v > 0)
    }
}

/// Atomic MVCC manifest store backed by `PutMode::Create` CAS + stale-writer fencing.
pub struct ManifestStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
    retain_epochs: u64,
    gc_grace_ms: i64,
}

impl ManifestStore {
    pub fn new(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        gc_grace_ms: i64,
    ) -> StoreResult<Self> {
        let project_id = project_id.into();
        validate_identifier("project_id", &project_id)?;
        Ok(Self {
            project_id,
            object_store,
            retain_epochs: AGENT_MANIFEST_RETAIN_EPOCHS,
            gc_grace_ms: gc_grace_ms.max(0),
        })
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn gc_grace_ms(&self) -> i64 {
        self.gc_grace_ms
    }

    fn prefix(&self) -> ObjectPath {
        ObjectPath::from(format!("agent_manifests/{}/", self.project_id))
    }

    pub(crate) fn epoch_path(&self, epoch: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "agent_manifests/{}/epoch_{:012}.json",
            self.project_id, epoch
        ))
    }

    async fn list_epochs(&self) -> StoreResult<Vec<(u64, ObjectMeta)>> {
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

    pub async fn load_latest(&self) -> StoreResult<AgentManifest> {
        let Some((_, meta)) = self.list_epochs().await?.pop() else {
            return Ok(AgentManifest::default());
        };
        let bytes = self.object_store.get(&meta.location).await?.bytes().await?;
        let manifest: AgentManifest = serde_json::from_slice(&bytes)?;
        Ok(manifest)
    }

    pub async fn commit_next(&self, mut next: AgentManifest) -> StoreResult<AgentManifest> {
        next.epoch = next
            .epoch
            .checked_add(1)
            .ok_or(StoreError::ResourceLimitExceeded("manifest epoch overflow"))?;
        if next.updated_at_ms == 0 {
            next.updated_at_ms = chrono::Utc::now().timestamp_millis();
        }
        let path = self.epoch_path(next.epoch);
        let payload = serde_json::to_vec(&next)?;
        let opts = PutOptions::from(PutMode::Create);

        match self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(payload)), opts)
            .await
        {
            Ok(_) => {
                if next.epoch > 1 {
                    match self
                        .object_store
                        .head(&self.epoch_path(next.epoch - 1))
                        .await
                    {
                        Ok(_) => {}
                        Err(OsError::NotFound { .. }) => {
                            let _ = self.object_store.delete(&path).await;
                            return Err(StoreError::ManifestConflict);
                        }
                        Err(other) => {
                            let _ = self.object_store.delete(&path).await;
                            return Err(StoreError::ObjectStore(other));
                        }
                    }
                }
                Ok(next)
            }
            Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                Err(StoreError::ManifestConflict)
            }
            Err(other) => Err(StoreError::ObjectStore(other)),
        }
    }

    /// Performs optimistic CAS mutation over the latest manifest with up to 16 retries.
    pub async fn commit_with_cas<F>(&self, mut apply: F) -> StoreResult<AgentManifest>
    where
        F: FnMut(&mut AgentManifest) -> StoreResult<bool>,
    {
        for attempt in 0..MAX_MANIFEST_CAS_RETRIES {
            let mut cur = self.load_latest().await?;
            let changed = apply(&mut cur)?;
            if !changed {
                return Ok(cur);
            }
            match self.commit_next(cur).await {
                Ok(committed) => return Ok(committed),
                Err(StoreError::ManifestConflict) if attempt + 1 < MAX_MANIFEST_CAS_RETRIES => {
                    let jitter_ms = 5 + ((attempt as u64 * 7) % 21);
                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(StoreError::ManifestConflict)
    }

    pub async fn gc_old_epochs(&self, now_ms: i64) -> StoreResult<usize> {
        let epochs = self.list_epochs().await?;
        let Some(latest) = epochs.last().map(|(ep, _)| *ep) else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        for (ep, meta) in epochs {
            let age_ms = now_ms.saturating_sub(meta.last_modified.timestamp_millis());
            if ep.saturating_add(self.retain_epochs) > latest || age_ms < self.gc_grace_ms {
                break;
            }
            match self.object_store.delete(&meta.location).await {
                Ok(()) | Err(OsError::NotFound { .. }) => deleted += 1,
                Err(e) => return Err(StoreError::ObjectStore(e)),
            }
        }
        Ok(deleted)
    }

    /// Garbage-collects superseded segment objects once `now_ms >= eligible_after_ms`,
    /// not referenced by active manifest `segments`, and not pinned by any active reader.
    pub async fn run_gc(&self, now_ms: i64, pin_tracker: &ReaderPinTracker) -> StoreResult<usize> {
        let manifest = self.load_latest().await?;
        if manifest.pending_gc.is_empty() && manifest.pending_s3_deletes.is_empty() {
            return Ok(0);
        }

        let active_paths: HashSet<&str> = manifest
            .segments
            .iter()
            .map(|s| s.object_path.as_str())
            .collect();

        let mut candidate_paths: Vec<String> = Vec::new();
        for entry in &manifest.pending_gc {
            if now_ms < entry.eligible_after_ms {
                continue;
            }
            if active_paths.contains(entry.object_path.as_str()) {
                continue;
            }
            if pin_tracker.is_pinned(&entry.object_path) {
                continue;
            }
            candidate_paths.push(entry.object_path.clone());
        }
        for entry in &manifest.pending_s3_deletes {
            if entry.eligible_after_ms.is_none_or(|t| now_ms < t) {
                continue;
            }
            if active_paths.contains(entry.object_path.as_str()) {
                continue;
            }
            if pin_tracker.is_pinned(&entry.object_path) {
                continue;
            }
            candidate_paths.push(entry.object_path.clone());
        }
        if candidate_paths.is_empty() {
            return Ok(0);
        }

        let mut deleted_paths: HashSet<String> = HashSet::new();
        for path in candidate_paths {
            let loc = ObjectPath::from(path.as_str());
            match self.object_store.delete(&loc).await {
                Ok(()) | Err(OsError::NotFound { .. }) => {
                    deleted_paths.insert(path);
                }
                Err(e) => {
                    tracing::warn!(
                        project_id = %self.project_id,
                        path = %path,
                        error = %e,
                        "failed to delete pending GC segment"
                    );
                }
            }
        }

        if deleted_paths.is_empty() {
            return Ok(0);
        }

        let deleted_count = deleted_paths.len();
        self.commit_with_cas(|m| {
            let before_gc = m.pending_gc.len();
            let before_s3 = m.pending_s3_deletes.len();
            m.pending_gc
                .retain(|e| !deleted_paths.contains(&e.object_path));
            m.pending_s3_deletes
                .retain(|e| !deleted_paths.contains(&e.object_path));
            if m.pending_gc.len() != before_gc || m.pending_s3_deletes.len() != before_s3 {
                m.updated_at_ms = now_ms;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .await?;

        Ok(deleted_count)
    }
}

/// Dense-sequence published S3 manifest (`agent_published/{project_id}/pub_{publish_seq:012}.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedAgentManifest {
    pub publish_seq: u64,
    pub writer_generation: u64,
    pub source_local_epoch: u64,
    pub published_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub segments: Vec<SegmentDescriptor>,
    #[serde(default)]
    pub committed_wal_seq: BTreeMap<String, u64>,
    #[serde(default)]
    pub erased_traces: BTreeMap<String, i64>,
}

impl PublishedAgentManifest {
    /// Materializes a local [`AgentManifest`] for `restore --from-published` or `NodeRole::Query`.
    pub fn to_local_manifest(
        &self,
        target_epoch: u64,
        target_writer_generation: u64,
    ) -> AgentManifest {
        AgentManifest {
            epoch: target_epoch,
            updated_at_ms: self.published_at_ms,
            writer_generation: target_writer_generation,
            last_published_seq: Some(self.publish_seq),
            fenced: false,
            segments: self.segments.clone(),
            committed_wal_seq: self.committed_wal_seq.clone(),
            pending_gc: Vec::new(),
            pending_s3_deletes: Vec::new(),
            s3_partition_last_upload_ms: BTreeMap::new(),
            erased_traces: self.erased_traces.clone(),
        }
    }
}

/// Conditional-create store for `agent_published/{project_id}/pub_{publish_seq:012}.json`.
pub struct PublishedAgentManifestStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl PublishedAgentManifestStore {
    pub fn new(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> StoreResult<Self> {
        let project_id = project_id.into();
        validate_identifier("project_id", &project_id)?;
        Ok(Self {
            project_id,
            object_store,
        })
    }

    fn prefix(&self) -> ObjectPath {
        ObjectPath::from(format!("agent_published/{}", self.project_id))
    }

    pub fn pub_path(&self, publish_seq: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "agent_published/{}/pub_{:012}.json",
            self.project_id, publish_seq
        ))
    }

    async fn list_pub_seqs(&self) -> StoreResult<Vec<(u64, ObjectMeta)>> {
        let prefix = self.prefix();
        let res = self.object_store.list_with_delimiter(Some(&prefix)).await?;
        let expected_dir = format!("agent_published/{}/", self.project_id);
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

    pub async fn latest_pub_seq(&self) -> StoreResult<Option<(u64, ObjectMeta)>> {
        Ok(self.list_pub_seqs().await?.pop())
    }

    pub async fn load_from_meta(&self, meta: &ObjectMeta) -> StoreResult<PublishedAgentManifest> {
        let bytes = self.object_store.get(&meta.location).await?.bytes().await?;
        let manifest: PublishedAgentManifest = serde_json::from_slice(&bytes)?;
        Ok(manifest)
    }

    pub async fn load_latest(&self) -> StoreResult<Option<PublishedAgentManifest>> {
        let Some((_, meta)) = self.latest_pub_seq().await? else {
            return Ok(None);
        };
        Ok(Some(self.load_from_meta(&meta).await?))
    }

    pub async fn load_seq(&self, publish_seq: u64) -> StoreResult<PublishedAgentManifest> {
        let path = self.pub_path(publish_seq);
        let bytes = self.object_store.get(&path).await?.bytes().await?;
        let manifest: PublishedAgentManifest = serde_json::from_slice(&bytes)?;
        Ok(manifest)
    }

    /// Writes `pub_manifest` create-only at `pub_{publish_seq:012}.json`, verifying
    /// `head(publish_seq - 1)` when `publish_seq > 0` so a stale writer behind GC cannot slip past.
    pub async fn publish_next(
        &self,
        pub_manifest: PublishedAgentManifest,
    ) -> StoreResult<PublishedAgentManifest> {
        let seq = pub_manifest.publish_seq;
        let path = self.pub_path(seq);
        let payload = serde_json::to_vec(&pub_manifest)?;
        let opts = PutOptions::from(PutMode::Create);
        match self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(payload)), opts)
            .await
        {
            Ok(_) => {
                if seq > 0 {
                    match self.object_store.head(&self.pub_path(seq - 1)).await {
                        Ok(_) => {}
                        Err(OsError::NotFound { .. }) => {
                            let _ = self.object_store.delete(&path).await;
                            return Err(StoreError::ManifestConflict);
                        }
                        Err(other) => {
                            let _ = self.object_store.delete(&path).await;
                            return Err(StoreError::ObjectStore(other));
                        }
                    }
                }
                Ok(pub_manifest)
            }
            Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                Err(StoreError::ManifestConflict)
            }
            Err(other) => Err(StoreError::ObjectStore(other)),
        }
    }
}

fn parse_pub_seq_from_filename(fname: &str) -> Option<u64> {
    let body = fname.strip_prefix("pub_")?.strip_suffix(".json")?;
    body.parse::<u64>().ok()
}

/// One per-partition merged S3 output batch planned in Step 1 of the agent upload cycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentUploadIntentBatch {
    pub date: String,
    pub shard_id: u16,
    pub source_segment_ids: Vec<String>,
    pub source_object_paths: Vec<String>,
    pub target_segment_prefix: String,
}

/// Local intent record written at Step 1 of the agent upload cycle
/// (`agent_manifests/{project_id}/upload_intent.json`, excluded from `ManifestStore::list_epochs`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentUploadIntent {
    pub intent_id: String,
    pub writer_generation: u64,
    pub base_local_epoch: u64,
    pub target_publish_seq: u64,
    pub created_at_ms: i64,
    #[serde(default)]
    pub batches: Vec<AgentUploadIntentBatch>,
}

pub struct AgentUploadIntentStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl AgentUploadIntentStore {
    pub fn new(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> StoreResult<Self> {
        let project_id = project_id.into();
        validate_identifier("project_id", &project_id)?;
        Ok(Self {
            project_id,
            object_store,
        })
    }

    pub fn path(&self) -> ObjectPath {
        ObjectPath::from(format!(
            "agent_manifests/{}/upload_intent.json",
            self.project_id
        ))
    }

    pub async fn load(&self) -> StoreResult<Option<AgentUploadIntent>> {
        let path = self.path();
        match self.object_store.get(&path).await {
            Ok(res) => {
                let bytes = res.bytes().await?;
                let intent: AgentUploadIntent = serde_json::from_slice(&bytes)?;
                Ok(Some(intent))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(StoreError::ObjectStore(e)),
        }
    }

    pub async fn save(&self, intent: &AgentUploadIntent) -> StoreResult<()> {
        let payload = serde_json::to_vec(intent)?;
        self.object_store
            .put_opts(
                &self.path(),
                PutPayload::from_bytes(Bytes::from(payload)),
                PutOptions::from(PutMode::Overwrite),
            )
            .await?;
        Ok(())
    }

    pub async fn clear(&self) -> StoreResult<()> {
        match self.object_store.delete(&self.path()).await {
            Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
            Err(e) => Err(StoreError::ObjectStore(e)),
        }
    }
}

/// Installs a local [`AgentManifest`] restored from `pub_manifest` at `epoch = 1` with
/// `writer_generation = pub_manifest.writer_generation + 1` (used by `restore --from-published`).
/// Also reconciles any S3 objects under `agent_segments/{project_id}/` not referenced by
/// `pub_manifest.segments` into `pending_s3_deletes` so superseded S3 segments are cleaned up
/// instead of leaking indefinitely across volume loss.
pub async fn install_restored_agent_manifest(
    project_id: &str,
    pub_manifest: &PublishedAgentManifest,
    object_store: Arc<dyn ObjectStore>,
) -> StoreResult<AgentManifest> {
    let store = ManifestStore::new(project_id, object_store.clone(), AGENT_GC_GRACE_MS)?;
    let next_gen = pub_manifest.writer_generation.saturating_add(1);
    let mut restored = pub_manifest.to_local_manifest(0, next_gen);
    let referenced_s3: HashSet<&str> = pub_manifest
        .segments
        .iter()
        .map(|s| s.object_path.as_str())
        .collect();
    let seg_prefix = ObjectPath::from(format!("agent_segments/{project_id}/"));
    let metas: Vec<_> = object_store.list(Some(&seg_prefix)).try_collect().await?;
    for meta in metas {
        let path_str = meta.location.as_ref();
        if path_str.ends_with(".plss") && !referenced_s3.contains(path_str) {
            let seg_id = path_str
                .rsplit('/')
                .next()
                .and_then(|f| f.strip_suffix(".plss"))
                .unwrap_or(path_str)
                .to_string();
            restored.pending_s3_deletes.push(PendingAgentS3Delete {
                object_path: path_str.to_string(),
                segment_id: seg_id,
                superseding_pub_seq: pub_manifest.publish_seq,
                superseded_at_ms: Some(pub_manifest.published_at_ms),
                eligible_after_ms: None,
                urgent_erasure_deadline_ms: None,
                erasure_tombstone_ids: Vec::new(),
            });
        }
    }
    restored.epoch = 0; // `commit_next` increments to 1
    store.commit_next(restored).await
}

fn parse_epoch_from_path(path: &str) -> Option<u64> {
    let fname = path.rsplit('/').next()?;
    let body = fname.strip_prefix("epoch_")?.strip_suffix(".json")?;
    body.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn sample_seg(id: &str, path: &str) -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: id.to_string(),
            object_path: path.to_string(),
            date: "2026-11-19".to_string(),
            shard_id: 0,
            level: 0,
            file_size: 1024,
            tail_offset: 900,
            zone_map: SegmentZoneMap::default(),
            trace_count: 1,
            created_at_ms: 1_000,
        }
    }

    #[tokio::test]
    async fn manifest_cas_retries_on_conflict_and_fences_stale_writers() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ms = Arc::new(ManifestStore::new("proj_cas", os.clone(), AGENT_GC_GRACE_MS).unwrap());

        let m1 = ms
            .commit_next(AgentManifest {
                updated_at_ms: 1_000,
                segments: vec![sample_seg(
                    "s1",
                    "agent_segments/proj_cas/2026-11-19/shard_00/s1.plss",
                )],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(m1.epoch, 1);

        // Duplicate epoch 2 fails with ManifestConflict
        let c1 = ms.commit_next(m1.clone()).await.unwrap();
        assert_eq!(c1.epoch, 2);
        let dup = ms.commit_next(m1.clone()).await;
        assert!(matches!(dup, Err(StoreError::ManifestConflict)));

        // Stale writer fence: delete epoch 1, then attempt commit with epoch=1 (creating epoch 2 -> missing pred epoch 1? No: create epoch 3 with epoch 2 deleted!)
        let m3 = ms.commit_next(c1.clone()).await.unwrap();
        assert_eq!(m3.epoch, 3);
        os.delete(&ms.epoch_path(3)).await.unwrap();
        let stale = ms.commit_next(m3).await;
        assert!(matches!(stale, Err(StoreError::ManifestConflict)));
        assert!(matches!(
            os.head(&ms.epoch_path(4)).await,
            Err(OsError::NotFound { .. })
        ));

        // Concurrent CAS tasks all succeed without losing segments
        let mut handles = Vec::new();
        for i in 0..6u32 {
            let ms_c = ms.clone();
            handles.push(tokio::spawn(async move {
                ms_c.commit_with_cas(|m| {
                    m.segments.push(sample_seg(
                        &format!("seg_{i}"),
                        &format!("agent_segments/proj_cas/2026-11-19/shard_00/seg_{i}.plss"),
                    ));
                    Ok(true)
                })
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        let latest = ms.load_latest().await.unwrap();
        assert_eq!(latest.segments.len(), 7);
    }

    #[tokio::test]
    async fn pending_gc_enforces_15_minute_grace_period() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ms = ManifestStore::new("proj_gc", os.clone(), AGENT_GC_GRACE_MS).unwrap();
        let pins = ReaderPinTracker::default();

        let seg_path = "agent_segments/proj_gc/2026-11-19/shard_00/old.plss".to_string();
        os.put(
            &ObjectPath::from(seg_path.as_str()),
            PutPayload::from_static(b"PLSS"),
        )
        .await
        .unwrap();

        let superseded_at = 1_000_000i64;
        ms.commit_with_cas(|m| {
            m.pending_gc.push(PendingGcEntry {
                object_path: seg_path.clone(),
                segment_id: "old".to_string(),
                superseded_at_ms: superseded_at,
                eligible_after_ms: superseded_at + AGENT_GC_GRACE_MS,
                urgent_erasure_deadline_ms: None,
                erasure_tombstone_ids: Vec::new(),
            });
            Ok(true)
        })
        .await
        .unwrap();

        // Before 15 minutes: must NOT delete
        assert_eq!(
            ms.run_gc(superseded_at + AGENT_GC_GRACE_MS - 1, &pins)
                .await
                .unwrap(),
            0
        );
        assert!(os.head(&ObjectPath::from(seg_path.as_str())).await.is_ok());

        // At 15 minutes while pinned: must NOT delete
        pins.pin_all(std::slice::from_ref(&seg_path));
        assert_eq!(
            ms.run_gc(superseded_at + AGENT_GC_GRACE_MS, &pins)
                .await
                .unwrap(),
            0
        );
        assert!(os.head(&ObjectPath::from(seg_path.as_str())).await.is_ok());

        // Unpinned and past 15 minutes: deletes object and clears pending_gc
        pins.unpin_all(std::slice::from_ref(&seg_path));
        assert_eq!(
            ms.run_gc(superseded_at + AGENT_GC_GRACE_MS, &pins)
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            os.head(&ObjectPath::from(seg_path.as_str())).await,
            Err(OsError::NotFound { .. })
        ));
        let final_m = ms.load_latest().await.unwrap();
        assert!(final_m.pending_gc.is_empty());
    }
}
