use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use pulse_wire::WireBatch;

use crate::error::{Result, StorageError};

pub const WAL_MAGIC: [u8; 4] = *b"PWAL";

/// Durable per-bucket Write-Ahead Log backed by `ObjectStore` with CRC32 frame verification.
pub struct BucketWal {
    project_id: String,
    bucket_id: u8,
    epoch: AtomicU64,
    next_seq: AtomicU64,
    object_store: Arc<dyn ObjectStore>,
}

impl BucketWal {
    pub fn new(
        project_id: impl Into<String>,
        bucket_id: u8,
        initial_epoch: u64,
        committed_seq: u64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            bucket_id,
            epoch: AtomicU64::new(initial_epoch),
            next_seq: AtomicU64::new(committed_seq + 1),
            object_store,
        }
    }

    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire).saturating_sub(1)
    }

    pub fn advance_epoch(&self, new_epoch: u64) {
        self.epoch.fetch_max(new_epoch, Ordering::AcqRel);
    }

    fn object_path(&self, epoch: u64, seq: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/epoch_{:010}_seq_{:012}.wal",
            self.project_id, self.bucket_id, epoch, seq
        ))
    }

    /// Appends a length-prefixed, CRC32-checked `WireBatch` record durably BEFORE returning `Ok(wal_seq)`.
    pub async fn append(&self, batch: &WireBatch) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let epoch = self.epoch.load(Ordering::Acquire);
        let payload = batch.encode_binary(1)?;
        let crc = crc32fast::hash(&payload);

        let mut frame = Vec::with_capacity(20 + payload.len());
        frame.extend_from_slice(&WAL_MAGIC);
        frame.extend_from_slice(&seq.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        let path = self.object_path(epoch, seq);
        self.object_store
            .put(&path, PutPayload::from_bytes(Bytes::from(frame)))
            .await?;
        Ok(seq)
    }

    /// Replays all WAL frames with `seq > committed_seq` in deterministic order, verifying CRC32 checksums.
    pub async fn recover(&self, committed_seq: u64) -> Result<Vec<(u64, WireBatch)>> {
        let prefix = ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/",
            self.project_id, self.bucket_id
        ));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut candidates: Vec<(u64, ObjectPath)> = Vec::new();

        for meta in metas {
            if let Some(seq) = parse_seq_from_wal_path(meta.location.as_ref()) {
                if seq > committed_seq {
                    candidates.push((seq, meta.location));
                }
            }
        }
        candidates.sort_by_key(|(seq, _)| *seq);

        let mut max_seen = committed_seq;
        let mut out = Vec::with_capacity(candidates.len());
        for (seq, loc) in candidates {
            let bytes = self.object_store.get(&loc).await?.bytes().await?;
            let (decoded_seq, batch) = decode_wal_frame(&bytes)?;
            let effective_seq = decoded_seq.max(seq);
            max_seen = max_seen.max(effective_seq);
            out.push((effective_seq, batch));
        }

        self.next_seq.fetch_max(max_seen + 1, Ordering::AcqRel);
        Ok(out)
    }

    /// Garbage-collects WAL records whose `seq <= flushed_seq` after an L1 segment flush commits.
    pub async fn purge_up_to(&self, flushed_seq: u64) -> Result<usize> {
        let prefix = ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/",
            self.project_id, self.bucket_id
        ));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut deleted = 0usize;
        for meta in metas {
            if let Some(seq) = parse_seq_from_wal_path(meta.location.as_ref()) {
                if seq <= flushed_seq {
                    let _ = self.object_store.delete(&meta.location).await;
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }
}

fn parse_seq_from_wal_path(path: &str) -> Option<u64> {
    let fname = path.rsplit('/').next()?;
    let without_ext = fname.strip_suffix(".wal")?;
    let (_, seq_str) = without_ext.rsplit_once("_seq_")?;
    seq_str.parse::<u64>().ok()
}

pub fn decode_wal_frame(bytes: &[u8]) -> Result<(u64, WireBatch)> {
    if bytes.len() < 20 {
        return Err(StorageError::Corrupted(
            "WAL frame shorter than 20-byte header".to_string(),
        ));
    }
    if bytes[0..4] != WAL_MAGIC {
        return Err(StorageError::Corrupted("invalid WAL magic".to_string()));
    }
    let mut seq_arr = [0u8; 8];
    seq_arr.copy_from_slice(&bytes[4..12]);
    let seq = u64::from_le_bytes(seq_arr);

    let len = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
    let expected_crc = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

    if bytes.len() < 20 + len {
        return Err(StorageError::Corrupted("WAL payload truncated".to_string()));
    }
    let payload = &bytes[20..20 + len];
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return Err(StorageError::WalChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let batch = WireBatch::decode_binary(payload)?;
    Ok((seq, batch))
}
