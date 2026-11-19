use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use pulse_wire::WireBatch;

use crate::error::{Result, StorageError};

/// V2 WAL frame magic covering `(epoch || seq || len || payload)` under CRC32.
pub const WAL_MAGIC: [u8; 4] = *b"PWL2";
pub const WAL_HEADER_LEN: usize = 28;

/// Durable per-bucket Write-Ahead Log backed by `ObjectStore` with header+payload CRC32 verification,
/// writer-epoch fencing, transient GET retries, and corrupt-frame quarantine.
pub struct BucketWal {
    project_id: String,
    bucket_id: u8,
    epoch: AtomicU64,
    min_valid_epoch: AtomicU64,
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
            min_valid_epoch: AtomicU64::new(0),
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

    /// Fences out any WAL frames written by stale writers with `epoch < min_epoch`.
    pub fn set_min_valid_epoch(&self, min_epoch: u64) {
        self.min_valid_epoch.fetch_max(min_epoch, Ordering::AcqRel);
        self.epoch.fetch_max(min_epoch, Ordering::AcqRel);
    }

    fn object_path(&self, epoch: u64, seq: u64) -> ObjectPath {
        ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/epoch_{:010}_seq_{:012}.wal",
            self.project_id, self.bucket_id, epoch, seq
        ))
    }

    fn quarantine_path(&self, filename: &str) -> ObjectPath {
        ObjectPath::from(format!(
            "wal_corrupt/{}/bucket_{:03}/{}",
            self.project_id, self.bucket_id, filename
        ))
    }

    /// Appends a length-prefixed, header+payload CRC32-checked `WireBatch` record durably BEFORE returning `Ok(wal_seq)`.
    pub async fn append(&self, batch: &WireBatch) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let epoch = self.epoch.load(Ordering::Acquire);
        let payload = batch.encode_binary(1)?;
        let len_u32 = u32::try_from(payload.len())
            .map_err(|_| StorageError::Corrupted("WAL payload exceeds 4 GiB".to_string()))?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&epoch.to_le_bytes());
        hasher.update(&seq.to_le_bytes());
        hasher.update(&len_u32.to_le_bytes());
        hasher.update(&payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
        frame.extend_from_slice(&WAL_MAGIC);
        frame.extend_from_slice(&epoch.to_le_bytes());
        frame.extend_from_slice(&seq.to_le_bytes());
        frame.extend_from_slice(&len_u32.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        let path = self.object_path(epoch, seq);
        self.object_store
            .put(&path, PutPayload::from_bytes(Bytes::from(frame)))
            .await?;
        Ok(seq)
    }

    async fn get_bytes_with_retry(&self, loc: &ObjectPath) -> Result<Option<Bytes>> {
        let mut last_err: Option<object_store::Error> = None;
        for attempt in 0..3u8 {
            match self.object_store.get(loc).await {
                Ok(res) => match res.bytes().await {
                    Ok(b) => return Ok(Some(b)),
                    Err(e) => last_err = Some(e),
                },
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => last_err = Some(e),
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(10 * (u64::from(attempt) + 1))).await;
            }
        }
        Err(StorageError::ObjectStore(last_err.unwrap()))
    }

    async fn quarantine_corrupt_frame(&self, loc: &ObjectPath, raw: &[u8], reason: &StorageError) {
        let filename = loc.as_ref().rsplit('/').next().unwrap_or("unknown.wal");
        let qpath = self.quarantine_path(filename);
        tracing::error!(
            project_id = %self.project_id,
            bucket_id = self.bucket_id,
            wal_path = %loc,
            quarantine_path = %qpath,
            error = %reason,
            "quarantining corrupt WAL frame so project recovery can proceed"
        );
        if self
            .object_store
            .put(&qpath, PutPayload::from_bytes(Bytes::copy_from_slice(raw)))
            .await
            .is_ok()
        {
            let _ = self.object_store.delete(loc).await;
        }
    }

    /// Replays all unfenced WAL frames with `seq > committed_seq` in deterministic `(epoch, seq)` order,
    /// verifying header+payload CRC32 checksums and quarantining corrupt frames.
    pub async fn recover(&self, committed_seq: u64) -> Result<Vec<(u64, WireBatch)>> {
        let min_epoch = self.min_valid_epoch.load(Ordering::Acquire);
        let prefix = ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/",
            self.project_id, self.bucket_id
        ));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut candidates: Vec<(u64, u64, ObjectPath)> = Vec::new();

        for meta in metas {
            if let Some((ep, seq)) = parse_epoch_and_seq_from_wal_path(meta.location.as_ref()) {
                if ep >= min_epoch && seq > committed_seq {
                    candidates.push((ep, seq, meta.location));
                }
            }
        }
        candidates.sort_by_key(|(ep, seq, _)| (*ep, *seq));

        let mut max_seen = committed_seq;
        let mut out = Vec::with_capacity(candidates.len());
        for (path_epoch, path_seq, loc) in candidates {
            max_seen = max_seen.max(path_seq);
            let Some(bytes) = self.get_bytes_with_retry(&loc).await? else {
                continue;
            };
            match decode_wal_frame_with_epoch(&bytes) {
                Ok((decoded_epoch, decoded_seq, batch)) => {
                    if decoded_epoch != path_epoch || decoded_seq != path_seq {
                        let mismatch = StorageError::Corrupted(format!(
                            "WAL path ({path_epoch}, {path_seq}) mismatches frame header ({decoded_epoch}, {decoded_seq})"
                        ));
                        self.quarantine_corrupt_frame(&loc, &bytes, &mismatch).await;
                        continue;
                    }
                    if decoded_epoch < min_epoch {
                        continue;
                    }
                    max_seen = max_seen.max(decoded_seq);
                    out.push((decoded_seq, batch));
                }
                Err(
                    err @ (StorageError::Corrupted(_)
                    | StorageError::WalChecksumMismatch { .. }
                    | StorageError::Wire(_)),
                ) => {
                    self.quarantine_corrupt_frame(&loc, &bytes, &err).await;
                }
                Err(other) => return Err(other),
            }
        }

        self.next_seq.fetch_max(max_seen + 1, Ordering::AcqRel);
        Ok(out)
    }

    /// Garbage-collects WAL records whose `seq <= flushed_seq` or `epoch < min_valid_epoch` after an L1 flush commits.
    pub async fn purge_up_to(&self, flushed_seq: u64) -> Result<usize> {
        let min_epoch = self.min_valid_epoch.load(Ordering::Acquire);
        let prefix = ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/",
            self.project_id, self.bucket_id
        ));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut deleted = 0usize;
        for meta in metas {
            if let Some((ep, seq)) = parse_epoch_and_seq_from_wal_path(meta.location.as_ref()) {
                if seq <= flushed_seq || ep < min_epoch {
                    let _ = self.object_store.delete(&meta.location).await;
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }
}

pub fn parse_epoch_and_seq_from_wal_path(path: &str) -> Option<(u64, u64)> {
    let fname = path.rsplit('/').next()?;
    let without_ext = fname.strip_suffix(".wal")?;
    let rest = without_ext.strip_prefix("epoch_")?;
    let (epoch_str, seq_str) = rest.split_once("_seq_")?;
    let epoch = epoch_str.parse::<u64>().ok()?;
    let seq = seq_str.parse::<u64>().ok()?;
    Some((epoch, seq))
}

pub fn decode_wal_frame_with_epoch(bytes: &[u8]) -> Result<(u64, u64, WireBatch)> {
    if bytes.len() < WAL_HEADER_LEN {
        return Err(StorageError::Corrupted(format!(
            "WAL frame shorter than {WAL_HEADER_LEN}-byte header"
        )));
    }
    if bytes[0..4] != WAL_MAGIC {
        return Err(StorageError::Corrupted("invalid WAL magic".to_string()));
    }
    let mut epoch_arr = [0u8; 8];
    epoch_arr.copy_from_slice(&bytes[4..12]);
    let epoch = u64::from_le_bytes(epoch_arr);

    let mut seq_arr = [0u8; 8];
    seq_arr.copy_from_slice(&bytes[12..20]);
    let seq = u64::from_le_bytes(seq_arr);

    let len = u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]) as usize;
    let expected_crc = u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);

    if bytes.len() != WAL_HEADER_LEN + len {
        return Err(StorageError::Corrupted(
            "WAL payload length mismatch".to_string(),
        ));
    }
    let payload = &bytes[WAL_HEADER_LEN..WAL_HEADER_LEN + len];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[4..24]); // epoch (8B) || seq (8B) || len (4B)
    hasher.update(payload);
    let actual_crc = hasher.finalize();

    if actual_crc != expected_crc {
        return Err(StorageError::WalChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let batch = WireBatch::decode_binary(payload)?;
    Ok((epoch, seq, batch))
}

pub fn decode_wal_frame(bytes: &[u8]) -> Result<(u64, WireBatch)> {
    let (_epoch, seq, batch) = decode_wal_frame_with_epoch(bytes)?;
    Ok((seq, batch))
}
