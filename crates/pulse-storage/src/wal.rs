use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use parking_lot::Mutex;
use pulse_wire::WireBatch;

use crate::error::{Result, StorageError};

/// V2 WAL frame magic covering `(epoch || seq || len || payload)` under CRC32.
pub const WAL_MAGIC: [u8; 4] = *b"PWL2";
/// V3 WAL frame magic carrying `(writer_id, offset, seen_ts_ms)` + `WireBatch` under CRC32 (`W3`).
pub const WAL_MAGIC_V3: [u8; 4] = *b"PWL3";
pub const WAL_HEADER_LEN: usize = 28;
const MAX_WAL_WRITER_ID_LEN: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalWriterOffset {
    pub writer_id: String,
    pub offset: u64,
    pub seen_ts_ms: i64,
}

#[derive(Debug, Clone)]
pub struct DecodedWalFrame {
    pub epoch: u64,
    pub seq: u64,
    pub batch: WireBatch,
    pub writer_offset: Option<WalWriterOffset>,
}

/// Durable per-bucket Write-Ahead Log backed by `ObjectStore` with header+payload CRC32 verification,
/// writer-epoch fencing, transient GET retries, and corrupt-frame quarantine.
pub struct BucketWal {
    project_id: String,
    bucket_id: u8,
    epoch: AtomicU64,
    min_valid_epoch: AtomicU64,
    next_seq: AtomicU64,
    object_store: Arc<dyn ObjectStore>,
    node_wal_bytes: Option<Arc<AtomicU64>>,
    live_file_sizes: Mutex<HashMap<ObjectPath, u64>>,
}

impl Drop for BucketWal {
    fn drop(&mut self) {
        if let Some(counter) = &self.node_wal_bytes {
            let rem: u64 = self.live_file_sizes.get_mut().values().sum();
            if rem > 0 {
                counter.fetch_sub(rem, Ordering::Relaxed);
            }
        }
    }
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
            node_wal_bytes: None,
            live_file_sizes: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_wal_bytes_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.node_wal_bytes = Some(counter);
        self
    }

    fn record_live_file(&self, path: ObjectPath, size: u64) {
        let mut sizes = self.live_file_sizes.lock();
        let prev = sizes.insert(path, size).unwrap_or(0);
        if let Some(counter) = &self.node_wal_bytes {
            if size >= prev {
                counter.fetch_add(size - prev, Ordering::Relaxed);
            } else {
                counter.fetch_sub(prev - size, Ordering::Relaxed);
            }
        }
    }

    fn unrecord_live_file(&self, path: &ObjectPath) {
        let mut sizes = self.live_file_sizes.lock();
        if let Some(prev) = sizes.remove(path) {
            if let Some(counter) = &self.node_wal_bytes {
                counter.fetch_sub(prev, Ordering::Relaxed);
            }
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
        self.append_with_offset(batch, None).await
    }

    /// Appends a WAL record, optionally encoding a `WalWriterOffset` (`PWL3`) inside the CRC32-covered payload.
    pub async fn append_with_offset(
        &self,
        batch: &WireBatch,
        writer_offset: Option<&WalWriterOffset>,
    ) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let epoch = self.epoch.load(Ordering::Acquire);
        let batch_bytes = batch.encode_binary(1)?;

        let (magic, payload) = match writer_offset {
            None => (WAL_MAGIC, batch_bytes),
            Some(wo) => {
                let wid_bytes = wo.writer_id.as_bytes();
                if wid_bytes.is_empty() || wid_bytes.len() > MAX_WAL_WRITER_ID_LEN {
                    return Err(StorageError::Corrupted(format!(
                        "invalid WAL writer_id length: {}",
                        wid_bytes.len()
                    )));
                }
                let wid_len = wid_bytes.len() as u32;
                let mut payload =
                    Vec::with_capacity(4 + wid_bytes.len() + 8 + 8 + batch_bytes.len());
                payload.extend_from_slice(&wid_len.to_le_bytes());
                payload.extend_from_slice(wid_bytes);
                payload.extend_from_slice(&wo.offset.to_le_bytes());
                payload.extend_from_slice(&wo.seen_ts_ms.to_le_bytes());
                payload.extend_from_slice(&batch_bytes);
                (WAL_MAGIC_V3, payload)
            }
        };

        let len_u32 = u32::try_from(payload.len())
            .map_err(|_| StorageError::Corrupted("WAL payload exceeds 4 GiB".to_string()))?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&epoch.to_le_bytes());
        hasher.update(&seq.to_le_bytes());
        hasher.update(&len_u32.to_le_bytes());
        hasher.update(&payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
        frame.extend_from_slice(&magic);
        frame.extend_from_slice(&epoch.to_le_bytes());
        frame.extend_from_slice(&seq.to_le_bytes());
        frame.extend_from_slice(&len_u32.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        let path = self.object_path(epoch, seq);
        let frame_len = frame.len() as u64;
        // Create-only: a second writer on this log fails loudly instead of replacing a record.
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(frame)), opts)
            .await
        {
            Ok(_) => {
                self.record_live_file(path, frame_len);
                Ok(seq)
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                tracing::error!(
                    project_id = %self.project_id,
                    bucket_id = self.bucket_id,
                    wal_path = %path,
                    "WAL object already exists: another node is writing this project"
                );
                Err(StorageError::NotWriter {
                    project_id: self.project_id.clone(),
                })
            }
            Err(e) => Err(e.into()),
        }
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
            && self.object_store.delete(loc).await.is_ok()
        {
            self.unrecord_live_file(loc);
        }
    }

    /// Replays all unfenced WAL frames with `seq > committed_seq` in deterministic `(epoch, seq)` order,
    /// verifying header+payload CRC32 checksums and quarantining corrupt frames.
    pub async fn recover(&self, committed_seq: u64) -> Result<Vec<(u64, WireBatch)>> {
        let frames = self.recover_with_offsets(committed_seq).await?;
        Ok(frames.into_iter().map(|f| (f.seq, f.batch)).collect())
    }

    /// Replays all unfenced WAL frames (`PWL2` and `PWL3`) with `seq > committed_seq`, preserving
    /// any embedded [`WalWriterOffset`] metadata.
    pub async fn recover_with_offsets(&self, committed_seq: u64) -> Result<Vec<DecodedWalFrame>> {
        let min_epoch = self.min_valid_epoch.load(Ordering::Acquire);
        let prefix = ObjectPath::from(format!(
            "wal/{}/bucket_{:03}/",
            self.project_id, self.bucket_id
        ));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut candidates: Vec<(u64, u64, ObjectPath)> = Vec::new();

        for meta in metas {
            if let Some((ep, seq)) = parse_epoch_and_seq_from_wal_path(meta.location.as_ref()) {
                self.record_live_file(meta.location.clone(), meta.size);
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
            match decode_wal_frame_full(&bytes) {
                Ok(frame) => {
                    if frame.epoch != path_epoch || frame.seq != path_seq {
                        let mismatch = StorageError::Corrupted(format!(
                            "WAL path ({path_epoch}, {path_seq}) mismatches frame header ({}, {})",
                            frame.epoch, frame.seq
                        ));
                        self.quarantine_corrupt_frame(&loc, &bytes, &mismatch).await;
                        continue;
                    }
                    if frame.epoch < min_epoch {
                        continue;
                    }
                    max_seen = max_seen.max(frame.seq);
                    out.push(frame);
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
                    if self.object_store.delete(&meta.location).await.is_ok() {
                        self.unrecord_live_file(&meta.location);
                    }
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

pub fn decode_wal_frame_full(bytes: &[u8]) -> Result<DecodedWalFrame> {
    if bytes.len() < WAL_HEADER_LEN {
        return Err(StorageError::Corrupted(format!(
            "WAL frame shorter than {WAL_HEADER_LEN}-byte header"
        )));
    }
    let magic = &bytes[0..4];
    if magic != WAL_MAGIC && magic != WAL_MAGIC_V3 {
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

    if magic == WAL_MAGIC {
        let batch = WireBatch::decode_binary(payload)?;
        return Ok(DecodedWalFrame {
            epoch,
            seq,
            batch,
            writer_offset: None,
        });
    }

    // PWL3 payload: writer_id_len(u32 LE) || writer_id || offset(u64 LE) || seen_ts_ms(i64 LE) || PLSB
    if payload.len() < 4 + 8 + 8 {
        return Err(StorageError::Corrupted(
            "PWL3 WAL payload truncated before writer offset metadata".to_string(),
        ));
    }
    let wid_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    if wid_len == 0 || wid_len > MAX_WAL_WRITER_ID_LEN || payload.len() < 4 + wid_len + 16 {
        return Err(StorageError::Corrupted(
            "PWL3 WAL writer_id length invalid or truncated".to_string(),
        ));
    }
    let wid_end = 4 + wid_len;
    let writer_id = std::str::from_utf8(&payload[4..wid_end])
        .map_err(|e| StorageError::Corrupted(format!("PWL3 writer_id UTF-8 error: {e}")))?
        .to_string();
    let offset = u64::from_le_bytes(payload[wid_end..wid_end + 8].try_into().unwrap());
    let seen_ts_ms = i64::from_le_bytes(payload[wid_end + 8..wid_end + 16].try_into().unwrap());
    let batch = WireBatch::decode_binary(&payload[wid_end + 16..])?;
    Ok(DecodedWalFrame {
        epoch,
        seq,
        batch,
        writer_offset: Some(WalWriterOffset {
            writer_id,
            offset,
            seen_ts_ms,
        }),
    })
}

pub fn decode_wal_frame_with_epoch(bytes: &[u8]) -> Result<(u64, u64, WireBatch)> {
    let frame = decode_wal_frame_full(bytes)?;
    Ok((frame.epoch, frame.seq, frame.batch))
}

pub fn decode_wal_frame(bytes: &[u8]) -> Result<(u64, WireBatch)> {
    let frame = decode_wal_frame_full(bytes)?;
    Ok((frame.seq, frame.batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use pulse_wire::WireEvent;

    #[tokio::test]
    async fn pwl3_and_pwl2_roundtrip_and_crc_tamper_quarantine() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal = BucketWal::new("proj_wal", 0, 1, 0, store.clone());

        let ev = WireEvent {
            event_uuid: [7u8; 16],
            routing_seed: "u1".into(),
            distinct_id: "u1".into(),
            session_id: "s1".into(),
            event_name: "Click".into(),
            client_event_ts_ms: 1_790_000_000_000,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        };
        let b1 = WireBatch::new(vec![ev]);
        let seq1 = wal.append(&b1).await.unwrap();
        assert_eq!(seq1, 1);

        let wo = WalWriterOffset {
            writer_id: "writer-uuid-1".into(),
            offset: 0,
            seen_ts_ms: 1_790_000_001_000,
        };
        let seq2 = wal.append_with_offset(&b1, Some(&wo)).await.unwrap();
        assert_eq!(seq2, 2);

        // Empty PWL3 offset marker also roundtrips cleanly.
        let wo_empty = WalWriterOffset {
            writer_id: "writer-uuid-1".into(),
            offset: 1,
            seen_ts_ms: 1_790_000_002_000,
        };
        let seq3 = wal
            .append_with_offset(&WireBatch::new(vec![]), Some(&wo_empty))
            .await
            .unwrap();
        assert_eq!(seq3, 3);

        let recovered = wal.recover_with_offsets(0).await.unwrap();
        assert_eq!(recovered.len(), 3);
        assert!(recovered[0].writer_offset.is_none());
        assert_eq!(recovered[1].writer_offset.as_ref(), Some(&wo));
        assert_eq!(recovered[2].writer_offset.as_ref(), Some(&wo_empty));
        assert!(recovered[2].batch.events.is_empty());

        // Legacy decode_wal_frame decodes PWL3 too.
        let raw3 = store
            .get(&ObjectPath::from(
                "wal/proj_wal/bucket_000/epoch_0000000001_seq_000000000002.wal",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let (dec_seq, dec_batch) = decode_wal_frame(&raw3).unwrap();
        assert_eq!(dec_seq, 2);
        assert_eq!(dec_batch.len(), 1);

        // Tamper a byte in PWL3 payload -> quarantined on recover_with_offsets.
        let mut tampered = raw3.to_vec();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xFF;
        store
            .put(
                &ObjectPath::from("wal/proj_wal/bucket_000/epoch_0000000001_seq_000000000002.wal"),
                PutPayload::from(tampered),
            )
            .await
            .unwrap();

        let wal2 = BucketWal::new("proj_wal", 0, 1, 0, store.clone());
        let recovered2 = wal2.recover_with_offsets(0).await.unwrap();
        assert_eq!(recovered2.len(), 2);
        assert!(store
            .head(&ObjectPath::from(
                "wal_corrupt/proj_wal/bucket_000/epoch_0000000001_seq_000000000002.wal"
            ))
            .await
            .is_ok());
    }
}
