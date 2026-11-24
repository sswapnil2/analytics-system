use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::model::TraceIngestBatch;
use crate::protocol::{
    decode_plst_batch, encode_plst_batch, MAX_PLST_SPANS, MAX_PLST_TRACES, MAX_PLST_UNCOMP_BYTES,
};
use crate::store::error::{validate_identifier, StoreError, StoreResult};
use crate::store::memtable::{approx_header_bytes, approx_span_bytes, ShardedMemTable};

pub const PTWL_MAGIC: [u8; 4] = *b"PTWL";
pub const PTWL_VERSION: u8 = 1;
pub const PTWL_HEADER_LEN: usize = 32;
pub const PTWL_FLAG_PLST: u8 = 0x01;
pub const PTWL_FLAG_WRITER_OFFSETS: u8 = 0x02;
pub const MAX_PTWL_PAYLOAD_BYTES: usize = MAX_PLST_UNCOMP_BYTES;

pub const DEFAULT_WAL_GROUP_COMMIT_MS: u64 = 10;
pub const MIN_WAL_GROUP_COMMIT_MS: u64 = 5;
pub const MAX_WAL_GROUP_COMMIT_MS: u64 = 25;
pub const DEFAULT_WAL_MAX_GROUP_ITEMS: usize = 64;
pub const DEFAULT_WAL_MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_WAL_QUEUE_CAP: usize = 4_096;

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub group_commit_ms: u64,
    pub max_group_items: usize,
    pub max_group_bytes: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            group_commit_ms: DEFAULT_WAL_GROUP_COMMIT_MS,
            max_group_items: DEFAULT_WAL_MAX_GROUP_ITEMS,
            max_group_bytes: DEFAULT_WAL_MAX_GROUP_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalCommitResult {
    pub writer_id: String,
    pub wal_seq: u64,
}

#[derive(Debug, Clone)]
pub struct DecodedWalFrame {
    pub seq: u64,
    pub timestamp_ms: i64,
    pub batch: TraceIngestBatch,
    pub writer_offsets: Vec<(String, u64, i64)>,
}

pub fn wal_object_path(project_id: &str, writer_id: &str, seq: u64) -> StoreResult<ObjectPath> {
    validate_identifier("project_id", project_id)?;
    validate_identifier("writer_id", writer_id)?;
    Ok(ObjectPath::from(format!(
        "agent_wal/{project_id}/{writer_id}/seq_{seq:012}.ptwl"
    )))
}

pub fn wal_corrupt_path(
    project_id: &str,
    writer_id: &str,
    filename: &str,
) -> StoreResult<ObjectPath> {
    validate_identifier("project_id", project_id)?;
    validate_identifier("writer_id", writer_id)?;
    validate_identifier("filename", filename)?;
    Ok(ObjectPath::from(format!(
        "agent_wal_corrupt/{project_id}/{writer_id}/{filename}"
    )))
}

/// Encodes a single `PTWL` v1 frame with exact 32-byte little-endian header:
/// `magic("PTWL", 4) | version(1) | flags(1) | reserved(2) | seq(u64) | timestamp_ms(i64) | payload_len(u32) | crc32(u32)`
/// where `crc32` covers `bytes[4..28] || payload[0..payload_len]`.
pub fn encode_ptwl_frame(
    seq: u64,
    timestamp_ms: i64,
    batch: &TraceIngestBatch,
) -> StoreResult<Vec<u8>> {
    encode_ptwl_frame_with_offsets(seq, timestamp_ms, batch, &[])
}

/// Encodes a `PTWL` frame optionally carrying `(stateless_writer_id, batch_offset, seen_ts_ms)`
/// offsets (`PTWL_FLAG_WRITER_OFFSETS`). When `writer_offsets` is empty, emits a pure v1 `PTWL_FLAG_PLST` frame.
pub fn encode_ptwl_frame_with_offsets(
    seq: u64,
    timestamp_ms: i64,
    batch: &TraceIngestBatch,
    writer_offsets: &[(String, u64, i64)],
) -> StoreResult<Vec<u8>> {
    let plst = encode_plst_batch(batch, true)?;
    let (flags, payload) = if writer_offsets.is_empty() {
        (PTWL_FLAG_PLST, plst)
    } else {
        let plst_len = u32::try_from(plst.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("PTWL PLST length overflow"))?;
        let count = u32::try_from(writer_offsets.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("PTWL writer_offsets overflow"))?;
        let mut buf = Vec::with_capacity(8 + plst.len() + writer_offsets.len() * 24);
        buf.extend_from_slice(&plst_len.to_le_bytes());
        buf.extend_from_slice(&plst);
        buf.extend_from_slice(&count.to_le_bytes());
        for (wid, offset, seen_ts_ms) in writer_offsets {
            let wid_bytes = wid.as_bytes();
            let wid_len = u16::try_from(wid_bytes.len()).map_err(|_| {
                StoreError::InvalidIdentifier("writer_id exceeds 65535 bytes".into())
            })?;
            buf.extend_from_slice(&wid_len.to_le_bytes());
            buf.extend_from_slice(wid_bytes);
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&seen_ts_ms.to_le_bytes());
        }
        (PTWL_FLAG_PLST | PTWL_FLAG_WRITER_OFFSETS, buf)
    };
    if payload.len() > MAX_PTWL_PAYLOAD_BYTES {
        return Err(StoreError::ResourceLimitExceeded(
            "PTWL payload exceeds 64 MiB limit",
        ));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("PTWL payload_len overflow"))?;

    let mut frame = Vec::with_capacity(PTWL_HEADER_LEN + payload.len());
    frame.extend_from_slice(&PTWL_MAGIC);
    frame.push(PTWL_VERSION);
    frame.push(flags);
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.extend_from_slice(&timestamp_ms.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&frame[4..28]);
    hasher.update(&payload);
    let crc = hasher.finalize();

    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub type WalWriterOffset = (String, u64, i64);

fn decode_writer_offsets_payload(payload: &[u8]) -> Result<(&[u8], Vec<WalWriterOffset>), String> {
    if payload.len() < 8 {
        return Err("truncated PTWL writer_offsets header".to_string());
    }
    let plst_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut pos = 4usize;
    let Some(plst_end) = pos.checked_add(plst_len) else {
        return Err("PTWL plst_len overflow".to_string());
    };
    if plst_end + 4 > payload.len() {
        return Err("truncated PTWL writer_offsets PLST slice".to_string());
    }
    let plst_slice = &payload[pos..plst_end];
    pos = plst_end;
    let count = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut offsets = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if pos + 2 > payload.len() {
            return Err("truncated PTWL writer_offset entry".to_string());
        }
        let wid_len = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if wid_len == 0 || pos + wid_len + 16 > payload.len() {
            return Err("invalid PTWL writer_id length".to_string());
        }
        let wid = std::str::from_utf8(&payload[pos..pos + wid_len])
            .map_err(|e| format!("invalid writer_id utf8 in PTWL: {e}"))?
            .to_string();
        pos += wid_len;
        let offset = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let seen_ts_ms = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        offsets.push((wid, offset, seen_ts_ms));
    }
    if pos != payload.len() {
        return Err("trailing bytes in PTWL writer_offsets payload".to_string());
    }
    Ok((plst_slice, offsets))
}

/// Decodes one or more `PTWL` v1 frames from `bytes`, tolerating truncated/corrupt tail entries
/// without panicking and returning any valid prefix frames decoded before the corruption.
pub fn decode_ptwl_frames_tolerant(bytes: &[u8]) -> (Vec<DecodedWalFrame>, Option<String>) {
    let mut frames = Vec::new();
    let mut pos = 0usize;

    while pos < bytes.len() {
        let rem = bytes.len() - pos;
        if rem < PTWL_HEADER_LEN {
            return (
                frames,
                Some(format!(
                    "truncated PTWL header at offset {pos} ({rem}B remaining)"
                )),
            );
        }
        let hdr = &bytes[pos..pos + PTWL_HEADER_LEN];
        if hdr[0..4] != PTWL_MAGIC {
            return (frames, Some(format!("invalid PTWL magic at offset {pos}")));
        }
        let version = hdr[4];
        if version != PTWL_VERSION {
            return (
                frames,
                Some(format!(
                    "unsupported PTWL version {version} at offset {pos}"
                )),
            );
        }
        let flags = hdr[5];
        if (flags & !(PTWL_FLAG_PLST | PTWL_FLAG_WRITER_OFFSETS)) != 0
            || (flags & PTWL_FLAG_PLST) == 0
        {
            return (
                frames,
                Some(format!("invalid PTWL flags 0x{flags:02x} at offset {pos}")),
            );
        }
        let seq = u64::from_le_bytes([
            hdr[8], hdr[9], hdr[10], hdr[11], hdr[12], hdr[13], hdr[14], hdr[15],
        ]);
        let timestamp_ms = i64::from_le_bytes([
            hdr[16], hdr[17], hdr[18], hdr[19], hdr[20], hdr[21], hdr[22], hdr[23],
        ]);
        let payload_len = u32::from_le_bytes([hdr[24], hdr[25], hdr[26], hdr[27]]) as usize;
        let expected_crc = u32::from_le_bytes([hdr[28], hdr[29], hdr[30], hdr[31]]);

        if payload_len > MAX_PTWL_PAYLOAD_BYTES {
            return (
                frames,
                Some(format!(
                    "PTWL payload_len {payload_len} exceeds max at offset {pos}"
                )),
            );
        }
        let Some(payload_start) = pos.checked_add(PTWL_HEADER_LEN) else {
            return (frames, Some("PTWL offset overflow".to_string()));
        };
        let Some(payload_end) = payload_start.checked_add(payload_len) else {
            return (frames, Some("PTWL payload end overflow".to_string()));
        };
        if payload_end > bytes.len() {
            return (
                frames,
                Some(format!("truncated PTWL payload at offset {pos}")),
            );
        }

        let payload = &bytes[payload_start..payload_end];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[pos + 4..pos + 28]);
        hasher.update(payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return (
                frames,
                Some(format!(
                    "PTWL CRC32 mismatch at offset {pos}: expected 0x{expected_crc:08x}, got 0x{actual_crc:08x}"
                )),
            );
        }

        let (plst_slice, writer_offsets) = if (flags & PTWL_FLAG_WRITER_OFFSETS) != 0 {
            match decode_writer_offsets_payload(payload) {
                Ok(parsed) => parsed,
                Err(reason) => {
                    return (
                        frames,
                        Some(format!(
                            "invalid writer_offsets in PTWL at offset {pos}: {reason}"
                        )),
                    );
                }
            }
        } else {
            (payload, Vec::new())
        };

        match decode_plst_batch(plst_slice) {
            Ok(batch) => {
                frames.push(DecodedWalFrame {
                    seq,
                    timestamp_ms,
                    batch,
                    writer_offsets,
                });
                pos = payload_end;
            }
            Err(e) => {
                return (
                    frames,
                    Some(format!("invalid PLST payload in PTWL at offset {pos}: {e}")),
                );
            }
        }
    }

    (frames, None)
}

fn parse_wal_path(path: &str) -> Option<(String, u64, String)> {
    let mut parts = path.rsplit('/');
    let filename = parts.next()?;
    let writer_id = parts.next()?;
    let body = filename.strip_prefix("seq_")?.strip_suffix(".ptwl")?;
    let seq = body.parse::<u64>().ok()?;
    Some((writer_id.to_string(), seq, filename.to_string()))
}

async fn get_bytes_with_retry(
    object_store: &Arc<dyn ObjectStore>,
    loc: &ObjectPath,
) -> StoreResult<Option<Bytes>> {
    let mut last_err: Option<object_store::Error> = None;
    for attempt in 0..3u8 {
        match object_store.get(loc).await {
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
    Err(StoreError::ObjectStore(last_err.unwrap()))
}

/// Manages WAL recovery, replay, corruption quarantine, and committed-seq purging for a project.
pub struct AgentWalStore {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
}

impl AgentWalStore {
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

    pub async fn replay_uncommitted(
        &self,
        committed_wal_seq: &BTreeMap<String, u64>,
        applied_seq: &mut HashMap<String, u64>,
        quarantine_corrupt: bool,
    ) -> StoreResult<Vec<(String, u64, TraceIngestBatch)>> {
        let frames = self
            .replay_uncommitted_with_offsets(committed_wal_seq, applied_seq, quarantine_corrupt)
            .await?;
        Ok(frames
            .into_iter()
            .map(|(wid, seq, batch, _)| (wid, seq, batch))
            .collect())
    }

    #[allow(clippy::type_complexity)]
    pub async fn replay_uncommitted_with_offsets(
        &self,
        committed_wal_seq: &BTreeMap<String, u64>,
        applied_seq: &mut HashMap<String, u64>,
        quarantine_corrupt: bool,
    ) -> StoreResult<Vec<(String, u64, TraceIngestBatch, Vec<(String, u64, i64)>)>> {
        let prefix = ObjectPath::from(format!("agent_wal/{}/", self.project_id));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;

        let mut candidates: Vec<(String, u64, String, ObjectPath)> = Vec::new();
        for meta in metas {
            if let Some((wid, seq, fname)) = parse_wal_path(meta.location.as_ref()) {
                if validate_identifier("writer_id", &wid).is_err() {
                    continue;
                }
                let committed = committed_wal_seq.get(&wid).copied().unwrap_or(0);
                let applied = applied_seq.get(&wid).copied().unwrap_or(0);
                if seq > committed && seq > applied {
                    candidates.push((wid, seq, fname, meta.location));
                }
            }
        }
        candidates.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut out = Vec::new();
        for (wid, path_seq, fname, loc) in candidates {
            let Some(bytes) = get_bytes_with_retry(&self.object_store, &loc).await? else {
                continue;
            };
            let (frames, corrupt_err) = decode_ptwl_frames_tolerant(&bytes);

            let mut max_frame_seq = path_seq;
            for frame in frames {
                max_frame_seq = max_frame_seq.max(frame.seq);
                let frame_seq = if frame.seq == 0 { path_seq } else { frame.seq };
                out.push((wid.clone(), frame_seq, frame.batch, frame.writer_offsets));
            }

            if let Some(err_reason) = corrupt_err {
                if quarantine_corrupt {
                    if let Ok(qpath) = wal_corrupt_path(&self.project_id, &wid, &fname) {
                        tracing::error!(
                            project_id = %self.project_id,
                            writer_id = %wid,
                            wal_path = %loc,
                            quarantine_path = %qpath,
                            reason = %err_reason,
                            "quarantining corrupt PTWL WAL object"
                        );
                        if self
                            .object_store
                            .put(&qpath, PutPayload::from_bytes(bytes.clone()))
                            .await
                            .is_ok()
                        {
                            let _ = self.object_store.delete(&loc).await;
                        }
                    }
                } else {
                    tracing::warn!(
                        project_id = %self.project_id,
                        writer_id = %wid,
                        wal_path = %loc,
                        reason = %err_reason,
                        "skipping corrupt PTWL WAL tail in read-only mode"
                    );
                }
            }

            applied_seq
                .entry(wid)
                .and_modify(|s| *s = (*s).max(max_frame_seq))
                .or_insert(max_frame_seq);
        }

        Ok(out)
    }

    pub async fn max_existing_seq_for_writer(&self, writer_id: &str) -> StoreResult<u64> {
        validate_identifier("writer_id", writer_id)?;
        let prefix = ObjectPath::from(format!("agent_wal/{}/{}/", self.project_id, writer_id));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut max_seq = 0u64;
        for meta in metas {
            if let Some((_, seq, _)) = parse_wal_path(meta.location.as_ref()) {
                max_seq = max_seq.max(seq);
            }
        }
        Ok(max_seq)
    }

    /// Deletes ONLY `agent_wal/{project_id}/{writer_id}/seq_{seq:012}.ptwl` where `seq <= committed_seq`.
    pub async fn purge_up_to(&self, writer_id: &str, committed_seq: u64) -> StoreResult<usize> {
        validate_identifier("writer_id", writer_id)?;
        if committed_seq == 0 {
            return Ok(0);
        }
        let prefix = ObjectPath::from(format!("agent_wal/{}/{}/", self.project_id, writer_id));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut deleted = 0usize;
        for meta in metas {
            if let Some((wid, seq, _)) = parse_wal_path(meta.location.as_ref()) {
                if wid == writer_id && seq <= committed_seq {
                    match self.object_store.delete(&meta.location).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted += 1,
                        Err(e) => return Err(StoreError::ObjectStore(e)),
                    }
                }
            }
        }
        Ok(deleted)
    }

    /// Scans uncommitted WAL objects (`seq > committed_wal_seq[wid]`) read-only and returns all
    /// referenced `trace_id`s (from both trace headers and span rows).
    pub async fn uncommitted_trace_ids(
        &self,
        committed_wal_seq: &BTreeMap<String, u64>,
    ) -> StoreResult<HashSet<String>> {
        let mut dummy_applied = HashMap::new();
        let replayed = self
            .replay_uncommitted(committed_wal_seq, &mut dummy_applied, false)
            .await?;
        let mut ids = HashSet::new();
        for (_wid, _seq, batch) in replayed {
            for tr in batch.traces {
                ids.insert(tr.trace_id);
            }
            for sp in batch.spans {
                ids.insert(sp.trace_id);
            }
        }
        Ok(ids)
    }
}

struct WalWriteReq {
    batch: TraceIngestBatch,
    est_bytes: usize,
    writer_offset: Option<(String, u64, i64)>,
    ack: oneshot::Sender<StoreResult<WalCommitResult>>,
}

/// Per-project multi-writer group-commit WAL writer (`5..=25` ms bounded batching).
/// Guarantees `ObjectStore::put_opts(..., PutMode::Create)` succeeds BEFORE inserting into
/// `ShardedMemTable` and BEFORE notifying caller `oneshot`s.
pub struct AgentWalWriter {
    project_id: String,
    writer_id: String,
    next_seq: Arc<AtomicU64>,
    fenced: Arc<AtomicBool>,
    tx: Mutex<Option<mpsc::Sender<WalWriteReq>>>,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl AgentWalWriter {
    pub fn spawn(
        project_id: impl Into<String>,
        writer_id: impl Into<String>,
        initial_next_seq: u64,
        config: WalConfig,
        object_store: Arc<dyn ObjectStore>,
        memtable: Arc<ShardedMemTable>,
    ) -> StoreResult<Self> {
        Self::spawn_with_fence(
            project_id,
            writer_id,
            initial_next_seq,
            config,
            object_store,
            memtable,
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub fn spawn_with_fence(
        project_id: impl Into<String>,
        writer_id: impl Into<String>,
        initial_next_seq: u64,
        config: WalConfig,
        object_store: Arc<dyn ObjectStore>,
        memtable: Arc<ShardedMemTable>,
        fenced: Arc<AtomicBool>,
    ) -> StoreResult<Self> {
        let project_id = project_id.into();
        let writer_id = writer_id.into();
        validate_identifier("project_id", &project_id)?;
        validate_identifier("writer_id", &writer_id)?;

        let clamped_ms = config
            .group_commit_ms
            .clamp(MIN_WAL_GROUP_COMMIT_MS, MAX_WAL_GROUP_COMMIT_MS);
        let max_items = config.max_group_items.max(1);
        let max_bytes = config.max_group_bytes.max(1024);

        let next_seq = Arc::new(AtomicU64::new(initial_next_seq.max(1)));
        let (tx, rx) = mpsc::channel(DEFAULT_WAL_QUEUE_CAP);

        let ctx = GroupCommitCtx {
            project_id: project_id.clone(),
            writer_id: writer_id.clone(),
            next_seq: next_seq.clone(),
            fenced: fenced.clone(),
            window: Duration::from_millis(clamped_ms),
            max_items,
            max_bytes,
            object_store,
            memtable,
        };
        let handle = tokio::spawn(run_group_commit_loop(ctx, rx));

        Ok(Self {
            project_id,
            writer_id,
            next_seq,
            fenced,
            tx: Mutex::new(Some(tx)),
            task: tokio::sync::Mutex::new(Some(handle)),
        })
    }

    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire).saturating_sub(1)
    }

    pub async fn write(&self, batch: TraceIngestBatch) -> StoreResult<WalCommitResult> {
        self.write_inner(batch, None).await
    }

    pub async fn write_with_offset(
        &self,
        batch: TraceIngestBatch,
        stateless_writer_id: String,
        batch_offset: u64,
        seen_ts_ms: i64,
    ) -> StoreResult<WalCommitResult> {
        self.write_inner(batch, Some((stateless_writer_id, batch_offset, seen_ts_ms)))
            .await
    }

    async fn write_inner(
        &self,
        batch: TraceIngestBatch,
        writer_offset: Option<(String, u64, i64)>,
    ) -> StoreResult<WalCommitResult> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(StoreError::NotWriter {
                project_id: self.project_id.clone(),
            });
        }
        let est_bytes = estimate_batch_bytes(&batch);
        let (ack_tx, ack_rx) = oneshot::channel();
        let req = WalWriteReq {
            batch,
            est_bytes,
            writer_offset,
            ack: ack_tx,
        };
        let sender = {
            let guard = self.tx.lock();
            guard.clone().ok_or(StoreError::Shutdown)?
        };
        match sender.try_send(req) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(StoreError::WalBackpressure {
                    project_id: self.project_id.clone(),
                    queued: DEFAULT_WAL_QUEUE_CAP,
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(StoreError::Shutdown),
        }
        ack_rx.await.map_err(|_| StoreError::Shutdown)?
    }

    pub async fn shutdown(&self) {
        {
            let mut guard = self.tx.lock();
            *guard = None;
        }
        let handle = {
            let mut guard = self.task.lock().await;
            guard.take()
        };
        if let Some(h) = handle {
            let _ = h.await;
        }
    }
}

/// Sizes a batch from its actual payloads, so groups are cut on real bytes. A per-span
/// constant let a group of large traces pass the 64 MiB frame limit and fail together.
fn estimate_batch_bytes(batch: &TraceIngestBatch) -> usize {
    let trace_b: usize = batch.traces.iter().map(approx_header_bytes).sum();
    let span_b: usize = batch.spans.iter().map(approx_span_bytes).sum();
    trace_b.saturating_add(span_b).max(64)
}

struct GroupCommitCtx {
    project_id: String,
    writer_id: String,
    next_seq: Arc<AtomicU64>,
    fenced: Arc<AtomicBool>,
    window: Duration,
    max_items: usize,
    max_bytes: usize,
    object_store: Arc<dyn ObjectStore>,
    memtable: Arc<ShardedMemTable>,
}

async fn run_group_commit_loop(ctx: GroupCommitCtx, mut rx: mpsc::Receiver<WalWriteReq>) {
    while let Some(first) = rx.recv().await {
        let mut group = Vec::with_capacity(ctx.max_items.min(64));
        let mut total_bytes = first.est_bytes;
        group.push(first);

        let deadline = Instant::now() + ctx.window;
        while group.len() < ctx.max_items && total_bytes < ctx.max_bytes {
            while let Ok(req) = rx.try_recv() {
                total_bytes = total_bytes.saturating_add(req.est_bytes);
                group.push(req);
                if group.len() >= ctx.max_items || total_bytes >= ctx.max_bytes {
                    break;
                }
            }
            if group.len() >= ctx.max_items || total_bytes >= ctx.max_bytes {
                break;
            }
            tokio::select! {
                maybe_req = rx.recv() => {
                    match maybe_req {
                        Some(req) => {
                            total_bytes = total_bytes.saturating_add(req.est_bytes);
                            group.push(req);
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }

        flush_wal_group(
            &ctx.project_id,
            &ctx.writer_id,
            &ctx.next_seq,
            &ctx.fenced,
            ctx.max_bytes,
            &ctx.object_store,
            &ctx.memtable,
            group,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_wal_group(
    project_id: &str,
    writer_id: &str,
    next_seq: &AtomicU64,
    fenced: &AtomicBool,
    max_bytes: usize,
    object_store: &Arc<dyn ObjectStore>,
    memtable: &Arc<ShardedMemTable>,
    group: Vec<WalWriteReq>,
) {
    if fenced.load(Ordering::Acquire) {
        for req in group {
            let _ = req.ack.send(Err(StoreError::NotWriter {
                project_id: project_id.to_string(),
            }));
        }
        return;
    }
    let mut chunks: Vec<Vec<WalWriteReq>> = Vec::new();
    let mut cur_chunk: Vec<WalWriteReq> = Vec::new();
    let mut cur_traces = 0usize;
    let mut cur_spans = 0usize;
    let mut cur_bytes = 0usize;

    for req in group {
        let rt = req.batch.traces.len();
        let rs = req.batch.spans.len();
        let rb = req.est_bytes;
        let would_exceed = !cur_chunk.is_empty()
            && (cur_traces.saturating_add(rt) > MAX_PLST_TRACES
                || cur_spans.saturating_add(rs) > MAX_PLST_SPANS
                || cur_bytes.saturating_add(rb) > max_bytes);
        if would_exceed {
            chunks.push(cur_chunk);
            cur_chunk = Vec::new();
            cur_traces = 0;
            cur_spans = 0;
            cur_bytes = 0;
        }
        cur_traces = cur_traces.saturating_add(rt);
        cur_spans = cur_spans.saturating_add(rs);
        cur_bytes = cur_bytes.saturating_add(rb);
        cur_chunk.push(req);
    }
    if !cur_chunk.is_empty() {
        chunks.push(cur_chunk);
    }

    for chunk in chunks {
        let mut merged = TraceIngestBatch::default();
        let mut acks = Vec::with_capacity(chunk.len());
        let mut coalesced_offsets: BTreeMap<String, (u64, i64)> = BTreeMap::new();
        for req in chunk {
            merged.traces.extend(req.batch.traces);
            merged.spans.extend(req.batch.spans);
            if let Some((swid, off, ts)) = req.writer_offset {
                coalesced_offsets
                    .entry(swid)
                    .and_modify(|(cur_off, cur_ts)| {
                        *cur_off = (*cur_off).max(off);
                        *cur_ts = (*cur_ts).max(ts);
                    })
                    .or_insert((off, ts));
            }
            acks.push(req.ack);
        }
        let writer_offsets: Vec<(String, u64, i64)> = coalesced_offsets
            .into_iter()
            .map(|(swid, (off, ts))| (swid, off, ts))
            .collect();

        let seq = next_seq.fetch_add(1, Ordering::AcqRel);
        let ts_ms = chrono::Utc::now().timestamp_millis();
        let frame_res = encode_ptwl_frame_with_offsets(seq, ts_ms, &merged, &writer_offsets);
        let frame = match frame_res {
            Ok(f) => f,
            Err(e) => {
                let msg = e.to_string();
                for ack in acks {
                    let _ = ack.send(Err(StoreError::Corrupted(msg.clone())));
                }
                continue;
            }
        };

        let path = match wal_object_path(project_id, writer_id, seq) {
            Ok(p) => p,
            Err(e) => {
                let msg = e.to_string();
                for ack in acks {
                    let _ = ack.send(Err(StoreError::InvalidIdentifier(msg.clone())));
                }
                continue;
            }
        };

        let opts = PutOptions::from(PutMode::Create);
        let is_empty_batch = merged.traces.is_empty() && merged.spans.is_empty();
        match object_store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(frame)), opts)
            .await
        {
            Ok(_) => {
                // Durability guarantee: WAL put_opts succeeded BEFORE L0 insert & BEFORE caller ack!
                let ins_res = memtable.insert_batch(writer_id, seq, merged);
                if ins_res.is_ok() && is_empty_batch {
                    memtable.record_accounted_wal_seq(writer_id, seq);
                }
                match ins_res {
                    Ok(_) => {
                        for ack in acks {
                            let _ = ack.send(Ok(WalCommitResult {
                                writer_id: writer_id.to_string(),
                                wal_seq: seq,
                            }));
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        for ack in acks {
                            let _ = ack.send(Err(StoreError::Corrupted(msg.clone())));
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                // An I/O error, not corruption: nothing was written, so the caller answers 503.
                for ack in acks {
                    let _ = ack.send(Err(StoreError::Io(std::io::Error::other(format!(
                        "PTWL PutMode::Create failed: {msg}"
                    )))));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AgentSpan, AgentTrace, SpanPhase, SpanType, TraceStatus};
    use object_store::memory::InMemory;
    use serde_json::Value;

    fn sample_batch(trace_id: &str) -> TraceIngestBatch {
        TraceIngestBatch {
            traces: vec![AgentTrace {
                trace_id: trace_id.to_string(),
                project_id: "proj_wal".to_string(),
                agent_name: "agent".to_string(),
                agent_version: "1.0".to_string(),
                prompt_version: None,
                model_config_version: None,
                tool_config_version: None,
                app_release: None,
                environment: "production".to_string(),
                session_id: Some("sess_1".to_string()),
                user_id: None,
                workflow_id: None,
                parent_trace_id: None,
                status: TraceStatus::Ok,
                start_ts_ms: 1_700_000_000_000,
                end_ts_ms: Some(1_700_000_000_100),
                duration_ms: 100,
                input: Value::Null,
                output: Value::Null,
                error_type: None,
                error_message: None,
                failure_source: None,
                metadata: Value::Null,
                tags: vec![],
                span_count: 1,
                llm_call_count: 1,
                tool_call_count: 0,
                error_span_count: 0,
                input_tokens: 5,
                output_tokens: 5,
                cached_tokens: 0,
                total_tokens: 10,
                total_cost_nanos: 1_000,
                total_cost_usd: 0.000001,
                primary_model: Some("gpt-4o".to_string()),
                primary_provider: Some("openai".to_string()),
                avg_eval_score: None,
                eval_pass_rate: None,
                has_loop_warning: false,
            }],
            spans: vec![AgentSpan {
                span_id: format!("sp_{trace_id}"),
                trace_id: trace_id.to_string(),
                project_id: "proj_wal".to_string(),
                parent_span_id: None,
                linked_span_ids: vec![],
                span_phase: SpanPhase::Final,
                depth: 0,
                name: "llm".to_string(),
                span_type: SpanType::Llm,
                status: TraceStatus::Ok,
                start_ts_ms: 1_700_000_000_000,
                end_ts_ms: Some(1_700_000_000_100),
                duration_ms: 100,
                provider: Some("openai".to_string()),
                model: Some("gpt-4o".to_string()),
                model_id: None,
                model_version: None,
                prompt_template_id: None,
                prompt_template_version: None,
                input_tokens: 5,
                output_tokens: 5,
                cached_tokens: 0,
                total_tokens: 10,
                cost_nanos: 1_000,
                cost_usd: 0.000001,
                temperature: None,
                finish_reason: None,
                model_params: Value::Null,
                tool_name: None,
                tool_version: None,
                tool_retry_count: 0,
                input: Value::Null,
                output: Value::Null,
                attributes: Value::Null,
                events: vec![],
                metadata: Value::Null,
                error_type: None,
                error_message: None,
                error_stack: None,
            }],
        }
    }

    #[test]
    fn ptwl_header_and_crc_roundtrip() {
        let batch = sample_batch("tr_hdr");
        let frame = encode_ptwl_frame(42, 1_700_000_000_000, &batch).unwrap();
        assert_eq!(&frame[0..4], b"PTWL");
        assert_eq!(frame[4], 1);
        assert_eq!(frame[5], PTWL_FLAG_PLST);
        assert_eq!(&frame[6..8], &0u16.to_le_bytes());
        assert_eq!(u64::from_le_bytes(frame[8..16].try_into().unwrap()), 42);
        assert_eq!(
            i64::from_le_bytes(frame[16..24].try_into().unwrap()),
            1_700_000_000_000
        );
        let payload_len = u32::from_le_bytes(frame[24..28].try_into().unwrap()) as usize;
        assert_eq!(PTWL_HEADER_LEN + payload_len, frame.len());

        let expected_crc = u32::from_le_bytes(frame[28..32].try_into().unwrap());
        let mut h = crc32fast::Hasher::new();
        h.update(&frame[4..28]);
        h.update(&frame[32..]);
        assert_eq!(h.finalize(), expected_crc);

        let (decoded, err) = decode_ptwl_frames_tolerant(&frame);
        assert!(err.is_none());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].seq, 42);
        assert_eq!(decoded[0].batch, batch);

        // Tamper seq in header -> CRC mismatch
        let mut bad = frame.clone();
        bad[8] ^= 0xFF;
        let (decoded_bad, err_bad) = decode_ptwl_frames_tolerant(&bad);
        assert!(decoded_bad.is_empty());
        assert!(err_bad.is_some());
    }

    #[tokio::test]
    async fn wal_group_commit_batches_concurrent_callers_and_persists_before_return() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mt = Arc::new(ShardedMemTable::new("proj_wal", 8));
        let wid = uuid::Uuid::now_v7().to_string();
        let writer = Arc::new(
            AgentWalWriter::spawn(
                "proj_wal",
                &wid,
                1,
                WalConfig {
                    group_commit_ms: 15,
                    max_group_items: 64,
                    max_group_bytes: 8 * 1024 * 1024,
                },
                os.clone(),
                mt.clone(),
            )
            .unwrap(),
        );

        let mut handles = Vec::new();
        for i in 0..16u32 {
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                w.write(sample_batch(&format!("tr_{i}"))).await
            }));
        }
        let mut seqs = Vec::new();
        for h in handles {
            let res = h.await.unwrap().unwrap();
            seqs.push(res.wal_seq);
            // Object MUST already exist on ObjectStore when write() returns
            let path = wal_object_path("proj_wal", &wid, res.wal_seq).unwrap();
            assert!(os.head(&path).await.is_ok());
        }
        // Group commit coalesced 16 callers into fewer than 16 WAL objects
        let metas: Vec<_> = os
            .list(Some(&ObjectPath::from(format!(
                "agent_wal/proj_wal/{wid}/"
            ))))
            .try_collect()
            .await
            .unwrap();
        assert!(
            metas.len() < 16,
            "expected coalescing, got {} files",
            metas.len()
        );
        assert_eq!(mt.total_traces(), 16);
        writer.shutdown().await;
    }

    #[tokio::test]
    async fn wal_replay_skips_and_quarantines_corrupt_partial_entries() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal = AgentWalStore::new("proj_wal", os.clone()).unwrap();
        let wid = uuid::Uuid::now_v7().to_string();

        let f1 = encode_ptwl_frame(1, 1_000, &sample_batch("tr_1")).unwrap();
        let mut f2 = encode_ptwl_frame(2, 2_000, &sample_batch("tr_2")).unwrap();
        f2.truncate(f2.len() - 10); // truncated partial tail
        let f3 = encode_ptwl_frame(3, 3_000, &sample_batch("tr_3")).unwrap();

        os.put(
            &wal_object_path("proj_wal", &wid, 1).unwrap(),
            PutPayload::from_bytes(Bytes::from(f1)),
        )
        .await
        .unwrap();
        os.put(
            &wal_object_path("proj_wal", &wid, 2).unwrap(),
            PutPayload::from_bytes(Bytes::from(f2)),
        )
        .await
        .unwrap();
        os.put(
            &wal_object_path("proj_wal", &wid, 3).unwrap(),
            PutPayload::from_bytes(Bytes::from(f3)),
        )
        .await
        .unwrap();

        // Read-only query node skips corrupt entry WITHOUT quarantining or deleting
        let mut ro_applied = HashMap::new();
        let ro_replayed = wal
            .replay_uncommitted(&BTreeMap::new(), &mut ro_applied, false)
            .await
            .unwrap();
        assert_eq!(ro_replayed.len(), 2);
        assert!(os
            .head(&wal_object_path("proj_wal", &wid, 2).unwrap())
            .await
            .is_ok());

        // Writer recovery quarantines corrupt entry to agent_wal_corrupt/ and deletes original
        let mut rw_applied = HashMap::new();
        let rw_replayed = wal
            .replay_uncommitted(&BTreeMap::new(), &mut rw_applied, true)
            .await
            .unwrap();
        assert_eq!(rw_replayed.len(), 2);
        assert!(os
            .head(&wal_object_path("proj_wal", &wid, 2).unwrap())
            .await
            .is_err());
        assert!(os
            .head(&wal_corrupt_path("proj_wal", &wid, "seq_000000000002.ptwl").unwrap())
            .await
            .is_ok());
    }

    /// Scalability review R-24: 24 requests of ~3 MiB each are 72 MiB together, over the 64 MiB
    /// frame limit. Sized by a per-span constant they went into one frame and all failed.
    #[tokio::test]
    async fn group_of_large_requests_is_split_on_real_bytes() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let memtable = Arc::new(ShardedMemTable::new("proj_wal", 4));
        let mut group = Vec::new();
        let mut acks = Vec::new();
        for i in 0..24 {
            let mut batch = sample_batch(&format!("big_{i}"));
            batch.spans[0].input =
                serde_json::json!({ "text": format!("{i:02}").repeat(3 * 1024 * 1024 / 2) });
            let (ack, rx) = oneshot::channel();
            group.push(WalWriteReq {
                est_bytes: estimate_batch_bytes(&batch),
                batch,
                writer_offset: None,
                ack,
            });
            acks.push(rx);
        }
        let next_seq = AtomicU64::new(1);
        let fenced = AtomicBool::new(false);
        flush_wal_group(
            "proj_wal",
            "w1",
            &next_seq,
            &fenced,
            DEFAULT_WAL_MAX_GROUP_BYTES,
            &os,
            &memtable,
            group,
        )
        .await;
        for rx in acks {
            rx.await.unwrap().expect("request committed");
        }
        assert!(
            next_seq.load(Ordering::Acquire) > 2,
            "split into several frames"
        );
    }

    #[tokio::test]
    async fn multi_writer_isolation_no_overwrite_or_premature_purge() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal = AgentWalStore::new("proj_wal", os.clone()).unwrap();
        let wid_a = uuid::Uuid::now_v7().to_string();
        let wid_b = uuid::Uuid::now_v7().to_string();

        for seq in 1..=3u64 {
            let fa = encode_ptwl_frame(seq, 1_000, &sample_batch(&format!("a_{seq}"))).unwrap();
            let fb = encode_ptwl_frame(seq, 1_000, &sample_batch(&format!("b_{seq}"))).unwrap();
            os.put(
                &wal_object_path("proj_wal", &wid_a, seq).unwrap(),
                PutPayload::from_bytes(Bytes::from(fa)),
            )
            .await
            .unwrap();
            os.put(
                &wal_object_path("proj_wal", &wid_b, seq).unwrap(),
                PutPayload::from_bytes(Bytes::from(fb)),
            )
            .await
            .unwrap();
        }

        let deleted = wal.purge_up_to(&wid_a, 2).await.unwrap();
        assert_eq!(deleted, 2);
        assert!(os
            .head(&wal_object_path("proj_wal", &wid_a, 1).unwrap())
            .await
            .is_err());
        assert!(os
            .head(&wal_object_path("proj_wal", &wid_a, 3).unwrap())
            .await
            .is_ok());
        // Writer B's WALs remain completely untouched
        for seq in 1..=3u64 {
            assert!(os
                .head(&wal_object_path("proj_wal", &wid_b, seq).unwrap())
                .await
                .is_ok());
        }
    }

    #[tokio::test]
    async fn corrupt_agent_wal_quarantines_locally_under_hot_store() {
        use futures::stream::BoxStream;
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions, PutOptions,
            PutResult,
        };
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        #[derive(Debug, Default)]
        struct CountingSeg {
            inner: InMemory,
            calls: AtomicUsize,
        }
        impl std::fmt::Display for CountingSeg {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "CountingSeg")
            }
        }
        #[async_trait::async_trait]
        impl ObjectStore for CountingSeg {
            async fn put_opts(
                &self,
                l: &ObjectPath,
                p: PutPayload,
                o: PutOptions,
            ) -> object_store::Result<PutResult> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.put_opts(l, p, o).await
            }
            async fn put_multipart_opts(
                &self,
                l: &ObjectPath,
                o: PutMultipartOptions,
            ) -> object_store::Result<Box<dyn MultipartUpload>> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.put_multipart_opts(l, o).await
            }
            async fn get_opts(
                &self,
                l: &ObjectPath,
                o: GetOptions,
            ) -> object_store::Result<GetResult> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.get_opts(l, o).await
            }
            async fn delete(&self, l: &ObjectPath) -> object_store::Result<()> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.delete(l).await
            }
            fn list(
                &self,
                p: Option<&ObjectPath>,
            ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.list(p)
            }
            async fn list_with_delimiter(
                &self,
                p: Option<&ObjectPath>,
            ) -> object_store::Result<ListResult> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.list_with_delimiter(p).await
            }
            async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.copy(from, to).await
            }
            async fn copy_if_not_exists(
                &self,
                from: &ObjectPath,
                to: &ObjectPath,
            ) -> object_store::Result<()> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.copy_if_not_exists(from, to).await
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let prev = std::env::var("PULSE_HOT_STORE").ok();
        std::env::set_var("PULSE_HOT_STORE", "local");
        let seg = Arc::new(CountingSeg::default());
        let os = pulse_storage::with_hot_store(seg.clone(), tmp.path())
            .await
            .unwrap();
        match prev {
            Some(v) => std::env::set_var("PULSE_HOT_STORE", v),
            None => std::env::remove_var("PULSE_HOT_STORE"),
        }
        seg.calls.store(0, AtomicOrdering::SeqCst);

        let wal = AgentWalStore::new("proj_wal", os.clone()).unwrap();
        let wid = uuid::Uuid::now_v7().to_string();
        let mut corrupt = encode_ptwl_frame(1, 1_000, &sample_batch("tr_bad")).unwrap();
        corrupt.truncate(corrupt.len() - 8);
        os.put(
            &wal_object_path("proj_wal", &wid, 1).unwrap(),
            PutPayload::from_bytes(Bytes::from(corrupt)),
        )
        .await
        .unwrap();

        let mut applied = HashMap::new();
        let replayed = wal
            .replay_uncommitted(&BTreeMap::new(), &mut applied, true)
            .await
            .unwrap();
        assert!(replayed.is_empty());
        assert_eq!(
            seg.calls.load(AtomicOrdering::SeqCst),
            0,
            "agent_wal_corrupt/ quarantine must make 0 calls to segments"
        );
        let qpath = wal_corrupt_path("proj_wal", &wid, "seq_000000000001.ptwl").unwrap();
        assert!(os.head(&qpath).await.is_ok());
        assert!(
            tmp.path()
                .join("wal")
                .join("agent_wal_corrupt")
                .join("proj_wal")
                .join(&wid)
                .join("seq_000000000001.ptwl")
                .is_file(),
            "quarantined agent WAL must reside on local disk under <data_dir>/wal"
        );
    }

    #[tokio::test]
    async fn ptwl_writer_offsets_roundtrip_and_multi_writer_group_commit() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mem = Arc::new(ShardedMemTable::new("proj_wal", 2));
        let writer = AgentWalWriter::spawn(
            "proj_wal",
            "local_wal_wid",
            1,
            WalConfig {
                group_commit_ms: 25,
                max_group_items: 16,
                max_group_bytes: 1024 * 1024,
            },
            os.clone(),
            mem.clone(),
        )
        .unwrap();

        let (r1, r2, r3) = tokio::join!(
            writer.write_with_offset(sample_batch("tr_1"), "sw_a".to_string(), 0, 10_000),
            writer.write_with_offset(sample_batch("tr_2"), "sw_a".to_string(), 2, 10_200),
            writer.write_with_offset(TraceIngestBatch::default(), "sw_b".to_string(), 5, 10_500),
        );
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();
        writer.shutdown().await;

        let wal = AgentWalStore::new("proj_wal", os).unwrap();
        let mut applied = HashMap::new();
        let replayed = wal
            .replay_uncommitted_with_offsets(&BTreeMap::new(), &mut applied, false)
            .await
            .unwrap();
        let mut max_seen: BTreeMap<String, (u64, i64)> = BTreeMap::new();
        for (_wid, _seq, _batch, offsets) in replayed {
            for (swid, off, ts) in offsets {
                max_seen
                    .entry(swid)
                    .and_modify(|(co, ct)| {
                        *co = (*co).max(off);
                        *ct = (*ct).max(ts);
                    })
                    .or_insert((off, ts));
            }
        }
        assert_eq!(max_seen.get("sw_a"), Some(&(2, 10_200)));
        assert_eq!(max_seen.get("sw_b"), Some(&(5, 10_500)));
    }
}
