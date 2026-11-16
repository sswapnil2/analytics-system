use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectStore, PutMode, PutOptions, PutPayload};
use pulse_wire::ReplayChunk;

use crate::error::{Result, StorageError};

#[derive(Debug, Clone)]
pub struct ReplayAppendResult {
    pub object_path: String,
    pub byte_offset: u64,
    pub replay_offset_kb: u32,
    pub chunk_bytes: usize,
}

#[derive(Debug, Clone)]
struct ReplayChunkMeta {
    seq: u64,
    location: ObjectPath,
    size: u64,
}

/// Validates `project_id` and `session_id` against `[A-Za-z0-9_-]{1,128}` (`P1-16`).
pub fn validate_replay_identifier(field: &'static str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(StorageError::InvalidIdentifier(format!(
            "invalid {field}: must match [A-Za-z0-9_-]{{1,128}}"
        )));
    }
    Ok(())
}

fn parse_chunk_seq(path: &str) -> Option<u64> {
    let fname = path.rsplit('/').next()?;
    let stem = fname.strip_suffix(".plsr")?;
    stem.parse::<u64>().ok()
}

/// Object-store backed `.plsr` wireframe replay store storing immutable per-chunk objects
/// at `replays/{project_id}/{session_id}/{seq:010}.plsr` (`P1-16`).
///
/// Eliminates O(n^2) read-modify-write object amplification, prevents multi-writer chunk loss via
/// conditional `PutMode::Create`, and avoids UTC midnight file splits across process restarts.
pub struct ReplayStore {
    object_store: Arc<dyn ObjectStore>,
}

impl ReplayStore {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }

    async fn list_session_chunks(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Vec<ReplayChunkMeta>> {
        validate_replay_identifier("project_id", project_id)?;
        validate_replay_identifier("session_id", session_id)?;
        let prefix = ObjectPath::from(format!("replays/{project_id}/{session_id}/"));
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut chunks = Vec::with_capacity(metas.len());
        for m in metas {
            if let Some(seq) = parse_chunk_seq(m.location.as_ref()) {
                chunks.push(ReplayChunkMeta {
                    seq,
                    location: m.location,
                    size: m.size,
                });
            }
        }
        chunks.sort_by_key(|c| c.seq);
        Ok(chunks)
    }

    /// Appends an immutable `ReplayChunk` object to `replays/<project_id>/<session_id>/<seq:010>.plsr`
    /// using `PutMode::Create` and returns its virtual stream byte offset (`replay_offset_kb`).
    pub async fn append_chunk(&self, chunk: &ReplayChunk) -> Result<ReplayAppendResult> {
        validate_replay_identifier("project_id", &chunk.project_id)?;
        validate_replay_identifier("session_id", &chunk.session_id)?;

        let encoded = Bytes::from(chunk.encode_binary(3)?);
        let chunk_bytes = encoded.len();

        for _attempt in 0..32u8 {
            let existing = self
                .list_session_chunks(&chunk.project_id, &chunk.session_id)
                .await?;
            let byte_offset: u64 = existing.iter().map(|c| c.size).sum();
            let next_seq: u64 = existing.last().map(|c| c.seq + 1).unwrap_or(0);

            let rel_path = format!(
                "replays/{}/{}/{:010}.plsr",
                chunk.project_id, chunk.session_id, next_seq
            );
            let obj_path = ObjectPath::from(rel_path.as_str());
            let opts = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };

            match self
                .object_store
                .put_opts(&obj_path, PutPayload::from_bytes(encoded.clone()), opts)
                .await
            {
                Ok(_) => {
                    return Ok(ReplayAppendResult {
                        object_path: rel_path,
                        byte_offset,
                        replay_offset_kb: (byte_offset / 1024) as u32,
                        chunk_bytes,
                    });
                }
                Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                    continue;
                }
                Err(other) => return Err(StorageError::ObjectStore(other)),
            }
        }

        Err(StorageError::ManifestConflict)
    }

    /// Returns total concatenated byte length across all chunks of `session_id`, or `None` if session has no chunks.
    pub async fn total_session_bytes(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Option<u64>> {
        let chunks = self.list_session_chunks(project_id, session_id).await?;
        if chunks.is_empty() {
            Ok(None)
        } else {
            Ok(Some(chunks.iter().map(|c| c.size).sum()))
        }
    }

    /// Performs a virtual byte-range read across immutable chunk objects starting at `start_byte_offset`.
    pub async fn read_bytes_from(
        &self,
        project_id: &str,
        session_id: &str,
        start_byte_offset: u64,
        max_len: Option<u64>,
    ) -> Result<Option<Bytes>> {
        let chunks = self.list_session_chunks(project_id, session_id).await?;
        if chunks.is_empty() {
            return Ok(None);
        }

        let total: u64 = chunks.iter().map(|c| c.size).sum();
        if start_byte_offset >= total {
            return Ok(Some(Bytes::new()));
        }
        let end = max_len
            .map(|m| start_byte_offset.saturating_add(m).min(total))
            .unwrap_or(total);

        let mut out = BytesMut::with_capacity((end - start_byte_offset) as usize);
        let mut cursor = 0u64;
        for c in chunks {
            let chunk_start = cursor;
            let chunk_end = cursor + c.size;
            cursor = chunk_end;

            if chunk_end <= start_byte_offset {
                continue;
            }
            if chunk_start >= end {
                break;
            }

            let rel_start = start_byte_offset.saturating_sub(chunk_start);
            let rel_end = (end - chunk_start).min(c.size);
            if rel_start == 0 && rel_end == c.size {
                let b = self.object_store.get(&c.location).await?.bytes().await?;
                out.extend_from_slice(&b);
            } else {
                let b = self
                    .object_store
                    .get_range(&c.location, rel_start..rel_end)
                    .await?;
                out.extend_from_slice(&b);
            }
        }
        Ok(Some(out.freeze()))
    }

    /// Reads and decodes `ReplayChunk` frames starting from `start_byte_offset`.
    /// If `start_byte_offset` falls inside a chunk (e.g., `offset_kb * 1024`), snaps down to the
    /// enclosing chunk boundary so binary Zstd `PLSR` frames always decode cleanly!
    pub async fn read_chunks(
        &self,
        project_id: &str,
        session_id: &str,
        start_byte_offset: u64,
    ) -> Result<Vec<ReplayChunk>> {
        let chunks = self.list_session_chunks(project_id, session_id).await?;
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut cursor = 0u64;
        for c in chunks {
            let chunk_end = cursor + c.size;
            if chunk_end > start_byte_offset {
                let raw = self.object_store.get(&c.location).await?.bytes().await?;
                let mut decoded = ReplayChunk::decode_stream(&raw)?;
                out.append(&mut decoded);
            }
            cursor = chunk_end;
        }
        Ok(out)
    }
}
