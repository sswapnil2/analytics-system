use std::collections::HashSet;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::DashSet;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectStore, PutMode, PutOptions, PutPayload};
use pulse_wire::ReplayChunk;
use xxhash_rust::xxh3::xxh3_64;

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

/// Validates `project_id` and `session_id` against `[A-Za-z0-9_-]{1,128}`.
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
/// at `replays/{project_id}/{session_id}/{seq:010}.plsr`.
///
/// Eliminates O(n^2) read-modify-write object amplification, prevents multi-writer chunk loss via
/// conditional `PutMode::Create`, and avoids UTC midnight file splits across process restarts.
pub struct ReplayStore {
    object_store: Arc<dyn ObjectStore>,
    recorded_sessions: DashSet<(String, String)>,
    recorded_hashes: DashSet<(String, u64)>,
    warmed_projects: DashSet<String>,
}

impl ReplayStore {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            recorded_sessions: DashSet::new(),
            recorded_hashes: DashSet::new(),
            warmed_projects: DashSet::new(),
        }
    }

    fn index_session(&self, project_id: &str, session_id: &str) {
        self.recorded_sessions
            .insert((project_id.to_string(), session_id.to_string()));
        self.recorded_hashes
            .insert((project_id.to_string(), xxh3_64(session_id.as_bytes())));
    }

    /// Scans `replays/{project_id}/` once to populate the in-memory recorded-session index.
    pub async fn warm_project(&self, project_id: &str) {
        if self.warmed_projects.contains(project_id) {
            return;
        }
        if validate_replay_identifier("project_id", project_id).is_err() {
            return;
        }
        let prefix = ObjectPath::from(format!("replays/{project_id}/"));
        if let Ok(mut stream) = self
            .object_store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
        {
            for m in stream.drain(..) {
                let mut parts = m.location.parts();
                let _p0 = parts.next(); // "replays"
                let _p1 = parts.next(); // project_id
                if let Some(sess_part) = parts.next() {
                    let sid = sess_part.as_ref();
                    if !sid.is_empty() {
                        self.index_session(project_id, sid);
                    }
                }
            }
            self.warmed_projects.insert(project_id.to_string());
        }
    }

    /// Returns the set of `xxh3_64(session_id)` hashes that currently have a recorded replay.
    pub async fn recorded_hashes_for_project(&self, project_id: &str) -> HashSet<u64> {
        self.warm_project(project_id).await;
        self.recorded_hashes
            .iter()
            .filter_map(|entry| (entry.0 == project_id).then_some(entry.1))
            .collect()
    }

    /// Returns `true` if `session_id` has at least one `.plsr` replay chunk in `project_id`.
    pub async fn has_session(&self, project_id: &str, session_id: &str) -> bool {
        let key = (project_id.to_string(), session_id.to_string());
        if self.recorded_sessions.contains(&key) {
            return true;
        }
        match self.total_session_bytes(project_id, session_id).await {
            Ok(Some(bytes)) if bytes > 0 => {
                self.index_session(project_id, session_id);
                true
            }
            _ => false,
        }
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
                    self.index_session(&chunk.project_id, &chunk.session_id);
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

    /// Deletes all `.plsr` replay chunks belonging to `session_id` under `project_id` (`Gap 4`).
    pub async fn delete_session(&self, project_id: &str, session_id: &str) -> Result<bool> {
        let chunks = self.list_session_chunks(project_id, session_id).await?;
        let had_any = !chunks.is_empty();
        for c in chunks {
            let _ = self.object_store.delete(&c.location).await;
        }
        self.recorded_sessions
            .remove(&(project_id.to_string(), session_id.to_string()));
        let target_hash = xxh3_64(session_id.as_bytes());
        let still_colliding = self
            .recorded_sessions
            .iter()
            .any(|e| e.0 == project_id && xxh3_64(e.1.as_bytes()) == target_hash);
        if !still_colliding {
            self.recorded_hashes
                .remove(&(project_id.to_string(), target_hash));
        }
        Ok(had_any)
    }
}
