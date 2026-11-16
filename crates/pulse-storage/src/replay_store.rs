use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectStore, PutPayload};
use pulse_wire::ReplayChunk;
use tokio::sync::Mutex;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct ReplayAppendResult {
    pub object_path: String,
    pub byte_offset: u64,
    pub replay_offset_kb: u32,
    pub chunk_bytes: usize,
}

/// Object-store backed `.plsr` wireframe replay store supporting exact byte-range jumps.
pub struct ReplayStore {
    object_store: Arc<dyn ObjectStore>,
    session_paths: DashMap<(String, String), String>,
    write_locks: DashMap<(String, String), Arc<Mutex<()>>>,
}

impl ReplayStore {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            session_paths: DashMap::new(),
            write_locks: DashMap::new(),
        }
    }

    fn lock_for(&self, project_id: &str, session_id: &str) -> Arc<Mutex<()>> {
        self.write_locks
            .entry((project_id.to_string(), session_id.to_string()))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn path_for(project_id: &str, date: NaiveDate, session_id: &str) -> String {
        format!(
            "replays/{}/{}/{}.plsr",
            project_id,
            date.format("%Y%m%d"),
            session_id
        )
    }

    /// Appends a `ReplayChunk` frame to `replays/<project_id>/<yyyymmdd>/<session_id>.plsr`
    /// and returns its starting byte offset (`replay_offset_kb`).
    pub async fn append_chunk(&self, chunk: &ReplayChunk) -> Result<ReplayAppendResult> {
        let lock = self.lock_for(&chunk.project_id, &chunk.session_id);
        let _guard = lock.lock().await;

        let date = DateTime::<Utc>::from_timestamp_millis(chunk.chunk_start_ts_ms)
            .map(|dt| dt.date_naive())
            .unwrap_or_else(|| Utc::now().date_naive());

        let rel_path = self
            .session_paths
            .get(&(chunk.project_id.clone(), chunk.session_id.clone()))
            .map(|r| r.clone())
            .unwrap_or_else(|| Self::path_for(&chunk.project_id, date, &chunk.session_id));

        let obj_path = ObjectPath::from(rel_path.as_str());
        let existing = match self.object_store.get(&obj_path).await {
            Ok(res) => res.bytes().await?.to_vec(),
            Err(OsError::NotFound { .. }) => Vec::new(),
            Err(other) => return Err(other.into()),
        };

        let byte_offset = existing.len() as u64;
        let encoded = chunk.encode_binary(3)?;
        let chunk_bytes = encoded.len();

        let mut combined = Vec::with_capacity(existing.len() + encoded.len());
        combined.extend_from_slice(&existing);
        combined.extend_from_slice(&encoded);

        self.object_store
            .put(&obj_path, PutPayload::from_bytes(Bytes::from(combined)))
            .await?;

        self.session_paths.insert(
            (chunk.project_id.clone(), chunk.session_id.clone()),
            rel_path.clone(),
        );

        Ok(ReplayAppendResult {
            object_path: rel_path,
            byte_offset,
            replay_offset_kb: (byte_offset / 1024) as u32,
            chunk_bytes,
        })
    }

    pub async fn resolve_session_path(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Option<String>> {
        if let Some(p) = self
            .session_paths
            .get(&(project_id.to_string(), session_id.to_string()))
        {
            return Ok(Some(p.clone()));
        }

        let prefix = ObjectPath::from(format!("replays/{project_id}/"));
        let suffix = format!("/{session_id}.plsr");
        let metas: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        for m in metas {
            let loc = m.location.as_ref();
            if loc.ends_with(&suffix) {
                self.session_paths.insert(
                    (project_id.to_string(), session_id.to_string()),
                    loc.to_string(),
                );
                return Ok(Some(loc.to_string()));
            }
        }
        Ok(None)
    }

    /// Performs a byte-range read starting at `start_byte_offset` (`offset_kb * 1024`) on `.plsr`.
    pub async fn read_bytes_from(
        &self,
        project_id: &str,
        session_id: &str,
        start_byte_offset: u64,
        max_len: Option<u64>,
    ) -> Result<Option<Bytes>> {
        let Some(rel_path) = self.resolve_session_path(project_id, session_id).await? else {
            return Ok(None);
        };
        let obj_path = ObjectPath::from(rel_path.as_str());
        let head = match self.object_store.head(&obj_path).await {
            Ok(h) => h,
            Err(OsError::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let total = head.size;
        if start_byte_offset >= total {
            return Ok(Some(Bytes::new()));
        }
        let end = max_len
            .map(|m| (start_byte_offset + m).min(total))
            .unwrap_or(total);
        let bytes = self
            .object_store
            .get_range(&obj_path, start_byte_offset..end)
            .await?;
        Ok(Some(bytes))
    }

    /// Reads and decodes `ReplayChunk` frames starting from `start_byte_offset`.
    pub async fn read_chunks(
        &self,
        project_id: &str,
        session_id: &str,
        start_byte_offset: u64,
    ) -> Result<Vec<ReplayChunk>> {
        let Some(bytes) = self
            .read_bytes_from(project_id, session_id, start_byte_offset, None)
            .await?
        else {
            return Ok(Vec::new());
        };
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        Ok(ReplayChunk::decode_stream(&bytes)?)
    }
}
