use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectStore, PutMode, PutOptions, PutPayload};
use parking_lot::Mutex as SyncMutex;
use pulse_wire::ReplayChunk;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, StorageError};
use crate::manifest::ReaderPinTracker;
use crate::store_factory::is_hot_routed_store;

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

/// Durable local watermark for an uploaded replay session (`hot/replay_cursors/{project_id}/{session_id}.json`).
/// Allows post-upload `append_chunk` calls to continue monotonic `seq` and `byte_offset` with 0 S3 calls.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplaySessionCursor {
    pub next_seq: u64,
    pub uploaded_bytes: u64,
    /// When a differing-bytes `AlreadyExists` shift fails mid-loop, `next_seq` advances to
    /// `s3_next_seq` so S3 sequences are never reused, while `local_committed_before_seq` records
    /// the local sequence boundary already committed to S3 so remaining un-shifted local chunks
    /// (`seq >= local_committed_before_seq`) are preserved and uploaded on retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_committed_before_seq: Option<u64>,
}

impl ReplaySessionCursor {
    fn local_committed_before_seq(&self) -> u64 {
        self.local_committed_before_seq.unwrap_or(self.next_seq)
    }
}

#[derive(Debug, Clone, Copy)]
struct SessionWriteState {
    next_seq: u64,
    total_bytes: u64,
}

struct LocalChunkPinGuard {
    tracker: Arc<ReaderPinTracker>,
    paths: Vec<String>,
}

impl Drop for LocalChunkPinGuard {
    fn drop(&mut self) {
        if !self.paths.is_empty() {
            self.tracker.unpin_all(&self.paths);
        }
    }
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

fn parse_replay_object_parts(location: &ObjectPath) -> Option<(String, String, u64)> {
    let s = location.as_ref();
    let seq = parse_chunk_seq(s)?;
    let mut parts = location.parts();
    let p0 = parts.next()?;
    if p0.as_ref() == "hot" {
        let p1 = parts.next()?;
        if p1.as_ref() != "replay" {
            return None;
        }
        let pid = parts.next()?.as_ref().to_string();
        let sid = parts.next()?.as_ref().to_string();
        let _fname = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        Some((pid, sid, seq))
    } else if p0.as_ref() == "replays" {
        let pid = parts.next()?.as_ref().to_string();
        let sid = parts.next()?.as_ref().to_string();
        let _fname = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        Some((pid, sid, seq))
    } else {
        None
    }
}

fn s3_session_prefix(project_id: &str, session_id: &str) -> String {
    format!("replays/{project_id}/{session_id}/")
}

fn s3_chunk_path(project_id: &str, session_id: &str, seq: u64) -> String {
    format!("replays/{project_id}/{session_id}/{seq:010}.plsr")
}

fn hot_session_prefix(project_id: &str, session_id: &str) -> String {
    format!("hot/replay/{project_id}/{session_id}/")
}

fn hot_chunk_path(project_id: &str, session_id: &str, seq: u64) -> String {
    format!("hot/replay/{project_id}/{session_id}/{seq:010}.plsr")
}

fn hot_cursor_path(project_id: &str, session_id: &str) -> String {
    format!("hot/replay_cursors/{project_id}/{session_id}.json")
}

/// Object-store backed `.plsr` wireframe replay store storing immutable per-chunk objects.
///
/// Under `PULSE_HOT_STORE=local` (`local_hot_tier = true`), new chunks are staged durably on local
/// disk under `hot/replay/{project_id}/{session_id}/{seq:010}.plsr` with 0 S3 calls, and uploaded
/// on the publish/upload cycle to S3 `replays/{project_id}/{session_id}/{seq:010}.plsr` via
/// `copy_if_not_exists` (re-sealing `PLSEAGM2` path-bound encryption AAD) with local cursor
/// persistence in `hot/replay_cursors/{project_id}/{session_id}.json`.
///
/// Under `PULSE_HOT_STORE=object` (`local_hot_tier = false`), chunks are written directly to
/// `replays/{project_id}/{session_id}/{seq:010}.plsr`.
pub struct ReplayStore {
    object_store: Arc<dyn ObjectStore>,
    local_hot_tier: AtomicBool,
    read_only: bool,
    recorded_sessions: DashSet<(String, String)>,
    recorded_hashes: DashSet<(String, u64)>,
    warmed_projects: DashSet<String>,
    append_locks: DashMap<(String, String), Arc<AsyncMutex<()>>>,
    upload_locks: DashMap<(String, String), Arc<AsyncMutex<()>>>,
    session_states: DashMap<(String, String), SessionWriteState>,
    dirty_sessions: DashSet<(String, String)>,
    local_scan_done: AtomicBool,
    local_scan_lock: AsyncMutex<()>,
    pin_tracker: Arc<ReaderPinTracker>,
    pending_local_deletes: SyncMutex<HashSet<String>>,
    /// Per-project invalidation/deletion epoch guarding `warm_project` / `has_session`
    /// against inserting stale S3/local listing results after a concurrent `invalidate_project`
    /// or `delete_session`. Never held across `.await`.
    project_invalidation_gen: SyncMutex<HashMap<String, u64>>,
}

impl ReplayStore {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        let local_hot_tier = is_hot_routed_store(object_store.as_ref());
        Self::new_with_options(object_store, local_hot_tier, false)
    }

    pub fn new_with_options(
        object_store: Arc<dyn ObjectStore>,
        local_hot_tier: bool,
        read_only: bool,
    ) -> Self {
        Self {
            object_store,
            local_hot_tier: AtomicBool::new(local_hot_tier),
            read_only,
            recorded_sessions: DashSet::new(),
            recorded_hashes: DashSet::new(),
            warmed_projects: DashSet::new(),
            append_locks: DashMap::new(),
            upload_locks: DashMap::new(),
            session_states: DashMap::new(),
            dirty_sessions: DashSet::new(),
            local_scan_done: AtomicBool::new(false),
            local_scan_lock: AsyncMutex::new(()),
            pin_tracker: Arc::new(ReaderPinTracker::default()),
            pending_local_deletes: SyncMutex::new(HashSet::new()),
            project_invalidation_gen: SyncMutex::new(HashMap::new()),
        }
    }

    pub fn is_local_hot_tier(&self) -> bool {
        self.local_hot_tier.load(Ordering::Acquire)
    }

    pub fn set_local_hot_tier(&self, enabled: bool) {
        self.local_hot_tier.store(enabled, Ordering::Release);
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns `true` when local hot-tier replay work may be pending (either the one-time startup
    /// scan has not run yet or at least one session has un-uploaded local chunks).
    pub fn has_pending_local_work(&self) -> bool {
        self.is_local_hot_tier()
            && !self.read_only
            && (!self.local_scan_done.load(Ordering::Acquire) || !self.dirty_sessions.is_empty())
    }

    /// Returns a snapshot of `(project_id, session_id)` pairs with pending local replay chunks.
    pub fn dirty_sessions_snapshot(&self) -> Vec<(String, String)> {
        self.dirty_sessions
            .iter()
            .map(|e| (e.0.clone(), e.1.clone()))
            .collect()
    }

    /// Invalidates cached replay session discovery for `project_id` (used by `NodeRole::Query` on
    /// manifest refresh or `erased_uids` updates).
    pub fn invalidate_project(&self, project_id: &str) {
        let mut gens = self.project_invalidation_gen.lock();
        let epoch = gens.entry(project_id.to_string()).or_insert(0);
        *epoch = epoch.wrapping_add(1);
        self.warmed_projects.remove(project_id);
        self.recorded_sessions.retain(|k| k.0 != project_id);
        self.recorded_hashes.retain(|k| k.0 != project_id);
    }

    /// Exposes the local chunk pin tracker for tests verifying concurrent reader pin deferral.
    pub fn pin_tracker(&self) -> &Arc<ReaderPinTracker> {
        &self.pin_tracker
    }

    /// Returns the number of deferred local chunk deletions awaiting reader unpin.
    pub fn pending_local_deletes_len(&self) -> usize {
        self.pending_local_deletes.lock().len()
    }

    fn append_lock_for(&self, project_id: &str, session_id: &str) -> Arc<AsyncMutex<()>> {
        self.append_locks
            .entry((project_id.to_string(), session_id.to_string()))
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn upload_lock_for(&self, project_id: &str, session_id: &str) -> Arc<AsyncMutex<()>> {
        self.upload_locks
            .entry((project_id.to_string(), session_id.to_string()))
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn index_session(&self, project_id: &str, session_id: &str) {
        self.recorded_sessions
            .insert((project_id.to_string(), session_id.to_string()));
        self.recorded_hashes
            .insert((project_id.to_string(), xxh3_64(session_id.as_bytes())));
    }

    fn remove_session_from_index(&self, project_id: &str, session_id: &str) {
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
    }

    async fn load_cursor(&self, project_id: &str, session_id: &str) -> Result<ReplaySessionCursor> {
        let path = ObjectPath::from(hot_cursor_path(project_id, session_id).as_str());
        match self.object_store.get(&path).await {
            Ok(res) => {
                let bytes = res.bytes().await?;
                let cursor: ReplaySessionCursor = serde_json::from_slice(&bytes)?;
                Ok(cursor)
            }
            Err(OsError::NotFound { .. }) => Ok(ReplaySessionCursor::default()),
            Err(e) => Err(StorageError::ObjectStore(e)),
        }
    }

    async fn save_cursor(
        &self,
        project_id: &str,
        session_id: &str,
        cursor: &ReplaySessionCursor,
    ) -> Result<()> {
        let path = ObjectPath::from(hot_cursor_path(project_id, session_id).as_str());
        let payload = serde_json::to_vec(cursor)?;
        self.object_store
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from(payload)),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn list_prefix_chunks(&self, prefix: &str) -> Result<Vec<ReplayChunkMeta>> {
        let obj_prefix = ObjectPath::from(prefix);
        let metas: Vec<_> = self
            .object_store
            .list(Some(&obj_prefix))
            .try_collect()
            .await?;
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

    /// Scans `replays/{project_id}/` (and `hot/replay/{project_id}/` when local hot tier is
    /// active on a writable node) to populate the in-memory recorded-session index.
    ///
    /// On `read_only` nodes (`NodeRole::Query`), this re-scans S3 `replays/{project_id}/` on
    /// demand when `recorded_hashes_for_project` is queried (never on idle ticks), so newly
    /// uploaded sessions from remote writers become visible immediately.
    pub async fn warm_project(&self, project_id: &str) {
        if !self.read_only && self.warmed_projects.contains(project_id) {
            return;
        }
        if validate_replay_identifier("project_id", project_id).is_err() {
            return;
        }

        for _attempt in 0..3 {
            let start_gen = {
                let gens = self.project_invalidation_gen.lock();
                gens.get(project_id).copied().unwrap_or(0)
            };

            let mut found_sids: HashSet<String> = HashSet::new();
            let mut s3_ok = false;
            let s3_prefix = ObjectPath::from(format!("replays/{project_id}/"));
            if let Ok(metas) = self
                .object_store
                .list(Some(&s3_prefix))
                .try_collect::<Vec<_>>()
                .await
            {
                s3_ok = true;
                for m in metas {
                    if let Some((pid, sid, _)) = parse_replay_object_parts(&m.location) {
                        if pid == project_id {
                            found_sids.insert(sid);
                        }
                    }
                }
            }

            let mut local_ok = true;
            if self.is_local_hot_tier() && !self.read_only {
                local_ok = false;
                let hot_prefix = ObjectPath::from(format!("hot/replay/{project_id}/"));
                if let Ok(metas) = self
                    .object_store
                    .list(Some(&hot_prefix))
                    .try_collect::<Vec<_>>()
                    .await
                {
                    local_ok = true;
                    for m in metas {
                        if let Some((pid, sid, _)) = parse_replay_object_parts(&m.location) {
                            if pid == project_id {
                                found_sids.insert(sid);
                            }
                        }
                    }
                }
            }

            let gens = self.project_invalidation_gen.lock();
            if gens.get(project_id).copied().unwrap_or(0) != start_gen {
                // Concurrent `invalidate_project` or `delete_session` ran while listing; discard
                // stale listing results and retry so deleted sessions are never resurrected.
                continue;
            }
            if self.read_only {
                if s3_ok {
                    self.recorded_sessions.retain(|k| k.0 != project_id);
                    self.recorded_hashes.retain(|k| k.0 != project_id);
                    for sid in found_sids {
                        self.index_session(project_id, &sid);
                    }
                }
            } else if s3_ok || local_ok {
                for sid in found_sids {
                    self.index_session(project_id, &sid);
                }
                if s3_ok && local_ok {
                    self.warmed_projects.insert(project_id.to_string());
                }
            }
            break;
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
        if validate_replay_identifier("project_id", project_id).is_err()
            || validate_replay_identifier("session_id", session_id).is_err()
        {
            return false;
        }
        let key = (project_id.to_string(), session_id.to_string());
        if self.recorded_sessions.contains(&key) {
            return true;
        }
        for _attempt in 0..2 {
            let start_gen = {
                let gens = self.project_invalidation_gen.lock();
                gens.get(project_id).copied().unwrap_or(0)
            };
            let mut exists = false;
            if self.is_local_hot_tier() && !self.read_only {
                if let Ok(local) = self
                    .list_prefix_chunks(&hot_session_prefix(project_id, session_id))
                    .await
                {
                    if !local.is_empty() {
                        exists = true;
                    }
                }
            }
            if !exists {
                if let Ok(Some(bytes)) = self.total_session_bytes(project_id, session_id).await {
                    if bytes > 0 {
                        exists = true;
                    }
                }
            }
            if !exists {
                return false;
            }
            let gens = self.project_invalidation_gen.lock();
            if gens.get(project_id).copied().unwrap_or(0) == start_gen {
                self.index_session(project_id, session_id);
                return true;
            }
        }
        false
    }

    async fn list_session_chunks(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Vec<ReplayChunkMeta>> {
        let (chunks, _) = self
            .list_and_pin_session_chunks(project_id, session_id, false)
            .await?;
        Ok(chunks)
    }

    async fn list_and_pin_session_chunks(
        &self,
        project_id: &str,
        session_id: &str,
        pin_local: bool,
    ) -> Result<(Vec<ReplayChunkMeta>, Option<LocalChunkPinGuard>)> {
        validate_replay_identifier("project_id", project_id)?;
        validate_replay_identifier("session_id", session_id)?;

        if !self.is_local_hot_tier() || self.read_only {
            let s3_chunks = self
                .list_prefix_chunks(&s3_session_prefix(project_id, session_id))
                .await?;
            return Ok((s3_chunks, None));
        }

        for attempt in 0..8u8 {
            let cursor_before = self.load_cursor(project_id, session_id).await?;
            let local_chunks = self
                .list_prefix_chunks(&hot_session_prefix(project_id, session_id))
                .await?;
            let pin_guard = if pin_local && !local_chunks.is_empty() {
                let hot_paths: Vec<String> = local_chunks
                    .iter()
                    .map(|c| c.location.as_ref().to_string())
                    .collect();
                self.pin_tracker.pin_all(&hot_paths);
                Some(LocalChunkPinGuard {
                    tracker: self.pin_tracker.clone(),
                    paths: hot_paths,
                })
            } else {
                None
            };
            let s3_chunks = self
                .list_prefix_chunks(&s3_session_prefix(project_id, session_id))
                .await?;
            let cursor_after = self.load_cursor(project_id, session_id).await?;

            if cursor_before != cursor_after && attempt < 7 {
                drop(pin_guard);
                tokio::task::yield_now().await;
                continue;
            }

            let committed_before = cursor_after.local_committed_before_seq();
            if cursor_after.local_committed_before_seq.is_some() && !local_chunks.is_empty() {
                // During recovery from a partial shift after `AlreadyExists` with differing
                // bytes, S3 holds `[0..cursor_after.next_seq)` (including already-shifted chunks)
                // while un-shifted local chunks with `seq >= committed_before` logically follow
                // them even if their local `seq` numbers overlap S3. Any S3 objects at
                // `seq >= cursor_after.next_seq` were copied by an in-flight `upload_session`
                // whose cursor has not been persisted yet, so `local_chunks` remains authoritative
                // for them until `save_cursor` advances `next_seq` / clears `local_committed_before_seq`.
                let mut out: Vec<ReplayChunkMeta> = s3_chunks
                    .into_iter()
                    .filter(|c| c.seq < cursor_after.next_seq)
                    .collect();
                for c in local_chunks {
                    if c.seq >= committed_before {
                        out.push(c);
                    }
                }
                return Ok((out, pin_guard));
            }

            let mut by_seq: BTreeMap<u64, ReplayChunkMeta> = BTreeMap::new();
            for c in local_chunks {
                if c.seq >= committed_before {
                    by_seq.insert(c.seq, c);
                }
            }
            for c in s3_chunks {
                // Prefer uploaded S3 `replays/` entry on seq collision.
                by_seq.insert(c.seq, c);
            }
            return Ok((by_seq.into_values().collect(), pin_guard));
        }

        unreachable!("loop returns on attempt == 7")
    }

    /// Appends an immutable `ReplayChunk` object and returns its virtual stream byte offset
    /// (`replay_offset_kb`).
    ///
    /// When `local_hot_tier` is active, writes locally to
    /// `hot/replay/{project_id}/{session_id}/{seq:010}.plsr` with **0 S3 calls**.
    pub async fn append_chunk(&self, chunk: &ReplayChunk) -> Result<ReplayAppendResult> {
        validate_replay_identifier("project_id", &chunk.project_id)?;
        validate_replay_identifier("session_id", &chunk.session_id)?;
        if self.read_only {
            return Err(StorageError::NotWriter {
                project_id: chunk.project_id.clone(),
            });
        }

        let encoded = Bytes::from(chunk.encode_binary(3)?);
        let chunk_bytes = encoded.len();

        if !self.is_local_hot_tier() {
            for _attempt in 0..32u8 {
                let existing = self
                    .list_prefix_chunks(&s3_session_prefix(&chunk.project_id, &chunk.session_id))
                    .await?;
                let byte_offset: u64 = existing.iter().map(|c| c.size).sum();
                let next_seq: u64 = existing.last().map(|c| c.seq + 1).unwrap_or(0);

                let rel_path = s3_chunk_path(&chunk.project_id, &chunk.session_id, next_seq);
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
            return Err(StorageError::ManifestConflict);
        }

        let lock = self.append_lock_for(&chunk.project_id, &chunk.session_id);
        let _guard = lock.lock().await;
        let key = (chunk.project_id.clone(), chunk.session_id.clone());

        for _attempt in 0..32u8 {
            let state = if let Some(existing) = self.session_states.get(&key) {
                *existing
            } else {
                let cursor = self
                    .load_cursor(&chunk.project_id, &chunk.session_id)
                    .await?;
                let committed_before = cursor.local_committed_before_seq();
                let local = self
                    .list_prefix_chunks(&hot_session_prefix(&chunk.project_id, &chunk.session_id))
                    .await?;
                let local_next_seq = local.last().map_or(0, |c| c.seq.saturating_add(1));
                let next_seq = cursor.next_seq.max(local_next_seq);
                let pending_local_bytes: u64 = local
                    .iter()
                    .filter(|c| c.seq >= committed_before)
                    .map(|c| c.size)
                    .sum();
                let st = SessionWriteState {
                    next_seq,
                    total_bytes: cursor.uploaded_bytes.saturating_add(pending_local_bytes),
                };
                self.session_states.insert(key.clone(), st);
                st
            };

            let next_seq = state.next_seq;
            let byte_offset = state.total_bytes;
            let rel_path = hot_chunk_path(&chunk.project_id, &chunk.session_id, next_seq);
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
                    self.session_states.insert(
                        key.clone(),
                        SessionWriteState {
                            next_seq: next_seq.saturating_add(1),
                            total_bytes: byte_offset.saturating_add(chunk_bytes as u64),
                        },
                    );
                    self.index_session(&chunk.project_id, &chunk.session_id);
                    self.dirty_sessions.insert(key);
                    return Ok(ReplayAppendResult {
                        object_path: rel_path,
                        byte_offset,
                        replay_offset_kb: (byte_offset / 1024) as u32,
                        chunk_bytes,
                    });
                }
                Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                    self.session_states.remove(&key);
                    continue;
                }
                Err(other) => return Err(StorageError::ObjectStore(other)),
            }
        }

        Err(StorageError::ManifestConflict)
    }

    async fn delete_local_chunk_or_defer(&self, hot_path: &str) {
        if self.pin_tracker.is_pinned(hot_path) {
            self.pending_local_deletes
                .lock()
                .insert(hot_path.to_string());
            return;
        }
        match self.object_store.delete(&ObjectPath::from(hot_path)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => {}
            Err(e) => {
                tracing::warn!(
                    path = %hot_path,
                    error = %e,
                    "deleting uploaded local replay chunk failed; deferring"
                );
                self.pending_local_deletes
                    .lock()
                    .insert(hot_path.to_string());
            }
        }
    }

    /// Deletes any deferred local `hot/replay/` chunk paths that are no longer pinned by readers.
    pub async fn drain_pending_local_deletes(&self) -> usize {
        let ready: Vec<String> = {
            let guard = self.pending_local_deletes.lock();
            if guard.is_empty() {
                return 0;
            }
            guard
                .iter()
                .filter(|p| !self.pin_tracker.is_pinned(p.as_str()))
                .cloned()
                .collect()
        };
        let mut deleted = 0usize;
        for path in ready {
            match self
                .object_store
                .delete(&ObjectPath::from(path.as_str()))
                .await
            {
                Ok(()) => {
                    self.pending_local_deletes.lock().remove(&path);
                    deleted += 1;
                }
                Err(OsError::NotFound { .. }) => {
                    self.pending_local_deletes.lock().remove(&path);
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path,
                        error = %e,
                        "deferred local replay chunk delete failed; will retry"
                    );
                }
            }
        }
        deleted
    }

    /// Scans `hot/replay/` once at startup / first upload cycle with **0 S3 calls**:
    /// deletes stale local `.plsr` files already committed in `hot/replay_cursors/`, and queues
    /// remaining un-uploaded sessions in `dirty_sessions` and the recorded-session index.
    pub async fn recover_pending_local_sessions_once(&self) -> Result<usize> {
        if !self.is_local_hot_tier() || self.read_only {
            return Ok(0);
        }
        if self.local_scan_done.load(Ordering::Acquire) {
            return Ok(0);
        }
        let _scan_guard = self.local_scan_lock.lock().await;
        if self.local_scan_done.load(Ordering::Acquire) {
            return Ok(0);
        }

        let hot_root = ObjectPath::from("hot/replay");
        let metas: Vec<_> = self
            .object_store
            .list(Some(&hot_root))
            .try_collect()
            .await?;

        let mut sessions: BTreeSet<(String, String)> = BTreeSet::new();
        for m in metas {
            if let Some((pid, sid, _seq)) = parse_replay_object_parts(&m.location) {
                if validate_replay_identifier("project_id", &pid).is_ok()
                    && validate_replay_identifier("session_id", &sid).is_ok()
                {
                    sessions.insert((pid, sid));
                }
            }
        }

        let mut recovered = 0usize;
        for (pid, sid) in sessions {
            let up_lock = self.upload_lock_for(&pid, &sid);
            let _up_guard = up_lock.lock().await;
            let app_lock = self.append_lock_for(&pid, &sid);
            let _app_guard = app_lock.lock().await;

            // Re-list under the per-session locks so a concurrent `delete_session` is never
            // overridden by stale pre-lock listing results.
            let local_chunks = self
                .list_prefix_chunks(&hot_session_prefix(&pid, &sid))
                .await?;
            if local_chunks.is_empty() {
                continue;
            }
            let cursor = self.load_cursor(&pid, &sid).await?;
            let committed_before = cursor.local_committed_before_seq();
            let mut has_pending = false;
            for c in local_chunks {
                if c.seq < committed_before {
                    self.delete_local_chunk_or_defer(c.location.as_ref()).await;
                } else {
                    has_pending = true;
                }
            }
            if has_pending {
                self.index_session(&pid, &sid);
                self.dirty_sessions.insert((pid, sid));
                recovered += 1;
            }
        }

        self.local_scan_done.store(true, Ordering::Release);
        Ok(recovered)
    }

    /// Uploads all pending local replay chunks for `(project_id, session_id)` to S3 `replays/`,
    /// re-sealing `PLSEAGM2` AAD via `copy_if_not_exists`, persisting `hot/replay_cursors/`, and
    /// deleting unpinned local `.plsr` files.
    pub async fn upload_session(&self, project_id: &str, session_id: &str) -> Result<usize> {
        validate_replay_identifier("project_id", project_id)?;
        validate_replay_identifier("session_id", session_id)?;
        if !self.is_local_hot_tier() || self.read_only {
            return Ok(0);
        }

        let up_lock = self.upload_lock_for(project_id, session_id);
        let _up_guard = up_lock.lock().await;
        let app_lock = self.append_lock_for(project_id, session_id);
        let key = (project_id.to_string(), session_id.to_string());

        let (cursor, to_upload) = {
            let _app_guard = app_lock.lock().await;
            let cursor = self.load_cursor(project_id, session_id).await?;
            let committed_before = cursor.local_committed_before_seq();
            let local = self
                .list_prefix_chunks(&hot_session_prefix(project_id, session_id))
                .await?;
            let mut to_upload = Vec::new();
            for c in local {
                if c.seq < committed_before {
                    self.delete_local_chunk_or_defer(c.location.as_ref()).await;
                } else {
                    to_upload.push(c);
                }
            }
            if to_upload.is_empty() {
                self.dirty_sessions.remove(&key);
                return Ok(0);
            }
            (cursor, to_upload)
        };

        let mut cur_cursor = cursor;
        let mut uploaded_locs: Vec<String> = Vec::new();
        let mut uploaded_seqs: HashSet<u64> = HashSet::new();
        let mut uploaded_count: usize = 0;

        for c in &to_upload {
            let target_seq = cur_cursor.next_seq.max(c.seq);
            let s3_loc =
                ObjectPath::from(s3_chunk_path(project_id, session_id, target_seq).as_str());
            match self
                .object_store
                .copy_if_not_exists(&c.location, &s3_loc)
                .await
            {
                Ok(()) => {
                    cur_cursor.next_seq = target_seq.saturating_add(1);
                    cur_cursor.uploaded_bytes = cur_cursor.uploaded_bytes.saturating_add(c.size);
                    if cur_cursor.local_committed_before_seq.is_some() {
                        let next_local = c.seq.saturating_add(1);
                        if next_local > cur_cursor.local_committed_before_seq() {
                            cur_cursor.local_committed_before_seq = Some(next_local);
                        }
                    }
                    uploaded_locs.push(c.location.as_ref().to_string());
                    uploaded_seqs.insert(c.seq);
                    uploaded_count += 1;
                }
                Err(OsError::AlreadyExists { .. }) | Err(OsError::Precondition { .. }) => {
                    let hot_bytes = self.object_store.get(&c.location).await?.bytes().await?;
                    let s3_bytes = self.object_store.get(&s3_loc).await?.bytes().await?;
                    if hot_bytes == s3_bytes {
                        cur_cursor.next_seq = target_seq.saturating_add(1);
                        cur_cursor.uploaded_bytes =
                            cur_cursor.uploaded_bytes.saturating_add(c.size);
                        if cur_cursor.local_committed_before_seq.is_some() {
                            let next_local = c.seq.saturating_add(1);
                            if next_local > cur_cursor.local_committed_before_seq() {
                                cur_cursor.local_committed_before_seq = Some(next_local);
                            }
                        }
                        uploaded_locs.push(c.location.as_ref().to_string());
                        uploaded_seqs.insert(c.seq);
                        uploaded_count += 1;
                    } else {
                        // S3 already contains a different chunk at `target_seq` (e.g., lost cursor
                        // on wiped volume). Serialize with `append_chunk`, shift remaining local
                        // chunks past `max_s3_seq + 1`, and persist the recovered cursor.
                        let _app_guard = app_lock.lock().await;
                        if !uploaded_locs.is_empty() {
                            let _ = self.save_cursor(project_id, session_id, &cur_cursor).await;
                            for loc in uploaded_locs.drain(..) {
                                self.delete_local_chunk_or_defer(&loc).await;
                            }
                        }

                        let s3_existing = self
                            .list_prefix_chunks(&s3_session_prefix(project_id, session_id))
                            .await?;
                        let mut s3_next_seq =
                            s3_existing.last().map_or(0, |x| x.seq.saturating_add(1));
                        let mut s3_total_bytes: u64 = s3_existing.iter().map(|x| x.size).sum();

                        let all_local = self
                            .list_prefix_chunks(&hot_session_prefix(project_id, session_id))
                            .await?;
                        let committed_before = cur_cursor.local_committed_before_seq();
                        let mut shifted_locs: Vec<String> = Vec::new();
                        let mut shifted_local_through = committed_before;
                        for lc in all_local {
                            if lc.seq < committed_before || uploaded_seqs.contains(&lc.seq) {
                                self.delete_local_chunk_or_defer(lc.location.as_ref()).await;
                                continue;
                            }
                            let target = ObjectPath::from(
                                s3_chunk_path(project_id, session_id, s3_next_seq).as_str(),
                            );
                            if let Err(e) = self
                                .object_store
                                .copy_if_not_exists(&lc.location, &target)
                                .await
                            {
                                if !shifted_locs.is_empty() {
                                    let partial = ReplaySessionCursor {
                                        next_seq: s3_next_seq,
                                        uploaded_bytes: s3_total_bytes,
                                        local_committed_before_seq: Some(shifted_local_through),
                                    };
                                    let _ =
                                        self.save_cursor(project_id, session_id, &partial).await;
                                    self.session_states.remove(&key);
                                    for loc in shifted_locs {
                                        self.delete_local_chunk_or_defer(&loc).await;
                                    }
                                }
                                return Err(StorageError::ObjectStore(e));
                            }
                            s3_next_seq = s3_next_seq.saturating_add(1);
                            s3_total_bytes = s3_total_bytes.saturating_add(lc.size);
                            shifted_local_through =
                                shifted_local_through.max(lc.seq.saturating_add(1));
                            shifted_locs.push(lc.location.as_ref().to_string());
                            uploaded_count += 1;
                        }

                        let final_cursor = ReplaySessionCursor {
                            next_seq: s3_next_seq,
                            uploaded_bytes: s3_total_bytes,
                            local_committed_before_seq: None,
                        };
                        self.save_cursor(project_id, session_id, &final_cursor)
                            .await?;
                        self.session_states.insert(
                            key.clone(),
                            SessionWriteState {
                                next_seq: final_cursor.next_seq,
                                total_bytes: final_cursor.uploaded_bytes,
                            },
                        );
                        for loc in shifted_locs {
                            self.delete_local_chunk_or_defer(&loc).await;
                        }
                        self.dirty_sessions.remove(&key);
                        return Ok(uploaded_count);
                    }
                }
                Err(e) => {
                    if !uploaded_locs.is_empty() {
                        let _app_guard = app_lock.lock().await;
                        let _ = self.save_cursor(project_id, session_id, &cur_cursor).await;
                        if cur_cursor.local_committed_before_seq.is_some() {
                            self.session_states.remove(&key);
                        }
                        for loc in &uploaded_locs {
                            self.delete_local_chunk_or_defer(loc).await;
                        }
                    }
                    return Err(StorageError::ObjectStore(e));
                }
            }
        }

        let _app_guard = app_lock.lock().await;
        let remaining = self
            .list_prefix_chunks(&hot_session_prefix(project_id, session_id))
            .await?;
        let committed_before = cur_cursor.local_committed_before_seq();
        if !remaining.iter().any(|c| c.seq >= committed_before) {
            cur_cursor.local_committed_before_seq = None;
            self.save_cursor(project_id, session_id, &cur_cursor)
                .await?;
            self.session_states.insert(
                key.clone(),
                SessionWriteState {
                    next_seq: cur_cursor.next_seq,
                    total_bytes: cur_cursor.uploaded_bytes,
                },
            );
            for loc in &uploaded_locs {
                self.delete_local_chunk_or_defer(loc).await;
            }
            self.dirty_sessions.remove(&key);
        } else {
            self.save_cursor(project_id, session_id, &cur_cursor)
                .await?;
            if cur_cursor.local_committed_before_seq.is_some() {
                self.session_states.remove(&key);
            }
            for loc in &uploaded_locs {
                self.delete_local_chunk_or_defer(loc).await;
            }
        }
        Ok(uploaded_count)
    }

    /// Uploads all dirty local sessions matching `project_filter`, recovering pending local files
    /// once on first call. Makes **0 S3 calls** when clean.
    pub async fn upload_dirty_sessions_with_filter<F>(&self, mut project_filter: F) -> Result<usize>
    where
        F: FnMut(&str) -> bool,
    {
        if !self.is_local_hot_tier() || self.read_only {
            return Ok(0);
        }
        self.recover_pending_local_sessions_once().await?;
        let dirty = self.dirty_sessions_snapshot();
        if dirty.is_empty() {
            return Ok(0);
        }

        let mut total_uploaded = 0usize;
        let mut first_err: Option<StorageError> = None;
        for (pid, sid) in dirty {
            if !project_filter(&pid) {
                continue;
            }
            match self.upload_session(&pid, &sid).await {
                Ok(n) => total_uploaded += n,
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(total_uploaded)
    }

    /// Seeds `hot/replay_cursors/{project_id}/{session_id}.json` from existing S3 `replays/{project_id}/`
    /// objects during `restore --from-published` so post-restore chunk writes continue monotonically.
    pub async fn seed_cursors_from_s3(
        object_store: &Arc<dyn ObjectStore>,
        project_id: &str,
    ) -> Result<usize> {
        validate_replay_identifier("project_id", project_id)?;
        let prefix = ObjectPath::from(format!("replays/{project_id}/"));
        let metas: Vec<_> = object_store.list(Some(&prefix)).try_collect().await?;
        let mut by_session: HashMap<String, (u64, u64)> = HashMap::new();
        for m in metas {
            if let Some((pid, sid, seq)) = parse_replay_object_parts(&m.location) {
                if pid == project_id {
                    let entry = by_session.entry(sid).or_insert((0, 0));
                    entry.0 = entry.0.max(seq.saturating_add(1));
                    entry.1 = entry.1.saturating_add(m.size);
                }
            }
        }
        let count = by_session.len();
        for (sid, (next_seq, uploaded_bytes)) in by_session {
            let cursor = ReplaySessionCursor {
                next_seq,
                uploaded_bytes,
                local_committed_before_seq: None,
            };
            let path = ObjectPath::from(hot_cursor_path(project_id, &sid).as_str());
            let payload = serde_json::to_vec(&cursor)?;
            object_store
                .put_opts(
                    &path,
                    PutPayload::from_bytes(Bytes::from(payload)),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        ..PutOptions::default()
                    },
                )
                .await?;
        }
        Ok(count)
    }

    async fn read_chunk_full(&self, c: &ReplayChunkMeta) -> Result<Bytes> {
        let res = self.object_store.get(&c.location).await?;
        Ok(res.bytes().await?)
    }

    async fn read_chunk_range(&self, c: &ReplayChunkMeta, range: Range<u64>) -> Result<Bytes> {
        Ok(self.object_store.get_range(&c.location, range).await?)
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
        for attempt in 0..4u8 {
            let (chunks, _pin_guard) = self
                .list_and_pin_session_chunks(project_id, session_id, true)
                .await?;
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
            let mut retry_after_hot_unlinked = false;
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
                let res = if rel_start == 0 && rel_end == c.size {
                    self.read_chunk_full(&c).await
                } else {
                    self.read_chunk_range(&c, rel_start..rel_end).await
                };
                match res {
                    Ok(b) => out.extend_from_slice(&b),
                    Err(StorageError::ObjectStore(OsError::NotFound { .. }))
                        if attempt < 3 && c.location.as_ref().starts_with("hot/replay/") =>
                    {
                        retry_after_hot_unlinked = true;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            if retry_after_hot_unlinked {
                drop(_pin_guard);
                tokio::task::yield_now().await;
                continue;
            }
            return Ok(Some(out.freeze()));
        }
        unreachable!("loop returns on attempt == 3")
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
        for attempt in 0..4u8 {
            let (chunks, _pin_guard) = self
                .list_and_pin_session_chunks(project_id, session_id, true)
                .await?;
            if chunks.is_empty() {
                return Ok(Vec::new());
            }

            let mut out = Vec::new();
            let mut cursor = 0u64;
            let mut retry_after_hot_unlinked = false;
            for c in chunks {
                let chunk_end = cursor + c.size;
                if chunk_end > start_byte_offset {
                    match self.read_chunk_full(&c).await {
                        Ok(raw) => {
                            let mut decoded = ReplayChunk::decode_stream(&raw)?;
                            out.append(&mut decoded);
                        }
                        Err(StorageError::ObjectStore(OsError::NotFound { .. }))
                            if attempt < 3 && c.location.as_ref().starts_with("hot/replay/") =>
                        {
                            retry_after_hot_unlinked = true;
                            break;
                        }
                        Err(e) => return Err(e),
                    }
                }
                cursor = chunk_end;
            }
            if retry_after_hot_unlinked {
                drop(_pin_guard);
                tokio::task::yield_now().await;
                continue;
            }
            return Ok(out);
        }
        unreachable!("loop returns on attempt == 3")
    }

    /// Deletes all `.plsr` replay chunks (both S3 `replays/` and local `hot/replay/` +
    /// `hot/replay_cursors/`) belonging to `session_id` under `project_id` (`Gap 4`).
    pub async fn delete_session(&self, project_id: &str, session_id: &str) -> Result<bool> {
        validate_replay_identifier("project_id", project_id)?;
        validate_replay_identifier("session_id", session_id)?;

        let up_lock = self.upload_lock_for(project_id, session_id);
        let _up_guard = up_lock.lock().await;
        let app_lock = self.append_lock_for(project_id, session_id);
        let _app_guard = app_lock.lock().await;

        let s3_chunks = self
            .list_prefix_chunks(&s3_session_prefix(project_id, session_id))
            .await?;
        let local_chunks = if self.is_local_hot_tier() {
            self.list_prefix_chunks(&hot_session_prefix(project_id, session_id))
                .await?
        } else {
            Vec::new()
        };

        let had_any = !s3_chunks.is_empty() || !local_chunks.is_empty();
        for c in s3_chunks {
            let _ = self.object_store.delete(&c.location).await;
        }
        for c in local_chunks {
            let _ = self.object_store.delete(&c.location).await;
        }
        if self.is_local_hot_tier() {
            let cursor_path = ObjectPath::from(hot_cursor_path(project_id, session_id).as_str());
            let _ = self.object_store.delete(&cursor_path).await;
            let hot_pref = hot_session_prefix(project_id, session_id);
            self.pending_local_deletes
                .lock()
                .retain(|p| !p.starts_with(&hot_pref));
        }

        let key = (project_id.to_string(), session_id.to_string());
        self.dirty_sessions.remove(&key);
        self.session_states.remove(&key);
        {
            let mut gens = self.project_invalidation_gen.lock();
            let epoch = gens.entry(project_id.to_string()).or_insert(0);
            *epoch = epoch.wrapping_add(1);
            self.remove_session_from_index(project_id, session_id);
        }
        Ok(had_any)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_factory::{with_hot_store, TEST_ENV_LOCK as ENV_LOCK};
    use crate::EncryptedObjectStore;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutResult, Result as OsResult,
    };
    use pulse_wire::{InteractionKind, NodeKind, WireNode, WireframeOp};
    use std::fmt;
    use std::sync::atomic::AtomicUsize;

    struct EnvGuard {
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set_local() -> Self {
            let prev = std::env::var("PULSE_HOT_STORE").ok();
            std::env::set_var("PULSE_HOT_STORE", "local");
            Self { prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var("PULSE_HOT_STORE", v),
                None => std::env::remove_var("PULSE_HOT_STORE"),
            }
        }
    }

    #[derive(Debug, Default)]
    struct FailNthS3PutStore {
        inner: InMemory,
        s3_puts: AtomicUsize,
        fail_on_put_num: AtomicUsize, // 0 = disabled
        calls: SyncMutex<BTreeMap<&'static str, usize>>,
    }

    impl FailNthS3PutStore {
        fn count(&self, op: &'static str) {
            *self.calls.lock().entry(op).or_default() += 1;
        }
        fn take_calls(&self) -> BTreeMap<&'static str, usize> {
            std::mem::take(&mut *self.calls.lock())
        }
    }

    impl fmt::Display for FailNthS3PutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FailNthS3PutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailNthS3PutStore {
        async fn put_opts(
            &self,
            l: &ObjectPath,
            p: PutPayload,
            o: PutOptions,
        ) -> OsResult<PutResult> {
            self.count("PUT");
            let n = self.s3_puts.fetch_add(1, Ordering::SeqCst) + 1;
            let fail_num = self.fail_on_put_num.load(Ordering::SeqCst);
            if fail_num != 0 && n == fail_num {
                return Err(OsError::Generic {
                    store: "FailNthS3PutStore",
                    source: Box::new(std::io::Error::other("injected S3 PUT failure")),
                });
            }
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjectPath,
            o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.count("PUT");
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
            self.count("GET");
            self.inner.get_opts(l, o).await
        }
        async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
            self.count("DELETE");
            self.inner.delete(l).await
        }
        fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.count("LIST");
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
            self.count("LIST");
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.count("PUT");
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.count("PUT");
            let n = self.s3_puts.fetch_add(1, Ordering::SeqCst) + 1;
            let fail_num = self.fail_on_put_num.load(Ordering::SeqCst);
            if fail_num != 0 && n == fail_num {
                return Err(OsError::Generic {
                    store: "FailNthS3PutStore",
                    source: Box::new(std::io::Error::other(
                        "injected S3 copy_if_not_exists failure",
                    )),
                });
            }
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn sample_chunk(pid: &str, sid: &str, ts_offset_ms: u32) -> ReplayChunk {
        ReplayChunk {
            project_id: pid.to_string(),
            session_id: sid.to_string(),
            chunk_start_ts_ms: 1_762_000_000_000,
            ops: vec![
                WireframeOp::FullSnapshot {
                    ts_offset_ms,
                    width: 390,
                    height: 844,
                    nodes: vec![WireNode {
                        node_id: 1,
                        parent_id: 0,
                        kind: NodeKind::Screen,
                        x: 0,
                        y: 0,
                        w: 390,
                        h: 844,
                        bg_rgba: 0x090D_16FF,
                        masked_label: "Home".to_string(),
                    }],
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: ts_offset_ms + 50,
                    x: 100,
                    y: 200,
                    kind: InteractionKind::Tap,
                },
            ],
        }
    }

    #[tokio::test]
    async fn local_tier_zero_s3_append_reseal_upload_and_continuation() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let tmp = tempfile::tempdir().unwrap();
        let s3 = Arc::new(FailNthS3PutStore::default());
        let hot_routed = with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(hot_routed.clone()));
        enc.set_master_key(Some([0x42u8; 32]));
        let store = ReplayStore::new_with_options(enc.as_object_store(), true, false);

        s3.take_calls();
        let r0 = store
            .append_chunk(&sample_chunk("proj_1", "sess_1", 0))
            .await
            .unwrap();
        let r1 = store
            .append_chunk(&sample_chunk("proj_1", "sess_1", 1000))
            .await
            .unwrap();
        assert_eq!(r0.object_path, "hot/replay/proj_1/sess_1/0000000000.plsr");
        assert_eq!(r1.object_path, "hot/replay/proj_1/sess_1/0000000001.plsr");
        assert_eq!(r0.byte_offset, 0);
        assert_eq!(r1.byte_offset, r0.chunk_bytes as u64);
        assert!(
            s3.take_calls().is_empty(),
            "append_chunk must make 0 S3 calls"
        );
        assert!(store.has_session("proj_1", "sess_1").await);
        assert!(
            s3.take_calls().is_empty(),
            "has_session for local replay must make 0 S3 calls"
        );

        // Verify local chunk is encrypted with PLSEAGM2.
        let raw_local_head = hot_routed
            .get_range(&ObjectPath::from(r0.object_path.as_str()), 0..8)
            .await
            .unwrap();
        assert_eq!(raw_local_head.as_ref(), b"PLSEAGM2");

        // Cycle upload re-seals to S3 `replays/` and deletes local `hot/replay/`.
        let uploaded = store
            .upload_dirty_sessions_with_filter(|_| true)
            .await
            .unwrap();
        assert_eq!(uploaded, 2);
        let calls = s3.take_calls();
        assert_eq!(calls.get("PUT").copied(), Some(2));
        assert!(hot_routed
            .list(Some(&ObjectPath::from("hot/replay")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());

        // Idle cycle makes 0 S3 calls.
        assert_eq!(
            store
                .upload_dirty_sessions_with_filter(|_| true)
                .await
                .unwrap(),
            0
        );
        assert!(
            s3.take_calls().is_empty(),
            "idle cycle must make 0 S3 calls"
        );

        // Fresh ReplayStore (simulating restart) continues at seq 2 with 0 S3 calls on append!
        let store2 = ReplayStore::new_with_options(enc.as_object_store(), true, false);
        assert_eq!(
            store2.recover_pending_local_sessions_once().await.unwrap(),
            0
        );
        s3.take_calls();
        let r2 = store2
            .append_chunk(&sample_chunk("proj_1", "sess_1", 2000))
            .await
            .unwrap();
        assert_eq!(r2.object_path, "hot/replay/proj_1/sess_1/0000000002.plsr");
        assert_eq!(r2.byte_offset, (r0.chunk_bytes + r1.chunk_bytes) as u64);
        assert!(s3.take_calls().is_empty());

        // Hybrid read across S3 (0, 1) + local (2), and read-only Query store reads S3 (0, 1).
        let all_chunks = store2.read_chunks("proj_1", "sess_1", 0).await.unwrap();
        assert_eq!(all_chunks.len(), 3);
        let total = store2
            .total_session_bytes("proj_1", "sess_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            total,
            (r0.chunk_bytes + r1.chunk_bytes + r2.chunk_bytes) as u64
        );
        let q_store = ReplayStore::new_with_options(
            Arc::new(EncryptedObjectStore::new(s3.clone())),
            false,
            true,
        );
        // Copy keyring by wrapping `s3` via shared keyring in real node; here test unencrypted S3 read or shared key:
        let enc_q = Arc::new(EncryptedObjectStore::new(s3.clone()));
        enc_q.set_master_key(Some([0x42u8; 32]));
        let q_store_enc = ReplayStore::new_with_options(enc_q.as_object_store(), false, true);
        let s3_chunks = q_store_enc
            .read_chunks("proj_1", "sess_1", 0)
            .await
            .unwrap();
        assert_eq!(s3_chunks.len(), 2);
        drop(q_store);
    }

    #[tokio::test]
    async fn partial_upload_crash_recovery_and_already_exists_idempotency() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let tmp = tempfile::tempdir().unwrap();
        let s3 = Arc::new(FailNthS3PutStore::default());
        let hot_routed = with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(hot_routed.clone()));
        enc.set_master_key(Some([0x55u8; 32]));
        let store = ReplayStore::new_with_options(enc.as_object_store(), true, false);

        for i in 0..3u32 {
            store
                .append_chunk(&sample_chunk("proj_c", "sess_c", i * 100))
                .await
                .unwrap();
        }
        // Fail on the 2nd S3 PUT (seq 1), so seq 0 succeeds and seq 1..2 fail.
        s3.fail_on_put_num.store(2, Ordering::SeqCst);
        let err = store.upload_session("proj_c", "sess_c").await.unwrap_err();
        assert!(matches!(err, StorageError::ObjectStore(_)));

        // Cursor persisted for seq 0, local seq 0 deleted, seq 1 & 2 still on disk.
        let remaining: Vec<_> = hot_routed
            .list(Some(&ObjectPath::from("hot/replay/proj_c/sess_c")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(remaining.len(), 2);

        // Also simulate a pre-cursor crash where seq 1 was copied to S3 before cursor updated.
        s3.fail_on_put_num.store(0, Ordering::SeqCst);
        enc.copy_if_not_exists(
            &ObjectPath::from("hot/replay/proj_c/sess_c/0000000001.plsr"),
            &ObjectPath::from("replays/proj_c/sess_c/0000000001.plsr"),
        )
        .await
        .unwrap();

        // Restarted ReplayStore recovers pending local sessions, handles AlreadyExists on seq 1
        // idempotently, uploads seq 2, and leaves 0 local chunks.
        let store2 = ReplayStore::new_with_options(enc.as_object_store(), true, false);
        assert_eq!(
            store2.recover_pending_local_sessions_once().await.unwrap(),
            1
        );
        let uploaded = store2
            .upload_dirty_sessions_with_filter(|_| true)
            .await
            .unwrap();
        assert_eq!(uploaded, 2);
        assert!(hot_routed
            .list(Some(&ObjectPath::from("hot/replay/proj_c/sess_c")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store2
                .read_chunks("proj_c", "sess_c", 0)
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn pin_tracker_defers_local_delete_and_delete_session_purges_both_tiers() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let tmp = tempfile::tempdir().unwrap();
        let s3 = Arc::new(FailNthS3PutStore::default());
        let hot_routed = with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let store = ReplayStore::new_with_options(hot_routed.clone(), true, false);

        let r0 = store
            .append_chunk(&sample_chunk("proj_p", "sess_p", 0))
            .await
            .unwrap();
        store
            .append_chunk(&sample_chunk("proj_p", "sess_p", 500))
            .await
            .unwrap();

        // Pin r0 as if a reader is actively streaming it during upload.
        store.pin_tracker().pin_all(&[r0.object_path.clone()]);
        assert_eq!(store.upload_session("proj_p", "sess_p").await.unwrap(), 2);
        assert_eq!(store.pending_local_deletes_len(), 1);
        assert!(hot_routed
            .head(&ObjectPath::from(r0.object_path.as_str()))
            .await
            .is_ok());

        // Once unpinned, drain removes the deferred local chunk.
        store.pin_tracker().unpin_all(&[r0.object_path.clone()]);
        assert_eq!(store.drain_pending_local_deletes().await, 1);
        assert_eq!(store.pending_local_deletes_len(), 0);
        assert!(hot_routed
            .head(&ObjectPath::from(r0.object_path.as_str()))
            .await
            .is_err());

        // Append another local chunk so session spans both S3 and local, then delete_session.
        store
            .append_chunk(&sample_chunk("proj_p", "sess_p", 1000))
            .await
            .unwrap();
        assert!(store.delete_session("proj_p", "sess_p").await.unwrap());
        assert!(!store.delete_session("proj_p", "sess_p").await.unwrap());
        assert!(!store.has_session("proj_p", "sess_p").await);
        assert!(hot_routed
            .list(Some(&ObjectPath::from("hot/replay/proj_p/sess_p")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
        assert!(hot_routed
            .list(Some(&ObjectPath::from("hot/replay_cursors/proj_p")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
        assert!(s3
            .list(Some(&ObjectPath::from("replays/proj_p/sess_p")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn recover_pending_vs_delete_session_does_not_resurrect() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let tmp = tempfile::tempdir().unwrap();
        let s3 = Arc::new(FailNthS3PutStore::default());
        let hot_routed = with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(hot_routed.clone()));
        let writer = ReplayStore::new_with_options(enc.as_object_store(), true, false);

        writer
            .append_chunk(&sample_chunk("proj_r", "sess_a", 0))
            .await
            .unwrap();
        writer
            .append_chunk(&sample_chunk("proj_r", "sess_b", 0))
            .await
            .unwrap();

        // Simulate restart: fresh ReplayStore hasn't scanned `hot/replay/` yet.
        let store = Arc::new(ReplayStore::new_with_options(
            enc.as_object_store(),
            true,
            false,
        ));
        // Hold `sess_a`'s upload lock so `recover_pending_local_sessions_once` pauses right after
        // listing `hot/replay/` (which already observed `sess_b`) and before processing `sess_b`.
        let lock_a = store.upload_lock_for("proj_r", "sess_a");
        let guard_a = lock_a.lock().await;
        let store_rec = store.clone();
        let rec_task = tokio::spawn(async move {
            store_rec
                .recover_pending_local_sessions_once()
                .await
                .unwrap()
        });
        // Give `rec_task` a moment to complete `list("hot/replay")` and block on `lock_a`.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert!(store.delete_session("proj_r", "sess_b").await.unwrap());
        drop(guard_a);

        let recovered = rec_task.await.unwrap();
        assert_eq!(recovered, 1, "only sess_a should be recovered");
        assert!(
            !store.has_session("proj_r", "sess_b").await,
            "deleted session must NOT be resurrected by recover_pending_local_sessions_once"
        );
        assert_eq!(
            store.dirty_sessions_snapshot(),
            vec![("proj_r".to_string(), "sess_a".to_string())]
        );
        let hashes = store.recorded_hashes_for_project("proj_r").await;
        assert!(hashes.contains(&xxh3_64(b"sess_a")));
        assert!(!hashes.contains(&xxh3_64(b"sess_b")));
    }

    #[tokio::test]
    async fn pinned_already_uploaded_chunk_not_duplicated_on_already_exists_shift() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let tmp = tempfile::tempdir().unwrap();
        let s3 = Arc::new(FailNthS3PutStore::default());
        let hot_routed = with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(hot_routed.clone()));
        let store = ReplayStore::new_with_options(enc.as_object_store(), true, false);

        // Append & upload chunk 0 while a reader pins local chunk 0 so it stays on disk.
        let r0 = store
            .append_chunk(&sample_chunk("proj_pin", "sess_pin", 0))
            .await
            .unwrap();
        store.pin_tracker().pin_all(&[r0.object_path.clone()]);
        assert_eq!(
            store.upload_session("proj_pin", "sess_pin").await.unwrap(),
            1
        );
        assert_eq!(store.pending_local_deletes_len(), 1);

        // Plant an alien S3 chunk at seq 1 with differing bytes, then append local chunk 1.
        let alien = Bytes::from(
            sample_chunk("proj_pin", "sess_pin", 999)
                .encode_binary(3)
                .unwrap(),
        );
        s3.put_opts(
            &ObjectPath::from("replays/proj_pin/sess_pin/0000000001.plsr"),
            PutPayload::from_bytes(alien),
            PutOptions::default(),
        )
        .await
        .unwrap();
        store
            .append_chunk(&sample_chunk("proj_pin", "sess_pin", 100))
            .await
            .unwrap();

        let uploaded = store.upload_session("proj_pin", "sess_pin").await.unwrap();
        assert_eq!(uploaded, 1, "only local chunk 1 should be shifted/uploaded");

        let s3_objs: Vec<_> = s3
            .list(Some(&ObjectPath::from("replays/proj_pin/sess_pin")))
            .try_collect()
            .await
            .unwrap();
        let mut s3_paths: Vec<String> = s3_objs
            .into_iter()
            .map(|m| m.location.as_ref().to_string())
            .collect();
        s3_paths.sort();
        assert_eq!(
            s3_paths,
            vec![
                "replays/proj_pin/sess_pin/0000000000.plsr",
                "replays/proj_pin/sess_pin/0000000001.plsr",
                "replays/proj_pin/sess_pin/0000000002.plsr",
            ],
            "pinned chunk 0 must not be duplicated to S3 on AlreadyExists shift"
        );

        store.pin_tracker().unpin_all(&[r0.object_path]);
        assert_eq!(store.drain_pending_local_deletes().await, 1);
        assert!(hot_routed
            .list(Some(&ObjectPath::from("hot/replay/proj_pin/sess_pin")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .read_chunks("proj_pin", "sess_pin", 0)
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn partial_failure_during_already_exists_shift_preserves_unshifted_chunks() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let tmp = tempfile::tempdir().unwrap();
        let s3 = Arc::new(FailNthS3PutStore::default());
        let hot_routed = with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(hot_routed.clone()));
        let store = ReplayStore::new_with_options(enc.as_object_store(), true, false);

        // Plant alien S3 chunks 0 and 1 directly in S3.
        let mut alien_bytes = 0u64;
        for seq in 0..2u32 {
            let encoded = Bytes::from(
                sample_chunk("proj_ps", "sess_ps", 900 + seq)
                    .encode_binary(3)
                    .unwrap(),
            );
            alien_bytes += encoded.len() as u64;
            enc.put_opts(
                &ObjectPath::from(format!("replays/proj_ps/sess_ps/{seq:010}.plsr")),
                PutPayload::from_bytes(encoded),
                PutOptions::default(),
            )
            .await
            .unwrap();
        }

        // Append 3 local chunks (seq 0, 1, 2) whose cursor is behind S3.
        let mut local_bytes = 0u64;
        for i in 0..3u32 {
            let res = store
                .append_chunk(&sample_chunk("proj_ps", "sess_ps", i * 100))
                .await
                .unwrap();
            local_bytes += res.chunk_bytes as u64;
        }

        // Reset S3 PUT counter and fail on PUT #3:
        // #1: copy_if_not_exists(local 0 -> S3 0) -> AlreadyExists (triggers differing-bytes shift)
        // #2: copy_if_not_exists(local 0 -> S3 2) -> Ok
        // #3: copy_if_not_exists(local 1 -> S3 3) -> injected error
        s3.s3_puts.store(0, Ordering::SeqCst);
        s3.fail_on_put_num.store(3, Ordering::SeqCst);
        let err = store
            .upload_session("proj_ps", "sess_ps")
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::ObjectStore(_)));

        // Un-shifted local chunks 1 and 2 must remain on disk while shifted chunk 0 was deleted.
        let mut remaining_local: Vec<String> = hot_routed
            .list(Some(&ObjectPath::from("hot/replay/proj_ps/sess_ps")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.as_ref().to_string())
            .collect();
        remaining_local.sort();
        assert_eq!(
            remaining_local,
            vec![
                "hot/replay/proj_ps/sess_ps/0000000001.plsr",
                "hot/replay/proj_ps/sess_ps/0000000002.plsr",
            ]
        );

        // Recover on a fresh ReplayStore after S3 recovers; un-shifted chunks 1 and 2 must NOT be
        // discarded and must upload to S3 seq 3 and 4 with 0 data loss and 0 duplication.
        s3.fail_on_put_num.store(0, Ordering::SeqCst);
        let store2 = ReplayStore::new_with_options(enc.as_object_store(), true, false);
        assert_eq!(
            store2.recover_pending_local_sessions_once().await.unwrap(),
            1
        );
        let uploaded = store2
            .upload_dirty_sessions_with_filter(|_| true)
            .await
            .unwrap();
        assert_eq!(uploaded, 2);

        let mut s3_paths: Vec<String> = s3
            .list(Some(&ObjectPath::from("replays/proj_ps/sess_ps")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.as_ref().to_string())
            .collect();
        s3_paths.sort();
        assert_eq!(
            s3_paths,
            vec![
                "replays/proj_ps/sess_ps/0000000000.plsr",
                "replays/proj_ps/sess_ps/0000000001.plsr",
                "replays/proj_ps/sess_ps/0000000002.plsr",
                "replays/proj_ps/sess_ps/0000000003.plsr",
                "replays/proj_ps/sess_ps/0000000004.plsr",
            ]
        );
        assert!(hot_routed
            .list(Some(&ObjectPath::from("hot/replay/proj_ps/sess_ps")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store2
                .read_chunks("proj_ps", "sess_ps", 0)
                .await
                .unwrap()
                .len(),
            5
        );

        // Post-recovery append must continue at seq 5 with exact byte offset.
        let r5 = store2
            .append_chunk(&sample_chunk("proj_ps", "sess_ps", 300))
            .await
            .unwrap();
        assert_eq!(r5.object_path, "hot/replay/proj_ps/sess_ps/0000000005.plsr");
        assert_eq!(r5.byte_offset, alien_bytes + local_bytes);
    }

    #[tokio::test]
    async fn warm_project_does_not_resurrect_session_invalidated_or_deleted_mid_list() {
        use tokio::sync::Notify;

        #[derive(Debug)]
        struct PauseFirstListStore {
            inner: Arc<InMemory>,
            started: Arc<Notify>,
            release: Arc<Notify>,
            list_calls: AtomicUsize,
        }

        impl fmt::Display for PauseFirstListStore {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "PauseFirstListStore({})", self.inner)
            }
        }

        #[async_trait]
        impl ObjectStore for PauseFirstListStore {
            async fn put_opts(
                &self,
                l: &ObjectPath,
                p: PutPayload,
                o: PutOptions,
            ) -> OsResult<PutResult> {
                self.inner.put_opts(l, p, o).await
            }
            async fn put_multipart_opts(
                &self,
                l: &ObjectPath,
                o: PutMultipartOptions,
            ) -> OsResult<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(l, o).await
            }
            async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
                self.inner.get_opts(l, o).await
            }
            async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
                self.inner.delete(l).await
            }
            fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
                let inner_stream = self.inner.list(p);
                let is_replays = p
                    .map(|path| path.as_ref().starts_with("replays/proj_race"))
                    .unwrap_or(false);
                if is_replays && self.list_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let started = self.started.clone();
                    let release = self.release.clone();
                    return Box::pin(
                        futures::stream::once(async move {
                            // Collect the pre-delete snapshot first, then pause before yielding.
                            let items: Vec<OsResult<ObjectMeta>> = inner_stream.collect().await;
                            started.notify_one();
                            release.notified().await;
                            futures::stream::iter(items)
                        })
                        .flatten(),
                    );
                }
                inner_stream
            }
            async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
                self.inner.list_with_delimiter(p).await
            }
            async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
                self.inner.copy(from, to).await
            }
            async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
                self.inner.copy_if_not_exists(from, to).await
            }
        }

        use futures::StreamExt;
        let inner = Arc::new(InMemory::new());
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let wrapped = Arc::new(PauseFirstListStore {
            inner: inner.clone(),
            started: started.clone(),
            release: release.clone(),
            list_calls: AtomicUsize::new(0),
        });

        let writer = ReplayStore::new_with_options(inner.clone(), false, false);
        writer
            .append_chunk(&sample_chunk("proj_race", "sess_deleted", 0))
            .await
            .unwrap();
        writer
            .append_chunk(&sample_chunk("proj_race", "sess_survivor", 0))
            .await
            .unwrap();

        let reader = Arc::new(ReplayStore::new_with_options(wrapped, false, true));
        let reader_bg = reader.clone();
        let warm_task =
            tokio::spawn(async move { reader_bg.recorded_hashes_for_project("proj_race").await });

        started.notified().await;
        // Delete `sess_deleted` from the backing store and invalidate `reader` while `warm_project`
        // is mid-list holding the pre-delete snapshot.
        assert!(writer
            .delete_session("proj_race", "sess_deleted")
            .await
            .unwrap());
        reader.invalidate_project("proj_race");
        release.notify_one();

        let hashes = warm_task.await.unwrap();
        assert!(
            !hashes.contains(&xxh3_64(b"sess_deleted")),
            "warm_project must not resurrect deleted session: {hashes:?}"
        );
        assert!(
            hashes.contains(&xxh3_64(b"sess_survivor")),
            "surviving session must remain indexed after retry: {hashes:?}"
        );
        assert!(
            !reader
                .recorded_sessions
                .contains(&("proj_race".to_string(), "sess_deleted".to_string())),
            "recorded_sessions must not contain deleted session"
        );
    }

    #[tokio::test]
    async fn list_session_chunks_preserves_unshifted_local_chunks_after_partial_shift() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let s3 = Arc::new(InMemory::new());
        let tmp = tempfile::tempdir().unwrap();
        let routed = crate::with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let store = ReplayStore::new_with_options(routed.clone(), true, false);

        // Pre-existing S3 chunks seq=0 and seq=1 (e.g., from an earlier volume), plus local
        // hot chunks seq=0 (already shifted/uploaded to S3 seq=2) and seq=1 (not yet shifted).
        for (i, path) in [
            (0u32, "replays/proj_shift/sess_s/0000000000.plsr"),
            (1, "replays/proj_shift/sess_s/0000000001.plsr"),
            (2, "replays/proj_shift/sess_s/0000000002.plsr"),
            (3, "hot/replay/proj_shift/sess_s/0000000000.plsr"),
            (4, "hot/replay/proj_shift/sess_s/0000000001.plsr"),
        ] {
            let chunk = sample_chunk("proj_shift", "sess_s", i);
            let encoded = Bytes::from(chunk.encode_binary(3).unwrap());
            routed
                .put(&ObjectPath::from(path), PutPayload::from_bytes(encoded))
                .await
                .unwrap();
        }
        let cursor = ReplaySessionCursor {
            next_seq: 3,
            uploaded_bytes: 300,
            local_committed_before_seq: Some(1),
        };
        store
            .save_cursor("proj_shift", "sess_s", &cursor)
            .await
            .unwrap();

        let listed = store
            .list_session_chunks("proj_shift", "sess_s")
            .await
            .unwrap();
        let paths: Vec<&str> = listed.iter().map(|c| c.location.as_ref()).collect();
        assert_eq!(
            paths,
            vec![
                "replays/proj_shift/sess_s/0000000000.plsr",
                "replays/proj_shift/sess_s/0000000001.plsr",
                "replays/proj_shift/sess_s/0000000002.plsr",
                "hot/replay/proj_shift/sess_s/0000000001.plsr",
            ],
            "unshifted local seq=1 must not be masked by S3 seq=1"
        );
        let decoded = store.read_chunks("proj_shift", "sess_s", 0).await.unwrap();
        assert_eq!(decoded.len(), 4);
    }

    #[tokio::test]
    async fn list_session_chunks_excludes_uncommitted_in_flight_shifted_s3_chunks() {
        let _lock = ENV_LOCK.lock().await;
        let _env = EnvGuard::set_local();
        let s3 = Arc::new(InMemory::new());
        let tmp = tempfile::tempdir().unwrap();
        let routed = crate::with_hot_store(s3.clone(), tmp.path()).await.unwrap();
        let store = ReplayStore::new_with_options(routed.clone(), true, false);

        // Simulate the exact window after `copy_if_not_exists` copied local seq=1 -> S3 seq=3 and
        // local seq=2 -> S3 seq=4, strictly before `save_cursor(&final_cursor)` persists:
        // persisted cursor still has `next_seq = 3, local_committed_before_seq = Some(1)`,
        // and local chunks seq=1, seq=2 are still on disk awaiting `save_cursor` + delete.
        for (i, path) in [
            (0u32, "replays/proj_win/sess_w/0000000000.plsr"),
            (1, "replays/proj_win/sess_w/0000000001.plsr"),
            (2, "replays/proj_win/sess_w/0000000002.plsr"),
            (3, "replays/proj_win/sess_w/0000000003.plsr"),
            (4, "replays/proj_win/sess_w/0000000004.plsr"),
            (3, "hot/replay/proj_win/sess_w/0000000001.plsr"),
            (4, "hot/replay/proj_win/sess_w/0000000002.plsr"),
        ] {
            let chunk = sample_chunk("proj_win", "sess_w", i);
            let encoded = Bytes::from(chunk.encode_binary(3).unwrap());
            routed
                .put(&ObjectPath::from(path), PutPayload::from_bytes(encoded))
                .await
                .unwrap();
        }
        let cursor = ReplaySessionCursor {
            next_seq: 3,
            uploaded_bytes: 300,
            local_committed_before_seq: Some(1),
        };
        store
            .save_cursor("proj_win", "sess_w", &cursor)
            .await
            .unwrap();

        let listed = store
            .list_session_chunks("proj_win", "sess_w")
            .await
            .unwrap();
        let paths: Vec<&str> = listed.iter().map(|c| c.location.as_ref()).collect();
        assert_eq!(
            paths,
            vec![
                "replays/proj_win/sess_w/0000000000.plsr",
                "replays/proj_win/sess_w/0000000001.plsr",
                "replays/proj_win/sess_w/0000000002.plsr",
                "hot/replay/proj_win/sess_w/0000000001.plsr",
                "hot/replay/proj_win/sess_w/0000000002.plsr",
            ],
            "uncommitted in-flight S3 copies >= cursor.next_seq must not duplicate local chunks"
        );
        let decoded = store.read_chunks("proj_win", "sess_w", 0).await.unwrap();
        assert_eq!(decoded.len(), 5);
    }
}
