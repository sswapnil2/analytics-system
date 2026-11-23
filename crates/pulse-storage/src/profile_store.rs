//! Object-store-backed durable User Profile Store built on SlateDB.
//!
//! Replaces the Postgres/SQLite `user_profiles` table so high-volume profile mutations,
//! alias merges, DPDP erasures, Studio pagination, and incremental query-node sync scale
//! independently of the control-plane relational database.
//!
//! ## Key Layout (under `profiles/<project_id>` on `EncryptedObjectStore`)
//! - `r/<distinct_id>`                              -> zstd-compressed JSON [`StoredUserProfile`]
//! - `c/<canonical_uid:08x>/<distinct_id>`          -> secondary index for O(aliases) canonical lookup
//! - `t/<u64::MAX - updated_ms:016x>/<distinct_id>` -> reverse-timestamp index for O(limit) newest-first pagination
//! - `u/<updated_ms:016x>/<distinct_id>`            -> forward-timestamp index for incremental sync since `since_ms`

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use pulse_identity::{IdentitySnapshot, ProjectIdentityGraph};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slatedb::config::{
    CompactorOptions, DbOptions, DbReaderOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, GcExecutionMode, WriteOptions,
};
use slatedb::{Db, DbReader, SlateDBError, WriteBatch};

use crate::erased::ErasedUids;
use crate::error::{Result, StorageError};

const PROFILE_ZSTD_MAGIC: &[u8; 4] = b"PZST";

/// Durable user profile document stored per `(project_id, distinct_id)` in SlateDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredUserProfile {
    pub project_id: String,
    pub distinct_id: String,
    pub canonical_uid: u32,
    pub props: BTreeMap<String, Value>,
    pub total_revenue: f64,
    pub updated_at_ms: i64,
}

fn encode_profile(p: &StoredUserProfile) -> Result<Vec<u8>> {
    let raw = serde_json::to_vec(p)
        .map_err(|e| StorageError::Corrupted(format!("failed to serialize user profile: {e}")))?;
    let compressed = zstd::bulk::compress(&raw, 1)
        .map_err(|e| StorageError::Corrupted(format!("failed to compress user profile: {e}")))?;
    let mut out = Vec::with_capacity(PROFILE_ZSTD_MAGIC.len() + compressed.len());
    out.extend_from_slice(PROFILE_ZSTD_MAGIC);
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn decode_profile(bytes: &[u8]) -> Result<StoredUserProfile> {
    if let Some(payload) = bytes.strip_prefix(PROFILE_ZSTD_MAGIC) {
        let decompressed = zstd::stream::decode_all(payload)
            .map_err(|e| StorageError::Corrupted(format!("corrupted compressed profile: {e}")))?;
        serde_json::from_slice(&decompressed)
            .map_err(|e| StorageError::Corrupted(format!("corrupted user profile json: {e}")))
    } else {
        serde_json::from_slice(bytes)
            .map_err(|e| StorageError::Corrupted(format!("corrupted user profile json: {e}")))
    }
}

#[inline]
fn record_key(distinct_id: &str) -> Vec<u8> {
    format!("r/{distinct_id}").into_bytes()
}

#[inline]
fn canonical_key(canonical_uid: u32, distinct_id: &str) -> Vec<u8> {
    format!("c/{canonical_uid:08x}/{distinct_id}").into_bytes()
}

#[inline]
fn canonical_prefix_bounds(canonical_uid: u32) -> (Vec<u8>, Vec<u8>) {
    let start = format!("c/{canonical_uid:08x}/").into_bytes();
    let end = format!("c/{canonical_uid:08x}0").into_bytes(); // '0' (0x30) > '/' (0x2f)
    (start, end)
}

#[inline]
fn rev_ts_key(updated_at_ms: i64, distinct_id: &str) -> Vec<u8> {
    let ts = updated_at_ms.max(0) as u64;
    let inv = u64::MAX - ts;
    format!("t/{inv:016x}/{distinct_id}").into_bytes()
}

#[inline]
fn fwd_ts_key(updated_at_ms: i64, distinct_id: &str) -> Vec<u8> {
    let ts = updated_at_ms.max(0) as u64;
    format!("u/{ts:016x}/{distinct_id}").into_bytes()
}

fn slatedb_err(e: SlateDBError) -> StorageError {
    StorageError::Corrupted(format!("profile slatedb error: {e}"))
}

/// Dedicated 2-thread Tokio runtime for `ProjectProfileStore` SlateDB scans.
///
/// Isolates `slatedb 0.6`'s `!Send` `scan()` future (internal `ouroboros` iterators) so callers
/// get a `Send + 'static` oneshot future that works identically on both `multi_thread` and
/// `current_thread` Tokio runtimes without ever deadlocking `Handle::block_on`.
fn profile_io_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("pulse-profile-io")
            .enable_all()
            .build()
            .expect("failed to build pulse-profile-io runtime")
    })
}

async fn scan_db_keys(
    db: Arc<Db>,
    start: Vec<u8>,
    end: Vec<u8>,
    max_keys: Option<usize>,
) -> Result<Vec<Vec<u8>>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    profile_io_runtime().spawn_blocking(move || {
        let res = profile_io_runtime().block_on(async move {
            let mut iter = db.scan(start..end).await.map_err(slatedb_err)?;
            let mut keys = Vec::new();
            while let Some(kv) = iter.next().await.map_err(slatedb_err)? {
                keys.push(kv.key.to_vec());
                if max_keys.is_some_and(|m| keys.len() >= m) {
                    break;
                }
            }
            Ok(keys)
        });
        let _ = tx.send(res);
    });
    rx.await
        .map_err(|_| StorageError::Corrupted("profile scan worker dropped".into()))?
}

async fn scan_reader_keys(
    reader: Arc<DbReader>,
    start: Vec<u8>,
    end: Vec<u8>,
    max_keys: Option<usize>,
) -> Result<Vec<Vec<u8>>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    profile_io_runtime().spawn_blocking(move || {
        let res = profile_io_runtime().block_on(async move {
            let mut iter = reader.scan(start..end).await.map_err(slatedb_err)?;
            let mut keys = Vec::new();
            while let Some(kv) = iter.next().await.map_err(slatedb_err)? {
                keys.push(kv.key.to_vec());
                if max_keys.is_some_and(|m| keys.len() >= m) {
                    break;
                }
            }
            Ok(keys)
        });
        let _ = tx.send(res);
    });
    rx.await
        .map_err(|_| StorageError::Corrupted("profile reader scan worker dropped".into()))?
}

async fn scan_db_records(db: Arc<Db>) -> Result<Vec<StoredUserProfile>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    profile_io_runtime().spawn_blocking(move || {
        let res = profile_io_runtime().block_on(async move {
            let mut iter = db
                .scan(b"r/".to_vec()..b"r0".to_vec())
                .await
                .map_err(slatedb_err)?;
            let mut profiles = Vec::new();
            while let Some(kv) = iter.next().await.map_err(slatedb_err)? {
                profiles.push(decode_profile(&kv.value)?);
            }
            Ok(profiles)
        });
        let _ = tx.send(res);
    });
    rx.await
        .map_err(|_| StorageError::Corrupted("profile export scan worker dropped".into()))?
}

fn encode_profile_snapshot(profiles: &[StoredUserProfile]) -> Result<Vec<u8>> {
    let raw = serde_json::to_vec(profiles).map_err(|e| {
        StorageError::Corrupted(format!("failed to serialize profile snapshot: {e}"))
    })?;
    let compressed = zstd::bulk::compress(&raw, 3).map_err(|e| {
        StorageError::Corrupted(format!("failed to compress profile snapshot: {e}"))
    })?;
    let mut out = Vec::with_capacity(PROFILE_ZSTD_MAGIC.len() + compressed.len());
    out.extend_from_slice(PROFILE_ZSTD_MAGIC);
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn decode_profile_snapshot(bytes: &[u8]) -> Result<Vec<StoredUserProfile>> {
    if let Some(payload) = bytes.strip_prefix(PROFILE_ZSTD_MAGIC) {
        let decompressed = zstd::stream::decode_all(payload).map_err(|e| {
            StorageError::Corrupted(format!("corrupted compressed profile snapshot: {e}"))
        })?;
        serde_json::from_slice(&decompressed)
            .map_err(|e| StorageError::Corrupted(format!("corrupted profile snapshot json: {e}")))
    } else {
        serde_json::from_slice(bytes)
            .map_err(|e| StorageError::Corrupted(format!("corrupted profile snapshot json: {e}")))
    }
}

async fn open_profile_db(db_prefix: &str, object_store: &Arc<dyn ObjectStore>) -> Result<Db> {
    let db_path = ObjectPath::from(db_prefix);
    for attempt in 0..8 {
        let gc_dir = GarbageCollectorDirectoryOptions {
            execution_mode: GcExecutionMode::Periodic(Duration::from_secs(60)),
            min_age: Duration::from_secs(300),
        };
        let opts = DbOptions {
            flush_interval: Some(Duration::from_millis(25)),
            compactor_options: Some(CompactorOptions {
                poll_interval: Duration::from_secs(1),
                ..CompactorOptions::default()
            }),
            garbage_collector_options: Some(GarbageCollectorOptions {
                manifest_options: Some(gc_dir),
                wal_options: Some(gc_dir),
                compacted_options: Some(gc_dir),
                gc_runtime: None,
            }),
            ..DbOptions::default()
        };
        match Db::open_with_opts(db_path.clone(), opts, object_store.clone()).await {
            Ok(opened) => return Ok(opened),
            Err(SlateDBError::ManifestVersionExists) | Err(SlateDBError::Fenced) if attempt < 7 => {
                tokio::time::sleep(Duration::from_millis(25 * (attempt + 1))).await;
            }
            Err(e) => return Err(slatedb_err(e)),
        }
    }
    Err(StorageError::Corrupted("profile slatedb fenced".into()))
}

async fn open_profile_reader(
    db_prefix: &str,
    object_store: &Arc<dyn ObjectStore>,
) -> Result<Option<DbReader>> {
    let db_path = ObjectPath::from(db_prefix);
    for attempt in 0..2 {
        match DbReader::open(
            db_path.clone(),
            object_store.clone(),
            None,
            DbReaderOptions::default(),
        )
        .await
        {
            Ok(r) => return Ok(Some(r)),
            Err(
                SlateDBError::LatestManifestMissing
                | SlateDBError::ManifestMissing(_)
                | SlateDBError::EmptyManifest
                | SlateDBError::InvalidDBState,
            ) => return Ok(None),
            Err(SlateDBError::ManifestVersionExists) if attempt == 0 => {
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            Err(e) => return Err(slatedb_err(e)),
        }
    }
    Ok(None)
}

/// Per-project durable User Profile Store backed by SlateDB on `ObjectStore`.
pub struct ProjectProfileStore {
    project_id: String,
    db_prefix: String,
    object_store: Arc<dyn ObjectStore>,
    read_only: bool,
    dirty_seq: AtomicU64,
    db: tokio::sync::Mutex<Option<Arc<Db>>>,
    /// Read-only stores keep one reader: it follows the writer by polling, and opening one per
    /// read wrote (then deleted) a SlateDB checkpoint each time, two PUTs per read.
    reader: tokio::sync::Mutex<Option<Arc<DbReader>>>,
}

// A SlateDB reader's poller panics if the reader is dropped without `close`, so close it on drop.
impl Drop for ProjectProfileStore {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.get_mut().take() {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    let _ = reader.close().await;
                });
            }
        }
    }
}

impl ProjectProfileStore {
    pub async fn open(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<Self>> {
        let project_id = project_id.into();
        let db_prefix = format!("profiles/{project_id}");
        let db = open_profile_db(&db_prefix, &object_store).await?;
        Ok(Arc::new(Self {
            project_id,
            db_prefix,
            object_store,
            read_only: false,
            dirty_seq: AtomicU64::new(0),
            db: tokio::sync::Mutex::new(Some(Arc::new(db))),
            reader: tokio::sync::Mutex::new(None),
        }))
    }

    pub fn open_read_only(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<Self> {
        let project_id = project_id.into();
        let db_prefix = format!("profiles/{project_id}");
        Self::open_read_only_at(project_id, db_prefix, object_store)
    }

    pub fn open_read_only_at(
        project_id: impl Into<String>,
        db_prefix: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            project_id: project_id.into(),
            db_prefix: db_prefix.into(),
            object_store,
            read_only: true,
            dirty_seq: AtomicU64::new(0),
            db: tokio::sync::Mutex::new(None),
            reader: tokio::sync::Mutex::new(None),
        })
    }

    pub fn db_prefix(&self) -> &str {
        &self.db_prefix
    }

    /// Monotonic counter incremented after every flushed write or delete (used to skip idle snapshots).
    pub fn dirty_seq(&self) -> u64 {
        self.dirty_seq.load(Ordering::Acquire)
    }

    /// This read-only store's reader, opened on first use. `None` while the store does not exist.
    async fn shared_reader(&self) -> Result<Option<Arc<DbReader>>> {
        let mut guard = self.reader.lock().await;
        if guard.is_none() {
            *guard = open_profile_reader(&self.db_prefix, &self.object_store)
                .await?
                .map(Arc::new);
        }
        Ok(guard.clone())
    }

    /// Upserts a single user profile atomically, maintaining all secondary indexes.
    pub async fn upsert(&self, prof: &StoredUserProfile) -> Result<()> {
        self.upsert_batch(std::slice::from_ref(prof)).await
    }

    /// Upserts a batch of user profiles in one atomic SlateDB `WriteBatch`.
    pub async fn upsert_batch(&self, profs: &[StoredUserProfile]) -> Result<()> {
        if self.read_only || profs.is_empty() {
            return Ok(());
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?;

        let mut batch = WriteBatch::new();
        // Track prior state per distinct_id inside the batch so repeated updates to the same
        // distinct_id in one batch clean up intermediate index keys cleanly.
        let mut seen_prev: std::collections::HashMap<String, (u32, i64)> =
            std::collections::HashMap::new();

        for prof in profs {
            let rk = record_key(&prof.distinct_id);
            if let Some(&(old_uid, old_ts)) = seen_prev.get(prof.distinct_id.as_str()) {
                if old_uid != prof.canonical_uid {
                    batch.delete(canonical_key(old_uid, &prof.distinct_id));
                }
                if old_ts != prof.updated_at_ms {
                    batch.delete(rev_ts_key(old_ts, &prof.distinct_id));
                    batch.delete(fwd_ts_key(old_ts, &prof.distinct_id));
                }
            } else if let Some(prev_bytes) = db.get(&rk).await.map_err(slatedb_err)? {
                if let Ok(prev) = decode_profile(&prev_bytes) {
                    if prev.canonical_uid != prof.canonical_uid {
                        batch.delete(canonical_key(prev.canonical_uid, &prof.distinct_id));
                    }
                    if prev.updated_at_ms != prof.updated_at_ms {
                        batch.delete(rev_ts_key(prev.updated_at_ms, &prof.distinct_id));
                        batch.delete(fwd_ts_key(prev.updated_at_ms, &prof.distinct_id));
                    }
                }
            }

            let encoded = encode_profile(prof)?;
            batch.put(&rk, &encoded);
            batch.put(canonical_key(prof.canonical_uid, &prof.distinct_id), b"");
            batch.put(rev_ts_key(prof.updated_at_ms, &prof.distinct_id), b"");
            batch.put(fwd_ts_key(prof.updated_at_ms, &prof.distinct_id), b"");
            seen_prev.insert(
                prof.distinct_id.clone(),
                (prof.canonical_uid, prof.updated_at_ms),
            );
        }

        let write_opts = WriteOptions {
            await_durable: false,
        };
        if let Err(e) = db.write_with_options(batch, &write_opts).await {
            let _ = guard.take();
            return Err(slatedb_err(e));
        }
        if let Err(e) = db.flush().await {
            let _ = guard.take();
            return Err(slatedb_err(e));
        }
        self.dirty_seq.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub async fn get_by_distinct_id(&self, distinct_id: &str) -> Result<Option<StoredUserProfile>> {
        let rk = record_key(distinct_id);
        if self.read_only {
            let Some(reader) = self.shared_reader().await? else {
                return Ok(None);
            };
            let res = match reader.get(&rk).await.map_err(slatedb_err)? {
                Some(b) => Some(decode_profile(&b)?),
                None => None,
            };
            return Ok(res);
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?;
        match db.get(&rk).await.map_err(slatedb_err)? {
            Some(b) => Ok(Some(decode_profile(&b)?)),
            None => Ok(None),
        }
    }

    /// Returns all profile rows belonging to `canonical_uid` using the `c/<uid>/` prefix index.
    pub async fn list_for_canonical(&self, canonical_uid: u32) -> Result<Vec<StoredUserProfile>> {
        let (start, end) = canonical_prefix_bounds(canonical_uid);
        let prefix_len = start.len();
        if self.read_only {
            let Some(reader) = self.shared_reader().await? else {
                return Ok(Vec::new());
            };
            let keys = scan_reader_keys(reader.clone(), start, end, None).await?;
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                if key.len() > prefix_len {
                    if let Ok(did) = std::str::from_utf8(&key[prefix_len..]) {
                        if let Some(b) = reader.get(record_key(did)).await.map_err(slatedb_err)? {
                            out.push(decode_profile(&b)?);
                        }
                    }
                }
            }
            return Ok(out);
        }

        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?
            .clone();
        let keys = scan_db_keys(db.clone(), start, end, None).await?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if key.len() > prefix_len {
                if let Ok(did) = std::str::from_utf8(&key[prefix_len..]) {
                    if let Some(b) = db.get(record_key(did)).await.map_err(slatedb_err)? {
                        out.push(decode_profile(&b)?);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Re-keys existing profile rows for `distinct_ids` to `new_canonical_uid` (after alias merge).
    pub async fn rekey(
        &self,
        distinct_ids: &[&str],
        new_canonical_uid: u32,
        now_ms: i64,
    ) -> Result<()> {
        if self.read_only || distinct_ids.is_empty() {
            return Ok(());
        }
        let mut updated = Vec::new();
        for &did in distinct_ids {
            if let Some(mut prof) = self.get_by_distinct_id(did).await? {
                if prof.canonical_uid != new_canonical_uid {
                    prof.canonical_uid = new_canonical_uid;
                    prof.updated_at_ms = now_ms.max(prof.updated_at_ms);
                    updated.push(prof);
                }
            }
        }
        if !updated.is_empty() {
            self.upsert_batch(&updated).await?;
        }
        Ok(())
    }

    /// Hard-deletes profile rows and all secondary index entries for `distinct_ids` (DPDP erasure).
    pub async fn delete_for_ids(&self, distinct_ids: &[&str]) -> Result<u64> {
        if self.read_only || distinct_ids.is_empty() {
            return Ok(0);
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?;
        let mut batch = WriteBatch::new();
        let mut deleted = 0u64;
        for &did in distinct_ids {
            let rk = record_key(did);
            if let Some(b) = db.get(&rk).await.map_err(slatedb_err)? {
                if let Ok(prev) = decode_profile(&b) {
                    batch.delete(canonical_key(prev.canonical_uid, did));
                    batch.delete(rev_ts_key(prev.updated_at_ms, did));
                    batch.delete(fwd_ts_key(prev.updated_at_ms, did));
                }
                batch.delete(&rk);
                deleted += 1;
            }
        }
        if deleted > 0 {
            let write_opts = WriteOptions {
                await_durable: false,
            };
            if let Err(e) = db.write_with_options(batch, &write_opts).await {
                let _ = guard.take();
                return Err(slatedb_err(e));
            }
            if let Err(e) = db.flush().await {
                let _ = guard.take();
                return Err(slatedb_err(e));
            }
            self.dirty_seq.fetch_add(1, Ordering::AcqRel);
        }
        Ok(deleted)
    }

    /// Newest-first paginated listing using the `t/` reverse-timestamp index, with optional
    /// case-insensitive substring search over `distinct_id` and `props`.
    pub async fn list_paginated(
        &self,
        search: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<StoredUserProfile>> {
        self.list_paginated_filtered(search, limit, offset, &ErasedUids::default(), None)
            .await
    }

    /// Newest-first paginated listing using the `t/` reverse-timestamp index, filtering out
    /// erased users (`updated_at_ms <= erased_at_ms`) *before* applying `offset`/`limit`.
    pub async fn list_paginated_filtered(
        &self,
        search: Option<&str>,
        limit: usize,
        offset: usize,
        erased: &ErasedUids,
        id_graph: Option<&ProjectIdentityGraph>,
    ) -> Result<Vec<StoredUserProfile>> {
        let lim = limit.clamp(1, 1000);
        let needle = search
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase());
        let start = b"t/".to_vec();
        let end = b"t0".to_vec();
        let max_keys = (needle.is_none() && erased.is_empty()).then(|| offset.saturating_add(lim));
        let expanded = expand_erased_with_identity(erased, id_graph);
        let id_snap = (!expanded.is_empty())
            .then(|| id_graph.map(ProjectIdentityGraph::snapshot))
            .flatten();

        if self.read_only {
            let Some(reader) = self.shared_reader().await? else {
                return Ok(Vec::new());
            };
            let keys = scan_reader_keys(reader.clone(), start, end, max_keys).await?;
            let mut out = Vec::with_capacity(lim);
            let mut matched = 0usize;
            for key in keys {
                if key.len() <= 19 {
                    continue;
                }
                let Ok(did) = std::str::from_utf8(&key[19..]) else {
                    continue;
                };
                if needle.is_none() && expanded.is_empty() {
                    if matched < offset {
                        matched += 1;
                        continue;
                    }
                    if let Some(b) = reader.get(record_key(did)).await.map_err(slatedb_err)? {
                        out.push(decode_profile(&b)?);
                        if out.len() >= lim {
                            break;
                        }
                    }
                    matched += 1;
                } else if let Some(b) = reader.get(record_key(did)).await.map_err(slatedb_err)? {
                    let prof = decode_profile(&b)?;
                    if !expanded.is_empty()
                        && is_profile_erased_preexpanded(
                            &prof,
                            &expanded,
                            id_graph,
                            id_snap.as_ref(),
                        )
                    {
                        continue;
                    }
                    if let Some(ref n) = needle {
                        if !profile_matches_needle(&prof, n) {
                            continue;
                        }
                    }
                    if matched >= offset {
                        out.push(prof);
                        if out.len() >= lim {
                            break;
                        }
                    }
                    matched += 1;
                }
            }
            return Ok(out);
        }

        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?
            .clone();
        let keys = scan_db_keys(db.clone(), start, end, max_keys).await?;
        let mut out = Vec::with_capacity(lim);
        let mut matched = 0usize;
        for key in keys {
            if key.len() <= 19 {
                continue;
            }
            let Ok(did) = std::str::from_utf8(&key[19..]) else {
                continue;
            };
            if needle.is_none() && expanded.is_empty() {
                if matched < offset {
                    matched += 1;
                    continue;
                }
                if let Some(b) = db.get(record_key(did)).await.map_err(slatedb_err)? {
                    out.push(decode_profile(&b)?);
                    if out.len() >= lim {
                        break;
                    }
                }
                matched += 1;
            } else if let Some(b) = db.get(record_key(did)).await.map_err(slatedb_err)? {
                let prof = decode_profile(&b)?;
                if !expanded.is_empty()
                    && is_profile_erased_preexpanded(&prof, &expanded, id_graph, id_snap.as_ref())
                {
                    continue;
                }
                if let Some(ref n) = needle {
                    if !profile_matches_needle(&prof, n) {
                        continue;
                    }
                }
                if matched >= offset {
                    out.push(prof);
                    if out.len() >= lim {
                        break;
                    }
                }
                matched += 1;
            }
        }
        Ok(out)
    }

    /// Returns all profiles with `updated_at_ms >= since_ms` using the `u/` forward-timestamp index.
    pub async fn list_updated_since(&self, since_ms: i64) -> Result<Vec<StoredUserProfile>> {
        let since_u64 = since_ms.max(0) as u64;
        let start = format!("u/{since_u64:016x}/").into_bytes();
        let end = b"u0".to_vec();

        if self.read_only {
            let Some(reader) = self.shared_reader().await? else {
                return Ok(Vec::new());
            };
            let keys = scan_reader_keys(reader.clone(), start, end, None).await?;
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                if key.len() > 19 {
                    if let Ok(did) = std::str::from_utf8(&key[19..]) {
                        if let Some(b) = reader.get(record_key(did)).await.map_err(slatedb_err)? {
                            out.push(decode_profile(&b)?);
                        }
                    }
                }
            }
            return Ok(out);
        }

        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?
            .clone();
        let keys = scan_db_keys(db.clone(), start, end, None).await?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if key.len() > 19 {
                if let Ok(did) = std::str::from_utf8(&key[19..]) {
                    if let Some(b) = db.get(record_key(did)).await.map_err(slatedb_err)? {
                        out.push(decode_profile(&b)?);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Returns `(total_profiles, paying_profiles, total_revenue)` over all profiles in this project.
    pub async fn revenue_summary(&self) -> Result<(u64, u64, f64)> {
        self.revenue_summary_filtered(&ErasedUids::default(), None)
            .await
    }

    /// Returns `(total_profiles, paying_profiles, total_revenue)` excluding erased profiles
    /// (`updated_at_ms <= erased_at_ms`).
    pub async fn revenue_summary_filtered(
        &self,
        erased: &ErasedUids,
        id_graph: Option<&ProjectIdentityGraph>,
    ) -> Result<(u64, u64, f64)> {
        let profiles = self.list_updated_since(0).await?;
        let expanded = expand_erased_with_identity(erased, id_graph);
        let id_snap = (!expanded.is_empty())
            .then(|| id_graph.map(ProjectIdentityGraph::snapshot))
            .flatten();
        let mut total_profiles = 0u64;
        let mut paying_profiles = 0u64;
        let mut total_revenue = 0.0f64;
        for p in profiles {
            if !expanded.is_empty()
                && is_profile_erased_preexpanded(&p, &expanded, id_graph, id_snap.as_ref())
            {
                continue;
            }
            total_profiles += 1;
            if p.total_revenue > 0.0 {
                paying_profiles += 1;
                total_revenue += p.total_revenue;
            }
        }
        Ok((total_profiles, paying_profiles, total_revenue))
    }

    pub async fn count(&self) -> Result<u64> {
        let start = b"r/".to_vec();
        let end = b"r0".to_vec();
        if self.read_only {
            let Some(reader) = self.shared_reader().await? else {
                return Ok(0);
            };
            let keys = scan_reader_keys(reader.clone(), start, end, None).await?;
            return Ok(keys.len() as u64);
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?
            .clone();
        let keys = scan_db_keys(db, start, end, None).await?;
        Ok(keys.len() as u64)
    }

    /// Exports all profile records (`r/`..`r0`) as `PZST`-compressed JSON for T1 logical snapshot.
    pub async fn export_snapshot_bytes(&self) -> Result<Vec<u8>> {
        if self.read_only {
            return Err(StorageError::Corrupted(
                "cannot export logical snapshot from read-only profile store".into(),
            ));
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?
            .clone();
        let profiles = scan_db_records(db).await?;
        tokio::task::spawn_blocking(move || encode_profile_snapshot(&profiles))
            .await
            .map_err(|_| {
                StorageError::Corrupted("profile export compression task panicked".into())
            })?
    }

    /// Restores `profiles/{project_id}` from a logical export produced by [`Self::export_snapshot_bytes`],
    /// rebuilding all secondary indexes deterministically and returning the open writable store.
    pub async fn restore_from_export(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        export_bytes: &[u8],
    ) -> Result<Arc<Self>> {
        let project_id = project_id.into();
        let profiles = decode_profile_snapshot(export_bytes)?;

        // Clear any existing profile SlateDB files under `profiles/{project_id}/`.
        let prefix = ObjectPath::from(format!("profiles/{project_id}"));
        let existing: Vec<_> = object_store.list(Some(&prefix)).try_collect().await?;
        for meta in existing {
            object_store.delete(&meta.location).await?;
        }

        let store = Self::open(project_id, object_store).await?;
        for chunk in profiles.chunks(10_000) {
            store.upsert_batch(chunk).await?;
        }
        Ok(store)
    }

    /// Restores profiles into a versioned `db_prefix` (e.g. `profiles/{project_id}/pub_{seq}`),
    /// closes the temporary writable SlateDB handle cleanly, and opens a read-only handle over it.
    pub async fn restore_from_export_read_only_at(
        project_id: impl Into<String>,
        db_prefix: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        export_bytes: &[u8],
    ) -> Result<Arc<Self>> {
        let project_id = project_id.into();
        let db_prefix = db_prefix.into();
        let profiles = decode_profile_snapshot(export_bytes)?;

        let prefix = ObjectPath::from(db_prefix.as_str());
        let existing: Vec<_> = object_store.list(Some(&prefix)).try_collect().await?;
        for meta in existing {
            object_store.delete(&meta.location).await?;
        }

        let db = open_profile_db(&db_prefix, &object_store).await?;
        let writable = Arc::new(Self {
            project_id: project_id.clone(),
            db_prefix: db_prefix.clone(),
            object_store: object_store.clone(),
            read_only: false,
            dirty_seq: AtomicU64::new(0),
            db: tokio::sync::Mutex::new(Some(Arc::new(db))),
            reader: tokio::sync::Mutex::new(None),
        });
        for chunk in profiles.chunks(10_000) {
            writable.upsert_batch(chunk).await?;
        }
        writable.close().await?;

        Ok(Self::open_read_only_at(project_id, db_prefix, object_store))
    }

    /// Creates a SlateDB checkpoint for Candidate A snapshot benchmarking/sync.
    pub async fn create_slatedb_checkpoint(&self) -> Result<slatedb::CheckpointCreateResult> {
        if self.read_only {
            return Err(StorageError::Corrupted(
                "cannot checkpoint read-only profile store".into(),
            ));
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.db_prefix, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?;
        db.create_checkpoint(
            slatedb::config::CheckpointScope::all_with_force_flush(true),
            &slatedb::config::CheckpointOptions {
                lifetime: Some(Duration::from_secs(300)),
                source: None,
            },
        )
        .await
        .map_err(slatedb_err)
    }

    /// Deletes a SlateDB checkpoint created by [`Self::create_slatedb_checkpoint`].
    pub async fn delete_slatedb_checkpoint(
        &self,
        checkpoint: &slatedb::CheckpointCreateResult,
    ) -> Result<()> {
        let db_path = ObjectPath::from(self.db_prefix.as_str());
        Db::delete_checkpoint(&db_path, self.object_store.clone(), checkpoint.id)
            .await
            .map_err(slatedb_err)
    }

    pub async fn close(&self) -> Result<()> {
        if self.read_only {
            // Releases the reader's checkpoint now instead of letting it expire.
            if let Some(reader) = self.reader.lock().await.take() {
                let _ = reader.close().await;
            }
            return Ok(());
        }
        let mut guard = self.db.lock().await;
        if let Some(db) = guard.take() {
            // SlateDB 0.6 panics in `close` when another writer fenced this handle; run it on its
            // own task so that becomes an error instead of taking down shutdown.
            match tokio::spawn(async move { db.close().await }).await {
                Ok(res) => res.map_err(slatedb_err)?,
                Err(e) => {
                    return Err(StorageError::Corrupted(format!(
                        "closing profile store for '{}' panicked (fenced by another writer?): {e}",
                        self.project_id
                    )))
                }
            }
        }
        Ok(())
    }
}

/// Expands an [`ErasedUids`] map through `id_graph` so that if any raw UID merged into a
/// canonical root is erased, that canonical root also carries the maximum erasure timestamp.
pub fn expand_erased_with_identity(
    erased: &ErasedUids,
    id_graph: Option<&ProjectIdentityGraph>,
) -> ErasedUids {
    if erased.is_empty() {
        return ErasedUids::default();
    }
    let Some(ig) = id_graph else {
        return erased.clone();
    };
    let snap = ig.snapshot();
    let mut expanded = erased.clone();
    for (uid, at_ms) in erased.iter() {
        let canon = snap.resolve(uid);
        expanded.insert(canon, at_ms);
    }
    expanded
}

/// Fast-path profile erasure check when `expanded` has already been computed by
/// [`expand_erased_with_identity`].
fn is_profile_erased_preexpanded(
    prof: &StoredUserProfile,
    expanded: &ErasedUids,
    id_graph: Option<&ProjectIdentityGraph>,
    id_snap: Option<&IdentitySnapshot>,
) -> bool {
    if expanded.is_empty() {
        return false;
    }
    if expanded.hides(prof.canonical_uid, prof.updated_at_ms) {
        return true;
    }
    if let Some(snap) = id_snap {
        let resolved_prof = snap.resolve(prof.canonical_uid);
        if resolved_prof != prof.canonical_uid && expanded.hides(resolved_prof, prof.updated_at_ms)
        {
            return true;
        }
    }
    if let Some(ig) = id_graph {
        if let Some(raw) = ig.lookup_raw(&prof.distinct_id) {
            if expanded.hides(raw, prof.updated_at_ms) {
                return true;
            }
        }
        if let Some(canon) = ig.lookup_canonical(&prof.distinct_id) {
            if expanded.hides(canon, prof.updated_at_ms) {
                return true;
            }
        }
    }
    false
}

/// Returns `true` when `prof` belongs to an erased user with `prof.updated_at_ms <= erased_at_ms`.
///
/// Checks `prof.canonical_uid`, `ig.lookup_raw(&prof.distinct_id)`,
/// `ig.lookup_canonical(&prof.distinct_id)`, and any erased raw UID whose canonical root in
/// `id_graph` matches the profile's user (so pre-merge and post-merge rows are all covered,
/// including UID `0`).
pub fn is_profile_erased(
    prof: &StoredUserProfile,
    erased: &ErasedUids,
    id_graph: Option<&ProjectIdentityGraph>,
) -> bool {
    if erased.is_empty() {
        return false;
    }
    if erased.hides(prof.canonical_uid, prof.updated_at_ms) {
        return true;
    }
    if let Some(ig) = id_graph {
        if let Some(raw) = ig.lookup_raw(&prof.distinct_id) {
            if erased.hides(raw, prof.updated_at_ms) {
                return true;
            }
        }
        let snap = ig.snapshot();
        let canon_from_prof = snap.resolve(prof.canonical_uid);
        if canon_from_prof != prof.canonical_uid
            && erased.hides(canon_from_prof, prof.updated_at_ms)
        {
            return true;
        }
        let canon_from_did = ig.lookup_canonical(&prof.distinct_id);
        if let Some(canon) = canon_from_did {
            if erased.hides(canon, prof.updated_at_ms) {
                return true;
            }
        }
        for (uid, at_ms) in erased.iter() {
            if prof.updated_at_ms <= at_ms {
                let resolved = snap.resolve(uid);
                if resolved == prof.canonical_uid
                    || resolved == canon_from_prof
                    || canon_from_did == Some(resolved)
                {
                    return true;
                }
            }
        }
    }
    false
}

fn profile_matches_needle(prof: &StoredUserProfile, lower_needle: &str) -> bool {
    if prof.distinct_id.to_lowercase().contains(lower_needle) {
        return true;
    }
    if let Ok(json_str) = serde_json::to_string(&prof.props) {
        if json_str.to_lowercase().contains(lower_needle) {
            return true;
        }
    }
    false
}

/// Multi-tenant registry managing per-project SlateDB [`ProjectProfileStore`] instances.
pub struct ProfileRouter {
    object_store: Arc<dyn ObjectStore>,
    read_only: bool,
    stores: DashMap<String, Arc<ProjectProfileStore>>,
    /// Read-only stores for projects this node does not write; each follows the writer itself.
    readers: DashMap<String, Arc<ProjectProfileStore>>,
    init_lock: tokio::sync::Mutex<()>,
}

impl ProfileRouter {
    pub fn new(object_store: Arc<dyn ObjectStore>, read_only: bool) -> Self {
        Self {
            object_store,
            read_only,
            stores: DashMap::new(),
            readers: DashMap::new(),
            init_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns the project's profile store only if it has already been opened in this process.
    pub fn get_if_open(&self, project_id: &str) -> Option<Arc<ProjectProfileStore>> {
        self.stores.get(project_id).map(|s| s.clone())
    }

    /// A read-only store for a project this node does not write, cached. Never fences the writer.
    pub fn open_reader(&self, project_id: &str) -> Arc<ProjectProfileStore> {
        self.readers
            .entry(project_id.to_string())
            .or_insert_with(|| {
                ProjectProfileStore::open_read_only(
                    project_id.to_string(),
                    self.object_store.clone(),
                )
            })
            .clone()
    }

    /// Drops the cached reader, e.g. once this node opens the project for writing. Its SlateDB
    /// reader is closed when the last handle to it goes.
    pub fn evict_reader(&self, project_id: &str) {
        self.readers.remove(project_id);
    }

    pub async fn get_or_open(&self, project_id: &str) -> Result<Arc<ProjectProfileStore>> {
        if let Some(s) = self.stores.get(project_id) {
            return Ok(s.clone());
        }
        let _guard = self.init_lock.lock().await;
        if let Some(s) = self.stores.get(project_id) {
            return Ok(s.clone());
        }
        let s = if self.read_only {
            ProjectProfileStore::open_read_only(project_id.to_string(), self.object_store.clone())
        } else {
            ProjectProfileStore::open(project_id.to_string(), self.object_store.clone()).await?
        };
        self.stores.insert(project_id.to_string(), s.clone());
        Ok(s)
    }

    /// Atomically replaces the cached read-only profile store for `project_id` (used by remote
    /// `NodeRole::Query` nodes after restoring a newer published snapshot locally), returning the
    /// previous store if one was cached.
    pub fn replace_read_only(
        &self,
        project_id: &str,
        store: Arc<ProjectProfileStore>,
    ) -> Option<Arc<ProjectProfileStore>> {
        let prev = self.stores.insert(project_id.to_string(), store);
        self.readers.remove(project_id);
        prev
    }

    /// Forgets a project's store so the next `get_or_open` reopens it from storage.
    pub fn evict(&self, project_id: &str) {
        self.stores.remove(project_id);
        self.evict_reader(project_id);
    }

    pub async fn close_all(&self) -> Result<()> {
        let stores: Vec<Arc<ProjectProfileStore>> =
            self.stores.iter().map(|e| e.value().clone()).collect();
        // Close every store even if one fails, and report the first failure.
        let mut first_err = None;
        let readers: Vec<Arc<ProjectProfileStore>> =
            self.readers.iter().map(|e| e.value().clone()).collect();
        for s in stores.into_iter().chain(readers) {
            if let Err(e) = s.close().await {
                first_err.get_or_insert(e);
            }
        }
        self.stores.clear();
        self.readers.clear();
        first_err.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use object_store::local::LocalFileSystem;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn profile_store_crud_pagination_rekey_delete_and_ro_reader() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let ps = ProjectProfileStore::open("proj_p", store.clone())
            .await
            .unwrap();
        let mut p1_props = BTreeMap::new();
        p1_props.insert("plan".into(), json!("Pro"));
        p1_props.insert("city".into(), json!("Bengaluru"));
        ps.upsert(&StoredUserProfile {
            project_id: "proj_p".into(),
            distinct_id: "u_1".into(),
            canonical_uid: 10,
            props: p1_props,
            total_revenue: 499.0,
            updated_at_ms: 1000,
        })
        .await
        .unwrap();

        let mut p2_props = BTreeMap::new();
        p2_props.insert("plan".into(), json!("Free"));
        ps.upsert(&StoredUserProfile {
            project_id: "proj_p".into(),
            distinct_id: "anon_1".into(),
            canonical_uid: 11,
            props: p2_props,
            total_revenue: 0.0,
            updated_at_ms: 2000,
        })
        .await
        .unwrap();

        // Newest-first pagination
        let page = ps.list_paginated(None, 10, 0).await.unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].distinct_id, "anon_1");
        assert_eq!(page[1].distinct_id, "u_1");

        // Search filter
        let found = ps.list_paginated(Some("bengaluru"), 10, 0).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].distinct_id, "u_1");

        // Rekey alias merge
        ps.rekey(&["anon_1", "u_1"], 10, 3000).await.unwrap();
        let canon_rows = ps.list_for_canonical(10).await.unwrap();
        assert_eq!(canon_rows.len(), 2);

        // Incremental updated_since
        let inc = ps.list_updated_since(2500).await.unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].distinct_id, "anon_1");

        // Read-only DbReader sees data without fencing writer
        let ro = ProjectProfileStore::open_read_only("proj_p", store.clone());
        let ro_got = ro.get_by_distinct_id("u_1").await.unwrap().unwrap();
        assert_eq!(ro_got.total_revenue, 499.0);
        assert_eq!(ro.list_for_canonical(10).await.unwrap().len(), 2);

        // DPDP delete
        let deleted = ps.delete_for_ids(&["u_1", "anon_1"]).await.unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(ps.count().await.unwrap(), 0);
        assert!(ps.list_for_canonical(10).await.unwrap().is_empty());

        ps.close().await.unwrap();
    }

    #[tokio::test]
    async fn dirty_seq_and_logical_export_restore_rebuild_secondary_indexes() {
        let dir1 = tempfile::tempdir().unwrap();
        let store1: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir1.path()).unwrap());
        let ps = ProjectProfileStore::open("proj_p2", store1).await.unwrap();
        assert_eq!(ps.dirty_seq(), 0);

        ps.upsert(&StoredUserProfile {
            project_id: "proj_p2".into(),
            distinct_id: "u_a".into(),
            canonical_uid: 5,
            props: BTreeMap::from([("tier".into(), json!("gold"))]),
            total_revenue: 100.0,
            updated_at_ms: 1000,
        })
        .await
        .unwrap();
        assert_eq!(ps.dirty_seq(), 1);

        ps.upsert(&StoredUserProfile {
            project_id: "proj_p2".into(),
            distinct_id: "u_b".into(),
            canonical_uid: 5,
            props: BTreeMap::from([("tier".into(), json!("silver"))]),
            total_revenue: 50.0,
            updated_at_ms: 2000,
        })
        .await
        .unwrap();
        assert_eq!(ps.dirty_seq(), 2);

        let exported = ps.export_snapshot_bytes().await.unwrap();
        assert!(exported.starts_with(PROFILE_ZSTD_MAGIC));
        ps.close().await.unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        let store2: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir2.path()).unwrap());
        let restored = ProjectProfileStore::restore_from_export("proj_p2", store2, &exported)
            .await
            .unwrap();
        assert_eq!(restored.count().await.unwrap(), 2);
        assert_eq!(restored.list_for_canonical(5).await.unwrap().len(), 2);
        let page = restored.list_paginated(None, 10, 0).await.unwrap();
        assert_eq!(page[0].distinct_id, "u_b");
        assert_eq!(page[1].distinct_id, "u_a");
        assert_eq!(restored.list_updated_since(1500).await.unwrap().len(), 1);
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn erased_profile_helpers_filter_uid0_aliases_pagination_search_and_resignups() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let ig = ProjectIdentityGraph::open("proj_er", store.clone())
            .await
            .unwrap();
        // UID 0 is valid (0-based): anon_0 -> user_0 (canonical_uid 1).
        let raw0 = ig.get_or_intern("anon_0", 0);
        assert_eq!(raw0, 0);
        let raw1 = ig.get_or_intern("user_0", 1);
        assert_eq!(raw1, 1);
        let canon_01 = ig.alias_durable("anon_0", "user_0", 0, 1).await.unwrap();
        // Separate user at UID 2 (kept), UID 3 (erased & re-signed up), UID 4 (kept).
        let uid2 = ig.get_or_intern("user_2", 2);
        let uid3 = ig.get_or_intern("user_3", 3);
        let uid4 = ig.get_or_intern("user_4", 0);

        let ps = ProjectProfileStore::open("proj_er", store.clone())
            .await
            .unwrap();
        ps.upsert(&StoredUserProfile {
            project_id: "proj_er".into(),
            distinct_id: "anon_0".into(),
            canonical_uid: raw0, // pre-merge canonical_uid = 0
            props: BTreeMap::from([("plan".into(), json!("vip"))]),
            total_revenue: 100.0,
            updated_at_ms: 1_000,
        })
        .await
        .unwrap();
        ps.upsert(&StoredUserProfile {
            project_id: "proj_er".into(),
            distinct_id: "user_0".into(),
            canonical_uid: canon_01,
            props: BTreeMap::from([("plan".into(), json!("vip"))]),
            total_revenue: 250.0,
            updated_at_ms: 1_500,
        })
        .await
        .unwrap();
        ps.upsert(&StoredUserProfile {
            project_id: "proj_er".into(),
            distinct_id: "user_2".into(),
            canonical_uid: uid2,
            props: BTreeMap::from([("plan".into(), json!("vip"))]),
            total_revenue: 40.0,
            updated_at_ms: 2_000,
        })
        .await
        .unwrap();
        ps.upsert(&StoredUserProfile {
            project_id: "proj_er".into(),
            distinct_id: "user_3".into(),
            canonical_uid: uid3,
            props: BTreeMap::from([("plan".into(), json!("vip"))]),
            total_revenue: 75.0,
            updated_at_ms: 3_000, // re-signup AFTER erasure at 2_500
        })
        .await
        .unwrap();
        ps.upsert(&StoredUserProfile {
            project_id: "proj_er".into(),
            distinct_id: "user_4".into(),
            canonical_uid: uid4,
            props: BTreeMap::from([("plan".into(), json!("free"))]),
            total_revenue: 0.0,
            updated_at_ms: 4_000,
        })
        .await
        .unwrap();

        // Erase raw UID 0 ONLY (merged into canon_01) at 2_000, and UID 3 at 2_500.
        let mut erased = ErasedUids::default();
        erased.insert(0, 2_000);
        erased.insert(uid3, 2_500);

        // Both anon_0 (uid 0) and user_0 (merged with raw uid 0) must be treated as erased.
        let p_anon = ps.get_by_distinct_id("anon_0").await.unwrap().unwrap();
        let p_user0 = ps.get_by_distinct_id("user_0").await.unwrap().unwrap();
        let p_user2 = ps.get_by_distinct_id("user_2").await.unwrap().unwrap();
        let p_user3 = ps.get_by_distinct_id("user_3").await.unwrap().unwrap();
        assert!(is_profile_erased(&p_anon, &erased, Some(&ig)));
        assert!(is_profile_erased(&p_user0, &erased, Some(&ig)));
        assert!(!is_profile_erased(&p_user2, &erased, Some(&ig)));
        assert!(
            !is_profile_erased(&p_user3, &erased, Some(&ig)),
            "post-erasure re-signup must survive"
        );

        // Pagination without search skips erased rows BEFORE applying offset & limit.
        let page0 = ps
            .list_paginated_filtered(None, 2, 0, &erased, Some(&ig))
            .await
            .unwrap();
        assert_eq!(
            page0
                .iter()
                .map(|p| p.distinct_id.as_str())
                .collect::<Vec<_>>(),
            vec!["user_4", "user_3"]
        );
        let page1 = ps
            .list_paginated_filtered(None, 2, 1, &erased, Some(&ig))
            .await
            .unwrap();
        assert_eq!(
            page1
                .iter()
                .map(|p| p.distinct_id.as_str())
                .collect::<Vec<_>>(),
            vec!["user_3", "user_2"]
        );

        // Search filter also excludes erased rows.
        let search = ps
            .list_paginated_filtered(Some("vip"), 10, 0, &erased, Some(&ig))
            .await
            .unwrap();
        assert_eq!(
            search
                .iter()
                .map(|p| p.distinct_id.as_str())
                .collect::<Vec<_>>(),
            vec!["user_3", "user_2"]
        );

        // Revenue summary excludes erased profiles (350.0) and retains user_2 + user_3 + user_4.
        let (total_p, paying_p, rev) = ps
            .revenue_summary_filtered(&erased, Some(&ig))
            .await
            .unwrap();
        assert_eq!(total_p, 3);
        assert_eq!(paying_p, 2);
        assert!((rev - 115.0).abs() < 1e-6, "rev={rev}");

        // Read-only reader matches writable results.
        let ro = ProjectProfileStore::open_read_only("proj_er", store);
        let ro_page = ro
            .list_paginated_filtered(None, 10, 0, &erased, Some(&ig))
            .await
            .unwrap();
        assert_eq!(ro_page.len(), 3);
        assert_eq!(
            ro.revenue_summary_filtered(&erased, Some(&ig))
                .await
                .unwrap(),
            (3, 2, 115.0)
        );

        ro.close().await.unwrap();
        ps.close().await.unwrap();
        ig.close().await.unwrap();
    }
}
