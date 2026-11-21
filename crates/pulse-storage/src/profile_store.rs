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
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slatedb::config::{
    CompactorOptions, DbOptions, DbReaderOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, GcExecutionMode, WriteOptions,
};
use slatedb::{Db, DbReader, SlateDBError, WriteBatch};

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

async fn open_profile_db(project_id: &str, object_store: &Arc<dyn ObjectStore>) -> Result<Db> {
    let db_path = ObjectPath::from(format!("profiles/{project_id}"));
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
    project_id: &str,
    object_store: &Arc<dyn ObjectStore>,
) -> Result<Option<DbReader>> {
    let db_path = ObjectPath::from(format!("profiles/{project_id}"));
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
    object_store: Arc<dyn ObjectStore>,
    read_only: bool,
    db: tokio::sync::Mutex<Option<Arc<Db>>>,
}

impl ProjectProfileStore {
    pub async fn open(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<Self>> {
        let project_id = project_id.into();
        let db = open_profile_db(&project_id, &object_store).await?;
        Ok(Arc::new(Self {
            project_id,
            object_store,
            read_only: false,
            db: tokio::sync::Mutex::new(Some(Arc::new(db))),
        }))
    }

    pub fn open_read_only(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            project_id: project_id.into(),
            object_store,
            read_only: true,
            db: tokio::sync::Mutex::new(None),
        })
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
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
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
        Ok(())
    }

    pub async fn get_by_distinct_id(&self, distinct_id: &str) -> Result<Option<StoredUserProfile>> {
        let rk = record_key(distinct_id);
        if self.read_only {
            let Some(reader) = open_profile_reader(&self.project_id, &self.object_store).await?
            else {
                return Ok(None);
            };
            let res = match reader.get(&rk).await.map_err(slatedb_err)? {
                Some(b) => Some(decode_profile(&b)?),
                None => None,
            };
            let _ = reader.close().await;
            return Ok(res);
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
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
            let Some(reader) = open_profile_reader(&self.project_id, &self.object_store).await?
            else {
                return Ok(Vec::new());
            };
            let reader = Arc::new(reader);
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
            if let Ok(r) = Arc::try_unwrap(reader) {
                let _ = r.close().await;
            }
            return Ok(out);
        }

        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
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
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
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
        let lim = limit.clamp(1, 1000);
        let needle = search
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase());
        let start = b"t/".to_vec();
        let end = b"t0".to_vec();
        let max_keys = needle.is_none().then(|| offset.saturating_add(lim));

        if self.read_only {
            let Some(reader) = open_profile_reader(&self.project_id, &self.object_store).await?
            else {
                return Ok(Vec::new());
            };
            let reader = Arc::new(reader);
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
                if needle.is_none() {
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
                    if profile_matches_needle(&prof, needle.as_deref().unwrap()) {
                        if matched >= offset {
                            out.push(prof);
                            if out.len() >= lim {
                                break;
                            }
                        }
                        matched += 1;
                    }
                }
            }
            if let Ok(r) = Arc::try_unwrap(reader) {
                let _ = r.close().await;
            }
            return Ok(out);
        }

        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
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
            if needle.is_none() {
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
                if profile_matches_needle(&prof, needle.as_deref().unwrap()) {
                    if matched >= offset {
                        out.push(prof);
                        if out.len() >= lim {
                            break;
                        }
                    }
                    matched += 1;
                }
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
            let Some(reader) = open_profile_reader(&self.project_id, &self.object_store).await?
            else {
                return Ok(Vec::new());
            };
            let reader = Arc::new(reader);
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
            if let Ok(r) = Arc::try_unwrap(reader) {
                let _ = r.close().await;
            }
            return Ok(out);
        }

        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
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
        let profiles = self.list_updated_since(0).await?;
        let mut total_profiles = 0u64;
        let mut paying_profiles = 0u64;
        let mut total_revenue = 0.0f64;
        for p in profiles {
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
            let Some(reader) = open_profile_reader(&self.project_id, &self.object_store).await?
            else {
                return Ok(0);
            };
            let reader = Arc::new(reader);
            let keys = scan_reader_keys(reader.clone(), start, end, None).await?;
            if let Ok(r) = Arc::try_unwrap(reader) {
                let _ = r.close().await;
            }
            return Ok(keys.len() as u64);
        }
        let mut guard = self.db.lock().await;
        if guard.is_none() {
            let reopened = open_profile_db(&self.project_id, &self.object_store).await?;
            *guard = Some(Arc::new(reopened));
        }
        let db = guard
            .as_ref()
            .ok_or_else(|| StorageError::Corrupted("profile store closed".into()))?
            .clone();
        let keys = scan_db_keys(db, start, end, None).await?;
        Ok(keys.len() as u64)
    }

    pub async fn close(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let mut guard = self.db.lock().await;
        if let Some(db) = guard.take() {
            db.close().await.map_err(slatedb_err)?;
        }
        Ok(())
    }
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
    init_lock: tokio::sync::Mutex<()>,
}

impl ProfileRouter {
    pub fn new(object_store: Arc<dyn ObjectStore>, read_only: bool) -> Self {
        Self {
            object_store,
            read_only,
            stores: DashMap::new(),
            init_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
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

    /// Forgets a project's store so the next `get_or_open` reopens it from storage.
    pub fn evict(&self, project_id: &str) {
        self.stores.remove(project_id);
    }

    pub async fn close_all(&self) -> Result<()> {
        let stores: Vec<Arc<ProjectProfileStore>> =
            self.stores.iter().map(|e| e.value().clone()).collect();
        for s in stores {
            s.close().await?;
        }
        self.stores.clear();
        Ok(())
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
}
