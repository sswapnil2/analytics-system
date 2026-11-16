use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use slatedb::config::{DbOptions, WriteOptions};
use slatedb::{Db, SlateDBError, WriteBatch};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("slatedb storage error: {0}")]
    SlateDb(#[from] SlateDBError),

    #[error("corrupted identity record in slatedb: {0}")]
    CorruptedRecord(&'static str),

    #[error("identity database already closed")]
    Closed,
}

pub type Result<T> = std::result::Result<T, IdentityError>;

/// 256-bit physical bucket presence mask (`buckets 0..=255`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketMask([u64; 4]);

impl BucketMask {
    #[inline]
    pub fn single(bucket_id: u8) -> Self {
        let mut m = Self([0u64; 4]);
        m.insert(bucket_id);
        m
    }

    #[inline]
    pub fn insert(&mut self, bucket_id: u8) -> bool {
        let word = (bucket_id >> 6) as usize;
        let bit = 1u64 << (bucket_id & 63);
        let was_unset = (self.0[word] & bit) == 0;
        self.0[word] |= bit;
        was_unset
    }

    #[inline]
    pub fn contains(&self, bucket_id: u8) -> bool {
        let word = (bucket_id >> 6) as usize;
        let bit = 1u64 << (bucket_id & 63);
        (self.0[word] & bit) != 0
    }

    #[inline]
    pub fn union_assign(&mut self, other: Self) {
        self.0[0] |= other.0[0];
        self.0[1] |= other.0[1];
        self.0[2] |= other.0[2];
        self.0[3] |= other.0[3];
    }

    #[inline]
    pub fn count_buckets(&self) -> u32 {
        self.0[0].count_ones()
            + self.0[1].count_ones()
            + self.0[2].count_ones()
            + self.0[3].count_ones()
    }

    #[inline]
    pub fn is_cross_bucket(&self) -> bool {
        self.count_buckets() > 1
    }
}

/// Immutable, lock-free query snapshot of canonical `u32 -> u32` mappings and cross-bucket users.
#[derive(Debug, Clone)]
pub struct IdentitySnapshot {
    pub canonical_map: Arc<Vec<u32>>,
    pub cross_bucket_users: Arc<RoaringBitmap>,
}

impl IdentitySnapshot {
    #[inline]
    pub fn resolve(&self, raw_uid: u32) -> u32 {
        self.canonical_map
            .get(raw_uid as usize)
            .copied()
            .unwrap_or(raw_uid)
    }

    #[inline]
    pub fn is_cross_bucket(&self, canonical_uid: u32) -> bool {
        self.cross_bucket_users.contains(canonical_uid)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.canonical_map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.canonical_map.is_empty()
    }
}

#[derive(Debug, Clone)]
enum MutationEntry {
    Intern {
        uid: u32,
        origin_bucket: u8,
        distinct_id: String,
    },
    ExtraBucket {
        uid: u32,
        bucket_id: u8,
    },
    Alias {
        anon_u32: u32,
        user_u32: u32,
        anon_bucket: u8,
        user_bucket: u8,
    },
}

const OP_INTERN: u8 = 1;
const OP_EXTRA_BUCKET: u8 = 2;
const OP_ALIAS: u8 = 3;

fn encode_mutation_chunk(entries: &[MutationEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + entries.len() * 20);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        match entry {
            MutationEntry::Intern {
                uid,
                origin_bucket,
                distinct_id,
            } => {
                buf.push(OP_INTERN);
                buf.extend_from_slice(&uid.to_le_bytes());
                buf.push(*origin_bucket);
                buf.extend_from_slice(&(distinct_id.len() as u32).to_le_bytes());
                buf.extend_from_slice(distinct_id.as_bytes());
            }
            MutationEntry::ExtraBucket { uid, bucket_id } => {
                buf.push(OP_EXTRA_BUCKET);
                buf.extend_from_slice(&uid.to_le_bytes());
                buf.push(*bucket_id);
            }
            MutationEntry::Alias {
                anon_u32,
                user_u32,
                anon_bucket,
                user_bucket,
            } => {
                buf.push(OP_ALIAS);
                buf.extend_from_slice(&anon_u32.to_le_bytes());
                buf.extend_from_slice(&user_u32.to_le_bytes());
                buf.push(*anon_bucket);
                buf.push(*user_bucket);
            }
        }
    }
    buf
}

fn decode_mutation_chunk(bytes: &[u8]) -> Result<Vec<MutationEntry>> {
    if bytes.len() < 4 {
        return Err(IdentityError::CorruptedRecord("chunk header < 4 bytes"));
    }
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut pos = 4usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= bytes.len() {
            return Err(IdentityError::CorruptedRecord("chunk op EOF"));
        }
        let op = bytes[pos];
        pos += 1;
        match op {
            OP_INTERN => {
                if pos + 9 > bytes.len() {
                    return Err(IdentityError::CorruptedRecord("intern header EOF"));
                }
                let uid = u32::from_le_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]);
                let origin_bucket = bytes[pos + 4];
                let slen = u32::from_le_bytes([
                    bytes[pos + 5],
                    bytes[pos + 6],
                    bytes[pos + 7],
                    bytes[pos + 8],
                ]) as usize;
                pos += 9;
                if pos + slen > bytes.len() {
                    return Err(IdentityError::CorruptedRecord("intern distinct_id EOF"));
                }
                let distinct_id = String::from_utf8(bytes[pos..pos + slen].to_vec())
                    .map_err(|_| IdentityError::CorruptedRecord("invalid utf8 distinct_id"))?;
                pos += slen;
                out.push(MutationEntry::Intern {
                    uid,
                    origin_bucket,
                    distinct_id,
                });
            }
            OP_EXTRA_BUCKET => {
                if pos + 5 > bytes.len() {
                    return Err(IdentityError::CorruptedRecord("extra_bucket EOF"));
                }
                let uid = u32::from_le_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]);
                let bucket_id = bytes[pos + 4];
                pos += 5;
                out.push(MutationEntry::ExtraBucket { uid, bucket_id });
            }
            OP_ALIAS => {
                if pos + 10 > bytes.len() {
                    return Err(IdentityError::CorruptedRecord("alias EOF"));
                }
                let anon_u32 = u32::from_le_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]);
                let user_u32 = u32::from_le_bytes([
                    bytes[pos + 4],
                    bytes[pos + 5],
                    bytes[pos + 6],
                    bytes[pos + 7],
                ]);
                let anon_bucket = bytes[pos + 8];
                let user_bucket = bytes[pos + 9];
                pos += 10;
                out.push(MutationEntry::Alias {
                    anon_u32,
                    user_u32,
                    anon_bucket,
                    user_bucket,
                });
            }
            _ => return Err(IdentityError::CorruptedRecord("unknown mutation opcode")),
        }
    }
    Ok(out)
}

struct GraphState {
    u32_to_external: Vec<String>,
    parent: Vec<u32>,
    bucket_masks: Vec<BucketMask>,
    cross_bucket_users: RoaringBitmap,
}

impl GraphState {
    fn new() -> Self {
        Self {
            u32_to_external: Vec::new(),
            parent: Vec::new(),
            bucket_masks: Vec::new(),
            cross_bucket_users: RoaringBitmap::new(),
        }
    }

    #[inline]
    fn find_root_mut(&mut self, mut uid: u32) -> u32 {
        while (uid as usize) < self.parent.len() {
            let p = self.parent[uid as usize];
            if p == uid {
                return uid;
            }
            let gp = self.parent[p as usize];
            self.parent[uid as usize] = gp;
            uid = gp;
        }
        uid
    }

    #[inline]
    fn find_root_ref(&self, mut uid: u32) -> u32 {
        while (uid as usize) < self.parent.len() {
            let p = self.parent[uid as usize];
            if p == uid {
                return uid;
            }
            uid = p;
        }
        uid
    }

    fn apply_alias(
        &mut self,
        anon_u32: u32,
        user_u32: u32,
        anon_bucket: u8,
        user_bucket: u8,
    ) -> u32 {
        self.bucket_masks[anon_u32 as usize].insert(anon_bucket);
        self.bucket_masks[user_u32 as usize].insert(user_bucket);

        let r_anon = self.find_root_mut(anon_u32);
        let r_user = self.find_root_mut(user_u32);

        let mut combined_mask = self.bucket_masks[r_user as usize];
        combined_mask.union_assign(self.bucket_masks[anon_u32 as usize]);
        combined_mask.union_assign(self.bucket_masks[user_u32 as usize]);
        combined_mask.union_assign(self.bucket_masks[r_anon as usize]);

        if r_anon != r_user {
            self.parent[r_anon as usize] = r_user;
            self.parent[anon_u32 as usize] = r_user;
            if self.cross_bucket_users.contains(r_anon) {
                self.cross_bucket_users.remove(r_anon);
            }
        }

        self.bucket_masks[r_user as usize] = combined_mask;
        if combined_mask.is_cross_bucket() {
            self.cross_bucket_users.insert(r_user);
        }
        r_user
    }

    fn compress_all_and_build_snapshot(&mut self) -> IdentitySnapshot {
        let len = self.parent.len();
        for i in 0..len {
            let mut root = self.parent[i];
            while self.parent[root as usize] != root {
                root = self.parent[root as usize];
            }
            self.parent[i] = root;
        }
        IdentitySnapshot {
            canonical_map: Arc::new(self.parent.clone()),
            cross_bucket_users: Arc::new(self.cross_bucket_users.clone()),
        }
    }
}

const META_CHUNK_COUNT_KEY: &[u8] = b"meta/chunk_count";

/// Durable per-project `distinct_id <-> u32` dictionary and zero-rewrite Union-Find alias graph.
pub struct ProjectIdentityGraph {
    project_id: String,
    fast_lookup: DashMap<String, (u32, u8)>,
    state: RwLock<GraphState>,
    snapshot_cache: RwLock<Option<IdentitySnapshot>>,
    snapshot_dirty: AtomicBool,
    next_chunk_seq: AtomicU64,
    pending_mutations: Mutex<Vec<MutationEntry>>,
    db: tokio::sync::Mutex<Option<Db>>,
}

impl ProjectIdentityGraph {
    /// Opens or recovers a `ProjectIdentityGraph` from SlateDB at `identity/<project_id>`.
    pub async fn open(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<Self>> {
        let project_id = project_id.into();
        let db_path = ObjectPath::from(format!("identity/{project_id}"));
        let mut db = None;
        for attempt in 0..8 {
            let opts = DbOptions {
                flush_interval: Some(Duration::from_millis(25)),
                compactor_options: None,
                ..DbOptions::default()
            };
            match Db::open_with_opts(db_path.clone(), opts, object_store.clone()).await {
                Ok(opened) => {
                    db = Some(opened);
                    break;
                }
                Err(SlateDBError::ManifestVersionExists) | Err(SlateDBError::Fenced)
                    if attempt < 7 =>
                {
                    tokio::time::sleep(Duration::from_millis(25 * (attempt + 1))).await;
                }
                Err(e) => return Err(IdentityError::SlateDb(e)),
            }
        }
        let db = db.ok_or(IdentityError::SlateDb(SlateDBError::Fenced))?;

        let chunk_count = match db.get(META_CHUNK_COUNT_KEY).await? {
            Some(b) if b.len() >= 8 => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&b[0..8]);
                u64::from_le_bytes(arr)
            }
            _ => 0,
        };

        let fast_lookup: DashMap<String, (u32, u8)> = DashMap::new();
        let mut state = GraphState::new();

        for seq in 0..chunk_count {
            let chunk_key = format!("log/chunk_{seq:012}");
            let Some(bytes) = db.get(chunk_key.as_bytes()).await? else {
                continue;
            };
            let entries = decode_mutation_chunk(&bytes)?;
            for entry in entries {
                match entry {
                    MutationEntry::Intern {
                        uid,
                        origin_bucket,
                        distinct_id,
                    } => {
                        if state.u32_to_external.len() <= uid as usize {
                            state
                                .u32_to_external
                                .resize(uid as usize + 1, String::new());
                            let old_len = state.parent.len();
                            state.parent.resize(uid as usize + 1, 0);
                            for idx in old_len..=uid as usize {
                                state.parent[idx] = idx as u32;
                            }
                            state
                                .bucket_masks
                                .resize(uid as usize + 1, BucketMask::default());
                        }
                        state.u32_to_external[uid as usize] = distinct_id.clone();
                        state.bucket_masks[uid as usize].insert(origin_bucket);
                        if state.bucket_masks[uid as usize].is_cross_bucket() {
                            state.cross_bucket_users.insert(uid);
                        }
                        fast_lookup.insert(distinct_id, (uid, origin_bucket));
                    }
                    MutationEntry::ExtraBucket { uid, bucket_id } => {
                        if (uid as usize) < state.bucket_masks.len() {
                            state.bucket_masks[uid as usize].insert(bucket_id);
                            let root = state.find_root_mut(uid);
                            state.bucket_masks[root as usize].insert(bucket_id);
                            if state.bucket_masks[root as usize].is_cross_bucket() {
                                state.cross_bucket_users.insert(root);
                            }
                        }
                    }
                    MutationEntry::Alias {
                        anon_u32,
                        user_u32,
                        anon_bucket,
                        user_bucket,
                    } => {
                        if (anon_u32 as usize) < state.parent.len()
                            && (user_u32 as usize) < state.parent.len()
                        {
                            state.apply_alias(anon_u32, user_u32, anon_bucket, user_bucket);
                        }
                    }
                }
            }
        }

        let initial_snap = state.compress_all_and_build_snapshot();

        Ok(Arc::new(Self {
            project_id,
            fast_lookup,
            state: RwLock::new(state),
            snapshot_cache: RwLock::new(Some(initial_snap)),
            snapshot_dirty: AtomicBool::new(false),
            next_chunk_seq: AtomicU64::new(chunk_count),
            pending_mutations: Mutex::new(Vec::new()),
            db: tokio::sync::Mutex::new(Some(db)),
        }))
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn user_count(&self) -> usize {
        self.state.read().u32_to_external.len()
    }

    /// Lock-free hot path lookup + monotonic `u32` allocation on miss.
    pub fn get_or_intern(&self, distinct_id: &str, bucket_id: u8) -> u32 {
        if let Some(entry) = self.fast_lookup.get(distinct_id) {
            let (uid, origin_bucket) = *entry;
            drop(entry);
            if origin_bucket != bucket_id {
                self.record_extra_bucket(uid, bucket_id);
            }
            return uid;
        }

        let mut state = self.state.write();
        if let Some(entry) = self.fast_lookup.get(distinct_id) {
            let (uid, origin_bucket) = *entry;
            drop(entry);
            drop(state);
            if origin_bucket != bucket_id {
                self.record_extra_bucket(uid, bucket_id);
            }
            return uid;
        }

        let uid = state.u32_to_external.len() as u32;
        let mask = BucketMask::single(bucket_id);
        state.u32_to_external.push(distinct_id.to_string());
        state.parent.push(uid);
        state.bucket_masks.push(mask);
        self.fast_lookup
            .insert(distinct_id.to_string(), (uid, bucket_id));

        self.snapshot_dirty.store(true, Ordering::Release);
        drop(state);

        self.pending_mutations.lock().push(MutationEntry::Intern {
            uid,
            origin_bucket: bucket_id,
            distinct_id: distinct_id.to_string(),
        });
        uid
    }

    fn record_extra_bucket(&self, uid: u32, bucket_id: u8) {
        {
            let state = self.state.read();
            let root = state.find_root_ref(uid);
            if state.bucket_masks[uid as usize].contains(bucket_id)
                && state.bucket_masks[root as usize].contains(bucket_id)
            {
                return;
            }
        }
        let mut state = self.state.write();
        let added_raw = state.bucket_masks[uid as usize].insert(bucket_id);
        let root = state.find_root_mut(uid);
        let added_root = state.bucket_masks[root as usize].insert(bucket_id);
        if state.bucket_masks[root as usize].is_cross_bucket() {
            state.cross_bucket_users.insert(root);
        }
        if added_raw || added_root {
            self.snapshot_dirty.store(true, Ordering::Release);
            drop(state);
            self.pending_mutations
                .lock()
                .push(MutationEntry::ExtraBucket { uid, bucket_id });
        }
    }

    /// Retroactively merges `anon_id -> user_id` without rewriting historical segments.
    /// Returns the canonical `u32` root (`root(user_u32)`).
    pub fn alias(&self, anon_id: &str, user_id: &str, anon_bucket: u8, user_bucket: u8) -> u32 {
        let anon_u32 = self.get_or_intern(anon_id, anon_bucket);
        let user_u32 = self.get_or_intern(user_id, user_bucket);
        if anon_u32 == user_u32 {
            return user_u32;
        }

        let canonical_root = {
            let mut state = self.state.write();
            let r = state.apply_alias(anon_u32, user_u32, anon_bucket, user_bucket);
            self.snapshot_dirty.store(true, Ordering::Release);
            r
        };

        self.pending_mutations.lock().push(MutationEntry::Alias {
            anon_u32,
            user_u32,
            anon_bucket,
            user_bucket,
        });
        canonical_root
    }

    /// Performs `alias(...)` and immediately flushes the mutation batch to SlateDB.
    pub async fn alias_durable(
        &self,
        anon_id: &str,
        user_id: &str,
        anon_bucket: u8,
        user_bucket: u8,
    ) -> Result<u32> {
        let root = self.alias(anon_id, user_id, anon_bucket, user_bucket);
        self.flush().await?;
        Ok(root)
    }

    pub fn lookup_raw(&self, distinct_id: &str) -> Option<u32> {
        self.fast_lookup.get(distinct_id).map(|e| e.0)
    }

    pub fn lookup_canonical(&self, distinct_id: &str) -> Option<u32> {
        let raw = self.lookup_raw(distinct_id)?;
        let state = self.state.read();
        Some(state.find_root_ref(raw))
    }

    pub fn external_id(&self, uid: u32) -> Option<String> {
        self.state.read().u32_to_external.get(uid as usize).cloned()
    }

    /// Returns an immutable `IdentitySnapshot` in < 5ms for lock-free vectorized query scans.
    pub fn snapshot(&self) -> IdentitySnapshot {
        if !self.snapshot_dirty.load(Ordering::Acquire) {
            if let Some(snap) = self.snapshot_cache.read().as_ref() {
                return snap.clone();
            }
        }

        let mut state = self.state.write();
        let snap = state.compress_all_and_build_snapshot();
        *self.snapshot_cache.write() = Some(snap.clone());
        self.snapshot_dirty.store(false, Ordering::Release);
        snap
    }

    /// Flushes all staged intern & alias records durably to SlateDB on `object_store`.
    pub async fn flush(&self) -> Result<()> {
        let entries = {
            let mut guard = self.pending_mutations.lock();
            if guard.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *guard)
        };

        let db_guard = self.db.lock().await;
        let db = db_guard.as_ref().ok_or(IdentityError::Closed)?;

        const CHUNK_SIZE: usize = 32_768;
        let write_opts = WriteOptions {
            await_durable: false,
        };
        let mut batch = WriteBatch::new();

        for slice in entries.chunks(CHUNK_SIZE) {
            let seq = self.next_chunk_seq.fetch_add(1, Ordering::AcqRel);
            let chunk_key = format!("log/chunk_{seq:012}");
            let chunk_bytes = encode_mutation_chunk(slice);
            batch.put(chunk_key.as_bytes(), &chunk_bytes);

            for item in slice {
                if let MutationEntry::Intern {
                    uid, distinct_id, ..
                } = item
                {
                    let mut dkey = Vec::with_capacity(2 + distinct_id.len());
                    dkey.extend_from_slice(b"d/");
                    dkey.extend_from_slice(distinct_id.as_bytes());
                    batch.put(&dkey, uid.to_le_bytes());
                }
            }
        }

        let new_count = self.next_chunk_seq.load(Ordering::Acquire);
        batch.put(META_CHUNK_COUNT_KEY, new_count.to_le_bytes());
        db.write_with_options(batch, &write_opts).await?;
        db.flush().await?;
        Ok(())
    }

    /// Flushes pending mutations and cleanly closes the underlying SlateDB handle.
    pub async fn close(&self) -> Result<()> {
        self.flush().await?;
        let mut db_guard = self.db.lock().await;
        if let Some(db) = db_guard.take() {
            db.close().await?;
        }
        Ok(())
    }
}

/// Multi-tenant registry managing `ProjectIdentityGraph` instances over shared object storage.
pub struct IdentityRouter {
    object_store: Arc<dyn ObjectStore>,
    graphs: DashMap<String, Arc<ProjectIdentityGraph>>,
    init_lock: tokio::sync::Mutex<()>,
}

impl IdentityRouter {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            graphs: DashMap::new(),
            init_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn get_or_open(&self, project_id: &str) -> Result<Arc<ProjectIdentityGraph>> {
        if let Some(g) = self.graphs.get(project_id) {
            return Ok(g.clone());
        }
        let _guard = self.init_lock.lock().await;
        if let Some(g) = self.graphs.get(project_id) {
            return Ok(g.clone());
        }
        let g =
            ProjectIdentityGraph::open(project_id.to_string(), self.object_store.clone()).await?;
        self.graphs.insert(project_id.to_string(), g.clone());
        Ok(g)
    }

    pub async fn flush_all(&self) -> Result<()> {
        let graphs: Vec<Arc<ProjectIdentityGraph>> =
            self.graphs.iter().map(|e| e.value().clone()).collect();
        for g in graphs {
            g.flush().await?;
        }
        Ok(())
    }

    pub async fn close_all(&self) -> Result<()> {
        let graphs: Vec<Arc<ProjectIdentityGraph>> =
            self.graphs.iter().map(|e| e.value().clone()).collect();
        for g in graphs {
            g.close().await?;
        }
        self.graphs.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use object_store::local::LocalFileSystem;

    use super::*;

    #[tokio::test]
    async fn gate1_250k_interns_50k_merges_slatedb_recovery_and_sub_5ms_snapshot() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));

        let pre_restart_snap = {
            let graph = ProjectIdentityGraph::open("proj_gate1", store.clone())
                .await
                .expect("open graph");

            for i in 0..50_000u32 {
                let uid = graph.get_or_intern(&format!("user_{i:06}"), (i % 256) as u8);
                assert_eq!(uid, i);
            }
            for i in 0..50_000u32 {
                let bucket = if i < 40_000 {
                    (i % 256) as u8
                } else {
                    ((i + 1) % 256) as u8
                };
                graph.get_or_intern(&format!("anon_a_{i:06}"), bucket);
            }
            for i in 0..10_000u32 {
                let bucket = ((i + 37) % 256) as u8;
                graph.get_or_intern(&format!("anon_b_{i:06}"), bucket);
            }
            for i in 110_000..250_000u32 {
                graph.get_or_intern(&format!("visitor_{i:06}"), (i % 256) as u8);
            }
            assert_eq!(graph.user_count(), 250_000);

            for i in 0..40_000u32 {
                let user_bucket = (i % 256) as u8;
                let anon_bucket = if i < 30_000 {
                    user_bucket
                } else {
                    ((i + 1) % 256) as u8
                };
                let root = graph.alias(
                    &format!("anon_a_{i:06}"),
                    &format!("user_{i:06}"),
                    anon_bucket,
                    user_bucket,
                );
                assert_eq!(root, i);
            }
            for i in 0..10_000u32 {
                let user_bucket = (i % 256) as u8;
                let anon_b_bucket = ((i + 37) % 256) as u8;
                let root = graph.alias(
                    &format!("anon_b_{i:06}"),
                    &format!("user_{i:06}"),
                    anon_b_bucket,
                    user_bucket,
                );
                assert_eq!(root, i);
            }

            let t0 = Instant::now();
            let snap = graph.snapshot();
            let elapsed = t0.elapsed();
            assert!(
                elapsed < Duration::from_millis(5),
                "expected < 5ms snapshot generation, took {elapsed:?}"
            );

            for i in 0..10_000u32 {
                let raw_a = graph.lookup_raw(&format!("anon_a_{i:06}")).unwrap();
                let raw_b = graph.lookup_raw(&format!("anon_b_{i:06}")).unwrap();
                assert_eq!(snap.resolve(raw_a), i);
                assert_eq!(snap.resolve(raw_b), i);
                assert_eq!(snap.resolve(i), i);
                assert!(snap.is_cross_bucket(i));
            }
            for i in 30_000..40_000u32 {
                assert!(snap.is_cross_bucket(i));
            }
            for i in 10_000..30_000u32 {
                assert!(!snap.is_cross_bucket(i));
            }

            graph.close().await.expect("flush and close slatedb");
            snap
        };

        let recovered = ProjectIdentityGraph::open("proj_gate1", store)
            .await
            .expect("reopen graph from slatedb");
        assert_eq!(recovered.user_count(), 250_000);

        let t_rec = Instant::now();
        let post_restart_snap = recovered.snapshot();
        let rec_snap_dur = t_rec.elapsed();
        assert!(
            rec_snap_dur < Duration::from_millis(5),
            "post-restart snapshot took {rec_snap_dur:?}"
        );

        assert_eq!(
            pre_restart_snap.canonical_map.as_slice(),
            post_restart_snap.canonical_map.as_slice(),
            "canonical u32 mapping mismatch after SlateDB recovery"
        );
        assert_eq!(
            *pre_restart_snap.cross_bucket_users, *post_restart_snap.cross_bucket_users,
            "cross_bucket_users bitmap mismatch after SlateDB recovery"
        );

        recovered.close().await.expect("close recovered");
    }
}
