use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use slatedb::config::{
    CompactorOptions, DbOptions, DbReaderOptions, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, GcExecutionMode, WriteOptions,
};
use slatedb::{Db, DbReader, SlateDBError, WriteBatch};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("slatedb storage error: {0}")]
    SlateDb(#[from] SlateDBError),

    #[error("corrupted identity record in slatedb: {0}")]
    CorruptedRecord(&'static str),

    #[error("cannot alias an already-identified user root into a different user")]
    IdentifiedMergeConflict,

    #[error("identity database already closed")]
    Closed,
}

pub type Result<T> = std::result::Result<T, IdentityError>;

/// 256-bit physical bucket presence mask (`buckets 0..=255`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
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

    #[inline]
    pub fn first_bucket(&self) -> u8 {
        for (w, &bits) in self.0.iter().enumerate() {
            if bits != 0 {
                return (w as u8) * 64 + bits.trailing_zeros() as u8;
            }
        }
        0
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
    let max_prealloc = count.min((bytes.len() - 4) / 6).min(65_536);
    let mut out = Vec::with_capacity(max_prealloc);
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

const CHECKPOINT_ZSTD_MAGIC: &[u8; 4] = b"PZST";
const EXTERNAL_ID_PAGE_SIZE: usize = 16_384;

/// Copy-on-write paged storage for external `distinct_id` strings.
///
/// Cloning the page directory for a checkpoint takes O(users / 16_384) `Arc` clones
/// (~1 microsecond for 2M users) instead of cloning millions of `String` heap allocations
/// while holding the identity state lock.
struct PagedStrings {
    pages: Vec<Arc<Vec<String>>>,
    len: usize,
}

impl PagedStrings {
    fn new() -> Self {
        Self {
            pages: Vec::new(),
            len: 0,
        }
    }

    /// Moves `items` into fixed-size pages in O(n). (`split_off` here used to copy the remaining tail
    /// on every page and keep the full capacity per page: O(n^2) memory, OOM at ~10M users, #67.)
    fn from_vec(items: Vec<String>) -> Self {
        let len = items.len();
        let mut pages = Vec::with_capacity(len.div_ceil(EXTERNAL_ID_PAGE_SIZE));
        let mut iter = items.into_iter();
        loop {
            let mut page = Vec::with_capacity(EXTERNAL_ID_PAGE_SIZE.min(iter.len()));
            page.extend(iter.by_ref().take(EXTERNAL_ID_PAGE_SIZE));
            if page.is_empty() {
                break;
            }
            pages.push(Arc::new(page));
        }
        Self { pages, len }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, s: String) {
        let need_new_page = self
            .pages
            .last()
            .is_none_or(|p| p.len() >= EXTERNAL_ID_PAGE_SIZE);
        if need_new_page {
            self.pages
                .push(Arc::new(Vec::with_capacity(EXTERNAL_ID_PAGE_SIZE)));
        }
        let last = self.pages.last_mut().expect("page pushed above");
        Arc::make_mut(last).push(s);
        self.len += 1;
    }

    fn set(&mut self, idx: usize, s: String) {
        let page_idx = idx / EXTERNAL_ID_PAGE_SIZE;
        let offset = idx % EXTERNAL_ID_PAGE_SIZE;
        Arc::make_mut(&mut self.pages[page_idx])[offset] = s;
    }

    #[inline]
    fn get(&self, idx: usize) -> Option<&str> {
        let page = self.pages.get(idx / EXTERNAL_ID_PAGE_SIZE)?;
        page.get(idx % EXTERNAL_ID_PAGE_SIZE).map(|s| s.as_str())
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        self.pages.iter().flat_map(|p| p.iter().map(|s| s.as_str()))
    }

    fn snapshot_pages(&self) -> Vec<Arc<Vec<String>>> {
        self.pages.clone()
    }
}

/// Compacted SlateDB checkpoint record bounding startup recovery cost.
#[derive(Serialize, Deserialize)]
struct IdentityCheckpoint {
    next_chunk_seq: u64,
    u32_to_external: Vec<String>,
    origin_buckets: Vec<u8>,
    parent: Vec<u32>,
    bucket_masks: Vec<BucketMask>,
    identified_roots: Vec<u32>,
}

/// Wire-compatible borrowed view for zero-String-allocation checkpoint serialization.
#[derive(Serialize)]
struct IdentityCheckpointRef<'a> {
    next_chunk_seq: u64,
    u32_to_external: Vec<&'a str>,
    origin_buckets: &'a [u8],
    parent: &'a [u32],
    bucket_masks: &'a [BucketMask],
    identified_roots: Vec<u32>,
}

/// Detached point-in-time snapshot taken under a brief shared read lock and
/// compressed/serialized off the lock on a blocking thread.
struct CheckpointSnapshot {
    u32_pages: Vec<Arc<Vec<String>>>,
    total_users: usize,
    origin_buckets: Vec<u8>,
    parent: Vec<u32>,
    bucket_masks: Vec<BucketMask>,
    identified_roots: RoaringBitmap,
}

impl CheckpointSnapshot {
    fn encode(mut self, next_chunk_seq: u64) -> Result<Vec<u8>> {
        // Path-compress the detached parent array off the lock.
        let len = self.parent.len();
        for i in 0..len {
            let mut root = self.parent[i];
            while self.parent[root as usize] != root {
                root = self.parent[root as usize];
            }
            self.parent[i] = root;
        }

        let mut u32_to_external = Vec::with_capacity(self.total_users);
        for page in &self.u32_pages {
            for s in page.iter() {
                u32_to_external.push(s.as_str());
            }
        }
        // Truncate in case the last page was shared with a concurrent push (only happens if
        // another thread appended after snapshot; `Arc::make_mut` clones on write, so lengths match).
        u32_to_external.truncate(self.total_users);

        let cp_ref = IdentityCheckpointRef {
            next_chunk_seq,
            u32_to_external,
            origin_buckets: &self.origin_buckets,
            parent: &self.parent,
            bucket_masks: &self.bucket_masks,
            identified_roots: self.identified_roots.iter().collect(),
        };
        let raw = bincode::serialize(&cp_ref)
            .map_err(|_| IdentityError::CorruptedRecord("failed to serialize meta/checkpoint"))?;
        let compressed = zstd::bulk::compress(&raw, 3)
            .map_err(|_| IdentityError::CorruptedRecord("failed to compress meta/checkpoint"))?;
        let mut out = Vec::with_capacity(CHECKPOINT_ZSTD_MAGIC.len() + compressed.len());
        out.extend_from_slice(CHECKPOINT_ZSTD_MAGIC);
        out.extend_from_slice(&compressed);
        Ok(out)
    }
}

/// Runs CPU-bound work with `block_in_place` on a multi-thread runtime (a no-op wrapper elsewhere,
/// where `block_in_place` would panic), so it doesn't stall the worker's other tasks.
fn cpu_bound<R>(f: impl FnOnce() -> R) -> R {
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

async fn run_blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| IdentityError::CorruptedRecord("identity restore task panicked"))?
}

fn decode_checkpoint(bytes: &[u8]) -> Result<IdentityCheckpoint> {
    if let Some(payload) = bytes.strip_prefix(CHECKPOINT_ZSTD_MAGIC) {
        let decompressed = zstd::stream::decode_all(payload)
            .map_err(|_| IdentityError::CorruptedRecord("corrupted compressed meta/checkpoint"))?;
        bincode::deserialize(&decompressed)
            .map_err(|_| IdentityError::CorruptedRecord("corrupted meta/checkpoint"))
    } else {
        bincode::deserialize(bytes)
            .map_err(|_| IdentityError::CorruptedRecord("corrupted meta/checkpoint"))
    }
}

struct GraphState {
    u32_to_external: PagedStrings,
    origin_buckets: Vec<u8>,
    parent: Vec<u32>,
    bucket_masks: Vec<BucketMask>,
    cross_bucket_users: RoaringBitmap,
    identified_roots: RoaringBitmap,
}

impl GraphState {
    fn new() -> Self {
        Self {
            u32_to_external: PagedStrings::new(),
            origin_buckets: Vec::new(),
            parent: Vec::new(),
            bucket_masks: Vec::new(),
            cross_bucket_users: RoaringBitmap::new(),
            identified_roots: RoaringBitmap::new(),
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
    ) -> Result<u32> {
        let r_anon = self.find_root_mut(anon_u32);
        let r_user = self.find_root_mut(user_u32);

        // `P1-08`: Never merge an already-identified user root into a different user!
        if r_anon != r_user && self.identified_roots.contains(r_anon) {
            return Err(IdentityError::IdentifiedMergeConflict);
        }

        self.bucket_masks[anon_u32 as usize].insert(anon_bucket);
        self.bucket_masks[user_u32 as usize].insert(user_bucket);

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

        self.identified_roots.insert(r_user);
        self.bucket_masks[r_user as usize] = combined_mask;
        if combined_mask.is_cross_bucket() {
            self.cross_bucket_users.insert(r_user);
        }
        Ok(r_user)
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

    /// Builds a path-compressed `IdentitySnapshot` under a shared read lock without blocking writers.
    fn build_snapshot_ref(&self) -> IdentitySnapshot {
        let len = self.parent.len();
        let mut compressed = Vec::with_capacity(len);
        for i in 0..len {
            compressed.push(self.find_root_ref(i as u32));
        }
        IdentitySnapshot {
            canonical_map: Arc::new(compressed),
            cross_bucket_users: Arc::new(self.cross_bucket_users.clone()),
        }
    }

    fn snapshot_for_checkpoint(&self) -> CheckpointSnapshot {
        CheckpointSnapshot {
            u32_pages: self.u32_to_external.snapshot_pages(),
            total_users: self.u32_to_external.len(),
            origin_buckets: self.origin_buckets.clone(),
            parent: self.parent.clone(),
            bucket_masks: self.bucket_masks.clone(),
            identified_roots: self.identified_roots.clone(),
        }
    }
}

const META_CHUNK_COUNT_KEY: &[u8] = b"meta/chunk_count";
const META_CHECKPOINT_KEY: &[u8] = b"meta/checkpoint";
/// Minimum mutations since the last checkpoint before compacting history into `meta/checkpoint`.
/// Combined with the `user_count / 2` amortized rule, this keeps checkpoint write amplification O(1)
/// instead of O(N^2) as user count grows into the millions.
const CHECKPOINT_MIN_MUTATIONS: u64 = 250_000;
/// Hard cap on uncompacted log chunks so recovery never replays thousands of tiny flushes.
const CHECKPOINT_MAX_CHUNKS: u64 = 512;

/// Durable per-project `distinct_id <-> u32` dictionary and zero-rewrite Union-Find alias graph.
pub struct ProjectIdentityGraph {
    project_id: String,
    object_store: Arc<dyn ObjectStore>,
    read_only: bool,
    fast_lookup: DashMap<String, (u32, u8)>,
    state: RwLock<GraphState>,
    snapshot_cache: RwLock<Option<IdentitySnapshot>>,
    snapshot_dirty: AtomicBool,
    next_chunk_seq: AtomicU64,
    last_checkpoint_seq: AtomicU64,
    mutations_since_checkpoint: AtomicU64,
    /// Guards against two checkpoints running at once (see `checkpoint_if_due`).
    checkpointing: AtomicBool,
    mutation_epoch: AtomicU64,
    flushed_epoch: AtomicU64,
    pending_mutations: Mutex<Vec<MutationEntry>>,
    db: tokio::sync::Mutex<Option<Db>>,
}

fn restore_checkpoint(
    state: &mut GraphState,
    fast_lookup: &DashMap<String, (u32, u8)>,
    cp: IdentityCheckpoint,
) -> Result<u64> {
    if cp.u32_to_external.len() != cp.parent.len()
        || cp.u32_to_external.len() != cp.bucket_masks.len()
        || cp.u32_to_external.len() != cp.origin_buckets.len()
    {
        return Err(IdentityError::CorruptedRecord(
            "inconsistent vector lengths in meta/checkpoint",
        ));
    }
    fast_lookup.clear();
    state.cross_bucket_users.clear();
    state.identified_roots.clear();
    for (idx, ext) in cp.u32_to_external.iter().enumerate() {
        fast_lookup.insert(ext.clone(), (idx as u32, cp.origin_buckets[idx]));
    }
    for (idx, mask) in cp.bucket_masks.iter().enumerate() {
        if cp.parent[idx] == idx as u32 && mask.is_cross_bucket() {
            state.cross_bucket_users.insert(idx as u32);
        }
    }
    for r in cp.identified_roots {
        state.identified_roots.insert(r);
    }
    state.u32_to_external = PagedStrings::from_vec(cp.u32_to_external);
    state.origin_buckets = cp.origin_buckets;
    state.parent = cp.parent;
    state.bucket_masks = cp.bucket_masks;
    Ok(cp.next_chunk_seq)
}

fn apply_mutation_entries(
    state: &mut GraphState,
    fast_lookup: &DashMap<String, (u32, u8)>,
    entries: Vec<MutationEntry>,
) -> Result<()> {
    for entry in entries {
        match entry {
            MutationEntry::Intern {
                uid,
                origin_bucket,
                distinct_id,
            } => {
                let cur_len = state.u32_to_external.len();
                if (uid as usize) > cur_len {
                    return Err(IdentityError::CorruptedRecord(
                        "non-contiguous intern uid gap in mutation log",
                    ));
                }
                if (uid as usize) == cur_len {
                    state.u32_to_external.push(distinct_id.clone());
                    state.origin_buckets.push(origin_bucket);
                    state.parent.push(uid);
                    state.bucket_masks.push(BucketMask::single(origin_bucket));
                } else {
                    state.u32_to_external.set(uid as usize, distinct_id.clone());
                    state.origin_buckets[uid as usize] = origin_bucket;
                    state.bucket_masks[uid as usize].insert(origin_bucket);
                }
                if state.bucket_masks[uid as usize].is_cross_bucket() {
                    state.cross_bucket_users.insert(uid);
                }
                fast_lookup.insert(distinct_id, (uid, origin_bucket));
            }
            MutationEntry::ExtraBucket { uid, bucket_id } => {
                if (uid as usize) >= state.bucket_masks.len() {
                    return Err(IdentityError::CorruptedRecord(
                        "dangling uid reference in ExtraBucket log entry",
                    ));
                }
                state.bucket_masks[uid as usize].insert(bucket_id);
                let root = state.find_root_mut(uid);
                state.bucket_masks[root as usize].insert(bucket_id);
                if state.bucket_masks[root as usize].is_cross_bucket() {
                    state.cross_bucket_users.insert(root);
                }
            }
            MutationEntry::Alias {
                anon_u32,
                user_u32,
                anon_bucket,
                user_bucket,
            } => {
                if (anon_u32 as usize) >= state.parent.len()
                    || (user_u32 as usize) >= state.parent.len()
                {
                    return Err(IdentityError::CorruptedRecord(
                        "dangling uid reference in Alias log entry",
                    ));
                }
                // Apply deterministically; ignore conflict only if historical duplicate alias was logged
                let _ = state.apply_alias(anon_u32, user_u32, anon_bucket, user_bucket);
            }
        }
    }
    Ok(())
}

async fn open_slatedb_handle(project_id: &str, object_store: &Arc<dyn ObjectStore>) -> Result<Db> {
    let db_path = ObjectPath::from(format!("identity/{project_id}"));
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
            Err(e) => return Err(IdentityError::SlateDb(e)),
        }
    }
    Err(IdentityError::SlateDb(SlateDBError::Fenced))
}

impl ProjectIdentityGraph {
    /// Opens or recovers a `ProjectIdentityGraph` from SlateDB at `identity/<project_id>`.
    pub async fn open(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<Self>> {
        let project_id = project_id.into();
        let db = open_slatedb_handle(&project_id, &object_store).await?;

        let chunk_count = match db.get(META_CHUNK_COUNT_KEY).await? {
            Some(b) if b.len() >= 8 => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&b[0..8]);
                u64::from_le_bytes(arr)
            }
            Some(_) => {
                return Err(IdentityError::CorruptedRecord(
                    "truncated meta/chunk_count value",
                ))
            }
            None => 0,
        };

        let mut fast_lookup: DashMap<String, (u32, u8)> = DashMap::new();
        let mut state = GraphState::new();
        let mut start_seq = 0u64;

        // Restore runs are CPU-bound for millions of users: keep them off async workers so request
        // deadlines and other tasks keep running during a cold open (#68).
        if let Some(cp_bytes) = db.get(META_CHECKPOINT_KEY).await? {
            (state, fast_lookup, start_seq) = run_blocking(move || {
                let cp = decode_checkpoint(&cp_bytes)?;
                let seq = restore_checkpoint(&mut state, &fast_lookup, cp)?;
                Ok((state, fast_lookup, seq))
            })
            .await?;
        }

        // Replay remaining log chunks strictly (`P1-11`: fail loudly on missing chunk or dangling ref!)
        let mut replayed_mutations = 0u64;
        for seq in start_seq..chunk_count {
            let chunk_key = format!("log/chunk_{seq:012}");
            let Some(bytes) = db.get(chunk_key.as_bytes()).await? else {
                return Err(IdentityError::CorruptedRecord(
                    "missing identity mutation log chunk",
                ));
            };
            let n;
            (state, fast_lookup, n) = run_blocking(move || {
                let entries = decode_mutation_chunk(&bytes)?;
                let n = entries.len() as u64;
                apply_mutation_entries(&mut state, &fast_lookup, entries)?;
                Ok((state, fast_lookup, n))
            })
            .await?;
            replayed_mutations = replayed_mutations.saturating_add(n);
        }

        let (state, initial_snap) = run_blocking(move || {
            let snap = state.compress_all_and_build_snapshot();
            Ok((state, snap))
        })
        .await?;

        Ok(Arc::new(Self {
            project_id,
            object_store,
            read_only: false,
            fast_lookup,
            state: RwLock::new(state),
            snapshot_cache: RwLock::new(Some(initial_snap)),
            snapshot_dirty: AtomicBool::new(false),
            next_chunk_seq: AtomicU64::new(chunk_count),
            last_checkpoint_seq: AtomicU64::new(start_seq),
            mutations_since_checkpoint: AtomicU64::new(replayed_mutations),
            checkpointing: AtomicBool::new(false),
            mutation_epoch: AtomicU64::new(0),
            flushed_epoch: AtomicU64::new(0),
            pending_mutations: Mutex::new(Vec::new()),
            db: tokio::sync::Mutex::new(Some(db)),
        }))
    }

    /// Opens a read-only `ProjectIdentityGraph` backed by `slatedb::DbReader` (never fencing active writers).
    pub async fn open_read_only(
        project_id: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<Self>> {
        let mut state = GraphState::new();
        let initial_snap = state.compress_all_and_build_snapshot();
        let graph = Arc::new(Self {
            project_id: project_id.into(),
            object_store,
            read_only: true,
            fast_lookup: DashMap::new(),
            state: RwLock::new(state),
            snapshot_cache: RwLock::new(Some(initial_snap)),
            snapshot_dirty: AtomicBool::new(false),
            next_chunk_seq: AtomicU64::new(0),
            last_checkpoint_seq: AtomicU64::new(0),
            mutations_since_checkpoint: AtomicU64::new(0),
            checkpointing: AtomicBool::new(false),
            mutation_epoch: AtomicU64::new(0),
            flushed_epoch: AtomicU64::new(0),
            pending_mutations: Mutex::new(Vec::new()),
            db: tokio::sync::Mutex::new(None),
        });
        graph.refresh().await?;
        Ok(graph)
    }

    /// Incrementally refreshes a read-only `ProjectIdentityGraph` from SlateDB using `DbReader`.
    /// Returns `Ok(true)` if new identity mutations were loaded, or `Ok(false)` if unchanged or writable.
    pub async fn refresh(&self) -> Result<bool> {
        if !self.read_only {
            return Ok(false);
        }
        let _refresh_guard = self.db.lock().await;

        let db_path = ObjectPath::from(format!("identity/{}", self.project_id));
        let reader = match DbReader::open(
            db_path.clone(),
            self.object_store.clone(),
            None,
            DbReaderOptions::default(),
        )
        .await
        {
            Ok(r) => r,
            Err(
                SlateDBError::LatestManifestMissing
                | SlateDBError::ManifestMissing(_)
                | SlateDBError::EmptyManifest
                | SlateDBError::InvalidDBState,
            ) => return Ok(false),
            Err(SlateDBError::ManifestVersionExists) => {
                tokio::time::sleep(Duration::from_millis(15)).await;
                match DbReader::open(
                    db_path,
                    self.object_store.clone(),
                    None,
                    DbReaderOptions::default(),
                )
                .await
                {
                    Ok(r) => r,
                    Err(
                        SlateDBError::LatestManifestMissing
                        | SlateDBError::ManifestMissing(_)
                        | SlateDBError::EmptyManifest
                        | SlateDBError::InvalidDBState,
                    ) => return Ok(false),
                    Err(e) => return Err(IdentityError::SlateDb(e)),
                }
            }
            Err(e) => return Err(IdentityError::SlateDb(e)),
        };

        let res: Result<bool> = async {
            let chunk_count = match reader.get(META_CHUNK_COUNT_KEY).await? {
                Some(b) if b.len() >= 8 => {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&b[0..8]);
                    u64::from_le_bytes(arr)
                }
                Some(_) => {
                    return Err(IdentityError::CorruptedRecord(
                        "truncated meta/chunk_count value",
                    ))
                }
                None => return Ok(false),
            };

            let mut cur_seq = self.next_chunk_seq.load(Ordering::Acquire);
            if chunk_count <= cur_seq {
                return Ok(false);
            }

            let mut cp_to_restore: Option<IdentityCheckpoint> = None;
            if let Some(cp_bytes) = reader.get(META_CHECKPOINT_KEY).await? {
                let cp = run_blocking(move || decode_checkpoint(&cp_bytes)).await?;
                if cp.next_chunk_seq > cur_seq {
                    cur_seq = cp.next_chunk_seq;
                    cp_to_restore = Some(cp);
                }
            }

            let mut decoded_chunks = Vec::with_capacity((chunk_count - cur_seq) as usize);
            for seq in cur_seq..chunk_count {
                let chunk_key = format!("log/chunk_{seq:012}");
                let Some(bytes) = reader.get(chunk_key.as_bytes()).await? else {
                    return Err(IdentityError::CorruptedRecord(
                        "missing identity mutation log chunk",
                    ));
                };
                decoded_chunks.push(decode_mutation_chunk(&bytes)?);
            }

            // Restores into `self` (not 'static), so block this worker in place rather than
            // `spawn_blocking`; either way the runtime keeps other tasks and timers moving (#68).
            cpu_bound(|| -> Result<()> {
                let mut state = self.state.write();
                if let Some(cp) = cp_to_restore {
                    let cp_seq = restore_checkpoint(&mut state, &self.fast_lookup, cp)?;
                    self.last_checkpoint_seq.store(cp_seq, Ordering::Release);
                }
                for entries in decoded_chunks {
                    apply_mutation_entries(&mut state, &self.fast_lookup, entries)?;
                }
                Ok(())
            })?;
            self.next_chunk_seq.store(chunk_count, Ordering::Release);
            self.snapshot_dirty.store(true, Ordering::Release);
            Ok(true)
        }
        .await;

        let _ = reader.close().await;
        res
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn user_count(&self) -> usize {
        self.state.read().u32_to_external.len()
    }

    pub fn has_pending_mutations(&self) -> bool {
        self.mutation_epoch.load(Ordering::Acquire) > self.flushed_epoch.load(Ordering::Acquire)
    }

    /// Lock-free hot path lookup + monotonic `u32` allocation on miss (`P1-11` ordered mutation push).
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
        state.origin_buckets.push(bucket_id);
        state.parent.push(uid);
        state.bucket_masks.push(mask);

        // Push to `pending_mutations` BEFORE publishing in `fast_lookup` or releasing `state`!
        {
            let mut pending = self.pending_mutations.lock();
            pending.push(MutationEntry::Intern {
                uid,
                origin_bucket: bucket_id,
                distinct_id: distinct_id.to_string(),
            });
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.fast_lookup
            .insert(distinct_id.to_string(), (uid, bucket_id));
        self.snapshot_dirty.store(true, Ordering::Release);
        drop(state);

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
            {
                let mut pending = self.pending_mutations.lock();
                pending.push(MutationEntry::ExtraBucket { uid, bucket_id });
                self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            }
            self.snapshot_dirty.store(true, Ordering::Release);
        }
    }

    /// Retroactively merges `anon_id -> user_id` without rewriting historical segments (`P1-08`, `P1-11`).
    /// Refuses with `Err(IdentityError::IdentifiedMergeConflict)` if `anon_id` already resolves to a different identified user.
    pub fn alias(
        &self,
        anon_id: &str,
        user_id: &str,
        anon_bucket: u8,
        user_bucket: u8,
    ) -> Result<u32> {
        let anon_u32 = self.get_or_intern(anon_id, anon_bucket);
        let user_u32 = self.get_or_intern(user_id, user_bucket);

        let mut state = self.state.write();
        let canonical_root = state.apply_alias(anon_u32, user_u32, anon_bucket, user_bucket)?;
        {
            let mut pending = self.pending_mutations.lock();
            pending.push(MutationEntry::Alias {
                anon_u32,
                user_u32,
                anon_bucket,
                user_bucket,
            });
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.snapshot_dirty.store(true, Ordering::Release);
        Ok(canonical_root)
    }

    /// Performs `alias(...)` and immediately flushes the mutation batch to SlateDB.
    pub async fn alias_durable(
        &self,
        anon_id: &str,
        user_id: &str,
        anon_bucket: u8,
        user_bucket: u8,
    ) -> Result<u32> {
        let root = self.alias(anon_id, user_id, anon_bucket, user_bucket)?;
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
        self.state
            .read()
            .u32_to_external
            .get(uid as usize)
            .map(str::to_string)
    }

    /// Expands a bitmap of canonical `u32` user IDs into a sorted, deduplicated `Vec<String>` of ALL
    /// external `distinct_id`s (both pre-login `anon_id`s and identified `user_id`s) belonging to those users.
    pub fn external_ids_for_canonical_roots(&self, canonical_roots: &RoaringBitmap) -> Vec<String> {
        if canonical_roots.is_empty() {
            return Vec::new();
        }
        let snap = self.snapshot();
        let st = self.state.read();
        let mut out = Vec::new();
        for (raw_uid, ext) in st.u32_to_external.iter().enumerate() {
            let canon = snap.resolve(raw_uid as u32);
            if canonical_roots.contains(canon) {
                out.push(ext.to_string());
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Returns a `RoaringBitmap` of every raw `u32` UID (including all merged `anon_id` aliases)
    /// whose canonical root matches `canonical_uid`, plus `canonical_uid` itself (`Gap 4`).
    pub fn all_raw_uids_for_canonical_root(&self, canonical_uid: u32) -> RoaringBitmap {
        let mut out = RoaringBitmap::new();
        out.insert(canonical_uid);
        let st = self.state.read();
        for raw_uid in 0..st.u32_to_external.len() {
            let raw = raw_uid as u32;
            if st.find_root_ref(raw) == canonical_uid {
                out.insert(raw);
            }
        }
        out
    }

    /// Returns an immutable `IdentitySnapshot` in < 5ms for lock-free vectorized query scans.
    pub fn snapshot(&self) -> IdentitySnapshot {
        if !self.snapshot_dirty.load(Ordering::Acquire) {
            if let Some(snap) = self.snapshot_cache.read().as_ref() {
                return snap.clone();
            }
        }

        let mut cache_guard = self.snapshot_cache.write();
        if !self.snapshot_dirty.load(Ordering::Acquire) {
            if let Some(snap) = cache_guard.as_ref() {
                return snap.clone();
            }
        }
        self.snapshot_dirty.store(false, Ordering::Release);
        let snap = self.state.read().build_snapshot_ref();
        *cache_guard = Some(snap.clone());
        snap
    }

    /// Flushes all staged intern & alias records durably to SlateDB on `object_store`.
    ///
    /// Uses epoch-based group-commit coalescing so concurrent callers share a single SlateDB flush,
    /// and retains `pending_mutations` and `next_chunk_seq` unchanged if the write or flush fails.
    /// Only writes small log chunks: checkpoints run separately in [`Self::checkpoint_if_due`] so
    /// ingest never waits on an O(users) checkpoint (#70).
    pub async fn flush(&self) -> Result<()> {
        self.flush_chunks(false).await.map(|_| ())
    }

    /// Writes pending mutations as log chunks. With `snapshot`, also returns a checkpoint snapshot
    /// taken atomically with the drained entries, so it covers exactly the chunks `< seq`, plus the
    /// `mutations_since_checkpoint` count it covers.
    async fn flush_chunks(&self, snapshot: bool) -> Result<Option<(CheckpointSnapshot, u64, u64)>> {
        if self.read_only {
            return Ok(None);
        }
        let target_epoch = self.mutation_epoch.load(Ordering::Acquire);
        if !snapshot && self.flushed_epoch.load(Ordering::Acquire) >= target_epoch {
            return Ok(None);
        }

        let mut db_guard = self.db.lock().await;
        if !snapshot && self.flushed_epoch.load(Ordering::Acquire) >= target_epoch {
            return Ok(None);
        }

        let base_seq = self.next_chunk_seq.load(Ordering::Acquire);
        // Acquire `state.read()` BEFORE `pending_mutations.lock()` (same lock order as writers).
        let (staged, batch_epoch, cp_snap) = {
            let st = snapshot.then(|| self.state.read());
            let guard = self.pending_mutations.lock();
            (
                guard.clone(),
                self.mutation_epoch.load(Ordering::Acquire),
                st.as_ref().map(|s| s.snapshot_for_checkpoint()),
            )
        };
        if staged.is_empty() {
            self.flushed_epoch.fetch_max(batch_epoch, Ordering::AcqRel);
            let muts = self.mutations_since_checkpoint.load(Ordering::Acquire);
            return Ok(cp_snap.map(|c| (c, base_seq, muts)));
        }
        let flushed_len = staged.len();

        if db_guard.is_none() {
            let reopened = open_slatedb_handle(&self.project_id, &self.object_store).await?;
            *db_guard = Some(reopened);
        }
        let db = db_guard.as_ref().ok_or(IdentityError::Closed)?;

        const CHUNK_SIZE: usize = 32_768;
        let mut new_seq = base_seq;
        let mut batch = WriteBatch::new();
        for slice in staged.chunks(CHUNK_SIZE) {
            let chunk_key = format!("log/chunk_{new_seq:012}");
            let chunk_bytes = encode_mutation_chunk(slice);
            batch.put(chunk_key.as_bytes(), &chunk_bytes);
            new_seq += 1;
        }
        batch.put(META_CHUNK_COUNT_KEY, new_seq.to_le_bytes());

        let write_opts = WriteOptions {
            await_durable: false,
        };
        if let Err(e) = db.write_with_options(batch, &write_opts).await {
            // SlateDB latches write errors on its handle; drop it so the next retry reopens cleanly
            let _ = db_guard.take();
            return Err(IdentityError::SlateDb(e));
        }
        if let Err(e) = db.flush().await {
            let _ = db_guard.take();
            return Err(IdentityError::SlateDb(e));
        }

        // Commit state ONLY after SlateDB flush succeeded
        self.next_chunk_seq.store(new_seq, Ordering::Release);
        let muts = self
            .mutations_since_checkpoint
            .fetch_add(flushed_len as u64, Ordering::AcqRel)
            + flushed_len as u64;
        self.pending_mutations.lock().drain(..flushed_len);
        self.flushed_epoch.fetch_max(batch_epoch, Ordering::AcqRel);
        Ok(cp_snap.map(|c| (c, new_seq, muts)))
    }

    /// Compacts the mutation log into `meta/checkpoint` once enough history has built up, bounding
    /// reopen cost. The O(users) encode runs with no lock held, so concurrent `flush()` calls (the
    /// ingest path) keep writing chunks; only the final small batch write takes the db lock.
    /// Returns `true` if a checkpoint was written.
    pub async fn checkpoint_if_due(&self) -> Result<bool> {
        if self.read_only {
            return Ok(false);
        }
        let prev_cp = self.last_checkpoint_seq.load(Ordering::Acquire);
        let muts = self.mutations_since_checkpoint.load(Ordering::Acquire)
            + self.pending_mutations.lock().len() as u64;
        let threshold = CHECKPOINT_MIN_MUTATIONS.max(self.user_count() as u64 / 2);
        let chunks = self
            .next_chunk_seq
            .load(Ordering::Acquire)
            .saturating_sub(prev_cp);
        if muts < threshold && chunks < CHECKPOINT_MAX_CHUNKS {
            return Ok(false);
        }
        if self.checkpointing.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }
        let res = self.write_checkpoint().await;
        self.checkpointing.store(false, Ordering::Release);
        res.map(|()| true)
    }

    async fn write_checkpoint(&self) -> Result<()> {
        let Some((cp_snap, seq, covered_muts)) = self.flush_chunks(true).await? else {
            return Ok(());
        };
        let prev_cp = self.last_checkpoint_seq.load(Ordering::Acquire);
        let cp_bytes = run_blocking(move || cp_snap.encode(seq)).await?;

        let mut db_guard = self.db.lock().await;
        if db_guard.is_none() {
            let reopened = open_slatedb_handle(&self.project_id, &self.object_store).await?;
            *db_guard = Some(reopened);
        }
        let db = db_guard.as_ref().ok_or(IdentityError::Closed)?;
        // Chunks `>= seq` (written by flushes since the snapshot) stay, and so does `chunk_count`.
        let mut batch = WriteBatch::new();
        batch.put(META_CHECKPOINT_KEY, &cp_bytes);
        for old_seq in prev_cp..seq {
            let old_key = format!("log/chunk_{old_seq:012}");
            batch.delete(old_key.as_bytes());
        }
        let write_opts = WriteOptions {
            await_durable: false,
        };
        if let Err(e) = db.write_with_options(batch, &write_opts).await {
            let _ = db_guard.take();
            return Err(IdentityError::SlateDb(e));
        }
        if let Err(e) = db.flush().await {
            let _ = db_guard.take();
            return Err(IdentityError::SlateDb(e));
        }
        self.last_checkpoint_seq.store(seq, Ordering::Release);
        self.mutations_since_checkpoint
            .fetch_sub(covered_muts, Ordering::AcqRel);
        Ok(())
    }

    /// Flushes pending mutations and cleanly closes the underlying SlateDB handle.
    pub async fn close(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        self.flush().await?;
        self.checkpoint_if_due().await?;
        let mut db_guard = self.db.lock().await;
        if let Some(db) = db_guard.take() {
            // See `ProjectProfileStore::close`: a fenced handle panics in SlateDB 0.6's `close`.
            match tokio::spawn(async move { db.close().await }).await {
                Ok(res) => res?,
                Err(e) => {
                    tracing::error!(project_id = %self.project_id, error = %e, "closing the identity store panicked (fenced by another writer?)");
                    return Err(IdentityError::Closed);
                }
            }
        }
        Ok(())
    }
}

/// Per-key locks for [`open_once`].
pub type OpenLocks = DashMap<String, Arc<tokio::sync::Mutex<()>>>;

/// Returns `map[key]`, opening it with `open` if absent. The open runs in a spawned task holding a
/// per-key lock and caches its own result, so a caller that gives up (a query deadline) neither
/// aborts a half-done open nor makes the next caller start over, and opening one project never
/// blocks another (#68). `on_insert` runs right after the value is cached.
pub async fn open_once<T, E>(
    map: &Arc<DashMap<String, Arc<T>>>,
    locks: &OpenLocks,
    key: &str,
    on_insert: impl FnOnce(&Arc<T>) + Send + 'static,
    open: impl std::future::Future<Output = std::result::Result<Arc<T>, E>> + Send + 'static,
) -> std::result::Result<Arc<T>, E>
where
    T: Send + Sync + 'static,
    E: Send + 'static,
{
    if let Some(v) = map.get(key) {
        return Ok(v.clone());
    }
    let lock = locks.entry(key.to_string()).or_default().clone();
    let (map, key) = (map.clone(), key.to_string());
    let task = tokio::spawn(async move {
        let _guard = lock.lock_owned().await;
        if let Some(v) = map.get(&key) {
            return Ok(v.clone());
        }
        let v = open.await?;
        map.insert(key, v.clone());
        on_insert(&v);
        Ok(v)
    });
    match task.await {
        Ok(res) => res,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

/// Multi-tenant registry managing `ProjectIdentityGraph` instances over shared object storage.
pub struct IdentityRouter {
    object_store: Arc<dyn ObjectStore>,
    read_only: bool,
    graphs: Arc<DashMap<String, Arc<ProjectIdentityGraph>>>,
    /// Read-only graphs for projects this node does not write, with when each was last refreshed.
    readers: DashMap<String, (Arc<ProjectIdentityGraph>, Instant)>,
    open_locks: OpenLocks,
}

/// How stale a cached reader may be before a read refreshes it. Refreshing loads only new chunks.
const READER_REFRESH: Duration = Duration::from_secs(10);

impl IdentityRouter {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            read_only: false,
            graphs: Arc::default(),
            readers: DashMap::new(),
            open_locks: OpenLocks::default(),
        }
    }

    pub fn new_read_only(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            read_only: true,
            graphs: Arc::default(),
            readers: DashMap::new(),
            open_locks: OpenLocks::default(),
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// A read-only graph for a project this node does not write. Never fences the writer.
    /// Cached: loaded once, then refreshed on a read at most every `READER_REFRESH`, so a project
    /// nobody reads costs nothing.
    pub async fn open_reader(&self, project_id: &str) -> Result<Arc<ProjectIdentityGraph>> {
        let stale = match self.readers.get(project_id) {
            Some(e) if e.1.elapsed() < READER_REFRESH => return Ok(e.0.clone()),
            Some(e) => Some(e.0.clone()),
            None => None,
        };
        if let Some(g) = stale {
            g.refresh().await?;
            self.readers
                .insert(project_id.to_string(), (g.clone(), Instant::now()));
            return Ok(g);
        }
        // Per-project, so one cold reader open doesn't block readers of other projects (#68).
        let lock = self
            .open_locks
            .entry(format!("reader:{project_id}"))
            .or_default()
            .clone();
        let _guard = lock.lock().await;
        if let Some(e) = self.readers.get(project_id) {
            return Ok(e.0.clone());
        }
        let g =
            ProjectIdentityGraph::open_read_only(project_id.to_string(), self.object_store.clone())
                .await?;
        self.readers
            .insert(project_id.to_string(), (g.clone(), Instant::now()));
        Ok(g)
    }

    /// Drops the cached reader, e.g. once this node opens the project for writing.
    pub fn evict_reader(&self, project_id: &str) {
        self.readers.remove(project_id);
    }

    pub async fn get_or_open(&self, project_id: &str) -> Result<Arc<ProjectIdentityGraph>> {
        if let Some(g) = self.graphs.get(project_id) {
            return Ok(g.clone());
        }
        let (store, read_only, pid) = (
            self.object_store.clone(),
            self.read_only,
            project_id.to_string(),
        );
        open_once(
            &self.graphs,
            &self.open_locks,
            project_id,
            |_| {},
            async move {
                if read_only {
                    ProjectIdentityGraph::open_read_only(pid, store).await
                } else {
                    ProjectIdentityGraph::open(pid, store).await
                }
            },
        )
        .await
    }

    /// Forgets a project's graph so the next `get_or_open` rebuilds it from storage.
    pub fn evict(&self, project_id: &str) {
        self.graphs.remove(project_id);
        self.readers.remove(project_id);
    }

    pub async fn refresh_all(&self) -> Result<usize> {
        let graphs: Vec<Arc<ProjectIdentityGraph>> =
            self.graphs.iter().map(|e| e.value().clone()).collect();
        let mut updated = 0usize;
        for g in graphs {
            if g.refresh().await? {
                updated += 1;
            }
        }
        Ok(updated)
    }

    pub async fn flush_all(&self) -> Result<()> {
        let graphs: Vec<Arc<ProjectIdentityGraph>> =
            self.graphs.iter().map(|e| e.value().clone()).collect();
        for g in graphs {
            g.flush().await?;
            // Background maintenance is the only regular checkpoint trigger (ingest only flushes).
            g.checkpoint_if_due().await?;
        }
        Ok(())
    }

    pub async fn close_all(&self) -> Result<()> {
        let graphs: Vec<Arc<ProjectIdentityGraph>> =
            self.graphs.iter().map(|e| e.value().clone()).collect();
        // Close every store even if one fails, and report the first failure.
        let mut first_err = None;
        for g in graphs {
            if let Err(e) = g.close().await {
                first_err.get_or_insert(e);
            }
        }
        self.graphs.clear();
        self.readers.clear();
        first_err.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use object_store::local::LocalFileSystem;

    use super::*;

    #[test]
    fn paged_strings_from_vec_is_linear_and_keeps_order() {
        let n = EXTERNAL_ID_PAGE_SIZE * 3 + 17;
        let paged = PagedStrings::from_vec((0..n).map(|i| i.to_string()).collect());
        assert_eq!(paged.len(), n);
        assert_eq!(paged.pages.len(), 4);
        // #67: every page used to keep the capacity of the whole remaining tail.
        assert!(paged
            .pages
            .iter()
            .all(|p| p.capacity() <= EXTERNAL_ID_PAGE_SIZE));
        assert!(paged.iter().enumerate().all(|(i, s)| s == i.to_string()));
    }

    /// #70: flushes that land while a checkpoint is encoding must survive reopen (chunks `>= seq`
    /// are kept), and the checkpoint must not replay or drop anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flushes_during_a_background_checkpoint_survive_reopen() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        let graph = ProjectIdentityGraph::open("proj_cp", store.clone())
            .await
            .expect("open");
        for i in 0..300_000u32 {
            graph.get_or_intern(&format!("u{i}"), (i % 256) as u8);
        }
        graph.flush().await.expect("flush");
        let cp = {
            let g = graph.clone();
            tokio::spawn(async move { g.checkpoint_if_due().await })
        };
        for batch in 0..50u32 {
            for i in 0..1_000u32 {
                let id = 300_000 + batch * 1_000 + i;
                graph.get_or_intern(&format!("u{id}"), (id % 256) as u8);
            }
            graph.flush().await.expect("flush during checkpoint");
        }
        assert!(cp.await.expect("join").expect("checkpoint"));
        assert!(graph.last_checkpoint_seq.load(Ordering::Acquire) > 0);
        // Close without a second checkpoint: reopen must use checkpoint + trailing chunks.
        graph.flush().await.expect("flush");
        graph
            .db
            .lock()
            .await
            .take()
            .unwrap()
            .close()
            .await
            .expect("close db");
        drop(graph);

        let reopened = ProjectIdentityGraph::open("proj_cp", store)
            .await
            .expect("reopen");
        assert_eq!(reopened.user_count(), 350_000);
        for id in [0u32, 299_999, 300_000, 349_999] {
            assert_eq!(reopened.lookup_raw(&format!("u{id}")), Some(id));
        }
    }

    /// #68: a caller that times out must not abort the open or cause a second one, and a slow open
    /// of one project must not block another.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_once_survives_caller_timeout_and_is_per_key() {
        let map: Arc<DashMap<String, Arc<u32>>> = Arc::default();
        let locks = OpenLocks::default();
        let opens = Arc::new(AtomicU64::new(0));
        let slow = |opens: Arc<AtomicU64>, v: u32, ms: u64| async move {
            opens.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok::<_, ()>(Arc::new(v))
        };

        let t = Instant::now();
        let timed_out = tokio::time::timeout(
            Duration::from_millis(50),
            open_once(&map, &locks, "big", |_| {}, slow(opens.clone(), 1, 400)),
        )
        .await;
        assert!(timed_out.is_err());
        assert!(t.elapsed() < Duration::from_millis(200));

        let t = Instant::now();
        let other = open_once(&map, &locks, "small", |_| {}, slow(opens.clone(), 2, 0))
            .await
            .unwrap();
        assert_eq!(*other, 2);
        assert!(
            t.elapsed() < Duration::from_millis(200),
            "blocked behind 'big'"
        );

        let big = open_once(&map, &locks, "big", |_| {}, slow(opens.clone(), 9, 0))
            .await
            .unwrap();
        assert_eq!(*big, 1, "second caller must get the first open's result");
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    fn rss_mb() -> u64 {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .expect("ps");
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(0)
            / 1024
    }

    /// Scale check for #67/#70: `PULSE_ID_BENCH_USERS=10000000 cargo test --release -p pulse-identity
    /// -- --ignored --nocapture identity_scale`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn identity_scale_ingest_flush_latency_and_reopen_memory() {
        let n: u32 = std::env::var("PULSE_ID_BENCH_USERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000_000);
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        {
            let graph = ProjectIdentityGraph::open("proj_scale", store.clone())
                .await
                .expect("open");
            let mut lat = Vec::new();
            let mut worst_cp = Duration::ZERO;
            for batch in 0..n.div_ceil(1_000) {
                for i in batch * 1_000..((batch + 1) * 1_000).min(n) {
                    graph.get_or_intern(&format!("user-{i:010}-device"), (i % 256) as u8);
                }
                let t = Instant::now();
                graph.flush().await.expect("flush");
                lat.push(t.elapsed());
                if batch % 1_000 == 999 {
                    let t = Instant::now();
                    graph.checkpoint_if_due().await.expect("checkpoint");
                    worst_cp = worst_cp.max(t.elapsed());
                }
            }
            lat.sort();
            println!(
                "users={n} flush p50={:?} p99={:?} max={:?} worst_checkpoint={worst_cp:?} rss={}MB",
                lat[lat.len() / 2],
                lat[lat.len() * 99 / 100],
                lat[lat.len() - 1],
                rss_mb()
            );
            graph.close().await.expect("close");
        }
        let before = rss_mb();
        let t = Instant::now();
        let graph = ProjectIdentityGraph::open("proj_scale", store.clone())
            .await
            .expect("reopen");
        let _ = graph.snapshot();
        println!(
            "reopen users={} took={:?} rss_before={before}MB rss_after={}MB",
            graph.user_count(),
            t.elapsed(),
            rss_mb()
        );
        assert_eq!(graph.user_count(), n as usize);
    }

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
                let root = graph
                    .alias(
                        &format!("anon_a_{i:06}"),
                        &format!("user_{i:06}"),
                        anon_bucket,
                        user_bucket,
                    )
                    .expect("alias anon_a");
                assert_eq!(root, i);
            }
            for i in 0..10_000u32 {
                let user_bucket = (i % 256) as u8;
                let anon_b_bucket = ((i + 37) % 256) as u8;
                let root = graph
                    .alias(
                        &format!("anon_b_{i:06}"),
                        &format!("user_{i:06}"),
                        anon_b_bucket,
                        user_bucket,
                    )
                    .expect("alias anon_b");
                assert_eq!(root, i);
            }

            let t0 = Instant::now();
            let snap = graph.snapshot();
            let elapsed = t0.elapsed();
            // Wall-clock budgets only hold in --release, like the exec and scale_slo gates;
            // a debug build on a shared CI runner takes several times longer.
            #[cfg(not(debug_assertions))]
            assert!(
                elapsed < Duration::from_millis(5),
                "expected < 5ms snapshot generation, took {elapsed:?}"
            );
            #[cfg(debug_assertions)]
            let _ = elapsed;

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
        #[cfg(not(debug_assertions))]
        assert!(
            rec_snap_dur < Duration::from_millis(5),
            "post-restart snapshot took {rec_snap_dur:?}"
        );
        #[cfg(debug_assertions)]
        let _ = rec_snap_dur;

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
