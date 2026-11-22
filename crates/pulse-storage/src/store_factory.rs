use std::fmt::{Debug, Display, Formatter};
use std::net::{IpAddr, ToSocketAddrs};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::BoxStream;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult, UploadPart,
};
use url::Url;

use crate::error::{Result, StorageError};

#[cfg(test)]
fn fsync_probe_map() -> &'static parking_lot::Mutex<std::collections::HashMap<PathBuf, (u64, u64)>>
{
    static PROBE: OnceLock<parking_lot::Mutex<std::collections::HashMap<PathBuf, (u64, u64)>>> =
        OnceLock::new();
    PROBE.get_or_init(|| parking_lot::Mutex::new(std::collections::HashMap::new()))
}

fn fsync_dir_sync(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

fn fsync_path_sync(target: &Path) -> std::io::Result<()> {
    let file = std::fs::File::open(target)?;
    file.sync_data()?;
    #[cfg(test)]
    {
        fsync_probe_map()
            .lock()
            .entry(target.to_path_buf())
            .or_default()
            .0 += 1;
    }
    if let Some(parent) = target.parent() {
        fsync_dir_sync(parent)?;
        #[cfg(test)]
        {
            fsync_probe_map()
                .lock()
                .entry(target.to_path_buf())
                .or_default()
                .1 += 1;
        }
    }
    Ok(())
}

async fn fsync_path(target: PathBuf) -> OsResult<()> {
    tokio::task::spawn_blocking(move || fsync_path_sync(&target))
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "DurableLocalObjectStore",
            source: Box::new(std::io::Error::other(e.to_string())),
        })?
        .map_err(|e| object_store::Error::Generic {
            store: "DurableLocalObjectStore",
            source: Box::new(e),
        })
}

#[derive(Debug)]
struct DurableMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    target: PathBuf,
}

#[async_trait]
impl MultipartUpload for DurableMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let res = self.inner.complete().await?;
        fsync_path(self.target.clone()).await?;
        Ok(res)
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.inner.abort().await
    }
}

/// Durable wrapper around `object_store::local::LocalFileSystem` that issues `File::sync_data`
/// on the written file AND `File::sync_all` on its parent directory after every `put_opts`,
/// `put_multipart_opts` (`complete`), and `copy*`, guaranteeing true RPO = 0 crash durability on
/// local ext4/APFS/XFS filesystems.
#[derive(Debug)]
pub struct DurableLocalObjectStore {
    root: PathBuf,
    inner: LocalFileSystem,
}

impl DurableLocalObjectStore {
    pub fn new_at(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let root = std::fs::canonicalize(dir)?;
        let inner = LocalFileSystem::new_with_prefix(&root)?;
        Ok(Self { root, inner })
    }

    fn fs_path_for(&self, location: &ObjectPath) -> PathBuf {
        let mut full = self.root.clone();
        for part in location.parts() {
            full.push(part.as_ref());
        }
        full
    }

    async fn fsync_target(&self, location: &ObjectPath) -> OsResult<()> {
        fsync_path(self.fs_path_for(location)).await
    }
}

impl Display for DurableLocalObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DurableLocalObjectStore({})", self.root.display())
    }
}

#[async_trait]
impl ObjectStore for DurableLocalObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let res = self.inner.put_opts(location, payload, opts).await?;
        self.fsync_target(location).await?;
        Ok(res)
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(DurableMultipartUpload {
            inner,
            target: self.fs_path_for(location),
        }))
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> OsResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &ObjectPath) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy(from, to).await?;
        self.fsync_target(to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.fsync_target(to).await
    }
}

/// Builds the single durable `ObjectStore` backend.
///
/// Honors `PULSE_OBJECT_STORE_URL` (`s3://bucket`, `memory://`, or `file:///path`).
/// Defaults to fsync-enforced `DurableLocalObjectStore` at `<data_dir>/object_store` so local/CI runs
/// require zero AWS credentials while maintaining RPO = 0 durability.
pub fn build_object_store(data_dir: &Path) -> Result<Arc<dyn ObjectStore>> {
    if let Ok(raw_url) = std::env::var("PULSE_OBJECT_STORE_URL") {
        let trimmed = raw_url.trim();
        if !trimmed.is_empty() {
            return build_from_url(trimmed);
        }
    }

    let root = data_dir.join("object_store");
    Ok(Arc::new(DurableLocalObjectStore::new_at(&root)?))
}

/// Key prefixes written on the acknowledge path: the event WAL and the agent trace WAL.
const WAL_PREFIXES: [&str; 5] = [
    "wal",
    "wal_corrupt",
    "wal_dead",
    "agent_wal",
    "agent_wal_corrupt",
];

/// Key prefixes for persistent hot SlateDB state (`identity/` and `profiles/`).
const HOT_PREFIXES: [&str; 2] = ["identity", "profiles"];

/// Union of all local hot-tier prefixes used for opposite-tier leftover detection.
const ALL_HOT_PREFIXES: [&str; 7] = [
    "wal",
    "wal_corrupt",
    "wal_dead",
    "agent_wal",
    "agent_wal_corrupt",
    "identity",
    "profiles",
];

const HOT_STORE_MODE_FILE: &str = "hot_store_mode";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotStoreMode {
    Local,
    Object,
}

impl HotStoreMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Object => "object",
        }
    }
}

fn parse_hot_store_env() -> Result<HotStoreMode> {
    match std::env::var("PULSE_HOT_STORE").as_deref().map(str::trim) {
        Ok("local") | Ok("") | Err(_) => Ok(HotStoreMode::Local),
        Ok("object") => Ok(HotStoreMode::Object),
        Ok(other) => Err(config_error(format!(
            "PULSE_HOT_STORE must be `local` or `object`, not `{other}`"
        ))),
    }
}

/// Returns `true` when `PULSE_HOT_STORE` selects local hot-tier storage (`local` or unset).
pub fn is_local_hot_store_mode() -> Result<bool> {
    Ok(parse_hot_store_env()? == HotStoreMode::Local)
}

async fn read_mode_marker(marker_path: &Path) -> Result<Option<HotStoreMode>> {
    let path = marker_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<Option<HotStoreMode>> {
        match std::fs::read_to_string(&path) {
            Ok(raw) => match raw.trim() {
                "local" => Ok(Some(HotStoreMode::Local)),
                "object" => Ok(Some(HotStoreMode::Object)),
                other => Err(config_error(format!(
                    "unrecognized hot-store mode marker in `{}`: `{other}`",
                    path.display()
                ))),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => Err(config_error(format!(
                "corrupt hot-store mode marker in `{}`: {e}",
                path.display()
            ))),
            Err(e) => Err(StorageError::Io(e)),
        }
    })
    .await
    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
}

async fn write_mode_marker(data_dir: &Path, marker_path: &Path, mode: HotStoreMode) -> Result<()> {
    let data_dir = data_dir.to_path_buf();
    let marker_path = marker_path.to_path_buf();
    let content = format!("{}\n", mode.as_str());
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::Write;
        std::fs::create_dir_all(&data_dir)?;
        let tmp_path = data_dir.join(format!(".hot_store_mode.tmp.{}", uuid::Uuid::new_v4()));
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)?;
            file.write_all(content.as_bytes())?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp_path, &marker_path)?;
        fsync_dir_sync(&data_dir)?;
        Ok(())
    })
    .await
    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
}

async fn init_local_hot_dirs(data_dir: &Path, wal_dir: &Path, hot_dir: &Path) -> Result<()> {
    let data_dir = data_dir.to_path_buf();
    let wal_dir = wal_dir.to_path_buf();
    let hot_dir = hot_dir.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(hot_dir.join("identity"))?;
        std::fs::create_dir_all(hot_dir.join("profiles"))?;
        fsync_dir_sync(&hot_dir)?;
        fsync_dir_sync(&data_dir)?;
        Ok(())
    })
    .await
    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
}

async fn first_object_under(
    store: &Arc<dyn ObjectStore>,
    prefixes: &[&str],
) -> Result<Option<(&'static str, ObjectPath)>> {
    // Only static prefixes are passed in; match directly.
    for prefix in prefixes {
        let mut objects = store.list(Some(&ObjectPath::from(*prefix)));
        if let Some(found) = futures::StreamExt::next(&mut objects).await {
            let meta = found?;
            let matched = match *prefix {
                "wal" => "wal",
                "wal_corrupt" => "wal_corrupt",
                "wal_dead" => "wal_dead",
                "agent_wal" => "agent_wal",
                "agent_wal_corrupt" => "agent_wal_corrupt",
                "identity" => "identity",
                "profiles" => "profiles",
                "manifests" => "manifests",
                "segments" => "segments",
                "published" => "published",
                _ => "other",
            };
            return Ok(Some((matched, meta.location)));
        }
    }
    Ok(None)
}

async fn refuse_opposite_tier_objects(
    store: &Arc<dyn ObjectStore>,
    place: &str,
    was: &str,
    now: &str,
) -> Result<()> {
    if let Some((prefix, loc)) = first_object_under(store, &ALL_HOT_PREFIXES).await? {
        if matches!(prefix, "wal" | "agent_wal") {
            return Err(config_error(format!(
                "{place} still holds unflushed WAL ({loc}); written with PULSE_HOT_STORE={was}, \
                 cannot start with PULSE_HOT_STORE={now}"
            )));
        }
        return Err(config_error(format!(
            "{place} still holds {was}-mode hot state ({loc}); cannot start with PULSE_HOT_STORE={now}"
        )));
    }
    Ok(())
}

/// P0-7: `PULSE_HOT_STORE=local` (default) keeps the WALs at `<data_dir>/wal` and the hot
/// SlateDB stores (`identity/`, `profiles/`) at `<data_dir>/hot`, fsync'd like
/// `DurableLocalObjectStore`, so acknowledging a request makes no segment-store call.
/// `object` keeps them in `segments`. A persistent `<data_dir>/hot_store_mode` marker prevents
/// silent mode switches or starting after losing local `identity/` / `profiles/`.
pub async fn with_hot_store(
    segments: Arc<dyn ObjectStore>,
    data_dir: &Path,
) -> Result<Arc<dyn ObjectStore>> {
    let desired = parse_hot_store_env()?;
    let wal_dir = data_dir.join("wal");
    let hot_dir = data_dir.join("hot");
    let marker_path = data_dir.join(HOT_STORE_MODE_FILE);

    let marker = read_mode_marker(&marker_path).await?;
    if let Some(existing) = marker {
        if existing != desired {
            return Err(config_error(format!(
                "hot-store mode mismatch: data directory `{}` is initialized for \
                 PULSE_HOT_STORE={}, cannot start with PULSE_HOT_STORE={}",
                data_dir.display(),
                existing.as_str(),
                desired.as_str()
            )));
        }
    }

    match desired {
        HotStoreMode::Object => {
            if wal_dir.exists() {
                let local_wal: Arc<dyn ObjectStore> =
                    Arc::new(DurableLocalObjectStore::new_at(&wal_dir)?);
                refuse_opposite_tier_objects(&local_wal, "the local disk", "local", "object")
                    .await?;
            }
            if hot_dir.exists() {
                let local_hot: Arc<dyn ObjectStore> =
                    Arc::new(DurableLocalObjectStore::new_at(&hot_dir)?);
                refuse_opposite_tier_objects(&local_hot, "the local disk", "local", "object")
                    .await?;
            }
            if marker.is_none() {
                write_mode_marker(data_dir, &marker_path, HotStoreMode::Object).await?;
            }
            Ok(segments)
        }
        HotStoreMode::Local => {
            refuse_opposite_tier_objects(&segments, "the object store", "object", "local").await?;

            if marker == Some(HotStoreMode::Local) {
                let id_dir = hot_dir.join("identity");
                let prof_dir = hot_dir.join("profiles");
                if !id_dir.is_dir() {
                    return Err(config_error(format!(
                        "hot-store mode is `local` but local identity store `{}` is missing; \
                         refusing to start to prevent silent UID reuse \
                         (recover with `pulse-node restore --from-published`)",
                        id_dir.display()
                    )));
                }
                if !prof_dir.is_dir() {
                    return Err(config_error(format!(
                        "hot-store mode is `local` but local profiles store `{}` is missing; \
                         refusing to start (recover with `pulse-node restore --from-published`)",
                        prof_dir.display()
                    )));
                }
                let wal_store: Arc<dyn ObjectStore> =
                    Arc::new(DurableLocalObjectStore::new_at(&wal_dir)?);
                let hot_store: Arc<dyn ObjectStore> =
                    Arc::new(DurableLocalObjectStore::new_at(&hot_dir)?);
                if first_object_under(&hot_store, &["identity"])
                    .await?
                    .is_none()
                {
                    let has_segments =
                        first_object_under(&segments, &["manifests", "segments", "published"])
                            .await?
                            .is_some();
                    let has_wal = first_object_under(&wal_store, &["wal"]).await?.is_some();
                    let has_profiles = first_object_under(&hot_store, &["profiles"])
                        .await?
                        .is_some();
                    if has_segments || has_wal || has_profiles {
                        return Err(config_error(format!(
                            "hot-store mode is `local` and local identity store `{}` is empty \
                             while existing project state was found; refusing to start to prevent \
                             silent UID reuse (recover with `pulse-node restore --from-published`)",
                            id_dir.display()
                        )));
                    }
                }
                Ok(Arc::new(HotRouted {
                    wal: wal_store,
                    hot: hot_store,
                    segments,
                }))
            } else {
                if let Some((_, loc)) = first_object_under(&segments, &["published"]).await? {
                    return Err(config_error(format!(
                        "hot-store mode is `local` and data directory `{}` is uninitialized \
                         (missing `{}`), while published state exists in the object store ({loc}); \
                         refusing to start to prevent silent UID reuse \
                         (recover with `pulse-node restore --from-published`)",
                        data_dir.display(),
                        HOT_STORE_MODE_FILE,
                    )));
                }
                init_local_hot_dirs(data_dir, &wal_dir, &hot_dir).await?;
                write_mode_marker(data_dir, &marker_path, HotStoreMode::Local).await?;
                let wal_store: Arc<dyn ObjectStore> =
                    Arc::new(DurableLocalObjectStore::new_at(&wal_dir)?);
                let hot_store: Arc<dyn ObjectStore> =
                    Arc::new(DurableLocalObjectStore::new_at(&hot_dir)?);
                Ok(Arc::new(HotRouted {
                    wal: wal_store,
                    hot: hot_store,
                    segments,
                }))
            }
        }
    }
}

/// Opens a `local`-mode [`HotRouted`] store for `pulse-node restore --from-published` without
/// triggering the uninitialized-`<data_dir>` `published/` refusal, and without writing the
/// `<data_dir>/hot_store_mode` marker until restore completes (see [`finalize_local_hot_store_marker`]).
pub async fn open_hot_store_for_restore(
    segments: Arc<dyn ObjectStore>,
    data_dir: &Path,
) -> Result<Arc<dyn ObjectStore>> {
    let desired = parse_hot_store_env()?;
    if desired != HotStoreMode::Local {
        return Err(config_error(
            "restore --from-published requires PULSE_HOT_STORE=local (or unset)".to_string(),
        ));
    }
    let marker_path = data_dir.join(HOT_STORE_MODE_FILE);
    if let Some(HotStoreMode::Object) = read_mode_marker(&marker_path).await? {
        return Err(config_error(format!(
            "hot-store mode mismatch: data directory `{}` is initialized for PULSE_HOT_STORE=object",
            data_dir.display()
        )));
    }
    refuse_opposite_tier_objects(&segments, "the object store", "object", "local").await?;

    let wal_dir = data_dir.join("wal");
    let hot_dir = data_dir.join("hot");
    init_local_hot_dirs(data_dir, &wal_dir, &hot_dir).await?;
    let wal_store: Arc<dyn ObjectStore> = Arc::new(DurableLocalObjectStore::new_at(&wal_dir)?);
    let hot_store: Arc<dyn ObjectStore> = Arc::new(DurableLocalObjectStore::new_at(&hot_dir)?);
    Ok(Arc::new(HotRouted {
        wal: wal_store,
        hot: hot_store,
        segments,
    }))
}

/// Writes `<data_dir>/hot_store_mode = "local\n"` after `restore --from-published` completes.
pub async fn finalize_local_hot_store_marker(data_dir: &Path) -> Result<()> {
    let desired = parse_hot_store_env()?;
    if desired != HotStoreMode::Local {
        return Err(config_error(
            "finalize_local_hot_store_marker requires PULSE_HOT_STORE=local".to_string(),
        ));
    }
    let marker_path = data_dir.join(HOT_STORE_MODE_FILE);
    write_mode_marker(data_dir, &marker_path, HotStoreMode::Local).await
}

/// A startup refusal or bad setting: not data corruption.
fn config_error(msg: String) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))
}

/// Sends WAL prefixes to `wal`, persistent hot SlateDB prefixes (`identity/`, `profiles/`) to
/// `hot`, and every other key to `segments`. Sits below encryption, so local hot tiers are
/// encrypted like everything else.
#[derive(Debug)]
struct HotRouted {
    wal: Arc<dyn ObjectStore>,
    hot: Arc<dyn ObjectStore>,
    segments: Arc<dyn ObjectStore>,
}

impl HotRouted {
    fn route(&self, p: Option<&ObjectPath>) -> &Arc<dyn ObjectStore> {
        match p.and_then(|p| p.parts().next()) {
            Some(first) if WAL_PREFIXES.contains(&first.as_ref()) => &self.wal,
            Some(first) if HOT_PREFIXES.contains(&first.as_ref()) => &self.hot,
            _ => &self.segments,
        }
    }
}

impl Display for HotRouted {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HotRouted(wal={}, hot={}, segments={})",
            self.wal, self.hot, self.segments
        )
    }
}

#[async_trait]
impl ObjectStore for HotRouted {
    async fn put_opts(&self, l: &ObjectPath, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
        self.route(Some(l)).put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.route(Some(l)).put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
        self.route(Some(l)).get_opts(l, o).await
    }
    async fn get_range(&self, l: &ObjectPath, r: Range<u64>) -> OsResult<Bytes> {
        self.route(Some(l)).get_range(l, r).await
    }
    async fn get_ranges(&self, l: &ObjectPath, rs: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        self.route(Some(l)).get_ranges(l, rs).await
    }
    async fn head(&self, l: &ObjectPath) -> OsResult<ObjectMeta> {
        self.route(Some(l)).head(l).await
    }
    async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
        self.route(Some(l)).delete(l).await
    }
    fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let p = p.filter(|p| p.parts().next().is_some());
        match p {
            Some(_) => self.route(p).list(p),
            None => Box::pin(futures::StreamExt::chain(
                self.segments.list(None),
                futures::StreamExt::chain(self.wal.list(None), self.hot.list(None)),
            )),
        }
    }
    async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
        let p = p.filter(|p| p.parts().next().is_some());
        if p.is_some() {
            return self.route(p).list_with_delimiter(p).await;
        }
        let mut out = self.segments.list_with_delimiter(None).await?;
        let wal = self.wal.list_with_delimiter(None).await?;
        let hot = self.hot.list_with_delimiter(None).await?;
        out.common_prefixes.extend(wal.common_prefixes);
        out.common_prefixes.extend(hot.common_prefixes);
        out.common_prefixes.sort();
        out.common_prefixes.dedup();
        out.objects.extend(wal.objects);
        out.objects.extend(hot.objects);
        Ok(out)
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        let src = self.route(Some(from));
        let dst = self.route(Some(to));
        if Arc::ptr_eq(src, dst) {
            dst.copy(from, to).await
        } else {
            let data = src.get(from).await?.bytes().await?;
            dst.put_opts(
                to,
                PutPayload::from_bytes(data),
                PutOptions::from(object_store::PutMode::Overwrite),
            )
            .await?;
            Ok(())
        }
    }
    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        let src = self.route(Some(from));
        let dst = self.route(Some(to));
        if Arc::ptr_eq(src, dst) {
            dst.copy_if_not_exists(from, to).await
        } else {
            let data = src.get(from).await?.bytes().await?;
            dst.put_opts(
                to,
                PutPayload::from_bytes(data),
                PutOptions::from(object_store::PutMode::Create),
            )
            .await?;
            Ok(())
        }
    }
}

pub fn build_from_url(raw_url: &str) -> Result<Arc<dyn ObjectStore>> {
    if raw_url.starts_with("memory://") {
        return Ok(Arc::new(InMemory::new()));
    }
    let parsed = Url::parse(raw_url)
        .map_err(|e| StorageError::Corrupted(format!("invalid PULSE_OBJECT_STORE_URL: {e}")))?;

    match parsed.scheme() {
        "s3" => {
            let bucket = parsed.host_str().ok_or_else(|| {
                StorageError::Corrupted("s3:// URL missing bucket host".to_string())
            })?;
            let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "ap-south-1".to_string());
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region(region);
            if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
                if !endpoint.is_empty() {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_allow_http(true)
                        .with_virtual_hosted_style_request(false);
                }
            }
            Ok(Arc::new(builder.build()?))
        }
        "file" => {
            let path = parsed
                .to_file_path()
                .map_err(|_| StorageError::Corrupted("invalid file:// URL path".to_string()))?;
            Ok(Arc::new(DurableLocalObjectStore::new_at(&path)?))
        }
        other => Err(StorageError::Corrupted(format!(
            "unsupported object store scheme: {other}"
        ))),
    }
}

fn named_memory_stores() -> &'static DashMap<String, Arc<InMemory>> {
    static STORES: OnceLock<DashMap<String, Arc<InMemory>>> = OnceLock::new();
    STORES.get_or_init(DashMap::new)
}

#[inline]
pub fn allow_private_endpoints() -> bool {
    std::env::var("PULSE_ALLOW_PRIVATE_ENDPOINTS").as_deref() == Ok("1")
}

/// True for any address an outbound customer-supplied URL must not reach: private, loopback,
/// link-local/metadata, CGNAT (incl. 100.100.100.200), benchmark, reserved, and IPv6 forms that
/// embed an IPv4 address (mapped `::ffff:a.b.c.d`, compatible `::a.b.c.d`, NAT64 `64:ff9b::/96`).
pub fn is_forbidden_ssrf_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_multicast()
                || a == 0 // 0.0.0.0/8
                || (a == 100 && (b & 0xc0) == 64) // 100.64.0.0/10 CGNAT
                || (a == 192 && b == 0 && c == 0) // 192.0.0.0/24
                || (a == 198 && (b & 0xfe) == 18) // 198.18.0.0/15
                || a >= 240 // 240.0.0.0/4 reserved
        }
        IpAddr::V6(v6) => {
            let seg = v6.segments();
            if let Some(v4) = v6.to_ipv4() {
                return is_forbidden_ssrf_ip(IpAddr::V4(v4));
            }
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg[0] & 0xfe00) == 0xfc00
                || (seg[0] & 0xffc0) == 0xfe80
                || (seg[0] == 0x64 && seg[1] == 0xff9b) // NAT64
        }
    }
}

/// Validates an outbound customer-supplied HTTP(S) URL against SSRF (`M1`), rejecting private,
/// loopback, and link-local cloud metadata targets unless `PULSE_ALLOW_PRIVATE_ENDPOINTS=1` is set.
pub fn validate_outbound_url_ssrf(raw_url: &str) -> Result<Url> {
    let trimmed = raw_url.trim();
    let parsed = Url::parse(trimmed)
        .map_err(|e| StorageError::Corrupted(format!("invalid outbound URL: {e}")))?;
    let allow_priv = allow_private_endpoints();
    match parsed.scheme() {
        "https" => {}
        "http" if allow_priv => {}
        "http" => {
            return Err(StorageError::Corrupted(
                "plaintext http:// endpoints are blocked by SSRF policy (https:// required)".into(),
            ))
        }
        other => {
            return Err(StorageError::Corrupted(format!(
                "unsupported URL scheme '{other}' (expected https://)"
            )))
        }
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| StorageError::Corrupted("URL is missing host".to_string()))?;
    let host_lower = host.trim_matches(['[', ']']).to_ascii_lowercase();
    if !allow_priv
        && (host_lower == "localhost"
            || host_lower.ends_with(".localhost")
            || host_lower == "metadata.google.internal")
    {
        return Err(StorageError::Corrupted(format!(
            "outbound host '{host}' is blocked by SSRF policy"
        )));
    }
    if let Ok(ip) = host_lower.parse::<IpAddr>() {
        if !allow_priv && is_forbidden_ssrf_ip(ip) {
            return Err(StorageError::Corrupted(format!(
                "outbound IP '{ip}' is blocked by SSRF policy"
            )));
        }
        return Ok(parsed);
    }

    let port = parsed.port_or_known_default().unwrap_or(443);
    if !allow_priv {
        // Fail closed: a name we can't resolve now is not "safe" (the client would resolve later).
        let addrs = (host, port).to_socket_addrs().map_err(|e| {
            StorageError::Corrupted(format!("outbound host '{host}' could not be resolved: {e}"))
        })?;
        check_resolved_addrs(host, addrs)?;
    }
    Ok(parsed)
}

/// Rejects the lookup if it is empty or any address is forbidden; returns the addresses so callers
/// can pin their HTTP client to exactly what was checked (no DNS rebinding between check and use).
pub fn check_resolved_addrs(
    host: &str,
    addrs: impl IntoIterator<Item = std::net::SocketAddr>,
) -> Result<Vec<std::net::SocketAddr>> {
    let addrs: Vec<_> = addrs.into_iter().collect();
    if addrs.is_empty() {
        return Err(StorageError::Corrupted(format!(
            "outbound host '{host}' resolved to no addresses"
        )));
    }
    if let Some(bad) = addrs.iter().find(|a| is_forbidden_ssrf_ip(a.ip())) {
        return Err(StorageError::Corrupted(format!(
            "outbound host '{host}' resolved to private/metadata IP {}",
            bad.ip()
        )));
    }
    Ok(addrs)
}

/// DNS resolver that re-checks every address at connect time, so a customer endpoint can't pass
/// validation and then rebind to an internal IP.
struct SsrfCheckingResolver;

impl reqwest::dns::Resolve for SsrfCheckingResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let addrs = check_resolved_addrs(&host, addrs)?;
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// object_store HTTP connector for customer stores: SSRF-checked DNS, no redirect following.
#[derive(Debug)]
struct SsrfGuardedConnector;

impl object_store::client::HttpConnector for SsrfGuardedConnector {
    fn connect(
        &self,
        _options: &object_store::ClientOptions,
    ) -> OsResult<object_store::client::HttpClient> {
        let allow_priv = allow_private_endpoints();
        // Mirrors object_store's defaults (30s/5s timeouts, compression off for Content-Length).
        // ponytail: per-store ClientOptions are ignored; nothing sets them for customer stores.
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .https_only(!allow_priv)
            // ponytail: no egress proxy support; the proxy host would be what gets resolved.
            .no_proxy();
        if !allow_priv {
            builder = builder.dns_resolver(Arc::new(SsrfCheckingResolver));
        }
        let client = builder.build().map_err(|e| object_store::Error::Generic {
            store: "S3",
            source: Box::new(e),
        })?;
        Ok(object_store::client::HttpClient::new(client))
    }
}

/// Builds a customer BYOC object store (S3-compatible, or named `memory://` in private test mode).
/// Explicitly refuses `file://` URLs and enforces SSRF validation on custom endpoints (`M1`).
pub fn build_customer_object_store(
    raw_url: &str,
    access_key: Option<&str>,
    secret_key: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
) -> Result<Arc<dyn ObjectStore>> {
    let trimmed = raw_url.trim();
    if trimmed.is_empty() {
        return Err(StorageError::Corrupted(
            "storage URL cannot be empty".to_string(),
        ));
    }
    let parsed = Url::parse(trimmed)
        .map_err(|e| StorageError::Corrupted(format!("invalid storage URL: {e}")))?;

    match parsed.scheme() {
        "s3" => {
            let bucket = parsed.host_str().ok_or_else(|| {
                StorageError::Corrupted("s3:// URL missing bucket host".to_string())
            })?;
            let region = region
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("ap-south-1");
            // The region is spliced into `https://s3.{region}.amazonaws.com`; anything beyond
            // [a-z0-9-] (e.g. `x@10.0.0.5:8443/`) would send signed requests to another host.
            if region.len() > 32
                || !region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                return Err(StorageError::Corrupted(format!(
                    "invalid S3 region '{region}'"
                )));
            }
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region)
                .with_http_connector(SsrfGuardedConnector);
            if let Some(ak) = access_key.filter(|s| !s.trim().is_empty()) {
                builder = builder.with_access_key_id(ak);
            }
            if let Some(sk) = secret_key.filter(|s| !s.trim().is_empty()) {
                builder = builder.with_secret_access_key(sk);
            }
            if let Some(ep) = endpoint.filter(|s| !s.trim().is_empty()) {
                let checked = validate_outbound_url_ssrf(ep)?;
                builder = builder
                    .with_endpoint(checked.as_str())
                    .with_virtual_hosted_style_request(false);
                if allow_private_endpoints() && checked.scheme() == "http" {
                    builder = builder.with_allow_http(true);
                }
            }
            Ok(Arc::new(builder.build()?))
        }
        "memory" if allow_private_endpoints() => {
            let key = trimmed.to_string();
            let store = named_memory_stores()
                .entry(key)
                .or_insert_with(|| Arc::new(InMemory::new()))
                .clone();
            Ok(store as Arc<dyn ObjectStore>)
        }
        "file" => Err(StorageError::Corrupted(
            "file:// storage URLs are disallowed by SSRF / local file probe guard".to_string(),
        )),
        other => Err(StorageError::Corrupted(format!(
            "unsupported customer BYOC storage scheme '{other}' (expected s3://)"
        ))),
    }
}

/// Validates a customer-supplied BYOC S3 object store URL using non-destructive `list_with_delimiter`.
pub async fn validate_object_store_connection(
    raw_url: &str,
    access_key: Option<&str>,
    secret_key: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
) -> Result<usize> {
    let store = build_customer_object_store(raw_url, access_key, secret_key, region, endpoint)?;
    let listed = store.list_with_delimiter(None).await?;
    Ok(listed.objects.len() + listed.common_prefixes.len())
}

#[cfg(test)]
mod ssrf_tests {
    use super::*;

    #[test]
    fn blocks_internal_addresses_including_embedded_ipv4() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "169.254.169.254",
            "100.100.100.200",
            "0.1.2.3",
            "198.18.0.1",
            "192.0.0.8",
            "240.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "::ffff:a9fe:a9fe",
            "::127.0.0.1",
            "64:ff9b::a9fe:a9fe",
            "fd00:ec2::254",
            "fe80::1",
        ] {
            assert!(
                is_forbidden_ssrf_ip(ip.parse().unwrap()),
                "{ip} should be blocked"
            );
        }
        for ip in ["8.8.8.8", "100.128.0.1", "2606:4700:4700::1111"] {
            assert!(
                !is_forbidden_ssrf_ip(ip.parse().unwrap()),
                "{ip} should be allowed"
            );
        }
        assert!(validate_outbound_url_ssrf("https://[::ffff:127.0.0.1]/x").is_err());
    }

    #[test]
    fn unresolvable_host_fails_closed() {
        assert!(validate_outbound_url_ssrf("https://nonexistent.invalid/hook").is_err());
        assert!(check_resolved_addrs("h", Vec::new()).is_err());
    }

    #[test]
    fn s3_region_must_be_plain() {
        let build = |r| build_customer_object_store("s3://bkt", None, None, Some(r), None);
        assert!(build("x@10.0.0.5:8443/").is_err());
        assert!(build("us-east-1.evil.com").is_err());
        assert!(build("eu-west-2").is_ok());
    }

    #[tokio::test]
    async fn s3_resolver_rejects_internal_hosts() {
        use reqwest::dns::Resolve;
        let name = |h: &str| h.parse::<reqwest::dns::Name>().unwrap();
        assert!(SsrfCheckingResolver
            .resolve(name("localhost"))
            .await
            .is_err());
        assert!(SsrfCheckingResolver
            .resolve(name("nonexistent.invalid"))
            .await
            .is_err());
    }
}

#[cfg(test)]
mod hot_store_and_multipart_tests {
    use super::*;
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct HotStoreEnvGuard {
        prev: Option<String>,
    }

    impl HotStoreEnvGuard {
        fn set(val: Option<&str>) -> Self {
            let prev = std::env::var("PULSE_HOT_STORE").ok();
            match val {
                Some(v) => std::env::set_var("PULSE_HOT_STORE", v),
                None => std::env::remove_var("PULSE_HOT_STORE"),
            }
            Self { prev }
        }
    }

    impl Drop for HotStoreEnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var("PULSE_HOT_STORE", v),
                None => std::env::remove_var("PULSE_HOT_STORE"),
            }
        }
    }

    #[derive(Debug, Default)]
    struct CountingStore {
        inner: InMemory,
        calls: AtomicUsize,
    }

    impl CountingStore {
        fn count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
        fn reset(&self) {
            self.calls.store(0, Ordering::SeqCst);
        }
    }

    impl Display for CountingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            l: &ObjectPath,
            p: PutPayload,
            o: PutOptions,
        ) -> OsResult<PutResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjectPath,
            o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(l, o).await
        }
        async fn get_range(&self, l: &ObjectPath, r: Range<u64>) -> OsResult<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(l, r).await
        }
        async fn get_ranges(&self, l: &ObjectPath, rs: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_ranges(l, rs).await
        }
        async fn head(&self, l: &ObjectPath) -> OsResult<ObjectMeta> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.head(l).await
        }
        async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(l).await
        }
        fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn count_files(dir: &Path) -> usize {
        let mut count = 0;
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                count += count_files(&p);
            } else {
                count += 1;
            }
        }
        count
    }

    #[tokio::test]
    async fn multipart_complete_fsyncs_file_and_parent_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new_at(tmp.path()).unwrap();
        let loc = ObjectPath::from("identity/proj_1/compacted/01.sst");
        let target = store.fs_path_for(&loc);

        let mut upload = store
            .put_multipart_opts(&loc, PutMultipartOptions::default())
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part1-"))
            .await
            .unwrap();
        upload
            .put_part(PutPayload::from_static(b"part2"))
            .await
            .unwrap();
        assert_eq!(
            fsync_probe_map().lock().get(&target).copied(),
            None,
            "fsync must not run before complete()"
        );

        upload.complete().await.unwrap();
        assert_eq!(
            fsync_probe_map().lock().get(&target).copied(),
            Some((1, 1)),
            "complete() must fsync both target file and parent directory"
        );
        let got = store.get(&loc).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), b"part1-part2");
        assert_eq!(count_files(tmp.path()), 1, "staging temp files cleaned up");
    }

    #[tokio::test]
    async fn multipart_abort_and_drop_cleanup_without_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DurableLocalObjectStore::new_at(tmp.path()).unwrap();

        let loc_abort = ObjectPath::from("profiles/proj_1/wal/01.sst");
        let target_abort = store.fs_path_for(&loc_abort);
        let mut up1 = store
            .put_multipart_opts(&loc_abort, PutMultipartOptions::default())
            .await
            .unwrap();
        up1.put_part(PutPayload::from_static(b"abc")).await.unwrap();
        up1.abort().await.unwrap();
        assert!(up1.complete().await.is_err(), "complete after abort fails");
        assert!(!target_abort.exists());
        assert_eq!(fsync_probe_map().lock().get(&target_abort).copied(), None);

        let loc_drop = ObjectPath::from("profiles/proj_1/wal/02.sst");
        let target_drop = store.fs_path_for(&loc_drop);
        {
            let mut up2 = store
                .put_multipart_opts(&loc_drop, PutMultipartOptions::default())
                .await
                .unwrap();
            up2.put_part(PutPayload::from_static(b"xyz")).await.unwrap();
        }
        assert!(!target_drop.exists());
        assert_eq!(fsync_probe_map().lock().get(&target_drop).copied(), None);
        assert_eq!(
            count_files(tmp.path()),
            0,
            "staging files removed on abort and drop"
        );
    }

    #[tokio::test]
    async fn hot_routed_routes_prefixes_lists_copies_and_get_ranges() {
        let _lock = ENV_LOCK.lock().await;
        let _env = HotStoreEnvGuard::set(Some("local"));
        let tmp = tempfile::tempdir().unwrap();
        let segments = Arc::new(CountingStore::default());
        let store = with_hot_store(segments.clone(), tmp.path()).await.unwrap();
        segments.reset();

        // All WAL prefixes (including agent_wal_corrupt) and HOT prefixes route locally with 0 S3 calls.
        for key in [
            "wal/p1/bucket_000/seq_0.wal",
            "wal_corrupt/p1/bucket_000/bad.wal",
            "wal_dead/p1/bucket_000/dead.plsb",
            "agent_wal/p1/w1/seq_0.ptwl",
            "agent_wal_corrupt/p1/w1/seq_0.ptwl",
            "identity/p1/manifest/000001.manifest",
            "profiles/p1/wal/000001.sst",
        ] {
            let p = ObjectPath::from(key);
            store
                .put(&p, PutPayload::from_static(b"hello-world"))
                .await
                .unwrap();
            let got = store.get(&p).await.unwrap().bytes().await.unwrap();
            assert_eq!(got.as_ref(), b"hello-world");
        }
        assert_eq!(
            segments.count(),
            0,
            "local WAL and hot prefixes must make zero calls to segments"
        );

        // get_ranges delegates properly.
        let ranges = store
            .get_ranges(
                &ObjectPath::from("identity/p1/manifest/000001.manifest"),
                &[0..5, 6..11],
            )
            .await
            .unwrap();
        assert_eq!(
            ranges,
            vec![Bytes::from_static(b"hello"), Bytes::from_static(b"world")]
        );
        assert_eq!(segments.count(), 0);

        // Non-hot prefix goes to segments.
        let seg_key = ObjectPath::from("segments/p1/20261122/bucket_000/seg_1.pvtx");
        store
            .put(&seg_key, PutPayload::from_static(b"seg-bytes"))
            .await
            .unwrap();
        assert_eq!(segments.count(), 1);

        // list(None) and list(Some("")) normalize and include all 3 tiers.
        let all_none: Vec<_> = store.list(None).try_collect().await.unwrap();
        let all_empty: Vec<_> = store
            .list(Some(&ObjectPath::from("")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(all_none.len(), 8);
        assert_eq!(all_empty.len(), 8);

        // list_with_delimiter(None) and list_with_delimiter(Some("")) merge and dedup common_prefixes.
        let delim_none = store.list_with_delimiter(None).await.unwrap();
        let delim_empty = store
            .list_with_delimiter(Some(&ObjectPath::from("")))
            .await
            .unwrap();
        let expected_prefixes: Vec<ObjectPath> = [
            "agent_wal",
            "agent_wal_corrupt",
            "identity",
            "profiles",
            "segments",
            "wal",
            "wal_corrupt",
            "wal_dead",
        ]
        .into_iter()
        .map(ObjectPath::from)
        .collect();
        assert_eq!(delim_none.common_prefixes, expected_prefixes);
        assert_eq!(delim_empty.common_prefixes, expected_prefixes);

        // Same-tier copy (wal -> wal_corrupt) and cross-tier copy (hot -> segments, segments -> hot).
        store
            .copy(
                &ObjectPath::from("agent_wal/p1/w1/seq_0.ptwl"),
                &ObjectPath::from("agent_wal_corrupt/p1/w1/copied.ptwl"),
            )
            .await
            .unwrap();
        let cross_dst = ObjectPath::from("manifests/p1/copied_from_hot.bin");
        store
            .copy(&ObjectPath::from("profiles/p1/wal/000001.sst"), &cross_dst)
            .await
            .unwrap();
        assert_eq!(
            store
                .get(&cross_dst)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"hello-world"
        );
        let err = store
            .copy_if_not_exists(&ObjectPath::from("profiles/p1/wal/000001.sst"), &cross_dst)
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::AlreadyExists { .. }));
        let cross_hot_dst = ObjectPath::from("profiles/p1/restored.bin");
        store
            .copy_if_not_exists(&seg_key, &cross_hot_dst)
            .await
            .unwrap();
        assert_eq!(
            store
                .get(&cross_hot_dst)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"seg-bytes"
        );
    }

    #[tokio::test]
    async fn mode_marker_and_startup_refusals() {
        let _lock = ENV_LOCK.lock().await;

        // 1. Invalid PULSE_HOT_STORE value refused.
        {
            let _env = HotStoreEnvGuard::set(Some("bogus"));
            let tmp = tempfile::tempdir().unwrap();
            let err = with_hot_store(Arc::new(InMemory::new()), tmp.path())
                .await
                .unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
        }

        // 2. Clean local init creates dirs & marker; same-mode restart works; object switch refused.
        {
            let _env = HotStoreEnvGuard::set(Some("local"));
            let tmp = tempfile::tempdir().unwrap();
            let seg: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let _ = with_hot_store(seg.clone(), tmp.path()).await.unwrap();
            assert_eq!(
                std::fs::read_to_string(tmp.path().join("hot_store_mode")).unwrap(),
                "local\n"
            );
            assert!(tmp.path().join("wal").is_dir());
            assert!(tmp.path().join("hot/identity").is_dir());
            assert!(tmp.path().join("hot/profiles").is_dir());

            // Empty restart succeeds.
            let _ = with_hot_store(seg.clone(), tmp.path()).await.unwrap();

            // Switching to object refused.
            let _env_obj = HotStoreEnvGuard::set(Some("object"));
            let err = with_hot_store(seg.clone(), tmp.path()).await.unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
            assert!(err.to_string().contains("PULSE_HOT_STORE=local"), "{err}");
        }

        // 3. Clean object init writes marker; local switch refused.
        {
            let _env = HotStoreEnvGuard::set(Some("object"));
            let tmp = tempfile::tempdir().unwrap();
            let seg: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let _ = with_hot_store(seg.clone(), tmp.path()).await.unwrap();
            assert_eq!(
                std::fs::read_to_string(tmp.path().join("hot_store_mode")).unwrap(),
                "object\n"
            );
            let _ = with_hot_store(seg.clone(), tmp.path()).await.unwrap();

            let _env_loc = HotStoreEnvGuard::set(Some("local"));
            let err = with_hot_store(seg.clone(), tmp.path()).await.unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
            assert!(err.to_string().contains("PULSE_HOT_STORE=object"), "{err}");
        }

        // 4. Corrupt/unrecognized marker refused.
        {
            let _env = HotStoreEnvGuard::set(Some("local"));
            let tmp = tempfile::tempdir().unwrap();
            std::fs::write(tmp.path().join("hot_store_mode"), "invalid\n").unwrap();
            let err = with_hot_store(Arc::new(InMemory::new()), tmp.path())
                .await
                .unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );

            std::fs::write(tmp.path().join("hot_store_mode"), [0xff, 0xfe]).unwrap();
            let err = with_hot_store(Arc::new(InMemory::new()), tmp.path())
                .await
                .unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
        }

        // 5. Local marker with missing hot/identity, missing hot/profiles, or wiped identity/* with state.
        {
            let _env = HotStoreEnvGuard::set(Some("local"));
            let tmp = tempfile::tempdir().unwrap();
            let seg: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let _ = with_hot_store(seg.clone(), tmp.path()).await.unwrap();

            // Delete hot/identity dir -> refused.
            std::fs::remove_dir_all(tmp.path().join("hot/identity")).unwrap();
            let err = with_hot_store(seg.clone(), tmp.path()).await.unwrap_err();
            assert!(
                err.to_string().contains("restore --from-published"),
                "{err}"
            );

            // Restore identity dir, delete hot/profiles dir -> refused.
            std::fs::create_dir_all(tmp.path().join("hot/identity")).unwrap();
            std::fs::remove_dir_all(tmp.path().join("hot/profiles")).unwrap();
            let err = with_hot_store(seg.clone(), tmp.path()).await.unwrap_err();
            assert!(
                err.to_string().contains("restore --from-published"),
                "{err}"
            );

            // Restore profiles dir; agent_wal alone is allowed with empty identity.
            std::fs::create_dir_all(tmp.path().join("hot/profiles")).unwrap();
            let store = with_hot_store(seg.clone(), tmp.path()).await.unwrap();
            store
                .put(
                    &ObjectPath::from("agent_wal/p1/w1/seq_0.ptwl"),
                    PutPayload::from_static(b"a"),
                )
                .await
                .unwrap();
            let _ = with_hot_store(seg.clone(), tmp.path()).await.unwrap();

            // Put a manifest in segments while identity/ is empty -> refused!
            seg.put(
                &ObjectPath::from("manifests/p1/epoch_1.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
            let err = with_hot_store(seg.clone(), tmp.path()).await.unwrap_err();
            assert!(
                err.to_string().contains("restore --from-published"),
                "{err}"
            );
        }

        // 5b. Uninitialized / wiped <data_dir> (no marker) with S3 published/ -> refused,
        //     and leaves <data_dir> untouched (no marker or hot/ created).
        {
            let _env = HotStoreEnvGuard::set(Some("local"));
            let tmp = tempfile::tempdir().unwrap();
            let seg: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            seg.put(
                &ObjectPath::from("published/p1/pub_000000000000.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
            let err = with_hot_store(seg.clone(), tmp.path()).await.unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
            assert!(
                err.to_string().contains("restore --from-published"),
                "{err}"
            );
            assert!(!tmp.path().join("hot_store_mode").exists());
            assert!(!tmp.path().join("hot").exists());
            assert!(!tmp.path().join("wal").exists());

            // Restore helpers open without refusal and finalize marker afterwards.
            let restore_store = open_hot_store_for_restore(seg.clone(), tmp.path())
                .await
                .unwrap();
            restore_store
                .put(
                    &ObjectPath::from("identity/p1/manifest/00000000000000000001.manifest"),
                    PutPayload::from_static(b"m"),
                )
                .await
                .unwrap();
            finalize_local_hot_store_marker(tmp.path()).await.unwrap();
            let _ = with_hot_store(seg, tmp.path()).await.unwrap();
        }

        // 6. Opposite-tier leftover checks (every WAL/HOT prefix in segments refused by local;
        //    every WAL/HOT prefix in local wal/ or hot/ refused by object).
        for prefix in ALL_HOT_PREFIXES {
            let _env = HotStoreEnvGuard::set(Some("local"));
            let tmp = tempfile::tempdir().unwrap();
            let seg: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            seg.put(
                &ObjectPath::from(format!("{prefix}/p1/obj")),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
            let err = with_hot_store(seg, tmp.path()).await.unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
        }

        for prefix in ALL_HOT_PREFIXES {
            let _env = HotStoreEnvGuard::set(Some("object"));
            let tmp = tempfile::tempdir().unwrap();
            let dir = if WAL_PREFIXES.contains(&prefix) {
                tmp.path().join("wal")
            } else {
                tmp.path().join("hot")
            };
            let local = DurableLocalObjectStore::new_at(&dir).unwrap();
            local
                .put(
                    &ObjectPath::from(format!("{prefix}/p1/obj")),
                    PutPayload::from_static(b"x"),
                )
                .await
                .unwrap();
            let err = with_hot_store(Arc::new(InMemory::new()), tmp.path())
                .await
                .unwrap_err();
            assert!(
                matches!(err, StorageError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidInput)
            );
        }
    }
}
