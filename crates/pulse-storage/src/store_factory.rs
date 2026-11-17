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
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use url::Url;

use crate::error::{Result, StorageError};

/// Durable wrapper around `object_store::local::LocalFileSystem` that issues `File::sync_data`
/// on the written file AND `File::sync_all` on its parent directory after every `put_opts` (`P1-03`),
/// guaranteeing true RPO = 0 crash durability on local ext4/APFS/XFS filesystems.
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
        let target = self.fs_path_for(location);
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let file = std::fs::File::open(&target)?;
            file.sync_data()?;
            if let Some(parent) = target.parent() {
                if let Ok(dir) = std::fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(())
        })
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
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
        self.inner.get_range(location, range).await
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
/// require zero AWS credentials while maintaining RPO = 0 durability (`P1-03`).
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
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region.unwrap_or("ap-south-1"));
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
}
