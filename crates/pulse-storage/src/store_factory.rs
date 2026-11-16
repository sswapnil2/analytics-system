use std::path::Path;
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use url::Url;

use crate::error::{Result, StorageError};

/// Builds the single durable `ObjectStore` backend.
///
/// Honors `PULSE_OBJECT_STORE_URL` (`s3://bucket`, `memory://`, or `file:///path`).
/// Defaults to local filesystem at `<data_dir>/object_store` so local/CI runs require zero AWS credentials,
/// while production `s3://...` activates AWS S3 (`ap-south-1` default) or MinIO automatically.
pub fn build_object_store(data_dir: &Path) -> Result<Arc<dyn ObjectStore>> {
    if let Ok(raw_url) = std::env::var("PULSE_OBJECT_STORE_URL") {
        let trimmed = raw_url.trim();
        if !trimmed.is_empty() {
            return build_from_url(trimmed);
        }
    }

    let root = data_dir.join("object_store");
    std::fs::create_dir_all(&root)?;
    let fs = LocalFileSystem::new_with_prefix(root)?;
    Ok(Arc::new(fs))
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
            std::fs::create_dir_all(&path)?;
            Ok(Arc::new(LocalFileSystem::new_with_prefix(path)?))
        }
        other => Err(StorageError::Corrupted(format!(
            "unsupported object store scheme: {other}"
        ))),
    }
}
