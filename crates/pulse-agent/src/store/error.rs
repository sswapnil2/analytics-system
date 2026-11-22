use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage error: {0}")]
    Storage(#[from] pulse_storage::StorageError),
    #[error("cache error: {0}")]
    Cache(String),
    #[error("{context} CRC32 mismatch: expected 0x{expected:08x}, got 0x{actual:08x}")]
    ChecksumMismatch {
        context: &'static str,
        expected: u32,
        actual: u32,
    },
    #[error("corrupted agent store artifact: {0}")]
    Corrupted(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(&'static str),
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error("manifest CAS conflict after retries")]
    ManifestConflict,
    #[error("WAL group-commit backpressure on project '{project_id}' (queued={queued})")]
    WalBackpressure { project_id: String, queued: usize },
    #[error("agent memtable over its byte limit on project '{project_id}' ({bytes} >= {max_bytes} bytes)")]
    L0Backpressure {
        project_id: String,
        bytes: usize,
        max_bytes: usize,
    },
    #[error("agent trace store is shutting down")]
    Shutdown,
}

pub type StoreResult<T> = Result<T, StoreError>;

impl StoreError {
    pub fn is_corrupt_or_checksum(&self) -> bool {
        matches!(
            self,
            Self::Corrupted(_)
                | Self::ChecksumMismatch { .. }
                | Self::Protocol(_)
                | Self::ResourceLimitExceeded(_)
        )
    }
}

/// Validates path-segment identifiers (`project_id`, `writer_id`, `segment_id`) to prevent
/// path traversal and invalid `ObjectPath` segments.
pub(crate) fn validate_identifier(field: &'static str, val: &str) -> StoreResult<()> {
    if val.is_empty() || val.len() > 128 {
        return Err(StoreError::InvalidIdentifier(format!(
            "{field} length must be 1..=128 bytes"
        )));
    }
    if val == "." || val == ".." || val.contains('/') || val.contains('\\') {
        return Err(StoreError::InvalidIdentifier(format!(
            "{field} contains forbidden path characters"
        )));
    }
    if !val
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
    {
        return Err(StoreError::InvalidIdentifier(format!(
            "{field} must contain only [A-Za-z0-9_.-]"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_identifier_rejects_traversal_and_unsafe_chars() {
        assert!(validate_identifier("project_id", "proj_1-abc.def").is_ok());
        assert!(validate_identifier("project_id", "").is_err());
        assert!(validate_identifier("project_id", ".").is_err());
        assert!(validate_identifier("project_id", "..").is_err());
        assert!(validate_identifier("project_id", "a/b").is_err());
        assert!(validate_identifier("project_id", "a\\b").is_err());
        assert!(validate_identifier("project_id", "bad space").is_err());
        let long = "a".repeat(129);
        assert!(validate_identifier("project_id", &long).is_err());
    }
}
