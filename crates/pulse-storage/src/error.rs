use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("wire error: {0}")]
    Wire(#[from] pulse_wire::WireError),

    #[error("identity error: {0}")]
    Identity(#[from] pulse_identity::IdentityError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("vortex error: {0}")]
    Vortex(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("wal crc32 mismatch: expected {expected:#x}, got {actual:#x}")]
    WalChecksumMismatch { expected: u32, actual: u32 },

    #[error("corrupted segment or wal file: {0}")]
    Corrupted(String),

    #[error("mvcc manifest conflict after retries")]
    ManifestConflict,

    #[error("identity high-water mark violation for project '{project_id}': identity user count {identity_user_count} < manifest high_water_mark {high_water_mark}")]
    HighWaterMarkViolation {
        project_id: String,
        identity_user_count: u64,
        high_water_mark: u64,
    },

    /// Rows held in memory and not yet in a segment reached their byte limit.
    #[error("l0 backpressure on project '{project_id}' ({bytes} >= {max_bytes} bytes)")]
    Backpressure {
        project_id: String,
        bytes: usize,
        max_bytes: usize,
    },

    #[error("this node does not hold the writer lease for project '{project_id}'")]
    NotWriter { project_id: String },

    #[error("project dictionary capacity exceeded for {kind} (max 65536)")]
    DictionaryFull { kind: &'static str },

    #[error("invalid storage identifier: {0}")]
    InvalidIdentifier(String),

    #[error("L0 memory backpressure on project '{project_id}' bucket {bucket_id} ({current_rows} >= {max_rows} rows)")]
    L0Backpressure {
        project_id: String,
        bucket_id: u8,
        current_rows: usize,
        max_rows: usize,
    },
}

impl From<vortex_error::VortexError> for StorageError {
    fn from(err: vortex_error::VortexError) -> Self {
        StorageError::Vortex(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
