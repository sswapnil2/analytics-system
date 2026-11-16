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

    #[error("project dictionary capacity exceeded for {kind} (max 65536)")]
    DictionaryFull { kind: &'static str },

    #[error("invalid storage identifier: {0}")]
    InvalidIdentifier(String),
}

impl From<vortex_error::VortexError> for StorageError {
    fn from(err: vortex_error::VortexError) -> Self {
        StorageError::Vortex(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
