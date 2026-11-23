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

    /// `what` is `"l0"` (rows in memory not yet in a segment), `"log"` (events written to the
    /// WAL and not yet applied), or `"ingest_buffer"` (queued events awaiting WAL write).
    #[error("{what} backpressure on project '{project_id}' ({bytes} >= {max_bytes} bytes)")]
    Backpressure {
        project_id: String,
        what: &'static str,
        bytes: usize,
        max_bytes: usize,
    },

    #[error("this node does not hold the writer lease for project '{project_id}'")]
    NotWriter { project_id: String },

    #[error("stale local state for project '{project_id}': published seq {pub_seq} (gen {pub_gen}) is ahead of local last_published_seq {local_last_pub_seq:?} (gen {local_gen}); recover with `pulse-node restore --from-published`")]
    StaleWriter {
        project_id: String,
        pub_seq: u64,
        pub_gen: u64,
        local_last_pub_seq: Option<u64>,
        local_gen: u64,
    },

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

    /// Local `hot/segments/` bytes awaiting upload to S3 reached 2x their configured limit.
    #[error("hot segment backpressure on project '{project_id}' ({bytes} >= {max_bytes} bytes)")]
    HotSegmentBackpressure {
        project_id: String,
        bytes: u64,
        max_bytes: u64,
    },

    #[error("storage unavailable: {0}")]
    Unavailable(String),
}

impl StorageError {
    /// Returns `true` when the error reflects I/O, object-store unavailability, writer fencing,
    /// or stale local state rather than bad input or data corruption (retryable -> HTTP 503).
    pub fn is_unavailable(&self) -> bool {
        match self {
            Self::Io(_)
            | Self::ObjectStore(_)
            | Self::NotWriter { .. }
            | Self::StaleWriter { .. }
            | Self::Unavailable(_) => true,
            Self::Identity(e) => e.is_store_unavailable(),
            _ => false,
        }
    }
}

impl From<vortex_error::VortexError> for StorageError {
    fn from(err: vortex_error::VortexError) -> Self {
        StorageError::Vortex(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
