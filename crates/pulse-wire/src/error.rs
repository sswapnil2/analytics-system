use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("invalid frame magic header: expected {expected:?}, got {actual:?}")]
    InvalidMagic { expected: [u8; 4], actual: [u8; 4] },

    #[error("unsupported wire protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("unsupported frame flags: 0x{0:02x}")]
    UnsupportedFlags(u8),

    #[error("unknown zstd dictionary id: {0}")]
    UnknownDictionary(u16),

    #[error("unexpected EOF or truncated binary frame ({0})")]
    TruncatedFrame(&'static str),

    #[error("invalid UTF-8 string in columnar string table: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("invalid property tag: {0}")]
    InvalidPropTag(u8),

    #[error("invalid replay opcode or enum tag: {0}")]
    InvalidReplayTag(u8),

    #[error("zstd compression/decompression error: {0}")]
    Zstd(#[from] std::io::Error),

    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid event_uuid string: {0}")]
    InvalidUuid(String),

    #[error("wire frame resource limit exceeded: {0}")]
    ResourceLimitExceeded(&'static str),
}

pub type Result<T> = std::result::Result<T, WireError>;
