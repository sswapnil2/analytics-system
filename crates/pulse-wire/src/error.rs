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

    #[error("json payload error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid event payload: {0}")]
    InvalidPayload(String),

    #[error("invalid event_uuid string: {0}")]
    InvalidUuid(String),

    #[error("wire frame resource limit exceeded: {0}")]
    ResourceLimitExceeded(&'static str),
}

pub type Result<T> = std::result::Result<T, WireError>;

/// Strips internal Rust struct/enum terminology from serde JSON parse errors (`EN-4`).
pub fn sanitize_json_error_message(raw: &str) -> String {
    let mut out = raw.to_string();
    while let Some(idx) = out.find("expected struct ") {
        let after = &out[idx + "expected struct ".len()..];
        let ident_len = after
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b':')
            .count();
        let end = idx + "expected struct ".len() + ident_len;
        out.replace_range(idx..end, "expected JSON object");
    }
    while let Some(idx) = out.find("expected unit variant ") {
        let after = &out[idx + "expected unit variant ".len()..];
        let ident_len = after
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b':')
            .count();
        let end = idx + "expected unit variant ".len() + ident_len;
        out.replace_range(idx..end, "expected valid option");
    }
    out = out.replace("expected a map", "expected JSON object");
    out.replace('`', "\"")
}
