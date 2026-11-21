//! Custom `PLST` v1 (`application/x-pulse-trace`) columnar wire protocol for AI Agent traces & spans.
//!
//! Layout:
//! - 20-byte little-endian header:
//!   `magic[4] ("PLST") | ver(u8=1) | flags(u8) | dict_id(u16) | uncomp_len(u32) | comp_len(u32) | compressed_crc32(u32)`
//! - Bounded Zstd or uncompressed body containing 5 columnar sections:
//!   1. String Table (`u32::MAX` = `None`, `0..N` = `Some`)
//!   2. Content-Addressed Blob Table (`xxh3_64` + length + exact byte equality dedup)
//!   3. Trace Header / Delta Rows
//!   4. Span Rows (with `span_phase`, `linked_span_ids`, `cost_nanos`, `SpanType::Guardrail`, orphan/streaming support)
//!   5. Columnar Span Events

use std::collections::HashMap;
use std::io::{Read, Write};

use serde_json::Value;
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

use crate::model::{
    AgentSpan, AgentTrace, FailureSource, SpanEvent, SpanPhase, SpanType, TraceIngestBatch,
    TraceStatus,
};
use crate::pricing::{cost_nanos_to_usd, usd_to_cost_nanos};

pub const PLST_MAGIC: [u8; 4] = *b"PLST";
pub const PLST_VERSION: u8 = 1;
pub const AGENT_DICT_ID: u16 = 2;
pub const MAX_PLST_UNCOMP_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// Wire body is raw Zstd (without dictionary). Mutually exclusive with `FLAG_DICT_ZSTD`.
pub const FLAG_RAW_ZSTD: u8 = 0x01;
/// Wire body is Zstd compressed with `AGENT_DICT_ID = 2` (`agent_zstd_dict()`).
pub const FLAG_DICT_ZSTD: u8 = 0x02;
/// Frame contains at least one active/partial trace or non-final (`Start`/`Update`) span.
pub const FLAG_PARTIAL: u8 = 0x04;
/// Frame contains at least one terminal trace or `Final` span.
pub const FLAG_FINAL: u8 = 0x08;
const KNOWN_FLAGS_MASK: u8 = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD | FLAG_PARTIAL | FLAG_FINAL;

pub const PLST_HEADER_LEN: usize = 20;
pub const MAX_PLST_TRACES: usize = 100_000;
pub const MAX_PLST_SPANS: usize = 500_000;
pub const MAX_PLST_EVENTS: usize = 2_000_000;
pub const MAX_PLST_STRINGS: usize = 2_000_000;
pub const MAX_PLST_BLOBS: usize = 500_000;
pub const MAX_PLST_SINGLE_STRING_BYTES: usize = 1024 * 1024; // 1 MiB
pub const MAX_PLST_MATERIALIZED_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
const MAX_INITIAL_VEC_CAP: usize = 65_536;
const NONE_IDX: u32 = u32::MAX;

const BLOB_ENC_JSON_UTF8: u8 = 0;
const BLOB_ENC_UTF8_TEXT: u8 = 1;

/// Pre-shared raw-content AI Agent Zstd dictionary (`AGENT_DICT_ID = 2`).
/// Compatible with Rust `zstd` (`Encoder::with_dictionary` / `Decoder::with_dictionary`),
/// Node `zlib` Zstd dictionaries, and Python `zstandard` / `compression.zstd` raw dicts.
const AGENT_ZSTD_DICT: &[u8] = concat!(
    r#"{"messages":[{"role":"system","content":"#,
    "\0",
    r#"{"role":"user","content":"#,
    "\0",
    r#"{"role":"assistant","content":"#,
    "\0",
    r#"{"role":"tool","tool_call_id":"#,
    "\0",
    r#""tool_calls":[{"id":"#,
    "\0",
    r#""type":"function","function":{"name":"#,
    "\0",
    r#""arguments":"#,
    "\0",
    r#""choices":[{"index":0,"message":"#,
    "\0",
    r#""finish_reason":"stop""#,
    "\0",
    r#""finish_reason":"tool_calls""#,
    "\0",
    r#""usage":{"prompt_tokens":"#,
    "\0",
    r#""completion_tokens":"#,
    "\0",
    r#""total_tokens":"#,
    "\0",
    "gen_ai.system\0gen_ai.request.model\0gen_ai.response.model\0",
    "gen_ai.usage.input_tokens\0gen_ai.usage.output_tokens\0",
    "gen_ai.operation.name\0gen_ai.tool.name\0gen_ai.response.finish_reasons\0",
    "llm.model_name\0llm.token_count.prompt\0llm.token_count.completion\0",
    "input.value\0output.value\0service.name\0deployment.environment\0",
    "openai\0anthropic\0google\0bedrock\0azure\0",
    "gpt-4o\0gpt-4o-mini\0gpt-4.1\0gpt-4.1-mini\0o3-mini\0",
    "claude-sonnet-4\0claude-3-5-sonnet\0claude-3-5-haiku\0",
    "gemini-2.5-pro\0gemini-2.5-flash\0",
    "production\0staging\0development\0unversioned\0",
    "agent\0llm\0tool\0retrieval\0embedding\0guardrail\0workflow\0",
    "start\0update\0final\0ok\0error\0timeout\0cancelled\0",
    "[REDACTED]\0_truncated\0_preview\0_original_bytes\0",
)
.as_bytes();

/// Returns the deterministic pre-shared AI Agent Zstd dictionary (`AGENT_DICT_ID = 2`).
pub fn agent_zstd_dict() -> &'static [u8] {
    AGENT_ZSTD_DICT
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid PLST magic: expected {expected:?}, got {actual:?}")]
    InvalidMagic { expected: [u8; 4], actual: [u8; 4] },
    #[error("unsupported PLST version: {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported PLST flags: 0x{0:02x}")]
    UnsupportedFlags(u8),
    #[error("unknown PLST dictionary id: {0}")]
    UnknownDictionary(u16),
    #[error("truncated PLST frame at {0}")]
    TruncatedFrame(&'static str),
    #[error("unexpected trailing bytes in PLST frame: {0}")]
    TrailingBytes(usize),
    #[error("PLST compressed CRC32 mismatch: expected 0x{expected:08x}, got 0x{actual:08x}")]
    CrcMismatch { expected: u32, actual: u32 },
    #[error("PLST length mismatch: expected {expected} bytes, got {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("invalid PLST header: {0}")]
    InvalidHeader(&'static str),
    #[error("PLST resource limit exceeded: {0}")]
    ResourceLimitExceeded(&'static str),
    #[error("invalid UTF-8 in PLST payload: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("invalid string table index: {0}")]
    InvalidStringIndex(u32),
    #[error("invalid blob table index: {0}")]
    InvalidBlobIndex(u32),
    #[error("invalid span index: {0}")]
    InvalidSpanIndex(u32),
    #[error("invalid trace index: {0}")]
    InvalidTraceIndex(u32),
    #[error("PLST blob {index} xxh3_64 mismatch: expected 0x{expected:016x}, got 0x{actual:016x}")]
    BlobChecksumMismatch {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("invalid {kind} enum tag: {tag}")]
    InvalidEnumTag { kind: &'static str, tag: u8 },
    #[error("Zstd error: {0}")]
    Zstd(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Bounded Zstd Decompression & Safe ByteCursor
// ---------------------------------------------------------------------------

fn drain_bounded_reader<R: Read>(
    mut reader: R,
    max_bytes: usize,
) -> Result<Vec<u8>, ProtocolError> {
    let mut out = Vec::new();
    let mut scratch = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut scratch)?;
        if n == 0 {
            break;
        }
        let next_len = out
            .len()
            .checked_add(n)
            .ok_or(ProtocolError::ResourceLimitExceeded(
                "decompressed length overflow",
            ))?;
        if next_len > max_bytes {
            return Err(ProtocolError::ResourceLimitExceeded(
                "Zstd decompressed output exceeds 64 MiB limit",
            ));
        }
        out.extend_from_slice(&scratch[..n]);
    }
    Ok(out)
}

fn decode_zstd_bounded(
    compressed: &[u8],
    dict: Option<&[u8]>,
    max_bytes: usize,
) -> Result<Vec<u8>, ProtocolError> {
    match dict {
        Some(d) => drain_bounded_reader(
            zstd::stream::Decoder::with_dictionary(compressed, d)?.single_frame(),
            max_bytes,
        ),
        None => drain_bounded_reader(
            zstd::stream::Decoder::new(compressed)?.single_frame(),
            max_bytes,
        ),
    }
}

struct ByteCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn read_slice(&mut self, len: usize, ctx: &'static str) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ProtocolError::TruncatedFrame(ctx))?;
        if end > self.buf.len() {
            return Err(ProtocolError::TruncatedFrame(ctx));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn read_u8(&mut self, ctx: &'static str) -> Result<u8, ProtocolError> {
        Ok(self.read_slice(1, ctx)?[0])
    }

    fn read_u16(&mut self, ctx: &'static str) -> Result<u16, ProtocolError> {
        let s = self.read_slice(2, ctx)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }

    fn read_u32(&mut self, ctx: &'static str) -> Result<u32, ProtocolError> {
        let s = self.read_slice(4, ctx)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn bounded_capacity(&self, count: usize, min_elem_wire_bytes: usize) -> usize {
        (self.remaining() / min_elem_wire_bytes.max(1))
            .min(count)
            .min(MAX_INITIAL_VEC_CAP)
    }

    fn read_u8_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u8>, ProtocolError> {
        Ok(self.read_slice(count, ctx)?.to_vec())
    }

    fn read_u16_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u16>, ProtocolError> {
        let bytes_len = count
            .checked_mul(2)
            .ok_or(ProtocolError::TruncatedFrame(ctx))?;
        let raw = self.read_slice(bytes_len, ctx)?;
        let mut out = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for chunk in raw.chunks_exact(2) {
            out.push(u16::from_le_bytes([chunk[0], chunk[1]]));
        }
        Ok(out)
    }

    fn read_u32_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u32>, ProtocolError> {
        let bytes_len = count
            .checked_mul(4)
            .ok_or(ProtocolError::TruncatedFrame(ctx))?;
        let raw = self.read_slice(bytes_len, ctx)?;
        let mut out = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for chunk in raw.chunks_exact(4) {
            out.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(out)
    }

    fn read_i32_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<i32>, ProtocolError> {
        let bytes_len = count
            .checked_mul(4)
            .ok_or(ProtocolError::TruncatedFrame(ctx))?;
        let raw = self.read_slice(bytes_len, ctx)?;
        let mut out = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for chunk in raw.chunks_exact(4) {
            out.push(i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(out)
    }

    fn read_i64_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<i64>, ProtocolError> {
        let bytes_len = count
            .checked_mul(8)
            .ok_or(ProtocolError::TruncatedFrame(ctx))?;
        let raw = self.read_slice(bytes_len, ctx)?;
        let mut out = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for chunk in raw.chunks_exact(8) {
            out.push(i64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            ]));
        }
        Ok(out)
    }

    fn read_u64_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u64>, ProtocolError> {
        let bytes_len = count
            .checked_mul(8)
            .ok_or(ProtocolError::TruncatedFrame(ctx))?;
        let raw = self.read_slice(bytes_len, ctx)?;
        let mut out = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for chunk in raw.chunks_exact(8) {
            out.push(u64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            ]));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Section 1 & 2 Builders (String Table + Content-Addressed Blob Table)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct StringTableBuilder<'a> {
    strings: Vec<&'a str>,
    map: HashMap<&'a str, u32>,
    blob_bytes: usize,
}

impl<'a> StringTableBuilder<'a> {
    fn intern(&mut self, s: &'a str) -> Result<u32, ProtocolError> {
        if let Some(&idx) = self.map.get(s) {
            return Ok(idx);
        }
        if s.len() > MAX_PLST_SINGLE_STRING_BYTES {
            return Err(ProtocolError::ResourceLimitExceeded(
                "single string exceeds MAX_PLST_SINGLE_STRING_BYTES",
            ));
        }
        if self.strings.len() >= MAX_PLST_STRINGS {
            return Err(ProtocolError::ResourceLimitExceeded(
                "string table exceeds MAX_PLST_STRINGS",
            ));
        }
        let next_blob_bytes =
            self.blob_bytes
                .checked_add(s.len())
                .ok_or(ProtocolError::ResourceLimitExceeded(
                    "string table byte size overflow",
                ))?;
        if next_blob_bytes > u32::MAX as usize {
            return Err(ProtocolError::ResourceLimitExceeded(
                "string table exceeds u32 offset range",
            ));
        }
        let idx = self.strings.len() as u32;
        if idx == NONE_IDX {
            return Err(ProtocolError::ResourceLimitExceeded(
                "string index collided with NONE_IDX",
            ));
        }
        self.strings.push(s);
        self.map.insert(s, idx);
        self.blob_bytes = next_blob_bytes;
        Ok(idx)
    }

    fn intern_opt(&mut self, s: Option<&'a str>) -> Result<u32, ProtocolError> {
        match s {
            Some(v) => self.intern(v),
            None => Ok(NONE_IDX),
        }
    }

    fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.strings.len() as u32).to_le_bytes());
        let mut offset: u32 = 0;
        out.extend_from_slice(&offset.to_le_bytes());
        for &s in &self.strings {
            offset += s.len() as u32;
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for &s in &self.strings {
            out.extend_from_slice(s.as_bytes());
        }
    }
}

#[derive(Default)]
struct BlobTableBuilder {
    hashes: Vec<u64>,
    encodings: Vec<u8>,
    ranges: Vec<(usize, usize)>,
    data: Vec<u8>,
    /// Keyed by `(xxh3_64, encoding, byte_len)` -> candidate `blob_id`s; exact byte equality checked.
    buckets: HashMap<(u64, u8, u32), Vec<u32>>,
}

impl BlobTableBuilder {
    fn intern_value(&mut self, val: &Value) -> Result<u32, ProtocolError> {
        let (encoding, bytes): (u8, Vec<u8>) = match val {
            Value::Null => return Ok(NONE_IDX),
            Value::String(s) => (BLOB_ENC_UTF8_TEXT, s.as_bytes().to_vec()),
            other => (BLOB_ENC_JSON_UTF8, serde_json::to_vec(other)?),
        };
        let len_u32 = u32::try_from(bytes.len())
            .map_err(|_| ProtocolError::ResourceLimitExceeded("blob byte length exceeds u32"))?;
        let hash = xxh3_64(&bytes);
        if let Some(candidates) = self.buckets.get(&(hash, encoding, len_u32)) {
            for &id in candidates {
                let (start, end) = self.ranges[id as usize];
                if self.data[start..end] == bytes[..] {
                    return Ok(id);
                }
            }
        }
        if self.hashes.len() >= MAX_PLST_BLOBS {
            return Err(ProtocolError::ResourceLimitExceeded(
                "blob table exceeds MAX_PLST_BLOBS",
            ));
        }
        let start = self.data.len();
        let end = start
            .checked_add(bytes.len())
            .ok_or(ProtocolError::ResourceLimitExceeded(
                "blob data length overflow",
            ))?;
        if end > u32::MAX as usize {
            return Err(ProtocolError::ResourceLimitExceeded(
                "blob table exceeds u32 offset range",
            ));
        }
        let id = self.hashes.len() as u32;
        if id == NONE_IDX {
            return Err(ProtocolError::ResourceLimitExceeded(
                "blob index collided with NONE_IDX",
            ));
        }
        self.hashes.push(hash);
        self.encodings.push(encoding);
        self.ranges.push((start, end));
        self.data.extend_from_slice(&bytes);
        self.buckets
            .entry((hash, encoding, len_u32))
            .or_default()
            .push(id);
        Ok(id)
    }

    fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.hashes.len() as u32).to_le_bytes());
        for &h in &self.hashes {
            out.extend_from_slice(&h.to_le_bytes());
        }
        out.extend_from_slice(&self.encodings);
        let mut offset: u32 = 0;
        out.extend_from_slice(&offset.to_le_bytes());
        for &(start, end) in &self.ranges {
            offset += (end - start) as u32;
            out.extend_from_slice(&offset.to_le_bytes());
        }
        out.extend_from_slice(&self.data);
    }
}

struct StringTableReader<'a> {
    strings: Vec<&'a str>,
    materialized_bytes: usize,
}

impl<'a> StringTableReader<'a> {
    fn read(cursor: &mut ByteCursor<'a>) -> Result<Self, ProtocolError> {
        let count = cursor.read_u32("string_table.count")? as usize;
        if count > MAX_PLST_STRINGS {
            return Err(ProtocolError::ResourceLimitExceeded(
                "string table count exceeds MAX_PLST_STRINGS",
            ));
        }
        let offsets_count = count
            .checked_add(1)
            .ok_or(ProtocolError::ResourceLimitExceeded(
                "string offsets overflow",
            ))?;
        let offsets = cursor.read_u32_vec(offsets_count, "string_table.offsets")?;
        if offsets[0] != 0 {
            return Err(ProtocolError::InvalidHeader(
                "string table offsets[0] must be 0",
            ));
        }
        let total_bytes = *offsets.last().unwrap_or(&0) as usize;
        let blob = cursor.read_slice(total_bytes, "string_table.data")?;
        let mut strings = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for pair in offsets.windows(2) {
            let start = pair[0] as usize;
            let end = pair[1] as usize;
            if end < start || end > total_bytes {
                return Err(ProtocolError::InvalidHeader(
                    "non-monotonic string table offsets",
                ));
            }
            if end - start > MAX_PLST_SINGLE_STRING_BYTES {
                return Err(ProtocolError::ResourceLimitExceeded(
                    "single string exceeds MAX_PLST_SINGLE_STRING_BYTES",
                ));
            }
            let s = std::str::from_utf8(&blob[start..end]).map_err(|_| {
                ProtocolError::InvalidUtf8(
                    String::from_utf8(blob[start..end].to_vec()).unwrap_err(),
                )
            })?;
            strings.push(s);
        }
        Ok(Self {
            strings,
            materialized_bytes: 0,
        })
    }

    fn get_req(&mut self, idx: u32) -> Result<String, ProtocolError> {
        if idx == NONE_IDX {
            return Err(ProtocolError::InvalidStringIndex(idx));
        }
        let &s = self
            .strings
            .get(idx as usize)
            .ok_or(ProtocolError::InvalidStringIndex(idx))?;
        self.materialized_bytes = self.materialized_bytes.checked_add(s.len()).ok_or(
            ProtocolError::ResourceLimitExceeded("materialized string bytes overflow"),
        )?;
        if self.materialized_bytes > MAX_PLST_MATERIALIZED_BYTES {
            return Err(ProtocolError::ResourceLimitExceeded(
                "materialized bytes exceed 64 MiB anti-amplification cap",
            ));
        }
        Ok(s.to_string())
    }

    fn get_opt(&mut self, idx: u32) -> Result<Option<String>, ProtocolError> {
        if idx == NONE_IDX {
            Ok(None)
        } else {
            self.get_req(idx).map(Some)
        }
    }
}

struct BlobTableReader<'a> {
    data: &'a [u8],
    encodings: Vec<u8>,
    ranges: Vec<(usize, usize)>,
    parsed_cache: Vec<Option<Value>>,
    materialized_bytes: usize,
}

impl<'a> BlobTableReader<'a> {
    fn read(cursor: &mut ByteCursor<'a>) -> Result<Self, ProtocolError> {
        let count = cursor.read_u32("blob_table.count")? as usize;
        if count > MAX_PLST_BLOBS {
            return Err(ProtocolError::ResourceLimitExceeded(
                "blob count exceeds MAX_PLST_BLOBS",
            ));
        }
        let hashes = cursor.read_u64_vec(count, "blob_table.xxh3_64")?;
        let encodings = cursor.read_u8_vec(count, "blob_table.encodings")?;
        for &enc in &encodings {
            if enc != BLOB_ENC_JSON_UTF8 && enc != BLOB_ENC_UTF8_TEXT {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "blob_encoding",
                    tag: enc,
                });
            }
        }
        let offsets_count = count
            .checked_add(1)
            .ok_or(ProtocolError::ResourceLimitExceeded(
                "blob offsets overflow",
            ))?;
        let offsets = cursor.read_u32_vec(offsets_count, "blob_table.offsets")?;
        if offsets[0] != 0 {
            return Err(ProtocolError::InvalidHeader(
                "blob table offsets[0] must be 0",
            ));
        }
        let total_bytes = *offsets.last().unwrap_or(&0) as usize;
        let data = cursor.read_slice(total_bytes, "blob_table.data")?;

        let mut ranges = Vec::with_capacity(count.min(MAX_INITIAL_VEC_CAP));
        for (idx, pair) in offsets.windows(2).enumerate() {
            let start = pair[0] as usize;
            let end = pair[1] as usize;
            if end < start || end > total_bytes {
                return Err(ProtocolError::InvalidHeader(
                    "non-monotonic blob table offsets",
                ));
            }
            let slice = &data[start..end];
            let actual = xxh3_64(slice);
            let expected = hashes[idx];
            if actual != expected {
                return Err(ProtocolError::BlobChecksumMismatch {
                    index: idx,
                    expected,
                    actual,
                });
            }
            ranges.push((start, end));
        }

        let parsed_cache = vec![None; count];
        Ok(Self {
            data,
            encodings,
            ranges,
            parsed_cache,
            materialized_bytes: 0,
        })
    }

    fn get_value(
        &mut self,
        idx: u32,
        shared_materialized: &mut usize,
    ) -> Result<Value, ProtocolError> {
        if idx == NONE_IDX {
            return Ok(Value::Null);
        }
        let i = idx as usize;
        let &(start, end) = self
            .ranges
            .get(i)
            .ok_or(ProtocolError::InvalidBlobIndex(idx))?;
        let byte_len = end - start;
        self.materialized_bytes = self.materialized_bytes.checked_add(byte_len).ok_or(
            ProtocolError::ResourceLimitExceeded("blob materialized bytes overflow"),
        )?;
        let total = shared_materialized.checked_add(byte_len).ok_or(
            ProtocolError::ResourceLimitExceeded("total materialized bytes overflow"),
        )?;
        if total > MAX_PLST_MATERIALIZED_BYTES {
            return Err(ProtocolError::ResourceLimitExceeded(
                "materialized blob bytes exceed 64 MiB anti-amplification cap",
            ));
        }
        *shared_materialized = total;

        if let Some(cached) = &self.parsed_cache[i] {
            return Ok(cached.clone());
        }
        let slice = &self.data[start..end];
        let val = match self.encodings[i] {
            BLOB_ENC_UTF8_TEXT => {
                let s = std::str::from_utf8(slice).map_err(|_| {
                    ProtocolError::InvalidUtf8(String::from_utf8(slice.to_vec()).unwrap_err())
                })?;
                Value::String(s.to_owned())
            }
            BLOB_ENC_JSON_UTF8 => serde_json::from_slice(slice)?,
            other => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "blob_encoding",
                    tag: other,
                });
            }
        };
        self.parsed_cache[i] = Some(val.clone());
        Ok(val)
    }
}

// ---------------------------------------------------------------------------
// Public Encode / Decode API
// ---------------------------------------------------------------------------

/// Encodes a `TraceIngestBatch` into a `PLST` v1 wire frame.
/// When `use_dict == true`, compresses using `AGENT_DICT_ID = 2` (`FLAG_DICT_ZSTD`).
/// When `use_dict == false`, compresses using raw Zstd (`FLAG_RAW_ZSTD`).
pub fn encode_plst_batch(
    batch: &TraceIngestBatch,
    use_dict: bool,
) -> Result<Vec<u8>, ProtocolError> {
    let comp_flag = if use_dict {
        FLAG_DICT_ZSTD
    } else {
        FLAG_RAW_ZSTD
    };
    encode_plst_batch_with_mode(batch, comp_flag)
}

/// Encodes a `TraceIngestBatch` into an uncompressed `PLST` v1 frame (for zero-dep SDK fallback/test vectors).
pub fn encode_plst_batch_uncompressed(batch: &TraceIngestBatch) -> Result<Vec<u8>, ProtocolError> {
    encode_plst_batch_with_mode(batch, 0)
}

fn encode_plst_batch_with_mode(
    batch: &TraceIngestBatch,
    comp_flag: u8,
) -> Result<Vec<u8>, ProtocolError> {
    if batch.traces.len() > MAX_PLST_TRACES {
        return Err(ProtocolError::ResourceLimitExceeded(
            "trace count exceeds MAX_PLST_TRACES",
        ));
    }
    if batch.spans.len() > MAX_PLST_SPANS {
        return Err(ProtocolError::ResourceLimitExceeded(
            "span count exceeds MAX_PLST_SPANS",
        ));
    }

    let uncomp = encode_columnar_payload(batch)?;
    if uncomp.len() > MAX_PLST_UNCOMP_BYTES {
        return Err(ProtocolError::ResourceLimitExceeded(
            "uncompressed PLST payload exceeds 64 MiB limit",
        ));
    }

    let (dict_id, body) = match comp_flag {
        0 => (0u16, uncomp.clone()),
        FLAG_RAW_ZSTD => {
            let compressed = zstd::stream::encode_all(uncomp.as_slice(), 3)?;
            (0u16, compressed)
        }
        FLAG_DICT_ZSTD => {
            let mut enc = zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())?;
            enc.write_all(&uncomp)?;
            let compressed = enc.finish()?;
            (AGENT_DICT_ID, compressed)
        }
        other => return Err(ProtocolError::UnsupportedFlags(other)),
    };

    let uncomp_len = u32::try_from(uncomp.len()).map_err(|_| {
        ProtocolError::ResourceLimitExceeded("uncompressed payload length exceeds u32")
    })?;
    let comp_len = u32::try_from(body.len()).map_err(|_| {
        ProtocolError::ResourceLimitExceeded("compressed payload length exceeds u32")
    })?;

    let has_partial = batch
        .traces
        .iter()
        .any(|t| t.status == TraceStatus::Running)
        || batch.spans.iter().any(|s| s.span_phase != SpanPhase::Final);
    let has_final = batch
        .traces
        .iter()
        .any(|t| t.status != TraceStatus::Running)
        || batch.spans.iter().any(|s| s.span_phase == SpanPhase::Final);
    let lifecycle_flags = if !has_partial && !has_final {
        FLAG_FINAL
    } else {
        (if has_partial { FLAG_PARTIAL } else { 0 }) | (if has_final { FLAG_FINAL } else { 0 })
    };
    let flags = comp_flag | lifecycle_flags;
    let crc = crc32fast::hash(&body);

    let mut frame = Vec::with_capacity(PLST_HEADER_LEN + body.len());
    frame.extend_from_slice(&PLST_MAGIC);
    frame.push(PLST_VERSION);
    frame.push(flags);
    frame.extend_from_slice(&dict_id.to_le_bytes());
    frame.extend_from_slice(&uncomp_len.to_le_bytes());
    frame.extend_from_slice(&comp_len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Decodes and validates a `PLST` v1 wire frame into a `TraceIngestBatch`.
pub fn decode_plst_batch(bytes: &[u8]) -> Result<TraceIngestBatch, ProtocolError> {
    if bytes.len() < PLST_HEADER_LEN {
        return Err(ProtocolError::TruncatedFrame("PLST 20B header"));
    }
    let mut header = ByteCursor::new(&bytes[..PLST_HEADER_LEN]);
    let magic_slice = header.read_slice(4, "magic")?;
    let magic: [u8; 4] = [
        magic_slice[0],
        magic_slice[1],
        magic_slice[2],
        magic_slice[3],
    ];
    if magic != PLST_MAGIC {
        return Err(ProtocolError::InvalidMagic {
            expected: PLST_MAGIC,
            actual: magic,
        });
    }
    let version = header.read_u8("version")?;
    if version != PLST_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let flags = header.read_u8("flags")?;
    if (flags & !KNOWN_FLAGS_MASK) != 0 {
        return Err(ProtocolError::UnsupportedFlags(flags));
    }
    let comp_bits = flags & (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD);
    if comp_bits == (FLAG_RAW_ZSTD | FLAG_DICT_ZSTD) {
        return Err(ProtocolError::UnsupportedFlags(flags));
    }
    let dict_id = header.read_u16("dict_id")?;
    if (flags & FLAG_DICT_ZSTD) != 0 {
        if dict_id != AGENT_DICT_ID {
            return Err(ProtocolError::UnknownDictionary(dict_id));
        }
    } else if dict_id != 0 {
        return Err(ProtocolError::InvalidHeader(
            "dict_id must be 0 when FLAG_DICT_ZSTD is unset",
        ));
    }

    let uncomp_len = header.read_u32("uncomp_len")? as usize;
    if uncomp_len > MAX_PLST_UNCOMP_BYTES {
        return Err(ProtocolError::ResourceLimitExceeded(
            "header uncomp_len exceeds 64 MiB limit",
        ));
    }
    let comp_len = header.read_u32("comp_len")? as usize;
    let compressed_crc32 = header.read_u32("compressed_crc32")?;

    let actual_body_len = bytes.len() - PLST_HEADER_LEN;
    if actual_body_len < comp_len {
        return Err(ProtocolError::TruncatedFrame("PLST body"));
    }
    if actual_body_len > comp_len {
        return Err(ProtocolError::TrailingBytes(actual_body_len - comp_len));
    }
    if comp_bits == 0 && comp_len != uncomp_len {
        return Err(ProtocolError::LengthMismatch {
            expected: uncomp_len,
            actual: comp_len,
        });
    }

    let body = &bytes[PLST_HEADER_LEN..];
    let actual_crc = crc32fast::hash(body);
    if actual_crc != compressed_crc32 {
        return Err(ProtocolError::CrcMismatch {
            expected: compressed_crc32,
            actual: actual_crc,
        });
    }

    let decompressed_owned: Vec<u8>;
    let payload_slice: &[u8] = match comp_bits {
        0 => body,
        FLAG_RAW_ZSTD => {
            decompressed_owned = decode_zstd_bounded(body, None, MAX_PLST_UNCOMP_BYTES)?;
            if decompressed_owned.len() != uncomp_len {
                return Err(ProtocolError::LengthMismatch {
                    expected: uncomp_len,
                    actual: decompressed_owned.len(),
                });
            }
            &decompressed_owned
        }
        FLAG_DICT_ZSTD => {
            decompressed_owned =
                decode_zstd_bounded(body, Some(agent_zstd_dict()), MAX_PLST_UNCOMP_BYTES)?;
            if decompressed_owned.len() != uncomp_len {
                return Err(ProtocolError::LengthMismatch {
                    expected: uncomp_len,
                    actual: decompressed_owned.len(),
                });
            }
            &decompressed_owned
        }
        _ => unreachable!(),
    };

    decode_columnar_payload(payload_slice)
}

// ---------------------------------------------------------------------------
// 5 Columnar Sections Encode / Decode
// ---------------------------------------------------------------------------

fn encode_columnar_payload(batch: &TraceIngestBatch) -> Result<Vec<u8>, ProtocolError> {
    let mut strings = StringTableBuilder::default();
    let mut blobs = BlobTableBuilder::default();

    let trace_count = batch.traces.len();
    let span_count = batch.spans.len();

    let mut trace_idx_by_id: HashMap<&str, u32> = HashMap::with_capacity(trace_count);
    for (idx, tr) in batch.traces.iter().enumerate() {
        trace_idx_by_id
            .entry(tr.trace_id.as_str())
            .or_insert(idx as u32);
    }

    // Pre-intern Section 3 (Traces)
    let mut tr_trace_ids = Vec::with_capacity(trace_count);
    let mut tr_project_ids = Vec::with_capacity(trace_count);
    let mut tr_session_ids = Vec::with_capacity(trace_count);
    let mut tr_user_ids = Vec::with_capacity(trace_count);
    let mut tr_workflow_ids = Vec::with_capacity(trace_count);
    let mut tr_parent_trace_ids = Vec::with_capacity(trace_count);
    let mut tr_agent_names = Vec::with_capacity(trace_count);
    let mut tr_agent_versions = Vec::with_capacity(trace_count);
    let mut tr_prompt_versions = Vec::with_capacity(trace_count);
    let mut tr_model_cfg_versions = Vec::with_capacity(trace_count);
    let mut tr_tool_cfg_versions = Vec::with_capacity(trace_count);
    let mut tr_app_releases = Vec::with_capacity(trace_count);
    let mut tr_environments = Vec::with_capacity(trace_count);

    let mut tr_statuses = Vec::with_capacity(trace_count);
    let mut tr_failure_sources = Vec::with_capacity(trace_count);
    let mut tr_start_deltas = Vec::with_capacity(trace_count);
    let mut tr_has_end = Vec::with_capacity(trace_count);
    let mut tr_end_offsets = Vec::with_capacity(trace_count);
    let mut tr_durations = Vec::with_capacity(trace_count);

    let mut tr_span_counts = Vec::with_capacity(trace_count);
    let mut tr_llm_counts = Vec::with_capacity(trace_count);
    let mut tr_tool_counts = Vec::with_capacity(trace_count);
    let mut tr_err_span_counts = Vec::with_capacity(trace_count);
    let mut tr_in_tokens = Vec::with_capacity(trace_count);
    let mut tr_out_tokens = Vec::with_capacity(trace_count);
    let mut tr_cached_tokens = Vec::with_capacity(trace_count);
    let mut tr_total_tokens = Vec::with_capacity(trace_count);
    let mut tr_cost_nanos = Vec::with_capacity(trace_count);

    let mut tr_primary_models = Vec::with_capacity(trace_count);
    let mut tr_primary_providers = Vec::with_capacity(trace_count);
    let mut tr_has_avg_eval = Vec::with_capacity(trace_count);
    let mut tr_avg_eval_bits = Vec::with_capacity(trace_count);
    let mut tr_has_pass_rate = Vec::with_capacity(trace_count);
    let mut tr_pass_rate_bits = Vec::with_capacity(trace_count);
    let mut tr_has_loop = Vec::with_capacity(trace_count);
    let mut tr_error_types = Vec::with_capacity(trace_count);
    let mut tr_error_messages = Vec::with_capacity(trace_count);

    let mut tr_tag_counts = Vec::with_capacity(trace_count);
    let mut tr_tag_str_ids = Vec::new();
    let mut tr_input_blobs = Vec::with_capacity(trace_count);
    let mut tr_output_blobs = Vec::with_capacity(trace_count);
    let mut tr_metadata_blobs = Vec::with_capacity(trace_count);

    let mut prev_tr_start: i64 = 0;
    for t in &batch.traces {
        tr_trace_ids.push(strings.intern(&t.trace_id)?);
        tr_project_ids.push(strings.intern(&t.project_id)?);
        tr_session_ids.push(strings.intern_opt(t.session_id.as_deref())?);
        tr_user_ids.push(strings.intern_opt(t.user_id.as_deref())?);
        tr_workflow_ids.push(strings.intern_opt(t.workflow_id.as_deref())?);
        tr_parent_trace_ids.push(strings.intern_opt(t.parent_trace_id.as_deref())?);
        tr_agent_names.push(strings.intern(&t.agent_name)?);
        tr_agent_versions.push(strings.intern(&t.agent_version)?);
        tr_prompt_versions.push(strings.intern_opt(t.prompt_version.as_deref())?);
        tr_model_cfg_versions.push(strings.intern_opt(t.model_config_version.as_deref())?);
        tr_tool_cfg_versions.push(strings.intern_opt(t.tool_config_version.as_deref())?);
        tr_app_releases.push(strings.intern_opt(t.app_release.as_deref())?);
        tr_environments.push(strings.intern(&t.environment)?);

        tr_statuses.push(t.status.to_u8());
        tr_failure_sources.push(t.failure_source.map_or(0, |fs| fs.to_u8()));
        tr_start_deltas.push(t.start_ts_ms.wrapping_sub(prev_tr_start));
        prev_tr_start = t.start_ts_ms;
        match t.end_ts_ms {
            Some(end) => {
                tr_has_end.push(1);
                tr_end_offsets.push(end.wrapping_sub(t.start_ts_ms));
            }
            None => {
                tr_has_end.push(0);
                tr_end_offsets.push(0);
            }
        }
        tr_durations.push(t.duration_ms);

        tr_span_counts.push(t.span_count);
        tr_llm_counts.push(t.llm_call_count);
        tr_tool_counts.push(t.tool_call_count);
        tr_err_span_counts.push(t.error_span_count);
        tr_in_tokens.push(t.input_tokens);
        tr_out_tokens.push(t.output_tokens);
        tr_cached_tokens.push(t.cached_tokens);
        tr_total_tokens.push(t.total_tokens);
        let cost_nanos = if t.total_cost_nanos != 0 {
            t.total_cost_nanos
        } else {
            usd_to_cost_nanos(t.total_cost_usd)
        };
        tr_cost_nanos.push(cost_nanos);

        tr_primary_models.push(strings.intern_opt(t.primary_model.as_deref())?);
        tr_primary_providers.push(strings.intern_opt(t.primary_provider.as_deref())?);
        match t.avg_eval_score {
            Some(s) => {
                tr_has_avg_eval.push(1);
                tr_avg_eval_bits.push(s.to_bits());
            }
            None => {
                tr_has_avg_eval.push(0);
                tr_avg_eval_bits.push(0);
            }
        }
        match t.eval_pass_rate {
            Some(r) => {
                tr_has_pass_rate.push(1);
                tr_pass_rate_bits.push(r.to_bits());
            }
            None => {
                tr_has_pass_rate.push(0);
                tr_pass_rate_bits.push(0);
            }
        }
        tr_has_loop.push(u8::from(t.has_loop_warning));
        tr_error_types.push(strings.intern_opt(t.error_type.as_deref())?);
        tr_error_messages.push(strings.intern_opt(t.error_message.as_deref())?);

        let tag_len = u16::try_from(t.tags.len()).map_err(|_| {
            ProtocolError::ResourceLimitExceeded("trace tag count exceeds u16::MAX")
        })?;
        tr_tag_counts.push(tag_len);
        for tag in &t.tags {
            tr_tag_str_ids.push(strings.intern(tag)?);
        }
        tr_input_blobs.push(blobs.intern_value(&t.input)?);
        tr_output_blobs.push(blobs.intern_value(&t.output)?);
        tr_metadata_blobs.push(blobs.intern_value(&t.metadata)?);
    }

    // Pre-intern Section 4 (Spans) & Section 5 (Events)
    let mut sp_trace_indices = Vec::with_capacity(span_count);
    let mut sp_trace_ids = Vec::with_capacity(span_count);
    let mut sp_project_ids = Vec::with_capacity(span_count);
    let mut sp_span_ids = Vec::with_capacity(span_count);
    let mut sp_parent_span_ids = Vec::with_capacity(span_count);
    let mut sp_linked_counts = Vec::with_capacity(span_count);
    let mut sp_linked_str_ids = Vec::new();
    let mut sp_depths = Vec::with_capacity(span_count);

    let mut sp_names = Vec::with_capacity(span_count);
    let mut sp_types = Vec::with_capacity(span_count);
    let mut sp_phases = Vec::with_capacity(span_count);
    let mut sp_statuses = Vec::with_capacity(span_count);
    let mut sp_start_deltas = Vec::with_capacity(span_count);
    let mut sp_has_end = Vec::with_capacity(span_count);
    let mut sp_end_offsets = Vec::with_capacity(span_count);
    let mut sp_durations = Vec::with_capacity(span_count);

    let mut sp_providers = Vec::with_capacity(span_count);
    let mut sp_models = Vec::with_capacity(span_count);
    let mut sp_model_ids = Vec::with_capacity(span_count);
    let mut sp_model_versions = Vec::with_capacity(span_count);
    let mut sp_prompt_tpl_ids = Vec::with_capacity(span_count);
    let mut sp_prompt_tpl_versions = Vec::with_capacity(span_count);
    let mut sp_in_tokens = Vec::with_capacity(span_count);
    let mut sp_out_tokens = Vec::with_capacity(span_count);
    let mut sp_cached_tokens = Vec::with_capacity(span_count);
    let mut sp_total_tokens = Vec::with_capacity(span_count);
    let mut sp_cost_nanos = Vec::with_capacity(span_count);
    let mut sp_has_temp = Vec::with_capacity(span_count);
    let mut sp_temp_bits = Vec::with_capacity(span_count);
    let mut sp_finish_reasons = Vec::with_capacity(span_count);
    let mut sp_model_params_blobs = Vec::with_capacity(span_count);
    let mut sp_tool_names = Vec::with_capacity(span_count);
    let mut sp_tool_versions = Vec::with_capacity(span_count);
    let mut sp_tool_retries = Vec::with_capacity(span_count);

    let mut sp_error_types = Vec::with_capacity(span_count);
    let mut sp_error_messages = Vec::with_capacity(span_count);
    let mut sp_error_stacks = Vec::with_capacity(span_count);
    let mut sp_input_blobs = Vec::with_capacity(span_count);
    let mut sp_output_blobs = Vec::with_capacity(span_count);
    let mut sp_attr_blobs = Vec::with_capacity(span_count);
    let mut sp_meta_blobs = Vec::with_capacity(span_count);

    let mut ev_span_indices = Vec::new();
    let mut ev_ts_deltas = Vec::new();
    let mut ev_names = Vec::new();
    let mut ev_attr_blobs = Vec::new();
    let mut prev_ev_ts: i64 = 0;

    let mut prev_sp_start: i64 = 0;
    for (s_idx, s) in batch.spans.iter().enumerate() {
        let tr_idx = trace_idx_by_id
            .get(s.trace_id.as_str())
            .copied()
            .unwrap_or(NONE_IDX);
        sp_trace_indices.push(tr_idx);
        sp_trace_ids.push(strings.intern(&s.trace_id)?);
        sp_project_ids.push(strings.intern(&s.project_id)?);
        sp_span_ids.push(strings.intern(&s.span_id)?);
        sp_parent_span_ids.push(strings.intern_opt(s.parent_span_id.as_deref())?);

        let lnk_len = u16::try_from(s.linked_span_ids.len()).map_err(|_| {
            ProtocolError::ResourceLimitExceeded("span linked_span_ids count exceeds u16::MAX")
        })?;
        sp_linked_counts.push(lnk_len);
        for lid in &s.linked_span_ids {
            sp_linked_str_ids.push(strings.intern(lid)?);
        }
        sp_depths.push(s.depth);

        sp_names.push(strings.intern(&s.name)?);
        sp_types.push(s.span_type.to_u8());
        sp_phases.push(s.span_phase.to_u8());
        sp_statuses.push(s.status.to_u8());
        sp_start_deltas.push(s.start_ts_ms.wrapping_sub(prev_sp_start));
        prev_sp_start = s.start_ts_ms;
        match s.end_ts_ms {
            Some(end) => {
                sp_has_end.push(1);
                sp_end_offsets.push(end.wrapping_sub(s.start_ts_ms));
            }
            None => {
                sp_has_end.push(0);
                sp_end_offsets.push(0);
            }
        }
        sp_durations.push(s.duration_ms);

        sp_providers.push(strings.intern_opt(s.provider.as_deref())?);
        sp_models.push(strings.intern_opt(s.model.as_deref())?);
        sp_model_ids.push(strings.intern_opt(s.model_id.as_deref())?);
        sp_model_versions.push(strings.intern_opt(s.model_version.as_deref())?);
        sp_prompt_tpl_ids.push(strings.intern_opt(s.prompt_template_id.as_deref())?);
        sp_prompt_tpl_versions.push(strings.intern_opt(s.prompt_template_version.as_deref())?);
        sp_in_tokens.push(s.input_tokens);
        sp_out_tokens.push(s.output_tokens);
        sp_cached_tokens.push(s.cached_tokens);
        sp_total_tokens.push(s.total_tokens);
        let cost_nanos = if s.cost_nanos != 0 {
            s.cost_nanos
        } else {
            usd_to_cost_nanos(s.cost_usd)
        };
        sp_cost_nanos.push(cost_nanos);

        match s.temperature {
            Some(temp) => {
                sp_has_temp.push(1);
                sp_temp_bits.push(temp.to_bits());
            }
            None => {
                sp_has_temp.push(0);
                sp_temp_bits.push(0);
            }
        }
        sp_finish_reasons.push(strings.intern_opt(s.finish_reason.as_deref())?);
        sp_model_params_blobs.push(blobs.intern_value(&s.model_params)?);
        sp_tool_names.push(strings.intern_opt(s.tool_name.as_deref())?);
        sp_tool_versions.push(strings.intern_opt(s.tool_version.as_deref())?);
        sp_tool_retries.push(s.tool_retry_count);

        sp_error_types.push(strings.intern_opt(s.error_type.as_deref())?);
        sp_error_messages.push(strings.intern_opt(s.error_message.as_deref())?);
        sp_error_stacks.push(strings.intern_opt(s.error_stack.as_deref())?);
        sp_input_blobs.push(blobs.intern_value(&s.input)?);
        sp_output_blobs.push(blobs.intern_value(&s.output)?);
        sp_attr_blobs.push(blobs.intern_value(&s.attributes)?);
        sp_meta_blobs.push(blobs.intern_value(&s.metadata)?);

        for ev in &s.events {
            if ev_span_indices.len() >= MAX_PLST_EVENTS {
                return Err(ProtocolError::ResourceLimitExceeded(
                    "span events exceed MAX_PLST_EVENTS",
                ));
            }
            ev_span_indices.push(s_idx as u32);
            ev_ts_deltas.push(ev.timestamp_ms.wrapping_sub(prev_ev_ts));
            prev_ev_ts = ev.timestamp_ms;
            ev_names.push(strings.intern(&ev.name)?);
            ev_attr_blobs.push(blobs.intern_value(&ev.attributes)?);
        }
    }

    let mut out = Vec::with_capacity(4096);
    // Section 1: String Table
    strings.write_to(&mut out);
    // Section 2: Blob Table
    blobs.write_to(&mut out);

    // Section 3: Trace Rows
    out.extend_from_slice(&(trace_count as u32).to_le_bytes());
    write_u32_slice(&mut out, &tr_trace_ids);
    write_u32_slice(&mut out, &tr_project_ids);
    write_u32_slice(&mut out, &tr_session_ids);
    write_u32_slice(&mut out, &tr_user_ids);
    write_u32_slice(&mut out, &tr_workflow_ids);
    write_u32_slice(&mut out, &tr_parent_trace_ids);
    write_u32_slice(&mut out, &tr_agent_names);
    write_u32_slice(&mut out, &tr_agent_versions);
    write_u32_slice(&mut out, &tr_prompt_versions);
    write_u32_slice(&mut out, &tr_model_cfg_versions);
    write_u32_slice(&mut out, &tr_tool_cfg_versions);
    write_u32_slice(&mut out, &tr_app_releases);
    write_u32_slice(&mut out, &tr_environments);

    out.extend_from_slice(&tr_statuses);
    out.extend_from_slice(&tr_failure_sources);
    write_i64_slice(&mut out, &tr_start_deltas);
    out.extend_from_slice(&tr_has_end);
    write_i64_slice(&mut out, &tr_end_offsets);
    write_i64_slice(&mut out, &tr_durations);

    write_i32_slice(&mut out, &tr_span_counts);
    write_i32_slice(&mut out, &tr_llm_counts);
    write_i32_slice(&mut out, &tr_tool_counts);
    write_i32_slice(&mut out, &tr_err_span_counts);
    write_i64_slice(&mut out, &tr_in_tokens);
    write_i64_slice(&mut out, &tr_out_tokens);
    write_i64_slice(&mut out, &tr_cached_tokens);
    write_i64_slice(&mut out, &tr_total_tokens);
    write_i64_slice(&mut out, &tr_cost_nanos);

    write_u32_slice(&mut out, &tr_primary_models);
    write_u32_slice(&mut out, &tr_primary_providers);
    out.extend_from_slice(&tr_has_avg_eval);
    write_u64_slice(&mut out, &tr_avg_eval_bits);
    out.extend_from_slice(&tr_has_pass_rate);
    write_u64_slice(&mut out, &tr_pass_rate_bits);
    out.extend_from_slice(&tr_has_loop);
    write_u32_slice(&mut out, &tr_error_types);
    write_u32_slice(&mut out, &tr_error_messages);

    write_u16_slice(&mut out, &tr_tag_counts);
    out.extend_from_slice(&(tr_tag_str_ids.len() as u32).to_le_bytes());
    write_u32_slice(&mut out, &tr_tag_str_ids);
    write_u32_slice(&mut out, &tr_input_blobs);
    write_u32_slice(&mut out, &tr_output_blobs);
    write_u32_slice(&mut out, &tr_metadata_blobs);

    // Section 4: Span Rows
    out.extend_from_slice(&(span_count as u32).to_le_bytes());
    write_u32_slice(&mut out, &sp_trace_indices);
    write_u32_slice(&mut out, &sp_trace_ids);
    write_u32_slice(&mut out, &sp_project_ids);
    write_u32_slice(&mut out, &sp_span_ids);
    write_u32_slice(&mut out, &sp_parent_span_ids);
    write_u16_slice(&mut out, &sp_linked_counts);
    out.extend_from_slice(&(sp_linked_str_ids.len() as u32).to_le_bytes());
    write_u32_slice(&mut out, &sp_linked_str_ids);
    write_i32_slice(&mut out, &sp_depths);

    write_u32_slice(&mut out, &sp_names);
    out.extend_from_slice(&sp_types);
    out.extend_from_slice(&sp_phases);
    out.extend_from_slice(&sp_statuses);
    write_i64_slice(&mut out, &sp_start_deltas);
    out.extend_from_slice(&sp_has_end);
    write_i64_slice(&mut out, &sp_end_offsets);
    write_i64_slice(&mut out, &sp_durations);

    write_u32_slice(&mut out, &sp_providers);
    write_u32_slice(&mut out, &sp_models);
    write_u32_slice(&mut out, &sp_model_ids);
    write_u32_slice(&mut out, &sp_model_versions);
    write_u32_slice(&mut out, &sp_prompt_tpl_ids);
    write_u32_slice(&mut out, &sp_prompt_tpl_versions);
    write_i64_slice(&mut out, &sp_in_tokens);
    write_i64_slice(&mut out, &sp_out_tokens);
    write_i64_slice(&mut out, &sp_cached_tokens);
    write_i64_slice(&mut out, &sp_total_tokens);
    write_i64_slice(&mut out, &sp_cost_nanos);
    out.extend_from_slice(&sp_has_temp);
    write_u64_slice(&mut out, &sp_temp_bits);
    write_u32_slice(&mut out, &sp_finish_reasons);
    write_u32_slice(&mut out, &sp_model_params_blobs);
    write_u32_slice(&mut out, &sp_tool_names);
    write_u32_slice(&mut out, &sp_tool_versions);
    write_i32_slice(&mut out, &sp_tool_retries);

    write_u32_slice(&mut out, &sp_error_types);
    write_u32_slice(&mut out, &sp_error_messages);
    write_u32_slice(&mut out, &sp_error_stacks);
    write_u32_slice(&mut out, &sp_input_blobs);
    write_u32_slice(&mut out, &sp_output_blobs);
    write_u32_slice(&mut out, &sp_attr_blobs);
    write_u32_slice(&mut out, &sp_meta_blobs);

    // Section 5: Columnar Span Events
    out.extend_from_slice(&(ev_span_indices.len() as u32).to_le_bytes());
    write_u32_slice(&mut out, &ev_span_indices);
    write_i64_slice(&mut out, &ev_ts_deltas);
    write_u32_slice(&mut out, &ev_names);
    write_u32_slice(&mut out, &ev_attr_blobs);

    Ok(out)
}

fn decode_columnar_payload(payload: &[u8]) -> Result<TraceIngestBatch, ProtocolError> {
    let mut cursor = ByteCursor::new(payload);
    let mut strings = StringTableReader::read(&mut cursor)?;
    let mut blobs = BlobTableReader::read(&mut cursor)?;
    let mut materialized_bytes: usize = 0;

    // Section 3: Trace Rows
    let trace_count = cursor.read_u32("traces.count")? as usize;
    if trace_count > MAX_PLST_TRACES {
        return Err(ProtocolError::ResourceLimitExceeded(
            "trace count exceeds MAX_PLST_TRACES",
        ));
    }

    let tr_trace_ids = cursor.read_u32_vec(trace_count, "traces.trace_ids")?;
    let tr_project_ids = cursor.read_u32_vec(trace_count, "traces.project_ids")?;
    let tr_session_ids = cursor.read_u32_vec(trace_count, "traces.session_ids")?;
    let tr_user_ids = cursor.read_u32_vec(trace_count, "traces.user_ids")?;
    let tr_workflow_ids = cursor.read_u32_vec(trace_count, "traces.workflow_ids")?;
    let tr_parent_trace_ids = cursor.read_u32_vec(trace_count, "traces.parent_trace_ids")?;
    let tr_agent_names = cursor.read_u32_vec(trace_count, "traces.agent_names")?;
    let tr_agent_versions = cursor.read_u32_vec(trace_count, "traces.agent_versions")?;
    let tr_prompt_versions = cursor.read_u32_vec(trace_count, "traces.prompt_versions")?;
    let tr_model_cfg_versions = cursor.read_u32_vec(trace_count, "traces.model_config_versions")?;
    let tr_tool_cfg_versions = cursor.read_u32_vec(trace_count, "traces.tool_config_versions")?;
    let tr_app_releases = cursor.read_u32_vec(trace_count, "traces.app_releases")?;
    let tr_environments = cursor.read_u32_vec(trace_count, "traces.environments")?;

    let tr_statuses = cursor.read_u8_vec(trace_count, "traces.statuses")?;
    let tr_failure_sources = cursor.read_u8_vec(trace_count, "traces.failure_sources")?;
    let tr_start_deltas = cursor.read_i64_vec(trace_count, "traces.start_ts_deltas")?;
    let tr_has_end = cursor.read_u8_vec(trace_count, "traces.has_end_ts")?;
    let tr_end_offsets = cursor.read_i64_vec(trace_count, "traces.end_ts_offsets")?;
    let tr_durations = cursor.read_i64_vec(trace_count, "traces.duration_ms")?;

    let tr_span_counts = cursor.read_i32_vec(trace_count, "traces.span_counts")?;
    let tr_llm_counts = cursor.read_i32_vec(trace_count, "traces.llm_call_counts")?;
    let tr_tool_counts = cursor.read_i32_vec(trace_count, "traces.tool_call_counts")?;
    let tr_err_span_counts = cursor.read_i32_vec(trace_count, "traces.error_span_counts")?;
    let tr_in_tokens = cursor.read_i64_vec(trace_count, "traces.input_tokens")?;
    let tr_out_tokens = cursor.read_i64_vec(trace_count, "traces.output_tokens")?;
    let tr_cached_tokens = cursor.read_i64_vec(trace_count, "traces.cached_tokens")?;
    let tr_total_tokens = cursor.read_i64_vec(trace_count, "traces.total_tokens")?;
    let tr_cost_nanos = cursor.read_i64_vec(trace_count, "traces.total_cost_nanos")?;

    let tr_primary_models = cursor.read_u32_vec(trace_count, "traces.primary_models")?;
    let tr_primary_providers = cursor.read_u32_vec(trace_count, "traces.primary_providers")?;
    let tr_has_avg_eval = cursor.read_u8_vec(trace_count, "traces.has_avg_eval")?;
    let tr_avg_eval_bits = cursor.read_u64_vec(trace_count, "traces.avg_eval_bits")?;
    let tr_has_pass_rate = cursor.read_u8_vec(trace_count, "traces.has_pass_rate")?;
    let tr_pass_rate_bits = cursor.read_u64_vec(trace_count, "traces.pass_rate_bits")?;
    let tr_has_loop = cursor.read_u8_vec(trace_count, "traces.has_loop_warning")?;
    let tr_error_types = cursor.read_u32_vec(trace_count, "traces.error_types")?;
    let tr_error_messages = cursor.read_u32_vec(trace_count, "traces.error_messages")?;

    let tr_tag_counts = cursor.read_u16_vec(trace_count, "traces.tag_counts")?;
    let total_tags = cursor.read_u32("traces.total_tags")? as usize;
    let sum_tag_counts: usize = tr_tag_counts
        .iter()
        .try_fold(0usize, |acc, &c| acc.checked_add(c as usize))
        .ok_or(ProtocolError::ResourceLimitExceeded(
            "tag count sum overflow",
        ))?;
    if sum_tag_counts != total_tags {
        return Err(ProtocolError::LengthMismatch {
            expected: sum_tag_counts,
            actual: total_tags,
        });
    }
    let tr_tag_str_ids = cursor.read_u32_vec(total_tags, "traces.tag_str_ids")?;
    let tr_input_blobs = cursor.read_u32_vec(trace_count, "traces.input_blobs")?;
    let tr_output_blobs = cursor.read_u32_vec(trace_count, "traces.output_blobs")?;
    let tr_metadata_blobs = cursor.read_u32_vec(trace_count, "traces.metadata_blobs")?;

    let mut traces = Vec::with_capacity(cursor.bounded_capacity(trace_count, 16));
    let mut prev_tr_start: i64 = 0;
    let mut tag_cursor: usize = 0;
    for i in 0..trace_count {
        let start_ts_ms = prev_tr_start.wrapping_add(tr_start_deltas[i]);
        prev_tr_start = start_ts_ms;
        let end_ts_ms = match tr_has_end[i] {
            0 => None,
            1 => Some(start_ts_ms.wrapping_add(tr_end_offsets[i])),
            tag => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "has_end_ts",
                    tag,
                })
            }
        };
        let status = TraceStatus::from_u8(tr_statuses[i]).ok_or(ProtocolError::InvalidEnumTag {
            kind: "trace_status",
            tag: tr_statuses[i],
        })?;
        let failure_source = match tr_failure_sources[i] {
            0 => None,
            v => Some(
                FailureSource::from_u8(v).ok_or(ProtocolError::InvalidEnumTag {
                    kind: "failure_source",
                    tag: v,
                })?,
            ),
        };
        let avg_eval_score = match tr_has_avg_eval[i] {
            0 => None,
            1 => Some(f64::from_bits(tr_avg_eval_bits[i])),
            tag => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "has_avg_eval_score",
                    tag,
                })
            }
        };
        let eval_pass_rate = match tr_has_pass_rate[i] {
            0 => None,
            1 => Some(f64::from_bits(tr_pass_rate_bits[i])),
            tag => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "has_eval_pass_rate",
                    tag,
                })
            }
        };
        let has_loop_warning = match tr_has_loop[i] {
            0 => false,
            1 => true,
            tag => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "has_loop_warning",
                    tag,
                })
            }
        };
        let tc = tr_tag_counts[i] as usize;
        let mut tags = Vec::with_capacity(tc.min(MAX_INITIAL_VEC_CAP));
        for &sid in &tr_tag_str_ids[tag_cursor..tag_cursor + tc] {
            tags.push(strings.get_req(sid)?);
        }
        tag_cursor += tc;

        let total_cost_nanos = tr_cost_nanos[i];
        traces.push(AgentTrace {
            trace_id: strings.get_req(tr_trace_ids[i])?,
            project_id: strings.get_req(tr_project_ids[i])?,
            agent_name: strings.get_req(tr_agent_names[i])?,
            agent_version: strings.get_req(tr_agent_versions[i])?,
            prompt_version: strings.get_opt(tr_prompt_versions[i])?,
            model_config_version: strings.get_opt(tr_model_cfg_versions[i])?,
            tool_config_version: strings.get_opt(tr_tool_cfg_versions[i])?,
            app_release: strings.get_opt(tr_app_releases[i])?,
            environment: strings.get_req(tr_environments[i])?,
            session_id: strings.get_opt(tr_session_ids[i])?,
            user_id: strings.get_opt(tr_user_ids[i])?,
            workflow_id: strings.get_opt(tr_workflow_ids[i])?,
            parent_trace_id: strings.get_opt(tr_parent_trace_ids[i])?,
            status,
            start_ts_ms,
            end_ts_ms,
            duration_ms: tr_durations[i],
            input: blobs.get_value(tr_input_blobs[i], &mut materialized_bytes)?,
            output: blobs.get_value(tr_output_blobs[i], &mut materialized_bytes)?,
            error_type: strings.get_opt(tr_error_types[i])?,
            error_message: strings.get_opt(tr_error_messages[i])?,
            failure_source,
            metadata: blobs.get_value(tr_metadata_blobs[i], &mut materialized_bytes)?,
            tags,
            span_count: tr_span_counts[i],
            llm_call_count: tr_llm_counts[i],
            tool_call_count: tr_tool_counts[i],
            error_span_count: tr_err_span_counts[i],
            input_tokens: tr_in_tokens[i],
            output_tokens: tr_out_tokens[i],
            cached_tokens: tr_cached_tokens[i],
            total_tokens: tr_total_tokens[i],
            total_cost_nanos,
            total_cost_usd: cost_nanos_to_usd(total_cost_nanos),
            primary_model: strings.get_opt(tr_primary_models[i])?,
            primary_provider: strings.get_opt(tr_primary_providers[i])?,
            avg_eval_score,
            eval_pass_rate,
            has_loop_warning,
        });
    }

    // Section 4: Span Rows
    let span_count = cursor.read_u32("spans.count")? as usize;
    if span_count > MAX_PLST_SPANS {
        return Err(ProtocolError::ResourceLimitExceeded(
            "span count exceeds MAX_PLST_SPANS",
        ));
    }
    let sp_trace_indices = cursor.read_u32_vec(span_count, "spans.trace_indices")?;
    let sp_trace_ids = cursor.read_u32_vec(span_count, "spans.trace_id_str_ids")?;
    let sp_project_ids = cursor.read_u32_vec(span_count, "spans.project_id_str_ids")?;
    let sp_span_ids = cursor.read_u32_vec(span_count, "spans.span_id_str_ids")?;
    let sp_parent_span_ids = cursor.read_u32_vec(span_count, "spans.parent_span_id_str_ids")?;
    let sp_linked_counts = cursor.read_u16_vec(span_count, "spans.linked_counts")?;
    let total_linked = cursor.read_u32("spans.total_linked")? as usize;
    let sum_linked_counts: usize = sp_linked_counts
        .iter()
        .try_fold(0usize, |acc, &c| acc.checked_add(c as usize))
        .ok_or(ProtocolError::ResourceLimitExceeded(
            "linked_span_ids count sum overflow",
        ))?;
    if sum_linked_counts != total_linked {
        return Err(ProtocolError::LengthMismatch {
            expected: sum_linked_counts,
            actual: total_linked,
        });
    }
    let sp_linked_str_ids = cursor.read_u32_vec(total_linked, "spans.linked_span_str_ids")?;
    let sp_depths = cursor.read_i32_vec(span_count, "spans.depths")?;

    let sp_names = cursor.read_u32_vec(span_count, "spans.names")?;
    let sp_types = cursor.read_u8_vec(span_count, "spans.span_types")?;
    let sp_phases = cursor.read_u8_vec(span_count, "spans.span_phases")?;
    let sp_statuses = cursor.read_u8_vec(span_count, "spans.statuses")?;
    let sp_start_deltas = cursor.read_i64_vec(span_count, "spans.start_ts_deltas")?;
    let sp_has_end = cursor.read_u8_vec(span_count, "spans.has_end_ts")?;
    let sp_end_offsets = cursor.read_i64_vec(span_count, "spans.end_ts_offsets")?;
    let sp_durations = cursor.read_i64_vec(span_count, "spans.duration_ms")?;

    let sp_providers = cursor.read_u32_vec(span_count, "spans.providers")?;
    let sp_models = cursor.read_u32_vec(span_count, "spans.models")?;
    let sp_model_ids = cursor.read_u32_vec(span_count, "spans.model_ids")?;
    let sp_model_versions = cursor.read_u32_vec(span_count, "spans.model_versions")?;
    let sp_prompt_tpl_ids = cursor.read_u32_vec(span_count, "spans.prompt_template_ids")?;
    let sp_prompt_tpl_versions =
        cursor.read_u32_vec(span_count, "spans.prompt_template_versions")?;
    let sp_in_tokens = cursor.read_i64_vec(span_count, "spans.input_tokens")?;
    let sp_out_tokens = cursor.read_i64_vec(span_count, "spans.output_tokens")?;
    let sp_cached_tokens = cursor.read_i64_vec(span_count, "spans.cached_tokens")?;
    let sp_total_tokens = cursor.read_i64_vec(span_count, "spans.total_tokens")?;
    let sp_cost_nanos = cursor.read_i64_vec(span_count, "spans.cost_nanos")?;
    let sp_has_temp = cursor.read_u8_vec(span_count, "spans.has_temperature")?;
    let sp_temp_bits = cursor.read_u64_vec(span_count, "spans.temperature_bits")?;
    let sp_finish_reasons = cursor.read_u32_vec(span_count, "spans.finish_reasons")?;
    let sp_model_params_blobs = cursor.read_u32_vec(span_count, "spans.model_params_blobs")?;
    let sp_tool_names = cursor.read_u32_vec(span_count, "spans.tool_names")?;
    let sp_tool_versions = cursor.read_u32_vec(span_count, "spans.tool_versions")?;
    let sp_tool_retries = cursor.read_i32_vec(span_count, "spans.tool_retry_counts")?;

    let sp_error_types = cursor.read_u32_vec(span_count, "spans.error_types")?;
    let sp_error_messages = cursor.read_u32_vec(span_count, "spans.error_messages")?;
    let sp_error_stacks = cursor.read_u32_vec(span_count, "spans.error_stacks")?;
    let sp_input_blobs = cursor.read_u32_vec(span_count, "spans.input_blobs")?;
    let sp_output_blobs = cursor.read_u32_vec(span_count, "spans.output_blobs")?;
    let sp_attr_blobs = cursor.read_u32_vec(span_count, "spans.attributes_blobs")?;
    let sp_meta_blobs = cursor.read_u32_vec(span_count, "spans.metadata_blobs")?;

    let mut spans = Vec::with_capacity(cursor.bounded_capacity(span_count, 16));
    let mut prev_sp_start: i64 = 0;
    let mut linked_cursor: usize = 0;
    for i in 0..span_count {
        let trace_id = strings.get_req(sp_trace_ids[i])?;
        let tr_idx = sp_trace_indices[i];
        if tr_idx != NONE_IDX {
            let tr = traces
                .get(tr_idx as usize)
                .ok_or(ProtocolError::InvalidTraceIndex(tr_idx))?;
            if tr.trace_id != trace_id {
                return Err(ProtocolError::InvalidTraceIndex(tr_idx));
            }
        }

        let start_ts_ms = prev_sp_start.wrapping_add(sp_start_deltas[i]);
        prev_sp_start = start_ts_ms;
        let end_ts_ms = match sp_has_end[i] {
            0 => None,
            1 => Some(start_ts_ms.wrapping_add(sp_end_offsets[i])),
            tag => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "span_has_end_ts",
                    tag,
                })
            }
        };
        let span_type = SpanType::from_u8(sp_types[i]).ok_or(ProtocolError::InvalidEnumTag {
            kind: "span_type",
            tag: sp_types[i],
        })?;
        let span_phase = SpanPhase::from_u8(sp_phases[i]).ok_or(ProtocolError::InvalidEnumTag {
            kind: "span_phase",
            tag: sp_phases[i],
        })?;
        let status = TraceStatus::from_u8(sp_statuses[i]).ok_or(ProtocolError::InvalidEnumTag {
            kind: "span_status",
            tag: sp_statuses[i],
        })?;
        let temperature = match sp_has_temp[i] {
            0 => None,
            1 => Some(f64::from_bits(sp_temp_bits[i])),
            tag => {
                return Err(ProtocolError::InvalidEnumTag {
                    kind: "has_temperature",
                    tag,
                })
            }
        };
        let lc = sp_linked_counts[i] as usize;
        let mut linked_span_ids = Vec::with_capacity(lc.min(MAX_INITIAL_VEC_CAP));
        for &sid in &sp_linked_str_ids[linked_cursor..linked_cursor + lc] {
            linked_span_ids.push(strings.get_req(sid)?);
        }
        linked_cursor += lc;

        let cost_nanos = sp_cost_nanos[i];
        spans.push(AgentSpan {
            span_id: strings.get_req(sp_span_ids[i])?,
            trace_id,
            project_id: strings.get_req(sp_project_ids[i])?,
            parent_span_id: strings.get_opt(sp_parent_span_ids[i])?,
            linked_span_ids,
            span_phase,
            depth: sp_depths[i],
            name: strings.get_req(sp_names[i])?,
            span_type,
            status,
            start_ts_ms,
            end_ts_ms,
            duration_ms: sp_durations[i],
            provider: strings.get_opt(sp_providers[i])?,
            model: strings.get_opt(sp_models[i])?,
            model_id: strings.get_opt(sp_model_ids[i])?,
            model_version: strings.get_opt(sp_model_versions[i])?,
            prompt_template_id: strings.get_opt(sp_prompt_tpl_ids[i])?,
            prompt_template_version: strings.get_opt(sp_prompt_tpl_versions[i])?,
            input_tokens: sp_in_tokens[i],
            output_tokens: sp_out_tokens[i],
            cached_tokens: sp_cached_tokens[i],
            total_tokens: sp_total_tokens[i],
            cost_nanos,
            cost_usd: cost_nanos_to_usd(cost_nanos),
            temperature,
            finish_reason: strings.get_opt(sp_finish_reasons[i])?,
            model_params: blobs.get_value(sp_model_params_blobs[i], &mut materialized_bytes)?,
            tool_name: strings.get_opt(sp_tool_names[i])?,
            tool_version: strings.get_opt(sp_tool_versions[i])?,
            tool_retry_count: sp_tool_retries[i],
            input: blobs.get_value(sp_input_blobs[i], &mut materialized_bytes)?,
            output: blobs.get_value(sp_output_blobs[i], &mut materialized_bytes)?,
            attributes: blobs.get_value(sp_attr_blobs[i], &mut materialized_bytes)?,
            events: Vec::new(),
            metadata: blobs.get_value(sp_meta_blobs[i], &mut materialized_bytes)?,
            error_type: strings.get_opt(sp_error_types[i])?,
            error_message: strings.get_opt(sp_error_messages[i])?,
            error_stack: strings.get_opt(sp_error_stacks[i])?,
        });
    }

    // Section 5: Columnar Span Events
    let event_count = cursor.read_u32("events.count")? as usize;
    if event_count > MAX_PLST_EVENTS {
        return Err(ProtocolError::ResourceLimitExceeded(
            "event count exceeds MAX_PLST_EVENTS",
        ));
    }
    let ev_span_indices = cursor.read_u32_vec(event_count, "events.span_indices")?;
    let ev_ts_deltas = cursor.read_i64_vec(event_count, "events.timestamp_deltas")?;
    let ev_names = cursor.read_u32_vec(event_count, "events.names")?;
    let ev_attr_blobs = cursor.read_u32_vec(event_count, "events.attributes_blobs")?;

    let mut prev_ev_ts: i64 = 0;
    for i in 0..event_count {
        let s_idx = ev_span_indices[i];
        let span = spans
            .get_mut(s_idx as usize)
            .ok_or(ProtocolError::InvalidSpanIndex(s_idx))?;
        let timestamp_ms = prev_ev_ts.wrapping_add(ev_ts_deltas[i]);
        prev_ev_ts = timestamp_ms;
        span.events.push(SpanEvent {
            name: strings.get_req(ev_names[i])?,
            timestamp_ms,
            attributes: blobs.get_value(ev_attr_blobs[i], &mut materialized_bytes)?,
        });
    }

    if cursor.remaining() != 0 {
        return Err(ProtocolError::TrailingBytes(cursor.remaining()));
    }

    Ok(TraceIngestBatch { traces, spans })
}

fn write_u16_slice(out: &mut Vec<u8>, items: &[u16]) {
    for &v in items {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn write_u32_slice(out: &mut Vec<u8>, items: &[u32]) {
    for &v in items {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn write_i32_slice(out: &mut Vec<u8>, items: &[i32]) {
    for &v in items {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn write_i64_slice(out: &mut Vec<u8>, items: &[i64]) {
    for &v in items {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn write_u64_slice(out: &mut Vec<u8>, items: &[u64]) {
    for &v in items {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn full_sample_batch() -> TraceIngestBatch {
        let shared_prompt = json!({
            "messages": [
                {"role": "system", "content": "You are a helpful billing agent."},
                {"role": "user", "content": "Refund order 123"}
            ]
        });

        let tr1 = AgentTrace {
            trace_id: "tr_1".into(),
            project_id: "proj_1".into(),
            agent_name: "billing-agent".into(),
            agent_version: "2.0.0".into(),
            prompt_version: Some("".into()), // Some("") distinct from None!
            model_config_version: None,
            tool_config_version: Some("tc_3".into()),
            app_release: Some("2026.11".into()),
            environment: "production".into(),
            session_id: Some("sess_1".into()),
            user_id: Some("usr_1".into()),
            workflow_id: Some("wf_refund".into()),
            parent_trace_id: Some("tr_parent".into()),
            status: TraceStatus::Ok,
            start_ts_ms: 1_700_000_000_000,
            end_ts_ms: Some(1_700_000_001_500),
            duration_ms: 1500,
            input: shared_prompt.clone(),
            output: Value::String("Refund approved".into()),
            error_type: None,
            error_message: None,
            failure_source: Some(FailureSource::Application),
            metadata: json!({"region": "us-east"}),
            tags: vec!["billing".into(), "vip".into()],
            span_count: 3,
            llm_call_count: 1,
            tool_call_count: 1,
            error_span_count: 0,
            input_tokens: 120,
            output_tokens: 45,
            cached_tokens: 20,
            total_tokens: 165,
            total_cost_nanos: 2_345_000,
            total_cost_usd: cost_nanos_to_usd(2_345_000),
            primary_model: Some("gpt-4o-mini".into()),
            primary_provider: Some("openai".into()),
            avg_eval_score: Some(0.95),
            eval_pass_rate: Some(1.0),
            has_loop_warning: false,
        };

        let sp1 = AgentSpan {
            span_id: "sp_llm".into(),
            trace_id: "tr_1".into(),
            project_id: "proj_1".into(),
            parent_span_id: None,
            linked_span_ids: vec!["sp_guard".into()],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: "openai.chat".into(),
            span_type: SpanType::Llm,
            status: TraceStatus::Ok,
            start_ts_ms: 1_700_000_000_050,
            end_ts_ms: Some(1_700_000_000_900),
            duration_ms: 850,
            provider: Some("openai".into()),
            model: Some("gpt-4o-mini".into()),
            model_id: Some("gpt-4o-mini-2024-07-18".into()),
            model_version: Some("2024-07-18".into()),
            prompt_template_id: Some("tpl_1".into()),
            prompt_template_version: Some("v2".into()),
            input_tokens: 120,
            output_tokens: 45,
            cached_tokens: 20,
            total_tokens: 165,
            cost_nanos: 2_345_000,
            cost_usd: cost_nanos_to_usd(2_345_000),
            temperature: Some(0.2),
            finish_reason: Some("stop".into()),
            model_params: json!({"max_tokens": 512}),
            tool_name: None,
            tool_version: None,
            tool_retry_count: 0,
            input: shared_prompt, // triggers intra-frame blob dedup with tr1.input
            output: Value::Null,  // distinct from Value::String("")
            attributes: json!({"gen_ai.system": "openai"}),
            events: vec![SpanEvent {
                name: "first_token".into(),
                timestamp_ms: 1_700_000_000_200,
                attributes: json!({"ttft_ms": 150}),
            }],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        };

        let sp2 = AgentSpan {
            span_id: "sp_guard".into(),
            trace_id: "tr_1".into(),
            project_id: "proj_1".into(),
            parent_span_id: Some("sp_llm".into()),
            linked_span_ids: vec![],
            span_phase: SpanPhase::Update,
            depth: 1,
            name: "pii.guardrail".into(),
            span_type: SpanType::Guardrail,
            status: TraceStatus::Running,
            start_ts_ms: 1_700_000_000_950,
            end_ts_ms: None,
            duration_ms: 0,
            provider: None,
            model: None,
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_tokens: 0,
            total_tokens: 0,
            cost_nanos: 0,
            cost_usd: 0.0,
            temperature: None,
            finish_reason: None,
            model_params: Value::Null,
            tool_name: None,
            tool_version: None,
            tool_retry_count: 0,
            input: Value::String("check".into()),
            output: Value::String("".into()),
            attributes: json!({"guardrail": "pii"}),
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        };

        // Orphan / streaming span-only trace_id
        let sp_orphan = AgentSpan {
            span_id: "sp_stream_start".into(),
            trace_id: "tr_streaming_only".into(),
            project_id: "proj_1".into(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: SpanPhase::Start,
            depth: 0,
            name: "tool.lookup".into(),
            span_type: SpanType::Tool,
            status: TraceStatus::Running,
            start_ts_ms: 1_700_000_001_000,
            end_ts_ms: None,
            duration_ms: 0,
            provider: None,
            model: None,
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_tokens: 0,
            total_tokens: 0,
            cost_nanos: 0,
            cost_usd: 0.0,
            temperature: None,
            finish_reason: None,
            model_params: Value::Null,
            tool_name: Some("lookup_order".into()),
            tool_version: Some("1.2".into()),
            tool_retry_count: 1,
            input: json!({"order_id": 123}),
            output: Value::Null,
            attributes: Value::Null,
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        };

        TraceIngestBatch {
            traces: vec![tr1],
            spans: vec![sp1, sp2, sp_orphan],
        }
    }

    #[test]
    fn lossless_roundtrip_across_dict_raw_and_uncompressed() {
        let batch = full_sample_batch();
        for frame in [
            encode_plst_batch(&batch, true).unwrap(),
            encode_plst_batch(&batch, false).unwrap(),
            encode_plst_batch_uncompressed(&batch).unwrap(),
        ] {
            let decoded = decode_plst_batch(&frame).unwrap();
            assert_eq!(decoded, batch);
        }

        // Empty batch also round-trips cleanly
        let empty = TraceIngestBatch::default();
        let empty_frame = encode_plst_batch(&empty, true).unwrap();
        assert_eq!(decode_plst_batch(&empty_frame).unwrap(), empty);
    }

    #[test]
    fn blob_table_deduplicates_identical_blobs_and_distinguishes_text_vs_json() {
        let mut builder = BlobTableBuilder::default();
        let id_str = builder.intern_value(&Value::String("123".into())).unwrap();
        let id_num = builder.intern_value(&json!(123)).unwrap();
        let id_str_dup = builder.intern_value(&Value::String("123".into())).unwrap();
        let id_null = builder.intern_value(&Value::Null).unwrap();

        assert_eq!(id_str, id_str_dup);
        assert_ne!(id_str, id_num);
        assert_eq!(id_null, NONE_IDX);
        assert_eq!(builder.hashes.len(), 2);

        // Synthetic hash-collision test: force two distinct blobs into the same hash bucket
        let mut coll_builder = BlobTableBuilder::default();
        let id_a = coll_builder
            .intern_value(&Value::String("alpha".into()))
            .unwrap();
        let fake_hash = coll_builder.hashes[id_a as usize];
        // Insert a fake candidate for ("omega", len=5) under the exact same bucket key as "alpha"
        let omega_bytes = b"omega";
        let start = coll_builder.data.len();
        coll_builder.data.extend_from_slice(omega_bytes);
        coll_builder.hashes.push(fake_hash);
        coll_builder.encodings.push(BLOB_ENC_UTF8_TEXT);
        coll_builder.ranges.push((start, start + 5));
        coll_builder
            .buckets
            .entry((fake_hash, BLOB_ENC_UTF8_TEXT, 5))
            .or_default()
            .push(1);
        // Searching for "alpha" still returns id_a (0), while "gamma" (len 5) does not match either
        assert_eq!(
            coll_builder
                .intern_value(&Value::String("alpha".into()))
                .unwrap(),
            id_a
        );
        // Simulate a real bucket collision by inserting "gamma" under "alpha"'s bucket key
        let gamma_id = coll_builder
            .intern_value(&Value::String("gamma".into()))
            .unwrap();
        assert_ne!(gamma_id, id_a);
    }

    #[test]
    fn rejects_blob_amplification_exceeding_64_mib() {
        // 1 blob of 512 KiB referenced 140 times (> 64 MiB materialized)
        let big_str = "x".repeat(512 * 1024);
        let mut spans = Vec::with_capacity(140);
        for i in 0..140 {
            spans.push(AgentSpan {
                span_id: format!("sp_{i}"),
                trace_id: "tr_amp".into(),
                project_id: "p1".into(),
                parent_span_id: None,
                linked_span_ids: vec![],
                span_phase: SpanPhase::Final,
                depth: 0,
                name: "s".into(),
                span_type: SpanType::Llm,
                status: TraceStatus::Ok,
                start_ts_ms: 1000,
                end_ts_ms: Some(1001),
                duration_ms: 1,
                provider: None,
                model: None,
                model_id: None,
                model_version: None,
                prompt_template_id: None,
                prompt_template_version: None,
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: 0,
                total_tokens: 0,
                cost_nanos: 0,
                cost_usd: 0.0,
                temperature: None,
                finish_reason: None,
                model_params: Value::Null,
                tool_name: None,
                tool_version: None,
                tool_retry_count: 0,
                input: Value::String(big_str.clone()),
                output: Value::Null,
                attributes: Value::Null,
                events: vec![],
                metadata: Value::Null,
                error_type: None,
                error_message: None,
                error_stack: None,
            });
        }
        let batch = TraceIngestBatch {
            traces: vec![],
            spans,
        };
        let frame = encode_plst_batch(&batch, true).unwrap();
        assert!(matches!(
            decode_plst_batch(&frame),
            Err(ProtocolError::ResourceLimitExceeded(_))
        ));
    }

    #[test]
    fn rejects_string_amplification_exceeding_64_mib() {
        // 1 string of 512 KiB referenced 140 times via trace error_message (> 64 MiB materialized)
        let big_str = "y".repeat(512 * 1024);
        let mut traces = Vec::with_capacity(140);
        for i in 0..140 {
            traces.push(AgentTrace {
                trace_id: format!("tr_{i}"),
                project_id: "p1".into(),
                agent_name: "a".into(),
                agent_version: "1".into(),
                prompt_version: None,
                model_config_version: None,
                tool_config_version: None,
                app_release: None,
                environment: "prod".into(),
                session_id: None,
                user_id: None,
                workflow_id: None,
                parent_trace_id: None,
                status: TraceStatus::Error,
                start_ts_ms: 1000,
                end_ts_ms: Some(1001),
                duration_ms: 1,
                input: Value::Null,
                output: Value::Null,
                error_type: None,
                error_message: Some(big_str.clone()),
                failure_source: None,
                metadata: Value::Null,
                tags: vec![],
                span_count: 0,
                llm_call_count: 0,
                tool_call_count: 0,
                error_span_count: 0,
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: 0,
                total_tokens: 0,
                total_cost_nanos: 0,
                total_cost_usd: 0.0,
                primary_model: None,
                primary_provider: None,
                avg_eval_score: None,
                eval_pass_rate: None,
                has_loop_warning: false,
            });
        }
        let batch = TraceIngestBatch {
            traces,
            spans: vec![],
        };
        let frame = encode_plst_batch(&batch, true).unwrap();
        assert!(matches!(
            decode_plst_batch(&frame),
            Err(ProtocolError::ResourceLimitExceeded(_))
        ));
    }

    #[test]
    fn rejects_hostile_and_corrupted_plst_frames() {
        let valid = encode_plst_batch(&full_sample_batch(), true).unwrap();

        // 1. Bad magic
        let mut bad_magic = valid.clone();
        bad_magic[0] = b'X';
        assert!(matches!(
            decode_plst_batch(&bad_magic),
            Err(ProtocolError::InvalidMagic { .. })
        ));

        // 2. Unsupported version
        let mut bad_ver = valid.clone();
        bad_ver[4] = 99;
        assert!(matches!(
            decode_plst_batch(&bad_ver),
            Err(ProtocolError::UnsupportedVersion(99))
        ));

        // 3. Unknown flags + mutually exclusive compression flags
        let mut bad_flags = valid.clone();
        bad_flags[5] = 0x80;
        assert!(matches!(
            decode_plst_batch(&bad_flags),
            Err(ProtocolError::UnsupportedFlags(0x80))
        ));
        bad_flags[5] = FLAG_RAW_ZSTD | FLAG_DICT_ZSTD;
        assert!(matches!(
            decode_plst_batch(&bad_flags),
            Err(ProtocolError::UnsupportedFlags(_))
        ));

        // 4. Unknown dictionary ID
        let mut bad_dict = valid.clone();
        bad_dict[6..8].copy_from_slice(&99u16.to_le_bytes());
        assert!(matches!(
            decode_plst_batch(&bad_dict),
            Err(ProtocolError::UnknownDictionary(99))
        ));

        // 5. UncompLen > 64 MiB
        let mut huge_uncomp = valid.clone();
        huge_uncomp[8..12].copy_from_slice(&(70 * 1024 * 1024u32).to_le_bytes());
        assert!(matches!(
            decode_plst_batch(&huge_uncomp),
            Err(ProtocolError::ResourceLimitExceeded(_))
        ));

        // 6. Tampered CRC32
        let mut bad_crc = valid.clone();
        bad_crc[16] ^= 0xff;
        assert!(matches!(
            decode_plst_batch(&bad_crc),
            Err(ProtocolError::CrcMismatch { .. })
        ));

        // 7. Trailing bytes
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(matches!(
            decode_plst_batch(&trailing),
            Err(ProtocolError::TrailingBytes(1))
        ));

        // 8. Truncated frame
        assert!(matches!(
            decode_plst_batch(&valid[..10]),
            Err(ProtocolError::TruncatedFrame(_))
        ));

        // 9. Zstd bomb (> 64 MiB decompressed) rejected with bounded memory
        let bomb_raw = vec![0u8; 65 * 1024 * 1024];
        let bomb_zstd = zstd::stream::encode_all(bomb_raw.as_slice(), 1).unwrap();
        let bomb_crc = crc32fast::hash(&bomb_zstd);
        let mut bomb_frame = Vec::with_capacity(PLST_HEADER_LEN + bomb_zstd.len());
        bomb_frame.extend_from_slice(&PLST_MAGIC);
        bomb_frame.push(PLST_VERSION);
        bomb_frame.push(FLAG_RAW_ZSTD | FLAG_FINAL);
        bomb_frame.extend_from_slice(&0u16.to_le_bytes());
        // Lie about uncomp_len (claim 1 KiB) to test streaming decompression cap!
        bomb_frame.extend_from_slice(&1024u32.to_le_bytes());
        bomb_frame.extend_from_slice(&(bomb_zstd.len() as u32).to_le_bytes());
        bomb_frame.extend_from_slice(&bomb_crc.to_le_bytes());
        bomb_frame.extend_from_slice(&bomb_zstd);
        assert!(matches!(
            decode_plst_batch(&bomb_frame),
            Err(ProtocolError::ResourceLimitExceeded(_))
        ));

        // 10. Bogus huge trace count in uncompressed payload
        let mut bogus_payload = Vec::new();
        bogus_payload.extend_from_slice(&0u32.to_le_bytes()); // string_count = 0
        bogus_payload.extend_from_slice(&0u32.to_le_bytes()); // string offset[0] = 0
        bogus_payload.extend_from_slice(&0u32.to_le_bytes()); // blob_count = 0
        bogus_payload.extend_from_slice(&0u32.to_le_bytes()); // blob offset[0] = 0
        bogus_payload.extend_from_slice(&99_999u32.to_le_bytes()); // trace_count = 99_999
        let bogus_crc = crc32fast::hash(&bogus_payload);
        let mut bogus_frame = Vec::with_capacity(PLST_HEADER_LEN + bogus_payload.len());
        bogus_frame.extend_from_slice(&PLST_MAGIC);
        bogus_frame.push(PLST_VERSION);
        bogus_frame.push(FLAG_FINAL);
        bogus_frame.extend_from_slice(&0u16.to_le_bytes());
        bogus_frame.extend_from_slice(&(bogus_payload.len() as u32).to_le_bytes());
        bogus_frame.extend_from_slice(&(bogus_payload.len() as u32).to_le_bytes());
        bogus_frame.extend_from_slice(&bogus_crc.to_le_bytes());
        bogus_frame.extend_from_slice(&bogus_payload);
        assert!(matches!(
            decode_plst_batch(&bogus_frame),
            Err(ProtocolError::TruncatedFrame(_))
        ));
    }
}
