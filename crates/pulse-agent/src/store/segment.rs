use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use serde_json::Value;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

use crate::model::{
    AgentSpan, AgentTrace, FailureSource, SpanEvent, SpanPhase, SpanType, TraceStatus,
};
use crate::pricing::{cost_nanos_to_usd, sync_span_cost, sync_trace_cost};
use crate::protocol::{agent_zstd_dict, MAX_PLST_UNCOMP_BYTES};
use crate::store::bloom::{SegmentIndexSection, SegmentZoneMap, DEFAULT_BLOOM_BITS_PER_KEY};
use crate::store::cache::AgentSegmentCache;
use crate::store::error::{validate_identifier, StoreError, StoreResult};
use crate::store::manifest::SegmentDescriptor;
use crate::store::memtable::{
    env_rank, failure_source_rank, merge_opt_str_max, merge_opt_str_phase_ranked_preserve_empty,
    merge_spans_crdt_with_ranks, merge_trace_headers_with_meta,
    recompute_trace_from_spans_with_explicit, status_rank, RankedTraceEntry, SpanFieldRanks,
    TraceCrdtMeta, TraceFieldRanks,
};

pub const PLSS_MAGIC: [u8; 4] = *b"PLSS";
pub const PLSS_VERSION: u16 = 1;
pub const PLSS_FOOTER_LEN: usize = 24;
pub const PLSS_DIR_ENTRY_LEN: usize = 20;
pub const PLSS_ALIGN_BYTES: usize = pulse_storage::encrypted_store::CHUNK_PLAIN_LEN; // 65_536
pub const MAX_PLSS_SEGMENT_BYTES: usize = 128 * 1024 * 1024;
pub const DEFAULT_CHUNK_TARGET_BYTES: usize = 128 * 1024;
pub const MAX_DECOMPRESSED_SECTION_BYTES: usize = MAX_PLST_UNCOMP_BYTES; // 64 MiB
pub const MAX_SECTION_ENTRIES: usize = 16;
pub const MAX_CHUNK_COUNT: usize = 4_096;
pub const COALESCE_MAX_GAP_BYTES: u64 = 64 * 1024;
pub const COALESCE_MAX_SPAN_BYTES: u64 = 4 * 1024 * 1024;

pub const SEC_FLAG_ZSTD_RAW: u16 = 0x0001;
pub const SEC_FLAG_ZSTD_DICT: u16 = 0x0002;
pub const SEC_FLAG_CHUNKED: u16 = 0x0004;
pub const SEC_FLAG_ALIGNED_64K: u16 = 0x0008;
pub const SEC_FLAG_CRDT_RANKS: u16 = 0x0010;

const CODEC_RAW: u8 = 0;
const CODEC_ZSTD_RAW: u8 = 1;
const CODEC_ZSTD_DICT: u8 = 2;

const BLOB_ENC_JSON_UTF8: u8 = 0;
const BLOB_ENC_UTF8_TEXT: u8 = 1;
pub(crate) const NONE_STR_IDX: u32 = u32::MAX;
const NULL_BLOB_REF: BlobRef = BlobRef {
    pack_block_idx: u32::MAX,
    blob_idx: u32::MAX,
};

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SectionTag {
    StringTable = 0x0001,
    TraceSummary = 0x0002,
    SpanMetrics = 0x0003,
    SpanTree = 0x0004,
    PayloadPack = 0x0005,
    IndexBloom = 0x0006,
    SessionRollup = 0x0007,
}

impl SectionTag {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0001 => Some(Self::StringTable),
            0x0002 => Some(Self::TraceSummary),
            0x0003 => Some(Self::SpanMetrics),
            0x0004 => Some(Self::SpanTree),
            0x0005 => Some(Self::PayloadPack),
            0x0006 => Some(Self::IndexBloom),
            0x0007 => Some(Self::SessionRollup),
            _ => None,
        }
    }
}

/// Exact 20-byte packed little-endian section directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionDirEntry {
    pub tag: u16,
    pub flags: u16,
    pub offset: u64,
    pub len: u32,
    pub crc32: u32,
}

impl SectionDirEntry {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.tag.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.crc32.to_le_bytes());
    }

    pub fn decode(s: &[u8]) -> StoreResult<Self> {
        if s.len() != PLSS_DIR_ENTRY_LEN {
            return Err(StoreError::Corrupted(
                "invalid PLSS directory entry size".into(),
            ));
        }
        Ok(Self {
            tag: u16::from_le_bytes([s[0], s[1]]),
            flags: u16::from_le_bytes([s[2], s[3]]),
            offset: u64::from_le_bytes([s[4], s[5], s[6], s[7], s[8], s[9], s[10], s[11]]),
            len: u32::from_le_bytes([s[12], s[13], s[14], s[15]]),
            crc32: u32::from_le_bytes([s[16], s[17], s[18], s[19]]),
        })
    }
}

/// Exact 24-byte `PLSS` v1 file footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlssFooter {
    pub dir_offset: u64,
    pub dir_len: u32,
    pub dir_crc32: u32,
    pub version_flags: u32,
    pub magic: [u8; 4],
}

impl PlssFooter {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.dir_offset.to_le_bytes());
        out.extend_from_slice(&self.dir_len.to_le_bytes());
        out.extend_from_slice(&self.dir_crc32.to_le_bytes());
        out.extend_from_slice(&self.version_flags.to_le_bytes());
        out.extend_from_slice(&self.magic);
    }

    pub fn decode(s: &[u8]) -> StoreResult<Self> {
        if s.len() != PLSS_FOOTER_LEN {
            return Err(StoreError::Corrupted("invalid PLSS footer size".into()));
        }
        let magic = [s[20], s[21], s[22], s[23]];
        if magic != PLSS_MAGIC {
            return Err(StoreError::Corrupted(format!(
                "invalid PLSS magic: {magic:?}"
            )));
        }
        let version_flags = u32::from_le_bytes([s[16], s[17], s[18], s[19]]);
        let version = (version_flags & 0xFFFF) as u16;
        if version != PLSS_VERSION {
            return Err(StoreError::Corrupted(format!(
                "unsupported PLSS version: {version}"
            )));
        }
        Ok(Self {
            dir_offset: u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]),
            dir_len: u32::from_le_bytes([s[8], s[9], s[10], s[11]]),
            dir_crc32: u32::from_le_bytes([s[12], s[13], s[14], s[15]]),
            version_flags,
            magic,
        })
    }
}

/// EOF-coalesced segment tail (`Tag 0x06` + Section Directory + 24B Footer).
#[derive(Debug, Clone)]
pub struct SegmentTail {
    pub footer: PlssFooter,
    pub directory: Vec<SectionDirEntry>,
    pub index: SegmentIndexSection,
}

impl SegmentTail {
    pub fn find_section(&self, tag: SectionTag) -> Option<&SectionDirEntry> {
        self.directory.iter().find(|e| e.tag == tag as u16)
    }

    pub fn tag02_has_ranks(&self) -> bool {
        self.find_section(SectionTag::TraceSummary)
            .is_some_and(|e| (e.flags & SEC_FLAG_CRDT_RANKS) != 0)
    }

    pub fn approx_bytes(&self) -> usize {
        64 + self.directory.len() * PLSS_DIR_ENTRY_LEN + self.index.approx_bytes()
    }
}

// ---------------------------------------------------------------------------
// Safe Binary Reader & Bounded Zstd Helpers
// ---------------------------------------------------------------------------

struct BinCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BinCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn read_slice(&mut self, len: usize, ctx: &'static str) -> StoreResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(StoreError::ResourceLimitExceeded(ctx))?;
        if end > self.buf.len() {
            return Err(StoreError::Corrupted(format!("truncated {ctx}")));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn read_u8(&mut self, ctx: &'static str) -> StoreResult<u8> {
        Ok(self.read_slice(1, ctx)?[0])
    }

    fn read_u16(&mut self, ctx: &'static str) -> StoreResult<u16> {
        let s = self.read_slice(2, ctx)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }

    fn read_u32(&mut self, ctx: &'static str) -> StoreResult<u32> {
        let s = self.read_slice(4, ctx)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn read_i32(&mut self, ctx: &'static str) -> StoreResult<i32> {
        let s = self.read_slice(4, ctx)?;
        Ok(i32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    fn read_u64(&mut self, ctx: &'static str) -> StoreResult<u64> {
        let s = self.read_slice(8, ctx)?;
        Ok(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    fn read_i64(&mut self, ctx: &'static str) -> StoreResult<i64> {
        let s = self.read_slice(8, ctx)?;
        Ok(i64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    fn read_str(&mut self, ctx: &'static str) -> StoreResult<String> {
        let len = self.read_u32(ctx)? as usize;
        if len > 16 * 1024 * 1024 {
            return Err(StoreError::ResourceLimitExceeded(ctx));
        }
        let s = self.read_slice(len, ctx)?;
        std::str::from_utf8(s)
            .map(str::to_string)
            .map_err(|e| StoreError::Corrupted(format!("invalid UTF-8 in {ctx}: {e}")))
    }

    fn read_opt_str(&mut self, ctx: &'static str) -> StoreResult<Option<String>> {
        let present = self.read_u8(ctx)?;
        match present {
            0 => Ok(None),
            1 => Ok(Some(self.read_str(ctx)?)),
            other => Err(StoreError::Corrupted(format!(
                "invalid presence byte {other} in {ctx}"
            ))),
        }
    }
}

fn write_str(out: &mut Vec<u8>, s: &str) -> StoreResult<()> {
    let b = s.as_bytes();
    let len = u32::try_from(b.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("string exceeds u32"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(b);
    Ok(())
}

fn write_opt_str(out: &mut Vec<u8>, s: Option<&str>) -> StoreResult<()> {
    match s {
        Some(v) => {
            out.push(1);
            write_str(out, v)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn drain_bounded_reader<R: Read>(mut reader: R, max_bytes: usize) -> StoreResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut scratch = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut scratch)?;
        if n == 0 {
            break;
        }
        let next = out
            .len()
            .checked_add(n)
            .ok_or(StoreError::ResourceLimitExceeded("Zstd output overflow"))?;
        if next > max_bytes {
            return Err(StoreError::ResourceLimitExceeded(
                "Zstd decompressed section exceeds 64 MiB limit",
            ));
        }
        out.extend_from_slice(&scratch[..n]);
    }
    Ok(out)
}

fn compress_block(raw: &[u8], use_dict: bool) -> StoreResult<(u8, Vec<u8>)> {
    if raw.is_empty() {
        return Ok((CODEC_RAW, Vec::new()));
    }
    if use_dict {
        let mut enc = zstd::stream::Encoder::with_dictionary(Vec::new(), 3, agent_zstd_dict())?;
        enc.write_all(raw)?;
        Ok((CODEC_ZSTD_DICT, enc.finish()?))
    } else {
        Ok((CODEC_ZSTD_RAW, zstd::stream::encode_all(raw, 3)?))
    }
}

fn decompress_block(
    compressed: &[u8],
    codec: u8,
    expected_uncomp_len: usize,
    expected_crc32: u32,
    ctx: &'static str,
) -> StoreResult<Vec<u8>> {
    if expected_uncomp_len > MAX_DECOMPRESSED_SECTION_BYTES {
        return Err(StoreError::ResourceLimitExceeded(
            "block uncomp_len exceeds 64 MiB limit",
        ));
    }
    let actual_crc = crc32fast::hash(compressed);
    if actual_crc != expected_crc32 {
        return Err(StoreError::ChecksumMismatch {
            context: ctx,
            expected: expected_crc32,
            actual: actual_crc,
        });
    }
    let out = match codec {
        CODEC_RAW => compressed.to_vec(),
        CODEC_ZSTD_RAW => drain_bounded_reader(
            zstd::stream::Decoder::new(compressed)?.single_frame(),
            MAX_DECOMPRESSED_SECTION_BYTES,
        )?,
        CODEC_ZSTD_DICT => drain_bounded_reader(
            zstd::stream::Decoder::with_dictionary(compressed, agent_zstd_dict())?.single_frame(),
            MAX_DECOMPRESSED_SECTION_BYTES,
        )?,
        other => {
            return Err(StoreError::Corrupted(format!(
                "unknown PLSS block codec {other} in {ctx}"
            )));
        }
    };
    if out.len() != expected_uncomp_len {
        return Err(StoreError::Corrupted(format!(
            "{ctx} decompressed length mismatch: expected {expected_uncomp_len}, got {}",
            out.len()
        )));
    }
    Ok(out)
}

fn encode_chunked_envelope(chunks: &[Vec<u8>], use_dict: bool) -> StoreResult<Vec<u8>> {
    let chunk_count = u32::try_from(chunks.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("chunk count exceeds u32"))?;
    let mut dir_bytes = Vec::with_capacity(chunks.len() * 16);
    let mut body_bytes = Vec::new();
    for raw in chunks {
        let uncomp_len = u32::try_from(raw.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("chunk uncomp_len overflow"))?;
        let (codec, comp) = compress_block(raw, use_dict)?;
        let comp_len = u32::try_from(comp.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("chunk comp_len overflow"))?;
        let crc = crc32fast::hash(&comp);
        dir_bytes.extend_from_slice(&uncomp_len.to_le_bytes());
        dir_bytes.extend_from_slice(&comp_len.to_le_bytes());
        dir_bytes.extend_from_slice(&crc.to_le_bytes());
        dir_bytes.push(codec);
        dir_bytes.extend_from_slice(&[0u8; 3]);
        body_bytes.extend_from_slice(&comp);
    }
    let mut out = Vec::with_capacity(4 + dir_bytes.len() + body_bytes.len());
    out.extend_from_slice(&chunk_count.to_le_bytes());
    out.extend_from_slice(&dir_bytes);
    out.extend_from_slice(&body_bytes);
    Ok(out)
}

fn decode_chunked_envelope(buf: &[u8], ctx: &'static str) -> StoreResult<Vec<Vec<u8>>> {
    let mut cur = BinCursor::new(buf);
    let chunk_count = cur.read_u32(ctx)? as usize;
    if chunk_count > MAX_CHUNK_COUNT {
        return Err(StoreError::ResourceLimitExceeded(
            "chunk count exceeds MAX_CHUNK_COUNT",
        ));
    }
    let mut meta = Vec::with_capacity(chunk_count.min(256));
    for _ in 0..chunk_count {
        let uncomp_len = cur.read_u32(ctx)? as usize;
        let comp_len = cur.read_u32(ctx)? as usize;
        let crc32 = cur.read_u32(ctx)?;
        let codec = cur.read_u8(ctx)?;
        let _pad = cur.read_slice(3, ctx)?;
        meta.push((uncomp_len, comp_len, crc32, codec));
    }
    let mut out = Vec::with_capacity(meta.len());
    let mut total_uncomp: usize = 0;
    let max_total_uncomp = MAX_DECOMPRESSED_SECTION_BYTES.saturating_mul(2);
    for (uncomp_len, comp_len, crc32, codec) in meta {
        let comp = cur.read_slice(comp_len, ctx)?;
        let decompressed = decompress_block(comp, codec, uncomp_len, crc32, ctx)?;
        total_uncomp = total_uncomp.checked_add(decompressed.len()).ok_or(
            StoreError::ResourceLimitExceeded("chunked section decompressed bytes overflow"),
        )?;
        if total_uncomp > max_total_uncomp {
            return Err(StoreError::ResourceLimitExceeded(
                "chunked section cumulative decompressed bytes exceed limit",
            ));
        }
        out.push(decompressed);
    }
    if cur.remaining() != 0 {
        return Err(StoreError::Corrupted(format!(
            "trailing bytes in chunked envelope {ctx}"
        )));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tag 0x01: String Table
// ---------------------------------------------------------------------------

struct StringInterner {
    strings: Vec<String>,
    map: HashMap<String, u32>,
}

impl StringInterner {
    fn new() -> Self {
        Self {
            strings: Vec::new(),
            map: HashMap::new(),
        }
    }

    fn intern(&mut self, s: &str) -> StoreResult<u32> {
        if let Some(&id) = self.map.get(s) {
            return Ok(id);
        }
        let id = u32::try_from(self.strings.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("string table overflow"))?;
        if id == NONE_STR_IDX {
            return Err(StoreError::ResourceLimitExceeded("string table overflow"));
        }
        self.strings.push(s.to_string());
        self.map.insert(s.to_string(), id);
        Ok(id)
    }

    fn intern_opt(&mut self, s: Option<&str>) -> StoreResult<u32> {
        match s {
            Some(v) => self.intern(v),
            None => Ok(NONE_STR_IDX),
        }
    }

    fn encode_section(&self) -> StoreResult<Vec<u8>> {
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut cur_strs: Vec<&str> = Vec::new();
        let mut cur_bytes = 0usize;

        for s in &self.strings {
            let add = 4 + s.len();
            if !cur_strs.is_empty() && cur_bytes.saturating_add(add) > DEFAULT_CHUNK_TARGET_BYTES {
                chunks.push(encode_string_chunk(&cur_strs)?);
                cur_strs.clear();
                cur_bytes = 0;
            }
            cur_strs.push(s.as_str());
            cur_bytes = cur_bytes.saturating_add(add);
        }
        if !cur_strs.is_empty() {
            chunks.push(encode_string_chunk(&cur_strs)?);
        }
        encode_chunked_envelope(&chunks, false)
    }
}

fn encode_string_chunk(strs: &[&str]) -> StoreResult<Vec<u8>> {
    let count = u32::try_from(strs.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("string chunk count overflow"))?;
    let total_data: usize = strs.iter().map(|s| s.len()).sum();
    let mut out = Vec::with_capacity(4 + (strs.len() + 1) * 4 + total_data);
    out.extend_from_slice(&count.to_le_bytes());
    let mut off = 0u32;
    out.extend_from_slice(&off.to_le_bytes());
    for s in strs {
        let len = u32::try_from(s.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("string chunk data overflow"))?;
        off = off
            .checked_add(len)
            .ok_or(StoreError::ResourceLimitExceeded(
                "string chunk offset overflow",
            ))?;
        out.extend_from_slice(&off.to_le_bytes());
    }
    for s in strs {
        out.extend_from_slice(s.as_bytes());
    }
    Ok(out)
}

#[derive(Debug, Clone, Default)]
pub struct SegmentStringTable {
    strings: Vec<String>,
}

impl SegmentStringTable {
    pub fn get(&self, id: u32) -> Option<&str> {
        if id == NONE_STR_IDX {
            None
        } else {
            self.strings.get(id as usize).map(String::as_str)
        }
    }

    pub fn get_opt(&self, id: u32, ctx: &'static str) -> StoreResult<Option<&str>> {
        if id == NONE_STR_IDX {
            Ok(None)
        } else {
            self.strings
                .get(id as usize)
                .map(|s| Some(s.as_str()))
                .ok_or_else(|| StoreError::Corrupted(format!("invalid string id {id} in {ctx}")))
        }
    }

    pub fn get_required(&self, id: u32, ctx: &'static str) -> StoreResult<&str> {
        self.get(id)
            .ok_or_else(|| StoreError::Corrupted(format!("invalid string id {id} in {ctx}")))
    }

    pub fn approx_bytes(&self) -> usize {
        self.strings
            .iter()
            .map(|s| 24 + s.len())
            .sum::<usize>()
            .saturating_add(32)
    }

    pub fn decode_section(buf: &[u8]) -> StoreResult<Self> {
        let chunks = decode_chunked_envelope(buf, "Tag 0x01")?;
        let mut strings = Vec::new();
        for chunk in chunks {
            let mut cur = BinCursor::new(&chunk);
            let count = cur.read_u32("Tag 0x01 chunk count")? as usize;
            let off_bytes = (count + 1)
                .checked_mul(4)
                .ok_or(StoreError::ResourceLimitExceeded(
                    "Tag 0x01 offsets overflow",
                ))?;
            if off_bytes > cur.remaining() {
                return Err(StoreError::Corrupted("truncated Tag 0x01 offsets".into()));
            }
            let mut offsets = Vec::with_capacity(count.min(4096) + 1);
            for _ in 0..=count {
                offsets.push(cur.read_u32("Tag 0x01 offset")? as usize);
            }
            let data = cur.read_slice(cur.remaining(), "Tag 0x01 utf8 data")?;
            if offsets[0] != 0 || offsets[count] != data.len() {
                return Err(StoreError::Corrupted(
                    "invalid Tag 0x01 string offsets".into(),
                ));
            }
            for w in offsets.windows(2) {
                if w[1] < w[0] || w[1] > data.len() {
                    return Err(StoreError::Corrupted(
                        "non-monotonic Tag 0x01 offsets".into(),
                    ));
                }
                let s = std::str::from_utf8(&data[w[0]..w[1]]).map_err(|e| {
                    StoreError::Corrupted(format!("invalid UTF-8 in Tag 0x01: {e}"))
                })?;
                strings.push(s.to_string());
            }
        }
        Ok(Self { strings })
    }
}

// ---------------------------------------------------------------------------
// Tag 0x02: Zero-Preview CRDT Trace Summary / Delta Block
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanCrdtDelta {
    pub span_id_str: u32,
    pub span_type: u8,
    pub span_phase: u8,
    pub status: u8,
    pub depth: i32,
    pub start_ts_ms: i64,
    pub end_ts_ms: Option<i64>,
    pub duration_ms: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub total_tokens: i64,
    pub cost_nanos: i64,
    pub model_str: u32,
    pub provider_str: u32,
    pub tool_name_str: u32,
    pub tool_name_phase: u8,
    pub tool_name_ts: i64,
    pub span_name_str: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSummaryRow {
    pub trace_id_str: u32,
    pub session_id_str: u32,
    pub user_id_str: u32,
    pub workflow_id_str: u32,
    pub parent_trace_id_str: u32,
    pub agent_name_str: u32,
    pub agent_version_str: u32,
    pub prompt_version_str: u32,
    pub model_config_version_str: u32,
    pub tool_config_version_str: u32,
    pub app_release_str: u32,
    pub environment_str: u32,
    pub primary_model_str: u32,
    pub primary_provider_str: u32,
    pub error_type_str: u32,
    pub tags_str: Vec<u32>,
    pub status: u8,
    pub failure_source: u8,
    pub has_end_ts: u8,
    pub has_loop_warning: u8,
    pub has_trace_payloads: u8,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub duration_ms: i64,
    pub span_count: i32,
    pub llm_call_count: i32,
    pub tool_call_count: i32,
    pub error_span_count: i32,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub total_tokens: i64,
    pub total_cost_nanos: i64,
    pub explicit_status: u8,
    pub explicit_failure_source: u8,
    pub output_rank: u8,
    pub metadata_rank: u8,
    pub error_type_rank: u8,
    pub error_message_rank: u8,
    pub explicit_has_loop_warning: u8,
    pub explicit_primary_model_str: u32,
    pub explicit_primary_provider_str: u32,
    pub span_crdts: Vec<SpanCrdtDelta>,
}

impl TraceSummaryRow {
    pub fn approx_bytes(&self) -> usize {
        208 + self.tags_str.len() * 4 + self.span_crdts.len() * 104
    }

    pub(crate) fn crdt_meta(&self, strings: &SegmentStringTable) -> StoreResult<TraceCrdtMeta> {
        let explicit_status = TraceStatus::from_u8(self.explicit_status).ok_or_else(|| {
            StoreError::Corrupted(format!("invalid explicit_status {}", self.explicit_status))
        })?;
        let explicit_failure_source = if self.explicit_failure_source == 0 {
            None
        } else {
            Some(
                FailureSource::from_u8(self.explicit_failure_source).ok_or_else(|| {
                    StoreError::Corrupted(format!(
                        "invalid explicit_failure_source {}",
                        self.explicit_failure_source
                    ))
                })?,
            )
        };
        if self.output_rank > 4
            || self.metadata_rank > 4
            || self.error_type_rank > 4
            || self.error_message_rank > 4
        {
            return Err(StoreError::Corrupted(
                "invalid TraceFieldRanks value (> 4) in Tag 0x02".into(),
            ));
        }
        if self.explicit_has_loop_warning > 1 {
            return Err(StoreError::Corrupted(format!(
                "invalid explicit_has_loop_warning {} in Tag 0x02",
                self.explicit_has_loop_warning
            )));
        }
        let explicit_primary_model = strings
            .get_opt(self.explicit_primary_model_str, "explicit_primary_model")?
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let explicit_primary_provider = strings
            .get_opt(
                self.explicit_primary_provider_str,
                "explicit_primary_provider",
            )?
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        Ok(TraceCrdtMeta {
            field_ranks: TraceFieldRanks {
                output_rank: self.output_rank,
                metadata_rank: self.metadata_rank,
                error_type_rank: self.error_type_rank,
                error_message_rank: self.error_message_rank,
            },
            explicit_status,
            explicit_failure_source,
            explicit_has_loop_warning: self.explicit_has_loop_warning != 0,
            explicit_primary_model,
            explicit_primary_provider,
        })
    }

    pub fn to_agent_trace(
        &self,
        project_id: &str,
        strings: &SegmentStringTable,
    ) -> StoreResult<AgentTrace> {
        let opt_s = |id: u32, ctx: &'static str| -> StoreResult<Option<String>> {
            strings.get_opt(id, ctx).map(|o| o.map(str::to_string))
        };
        let req_s = |id: u32, ctx: &'static str| -> StoreResult<String> {
            strings.get_required(id, ctx).map(str::to_string)
        };
        strings.get_opt(self.explicit_primary_model_str, "explicit_primary_model")?;
        strings.get_opt(
            self.explicit_primary_provider_str,
            "explicit_primary_provider",
        )?;
        let status = TraceStatus::from_u8(self.status).ok_or_else(|| {
            StoreError::Corrupted(format!("invalid trace status {}", self.status))
        })?;
        let failure_source = if self.failure_source == 0 {
            None
        } else {
            Some(FailureSource::from_u8(self.failure_source).ok_or_else(|| {
                StoreError::Corrupted(format!("invalid failure_source {}", self.failure_source))
            })?)
        };
        let mut tags = Vec::with_capacity(self.tags_str.len());
        for &tid in &self.tags_str {
            tags.push(strings.get_required(tid, "trace tag")?.to_string());
        }
        Ok(AgentTrace {
            trace_id: req_s(self.trace_id_str, "trace_id")?,
            project_id: project_id.to_string(),
            agent_name: req_s(self.agent_name_str, "agent_name")?,
            agent_version: req_s(self.agent_version_str, "agent_version")?,
            prompt_version: opt_s(self.prompt_version_str, "prompt_version")?,
            model_config_version: opt_s(self.model_config_version_str, "model_config_version")?,
            tool_config_version: opt_s(self.tool_config_version_str, "tool_config_version")?,
            app_release: opt_s(self.app_release_str, "app_release")?,
            environment: req_s(self.environment_str, "environment")?,
            session_id: opt_s(self.session_id_str, "session_id")?,
            user_id: opt_s(self.user_id_str, "user_id")?,
            workflow_id: opt_s(self.workflow_id_str, "workflow_id")?,
            parent_trace_id: opt_s(self.parent_trace_id_str, "parent_trace_id")?,
            status,
            start_ts_ms: self.start_ts_ms,
            end_ts_ms: (self.has_end_ts != 0).then_some(self.end_ts_ms),
            duration_ms: self.duration_ms,
            input: Value::Null,
            output: Value::Null,
            error_type: opt_s(self.error_type_str, "error_type")?,
            error_message: None,
            failure_source,
            metadata: Value::Null,
            tags,
            span_count: self.span_count,
            llm_call_count: self.llm_call_count,
            tool_call_count: self.tool_call_count,
            error_span_count: self.error_span_count,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cached_tokens: self.cached_tokens,
            total_tokens: self.total_tokens,
            total_cost_nanos: self.total_cost_nanos,
            total_cost_usd: cost_nanos_to_usd(self.total_cost_nanos),
            primary_model: opt_s(self.primary_model_str, "primary_model")?,
            primary_provider: opt_s(self.primary_provider_str, "primary_provider")?,
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning: self.has_loop_warning != 0,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedSpanCrdt {
    pub span_id: String,
    pub span_type: u8,
    pub span_phase: u8,
    pub status: u8,
    pub depth: i32,
    pub start_ts_ms: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub total_tokens: i64,
    pub cost_nanos: i64,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub tool_name: Option<String>,
    pub tool_name_rank: Option<(u8, i64)>,
    pub span_name: Option<String>,
    pub has_v1_gap: bool,
}

fn detect_execution_loop_crdts(crdts: &HashMap<String, ResolvedSpanCrdt>) -> bool {
    let mut ordered: Vec<&ResolvedSpanCrdt> = crdts.values().collect();
    ordered.sort_by(|a, b| {
        a.start_ts_ms
            .cmp(&b.start_ts_ms)
            .then_with(|| a.span_id.cmp(&b.span_id))
            .then_with(|| a.tool_name.as_deref().cmp(&b.tool_name.as_deref()))
            .then_with(|| a.span_name.as_deref().cmp(&b.span_name.as_deref()))
    });

    let mut llm_count = 0usize;
    let mut tool_counts: HashMap<&str, usize> = HashMap::new();
    let mut last_tool: Option<&str> = None;
    let mut consecutive = 0usize;

    for c in ordered {
        if c.span_type == SpanType::Llm.to_u8() {
            llm_count += 1;
        } else if c.span_type == SpanType::Tool.to_u8() {
            if let Some(name) = c.tool_name.as_deref().or(c.span_name.as_deref()) {
                let total = tool_counts.entry(name).or_insert(0);
                *total += 1;
                if last_tool == Some(name) {
                    consecutive += 1;
                } else {
                    last_tool = Some(name);
                    consecutive = 1;
                }
                if consecutive >= 4 || *total >= 6 {
                    return true;
                }
            }
        }
    }
    llm_count >= 12
}

fn classify_failure_source_crdts(crdts: &HashMap<String, ResolvedSpanCrdt>) -> FailureSource {
    let mut err_spans: Vec<&ResolvedSpanCrdt> = crdts
        .values()
        .filter(|c| {
            c.status == TraceStatus::Error.to_u8() || c.status == TraceStatus::Timeout.to_u8()
        })
        .collect();
    if err_spans.is_empty() {
        return FailureSource::Unknown;
    }
    err_spans.sort_by(|a, b| {
        b.depth
            .cmp(&a.depth)
            .then_with(|| a.start_ts_ms.cmp(&b.start_ts_ms))
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    let root = err_spans[0];
    match SpanType::from_u8(root.span_type).unwrap_or(SpanType::Custom) {
        SpanType::Llm | SpanType::Embedding => FailureSource::Model,
        SpanType::Tool => FailureSource::Tool,
        SpanType::Http | SpanType::Db | SpanType::Retrieval => FailureSource::External,
        SpanType::Agent | SpanType::Workflow | SpanType::Guardrail | SpanType::Custom => {
            FailureSource::Application
        }
    }
}

/// CRDT-merges two `TraceSummaryRow`s across segments by deterministic status precedence
/// and `span_id` deduplication (`Final > Update > Start`).
#[allow(dead_code)]
pub(crate) fn merge_trace_summary_rows(
    dst: &mut AgentTrace,
    dst_crdts: &mut HashMap<String, ResolvedSpanCrdt>,
    src_trace: AgentTrace,
    src_crdts: Vec<ResolvedSpanCrdt>,
) {
    let mut dst_ranks = TraceFieldRanks::from_trace(dst);
    let src_ranks = TraceFieldRanks::from_trace(&src_trace);
    merge_trace_summary_rows_ranked(
        dst,
        &mut dst_ranks,
        dst_crdts,
        src_trace,
        src_ranks,
        src_crdts,
    );
}

#[allow(dead_code)]
pub(crate) fn merge_trace_summary_rows_ranked(
    dst: &mut AgentTrace,
    dst_ranks: &mut TraceFieldRanks,
    dst_crdts: &mut HashMap<String, ResolvedSpanCrdt>,
    src_trace: AgentTrace,
    src_ranks: TraceFieldRanks,
    src_crdts: Vec<ResolvedSpanCrdt>,
) {
    let mut dst_meta = TraceCrdtMeta {
        field_ranks: *dst_ranks,
        explicit_status: dst.status,
        explicit_failure_source: dst.failure_source,
        explicit_has_loop_warning: dst.has_loop_warning,
        explicit_primary_model: dst.primary_model.clone().filter(|s| !s.is_empty()),
        explicit_primary_provider: dst.primary_provider.clone().filter(|s| !s.is_empty()),
    };
    let src_meta = TraceCrdtMeta {
        field_ranks: src_ranks,
        explicit_status: src_trace.status,
        explicit_failure_source: src_trace.failure_source,
        explicit_has_loop_warning: src_trace.has_loop_warning,
        explicit_primary_model: src_trace.primary_model.clone().filter(|s| !s.is_empty()),
        explicit_primary_provider: src_trace.primary_provider.clone().filter(|s| !s.is_empty()),
    };
    merge_trace_summary_rows_with_meta(
        dst,
        &mut dst_meta,
        dst_crdts,
        src_trace,
        src_meta,
        src_crdts,
    );
    *dst_ranks = dst_meta.field_ranks;
}

pub(crate) fn merge_trace_summary_rows_with_meta(
    dst: &mut AgentTrace,
    dst_meta: &mut TraceCrdtMeta,
    dst_crdts: &mut HashMap<String, ResolvedSpanCrdt>,
    src_trace: AgentTrace,
    src_meta: TraceCrdtMeta,
    src_crdts: Vec<ResolvedSpanCrdt>,
) {
    merge_trace_headers_with_meta(dst, dst_meta, src_trace, src_meta);
    for c in src_crdts {
        let new_rank = (c.span_phase, c.start_ts_ms);
        dst_crdts
            .entry(c.span_id.clone())
            .and_modify(|cur| {
                let cur_rank = (cur.span_phase, cur.start_ts_ms);
                if new_rank > cur_rank {
                    let mut next = c.clone();
                    if next.span_phase == cur.span_phase {
                        if let (Some(cur_st), Some(new_st)) = (
                            TraceStatus::from_u8(cur.status),
                            TraceStatus::from_u8(next.status),
                        ) {
                            if status_rank(cur_st) > status_rank(new_st) {
                                next.status = cur.status;
                            }
                        }
                    }
                    merge_opt_str_max(&mut next.model, cur.model.take());
                    merge_opt_str_max(&mut next.provider, cur.provider.take());
                    next.has_v1_gap |= cur.has_v1_gap;
                    merge_opt_str_phase_ranked_preserve_empty(
                        &mut next.tool_name,
                        &mut next.tool_name_rank,
                        cur.tool_name.take(),
                        cur.tool_name_rank,
                    );
                    if next.span_name.is_none() {
                        next.span_name = cur.span_name.take();
                    }
                    *cur = next;
                } else if new_rank < cur_rank {
                    if c.span_phase == cur.span_phase {
                        if let (Some(cur_st), Some(new_st)) = (
                            TraceStatus::from_u8(cur.status),
                            TraceStatus::from_u8(c.status),
                        ) {
                            if status_rank(new_st) > status_rank(cur_st) {
                                cur.status = c.status;
                            }
                        }
                    }
                    merge_opt_str_max(&mut cur.model, c.model.clone());
                    merge_opt_str_max(&mut cur.provider, c.provider.clone());
                    cur.has_v1_gap |= c.has_v1_gap;
                    merge_opt_str_phase_ranked_preserve_empty(
                        &mut cur.tool_name,
                        &mut cur.tool_name_rank,
                        c.tool_name.clone(),
                        c.tool_name_rank,
                    );
                    if cur.span_name.is_none() {
                        cur.span_name = c.span_name.clone();
                    }
                } else {
                    if let (Some(cur_st), Some(new_st)) = (
                        TraceStatus::from_u8(cur.status),
                        TraceStatus::from_u8(c.status),
                    ) {
                        if status_rank(new_st) > status_rank(cur_st) {
                            cur.status = c.status;
                        }
                    }
                    cur.depth = cur.depth.max(c.depth);
                    let cur_st_rank = match SpanType::from_u8(cur.span_type) {
                        Some(SpanType::Custom) | None => 0u8,
                        Some(st) => st.to_u8().saturating_add(1),
                    };
                    let new_st_rank = match SpanType::from_u8(c.span_type) {
                        Some(SpanType::Custom) | None => 0u8,
                        Some(st) => st.to_u8().saturating_add(1),
                    };
                    if new_st_rank > cur_st_rank {
                        cur.span_type = c.span_type;
                    }
                    merge_opt_str_max(&mut cur.model, c.model.clone());
                    merge_opt_str_max(&mut cur.provider, c.provider.clone());
                    cur.has_v1_gap |= c.has_v1_gap;
                    merge_opt_str_phase_ranked_preserve_empty(
                        &mut cur.tool_name,
                        &mut cur.tool_name_rank,
                        c.tool_name.clone(),
                        c.tool_name_rank,
                    );
                    match (&mut cur.span_name, c.span_name.as_ref()) {
                        (None, Some(sn)) => cur.span_name = Some(sn.clone()),
                        (Some(cur_sn), Some(new_sn)) if new_sn > cur_sn => {
                            *cur_sn = new_sn.clone();
                        }
                        _ => {}
                    }
                    cur.input_tokens = cur.input_tokens.max(c.input_tokens).max(0);
                    cur.output_tokens = cur.output_tokens.max(c.output_tokens).max(0);
                    cur.cached_tokens = cur.cached_tokens.max(c.cached_tokens).max(0);
                    cur.total_tokens = cur.total_tokens.max(c.total_tokens).max(0);
                    cur.cost_nanos = cur.cost_nanos.max(c.cost_nanos).max(0);
                }
            })
            .or_insert(c);
    }

    dst.status = dst_meta.explicit_status;
    dst.failure_source = dst_meta.explicit_failure_source;
    dst.has_loop_warning = dst_meta.explicit_has_loop_warning;
    dst.primary_model = dst_meta.explicit_primary_model.clone();
    dst.primary_provider = dst_meta.explicit_primary_provider.clone();
    if !dst_crdts.is_empty() {
        let mut llm_calls = 0i32;
        let mut tool_calls = 0i32;
        let mut err_spans = 0i32;
        let mut max_span_status: Option<TraceStatus> = None;
        let mut in_t = 0i64;
        let mut out_t = 0i64;
        let mut cac_t = 0i64;
        let mut tot_t = 0i64;
        let mut cost_n = 0i64;
        for c in dst_crdts.values() {
            if c.span_type == SpanType::Llm.to_u8() {
                llm_calls = llm_calls.saturating_add(1);
                merge_opt_str_max(&mut dst.primary_model, c.model.clone());
                merge_opt_str_max(&mut dst.primary_provider, c.provider.clone());
            } else if c.span_type == SpanType::Tool.to_u8() {
                tool_calls = tool_calls.saturating_add(1);
            }
            if let Some(st) = TraceStatus::from_u8(c.status) {
                if max_span_status.is_none_or(|cur| status_rank(st) > status_rank(cur)) {
                    max_span_status = Some(st);
                }
            }
            if c.status == TraceStatus::Error.to_u8() || c.status == TraceStatus::Timeout.to_u8() {
                err_spans = err_spans.saturating_add(1);
            }
            let c_in = c.input_tokens.max(0);
            let c_out = c.output_tokens.max(0);
            in_t = in_t.saturating_add(c_in);
            out_t = out_t.saturating_add(c_out);
            cac_t = cac_t.saturating_add(c.cached_tokens.max(0));
            tot_t = tot_t.saturating_add(c.total_tokens.max(c_in.saturating_add(c_out)).max(0));
            cost_n = cost_n.saturating_add(c.cost_nanos.max(0));
        }
        dst.span_count = i32::try_from(dst_crdts.len()).unwrap_or(i32::MAX);
        dst.llm_call_count = llm_calls;
        dst.tool_call_count = tool_calls;
        dst.error_span_count = err_spans;
        dst.input_tokens = in_t;
        dst.output_tokens = out_t;
        dst.cached_tokens = cac_t;
        dst.total_tokens = tot_t.max(dst.total_tokens);
        if cost_n > 0 {
            dst.total_cost_nanos = cost_n;
            dst.total_cost_usd = cost_nanos_to_usd(cost_n);
        }
        if let Some(ms) = max_span_status {
            if status_rank(ms) > status_rank(dst.status) {
                dst.status = ms;
            }
        }
        if err_spans > 0 || matches!(dst.status, TraceStatus::Error | TraceStatus::Timeout) {
            let span_fs = classify_failure_source_crdts(dst_crdts);
            if failure_source_rank(Some(span_fs)) > failure_source_rank(dst.failure_source) {
                dst.failure_source = Some(span_fs);
            }
        }
        if detect_execution_loop_crdts(dst_crdts) {
            dst.has_loop_warning = true;
        }
    }
}

fn encode_trace_summary_section(rows: &[TraceSummaryRow]) -> StoreResult<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut buf = Vec::new();
    let mut row_count = 0u32;

    let flush_chunk = |chunks: &mut Vec<Vec<u8>>, buf: &mut Vec<u8>, cnt: &mut u32| {
        if *cnt == 0 {
            return;
        }
        let mut chunk = Vec::with_capacity(4 + buf.len());
        chunk.extend_from_slice(&cnt.to_le_bytes());
        chunk.extend_from_slice(buf);
        chunks.push(chunk);
        buf.clear();
        *cnt = 0;
    };

    for row in rows {
        let est = 166 + row.tags_str.len() * 4 + row.span_crdts.len() * 101;
        if row_count > 0 && buf.len().saturating_add(est) > DEFAULT_CHUNK_TARGET_BYTES {
            flush_chunk(&mut chunks, &mut buf, &mut row_count);
        }
        buf.extend_from_slice(&row.trace_id_str.to_le_bytes());
        buf.extend_from_slice(&row.session_id_str.to_le_bytes());
        buf.extend_from_slice(&row.user_id_str.to_le_bytes());
        buf.extend_from_slice(&row.workflow_id_str.to_le_bytes());
        buf.extend_from_slice(&row.parent_trace_id_str.to_le_bytes());
        buf.extend_from_slice(&row.agent_name_str.to_le_bytes());
        buf.extend_from_slice(&row.agent_version_str.to_le_bytes());
        buf.extend_from_slice(&row.prompt_version_str.to_le_bytes());
        buf.extend_from_slice(&row.model_config_version_str.to_le_bytes());
        buf.extend_from_slice(&row.tool_config_version_str.to_le_bytes());
        buf.extend_from_slice(&row.app_release_str.to_le_bytes());
        buf.extend_from_slice(&row.environment_str.to_le_bytes());
        buf.extend_from_slice(&row.primary_model_str.to_le_bytes());
        buf.extend_from_slice(&row.primary_provider_str.to_le_bytes());
        buf.extend_from_slice(&row.error_type_str.to_le_bytes());

        let tag_cnt = u16::try_from(row.tags_str.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("trace tags overflow"))?;
        buf.extend_from_slice(&tag_cnt.to_le_bytes());
        for &t in &row.tags_str {
            buf.extend_from_slice(&t.to_le_bytes());
        }

        buf.push(row.status);
        buf.push(row.failure_source);
        buf.push(row.has_end_ts);
        buf.push(row.has_loop_warning);
        buf.push(row.has_trace_payloads);
        buf.extend_from_slice(&row.start_ts_ms.to_le_bytes());
        buf.extend_from_slice(&row.end_ts_ms.to_le_bytes());
        buf.extend_from_slice(&row.duration_ms.to_le_bytes());
        buf.extend_from_slice(&row.span_count.to_le_bytes());
        buf.extend_from_slice(&row.llm_call_count.to_le_bytes());
        buf.extend_from_slice(&row.tool_call_count.to_le_bytes());
        buf.extend_from_slice(&row.error_span_count.to_le_bytes());
        buf.extend_from_slice(&row.input_tokens.to_le_bytes());
        buf.extend_from_slice(&row.output_tokens.to_le_bytes());
        buf.extend_from_slice(&row.cached_tokens.to_le_bytes());
        buf.extend_from_slice(&row.total_tokens.to_le_bytes());
        buf.extend_from_slice(&row.total_cost_nanos.to_le_bytes());
        buf.push(row.explicit_status);
        buf.push(row.explicit_failure_source);
        buf.push(row.output_rank);
        buf.push(row.metadata_rank);
        buf.push(row.error_type_rank);
        buf.push(row.error_message_rank);
        buf.push(row.explicit_has_loop_warning);
        buf.extend_from_slice(&row.explicit_primary_model_str.to_le_bytes());
        buf.extend_from_slice(&row.explicit_primary_provider_str.to_le_bytes());

        let crdt_cnt = u32::try_from(row.span_crdts.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("span crdt count overflow"))?;
        buf.extend_from_slice(&crdt_cnt.to_le_bytes());
        for c in &row.span_crdts {
            buf.extend_from_slice(&c.span_id_str.to_le_bytes());
            buf.push(c.span_type);
            buf.push(c.span_phase);
            buf.push(c.status);
            buf.push(u8::from(c.end_ts_ms.is_some()));
            buf.extend_from_slice(&c.depth.to_le_bytes());
            buf.extend_from_slice(&c.start_ts_ms.to_le_bytes());
            buf.extend_from_slice(&c.end_ts_ms.unwrap_or(0).to_le_bytes());
            buf.extend_from_slice(&c.duration_ms.to_le_bytes());
            buf.extend_from_slice(&c.input_tokens.to_le_bytes());
            buf.extend_from_slice(&c.output_tokens.to_le_bytes());
            buf.extend_from_slice(&c.cached_tokens.to_le_bytes());
            buf.extend_from_slice(&c.total_tokens.to_le_bytes());
            buf.extend_from_slice(&c.cost_nanos.to_le_bytes());
            buf.extend_from_slice(&c.model_str.to_le_bytes());
            buf.extend_from_slice(&c.provider_str.to_le_bytes());
            buf.extend_from_slice(&c.tool_name_str.to_le_bytes());
            buf.push(c.tool_name_phase);
            buf.extend_from_slice(&c.tool_name_ts.to_le_bytes());
            buf.extend_from_slice(&c.span_name_str.to_le_bytes());
        }
        row_count = row_count.saturating_add(1);
    }
    flush_chunk(&mut chunks, &mut buf, &mut row_count);
    encode_chunked_envelope(&chunks, false)
}

#[allow(dead_code)]
fn decode_trace_summary_section(buf: &[u8]) -> StoreResult<Vec<TraceSummaryRow>> {
    decode_trace_summary_section_with_ranks(buf, true)
}

fn decode_trace_summary_section_with_ranks(
    buf: &[u8],
    has_ranks: bool,
) -> StoreResult<Vec<TraceSummaryRow>> {
    let chunks = decode_chunked_envelope(buf, "Tag 0x02")?;
    let mut rows = Vec::new();
    let min_row = if has_ranks { 166 } else { 151 };
    for chunk in chunks {
        let mut cur = BinCursor::new(&chunk);
        let cnt = cur.read_u32("Tag 0x02 count")? as usize;
        if cnt > cur.remaining() / min_row + 1 {
            return Err(StoreError::Corrupted(
                "Tag 0x02 row count exceeds remaining".into(),
            ));
        }
        for _ in 0..cnt {
            let trace_id_str = cur.read_u32("Tag 0x02")?;
            let session_id_str = cur.read_u32("Tag 0x02")?;
            let user_id_str = cur.read_u32("Tag 0x02")?;
            let workflow_id_str = cur.read_u32("Tag 0x02")?;
            let parent_trace_id_str = cur.read_u32("Tag 0x02")?;
            let agent_name_str = cur.read_u32("Tag 0x02")?;
            let agent_version_str = cur.read_u32("Tag 0x02")?;
            let prompt_version_str = cur.read_u32("Tag 0x02")?;
            let model_config_version_str = cur.read_u32("Tag 0x02")?;
            let tool_config_version_str = cur.read_u32("Tag 0x02")?;
            let app_release_str = cur.read_u32("Tag 0x02")?;
            let environment_str = cur.read_u32("Tag 0x02")?;
            let primary_model_str = cur.read_u32("Tag 0x02")?;
            let primary_provider_str = cur.read_u32("Tag 0x02")?;
            let error_type_str = cur.read_u32("Tag 0x02")?;

            let tag_cnt = cur.read_u16("Tag 0x02 tag_cnt")? as usize;
            let mut tags_str = Vec::with_capacity(tag_cnt.min(64));
            for _ in 0..tag_cnt {
                tags_str.push(cur.read_u32("Tag 0x02 tag")?);
            }

            let status = cur.read_u8("Tag 0x02 status")?;
            let parsed_st = TraceStatus::from_u8(status).ok_or_else(|| {
                StoreError::Corrupted(format!("invalid Tag 0x02 status {status}"))
            })?;
            let failure_source = cur.read_u8("Tag 0x02 failure_source")?;
            if failure_source != 0 && FailureSource::from_u8(failure_source).is_none() {
                return Err(StoreError::Corrupted(format!(
                    "invalid Tag 0x02 failure_source {failure_source}"
                )));
            }
            let has_end_ts = cur.read_u8("Tag 0x02 has_end_ts")?;
            let has_loop_warning = cur.read_u8("Tag 0x02 has_loop_warning")?;
            let has_trace_payloads = cur.read_u8("Tag 0x02 has_trace_payloads")?;
            let start_ts_ms = cur.read_i64("Tag 0x02")?;
            let end_ts_ms = cur.read_i64("Tag 0x02")?;
            let duration_ms = cur.read_i64("Tag 0x02")?;
            let span_count = cur.read_i32("Tag 0x02")?;
            let llm_call_count = cur.read_i32("Tag 0x02")?;
            let tool_call_count = cur.read_i32("Tag 0x02")?;
            let error_span_count = cur.read_i32("Tag 0x02")?;
            let input_tokens = cur.read_i64("Tag 0x02")?;
            let output_tokens = cur.read_i64("Tag 0x02")?;
            let cached_tokens = cur.read_i64("Tag 0x02")?;
            let total_tokens = cur.read_i64("Tag 0x02")?;
            let total_cost_nanos = cur.read_i64("Tag 0x02")?;

            let (
                explicit_status,
                explicit_failure_source,
                output_rank,
                metadata_rank,
                error_type_rank,
                error_message_rank,
                explicit_has_loop_warning,
                explicit_primary_model_str,
                explicit_primary_provider_str,
            ) = if has_ranks {
                let es = cur.read_u8("Tag 0x02 explicit_status")?;
                let efs = cur.read_u8("Tag 0x02 explicit_failure_source")?;
                let or = cur.read_u8("Tag 0x02 output_rank")?;
                let mr = cur.read_u8("Tag 0x02 metadata_rank")?;
                let etr = cur.read_u8("Tag 0x02 error_type_rank")?;
                let emr = cur.read_u8("Tag 0x02 error_message_rank")?;
                let ehlw = cur.read_u8("Tag 0x02 explicit_has_loop_warning")?;
                let epm_str = cur.read_u32("Tag 0x02 explicit_primary_model_str")?;
                let epp_str = cur.read_u32("Tag 0x02 explicit_primary_provider_str")?;
                if TraceStatus::from_u8(es).is_none() {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x02 explicit_status {es}"
                    )));
                }
                if efs != 0 && FailureSource::from_u8(efs).is_none() {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x02 explicit_failure_source {efs}"
                    )));
                }
                if or > 4 || mr > 4 || etr > 4 || emr > 4 {
                    return Err(StoreError::Corrupted(
                        "invalid Tag 0x02 field rank (> 4)".into(),
                    ));
                }
                if ehlw > 1 {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x02 explicit_has_loop_warning {ehlw}"
                    )));
                }
                (es, efs, or, mr, etr, emr, ehlw, epm_str, epp_str)
            } else {
                let etr = if error_type_str != NONE_STR_IDX {
                    status_rank(parsed_st)
                } else {
                    0
                };
                (
                    status,
                    failure_source,
                    0,
                    0,
                    etr,
                    0,
                    u8::from(has_loop_warning != 0),
                    primary_model_str,
                    primary_provider_str,
                )
            };

            let crdt_cnt = cur.read_u32("Tag 0x02 crdt_cnt")? as usize;
            let min_crdt = if has_ranks { 101 } else { 84 };
            if crdt_cnt > cur.remaining() / min_crdt + 1 {
                return Err(StoreError::Corrupted(
                    "Tag 0x02 crdt_cnt exceeds remaining".into(),
                ));
            }
            let mut span_crdts = Vec::with_capacity(crdt_cnt.min(1024));
            for _ in 0..crdt_cnt {
                let span_id_str = cur.read_u32("Tag 0x02 crdt")?;
                let span_type = cur.read_u8("Tag 0x02 crdt")?;
                if SpanType::from_u8(span_type).is_none() {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x02 crdt span_type {span_type}"
                    )));
                }
                let span_phase = cur.read_u8("Tag 0x02 crdt")?;
                if SpanPhase::from_u8(span_phase).is_none() {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x02 crdt span_phase {span_phase}"
                    )));
                }
                let sp_status = cur.read_u8("Tag 0x02 crdt")?;
                if TraceStatus::from_u8(sp_status).is_none() {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x02 crdt status {sp_status}"
                    )));
                }
                let has_sp_end = cur.read_u8("Tag 0x02 crdt")?;
                let depth = cur.read_i32("Tag 0x02 crdt")?;
                let sp_start = cur.read_i64("Tag 0x02 crdt")?;
                let sp_end = cur.read_i64("Tag 0x02 crdt")?;
                let sp_dur = cur.read_i64("Tag 0x02 crdt")?;
                let sp_in = cur.read_i64("Tag 0x02 crdt")?;
                let sp_out = cur.read_i64("Tag 0x02 crdt")?;
                let sp_cac = cur.read_i64("Tag 0x02 crdt")?;
                let sp_tot = cur.read_i64("Tag 0x02 crdt")?;
                let sp_cost = cur.read_i64("Tag 0x02 crdt")?;
                let model_str = cur.read_u32("Tag 0x02 crdt")?;
                let provider_str = cur.read_u32("Tag 0x02 crdt")?;
                let (tool_name_str, tool_name_phase, tool_name_ts, span_name_str) = if has_ranks {
                    let tn_str = cur.read_u32("Tag 0x02 crdt tool_name_str")?;
                    let tn_phase = cur.read_u8("Tag 0x02 crdt tool_name_phase")?;
                    if tn_phase != 0xFF && SpanPhase::from_u8(tn_phase).is_none() {
                        return Err(StoreError::Corrupted(format!(
                            "invalid Tag 0x02 tool_name_phase {tn_phase}"
                        )));
                    }
                    let tn_ts = cur.read_i64("Tag 0x02 crdt tool_name_ts")?;
                    let sn_str = cur.read_u32("Tag 0x02 crdt span_name_str")?;
                    if sn_str == NONE_STR_IDX {
                        return Err(StoreError::Corrupted(
                            "invalid Tag 0x02 span_name_str (NONE_STR_IDX)".into(),
                        ));
                    }
                    (tn_str, tn_phase, tn_ts, sn_str)
                } else {
                    (NONE_STR_IDX, 0xFF, 0, NONE_STR_IDX)
                };
                span_crdts.push(SpanCrdtDelta {
                    span_id_str,
                    span_type,
                    span_phase,
                    status: sp_status,
                    depth,
                    start_ts_ms: sp_start,
                    end_ts_ms: (has_sp_end != 0).then_some(sp_end),
                    duration_ms: sp_dur,
                    input_tokens: sp_in,
                    output_tokens: sp_out,
                    cached_tokens: sp_cac,
                    total_tokens: sp_tot,
                    cost_nanos: sp_cost,
                    model_str,
                    provider_str,
                    tool_name_str,
                    tool_name_phase,
                    tool_name_ts,
                    span_name_str,
                });
            }
            rows.push(TraceSummaryRow {
                trace_id_str,
                session_id_str,
                user_id_str,
                workflow_id_str,
                parent_trace_id_str,
                agent_name_str,
                agent_version_str,
                prompt_version_str,
                model_config_version_str,
                tool_config_version_str,
                app_release_str,
                environment_str,
                primary_model_str,
                primary_provider_str,
                error_type_str,
                tags_str,
                status,
                failure_source,
                has_end_ts,
                has_loop_warning,
                has_trace_payloads,
                start_ts_ms,
                end_ts_ms,
                duration_ms,
                span_count,
                llm_call_count,
                tool_call_count,
                error_span_count,
                input_tokens,
                output_tokens,
                cached_tokens,
                total_tokens,
                total_cost_nanos,
                explicit_status,
                explicit_failure_source,
                output_rank,
                metadata_rank,
                error_type_rank,
                error_message_rank,
                explicit_has_loop_warning,
                explicit_primary_model_str,
                explicit_primary_provider_str,
                span_crdts,
            });
        }
        if cur.remaining() != 0 {
            return Err(StoreError::Corrupted(
                "trailing bytes in Tag 0x02 chunk".into(),
            ));
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tag 0x03: Span Metrics Block
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanMetricRow {
    pub trace_idx: u32,
    pub trace_id_str: u32,
    pub span_id_str: u32,
    pub span_type: u8,
    pub span_phase: u8,
    pub status: u8,
    pub start_ts_ms: i64,
    pub duration_ms: i64,
    pub provider_str: u32,
    pub model_str: u32,
    pub tool_name_str: u32,
    pub tool_retry_count: i32,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub total_tokens: i64,
    pub cost_nanos: i64,
}

fn encode_span_metrics_section(rows: &[SpanMetricRow]) -> StoreResult<Vec<u8>> {
    const ROW_BYTES: usize = 87;
    let mut chunks = Vec::new();
    let mut buf = Vec::new();
    let mut cnt = 0u32;

    let flush = |chunks: &mut Vec<Vec<u8>>, buf: &mut Vec<u8>, cnt: &mut u32| {
        if *cnt == 0 {
            return;
        }
        let mut c = Vec::with_capacity(4 + buf.len());
        c.extend_from_slice(&cnt.to_le_bytes());
        c.extend_from_slice(buf);
        chunks.push(c);
        buf.clear();
        *cnt = 0;
    };

    for r in rows {
        if cnt > 0 && buf.len().saturating_add(ROW_BYTES) > DEFAULT_CHUNK_TARGET_BYTES {
            flush(&mut chunks, &mut buf, &mut cnt);
        }
        buf.extend_from_slice(&r.trace_idx.to_le_bytes());
        buf.extend_from_slice(&r.trace_id_str.to_le_bytes());
        buf.extend_from_slice(&r.span_id_str.to_le_bytes());
        buf.push(r.span_type);
        buf.push(r.span_phase);
        buf.push(r.status);
        buf.extend_from_slice(&r.start_ts_ms.to_le_bytes());
        buf.extend_from_slice(&r.duration_ms.to_le_bytes());
        buf.extend_from_slice(&r.provider_str.to_le_bytes());
        buf.extend_from_slice(&r.model_str.to_le_bytes());
        buf.extend_from_slice(&r.tool_name_str.to_le_bytes());
        buf.extend_from_slice(&r.tool_retry_count.to_le_bytes());
        buf.extend_from_slice(&r.input_tokens.to_le_bytes());
        buf.extend_from_slice(&r.output_tokens.to_le_bytes());
        buf.extend_from_slice(&r.cached_tokens.to_le_bytes());
        buf.extend_from_slice(&r.total_tokens.to_le_bytes());
        buf.extend_from_slice(&r.cost_nanos.to_le_bytes());
        cnt = cnt.saturating_add(1);
    }
    flush(&mut chunks, &mut buf, &mut cnt);
    encode_chunked_envelope(&chunks, false)
}

fn decode_span_metrics_section(buf: &[u8]) -> StoreResult<Vec<SpanMetricRow>> {
    let chunks = decode_chunked_envelope(buf, "Tag 0x03")?;
    let mut rows = Vec::new();
    for chunk in chunks {
        let mut cur = BinCursor::new(&chunk);
        let cnt = cur.read_u32("Tag 0x03 count")? as usize;
        if cnt > cur.remaining() / 87 + 1 {
            return Err(StoreError::Corrupted(
                "Tag 0x03 count exceeds remaining".into(),
            ));
        }
        for _ in 0..cnt {
            let trace_idx = cur.read_u32("Tag 0x03")?;
            let trace_id_str = cur.read_u32("Tag 0x03")?;
            let span_id_str = cur.read_u32("Tag 0x03")?;
            let span_type = cur.read_u8("Tag 0x03")?;
            if SpanType::from_u8(span_type).is_none() {
                return Err(StoreError::Corrupted(format!(
                    "invalid Tag 0x03 span_type {span_type}"
                )));
            }
            let span_phase = cur.read_u8("Tag 0x03")?;
            if SpanPhase::from_u8(span_phase).is_none() {
                return Err(StoreError::Corrupted(format!(
                    "invalid Tag 0x03 span_phase {span_phase}"
                )));
            }
            let status = cur.read_u8("Tag 0x03")?;
            if TraceStatus::from_u8(status).is_none() {
                return Err(StoreError::Corrupted(format!(
                    "invalid Tag 0x03 status {status}"
                )));
            }
            rows.push(SpanMetricRow {
                trace_idx,
                trace_id_str,
                span_id_str,
                span_type,
                span_phase,
                status,
                start_ts_ms: cur.read_i64("Tag 0x03")?,
                duration_ms: cur.read_i64("Tag 0x03")?,
                provider_str: cur.read_u32("Tag 0x03")?,
                model_str: cur.read_u32("Tag 0x03")?,
                tool_name_str: cur.read_u32("Tag 0x03")?,
                tool_retry_count: cur.read_i32("Tag 0x03")?,
                input_tokens: cur.read_i64("Tag 0x03")?,
                output_tokens: cur.read_i64("Tag 0x03")?,
                cached_tokens: cur.read_i64("Tag 0x03")?,
                total_tokens: cur.read_i64("Tag 0x03")?,
                cost_nanos: cur.read_i64("Tag 0x03")?,
            });
        }
        if cur.remaining() != 0 {
            return Err(StoreError::Corrupted("trailing bytes in Tag 0x03".into()));
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tag 0x05: 64 KiB-Aligned Trace-Contiguous Payload Pack (BlobPack)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlobRef {
    pack_block_idx: u32,
    blob_idx: u32,
}

impl BlobRef {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.pack_block_idx.to_le_bytes());
        out.extend_from_slice(&self.blob_idx.to_le_bytes());
    }

    fn decode(cur: &mut BinCursor<'_>) -> StoreResult<Self> {
        Ok(Self {
            pack_block_idx: cur.read_u32("BlobRef.block")?,
            blob_idx: cur.read_u32("BlobRef.idx")?,
        })
    }
}

#[derive(Debug, Clone)]
struct Tag05BlockMeta {
    file_offset: u64,
    comp_len: u32,
    uncomp_len: u32,
    crc32: u32,
    codec: u8,
    first_trace_idx: u32,
}

struct BuiltBlobBlock {
    first_trace_idx: u32,
    uncomp_len: u32,
    crc32: u32,
    comp: Vec<u8>,
}

fn approx_json_len(v: &Value) -> usize {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 5,
        Value::Number(_) => 12,
        Value::String(s) => s.len(),
        Value::Array(arr) => 2 + arr.iter().map(|x| approx_json_len(x) + 1).sum::<usize>(),
        Value::Object(map) => {
            2 + map
                .iter()
                .map(|(k, val)| k.len() + 4 + approx_json_len(val))
                .sum::<usize>()
        }
    }
}

fn estimate_trace_blob_bytes(trace: &AgentTrace, spans: &[AgentSpan]) -> usize {
    let mut est = approx_json_len(&trace.input)
        .saturating_add(approx_json_len(&trace.output))
        .saturating_add(approx_json_len(&trace.metadata));
    for sp in spans {
        est = est
            .saturating_add(approx_json_len(&sp.model_params))
            .saturating_add(approx_json_len(&sp.input))
            .saturating_add(approx_json_len(&sp.output))
            .saturating_add(approx_json_len(&sp.attributes))
            .saturating_add(approx_json_len(&sp.metadata));
        for ev in &sp.events {
            est = est.saturating_add(approx_json_len(&ev.attributes));
        }
    }
    est
}

struct BlobPackAccumulator {
    blocks: Vec<(u32, Vec<u8>)>, // (first_trace_idx, compressed_block_bytes)
    block_meta_uncomp: Vec<(u32, u32)>, // (uncomp_len, crc32)
    cur_first_trace_idx: u32,
    cur_hashes: Vec<u64>,
    cur_encodings: Vec<u8>,
    cur_offsets: Vec<u32>,
    cur_data: Vec<u8>,
    cur_dedup: HashMap<(u64, u8, usize), Vec<u32>>,
}

impl BlobPackAccumulator {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            block_meta_uncomp: Vec::new(),
            cur_first_trace_idx: 0,
            cur_hashes: Vec::new(),
            cur_encodings: Vec::new(),
            cur_offsets: vec![0],
            cur_data: Vec::new(),
            cur_dedup: HashMap::new(),
        }
    }

    /// Flushes the active block at a trace boundary so a trace's span and trace payloads
    /// stay within a single `Tag 0x05` block whenever the trace itself fits within 256 KiB.
    fn maybe_flush_for_next_trace(&mut self, estimated_trace_bytes: usize) -> StoreResult<()> {
        if self.cur_hashes.is_empty() {
            return Ok(());
        }
        if self.cur_data.len() >= DEFAULT_CHUNK_TARGET_BYTES
            || (self.cur_data.len() >= DEFAULT_CHUNK_TARGET_BYTES / 4
                && self.cur_data.len().saturating_add(estimated_trace_bytes)
                    > DEFAULT_CHUNK_TARGET_BYTES)
        {
            self.flush_current_block()?;
        }
        Ok(())
    }

    fn intern_value(&mut self, trace_idx: u32, val: &Value) -> StoreResult<BlobRef> {
        const MAX_MID_TRACE_BLOB_PACK_BYTES: usize = DEFAULT_CHUNK_TARGET_BYTES * 2;
        if val.is_null() {
            return Ok(NULL_BLOB_REF);
        }
        let (encoding, bytes) = match val {
            Value::String(s) => (BLOB_ENC_UTF8_TEXT, s.as_bytes().to_vec()),
            other => (BLOB_ENC_JSON_UTF8, serde_json::to_vec(other)?),
        };
        if self.cur_hashes.is_empty() {
            self.cur_first_trace_idx = trace_idx;
        } else if self.cur_data.len().saturating_add(bytes.len()) > MAX_MID_TRACE_BLOB_PACK_BYTES {
            self.flush_current_block()?;
            self.cur_first_trace_idx = trace_idx;
        }

        let hash = xxh3_64(&bytes);
        let key = (hash, encoding, bytes.len());
        if let Some(candidates) = self.cur_dedup.get(&key) {
            for &blob_idx in candidates {
                let idx = blob_idx as usize;
                let s = self.cur_offsets[idx] as usize;
                let e = self.cur_offsets[idx + 1] as usize;
                if self.cur_encodings[idx] == encoding && &self.cur_data[s..e] == bytes.as_slice() {
                    return Ok(BlobRef {
                        pack_block_idx: self.blocks.len() as u32,
                        blob_idx,
                    });
                }
            }
        }

        let blob_idx = u32::try_from(self.cur_hashes.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("BlobPack blob count overflow"))?;
        let new_end = u32::try_from(
            self.cur_data
                .len()
                .checked_add(bytes.len())
                .ok_or(StoreError::ResourceLimitExceeded("BlobPack data overflow"))?,
        )
        .map_err(|_| StoreError::ResourceLimitExceeded("BlobPack data exceeds u32"))?;

        self.cur_hashes.push(hash);
        self.cur_encodings.push(encoding);
        self.cur_offsets.push(new_end);
        self.cur_data.extend_from_slice(&bytes);
        self.cur_dedup.entry(key).or_default().push(blob_idx);

        Ok(BlobRef {
            pack_block_idx: self.blocks.len() as u32,
            blob_idx,
        })
    }

    fn flush_current_block(&mut self) -> StoreResult<()> {
        if self.cur_hashes.is_empty() {
            return Ok(());
        }
        let blob_count = self.cur_hashes.len() as u32;
        let mut raw = Vec::with_capacity(4 + self.cur_hashes.len() * 13 + 4 + self.cur_data.len());
        raw.extend_from_slice(&blob_count.to_le_bytes());
        for h in &self.cur_hashes {
            raw.extend_from_slice(&h.to_le_bytes());
        }
        raw.extend_from_slice(&self.cur_encodings);
        for off in &self.cur_offsets {
            raw.extend_from_slice(&off.to_le_bytes());
        }
        raw.extend_from_slice(&self.cur_data);

        let uncomp_len = u32::try_from(raw.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("BlobPack uncomp_len overflow"))?;
        let (_codec, comp) = compress_block(&raw, true)?;
        let crc = crc32fast::hash(&comp);
        self.blocks.push((self.cur_first_trace_idx, comp));
        self.block_meta_uncomp.push((uncomp_len, crc));

        self.cur_hashes.clear();
        self.cur_encodings.clear();
        self.cur_offsets.clear();
        self.cur_offsets.push(0);
        self.cur_data.clear();
        self.cur_dedup.clear();
        Ok(())
    }

    fn finish(mut self) -> StoreResult<Vec<BuiltBlobBlock>> {
        self.flush_current_block()?;
        let mut out = Vec::with_capacity(self.blocks.len());
        for ((first_trace_idx, comp), (uncomp_len, crc32)) in self
            .blocks
            .into_iter()
            .zip(self.block_meta_uncomp.into_iter())
        {
            out.push(BuiltBlobBlock {
                first_trace_idx,
                uncomp_len,
                crc32,
                comp,
            });
        }
        Ok(out)
    }
}

struct DecodedBlobPack {
    encodings: Vec<u8>,
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl DecodedBlobPack {
    fn decode(raw: &[u8]) -> StoreResult<Self> {
        let mut cur = BinCursor::new(raw);
        let blob_count = cur.read_u32("BlobPack blob_count")? as usize;
        if blob_count > crate::protocol::MAX_PLST_BLOBS {
            return Err(StoreError::ResourceLimitExceeded(
                "BlobPack blob_count exceeds limit",
            ));
        }
        let min_bytes = blob_count
            .checked_mul(13)
            .and_then(|v| v.checked_add(4))
            .ok_or(StoreError::ResourceLimitExceeded(
                "BlobPack header overflow",
            ))?;
        if min_bytes > cur.remaining() {
            return Err(StoreError::Corrupted("truncated BlobPack header".into()));
        }
        let mut hashes = Vec::with_capacity(blob_count.min(4096));
        for _ in 0..blob_count {
            hashes.push(cur.read_u64("BlobPack hash")?);
        }
        let encodings = cur.read_slice(blob_count, "BlobPack encodings")?.to_vec();
        let mut offsets = Vec::with_capacity(blob_count.min(4096) + 1);
        for _ in 0..=blob_count {
            offsets.push(cur.read_u32("BlobPack offset")?);
        }
        let data = cur.read_slice(cur.remaining(), "BlobPack data")?.to_vec();
        if offsets[0] != 0 || offsets[blob_count] as usize != data.len() {
            return Err(StoreError::Corrupted("invalid BlobPack offsets".into()));
        }
        for i in 0..blob_count {
            let s = offsets[i] as usize;
            let e = offsets[i + 1] as usize;
            if e < s || e > data.len() {
                return Err(StoreError::Corrupted(
                    "non-monotonic BlobPack offsets".into(),
                ));
            }
            let actual = xxh3_64(&data[s..e]);
            if actual != hashes[i] {
                return Err(StoreError::Corrupted(format!(
                    "BlobPack xxh3_64 mismatch at blob {i}"
                )));
            }
        }
        Ok(Self {
            encodings,
            offsets,
            data,
        })
    }

    fn materialize(&self, blob_ref: BlobRef) -> StoreResult<Value> {
        if blob_ref == NULL_BLOB_REF {
            return Ok(Value::Null);
        }
        let idx = blob_ref.blob_idx as usize;
        if idx >= self.encodings.len() {
            return Err(StoreError::Corrupted(format!(
                "invalid blob_idx {}",
                blob_ref.blob_idx
            )));
        }
        let s = self.offsets[idx] as usize;
        let e = self.offsets[idx + 1] as usize;
        let slice = &self.data[s..e];
        match self.encodings[idx] {
            BLOB_ENC_UTF8_TEXT => {
                let text = std::str::from_utf8(slice).map_err(|err| {
                    StoreError::Corrupted(format!("invalid UTF-8 blob text: {err}"))
                })?;
                Ok(Value::String(text.to_string()))
            }
            BLOB_ENC_JSON_UTF8 => Ok(serde_json::from_slice(slice)?),
            other => Err(StoreError::Corrupted(format!(
                "unknown blob encoding {other}"
            ))),
        }
    }
}

fn encode_tag05_dir_and_blocks(
    tag05_file_start: u64,
    blocks: Vec<BuiltBlobBlock>,
) -> StoreResult<(Vec<u8>, Vec<Tag05BlockMeta>)> {
    let block_count = u32::try_from(blocks.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x05 block_count overflow"))?;
    let header_and_dir_len = 8usize
        .checked_add(blocks.len() * 28)
        .ok_or(StoreError::ResourceLimitExceeded("Tag 0x05 dir overflow"))?;

    let mut sec = vec![0u8; header_and_dir_len];
    sec[0..4].copy_from_slice(&block_count.to_le_bytes());
    sec[4..8].copy_from_slice(&0u32.to_le_bytes());

    let mut metas = Vec::with_capacity(blocks.len());
    for (i, blk) in blocks.into_iter().enumerate() {
        let cur_file_off = tag05_file_start.checked_add(sec.len() as u64).ok_or(
            StoreError::ResourceLimitExceeded("Tag 0x05 file offset overflow"),
        )?;
        let rem = (cur_file_off as usize) % PLSS_ALIGN_BYTES;
        if rem != 0 {
            let pad = PLSS_ALIGN_BYTES - rem;
            sec.resize(sec.len() + pad, 0u8);
        }
        let aligned_offset = tag05_file_start.checked_add(sec.len() as u64).ok_or(
            StoreError::ResourceLimitExceeded("Tag 0x05 aligned offset overflow"),
        )?;
        debug_assert_eq!(aligned_offset as usize % PLSS_ALIGN_BYTES, 0);

        let comp_len = u32::try_from(blk.comp.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x05 comp_len overflow"))?;
        sec.extend_from_slice(&blk.comp);

        let dir_off = 8 + i * 28;
        sec[dir_off..dir_off + 8].copy_from_slice(&aligned_offset.to_le_bytes());
        sec[dir_off + 8..dir_off + 12].copy_from_slice(&comp_len.to_le_bytes());
        sec[dir_off + 12..dir_off + 16].copy_from_slice(&blk.uncomp_len.to_le_bytes());
        sec[dir_off + 16..dir_off + 20].copy_from_slice(&blk.crc32.to_le_bytes());
        sec[dir_off + 20] = CODEC_ZSTD_DICT;
        sec[dir_off + 21..dir_off + 24].copy_from_slice(&[0u8; 3]);
        sec[dir_off + 24..dir_off + 28].copy_from_slice(&blk.first_trace_idx.to_le_bytes());

        metas.push(Tag05BlockMeta {
            file_offset: aligned_offset,
            comp_len,
            uncomp_len: blk.uncomp_len,
            crc32: blk.crc32,
            codec: CODEC_ZSTD_DICT,
            first_trace_idx: blk.first_trace_idx,
        });
    }

    Ok((sec, metas))
}

fn decode_tag05_dir(buf: &[u8]) -> StoreResult<Vec<Tag05BlockMeta>> {
    let mut cur = BinCursor::new(buf);
    let block_count = cur.read_u32("Tag 0x05 block_count")? as usize;
    let _reserved = cur.read_u32("Tag 0x05 reserved")?;
    if block_count > MAX_CHUNK_COUNT * 4 || block_count * 28 > cur.remaining() {
        return Err(StoreError::Corrupted("invalid Tag 0x05 directory".into()));
    }
    let mut metas = Vec::with_capacity(block_count.min(1024));
    for _ in 0..block_count {
        let file_offset = cur.read_u64("Tag 0x05 dir")?;
        let comp_len = cur.read_u32("Tag 0x05 dir")?;
        let uncomp_len = cur.read_u32("Tag 0x05 dir")?;
        let crc32 = cur.read_u32("Tag 0x05 dir")?;
        let codec = cur.read_u8("Tag 0x05 dir")?;
        let _pad = cur.read_slice(3, "Tag 0x05 dir")?;
        let first_trace_idx = cur.read_u32("Tag 0x05 dir")?;
        metas.push(Tag05BlockMeta {
            file_offset,
            comp_len,
            uncomp_len,
            crc32,
            codec,
            first_trace_idx,
        });
    }
    Ok(metas)
}

// ---------------------------------------------------------------------------
// Tag 0x04: Trace-Clustered Span Tree + Span Events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TraceTreeRefs {
    input_ref: BlobRef,
    output_ref: BlobRef,
    metadata_ref: BlobRef,
    spans: Vec<SpanTreeRefs>,
}

#[derive(Debug, Clone)]
struct SpanTreeRefs {
    model_params_ref: BlobRef,
    input_ref: BlobRef,
    output_ref: BlobRef,
    attributes_ref: BlobRef,
    metadata_ref: BlobRef,
    event_attr_refs: Vec<BlobRef>,
}

#[derive(Debug, Clone)]
struct Tag04TraceToBlock {
    trace_id_hash: u64,
    block_start: u32,
    block_count: u32,
}

#[derive(Debug, Clone)]
struct Tag04BlockMeta {
    rel_offset: u64,
    comp_len: u32,
    uncomp_len: u32,
    crc32: u32,
    codec: u8,
}

struct Tag04Index {
    index_bytes: u32,
    trace_to_block: Vec<Tag04TraceToBlock>,
    block_dir: Vec<Tag04BlockMeta>,
}

impl Tag04Index {
    fn encode_bytes(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(12 + self.trace_to_block.len() * 16 + self.block_dir.len() * 24);
        out.extend_from_slice(&(self.block_dir.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.trace_to_block.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.index_bytes.to_le_bytes());
        for t in &self.trace_to_block {
            out.extend_from_slice(&t.trace_id_hash.to_le_bytes());
            out.extend_from_slice(&t.block_start.to_le_bytes());
            out.extend_from_slice(&t.block_count.to_le_bytes());
        }
        for b in &self.block_dir {
            out.extend_from_slice(&b.rel_offset.to_le_bytes());
            out.extend_from_slice(&b.comp_len.to_le_bytes());
            out.extend_from_slice(&b.uncomp_len.to_le_bytes());
            out.extend_from_slice(&b.crc32.to_le_bytes());
            out.push(b.codec);
            out.extend_from_slice(&[0u8; 3]);
        }
        out
    }

    fn decode(buf: &[u8]) -> StoreResult<Self> {
        let mut cur = BinCursor::new(buf);
        let block_count = cur.read_u32("Tag 0x04 block_count")? as usize;
        let trace_count = cur.read_u32("Tag 0x04 trace_count")? as usize;
        let index_bytes = cur.read_u32("Tag 0x04 index_bytes")?;
        if block_count > MAX_CHUNK_COUNT * 4 || trace_count > crate::protocol::MAX_PLST_TRACES {
            return Err(StoreError::ResourceLimitExceeded("Tag 0x04 index overflow"));
        }
        let needed = trace_count
            .checked_mul(16)
            .and_then(|v| v.checked_add(block_count.checked_mul(24)?))
            .ok_or(StoreError::ResourceLimitExceeded(
                "Tag 0x04 index size overflow",
            ))?;
        if needed != index_bytes as usize || needed > cur.remaining() {
            return Err(StoreError::Corrupted("invalid Tag 0x04 index size".into()));
        }
        let mut trace_to_block = Vec::with_capacity(trace_count.min(4096));
        for _ in 0..trace_count {
            let trace_id_hash = cur.read_u64("Tag 0x04")?;
            let entry_block_start = cur.read_u32("Tag 0x04")?;
            let entry_block_count = cur.read_u32("Tag 0x04")?;
            let in_bounds = entry_block_start
                .checked_add(entry_block_count)
                .is_some_and(|end| (end as usize) <= block_count);
            if !in_bounds {
                return Err(StoreError::Corrupted(
                    "Tag 0x04 trace_to_block range exceeds block_count".into(),
                ));
            }
            trace_to_block.push(Tag04TraceToBlock {
                trace_id_hash,
                block_start: entry_block_start,
                block_count: entry_block_count,
            });
        }
        let mut block_dir = Vec::with_capacity(block_count.min(1024));
        for _ in 0..block_count {
            let rel_offset = cur.read_u64("Tag 0x04")?;
            let comp_len = cur.read_u32("Tag 0x04")?;
            let uncomp_len = cur.read_u32("Tag 0x04")?;
            let crc32 = cur.read_u32("Tag 0x04")?;
            let codec = cur.read_u8("Tag 0x04")?;
            let _pad = cur.read_slice(3, "Tag 0x04")?;
            block_dir.push(Tag04BlockMeta {
                rel_offset,
                comp_len,
                uncomp_len,
                crc32,
                codec,
            });
        }
        Ok(Self {
            index_bytes,
            trace_to_block,
            block_dir,
        })
    }
}

fn encode_tag04_entry(
    out: &mut Vec<u8>,
    trace: &AgentTrace,
    trace_meta: &TraceCrdtMeta,
    spans: &[(AgentSpan, SpanFieldRanks)],
    refs: &TraceTreeRefs,
) -> StoreResult<()> {
    write_str(out, &trace.trace_id)?;
    write_str(out, &trace.project_id)?;
    write_str(out, &trace.agent_name)?;
    write_str(out, &trace.agent_version)?;
    write_opt_str(out, trace.prompt_version.as_deref())?;
    write_opt_str(out, trace.model_config_version.as_deref())?;
    write_opt_str(out, trace.tool_config_version.as_deref())?;
    write_opt_str(out, trace.app_release.as_deref())?;
    write_str(out, &trace.environment)?;
    write_opt_str(out, trace.session_id.as_deref())?;
    write_opt_str(out, trace.user_id.as_deref())?;
    write_opt_str(out, trace.workflow_id.as_deref())?;
    write_opt_str(out, trace.parent_trace_id.as_deref())?;
    out.push(trace.status.to_u8());
    out.extend_from_slice(&trace.start_ts_ms.to_le_bytes());
    out.push(u8::from(trace.end_ts_ms.is_some()));
    out.extend_from_slice(&trace.end_ts_ms.unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&trace.duration_ms.to_le_bytes());
    refs.input_ref.encode_into(out);
    refs.output_ref.encode_into(out);
    refs.metadata_ref.encode_into(out);
    write_opt_str(out, trace.error_type.as_deref())?;
    write_opt_str(out, trace.error_message.as_deref())?;
    out.push(trace.failure_source.map_or(0, |f| f.to_u8()));
    let tag_cnt = u16::try_from(trace.tags.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x04 trace tags overflow"))?;
    out.extend_from_slice(&tag_cnt.to_le_bytes());
    for t in &trace.tags {
        write_str(out, t)?;
    }
    out.extend_from_slice(&trace.span_count.to_le_bytes());
    out.extend_from_slice(&trace.llm_call_count.to_le_bytes());
    out.extend_from_slice(&trace.tool_call_count.to_le_bytes());
    out.extend_from_slice(&trace.error_span_count.to_le_bytes());
    out.extend_from_slice(&trace.input_tokens.to_le_bytes());
    out.extend_from_slice(&trace.output_tokens.to_le_bytes());
    out.extend_from_slice(&trace.cached_tokens.to_le_bytes());
    out.extend_from_slice(&trace.total_tokens.to_le_bytes());
    out.extend_from_slice(&trace.total_cost_nanos.to_le_bytes());
    write_opt_str(out, trace.primary_model.as_deref())?;
    write_opt_str(out, trace.primary_provider.as_deref())?;
    out.push(u8::from(trace.has_loop_warning));
    out.push(trace_meta.explicit_status.to_u8());
    out.push(trace_meta.explicit_failure_source.map_or(0, |f| f.to_u8()));
    out.push(trace_meta.field_ranks.output_rank);
    out.push(trace_meta.field_ranks.metadata_rank);
    out.push(trace_meta.field_ranks.error_type_rank);
    out.push(trace_meta.field_ranks.error_message_rank);
    out.push(u8::from(trace_meta.explicit_has_loop_warning));
    write_opt_str(out, trace_meta.explicit_primary_model.as_deref())?;
    write_opt_str(out, trace_meta.explicit_primary_provider.as_deref())?;

    let span_cnt = u32::try_from(spans.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x04 span_cnt overflow"))?;
    out.extend_from_slice(&span_cnt.to_le_bytes());
    for ((sp, sp_ranks), sp_refs) in spans.iter().zip(refs.spans.iter()) {
        write_str(out, &sp.span_id)?;
        write_str(out, &sp.trace_id)?;
        write_str(out, &sp.project_id)?;
        write_opt_str(out, sp.parent_span_id.as_deref())?;
        let linked_cnt = u16::try_from(sp.linked_span_ids.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x04 linked_span_ids overflow"))?;
        out.extend_from_slice(&linked_cnt.to_le_bytes());
        for lid in &sp.linked_span_ids {
            write_str(out, lid)?;
        }
        out.push(sp.span_phase.to_u8());
        out.extend_from_slice(&sp.depth.to_le_bytes());
        write_str(out, &sp.name)?;
        out.push(sp.span_type.to_u8());
        out.push(sp.status.to_u8());
        out.extend_from_slice(&sp.start_ts_ms.to_le_bytes());
        out.push(u8::from(sp.end_ts_ms.is_some()));
        out.extend_from_slice(&sp.end_ts_ms.unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&sp.duration_ms.to_le_bytes());
        write_opt_str(out, sp.provider.as_deref())?;
        write_opt_str(out, sp.model.as_deref())?;
        write_opt_str(out, sp.model_id.as_deref())?;
        write_opt_str(out, sp.model_version.as_deref())?;
        write_opt_str(out, sp.prompt_template_id.as_deref())?;
        write_opt_str(out, sp.prompt_template_version.as_deref())?;
        out.extend_from_slice(&sp.input_tokens.to_le_bytes());
        out.extend_from_slice(&sp.output_tokens.to_le_bytes());
        out.extend_from_slice(&sp.cached_tokens.to_le_bytes());
        out.extend_from_slice(&sp.total_tokens.to_le_bytes());
        out.extend_from_slice(&sp.cost_nanos.to_le_bytes());
        match sp.temperature {
            Some(t) => {
                out.push(1);
                out.extend_from_slice(&t.to_le_bytes());
            }
            None => out.push(0),
        }
        write_opt_str(out, sp.finish_reason.as_deref())?;
        sp_refs.model_params_ref.encode_into(out);
        write_opt_str(out, sp.tool_name.as_deref())?;
        write_opt_str(out, sp.tool_version.as_deref())?;
        out.extend_from_slice(&sp.tool_retry_count.to_le_bytes());
        sp_refs.input_ref.encode_into(out);
        sp_refs.output_ref.encode_into(out);
        sp_refs.attributes_ref.encode_into(out);
        sp_refs.metadata_ref.encode_into(out);
        write_opt_str(out, sp.error_type.as_deref())?;
        write_opt_str(out, sp.error_message.as_deref())?;
        write_opt_str(out, sp.error_stack.as_deref())?;

        let ev_cnt = u32::try_from(sp.events.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x04 event count overflow"))?;
        out.extend_from_slice(&ev_cnt.to_le_bytes());
        for (ev, attr_ref) in sp.events.iter().zip(sp_refs.event_attr_refs.iter()) {
            write_str(out, &ev.name)?;
            out.extend_from_slice(&ev.timestamp_ms.to_le_bytes());
            attr_ref.encode_into(out);
        }

        let rank_fields: [Option<(u8, i64)>; 9] = [
            sp_ranks.output_rank,
            sp_ranks.attributes_rank,
            sp_ranks.metadata_rank,
            sp_ranks.model_params_rank,
            sp_ranks.finish_reason_rank,
            sp_ranks.error_type_rank,
            sp_ranks.error_message_rank,
            sp_ranks.error_stack_rank,
            sp_ranks.tool_name_rank,
        ];
        let mut rank_mask: u16 = 0;
        for (bit, slot) in rank_fields.iter().enumerate() {
            if slot.is_some() {
                rank_mask |= 1u16 << bit;
            }
        }
        out.extend_from_slice(&rank_mask.to_le_bytes());
        for slot in rank_fields.into_iter().flatten() {
            out.push(slot.0);
            out.extend_from_slice(&slot.1.to_le_bytes());
        }
    }
    Ok(())
}

struct DecodedTag04Entry {
    trace: AgentTrace,
    trace_meta: TraceCrdtMeta,
    trace_refs: TraceTreeRefs,
    spans: Vec<(AgentSpan, SpanFieldRanks)>,
}

fn decode_tag04_block(raw: &[u8], has_ranks: bool) -> StoreResult<Vec<DecodedTag04Entry>> {
    let mut cur = BinCursor::new(raw);
    let count = cur.read_u32("Tag 0x04 block entry_count")? as usize;
    if count > crate::protocol::MAX_PLST_TRACES {
        return Err(StoreError::ResourceLimitExceeded(
            "Tag 0x04 entry count overflow",
        ));
    }
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let trace_id = cur.read_str("Tag 0x04")?;
        let project_id = cur.read_str("Tag 0x04")?;
        let agent_name = cur.read_str("Tag 0x04")?;
        let agent_version = cur.read_str("Tag 0x04")?;
        let prompt_version = cur.read_opt_str("Tag 0x04")?;
        let model_config_version = cur.read_opt_str("Tag 0x04")?;
        let tool_config_version = cur.read_opt_str("Tag 0x04")?;
        let app_release = cur.read_opt_str("Tag 0x04")?;
        let environment = cur.read_str("Tag 0x04")?;
        let session_id = cur.read_opt_str("Tag 0x04")?;
        let user_id = cur.read_opt_str("Tag 0x04")?;
        let workflow_id = cur.read_opt_str("Tag 0x04")?;
        let parent_trace_id = cur.read_opt_str("Tag 0x04")?;
        let status_u8 = cur.read_u8("Tag 0x04")?;
        let status = TraceStatus::from_u8(status_u8)
            .ok_or_else(|| StoreError::Corrupted(format!("invalid status {status_u8}")))?;
        let start_ts_ms = cur.read_i64("Tag 0x04")?;
        let has_end = cur.read_u8("Tag 0x04")?;
        let end_ts = cur.read_i64("Tag 0x04")?;
        let duration_ms = cur.read_i64("Tag 0x04")?;
        let tr_in_ref = BlobRef::decode(&mut cur)?;
        let tr_out_ref = BlobRef::decode(&mut cur)?;
        let tr_meta_ref = BlobRef::decode(&mut cur)?;
        let error_type = cur.read_opt_str("Tag 0x04")?;
        let error_message = cur.read_opt_str("Tag 0x04")?;
        let fs_u8 = cur.read_u8("Tag 0x04")?;
        let failure_source =
            if fs_u8 == 0 {
                None
            } else {
                Some(FailureSource::from_u8(fs_u8).ok_or_else(|| {
                    StoreError::Corrupted(format!("invalid failure_source {fs_u8}"))
                })?)
            };
        let tag_cnt = cur.read_u16("Tag 0x04")? as usize;
        let mut tags = Vec::with_capacity(tag_cnt.min(64));
        for _ in 0..tag_cnt {
            tags.push(cur.read_str("Tag 0x04 tag")?);
        }
        let span_count = cur.read_i32("Tag 0x04")?;
        let llm_call_count = cur.read_i32("Tag 0x04")?;
        let tool_call_count = cur.read_i32("Tag 0x04")?;
        let error_span_count = cur.read_i32("Tag 0x04")?;
        let input_tokens = cur.read_i64("Tag 0x04")?;
        let output_tokens = cur.read_i64("Tag 0x04")?;
        let cached_tokens = cur.read_i64("Tag 0x04")?;
        let total_tokens = cur.read_i64("Tag 0x04")?;
        let total_cost_nanos = cur.read_i64("Tag 0x04")?;
        let primary_model = cur.read_opt_str("Tag 0x04")?;
        let primary_provider = cur.read_opt_str("Tag 0x04")?;
        let has_loop_warning = cur.read_u8("Tag 0x04")? != 0;

        let explicit_meta = if has_ranks {
            let es_u8 = cur.read_u8("Tag 0x04 explicit_status")?;
            let explicit_status = TraceStatus::from_u8(es_u8).ok_or_else(|| {
                StoreError::Corrupted(format!("invalid Tag 0x04 explicit_status {es_u8}"))
            })?;
            let efs_u8 = cur.read_u8("Tag 0x04 explicit_failure_source")?;
            let explicit_failure_source = if efs_u8 == 0 {
                None
            } else {
                Some(FailureSource::from_u8(efs_u8).ok_or_else(|| {
                    StoreError::Corrupted(format!(
                        "invalid Tag 0x04 explicit_failure_source {efs_u8}"
                    ))
                })?)
            };
            let output_rank = cur.read_u8("Tag 0x04 output_rank")?;
            let metadata_rank = cur.read_u8("Tag 0x04 metadata_rank")?;
            let error_type_rank = cur.read_u8("Tag 0x04 error_type_rank")?;
            let error_message_rank = cur.read_u8("Tag 0x04 error_message_rank")?;
            let ehlw = cur.read_u8("Tag 0x04 explicit_has_loop_warning")?;
            if output_rank > 4 || metadata_rank > 4 || error_type_rank > 4 || error_message_rank > 4
            {
                return Err(StoreError::Corrupted(
                    "invalid Tag 0x04 TraceFieldRanks value (> 4)".into(),
                ));
            }
            if ehlw > 1 {
                return Err(StoreError::Corrupted(format!(
                    "invalid Tag 0x04 explicit_has_loop_warning {ehlw}"
                )));
            }
            let explicit_primary_model = cur
                .read_opt_str("Tag 0x04 explicit_primary_model")?
                .filter(|s| !s.is_empty());
            let explicit_primary_provider = cur
                .read_opt_str("Tag 0x04 explicit_primary_provider")?
                .filter(|s| !s.is_empty());
            Some(TraceCrdtMeta {
                field_ranks: TraceFieldRanks {
                    output_rank,
                    metadata_rank,
                    error_type_rank,
                    error_message_rank,
                },
                explicit_status,
                explicit_failure_source,
                explicit_has_loop_warning: ehlw != 0,
                explicit_primary_model,
                explicit_primary_provider,
            })
        } else {
            None
        };

        let trace = AgentTrace {
            trace_id,
            project_id,
            agent_name,
            agent_version,
            prompt_version,
            model_config_version,
            tool_config_version,
            app_release,
            environment,
            session_id,
            user_id,
            workflow_id,
            parent_trace_id,
            status,
            start_ts_ms,
            end_ts_ms: (has_end != 0).then_some(end_ts),
            duration_ms,
            input: Value::Null,
            output: Value::Null,
            error_type,
            error_message,
            failure_source,
            metadata: Value::Null,
            tags,
            span_count,
            llm_call_count,
            tool_call_count,
            error_span_count,
            input_tokens,
            output_tokens,
            cached_tokens,
            total_tokens,
            total_cost_nanos,
            total_cost_usd: cost_nanos_to_usd(total_cost_nanos),
            primary_model,
            primary_provider,
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning,
        };
        let trace_meta = explicit_meta.unwrap_or_else(|| TraceCrdtMeta::from_trace(&trace));

        let sp_cnt = cur.read_u32("Tag 0x04 sp_cnt")? as usize;
        let mut spans = Vec::with_capacity(sp_cnt.min(1024));
        let mut span_refs = Vec::with_capacity(sp_cnt.min(1024));
        for _ in 0..sp_cnt {
            let span_id = cur.read_str("Tag 0x04")?;
            let sp_trace_id = cur.read_str("Tag 0x04")?;
            let sp_project_id = cur.read_str("Tag 0x04")?;
            let parent_span_id = cur.read_opt_str("Tag 0x04")?;
            let linked_cnt = cur.read_u16("Tag 0x04")? as usize;
            let mut linked_span_ids = Vec::with_capacity(linked_cnt.min(64));
            for _ in 0..linked_cnt {
                linked_span_ids.push(cur.read_str("Tag 0x04 linked")?);
            }
            let phase_u8 = cur.read_u8("Tag 0x04")?;
            let span_phase = SpanPhase::from_u8(phase_u8)
                .ok_or_else(|| StoreError::Corrupted(format!("invalid span_phase {phase_u8}")))?;
            let depth = cur.read_i32("Tag 0x04")?;
            let name = cur.read_str("Tag 0x04")?;
            let st_u8 = cur.read_u8("Tag 0x04")?;
            let span_type = SpanType::from_u8(st_u8)
                .ok_or_else(|| StoreError::Corrupted(format!("invalid span_type {st_u8}")))?;
            let sp_status_u8 = cur.read_u8("Tag 0x04")?;
            let sp_status = TraceStatus::from_u8(sp_status_u8).ok_or_else(|| {
                StoreError::Corrupted(format!("invalid span status {sp_status_u8}"))
            })?;
            let sp_start = cur.read_i64("Tag 0x04")?;
            let sp_has_end = cur.read_u8("Tag 0x04")?;
            let sp_end = cur.read_i64("Tag 0x04")?;
            let sp_dur = cur.read_i64("Tag 0x04")?;
            let provider = cur.read_opt_str("Tag 0x04")?;
            let model = cur.read_opt_str("Tag 0x04")?;
            let model_id = cur.read_opt_str("Tag 0x04")?;
            let model_version = cur.read_opt_str("Tag 0x04")?;
            let prompt_template_id = cur.read_opt_str("Tag 0x04")?;
            let prompt_template_version = cur.read_opt_str("Tag 0x04")?;
            let sp_in = cur.read_i64("Tag 0x04")?;
            let sp_out = cur.read_i64("Tag 0x04")?;
            let sp_cac = cur.read_i64("Tag 0x04")?;
            let sp_tot = cur.read_i64("Tag 0x04")?;
            let sp_cost = cur.read_i64("Tag 0x04")?;
            let has_temp = cur.read_u8("Tag 0x04")?;
            let temperature = if has_temp != 0 {
                let s = cur.read_slice(8, "Tag 0x04 temp")?;
                Some(f64::from_le_bytes(s.try_into().unwrap()))
            } else {
                None
            };
            let finish_reason = cur.read_opt_str("Tag 0x04")?;
            let model_params_ref = BlobRef::decode(&mut cur)?;
            let tool_name = cur.read_opt_str("Tag 0x04")?;
            let tool_version = cur.read_opt_str("Tag 0x04")?;
            let tool_retry_count = cur.read_i32("Tag 0x04")?;
            let sp_in_ref = BlobRef::decode(&mut cur)?;
            let sp_out_ref = BlobRef::decode(&mut cur)?;
            let sp_attr_ref = BlobRef::decode(&mut cur)?;
            let sp_meta_ref = BlobRef::decode(&mut cur)?;
            let sp_err_type = cur.read_opt_str("Tag 0x04")?;
            let sp_err_msg = cur.read_opt_str("Tag 0x04")?;
            let sp_err_stack = cur.read_opt_str("Tag 0x04")?;

            let ev_cnt = cur.read_u32("Tag 0x04 ev_cnt")? as usize;
            let mut events = Vec::with_capacity(ev_cnt.min(64));
            let mut ev_refs = Vec::with_capacity(ev_cnt.min(64));
            for _ in 0..ev_cnt {
                let ev_name = cur.read_str("Tag 0x04 ev")?;
                let ev_ts = cur.read_i64("Tag 0x04 ev")?;
                let ev_ref = BlobRef::decode(&mut cur)?;
                events.push(SpanEvent {
                    name: ev_name,
                    timestamp_ms: ev_ts,
                    attributes: Value::Null,
                });
                ev_refs.push(ev_ref);
            }

            let sp_ranks_opt = if has_ranks {
                let rank_mask = cur.read_u16("Tag 0x04 rank_mask")?;
                if (rank_mask & !0x01FF) != 0 {
                    return Err(StoreError::Corrupted(format!(
                        "invalid Tag 0x04 rank_mask 0x{rank_mask:04x}"
                    )));
                }
                let mut slots: [Option<(u8, i64)>; 9] = [None; 9];
                for (bit, slot) in slots.iter_mut().enumerate() {
                    if (rank_mask & (1u16 << bit)) != 0 {
                        let p = cur.read_u8("Tag 0x04 span rank phase")?;
                        if SpanPhase::from_u8(p).is_none() {
                            return Err(StoreError::Corrupted(format!(
                                "invalid Tag 0x04 SpanFieldRanks phase {p}"
                            )));
                        }
                        let ts = cur.read_i64("Tag 0x04 span rank ts")?;
                        *slot = Some((p, ts));
                    }
                }
                Some(SpanFieldRanks {
                    output_rank: slots[0],
                    attributes_rank: slots[1],
                    metadata_rank: slots[2],
                    model_params_rank: slots[3],
                    finish_reason_rank: slots[4],
                    error_type_rank: slots[5],
                    error_message_rank: slots[6],
                    error_stack_rank: slots[7],
                    tool_name_rank: slots[8],
                })
            } else {
                None
            };

            let sp = AgentSpan {
                span_id,
                trace_id: sp_trace_id,
                project_id: sp_project_id,
                parent_span_id,
                linked_span_ids,
                span_phase,
                depth,
                name,
                span_type,
                status: sp_status,
                start_ts_ms: sp_start,
                end_ts_ms: (sp_has_end != 0).then_some(sp_end),
                duration_ms: sp_dur,
                provider,
                model,
                model_id,
                model_version,
                prompt_template_id,
                prompt_template_version,
                input_tokens: sp_in,
                output_tokens: sp_out,
                cached_tokens: sp_cac,
                total_tokens: sp_tot,
                cost_nanos: sp_cost,
                cost_usd: cost_nanos_to_usd(sp_cost),
                temperature,
                finish_reason,
                model_params: Value::Null,
                tool_name,
                tool_version,
                tool_retry_count,
                input: Value::Null,
                output: Value::Null,
                attributes: Value::Null,
                events,
                metadata: Value::Null,
                error_type: sp_err_type,
                error_message: sp_err_msg,
                error_stack: sp_err_stack,
            };
            let sp_ranks = sp_ranks_opt.unwrap_or_else(|| SpanFieldRanks::from_span(&sp));
            spans.push((sp, sp_ranks));
            span_refs.push(SpanTreeRefs {
                model_params_ref,
                input_ref: sp_in_ref,
                output_ref: sp_out_ref,
                attributes_ref: sp_attr_ref,
                metadata_ref: sp_meta_ref,
                event_attr_refs: ev_refs,
            });
        }

        out.push(DecodedTag04Entry {
            trace,
            trace_meta,
            trace_refs: TraceTreeRefs {
                input_ref: tr_in_ref,
                output_ref: tr_out_ref,
                metadata_ref: tr_meta_ref,
                spans: span_refs,
            },
            spans,
        });
    }
    if cur.remaining() != 0 {
        return Err(StoreError::Corrupted(
            "trailing bytes in Tag 0x04 block".into(),
        ));
    }
    Ok(out)
}

fn encode_tag04_section(
    entries: &[RankedTraceEntry],
    all_refs: &[TraceTreeRefs],
) -> StoreResult<Vec<u8>> {
    let mut blocks: Vec<(Vec<u8>, u32, u32)> = Vec::new(); // (comp, uncomp_len, crc32)
    let mut trace_to_block = Vec::with_capacity(entries.len());

    let mut cur_raw = Vec::new();
    let mut cur_cnt = 0u32;
    let mut cur_block_idx = 0u32;

    let flush_blk = |raw: &mut Vec<u8>,
                     cnt: &mut u32,
                     blocks: &mut Vec<(Vec<u8>, u32, u32)>|
     -> StoreResult<()> {
        if *cnt == 0 {
            return Ok(());
        }
        let mut full = Vec::with_capacity(4 + raw.len());
        full.extend_from_slice(&cnt.to_le_bytes());
        full.extend_from_slice(raw);
        let uncomp_len = u32::try_from(full.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x04 block uncomp_len overflow"))?;
        let (_codec, comp) = compress_block(&full, true)?;
        let crc = crc32fast::hash(&comp);
        blocks.push((comp, uncomp_len, crc));
        raw.clear();
        *cnt = 0;
        Ok(())
    };

    for ((tr, meta, spans), refs) in entries.iter().zip(all_refs.iter()) {
        let mut entry_buf = Vec::new();
        encode_tag04_entry(&mut entry_buf, tr, meta, spans, refs)?;
        if cur_cnt > 0 && cur_raw.len().saturating_add(entry_buf.len()) > DEFAULT_CHUNK_TARGET_BYTES
        {
            flush_blk(&mut cur_raw, &mut cur_cnt, &mut blocks)?;
            cur_block_idx = blocks.len() as u32;
        }
        cur_raw.extend_from_slice(&entry_buf);
        cur_cnt = cur_cnt.saturating_add(1);
        trace_to_block.push(Tag04TraceToBlock {
            trace_id_hash: xxh3_64(tr.trace_id.as_bytes()),
            block_start: cur_block_idx,
            block_count: 1,
        });
    }
    flush_blk(&mut cur_raw, &mut cur_cnt, &mut blocks)?;

    let mut block_dir = Vec::with_capacity(blocks.len());
    let mut rel_offset = 0u64;
    let mut block_bytes = Vec::new();
    for (comp, uncomp_len, crc) in blocks {
        let comp_len = u32::try_from(comp.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x04 comp_len overflow"))?;
        block_dir.push(Tag04BlockMeta {
            rel_offset,
            comp_len,
            uncomp_len,
            crc32: crc,
            codec: CODEC_ZSTD_DICT,
        });
        rel_offset =
            rel_offset
                .checked_add(comp_len as u64)
                .ok_or(StoreError::ResourceLimitExceeded(
                    "Tag 0x04 rel_offset overflow",
                ))?;
        block_bytes.extend_from_slice(&comp);
    }

    let index_bytes = trace_to_block
        .len()
        .checked_mul(16)
        .and_then(|v| v.checked_add(block_dir.len().checked_mul(24)?))
        .and_then(|v| u32::try_from(v).ok())
        .ok_or(StoreError::ResourceLimitExceeded(
            "Tag 0x04 index_bytes overflow",
        ))?;
    let idx = Tag04Index {
        index_bytes,
        trace_to_block,
        block_dir,
    };
    let mut out = idx.encode_bytes();
    out.extend_from_slice(&block_bytes);
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tag 0x07: Session Rollup Block
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRollupRow {
    pub session_id_str: u32,
    pub user_id_str: u32,
    pub environment_str: u32,
    pub agent_name_strs: Vec<u32>,
    pub first_ts_ms: i64,
    pub last_ts_ms: i64,
    pub trace_count: i64,
    pub error_count: i64,
    pub total_tokens: i64,
    pub total_cost_nanos: i64,
    pub trace_id_strs: Vec<u32>,
}

impl SessionRollupRow {
    pub fn approx_bytes(&self) -> usize {
        64 + self.agent_name_strs.len() * 4 + self.trace_id_strs.len() * 4
    }
}

fn encode_session_rollup_section(rows: &[SessionRollupRow]) -> StoreResult<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut buf = Vec::new();
    let mut cnt = 0u32;

    let flush = |chunks: &mut Vec<Vec<u8>>, buf: &mut Vec<u8>, cnt: &mut u32| {
        if *cnt == 0 {
            return;
        }
        let mut c = Vec::with_capacity(4 + buf.len());
        c.extend_from_slice(&cnt.to_le_bytes());
        c.extend_from_slice(buf);
        chunks.push(c);
        buf.clear();
        *cnt = 0;
    };

    for r in rows {
        let est = 64 + r.agent_name_strs.len() * 4 + r.trace_id_strs.len() * 4;
        if cnt > 0 && buf.len().saturating_add(est) > DEFAULT_CHUNK_TARGET_BYTES {
            flush(&mut chunks, &mut buf, &mut cnt);
        }
        buf.extend_from_slice(&r.session_id_str.to_le_bytes());
        buf.extend_from_slice(&r.user_id_str.to_le_bytes());
        buf.extend_from_slice(&r.environment_str.to_le_bytes());
        let acnt = u32::try_from(r.agent_name_strs.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x07 agent_names overflow"))?;
        buf.extend_from_slice(&acnt.to_le_bytes());
        for &a in &r.agent_name_strs {
            buf.extend_from_slice(&a.to_le_bytes());
        }
        buf.extend_from_slice(&r.first_ts_ms.to_le_bytes());
        buf.extend_from_slice(&r.last_ts_ms.to_le_bytes());
        buf.extend_from_slice(&r.trace_count.to_le_bytes());
        buf.extend_from_slice(&r.error_count.to_le_bytes());
        buf.extend_from_slice(&r.total_tokens.to_le_bytes());
        buf.extend_from_slice(&r.total_cost_nanos.to_le_bytes());
        let tcnt = u32::try_from(r.trace_id_strs.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("Tag 0x07 trace_ids overflow"))?;
        buf.extend_from_slice(&tcnt.to_le_bytes());
        for &tid in &r.trace_id_strs {
            buf.extend_from_slice(&tid.to_le_bytes());
        }
        cnt = cnt.saturating_add(1);
    }
    flush(&mut chunks, &mut buf, &mut cnt);
    encode_chunked_envelope(&chunks, false)
}

fn decode_session_rollup_section(buf: &[u8]) -> StoreResult<Vec<SessionRollupRow>> {
    let chunks = decode_chunked_envelope(buf, "Tag 0x07")?;
    let mut rows = Vec::new();
    for chunk in chunks {
        let mut cur = BinCursor::new(&chunk);
        let cnt = cur.read_u32("Tag 0x07 count")? as usize;
        for _ in 0..cnt {
            let session_id_str = cur.read_u32("Tag 0x07")?;
            let user_id_str = cur.read_u32("Tag 0x07")?;
            let environment_str = cur.read_u32("Tag 0x07")?;
            let acnt = cur.read_u32("Tag 0x07 acnt")? as usize;
            if acnt > cur.remaining() / 4 + 1 {
                return Err(StoreError::Corrupted(
                    "Tag 0x07 acnt exceeds remaining".into(),
                ));
            }
            let mut agent_name_strs = Vec::with_capacity(acnt.min(64));
            for _ in 0..acnt {
                agent_name_strs.push(cur.read_u32("Tag 0x07")?);
            }
            let first_ts_ms = cur.read_i64("Tag 0x07")?;
            let last_ts_ms = cur.read_i64("Tag 0x07")?;
            let trace_count = cur.read_i64("Tag 0x07")?;
            let error_count = cur.read_i64("Tag 0x07")?;
            let total_tokens = cur.read_i64("Tag 0x07")?;
            let total_cost_nanos = cur.read_i64("Tag 0x07")?;
            let tcnt = cur.read_u32("Tag 0x07 tcnt")? as usize;
            if tcnt > cur.remaining() / 4 + 1 {
                return Err(StoreError::Corrupted(
                    "Tag 0x07 tcnt exceeds remaining".into(),
                ));
            }
            let mut trace_id_strs = Vec::with_capacity(tcnt.min(1024));
            for _ in 0..tcnt {
                trace_id_strs.push(cur.read_u32("Tag 0x07")?);
            }
            rows.push(SessionRollupRow {
                session_id_str,
                user_id_str,
                environment_str,
                agent_name_strs,
                first_ts_ms,
                last_ts_ms,
                trace_count,
                error_count,
                total_tokens,
                total_cost_nanos,
                trace_id_strs,
            });
        }
        if cur.remaining() != 0 {
            return Err(StoreError::Corrupted("trailing bytes in Tag 0x07".into()));
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// SegmentWriter (PLSS v1 + Segment Splitting <= max_segment_bytes)
// ---------------------------------------------------------------------------

pub fn segment_object_path(
    project_id: &str,
    date: &str,
    shard_id: u16,
    segment_id: &str,
) -> StoreResult<ObjectPath> {
    segment_object_path_with_prefix("agent_segments", project_id, date, shard_id, segment_id)
}

pub fn segment_object_path_with_prefix(
    prefix_root: &str,
    project_id: &str,
    date: &str,
    shard_id: u16,
    segment_id: &str,
) -> StoreResult<ObjectPath> {
    validate_identifier("project_id", project_id)?;
    validate_identifier("date", date)?;
    validate_identifier("segment_id", segment_id)?;
    Ok(ObjectPath::from(format!(
        "{prefix_root}/{project_id}/{date}/shard_{shard_id:02}/{segment_id}.plss"
    )))
}

pub struct SegmentWriter;

impl SegmentWriter {
    pub fn build_plss_bytes(
        entries: Vec<(AgentTrace, Vec<AgentSpan>)>,
    ) -> StoreResult<(Vec<u8>, u64, SegmentZoneMap)> {
        let ranked = entries
            .into_iter()
            .map(|(tr, sps)| {
                let meta = TraceCrdtMeta::from_trace(&tr);
                let ranked_sps = sps
                    .into_iter()
                    .map(|s| {
                        let sr = SpanFieldRanks::from_span(&s);
                        (s, sr)
                    })
                    .collect();
                (tr, meta, ranked_sps)
            })
            .collect();
        Self::build_plss_bytes_ranked(ranked)
    }

    pub(crate) fn build_plss_bytes_ranked(
        mut entries: Vec<RankedTraceEntry>,
    ) -> StoreResult<(Vec<u8>, u64, SegmentZoneMap)> {
        // Normalize, CRDT-dedup spans, and sort deterministically by (start_ts_ms, trace_id)
        for (trace, meta, spans) in &mut entries {
            let mut span_map = HashMap::with_capacity(spans.len());
            merge_spans_crdt_with_ranks(&mut span_map, spans.drain(..));
            let mut deduped_ranked: Vec<(AgentSpan, SpanFieldRanks)> =
                span_map.into_values().collect();
            deduped_ranked.sort_by(|a, b| {
                a.0.start_ts_ms
                    .cmp(&b.0.start_ts_ms)
                    .then_with(|| a.0.span_id.cmp(&b.0.span_id))
            });
            let deduped_spans: Vec<AgentSpan> =
                deduped_ranked.iter().map(|(s, _)| s.clone()).collect();
            recompute_trace_from_spans_with_explicit(
                trace,
                meta.explicit_status,
                meta.explicit_failure_source,
                meta.explicit_has_loop_warning,
                meta.explicit_primary_model.clone(),
                meta.explicit_primary_provider.clone(),
                &deduped_spans,
            );
            *spans = deduped_ranked;
            sync_trace_cost(trace);
            for (sp, _) in spans.iter_mut() {
                sync_span_cost(sp);
            }
        }
        entries.sort_by(|a, b| {
            a.0.start_ts_ms
                .cmp(&b.0.start_ts_ms)
                .then_with(|| a.0.trace_id.cmp(&b.0.trace_id))
        });

        let mut interner = StringInterner::new();
        let mut blob_acc = BlobPackAccumulator::new();
        let mut summary_rows = Vec::with_capacity(entries.len());
        let mut span_metric_rows = Vec::new();
        let mut tree_refs = Vec::with_capacity(entries.len());

        struct SessionAgg {
            user_id: Option<String>,
            environment: Option<String>,
            agent_names: BTreeSet<String>,
            first_ts_ms: i64,
            last_ts_ms: i64,
            trace_count: i64,
            error_count: i64,
            total_tokens: i64,
            total_cost_nanos: i64,
            trace_ids: BTreeSet<String>,
        }
        let mut sessions: BTreeMap<String, SessionAgg> = BTreeMap::new();

        for (idx, (trace, meta, spans)) in entries.iter().enumerate() {
            let plain_spans: Vec<AgentSpan> = spans.iter().map(|(s, _)| s.clone()).collect();
            blob_acc.maybe_flush_for_next_trace(estimate_trace_blob_bytes(trace, &plain_spans))?;
            let trace_idx = idx as u32;
            let trace_id_str = interner.intern(&trace.trace_id)?;
            let mut tags_str = Vec::with_capacity(trace.tags.len());
            for t in &trace.tags {
                tags_str.push(interner.intern(t)?);
            }

            let mut span_crdts = Vec::with_capacity(spans.len());
            let mut span_refs = Vec::with_capacity(spans.len());
            for (sp, sp_ranks) in spans {
                let span_id_str = interner.intern(&sp.span_id)?;
                let prov_str = interner.intern_opt(sp.provider.as_deref())?;
                let model_str = interner.intern_opt(sp.model.as_deref())?;
                let tool_str = interner.intern_opt(sp.tool_name.as_deref())?;
                let span_name_str = interner.intern(&sp.name)?;
                let effective_tool_rank = sp_ranks.tool_name_rank.or_else(|| {
                    sp.tool_name
                        .as_ref()
                        .map(|_| (sp.span_phase.priority(), sp.start_ts_ms))
                });
                let (tool_name_phase, tool_name_ts) = match effective_tool_rank {
                    Some((p, ts)) => (p, ts),
                    None => (0xFF, 0),
                };

                span_crdts.push(SpanCrdtDelta {
                    span_id_str,
                    span_type: sp.span_type.to_u8(),
                    span_phase: sp.span_phase.to_u8(),
                    status: sp.status.to_u8(),
                    depth: sp.depth,
                    start_ts_ms: sp.start_ts_ms,
                    end_ts_ms: sp.end_ts_ms,
                    duration_ms: sp.duration_ms,
                    input_tokens: sp.input_tokens,
                    output_tokens: sp.output_tokens,
                    cached_tokens: sp.cached_tokens,
                    total_tokens: sp.total_tokens,
                    cost_nanos: sp.cost_nanos,
                    model_str,
                    provider_str: prov_str,
                    tool_name_str: tool_str,
                    tool_name_phase,
                    tool_name_ts,
                    span_name_str,
                });

                span_metric_rows.push(SpanMetricRow {
                    trace_idx,
                    trace_id_str,
                    span_id_str,
                    span_type: sp.span_type.to_u8(),
                    span_phase: sp.span_phase.to_u8(),
                    status: sp.status.to_u8(),
                    start_ts_ms: sp.start_ts_ms,
                    duration_ms: sp.duration_ms,
                    provider_str: prov_str,
                    model_str,
                    tool_name_str: tool_str,
                    tool_retry_count: sp.tool_retry_count,
                    input_tokens: sp.input_tokens,
                    output_tokens: sp.output_tokens,
                    cached_tokens: sp.cached_tokens,
                    total_tokens: sp.total_tokens,
                    cost_nanos: sp.cost_nanos,
                });

                let mut ev_refs = Vec::with_capacity(sp.events.len());
                for ev in &sp.events {
                    ev_refs.push(blob_acc.intern_value(trace_idx, &ev.attributes)?);
                }
                span_refs.push(SpanTreeRefs {
                    model_params_ref: blob_acc.intern_value(trace_idx, &sp.model_params)?,
                    input_ref: blob_acc.intern_value(trace_idx, &sp.input)?,
                    output_ref: blob_acc.intern_value(trace_idx, &sp.output)?,
                    attributes_ref: blob_acc.intern_value(trace_idx, &sp.attributes)?,
                    metadata_ref: blob_acc.intern_value(trace_idx, &sp.metadata)?,
                    event_attr_refs: ev_refs,
                });
            }

            let tr_in_ref = blob_acc.intern_value(trace_idx, &trace.input)?;
            let tr_out_ref = blob_acc.intern_value(trace_idx, &trace.output)?;
            let tr_meta_ref = blob_acc.intern_value(trace_idx, &trace.metadata)?;
            let has_trace_payloads = u8::from(
                tr_in_ref != NULL_BLOB_REF
                    || tr_out_ref != NULL_BLOB_REF
                    || tr_meta_ref != NULL_BLOB_REF
                    || trace.error_message.is_some(),
            );
            tree_refs.push(TraceTreeRefs {
                input_ref: tr_in_ref,
                output_ref: tr_out_ref,
                metadata_ref: tr_meta_ref,
                spans: span_refs,
            });

            summary_rows.push(TraceSummaryRow {
                trace_id_str,
                session_id_str: interner.intern_opt(trace.session_id.as_deref())?,
                user_id_str: interner.intern_opt(trace.user_id.as_deref())?,
                workflow_id_str: interner.intern_opt(trace.workflow_id.as_deref())?,
                parent_trace_id_str: interner.intern_opt(trace.parent_trace_id.as_deref())?,
                agent_name_str: interner.intern(&trace.agent_name)?,
                agent_version_str: interner.intern(&trace.agent_version)?,
                prompt_version_str: interner.intern_opt(trace.prompt_version.as_deref())?,
                model_config_version_str: interner
                    .intern_opt(trace.model_config_version.as_deref())?,
                tool_config_version_str: interner
                    .intern_opt(trace.tool_config_version.as_deref())?,
                app_release_str: interner.intern_opt(trace.app_release.as_deref())?,
                environment_str: interner.intern(&trace.environment)?,
                primary_model_str: interner.intern_opt(trace.primary_model.as_deref())?,
                primary_provider_str: interner.intern_opt(trace.primary_provider.as_deref())?,
                error_type_str: interner.intern_opt(trace.error_type.as_deref())?,
                tags_str,
                status: trace.status.to_u8(),
                failure_source: trace.failure_source.map_or(0, |f| f.to_u8()),
                has_end_ts: u8::from(trace.end_ts_ms.is_some()),
                has_loop_warning: u8::from(trace.has_loop_warning),
                has_trace_payloads,
                start_ts_ms: trace.start_ts_ms,
                end_ts_ms: trace.end_ts_ms.unwrap_or(0),
                duration_ms: trace.duration_ms,
                span_count: trace.span_count,
                llm_call_count: trace.llm_call_count,
                tool_call_count: trace.tool_call_count,
                error_span_count: trace.error_span_count,
                input_tokens: trace.input_tokens,
                output_tokens: trace.output_tokens,
                cached_tokens: trace.cached_tokens,
                total_tokens: trace.total_tokens,
                total_cost_nanos: trace.total_cost_nanos,
                explicit_status: meta.explicit_status.to_u8(),
                explicit_failure_source: meta.explicit_failure_source.map_or(0, |f| f.to_u8()),
                output_rank: meta.field_ranks.output_rank,
                metadata_rank: meta.field_ranks.metadata_rank,
                error_type_rank: meta.field_ranks.error_type_rank,
                error_message_rank: meta.field_ranks.error_message_rank,
                explicit_has_loop_warning: u8::from(meta.explicit_has_loop_warning),
                explicit_primary_model_str: interner
                    .intern_opt(meta.explicit_primary_model.as_deref())?,
                explicit_primary_provider_str: interner
                    .intern_opt(meta.explicit_primary_provider.as_deref())?,
                span_crdts,
            });

            if let Some(sid) = trace.session_id.as_deref().filter(|s| !s.is_empty()) {
                let is_err = matches!(trace.status, TraceStatus::Error | TraceStatus::Timeout);
                let agg = sessions
                    .entry(sid.to_string())
                    .or_insert_with(|| SessionAgg {
                        user_id: trace.user_id.clone().filter(|s| !s.is_empty()),
                        environment: (!trace.environment.is_empty())
                            .then(|| trace.environment.clone()),
                        agent_names: BTreeSet::new(),
                        first_ts_ms: trace.start_ts_ms,
                        last_ts_ms: trace.start_ts_ms,
                        trace_count: 0,
                        error_count: 0,
                        total_tokens: 0,
                        total_cost_nanos: 0,
                        trace_ids: BTreeSet::new(),
                    });
                merge_opt_str_max(&mut agg.user_id, trace.user_id.clone());
                if !trace.environment.is_empty() {
                    let cand_env = trace.environment.as_str();
                    let take_env = match agg.environment.as_deref() {
                        None => true,
                        Some(cur) => (env_rank(cand_env), cand_env) > (env_rank(cur), cur),
                    };
                    if take_env {
                        agg.environment = Some(trace.environment.clone());
                    }
                }
                if !trace.agent_name.is_empty() {
                    agg.agent_names.insert(trace.agent_name.clone());
                }
                agg.first_ts_ms = agg.first_ts_ms.min(trace.start_ts_ms);
                agg.last_ts_ms = agg.last_ts_ms.max(trace.start_ts_ms);
                agg.trace_count = agg.trace_count.saturating_add(1);
                if is_err {
                    agg.error_count = agg.error_count.saturating_add(1);
                }
                agg.total_tokens = agg.total_tokens.saturating_add(trace.total_tokens.max(0));
                agg.total_cost_nanos = agg
                    .total_cost_nanos
                    .saturating_add(trace.total_cost_nanos.max(0));
                agg.trace_ids.insert(trace.trace_id.clone());
            }
        }

        let mut session_rows = Vec::with_capacity(sessions.len());
        for (sid, agg) in sessions {
            let mut agent_name_strs = Vec::with_capacity(agg.agent_names.len());
            for a in &agg.agent_names {
                agent_name_strs.push(interner.intern(a)?);
            }
            let mut trace_id_strs = Vec::with_capacity(agg.trace_ids.len());
            for tid in &agg.trace_ids {
                trace_id_strs.push(interner.intern(tid)?);
            }
            session_rows.push(SessionRollupRow {
                session_id_str: interner.intern(&sid)?,
                user_id_str: interner.intern_opt(agg.user_id.as_deref())?,
                environment_str: interner.intern_opt(agg.environment.as_deref())?,
                agent_name_strs,
                first_ts_ms: agg.first_ts_ms,
                last_ts_ms: agg.last_ts_ms,
                trace_count: agg.trace_count,
                error_count: agg.error_count,
                total_tokens: agg.total_tokens,
                total_cost_nanos: agg.total_cost_nanos,
                trace_id_strs,
            });
        }

        let sec_01 = interner.encode_section()?;
        let sec_02 = encode_trace_summary_section(&summary_rows)?;
        let sec_03 = encode_span_metrics_section(&span_metric_rows)?;
        let sec_04 = encode_tag04_section(&entries, &tree_refs)?;
        let blob_blocks = blob_acc.finish()?;
        let sec_07 = encode_session_rollup_section(&session_rows)?;
        let index_entries: Vec<(AgentTrace, Vec<AgentSpan>)> = entries
            .iter()
            .map(|(tr, _, sps)| (tr.clone(), sps.iter().map(|(s, _)| s.clone()).collect()))
            .collect();
        let mut index_sec =
            SegmentIndexSection::build_from_traces(&index_entries, DEFAULT_BLOOM_BITS_PER_KEY);
        for (idx, (_tr, meta, _sps)) in entries.iter().enumerate() {
            if let Some(epm) = meta
                .explicit_primary_model
                .as_deref()
                .filter(|s| !s.is_empty())
            {
                let trace_idx = idx as u32;
                if let Some(bm) = index_sec.model_bitmaps.get_mut(epm) {
                    bm.insert(trace_idx);
                } else if index_sec.model_bitmaps.len()
                    >= crate::store::bloom::MAX_TAG06_DICT_ENTRIES
                {
                    index_sec.model_truncated = true;
                } else {
                    let mut bm = roaring::RoaringBitmap::new();
                    bm.insert(trace_idx);
                    index_sec.model_bitmaps.insert(epm.to_string(), bm);
                }
            }
        }
        let zone_map = index_sec.zone_map.clone();
        let sec_06 = index_sec.encode()?;

        let mut file = Vec::with_capacity(
            sec_01.len()
                + sec_02.len()
                + sec_03.len()
                + sec_04.len()
                + sec_07.len()
                + sec_06.len()
                + 256,
        );
        let mut dir_entries = Vec::with_capacity(7);

        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::StringTable,
            SEC_FLAG_ZSTD_RAW | SEC_FLAG_CHUNKED,
            &sec_01,
        )?;
        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::TraceSummary,
            SEC_FLAG_ZSTD_RAW | SEC_FLAG_CHUNKED | SEC_FLAG_CRDT_RANKS,
            &sec_02,
        )?;
        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::SpanMetrics,
            SEC_FLAG_ZSTD_RAW | SEC_FLAG_CHUNKED,
            &sec_03,
        )?;
        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::SpanTree,
            SEC_FLAG_ZSTD_DICT | SEC_FLAG_CRDT_RANKS,
            &sec_04,
        )?;

        let tag05_start = file.len() as u64;
        let (sec_05, _metas) = encode_tag05_dir_and_blocks(tag05_start, blob_blocks)?;
        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::PayloadPack,
            SEC_FLAG_ZSTD_DICT | SEC_FLAG_ALIGNED_64K,
            &sec_05,
        )?;

        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::SessionRollup,
            SEC_FLAG_ZSTD_RAW | SEC_FLAG_CHUNKED,
            &sec_07,
        )?;

        let tail_offset = file.len() as u64;
        push_section_bytes(
            &mut file,
            &mut dir_entries,
            SectionTag::IndexBloom,
            0,
            &sec_06,
        )?;

        let dir_offset = file.len() as u64;
        let mut dir_bytes = Vec::with_capacity(dir_entries.len() * PLSS_DIR_ENTRY_LEN);
        for d in &dir_entries {
            d.encode_into(&mut dir_bytes);
        }
        let dir_len = u32::try_from(dir_bytes.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("directory exceeds u32"))?;
        let dir_crc32 = crc32fast::hash(&dir_bytes);
        file.extend_from_slice(&dir_bytes);

        let footer = PlssFooter {
            dir_offset,
            dir_len,
            dir_crc32,
            version_flags: u32::from(PLSS_VERSION),
            magic: PLSS_MAGIC,
        };
        footer.encode_into(&mut file);

        Ok((file, tail_offset, zone_map))
    }

    /// Splits `entries` into one or more immutable `.plss` segments each `<= max_segment_bytes`,
    /// writes them durably to `ObjectStore`, and returns their `SegmentDescriptor`s.
    pub async fn write_segments_split(
        project_id: &str,
        date: &str,
        shard_id: u16,
        level: u8,
        entries: Vec<(AgentTrace, Vec<AgentSpan>)>,
        max_segment_bytes: usize,
        object_store: &Arc<dyn ObjectStore>,
    ) -> StoreResult<Vec<SegmentDescriptor>> {
        let ranked = entries
            .into_iter()
            .map(|(tr, sps)| {
                let meta = TraceCrdtMeta::from_trace(&tr);
                let ranked_sps = sps
                    .into_iter()
                    .map(|s| {
                        let sr = SpanFieldRanks::from_span(&s);
                        (s, sr)
                    })
                    .collect();
                (tr, meta, ranked_sps)
            })
            .collect();
        Self::write_segments_split_ranked(
            project_id,
            date,
            shard_id,
            level,
            ranked,
            max_segment_bytes,
            object_store,
        )
        .await
    }

    pub(crate) async fn write_segments_split_ranked(
        project_id: &str,
        date: &str,
        shard_id: u16,
        level: u8,
        entries: Vec<RankedTraceEntry>,
        max_segment_bytes: usize,
        object_store: &Arc<dyn ObjectStore>,
    ) -> StoreResult<Vec<SegmentDescriptor>> {
        Self::write_segments_split_ranked_with_prefix_and_mode(
            "agent_segments",
            project_id,
            date,
            shard_id,
            level,
            entries,
            max_segment_bytes,
            object_store,
            PutMode::Overwrite,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn write_segments_split_ranked_with_prefix_and_mode(
        prefix_root: &str,
        project_id: &str,
        date: &str,
        shard_id: u16,
        level: u8,
        entries: Vec<RankedTraceEntry>,
        max_segment_bytes: usize,
        object_store: &Arc<dyn ObjectStore>,
        put_mode: PutMode,
        id_prefix: Option<&str>,
    ) -> StoreResult<Vec<SegmentDescriptor>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let cap = max_segment_bytes.clamp(1024, MAX_PLSS_SEGMENT_BYTES);
        let soft_cap = (cap * 7) / 8;

        // Partition entries into size-bounded batches; verify actual PLSS size and recursively split if needed.
        let mut work_queue: Vec<Vec<RankedTraceEntry>> = Vec::new();
        let mut cur_batch = Vec::new();
        let mut est_bytes = 0usize;
        for (tr, meta, sp) in entries {
            let entry_est = estimate_ranked_entry_plss_bytes(&tr, &sp);
            if !cur_batch.is_empty() && est_bytes.saturating_add(entry_est) > soft_cap {
                work_queue.push(std::mem::take(&mut cur_batch));
                est_bytes = 0;
            }
            est_bytes = est_bytes.saturating_add(entry_est);
            cur_batch.push((tr, meta, sp));
        }
        if !cur_batch.is_empty() {
            work_queue.push(cur_batch);
        }

        let mut built_segments = Vec::new();
        let mut idx = 0usize;
        while idx < work_queue.len() {
            let batch = std::mem::take(&mut work_queue[idx]);
            idx += 1;
            let (bytes, tail_offset, zone_map) = Self::build_plss_bytes_ranked(batch.clone())?;
            if bytes.len() > cap && batch.len() > 1 {
                let mid = batch.len() / 2;
                let mut iter = batch.into_iter();
                let left: Vec<_> = iter.by_ref().take(mid).collect();
                let right: Vec<_> = iter.collect();
                work_queue.insert(idx, right);
                work_queue.insert(idx, left);
                continue;
            }
            built_segments.push((bytes, tail_offset, zone_map));
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut descriptors = Vec::with_capacity(built_segments.len());
        let opts = PutOptions {
            mode: put_mode,
            ..PutOptions::default()
        };
        for (part_idx, (bytes, tail_offset, zone_map)) in built_segments.into_iter().enumerate() {
            let seg_id = match id_prefix {
                Some(prefix) => format!("{prefix}_{part_idx:04}"),
                None => Uuid::now_v7().to_string(),
            };
            let path =
                segment_object_path_with_prefix(prefix_root, project_id, date, shard_id, &seg_id)?;
            let file_size = bytes.len() as u64;
            let trace_count = zone_map.trace_count;
            object_store
                .put_opts(
                    &path,
                    PutPayload::from_bytes(Bytes::from(bytes)),
                    opts.clone(),
                )
                .await?;
            descriptors.push(SegmentDescriptor {
                segment_id: seg_id,
                object_path: path.as_ref().to_string(),
                date: date.to_string(),
                shard_id,
                level,
                file_size,
                tail_offset,
                zone_map,
                trace_count,
                created_at_ms: now_ms,
            });
        }
        Ok(descriptors)
    }
}

fn push_section_bytes(
    file: &mut Vec<u8>,
    dir_entries: &mut Vec<SectionDirEntry>,
    tag: SectionTag,
    flags: u16,
    bytes: &[u8],
) -> StoreResult<()> {
    let offset = file.len() as u64;
    let len = u32::try_from(bytes.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("section exceeds u32"))?;
    let crc32 = crc32fast::hash(bytes);
    file.extend_from_slice(bytes);
    dir_entries.push(SectionDirEntry {
        tag: tag as u16,
        flags,
        offset,
        len,
        crc32,
    });
    Ok(())
}

fn estimate_ranked_entry_plss_bytes(
    trace: &AgentTrace,
    spans: &[(AgentSpan, SpanFieldRanks)],
) -> usize {
    let mut b = 512 + trace.input.to_string().len() + trace.output.to_string().len();
    for (sp, _) in spans {
        b = b.saturating_add(
            256 + sp.input.to_string().len()
                + sp.output.to_string().len()
                + sp.attributes.to_string().len(),
        );
    }
    b
}

// ---------------------------------------------------------------------------
// PlssReader (EOF Coalesced Tail + 1-2 Range Trace Hydration)
// ---------------------------------------------------------------------------

fn validate_tag05_dir_len(bc: usize, sec05_len: u32) -> StoreResult<usize> {
    if bc > MAX_CHUNK_COUNT * 4 {
        return Err(StoreError::ResourceLimitExceeded(
            "Tag 0x05 block_count exceeds limit",
        ));
    }
    let needed = bc.checked_mul(28).and_then(|v| v.checked_add(8)).ok_or(
        StoreError::ResourceLimitExceeded("Tag 0x05 directory size overflow"),
    )?;
    if needed > sec05_len as usize {
        return Err(StoreError::Corrupted(
            "Tag 0x05 directory exceeds section length".into(),
        ));
    }
    Ok(needed)
}

pub struct PlssReader {
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<AgentSegmentCache>,
}

impl PlssReader {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache: Arc<AgentSegmentCache>) -> Self {
        Self {
            object_store,
            cache,
        }
    }

    pub async fn read_eof_tail(&self, seg: &SegmentDescriptor) -> StoreResult<Arc<SegmentTail>> {
        if let Some(cached) = self.cache.tail_cache().get(&seg.segment_id) {
            return Ok(cached);
        }
        let cache_key = format!("eof:{}", seg.segment_id);
        let raw_tail = if let Some(plain) = self.cache.hybrid_get_unsealed(&cache_key).await {
            Bytes::from(plain)
        } else {
            if seg.tail_offset >= seg.file_size
                || seg.file_size.saturating_sub(seg.tail_offset) < PLSS_FOOTER_LEN as u64
            {
                return Err(StoreError::Corrupted(format!(
                    "invalid tail_offset {} for file_size {}",
                    seg.tail_offset, seg.file_size
                )));
            }
            let path = ObjectPath::from(seg.object_path.as_str());
            let fetched = self
                .object_store
                .get_range(&path, seg.tail_offset..seg.file_size)
                .await?;
            self.cache.hybrid_insert_sealed(cache_key, &fetched);
            fetched
        };

        let tail = Arc::new(decode_tail_slice(
            seg.tail_offset,
            seg.file_size,
            &raw_tail,
        )?);
        self.cache
            .tail_cache()
            .insert(seg.segment_id.clone(), tail.clone());
        Ok(tail)
    }

    pub async fn read_section(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
        tag: SectionTag,
    ) -> StoreResult<Bytes> {
        let entry = tail.find_section(tag).ok_or_else(|| {
            StoreError::Corrupted(format!(
                "missing section 0x{:04x} in {}",
                tag as u16, seg.segment_id
            ))
        })?;
        let cache_key = format!("sec:{}:{:02x}", seg.segment_id, tag as u16);
        let raw = if let Some(plain) = self.cache.hybrid_get_unsealed(&cache_key).await {
            Bytes::from(plain)
        } else {
            let end = entry
                .offset
                .checked_add(entry.len as u64)
                .ok_or(StoreError::ResourceLimitExceeded("section end overflow"))?;
            if end > tail.footer.dir_offset {
                return Err(StoreError::Corrupted(
                    "section bounds exceed directory offset".into(),
                ));
            }
            let path = ObjectPath::from(seg.object_path.as_str());
            let fetched = self
                .object_store
                .get_range(&path, entry.offset..end)
                .await?;
            self.cache.hybrid_insert_sealed(cache_key, &fetched);
            fetched
        };
        if raw.len() != entry.len as usize {
            return Err(StoreError::Corrupted("section length mismatch".into()));
        }
        let actual_crc = crc32fast::hash(&raw);
        if actual_crc != entry.crc32 {
            return Err(StoreError::ChecksumMismatch {
                context: "PLSS section",
                expected: entry.crc32,
                actual: actual_crc,
            });
        }
        Ok(raw)
    }

    pub async fn read_string_table(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
    ) -> StoreResult<Arc<SegmentStringTable>> {
        if let Some(cached) = self.cache.strings_cache().get(&seg.segment_id) {
            return Ok(cached);
        }
        let raw = self
            .read_section(seg, tail, SectionTag::StringTable)
            .await?;
        let table = Arc::new(SegmentStringTable::decode_section(&raw)?);
        self.cache
            .strings_cache()
            .insert(seg.segment_id.clone(), table.clone());
        Ok(table)
    }

    pub async fn read_trace_summaries(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
    ) -> StoreResult<Arc<Vec<TraceSummaryRow>>> {
        if let Some(cached) = self.cache.summaries_cache().get(&seg.segment_id) {
            return Ok(cached);
        }
        let sec02 = tail.find_section(SectionTag::TraceSummary).ok_or_else(|| {
            StoreError::Corrupted(format!("missing section 0x0002 in {}", seg.segment_id))
        })?;
        let has_ranks = (sec02.flags & SEC_FLAG_CRDT_RANKS) != 0;
        let raw = self
            .read_section(seg, tail, SectionTag::TraceSummary)
            .await?;
        let rows = Arc::new(decode_trace_summary_section_with_ranks(&raw, has_ranks)?);
        self.cache
            .summaries_cache()
            .insert(seg.segment_id.clone(), rows.clone());
        Ok(rows)
    }

    pub async fn read_span_metrics(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
    ) -> StoreResult<Arc<Vec<SpanMetricRow>>> {
        if let Some(cached) = self.cache.span_metrics_cache().get(&seg.segment_id) {
            return Ok(cached);
        }
        let raw = self
            .read_section(seg, tail, SectionTag::SpanMetrics)
            .await?;
        let rows = Arc::new(decode_span_metrics_section(&raw)?);
        self.cache
            .span_metrics_cache()
            .insert(seg.segment_id.clone(), rows.clone());
        Ok(rows)
    }

    pub async fn read_session_rollups(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
    ) -> StoreResult<Arc<Vec<SessionRollupRow>>> {
        if let Some(cached) = self.cache.session_rollups_cache().get(&seg.segment_id) {
            return Ok(cached);
        }
        let raw = self
            .read_section(seg, tail, SectionTag::SessionRollup)
            .await?;
        let rows = Arc::new(decode_session_rollup_section(&raw)?);
        self.cache
            .session_rollups_cache()
            .insert(seg.segment_id.clone(), rows.clone());
        Ok(rows)
    }

    /// Hydrates a single trace + its spans + all payloads using coalesced `get_range` calls
    /// (typically 1–2 `get_range` calls after `read_eof_tail`).
    pub async fn read_trace_ranges(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
        trace_id: &str,
    ) -> StoreResult<Option<(AgentTrace, Vec<AgentSpan>)>> {
        Ok(self
            .read_trace_ranges_ranked(seg, tail, trace_id)
            .await?
            .map(|(tr, _meta, spans)| (tr, spans.into_iter().map(|(s, _)| s).collect())))
    }

    pub(crate) async fn read_trace_ranges_ranked(
        &self,
        seg: &SegmentDescriptor,
        tail: &SegmentTail,
        trace_id: &str,
    ) -> StoreResult<Option<(AgentTrace, TraceCrdtMeta, Vec<(AgentSpan, SpanFieldRanks)>)>> {
        if !tail.index.may_contain_trace(trace_id) {
            return Ok(None);
        }
        let sec04 = tail
            .find_section(SectionTag::SpanTree)
            .ok_or_else(|| StoreError::Corrupted("missing Tag 0x04".into()))?;
        let has_ranks = (sec04.flags & SEC_FLAG_CRDT_RANKS) != 0;
        let sec05 = tail
            .find_section(SectionTag::PayloadPack)
            .ok_or_else(|| StoreError::Corrupted("missing Tag 0x05".into()))?;

        let path = ObjectPath::from(seg.object_path.as_str());
        let p4idx_key = format!("p4idx:{}", seg.segment_id);
        let p5dir_key = format!("p5dir:{}", seg.segment_id);

        let cached_p4idx = self.cache.hybrid_get_unsealed(&p4idx_key).await;
        let cached_p5dir = self.cache.hybrid_get_unsealed(&p5dir_key).await;

        let sec04_end = sec04
            .offset
            .checked_add(sec04.len as u64)
            .ok_or(StoreError::ResourceLimitExceeded("Tag 0x04 end overflow"))?;
        let sec05_end = sec05
            .offset
            .checked_add(sec05.len as u64)
            .ok_or(StoreError::ResourceLimitExceeded("Tag 0x05 end overflow"))?;

        // Fast cold path: when Tag 0x04 <= 256 KiB, Tag 0x05 immediately follows Tag 0x04,
        // and indices aren't cached, fetch Tag 0x04 + Tag 0x05 dir in 1 get_range
        let mut prefetched_sec04: Option<Bytes> = None;
        let (p4idx, p5dir) = if cached_p4idx.is_none()
            && cached_p5dir.is_none()
            && sec04.len <= 256 * 1024
            && sec05.offset == sec04_end
        {
            let probe_p5 = (sec05.len as u64).min(4096);
            let fetch_end = sec05
                .offset
                .checked_add(probe_p5)
                .ok_or(StoreError::ResourceLimitExceeded("Tag 0x05 probe overflow"))?
                .min(sec05_end);
            let combined = self
                .object_store
                .get_range(&path, sec04.offset..fetch_end)
                .await?;
            let s04_len = sec04.len as usize;
            if combined.len() < s04_len {
                return Err(StoreError::Corrupted("short read for Tag 0x04".into()));
            }
            let s04_slice = combined.slice(0..s04_len);
            if crc32fast::hash(&s04_slice) != sec04.crc32 {
                return Err(StoreError::ChecksumMismatch {
                    context: "Tag 0x04",
                    expected: sec04.crc32,
                    actual: crc32fast::hash(&s04_slice),
                });
            }
            let idx4 = Tag04Index::decode(&s04_slice)?;
            let idx4_total = 12 + idx4.index_bytes as usize;
            self.cache
                .hybrid_insert_sealed(p4idx_key, &s04_slice[..idx4_total]);
            prefetched_sec04 = Some(s04_slice);

            let p5_probe = &combined[s04_len..];
            if p5_probe.len() < 8 {
                return Err(StoreError::Corrupted("truncated Tag 0x05 header".into()));
            }
            let bc =
                u32::from_le_bytes([p5_probe[0], p5_probe[1], p5_probe[2], p5_probe[3]]) as usize;
            let needed = validate_tag05_dir_len(bc, sec05.len)?;
            let p5_full_dir_bytes = if needed <= p5_probe.len() {
                Bytes::copy_from_slice(&p5_probe[..needed])
            } else {
                self.object_store
                    .get_range(&path, sec05.offset..sec05.offset + needed as u64)
                    .await?
            };
            let dir5 = decode_tag05_dir(&p5_full_dir_bytes)?;
            self.cache
                .hybrid_insert_sealed(p5dir_key, &p5_full_dir_bytes);
            (idx4, dir5)
        } else {
            let idx4 = if let Some(b) = cached_p4idx {
                Tag04Index::decode(&b)?
            } else {
                let first_len = (sec04.len as u64).min(65_536);
                let head = self
                    .object_store
                    .get_range(&path, sec04.offset..sec04.offset + first_len)
                    .await?;
                if head.len() < 12 {
                    return Err(StoreError::Corrupted("truncated Tag 0x04 header".into()));
                }
                let idx_bytes = u32::from_le_bytes([head[8], head[9], head[10], head[11]]) as usize;
                let total_idx = 12usize
                    .checked_add(idx_bytes)
                    .ok_or(StoreError::ResourceLimitExceeded("Tag 0x04 index overflow"))?;
                let full_idx_bytes = if total_idx <= head.len() {
                    head.slice(0..total_idx)
                } else {
                    if total_idx > sec04.len as usize {
                        return Err(StoreError::Corrupted(
                            "Tag 0x04 index exceeds section length".into(),
                        ));
                    }
                    self.object_store
                        .get_range(&path, sec04.offset..sec04.offset + total_idx as u64)
                        .await?
                };
                let parsed = Tag04Index::decode(&full_idx_bytes)?;
                self.cache.hybrid_insert_sealed(p4idx_key, &full_idx_bytes);
                parsed
            };

            let dir5 = if let Some(b) = cached_p5dir {
                decode_tag05_dir(&b)?
            } else {
                let probe_len = (sec05.len as u64).min(4096);
                let head = self
                    .object_store
                    .get_range(&path, sec05.offset..sec05.offset + probe_len)
                    .await?;
                if head.len() < 8 {
                    return Err(StoreError::Corrupted("truncated Tag 0x05 header".into()));
                }
                let bc = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
                let needed = validate_tag05_dir_len(bc, sec05.len)?;
                let full_dir = if needed <= head.len() {
                    head.slice(0..needed)
                } else {
                    self.object_store
                        .get_range(&path, sec05.offset..sec05.offset + needed as u64)
                        .await?
                };
                let parsed = decode_tag05_dir(&full_dir)?;
                self.cache.hybrid_insert_sealed(p5dir_key, &full_dir);
                parsed
            };
            (idx4, dir5)
        };

        let target_hash = xxh3_64(trace_id.as_bytes());
        let mut candidate_p4_blocks: Vec<u32> = Vec::new();
        let mut candidate_trace_indices: Vec<u32> = Vec::new();
        for (t_idx, entry) in p4idx.trace_to_block.iter().enumerate() {
            if entry.trace_id_hash == target_hash {
                candidate_trace_indices.push(t_idx as u32);
                for b in entry.block_start..entry.block_start.saturating_add(entry.block_count) {
                    if !candidate_p4_blocks.contains(&b) {
                        candidate_p4_blocks.push(b);
                    }
                }
            }
        }
        if candidate_p4_blocks.is_empty() {
            return Ok(None);
        }

        // Determine needed Tag 0x04 and Tag 0x05 block ranges that are not cached so we can coalesce them
        let p4_body_base = sec04
            .offset
            .checked_add(12)
            .and_then(|v| v.checked_add(p4idx.index_bytes as u64))
            .ok_or(StoreError::ResourceLimitExceeded(
                "Tag 0x04 body base overflow",
            ))?;
        let mut needed_p4: Vec<u32> = Vec::new();
        let mut p4_decoded_blocks: HashMap<u32, Vec<DecodedTag04Entry>> = HashMap::new();

        for &blk_idx in &candidate_p4_blocks {
            let meta = p4idx.block_dir.get(blk_idx as usize).ok_or_else(|| {
                StoreError::Corrupted(format!("invalid Tag 0x04 block index {blk_idx}"))
            })?;
            let start = p4_body_base.checked_add(meta.rel_offset).ok_or(
                StoreError::ResourceLimitExceeded("Tag 0x04 block start overflow"),
            )?;
            let end = start.checked_add(meta.comp_len as u64).ok_or(
                StoreError::ResourceLimitExceeded("Tag 0x04 block end overflow"),
            )?;
            if end > sec04_end {
                return Err(StoreError::Corrupted("Tag 0x04 block out of bounds".into()));
            }
            if let Some(ref s04) = prefetched_sec04 {
                let rel_s = (start - sec04.offset) as usize;
                let rel_e = (end - sec04.offset) as usize;
                let uncomp = decompress_block(
                    &s04[rel_s..rel_e],
                    meta.codec,
                    meta.uncomp_len as usize,
                    meta.crc32,
                    "Tag 0x04 block",
                )?;
                let key = format!("p4blk:{}:{blk_idx}", seg.segment_id);
                self.cache.hybrid_insert_sealed(key, &uncomp);
                p4_decoded_blocks.insert(blk_idx, decode_tag04_block(&uncomp, has_ranks)?);
            } else {
                let key = format!("p4blk:{}:{blk_idx}", seg.segment_id);
                if let Some(plain) = self.cache.hybrid_get_unsealed(&key).await {
                    p4_decoded_blocks.insert(blk_idx, decode_tag04_block(&plain, has_ranks)?);
                } else {
                    needed_p4.push(blk_idx);
                }
            }
        }

        // If all candidate Tag 0x04 blocks are already decoded (prefetched or cached), read exact Tag 0x05 block refs;
        // otherwise predict Tag 0x05 blocks from trace_idx so cold/warm reads coalesce Tag 0x04 + Tag 0x05.
        let mut found: Option<DecodedTag04Entry> = None;
        let mut predicted_p5: Vec<u32> = Vec::new();
        if needed_p4.is_empty() {
            for blk_idx in &candidate_p4_blocks {
                if let Some(entries) = p4_decoded_blocks.remove(blk_idx) {
                    for e in entries {
                        if e.trace.trace_id == trace_id {
                            found = Some(e);
                            break;
                        }
                    }
                }
                if found.is_some() {
                    break;
                }
            }
            let Some(ref e) = found else {
                return Ok(None);
            };
            let mut push_ref = |r: BlobRef| {
                if r != NULL_BLOB_REF && !predicted_p5.contains(&r.pack_block_idx) {
                    predicted_p5.push(r.pack_block_idx);
                }
            };
            push_ref(e.trace_refs.input_ref);
            push_ref(e.trace_refs.output_ref);
            push_ref(e.trace_refs.metadata_ref);
            for sr in &e.trace_refs.spans {
                push_ref(sr.model_params_ref);
                push_ref(sr.input_ref);
                push_ref(sr.output_ref);
                push_ref(sr.attributes_ref);
                push_ref(sr.metadata_ref);
                for &er in &sr.event_attr_refs {
                    push_ref(er);
                }
            }
        } else {
            for &t_idx in &candidate_trace_indices {
                if !p5dir.is_empty() {
                    let pos = p5dir.partition_point(|b| b.first_trace_idx <= t_idx);
                    if pos > 0 {
                        let b = (pos - 1) as u32;
                        if !predicted_p5.contains(&b) {
                            predicted_p5.push(b);
                        }
                        if pos >= 2
                            && p5dir[pos - 1].first_trace_idx == t_idx
                            && p5dir[pos - 2].first_trace_idx < t_idx
                        {
                            let prev_b = (pos - 2) as u32;
                            if !predicted_p5.contains(&prev_b) {
                                predicted_p5.push(prev_b);
                            }
                        }
                    }
                    for (bi, bm) in p5dir.iter().enumerate() {
                        if bm.first_trace_idx == t_idx && !predicted_p5.contains(&(bi as u32)) {
                            predicted_p5.push(bi as u32);
                        }
                    }
                }
            }
        }

        let mut raw_ranges: Vec<Range<u64>> = Vec::new();
        for &b in &needed_p4 {
            let meta = &p4idx.block_dir[b as usize];
            let s = p4_body_base + meta.rel_offset;
            raw_ranges.push(s..s + meta.comp_len as u64);
        }
        let mut needed_p5_predicted: Vec<u32> = Vec::new();
        let mut p5_packs: HashMap<u32, DecodedBlobPack> = HashMap::new();
        for &b in &predicted_p5 {
            let key = format!("p5blk:{}:{b}", seg.segment_id);
            if let Some(plain) = self.cache.hybrid_get_unsealed(&key).await {
                p5_packs.insert(b, DecodedBlobPack::decode(&plain)?);
            } else if let Some(meta) = p5dir.get(b as usize) {
                let s = meta.file_offset;
                let e = s.checked_add(meta.comp_len as u64).ok_or(
                    StoreError::ResourceLimitExceeded("Tag 0x05 block end overflow"),
                )?;
                if s < sec05.offset || e > sec05_end {
                    return Err(StoreError::Corrupted("Tag 0x05 block out of bounds".into()));
                }
                needed_p5_predicted.push(b);
                raw_ranges.push(s..e);
            }
        }

        if !raw_ranges.is_empty() {
            let fetched = coalesce_and_fetch_ranges(
                &self.object_store,
                &path,
                &raw_ranges,
                COALESCE_MAX_GAP_BYTES,
                COALESCE_MAX_SPAN_BYTES,
            )
            .await?;

            for &b in &needed_p4 {
                let meta = &p4idx.block_dir[b as usize];
                let s = p4_body_base + meta.rel_offset;
                let comp = slice_from_coalesced(&fetched, s..s + meta.comp_len as u64)?;
                let uncomp = decompress_block(
                    comp,
                    meta.codec,
                    meta.uncomp_len as usize,
                    meta.crc32,
                    "Tag 0x04 block",
                )?;
                let key = format!("p4blk:{}:{b}", seg.segment_id);
                self.cache.hybrid_insert_sealed(key, &uncomp);
                p4_decoded_blocks.insert(b, decode_tag04_block(&uncomp, has_ranks)?);
            }

            for &b in &needed_p5_predicted {
                let meta = &p5dir[b as usize];
                let s = meta.file_offset;
                let comp = slice_from_coalesced(&fetched, s..s + meta.comp_len as u64)?;
                let uncomp = decompress_block(
                    comp,
                    meta.codec,
                    meta.uncomp_len as usize,
                    meta.crc32,
                    "Tag 0x05 block",
                )?;
                let key = format!("p5blk:{}:{b}", seg.segment_id);
                self.cache.hybrid_insert_sealed(key, &uncomp);
                p5_packs.insert(b, DecodedBlobPack::decode(&uncomp)?);
            }
        }

        // Locate exact matching trace entry if not already found
        if found.is_none() {
            for blk_idx in candidate_p4_blocks {
                if let Some(entries) = p4_decoded_blocks.remove(&blk_idx) {
                    for entry in entries {
                        if entry.trace.trace_id == trace_id {
                            found = Some(entry);
                            break;
                        }
                    }
                }
                if found.is_some() {
                    break;
                }
            }
        }
        let Some(mut entry) = found else {
            return Ok(None);
        };

        // Ensure all referenced Tag 0x05 blocks are loaded (normally 0 extra fetches because predicted!)
        let mut extra_p5: Vec<u32> = Vec::new();
        let collect_ref =
            |r: BlobRef, p5_packs: &HashMap<u32, DecodedBlobPack>, extra: &mut Vec<u32>| {
                if r != NULL_BLOB_REF
                    && !p5_packs.contains_key(&r.pack_block_idx)
                    && !extra.contains(&r.pack_block_idx)
                {
                    extra.push(r.pack_block_idx);
                }
            };
        collect_ref(entry.trace_refs.input_ref, &p5_packs, &mut extra_p5);
        collect_ref(entry.trace_refs.output_ref, &p5_packs, &mut extra_p5);
        collect_ref(entry.trace_refs.metadata_ref, &p5_packs, &mut extra_p5);
        for sr in &entry.trace_refs.spans {
            collect_ref(sr.model_params_ref, &p5_packs, &mut extra_p5);
            collect_ref(sr.input_ref, &p5_packs, &mut extra_p5);
            collect_ref(sr.output_ref, &p5_packs, &mut extra_p5);
            collect_ref(sr.attributes_ref, &p5_packs, &mut extra_p5);
            collect_ref(sr.metadata_ref, &p5_packs, &mut extra_p5);
            for &er in &sr.event_attr_refs {
                collect_ref(er, &p5_packs, &mut extra_p5);
            }
        }
        if !extra_p5.is_empty() {
            let mut ranges = Vec::new();
            for &b in &extra_p5 {
                let meta = p5dir.get(b as usize).ok_or_else(|| {
                    StoreError::Corrupted(format!("invalid Tag 0x05 block idx {b}"))
                })?;
                let end = meta.file_offset.checked_add(meta.comp_len as u64).ok_or(
                    StoreError::ResourceLimitExceeded("Tag 0x05 block end overflow"),
                )?;
                if meta.file_offset < sec05.offset || end > sec05_end {
                    return Err(StoreError::Corrupted("Tag 0x05 block out of bounds".into()));
                }
                ranges.push(meta.file_offset..end);
            }
            let fetched = coalesce_and_fetch_ranges(
                &self.object_store,
                &path,
                &ranges,
                COALESCE_MAX_GAP_BYTES,
                COALESCE_MAX_SPAN_BYTES,
            )
            .await?;
            for &b in &extra_p5 {
                let meta = &p5dir[b as usize];
                let comp = slice_from_coalesced(
                    &fetched,
                    meta.file_offset..meta.file_offset + meta.comp_len as u64,
                )?;
                let uncomp = decompress_block(
                    comp,
                    meta.codec,
                    meta.uncomp_len as usize,
                    meta.crc32,
                    "Tag 0x05 block",
                )?;
                let key = format!("p5blk:{}:{b}", seg.segment_id);
                self.cache.hybrid_insert_sealed(key, &uncomp);
                p5_packs.insert(b, DecodedBlobPack::decode(&uncomp)?);
            }
        }

        hydrate_trace_entry(&mut entry, &p5_packs)?;
        Ok(Some((entry.trace, entry.trace_meta, entry.spans)))
    }

    /// Reads all `(AgentTrace, Vec<AgentSpan>)` entries in a segment (used by M1c compaction).
    #[allow(dead_code)]
    pub async fn read_all_traces_and_spans(
        &self,
        seg: &SegmentDescriptor,
    ) -> StoreResult<Vec<(AgentTrace, Vec<AgentSpan>)>> {
        Ok(self
            .read_all_traces_and_spans_ranked(seg)
            .await?
            .into_iter()
            .map(|(tr, _meta, spans)| (tr, spans.into_iter().map(|(s, _)| s).collect()))
            .collect())
    }

    pub(crate) async fn read_all_traces_and_spans_ranked(
        &self,
        seg: &SegmentDescriptor,
    ) -> StoreResult<Vec<(AgentTrace, TraceCrdtMeta, Vec<(AgentSpan, SpanFieldRanks)>)>> {
        let tail = self.read_eof_tail(seg).await?;
        let sec04_entry = tail
            .find_section(SectionTag::SpanTree)
            .ok_or_else(|| StoreError::Corrupted("missing Tag 0x04".into()))?;
        let has_ranks = (sec04_entry.flags & SEC_FLAG_CRDT_RANKS) != 0;
        let sec04_raw = self.read_section(seg, &tail, SectionTag::SpanTree).await?;
        let sec05_raw = self
            .read_section(seg, &tail, SectionTag::PayloadPack)
            .await?;
        let sec05_entry = tail
            .find_section(SectionTag::PayloadPack)
            .ok_or_else(|| StoreError::Corrupted("missing Tag 0x05".into()))?;

        let idx4 = Tag04Index::decode(&sec04_raw)?;
        let p4_body_offset = 12 + idx4.index_bytes as usize;
        let dir5 = decode_tag05_dir(&sec05_raw)?;

        let mut p5_packs = HashMap::with_capacity(dir5.len());
        for (bi, meta) in dir5.iter().enumerate() {
            let rel_s = meta
                .file_offset
                .checked_sub(sec05_entry.offset)
                .ok_or_else(|| StoreError::Corrupted("Tag 0x05 offset underflow".into()))?
                as usize;
            let rel_e = rel_s
                .checked_add(meta.comp_len as usize)
                .ok_or(StoreError::ResourceLimitExceeded("Tag 0x05 len overflow"))?;
            if rel_e > sec05_raw.len() {
                return Err(StoreError::Corrupted(
                    "Tag 0x05 block exceeds section".into(),
                ));
            }
            let uncomp = decompress_block(
                &sec05_raw[rel_s..rel_e],
                meta.codec,
                meta.uncomp_len as usize,
                meta.crc32,
                "Tag 0x05 block",
            )?;
            p5_packs.insert(bi as u32, DecodedBlobPack::decode(&uncomp)?);
        }

        let mut out = Vec::new();
        for meta in &idx4.block_dir {
            let s = p4_body_offset.checked_add(meta.rel_offset as usize).ok_or(
                StoreError::ResourceLimitExceeded("Tag 0x04 offset overflow"),
            )?;
            let e = s
                .checked_add(meta.comp_len as usize)
                .ok_or(StoreError::ResourceLimitExceeded("Tag 0x04 len overflow"))?;
            if e > sec04_raw.len() {
                return Err(StoreError::Corrupted(
                    "Tag 0x04 block exceeds section".into(),
                ));
            }
            let uncomp = decompress_block(
                &sec04_raw[s..e],
                meta.codec,
                meta.uncomp_len as usize,
                meta.crc32,
                "Tag 0x04 block",
            )?;
            let mut entries = decode_tag04_block(&uncomp, has_ranks)?;
            for entry in &mut entries {
                hydrate_trace_entry(entry, &p5_packs)?;
            }
            for entry in entries {
                out.push((entry.trace, entry.trace_meta, entry.spans));
            }
        }
        Ok(out)
    }
}

fn hydrate_trace_entry(
    entry: &mut DecodedTag04Entry,
    p5_packs: &HashMap<u32, DecodedBlobPack>,
) -> StoreResult<()> {
    let mat = |r: BlobRef| -> StoreResult<Value> {
        if r == NULL_BLOB_REF {
            return Ok(Value::Null);
        }
        let pack = p5_packs.get(&r.pack_block_idx).ok_or_else(|| {
            StoreError::Corrupted(format!("missing BlobPack block {}", r.pack_block_idx))
        })?;
        pack.materialize(r)
    };
    entry.trace.input = mat(entry.trace_refs.input_ref)?;
    entry.trace.output = mat(entry.trace_refs.output_ref)?;
    entry.trace.metadata = mat(entry.trace_refs.metadata_ref)?;
    for ((sp, _), sr) in entry.spans.iter_mut().zip(entry.trace_refs.spans.iter()) {
        sp.model_params = mat(sr.model_params_ref)?;
        sp.input = mat(sr.input_ref)?;
        sp.output = mat(sr.output_ref)?;
        sp.attributes = mat(sr.attributes_ref)?;
        sp.metadata = mat(sr.metadata_ref)?;
        for (ev, &er) in sp.events.iter_mut().zip(sr.event_attr_refs.iter()) {
            ev.attributes = mat(er)?;
        }
    }
    Ok(())
}

pub(crate) fn decode_tail_slice(
    tail_offset: u64,
    file_size: u64,
    raw_tail: &[u8],
) -> StoreResult<SegmentTail> {
    let expected_len = file_size
        .checked_sub(tail_offset)
        .ok_or_else(|| StoreError::Corrupted("tail_offset > file_size".into()))?;
    if raw_tail.len() as u64 != expected_len || raw_tail.len() < PLSS_FOOTER_LEN {
        return Err(StoreError::Corrupted("invalid PLSS tail length".into()));
    }
    let footer = PlssFooter::decode(&raw_tail[raw_tail.len() - PLSS_FOOTER_LEN..])?;
    let dir_rel_start = footer
        .dir_offset
        .checked_sub(tail_offset)
        .ok_or_else(|| StoreError::Corrupted("dir_offset < tail_offset".into()))?
        as usize;
    let dir_len = footer.dir_len as usize;
    if dir_len % PLSS_DIR_ENTRY_LEN != 0 || dir_len / PLSS_DIR_ENTRY_LEN > MAX_SECTION_ENTRIES {
        return Err(StoreError::Corrupted("invalid PLSS dir_len".into()));
    }
    let dir_rel_end = dir_rel_start
        .checked_add(dir_len)
        .ok_or(StoreError::ResourceLimitExceeded("dir_end overflow"))?;
    if dir_rel_end != raw_tail.len() - PLSS_FOOTER_LEN {
        return Err(StoreError::Corrupted(
            "directory does not end immediately before footer".into(),
        ));
    }
    let dir_slice = &raw_tail[dir_rel_start..dir_rel_end];
    let actual_dir_crc = crc32fast::hash(dir_slice);
    if actual_dir_crc != footer.dir_crc32 {
        return Err(StoreError::ChecksumMismatch {
            context: "PLSS directory",
            expected: footer.dir_crc32,
            actual: actual_dir_crc,
        });
    }

    let mut directory = Vec::with_capacity(dir_len / PLSS_DIR_ENTRY_LEN);
    for chunk in dir_slice.chunks_exact(PLSS_DIR_ENTRY_LEN) {
        let entry = SectionDirEntry::decode(chunk)?;
        let end = entry
            .offset
            .checked_add(entry.len as u64)
            .ok_or(StoreError::ResourceLimitExceeded("section end overflow"))?;
        if end > footer.dir_offset {
            return Err(StoreError::Corrupted(
                "section bounds exceed dir_offset".into(),
            ));
        }
        directory.push(entry);
    }

    // Verify sections do not overlap
    let mut sorted = directory.clone();
    sorted.sort_by_key(|d| d.offset);
    for w in sorted.windows(2) {
        if w[0].offset + w[0].len as u64 > w[1].offset {
            return Err(StoreError::Corrupted("overlapping PLSS sections".into()));
        }
    }

    let tag06_entry = directory
        .iter()
        .find(|d| d.tag == SectionTag::IndexBloom as u16)
        .ok_or_else(|| StoreError::Corrupted("missing EOF Tag 0x06".into()))?;
    if tag06_entry.offset != tail_offset {
        return Err(StoreError::Corrupted(
            "Tag 0x06 offset mismatches tail_offset".into(),
        ));
    }
    let tag06_end = (tag06_entry.len as usize).min(dir_rel_start);
    if tag06_end != tag06_entry.len as usize {
        return Err(StoreError::Corrupted(
            "Tag 0x06 extends past directory".into(),
        ));
    }
    let tag06_slice = &raw_tail[..tag06_end];
    let actual_06_crc = crc32fast::hash(tag06_slice);
    if actual_06_crc != tag06_entry.crc32 {
        return Err(StoreError::ChecksumMismatch {
            context: "Tag 0x06",
            expected: tag06_entry.crc32,
            actual: actual_06_crc,
        });
    }
    let index = SegmentIndexSection::decode(tag06_slice)?;

    Ok(SegmentTail {
        footer,
        directory,
        index,
    })
}

async fn coalesce_and_fetch_ranges(
    object_store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    ranges: &[Range<u64>],
    max_gap: u64,
    max_span: u64,
) -> StoreResult<Vec<(Range<u64>, Bytes)>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let mut sorted: Vec<Range<u64>> = ranges.to_vec();
    sorted.sort_by_key(|r| (r.start, r.end));

    let mut merged: Vec<Range<u64>> = Vec::new();
    for r in sorted {
        if let Some(last) = merged.last_mut() {
            if r.start <= last.end.saturating_add(max_gap)
                && r.end.max(last.end).saturating_sub(last.start) <= max_span
            {
                last.end = last.end.max(r.end);
                continue;
            }
        }
        merged.push(r);
    }

    let mut out = Vec::with_capacity(merged.len());
    for mr in merged {
        let bytes = object_store.get_range(path, mr.clone()).await?;
        out.push((mr, bytes));
    }
    Ok(out)
}

fn slice_from_coalesced(fetched: &[(Range<u64>, Bytes)], target: Range<u64>) -> StoreResult<&[u8]> {
    for (mr, bytes) in fetched {
        if target.start >= mr.start && target.end <= mr.end {
            let s = (target.start - mr.start) as usize;
            let e = (target.end - mr.start) as usize;
            if e <= bytes.len() {
                return Ok(&bytes[s..e]);
            }
        }
    }
    Err(StoreError::Corrupted(
        "missing coalesced range slice".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutOptions, PutResult, Result as OsResult,
    };
    use pulse_storage::EncryptedObjectStore;
    use serde_json::json;
    use std::fmt::{Debug, Display, Formatter};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        get_range_calls: AtomicUsize,
    }

    impl Debug for CountingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
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
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
            self.get_range_calls.fetch_add(1, Ordering::SeqCst);
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
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn mk_rich_trace_and_spans(
        id: &str,
        ts: i64,
        shared_payload: Value,
    ) -> (AgentTrace, Vec<AgentSpan>) {
        let trace = AgentTrace {
            trace_id: id.to_string(),
            project_id: "proj_seg".to_string(),
            agent_name: "planner".to_string(),
            agent_version: "1.2.0".to_string(),
            prompt_version: Some("p1".to_string()),
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".to_string(),
            session_id: Some("sess_1".to_string()),
            user_id: Some("user_1".to_string()),
            workflow_id: None,
            parent_trace_id: None,
            status: TraceStatus::Ok,
            start_ts_ms: ts,
            end_ts_ms: Some(ts + 250),
            duration_ms: 250,
            input: shared_payload.clone(),
            output: json!({"answer": format!("result for {id}")}),
            error_type: None,
            error_message: None,
            failure_source: None,
            metadata: json!({"region": "us-east"}),
            tags: vec!["v2".to_string()],
            span_count: 1,
            llm_call_count: 1,
            tool_call_count: 0,
            error_span_count: 0,
            input_tokens: 100,
            output_tokens: 50,
            cached_tokens: 10,
            total_tokens: 150,
            total_cost_nanos: 3_500_000,
            total_cost_usd: 0.0035,
            primary_model: Some("gpt-4o".to_string()),
            primary_provider: Some("openai".to_string()),
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning: false,
        };
        let span = AgentSpan {
            span_id: format!("sp_{id}"),
            trace_id: id.to_string(),
            project_id: "proj_seg".to_string(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: "llm_step".to_string(),
            span_type: SpanType::Llm,
            status: TraceStatus::Ok,
            start_ts_ms: ts,
            end_ts_ms: Some(ts + 200),
            duration_ms: 200,
            provider: Some("openai".to_string()),
            model: Some("gpt-4o".to_string()),
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 100,
            output_tokens: 50,
            cached_tokens: 10,
            total_tokens: 150,
            cost_nanos: 3_500_000,
            cost_usd: 0.0035,
            temperature: Some(0.2),
            finish_reason: Some("stop".to_string()),
            model_params: json!({"top_p": 1.0}),
            tool_name: None,
            tool_version: None,
            tool_retry_count: 0,
            input: shared_payload,
            output: json!({"text": "done"}),
            attributes: json!({"step": 1}),
            events: vec![SpanEvent {
                name: "token".to_string(),
                timestamp_ms: ts + 50,
                attributes: json!({"idx": 0}),
            }],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        };
        (trace, vec![span])
    }

    #[tokio::test]
    async fn plss_exact_directory_20b_footer_24b_and_eof_coalesced_tag_06() {
        let entries = vec![mk_rich_trace_and_spans(
            "tr_1",
            1_700_000_000_000,
            json!({"prompt": "hello"}),
        )];
        let (bytes, tail_offset, _) = SegmentWriter::build_plss_bytes(entries).unwrap();
        let footer = PlssFooter::decode(&bytes[bytes.len() - 24..]).unwrap();
        assert_eq!(footer.magic, *b"PLSS");
        assert_eq!(footer.dir_len, 7 * 20);
        assert_eq!(
            footer.dir_offset + footer.dir_len as u64 + 24,
            bytes.len() as u64
        );
        let dir_crc = crc32fast::hash(
            &bytes[footer.dir_offset as usize
                ..(footer.dir_offset as usize + footer.dir_len as usize)],
        );
        assert_eq!(dir_crc, footer.dir_crc32);

        let os = Arc::new(CountingStore {
            inner: Arc::new(InMemory::new()),
            get_range_calls: AtomicUsize::new(0),
        });
        let os_dyn: Arc<dyn ObjectStore> = os.clone();
        let path = segment_object_path("proj_seg", "2026-11-19", 0, "seg1").unwrap();
        os_dyn
            .put(&path, PutPayload::from_bytes(Bytes::from(bytes.clone())))
            .await
            .unwrap();

        let cache = AgentSegmentCache::open(None, 8 * 1024 * 1024, None, 0)
            .await
            .unwrap();
        let reader = PlssReader::new(os_dyn, cache.clone());
        let seg = SegmentDescriptor {
            segment_id: "seg1".into(),
            object_path: path.as_ref().to_string(),
            date: "2026-11-19".into(),
            shard_id: 0,
            level: 0,
            file_size: bytes.len() as u64,
            tail_offset,
            zone_map: SegmentZoneMap::default(),
            trace_count: 1,
            created_at_ms: 1_000,
        };

        let tail = reader.read_eof_tail(&seg).await.unwrap();
        assert_eq!(os.get_range_calls.load(Ordering::SeqCst), 1);
        assert!(tail.index.may_contain_trace("tr_1"));
        assert_eq!(tail.directory.len(), 7);
        assert_eq!(
            tail.find_section(SectionTag::IndexBloom).unwrap().offset,
            tail_offset
        );
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn tag_02_has_no_text_previews_and_crdt_merges_status_and_spans() {
        let secret_text = "UNIQUE_PREVIEW_SENTINEL_DO_NOT_INLINE_IN_TAG_02";
        let (tr, sp) = mk_rich_trace_and_spans(
            "tr_no_preview",
            1_700_000_000_000,
            json!({"secret": secret_text}),
        );
        let (bytes, tail_offset, _) = SegmentWriter::build_plss_bytes(vec![(tr, sp)]).unwrap();
        let tail = decode_tail_slice(
            tail_offset,
            bytes.len() as u64,
            &bytes[tail_offset as usize..],
        )
        .unwrap();
        let sec01 = tail.find_section(SectionTag::StringTable).unwrap();
        let sec02 = tail.find_section(SectionTag::TraceSummary).unwrap();
        let s01_raw = &bytes[sec01.offset as usize..(sec01.offset as usize + sec01.len as usize)];
        let s02_raw = &bytes[sec02.offset as usize..(sec02.offset as usize + sec02.len as usize)];
        let strings = SegmentStringTable::decode_section(s01_raw).unwrap();
        let summaries = decode_trace_summary_section(s02_raw).unwrap();
        assert_eq!(summaries.len(), 1);
        let materialized = summaries[0].to_agent_trace("proj_seg", &strings).unwrap();
        let meta = summaries[0].crdt_meta(&strings).unwrap();
        assert_eq!(meta.explicit_primary_model, materialized.primary_model);
        let mut bad_epm = summaries[0].clone();
        bad_epm.explicit_primary_model_str = 999_999;
        assert!(matches!(
            bad_epm.crdt_meta(&strings),
            Err(StoreError::Corrupted(_))
        ));
        assert!(matches!(
            bad_epm.to_agent_trace("proj_seg", &strings),
            Err(StoreError::Corrupted(_))
        ));
        let mut bad_epp = summaries[0].clone();
        bad_epp.explicit_primary_provider_str = 999_999;
        assert!(matches!(
            bad_epp.crdt_meta(&strings),
            Err(StoreError::Corrupted(_))
        ));
        assert!(matches!(
            bad_epp.to_agent_trace("proj_seg", &strings),
            Err(StoreError::Corrupted(_))
        ));
        assert!(materialized.input.is_null());
        assert!(materialized.output.is_null());
        assert!(materialized.error_message.is_none());
        // Verify sentinel string is absent from StringTable as well
        assert!(!String::from_utf8_lossy(s01_raw).contains(secret_text));

        // Verify CRDT merge across duplicate span_ids (Start vs Final)
        let mut dst = materialized;
        let mut crdt_map = HashMap::new();
        let dst_src1 = dst.clone();
        merge_trace_summary_rows(
            &mut dst,
            &mut crdt_map,
            dst_src1,
            vec![ResolvedSpanCrdt {
                span_id: "sp_tr_no_preview".to_string(),
                span_type: SpanType::Llm.to_u8(),
                span_phase: SpanPhase::Start.to_u8(),
                status: TraceStatus::Running.to_u8(),
                depth: 0,
                start_ts_ms: 1_700_000_000_000,
                input_tokens: 100,
                output_tokens: 50,
                cached_tokens: 10,
                total_tokens: 150,
                cost_nanos: 3_500_000,
                model: None,
                provider: None,
                tool_name: None,
                tool_name_rank: None,
                span_name: Some("call".to_string()),
                has_v1_gap: false,
            }],
        );
        let dst_src2 = dst.clone();
        merge_trace_summary_rows(
            &mut dst,
            &mut crdt_map,
            dst_src2,
            vec![ResolvedSpanCrdt {
                span_id: "sp_tr_no_preview".to_string(),
                span_type: SpanType::Llm.to_u8(),
                span_phase: SpanPhase::Final.to_u8(),
                status: TraceStatus::Ok.to_u8(),
                depth: 0,
                start_ts_ms: 1_700_000_000_000,
                input_tokens: 100,
                output_tokens: 50,
                cached_tokens: 10,
                total_tokens: 150,
                cost_nanos: 3_500_000,
                model: None,
                provider: None,
                tool_name: None,
                tool_name_rank: None,
                span_name: Some("call".to_string()),
                has_v1_gap: false,
            }],
        );
        assert_eq!(dst.span_count, 1);
        assert_eq!(dst.total_cost_nanos, 3_500_000);
        assert_eq!(dst.status, TraceStatus::Ok);
    }

    #[tokio::test]
    async fn tag_05_64k_aligned_dedup_and_coalesced_hydration_with_encrypted_store() {
        let in_mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let enc = Arc::new(EncryptedObjectStore::new(in_mem));
        enc.set_master_key(Some([0x42u8; 32]));
        let counting = Arc::new(CountingStore {
            inner: enc.as_object_store(),
            get_range_calls: AtomicUsize::new(0),
        });
        let os_dyn: Arc<dyn ObjectStore> = counting.clone();

        let shared_payload = json!({"system": "x".repeat(4096)});
        let (tr1, sp1) = mk_rich_trace_and_spans("tr_a", 1_700_000_000_000, shared_payload.clone());
        let (tr2, sp2) = mk_rich_trace_and_spans("tr_b", 1_700_000_001_000, shared_payload);
        let (bytes, tail_offset, zone) =
            SegmentWriter::build_plss_bytes(vec![(tr1.clone(), sp1.clone()), (tr2, sp2)]).unwrap();

        let tail = decode_tail_slice(
            tail_offset,
            bytes.len() as u64,
            &bytes[tail_offset as usize..],
        )
        .unwrap();
        let sec05 = tail.find_section(SectionTag::PayloadPack).unwrap();
        let s05_raw = &bytes[sec05.offset as usize..(sec05.offset as usize + sec05.len as usize)];
        let dir5 = decode_tag05_dir(s05_raw).unwrap();
        assert!(!dir5.is_empty());
        for blk in &dir5 {
            assert_eq!(
                blk.file_offset as usize % PLSS_ALIGN_BYTES,
                0,
                "Tag 0x05 block must be 64 KiB aligned"
            );
        }

        // Verify intra-block dedup reduced blob count (shared_payload reused across trace.input & span.input)
        let rel_s = (dir5[0].file_offset - sec05.offset) as usize;
        let rel_e = rel_s + dir5[0].comp_len as usize;
        let pack_raw = decompress_block(
            &s05_raw[rel_s..rel_e],
            dir5[0].codec,
            dir5[0].uncomp_len as usize,
            dir5[0].crc32,
            "test",
        )
        .unwrap();
        let blob_cnt = u32::from_le_bytes(pack_raw[0..4].try_into().unwrap());
        // 2 traces * (output + metadata + 3 span payloads + 1 event + 1 shared_payload) -> shared_payload stored once!
        assert!(
            blob_cnt <= 13,
            "expected deduped blob count <= 13, got {blob_cnt}"
        );

        let path = segment_object_path("proj_seg", "2026-11-19", 0, "seg_enc").unwrap();
        os_dyn
            .put(&path, PutPayload::from_bytes(Bytes::from(bytes.clone())))
            .await
            .unwrap();

        let cache = AgentSegmentCache::open(Some(enc), 8 * 1024 * 1024, None, 0)
            .await
            .unwrap();
        let reader = PlssReader::new(os_dyn, cache.clone());
        let seg = SegmentDescriptor {
            segment_id: "seg_enc".into(),
            object_path: path.as_ref().to_string(),
            date: "2026-11-19".into(),
            shard_id: 0,
            level: 0,
            file_size: bytes.len() as u64,
            tail_offset,
            zone_map: zone,
            trace_count: 2,
            created_at_ms: 1_000,
        };

        let tail = reader.read_eof_tail(&seg).await.unwrap();
        counting.get_range_calls.store(0, Ordering::SeqCst);
        let hydrated = reader
            .read_trace_ranges(&seg, &tail, "tr_a")
            .await
            .unwrap()
            .unwrap();
        let calls = counting.get_range_calls.load(Ordering::SeqCst);
        assert!(calls <= 2, "expected <= 2 get_range calls, got {calls}");
        assert_eq!(hydrated.0.trace_id, tr1.trace_id);
        assert_eq!(hydrated.0.input, tr1.input);
        assert_eq!(hydrated.0.output, tr1.output);
        assert_eq!(hydrated.1.len(), 1);
        assert_eq!(hydrated.1[0].input, sp1[0].input);
        assert_eq!(hydrated.1[0].events.len(), 1);

        // Second call hits p4idx/p5dir/p4blk/p5blk caches with 0 ObjectStore reads
        counting.get_range_calls.store(0, Ordering::SeqCst);
        let warm = reader
            .read_trace_ranges(&seg, &tail, "tr_a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(counting.get_range_calls.load(Ordering::SeqCst), 0);
        assert_eq!(warm.0.trace_id, tr1.trace_id);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn cold_single_trace_hydration_avoids_mid_trace_tag05_split() {
        let os = Arc::new(CountingStore {
            inner: Arc::new(InMemory::new()),
            get_range_calls: AtomicUsize::new(0),
        });
        let os_dyn: Arc<dyn ObjectStore> = os.clone();

        // Build 6 traces each with distinct ~90 KiB pseudo-random payloads across span + trace
        let mut entries = Vec::new();
        for i in 0..6usize {
            let mut s1 = String::with_capacity(48 * 1024);
            let mut s2 = String::with_capacity(48 * 1024);
            let mut x = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for _ in 0..48 * 1024 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                s1.push((b'A' + (x % 26) as u8) as char);
                s2.push((b'a' + ((x >> 8) % 26) as u8) as char);
            }
            let (mut tr, mut sp) = mk_rich_trace_and_spans(
                &format!("tr_mid_{i}"),
                1_700_000_000_000 + (i as i64) * 1000,
                json!({"trace_in": s1}),
            );
            tr.output = json!({"trace_out": s2});
            sp[0].output = json!({"span_out": tr.output.clone()});
            entries.push((tr, sp));
        }

        let (bytes, tail_offset, zone) = SegmentWriter::build_plss_bytes(entries).unwrap();
        let path = segment_object_path("proj_seg", "2026-11-19", 0, "seg_no_split").unwrap();
        os_dyn
            .put(&path, PutPayload::from_bytes(Bytes::from(bytes.clone())))
            .await
            .unwrap();
        let seg = SegmentDescriptor {
            segment_id: "seg_no_split".into(),
            object_path: path.as_ref().to_string(),
            date: "2026-11-19".into(),
            shard_id: 0,
            level: 0,
            file_size: bytes.len() as u64,
            tail_offset,
            zone_map: zone,
            trace_count: 6,
            created_at_ms: 1_000,
        };

        for i in 0..6usize {
            let cache = AgentSegmentCache::open(None, 8 * 1024 * 1024, None, 0)
                .await
                .unwrap();
            let reader = PlssReader::new(os_dyn.clone(), cache.clone());
            let tail = reader.read_eof_tail(&seg).await.unwrap();
            os.get_range_calls.store(0, Ordering::SeqCst);
            let res = reader
                .read_trace_ranges(&seg, &tail, &format!("tr_mid_{i}"))
                .await
                .unwrap()
                .unwrap();
            let calls = os.get_range_calls.load(Ordering::SeqCst);
            assert!(
                calls <= 2,
                "cold trace tr_mid_{i} required {calls} get_range calls (expected <= 2)"
            );
            assert_eq!(res.0.trace_id, format!("tr_mid_{i}"));
            cache.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn segment_splitter_respects_cap() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut entries = Vec::new();
        for i in 0..20 {
            entries.push(mk_rich_trace_and_spans(
                &format!("tr_split_{i:02}"),
                1_700_000_000_000 + i * 100,
                json!({"data": format!("unique_{i}_") + &"a".repeat(8192)}),
            ));
        }
        let max_bytes = 96 * 1024;
        let segs = SegmentWriter::write_segments_split(
            "proj_split",
            "2026-11-19",
            0,
            0,
            entries,
            max_bytes,
            &os,
        )
        .await
        .unwrap();
        assert!(segs.len() > 1, "expected multi-segment split");
        for s in &segs {
            assert!(
                s.file_size as usize <= max_bytes || s.trace_count == 1,
                "segment {} exceeded max_bytes: {}",
                s.segment_id,
                s.file_size
            );
        }
    }

    #[test]
    fn hostile_plss_corruption_rejected() {
        let (bytes, tail_offset, _) =
            SegmentWriter::build_plss_bytes(vec![mk_rich_trace_and_spans(
                "tr_corrupt",
                1_700_000_000_000,
                json!({"a": 1}),
            )])
            .unwrap();

        // 1. Bad footer magic
        let mut bad_magic = bytes.clone();
        bad_magic[bytes.len() - 1] = b'X';
        assert!(decode_tail_slice(
            tail_offset,
            bad_magic.len() as u64,
            &bad_magic[tail_offset as usize..]
        )
        .is_err());

        // 2. Tampered dir_crc32
        let mut bad_crc = bytes.clone();
        let len = bad_crc.len();
        bad_crc[len - 10] ^= 0xFF;
        assert!(decode_tail_slice(
            tail_offset,
            bad_crc.len() as u64,
            &bad_crc[tail_offset as usize..]
        )
        .is_err());

        // 3. Tampered Tag 0x06 byte (section CRC32 mismatch)
        let mut bad_06 = bytes.clone();
        bad_06[tail_offset as usize] ^= 0xFF;
        assert!(decode_tail_slice(
            tail_offset,
            bad_06.len() as u64,
            &bad_06[tail_offset as usize..]
        )
        .is_err());

        // 4. Overlapping section offsets in directory
        let mut bad_overlap = bytes.clone();
        let footer = PlssFooter::decode(&bad_overlap[bad_overlap.len() - 24..]).unwrap();
        let dir_start = footer.dir_offset as usize;
        // Overwrite second directory entry's offset to 0 (overlapping entry 0)
        bad_overlap[dir_start + PLSS_DIR_ENTRY_LEN + 4..dir_start + PLSS_DIR_ENTRY_LEN + 12]
            .copy_from_slice(&0u64.to_le_bytes());
        let new_crc = crc32fast::hash(&bad_overlap[dir_start..dir_start + footer.dir_len as usize]);
        let blen = bad_overlap.len();
        bad_overlap[blen - 12..blen - 8].copy_from_slice(&new_crc.to_le_bytes());
        assert!(decode_tail_slice(
            tail_offset,
            bad_overlap.len() as u64,
            &bad_overlap[tail_offset as usize..]
        )
        .is_err());

        // 5. Zstd decompression bomb exceeding 64 MiB limit
        let huge = vec![0u8; 65 * 1024 * 1024];
        let comp = zstd::stream::encode_all(huge.as_slice(), 1).unwrap();
        let crc = crc32fast::hash(&comp);
        assert!(decompress_block(&comp, CODEC_ZSTD_RAW, huge.len(), crc, "bomb").is_err());

        // 6. merge_trace_summary_rows equal-phase higher status_rank and Timeout preservation
        let (tr_a, _) = mk_rich_trace_and_spans("tr_sum", 1_700_000_000_000, json!({"a": 1}));
        let mut dst = tr_a.clone();
        let mut crdt_map = HashMap::new();
        merge_trace_summary_rows(
            &mut dst,
            &mut crdt_map,
            tr_a.clone(),
            vec![ResolvedSpanCrdt {
                span_id: "sp_1".to_string(),
                span_type: SpanType::Llm.to_u8(),
                span_phase: SpanPhase::Final.to_u8(),
                status: TraceStatus::Error.to_u8(),
                depth: 0,
                start_ts_ms: 1_700_000_000_000,
                input_tokens: 10,
                output_tokens: 5,
                cached_tokens: 0,
                total_tokens: 15,
                cost_nanos: 1000,
                model: None,
                provider: None,
                tool_name: None,
                tool_name_rank: None,
                span_name: Some("call".to_string()),
                has_v1_gap: false,
            }],
        );
        merge_trace_summary_rows(
            &mut dst,
            &mut crdt_map,
            tr_a.clone(),
            vec![ResolvedSpanCrdt {
                span_id: "sp_1".to_string(),
                span_type: SpanType::Llm.to_u8(),
                span_phase: SpanPhase::Final.to_u8(),
                status: TraceStatus::Ok.to_u8(),
                depth: 0,
                start_ts_ms: 1_700_000_000_000,
                input_tokens: 10,
                output_tokens: 5,
                cached_tokens: 0,
                total_tokens: 15,
                cost_nanos: 1000,
                model: None,
                provider: None,
                tool_name: None,
                tool_name_rank: None,
                span_name: Some("call".to_string()),
                has_v1_gap: false,
            }],
        );
        assert_eq!(dst.status, TraceStatus::Error);

        let mut dst_to = tr_a.clone();
        let mut crdt_to = HashMap::new();
        merge_trace_summary_rows(
            &mut dst_to,
            &mut crdt_to,
            tr_a,
            vec![ResolvedSpanCrdt {
                span_id: "sp_to".to_string(),
                span_type: SpanType::Tool.to_u8(),
                span_phase: SpanPhase::Final.to_u8(),
                status: TraceStatus::Timeout.to_u8(),
                depth: 0,
                start_ts_ms: 1_700_000_000_000,
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: 0,
                total_tokens: 0,
                cost_nanos: 0,
                model: None,
                provider: None,
                tool_name: Some("tool".to_string()),
                tool_name_rank: Some((SpanPhase::Final.to_u8(), 1_700_000_000_000)),
                span_name: Some("tool".to_string()),
                has_v1_gap: false,
            }],
        );
        assert_eq!(dst_to.status, TraceStatus::Timeout);
        assert_eq!(dst_to.error_span_count, 1);
    }

    #[tokio::test]
    async fn hostile_read_trace_ranges_rejects_oversized_tag05_and_reordered_sections() {
        let (base_bytes, tail_offset, zone) =
            SegmentWriter::build_plss_bytes(vec![mk_rich_trace_and_spans(
                "tr_hostile",
                1_700_000_000_000,
                json!({"k": "v"}),
            )])
            .unwrap();

        fn patch_dir_and_crc(buf: &mut [u8], mutate_dir: impl FnOnce(&mut [SectionDirEntry])) {
            let footer = PlssFooter::decode(&buf[buf.len() - 24..]).unwrap();
            let dir_start = footer.dir_offset as usize;
            let dir_end = dir_start + footer.dir_len as usize;
            let mut entries = Vec::new();
            for c in buf[dir_start..dir_end].chunks_exact(PLSS_DIR_ENTRY_LEN) {
                entries.push(SectionDirEntry::decode(c).unwrap());
            }
            mutate_dir(&mut entries);
            let mut new_dir = Vec::new();
            for e in &entries {
                e.encode_into(&mut new_dir);
            }
            buf[dir_start..dir_end].copy_from_slice(&new_dir);
            let new_crc = crc32fast::hash(&buf[dir_start..dir_end]);
            let blen = buf.len();
            buf[blen - 12..blen - 8].copy_from_slice(&new_crc.to_le_bytes());
        }

        let tail = decode_tail_slice(
            tail_offset,
            base_bytes.len() as u64,
            &base_bytes[tail_offset as usize..],
        )
        .unwrap();
        let sec04 = tail.find_section(SectionTag::SpanTree).unwrap();
        let sec05 = tail.find_section(SectionTag::PayloadPack).unwrap();

        // Case 1: Tag 0x05 block_count = u32::MAX (must fail before any oversized get_range)
        {
            let mut bad_bc = base_bytes.clone();
            bad_bc[sec05.offset as usize..sec05.offset as usize + 4]
                .copy_from_slice(&u32::MAX.to_le_bytes());
            let new_05_crc = crc32fast::hash(
                &bad_bc[sec05.offset as usize..sec05.offset as usize + sec05.len as usize],
            );
            patch_dir_and_crc(&mut bad_bc, |dirs| {
                for d in dirs {
                    if d.tag == SectionTag::PayloadPack as u16 {
                        d.crc32 = new_05_crc;
                    }
                }
            });
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = segment_object_path("proj_h", "2026-11-19", 0, "bad_bc").unwrap();
            os.put(&path, PutPayload::from_bytes(Bytes::from(bad_bc.clone())))
                .await
                .unwrap();
            let cache = AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                .await
                .unwrap();
            let reader = PlssReader::new(os, cache.clone());
            let seg = SegmentDescriptor {
                segment_id: "bad_bc".into(),
                object_path: path.as_ref().to_string(),
                date: "2026-11-19".into(),
                shard_id: 0,
                level: 0,
                file_size: bad_bc.len() as u64,
                tail_offset,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let t = reader.read_eof_tail(&seg).await.unwrap();
            let err = reader
                .read_trace_ranges(&seg, &t, "tr_hostile")
                .await
                .unwrap_err();
            assert!(matches!(err, StoreError::ResourceLimitExceeded(_)));

            // Also test fallback path (when p4idx is already cached so fast path is skipped)
            cache.hybrid_insert_sealed(
                "p4idx:bad_bc".to_string(),
                &bad_bc[sec04.offset as usize..sec04.offset as usize + sec04.len as usize],
            );
            let err2 = reader
                .read_trace_ranges(&seg, &t, "tr_hostile")
                .await
                .unwrap_err();
            assert!(matches!(err2, StoreError::ResourceLimitExceeded(_)));
            cache.close().await.unwrap();
        }

        // Case 2: Reordered sections (sec05.offset < sec04.offset) must NOT panic with inverted range
        {
            let mut reordered = base_bytes.clone();
            let s04_off = sec04.offset;
            let s04_len = sec04.len;
            let s04_crc = sec04.crc32;
            let s05_off = sec05.offset;
            let s05_len = sec05.len;
            let s05_crc = sec05.crc32;
            patch_dir_and_crc(&mut reordered, |dirs| {
                for d in dirs {
                    if d.tag == SectionTag::SpanTree as u16 {
                        d.offset = s05_off;
                        d.len = s05_len;
                        d.crc32 = s05_crc;
                    } else if d.tag == SectionTag::PayloadPack as u16 {
                        d.offset = s04_off;
                        d.len = s04_len;
                        d.crc32 = s04_crc;
                    }
                }
            });
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = segment_object_path("proj_h", "2026-11-19", 0, "reordered").unwrap();
            os.put(
                &path,
                PutPayload::from_bytes(Bytes::from(reordered.clone())),
            )
            .await
            .unwrap();
            let cache = AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                .await
                .unwrap();
            let reader = PlssReader::new(os, cache.clone());
            let seg = SegmentDescriptor {
                segment_id: "reordered".into(),
                object_path: path.as_ref().to_string(),
                date: "2026-11-19".into(),
                shard_id: 0,
                level: 0,
                file_size: reordered.len() as u64,
                tail_offset,
                zone_map: zone.clone(),
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let t = reader.read_eof_tail(&seg).await.unwrap();
            let res = reader.read_trace_ranges(&seg, &t, "tr_hostile").await;
            assert!(res.is_err(), "expected clean Err on swapped sec04/sec05");
            cache.close().await.unwrap();
        }

        // Case 3: Gapped sections (sec05.offset > sec04_end, e.g. swapping sec05 and sec07)
        {
            let sec07 = tail.find_section(SectionTag::SessionRollup).unwrap();
            let mut gapped = base_bytes.clone();
            let s05_off = sec05.offset;
            let s05_len = sec05.len;
            let s05_crc = sec05.crc32;
            let s07_off = sec07.offset;
            let s07_len = sec07.len;
            let s07_crc = sec07.crc32;
            patch_dir_and_crc(&mut gapped, |dirs| {
                for d in dirs {
                    if d.tag == SectionTag::PayloadPack as u16 {
                        d.offset = s07_off;
                        d.len = s07_len;
                        d.crc32 = s07_crc;
                    } else if d.tag == SectionTag::SessionRollup as u16 {
                        d.offset = s05_off;
                        d.len = s05_len;
                        d.crc32 = s05_crc;
                    }
                }
            });
            let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let path = segment_object_path("proj_h", "2026-11-19", 0, "gapped").unwrap();
            os.put(&path, PutPayload::from_bytes(Bytes::from(gapped.clone())))
                .await
                .unwrap();
            let cache = AgentSegmentCache::open(None, 4 * 1024 * 1024, None, 0)
                .await
                .unwrap();
            let reader = PlssReader::new(os, cache.clone());
            let seg = SegmentDescriptor {
                segment_id: "gapped".into(),
                object_path: path.as_ref().to_string(),
                date: "2026-11-19".into(),
                shard_id: 0,
                level: 0,
                file_size: gapped.len() as u64,
                tail_offset,
                zone_map: zone,
                trace_count: 1,
                created_at_ms: 1_000,
            };
            let t = reader.read_eof_tail(&seg).await.unwrap();
            let res = reader.read_trace_ranges(&seg, &t, "tr_hostile").await;
            assert!(res.is_err(), "expected clean Err on gapped sec04/sec05");
            cache.close().await.unwrap();
        }
    }
}
