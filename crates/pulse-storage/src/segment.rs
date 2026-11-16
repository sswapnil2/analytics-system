use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{
    Array as ArrowArray, ArrayRef, RecordBatch, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use bytemuck::{Pod, Zeroable};
use bytes::Bytes;
use chrono::NaiveDate;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use pulse_wire::{decode_zstd_bounded, PropValue, MAX_DECOMPRESSED_BYTES};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use vortex::expr::{root, select};
use vortex_array::arrow::FromArrowArray;
use vortex_array::iter::ArrayIteratorExt;
use vortex_array::ArrayRef as VortexArrayRef;
use vortex_buffer::{ByteBuffer, ByteBufferMut};
use vortex_dtype::FieldName;
use vortex_file::{VortexOpenOptions, VortexWriteOptions};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Result, StorageError};
use crate::mem_bucket::StoredRow;

pub const MAX_TIER_B_KEYS: usize = 512;
pub const MAX_INVERTED_INDEX_CARDINALITY: usize = 16_384;

/// Fixed 56-byte `Pod` struct stored contiguously in every segment's `SessionIndex` section.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct SessionHeader {
    pub session_hash: u64,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub event_bloom_64: u64,
    pub session_id_idx: u32,
    pub raw_uid: u32,
    pub event_count: u32,
    pub entry_event_id: u16,
    pub exit_event_id: u16,
    /// Bit 0 = `rage_tap`, Bit 1 = `dead_tap`, Bit 2 = `error`.
    pub flags: u16,
    pub _pad: [u8; 6],
}

impl SessionHeader {
    #[inline]
    pub fn event_bit(event_id: u16) -> u64 {
        1u64 << (event_id % 64)
    }

    #[inline]
    pub fn may_contain_any_event(&self, mask: u64) -> bool {
        mask == 0 || (self.event_bloom_64 & mask) != 0
    }
}

#[inline]
pub fn tier_c_key_bit(prop_key_id: u16) -> u64 {
    1u64 << (xxh3_64(&prop_key_id.to_le_bytes()) & 63)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierBColumnEntry {
    pub prop_key_id: u16,
    pub has_i64: bool,
    pub has_f64: bool,
    pub has_bool: bool,
    pub has_str: bool,
    #[serde(default)]
    pub has_null: bool,
    #[serde(default)]
    pub bool_indexed: bool,
    #[serde(default)]
    pub str_indexed_complete: bool,
}

impl TierBColumnEntry {
    pub fn col_i64_name(prop_key_id: u16) -> String {
        format!("p_{prop_key_id}_i64")
    }
    pub fn col_f64_name(prop_key_id: u16) -> String {
        format!("p_{prop_key_id}_f64")
    }
    pub fn col_bool_name(prop_key_id: u16) -> String {
        format!("p_{prop_key_id}_bool")
    }
    pub fn col_str_name(prop_key_id: u16) -> String {
        format!("p_{prop_key_id}_str")
    }
    pub fn col_null_name(prop_key_id: u16) -> String {
        format!("p_{prop_key_id}_null")
    }
}

/// Segment statistics and Tier B/C column catalog stored in `Manifest` `SegmentMeta`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentFooterMeta {
    pub row_count: u32,
    pub session_count: u32,
    pub min_ts_ms: i64,
    pub max_ts_ms: i64,
    pub segment_event_bloom_64: u64,
    pub tier_c_bloom_64: u64,
    pub has_tier_c_column: bool,
    pub tier_b_columns: Vec<TierBColumnEntry>,
}

/// Pointer & byte-range offsets stored in the MVCC `Manifest` for an immutable L1 Vortex segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub segment_id: String,
    pub object_path: String,
    pub date: NaiveDate,
    pub bucket_id: u8,
    pub file_size: u64,
    pub vortex_len: u64,
    pub inverted_idx_offset: u64,
    pub session_idx_offset: u64,
    pub footer: SegmentFooterMeta,
}

/// Decoded `SessionIndex` section of a segment.
#[derive(Debug, Clone)]
pub struct SegmentSessionIndex {
    pub headers: Vec<SessionHeader>,
    pub session_ids: Vec<String>,
}

impl SegmentSessionIndex {
    pub fn session_id(&self, hdr: &SessionHeader) -> &str {
        self.session_ids
            .get(hdr.session_id_idx as usize)
            .map(|s| s.as_str())
            .unwrap_or("")
    }

    /// Computes `(row_start, row_end)` for every session matching `predicate`.
    pub fn matching_session_ranges<F>(&self, mut predicate: F) -> Vec<(usize, usize, SessionHeader)>
    where
        F: FnMut(&SessionHeader) -> bool,
    {
        let mut cursor = 0usize;
        let mut out = Vec::new();
        for hdr in &self.headers {
            let len = hdr.event_count as usize;
            if predicate(hdr) {
                out.push((cursor, cursor + len, *hdr));
            }
            cursor += len;
        }
        out
    }
}

/// Type discriminator preventing `PropValue::Bool(true)` and `PropValue::Str("true")` from colliding
/// in `SegmentInvertedIndex` (`P1-15`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum InvertedValueTag {
    Bool = 1,
    Str = 2,
}

impl InvertedValueTag {
    fn from_byte(b: u8) -> Result<Self> {
        match b {
            1 => Ok(Self::Bool),
            2 => Ok(Self::Str),
            other => Err(StorageError::Corrupted(format!(
                "invalid inverted index value tag {other}"
            ))),
        }
    }
}

/// Decoded per-segment Tier B categorical inverted `RoaringBitmap` index (`P1-15`, `P2-01`).
#[derive(Debug, Clone, Default)]
pub struct SegmentInvertedIndex {
    /// `(prop_key_id, value_type_tag, value_repr) -> matching segment row indices`.
    pub bitmaps: HashMap<(u16, InvertedValueTag, String), RoaringBitmap>,
}

impl SegmentInvertedIndex {
    pub fn get_bool(&self, prop_key_id: u16, value: bool) -> Option<&RoaringBitmap> {
        let s = if value { "true" } else { "false" };
        self.bitmaps
            .get(&(prop_key_id, InvertedValueTag::Bool, s.to_string()))
    }

    pub fn get_str(&self, prop_key_id: u16, value: &str) -> Option<&RoaringBitmap> {
        self.bitmaps
            .get(&(prop_key_id, InvertedValueTag::Str, value.to_string()))
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&(self.bitmaps.len() as u32).to_le_bytes());
        for ((key_id, tag, val), bm) in &self.bitmaps {
            raw.extend_from_slice(&key_id.to_le_bytes());
            raw.push(*tag as u8);
            raw.extend_from_slice(&(val.len() as u32).to_le_bytes());
            raw.extend_from_slice(val.as_bytes());
            let mut bm_bytes = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut bm_bytes)?;
            raw.extend_from_slice(&(bm_bytes.len() as u32).to_le_bytes());
            raw.extend_from_slice(&bm_bytes);
        }
        Ok(zstd::stream::encode_all(raw.as_slice(), 1)?)
    }

    pub fn decode(compressed: &[u8]) -> Result<Self> {
        if compressed.is_empty() {
            return Ok(Self::default());
        }
        let raw = decode_zstd_bounded(compressed, None, MAX_DECOMPRESSED_BYTES)?;
        if raw.len() < 4 {
            return Err(StorageError::Corrupted(
                "truncated inverted index".to_string(),
            ));
        }
        let count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        let mut pos = 4usize;
        let mut bitmaps = HashMap::with_capacity(count.min((raw.len() - 4) / 11));
        for _ in 0..count {
            if pos + 7 > raw.len() {
                return Err(StorageError::Corrupted(
                    "inverted index header EOF".to_string(),
                ));
            }
            let key_id = u16::from_le_bytes([raw[pos], raw[pos + 1]]);
            let tag = InvertedValueTag::from_byte(raw[pos + 2])?;
            pos += 3;
            let s_len =
                u32::from_le_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]) as usize;
            pos += 4;
            if pos + s_len + 4 > raw.len() {
                return Err(StorageError::Corrupted(
                    "inverted index string EOF".to_string(),
                ));
            }
            let val = String::from_utf8(raw[pos..pos + s_len].to_vec())
                .map_err(|e| StorageError::Corrupted(format!("invalid utf8 in index: {e}")))?;
            pos += s_len;
            let bm_len =
                u32::from_le_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]) as usize;
            pos += 4;
            if pos + bm_len > raw.len() {
                return Err(StorageError::Corrupted(
                    "inverted index bitmap EOF".to_string(),
                ));
            }
            let bm = RoaringBitmap::deserialize_from(&raw[pos..pos + bm_len])?;
            pos += bm_len;
            bitmaps.insert((key_id, tag, val), bm);
        }
        Ok(Self { bitmaps })
    }
}

struct SortedRowEntry {
    session_seq: u32,
    row: StoredRow,
}

fn prepare_and_sort_rows(rows: Vec<StoredRow>) -> Vec<SortedRowEntry> {
    // 1. Deduplicate by 64-bit event_uuid fingerprint so WAL replays or overlapping compactions never duplicate rows.
    let mut seen_fp = HashSet::with_capacity(rows.len());
    let mut deduped = Vec::with_capacity(rows.len());
    for r in rows {
        if r.event_uuid_fp == 0 || seen_fp.insert(r.event_uuid_fp) {
            deduped.push(r);
        }
    }

    // 2. Compute earliest timestamp per `(uid_internal, session_hash)` to assign monotonic `session_seq` per user.
    let mut session_min_ts: HashMap<(u32, u64), i64> = HashMap::new();
    for r in &deduped {
        let entry = session_min_ts
            .entry((r.uid_internal, r.session_hash))
            .or_insert(r.event_ts_ms);
        if r.event_ts_ms < *entry {
            *entry = r.event_ts_ms;
        }
    }

    let mut per_user_sessions: HashMap<u32, Vec<(i64, u64)>> = HashMap::new();
    for ((uid, shash), min_ts) in session_min_ts {
        per_user_sessions
            .entry(uid)
            .or_default()
            .push((min_ts, shash));
    }

    let mut session_seq_map: HashMap<(u32, u64), u32> = HashMap::new();
    for (uid, mut list) in per_user_sessions {
        list.sort_unstable();
        for (seq, (_, shash)) in list.into_iter().enumerate() {
            session_seq_map.insert((uid, shash), seq as u32);
        }
    }

    let mut entries: Vec<SortedRowEntry> = deduped
        .into_iter()
        .map(|row| {
            let session_seq = session_seq_map
                .get(&(row.uid_internal, row.session_hash))
                .copied()
                .unwrap_or(0);
            SortedRowEntry { session_seq, row }
        })
        .collect();

    // 3. Enforce Physical Sort Law: `(uid_internal, session_seq, event_ts_ms)`
    entries.sort_by(|a, b| {
        a.row
            .uid_internal
            .cmp(&b.row.uid_internal)
            .then_with(|| a.session_seq.cmp(&b.session_seq))
            .then_with(|| a.row.event_ts_ms.cmp(&b.row.event_ts_ms))
            .then_with(|| a.row.event_uuid_fp.cmp(&b.row.event_uuid_fp))
    });

    entries
}

/// Writes sorted rows into an immutable Vortex-backed Tier A/B/C segment on `ObjectStore`.
pub async fn write_vortex_segment(
    object_store: &Arc<dyn ObjectStore>,
    project_id: &str,
    date: NaiveDate,
    bucket_id: u8,
    segment_id: &str,
    rows: Vec<StoredRow>,
) -> Result<Option<SegmentMeta>> {
    let entries = prepare_and_sort_rows(rows);
    if entries.is_empty() {
        return Ok(None);
    }

    let n = entries.len();
    let mut uid_vec = Vec::with_capacity(n);
    let mut shash_vec = Vec::with_capacity(n);
    let mut uuid_fp_vec = Vec::with_capacity(n);
    let mut event_id_vec = Vec::with_capacity(n);
    let mut ts_vec = Vec::with_capacity(n);
    let mut replay_kb_vec = Vec::with_capacity(n);

    let mut min_ts_ms = i64::MAX;
    let mut max_ts_ms = i64::MIN;
    let mut segment_event_bloom_64 = 0u64;

    // Count property key frequencies to promote Top 512 keys into Tier B columns (`P2-09l` includes Null).
    // Tuple: `(count, has_i64, has_f64, has_bool, has_str, has_null)`
    let mut key_freq: HashMap<u16, (usize, bool, bool, bool, bool, bool)> = HashMap::new();
    for e in &entries {
        let r = &e.row;
        min_ts_ms = min_ts_ms.min(r.event_ts_ms);
        max_ts_ms = max_ts_ms.max(r.event_ts_ms);
        segment_event_bloom_64 |= SessionHeader::event_bit(r.event_id);

        uid_vec.push(r.uid_internal);
        shash_vec.push(r.session_hash);
        uuid_fp_vec.push(r.event_uuid_fp);
        event_id_vec.push(r.event_id);
        ts_vec.push(r.event_ts_ms);
        replay_kb_vec.push(r.replay_offset_kb);

        for (pk, pv) in &r.props {
            let stat = key_freq
                .entry(*pk)
                .or_insert((0, false, false, false, false, false));
            stat.0 += 1;
            match pv {
                PropValue::Int(_) => stat.1 = true,
                PropValue::Float(_) => stat.2 = true,
                PropValue::Bool(_) => stat.3 = true,
                PropValue::Str(_) => stat.4 = true,
                PropValue::Null => stat.5 = true,
            }
        }
    }

    let mut ranked_keys: Vec<(u16, usize, bool, bool, bool, bool, bool)> = key_freq
        .into_iter()
        .map(|(k, (cnt, hi, hf, hb, hs, hn))| (k, cnt, hi, hf, hb, hs, hn))
        .collect();
    ranked_keys.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let promoted: BTreeMap<u16, (bool, bool, bool, bool, bool)> = ranked_keys
        .into_iter()
        .take(MAX_TIER_B_KEYS)
        .filter(|(_, _, hi, hf, hb, hs, hn)| *hi || *hf || *hb || *hs || *hn)
        .map(|(k, _, hi, hf, hb, hs, hn)| (k, (hi, hf, hb, hs, hn)))
        .collect();

    struct TierBBuilders {
        i64_b: Option<Int64Builder>,
        f64_b: Option<Float64Builder>,
        bool_b: Option<BooleanBuilder>,
        str_b: Option<StringBuilder>,
        null_b: Option<BooleanBuilder>,
        str_card: HashMap<String, RoaringBitmap>,
        str_overflowed_card: bool,
        bool_true: RoaringBitmap,
        bool_false: RoaringBitmap,
    }

    let mut tier_b_builders: BTreeMap<u16, TierBBuilders> = BTreeMap::new();
    for (&pk, &(hi, hf, hb, hs, hn)) in &promoted {
        tier_b_builders.insert(
            pk,
            TierBBuilders {
                i64_b: hi.then(|| Int64Builder::with_capacity(n)),
                f64_b: hf.then(|| Float64Builder::with_capacity(n)),
                bool_b: hb.then(|| BooleanBuilder::with_capacity(n)),
                str_b: hs.then(|| StringBuilder::with_capacity(n, n * 8)),
                null_b: hn.then(|| BooleanBuilder::with_capacity(n)),
                str_card: HashMap::new(),
                str_overflowed_card: false,
                bool_true: RoaringBitmap::new(),
                bool_false: RoaringBitmap::new(),
            },
        );
    }

    let mut tier_c_bloom_64 = 0u64;
    let mut tier_c_rows: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

    // Build SessionIndex concurrently in the same single pass
    let mut session_headers: Vec<SessionHeader> = Vec::new();
    let mut session_ids: Vec<String> = Vec::new();
    let mut cur_session_key: Option<(u32, u32)> = None;

    for (row_idx, entry) in entries.iter().enumerate() {
        let r = &entry.row;
        let skey = (r.uid_internal, entry.session_seq);
        if cur_session_key != Some(skey) {
            let sid_idx = session_ids.len() as u32;
            session_ids.push(r.session_id.clone());
            session_headers.push(SessionHeader {
                session_hash: r.session_hash,
                start_ts_ms: r.event_ts_ms,
                end_ts_ms: r.event_ts_ms,
                event_bloom_64: SessionHeader::event_bit(r.event_id),
                session_id_idx: sid_idx,
                raw_uid: r.uid_internal,
                event_count: 1,
                entry_event_id: r.event_id,
                exit_event_id: r.event_id,
                flags: r.session_flags,
                _pad: [0u8; 6],
            });
            cur_session_key = Some(skey);
        } else if let Some(hdr) = session_headers.last_mut() {
            hdr.end_ts_ms = hdr.end_ts_ms.max(r.event_ts_ms);
            hdr.start_ts_ms = hdr.start_ts_ms.min(r.event_ts_ms);
            hdr.event_bloom_64 |= SessionHeader::event_bit(r.event_id);
            hdr.event_count += 1;
            hdr.exit_event_id = r.event_id;
            hdr.flags |= r.session_flags;
        }

        // Per-row property dispatch (preserving explicit `PropValue::Null` in both Tier B and Tier C — `P2-09l`)
        let mut row_promoted_seen: HashMap<u16, &PropValue> = HashMap::new();
        let mut overflow_props: Vec<(u16, &PropValue)> = Vec::new();

        for (pk, pv) in &r.props {
            if promoted.contains_key(pk) {
                row_promoted_seen.insert(*pk, pv);
            } else {
                tier_c_bloom_64 |= tier_c_key_bit(*pk);
                overflow_props.push((*pk, pv));
            }
        }

        for (&pk, builders) in &mut tier_b_builders {
            let val = row_promoted_seen.get(&pk).copied();
            if let Some(b) = &mut builders.i64_b {
                if let Some(PropValue::Int(v)) = val {
                    b.append_value(*v);
                } else {
                    b.append_null();
                }
            }
            if let Some(b) = &mut builders.f64_b {
                if let Some(PropValue::Float(v)) = val {
                    b.append_value(*v);
                } else {
                    b.append_null();
                }
            }
            if let Some(b) = &mut builders.bool_b {
                if let Some(PropValue::Bool(v)) = val {
                    b.append_value(*v);
                    if *v {
                        builders.bool_true.insert(row_idx as u32);
                    } else {
                        builders.bool_false.insert(row_idx as u32);
                    }
                } else {
                    b.append_null();
                }
            }
            if let Some(b) = &mut builders.str_b {
                if let Some(PropValue::Str(s)) = val {
                    b.append_value(s);
                    if !builders.str_overflowed_card {
                        if let Some(bm) = builders.str_card.get_mut(s) {
                            bm.insert(row_idx as u32);
                        } else if builders.str_card.len() < MAX_INVERTED_INDEX_CARDINALITY {
                            let mut bm = RoaringBitmap::new();
                            bm.insert(row_idx as u32);
                            builders.str_card.insert(s.clone(), bm);
                        } else {
                            builders.str_card.clear();
                            builders.str_overflowed_card = true;
                        }
                    }
                } else {
                    b.append_null();
                }
            }
            if let Some(b) = &mut builders.null_b {
                if matches!(val, Some(PropValue::Null)) {
                    b.append_value(true);
                } else {
                    b.append_null();
                }
            }
        }

        if overflow_props.is_empty() {
            tier_c_rows.push(None);
        } else {
            tier_c_rows.push(Some(encode_tier_c_row(&overflow_props)));
        }
    }

    // Assemble Arrow Schema + Arrays -> Vortex Columnar File
    let mut fields: Vec<Field> = vec![
        Field::new("uid_internal", DataType::UInt32, false),
        Field::new("session_hash", DataType::UInt64, false),
        Field::new("event_uuid_fp", DataType::UInt64, false),
        Field::new("event_id", DataType::UInt16, false),
        Field::new("event_ts_ms", DataType::Int64, false),
        Field::new("replay_offset_kb", DataType::UInt32, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(uid_vec)),
        Arc::new(UInt64Array::from(shash_vec)),
        Arc::new(UInt64Array::from(uuid_fp_vec)),
        Arc::new(UInt16Array::from(event_id_vec)),
        Arc::new(arrow_array::Int64Array::from(ts_vec)),
        Arc::new(UInt32Array::from(replay_kb_vec)),
    ];

    let mut inverted_idx = SegmentInvertedIndex::default();
    let mut tier_b_catalog = Vec::with_capacity(tier_b_builders.len());

    for (pk, mut b) in tier_b_builders {
        let has_i64 = b.i64_b.is_some();
        let has_f64 = b.f64_b.is_some();
        let has_bool = b.bool_b.is_some();
        let has_str = b.str_b.is_some();
        let has_null = b.null_b.is_some();
        let mut bool_indexed = false;
        let mut str_indexed_complete = false;

        if let Some(mut ib) = b.i64_b.take() {
            fields.push(Field::new(
                TierBColumnEntry::col_i64_name(pk),
                DataType::Int64,
                true,
            ));
            columns.push(Arc::new(ib.finish()));
        }
        if let Some(mut fb) = b.f64_b.take() {
            fields.push(Field::new(
                TierBColumnEntry::col_f64_name(pk),
                DataType::Float64,
                true,
            ));
            columns.push(Arc::new(fb.finish()));
        }
        if let Some(mut bb) = b.bool_b.take() {
            fields.push(Field::new(
                TierBColumnEntry::col_bool_name(pk),
                DataType::Boolean,
                true,
            ));
            columns.push(Arc::new(bb.finish()));
            if !b.bool_true.is_empty() {
                inverted_idx.bitmaps.insert(
                    (pk, InvertedValueTag::Bool, "true".to_string()),
                    b.bool_true,
                );
            }
            if !b.bool_false.is_empty() {
                inverted_idx.bitmaps.insert(
                    (pk, InvertedValueTag::Bool, "false".to_string()),
                    b.bool_false,
                );
            }
            bool_indexed = true;
        }
        if let Some(mut sb) = b.str_b.take() {
            fields.push(Field::new(
                TierBColumnEntry::col_str_name(pk),
                DataType::Utf8,
                true,
            ));
            columns.push(Arc::new(sb.finish()));
            if !b.str_overflowed_card {
                for (val, bm) in b.str_card {
                    inverted_idx
                        .bitmaps
                        .insert((pk, InvertedValueTag::Str, val), bm);
                }
                str_indexed_complete = true;
            }
        }
        if let Some(mut nb) = b.null_b.take() {
            fields.push(Field::new(
                TierBColumnEntry::col_null_name(pk),
                DataType::Boolean,
                true,
            ));
            columns.push(Arc::new(nb.finish()));
        }

        tier_b_catalog.push(TierBColumnEntry {
            prop_key_id: pk,
            has_i64,
            has_f64,
            has_bool,
            has_str,
            has_null,
            bool_indexed,
            str_indexed_complete,
        });
    }

    let has_tier_c_column = tier_c_bloom_64 != 0;
    if has_tier_c_column {
        let mut bin_b = BinaryBuilder::with_capacity(n, n * 16);
        for item in &tier_c_rows {
            match item {
                Some(bytes) => bin_b.append_value(bytes),
                None => bin_b.append_null(),
            }
        }
        fields.push(Field::new("tier_c_overflow", DataType::Binary, true));
        columns.push(Arc::new(bin_b.finish()));
    }

    let schema = Arc::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema, columns)?;
    let vortex_array = VortexArrayRef::from_arrow(record_batch, false);
    let vortex_buf = VortexWriteOptions::default()
        .write(ByteBufferMut::empty(), vortex_array.to_array_stream())
        .await?
        .freeze();

    let vortex_bytes = vortex_buf.as_slice();
    let inverted_bytes = inverted_idx.encode()?;
    let session_idx_bytes = encode_session_index(&session_headers, &session_ids)?;

    let footer_meta = SegmentFooterMeta {
        row_count: n as u32,
        session_count: session_headers.len() as u32,
        min_ts_ms,
        max_ts_ms,
        segment_event_bloom_64,
        tier_c_bloom_64,
        has_tier_c_column,
        tier_b_columns: tier_b_catalog,
    };

    let vortex_len = vortex_bytes.len() as u64;
    let inverted_idx_offset = vortex_len;
    let session_idx_offset = inverted_idx_offset + inverted_bytes.len() as u64;
    let total_size = session_idx_offset + session_idx_bytes.len() as u64;

    let mut container = Vec::with_capacity(total_size as usize);
    container.extend_from_slice(vortex_bytes);
    container.extend_from_slice(&inverted_bytes);
    container.extend_from_slice(&session_idx_bytes);

    let yyyymmdd = date.format("%Y%m%d").to_string();
    let rel_path =
        format!("segments/{project_id}/{yyyymmdd}/bucket_{bucket_id:03}/{segment_id}.pvtx");
    let obj_path = ObjectPath::from(rel_path.as_str());
    object_store
        .put(&obj_path, PutPayload::from_bytes(Bytes::from(container)))
        .await?;

    Ok(Some(SegmentMeta {
        segment_id: segment_id.to_string(),
        object_path: rel_path,
        date,
        bucket_id,
        file_size: total_size,
        vortex_len,
        inverted_idx_offset,
        session_idx_offset,
        footer: footer_meta,
    }))
}

fn encode_session_index(headers: &[SessionHeader], session_ids: &[String]) -> Result<Vec<u8>> {
    let hdr_bytes: &[u8] = bytemuck::cast_slice(headers);
    let mut raw = Vec::with_capacity(8 + hdr_bytes.len() + session_ids.len() * 16);
    raw.extend_from_slice(&(headers.len() as u32).to_le_bytes());
    raw.extend_from_slice(hdr_bytes);
    raw.extend_from_slice(&(session_ids.len() as u32).to_le_bytes());
    for s in session_ids {
        raw.extend_from_slice(&(s.len() as u32).to_le_bytes());
        raw.extend_from_slice(s.as_bytes());
    }
    Ok(zstd::stream::encode_all(raw.as_slice(), 1)?)
}

pub fn decode_session_index(compressed: &[u8]) -> Result<SegmentSessionIndex> {
    let raw = decode_zstd_bounded(compressed, None, MAX_DECOMPRESSED_BYTES)?;
    if raw.len() < 4 {
        return Err(StorageError::Corrupted(
            "session index shorter than 4 bytes".to_string(),
        ));
    }
    let hdr_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let hdr_byte_len = hdr_count
        .checked_mul(std::mem::size_of::<SessionHeader>())
        .ok_or_else(|| StorageError::Corrupted("session index header count overflow".into()))?;
    if raw.len() < 4 + hdr_byte_len + 4 {
        return Err(StorageError::Corrupted(
            "session index truncated headers".to_string(),
        ));
    }
    let mut headers = vec![SessionHeader::zeroed(); hdr_count];
    bytemuck::cast_slice_mut::<SessionHeader, u8>(&mut headers)
        .copy_from_slice(&raw[4..4 + hdr_byte_len]);

    let mut pos = 4 + hdr_byte_len;
    let str_count =
        u32::from_le_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]) as usize;
    pos += 4;
    let mut session_ids = Vec::with_capacity(str_count.min((raw.len() - pos) / 4));
    for _ in 0..str_count {
        if pos + 4 > raw.len() {
            return Err(StorageError::Corrupted(
                "session index string len EOF".to_string(),
            ));
        }
        let slen =
            u32::from_le_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]) as usize;
        pos += 4;
        if pos + slen > raw.len() {
            return Err(StorageError::Corrupted(
                "session index string data EOF".to_string(),
            ));
        }
        let s = String::from_utf8(raw[pos..pos + slen].to_vec())
            .map_err(|e| StorageError::Corrupted(format!("session_id utf8 error: {e}")))?;
        pos += slen;
        session_ids.push(s);
    }

    Ok(SegmentSessionIndex {
        headers,
        session_ids,
    })
}

fn encode_tier_c_row(props: &[(u16, &PropValue)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + props.len() * 8);
    out.extend_from_slice(&(props.len() as u16).to_le_bytes());
    for (pk, pv) in props {
        out.extend_from_slice(&pk.to_le_bytes());
        out.push(pv.tag());
        match pv {
            PropValue::Null => {}
            PropValue::Bool(b) => out.push(u8::from(*b)),
            PropValue::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
            PropValue::Float(f) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
            PropValue::Str(s) => {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
    out
}

pub fn decode_tier_c_row(bytes: &[u8]) -> Result<Vec<(u16, PropValue)>> {
    if bytes.len() < 2 {
        return Ok(Vec::new());
    }
    let count = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let mut pos = 2usize;
    let mut out = Vec::with_capacity(count.min((bytes.len() - 2) / 3));
    for _ in 0..count {
        if pos + 3 > bytes.len() {
            return Err(StorageError::Corrupted("tier_c header EOF".to_string()));
        }
        let pk = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]);
        let tag = bytes[pos + 2];
        pos += 3;
        let val = match tag {
            PropValue::TAG_NULL => PropValue::Null,
            PropValue::TAG_BOOL => {
                if pos >= bytes.len() {
                    return Err(StorageError::Corrupted("tier_c bool EOF".to_string()));
                }
                let b = bytes[pos] != 0;
                pos += 1;
                PropValue::Bool(b)
            }
            PropValue::TAG_INT => {
                if pos + 8 > bytes.len() {
                    return Err(StorageError::Corrupted("tier_c int EOF".to_string()));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[pos..pos + 8]);
                pos += 8;
                PropValue::Int(i64::from_le_bytes(arr))
            }
            PropValue::TAG_FLOAT => {
                if pos + 8 > bytes.len() {
                    return Err(StorageError::Corrupted("tier_c float EOF".to_string()));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[pos..pos + 8]);
                pos += 8;
                PropValue::Float(f64::from_bits(u64::from_le_bytes(arr)))
            }
            PropValue::TAG_STR => {
                if pos + 4 > bytes.len() {
                    return Err(StorageError::Corrupted("tier_c str len EOF".to_string()));
                }
                let slen = u32::from_le_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]) as usize;
                pos += 4;
                if pos + slen > bytes.len() {
                    return Err(StorageError::Corrupted("tier_c str EOF".to_string()));
                }
                let s = String::from_utf8(bytes[pos..pos + slen].to_vec())
                    .map_err(|e| StorageError::Corrupted(format!("tier_c utf8: {e}")))?;
                pos += slen;
                PropValue::Str(s)
            }
            other => {
                return Err(StorageError::Corrupted(format!(
                    "unknown tier_c tag {other}"
                )))
            }
        };
        out.push((pk, val));
    }
    Ok(out)
}

/// Byte-range reader for immutable Vortex Tier A/B/C segments with instrumentation counters.
pub struct VortexSegmentReader {
    object_store: Arc<dyn ObjectStore>,
    pub session_index_reads: AtomicU64,
    pub tier_ab_reads: AtomicU64,
}

impl VortexSegmentReader {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            session_index_reads: AtomicU64::new(0),
            tier_ab_reads: AtomicU64::new(0),
        }
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    /// Reads ONLY the `SessionIndex` byte-range without touching Tier A/B Vortex columns.
    pub async fn read_session_index(&self, meta: &SegmentMeta) -> Result<SegmentSessionIndex> {
        self.session_index_reads.fetch_add(1, Ordering::Relaxed);
        let path = ObjectPath::from(meta.object_path.as_str());
        let bytes = self
            .object_store
            .get_range(&path, meta.session_idx_offset..meta.file_size)
            .await?;
        decode_session_index(&bytes)
    }

    /// Reads ONLY the Tier B inverted `RoaringBitmap` index section.
    pub async fn read_inverted_index(&self, meta: &SegmentMeta) -> Result<SegmentInvertedIndex> {
        let path = ObjectPath::from(meta.object_path.as_str());
        let bytes = self
            .object_store
            .get_range(&path, meta.inverted_idx_offset..meta.session_idx_offset)
            .await?;
        SegmentInvertedIndex::decode(&bytes)
    }

    /// Fetches raw Vortex payload `[0..vortex_len]` (used directly or via `foyer` block cache in `pulse-exec`).
    pub async fn fetch_vortex_bytes(&self, meta: &SegmentMeta) -> Result<Bytes> {
        self.tier_ab_reads.fetch_add(1, Ordering::Relaxed);
        let path = ObjectPath::from(meta.object_path.as_str());
        let bytes = self
            .object_store
            .get_range(&path, 0..meta.vortex_len)
            .await?;
        Ok(bytes)
    }

    /// Projects a subset of columns from raw Vortex bytes into an Arrow `RecordBatch` synchronously.
    pub fn project_from_vortex_bytes_sync(
        &self,
        vortex_bytes: Bytes,
        columns: &[&str],
    ) -> Result<RecordBatch> {
        let field_names: Vec<FieldName> = columns.iter().map(|&c| FieldName::from(c)).collect();
        let vfile = VortexOpenOptions::in_memory().open(ByteBuffer::from(vortex_bytes))?;
        let array = vfile
            .scan()?
            .with_projection(select(field_names, root()))
            .into_array_iter()?
            .read_all()?;
        Ok(RecordBatch::try_from(array.as_ref())?)
    }

    /// Projects a subset of columns from raw Vortex bytes into an Arrow `RecordBatch`.
    pub async fn project_from_vortex_bytes(
        &self,
        vortex_bytes: Bytes,
        columns: &[&str],
    ) -> Result<RecordBatch> {
        self.project_from_vortex_bytes_sync(vortex_bytes, columns)
    }

    /// Projects columns from a segment, reading Vortex bytes only when invoked.
    pub async fn project_columns(
        &self,
        meta: &SegmentMeta,
        columns: &[&str],
    ) -> Result<RecordBatch> {
        let vortex_bytes = self.fetch_vortex_bytes(meta).await?;
        self.project_from_vortex_bytes(vortex_bytes, columns).await
    }

    /// Prunes sessions via `SessionIndex` 64-bit Bloom bitmask and flags BEFORE touching Tier A/B arrays!
    /// Returns `(skipped_sessions, matched_sessions, Option<RecordBatch of Tier A columns>)`.
    pub async fn scan_sessions_pruned(
        &self,
        meta: &SegmentMeta,
        any_events_mask: u64,
        required_flags: u16,
    ) -> Result<(
        usize,
        Vec<(usize, usize, SessionHeader)>,
        Option<RecordBatch>,
    )> {
        let sidx = self.read_session_index(meta).await?;
        let total = sidx.headers.len();
        let matched = sidx.matching_session_ranges(|hdr| {
            hdr.may_contain_any_event(any_events_mask)
                && (hdr.flags & required_flags) == required_flags
        });
        let skipped = total.saturating_sub(matched.len());
        if matched.is_empty() {
            return Ok((skipped, matched, None));
        }
        let batch = self
            .project_columns(
                meta,
                &[
                    "uid_internal",
                    "session_hash",
                    "event_id",
                    "event_ts_ms",
                    "replay_offset_kb",
                ],
            )
            .await?;
        Ok((skipped, matched, Some(batch)))
    }

    /// Reconstructs full `StoredRow` vector from an L1 segment (used by background `Compactor` and tests).
    pub async fn read_all_rows(&self, meta: &SegmentMeta) -> Result<Vec<StoredRow>> {
        let sidx = self.read_session_index(meta).await?;
        let vortex_bytes = self.fetch_vortex_bytes(meta).await?;
        let vfile = VortexOpenOptions::in_memory().open(ByteBuffer::from(vortex_bytes))?;
        let array = vfile.scan()?.into_array_iter()?.read_all()?;
        let batch = RecordBatch::try_from(array.as_ref())?;

        let n = batch.num_rows();
        let uid_col = batch
            .column_by_name("uid_internal")
            .ok_or_else(|| StorageError::Corrupted("missing uid_internal".into()))?
            .as_primitive::<UInt32Type>();
        let shash_col = batch
            .column_by_name("session_hash")
            .ok_or_else(|| StorageError::Corrupted("missing session_hash".into()))?
            .as_primitive::<UInt64Type>();
        let uuid_fp_col = batch
            .column_by_name("event_uuid_fp")
            .ok_or_else(|| StorageError::Corrupted("missing event_uuid_fp".into()))?
            .as_primitive::<UInt64Type>();
        let ev_col = batch
            .column_by_name("event_id")
            .ok_or_else(|| StorageError::Corrupted("missing event_id".into()))?
            .as_primitive::<UInt16Type>();
        let ts_col = batch
            .column_by_name("event_ts_ms")
            .ok_or_else(|| StorageError::Corrupted("missing event_ts_ms".into()))?
            .as_primitive::<Int64Type>();
        let rep_col = batch
            .column_by_name("replay_offset_kb")
            .ok_or_else(|| StorageError::Corrupted("missing replay_offset_kb".into()))?
            .as_primitive::<UInt32Type>();

        let mut row_session_meta: Vec<(&str, u16)> = Vec::with_capacity(n);
        for hdr in &sidx.headers {
            let sid = sidx.session_id(hdr);
            for _ in 0..hdr.event_count {
                row_session_meta.push((sid, hdr.flags));
            }
        }

        let tier_c_col = if meta.footer.has_tier_c_column {
            batch
                .column_by_name("tier_c_overflow")
                .map(|c| c.as_binary::<i32>())
        } else {
            None
        };

        let mut tb_cols = Vec::with_capacity(meta.footer.tier_b_columns.len());
        for entry in &meta.footer.tier_b_columns {
            let pk = entry.prop_key_id;
            let c_i64 = entry
                .has_i64
                .then(|| batch.column_by_name(&TierBColumnEntry::col_i64_name(pk)))
                .flatten();
            let c_f64 = entry
                .has_f64
                .then(|| batch.column_by_name(&TierBColumnEntry::col_f64_name(pk)))
                .flatten();
            let c_bool = entry
                .has_bool
                .then(|| batch.column_by_name(&TierBColumnEntry::col_bool_name(pk)))
                .flatten();
            let c_str = entry
                .has_str
                .then(|| batch.column_by_name(&TierBColumnEntry::col_str_name(pk)))
                .flatten();
            let c_null = entry
                .has_null
                .then(|| batch.column_by_name(&TierBColumnEntry::col_null_name(pk)))
                .flatten();
            tb_cols.push((pk, c_i64, c_f64, c_bool, c_str, c_null));
        }

        let mut out = Vec::with_capacity(n);
        for row_idx in 0..n {
            let mut props = Vec::new();
            for &(pk, c_i64, c_f64, c_bool, c_str, c_null) in &tb_cols {
                if let Some(col) = c_i64 {
                    if !col.is_null(row_idx) {
                        props.push((
                            pk,
                            PropValue::Int(col.as_primitive::<Int64Type>().value(row_idx)),
                        ));
                        continue;
                    }
                }
                if let Some(col) = c_f64 {
                    if !col.is_null(row_idx) {
                        props.push((
                            pk,
                            PropValue::Float(col.as_primitive::<Float64Type>().value(row_idx)),
                        ));
                        continue;
                    }
                }
                if let Some(col) = c_bool {
                    if !col.is_null(row_idx) {
                        props.push((pk, PropValue::Bool(col.as_boolean().value(row_idx))));
                        continue;
                    }
                }
                if let Some(col) = c_str {
                    if !col.is_null(row_idx) {
                        let s = if let Some(sa) = col.as_string_opt::<i32>() {
                            sa.value(row_idx).to_string()
                        } else if let Some(sa) = col.as_string_opt::<i64>() {
                            sa.value(row_idx).to_string()
                        } else if let Some(sv) = col.as_string_view_opt() {
                            sv.value(row_idx).to_string()
                        } else {
                            String::new()
                        };
                        props.push((pk, PropValue::Str(s)));
                        continue;
                    }
                }
                if let Some(col) = c_null {
                    if !col.is_null(row_idx) && col.as_boolean().value(row_idx) {
                        props.push((pk, PropValue::Null));
                    }
                }
            }

            if let Some(tc) = tier_c_col {
                if !tc.is_null(row_idx) {
                    let mut extra = decode_tier_c_row(tc.value(row_idx))?;
                    props.append(&mut extra);
                }
            }

            let (sid, flags) = row_session_meta.get(row_idx).copied().unwrap_or(("", 0));

            out.push(StoredRow {
                l0_seq: 0,
                wal_seq: 0,
                event_uuid_fp: uuid_fp_col.value(row_idx),
                uid_internal: uid_col.value(row_idx),
                session_hash: shash_col.value(row_idx),
                session_id: sid.to_string(),
                event_id: ev_col.value(row_idx),
                event_ts_ms: ts_col.value(row_idx),
                replay_offset_kb: rep_col.value(row_idx),
                session_flags: flags,
                props,
            });
        }

        Ok(out)
    }
}
