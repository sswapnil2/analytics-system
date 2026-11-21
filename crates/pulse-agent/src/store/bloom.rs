use std::collections::{BTreeMap, HashSet};
use std::io::Cursor;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::model::{AgentSpan, AgentTrace, TraceListFilter, TraceStatus};
use crate::pricing::usd_to_cost_nanos;
use crate::store::error::{StoreError, StoreResult};

pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 12;
pub const MIN_BLOOM_BITS_PER_KEY: usize = 12;
pub const MAX_BLOOM_BLOCKS: usize = 1_048_576; // 32 MiB
pub const MAX_TAG06_DICT_ENTRIES: usize = 4_096;
pub const TAG06_VERSION: u8 = 1;

const SPLIT_BLOOM_SALTS: [u32; 8] = [
    0x47b6_137b,
    0x4497_4d91,
    0x8824_ad5b,
    0xa2b7_289d,
    0x7054_95c7,
    0x2df1_424b,
    0x9efc_4947,
    0x5c6b_fb31,
];

/// Cache-line-aligned 256-bit Split-Block Bloom Filter block (`k=8` probes, 1 bit per 32-bit word).
#[repr(C, align(32))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BloomBlock256(pub [u32; 8]);

/// Dynamically sized 256-bit Split-Block Bloom Filter (`>= 12` bits/key floor, `< 1%` FP rate).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SplitBlockBloom {
    blocks: Vec<BloomBlock256>,
}

impl SplitBlockBloom {
    pub fn with_capacity(num_keys: usize, bits_per_key: usize) -> Self {
        if num_keys == 0 {
            return Self { blocks: Vec::new() };
        }
        let bpk = bits_per_key.max(MIN_BLOOM_BITS_PER_KEY);
        let num_blocks = num_keys
            .saturating_mul(bpk)
            .div_ceil(256)
            .clamp(1, MAX_BLOOM_BLOCKS);
        Self {
            blocks: vec![BloomBlock256::default(); num_blocks],
        }
    }

    #[inline]
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    #[inline]
    pub fn serialized_len(&self) -> usize {
        4 + self.blocks.len().saturating_mul(32)
    }

    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        if self.blocks.is_empty() {
            return;
        }
        let h = xxh3_64(key);
        let block_idx = (((h >> 32) as u128 * (self.blocks.len() as u128)) >> 32) as usize;
        let x = h as u32;
        let block = &mut self.blocks[block_idx];
        for (i, &salt) in SPLIT_BLOOM_SALTS.iter().enumerate() {
            let bit = x.wrapping_mul(salt) >> 27;
            block.0[i] |= 1u32 << bit;
        }
    }

    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.blocks.is_empty() {
            return false;
        }
        let h = xxh3_64(key);
        let block_idx = (((h >> 32) as u128 * (self.blocks.len() as u128)) >> 32) as usize;
        let x = h as u32;
        let block = &self.blocks[block_idx];
        for (i, &salt) in SPLIT_BLOOM_SALTS.iter().enumerate() {
            let bit = x.wrapping_mul(salt) >> 27;
            if (block.0[i] & (1u32 << bit)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) -> StoreResult<()> {
        let count = u32::try_from(self.blocks.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("bloom block count overflow"))?;
        out.extend_from_slice(&count.to_le_bytes());
        for blk in &self.blocks {
            for word in &blk.0 {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }
        Ok(())
    }

    pub fn decode_from(buf: &[u8], pos: &mut usize) -> StoreResult<Self> {
        let end_count = pos
            .checked_add(4)
            .ok_or_else(|| StoreError::Corrupted("bloom block count offset overflow".into()))?;
        if end_count > buf.len() {
            return Err(StoreError::Corrupted("truncated bloom block count".into()));
        }
        let count =
            u32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]) as usize;
        *pos = end_count;
        if count > MAX_BLOOM_BLOCKS {
            return Err(StoreError::ResourceLimitExceeded(
                "bloom block count exceeds MAX_BLOOM_BLOCKS",
            ));
        }
        let byte_len = count
            .checked_mul(32)
            .ok_or(StoreError::ResourceLimitExceeded(
                "bloom byte length overflow",
            ))?;
        let end_bytes = pos
            .checked_add(byte_len)
            .ok_or(StoreError::ResourceLimitExceeded(
                "bloom end offset overflow",
            ))?;
        if end_bytes > buf.len() {
            return Err(StoreError::Corrupted("truncated bloom filter body".into()));
        }
        let mut blocks = Vec::with_capacity(count.min(4096));
        for chunk in buf[*pos..end_bytes].chunks_exact(32) {
            let mut words = [0u32; 8];
            for (i, w) in chunk.chunks_exact(4).enumerate() {
                words[i] = u32::from_le_bytes([w[0], w[1], w[2], w[3]]);
            }
            blocks.push(BloomBlock256(words));
        }
        *pos = end_bytes;
        Ok(Self { blocks })
    }
}

/// Segment-level min/max and count statistics for O(1) range pruning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SegmentZoneMap {
    pub min_start_ts_ms: i64,
    pub max_start_ts_ms: i64,
    pub min_duration_ms: i64,
    pub max_duration_ms: i64,
    pub min_cost_nanos: i64,
    pub max_cost_nanos: i64,
    pub trace_count: u32,
    pub span_count: u32,
    pub session_count: u32,
    pub error_count: u32,
    pub loop_count: u32,
}

impl SegmentZoneMap {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.min_start_ts_ms.to_le_bytes());
        out.extend_from_slice(&self.max_start_ts_ms.to_le_bytes());
        out.extend_from_slice(&self.min_duration_ms.to_le_bytes());
        out.extend_from_slice(&self.max_duration_ms.to_le_bytes());
        out.extend_from_slice(&self.min_cost_nanos.to_le_bytes());
        out.extend_from_slice(&self.max_cost_nanos.to_le_bytes());
        out.extend_from_slice(&self.trace_count.to_le_bytes());
        out.extend_from_slice(&self.span_count.to_le_bytes());
        out.extend_from_slice(&self.session_count.to_le_bytes());
        out.extend_from_slice(&self.error_count.to_le_bytes());
        out.extend_from_slice(&self.loop_count.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // 72B aligned pad
    }

    pub fn decode_from(buf: &[u8], pos: &mut usize) -> StoreResult<Self> {
        let end = pos
            .checked_add(72)
            .ok_or_else(|| StoreError::Corrupted("zone map offset overflow".into()))?;
        if end > buf.len() {
            return Err(StoreError::Corrupted("truncated SegmentZoneMap".into()));
        }
        let s = &buf[*pos..end];
        *pos = end;
        let read_i64 = |off: usize| -> i64 {
            i64::from_le_bytes([
                s[off],
                s[off + 1],
                s[off + 2],
                s[off + 3],
                s[off + 4],
                s[off + 5],
                s[off + 6],
                s[off + 7],
            ])
        };
        let read_u32 = |off: usize| -> u32 {
            u32::from_le_bytes([s[off], s[off + 1], s[off + 2], s[off + 3]])
        };
        Ok(Self {
            min_start_ts_ms: read_i64(0),
            max_start_ts_ms: read_i64(8),
            min_duration_ms: read_i64(16),
            max_duration_ms: read_i64(24),
            min_cost_nanos: read_i64(32),
            max_cost_nanos: read_i64(40),
            trace_count: read_u32(48),
            span_count: read_u32(52),
            session_count: read_u32(56),
            error_count: read_u32(60),
            loop_count: read_u32(64),
        })
    }
}

/// Self-contained `Tag 0x06` EOF pruning section: Split-Block Blooms + Roaring dictionary bitsets + Zone Maps.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SegmentIndexSection {
    pub zone_map: SegmentZoneMap,
    pub trace_id_bloom: SplitBlockBloom,
    pub session_id_bloom: SplitBlockBloom,
    pub user_id_bloom: SplitBlockBloom,
    pub parent_trace_id_bloom: SplitBlockBloom,
    pub agent_name_bitmaps: BTreeMap<String, RoaringBitmap>,
    pub agent_name_truncated: bool,
    pub model_bitmaps: BTreeMap<String, RoaringBitmap>,
    pub model_truncated: bool,
    pub tool_name_bitmaps: BTreeMap<String, RoaringBitmap>,
    pub tool_name_truncated: bool,
    pub environment_bitmaps: BTreeMap<String, RoaringBitmap>,
    pub environment_truncated: bool,
    pub status_bitmaps: BTreeMap<u8, RoaringBitmap>,
}

fn insert_dict_bit(
    map: &mut BTreeMap<String, RoaringBitmap>,
    truncated: &mut bool,
    key: &str,
    trace_idx: u32,
) {
    if key.is_empty() {
        return;
    }
    if let Some(bm) = map.get_mut(key) {
        bm.insert(trace_idx);
        return;
    }
    if map.len() >= MAX_TAG06_DICT_ENTRIES {
        *truncated = true;
        return;
    }
    let mut bm = RoaringBitmap::new();
    bm.insert(trace_idx);
    map.insert(key.to_string(), bm);
}

impl SegmentIndexSection {
    /// Builds `Tag 0x06` from trace entries already ordered as `trace_idx = 0..traces.len()`.
    pub fn build_from_traces(traces: &[(AgentTrace, Vec<AgentSpan>)], bits_per_key: usize) -> Self {
        let mut trace_keys = Vec::with_capacity(traces.len());
        let mut session_keys: HashSet<&str> = HashSet::new();
        let mut user_keys: HashSet<&str> = HashSet::new();
        let mut parent_trace_keys: HashSet<&str> = HashSet::new();

        let mut zone = SegmentZoneMap::default();
        let mut agent_name_bitmaps = BTreeMap::new();
        let mut agent_name_truncated = false;
        let mut model_bitmaps = BTreeMap::new();
        let mut model_truncated = false;
        let mut tool_name_bitmaps = BTreeMap::new();
        let mut tool_name_truncated = false;
        let mut environment_bitmaps = BTreeMap::new();
        let mut environment_truncated = false;
        let mut status_bitmaps: BTreeMap<u8, RoaringBitmap> = BTreeMap::new();

        let mut total_spans: u64 = 0;
        for (idx, (trace, spans)) in traces.iter().enumerate() {
            let trace_idx = idx as u32;
            trace_keys.push(trace.trace_id.as_str());
            if let Some(sid) = trace.session_id.as_deref().filter(|s| !s.is_empty()) {
                session_keys.insert(sid);
            }
            if let Some(uid) = trace.user_id.as_deref().filter(|s| !s.is_empty()) {
                user_keys.insert(uid);
            }
            if let Some(ptid) = trace.parent_trace_id.as_deref().filter(|s| !s.is_empty()) {
                parent_trace_keys.insert(ptid);
            }

            if idx == 0 {
                zone.min_start_ts_ms = trace.start_ts_ms;
                zone.max_start_ts_ms = trace.start_ts_ms;
                zone.min_duration_ms = trace.duration_ms;
                zone.max_duration_ms = trace.duration_ms;
                zone.min_cost_nanos = trace.total_cost_nanos;
                zone.max_cost_nanos = trace.total_cost_nanos;
            } else {
                zone.min_start_ts_ms = zone.min_start_ts_ms.min(trace.start_ts_ms);
                zone.max_start_ts_ms = zone.max_start_ts_ms.max(trace.start_ts_ms);
                zone.min_duration_ms = zone.min_duration_ms.min(trace.duration_ms);
                zone.max_duration_ms = zone.max_duration_ms.max(trace.duration_ms);
                zone.min_cost_nanos = zone.min_cost_nanos.min(trace.total_cost_nanos);
                zone.max_cost_nanos = zone.max_cost_nanos.max(trace.total_cost_nanos);
            }

            if matches!(trace.status, TraceStatus::Error | TraceStatus::Timeout) {
                zone.error_count = zone.error_count.saturating_add(1);
            }
            if trace.has_loop_warning {
                zone.loop_count = zone.loop_count.saturating_add(1);
            }
            total_spans = total_spans.saturating_add(spans.len() as u64);

            insert_dict_bit(
                &mut agent_name_bitmaps,
                &mut agent_name_truncated,
                &trace.agent_name,
                trace_idx,
            );
            insert_dict_bit(
                &mut environment_bitmaps,
                &mut environment_truncated,
                &trace.environment,
                trace_idx,
            );
            if let Some(pm) = trace.primary_model.as_deref() {
                insert_dict_bit(&mut model_bitmaps, &mut model_truncated, pm, trace_idx);
            }
            for sp in spans {
                if let Some(m) = sp.model.as_deref() {
                    insert_dict_bit(&mut model_bitmaps, &mut model_truncated, m, trace_idx);
                }
                if let Some(t) = sp.tool_name.as_deref() {
                    insert_dict_bit(
                        &mut tool_name_bitmaps,
                        &mut tool_name_truncated,
                        t,
                        trace_idx,
                    );
                }
            }
            status_bitmaps
                .entry(trace.status.to_u8())
                .or_default()
                .insert(trace_idx);
        }

        zone.trace_count = u32::try_from(traces.len()).unwrap_or(u32::MAX);
        zone.span_count = u32::try_from(total_spans).unwrap_or(u32::MAX);
        zone.session_count = u32::try_from(session_keys.len()).unwrap_or(u32::MAX);

        let bpk = bits_per_key.max(MIN_BLOOM_BITS_PER_KEY);
        let mut trace_id_bloom = SplitBlockBloom::with_capacity(trace_keys.len(), bpk);
        for k in trace_keys {
            trace_id_bloom.insert(k.as_bytes());
        }
        let mut session_id_bloom = SplitBlockBloom::with_capacity(session_keys.len(), bpk);
        for k in session_keys {
            session_id_bloom.insert(k.as_bytes());
        }
        let mut user_id_bloom = SplitBlockBloom::with_capacity(user_keys.len(), bpk);
        for k in user_keys {
            user_id_bloom.insert(k.as_bytes());
        }
        let mut parent_trace_id_bloom =
            SplitBlockBloom::with_capacity(parent_trace_keys.len(), bpk);
        for k in parent_trace_keys {
            parent_trace_id_bloom.insert(k.as_bytes());
        }

        Self {
            zone_map: zone,
            trace_id_bloom,
            session_id_bloom,
            user_id_bloom,
            parent_trace_id_bloom,
            agent_name_bitmaps,
            agent_name_truncated,
            model_bitmaps,
            model_truncated,
            tool_name_bitmaps,
            tool_name_truncated,
            environment_bitmaps,
            environment_truncated,
            status_bitmaps,
        }
    }

    #[inline]
    pub fn may_contain_trace(&self, trace_id: &str) -> bool {
        self.trace_id_bloom.may_contain(trace_id.as_bytes())
    }

    #[inline]
    pub fn may_contain_session(&self, session_id: &str) -> bool {
        self.session_id_bloom.may_contain(session_id.as_bytes())
    }

    #[inline]
    pub fn may_contain_user(&self, user_id: &str) -> bool {
        self.user_id_bloom.may_contain(user_id.as_bytes())
    }

    #[inline]
    pub fn may_contain_parent_trace(&self, parent_trace_id: &str) -> bool {
        self.parent_trace_id_bloom
            .may_contain(parent_trace_id.as_bytes())
    }

    pub fn zone_map_matches_trace_filter(&self, filter: &TraceListFilter) -> bool {
        if self.zone_map.trace_count == 0 {
            return false;
        }
        if let Some(start) = filter.start_ts_ms {
            if self.zone_map.max_start_ts_ms < start {
                return false;
            }
        }
        if let Some(end) = filter.end_ts_ms {
            if self.zone_map.min_start_ts_ms > end {
                return false;
            }
        }
        if let Some(min_dur) = filter.min_duration_ms {
            if self.zone_map.max_duration_ms < min_dur {
                return false;
            }
        }
        if let Some(max_dur) = filter.max_duration_ms {
            if self.zone_map.min_duration_ms > max_dur {
                return false;
            }
        }
        if let Some(min_cost) = filter.min_cost_usd.filter(|c| !c.is_nan()) {
            let min_nanos = usd_to_cost_nanos(min_cost).saturating_sub(1);
            if self.zone_map.max_cost_nanos < min_nanos {
                return false;
            }
        }
        if let Some(max_cost) = filter.max_cost_usd.filter(|c| !c.is_nan()) {
            let max_nanos = usd_to_cost_nanos(max_cost).saturating_add(1);
            if self.zone_map.min_cost_nanos > max_nanos {
                return false;
            }
        }
        if filter.has_error == Some(true) && self.zone_map.error_count == 0 {
            return false;
        }
        if filter.has_loop_warning == Some(true) && self.zone_map.loop_count == 0 {
            return false;
        }
        if let Some(sid) = filter.session_id.as_deref().filter(|s| !s.is_empty()) {
            if !self.may_contain_session(sid) {
                return false;
            }
        }
        if let Some(uid) = filter.user_id.as_deref().filter(|s| !s.is_empty()) {
            if !self.may_contain_user(uid) {
                return false;
            }
        }
        if let Some(ptid) = filter.parent_trace_id.as_deref().filter(|s| !s.is_empty()) {
            if !self.may_contain_parent_trace(ptid) {
                return false;
            }
        }
        true
    }

    /// Returns candidate `trace_idx` bitmap by intersecting dictionary bitsets for active filters,
    /// or `None` if no dictionary filter applies (or all matching dimensions were truncated).
    pub fn candidate_trace_indices(&self, filter: &TraceListFilter) -> Option<RoaringBitmap> {
        let mut acc: Option<RoaringBitmap> = None;

        let intersect_or_set = |acc: &mut Option<RoaringBitmap>, bm: &RoaringBitmap| match acc {
            Some(cur) => *cur &= bm,
            None => *acc = Some(bm.clone()),
        };

        if let Some(agent) = filter.agent_name.as_deref().filter(|s| !s.is_empty()) {
            if let Some(bm) = self.agent_name_bitmaps.get(agent) {
                intersect_or_set(&mut acc, bm);
            } else if !self.agent_name_truncated {
                return Some(RoaringBitmap::new());
            }
        }

        if let Some(model) = filter.model.as_deref().filter(|s| !s.is_empty()) {
            if let Some(bm) = self.model_bitmaps.get(model) {
                intersect_or_set(&mut acc, bm);
            } else if !self.model_truncated {
                return Some(RoaringBitmap::new());
            }
        }

        if let Some(env) = filter.environment.as_deref().filter(|s| !s.is_empty()) {
            if let Some(bm) = self.environment_bitmaps.get(env) {
                intersect_or_set(&mut acc, bm);
            } else if !self.environment_truncated {
                return Some(RoaringBitmap::new());
            }
        }

        if let Some(status_str) = filter.status.as_deref().filter(|s| !s.is_empty()) {
            let st_u8 = TraceStatus::parse(status_str).to_u8();
            if let Some(bm) = self.status_bitmaps.get(&st_u8) {
                intersect_or_set(&mut acc, bm);
            } else {
                return Some(RoaringBitmap::new());
            }
        }

        if let Some(has_err) = filter.has_error {
            let mut bm = RoaringBitmap::new();
            if has_err {
                if let Some(e) = self.status_bitmaps.get(&TraceStatus::Error.to_u8()) {
                    bm |= e;
                }
                if let Some(t) = self.status_bitmaps.get(&TraceStatus::Timeout.to_u8()) {
                    bm |= t;
                }
            } else {
                for st in [
                    TraceStatus::Ok,
                    TraceStatus::Running,
                    TraceStatus::Cancelled,
                ] {
                    if let Some(b) = self.status_bitmaps.get(&st.to_u8()) {
                        bm |= b;
                    }
                }
            }
            intersect_or_set(&mut acc, &bm);
        }

        acc
    }

    pub fn candidate_traces_for_tool(&self, tool_name: &str) -> Option<RoaringBitmap> {
        if tool_name.is_empty() {
            return None;
        }
        if let Some(bm) = self.tool_name_bitmaps.get(tool_name) {
            Some(bm.clone())
        } else if !self.tool_name_truncated {
            Some(RoaringBitmap::new())
        } else {
            None
        }
    }

    pub fn encode(&self) -> StoreResult<Vec<u8>> {
        let mut out = Vec::with_capacity(256);
        let flags: u8 = (u8::from(self.agent_name_truncated))
            | (u8::from(self.model_truncated) << 1)
            | (u8::from(self.tool_name_truncated) << 2)
            | (u8::from(self.environment_truncated) << 3);
        out.push(TAG06_VERSION);
        out.push(flags);
        out.extend_from_slice(&0u16.to_le_bytes());

        self.zone_map.encode_into(&mut out);

        self.trace_id_bloom.encode_into(&mut out)?;
        self.session_id_bloom.encode_into(&mut out)?;
        self.user_id_bloom.encode_into(&mut out)?;
        self.parent_trace_id_bloom.encode_into(&mut out)?;

        encode_str_bitmap_map(&self.agent_name_bitmaps, &mut out)?;
        encode_str_bitmap_map(&self.model_bitmaps, &mut out)?;
        encode_str_bitmap_map(&self.tool_name_bitmaps, &mut out)?;
        encode_str_bitmap_map(&self.environment_bitmaps, &mut out)?;

        let status_cnt = u8::try_from(self.status_bitmaps.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("status bitmap count overflow"))?;
        out.push(status_cnt);
        for (&st, bm) in &self.status_bitmaps {
            out.push(st);
            let mut bm_bytes = Vec::with_capacity(bm.serialized_size());
            bm.serialize_into(&mut bm_bytes)?;
            let bm_len = u32::try_from(bm_bytes.len())
                .map_err(|_| StoreError::ResourceLimitExceeded("roaring bitmap overflow"))?;
            out.extend_from_slice(&bm_len.to_le_bytes());
            out.extend_from_slice(&bm_bytes);
        }

        Ok(out)
    }

    pub fn decode(buf: &[u8]) -> StoreResult<Self> {
        if buf.len() < 4 {
            return Err(StoreError::Corrupted("truncated Tag 0x06 header".into()));
        }
        let version = buf[0];
        if version != TAG06_VERSION {
            return Err(StoreError::Corrupted(format!(
                "unsupported Tag 0x06 version: {version}"
            )));
        }
        let flags = buf[1];
        let mut pos = 4usize;

        let zone_map = SegmentZoneMap::decode_from(buf, &mut pos)?;
        let trace_id_bloom = SplitBlockBloom::decode_from(buf, &mut pos)?;
        let session_id_bloom = SplitBlockBloom::decode_from(buf, &mut pos)?;
        let user_id_bloom = SplitBlockBloom::decode_from(buf, &mut pos)?;
        let parent_trace_id_bloom = SplitBlockBloom::decode_from(buf, &mut pos)?;

        let agent_name_bitmaps = decode_str_bitmap_map(buf, &mut pos)?;
        let model_bitmaps = decode_str_bitmap_map(buf, &mut pos)?;
        let tool_name_bitmaps = decode_str_bitmap_map(buf, &mut pos)?;
        let environment_bitmaps = decode_str_bitmap_map(buf, &mut pos)?;

        if pos >= buf.len() {
            return Err(StoreError::Corrupted(
                "truncated Tag 0x06 status bitmap count".into(),
            ));
        }
        let status_cnt = buf[pos] as usize;
        pos += 1;
        let mut status_bitmaps = BTreeMap::new();
        for _ in 0..status_cnt {
            let end_hdr = pos
                .checked_add(5)
                .ok_or_else(|| StoreError::Corrupted("status bitmap header overflow".into()))?;
            if end_hdr > buf.len() {
                return Err(StoreError::Corrupted(
                    "truncated Tag 0x06 status bitmap header".into(),
                ));
            }
            let st = buf[pos];
            let bm_len =
                u32::from_le_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
                    as usize;
            pos = end_hdr;
            let end_bm = pos
                .checked_add(bm_len)
                .ok_or(StoreError::ResourceLimitExceeded("roaring bitmap overflow"))?;
            if end_bm > buf.len() {
                return Err(StoreError::Corrupted(
                    "truncated Tag 0x06 status bitmap body".into(),
                ));
            }
            let bm = RoaringBitmap::deserialize_from(Cursor::new(&buf[pos..end_bm]))
                .map_err(|e| StoreError::Corrupted(format!("invalid roaring bitmap: {e}")))?;
            pos = end_bm;
            status_bitmaps.insert(st, bm);
        }

        if pos != buf.len() {
            return Err(StoreError::Corrupted(format!(
                "unexpected trailing bytes in Tag 0x06: {}",
                buf.len() - pos
            )));
        }

        Ok(Self {
            zone_map,
            trace_id_bloom,
            session_id_bloom,
            user_id_bloom,
            parent_trace_id_bloom,
            agent_name_bitmaps,
            agent_name_truncated: (flags & 0x01) != 0,
            model_bitmaps,
            model_truncated: (flags & 0x02) != 0,
            tool_name_bitmaps,
            tool_name_truncated: (flags & 0x04) != 0,
            environment_bitmaps,
            environment_truncated: (flags & 0x08) != 0,
            status_bitmaps,
        })
    }

    pub fn approx_bytes(&self) -> usize {
        let bloom_bytes = self.trace_id_bloom.serialized_len()
            + self.session_id_bloom.serialized_len()
            + self.user_id_bloom.serialized_len()
            + self.parent_trace_id_bloom.serialized_len();
        let dict_bytes: usize = self
            .agent_name_bitmaps
            .iter()
            .chain(self.model_bitmaps.iter())
            .chain(self.tool_name_bitmaps.iter())
            .chain(self.environment_bitmaps.iter())
            .map(|(k, bm)| 24 + k.len() + bm.serialized_size())
            .sum();
        let status_bytes: usize = self
            .status_bitmaps
            .values()
            .map(|bm| 8 + bm.serialized_size())
            .sum();
        128 + bloom_bytes + dict_bytes + status_bytes
    }
}

fn encode_str_bitmap_map(
    map: &BTreeMap<String, RoaringBitmap>,
    out: &mut Vec<u8>,
) -> StoreResult<()> {
    let count = u32::try_from(map.len())
        .map_err(|_| StoreError::ResourceLimitExceeded("dict bitmap count overflow"))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (k, bm) in map {
        let key_bytes = k.as_bytes();
        let key_len = u16::try_from(key_bytes.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("dict key exceeds 64 KiB"))?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(key_bytes);
        let mut bm_bytes = Vec::with_capacity(bm.serialized_size());
        bm.serialize_into(&mut bm_bytes)?;
        let bm_len = u32::try_from(bm_bytes.len())
            .map_err(|_| StoreError::ResourceLimitExceeded("roaring bitmap overflow"))?;
        out.extend_from_slice(&bm_len.to_le_bytes());
        out.extend_from_slice(&bm_bytes);
    }
    Ok(())
}

fn decode_str_bitmap_map(
    buf: &[u8],
    pos: &mut usize,
) -> StoreResult<BTreeMap<String, RoaringBitmap>> {
    let end_cnt = pos
        .checked_add(4)
        .ok_or_else(|| StoreError::Corrupted("dict count offset overflow".into()))?;
    if end_cnt > buf.len() {
        return Err(StoreError::Corrupted("truncated dict bitmap count".into()));
    }
    let count =
        u32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]) as usize;
    *pos = end_cnt;
    if count > MAX_TAG06_DICT_ENTRIES {
        return Err(StoreError::ResourceLimitExceeded(
            "dict bitmap entry count exceeds MAX_TAG06_DICT_ENTRIES",
        ));
    }
    let mut map = BTreeMap::new();
    for _ in 0..count {
        let end_klen = pos
            .checked_add(2)
            .ok_or_else(|| StoreError::Corrupted("dict key len offset overflow".into()))?;
        if end_klen > buf.len() {
            return Err(StoreError::Corrupted("truncated dict key length".into()));
        }
        let klen = u16::from_le_bytes([buf[*pos], buf[*pos + 1]]) as usize;
        *pos = end_klen;
        let end_key = pos
            .checked_add(klen)
            .ok_or_else(|| StoreError::Corrupted("dict key offset overflow".into()))?;
        if end_key > buf.len() {
            return Err(StoreError::Corrupted("truncated dict key bytes".into()));
        }
        let key = std::str::from_utf8(&buf[*pos..end_key])
            .map_err(|e| StoreError::Corrupted(format!("invalid UTF-8 in Tag 0x06 key: {e}")))?
            .to_string();
        *pos = end_key;

        let end_blen = pos
            .checked_add(4)
            .ok_or_else(|| StoreError::Corrupted("dict bitmap len offset overflow".into()))?;
        if end_blen > buf.len() {
            return Err(StoreError::Corrupted("truncated dict bitmap len".into()));
        }
        let blen =
            u32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]) as usize;
        *pos = end_blen;
        let end_bm = pos
            .checked_add(blen)
            .ok_or(StoreError::ResourceLimitExceeded("roaring bitmap overflow"))?;
        if end_bm > buf.len() {
            return Err(StoreError::Corrupted("truncated dict bitmap body".into()));
        }
        let bm = RoaringBitmap::deserialize_from(Cursor::new(&buf[*pos..end_bm]))
            .map_err(|e| StoreError::Corrupted(format!("invalid roaring bitmap: {e}")))?;
        *pos = end_bm;
        map.insert(key, bm);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{SpanPhase, SpanType};
    use serde_json::Value;

    fn make_trace(
        id: &str,
        agent: &str,
        env: &str,
        status: TraceStatus,
        model: &str,
    ) -> AgentTrace {
        AgentTrace {
            trace_id: id.to_string(),
            project_id: "proj_1".to_string(),
            agent_name: agent.to_string(),
            agent_version: "1.0.0".to_string(),
            prompt_version: None,
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: env.to_string(),
            session_id: Some(format!("sess_{id}")),
            user_id: Some(format!("user_{id}")),
            workflow_id: None,
            parent_trace_id: Some(format!("pt_{id}")),
            status,
            start_ts_ms: 1_700_000_000_000,
            end_ts_ms: Some(1_700_000_000_500),
            duration_ms: 500,
            input: Value::Null,
            output: Value::Null,
            error_type: None,
            error_message: None,
            failure_source: None,
            metadata: Value::Null,
            tags: vec![],
            span_count: 1,
            llm_call_count: 1,
            tool_call_count: 0,
            error_span_count: if status == TraceStatus::Error { 1 } else { 0 },
            input_tokens: 10,
            output_tokens: 20,
            cached_tokens: 0,
            total_tokens: 30,
            total_cost_nanos: 1_500_000,
            total_cost_usd: 0.0015,
            primary_model: Some(model.to_string()),
            primary_provider: Some("openai".to_string()),
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning: false,
        }
    }

    fn make_span(id: &str, trace_id: &str, model: Option<&str>, tool: Option<&str>) -> AgentSpan {
        AgentSpan {
            span_id: id.to_string(),
            trace_id: trace_id.to_string(),
            project_id: "proj_1".to_string(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: "step".to_string(),
            span_type: if tool.is_some() {
                SpanType::Tool
            } else {
                SpanType::Llm
            },
            status: TraceStatus::Ok,
            start_ts_ms: 1_700_000_000_000,
            end_ts_ms: Some(1_700_000_000_100),
            duration_ms: 100,
            provider: Some("openai".to_string()),
            model: model.map(str::to_string),
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 10,
            output_tokens: 20,
            cached_tokens: 0,
            total_tokens: 30,
            cost_nanos: 1_500_000,
            cost_usd: 0.0015,
            temperature: None,
            finish_reason: None,
            model_params: Value::Null,
            tool_name: tool.map(str::to_string),
            tool_version: None,
            tool_retry_count: 0,
            input: Value::Null,
            output: Value::Null,
            attributes: Value::Null,
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        }
    }

    #[test]
    fn split_block_bloom_zero_fn_and_under_1pct_fp() {
        let empty = SplitBlockBloom::with_capacity(0, DEFAULT_BLOOM_BITS_PER_KEY);
        assert_eq!(empty.num_blocks(), 0);
        assert!(!empty.may_contain(b"anything"));

        let mut bloom = SplitBlockBloom::with_capacity(10_000, DEFAULT_BLOOM_BITS_PER_KEY);
        for i in 0..10_000u64 {
            let k = format!("trace_key_{i:06}");
            bloom.insert(k.as_bytes());
        }
        // 0 false negatives
        for i in 0..10_000u64 {
            let k = format!("trace_key_{i:06}");
            assert!(bloom.may_contain(k.as_bytes()), "false negative at {i}");
        }
        // < 1% false positives over 100,000 probes
        let mut fp = 0usize;
        for i in 10_000..110_000u64 {
            let k = format!("other_key_{i:06}");
            if bloom.may_contain(k.as_bytes()) {
                fp += 1;
            }
        }
        let fp_rate = fp as f64 / 100_000.0;
        assert!(fp_rate < 0.01, "expected FP < 1%, got {fp_rate:.4}");

        // Wire roundtrip + oversize guard
        let mut encoded = Vec::new();
        bloom.encode_into(&mut encoded).unwrap();
        let mut pos = 0;
        let decoded = SplitBlockBloom::decode_from(&encoded, &mut pos).unwrap();
        assert_eq!(pos, encoded.len());
        assert_eq!(decoded, bloom);

        let oversized = (MAX_BLOOM_BLOCKS as u32 + 1).to_le_bytes();
        let mut p = 0;
        assert!(SplitBlockBloom::decode_from(&oversized, &mut p).is_err());

        // Passing bits_per_key = 10 is floored to MIN_BLOOM_BITS_PER_KEY = 12 so FP stays < 1%
        let mut floored = SplitBlockBloom::with_capacity(10_000, 10);
        assert_eq!(floored.num_blocks(), bloom.num_blocks());
        for i in 0..10_000u64 {
            floored.insert(format!("floored_key_{i:06}").as_bytes());
        }
        let mut fp_floored = 0usize;
        for i in 10_000..110_000u64 {
            if floored.may_contain(format!("miss_key_{i:06}").as_bytes()) {
                fp_floored += 1;
            }
        }
        let floored_fp_rate = fp_floored as f64 / 100_000.0;
        assert!(
            floored_fp_rate < 0.01,
            "expected floored FP < 1%, got {floored_fp_rate:.4}"
        );
    }

    #[test]
    fn tag_06_roaring_bitmaps_and_zone_maps() {
        let traces = vec![
            (
                make_trace("tr_0", "alpha", "production", TraceStatus::Ok, "gpt-4o"),
                vec![make_span(
                    "sp_0",
                    "tr_0",
                    Some("gpt-4o-mini"),
                    Some("search"),
                )],
            ),
            (
                make_trace(
                    "tr_1",
                    "alpha",
                    "staging",
                    TraceStatus::Error,
                    "claude-sonnet-4",
                ),
                vec![make_span("sp_1", "tr_1", None, Some("sql_exec"))],
            ),
            (
                make_trace("tr_2", "beta", "production", TraceStatus::Ok, "gpt-4o"),
                vec![make_span("sp_2", "tr_2", Some("gpt-4o"), None)],
            ),
        ];
        let idx = SegmentIndexSection::build_from_traces(&traces, DEFAULT_BLOOM_BITS_PER_KEY);
        let raw = idx.encode().unwrap();
        let decoded = SegmentIndexSection::decode(&raw).unwrap();
        assert_eq!(decoded, idx);

        assert!(decoded.may_contain_trace("tr_1"));
        assert!(decoded.may_contain_session("sess_tr_2"));
        assert!(decoded.may_contain_user("user_tr_0"));
        assert!(decoded.may_contain_parent_trace("pt_tr_1"));
        assert_eq!(decoded.zone_map.trace_count, 3);
        assert_eq!(decoded.zone_map.error_count, 1);

        let filter = TraceListFilter {
            agent_name: Some("alpha".into()),
            model: Some("gpt-4o-mini".into()),
            environment: Some("production".into()),
            ..Default::default()
        };
        let candidates = decoded.candidate_trace_indices(&filter).unwrap();
        assert_eq!(candidates.iter().collect::<Vec<_>>(), vec![0]);

        let tool_bm = decoded.candidate_traces_for_tool("sql_exec").unwrap();
        assert_eq!(tool_bm.iter().collect::<Vec<_>>(), vec![1]);

        let miss = TraceListFilter {
            agent_name: Some("nonexistent".into()),
            ..Default::default()
        };
        assert!(decoded.candidate_trace_indices(&miss).unwrap().is_empty());

        // Truncation fallback safety: missing key in truncated dict must NOT prune to empty
        let mut trunc_traces = Vec::with_capacity(MAX_TAG06_DICT_ENTRIES + 5);
        for i in 0..(MAX_TAG06_DICT_ENTRIES + 5) {
            trunc_traces.push((
                make_trace(
                    &format!("tr_{i}"),
                    &format!("agent_{i}"),
                    "production",
                    TraceStatus::Ok,
                    "gpt-4o",
                ),
                vec![],
            ));
        }
        let trunc_idx =
            SegmentIndexSection::build_from_traces(&trunc_traces, DEFAULT_BLOOM_BITS_PER_KEY);
        assert!(trunc_idx.agent_name_truncated);
        let fallback_filter = TraceListFilter {
            agent_name: Some(format!("agent_{}", MAX_TAG06_DICT_ENTRIES + 2)),
            ..Default::default()
        };
        assert!(trunc_idx
            .candidate_trace_indices(&fallback_filter)
            .is_none());
    }
}
