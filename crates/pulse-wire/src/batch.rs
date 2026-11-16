use std::collections::HashMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::error::{Result, WireError};
use crate::event::{PropValue, WireEvent};

pub const BATCH_MAGIC: [u8; 4] = *b"PLSB";
pub const WIRE_VERSION: u8 = 1;

pub const FLAG_RAW_ZSTD: u8 = 0x01;
pub const FLAG_DICT_ZSTD: u8 = 0x02;

pub const DEFAULT_DICT_ID: u16 = 1;

/// Maximum decompressed frame size (64 MiB) across all Zstd wire frames (`PLSB` & `PLSR`).
pub const MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
/// Maximum cumulative string bytes materialized when expanding string-table references in a batch (64 MiB).
pub const MAX_MATERIALIZED_STRING_BYTES: usize = 64 * 1024 * 1024;
/// Maximum byte length of a single string field on the wire (64 KiB).
pub const MAX_FIELD_STRING_BYTES: usize = 64 * 1024;
/// Maximum events permitted in a single `WireBatch`.
pub const MAX_BATCH_EVENTS: usize = 250_000;
/// Maximum pre-allocated `Vec` elements before verifying individual entries from the stream.
pub(crate) const MAX_INITIAL_VEC_CAP: usize = 65_536;

/// Shared bounded Zstd decoder preventing decompression-bomb memory exhaustion (`P0-05`).
pub fn decode_zstd_bounded(
    compressed: &[u8],
    dict: Option<&[u8]>,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    fn drain_bounded<R: Read>(mut reader: R, max_bytes: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut scratch = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut scratch)?;
            if n == 0 {
                break;
            }
            if out.len().saturating_add(n) > max_bytes {
                return Err(WireError::ResourceLimitExceeded(
                    "decompressed zstd payload exceeds 64 MiB limit",
                ));
            }
            if out.capacity() < out.len() + n {
                let target_cap = (out.capacity().max(64 * 1024) * 2)
                    .max(out.len() + n)
                    .min(max_bytes);
                out.reserve_exact(target_cap - out.capacity());
            }
            out.extend_from_slice(&scratch[..n]);
        }
        Ok(out)
    }

    match dict {
        Some(d) => drain_bounded(zstd::Decoder::with_dictionary(compressed, d)?, max_bytes),
        None => drain_bounded(zstd::Decoder::new(compressed)?, max_bytes),
    }
}

/// Registry of pre-shared Zstd dictionaries used by SDKs and edge gateways.
#[derive(Debug, Clone)]
pub struct DictionaryRegistry {
    dicts: HashMap<u16, Vec<u8>>,
}

impl Default for DictionaryRegistry {
    fn default() -> Self {
        let mut reg = Self {
            dicts: HashMap::new(),
        };
        // Pre-shared baseline telemetry dictionary containing common mobile/web analytics keys & tokens.
        let preset_tokens = b"App_Open\x00Session_Start\x00KYC_PAN_Submitted\x00Video_KYC_Completed\x00\
            UPI_Autopay_Mandate_Approved\x00First_Disbursal_Success\x00Checkout_Started\x00Payment_Success\x00\
            NSDL_Gateway_504_Timeout\x00OTP_Verified\x00Browse_Credit_Card\x00View_Loan_Calculator\x00\
            amount\x00currency\x00INR\x00platform\x00Android\x00iOS\x00Web\x00network\x00Jio_4G\x00Airtel_5G\x00\
            app_version\x00city\x00Bengaluru\x00Mumbai\x00Delhi\x00Hyderabad\x00Pune\x00Chennai\x00\
            payment_method\x00upi\x00card\x00netbanking\x00merchant_id\x00sku_id\x00$exp_\x00control\x00treatment";
        reg.register(DEFAULT_DICT_ID, preset_tokens.to_vec());
        reg
    }
}

impl DictionaryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, id: u16, dict_bytes: Vec<u8>) {
        self.dicts.insert(id, dict_bytes);
    }

    pub fn train_and_register(
        &mut self,
        id: u16,
        samples: &[Vec<u8>],
        max_size: usize,
    ) -> Result<()> {
        let dict = zstd::dict::from_samples(samples, max_size)?;
        self.dicts.insert(id, dict);
        Ok(())
    }

    pub fn get(&self, id: u16) -> Option<&[u8]> {
        self.dicts.get(&id).map(|v| v.as_slice())
    }
}

/// Columnar batch container for analytical events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireBatch {
    pub events: Vec<WireEvent>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JsonBatchEnvelope {
    Wrapped { events: Vec<WireEvent> },
    Bare(Vec<WireEvent>),
    Single(WireEvent),
}

impl WireBatch {
    pub fn new(events: Vec<WireEvent>) -> Self {
        Self { events }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Serializes the batch as standard UTF-8 JSON (`{"events": [...]}`).
    pub fn encode_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserializes a JSON payload accepting `{"events":[...]}`, `[...]`, or a single event `{...}`.
    pub fn decode_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_DECOMPRESSED_BYTES {
            return Err(WireError::ResourceLimitExceeded(
                "json batch exceeds 64 MiB limit",
            ));
        }
        let env: JsonBatchEnvelope = serde_json::from_slice(bytes)?;
        let events = match env {
            JsonBatchEnvelope::Wrapped { events } => events,
            JsonBatchEnvelope::Bare(events) => events,
            JsonBatchEnvelope::Single(ev) => vec![ev],
        };
        if events.len() > MAX_BATCH_EVENTS {
            return Err(WireError::ResourceLimitExceeded(
                "json batch event count exceeds limit",
            ));
        }
        Ok(Self { events })
    }

    /// Encodes the batch into Pulse's `PLSB` columnar binary layout compressed with raw Zstd (`flags = 0x01`).
    pub fn encode_binary(&self, zstd_level: i32) -> Result<Vec<u8>> {
        let uncompressed = self.encode_columnar_payload();
        let compressed = zstd::stream::encode_all(uncompressed.as_slice(), zstd_level)?;

        let mut out = Vec::with_capacity(6 + compressed.len());
        out.extend_from_slice(&BATCH_MAGIC);
        out.push(WIRE_VERSION);
        out.push(FLAG_RAW_ZSTD);
        out.extend_from_slice(&compressed);
        Ok(out)
    }

    /// Encodes the batch into `PLSB` columnar binary layout with a pre-shared Zstd dictionary (`flags = 0x02`).
    pub fn encode_binary_with_dict(
        &self,
        dict_id: u16,
        registry: &DictionaryRegistry,
        zstd_level: i32,
    ) -> Result<Vec<u8>> {
        let dict = registry
            .get(dict_id)
            .ok_or(WireError::UnknownDictionary(dict_id))?;
        let uncompressed = self.encode_columnar_payload();

        let mut compressed = Vec::new();
        {
            let mut encoder = zstd::Encoder::with_dictionary(&mut compressed, zstd_level, dict)?;
            encoder.write_all(&uncompressed)?;
            encoder.finish()?;
        }

        let mut out = Vec::with_capacity(8 + compressed.len());
        out.extend_from_slice(&BATCH_MAGIC);
        out.push(WIRE_VERSION);
        out.push(FLAG_DICT_ZSTD);
        out.extend_from_slice(&dict_id.to_le_bytes());
        out.extend_from_slice(&compressed);
        Ok(out)
    }

    /// Decodes a `PLSB` binary frame using the default dictionary registry.
    pub fn decode_binary(bytes: &[u8]) -> Result<Self> {
        Self::decode_binary_with_registry(bytes, &DictionaryRegistry::default())
    }

    /// Decodes a `PLSB` binary frame with an explicit dictionary registry.
    pub fn decode_binary_with_registry(
        bytes: &[u8],
        registry: &DictionaryRegistry,
    ) -> Result<Self> {
        if bytes.len() < 6 {
            return Err(WireError::TruncatedFrame("PLSB header < 6 bytes"));
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if magic != BATCH_MAGIC {
            return Err(WireError::InvalidMagic {
                expected: BATCH_MAGIC,
                actual: magic,
            });
        }

        let version = bytes[4];
        if version != WIRE_VERSION {
            return Err(WireError::UnsupportedVersion(version));
        }

        let flags = bytes[5];
        let decompressed = match flags {
            FLAG_RAW_ZSTD => decode_zstd_bounded(&bytes[6..], None, MAX_DECOMPRESSED_BYTES)?,
            FLAG_DICT_ZSTD => {
                if bytes.len() < 8 {
                    return Err(WireError::TruncatedFrame("missing dictionary id"));
                }
                let dict_id = u16::from_le_bytes([bytes[6], bytes[7]]);
                let dict = registry
                    .get(dict_id)
                    .ok_or(WireError::UnknownDictionary(dict_id))?;
                decode_zstd_bounded(&bytes[8..], Some(dict), MAX_DECOMPRESSED_BYTES)?
            }
            other => return Err(WireError::UnsupportedFlags(other)),
        };

        Self::decode_columnar_payload(&decompressed)
    }

    /// Auto-detects `PLSB` binary vs JSON payload based on Content-Type or magic bytes.
    pub fn decode_auto(
        bytes: &[u8],
        content_type: Option<&str>,
        registry: &DictionaryRegistry,
    ) -> Result<Self> {
        let is_binary = match content_type {
            Some(ct) if ct.contains("application/x-pulse-frame") => true,
            Some(ct) if ct.contains("application/json") => false,
            _ => bytes.len() >= 4 && bytes[0..4] == BATCH_MAGIC,
        };
        if is_binary {
            Self::decode_binary_with_registry(bytes, registry)
        } else {
            Self::decode_json(bytes)
        }
    }

    fn encode_columnar_payload<'a>(&'a self) -> Vec<u8> {
        let n = self.events.len();

        // 1. Deduplicated batch string dictionary
        let mut str_to_id: HashMap<&'a str, u32> = HashMap::with_capacity(n * 2 + 32);
        let mut str_blob = Vec::with_capacity(n * 32);
        let mut str_offsets: Vec<u32> = Vec::with_capacity(n * 2 + 33);
        str_offsets.push(0);

        let mut intern = |s: &'a str| -> u32 {
            if let Some(&id) = str_to_id.get(s) {
                id
            } else {
                let id = (str_offsets.len() - 1) as u32;
                str_blob.extend_from_slice(s.as_bytes());
                str_offsets.push(str_blob.len() as u32);
                str_to_id.insert(s, id);
                id
            }
        };

        // 2. Columnar fixed-length & typed vectors
        let mut uuids = Vec::with_capacity(n * 16);
        let mut routing_ids = Vec::with_capacity(n);
        let mut distinct_ids = Vec::with_capacity(n);
        let mut session_ids = Vec::with_capacity(n);
        let mut event_name_ids = Vec::with_capacity(n);
        let mut event_ts_deltas = Vec::with_capacity(n);
        let mut sent_ts_deltas = Vec::with_capacity(n);
        let mut replay_offsets = Vec::with_capacity(n);

        let mut prop_counts: Vec<u16> = Vec::with_capacity(n);
        let mut prop_key_ids: Vec<u32> = Vec::with_capacity(n * 4);
        let mut prop_tags: Vec<u8> = Vec::with_capacity(n * 4);
        let mut bool_vals: Vec<u8> = Vec::new();
        let mut int_vals: Vec<i64> = Vec::new();
        let mut float_bits: Vec<u64> = Vec::new();
        let mut str_val_ids: Vec<u32> = Vec::new();

        let mut prev_event_ts: i64 = 0;
        let mut prev_sent_ts: i64 = 0;

        for ev in &self.events {
            uuids.extend_from_slice(&ev.event_uuid);
            routing_ids.push(intern(&ev.routing_seed));
            distinct_ids.push(intern(&ev.distinct_id));
            session_ids.push(intern(&ev.session_id));
            event_name_ids.push(intern(&ev.event_name));

            event_ts_deltas.push(ev.client_event_ts_ms.wrapping_sub(prev_event_ts));
            prev_event_ts = ev.client_event_ts_ms;

            sent_ts_deltas.push(ev.client_sent_ts_ms.wrapping_sub(prev_sent_ts));
            prev_sent_ts = ev.client_sent_ts_ms;

            replay_offsets.push(ev.replay_offset_kb);

            let p_count = ev.props.len().min(u16::MAX as usize) as u16;
            prop_counts.push(p_count);
            for (k, v) in ev.props.iter().take(p_count as usize) {
                prop_key_ids.push(intern(k));
                prop_tags.push(v.tag());
                match v {
                    PropValue::Null => {}
                    PropValue::Bool(b) => bool_vals.push(u8::from(*b)),
                    PropValue::Int(i) => int_vals.push(*i),
                    PropValue::Float(f) => float_bits.push(f.to_bits()),
                    PropValue::Str(s) => str_val_ids.push(intern(s)),
                }
            }
        }

        let unique_strings = (str_offsets.len() - 1) as u32;
        let total_props = prop_tags.len() as u32;

        let mut buf = Vec::with_capacity(64 + str_blob.len() + n * 48 + (total_props as usize) * 8);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        buf.extend_from_slice(&unique_strings.to_le_bytes());
        for &off in &str_offsets {
            buf.extend_from_slice(&off.to_le_bytes());
        }
        buf.extend_from_slice(&str_blob);

        // Write columnar arrays
        buf.extend_from_slice(&uuids);
        write_u32_slice(&mut buf, &routing_ids);
        write_u32_slice(&mut buf, &distinct_ids);
        write_u32_slice(&mut buf, &session_ids);
        write_u32_slice(&mut buf, &event_name_ids);
        write_i64_slice(&mut buf, &event_ts_deltas);
        write_i64_slice(&mut buf, &sent_ts_deltas);
        write_u32_slice(&mut buf, &replay_offsets);

        write_u16_slice(&mut buf, &prop_counts);
        buf.extend_from_slice(&total_props.to_le_bytes());
        write_u32_slice(&mut buf, &prop_key_ids);
        buf.extend_from_slice(&prop_tags);

        buf.extend_from_slice(&(bool_vals.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bool_vals);

        buf.extend_from_slice(&(int_vals.len() as u32).to_le_bytes());
        write_i64_slice(&mut buf, &int_vals);

        buf.extend_from_slice(&(float_bits.len() as u32).to_le_bytes());
        write_u64_slice(&mut buf, &float_bits);

        buf.extend_from_slice(&(str_val_ids.len() as u32).to_le_bytes());
        write_u32_slice(&mut buf, &str_val_ids);

        buf
    }

    fn decode_columnar_payload(bytes: &[u8]) -> Result<Self> {
        let mut cursor = ByteCursor::new(bytes);
        let n = cursor.read_u32("event count")? as usize;
        if n > MAX_BATCH_EVENTS {
            return Err(WireError::ResourceLimitExceeded(
                "batch event count exceeds MAX_BATCH_EVENTS",
            ));
        }
        let unique_strings = cursor.read_u32("unique string count")? as usize;
        let offset_count = unique_strings
            .checked_add(1)
            .ok_or(WireError::TruncatedFrame("string offset count overflow"))?;

        let offsets = cursor.read_u32_vec(offset_count, "string offsets")?;
        let blob_len = *offsets
            .last()
            .ok_or(WireError::TruncatedFrame("empty string offset table"))?
            as usize;
        let str_blob = cursor.read_slice(blob_len, "string blob")?;

        let mut strings: Vec<&str> = Vec::with_capacity(unique_strings.min(MAX_INITIAL_VEC_CAP));
        for i in 0..unique_strings {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            if end < start || end > str_blob.len() {
                return Err(WireError::TruncatedFrame("invalid string offset bounds"));
            }
            if end - start > MAX_FIELD_STRING_BYTES {
                return Err(WireError::ResourceLimitExceeded(
                    "string table entry exceeds 64 KiB field limit",
                ));
            }
            let s = std::str::from_utf8(&str_blob[start..end]).map_err(|e| {
                WireError::TruncatedFrame(if e.error_len().is_some() {
                    "invalid utf8 in string table"
                } else {
                    "truncated utf8 in string table"
                })
            })?;
            strings.push(s);
        }

        let uuid_bytes = cursor.read_slice(
            n.checked_mul(16)
                .ok_or(WireError::TruncatedFrame("uuid length overflow"))?,
            "uuids",
        )?;
        let routing_ids = cursor.read_u32_vec(n, "routing_ids")?;
        let distinct_ids = cursor.read_u32_vec(n, "distinct_ids")?;
        let session_ids = cursor.read_u32_vec(n, "session_ids")?;
        let event_name_ids = cursor.read_u32_vec(n, "event_name_ids")?;
        let event_ts_deltas = cursor.read_i64_vec(n, "event_ts_deltas")?;
        let sent_ts_deltas = cursor.read_i64_vec(n, "sent_ts_deltas")?;
        let replay_offsets = cursor.read_u32_vec(n, "replay_offsets")?;

        let prop_counts = cursor.read_u16_vec(n, "prop_counts")?;
        let total_props = cursor.read_u32("total_props")? as usize;
        let prop_key_ids = cursor.read_u32_vec(total_props, "prop_key_ids")?;
        let prop_tags = cursor.read_slice(total_props, "prop_tags")?;

        let bool_len = cursor.read_u32("bool_len")? as usize;
        let bool_vals = cursor.read_slice(bool_len, "bool_vals")?;

        let int_len = cursor.read_u32("int_len")? as usize;
        let int_vals = cursor.read_i64_vec(int_len, "int_vals")?;

        let float_len = cursor.read_u32("float_len")? as usize;
        let float_bits = cursor.read_u64_vec(float_len, "float_bits")?;

        let str_val_len = cursor.read_u32("str_val_len")? as usize;
        let str_val_ids = cursor.read_u32_vec(str_val_len, "str_val_ids")?;

        let mut materialized_str_bytes = 0usize;
        let mut take_str = |idx: u32| -> Result<String> {
            let s = *strings.get(idx as usize).ok_or(WireError::TruncatedFrame(
                "string table index out of bounds",
            ))?;
            materialized_str_bytes = materialized_str_bytes.saturating_add(s.len());
            if materialized_str_bytes > MAX_MATERIALIZED_STRING_BYTES {
                return Err(WireError::ResourceLimitExceeded(
                    "materialized batch string bytes exceed 64 MiB limit",
                ));
            }
            Ok(s.to_owned())
        };

        let mut events = Vec::with_capacity(n.min(MAX_INITIAL_VEC_CAP));
        let mut prev_event_ts: i64 = 0;
        let mut prev_sent_ts: i64 = 0;

        let mut p_idx = 0usize;
        let mut b_idx = 0usize;
        let mut i_idx = 0usize;
        let mut f_idx = 0usize;
        let mut s_idx = 0usize;

        for row in 0..n {
            let mut event_uuid = [0u8; 16];
            event_uuid.copy_from_slice(&uuid_bytes[row * 16..(row + 1) * 16]);

            let client_event_ts_ms = prev_event_ts.wrapping_add(event_ts_deltas[row]);
            prev_event_ts = client_event_ts_ms;

            let client_sent_ts_ms = prev_sent_ts.wrapping_add(sent_ts_deltas[row]);
            prev_sent_ts = client_sent_ts_ms;

            let p_count = prop_counts[row] as usize;
            if p_idx + p_count > total_props {
                return Err(WireError::TruncatedFrame("prop count exceeds total_props"));
            }
            let mut props = Vec::with_capacity(p_count.min(256));
            for _ in 0..p_count {
                let key = take_str(prop_key_ids[p_idx])?;
                let tag = prop_tags[p_idx];
                p_idx += 1;

                let val = match tag {
                    PropValue::TAG_NULL => PropValue::Null,
                    PropValue::TAG_BOOL => {
                        let b = *bool_vals
                            .get(b_idx)
                            .ok_or(WireError::TruncatedFrame("bool_vals underflow"))?;
                        b_idx += 1;
                        PropValue::Bool(b != 0)
                    }
                    PropValue::TAG_INT => {
                        let v = *int_vals
                            .get(i_idx)
                            .ok_or(WireError::TruncatedFrame("int_vals underflow"))?;
                        i_idx += 1;
                        PropValue::Int(v)
                    }
                    PropValue::TAG_FLOAT => {
                        let bits = *float_bits
                            .get(f_idx)
                            .ok_or(WireError::TruncatedFrame("float_bits underflow"))?;
                        f_idx += 1;
                        PropValue::Float(f64::from_bits(bits))
                    }
                    PropValue::TAG_STR => {
                        let sid = *str_val_ids
                            .get(s_idx)
                            .ok_or(WireError::TruncatedFrame("str_val_ids underflow"))?;
                        s_idx += 1;
                        PropValue::Str(take_str(sid)?)
                    }
                    other => return Err(WireError::InvalidPropTag(other)),
                };
                props.push((key, val));
            }

            let routing_seed = take_str(routing_ids[row])?;
            let distinct_id = take_str(distinct_ids[row])?;
            let session_id = take_str(session_ids[row])?;
            let event_name = take_str(event_name_ids[row])?;

            events.push(WireEvent {
                event_uuid,
                routing_seed,
                distinct_id,
                session_id,
                event_name,
                client_event_ts_ms,
                client_sent_ts_ms,
                props,
                replay_offset_kb: replay_offsets[row],
            });
        }

        Ok(Self { events })
    }
}

#[inline]
fn write_u16_slice(out: &mut Vec<u8>, vals: &[u16]) {
    out.reserve(vals.len() * 2);
    for &v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

#[inline]
fn write_u32_slice(out: &mut Vec<u8>, vals: &[u32]) {
    out.reserve(vals.len() * 4);
    for &v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

#[inline]
fn write_i64_slice(out: &mut Vec<u8>, vals: &[i64]) {
    out.reserve(vals.len() * 8);
    for &v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

#[inline]
fn write_u64_slice(out: &mut Vec<u8>, vals: &[u64]) {
    out.reserve(vals.len() * 8);
    for &v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

pub(crate) struct ByteCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ByteCursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn read_u8(&mut self, ctx: &'static str) -> Result<u8> {
        let s = self.read_slice(1, ctx)?;
        Ok(s[0])
    }

    pub fn read_u16(&mut self, ctx: &'static str) -> Result<u16> {
        let s = self.read_slice(2, ctx)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }

    pub fn read_u32(&mut self, ctx: &'static str) -> Result<u32> {
        let s = self.read_slice(4, ctx)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    pub fn read_i64(&mut self, ctx: &'static str) -> Result<i64> {
        let s = self.read_slice(8, ctx)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(s);
        Ok(i64::from_le_bytes(arr))
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn bounded_capacity(&self, count: usize, min_elem_wire_bytes: usize) -> usize {
        let max_by_bytes = self.remaining() / min_elem_wire_bytes.max(1);
        count.min(max_by_bytes).min(MAX_INITIAL_VEC_CAP)
    }

    pub fn read_string(&mut self, ctx: &'static str) -> Result<String> {
        let len = self.read_u32(ctx)? as usize;
        if len > MAX_FIELD_STRING_BYTES {
            return Err(WireError::ResourceLimitExceeded(
                "wire string field exceeds 64 KiB limit",
            ));
        }
        let bytes = self.read_slice(len, ctx)?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    pub fn read_slice(&mut self, len: usize, ctx: &'static str) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(WireError::TruncatedFrame(ctx))?;
        if end > self.data.len() {
            return Err(WireError::TruncatedFrame(ctx));
        }
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    pub fn read_u16_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u16>> {
        let bytes = self.read_slice(
            count.checked_mul(2).ok_or(WireError::TruncatedFrame(ctx))?,
            ctx,
        )?;
        let mut out = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(2) {
            out.push(u16::from_le_bytes([chunk[0], chunk[1]]));
        }
        Ok(out)
    }

    pub fn read_u32_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u32>> {
        let bytes = self.read_slice(
            count.checked_mul(4).ok_or(WireError::TruncatedFrame(ctx))?,
            ctx,
        )?;
        let mut out = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(4) {
            out.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(out)
    }

    pub fn read_i64_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<i64>> {
        let bytes = self.read_slice(
            count.checked_mul(8).ok_or(WireError::TruncatedFrame(ctx))?,
            ctx,
        )?;
        let mut out = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(8) {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(chunk);
            out.push(i64::from_le_bytes(arr));
        }
        Ok(out)
    }

    pub fn read_u64_vec(&mut self, count: usize, ctx: &'static str) -> Result<Vec<u64>> {
        let bytes = self.read_slice(
            count.checked_mul(8).ok_or(WireError::TruncatedFrame(ctx))?,
            ctx,
        )?;
        let mut out = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(8) {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(chunk);
            out.push(u64::from_le_bytes(arr));
        }
        Ok(out)
    }
}
