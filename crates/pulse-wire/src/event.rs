use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

/// Polymorphic property value carried inside an analytical event.
#[derive(Debug, Clone, PartialEq)]
pub enum PropValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl PropValue {
    pub const TAG_NULL: u8 = 0;
    pub const TAG_BOOL: u8 = 1;
    pub const TAG_INT: u8 = 2;
    pub const TAG_FLOAT: u8 = 3;
    pub const TAG_STR: u8 = 4;

    #[inline]
    pub fn tag(&self) -> u8 {
        match self {
            PropValue::Null => Self::TAG_NULL,
            PropValue::Bool(_) => Self::TAG_BOOL,
            PropValue::Int(_) => Self::TAG_INT,
            PropValue::Float(_) => Self::TAG_FLOAT,
            PropValue::Str(_) => Self::TAG_STR,
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            PropValue::Int(v) => Some(*v as f64),
            PropValue::Float(v) => Some(*v),
            PropValue::Str(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str_cow(&self) -> std::borrow::Cow<'_, str> {
        match self {
            PropValue::Null => std::borrow::Cow::Borrowed("null"),
            PropValue::Bool(true) => std::borrow::Cow::Borrowed("true"),
            PropValue::Bool(false) => std::borrow::Cow::Borrowed("false"),
            PropValue::Int(v) => std::borrow::Cow::Owned(v.to_string()),
            PropValue::Float(v) => std::borrow::Cow::Owned(v.to_string()),
            PropValue::Str(s) => std::borrow::Cow::Borrowed(s.as_str()),
        }
    }
}

impl Serialize for PropValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            PropValue::Null => serializer.serialize_none(),
            PropValue::Bool(b) => serializer.serialize_bool(*b),
            PropValue::Int(i) => serializer.serialize_i64(*i),
            PropValue::Float(f) => serializer.serialize_f64(*f),
            PropValue::Str(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for PropValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let val = serde_json::Value::deserialize(deserializer)?;
        Ok(Self::from_json_value(val))
    }
}

impl PropValue {
    pub fn from_json_value(val: serde_json::Value) -> Self {
        match val {
            serde_json::Value::Null => PropValue::Null,
            serde_json::Value::Bool(b) => PropValue::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    PropValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    if u <= i64::MAX as u64 {
                        PropValue::Int(u as i64)
                    } else {
                        PropValue::Float(u as f64)
                    }
                } else if let Some(f) = n.as_f64() {
                    PropValue::Float(f)
                } else {
                    PropValue::Null
                }
            }
            serde_json::Value::String(s) => PropValue::Str(s),
            other => PropValue::Str(other.to_string()),
        }
    }
}

/// Canonical Analytical Event Envelope (`WireEvent`).
#[derive(Debug, Clone, PartialEq)]
pub struct WireEvent {
    /// 128-bit deterministic client hash for idempotent deduplication.
    pub event_uuid: [u8; 16],
    /// Immutable `device_install_id` so anonymous and post-login events route to the same bucket.
    pub routing_seed: String,
    /// Current external identifier (`anon_id` or logged-in `user_id`).
    pub distinct_id: String,
    /// Client session identifier.
    pub session_id: String,
    /// Event name (e.g., `"UPI_Autopay_Mandate_Approved"`).
    pub event_name: String,
    /// Client-side event occurrence timestamp (epoch ms).
    pub client_event_ts_ms: i64,
    /// Client-side batch transmission timestamp (epoch ms).
    pub client_sent_ts_ms: i64,
    /// Ordered polymorphic event properties.
    pub props: Vec<(String, PropValue)>,
    /// Approximate byte offset (in KiB) into the session's `.plsr` replay object.
    pub replay_offset_kb: u32,
}

impl WireEvent {
    /// Computes a deterministic 128-bit `event_uuid` from key event fields and salt.
    pub fn deterministic_uuid(
        distinct_id: &str,
        session_id: &str,
        event_name: &str,
        client_event_ts_ms: i64,
        seq_salt: u64,
    ) -> [u8; 16] {
        let mut buf =
            Vec::with_capacity(distinct_id.len() + session_id.len() + event_name.len() + 24);
        buf.extend_from_slice(distinct_id.as_bytes());
        buf.push(0x1f);
        buf.extend_from_slice(session_id.as_bytes());
        buf.push(0x1f);
        buf.extend_from_slice(event_name.as_bytes());
        buf.extend_from_slice(&client_event_ts_ms.to_le_bytes());
        buf.extend_from_slice(&seq_salt.to_le_bytes());
        xxh3_128(&buf).to_le_bytes()
    }

    /// Computes a canonical 128-bit content hash over all event fields AND sorted properties (`P1-14`)
    /// so JSON events without a client-supplied `event_uuid` never collide across distinct payloads.
    pub fn canonical_content_uuid(
        distinct_id: &str,
        session_id: &str,
        event_name: &str,
        client_event_ts_ms: i64,
        replay_offset_kb: u32,
        props: &[(String, PropValue)],
    ) -> [u8; 16] {
        let mut sorted_indices: Vec<usize> = (0..props.len()).collect();
        sorted_indices.sort_by(|&a, &b| props[a].0.cmp(&props[b].0));

        let mut buf =
            Vec::with_capacity(distinct_id.len() + session_id.len() + event_name.len() + 64);
        buf.extend_from_slice(&(distinct_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(distinct_id.as_bytes());
        buf.extend_from_slice(&(session_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(session_id.as_bytes());
        buf.extend_from_slice(&(event_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(event_name.as_bytes());
        buf.extend_from_slice(&client_event_ts_ms.to_le_bytes());
        buf.extend_from_slice(&replay_offset_kb.to_le_bytes());
        buf.extend_from_slice(&(props.len() as u32).to_le_bytes());
        for idx in sorted_indices {
            let (k, v) = &props[idx];
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k.as_bytes());
            buf.push(v.tag());
            match v {
                PropValue::Null => {}
                PropValue::Bool(b) => buf.push(u8::from(*b)),
                PropValue::Int(i) => buf.extend_from_slice(&i.to_le_bytes()),
                PropValue::Float(f) => buf.extend_from_slice(&f.to_bits().to_le_bytes()),
                PropValue::Str(s) => {
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
            }
        }
        xxh3_128(&buf).to_le_bytes()
    }

    pub fn get_prop(&self, key: &str) -> Option<&PropValue> {
        self.props.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

#[derive(Serialize)]
struct WireEventJsonOut<'a> {
    event_uuid: String,
    routing_seed: &'a str,
    distinct_id: &'a str,
    session_id: &'a str,
    event_name: &'a str,
    client_event_ts_ms: i64,
    client_sent_ts_ms: i64,
    props: BTreeMap<&'a str, &'a PropValue>,
    replay_offset_kb: u32,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EventUuidInput {
    Str(String),
    Bytes([u8; 16]),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PropsInput {
    Map(serde_json::Map<String, serde_json::Value>),
    Pairs(Vec<(String, PropValue)>),
}

#[derive(Deserialize)]
struct WireEventJsonIn {
    event_uuid: Option<EventUuidInput>,
    routing_seed: Option<String>,
    distinct_id: String,
    #[serde(default)]
    session_id: String,
    event_name: String,
    client_event_ts_ms: i64,
    client_sent_ts_ms: Option<i64>,
    #[serde(default)]
    props: Option<PropsInput>,
    #[serde(default)]
    replay_offset_kb: u32,
}

impl Serialize for WireEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = BTreeMap::new();
        for (k, v) in &self.props {
            map.insert(k.as_str(), v);
        }
        let out = WireEventJsonOut {
            event_uuid: Uuid::from_bytes(self.event_uuid).to_string(),
            routing_seed: &self.routing_seed,
            distinct_id: &self.distinct_id,
            session_id: &self.session_id,
            event_name: &self.event_name,
            client_event_ts_ms: self.client_event_ts_ms,
            client_sent_ts_ms: self.client_sent_ts_ms,
            props: map,
            replay_offset_kb: self.replay_offset_kb,
        };
        out.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WireEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let input = WireEventJsonIn::deserialize(deserializer)?;
        let routing_seed = input
            .routing_seed
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| input.distinct_id.clone());
        let session_id = if input.session_id.is_empty() {
            format!("sess_{}", input.distinct_id)
        } else {
            input.session_id
        };
        // `0` indicates no client transmission timestamp was supplied (e.g., server-side or historical backfill JSON) (`P1-13`).
        let client_sent_ts_ms = input.client_sent_ts_ms.unwrap_or(0);

        let props = match input.props {
            Some(PropsInput::Map(m)) => m
                .into_iter()
                .map(|(k, v)| (k, PropValue::from_json_value(v)))
                .collect(),
            Some(PropsInput::Pairs(p)) => p,
            None => Vec::new(),
        };

        let event_uuid = match input.event_uuid {
            Some(EventUuidInput::Bytes(b)) => b,
            Some(EventUuidInput::Str(s)) => {
                if let Ok(u) = Uuid::parse_str(&s) {
                    *u.as_bytes()
                } else {
                    xxh3_128(s.as_bytes()).to_le_bytes()
                }
            }
            None => Self::canonical_content_uuid(
                &input.distinct_id,
                &session_id,
                &input.event_name,
                input.client_event_ts_ms,
                input.replay_offset_kb,
                &props,
            ),
        };

        Ok(WireEvent {
            event_uuid,
            routing_seed,
            distinct_id: input.distinct_id,
            session_id,
            event_name: input.event_name,
            client_event_ts_ms: input.client_event_ts_ms,
            client_sent_ts_ms,
            props,
            replay_offset_kb: input.replay_offset_kb,
        })
    }
}
