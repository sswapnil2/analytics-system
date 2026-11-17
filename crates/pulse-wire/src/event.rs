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

fn parse_timestamp_ms(val: &serde_json::Value) -> Option<i64> {
    match val {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if (1..100_000_000_000).contains(&i) {
                    Some(i.saturating_mul(1000))
                } else {
                    Some(i)
                }
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    None
                } else if f > 0.0 && f < 100_000_000_000.0 {
                    Some((f * 1000.0).round() as i64)
                } else {
                    Some(f.round() as i64)
                }
            } else {
                None
            }
        }
        serde_json::Value::String(s) => {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                if (1..100_000_000_000).contains(&i) {
                    Some(i.saturating_mul(1000))
                } else {
                    Some(i)
                }
            } else {
                chrono::DateTime::parse_from_rfc3339(t)
                    .ok()
                    .map(|dt| dt.timestamp_millis())
            }
        }
        _ => None,
    }
}

fn non_empty_str<'a>(
    map: &'a serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<&'a str> {
    for k in keys {
        if let Some(s) = map.get(*k).and_then(|v| v.as_str()) {
            let t = s.trim();
            if !t.is_empty() {
                return Some(t);
            }
        }
    }
    None
}

impl<'de> Deserialize<'de> for WireEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let mut obj = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;

        // Support Pulse native (`event_name`), Segment (`event`), or generic (`name`) (`PR-8`)
        let event_name = non_empty_str(&obj, &["event_name", "event", "name"])
            .ok_or_else(|| {
                serde::de::Error::custom("missing event name: provide \"event_name\" or \"event\"")
            })?
            .to_string();

        // Parse properties (`props` or Segment `properties`) before falling back for IDs
        let props_val = obj.remove("props").or_else(|| obj.remove("properties"));
        let nested_props_map = match &props_val {
            Some(serde_json::Value::Object(m)) => Some(m),
            _ => None,
        };

        let distinct_id = non_empty_str(
            &obj,
            &[
                "distinct_id",
                "user_id",
                "userId",
                "anonymous_id",
                "anonymousId",
                "anon_id",
            ],
        )
        .or_else(|| {
            nested_props_map.and_then(|m| {
                non_empty_str(
                    m,
                    &[
                        "distinct_id",
                        "user_id",
                        "userId",
                        "anonymous_id",
                        "anonymousId",
                    ],
                )
            })
        })
        .ok_or_else(|| {
            serde::de::Error::custom(
                "missing user identifier: provide \"distinct_id\", \"user_id\", or \"anonymous_id\"",
            )
        })?
        .to_string();

        let routing_seed = non_empty_str(
            &obj,
            &["routing_seed", "device_id", "anonymous_id", "anonymousId"],
        )
        .map(str::to_string)
        .unwrap_or_else(|| distinct_id.clone());

        let session_id = non_empty_str(&obj, &["session_id", "sessionId"])
            .or_else(|| {
                nested_props_map.and_then(|m| non_empty_str(m, &["session_id", "sessionId"]))
            })
            .map(str::to_string)
            .unwrap_or_else(|| format!("sess_{distinct_id}"));

        let client_event_ts_ms = ["client_event_ts_ms", "timestamp_ms", "timestamp", "time"]
            .iter()
            .find_map(|k| obj.get(*k).and_then(parse_timestamp_ms))
            .or_else(|| nested_props_map.and_then(|m| m.get("time").and_then(parse_timestamp_ms)))
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        let client_sent_ts_ms = ["client_sent_ts_ms", "sent_at", "sentAt"]
            .iter()
            .find_map(|k| obj.get(*k).and_then(parse_timestamp_ms))
            .unwrap_or(0);

        let replay_offset_kb = obj
            .get("replay_offset_kb")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let props: Vec<(String, PropValue)> = match props_val {
            Some(serde_json::Value::Object(m)) => m
                .into_iter()
                .map(|(k, v)| (k, PropValue::from_json_value(v)))
                .collect(),
            Some(v @ serde_json::Value::Array(_)) => Vec::<(String, PropValue)>::deserialize(v)
                .map_err(|_| {
                    serde::de::Error::custom(
                        "\"props\" / \"properties\" must be a JSON object or [key, value] list",
                    )
                })?,
            Some(serde_json::Value::Null) | None => Vec::new(),
            Some(_) => {
                return Err(serde::de::Error::custom(
                    "\"props\" / \"properties\" must be a JSON object",
                ));
            }
        };

        let raw_uuid = obj
            .remove("event_uuid")
            .or_else(|| obj.remove("message_id"))
            .or_else(|| obj.remove("messageId"))
            .or_else(|| obj.remove("insert_id"));
        let event_uuid = match raw_uuid {
            Some(serde_json::Value::Array(arr)) => {
                let bytes =
                    <[u8; 16]>::deserialize(serde_json::Value::Array(arr)).map_err(|_| {
                        serde::de::Error::custom("\"event_uuid\" byte array must have 16 bytes")
                    })?;
                bytes
            }
            Some(serde_json::Value::String(s)) if !s.trim().is_empty() => {
                if let Ok(u) = Uuid::parse_str(s.trim()) {
                    *u.as_bytes()
                } else {
                    xxh3_128(s.trim().as_bytes()).to_le_bytes()
                }
            }
            _ => Self::canonical_content_uuid(
                &distinct_id,
                &session_id,
                &event_name,
                client_event_ts_ms,
                replay_offset_kb,
                &props,
            ),
        };

        Ok(WireEvent {
            event_uuid,
            routing_seed,
            distinct_id,
            session_id,
            event_name,
            client_event_ts_ms,
            client_sent_ts_ms,
            props,
            replay_offset_kb,
        })
    }
}
