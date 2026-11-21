//! OpenTelemetry OTLP/JSON and OTLP/Protobuf (`ExportTraceServiceRequest`) normalizer.
//!
//! Maps OpenTelemetry GenAI semantic conventions (`gen_ai.*`), OpenInference (`llm.*`, `tool.*`,
//! `input.value`, `output.value`), and standard OTel attributes (`service.name`, `service.version`,
//! `deployment.environment`) into Pulse's canonical `TraceIngestBatch`.

use std::collections::{HashMap, HashSet};

use prost::Message;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use thiserror::Error;

use crate::model::{
    AgentSpan, AgentTrace, SpanEvent, SpanPhase, SpanType, TraceIngestBatch, TraceStatus,
};
use crate::pricing::{cost_nanos_to_usd, usd_to_cost_nanos};
use crate::MAX_LINKED_SPANS_PER_SPAN;

#[derive(Debug, Error)]
pub enum OtlpError {
    #[error("invalid OTLP protobuf payload: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpExportTraceRequest {
    #[serde(default)]
    pub resource_spans: Vec<OtlpResourceSpans>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpResourceSpans {
    #[serde(default)]
    pub resource: Option<OtlpResource>,
    #[serde(default)]
    pub scope_spans: Vec<OtlpScopeSpans>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpResource {
    #[serde(default)]
    pub attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OtlpInstrumentationScope {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpScopeSpans {
    #[serde(default)]
    pub scope: Option<OtlpInstrumentationScope>,
    #[serde(default)]
    pub spans: Vec<OtlpSpan>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpSpanLink {
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub span_id: String,
    #[serde(default)]
    pub attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpSpan {
    pub trace_id: String,
    pub span_id: String,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub start_time_unix_nano: Value,
    #[serde(default)]
    pub end_time_unix_nano: Value,
    #[serde(default)]
    pub attributes: Vec<OtlpKeyValue>,
    #[serde(default)]
    pub events: Vec<OtlpEvent>,
    #[serde(default)]
    pub links: Vec<OtlpSpanLink>,
    #[serde(default)]
    pub status: Option<OtlpStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpEvent {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub time_unix_nano: Value,
    #[serde(default)]
    pub attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpStatus {
    #[serde(default)]
    pub code: Value,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpKeyValue {
    pub key: String,
    #[serde(default)]
    pub value: Option<OtlpAnyValue>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OtlpAnyValue {
    #[serde(default)]
    pub string_value: Option<String>,
    #[serde(default)]
    pub int_value: Option<Value>,
    #[serde(default)]
    pub double_value: Option<f64>,
    #[serde(default)]
    pub bool_value: Option<bool>,
    #[serde(default)]
    pub array_value: Option<OtlpArrayValue>,
    #[serde(default)]
    pub kvlist_value: Option<OtlpKvList>,
    #[serde(default)]
    pub bytes_value: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpArrayValue {
    #[serde(default)]
    pub values: Vec<OtlpAnyValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpKvList {
    #[serde(default)]
    pub values: Vec<OtlpKeyValue>,
}

// ---------------------------------------------------------------------------
// Hand-derived zero-protoc OTLP/Protobuf wire messages (opentelemetry-proto v1)
// ---------------------------------------------------------------------------

pub mod pb {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct PbExportTraceServiceRequest {
        #[prost(message, repeated, tag = "1")]
        pub resource_spans: Vec<PbResourceSpans>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbResourceSpans {
        #[prost(message, optional, tag = "1")]
        pub resource: Option<PbResource>,
        #[prost(message, repeated, tag = "2")]
        pub scope_spans: Vec<PbScopeSpans>,
        #[prost(string, tag = "3")]
        pub schema_url: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbResource {
        #[prost(message, repeated, tag = "1")]
        pub attributes: Vec<PbKeyValue>,
        #[prost(uint32, tag = "2")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbScopeSpans {
        #[prost(message, optional, tag = "1")]
        pub scope: Option<PbInstrumentationScope>,
        #[prost(message, repeated, tag = "2")]
        pub spans: Vec<PbSpan>,
        #[prost(string, tag = "3")]
        pub schema_url: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbInstrumentationScope {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub version: String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: Vec<PbKeyValue>,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbSpan {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: String,
        #[prost(bytes = "vec", tag = "4")]
        pub parent_span_id: Vec<u8>,
        #[prost(string, tag = "5")]
        pub name: String,
        #[prost(int32, tag = "6")]
        pub kind: i32,
        #[prost(fixed64, tag = "7")]
        pub start_time_unix_nano: u64,
        #[prost(fixed64, tag = "8")]
        pub end_time_unix_nano: u64,
        #[prost(message, repeated, tag = "9")]
        pub attributes: Vec<PbKeyValue>,
        #[prost(uint32, tag = "10")]
        pub dropped_attributes_count: u32,
        #[prost(message, repeated, tag = "11")]
        pub events: Vec<PbSpanEvent>,
        #[prost(uint32, tag = "12")]
        pub dropped_events_count: u32,
        #[prost(message, repeated, tag = "13")]
        pub links: Vec<PbSpanLink>,
        #[prost(uint32, tag = "14")]
        pub dropped_links_count: u32,
        #[prost(message, optional, tag = "15")]
        pub status: Option<PbStatus>,
        #[prost(fixed32, tag = "16")]
        pub flags: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbSpanEvent {
        #[prost(fixed64, tag = "1")]
        pub time_unix_nano: u64,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(message, repeated, tag = "3")]
        pub attributes: Vec<PbKeyValue>,
        #[prost(uint32, tag = "4")]
        pub dropped_attributes_count: u32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbSpanLink {
        #[prost(bytes = "vec", tag = "1")]
        pub trace_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub span_id: Vec<u8>,
        #[prost(string, tag = "3")]
        pub trace_state: String,
        #[prost(message, repeated, tag = "4")]
        pub attributes: Vec<PbKeyValue>,
        #[prost(uint32, tag = "5")]
        pub dropped_attributes_count: u32,
        #[prost(fixed32, tag = "6")]
        pub flags: u32,
    }

    /// OpenTelemetry Status: tag 1 is reserved (deprecated), message = 2, code = 3.
    #[derive(Clone, PartialEq, Message)]
    pub struct PbStatus {
        #[prost(string, tag = "2")]
        pub message: String,
        #[prost(int32, tag = "3")]
        pub code: i32,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbKeyValue {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<PbAnyValue>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbAnyValue {
        #[prost(oneof = "pb_any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
        pub value: Option<pb_any_value::Value>,
    }

    pub mod pb_any_value {
        use super::{PbArrayValue, PbKeyValueList};
        use prost::Oneof;

        #[derive(Clone, PartialEq, Oneof)]
        pub enum Value {
            #[prost(string, tag = "1")]
            StringValue(String),
            #[prost(bool, tag = "2")]
            BoolValue(bool),
            #[prost(int64, tag = "3")]
            IntValue(i64),
            #[prost(double, tag = "4")]
            DoubleValue(f64),
            #[prost(message, tag = "5")]
            ArrayValue(PbArrayValue),
            #[prost(message, tag = "6")]
            KvlistValue(PbKeyValueList),
            #[prost(bytes, tag = "7")]
            BytesValue(Vec<u8>),
        }
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbArrayValue {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<PbAnyValue>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct PbKeyValueList {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<PbKeyValue>,
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn pb_any_to_otlp(v: pb::PbAnyValue) -> OtlpAnyValue {
    use pb::pb_any_value::Value as PbVal;
    match v.value {
        Some(PbVal::StringValue(s)) => OtlpAnyValue {
            string_value: Some(s),
            ..Default::default()
        },
        Some(PbVal::BoolValue(b)) => OtlpAnyValue {
            bool_value: Some(b),
            ..Default::default()
        },
        Some(PbVal::IntValue(i)) => OtlpAnyValue {
            int_value: Some(Value::Number(i.into())),
            ..Default::default()
        },
        Some(PbVal::DoubleValue(d)) => OtlpAnyValue {
            double_value: Some(d),
            ..Default::default()
        },
        Some(PbVal::ArrayValue(arr)) => OtlpAnyValue {
            array_value: Some(OtlpArrayValue {
                values: arr.values.into_iter().map(pb_any_to_otlp).collect(),
            }),
            ..Default::default()
        },
        Some(PbVal::KvlistValue(kv)) => OtlpAnyValue {
            kvlist_value: Some(OtlpKvList {
                values: kv.values.into_iter().map(pb_kv_to_otlp).collect(),
            }),
            ..Default::default()
        },
        Some(PbVal::BytesValue(b)) => {
            let s = match String::from_utf8(b) {
                Ok(utf8) => utf8,
                Err(e) => encode_hex(e.as_bytes()),
            };
            OtlpAnyValue {
                bytes_value: Some(s),
                ..Default::default()
            }
        }
        None => OtlpAnyValue::default(),
    }
}

fn pb_kv_to_otlp(kv: pb::PbKeyValue) -> OtlpKeyValue {
    OtlpKeyValue {
        key: kv.key,
        value: kv.value.map(pb_any_to_otlp),
    }
}

/// Decodes an OTLP/Protobuf `ExportTraceServiceRequest` wire payload without `protoc`.
pub fn decode_otlp_protobuf(bytes: &[u8]) -> Result<OtlpExportTraceRequest, OtlpError> {
    let req = pb::PbExportTraceServiceRequest::decode(bytes)?;
    let resource_spans = req
        .resource_spans
        .into_iter()
        .map(|rs| OtlpResourceSpans {
            resource: rs.resource.map(|r| OtlpResource {
                attributes: r.attributes.into_iter().map(pb_kv_to_otlp).collect(),
            }),
            scope_spans: rs
                .scope_spans
                .into_iter()
                .map(|ss| OtlpScopeSpans {
                    scope: ss.scope.map(|sc| OtlpInstrumentationScope {
                        name: sc.name,
                        version: sc.version,
                        attributes: sc.attributes.into_iter().map(pb_kv_to_otlp).collect(),
                    }),
                    spans: ss
                        .spans
                        .into_iter()
                        .map(|sp| {
                            let parent_span_id = if sp.parent_span_id.is_empty()
                                || sp.parent_span_id.iter().all(|&b| b == 0)
                            {
                                None
                            } else {
                                Some(encode_hex(&sp.parent_span_id))
                            };
                            OtlpSpan {
                                trace_id: encode_hex(&sp.trace_id),
                                span_id: encode_hex(&sp.span_id),
                                parent_span_id,
                                name: sp.name,
                                start_time_unix_nano: Value::Number(sp.start_time_unix_nano.into()),
                                end_time_unix_nano: Value::Number(sp.end_time_unix_nano.into()),
                                attributes: sp.attributes.into_iter().map(pb_kv_to_otlp).collect(),
                                events: sp
                                    .events
                                    .into_iter()
                                    .map(|ev| OtlpEvent {
                                        name: ev.name,
                                        time_unix_nano: Value::Number(ev.time_unix_nano.into()),
                                        attributes: ev
                                            .attributes
                                            .into_iter()
                                            .map(pb_kv_to_otlp)
                                            .collect(),
                                    })
                                    .collect(),
                                links: sp
                                    .links
                                    .into_iter()
                                    .map(|lk| OtlpSpanLink {
                                        trace_id: encode_hex(&lk.trace_id),
                                        span_id: encode_hex(&lk.span_id),
                                        attributes: lk
                                            .attributes
                                            .into_iter()
                                            .map(pb_kv_to_otlp)
                                            .collect(),
                                    })
                                    .collect(),
                                status: sp.status.map(|st| OtlpStatus {
                                    code: Value::Number((st.code as i64).into()),
                                    message: if st.message.is_empty() {
                                        None
                                    } else {
                                        Some(st.message)
                                    },
                                }),
                            }
                        })
                        .collect(),
                })
                .collect(),
        })
        .collect();

    Ok(OtlpExportTraceRequest { resource_spans })
}

/// Decodes and normalizes an OTLP/Protobuf `ExportTraceServiceRequest` into a `TraceIngestBatch`.
pub fn normalize_otlp_protobuf(bytes: &[u8]) -> Result<TraceIngestBatch, OtlpError> {
    decode_otlp_protobuf(bytes).map(normalize_otlp_traces)
}

fn any_to_json(v: &OtlpAnyValue) -> Value {
    if let Some(s) = &v.string_value {
        return Value::String(s.clone());
    }
    if let Some(b) = &v.bytes_value {
        return Value::String(b.clone());
    }
    if let Some(i) = &v.int_value {
        if let Some(n) = i.as_i64() {
            return Value::Number(n.into());
        }
        if let Some(s) = i.as_str().and_then(|s| s.parse::<i64>().ok()) {
            return Value::Number(s.into());
        }
    }
    if let Some(d) = v.double_value {
        if let Some(n) = serde_json::Number::from_f64(d) {
            return Value::Number(n);
        }
    }
    if let Some(b) = v.bool_value {
        return Value::Bool(b);
    }
    if let Some(arr) = &v.array_value {
        return Value::Array(arr.values.iter().map(any_to_json).collect());
    }
    if let Some(kv) = &v.kvlist_value {
        let mut map = Map::new();
        for item in &kv.values {
            if let Some(iv) = &item.value {
                map.insert(item.key.clone(), any_to_json(iv));
            }
        }
        return Value::Object(map);
    }
    Value::Null
}

fn attrs_to_map(attrs: &[OtlpKeyValue]) -> HashMap<String, Value> {
    let mut map = HashMap::with_capacity(attrs.len());
    for kv in attrs {
        if let Some(v) = &kv.value {
            map.insert(kv.key.clone(), any_to_json(v));
        }
    }
    map
}

fn nanos_to_ms(v: &Value) -> i64 {
    if let Some(n) = v.as_i64() {
        return n / 1_000_000;
    }
    if let Some(u) = v.as_u64() {
        return (u / 1_000_000).min(i64::MAX as u64) as i64;
    }
    if let Some(f) = v.as_f64() {
        if f.is_finite() {
            let ms = (f / 1_000_000.0).trunc();
            if ms >= i64::MAX as f64 {
                return i64::MAX;
            }
            if ms <= i64::MIN as f64 {
                return i64::MIN;
            }
            return ms as i64;
        }
        return if f.is_infinite() {
            if f.is_sign_positive() {
                i64::MAX
            } else {
                i64::MIN
            }
        } else {
            0
        };
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.trim().parse::<i128>() {
            return (n / 1_000_000).clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        }
    }
    0
}

fn str_attr(map: &HashMap<String, Value>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(v) = map.get(*k) {
            if let Some(s) = v.as_str() {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

fn i64_attr(map: &HashMap<String, Value>, keys: &[&str]) -> i64 {
    for k in keys {
        if let Some(v) = map.get(*k) {
            if let Some(n) = v.as_i64() {
                return n;
            }
            if let Some(s) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
                return s;
            }
        }
    }
    0
}

fn f64_attr(map: &HashMap<String, Value>, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(v) = map.get(*k) {
            if let Some(f) = v.as_f64() {
                return Some(f);
            }
            if let Some(s) = v.as_str().and_then(|s| s.parse::<f64>().ok()) {
                return Some(s);
            }
        }
    }
    None
}

fn is_valid_otel_id(raw: &str) -> bool {
    let trimmed = raw.trim();
    !trimmed.is_empty() && !trimmed.chars().all(|c| c == '0')
}

fn collect_linked_span_ids(
    span_id: &str,
    links: &[OtlpSpanLink],
    attrs: &HashMap<String, Value>,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    let mut push_id = |candidate: &str| {
        let trimmed = candidate.trim();
        if is_valid_otel_id(trimmed)
            && trimmed != span_id
            && out.len() < MAX_LINKED_SPANS_PER_SPAN
            && seen.insert(trimmed.to_string())
        {
            out.push(trimmed.to_string());
        }
    };

    for link in links {
        push_id(&link.span_id);
    }

    if let Some(val) = attrs.get("pulse.linked_span_ids") {
        match val {
            Value::Array(arr) => {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        push_id(s);
                    }
                }
            }
            Value::String(s) => {
                if let Ok(Value::Array(arr)) = serde_json::from_str::<Value>(s) {
                    for item in arr {
                        if let Some(id) = item.as_str() {
                            push_id(id);
                        }
                    }
                } else {
                    for part in s.split(',') {
                        push_id(part);
                    }
                }
            }
            _ => {}
        }
    }

    out
}

fn extract_parent_trace_id(
    attrs: &HashMap<String, Value>,
    res_attrs: &HashMap<String, Value>,
    links: &[OtlpSpanLink],
) -> Option<String> {
    if let Some(pt) =
        str_attr(attrs, &["pulse.parent_trace_id", "gen_ai.parent_trace_id"]).or_else(|| {
            str_attr(
                res_attrs,
                &["pulse.parent_trace_id", "gen_ai.parent_trace_id"],
            )
        })
    {
        if is_valid_otel_id(&pt) {
            return Some(pt);
        }
    }
    for link in links {
        let link_attrs = attrs_to_map(&link.attributes);
        if str_attr(&link_attrs, &["pulse.relationship"])
            .is_some_and(|r| r.eq_ignore_ascii_case("parent_trace"))
            && is_valid_otel_id(&link.trace_id)
        {
            return Some(link.trace_id.trim().to_string());
        }
    }
    None
}

fn infer_span_type(name: &str, attrs: &HashMap<String, Value>) -> SpanType {
    if let Some(op) = str_attr(
        attrs,
        &[
            "pulse.span_type",
            "gen_ai.operation.name",
            "openinference.span.kind",
        ],
    ) {
        let lower = op.to_ascii_lowercase();
        if lower.contains("guardrail") || lower.contains("safety") || lower.contains("moderation") {
            return SpanType::Guardrail;
        }
        if lower.contains("chat")
            || lower.contains("completion")
            || lower.contains("llm")
            || lower.contains("generate")
        {
            return SpanType::Llm;
        }
        if lower.contains("tool") || lower.contains("function") {
            return SpanType::Tool;
        }
        if lower.contains("embed") {
            return SpanType::Embedding;
        }
        if lower.contains("retriev") || lower.contains("rag") {
            return SpanType::Retrieval;
        }
        if lower.contains("agent") || lower.contains("chain") {
            return SpanType::Agent;
        }
        if lower.contains("workflow") || lower.contains("graph") {
            return SpanType::Workflow;
        }
    }
    if attrs.contains_key("pulse.guardrail.name") || attrs.contains_key("guardrail.name") {
        return SpanType::Guardrail;
    }
    if attrs.contains_key("gen_ai.request.model") || attrs.contains_key("llm.model_name") {
        return SpanType::Llm;
    }
    if attrs.contains_key("gen_ai.tool.name") || attrs.contains_key("tool.name") {
        return SpanType::Tool;
    }
    if attrs.contains_key("http.request.method") || attrs.contains_key("http.method") {
        return SpanType::Http;
    }
    if attrs.contains_key("db.system") {
        return SpanType::Db;
    }
    let nl = name.to_ascii_lowercase();
    if nl.contains("guardrail") {
        SpanType::Guardrail
    } else if nl.contains("tool") {
        SpanType::Tool
    } else if nl.contains("llm")
        || nl.contains("chat")
        || nl.contains("openai")
        || nl.contains("anthropic")
        || nl.contains("gemini")
    {
        SpanType::Llm
    } else {
        SpanType::Custom
    }
}

/// Converts an OTLP/JSON (or decoded OTLP/Protobuf) `ExportTraceServiceRequest` into a canonical `TraceIngestBatch`.
pub fn normalize_otlp_traces(req: OtlpExportTraceRequest) -> TraceIngestBatch {
    let mut traces_by_id: HashMap<String, AgentTrace> = HashMap::new();
    let mut spans_out: Vec<AgentSpan> = Vec::new();

    for rs in req.resource_spans {
        let res_attrs = rs
            .resource
            .as_ref()
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or_default();
        let res_agent = str_attr(&res_attrs, &["pulse.agent.name", "service.name"])
            .unwrap_or_else(|| "otel-agent".to_string());
        let res_version = str_attr(&res_attrs, &["pulse.agent.version", "service.version"])
            .unwrap_or_else(|| "unversioned".to_string());
        let res_env = str_attr(
            &res_attrs,
            &[
                "deployment.environment.name",
                "deployment.environment",
                "pulse.environment",
            ],
        )
        .unwrap_or_else(|| "production".to_string());
        let res_prompt_ver = str_attr(
            &res_attrs,
            &["pulse.prompt.version", "gen_ai.prompt.version"],
        );

        for ss in rs.scope_spans {
            let scope_name = ss
                .scope
                .as_ref()
                .map(|s| s.name.trim().to_string())
                .filter(|s| !s.is_empty());

            for sp in ss.spans {
                let mut attrs = attrs_to_map(&sp.attributes);
                if let Some(ref sc_name) = scope_name {
                    attrs
                        .entry("otel.scope.name".to_string())
                        .or_insert_with(|| Value::String(sc_name.clone()));
                }

                let start_ms = nanos_to_ms(&sp.start_time_unix_nano);
                let end_ms = nanos_to_ms(&sp.end_time_unix_nano);
                let duration_ms = end_ms.saturating_sub(start_ms).max(0);
                let parent = sp
                    .parent_span_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| is_valid_otel_id(s))
                    .map(str::to_string);

                let is_err = sp.status.as_ref().is_some_and(|st| {
                    st.code.as_i64() == Some(2)
                        || st.code.as_str().is_some_and(|s| {
                            s.eq_ignore_ascii_case("STATUS_CODE_ERROR")
                                || s.eq_ignore_ascii_case("error")
                        })
                });
                let span_phase = str_attr(&attrs, &["pulse.span_phase", "span_phase"])
                    .map(|s| SpanPhase::parse(&s))
                    .unwrap_or_default();
                let status = if is_err {
                    TraceStatus::Error
                } else if span_phase != SpanPhase::Final && end_ms == 0 {
                    TraceStatus::Running
                } else {
                    TraceStatus::Ok
                };

                let span_type = infer_span_type(&sp.name, &attrs);
                let linked_span_ids = collect_linked_span_ids(&sp.span_id, &sp.links, &attrs);
                let parent_trace_id = extract_parent_trace_id(&attrs, &res_attrs, &sp.links);

                let provider = str_attr(
                    &attrs,
                    &["gen_ai.system", "gen_ai.provider.name", "llm.provider"],
                );
                let model = str_attr(
                    &attrs,
                    &[
                        "gen_ai.response.model",
                        "gen_ai.request.model",
                        "llm.model_name",
                    ],
                );
                let input_tokens = i64_attr(
                    &attrs,
                    &[
                        "gen_ai.usage.input_tokens",
                        "gen_ai.usage.prompt_tokens",
                        "llm.token_count.prompt",
                    ],
                )
                .max(0);
                let output_tokens = i64_attr(
                    &attrs,
                    &[
                        "gen_ai.usage.output_tokens",
                        "gen_ai.usage.completion_tokens",
                        "llm.token_count.completion",
                    ],
                )
                .max(0);
                let cached_tokens = i64_attr(
                    &attrs,
                    &[
                        "gen_ai.usage.cache_read_input_tokens",
                        "llm.token_count.prompt_details.cache_read",
                    ],
                )
                .max(0);
                let total_tokens = {
                    let explicit = i64_attr(
                        &attrs,
                        &["gen_ai.usage.total_tokens", "llm.token_count.total"],
                    );
                    if explicit > 0 {
                        explicit
                    } else {
                        input_tokens.saturating_add(output_tokens)
                    }
                }
                .max(0);

                let explicit_nanos =
                    i64_attr(&attrs, &["pulse.cost_nanos", "gen_ai.usage.cost_nanos"]);
                let (cost_nanos, cost_usd) = if explicit_nanos > 0 {
                    (explicit_nanos, cost_nanos_to_usd(explicit_nanos))
                } else {
                    let usd = f64_attr(
                        &attrs,
                        &["gen_ai.usage.cost", "pulse.cost_usd", "llm.cost.total"],
                    )
                    .unwrap_or(0.0);
                    let nanos = usd_to_cost_nanos(usd);
                    (
                        nanos,
                        if nanos > 0 {
                            cost_nanos_to_usd(nanos)
                        } else {
                            0.0
                        },
                    )
                };

                let temperature = f64_attr(
                    &attrs,
                    &[
                        "gen_ai.request.temperature",
                        "llm.invocation_parameters.temperature",
                    ],
                );
                let finish_reason = str_attr(
                    &attrs,
                    &[
                        "gen_ai.response.finish_reasons",
                        "gen_ai.response.finish_reason",
                    ],
                );
                let tool_name = str_attr(&attrs, &["gen_ai.tool.name", "tool.name"]);
                let tool_version = str_attr(&attrs, &["gen_ai.tool.version", "tool.version"]);

                let input = attrs
                    .get("gen_ai.prompt")
                    .or_else(|| attrs.get("input.value"))
                    .or_else(|| attrs.get("pulse.input"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let output = attrs
                    .get("gen_ai.completion")
                    .or_else(|| attrs.get("output.value"))
                    .or_else(|| attrs.get("pulse.output"))
                    .cloned()
                    .unwrap_or(Value::Null);

                let events: Vec<SpanEvent> = sp
                    .events
                    .iter()
                    .map(|e| SpanEvent {
                        name: e.name.clone(),
                        timestamp_ms: nanos_to_ms(&e.time_unix_nano),
                        attributes: Value::Object(
                            attrs_to_map(&e.attributes).into_iter().collect(),
                        ),
                    })
                    .collect();

                let error_message = if is_err {
                    sp.status
                        .as_ref()
                        .and_then(|s| s.message.clone())
                        .or_else(|| str_attr(&attrs, &["exception.message", "error.message"]))
                } else {
                    None
                };
                let error_type = if is_err {
                    str_attr(&attrs, &["exception.type", "error.type"])
                } else {
                    None
                };

                let agent_name = str_attr(&attrs, &["pulse.agent.name", "gen_ai.agent.name"])
                    .unwrap_or_else(|| res_agent.clone());
                let session_id = str_attr(
                    &attrs,
                    &["session.id", "pulse.session_id", "gen_ai.conversation.id"],
                )
                .or_else(|| {
                    str_attr(
                        &res_attrs,
                        &["session.id", "pulse.session_id", "gen_ai.conversation.id"],
                    )
                });
                let user_id = str_attr(&attrs, &["enduser.id", "user.id", "pulse.user_id"])
                    .or_else(|| str_attr(&res_attrs, &["enduser.id", "user.id", "pulse.user_id"]));
                let workflow_id = str_attr(&attrs, &["pulse.workflow_id"])
                    .or_else(|| str_attr(&res_attrs, &["pulse.workflow_id"]));

                let trace_entry =
                    traces_by_id
                        .entry(sp.trace_id.clone())
                        .or_insert_with(|| AgentTrace {
                            trace_id: sp.trace_id.clone(),
                            project_id: String::new(),
                            agent_name: agent_name.clone(),
                            agent_version: str_attr(&attrs, &["pulse.agent.version"])
                                .unwrap_or_else(|| res_version.clone()),
                            prompt_version: str_attr(&attrs, &["pulse.prompt.version"])
                                .or_else(|| res_prompt_ver.clone()),
                            model_config_version: str_attr(&attrs, &["pulse.model_config.version"]),
                            tool_config_version: str_attr(&attrs, &["pulse.tool_config.version"]),
                            app_release: str_attr(
                                &res_attrs,
                                &["service.release", "pulse.app_release"],
                            ),
                            environment: res_env.clone(),
                            session_id: session_id.clone(),
                            user_id: user_id.clone(),
                            workflow_id: workflow_id.clone(),
                            parent_trace_id: parent_trace_id.clone(),
                            status,
                            start_ts_ms: start_ms,
                            end_ts_ms: if end_ms > 0 { Some(end_ms) } else { None },
                            duration_ms,
                            input: if parent.is_none() {
                                input.clone()
                            } else {
                                Value::Null
                            },
                            output: if parent.is_none() {
                                output.clone()
                            } else {
                                Value::Null
                            },
                            error_type: error_type.clone(),
                            error_message: error_message.clone(),
                            failure_source: None,
                            metadata: json!({ "source": "otlp" }),
                            tags: vec!["otlp".to_string()],
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

                // Backfill trace-level metadata if a child span arrived before root/metadata span
                if trace_entry.session_id.is_none() {
                    trace_entry.session_id = session_id;
                }
                if trace_entry.user_id.is_none() {
                    trace_entry.user_id = user_id;
                }
                if trace_entry.workflow_id.is_none() {
                    trace_entry.workflow_id = workflow_id;
                }
                if trace_entry.parent_trace_id.is_none() {
                    trace_entry.parent_trace_id = parent_trace_id;
                }

                if start_ms > 0
                    && (trace_entry.start_ts_ms == 0 || start_ms < trace_entry.start_ts_ms)
                {
                    trace_entry.start_ts_ms = start_ms;
                }
                if end_ms > 0 && trace_entry.end_ts_ms.is_none_or(|e| end_ms > e) {
                    trace_entry.end_ts_ms = Some(end_ms);
                }
                if let Some(e) = trace_entry.end_ts_ms {
                    trace_entry.duration_ms = e.saturating_sub(trace_entry.start_ts_ms).max(0);
                }
                if parent.is_none() {
                    if !input.is_null() {
                        trace_entry.input = input.clone();
                    }
                    if !output.is_null() {
                        trace_entry.output = output.clone();
                    }
                }
                if is_err {
                    trace_entry.status = TraceStatus::Error;
                    if trace_entry.error_type.is_none() {
                        trace_entry.error_type = error_type.clone();
                    }
                    if trace_entry.error_message.is_none() {
                        trace_entry.error_message = error_message.clone();
                    }
                } else if trace_entry.status == TraceStatus::Running
                    && span_phase == SpanPhase::Final
                {
                    trace_entry.status = TraceStatus::Ok;
                }

                // Rollup span metrics into trace_entry
                trace_entry.span_count = trace_entry.span_count.saturating_add(1);
                if span_type == SpanType::Llm {
                    trace_entry.llm_call_count = trace_entry.llm_call_count.saturating_add(1);
                }
                if span_type == SpanType::Tool {
                    trace_entry.tool_call_count = trace_entry.tool_call_count.saturating_add(1);
                }
                if is_err {
                    trace_entry.error_span_count = trace_entry.error_span_count.saturating_add(1);
                }
                trace_entry.input_tokens =
                    trace_entry.input_tokens.saturating_add(input_tokens.max(0));
                trace_entry.output_tokens = trace_entry
                    .output_tokens
                    .saturating_add(output_tokens.max(0));
                trace_entry.cached_tokens = trace_entry
                    .cached_tokens
                    .saturating_add(cached_tokens.max(0));
                trace_entry.total_tokens =
                    trace_entry.total_tokens.saturating_add(total_tokens.max(0));
                trace_entry.total_cost_nanos = trace_entry
                    .total_cost_nanos
                    .saturating_add(cost_nanos.max(0));
                trace_entry.total_cost_usd = cost_nanos_to_usd(trace_entry.total_cost_nanos);
                if trace_entry.primary_model.is_none() && model.is_some() {
                    trace_entry.primary_model = model.clone();
                }
                if trace_entry.primary_provider.is_none() && provider.is_some() {
                    trace_entry.primary_provider = provider.clone();
                }

                spans_out.push(AgentSpan {
                    span_id: sp.span_id,
                    trace_id: sp.trace_id,
                    project_id: String::new(),
                    parent_span_id: parent,
                    linked_span_ids,
                    span_phase,
                    depth: 0,
                    name: sp.name,
                    span_type,
                    status,
                    start_ts_ms: start_ms,
                    end_ts_ms: if end_ms > 0 { Some(end_ms) } else { None },
                    duration_ms,
                    provider,
                    model: model.clone(),
                    model_id: model,
                    model_version: None,
                    prompt_template_id: str_attr(&attrs, &["pulse.prompt.template_id"]),
                    prompt_template_version: str_attr(&attrs, &["pulse.prompt.version"]),
                    input_tokens,
                    output_tokens,
                    cached_tokens,
                    total_tokens,
                    cost_nanos,
                    cost_usd,
                    temperature,
                    finish_reason,
                    model_params: Value::Null,
                    tool_name,
                    tool_version,
                    tool_retry_count: i64_attr(&attrs, &["pulse.tool.retry_count"])
                        .clamp(0, i32::MAX as i64) as i32,
                    input,
                    output,
                    attributes: Value::Object(attrs.into_iter().collect()),
                    events,
                    metadata: Value::Null,
                    error_type,
                    error_message,
                    error_stack: None,
                });
            }
        }
    }

    TraceIngestBatch {
        traces: traces_by_id.into_values().collect(),
        spans: spans_out,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_otlp_genai_span_with_guardrail_links_and_rollups() {
        let raw = json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": "support-agent" } },
                        { "key": "service.version", "value": { "stringValue": "2.1.0" } }
                    ]
                },
                "scopeSpans": [{
                    "scope": { "name": "pulse.otel", "version": "1.0.0" },
                    "spans": [
                        {
                            "traceId": "tr_otlp_1",
                            "spanId": "sp_child",
                            "parentSpanId": "sp_root",
                            "name": "safety.guardrail",
                            "startTimeUnixNano": "1700000000100000000",
                            "endTimeUnixNano": "1700000000250000000",
                            "links": [
                                { "traceId": "tr_otlp_1", "spanId": "sp_ref_1" },
                                { "traceId": "tr_parent", "spanId": "sp_p", "attributes": [
                                    { "key": "pulse.relationship", "value": { "stringValue": "parent_trace" } }
                                ]}
                            ],
                            "attributes": [
                                { "key": "pulse.span_type", "value": { "stringValue": "guardrail" } },
                                { "key": "pulse.span_phase", "value": { "stringValue": "update" } },
                                { "key": "pulse.cost_nanos", "value": { "intValue": "500000" } }
                            ]
                        },
                        {
                            "traceId": "tr_otlp_1",
                            "spanId": "sp_root",
                            "name": "openai.chat",
                            "startTimeUnixNano": "1700000000000000000",
                            "endTimeUnixNano": "1700000001200000000",
                            "attributes": [
                                { "key": "session.id", "value": { "stringValue": "sess_9" } },
                                { "key": "gen_ai.system", "value": { "stringValue": "openai" } },
                                { "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4o-mini" } },
                                { "key": "gen_ai.usage.input_tokens", "value": { "intValue": "500" } },
                                { "key": "gen_ai.usage.output_tokens", "value": { "intValue": "120" } },
                                { "key": "gen_ai.usage.cost", "value": { "doubleValue": 0.0015 } }
                            ]
                        }
                    ]
                }]
            }]
        });
        let req: OtlpExportTraceRequest = serde_json::from_value(raw).unwrap();
        let batch = normalize_otlp_traces(req);
        assert_eq!(batch.traces.len(), 1);
        assert_eq!(batch.spans.len(), 2);

        let tr = &batch.traces[0];
        assert_eq!(tr.agent_name, "support-agent");
        assert_eq!(tr.agent_version, "2.1.0");
        assert_eq!(tr.session_id.as_deref(), Some("sess_9"));
        assert_eq!(tr.parent_trace_id.as_deref(), Some("tr_parent"));
        assert_eq!(tr.span_count, 2);
        assert_eq!(tr.llm_call_count, 1);
        assert_eq!(tr.total_tokens, 620);
        assert_eq!(tr.total_cost_nanos, 2_000_000);
        assert_eq!(tr.duration_ms, 1200);

        let child = &batch.spans[0];
        assert_eq!(child.span_type, SpanType::Guardrail);
        assert_eq!(child.span_phase, SpanPhase::Update);
        assert_eq!(child.linked_span_ids, vec!["sp_ref_1", "sp_p"]);
        assert_eq!(child.cost_nanos, 500_000);

        let root = &batch.spans[1];
        assert_eq!(root.span_type, SpanType::Llm);
        assert_eq!(root.span_phase, SpanPhase::Final);
        assert_eq!(root.cost_nanos, 1_500_000);
    }

    #[test]
    fn decodes_otlp_protobuf_wire_format_with_exact_otel_tags() {
        use pb::pb_any_value::Value as PbVal;
        let msg = pb::PbExportTraceServiceRequest {
            resource_spans: vec![pb::PbResourceSpans {
                resource: Some(pb::PbResource {
                    attributes: vec![
                        pb::PbKeyValue {
                            key: "service.name".into(),
                            value: Some(pb::PbAnyValue {
                                value: Some(PbVal::StringValue("pb-agent".into())),
                            }),
                        },
                        pb::PbKeyValue {
                            key: "pulse.parent_trace_id".into(),
                            value: Some(pb::PbAnyValue {
                                value: Some(PbVal::StringValue("tr_parent_pb".into())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![pb::PbScopeSpans {
                    scope: Some(pb::PbInstrumentationScope {
                        name: "otel-rs".into(),
                        version: "0.2.0".into(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![pb::PbSpan {
                        trace_id: vec![0xaa; 16],
                        span_id: vec![0xbb; 8],
                        trace_state: String::new(),
                        parent_span_id: vec![0; 8], // all-zero -> None
                        name: "tool.search".into(),
                        kind: 1,
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_450_000_000,
                        attributes: vec![
                            pb::PbKeyValue {
                                key: "gen_ai.tool.name".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::StringValue("web_search".into())),
                                }),
                            },
                            pb::PbKeyValue {
                                key: "pulse.span_phase".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::StringValue("start".into())),
                                }),
                            },
                            pb::PbKeyValue {
                                key: "flag.ok".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::BoolValue(true)),
                                }),
                            },
                            pb::PbKeyValue {
                                key: "gen_ai.usage.input_tokens".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::IntValue(120)),
                                }),
                            },
                            pb::PbKeyValue {
                                key: "gen_ai.usage.cost".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::DoubleValue(0.002)),
                                }),
                            },
                            pb::PbKeyValue {
                                key: "tags.arr".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::ArrayValue(pb::PbArrayValue {
                                        values: vec![pb::PbAnyValue {
                                            value: Some(PbVal::StringValue("v1".into())),
                                        }],
                                    })),
                                }),
                            },
                            pb::PbKeyValue {
                                key: "kv.meta".into(),
                                value: Some(pb::PbAnyValue {
                                    value: Some(PbVal::KvlistValue(pb::PbKeyValueList {
                                        values: vec![pb::PbKeyValue {
                                            key: "k".into(),
                                            value: Some(pb::PbAnyValue {
                                                value: Some(PbVal::BytesValue(b"raw".to_vec())),
                                            }),
                                        }],
                                    })),
                                }),
                            },
                        ],
                        dropped_attributes_count: 0,
                        events: vec![pb::PbSpanEvent {
                            time_unix_nano: 1_700_000_000_100_000_000,
                            name: "tool.invoked".into(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                        }],
                        dropped_events_count: 0,
                        links: vec![pb::PbSpanLink {
                            trace_id: vec![0xaa; 16],
                            span_id: vec![0xcc; 8],
                            trace_state: String::new(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                            flags: 0,
                        }],
                        dropped_links_count: 0,
                        status: Some(pb::PbStatus {
                            message: "tool timeout".into(),
                            code: 2, // STATUS_CODE_ERROR
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let wire = msg.encode_to_vec();
        let batch = normalize_otlp_protobuf(&wire).unwrap();
        assert_eq!(batch.traces.len(), 1);
        assert_eq!(batch.spans.len(), 1);

        let tr = &batch.traces[0];
        assert_eq!(tr.trace_id, "aa".repeat(16));
        assert_eq!(tr.agent_name, "pb-agent");
        assert_eq!(tr.parent_trace_id.as_deref(), Some("tr_parent_pb"));
        assert_eq!(tr.status, TraceStatus::Error);
        assert_eq!(tr.error_message.as_deref(), Some("tool timeout"));
        assert_eq!(tr.total_cost_nanos, 2_000_000);

        let sp = &batch.spans[0];
        assert_eq!(sp.span_id, "bb".repeat(8));
        assert_eq!(sp.parent_span_id, None);
        assert_eq!(sp.span_type, SpanType::Tool);
        assert_eq!(sp.span_phase, SpanPhase::Start);
        assert_eq!(sp.duration_ms, 450);
        assert_eq!(sp.events.len(), 1);
        assert_eq!(sp.events[0].name, "tool.invoked");
        assert_eq!(sp.linked_span_ids, vec!["cc".repeat(8)]);
        assert_eq!(sp.attributes["kv.meta"]["k"], json!("raw"));
        assert_eq!(sp.attributes["flag.ok"], json!(true));
    }

    #[test]
    fn hostile_extreme_timestamps_and_numeric_attrs_never_overflow_or_wrap() {
        // Challenger 1 reproducer: negative extreme string timestamp + positive end timestamp,
        // plus positive out-of-range i64*1e6 string timestamp, multi-span rollup, float/u64 timestamps,
        // and extreme token / retry_count attributes.
        let raw = json!({
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [
                        {
                            "traceId": "tr_ext_1",
                            "spanId": "sp_ext_1",
                            "name": "extreme.ts.1",
                            "startTimeUnixNano": "-9223372036854775808000000",
                            "endTimeUnixNano": "1700000000000000000"
                        },
                        {
                            "traceId": "tr_ext_2",
                            "spanId": "sp_ext_2",
                            "name": "extreme.ts.2",
                            "startTimeUnixNano": "-9223372036854775808000000",
                            "endTimeUnixNano": "9223372036854775808000000"
                        },
                        {
                            "traceId": "tr_ext_3",
                            "spanId": "sp_ext_3a",
                            "name": "extreme.ts.3a",
                            "startTimeUnixNano": "-9223372036854775808000000",
                            "endTimeUnixNano": "1700000000000000000"
                        },
                        {
                            "traceId": "tr_ext_3",
                            "spanId": "sp_ext_3b",
                            "name": "extreme.ts.3b",
                            "startTimeUnixNano": "1700000000000000000",
                            "endTimeUnixNano": "9223372036854775808000000",
                            "attributes": [
                                { "key": "gen_ai.usage.input_tokens", "value": { "intValue": i64::MAX } },
                                { "key": "gen_ai.usage.output_tokens", "value": { "intValue": i64::MAX } },
                                { "key": "pulse.tool.retry_count", "value": { "intValue": 5_000_000_000_i64 } }
                            ]
                        },
                        {
                            "traceId": "tr_ext_4",
                            "spanId": "sp_ext_4",
                            "name": "extreme.ts.4.reversed_and_float",
                            "startTimeUnixNano": 1e30,
                            "endTimeUnixNano": -1e30,
                            "attributes": [
                                { "key": "pulse.tool.retry_count", "value": { "intValue": -5 } }
                            ]
                        }
                    ]
                }]
            }]
        });

        let req: OtlpExportTraceRequest = serde_json::from_value(raw).unwrap();
        let batch = normalize_otlp_traces(req);
        assert_eq!(batch.spans.len(), 5);
        assert_eq!(batch.traces.len(), 4);

        let sp1 = batch
            .spans
            .iter()
            .find(|s| s.span_id == "sp_ext_1")
            .unwrap();
        assert_eq!(sp1.start_ts_ms, i64::MIN);
        assert_eq!(sp1.end_ts_ms, Some(1_700_000_000_000));
        assert_eq!(sp1.duration_ms, i64::MAX);

        let sp2 = batch
            .spans
            .iter()
            .find(|s| s.span_id == "sp_ext_2")
            .unwrap();
        assert_eq!(sp2.start_ts_ms, i64::MIN);
        assert_eq!(sp2.end_ts_ms, Some(i64::MAX));
        assert_eq!(sp2.duration_ms, i64::MAX);

        let sp3b = batch
            .spans
            .iter()
            .find(|s| s.span_id == "sp_ext_3b")
            .unwrap();
        assert_eq!(sp3b.total_tokens, i64::MAX);
        assert_eq!(sp3b.tool_retry_count, i32::MAX);

        let sp4 = batch
            .spans
            .iter()
            .find(|s| s.span_id == "sp_ext_4")
            .unwrap();
        assert_eq!(sp4.start_ts_ms, i64::MAX);
        assert_eq!(sp4.end_ts_ms, None);
        assert_eq!(sp4.duration_ms, 0);
        assert_eq!(sp4.tool_retry_count, 0);

        let tr1 = batch
            .traces
            .iter()
            .find(|t| t.trace_id == "tr_ext_1")
            .unwrap();
        assert_eq!(tr1.duration_ms, i64::MAX);

        let tr2 = batch
            .traces
            .iter()
            .find(|t| t.trace_id == "tr_ext_2")
            .unwrap();
        assert_eq!(tr2.duration_ms, i64::MAX);

        let tr3 = batch
            .traces
            .iter()
            .find(|t| t.trace_id == "tr_ext_3")
            .unwrap();
        assert_eq!(tr3.start_ts_ms, i64::MIN);
        assert_eq!(tr3.end_ts_ms, Some(i64::MAX));
        assert_eq!(tr3.duration_ms, i64::MAX);
        assert_eq!(tr3.total_tokens, i64::MAX);

        // Direct nanos_to_ms checks on boundary inputs
        assert_eq!(nanos_to_ms(&json!("-9223372036854775808000000")), i64::MIN);
        assert_eq!(nanos_to_ms(&json!("9223372036854775808000000")), i64::MAX);
        assert_eq!(
            nanos_to_ms(&json!("99999999999999999999999999999999999999999")),
            0
        );
        assert_eq!(nanos_to_ms(&json!(u64::MAX)), (u64::MAX / 1_000_000) as i64);
    }
}
