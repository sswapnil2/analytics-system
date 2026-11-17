use chrono::Utc;
use pulse_exec::{
    FunnelOrderMode, FunnelQuery, FunnelStepSpec, GlobalFilter, InsightsMetric, InsightsQuery,
    InsightsSeriesSpec, QueryEngine, RetentionMode, RetentionQuery, SessionSearchQuery,
    TimeInterval, TimeRange,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::db::MetaStore;

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default = "default_jsonrpc")]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

fn default_jsonrpc() -> String {
    "2.0".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

impl JsonRpcResponse {
    pub fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
            }),
        }
    }
}

/// Built-in Model Context Protocol (MCP) JSON-RPC 2.0 handler (`initialize`, `tools/list`, `tools/call`).
///
/// Strictly bound to `bound_project_id` of the authenticated principal; any attempt to override
/// `project_id` in tool arguments to a different project is rejected (`P0-04`).
pub async fn handle_mcp_request(
    req: JsonRpcRequest,
    bound_project_id: &str,
    meta: &MetaStore,
    qengine: &QueryEngine,
) -> JsonRpcResponse {
    let _ = &req.jsonrpc;
    match req.method.as_str() {
        "initialize" => JsonRpcResponse::ok(
            req.id,
            json!({
                "protocolVersion": negotiate_protocol_version(&req.params),
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "pulse-analytics-mcp",
                    "version": "0.1.0"
                }
            }),
        ),
        "ping" => JsonRpcResponse::ok(req.id, json!({})),
        "tools/list" => JsonRpcResponse::ok(
            req.id,
            json!({
                "tools": [
                    {
                        "name": "get_lexicon_schema",
                        "description": "Returns governed event and property schema definitions from the Pulse Lexicon for the authenticated project.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {}
                        }
                    },
                    {
                        "name": "query_insights",
                        "description": "Executes an Insights time-series & breakdown query (Uniques, Totals, Sessions, DAU/WAU/MAU, percentiles).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "event_name": { "type": "string" },
                                "metric": { "type": "string", "enum": ["uniques", "totals", "sessions", "dau", "wau", "mau"] },
                                "days": { "type": "integer", "default": 14 },
                                "breakdown_by": { "type": "string" }
                            },
                            "required": ["event_name"]
                        }
                    },
                    {
                        "name": "query_funnel",
                        "description": "Executes an exact unsampled u32 + RoaringBitmap Funnel conversion query across ordered steps.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "steps": { "type": "array", "items": { "type": "string" } },
                                "order_mode": { "type": "string", "enum": ["loose_order", "strict_order", "any_order"] },
                                "days": { "type": "integer", "default": 14 },
                                "breakdown_by": { "type": "string" }
                            },
                            "required": ["steps"]
                        }
                    },
                    {
                        "name": "query_retention",
                        "description": "Computes exact cohort retention matrix (n_day, unbounded, or frequency_stickiness).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "start_event": { "type": "string" },
                                "return_event": { "type": "string" },
                                "mode": { "type": "string", "enum": ["n_day", "unbounded", "frequency_stickiness"] },
                                "num_periods": { "type": "integer", "default": 7 },
                                "days": { "type": "integer", "default": 14 }
                            },
                            "required": ["start_event", "return_event"]
                        }
                    },
                    {
                        "name": "inspect_frustrated_sessions",
                        "description": "Uses 64-bit SessionIndex Bloom pruning to surface sessions with Rage Taps, Dead Taps, or errors.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "event_name": { "type": "string" },
                                "only_rage_tap": { "type": "boolean", "default": true },
                                "only_dead_tap": { "type": "boolean", "default": false },
                                "only_errors": { "type": "boolean", "default": false },
                                "limit": { "type": "integer", "default": 10 }
                            }
                        }
                    }
                ]
            }),
        ),
        "tools/call" => {
            let Some(tool_name) = req.params.get("name").and_then(|v| v.as_str()) else {
                return JsonRpcResponse::err(req.id, -32602, "missing tool 'name'");
            };
            let args = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));

            if let Some(arg_pid) = args
                .get("project_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                if arg_pid != bound_project_id {
                    return JsonRpcResponse::err(
                        req.id,
                        -32602,
                        "project_id argument cannot override authenticated principal scope",
                    );
                }
            }

            match invoke_tool(tool_name, bound_project_id, &args, meta, qengine).await {
                Ok(payload) => JsonRpcResponse::ok(
                    req.id,
                    json!({
                        "content": [
                            {
                                "type": "text",
                                "text": serde_json::to_string_pretty(&payload).unwrap_or_default()
                            }
                        ],
                        "structuredContent": payload
                    }),
                ),
                // Unknown tool is a protocol error; anything else is a tool execution error the
                // model should see (MCP spec: `isError` result, not a JSON-RPC error).
                Err(msg) if msg.starts_with("unknown MCP tool") => {
                    JsonRpcResponse::err(req.id, -32602, msg)
                }
                Err(msg) => JsonRpcResponse::ok(
                    req.id,
                    json!({
                        "content": [{ "type": "text", "text": msg }],
                        "isError": true
                    }),
                ),
            }
        }
        other => JsonRpcResponse::err(req.id, -32601, format!("method not found: {other}")),
    }
}

/// MCP revisions this server can speak; it has no SSE stream, sampling or resources, so the
/// differences between them do not affect it.
const SUPPORTED_PROTOCOL_VERSIONS: [&str; 4] =
    ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Echoes the client's requested protocol version when supported, otherwise offers the latest.
fn negotiate_protocol_version(params: &Value) -> &'static str {
    let requested = params.get("protocolVersion").and_then(|v| v.as_str());
    SUPPORTED_PROTOCOL_VERSIONS
        .into_iter()
        .find(|v| Some(*v) == requested)
        .unwrap_or(SUPPORTED_PROTOCOL_VERSIONS[0])
}

fn default_range_days(args: &Value) -> TimeRange {
    if let (Some(s), Some(e)) = (
        args.get("start_ts_ms").and_then(|v| v.as_i64()),
        args.get("end_ts_ms").and_then(|v| v.as_i64()),
    ) {
        return TimeRange {
            start_ts_ms: s,
            end_ts_ms: e,
        };
    }
    let days = args
        .get("days")
        .and_then(|v| v.as_i64())
        .unwrap_or(14)
        .clamp(1, 365);
    let now = Utc::now().timestamp_millis();
    let today_floor = TimeInterval::Day.align_floor(now);
    TimeRange {
        start_ts_ms: today_floor - (days - 1) * 86_400_000,
        end_ts_ms: today_floor + 86_400_000,
    }
}

async fn invoke_tool(
    tool_name: &str,
    project_id: &str,
    args: &Value,
    meta: &MetaStore,
    qengine: &QueryEngine,
) -> Result<Value, String> {
    match tool_name {
        "get_lexicon_schema" => {
            let events = meta
                .list_lexicon_events(project_id)
                .await
                .map_err(|e| e.to_string())?;
            let properties = meta
                .list_lexicon_properties(project_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({
                "project_id": project_id,
                "events": events,
                "properties": properties
            }))
        }
        "query_insights" => {
            let q: InsightsQuery = if args.get("series").is_some() {
                serde_json::from_value(args.clone()).map_err(|e| e.to_string())?
            } else {
                let ev = args
                    .get("event_name")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| "missing required 'event_name'".to_string())?
                    .to_string();
                let metric = match args
                    .get("metric")
                    .and_then(|v| v.as_str())
                    .unwrap_or("uniques")
                {
                    "totals" => InsightsMetric::Totals,
                    "sessions" => InsightsMetric::Sessions,
                    "dau" => InsightsMetric::Dau,
                    "wau" => InsightsMetric::Wau,
                    "mau" => InsightsMetric::Mau,
                    _ => InsightsMetric::Uniques,
                };
                let breakdown_by = args
                    .get("breakdown_by")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| vec![s.to_string()])
                    .unwrap_or_default();
                InsightsQuery {
                    time_range: default_range_days(args),
                    interval: TimeInterval::Day,
                    series: vec![InsightsSeriesSpec {
                        label: "A".to_string(),
                        event_name: ev,
                        metric,
                        filters: Vec::new(),
                    }],
                    breakdown_by,
                    global_filter: GlobalFilter::default(),
                    compare_previous_period: false,
                    formula: None,
                }
            };
            let res = qengine
                .query_insights(project_id, &q)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(res).map_err(|e| e.to_string())?)
        }
        "query_funnel" => {
            let q: FunnelQuery = if args
                .get("steps")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .is_some_and(|x| x.is_object())
            {
                serde_json::from_value(args.clone()).map_err(|e| e.to_string())?
            } else {
                let step_names: Vec<String> = args
                    .get("steps")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().filter(|s| !s.is_empty()).map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                if step_names.len() < 2 {
                    return Err("query_funnel requires at least 2 step event names".to_string());
                }
                let order_mode = match args
                    .get("order_mode")
                    .and_then(|v| v.as_str())
                    .unwrap_or("loose_order")
                {
                    "strict_order" => FunnelOrderMode::StrictOrder,
                    "any_order" => FunnelOrderMode::AnyOrder,
                    _ => FunnelOrderMode::LooseOrder,
                };
                FunnelQuery {
                    time_range: default_range_days(args),
                    steps: step_names
                        .into_iter()
                        .map(|event_name| FunnelStepSpec {
                            event_name,
                            filters: Vec::new(),
                            first_time_ever: false,
                        })
                        .collect(),
                    order_mode,
                    conversion_window_ms: 7 * 86_400_000,
                    exclusions: Vec::new(),
                    hold_constant_props: Vec::new(),
                    breakdown_by: args
                        .get("breakdown_by")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                    global_filter: GlobalFilter::default(),
                }
            };
            let res = qengine
                .query_funnel(project_id, &q)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(res).map_err(|e| e.to_string())?)
        }
        "query_retention" => {
            let start_event = args
                .get("start_event")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing required 'start_event'".to_string())?
                .to_string();
            let return_event = args
                .get("return_event")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing required 'return_event'".to_string())?
                .to_string();
            let mode = match args.get("mode").and_then(|v| v.as_str()).unwrap_or("n_day") {
                "unbounded" => RetentionMode::Unbounded,
                "frequency_stickiness" => RetentionMode::FrequencyStickiness,
                _ => RetentionMode::NDay,
            };
            let num_periods = args
                .get("num_periods")
                .and_then(|v| v.as_u64())
                .unwrap_or(7) as usize;
            let q = RetentionQuery {
                time_range: default_range_days(args),
                start_event,
                return_event,
                mode,
                unit: TimeInterval::Day,
                num_periods,
                global_filter: GlobalFilter::default(),
            };
            let res = qengine
                .query_retention(project_id, &q)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(res).map_err(|e| e.to_string())?)
        }
        "inspect_frustrated_sessions" => {
            let sq = SessionSearchQuery {
                time_range: Some(default_range_days(args)),
                event_name: args
                    .get("event_name")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
                only_rage_tap: args
                    .get("only_rage_tap")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                only_dead_tap: args
                    .get("only_dead_tap")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                only_errors: args
                    .get("only_errors")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                limit: Some(args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize),
            };
            let res = qengine
                .search_sessions(project_id, &sq)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(res).map_err(|e| e.to_string())?)
        }
        other => Err(format!("unknown MCP tool: {other}")),
    }
}
