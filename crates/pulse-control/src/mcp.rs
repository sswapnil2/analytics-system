use chrono::Utc;
use pulse_exec::{
    FlowDirection, FlowsQuery, FunnelOrderMode, FunnelQuery, FunnelStepSpec, GlobalFilter,
    InsightsMetric, InsightsQuery, InsightsSeriesSpec, QueryEngine, RetentionMode, RetentionQuery,
    SessionSearchQuery, TimeInterval, TimeRange,
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
/// `project_id` in tool arguments to a different project is rejected.
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
                        "description": "Lists the events and properties this project tracks, with descriptions and 24-hour volume. Call this first so you use exact event names in the other tools.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {}
                        }
                    },
                    {
                        "name": "query_insights",
                        "description": "Counts how often an event happened over time: unique users, total events, sessions, or daily/weekly/monthly active users, per day. Use it for questions like 'how many people paid last week' or 'is signup growing'. Set compare_previous_period to get the preceding period of the same length for a week-over-week answer, and breakdown_by to split by a property (prefix user properties with 'user.', e.g. 'user.plan').",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "event_name": { "type": "string", "description": "Exact event name from get_lexicon_schema." },
                                "metric": { "type": "string", "enum": ["uniques", "totals", "sessions", "dau", "wau", "mau"], "default": "uniques", "description": "uniques = distinct users; totals = event count; sessions = distinct sessions." },
                                "days": { "type": "integer", "default": 14, "description": "Look back this many days, ending today in the project's time zone. Ignored when start_ts_ms and end_ts_ms are given." },
                                "start_ts_ms": { "type": "integer", "description": "Absolute range start, Unix epoch milliseconds (inclusive). Use with end_ts_ms to compare specific weeks or a period before and after a release." },
                                "end_ts_ms": { "type": "integer", "description": "Absolute range end, Unix epoch milliseconds (exclusive)." },
                                "breakdown_by": { "type": "string", "description": "Event property, or user property as 'user.<name>'." },
                                "compare_previous_period": { "type": "boolean", "default": false, "description": "Also return the immediately preceding period of the same length." }
                            },
                            "required": ["event_name"]
                        }
                    },
                    {
                        "name": "query_funnel",
                        "description": "Measures how many users go through a sequence of events within 7 days, and where they drop off. Use it for 'what share of signups pay' or 'which onboarding step loses the most people'. Results are exact, not sampled.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "steps": { "type": "array", "items": { "type": "string" }, "minItems": 2, "description": "Exact event names, in order." },
                                "order_mode": { "type": "string", "enum": ["loose_order", "strict_order", "any_order"], "default": "loose_order", "description": "loose_order: steps in order, other events allowed in between; strict_order: no other events in between; any_order: steps in any order." },
                                "days": { "type": "integer", "default": 14, "description": "Look back this many days, ending today in the project's time zone. Ignored when start_ts_ms and end_ts_ms are given." },
                                "start_ts_ms": { "type": "integer", "description": "Absolute range start, Unix epoch milliseconds (inclusive). Use with end_ts_ms to compare specific weeks or a period before and after a release." },
                                "end_ts_ms": { "type": "integer", "description": "Absolute range end, Unix epoch milliseconds (exclusive)." },
                                "breakdown_by": { "type": "string", "description": "Event property, or user property as 'user.<name>'." }
                            },
                            "required": ["steps"]
                        }
                    },
                    {
                        "name": "query_retention",
                        "description": "Measures whether users come back: of the users who did start_event on a given day, what share did return_event 1, 2, ... N days later. Use it for 'what is our day-7 retention'. return_event may be '$any_event' to count any activity.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "start_event": { "type": "string", "description": "Exact event name that puts a user in a cohort (e.g. a signup)." },
                                "return_event": { "type": "string", "description": "Exact event name that counts as coming back, or '$any_event'." },
                                "mode": { "type": "string", "enum": ["n_day", "unbounded", "frequency_stickiness"], "default": "n_day", "description": "n_day: returned on exactly day N; unbounded: returned on day N or later; frequency_stickiness: how many distinct days users returned." },
                                "num_periods": { "type": "integer", "default": 7, "description": "Number of days to follow each cohort." },
                                "days": { "type": "integer", "default": 14, "description": "Look back this many days, ending today in the project's time zone. Ignored when start_ts_ms and end_ts_ms are given." },
                                "start_ts_ms": { "type": "integer", "description": "Absolute range start, Unix epoch milliseconds (inclusive). Use with end_ts_ms to compare specific weeks or a period before and after a release." },
                                "end_ts_ms": { "type": "integer", "description": "Absolute range end, Unix epoch milliseconds (exclusive)." }
                            },
                            "required": ["start_event", "return_event"]
                        }
                    },
                    {
                        "name": "inspect_frustrated_sessions",
                        "description": "Finds recorded user sessions with frustration signals (rage taps, dead taps, errors), optionally only sessions that contain a given event. Use it to explain why users dropped off. Identifiers in the results are masked.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "event_name": { "type": "string" },
                                "only_rage_tap": { "type": "boolean", "default": true },
                                "only_dead_tap": { "type": "boolean", "default": false },
                                "only_errors": { "type": "boolean", "default": false },
                                "only_with_replay": { "type": "boolean", "default": false },
                                "limit": { "type": "integer", "default": 10 }
                            }
                        }
                    },
                    {
                        "name": "query_flows",
                        "description": "Shows the most common paths users take after (or before) an event: for each step, the top next events and how many users went there. Use it for 'what do people do after signing up' or 'what leads to checkout'.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "anchor_event": { "type": "string", "description": "Exact event name the paths start from (or end at, with direction reverse)." },
                                "direction": { "type": "string", "enum": ["forward", "reverse"], "default": "forward", "description": "forward: what happens after the anchor; reverse: what happens before it." },
                                "steps": { "type": "integer", "default": 3, "description": "How many steps to follow, 1 to 6." },
                                "top_n_per_step": { "type": "integer", "default": 5, "description": "Events kept per step; the rest are grouped." },
                                "days": { "type": "integer", "default": 14, "description": "Look back this many days, ending today in the project's time zone. Ignored when start_ts_ms and end_ts_ms are given." },
                                "start_ts_ms": { "type": "integer", "description": "Absolute range start, Unix epoch milliseconds (inclusive)." },
                                "end_ts_ms": { "type": "integer", "description": "Absolute range end, Unix epoch milliseconds (exclusive)." }
                            },
                            "required": ["anchor_event"]
                        }
                    },
                    {
                        "name": "get_user",
                        "description": "Looks up one user by distinct id: their profile properties and their most recent events (newest first). Use it to explain what a specific user did.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "distinct_id": { "type": "string", "description": "The user's id as sent in events or identify." },
                                "limit": { "type": "integer", "default": 50, "description": "Most recent events to return, up to 200." }
                            },
                            "required": ["distinct_id"]
                        }
                    },
                    {
                        "name": "list_property_values",
                        "description": "Lists the most common values of a property, with event counts. Call it before filtering or splitting so you use values that exist, e.g. the exact plan names.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "property": { "type": "string", "description": "Event property, or user property as 'user.<name>'." },
                                "event_name": { "type": "string", "description": "Only count this event. Default: all events." },
                                "limit": { "type": "integer", "default": 20, "description": "Values to return, up to 100." },
                                "days": { "type": "integer", "default": 14, "description": "Look back this many days, ending today in the project's time zone. Ignored when start_ts_ms and end_ts_ms are given." },
                                "start_ts_ms": { "type": "integer", "description": "Absolute range start, Unix epoch milliseconds (inclusive)." },
                                "end_ts_ms": { "type": "integer", "description": "Absolute range end, Unix epoch milliseconds (exclusive)." }
                            },
                            "required": ["property"]
                        }
                    },
                    {
                        "name": "list_cohorts",
                        "description": "Lists the saved cohorts (named groups of users) with their definitions and current size.",
                        "inputSchema": { "type": "object", "properties": {} }
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
                                "text": serde_json::to_string(&payload).unwrap_or_default()
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

fn default_range_days(args: &Value, tz: chrono_tz::Tz) -> TimeRange {
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
    let today_floor = TimeInterval::Day.align_floor_tz(now, tz);
    let mut start = today_floor;
    for _ in 1..days {
        start = TimeInterval::Day.align_floor_tz(start - 1, tz);
    }
    TimeRange {
        start_ts_ms: start,
        end_ts_ms: TimeInterval::Day
            .next_bucket_tz(today_floor, tz)
            .unwrap_or(today_floor + 86_400_000),
    }
}

/// Every event name the project can query: stored events, Lexicon entries and custom events.
async fn known_event_names(
    project_id: &str,
    meta: &MetaStore,
    qengine: &QueryEngine,
) -> Result<Vec<String>, String> {
    let mut names: std::collections::BTreeSet<String> = qengine
        .storage()
        .get_or_open_project(project_id)
        .await
        .map_err(|e| e.to_string())?
        .dictionary()
        .all_events()
        .into_iter()
        .map(|(_, n)| n)
        .collect();
    names.extend(
        meta.list_lexicon_events(project_id)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|e| e.name),
    );
    let cat = qengine.catalogs().for_project(project_id);
    names.extend(cat.custom_events.iter().map(|e| e.key().clone()));
    Ok(names.into_iter().collect())
}

/// Refuses names the project has never seen, so a typo is an error rather than a confident 0.
fn check_event_names<'a>(
    used: impl IntoIterator<Item = &'a str>,
    known: &[String],
) -> Result<(), String> {
    for name in used {
        if name == "$any_event" || known.iter().any(|k| k == name) {
            continue;
        }
        let closest = known
            .iter()
            .map(|k| (edit_distance(&k.to_lowercase(), &name.to_lowercase()), k))
            .min_by_key(|(d, _)| *d)
            .filter(|(d, k)| *d <= (k.chars().count() / 3).max(2));
        return Err(match closest {
            Some((_, k)) => format!(
                "unknown event '{name}'. Did you mean '{k}'? Call get_lexicon_schema for the exact names."
            ),
            None => format!(
                "unknown event '{name}': this project has no such event. Call get_lexicon_schema for the exact names."
            ),
        });
    }
    Ok(())
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            cur[j + 1] = (prev[j] + usize::from(ca != *cb))
                .min(prev[j + 1] + 1)
                .min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

fn pick<T: Copy>(args: &Value, key: &str, default: T, options: &[(&str, T)]) -> Result<T, String> {
    match args.get(key).and_then(|v| v.as_str()) {
        None => Ok(default),
        Some(raw) => options
            .iter()
            .find(|(n, _)| *n == raw)
            .map(|(_, v)| *v)
            .ok_or_else(|| {
                let allowed: Vec<&str> = options.iter().map(|(n, _)| *n).collect();
                format!(
                    "invalid {key} '{raw}': expected one of {}",
                    allowed.join(", ")
                )
            }),
    }
}

async fn invoke_tool(
    tool_name: &str,
    project_id: &str,
    args: &Value,
    meta: &MetaStore,
    qengine: &QueryEngine,
) -> Result<Value, String> {
    let tz = qengine.catalogs().for_project(project_id).timezone();
    let range = default_range_days(args, tz);
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
                let metric = match args.get("metric").and_then(|v| v.as_str()) {
                    None | Some("uniques") => InsightsMetric::Uniques,
                    Some("totals") => InsightsMetric::Totals,
                    Some("sessions") => InsightsMetric::Sessions,
                    Some("dau") => InsightsMetric::Dau,
                    Some("wau") => InsightsMetric::Wau,
                    Some("mau") => InsightsMetric::Mau,
                    Some(other) => {
                        return Err(format!(
                            "invalid metric '{other}': expected one of uniques, totals, sessions, dau, wau, mau"
                        ))
                    }
                };
                let breakdown_by = args
                    .get("breakdown_by")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| vec![s.to_string()])
                    .unwrap_or_default();
                InsightsQuery {
                    time_range: range,
                    interval: TimeInterval::Day,
                    series: vec![InsightsSeriesSpec {
                        label: "A".to_string(),
                        event_name: ev,
                        metric,
                        filters: Vec::new(),
                    }],
                    breakdown_by,
                    global_filter: GlobalFilter::default(),
                    compare_previous_period: args
                        .get("compare_previous_period")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    formula: None,
                    sampling: Default::default(),
                }
            };
            let known = known_event_names(project_id, meta, qengine).await?;
            check_event_names(q.series.iter().map(|s| s.event_name.as_str()), &known)?;
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
                let order_mode = pick(
                    args,
                    "order_mode",
                    FunnelOrderMode::LooseOrder,
                    &[
                        ("loose_order", FunnelOrderMode::LooseOrder),
                        ("strict_order", FunnelOrderMode::StrictOrder),
                        ("any_order", FunnelOrderMode::AnyOrder),
                    ],
                )?;
                FunnelQuery {
                    time_range: range,
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
                    sampling: Default::default(),
                }
            };
            let known = known_event_names(project_id, meta, qengine).await?;
            check_event_names(q.steps.iter().map(|s| s.event_name.as_str()), &known)?;
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
            let mode = pick(
                args,
                "mode",
                RetentionMode::NDay,
                &[
                    ("n_day", RetentionMode::NDay),
                    ("unbounded", RetentionMode::Unbounded),
                    ("frequency_stickiness", RetentionMode::FrequencyStickiness),
                ],
            )?;
            let known = known_event_names(project_id, meta, qengine).await?;
            check_event_names([start_event.as_str(), return_event.as_str()], &known)?;
            let num_periods = args
                .get("num_periods")
                .and_then(|v| v.as_u64())
                .unwrap_or(7) as usize;
            let q = RetentionQuery {
                time_range: range,
                start_event,
                return_event,
                mode,
                unit: TimeInterval::Day,
                num_periods,
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            };
            let res = qengine
                .query_retention(project_id, &q)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(res).map_err(|e| e.to_string())?)
        }
        "inspect_frustrated_sessions" => {
            let sq = SessionSearchQuery {
                time_range: Some(range),
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
                only_with_replay: args
                    .get("only_with_replay")
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
        "query_flows" => {
            let anchor_event = args
                .get("anchor_event")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing required 'anchor_event'".to_string())?
                .to_string();
            let direction = pick(
                args,
                "direction",
                FlowDirection::Forward,
                &[
                    ("forward", FlowDirection::Forward),
                    ("reverse", FlowDirection::Reverse),
                ],
            )?;
            let known = known_event_names(project_id, meta, qengine).await?;
            check_event_names([anchor_event.as_str()], &known)?;
            let q = FlowsQuery {
                time_range: range,
                anchor_event,
                direction,
                steps: args
                    .get("steps")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(3)
                    .clamp(1, 6) as usize,
                top_n_per_step: Some(
                    args.get("top_n_per_step")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(5)
                        .clamp(1, 20) as usize,
                ),
                global_filter: GlobalFilter::default(),
                sampling: Default::default(),
            };
            let res = qengine
                .query_flows(project_id, &q)
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(res).map_err(|e| e.to_string())?)
        }
        "get_user" => {
            let distinct_id = args
                .get("distinct_id")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing required 'distinct_id'".to_string())?;
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(50)
                .clamp(1, 200) as usize;
            let pstore = qengine
                .storage()
                .profile_router()
                .get_or_open(project_id)
                .await
                .map_err(|e| e.to_string())?;
            let profile = pstore
                .get_by_distinct_id(distinct_id)
                .await
                .map_err(|e| e.to_string())?
                .map(crate::ControlPlaneService::from_stored_profile);
            let id_graph = qengine
                .storage()
                .identity_router()
                .get_or_open(project_id)
                .await
                .map_err(|e| e.to_string())?;
            let canon = id_graph
                .lookup_canonical(distinct_id)
                .or_else(|| {
                    id_graph
                        .lookup_raw(distinct_id)
                        .map(|u| id_graph.snapshot().resolve(u))
                })
                .or_else(|| profile.as_ref().map(|p| p.canonical_uid));
            let Some(canon) = canon else {
                return Err(format!(
                    "no user with distinct_id '{distinct_id}' in this project"
                ));
            };
            let events = qengine
                .export_events(project_id, None, Some(canon), limit, true)
                .await
                .map_err(|e| e.to_string())?;
            // Every id merged into this user contributes; the newest value of each key wins.
            let rows: Vec<_> = pstore
                .list_for_canonical(canon)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(crate::ControlPlaneService::from_stored_profile)
                .collect();
            let merged = crate::api::merge_profile_rows(rows).remove(&canon);
            Ok(json!({
                "distinct_id": distinct_id,
                "properties": merged.as_ref().map(|p| &p.props),
                "total_revenue": merged.as_ref().map(|p| p.total_revenue),
                "recent_events": events,
            }))
        }
        "list_property_values" => {
            let property = args
                .get("property")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "missing required 'property'".to_string())?
                .to_string();
            let event_name = args
                .get("event_name")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or(pulse_exec::ANY_EVENT)
                .to_string();
            let known = known_event_names(project_id, meta, qengine).await?;
            check_event_names([event_name.as_str()], &known)?;
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(20)
                .clamp(1, 100) as usize;
            let q = InsightsQuery {
                time_range: range,
                interval: TimeInterval::Day,
                series: vec![InsightsSeriesSpec {
                    label: "A".to_string(),
                    event_name,
                    metric: InsightsMetric::Totals,
                    filters: Vec::new(),
                }],
                breakdown_by: vec![property.clone()],
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
                sampling: Default::default(),
            };
            let res = qengine
                .query_insights(project_id, &q)
                .await
                .map_err(|e| e.to_string())?;
            let mut values: Vec<(String, f64)> = res
                .series
                .into_iter()
                .map(|s| (s.breakdown_key, s.overall_value.unwrap_or(0.0)))
                .filter(|(_, n)| *n > 0.0)
                .collect();
            values.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let total = values.len();
            values.truncate(limit);
            Ok(json!({
                "property": property,
                "distinct_values": total,
                "values": values.into_iter().map(|(v, n)| json!({"value": v, "events": n})).collect::<Vec<_>>(),
            }))
        }
        "list_cohorts" => {
            let cohorts = meta
                .list_cohorts(project_id)
                .await
                .map_err(|e| e.to_string())?;
            let mut out = Vec::with_capacity(cohorts.len());
            for c in cohorts {
                let size = qengine
                    .evaluate_saved_cohort_uids(project_id, &c.id)
                    .await
                    .map(|v| v.len())
                    .ok();
                out.push(json!({
                    "id": c.id,
                    "name": c.name,
                    "description": c.description,
                    // A fixed list would be every uid; describe it instead.
                    "definition": match &c.definition {
                        pulse_exec::BehavioralCohortDef::Users { canonical_uids } => {
                            json!({"kind": "users", "fixed_list_of": canonical_uids.len()})
                        }
                        d => json!(d),
                    },
                    "users": size,
                }));
            }
            Ok(json!({ "cohorts": out }))
        }
        other => Err(format!("unknown MCP tool: {other}")),
    }
}
