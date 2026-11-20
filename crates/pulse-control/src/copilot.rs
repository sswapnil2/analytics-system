use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use pulse_exec::{
    FlowDirection, FlowsQuery, FunnelOrderMode, FunnelQuery, FunnelResult, FunnelStepSpec,
    GlobalFilter, InsightsMetric, InsightsQuery, InsightsSeriesSpec, QueryEngine, RetentionMode,
    RetentionQuery, SessionSearchQuery, TimeInterval, TimeRange,
};
use pulse_storage::{allow_private_endpoints, check_resolved_addrs, validate_outbound_url_ssrf};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::db::MetaStore;
use crate::models::ByokCredentialRecord;

/// Validates a customer-supplied outbound URL and returns a client that can only reach the
/// addresses that passed the SSRF check: redirects are off, and DNS for the host is pinned to the
/// checked IPs so a rebinding name can't swap in an internal address after validation.
pub(crate) async fn pinned_outbound_client(
    raw_url: &str,
    timeout: std::time::Duration,
) -> Result<(reqwest::Url, reqwest::Client), String> {
    let url = validate_outbound_url_ssrf(raw_url).map_err(|e| e.to_string())?;
    let mut builder = reqwest::Client::builder()
        .timeout(timeout)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none());
    if let (Some(host), false) = (url.domain(), allow_private_endpoints()) {
        let port = url.port_or_known_default().unwrap_or(443);
        let addrs = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| format!("outbound host '{host}' could not be resolved: {e}"))?;
        let addrs = check_resolved_addrs(host, addrs).map_err(|e| e.to_string())?;
        builder = builder.resolve_to_addrs(host, &addrs);
    }
    let client = builder.build().map_err(|e| e.to_string())?;
    Ok((url, client))
}

#[derive(Debug, Clone, Deserialize)]
pub struct CopilotRequest {
    pub prompt: String,
    #[serde(default)]
    pub start_ts_ms: Option<i64>,
    #[serde(default)]
    pub end_ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopilotResponse {
    pub provider: String,
    pub model: String,
    pub answer_markdown: String,
    pub root_cause_step: Option<String>,
    pub dropoff_rate_pct: Option<f64>,
    pub cited_sessions: Vec<String>,
    pub tools_executed: Vec<String>,
    pub metrics_summary: Value,
    pub exec_time_ms: u64,
}

#[derive(Debug)]
pub enum CopilotError {
    BadRequest(String),
    Upstream(String),
}

impl std::fmt::Display for CopilotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(s) | Self::Upstream(s) => write!(f, "{s}"),
        }
    }
}

async fn select_events_for_prompt(
    qengine: &Arc<QueryEngine>,
    project_id: &str,
    prompt_lower: &str,
) -> Vec<String> {
    let mut dict_events: Vec<(u16, String)> = Vec::new();
    if let Ok(ps) = qengine.storage().get_or_open_project(project_id).await {
        dict_events = ps.dictionary().all_events();
        dict_events.sort_by_key(|(eid, _)| *eid);
    }
    let all_names: Vec<String> = dict_events
        .into_iter()
        .map(|(_, n)| n)
        .filter(|n| !n.starts_with('$'))
        .collect();

    // First prefer any project event names explicitly referenced by the user's question.
    let mut mentioned: Vec<String> = all_names
        .iter()
        .filter(|name| {
            let nl = name.to_ascii_lowercase();
            let spaced = nl.replace('_', " ");
            prompt_lower.contains(&nl) || prompt_lower.contains(&spaced)
        })
        .cloned()
        .collect();
    mentioned.truncate(4);
    if mentioned.len() >= 2 {
        return mentioned;
    }

    // Otherwise select up to 4 events from the project's actual event dictionary in registration order.
    let mut selected = mentioned;
    for name in all_names {
        if !selected.contains(&name) {
            selected.push(name);
            if selected.len() >= 4 {
                break;
            }
        }
    }
    selected
}

fn find_biggest_dropoff(funnel: &FunnelResult) -> Option<(String, f64)> {
    let mut worst: Option<(String, f64)> = None;
    for pair in funnel.overall_steps.windows(2) {
        let prev = pair[0].entered_users;
        let next = pair[1].entered_users;
        if prev == 0 {
            continue;
        }
        let drop_pct = ((prev.saturating_sub(next)) as f64 / (prev as f64)) * 100.0;
        let step_label = format!("{} -> {}", pair[0].event_name, pair[1].event_name);
        if worst
            .as_ref()
            .is_none_or(|(_, best_pct)| drop_pct > *best_pct)
        {
            worst = Some((step_label, drop_pct));
        }
    }
    worst
}

/// Executes question-driven analytics tools against `QueryEngine` and sends the PII-safe tool
/// context + prompt directly to the customer's configured LLM provider (`H4`).
/// Fails explicitly (`CopilotError::Upstream`) if the external LLM call fails — never returns fake templated text.
pub async fn run_byok_copilot(
    qengine: &Arc<QueryEngine>,
    _meta: &MetaStore,
    project_id: &str,
    llm_cred: &ByokCredentialRecord,
    llm_api_key: &str,
    req: &CopilotRequest,
) -> Result<CopilotResponse, CopilotError> {
    let t0 = Instant::now();
    let prompt = req.prompt.trim();
    if prompt.is_empty() {
        return Err(CopilotError::BadRequest("prompt cannot be empty".into()));
    }
    let prompt_lower = prompt.to_ascii_lowercase();

    let now_ms = Utc::now().timestamp_millis();
    let seg_min_ts = qengine
        .storage()
        .get_or_open_project(project_id)
        .await
        .ok()
        .and_then(|ps| {
            ps.snapshot()
                .manifest
                .active_segments
                .values()
                .flatten()
                .map(|m| m.footer.min_ts_ms)
                .min()
        });
    let start_ts_ms = req.start_ts_ms.unwrap_or_else(|| {
        seg_min_ts
            .unwrap_or(0)
            .min(now_ms - 365 * 86_400_000)
            .max(0)
    });
    let end_ts_ms = req
        .end_ts_ms
        .unwrap_or(now_ms + 86_400_000)
        .max(start_ts_ms);
    let range = TimeRange {
        start_ts_ms,
        end_ts_ms,
    };

    let selected_events = select_events_for_prompt(qengine, project_id, &prompt_lower).await;
    if selected_events.is_empty() {
        return Err(CopilotError::BadRequest(
            "project has no ingested non-system events yet".into(),
        ));
    }

    let mut tools_executed = Vec::new();
    let mut cited_sessions: Vec<String> = Vec::new();
    let mut metrics_map = serde_json::Map::new();

    // 1. Always gather question-relevant Insights series counts.
    let ins_series: Vec<InsightsSeriesSpec> = selected_events
        .iter()
        .take(3)
        .enumerate()
        .map(|(i, ev)| InsightsSeriesSpec {
            label: format!("S{}", i + 1),
            event_name: ev.clone(),
            metric: InsightsMetric::Uniques,
            filters: Vec::new(),
        })
        .collect();
    if let Ok(ins) = qengine
        .query_insights(
            project_id,
            &InsightsQuery {
                time_range: range,
                interval: TimeInterval::Day,
                series: ins_series,
                breakdown_by: if prompt_lower.contains("plan") {
                    vec!["plan".to_string()]
                } else {
                    Vec::new()
                },
                global_filter: GlobalFilter::default(),
                compare_previous_period: false,
                formula: None,
            },
        )
        .await
    {
        tools_executed.push("query_insights".to_string());
        metrics_map.insert(
            "insights".to_string(),
            json!(ins
                .series
                .iter()
                .map(|s| json!({
                    "series": s.label,
                    "breakdown": s.breakdown_key,
                    "uniques": s.overall_value,
                }))
                .collect::<Vec<_>>()),
        );
    }

    // 2. Execute Funnel if >= 2 events exist
    let mut root_cause_step = None;
    let mut dropoff_rate_pct = None;
    if selected_events.len() >= 2 {
        let funnel_res = qengine
            .query_funnel(
                project_id,
                &FunnelQuery {
                    time_range: range,
                    steps: selected_events
                        .iter()
                        .map(|name| FunnelStepSpec {
                            event_name: name.clone(),
                            filters: Vec::new(),
                            first_time_ever: false,
                        })
                        .collect(),
                    order_mode: FunnelOrderMode::LooseOrder,
                    conversion_window_ms: 14 * 86_400_000,
                    exclusions: Vec::new(),
                    hold_constant_props: Vec::new(),
                    breakdown_by: None,
                    global_filter: GlobalFilter::default(),
                },
            )
            .await
            .map_err(|e| CopilotError::BadRequest(e.to_string()))?;

        tools_executed.push("query_funnel".to_string());
        if let Some((step, pct)) = find_biggest_dropoff(&funnel_res) {
            root_cause_step = Some(step);
            dropoff_rate_pct = Some(pct);
        }
        for s in &funnel_res.dropped_sample_sessions {
            if !cited_sessions.contains(&s.session_id) && cited_sessions.len() < 4 {
                cited_sessions.push(s.session_id.clone());
            }
        }
        metrics_map.insert(
            "funnel_steps".to_string(),
            json!(funnel_res
                .overall_steps
                .iter()
                .map(|s| json!({
                    "step": s.event_name,
                    "entered_users": s.entered_users,
                    "dropped_users": s.dropped_users,
                    "conversion_from_start_pct": (s.conversion_from_start_pct * 100.0).round() / 100.0,
                }))
                .collect::<Vec<_>>()),
        );
    }

    // 3. Conditionally run Retention / Flows / Sessions based on user's question
    if (prompt_lower.contains("reten")
        || prompt_lower.contains("retain")
        || prompt_lower.contains("churn")
        || prompt_lower.contains("cohort"))
        && selected_events.len() >= 2
    {
        if let Ok(ret) = qengine
            .query_retention(
                project_id,
                &RetentionQuery {
                    time_range: range,
                    start_event: selected_events[0].clone(),
                    return_event: selected_events.last().cloned().unwrap(),
                    mode: RetentionMode::NDay,
                    unit: TimeInterval::Day,
                    num_periods: 14,
                    global_filter: GlobalFilter::default(),
                },
            )
            .await
        {
            tools_executed.push("query_retention".to_string());
            metrics_map.insert(
                "retention_cohorts".to_string(),
                json!(ret
                    .cohorts
                    .iter()
                    .take(5)
                    .map(|c| json!({
                        "label": c.cohort_label,
                        "size": c.cohort_size,
                        "pcts": c.retained_pcts,
                    }))
                    .collect::<Vec<_>>()),
            );
        }
    }

    if prompt_lower.contains("flow")
        || prompt_lower.contains("path")
        || prompt_lower.contains("journey")
        || prompt_lower.contains("next")
    {
        if let Ok(fl) = qengine
            .query_flows(
                project_id,
                &FlowsQuery {
                    time_range: range,
                    anchor_event: selected_events[0].clone(),
                    direction: FlowDirection::Forward,
                    steps: 4,
                    top_n_per_step: Some(8),
                    global_filter: GlobalFilter::default(),
                },
            )
            .await
        {
            tools_executed.push("query_flows".to_string());
            metrics_map.insert(
                "top_flows".to_string(),
                json!(fl.links.iter().take(10).collect::<Vec<_>>()),
            );
        }
    }

    let err_sessions = qengine
        .search_sessions(
            project_id,
            &SessionSearchQuery {
                time_range: None,
                event_name: None,
                only_rage_tap: false,
                only_dead_tap: false,
                only_errors: true,
                only_with_replay: false,
                limit: Some(5),
            },
        )
        .await
        .map_err(|e| CopilotError::BadRequest(e.to_string()))?;
    tools_executed.push("search_sessions".to_string());
    for s in &err_sessions.sessions {
        if !cited_sessions.contains(&s.session_id) && cited_sessions.len() < 6 {
            cited_sessions.push(s.session_id.clone());
        }
    }
    metrics_map.insert(
        "error_sessions_found".to_string(),
        json!(err_sessions.sessions.len()),
    );

    if prompt_lower.contains("rev")
        || prompt_lower.contains("arpu")
        || prompt_lower.contains("ltv")
        || prompt_lower.contains("pay")
    {
        let (_, paying, total_rev) = match qengine
            .storage()
            .profile_router()
            .get_or_open(project_id)
            .await
        {
            Ok(ps) => ps.revenue_summary().await.unwrap_or((0, 0, 0.0)),
            Err(_) => (0, 0, 0.0),
        };
        metrics_map.insert(
            "revenue".to_string(),
            json!({ "total_revenue": total_rev, "paying_users": paying }),
        );
    }

    let (model, custom_endpoint) =
        llm_model_and_endpoint(&llm_cred.provider, &llm_cred.config_json);
    let custom_endpoint = custom_endpoint.as_deref();

    let metrics_summary = Value::Object(metrics_map);

    let answer_markdown = call_external_llm(
        &llm_cred.provider,
        &model,
        llm_api_key,
        custom_endpoint,
        prompt,
        &metrics_summary,
        &cited_sessions,
    )
    .await?;

    Ok(CopilotResponse {
        provider: llm_cred.provider.clone(),
        model,
        answer_markdown,
        root_cause_step,
        dropoff_rate_pct,
        cited_sessions,
        tools_executed,
        metrics_summary,
        exec_time_ms: t0.elapsed().as_millis() as u64,
    })
}

/// The model and optional custom endpoint from an LLM credential's config.
pub(crate) fn llm_model_and_endpoint(provider: &str, cfg: &Value) -> (String, Option<String>) {
    let model = cfg
        .get("model")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(match provider {
            "anthropic" => "claude-sonnet-4-5",
            "gemini" => "gemini-2.5-flash",
            _ => "gpt-4o-mini",
        })
        .to_string();
    let endpoint = cfg
        .get("endpoint")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    (model, endpoint)
}

pub(crate) async fn call_external_llm(
    provider: &str,
    model: &str,
    api_key: &str,
    custom_endpoint: Option<&str>,
    prompt: &str,
    metrics: &Value,
    cited_sessions: &[String],
) -> Result<String, CopilotError> {
    let default_url = match provider {
        "anthropic" => "https://api.anthropic.com/v1/messages".to_string(),
        "gemini" => format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        ),
        _ => "https://api.openai.com/v1/chat/completions".to_string(),
    };
    let target_url = custom_endpoint.map(str::to_string).unwrap_or(default_url);
    let (_, client) = pinned_outbound_client(&target_url, std::time::Duration::from_secs(15))
        .await
        .map_err(|e| {
            CopilotError::BadRequest(format!("LLM endpoint blocked by SSRF policy: {e}"))
        })?;

    let system_prompt = "You are Pulse Analytics BYOK Copilot. Note: session IDs and aggregate query JSON are sent here under customer BYOK consent. Answer the user's question directly in concise Markdown, referencing exact metrics and session IDs from the provided tool outputs.";
    let user_msg =
        format!("Question: {prompt}\nMetrics: {metrics}\nCited Sessions: {cited_sessions:?}");

    match provider {
        "anthropic" => {
            let res = client
                .post(&target_url)
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01")
                .json(&json!({
                    "model": model,
                    "max_tokens": 500,
                    "system": system_prompt,
                    "messages": [{ "role": "user", "content": user_msg }],
                }))
                .send()
                .await
                .map_err(|e| CopilotError::Upstream(format!("anthropic request failed: {e}")))?;
            if !res.status().is_success() {
                return Err(CopilotError::Upstream(format!(
                    "anthropic returned HTTP {}",
                    res.status()
                )));
            }
            let body: Value = res
                .json()
                .await
                .map_err(|e| CopilotError::Upstream(e.to_string()))?;
            body["content"][0]["text"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| CopilotError::Upstream("missing text in anthropic response".into()))
        }
        "gemini" => {
            let res = client
                .post(&target_url)
                .header("x-goog-api-key", api_key)
                .json(&json!({
                    "contents": [{ "parts": [{ "text": format!("{system_prompt}\n\n{user_msg}") }] }]
                }))
                .send()
                .await
                .map_err(|e| CopilotError::Upstream(format!("gemini request failed: {e}")))?;
            if !res.status().is_success() {
                return Err(CopilotError::Upstream(format!(
                    "gemini returned HTTP {}",
                    res.status()
                )));
            }
            let body: Value = res
                .json()
                .await
                .map_err(|e| CopilotError::Upstream(e.to_string()))?;
            body["candidates"][0]["content"]["parts"][0]["text"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| CopilotError::Upstream("missing text in gemini response".into()))
        }
        _ => {
            let res = client
                .post(&target_url)
                .bearer_auth(api_key)
                .json(&json!({
                    "model": model,
                    "messages": [
                        { "role": "system", "content": system_prompt },
                        { "role": "user", "content": user_msg }
                    ],
                    "max_tokens": 500
                }))
                .send()
                .await
                .map_err(|e| CopilotError::Upstream(format!("openai request failed: {e}")))?;
            if !res.status().is_success() {
                return Err(CopilotError::Upstream(format!(
                    "openai returned HTTP {}",
                    res.status()
                )));
            }
            let body: Value = res
                .json()
                .await
                .map_err(|e| CopilotError::Upstream(e.to_string()))?;
            body["choices"][0]["message"]["content"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| CopilotError::Upstream("missing content in openai response".into()))
        }
    }
}
