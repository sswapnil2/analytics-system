//! Modular Evaluation Framework for AI Agent Executions.
//!
//! Supports deterministic, rule-based, schema, tool-quality, safety, performance, and LLM-as-judge
//! evaluators at Session, Trace, and Span target levels behind a unified `EvaluatorOutcome` contract.

use std::collections::HashMap;
use std::time::Instant;

use chrono::Utc;
use regex::Regex;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::model::{
    AgentEvaluation, AgentSpan, AgentTrace, EvalTargetType, EvaluatorConfig, EvaluatorType,
    ScoreType, SpanType, TraceStatus,
};
use crate::pricing::{cost_nanos_to_usd, usd_to_cost_nanos};
use crate::privacy::detect_pii_patterns;
use crate::stats::detect_execution_loop;

#[derive(Debug, Clone)]
pub struct EvalContext<'a> {
    pub trace: &'a AgentTrace,
    pub spans: &'a [AgentSpan],
    pub expected_output: Option<&'a Value>,
    pub expected_behavior: Option<&'a Value>,
}

#[derive(Debug, Clone)]
pub struct SpanEvalContext<'a> {
    pub span: &'a AgentSpan,
    pub trace: Option<&'a AgentTrace>,
    pub expected_output: Option<&'a Value>,
    pub expected_behavior: Option<&'a Value>,
}

#[derive(Debug, Clone)]
pub struct SessionEvalContext<'a> {
    pub project_id: &'a str,
    pub session_id: &'a str,
    pub traces: &'a [AgentTrace],
    pub spans: &'a [AgentSpan],
    pub expected_output: Option<&'a Value>,
    pub expected_behavior: Option<&'a Value>,
}

#[derive(Debug, Clone)]
pub enum EvalTarget<'a> {
    Trace(EvalContext<'a>),
    Span(SpanEvalContext<'a>),
    Session(SessionEvalContext<'a>),
}

#[derive(Debug, Clone)]
pub struct EvaluatorOutcome {
    pub score: f64,
    pub score_type: ScoreType,
    pub passed: Option<bool>,
    pub label: Option<String>,
    pub reasoning: String,
    pub error: Option<String>,
    pub metadata: Value,
}

fn value_as_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut cut = max_bytes;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    &s[..cut]
}

fn trace_cost_nanos(trace: &AgentTrace) -> i64 {
    if trace.total_cost_nanos > 0 {
        trace.total_cost_nanos
    } else {
        usd_to_cost_nanos(trace.total_cost_usd)
    }
}

fn span_cost_nanos(span: &AgentSpan) -> i64 {
    if span.cost_nanos > 0 {
        span.cost_nanos
    } else {
        usd_to_cost_nanos(span.cost_usd)
    }
}

struct TargetView<'a> {
    project_id: String,
    target_type: EvalTargetType,
    session_id: String,
    trace_id: Option<String>,
    span_id: Option<String>,
    agent_name: &'a str,
    agent_version: &'a str,
    input: &'a Value,
    output: &'a Value,
    duration_ms: i64,
    cost_nanos: i64,
    total_tokens: i64,
    has_root_error: bool,
    root_status: TraceStatus,
    traces: &'a [AgentTrace],
    spans: &'a [AgentSpan],
    expected_output: Option<&'a Value>,
    expected_behavior: Option<&'a Value>,
}

fn build_view<'a>(target: &'a EvalTarget<'a>) -> TargetView<'a> {
    match target {
        EvalTarget::Trace(ctx) => TargetView {
            project_id: ctx.trace.project_id.clone(),
            target_type: EvalTargetType::Trace,
            session_id: ctx.trace.session_id.clone().unwrap_or_default(),
            trace_id: Some(ctx.trace.trace_id.clone()),
            span_id: None,
            agent_name: &ctx.trace.agent_name,
            agent_version: &ctx.trace.agent_version,
            input: &ctx.trace.input,
            output: &ctx.trace.output,
            duration_ms: ctx.trace.duration_ms,
            cost_nanos: trace_cost_nanos(ctx.trace),
            total_tokens: ctx.trace.total_tokens,
            has_root_error: matches!(ctx.trace.status, TraceStatus::Error | TraceStatus::Timeout),
            root_status: ctx.trace.status,
            traces: std::slice::from_ref(ctx.trace),
            spans: ctx.spans,
            expected_output: ctx.expected_output,
            expected_behavior: ctx.expected_behavior,
        },
        EvalTarget::Span(ctx) => {
            let project_id = if !ctx.span.project_id.is_empty() {
                ctx.span.project_id.clone()
            } else {
                ctx.trace.map(|t| t.project_id.clone()).unwrap_or_default()
            };
            let session_id = ctx
                .trace
                .and_then(|t| t.session_id.clone())
                .unwrap_or_default();
            TargetView {
                project_id,
                target_type: EvalTargetType::Span,
                session_id,
                trace_id: Some(ctx.span.trace_id.clone()),
                span_id: Some(ctx.span.span_id.clone()),
                agent_name: ctx.trace.map(|t| t.agent_name.as_str()).unwrap_or("span"),
                agent_version: ctx
                    .trace
                    .map(|t| t.agent_version.as_str())
                    .unwrap_or("unversioned"),
                input: &ctx.span.input,
                output: &ctx.span.output,
                duration_ms: ctx.span.duration_ms,
                cost_nanos: span_cost_nanos(ctx.span),
                total_tokens: ctx.span.total_tokens,
                has_root_error: matches!(
                    ctx.span.status,
                    TraceStatus::Error | TraceStatus::Timeout
                ),
                root_status: ctx.span.status,
                traces: &[],
                spans: std::slice::from_ref(ctx.span),
                expected_output: ctx.expected_output,
                expected_behavior: ctx.expected_behavior,
            }
        }
        EvalTarget::Session(ctx) => {
            let latest_trace = ctx.traces.iter().max_by_key(|t| t.start_ts_ms);
            let earliest_trace = ctx.traces.iter().min_by_key(|t| t.start_ts_ms);
            let latest_output_trace = ctx
                .traces
                .iter()
                .filter(|t| !t.output.is_null())
                .max_by_key(|t| t.start_ts_ms);

            let duration_ms = ctx
                .traces
                .iter()
                .fold(0i64, |acc, t| acc.saturating_add(t.duration_ms.max(0)));
            let cost_nanos = ctx
                .traces
                .iter()
                .fold(0i64, |acc, t| acc.saturating_add(trace_cost_nanos(t)));
            let total_tokens = if !ctx.traces.is_empty() {
                ctx.traces
                    .iter()
                    .fold(0i64, |acc, t| acc.saturating_add(t.total_tokens.max(0)))
            } else {
                ctx.spans
                    .iter()
                    .fold(0i64, |acc, s| acc.saturating_add(s.total_tokens.max(0)))
            };
            let has_trace_error = ctx
                .traces
                .iter()
                .any(|t| matches!(t.status, TraceStatus::Error | TraceStatus::Timeout));
            let root_status = if has_trace_error {
                TraceStatus::Error
            } else {
                latest_trace.map(|t| t.status).unwrap_or(TraceStatus::Ok)
            };

            TargetView {
                project_id: ctx.project_id.to_string(),
                target_type: EvalTargetType::Session,
                session_id: ctx.session_id.to_string(),
                trace_id: None,
                span_id: None,
                agent_name: latest_trace
                    .map(|t| t.agent_name.as_str())
                    .unwrap_or("session"),
                agent_version: latest_trace
                    .map(|t| t.agent_version.as_str())
                    .unwrap_or("unversioned"),
                input: earliest_trace.map(|t| &t.input).unwrap_or(&Value::Null),
                output: latest_output_trace
                    .map(|t| &t.output)
                    .unwrap_or(&Value::Null),
                duration_ms,
                cost_nanos,
                total_tokens,
                has_root_error: has_trace_error,
                root_status,
                traces: ctx.traces,
                spans: ctx.spans,
                expected_output: ctx.expected_output,
                expected_behavior: ctx.expected_behavior,
            }
        }
    }
}

/// Runs any built-in deterministic / rule / schema / safety / performance evaluator synchronously on a trace.
/// Returns `None` if `cfg.kind == "llm_judge"` (which runs asynchronously via BYOK LLM in `pulse-control`).
pub fn run_deterministic_evaluator(
    cfg: &EvaluatorConfig,
    ctx: &EvalContext<'_>,
) -> Option<AgentEvaluation> {
    run_deterministic_evaluator_for_target(cfg, &EvalTarget::Trace(ctx.clone()))
}

/// Runs a deterministic evaluator on a single span.
pub fn run_deterministic_evaluator_on_span(
    cfg: &EvaluatorConfig,
    ctx: &SpanEvalContext<'_>,
) -> Option<AgentEvaluation> {
    run_deterministic_evaluator_for_target(cfg, &EvalTarget::Span(ctx.clone()))
}

/// Runs a deterministic evaluator on a session (traces + spans).
pub fn run_deterministic_evaluator_on_session(
    cfg: &EvaluatorConfig,
    ctx: &SessionEvalContext<'_>,
) -> Option<AgentEvaluation> {
    run_deterministic_evaluator_for_target(cfg, &EvalTarget::Session(ctx.clone()))
}

/// Runs any built-in deterministic evaluator on a `Session`, `Trace`, or `Span` target.
pub fn run_deterministic_evaluator_for_target(
    cfg: &EvaluatorConfig,
    target: &EvalTarget<'_>,
) -> Option<AgentEvaluation> {
    if cfg.kind == "llm_judge" {
        return None;
    }
    let started = Instant::now();
    let view = build_view(target);
    let outcome = match cfg.kind.as_str() {
        "no_errors" => eval_no_errors(&view),
        "no_loop" => eval_no_loop(&view),
        "latency_budget" => eval_latency_budget(&cfg.config, &view),
        "cost_budget" => eval_cost_budget(&cfg.config, &view),
        "token_budget" => eval_token_budget(&cfg.config, &view),
        "output_not_empty" => eval_output_not_empty(&view),
        "exact_match" => eval_exact_match(&view),
        "regex_match" => eval_regex_match(&cfg.config, &view),
        "contains_keywords" => eval_contains_keywords(&cfg.config, &view),
        "json_schema_keys" => eval_json_schema_keys(&cfg.config, &view),
        "tool_sequence" => eval_tool_sequence(&cfg.config, &view),
        "pii_leakage" => eval_pii_leakage(&view),
        other => EvaluatorOutcome {
            score: 0.0,
            score_type: ScoreType::Boolean,
            passed: Some(false),
            label: Some("unknown_evaluator".to_string()),
            reasoning: format!("Unknown deterministic evaluator kind '{other}'"),
            error: Some(format!("unsupported evaluator kind: {other}")),
            metadata: Value::Null,
        },
    };
    let exec_ms = started.elapsed().as_millis() as i64;
    Some(AgentEvaluation {
        id: format!("eval_{}", Uuid::new_v4().simple()),
        project_id: view.project_id,
        target_type: view.target_type,
        session_id: view.session_id,
        trace_id: view.trace_id,
        span_id: view.span_id,
        experiment_run_id: None,
        evaluator_id: cfg.id.clone(),
        evaluator_name: cfg.name.clone(),
        evaluator_type: cfg.evaluator_type,
        evaluator_version: cfg.version.clone(),
        score: outcome.score,
        score_type: outcome.score_type,
        passed: outcome.passed,
        label: outcome.label,
        reasoning: Some(outcome.reasoning),
        execution_ms: exec_ms,
        error: outcome.error,
        metadata: outcome.metadata,
        created_at_ms: Utc::now().timestamp_millis(),
    })
}

fn eval_no_errors(view: &TargetView<'_>) -> EvaluatorOutcome {
    let failed_spans: Vec<&str> = view
        .spans
        .iter()
        .filter(|s| matches!(s.status, TraceStatus::Error | TraceStatus::Timeout))
        .map(|s| s.name.as_str())
        .collect();
    let failed_traces = view
        .traces
        .iter()
        .filter(|t| matches!(t.status, TraceStatus::Error | TraceStatus::Timeout))
        .count();
    let passed = !view.has_root_error && failed_spans.is_empty() && failed_traces == 0;
    EvaluatorOutcome {
        score: if passed { 1.0 } else { 0.0 },
        score_type: ScoreType::Boolean,
        passed: Some(passed),
        label: Some(if passed { "pass" } else { "fail" }.to_string()),
        reasoning: if passed {
            "Execution completed with zero failed spans.".to_string()
        } else {
            format!(
                "{} status='{}' with {} failed span(s): {:?}",
                view.target_type.as_str(),
                view.root_status.as_str(),
                failed_spans.len(),
                failed_spans
            )
        },
        error: None,
        metadata: json!({
            "failed_span_count": failed_spans.len(),
            "failed_trace_count": failed_traces,
        }),
    }
}

fn eval_no_loop(view: &TargetView<'_>) -> EvaluatorOutcome {
    let loop_reason = if view.target_type == EvalTargetType::Session {
        let mut by_trace: HashMap<&str, Vec<AgentSpan>> = HashMap::new();
        for sp in view.spans {
            by_trace
                .entry(sp.trace_id.as_str())
                .or_default()
                .push(sp.clone());
        }
        by_trace
            .into_values()
            .find_map(|trace_spans| detect_execution_loop(&trace_spans))
    } else {
        detect_execution_loop(view.spans)
    };

    match loop_reason {
        None => EvaluatorOutcome {
            score: 1.0,
            score_type: ScoreType::Boolean,
            passed: Some(true),
            label: Some("clean".to_string()),
            reasoning: "No execution loop detected.".to_string(),
            error: None,
            metadata: Value::Null,
        },
        Some(reason) => EvaluatorOutcome {
            score: 0.0,
            score_type: ScoreType::Boolean,
            passed: Some(false),
            label: Some("loop_detected".to_string()),
            reasoning: reason,
            error: None,
            metadata: Value::Null,
        },
    }
}

fn eval_latency_budget(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let max_ms = cfg
        .get("max_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or(5_000)
        .max(1);
    let actual = view.duration_ms;
    let passed = actual <= max_ms;
    let score = if passed {
        1.0
    } else {
        (max_ms as f64 / actual.max(1) as f64).clamp(0.0, 1.0)
    };
    EvaluatorOutcome {
        score,
        score_type: ScoreType::Numeric,
        passed: Some(passed),
        label: Some(if passed { "within_budget" } else { "slow" }.to_string()),
        reasoning: format!("Execution took {actual} ms (budget: <= {max_ms} ms)."),
        error: None,
        metadata: json!({ "actual_ms": actual, "max_ms": max_ms }),
    }
}

fn eval_cost_budget(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let max_nanos = cfg
        .get("max_nanos")
        .and_then(|v| v.as_i64())
        .or_else(|| {
            cfg.get("max_usd")
                .and_then(|v| v.as_f64())
                .map(usd_to_cost_nanos)
        })
        .unwrap_or_else(|| usd_to_cost_nanos(0.10))
        .max(1);
    let actual_nanos = view.cost_nanos.max(0);
    let passed = actual_nanos <= max_nanos;
    let score = if passed {
        1.0
    } else {
        (max_nanos as f64 / actual_nanos.max(1) as f64).clamp(0.0, 1.0)
    };
    let actual_usd = cost_nanos_to_usd(actual_nanos);
    let max_usd = cost_nanos_to_usd(max_nanos);
    EvaluatorOutcome {
        score,
        score_type: ScoreType::Numeric,
        passed: Some(passed),
        label: Some(
            if passed {
                "within_budget"
            } else {
                "over_budget"
            }
            .to_string(),
        ),
        reasoning: format!("Execution cost ${actual_usd:.6} (budget: <= ${max_usd:.6})."),
        error: None,
        metadata: json!({
            "actual_nanos": actual_nanos,
            "max_nanos": max_nanos,
            "actual_usd": actual_usd,
            "max_usd": max_usd,
        }),
    }
}

fn eval_token_budget(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let max_tokens = cfg
        .get("max_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(16_000)
        .max(1);
    let actual = view.total_tokens;
    let passed = actual <= max_tokens;
    EvaluatorOutcome {
        score: if passed {
            1.0
        } else {
            (max_tokens as f64 / actual.max(1) as f64).clamp(0.0, 1.0)
        },
        score_type: ScoreType::Numeric,
        passed: Some(passed),
        label: Some(
            if passed {
                "within_budget"
            } else {
                "over_budget"
            }
            .to_string(),
        ),
        reasoning: format!("Consumed {actual} tokens (budget: <= {max_tokens})."),
        error: None,
        metadata: json!({ "actual_tokens": actual, "max_tokens": max_tokens }),
    }
}

fn eval_output_not_empty(view: &TargetView<'_>) -> EvaluatorOutcome {
    let text = value_as_text(view.output);
    let passed = !text.trim().is_empty();
    EvaluatorOutcome {
        score: if passed { 1.0 } else { 0.0 },
        score_type: ScoreType::Boolean,
        passed: Some(passed),
        label: Some(if passed { "non_empty" } else { "empty" }.to_string()),
        reasoning: if passed {
            format!("Output has {} characters.", text.len())
        } else {
            format!("{} produced empty output.", view.target_type.as_str())
        },
        error: None,
        metadata: Value::Null,
    }
}

fn eval_exact_match(view: &TargetView<'_>) -> EvaluatorOutcome {
    let Some(expected) = view.expected_output.filter(|v| !v.is_null()) else {
        return EvaluatorOutcome {
            score: 0.0,
            score_type: ScoreType::Boolean,
            passed: None,
            label: Some("skipped".to_string()),
            reasoning: "No expected_output provided for exact_match.".to_string(),
            error: None,
            metadata: Value::Null,
        };
    };
    let actual = value_as_text(view.output).trim().to_string();
    let exp = value_as_text(expected).trim().to_string();
    let passed = actual == exp;
    EvaluatorOutcome {
        score: if passed { 1.0 } else { 0.0 },
        score_type: ScoreType::Boolean,
        passed: Some(passed),
        label: Some(if passed { "exact_match" } else { "mismatch" }.to_string()),
        reasoning: if passed {
            "Output matches expected output exactly.".to_string()
        } else {
            "Output differs from expected_output.".to_string()
        },
        error: None,
        metadata: Value::Null,
    }
}

fn eval_regex_match(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let Some(pattern) = cfg.get("pattern").and_then(|v| v.as_str()) else {
        return EvaluatorOutcome {
            score: 0.0,
            score_type: ScoreType::Boolean,
            passed: Some(false),
            label: Some("config_error".to_string()),
            reasoning: "Missing 'pattern' in regex_match config.".to_string(),
            error: Some("missing pattern".to_string()),
            metadata: Value::Null,
        };
    };
    let invert = cfg.get("invert").and_then(|v| v.as_bool()).unwrap_or(false);
    match Regex::new(pattern) {
        Ok(re) => {
            let text = value_as_text(view.output);
            let matched = re.is_match(&text);
            let passed = if invert { !matched } else { matched };
            EvaluatorOutcome {
                score: if passed { 1.0 } else { 0.0 },
                score_type: ScoreType::Boolean,
                passed: Some(passed),
                label: Some(if passed { "pass" } else { "fail" }.to_string()),
                reasoning: format!(
                    "Regex '{}' {} output (invert={}).",
                    pattern,
                    if matched { "matched" } else { "did not match" },
                    invert
                ),
                error: None,
                metadata: Value::Null,
            }
        }
        Err(e) => EvaluatorOutcome {
            score: 0.0,
            score_type: ScoreType::Boolean,
            passed: Some(false),
            label: Some("regex_error".to_string()),
            reasoning: format!("Invalid regex: {e}"),
            error: Some(e.to_string()),
            metadata: Value::Null,
        },
    }
}

fn eval_contains_keywords(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let keywords: Vec<String> = cfg
        .get("keywords")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_ascii_lowercase()))
                .collect()
        })
        .unwrap_or_default();
    if keywords.is_empty() {
        return EvaluatorOutcome {
            score: 1.0,
            score_type: ScoreType::Numeric,
            passed: Some(true),
            label: Some("pass".to_string()),
            reasoning: "No keywords configured.".to_string(),
            error: None,
            metadata: Value::Null,
        };
    }
    let text = value_as_text(view.output).to_ascii_lowercase();
    let matched: Vec<&String> = keywords
        .iter()
        .filter(|k| text.contains(k.as_str()))
        .collect();
    let score = matched.len() as f64 / keywords.len() as f64;
    let require_all = cfg
        .get("require_all")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let passed = if require_all {
        matched.len() == keywords.len()
    } else {
        !matched.is_empty()
    };
    EvaluatorOutcome {
        score,
        score_type: ScoreType::Numeric,
        passed: Some(passed),
        label: Some(if passed { "pass" } else { "fail" }.to_string()),
        reasoning: format!(
            "Matched {}/{} required keyword(s).",
            matched.len(),
            keywords.len()
        ),
        error: None,
        metadata: json!({ "matched": matched.len(), "total": keywords.len() }),
    }
}

fn eval_json_schema_keys(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let required: Vec<String> = cfg
        .get("required_keys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let parsed: Option<Value> = match view.output {
        Value::Object(_) => Some(view.output.clone()),
        Value::String(s) => serde_json::from_str(s).ok(),
        _ => None,
    };
    let Some(Value::Object(map)) = parsed else {
        return EvaluatorOutcome {
            score: 0.0,
            score_type: ScoreType::Boolean,
            passed: Some(false),
            label: Some("invalid_json".to_string()),
            reasoning: "Output is not a valid JSON object.".to_string(),
            error: None,
            metadata: Value::Null,
        };
    };
    let missing: Vec<&String> = required.iter().filter(|k| !map.contains_key(*k)).collect();
    let score = if required.is_empty() {
        1.0
    } else {
        (required.len() - missing.len()) as f64 / required.len() as f64
    };
    let passed = missing.is_empty();
    EvaluatorOutcome {
        score,
        score_type: ScoreType::Numeric,
        passed: Some(passed),
        label: Some(
            if passed {
                "valid_schema"
            } else {
                "missing_keys"
            }
            .to_string(),
        ),
        reasoning: if passed {
            format!(
                "JSON output contains all {} required key(s).",
                required.len()
            )
        } else {
            format!("JSON output missing required key(s): {missing:?}")
        },
        error: None,
        metadata: json!({ "missing_keys": missing }),
    }
}

fn eval_tool_sequence(cfg: &Value, view: &TargetView<'_>) -> EvaluatorOutcome {
    let expected: Vec<String> = cfg
        .get("expected_tools")
        .or_else(|| view.expected_behavior.and_then(|b| b.get("expected_tools")))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let mut deduped: HashMap<&str, &AgentSpan> = HashMap::new();
    let mut unkeyed: Vec<&AgentSpan> = Vec::new();
    for sp in view.spans.iter().filter(|s| s.span_type == SpanType::Tool) {
        if sp.span_id.is_empty() {
            unkeyed.push(sp);
        } else {
            deduped
                .entry(sp.span_id.as_str())
                .and_modify(|existing| {
                    let new_rank = (sp.span_phase.priority(), sp.start_ts_ms);
                    let old_rank = (existing.span_phase.priority(), existing.start_ts_ms);
                    if new_rank >= old_rank {
                        *existing = sp;
                    }
                })
                .or_insert(sp);
        }
    }

    let mut tool_spans: Vec<&AgentSpan> = deduped.into_values().chain(unkeyed).collect();
    tool_spans.sort_by_key(|s| s.start_ts_ms);
    let actual: Vec<String> = tool_spans
        .iter()
        .map(|s| s.tool_name.clone().unwrap_or_else(|| s.name.clone()))
        .collect();

    if expected.is_empty() {
        return EvaluatorOutcome {
            score: 1.0,
            score_type: ScoreType::Numeric,
            passed: Some(true),
            label: Some("no_expectation".to_string()),
            reasoning: format!("Observed {} tool call(s): {actual:?}", actual.len()),
            error: None,
            metadata: json!({ "actual_tools": actual }),
        };
    }

    let strict_order = cfg
        .get("strict_order")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let (score, passed) = if strict_order {
        let ok = actual == expected;
        (if ok { 1.0 } else { 0.0 }, ok)
    } else {
        let hits = expected.iter().filter(|t| actual.contains(*t)).count();
        let s = hits as f64 / expected.len() as f64;
        (s, hits == expected.len())
    };
    EvaluatorOutcome {
        score,
        score_type: ScoreType::Numeric,
        passed: Some(passed),
        label: Some(
            if passed {
                "tools_matched"
            } else {
                "tools_mismatch"
            }
            .to_string(),
        ),
        reasoning: format!("Expected tools {expected:?}; observed {actual:?}."),
        error: None,
        metadata: json!({ "expected_tools": expected, "actual_tools": actual }),
    }
}

fn eval_pii_leakage(view: &TargetView<'_>) -> EvaluatorOutcome {
    let text = value_as_text(view.output);
    let hits = detect_pii_patterns(&text);
    let passed = hits.is_empty();
    EvaluatorOutcome {
        score: if passed { 1.0 } else { 0.0 },
        score_type: ScoreType::Boolean,
        passed: Some(passed),
        label: Some(if passed { "safe" } else { "pii_detected" }.to_string()),
        reasoning: if passed {
            "No PII or credential patterns detected in output.".to_string()
        } else {
            format!("Detected potential sensitive data in output: {hits:?}")
        },
        error: None,
        metadata: json!({ "detected_types": hits }),
    }
}

/// Builds a structured prompt for LLM-as-judge evaluators on a trace (UTF-8 safe).
pub fn build_llm_judge_prompt(cfg: &EvaluatorConfig, ctx: &EvalContext<'_>) -> String {
    build_llm_judge_prompt_for_target(cfg, &EvalTarget::Trace(ctx.clone()))
}

/// Builds a structured prompt for LLM-as-judge evaluators on a `Session`, `Trace`, or `Span` target.
pub fn build_llm_judge_prompt_for_target(cfg: &EvaluatorConfig, target: &EvalTarget<'_>) -> String {
    let view = build_view(target);
    let rubric = cfg
        .config
        .get("rubric")
        .and_then(|v| v.as_str())
        .unwrap_or("Evaluate whether the agent output is accurate, helpful, safe, and directly addresses the input.");
    let input_txt = value_as_text(view.input);
    let output_txt = value_as_text(view.output);
    let expected_txt = view.expected_output.map(value_as_text).unwrap_or_default();
    format!(
        "You are an objective AI agent evaluator.\n\
         Target: {}\n\
         Rubric: {rubric}\n\n\
         Agent: {} (version {})\n\
         Input: {}\n\
         Output: {}\n\
         {}\n\
         Respond ONLY with a JSON object: {{\"score\": <float 0.0..1.0>, \"passed\": <true|false>, \"reasoning\": \"<concise explanation>\"}}.",
        view.target_type.as_str(),
        view.agent_name,
        view.agent_version,
        truncate_utf8(&input_txt, 4000),
        truncate_utf8(&output_txt, 4000),
        if expected_txt.is_empty() {
            String::new()
        } else {
            format!("Expected Output: {}", truncate_utf8(&expected_txt, 2000))
        }
    )
}

/// Parses a JSON response from an LLM-as-judge provider into a trace-level `AgentEvaluation`.
pub fn parse_llm_judge_response(
    cfg: &EvaluatorConfig,
    trace: &AgentTrace,
    raw_text: &str,
    execution_ms: i64,
) -> AgentEvaluation {
    parse_llm_judge_response_for_target(
        cfg,
        &trace.project_id,
        EvalTargetType::Trace,
        trace.session_id.as_deref().unwrap_or(""),
        Some(trace.trace_id.clone()),
        None,
        raw_text,
        execution_ms,
    )
}

/// Parses a JSON response from an LLM-as-judge provider into a multi-level `AgentEvaluation`.
#[allow(clippy::too_many_arguments)]
pub fn parse_llm_judge_response_for_target(
    cfg: &EvaluatorConfig,
    project_id: &str,
    target_type: EvalTargetType,
    session_id: &str,
    trace_id: Option<String>,
    span_id: Option<String>,
    raw_text: &str,
    execution_ms: i64,
) -> AgentEvaluation {
    // Extract JSON block if wrapped in markdown fences
    let cleaned = raw_text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    let parsed: Result<Value, _> = serde_json::from_str(cleaned);
    let (score, passed, reasoning, error) = match parsed {
        Ok(v) => {
            let s = v
                .get("score")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0)
                .clamp(0.0, 1.0);
            let p = v
                .get("passed")
                .and_then(|x| x.as_bool())
                .unwrap_or(s >= 0.7);
            let r = v
                .get("reasoning")
                .and_then(|x| x.as_str())
                .unwrap_or("LLM judge completed evaluation.")
                .to_string();
            (s, Some(p), Some(r), None)
        }
        Err(e) => (
            0.0,
            Some(false),
            Some("Failed to parse structured JSON from LLM judge.".to_string()),
            Some(e.to_string()),
        ),
    };

    AgentEvaluation {
        id: format!("eval_{}", Uuid::new_v4().simple()),
        project_id: project_id.to_string(),
        target_type,
        session_id: session_id.to_string(),
        trace_id,
        span_id,
        experiment_run_id: None,
        evaluator_id: cfg.id.clone(),
        evaluator_name: cfg.name.clone(),
        evaluator_type: EvaluatorType::LlmJudge,
        evaluator_version: cfg.version.clone(),
        score,
        score_type: ScoreType::Numeric,
        passed,
        label: passed.map(|p| if p { "pass" } else { "fail" }.to_string()),
        reasoning,
        execution_ms,
        error,
        metadata: Value::Null,
        created_at_ms: Utc::now().timestamp_millis(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SpanPhase;

    fn sample_trace(trace_id: &str, session_id: Option<&str>) -> AgentTrace {
        AgentTrace {
            trace_id: trace_id.into(),
            project_id: "p1".into(),
            agent_name: "billing-agent".into(),
            agent_version: "1.0.0".into(),
            prompt_version: Some("p_v1".into()),
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".into(),
            session_id: session_id.map(str::to_string),
            user_id: None,
            workflow_id: None,
            parent_trace_id: None,
            status: TraceStatus::Ok,
            start_ts_ms: 1000,
            end_ts_ms: Some(2500),
            duration_ms: 1500,
            input: json!({"query": "refund order 123"}),
            output: json!({"status": "approved", "refund_id": "rf_99"}),
            error_type: None,
            error_message: None,
            failure_source: None,
            metadata: Value::Null,
            tags: vec![],
            span_count: 1,
            llm_call_count: 1,
            tool_call_count: 0,
            error_span_count: 0,
            input_tokens: 100,
            output_tokens: 50,
            cached_tokens: 0,
            total_tokens: 150,
            total_cost_nanos: 2_000_000,
            total_cost_usd: 0.002,
            primary_model: Some("gpt-4o-mini".into()),
            primary_provider: Some("openai".into()),
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning: false,
        }
    }

    fn sample_span(span_id: &str, trace_id: &str, span_type: SpanType) -> AgentSpan {
        AgentSpan {
            span_id: span_id.into(),
            trace_id: trace_id.into(),
            project_id: "p1".into(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: span_id.into(),
            span_type,
            status: TraceStatus::Ok,
            start_ts_ms: 1000,
            end_ts_ms: Some(1500),
            duration_ms: 500,
            provider: Some("openai".into()),
            model: Some("gpt-4o-mini".into()),
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 100,
            output_tokens: 50,
            cached_tokens: 0,
            total_tokens: 150,
            cost_nanos: 1_000_000,
            cost_usd: 0.001,
            temperature: None,
            finish_reason: None,
            model_params: Value::Null,
            tool_name: None,
            tool_version: None,
            tool_retry_count: 0,
            input: json!({"prompt": "hi"}),
            output: json!({"answer": "ok"}),
            attributes: Value::Null,
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        }
    }

    fn make_cfg(kind: &str, config: Value) -> EvaluatorConfig {
        EvaluatorConfig {
            id: format!("ev_{kind}"),
            project_id: "p1".into(),
            name: kind.into(),
            description: String::new(),
            target_type: EvalTargetType::Trace,
            evaluator_type: EvaluatorType::Deterministic,
            kind: kind.into(),
            version: "1.0.0".into(),
            enabled: true,
            agent_name: None,
            environment: None,
            sample_rate: 1.0,
            config,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    #[test]
    fn runs_schema_and_budget_evaluators() {
        let trace = sample_trace("tr_1", Some("sess_1"));
        let ctx = EvalContext {
            trace: &trace,
            spans: &[],
            expected_output: None,
            expected_behavior: None,
        };
        let cfg = EvaluatorConfig {
            id: "ev_schema".into(),
            project_id: "p1".into(),
            name: "Required JSON keys".into(),
            description: String::new(),
            target_type: EvalTargetType::Trace,
            evaluator_type: EvaluatorType::Schema,
            kind: "json_schema_keys".into(),
            version: "1.0.0".into(),
            enabled: true,
            agent_name: None,
            environment: None,
            sample_rate: 1.0,
            config: json!({ "required_keys": ["status", "refund_id"] }),
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let res = run_deterministic_evaluator(&cfg, &ctx).unwrap();
        assert_eq!(res.target_type, EvalTargetType::Trace);
        assert_eq!(res.session_id, "sess_1");
        assert_eq!(res.trace_id.as_deref(), Some("tr_1"));
        assert_eq!(res.span_id, None);
        assert_eq!(res.passed, Some(true));
        assert!((res.score - 1.0).abs() < 1e-9);
    }

    #[test]
    fn runs_span_and_session_evaluators_including_per_trace_no_loop_and_cost_nanos() {
        let traces: Vec<AgentTrace> = (0..15)
            .map(|i| sample_trace(&format!("tr_{i}"), Some("sess_multi")))
            .collect();
        let spans: Vec<AgentSpan> = (0..15)
            .map(|i| sample_span(&format!("sp_{i}"), &format!("tr_{i}"), SpanType::Llm))
            .collect();

        // Span-level eval
        let sp_ctx = SpanEvalContext {
            span: &spans[0],
            trace: Some(&traces[0]),
            expected_output: None,
            expected_behavior: None,
        };
        let cost_cfg = make_cfg("cost_budget", json!({ "max_nanos": 1_500_000 }));
        let sp_res = run_deterministic_evaluator_on_span(&cost_cfg, &sp_ctx).unwrap();
        assert_eq!(sp_res.target_type, EvalTargetType::Span);
        assert_eq!(sp_res.session_id, "sess_multi");
        assert_eq!(sp_res.trace_id.as_deref(), Some("tr_0"));
        assert_eq!(sp_res.span_id.as_deref(), Some("sp_0"));
        assert_eq!(sp_res.passed, Some(true));

        // Session-level no_loop: 15 traces x 1 LLM span = 15 LLM spans total, must NOT false-positive
        let sess_ctx = SessionEvalContext {
            project_id: "p1",
            session_id: "sess_multi",
            traces: &traces,
            spans: &spans,
            expected_output: None,
            expected_behavior: None,
        };
        let loop_cfg = make_cfg("no_loop", json!({}));
        let sess_loop = run_deterministic_evaluator_on_session(&loop_cfg, &sess_ctx).unwrap();
        assert_eq!(sess_loop.target_type, EvalTargetType::Session);
        assert_eq!(sess_loop.trace_id, None);
        assert_eq!(sess_loop.span_id, None);
        assert_eq!(sess_loop.passed, Some(true));

        // Session-level cost_budget in cost_nanos: 15 * 2_000_000 = 30_000_000 > 20_000_000
        let sess_cost_cfg = make_cfg("cost_budget", json!({ "max_nanos": 20_000_000 }));
        let sess_cost = run_deterministic_evaluator_on_session(&sess_cost_cfg, &sess_ctx).unwrap();
        assert_eq!(sess_cost.passed, Some(false));
    }

    #[test]
    fn llm_judge_prompt_truncates_multibyte_utf8_without_panicking() {
        let mut trace = sample_trace("tr_utf8", None);
        trace.input = Value::String("🚀".repeat(1200)); // 4800 bytes of 4-byte UTF-8
        trace.output = Value::String("é".repeat(2500)); // 5000 bytes of 2-byte UTF-8
        let expected = Value::String("中".repeat(1000)); // 3000 bytes of 3-byte UTF-8
        let cfg = make_cfg("llm_judge", json!({ "rubric": "accuracy" }));
        let ctx = EvalContext {
            trace: &trace,
            spans: &[],
            expected_output: Some(&expected),
            expected_behavior: None,
        };
        let prompt = build_llm_judge_prompt(&cfg, &ctx);
        assert!(prompt.contains("Target: trace"));
        assert!(prompt.contains("Expected Output:"));
    }
}
