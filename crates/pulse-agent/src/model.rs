use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TraceStatus {
    #[default]
    Ok,
    Running,
    Error,
    Timeout,
    Cancelled,
}

impl TraceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Running => "running",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "running" | "in_progress" | "pending" => Self::Running,
            "error" | "failed" | "failure" => Self::Error,
            "timeout" | "timed_out" => Self::Timeout,
            "cancelled" | "canceled" => Self::Cancelled,
            _ => Self::Ok,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::Running => 1,
            Self::Error => 2,
            Self::Timeout => 3,
            Self::Cancelled => 4,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::Running),
            2 => Some(Self::Error),
            3 => Some(Self::Timeout),
            4 => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SpanPhase {
    Start,
    Update,
    #[default]
    Final,
}

impl SpanPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Update => "update",
            Self::Final => "final",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "start" => Self::Start,
            "update" => Self::Update,
            _ => Self::Final,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Start => 0,
            Self::Update => 1,
            Self::Final => 2,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Start),
            1 => Some(Self::Update),
            2 => Some(Self::Final),
            _ => None,
        }
    }

    /// Deterministic merge priority for CRDT / streaming deduplication (`Final > Update > Start`).
    pub fn priority(&self) -> u8 {
        self.to_u8()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SpanType {
    Agent,
    Llm,
    Tool,
    Retrieval,
    Embedding,
    Guardrail,
    Http,
    Db,
    Workflow,
    #[default]
    Custom,
}

impl SpanType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Llm => "llm",
            Self::Tool => "tool",
            Self::Retrieval => "retrieval",
            Self::Embedding => "embedding",
            Self::Guardrail => "guardrail",
            Self::Http => "http",
            Self::Db => "db",
            Self::Workflow => "workflow",
            Self::Custom => "custom",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "agent" | "chain" => Self::Agent,
            "llm" | "generation" | "chat" | "completion" => Self::Llm,
            "tool" | "function" | "tool_call" => Self::Tool,
            "retrieval" | "retriever" | "rag" => Self::Retrieval,
            "embedding" | "embeddings" => Self::Embedding,
            "guardrail" | "safety" | "moderation" => Self::Guardrail,
            "http" | "network" => Self::Http,
            "db" | "database" | "sql" => Self::Db,
            "workflow" | "step" | "graph" => Self::Workflow,
            _ => Self::Custom,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Agent => 0,
            Self::Llm => 1,
            Self::Tool => 2,
            Self::Retrieval => 3,
            Self::Embedding => 4,
            Self::Guardrail => 5,
            Self::Http => 6,
            Self::Db => 7,
            Self::Workflow => 8,
            Self::Custom => 9,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Agent),
            1 => Some(Self::Llm),
            2 => Some(Self::Tool),
            3 => Some(Self::Retrieval),
            4 => Some(Self::Embedding),
            5 => Some(Self::Guardrail),
            6 => Some(Self::Http),
            7 => Some(Self::Db),
            8 => Some(Self::Workflow),
            9 => Some(Self::Custom),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FailureSource {
    Model,
    Tool,
    Application,
    Prompt,
    External,
    #[default]
    Unknown,
}

impl FailureSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Tool => "tool",
            Self::Application => "application",
            Self::Prompt => "prompt",
            Self::External => "external",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "model" | "llm" => Self::Model,
            "tool" => Self::Tool,
            "application" | "app" | "code" => Self::Application,
            "prompt" => Self::Prompt,
            "external" | "http" | "network" | "db" => Self::External,
            _ => Self::Unknown,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Model => 1,
            Self::Tool => 2,
            Self::Application => 3,
            Self::Prompt => 4,
            Self::External => 5,
            Self::Unknown => 6,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Model),
            2 => Some(Self::Tool),
            3 => Some(Self::Application),
            4 => Some(Self::Prompt),
            5 => Some(Self::External),
            6 => Some(Self::Unknown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp_ms: i64,
    #[serde(default)]
    pub attributes: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSpan {
    pub span_id: String,
    pub trace_id: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub linked_span_ids: Vec<String>,
    #[serde(default)]
    pub span_phase: SpanPhase,
    #[serde(default)]
    pub depth: i32,
    pub name: String,
    #[serde(default)]
    pub span_type: SpanType,
    #[serde(default)]
    pub status: TraceStatus,
    pub start_ts_ms: i64,
    #[serde(default)]
    pub end_ts_ms: Option<i64>,
    #[serde(default)]
    pub duration_ms: i64,

    // Generation fields
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub prompt_template_id: Option<String>,
    #[serde(default)]
    pub prompt_template_version: Option<String>,
    #[serde(default)]
    pub input_tokens: i64,
    #[serde(default)]
    pub output_tokens: i64,
    #[serde(default)]
    pub cached_tokens: i64,
    #[serde(default)]
    pub total_tokens: i64,
    #[serde(default)]
    pub cost_nanos: i64,
    #[serde(default)]
    pub cost_usd: f64,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub finish_reason: Option<String>,
    #[serde(default)]
    pub model_params: Value,

    // Tool fields
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_version: Option<String>,
    #[serde(default)]
    pub tool_retry_count: i32,

    // Payloads & diagnostics
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub attributes: Value,
    #[serde(default)]
    pub events: Vec<SpanEvent>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub error_type: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub error_stack: Option<String>,
}

impl AgentSpan {
    /// Reconciles `cost_nanos` ($10^{-9}$ USD) and `cost_usd` after JSON deserialization or ingestion.
    pub fn normalize_cost(&mut self) {
        crate::pricing::sync_span_cost(self);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTrace {
    pub trace_id: String,
    #[serde(default)]
    pub project_id: String,
    pub agent_name: String,
    #[serde(default = "default_version")]
    pub agent_version: String,
    #[serde(default)]
    pub prompt_version: Option<String>,
    #[serde(default)]
    pub model_config_version: Option<String>,
    #[serde(default)]
    pub tool_config_version: Option<String>,
    #[serde(default)]
    pub app_release: Option<String>,
    #[serde(default = "default_env")]
    pub environment: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub parent_trace_id: Option<String>,
    #[serde(default)]
    pub status: TraceStatus,
    pub start_ts_ms: i64,
    #[serde(default)]
    pub end_ts_ms: Option<i64>,
    #[serde(default)]
    pub duration_ms: i64,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub error_type: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub failure_source: Option<FailureSource>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub tags: Vec<String>,

    // Rolled-up summary metrics
    #[serde(default)]
    pub span_count: i32,
    #[serde(default)]
    pub llm_call_count: i32,
    #[serde(default)]
    pub tool_call_count: i32,
    #[serde(default)]
    pub error_span_count: i32,
    #[serde(default)]
    pub input_tokens: i64,
    #[serde(default)]
    pub output_tokens: i64,
    #[serde(default)]
    pub cached_tokens: i64,
    #[serde(default)]
    pub total_tokens: i64,
    #[serde(default)]
    pub total_cost_nanos: i64,
    #[serde(default)]
    pub total_cost_usd: f64,
    #[serde(default)]
    pub primary_model: Option<String>,
    #[serde(default)]
    pub primary_provider: Option<String>,
    #[serde(default)]
    pub avg_eval_score: Option<f64>,
    #[serde(default)]
    pub eval_pass_rate: Option<f64>,
    #[serde(default)]
    pub has_loop_warning: bool,
}

impl AgentTrace {
    /// Reconciles `total_cost_nanos` ($10^{-9}$ USD) and `total_cost_usd` after JSON deserialization or ingestion.
    pub fn normalize_cost(&mut self) {
        crate::pricing::sync_trace_cost(self);
    }
}

fn default_version() -> String {
    "unversioned".to_string()
}

fn default_env() -> String {
    "production".to_string()
}

/// Incoming batch payload for `POST /v1/agent/traces`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TraceIngestBatch {
    #[serde(default)]
    pub traces: Vec<AgentTrace>,
    #[serde(default)]
    pub spans: Vec<AgentSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceIngestResponse {
    pub accepted_traces: usize,
    pub accepted_spans: usize,
    pub truncated_payloads: usize,
    pub redacted_fields: usize,
    pub dropped_spans: usize,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetail {
    pub trace: AgentTrace,
    pub spans: Vec<AgentSpan>,
    pub evaluations: Vec<AgentEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceListFilter {
    pub agent_name: Option<String>,
    pub agent_version: Option<String>,
    pub prompt_version: Option<String>,
    pub environment: Option<String>,
    pub status: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub tag: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub parent_trace_id: Option<String>,
    pub has_error: Option<bool>,
    pub has_loop_warning: Option<bool>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub min_cost_usd: Option<f64>,
    pub max_cost_usd: Option<f64>,
    pub min_eval_score: Option<f64>,
    pub max_eval_score: Option<f64>,
    pub start_ts_ms: Option<i64>,
    pub end_ts_ms: Option<i64>,
    pub search: Option<String>,
    pub sort_by: Option<String>,
    pub sort_dir: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceListResponse {
    pub traces: Vec<AgentTrace>,
    pub total: i64,
    pub limit: usize,
    pub offset: usize,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

// ---------------------------------------------------------------------------
// Sessions / Conversations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionListFilter {
    pub user_id: Option<String>,
    pub agent_name: Option<String>,
    pub environment: Option<String>,
    pub has_error: Option<bool>,
    pub start_ts_ms: Option<i64>,
    pub end_ts_ms: Option<i64>,
    pub search: Option<String>,
    pub sort_by: Option<String>,
    pub sort_dir: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub agent_names: Vec<String>,
    #[serde(default)]
    pub environment: Option<String>,
    pub first_ts_ms: i64,
    pub last_ts_ms: i64,
    pub trace_count: i64,
    pub error_count: i64,
    pub total_tokens: i64,
    #[serde(default)]
    pub total_cost_nanos: i64,
    #[serde(default)]
    pub total_cost_usd: f64,
    #[serde(default)]
    pub avg_eval_score: Option<f64>,
    #[serde(default)]
    pub eval_pass_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionListResponse {
    pub sessions: Vec<SessionSummary>,
    pub total: i64,
    pub limit: usize,
    pub offset: usize,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub summary: SessionSummary,
    pub traces: Vec<AgentTrace>,
    #[serde(default)]
    pub evaluations: Vec<AgentEvaluation>,
}

// ---------------------------------------------------------------------------
// Evaluations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvalTargetType {
    Session,
    #[default]
    Trace,
    Span,
}

impl EvalTargetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Trace => "trace",
            Self::Span => "span",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "session" | "conversation" | "thread" => Self::Session,
            "span" => Self::Span,
            _ => Self::Trace,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvaluatorType {
    #[default]
    Deterministic,
    Rule,
    Schema,
    Safety,
    Performance,
    ToolQuality,
    Rag,
    LlmJudge,
    Custom,
    External,
}

impl EvaluatorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Rule => "rule",
            Self::Schema => "schema",
            Self::Safety => "safety",
            Self::Performance => "performance",
            Self::ToolQuality => "tool_quality",
            Self::Rag => "rag",
            Self::LlmJudge => "llm_judge",
            Self::Custom => "custom",
            Self::External => "external",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "rule" => Self::Rule,
            "schema" => Self::Schema,
            "safety" => Self::Safety,
            "performance" => Self::Performance,
            "tool_quality" | "tool" => Self::ToolQuality,
            "rag" => Self::Rag,
            "llm_judge" | "llm" => Self::LlmJudge,
            "custom" => Self::Custom,
            "external" => Self::External,
            _ => Self::Deterministic,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScoreType {
    #[default]
    Numeric,
    Boolean,
    Categorical,
}

impl ScoreType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Numeric => "numeric",
            Self::Boolean => "boolean",
            Self::Categorical => "categorical",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "boolean" | "bool" => Self::Boolean,
            "categorical" | "category" => Self::Categorical,
            _ => Self::Numeric,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvaluation {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub target_type: EvalTargetType,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub span_id: Option<String>,
    #[serde(default)]
    pub experiment_run_id: Option<String>,
    pub evaluator_id: String,
    pub evaluator_name: String,
    #[serde(default)]
    pub evaluator_type: EvaluatorType,
    #[serde(default = "default_eval_version")]
    pub evaluator_version: String,
    pub score: f64,
    #[serde(default)]
    pub score_type: ScoreType,
    #[serde(default)]
    pub passed: Option<bool>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub execution_ms: i64,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvalRollup {
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub target_type: EvalTargetType,
    pub target_id: String,
    pub avg_score: f64,
    #[serde(default)]
    pub pass_rate: Option<f64>,
    pub eval_count: i64,
    #[serde(alias = "updated_at_ms")]
    pub updated_at: i64,
}

fn default_eval_version() -> String {
    "1.0.0".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluatorConfig {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub target_type: EvalTargetType,
    #[serde(default)]
    pub evaluator_type: EvaluatorType,
    /// Built-in kind (e.g. `latency_budget`, `cost_budget`, `no_errors`, `no_loop`,
    /// `tool_sequence`, `regex_match`, `contains_keywords`, `json_schema_keys`,
    /// `pii_leakage`, `output_not_empty`, `exact_match`, `llm_judge`).
    pub kind: String,
    #[serde(default = "default_eval_version")]
    pub version: String,
    #[serde(default)]
    pub enabled: bool,
    /// Optional agent scope; None applies to all agents.
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    /// Sampling rate in [0.0, 1.0] for automatic on-ingest evaluation.
    #[serde(default = "default_one")]
    pub sample_rate: f64,
    #[serde(default)]
    pub config: Value,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

fn default_one() -> f64 {
    1.0
}

// ---------------------------------------------------------------------------
// Datasets & Experiments
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDataset {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub version: i32,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub item_count: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDatasetItem {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub dataset_id: String,
    pub dataset_version: i32,
    #[serde(default)]
    pub name: Option<String>,
    pub input: Value,
    #[serde(default)]
    pub expected_output: Value,
    #[serde(default)]
    pub expected_behavior: Value,
    #[serde(default)]
    pub source_trace_id: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentExperiment {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub dataset_id: String,
    pub dataset_version: i32,
    pub agent_name: String,
    pub agent_version: String,
    #[serde(default)]
    pub prompt_version: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub evaluator_ids: Vec<String>,
    /// `pending | running | completed | failed`
    pub status: String,
    #[serde(default)]
    pub total_items: i32,
    #[serde(default)]
    pub completed_items: i32,
    #[serde(default)]
    pub passed_items: i32,
    #[serde(default)]
    pub avg_score: Option<f64>,
    #[serde(default)]
    pub avg_latency_ms: Option<f64>,
    #[serde(default)]
    pub total_cost_usd: f64,
    #[serde(default)]
    pub metadata: Value,
    pub created_at_ms: i64,
    #[serde(default)]
    pub completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentExperimentRun {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub experiment_id: String,
    pub dataset_item_id: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    pub status: String,
    #[serde(default)]
    pub actual_output: Value,
    #[serde(default)]
    pub latency_ms: i64,
    #[serde(default)]
    pub cost_usd: f64,
    #[serde(default)]
    pub total_tokens: i64,
    #[serde(default)]
    pub avg_score: Option<f64>,
    #[serde(default)]
    pub passed: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub evaluations: Vec<AgentEvaluation>,
    pub created_at_ms: i64,
}

// ---------------------------------------------------------------------------
// Analytics, Monitoring, Privacy & Pricing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAnalyticsOverview {
    pub total_executions: i64,
    pub successful_executions: i64,
    pub failed_executions: i64,
    pub error_rate: f64,
    pub success_rate: f64,
    pub loop_warning_count: i64,
    pub latency: LatencyPercentiles,
    pub model_latency: LatencyPercentiles,
    pub tool_latency: LatencyPercentiles,
    pub total_tokens: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub total_cost_usd: f64,
    pub cost_per_execution_usd: f64,
    pub cost_per_successful_execution_usd: f64,
    pub avg_eval_score: Option<f64>,
    pub eval_pass_rate: Option<f64>,
    pub failure_sources: BTreeMap<String, i64>,
    pub timeseries: Vec<AnalyticsTimeBucket>,
    pub by_agent: Vec<DimensionBreakdown>,
    pub by_version: Vec<DimensionBreakdown>,
    pub by_model: Vec<DimensionBreakdown>,
    pub by_tool: Vec<ToolBreakdown>,
    pub by_environment: Vec<DimensionBreakdown>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsTimeBucket {
    pub bucket_ts_ms: i64,
    pub executions: i64,
    pub errors: i64,
    pub error_rate: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub total_cost_usd: f64,
    pub total_tokens: i64,
    pub avg_eval_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionBreakdown {
    pub key: String,
    #[serde(default)]
    pub secondary: Option<String>,
    pub executions: i64,
    pub successes: i64,
    pub failures: i64,
    pub error_rate: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
    pub cost_per_execution_usd: f64,
    pub avg_eval_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolBreakdown {
    pub tool_name: String,
    pub invocations: i64,
    pub failures: i64,
    pub failure_rate: f64,
    pub total_retries: i64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub total_cost_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AnalyticsFilter {
    #[serde(default)]
    pub start_ts_ms: Option<i64>,
    #[serde(default)]
    pub end_ts_ms: Option<i64>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub agent_version: Option<String>,
    #[serde(default)]
    pub prompt_version: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
}

impl AnalyticsFilter {
    pub fn to_trace_list_filter(&self) -> TraceListFilter {
        TraceListFilter {
            agent_name: self.agent_name.clone(),
            agent_version: self.agent_version.clone(),
            prompt_version: self.prompt_version.clone(),
            environment: self.environment.clone(),
            status: self.status.clone(),
            model: self.model.clone(),
            provider: self.provider.clone(),
            tag: self.tag.clone(),
            user_id: self.user_id.clone(),
            session_id: self.session_id.clone(),
            start_ts_ms: self.start_ts_ms,
            end_ts_ms: self.end_ts_ms,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CompareFilter {
    #[serde(default)]
    pub start_ts_ms: Option<i64>,
    #[serde(default)]
    pub end_ts_ms: Option<i64>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub baseline: Option<AnalyticsFilter>,
    #[serde(default)]
    pub candidate: Option<AnalyticsFilter>,
    #[serde(default, alias = "baseline_version")]
    pub baseline_agent_version: Option<String>,
    #[serde(default, alias = "candidate_version")]
    pub candidate_agent_version: Option<String>,
    #[serde(default)]
    pub baseline_prompt_version: Option<String>,
    #[serde(default)]
    pub candidate_prompt_version: Option<String>,
    #[serde(default)]
    pub baseline_model: Option<String>,
    #[serde(default)]
    pub candidate_model: Option<String>,
    #[serde(default)]
    pub baseline_environment: Option<String>,
    #[serde(default)]
    pub candidate_environment: Option<String>,
    #[serde(default)]
    pub baseline_label: Option<String>,
    #[serde(default)]
    pub candidate_label: Option<String>,
}

impl CompareFilter {
    pub fn resolve_groups(&self) -> (AnalyticsFilter, AnalyticsFilter) {
        let mut base = self.baseline.clone().unwrap_or_default();
        let mut cand = self.candidate.clone().unwrap_or_default();

        for group in [&mut base, &mut cand] {
            if group.start_ts_ms.is_none() {
                group.start_ts_ms = self.start_ts_ms;
            }
            if group.end_ts_ms.is_none() {
                group.end_ts_ms = self.end_ts_ms;
            }
            if group.agent_name.is_none() {
                group.agent_name = self.agent_name.clone();
            }
            if group.environment.is_none() {
                group.environment = self.environment.clone();
            }
        }

        if let Some(v) = self
            .baseline_agent_version
            .as_ref()
            .filter(|s| !s.is_empty())
        {
            base.agent_version = Some(v.clone());
        }
        if let Some(v) = self
            .candidate_agent_version
            .as_ref()
            .filter(|s| !s.is_empty())
        {
            cand.agent_version = Some(v.clone());
        }
        if let Some(v) = self
            .baseline_prompt_version
            .as_ref()
            .filter(|s| !s.is_empty())
        {
            base.prompt_version = Some(v.clone());
        }
        if let Some(v) = self
            .candidate_prompt_version
            .as_ref()
            .filter(|s| !s.is_empty())
        {
            cand.prompt_version = Some(v.clone());
        }
        if let Some(v) = self.baseline_model.as_ref().filter(|s| !s.is_empty()) {
            base.model = Some(v.clone());
        }
        if let Some(v) = self.candidate_model.as_ref().filter(|s| !s.is_empty()) {
            cand.model = Some(v.clone());
        }
        if let Some(v) = self.baseline_environment.as_ref().filter(|s| !s.is_empty()) {
            base.environment = Some(v.clone());
        }
        if let Some(v) = self
            .candidate_environment
            .as_ref()
            .filter(|s| !s.is_empty())
        {
            cand.environment = Some(v.clone());
        }

        (base, cand)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareGroupMetrics {
    pub sample_size: usize,
    pub executions: i64,
    pub successes: i64,
    pub failures: i64,
    pub success_rate: f64,
    pub error_rate: f64,
    pub loop_warning_count: i64,
    pub loop_rate: f64,
    pub latency: LatencyPercentiles,
    pub total_tokens: i64,
    pub avg_tokens: f64,
    pub total_cost_nanos: i64,
    pub total_cost_usd: f64,
    pub avg_cost_usd: f64,
    pub avg_eval_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareResponse {
    pub baseline_label: String,
    pub candidate_label: String,
    pub baseline_sample_size: usize,
    pub candidate_sample_size: usize,
    pub min_significance_samples: usize,
    pub sufficient_samples: bool,
    /// `significant_regression | significant_improvement | no_significant_change | insufficient_data`
    pub overall_verdict: String,
    pub baseline: CompareGroupMetrics,
    pub candidate: CompareGroupMetrics,
    pub latency_ms: crate::stats::MetricDelta,
    pub cost_usd: crate::stats::MetricDelta,
    pub total_tokens: crate::stats::MetricDelta,
    pub error_rate: crate::stats::MetricDelta,
    pub success_rate: crate::stats::MetricDelta,
    pub eval_score: crate::stats::MetricDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAlertRule {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub name: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    /// `failure_rate | p95_latency_ms | cost_usd | eval_score_drop | tool_failure_rate | loop_rate | avg_tokens`
    pub metric: String,
    /// `gt | gte | lt | lte`
    pub operator: String,
    pub threshold: f64,
    pub window_minutes: i32,
    pub cooldown_minutes: i32,
    pub enabled: bool,
    /// `ok | firing`
    pub state: String,
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub last_evaluated_ms: Option<i64>,
    #[serde(default)]
    pub last_triggered_ms: Option<i64>,
    #[serde(default)]
    pub last_resolved_ms: Option<i64>,
    #[serde(default)]
    pub last_value: Option<f64>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAlertEvent {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub rule_id: String,
    pub rule_name: String,
    /// `triggered | resolved`
    pub event_type: String,
    pub metric: String,
    pub observed_value: f64,
    pub threshold: f64,
    pub delivered: bool,
    #[serde(default)]
    pub delivery_error: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPrivacyPolicy {
    pub project_id: String,
    pub capture_inputs: bool,
    pub capture_outputs: bool,
    pub capture_tool_args: bool,
    pub capture_tool_results: bool,
    pub capture_prompts: bool,
    pub capture_responses: bool,
    pub capture_user_id: bool,
    pub redact_pii: bool,
    #[serde(default)]
    pub redact_keys: Vec<String>,
    #[serde(default)]
    pub exclude_attributes: Vec<String>,
    pub retention_days: i64,
    pub sample_rate: f64,
    pub updated_at_ms: i64,
}

impl AgentPrivacyPolicy {
    /// Privacy-conscious defaults: automatic PII redaction enabled, standard secret keys redacted,
    /// 30-day retention, user_id capture off until opted in.
    pub fn default_for_project(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            capture_inputs: true,
            capture_outputs: true,
            capture_tool_args: true,
            capture_tool_results: true,
            capture_prompts: true,
            capture_responses: true,
            capture_user_id: false,
            redact_pii: true,
            redact_keys: vec![
                "password".to_string(),
                "secret".to_string(),
                "token".to_string(),
                "api_key".to_string(),
                "authorization".to_string(),
                "cookie".to_string(),
                "ssn".to_string(),
                "credit_card".to_string(),
            ],
            exclude_attributes: Vec::new(),
            retention_days: crate::DEFAULT_RETENTION_DAYS,
            sample_rate: 1.0,
            updated_at_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPricingRule {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub provider: String,
    pub model_pattern: String,
    pub input_per_1m_usd: f64,
    pub output_per_1m_usd: f64,
    pub cached_input_per_1m_usd: f64,
    pub updated_at_ms: i64,
}

// ---------------------------------------------------------------------------
// 3-Level GDPR / Privacy Erasure Tombstones
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErasureLevel {
    Trace,
    Session,
    User,
}

impl ErasureLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Session => "session",
            Self::User => "user",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "session" | "conversation" | "thread" => Self::Session,
            "user" | "enduser" => Self::User,
            _ => Self::Trace,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Trace => 0,
            Self::Session => 1,
            Self::User => 2,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Trace),
            1 => Some(Self::Session),
            2 => Some(Self::User),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentErasureTombstone {
    pub id: String,
    #[serde(default)]
    pub project_id: String,
    pub level: ErasureLevel,
    pub target_id: String,
    pub created_at: i64,
    #[serde(default)]
    pub expires_at: Option<i64>,
}

impl AgentErasureTombstone {
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at.is_some_and(|exp| now_ms >= exp)
    }

    pub fn matches_trace(&self, trace: &AgentTrace) -> bool {
        if !self.project_id.is_empty()
            && !trace.project_id.is_empty()
            && self.project_id != trace.project_id
        {
            return false;
        }
        match self.level {
            ErasureLevel::Trace => trace.trace_id == self.target_id,
            ErasureLevel::Session => trace.session_id.as_deref() == Some(self.target_id.as_str()),
            ErasureLevel::User => trace.user_id.as_deref() == Some(self.target_id.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserializes_legacy_span_and_trace_json_with_defaults() {
        let legacy_span = json!({
            "span_id": "sp_1",
            "trace_id": "tr_1",
            "name": "llm_call",
            "span_type": "llm",
            "start_ts_ms": 1000,
            "cost_usd": 0.0025
        });
        let mut span: AgentSpan = serde_json::from_value(legacy_span).unwrap();
        assert_eq!(span.span_phase, SpanPhase::Final);
        assert!(span.linked_span_ids.is_empty());
        assert_eq!(span.cost_nanos, 0);
        span.normalize_cost();
        assert_eq!(span.cost_nanos, 2_500_000);

        let legacy_trace = json!({
            "trace_id": "tr_1",
            "agent_name": "support-bot",
            "start_ts_ms": 1000,
            "total_cost_usd": 0.015
        });
        let mut trace: AgentTrace = serde_json::from_value(legacy_trace).unwrap();
        assert_eq!(trace.parent_trace_id, None);
        assert_eq!(trace.total_cost_nanos, 0);
        trace.normalize_cost();
        assert_eq!(trace.total_cost_nanos, 15_000_000);
    }

    #[test]
    fn span_phase_and_guardrail_roundtrip_serde_and_u8() {
        assert_eq!(
            serde_json::to_string(&SpanPhase::Start).unwrap(),
            "\"start\""
        );
        assert_eq!(
            serde_json::to_string(&SpanPhase::Update).unwrap(),
            "\"update\""
        );
        assert_eq!(
            serde_json::to_string(&SpanPhase::Final).unwrap(),
            "\"final\""
        );
        for phase in [SpanPhase::Start, SpanPhase::Update, SpanPhase::Final] {
            assert_eq!(SpanPhase::from_u8(phase.to_u8()), Some(phase));
        }
        assert_eq!(SpanPhase::from_u8(99), None);

        assert_eq!(
            serde_json::to_string(&SpanType::Guardrail).unwrap(),
            "\"guardrail\""
        );
        assert_eq!(SpanType::parse("safety"), SpanType::Guardrail);
        assert_eq!(SpanType::parse("moderation"), SpanType::Guardrail);
        assert_eq!(
            SpanType::from_u8(SpanType::Guardrail.to_u8()),
            Some(SpanType::Guardrail)
        );
    }

    #[test]
    fn erasure_tombstone_matches_three_levels_and_expiry() {
        let trace = AgentTrace {
            trace_id: "tr_99".into(),
            project_id: "proj_1".into(),
            agent_name: "agent".into(),
            agent_version: "1.0.0".into(),
            prompt_version: None,
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".into(),
            session_id: Some("sess_42".into()),
            user_id: Some("usr_7".into()),
            workflow_id: None,
            parent_trace_id: None,
            status: TraceStatus::Ok,
            start_ts_ms: 1000,
            end_ts_ms: Some(1100),
            duration_ms: 100,
            input: Value::Null,
            output: Value::Null,
            error_type: None,
            error_message: None,
            failure_source: None,
            metadata: Value::Null,
            tags: vec![],
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
        };

        let t_trace = AgentErasureTombstone {
            id: "er_1".into(),
            project_id: "proj_1".into(),
            level: ErasureLevel::Trace,
            target_id: "tr_99".into(),
            created_at: 1000,
            expires_at: Some(2000),
        };
        assert!(t_trace.matches_trace(&trace));
        assert!(!t_trace.is_expired(1999));
        assert!(t_trace.is_expired(2000));

        let t_session = AgentErasureTombstone {
            level: ErasureLevel::Session,
            target_id: "sess_42".into(),
            expires_at: None,
            ..t_trace.clone()
        };
        assert!(t_session.matches_trace(&trace));
        assert!(!t_session.is_expired(i64::MAX));

        let t_user = AgentErasureTombstone {
            level: ErasureLevel::User,
            target_id: "usr_7".into(),
            ..t_trace.clone()
        };
        assert!(t_user.matches_trace(&trace));

        let t_other_proj = AgentErasureTombstone {
            project_id: "proj_other".into(),
            ..t_trace
        };
        assert!(!t_other_proj.matches_trace(&trace));
    }
}
