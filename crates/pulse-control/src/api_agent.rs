//! AI Agent Observability & Evaluation HTTP APIs, Ingestion Pipeline, Alerts & Retention.
//!
//! Zero SQL writes occur on trace/span ingestion (`POST /v1/agent/traces` and
//! `POST /v1/otel/v1/traces`); traces and spans flow directly into `AgentTraceStore`
//! (`PTWL` WAL + L0 + `PLSS` segments) after read-only privacy/pricing governance.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use chrono::Utc;
use pulse_agent::{
    compare_continuous_metric, cost_nanos_to_usd, decode_plst_batch, estimate_cost_nanos,
    normalize_otlp_protobuf, normalize_otlp_traces, run_deterministic_evaluator, sanitize_batch,
    sync_span_cost, sync_trace_cost, AgentAlertEvent, AgentAlertRule, AgentAnalyticsOverview,
    AgentDataset, AgentDatasetItem, AgentErasureTombstone, AgentEvalRollup, AgentEvaluation,
    AgentExperiment, AgentExperimentRun, AgentPrivacyPolicy, AgentSpan, AgentTrace,
    AnalyticsFilter, CompareFilter, CompareResponse, ErasureLevel, EvalContext, EvalTargetType,
    EvaluatorConfig, EvaluatorType, ModelPricingRule, OtlpExportTraceRequest, ProtocolError,
    ScoreType, SessionDetail, SessionListFilter, SessionListResponse, SpanPhase, SpanType,
    StoreError, TraceDetail, TraceIngestBatch, TraceIngestResponse, TraceListFilter,
    TraceListResponse, TraceStatus, MIN_SIGNIFICANCE_SAMPLES, PLST_MAGIC,
};
use pulse_storage::validate_outbound_url_ssrf;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

use crate::agent_eval_worker::EvalJob;
use crate::api::{
    alert_condition_met, post_signed_webhook, webhook_signing_secret, ApiRes, AuthPrincipal,
    ControlPlaneService, CtrlError, ALERT_OPERATORS,
};
use crate::models::{unseal_byok_config_json, unseal_secret};

const MAX_AGENT_INGEST_BODY_BYTES: usize = 8 * 1024 * 1024;
const ALLOWED_ALERT_METRICS: [&str; 7] = [
    "failure_rate",
    "p95_latency_ms",
    "cost_usd",
    "eval_score_drop",
    "tool_failure_rate",
    "loop_rate",
    "avg_tokens",
];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentRetentionStats {
    pub projects_checked: usize,
    pub segments_pruned: usize,
    pub evaluations_pruned: u64,
    pub tombstones_pruned: u64,
}

/// Builds the public agent trace ingestion router (`POST /v1/agent/traces` and
/// `POST /v1/otel/v1/traces`), protected by `x-pulse-key: pk_...` (with
/// `Authorization: Bearer ...` fallback), permissive CORS, `8 MiB` body cap, and
/// per-project rate limiting.
pub fn build_agent_ingest_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .route("/v1/agent/traces", post(handle_ingest_agent_traces))
        .route("/v1/otel/v1/traces", post(handle_ingest_otel_traces))
        .layer(DefaultBodyLimit::max(MAX_AGENT_INGEST_BODY_BYTES))
        .layer(CorsLayer::permissive())
        .with_state(ctrl)
}

/// Protected project-scoped `/v1/projects/:id/agent/...` routes mounted inside
/// `build_admin_control_router` above `require_admin_auth`.
pub(crate) fn agent_admin_routes() -> Router<Arc<ControlPlaneService>> {
    Router::new()
        .route("/v1/projects/:id/agent/sessions", get(handle_list_sessions))
        .route(
            "/v1/projects/:id/agent/sessions/:session_id",
            get(handle_get_session),
        )
        .route("/v1/projects/:id/agent/traces", get(handle_list_traces))
        .route(
            "/v1/projects/:id/agent/traces/:trace_id",
            get(handle_get_trace).delete(handle_delete_trace_or_erase),
        )
        .route("/v1/projects/:id/agent/erasure", post(handle_post_erasure))
        .route(
            "/v1/projects/:id/agent/analytics",
            get(handle_agent_analytics),
        )
        .route("/v1/projects/:id/agent/compare", get(handle_agent_compare))
        .route(
            "/v1/projects/:id/agent/evaluators",
            get(handle_list_evaluators).post(handle_upsert_evaluator),
        )
        .route(
            "/v1/projects/:id/agent/evaluators/:eval_id",
            delete(handle_delete_evaluator),
        )
        .route(
            "/v1/projects/:id/agent/traces/:trace_id/evaluate",
            post(handle_trigger_trace_eval),
        )
        .route("/v1/projects/:id/agent/evaluate", post(handle_trigger_eval))
        .route(
            "/v1/projects/:id/agent/evaluations",
            get(handle_list_evaluations).post(handle_create_evaluation),
        )
        .route(
            "/v1/projects/:id/agent/datasets",
            get(handle_list_datasets).post(handle_upsert_dataset),
        )
        .route(
            "/v1/projects/:id/agent/datasets/:dataset_id",
            delete(handle_delete_dataset),
        )
        .route(
            "/v1/projects/:id/agent/datasets/:dataset_id/items",
            get(handle_list_dataset_items).post(handle_upsert_dataset_item),
        )
        .route(
            "/v1/projects/:id/agent/datasets/:dataset_id/import",
            post(handle_import_dataset_items),
        )
        .route(
            "/v1/projects/:id/agent/datasets/:dataset_id/items/:item_id",
            delete(handle_delete_dataset_item),
        )
        .route(
            "/v1/projects/:id/agent/traces/:trace_id/promote",
            post(handle_promote_trace),
        )
        .route(
            "/v1/projects/:id/agent/experiments",
            get(handle_list_experiments).post(handle_create_experiment),
        )
        .route(
            "/v1/projects/:id/agent/experiments/compare",
            get(handle_compare_experiments),
        )
        .route(
            "/v1/projects/:id/agent/experiments/:exp_id",
            get(handle_get_experiment),
        )
        .route(
            "/v1/projects/:id/agent/experiments/:exp_id/runs",
            post(handle_create_experiment_run),
        )
        .route(
            "/v1/projects/:id/agent/alerts",
            get(handle_list_agent_alerts).post(handle_upsert_agent_alert),
        )
        .route(
            "/v1/projects/:id/agent/alerts/events",
            get(handle_list_agent_alert_events),
        )
        .route(
            "/v1/projects/:id/agent/alerts/evaluate",
            post(handle_evaluate_agent_alerts_all),
        )
        .route(
            "/v1/projects/:id/agent/alerts/:rule_id",
            delete(handle_delete_agent_alert),
        )
        .route(
            "/v1/projects/:id/agent/alerts/:rule_id/evaluate",
            post(handle_evaluate_agent_alert_one),
        )
        .route(
            "/v1/projects/:id/agent/privacy",
            get(handle_get_privacy).put(handle_put_privacy),
        )
        .route(
            "/v1/projects/:id/agent/pricing",
            get(handle_list_pricing).post(handle_upsert_pricing),
        )
        .route(
            "/v1/projects/:id/agent/pricing/:rule_id",
            delete(handle_delete_pricing),
        )
}

// ===========================================================================
// Public Ingest Pipeline (Zero SQL Writes)
// ===========================================================================

enum IngestError {
    Unauthorized(&'static str),
    BadRequest(String),
    PayloadTooLarge(String),
    RateLimited(u64, &'static str),
    ServiceUnavailable(String),
    Internal(String),
}

impl IntoResponse for IngestError {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "status": "error", "code": "unauthorized", "message": msg })),
            )
                .into_response(),
            Self::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "status": "error", "code": "bad_request", "message": msg })),
            )
                .into_response(),
            Self::PayloadTooLarge(msg) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({ "status": "error", "code": "payload_too_large", "message": msg })),
            )
                .into_response(),
            Self::RateLimited(retry_after, code) => {
                let mut resp = (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "status": "error",
                        "code": code,
                        "retry_after_secs": retry_after,
                        "message": "rate limit exceeded"
                    })),
                )
                    .into_response();
                if let Ok(hv) = HeaderValue::from_str(&retry_after.max(1).to_string()) {
                    resp.headers_mut().insert(header::RETRY_AFTER, hv);
                }
                resp
            }
            Self::ServiceUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "error", "code": "service_unavailable", "message": msg })),
            )
                .into_response(),
            Self::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "status": "error", "code": "internal_error", "message": msg })),
            )
                .into_response(),
        }
    }
}

fn extract_ingest_key(headers: &HeaderMap) -> Option<&str> {
    if let Some(v) = headers.get("x-pulse-key").and_then(|h| h.to_str().ok()) {
        let trimmed = v.trim();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    if let Some(v) = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        let trimmed = v.trim();
        if trimmed.len() > 7 && trimmed[..7].eq_ignore_ascii_case("bearer ") {
            let tok = trimmed[7..].trim();
            if !tok.is_empty() {
                return Some(tok);
            }
        }
    }
    None
}

async fn authenticate_ingest_request(
    ctrl: &ControlPlaneService,
    headers: &HeaderMap,
) -> Result<String, IngestError> {
    let key = extract_ingest_key(headers)
        .ok_or(IngestError::Unauthorized("missing x-pulse-key header"))?;
    ctrl.authenticate_api_key_str(key)
        .await
        .ok_or(IngestError::Unauthorized(
            "invalid or unauthorized ingest key",
        ))
}

async fn handle_ingest_agent_traces(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<TraceIngestResponse>, IngestError> {
    let project_id = authenticate_ingest_request(&ctrl, &headers).await?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    let is_plst = if content_type.contains("application/x-pulse-trace") {
        true
    } else if content_type.contains("application/json") {
        false
    } else {
        body.starts_with(&PLST_MAGIC)
    };

    let batch = if is_plst {
        decode_plst_batch(&body).map_err(|e| match e {
            ProtocolError::ResourceLimitExceeded(msg) => {
                IngestError::PayloadTooLarge(msg.to_string())
            }
            other => IngestError::BadRequest(format!("invalid PLST trace payload: {other}")),
        })?
    } else {
        serde_json::from_slice::<TraceIngestBatch>(&body)
            .map_err(|e| IngestError::BadRequest(format!("invalid JSON trace payload: {e}")))?
    };

    ingest_canonical_batch(&ctrl, &project_id, batch).await
}

async fn handle_ingest_otel_traces(
    State(ctrl): State<Arc<ControlPlaneService>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<TraceIngestResponse>, IngestError> {
    let project_id = authenticate_ingest_request(&ctrl, &headers).await?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    let is_protobuf = if content_type.contains("application/x-protobuf")
        || content_type.contains("application/protobuf")
    {
        true
    } else if content_type.contains("application/json") {
        false
    } else {
        !matches!(
            body.iter().copied().find(|b| !b.is_ascii_whitespace()),
            Some(b'{') | Some(b'[')
        )
    };

    let batch = if is_protobuf {
        normalize_otlp_protobuf(&body)
            .map_err(|e| IngestError::BadRequest(format!("invalid OTLP protobuf payload: {e}")))?
    } else {
        let req = serde_json::from_slice::<OtlpExportTraceRequest>(&body)
            .map_err(|e| IngestError::BadRequest(format!("invalid OTLP JSON payload: {e}")))?;
        normalize_otlp_traces(req)
    };

    ingest_canonical_batch(&ctrl, &project_id, batch).await
}

async fn ingest_canonical_batch(
    ctrl: &ControlPlaneService,
    project_id: &str,
    mut batch: TraceIngestBatch,
) -> Result<Json<TraceIngestResponse>, IngestError> {
    let units = (batch.traces.len() + batch.spans.len()).max(1) as u64;
    if let Err(retry_after) = ctrl.agent_rate_limit().charge(project_id, units) {
        return Err(IngestError::RateLimited(retry_after, "rate_limited"));
    }

    // Read-only MetaStore lookups (ZERO SQL writes on ingest).
    let policy = ctrl
        .meta()
        .get_agent_privacy_policy(project_id)
        .await
        .map_err(|e| IngestError::Internal(e.to_string()))?;
    let pricing_rules = ctrl
        .meta()
        .list_model_pricing(project_id)
        .await
        .map_err(|e| IngestError::Internal(e.to_string()))?;

    // Deterministic per-trace sampling when sample_rate < 1.0.
    if policy.sample_rate < 1.0 {
        if !policy.sample_rate.is_finite() || policy.sample_rate <= 0.0 {
            batch.traces.clear();
            batch.spans.clear();
        } else {
            let sr = policy.sample_rate;
            let keep = |trace_id: &str| -> bool {
                (xxh3_64(trace_id.as_bytes()) as f64 / u64::MAX as f64) < sr
            };
            batch.traces.retain(|t| keep(&t.trace_id));
            batch.spans.retain(|s| keep(&s.trace_id));
        }
    }

    if batch.traces.is_empty() && batch.spans.is_empty() {
        return Ok(Json(TraceIngestResponse {
            accepted_traces: 0,
            accepted_spans: 0,
            truncated_payloads: 0,
            redacted_fields: 0,
            dropped_spans: 0,
            warnings: Vec::new(),
        }));
    }

    // Normalize project_id, clamp tokens, and compute nanodollar costs before sanitize_batch
    // (which rolls up span costs onto traces).
    let mut traces_with_positive_span_cost = HashSet::new();
    for sp in &mut batch.spans {
        sp.project_id = project_id.to_string();
        sp.input_tokens = sp.input_tokens.max(0);
        sp.output_tokens = sp.output_tokens.max(0);
        sp.cached_tokens = sp.cached_tokens.max(0);
        sp.total_tokens = sp.total_tokens.max(0);
        sync_span_cost(sp);
        if sp.cost_nanos == 0
            && (sp.input_tokens > 0 || sp.output_tokens > 0 || sp.cached_tokens > 0)
        {
            sp.cost_nanos = estimate_cost_nanos(
                sp.provider.as_deref(),
                sp.model.as_deref().or(sp.model_id.as_deref()),
                sp.input_tokens,
                sp.output_tokens,
                sp.cached_tokens,
                &pricing_rules,
            );
            sp.cost_usd = cost_nanos_to_usd(sp.cost_nanos);
        }
        if sp.cost_nanos > 0 {
            traces_with_positive_span_cost.insert(sp.trace_id.clone());
        }
    }

    for tr in &mut batch.traces {
        tr.project_id = project_id.to_string();
        tr.input_tokens = tr.input_tokens.max(0);
        tr.output_tokens = tr.output_tokens.max(0);
        tr.cached_tokens = tr.cached_tokens.max(0);
        tr.total_tokens = tr.total_tokens.max(0);
        sync_trace_cost(tr);
        if tr.total_cost_nanos == 0
            && !traces_with_positive_span_cost.contains(&tr.trace_id)
            && (tr.input_tokens > 0 || tr.output_tokens > 0 || tr.cached_tokens > 0)
        {
            tr.total_cost_nanos = estimate_cost_nanos(
                tr.primary_provider.as_deref(),
                tr.primary_model.as_deref(),
                tr.input_tokens,
                tr.output_tokens,
                tr.cached_tokens,
                &pricing_rules,
            );
            tr.total_cost_usd = cost_nanos_to_usd(tr.total_cost_nanos);
        }
    }

    let sanitize_stats = sanitize_batch(&mut batch, &policy);
    let blobs_deduped = count_intra_batch_deduped_blobs(&batch);
    let eval_candidates = collect_eval_candidates(&batch);
    let batch_match_keys = collect_batch_match_keys(&batch);
    let ingest_started_ms = Utc::now().timestamp_millis();

    let mut resp = ctrl
        .agent_store()
        .ingest_batch(project_id, batch)
        .await
        .map_err(|e| match e {
            StoreError::InvalidIdentifier(msg) => IngestError::BadRequest(msg),
            StoreError::WalBackpressure { .. } | StoreError::L0Backpressure { .. } => {
                IngestError::RateLimited(1, "l0_backpressure")
            }
            StoreError::Shutdown
            | StoreError::NotWriter { .. }
            | StoreError::StaleWriter { .. } => IngestError::ServiceUnavailable(e.to_string()),
            // The WAL write failed (disk full, unwritable directory, store down): nothing acked.
            e @ (StoreError::Io(_) | StoreError::ObjectStore(_)) => {
                IngestError::ServiceUnavailable(e.to_string())
            }
            other => IngestError::Internal(other.to_string()),
        })?;

    if let Ok(purged_finite) = ctrl
        .meta()
        .list_purged_finite_erasure_tombstones(project_id, ingest_started_ms)
        .await
    {
        if !purged_finite.is_empty() {
            let matching_ids: Vec<String> = purged_finite
                .into_iter()
                .filter(|tb| batch_match_keys.may_match(tb))
                .map(|tb| tb.id)
                .collect();
            if !matching_ids.is_empty() {
                match ctrl
                    .meta()
                    .reset_erasure_tombstones_purged(&matching_ids)
                    .await
                {
                    Ok(n) if n > 0 => ctrl.notify_agent_erasure(),
                    Ok(_) => {}
                    Err(err) => tracing::warn!(
                        project_id = %project_id,
                        error = %err,
                        "failed to reopen matching purged finite erasure tombstones on ingest"
                    ),
                }
            }
        }
    }

    resp.truncated_payloads = sanitize_stats.truncated_payloads;
    resp.redacted_fields = sanitize_stats.redacted_fields;
    if resp.dropped_spans > 0 {
        ctrl.agent_metrics()
            .spans_dropped_trace_limit
            .fetch_add(resp.dropped_spans as u64, Ordering::Relaxed);
        resp.warnings.push(format!(
            "{} span(s) dropped: their trace reached its span or size cap",
            resp.dropped_spans
        ));
    }
    resp.dropped_spans += sanitize_stats.dropped_spans;
    if sanitize_stats.dropped_spans > 0 {
        resp.warnings.push(format!(
            "{} span(s) dropped for exceeding MAX_SPANS_PER_TRACE",
            sanitize_stats.dropped_spans
        ));
    }

    ctrl.agent_metrics()
        .traces_ingested
        .fetch_add(resp.accepted_traces as u64, Ordering::Relaxed);
    ctrl.agent_metrics()
        .spans_ingested
        .fetch_add(resp.accepted_spans as u64, Ordering::Relaxed);
    if blobs_deduped > 0 {
        ctrl.agent_metrics()
            .blobs_deduped
            .fetch_add(blobs_deduped, Ordering::Relaxed);
    }

    for (trace_id, session_id) in eval_candidates {
        ctrl.try_enqueue_eval(EvalJob {
            project_id: project_id.to_string(),
            session_id,
            trace_id: Some(trace_id),
            span_id: None,
            evaluator_ids: None,
        });
    }

    Ok(Json(resp))
}

fn count_intra_batch_deduped_blobs(batch: &TraceIngestBatch) -> u64 {
    let mut seen: HashMap<(u64, usize), Vec<Vec<u8>>> = HashMap::new();
    let mut deduped = 0u64;

    let mut observe = |val: &Value| {
        if val.is_null() {
            return;
        }
        let Ok(bytes) = serde_json::to_vec(val) else {
            return;
        };
        if bytes.is_empty() {
            return;
        }
        let key = (xxh3_64(&bytes), bytes.len());
        let bucket = seen.entry(key).or_default();
        if bucket.iter().any(|existing| existing == &bytes) {
            deduped += 1;
        } else {
            bucket.push(bytes);
        }
    };

    for tr in &batch.traces {
        observe(&tr.input);
        observe(&tr.output);
        observe(&tr.metadata);
    }
    for sp in &batch.spans {
        observe(&sp.input);
        observe(&sp.output);
        observe(&sp.attributes);
        observe(&sp.metadata);
        observe(&sp.model_params);
        for ev in &sp.events {
            observe(&ev.attributes);
        }
    }
    deduped
}

struct BatchMatchKeys {
    trace_ids: HashSet<String>,
    session_ids: HashSet<String>,
    user_ids: HashSet<String>,
    has_orphan_spans: bool,
}

impl BatchMatchKeys {
    fn may_match(&self, tb: &AgentErasureTombstone) -> bool {
        match tb.level {
            ErasureLevel::Trace => self.trace_ids.contains(&tb.target_id),
            ErasureLevel::Session => {
                self.session_ids.contains(&tb.target_id) || self.has_orphan_spans
            }
            ErasureLevel::User => self.user_ids.contains(&tb.target_id) || self.has_orphan_spans,
        }
    }
}

fn collect_batch_match_keys(batch: &TraceIngestBatch) -> BatchMatchKeys {
    let mut trace_ids = HashSet::with_capacity(batch.traces.len() + batch.spans.len());
    let mut session_ids = HashSet::new();
    let mut user_ids = HashSet::new();
    let mut trace_header_ids = HashSet::with_capacity(batch.traces.len());
    for tr in &batch.traces {
        trace_ids.insert(tr.trace_id.clone());
        trace_header_ids.insert(tr.trace_id.as_str());
        if let Some(ref sid) = tr.session_id {
            if !sid.is_empty() {
                session_ids.insert(sid.clone());
            }
        }
        if let Some(ref uid) = tr.user_id {
            if !uid.is_empty() {
                user_ids.insert(uid.clone());
            }
        }
    }
    let mut has_orphan_spans = false;
    for sp in &batch.spans {
        trace_ids.insert(sp.trace_id.clone());
        if !trace_header_ids.contains(sp.trace_id.as_str()) {
            has_orphan_spans = true;
        }
    }
    BatchMatchKeys {
        trace_ids,
        session_ids,
        user_ids,
        has_orphan_spans,
    }
}

fn collect_eval_candidates(batch: &TraceIngestBatch) -> Vec<(String, Option<String>)> {
    let mut session_by_trace: HashMap<&str, Option<String>> = HashMap::new();
    let mut ready_traces: HashSet<String> = HashSet::new();
    for tr in &batch.traces {
        session_by_trace.insert(tr.trace_id.as_str(), tr.session_id.clone());
        if tr.status != TraceStatus::Running {
            ready_traces.insert(tr.trace_id.clone());
        }
    }
    for sp in &batch.spans {
        if sp.span_phase == SpanPhase::Final {
            ready_traces.insert(sp.trace_id.clone());
        }
    }
    ready_traces
        .into_iter()
        .map(|tid| {
            let sid = session_by_trace.get(tid.as_str()).cloned().flatten();
            (tid, sid)
        })
        .collect()
}

// ===========================================================================
// Store/Meta Error Mapping Helpers
// ===========================================================================

fn map_store_err(err: StoreError) -> CtrlError {
    match err {
        StoreError::InvalidIdentifier(msg) => CtrlError::bad(msg),
        StoreError::WalBackpressure { .. } | StoreError::L0Backpressure { .. } => {
            CtrlError::new(StatusCode::TOO_MANY_REQUESTS, err.to_string())
        }
        StoreError::Shutdown | StoreError::NotWriter { .. } | StoreError::StaleWriter { .. } => {
            CtrlError::new(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
        }
        other => CtrlError::internal(other),
    }
}

async fn active_tombstones(
    ctrl: &ControlPlaneService,
    project_id: &str,
) -> Result<Vec<AgentErasureTombstone>, CtrlError> {
    ctrl.meta()
        .list_active_erasure_tombstones(project_id, Utc::now().timestamp_millis())
        .await
        .map_err(CtrlError::meta)
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", &Uuid::new_v4().simple().to_string()[..12])
}

// ===========================================================================
// Sessions, Traces, Erasure, Analytics & Compare
// ===========================================================================

fn norm_opt_str(opt: &mut Option<String>) {
    *opt = opt
        .take()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
}

fn normalize_session_list_filter(f: &mut SessionListFilter) {
    norm_opt_str(&mut f.user_id);
    norm_opt_str(&mut f.agent_name);
    norm_opt_str(&mut f.environment);
    norm_opt_str(&mut f.search);
    norm_opt_str(&mut f.sort_by);
    norm_opt_str(&mut f.sort_dir);
    norm_opt_str(&mut f.cursor);
}

fn normalize_trace_list_filter(f: &mut TraceListFilter) {
    norm_opt_str(&mut f.agent_name);
    norm_opt_str(&mut f.agent_version);
    norm_opt_str(&mut f.prompt_version);
    norm_opt_str(&mut f.environment);
    norm_opt_str(&mut f.status);
    norm_opt_str(&mut f.model);
    norm_opt_str(&mut f.provider);
    norm_opt_str(&mut f.tag);
    norm_opt_str(&mut f.user_id);
    norm_opt_str(&mut f.session_id);
    norm_opt_str(&mut f.parent_trace_id);
    norm_opt_str(&mut f.search);
    norm_opt_str(&mut f.sort_by);
    norm_opt_str(&mut f.sort_dir);
    norm_opt_str(&mut f.cursor);
}

fn normalize_analytics_filter(f: &mut AnalyticsFilter) {
    norm_opt_str(&mut f.agent_name);
    norm_opt_str(&mut f.agent_version);
    norm_opt_str(&mut f.prompt_version);
    norm_opt_str(&mut f.model);
    norm_opt_str(&mut f.provider);
    norm_opt_str(&mut f.tool_name);
    norm_opt_str(&mut f.environment);
    norm_opt_str(&mut f.status);
    norm_opt_str(&mut f.user_id);
    norm_opt_str(&mut f.session_id);
    norm_opt_str(&mut f.tag);
}

fn normalize_compare_filter(f: &mut CompareFilter) {
    norm_opt_str(&mut f.agent_name);
    norm_opt_str(&mut f.environment);
    norm_opt_str(&mut f.baseline_agent_version);
    norm_opt_str(&mut f.candidate_agent_version);
    norm_opt_str(&mut f.baseline_prompt_version);
    norm_opt_str(&mut f.candidate_prompt_version);
    norm_opt_str(&mut f.baseline_model);
    norm_opt_str(&mut f.candidate_model);
    norm_opt_str(&mut f.baseline_environment);
    norm_opt_str(&mut f.candidate_environment);
    norm_opt_str(&mut f.baseline_label);
    norm_opt_str(&mut f.candidate_label);
    if let Some(b) = f.baseline.as_mut() {
        normalize_analytics_filter(b);
    }
    if let Some(c) = f.candidate.as_mut() {
        normalize_analytics_filter(c);
    }
}

async fn handle_list_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(mut filter): Query<SessionListFilter>,
) -> ApiRes<SessionListResponse> {
    ctrl.authorize_and_check(&principal, &id).await?;
    normalize_session_list_filter(&mut filter);
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let mut resp = ctrl
        .agent_store()
        .list_sessions(&id, &filter, &tombstones)
        .await
        .map_err(map_store_err)?;

    let session_ids: Vec<String> = resp.sessions.iter().map(|s| s.session_id.clone()).collect();
    let rollups = ctrl
        .meta()
        .get_eval_rollups_batch(&id, EvalTargetType::Session, &session_ids)
        .await
        .map_err(CtrlError::meta)?;
    for s in &mut resp.sessions {
        if let Some(r) = rollups.get(&(EvalTargetType::Session, s.session_id.clone())) {
            s.avg_eval_score = Some(r.avg_score);
            s.eval_pass_rate = r.pass_rate;
        }
    }
    Ok(Json(resp))
}

async fn handle_get_session(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, session_id)): AxumPath<(String, String)>,
) -> ApiRes<SessionDetail> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let Some(mut detail) = ctrl
        .agent_store()
        .get_session_detail(&id, &session_id, &tombstones)
        .await
        .map_err(map_store_err)?
    else {
        return Err(CtrlError::not_found("agent session not found"));
    };

    let sess_rollups = ctrl
        .meta()
        .get_eval_rollups_batch(
            &id,
            EvalTargetType::Session,
            std::slice::from_ref(&session_id),
        )
        .await
        .map_err(CtrlError::meta)?;
    if let Some(r) = sess_rollups.get(&(EvalTargetType::Session, session_id.clone())) {
        detail.summary.avg_eval_score = Some(r.avg_score);
        detail.summary.eval_pass_rate = r.pass_rate;
    }

    let trace_ids: Vec<String> = detail.traces.iter().map(|t| t.trace_id.clone()).collect();
    let trace_rollups = ctrl
        .meta()
        .get_eval_rollups_batch(&id, EvalTargetType::Trace, &trace_ids)
        .await
        .map_err(CtrlError::meta)?;
    for tr in &mut detail.traces {
        if let Some(r) = trace_rollups.get(&(EvalTargetType::Trace, tr.trace_id.clone())) {
            tr.avg_eval_score = Some(r.avg_score);
            tr.eval_pass_rate = r.pass_rate;
        }
    }

    detail.evaluations = ctrl
        .meta()
        .list_session_evaluations(&id, &session_id)
        .await
        .map_err(CtrlError::meta)?;

    Ok(Json(detail))
}

async fn handle_list_traces(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(mut filter): Query<TraceListFilter>,
) -> ApiRes<TraceListResponse> {
    ctrl.authorize_and_check(&principal, &id).await?;
    normalize_trace_list_filter(&mut filter);
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let rollups = ctrl
        .meta()
        .list_agent_eval_rollups(&id, Some(EvalTargetType::Trace))
        .await
        .map_err(CtrlError::meta)?;
    let resp = ctrl
        .agent_store()
        .list_traces(&id, &filter, &rollups, &tombstones)
        .await
        .map_err(map_store_err)?;
    Ok(Json(resp))
}

async fn handle_get_trace(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, trace_id)): AxumPath<(String, String)>,
) -> ApiRes<TraceDetail> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let Some(mut detail) = ctrl
        .agent_store()
        .get_trace_detail(&id, &trace_id, &tombstones)
        .await
        .map_err(map_store_err)?
    else {
        return Err(CtrlError::not_found("agent trace not found"));
    };

    let rollups = ctrl
        .meta()
        .get_eval_rollups_batch(&id, EvalTargetType::Trace, std::slice::from_ref(&trace_id))
        .await
        .map_err(CtrlError::meta)?;
    if let Some(r) = rollups.get(&(EvalTargetType::Trace, trace_id.clone())) {
        detail.trace.avg_eval_score = Some(r.avg_score);
        detail.trace.eval_pass_rate = r.pass_rate;
    }

    detail.evaluations = ctrl
        .meta()
        .list_trace_evaluations(&id, &trace_id)
        .await
        .map_err(CtrlError::meta)?;

    Ok(Json(detail))
}

#[derive(Deserialize, Default)]
struct ErasureQuery {
    level: Option<String>,
    scope: Option<String>,
    target_id: Option<String>,
}

#[derive(Deserialize, Default)]
struct ErasureReq {
    level: Option<String>,
    scope: Option<String>,
    target_id: Option<String>,
    expires_at: Option<i64>,
}

fn parse_erasure_level(raw: &str) -> Result<ErasureLevel, CtrlError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(ErasureLevel::Trace),
        "session" | "conversation" | "thread" => Ok(ErasureLevel::Session),
        "user" | "enduser" => Ok(ErasureLevel::User),
        other => Err(CtrlError::bad(format!(
            "invalid erasure level '{other}': expected trace, session, or user"
        ))),
    }
}

async fn execute_erasure(
    ctrl: &ControlPlaneService,
    principal: &AuthPrincipal,
    project_id: &str,
    level: ErasureLevel,
    target_id: String,
    expires_at: Option<i64>,
) -> ApiRes<Value> {
    let target_id = target_id.trim().to_string();
    if target_id.is_empty() {
        return Err(CtrlError::bad("erasure target_id cannot be empty"));
    }

    // For User-level erasure, resolve matching session_ids and trace_ids from AgentTraceStore
    // BEFORE inserting the new User tombstone (using only pre-existing tombstones).
    let user_targets = if level == ErasureLevel::User {
        let existing_tombstones = active_tombstones(ctrl, project_id).await?;
        let empty_rollups = HashMap::new();
        let mut filter = TraceListFilter {
            user_id: Some(target_id.clone()),
            limit: Some(100),
            ..Default::default()
        };
        let mut session_ids = HashSet::new();
        let mut trace_ids = HashSet::new();
        loop {
            let page = ctrl
                .agent_store()
                .list_traces(project_id, &filter, &empty_rollups, &existing_tombstones)
                .await
                .map_err(map_store_err)?;
            for tr in page.traces {
                trace_ids.insert(tr.trace_id);
                if let Some(sid) = tr.session_id.filter(|s| !s.is_empty()) {
                    session_ids.insert(sid);
                }
            }
            match page.next_cursor {
                Some(c) if !c.is_empty() => filter.cursor = Some(c),
                _ => break,
            }
        }
        Some((
            session_ids.into_iter().collect::<Vec<_>>(),
            trace_ids.into_iter().collect::<Vec<_>>(),
        ))
    } else {
        None
    };

    let tb = AgentErasureTombstone {
        id: new_id("ers"),
        project_id: project_id.to_string(),
        level,
        target_id: target_id.clone(),
        created_at: Utc::now().timestamp_millis(),
        expires_at,
    };
    ctrl.meta()
        .insert_erasure_tombstone(&tb)
        .await
        .map_err(CtrlError::meta)?;

    if !ctrl.agent_store().is_read_only() {
        let _ = ctrl
            .agent_store()
            .purge_erased_in_l0(project_id, std::slice::from_ref(&tb))
            .await;
    }
    ctrl.notify_agent_erasure();

    let purged_evaluations = match user_targets {
        Some((sids, tids)) => ctrl
            .meta()
            .purge_evaluations_for_targets(project_id, &sids, &tids)
            .await
            .map_err(CtrlError::meta)?,
        None => ctrl
            .meta()
            .purge_erased_evaluations(project_id, level, &target_id)
            .await
            .map_err(CtrlError::meta)?,
    };

    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "agent.erasure",
        &format!("project:{project_id}"),
        json!({
            "tombstone_id": tb.id,
            "level": tb.level.as_str(),
            "target_id": tb.target_id,
            "purged_evaluations": purged_evaluations,
        }),
    )
    .await;

    Ok(Json(json!({
        "status": "ok",
        "tombstone": tb,
        "purged_evaluations": purged_evaluations,
    })))
}

async fn handle_delete_trace_or_erase(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, trace_id)): AxumPath<(String, String)>,
    Query(q): Query<ErasureQuery>,
    body: Bytes,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let req: ErasureReq = if body.is_empty() {
        ErasureReq::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| CtrlError::bad(format!("invalid JSON body: {e}")))?
    };
    let raw_level = q.level.or(q.scope).or(req.level).or(req.scope);
    let level = match raw_level {
        Some(ref s) if !s.trim().is_empty() => parse_erasure_level(s)?,
        _ => ErasureLevel::Trace,
    };
    let target_id = q
        .target_id
        .or(req.target_id)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(trace_id);

    execute_erasure(&ctrl, &principal, &id, level, target_id, req.expires_at).await
}

async fn handle_post_erasure(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<ErasureReq>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let raw_level = req
        .level
        .or(req.scope)
        .ok_or_else(|| CtrlError::bad("erasure level is required (trace, session, or user)"))?;
    let level = parse_erasure_level(&raw_level)?;
    let target_id = req
        .target_id
        .ok_or_else(|| CtrlError::bad("erasure target_id is required"))?;

    execute_erasure(&ctrl, &principal, &id, level, target_id, req.expires_at).await
}

async fn collect_filtered_traces_with_rollups(
    ctrl: &ControlPlaneService,
    project_id: &str,
    filter: &AnalyticsFilter,
    rollups: &HashMap<(EvalTargetType, String), AgentEvalRollup>,
    tombstones: &[AgentErasureTombstone],
) -> Result<Vec<AgentTrace>, CtrlError> {
    let mut tf = filter.to_trace_list_filter();
    tf.limit = Some(100);
    tf.cursor = None;
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    loop {
        let page = ctrl
            .agent_store()
            .list_traces(project_id, &tf, rollups, tombstones)
            .await
            .map_err(map_store_err)?;
        for tr in page.traces {
            if seen.insert(tr.trace_id.clone()) {
                out.push(tr);
            }
        }
        match page.next_cursor {
            Some(c) if !c.is_empty() => tf.cursor = Some(c),
            _ => break,
        }
    }
    Ok(out)
}

async fn handle_agent_analytics(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(mut filter): Query<AnalyticsFilter>,
) -> ApiRes<AgentAnalyticsOverview> {
    ctrl.authorize_and_check(&principal, &id).await?;
    normalize_analytics_filter(&mut filter);
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let rollups = ctrl
        .meta()
        .list_agent_eval_rollups(&id, Some(EvalTargetType::Trace))
        .await
        .map_err(CtrlError::meta)?;
    let mut overview = ctrl
        .agent_store()
        .query_analytics_with_rollups(&id, &filter, &tombstones, &rollups)
        .await
        .map_err(map_store_err)?;

    if !rollups.is_empty() && filter.tool_name.is_none() {
        let traces =
            collect_filtered_traces_with_rollups(&ctrl, &id, &filter, &rollups, &tombstones)
                .await?;
        enrich_analytics_overview_from_traces(&filter, &traces, &mut overview);
    }

    Ok(Json(overview))
}

fn enrich_analytics_overview_from_traces(
    filter: &AnalyticsFilter,
    traces: &[AgentTrace],
    overview: &mut AgentAnalyticsOverview,
) {
    let eval_scores: Vec<f64> = traces.iter().filter_map(|t| t.avg_eval_score).collect();
    if !eval_scores.is_empty() {
        overview.avg_eval_score = Some(eval_scores.iter().sum::<f64>() / eval_scores.len() as f64);
    }
    let eval_passes: Vec<f64> = traces.iter().filter_map(|t| t.eval_pass_rate).collect();
    if !eval_passes.is_empty() {
        overview.eval_pass_rate = Some(eval_passes.iter().sum::<f64>() / eval_passes.len() as f64);
    }

    let mut agent_scores: HashMap<&str, Vec<f64>> = HashMap::new();
    let mut env_scores: HashMap<&str, Vec<f64>> = HashMap::new();
    let mut ver_scores: HashMap<(String, Option<String>), Vec<f64>> = HashMap::new();
    for tr in traces {
        if let Some(s) = tr.avg_eval_score {
            agent_scores
                .entry(tr.agent_name.as_str())
                .or_default()
                .push(s);
            env_scores
                .entry(tr.environment.as_str())
                .or_default()
                .push(s);
            ver_scores
                .entry((
                    format!("{}@{}", tr.agent_name, tr.agent_version),
                    tr.prompt_version.clone(),
                ))
                .or_default()
                .push(s);
        }
    }
    for row in &mut overview.by_agent {
        if let Some(xs) = agent_scores.get(row.key.as_str()) {
            row.avg_eval_score = Some(xs.iter().sum::<f64>() / xs.len() as f64);
        }
    }
    for row in &mut overview.by_environment {
        if let Some(xs) = env_scores.get(row.key.as_str()) {
            row.avg_eval_score = Some(xs.iter().sum::<f64>() / xs.len() as f64);
        }
    }
    for row in &mut overview.by_version {
        if let Some(xs) = ver_scores.get(&(row.key.clone(), row.secondary.clone())) {
            row.avg_eval_score = Some(xs.iter().sum::<f64>() / xs.len() as f64);
        }
    }

    if !overview.timeseries.is_empty() {
        let min_ts = traces.iter().map(|t| t.start_ts_ms).min().unwrap_or(0);
        let max_ts = traces.iter().map(|t| t.start_ts_ms).max().unwrap_or(min_ts);
        let win_start = filter.start_ts_ms.unwrap_or(min_ts);
        let win_end = filter.end_ts_ms.unwrap_or(max_ts);
        let span_ms = win_end.saturating_sub(win_start).max(1);
        let bucket_ms: i64 = if span_ms <= 48 * 3600 * 1000 {
            3600 * 1000
        } else {
            24 * 3600 * 1000
        };
        let mut bucket_scores: HashMap<i64, Vec<f64>> = HashMap::new();
        for tr in traces {
            if let Some(s) = tr.avg_eval_score {
                let b = tr
                    .start_ts_ms
                    .div_euclid(bucket_ms)
                    .saturating_mul(bucket_ms);
                bucket_scores.entry(b).or_default().push(s);
            }
        }
        for b in &mut overview.timeseries {
            if let Some(xs) = bucket_scores.get(&b.bucket_ts_ms) {
                b.avg_eval_score = Some(xs.iter().sum::<f64>() / xs.len() as f64);
            }
        }
    }
}

async fn handle_agent_compare(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(mut filter): Query<CompareFilter>,
) -> ApiRes<CompareResponse> {
    ctrl.authorize_and_check(&principal, &id).await?;
    normalize_compare_filter(&mut filter);
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let mut resp = ctrl
        .agent_store()
        .compare_groups(&id, &filter, &tombstones)
        .await
        .map_err(map_store_err)?;

    let rollups = ctrl
        .meta()
        .list_agent_eval_rollups(&id, Some(EvalTargetType::Trace))
        .await
        .map_err(CtrlError::meta)?;
    if !rollups.is_empty()
        && resp.eval_score.sample_size_baseline == 0
        && resp.eval_score.sample_size_candidate == 0
    {
        let (base_filter, cand_filter) = filter.resolve_groups();
        if base_filter.tool_name.is_none() && cand_filter.tool_name.is_none() {
            let base_traces = collect_filtered_traces_with_rollups(
                &ctrl,
                &id,
                &base_filter,
                &rollups,
                &tombstones,
            )
            .await?;
            let cand_traces = collect_filtered_traces_with_rollups(
                &ctrl,
                &id,
                &cand_filter,
                &rollups,
                &tombstones,
            )
            .await?;

            let base_scores: Vec<f64> = base_traces
                .iter()
                .filter_map(|t| t.avg_eval_score)
                .collect();
            let cand_scores: Vec<f64> = cand_traces
                .iter()
                .filter_map(|t| t.avg_eval_score)
                .collect();

            resp.baseline.avg_eval_score = if base_scores.is_empty() {
                None
            } else {
                Some(base_scores.iter().sum::<f64>() / base_scores.len() as f64)
            };
            resp.candidate.avg_eval_score = if cand_scores.is_empty() {
                None
            } else {
                Some(cand_scores.iter().sum::<f64>() / cand_scores.len() as f64)
            };
            resp.eval_score = compare_continuous_metric(&base_scores, &cand_scores, true);

            let deltas = [
                &resp.latency_ms,
                &resp.cost_usd,
                &resp.total_tokens,
                &resp.error_rate,
                &resp.success_rate,
                &resp.eval_score,
            ];
            resp.overall_verdict = if !resp.sufficient_samples {
                "insufficient_data".to_string()
            } else if deltas.iter().any(|d| d.verdict == "significant_regression") {
                "significant_regression".to_string()
            } else if deltas
                .iter()
                .any(|d| d.verdict == "significant_improvement")
            {
                "significant_improvement".to_string()
            } else {
                "no_significant_change".to_string()
            };
        }
    }

    Ok(Json(resp))
}

// ===========================================================================
// Evaluators & Evaluations
// ===========================================================================

async fn handle_list_evaluators(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let evaluators = ctrl
        .meta()
        .list_evaluator_configs(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "evaluators": evaluators })))
}

#[derive(Deserialize)]
struct CreateEvaluatorReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    target_type: Option<String>,
    #[serde(default)]
    evaluator_type: Option<String>,
    kind: String,
    #[serde(default = "default_eval_ver")]
    version: String,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    agent_name: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default = "default_sample_one")]
    sample_rate: f64,
    #[serde(default)]
    config: Value,
}

fn default_eval_ver() -> String {
    "1.0.0".to_string()
}

fn default_true() -> bool {
    true
}

fn default_sample_one() -> f64 {
    1.0
}

fn parse_eval_target_type_strict(raw: &str) -> Result<EvalTargetType, CtrlError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "session" | "conversation" | "thread" => Ok(EvalTargetType::Session),
        "trace" => Ok(EvalTargetType::Trace),
        "span" => Ok(EvalTargetType::Span),
        other => Err(CtrlError::bad(format!(
            "invalid target_type '{other}': expected session, trace, or span"
        ))),
    }
}

fn validate_regex_evaluator_config(cfg: &EvaluatorConfig) -> Result<(), CtrlError> {
    let pattern = cfg
        .config
        .get("pattern")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CtrlError::bad("regex_match evaluator requires config.pattern"))?;
    let dummy_trace = AgentTrace {
        trace_id: "tr_validate".to_string(),
        project_id: cfg.project_id.clone(),
        agent_name: "validate".to_string(),
        agent_version: "1.0.0".to_string(),
        prompt_version: None,
        model_config_version: None,
        tool_config_version: None,
        app_release: None,
        environment: "production".to_string(),
        session_id: None,
        user_id: None,
        workflow_id: None,
        parent_trace_id: None,
        status: TraceStatus::Ok,
        start_ts_ms: 0,
        end_ts_ms: Some(0),
        duration_ms: 0,
        input: Value::Null,
        output: Value::String(String::new()),
        error_type: None,
        error_message: None,
        failure_source: None,
        metadata: Value::Null,
        tags: Vec::new(),
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
    let ev = run_deterministic_evaluator(
        cfg,
        &EvalContext {
            trace: &dummy_trace,
            spans: &[],
            expected_output: None,
            expected_behavior: None,
        },
    );
    if let Some(err) = ev.and_then(|e| e.error) {
        return Err(CtrlError::bad(format!(
            "invalid regex_match pattern '{pattern}': {err}"
        )));
    }
    Ok(())
}

async fn handle_upsert_evaluator(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateEvaluatorReq>,
) -> ApiRes<EvaluatorConfig> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let name = req.name.trim().to_string();
    let kind = req.kind.trim().to_string();
    if name.is_empty() || kind.is_empty() {
        return Err(CtrlError::bad("evaluator name and kind are required"));
    }
    let target_type = match req
        .target_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => parse_eval_target_type_strict(raw)?,
        None => EvalTargetType::Trace,
    };
    let evaluator_type = req
        .evaluator_type
        .as_deref()
        .map(EvaluatorType::parse)
        .unwrap_or_else(|| {
            if kind.eq_ignore_ascii_case("llm_judge") {
                EvaluatorType::LlmJudge
            } else {
                EvaluatorType::Deterministic
            }
        });
    let sample_rate = if req.sample_rate.is_finite() {
        req.sample_rate.clamp(0.0, 1.0)
    } else {
        1.0
    };

    let now_ms = Utc::now().timestamp_millis();
    let eval_id = req
        .id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| new_id("evc"));
    let existing = ctrl
        .meta()
        .get_evaluator_config(&id, &eval_id)
        .await
        .map_err(CtrlError::meta)?;

    let cfg = EvaluatorConfig {
        id: eval_id,
        project_id: id,
        name,
        description: req.description,
        target_type,
        evaluator_type,
        kind: kind.clone(),
        version: if req.version.trim().is_empty() {
            "1.0.0".to_string()
        } else {
            req.version.trim().to_string()
        },
        enabled: req.enabled,
        agent_name: req
            .agent_name
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        environment: req
            .environment
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        sample_rate,
        config: req.config,
        created_at_ms: existing.map(|e| e.created_at_ms).unwrap_or(now_ms),
        updated_at_ms: now_ms,
    };
    if kind.eq_ignore_ascii_case("regex_match") {
        validate_regex_evaluator_config(&cfg)?;
    }

    ctrl.meta()
        .upsert_evaluator_config(&cfg)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(cfg))
}

async fn handle_delete_evaluator(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, eval_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let deleted = ctrl
        .meta()
        .delete_evaluator_config(&id, &eval_id)
        .await
        .map_err(CtrlError::meta)?;
    if !deleted {
        return Err(CtrlError::not_found("evaluator config not found"));
    }
    Ok(Json(json!({ "status": "ok", "id": eval_id })))
}

#[derive(Deserialize, Default)]
struct TriggerEvalReq {
    #[serde(default)]
    evaluator_ids: Option<Vec<String>>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    span_id: Option<String>,
}

fn normalize_eval_ids(ids: Option<Vec<String>>) -> Option<Vec<String>> {
    ids.map(|v| {
        v.into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
    })
    .filter(|v| !v.is_empty())
}

async fn handle_trigger_trace_eval(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, trace_id)): AxumPath<(String, String)>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), CtrlError> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let req: TriggerEvalReq = if body.is_empty() {
        TriggerEvalReq::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| CtrlError::bad(format!("invalid JSON body: {e}")))?
    };

    let tombstones = active_tombstones(&ctrl, &id).await?;
    let Some(detail) = ctrl
        .agent_store()
        .get_trace_detail(&id, &trace_id, &tombstones)
        .await
        .map_err(map_store_err)?
    else {
        return Err(CtrlError::not_found("agent trace not found"));
    };

    let session_id = req
        .session_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or(detail.trace.session_id);
    let span_id = req
        .span_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let evaluator_ids = normalize_eval_ids(req.evaluator_ids);

    if !ctrl.try_enqueue_eval(EvalJob {
        project_id: id,
        session_id,
        trace_id: Some(trace_id.clone()),
        span_id,
        evaluator_ids,
    }) {
        return Err(CtrlError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "evaluation queue is full",
        ));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "queued", "trace_id": trace_id })),
    ))
}

async fn handle_trigger_eval(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<TriggerEvalReq>,
) -> Result<(StatusCode, Json<Value>), CtrlError> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let trace_id = req
        .trace_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let mut session_id = req
        .session_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let span_id = req
        .span_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if trace_id.is_none() && session_id.is_none() {
        return Err(CtrlError::bad(
            "either trace_id or session_id is required to trigger evaluation",
        ));
    }

    let tombstones = active_tombstones(&ctrl, &id).await?;
    if let Some(ref tid) = trace_id {
        let Some(detail) = ctrl
            .agent_store()
            .get_trace_detail(&id, tid, &tombstones)
            .await
            .map_err(map_store_err)?
        else {
            return Err(CtrlError::not_found("agent trace not found"));
        };
        if session_id.is_none() {
            session_id = detail.trace.session_id;
        }
    } else if let Some(ref sid) = session_id {
        let exists = ctrl
            .agent_store()
            .get_session_detail(&id, sid, &tombstones)
            .await
            .map_err(map_store_err)?
            .is_some();
        if !exists {
            return Err(CtrlError::not_found("agent session not found"));
        }
    }

    let evaluator_ids = normalize_eval_ids(req.evaluator_ids);
    if !ctrl.try_enqueue_eval(EvalJob {
        project_id: id,
        session_id: session_id.clone(),
        trace_id: trace_id.clone(),
        span_id,
        evaluator_ids,
    }) {
        return Err(CtrlError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "evaluation queue is full",
        ));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "queued",
            "trace_id": trace_id,
            "session_id": session_id,
        })),
    ))
}

#[derive(Deserialize, Default)]
struct ListEvaluationsQuery {
    trace_id: Option<String>,
    session_id: Option<String>,
    span_id: Option<String>,
    target_type: Option<String>,
    limit: Option<usize>,
}

async fn handle_list_evaluations(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<ListEvaluationsQuery>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let mut evals = if let Some(tid) = q
        .trace_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ctrl.meta()
            .list_trace_evaluations(&id, tid)
            .await
            .map_err(CtrlError::meta)?
    } else if let Some(sid) = q
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ctrl.meta()
            .list_session_evaluations(&id, sid)
            .await
            .map_err(CtrlError::meta)?
    } else if let Some(spid) = q
        .span_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        ctrl.meta()
            .list_span_evaluations(&id, spid)
            .await
            .map_err(CtrlError::meta)?
    } else {
        let tt = q
            .target_type
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(parse_eval_target_type_strict)
            .transpose()?;
        ctrl.meta()
            .list_project_evaluations_by_target(&id, tt, limit)
            .await
            .map_err(CtrlError::meta)?
    };
    if let Some(tt_raw) = q
        .target_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let tt = parse_eval_target_type_strict(tt_raw)?;
        evals.retain(|e| e.target_type == tt);
    }
    evals.truncate(limit);
    Ok(Json(json!({ "evaluations": evals })))
}

#[derive(Deserialize)]
struct CreateEvaluationReq {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    target_type: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default)]
    span_id: Option<String>,
    #[serde(default)]
    experiment_run_id: Option<String>,
    evaluator_id: String,
    evaluator_name: String,
    #[serde(default)]
    evaluator_type: Option<String>,
    #[serde(default = "default_eval_ver")]
    evaluator_version: String,
    score: f64,
    #[serde(default)]
    score_type: Option<String>,
    #[serde(default)]
    passed: Option<bool>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    execution_ms: i64,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    metadata: Value,
}

async fn handle_create_evaluation(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateEvaluationReq>,
) -> ApiRes<AgentEvaluation> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if !req.score.is_finite() {
        return Err(CtrlError::bad("evaluation score must be finite"));
    }
    let evaluator_id = req.evaluator_id.trim().to_string();
    let evaluator_name = req.evaluator_name.trim().to_string();
    if evaluator_id.is_empty() || evaluator_name.is_empty() {
        return Err(CtrlError::bad(
            "evaluator_id and evaluator_name are required",
        ));
    }

    let target_type = match req
        .target_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => parse_eval_target_type_strict(raw)?,
        None => EvalTargetType::Trace,
    };

    let session_id = req
        .session_id
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let mut trace_id = req
        .trace_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let mut span_id = req
        .span_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    match target_type {
        EvalTargetType::Session => {
            if session_id.is_empty() {
                return Err(CtrlError::bad(
                    "session_id is required when target_type is 'session'",
                ));
            }
            trace_id = None;
            span_id = None;
        }
        EvalTargetType::Trace => {
            if trace_id.is_none() {
                return Err(CtrlError::bad(
                    "trace_id is required when target_type is 'trace'",
                ));
            }
            span_id = None;
        }
        EvalTargetType::Span => {
            if span_id.is_none() {
                return Err(CtrlError::bad(
                    "span_id is required when target_type is 'span'",
                ));
            }
        }
    }

    // Normalize empty / whitespace-only error strings to None so `AND error IS NULL` rollups
    // remain accurate.
    let error = req
        .error
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let ev = AgentEvaluation {
        id: req
            .id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| new_id("eval")),
        project_id: id,
        target_type,
        session_id,
        trace_id,
        span_id,
        experiment_run_id: req
            .experiment_run_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        evaluator_id,
        evaluator_name,
        evaluator_type: req
            .evaluator_type
            .as_deref()
            .map(EvaluatorType::parse)
            .unwrap_or(EvaluatorType::External),
        evaluator_version: if req.evaluator_version.trim().is_empty() {
            "1.0.0".to_string()
        } else {
            req.evaluator_version.trim().to_string()
        },
        score: req.score,
        score_type: req
            .score_type
            .as_deref()
            .map(ScoreType::parse)
            .unwrap_or(ScoreType::Numeric),
        passed: req.passed,
        label: req
            .label
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        reasoning: req
            .reasoning
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        execution_ms: req.execution_ms.max(0),
        error,
        metadata: req.metadata,
        created_at_ms: Utc::now().timestamp_millis(),
    };

    ctrl.meta()
        .insert_agent_evaluation(&ev)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(ev))
}

// ===========================================================================
// Datasets, Items, Bulk Import & Promote
// ===========================================================================

async fn handle_list_datasets(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let datasets = ctrl
        .meta()
        .list_agent_datasets(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "datasets": datasets })))
}

#[derive(Deserialize)]
struct CreateDatasetReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default = "default_ds_version")]
    version: i32,
    #[serde(default)]
    tags: Vec<String>,
}

fn default_ds_version() -> i32 {
    1
}

async fn handle_upsert_dataset(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateDatasetReq>,
) -> ApiRes<AgentDataset> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return Err(CtrlError::bad("dataset name is required"));
    }
    let now_ms = Utc::now().timestamp_millis();
    let ds_id = req
        .id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| new_id("ds"));
    let existing = ctrl
        .meta()
        .get_agent_dataset(&id, &ds_id)
        .await
        .map_err(CtrlError::meta)?;
    let ds = AgentDataset {
        id: ds_id,
        project_id: id,
        name,
        description: req.description,
        version: req.version.max(1),
        tags: req.tags,
        item_count: existing.as_ref().map(|d| d.item_count).unwrap_or(0),
        created_at_ms: existing.map(|d| d.created_at_ms).unwrap_or(now_ms),
        updated_at_ms: now_ms,
    };
    ctrl.meta()
        .upsert_agent_dataset(&ds)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(ds))
}

async fn handle_delete_dataset(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dataset_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let deleted = ctrl
        .meta()
        .delete_agent_dataset(&id, &dataset_id)
        .await
        .map_err(CtrlError::meta)?;
    if !deleted {
        return Err(CtrlError::not_found("dataset not found"));
    }
    Ok(Json(json!({ "status": "ok", "id": dataset_id })))
}

async fn ensure_dataset_owned(
    ctrl: &ControlPlaneService,
    project_id: &str,
    dataset_id: &str,
) -> Result<AgentDataset, CtrlError> {
    ctrl.meta()
        .get_agent_dataset(project_id, dataset_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("dataset not found"))
}

async fn handle_list_dataset_items(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dataset_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    ensure_dataset_owned(&ctrl, &id, &dataset_id).await?;
    let items = ctrl
        .meta()
        .list_dataset_items(&id, &dataset_id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "items": items })))
}

#[derive(Deserialize)]
struct CreateDatasetItemReq {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    dataset_version: Option<i32>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    expected_output: Value,
    #[serde(default)]
    expected_behavior: Value,
    #[serde(default)]
    source_trace_id: Option<String>,
    #[serde(default)]
    metadata: Value,
    #[serde(default)]
    tags: Vec<String>,
}

fn build_dataset_item(
    project_id: &str,
    dataset: &AgentDataset,
    req: CreateDatasetItemReq,
    now_ms: i64,
) -> AgentDatasetItem {
    AgentDatasetItem {
        id: req
            .id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| new_id("dsi")),
        project_id: project_id.to_string(),
        dataset_id: dataset.id.clone(),
        dataset_version: req.dataset_version.unwrap_or(dataset.version).max(1),
        name: req
            .name
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        input: req.input,
        expected_output: req.expected_output,
        expected_behavior: req.expected_behavior,
        source_trace_id: req
            .source_trace_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        metadata: req.metadata,
        tags: req.tags,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    }
}

async fn handle_upsert_dataset_item(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dataset_id)): AxumPath<(String, String)>,
    Json(req): Json<CreateDatasetItemReq>,
) -> ApiRes<AgentDatasetItem> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let ds = ensure_dataset_owned(&ctrl, &id, &dataset_id).await?;
    let now_ms = Utc::now().timestamp_millis();
    let item = build_dataset_item(&id, &ds, req, now_ms);
    ctrl.meta()
        .upsert_dataset_item(&item)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(item))
}

#[derive(Deserialize)]
struct ImportDatasetItemsReq {
    #[serde(default)]
    items: Vec<CreateDatasetItemReq>,
}

async fn handle_import_dataset_items(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dataset_id)): AxumPath<(String, String)>,
    Json(req): Json<ImportDatasetItemsReq>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let ds = ensure_dataset_owned(&ctrl, &id, &dataset_id).await?;
    let now_ms = Utc::now().timestamp_millis();
    let mut imported = Vec::with_capacity(req.items.len());
    for item_req in req.items {
        let item = build_dataset_item(&id, &ds, item_req, now_ms);
        ctrl.meta()
            .upsert_dataset_item(&item)
            .await
            .map_err(CtrlError::meta)?;
        imported.push(item);
    }
    Ok(Json(json!({
        "imported": imported.len(),
        "items": imported,
    })))
}

async fn handle_delete_dataset_item(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dataset_id, item_id)): AxumPath<(String, String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    ensure_dataset_owned(&ctrl, &id, &dataset_id).await?;
    let deleted = ctrl
        .meta()
        .delete_dataset_item(&id, &dataset_id, &item_id)
        .await
        .map_err(CtrlError::meta)?;
    if !deleted {
        return Err(CtrlError::not_found("dataset item not found"));
    }
    Ok(Json(json!({ "status": "ok", "id": item_id })))
}

#[derive(Deserialize)]
struct PromoteTraceReq {
    dataset_id: String,
    #[serde(default)]
    dataset_version: Option<i32>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    expected_output: Option<Value>,
    #[serde(default)]
    expected_behavior: Option<Value>,
    #[serde(default)]
    tags: Vec<String>,
}

async fn handle_promote_trace(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, trace_id)): AxumPath<(String, String)>,
    Json(req): Json<PromoteTraceReq>,
) -> ApiRes<AgentDatasetItem> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let ds = ensure_dataset_owned(&ctrl, &id, req.dataset_id.trim()).await?;
    let tombstones = active_tombstones(&ctrl, &id).await?;
    let Some(detail) = ctrl
        .agent_store()
        .get_trace_detail(&id, &trace_id, &tombstones)
        .await
        .map_err(map_store_err)?
    else {
        return Err(CtrlError::not_found("agent trace not found"));
    };

    let expected_output = req
        .expected_output
        .unwrap_or_else(|| detail.trace.output.clone());
    let expected_behavior = req.expected_behavior.unwrap_or_else(|| {
        let mut tool_spans: Vec<&AgentSpan> = detail
            .spans
            .iter()
            .filter(|s| s.span_type == SpanType::Tool)
            .collect();
        tool_spans.sort_by_key(|s| s.start_ts_ms);
        let expected_tools: Vec<String> = tool_spans
            .into_iter()
            .filter_map(|s| {
                s.tool_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .or_else(|| {
                        let n = s.name.trim();
                        if n.is_empty() {
                            None
                        } else {
                            Some(n)
                        }
                    })
                    .map(str::to_string)
            })
            .collect();
        json!({ "expected_tools": expected_tools })
    });

    let now_ms = Utc::now().timestamp_millis();
    let item = AgentDatasetItem {
        id: new_id("dsi"),
        project_id: id,
        dataset_id: ds.id,
        dataset_version: req.dataset_version.unwrap_or(ds.version).max(1),
        name: req
            .name
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| Some(format!("trace:{trace_id}"))),
        input: detail.trace.input.clone(),
        expected_output,
        expected_behavior,
        source_trace_id: Some(trace_id),
        metadata: detail.trace.metadata.clone(),
        tags: if req.tags.is_empty() {
            detail.trace.tags.clone()
        } else {
            req.tags
        },
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    };

    ctrl.meta()
        .upsert_dataset_item(&item)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(item))
}

// ===========================================================================
// Experiments, Runs & Regression Compare
// ===========================================================================

async fn handle_list_experiments(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let experiments = ctrl
        .meta()
        .list_agent_experiments(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "experiments": experiments })))
}

#[derive(Deserialize)]
struct CreateExperimentReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    description: String,
    dataset_id: String,
    #[serde(default)]
    dataset_version: Option<i32>,
    agent_name: String,
    #[serde(default = "default_exp_version")]
    agent_version: String,
    #[serde(default)]
    prompt_version: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    evaluator_ids: Vec<String>,
    #[serde(default)]
    metadata: Value,
}

fn default_exp_version() -> String {
    "unversioned".to_string()
}

async fn handle_create_experiment(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateExperimentReq>,
) -> ApiRes<AgentExperiment> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let name = req.name.trim().to_string();
    let agent_name = req.agent_name.trim().to_string();
    if name.is_empty() || agent_name.is_empty() {
        return Err(CtrlError::bad(
            "experiment name and agent_name are required",
        ));
    }
    let ds = ensure_dataset_owned(&ctrl, &id, req.dataset_id.trim()).await?;
    let items = ctrl
        .meta()
        .list_dataset_items(&id, &ds.id)
        .await
        .map_err(CtrlError::meta)?;
    let now_ms = Utc::now().timestamp_millis();
    let exp = AgentExperiment {
        id: req
            .id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| new_id("exp")),
        project_id: id,
        name,
        description: req.description,
        dataset_id: ds.id,
        dataset_version: req.dataset_version.unwrap_or(ds.version).max(1),
        agent_name,
        agent_version: if req.agent_version.trim().is_empty() {
            "unversioned".to_string()
        } else {
            req.agent_version.trim().to_string()
        },
        prompt_version: req
            .prompt_version
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        model: req
            .model
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        evaluator_ids: req.evaluator_ids,
        status: "pending".to_string(),
        total_items: items.len() as i32,
        completed_items: 0,
        passed_items: 0,
        avg_score: None,
        avg_latency_ms: None,
        total_cost_usd: 0.0,
        metadata: req.metadata,
        created_at_ms: now_ms,
        completed_at_ms: None,
    };

    ctrl.meta()
        .upsert_agent_experiment(&exp)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(exp))
}

async fn handle_get_experiment(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, exp_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let exp = ctrl
        .meta()
        .get_agent_experiment(&id, &exp_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("experiment not found"))?;
    let runs = ctrl
        .meta()
        .list_experiment_runs(&id, &exp_id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({
        "experiment": exp,
        "runs": runs,
    })))
}

#[derive(Deserialize)]
struct CreateExperimentRunReq {
    dataset_item_id: String,
    #[serde(default)]
    trace_id: Option<String>,
    #[serde(default = "default_run_status")]
    status: String,
    #[serde(default)]
    actual_output: Value,
    #[serde(default)]
    latency_ms: i64,
    #[serde(default)]
    cost_usd: f64,
    #[serde(default)]
    total_tokens: i64,
    #[serde(default)]
    avg_score: Option<f64>,
    #[serde(default)]
    passed: Option<bool>,
    #[serde(default)]
    error: Option<String>,
}

fn default_run_status() -> String {
    "ok".to_string()
}

async fn handle_create_experiment_run(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, exp_id)): AxumPath<(String, String)>,
    Json(req): Json<CreateExperimentRunReq>,
) -> ApiRes<AgentExperimentRun> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let mut exp = ctrl
        .meta()
        .get_agent_experiment(&id, &exp_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("experiment not found"))?;

    let item_id = req.dataset_item_id.trim().to_string();
    if item_id.is_empty() {
        return Err(CtrlError::bad("dataset_item_id is required"));
    }
    if req.avg_score.is_some_and(|s| !s.is_finite()) || !req.cost_usd.is_finite() {
        return Err(CtrlError::bad("avg_score and cost_usd must be finite"));
    }

    let now_ms = Utc::now().timestamp_millis();
    let run = AgentExperimentRun {
        id: new_id("run"),
        project_id: id.clone(),
        experiment_id: exp_id.clone(),
        dataset_item_id: item_id,
        trace_id: req
            .trace_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        status: if req.status.trim().is_empty() {
            "ok".to_string()
        } else {
            req.status.trim().to_string()
        },
        actual_output: req.actual_output,
        latency_ms: req.latency_ms.max(0),
        cost_usd: req.cost_usd.max(0.0),
        total_tokens: req.total_tokens.max(0),
        avg_score: req.avg_score,
        passed: req.passed,
        error: req
            .error
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        evaluations: Vec::new(),
        created_at_ms: now_ms,
    };

    ctrl.meta()
        .insert_experiment_run(&run)
        .await
        .map_err(CtrlError::meta)?;

    // Recompute parent AgentExperiment aggregates across all recorded runs.
    let runs = ctrl
        .meta()
        .list_experiment_runs(&id, &exp_id)
        .await
        .map_err(CtrlError::meta)?;
    let completed = runs.len() as i32;
    let passed = runs.iter().filter(|r| r.passed == Some(true)).count() as i32;
    let scores: Vec<f64> = runs.iter().filter_map(|r| r.avg_score).collect();
    let avg_score = if scores.is_empty() {
        None
    } else {
        Some(scores.iter().sum::<f64>() / scores.len() as f64)
    };
    let avg_latency_ms = if runs.is_empty() {
        None
    } else {
        Some(runs.iter().map(|r| r.latency_ms as f64).sum::<f64>() / runs.len() as f64)
    };
    let total_cost_usd: f64 = runs.iter().map(|r| r.cost_usd.max(0.0)).sum();

    exp.completed_items = completed;
    exp.passed_items = passed;
    exp.avg_score = avg_score;
    exp.avg_latency_ms = avg_latency_ms;
    exp.total_cost_usd = total_cost_usd;
    if exp.total_items > 0 && completed >= exp.total_items {
        exp.status = "completed".to_string();
        if exp.completed_at_ms.is_none() {
            exp.completed_at_ms = Some(now_ms);
        }
    } else {
        exp.status = "running".to_string();
    }

    ctrl.meta()
        .upsert_agent_experiment(&exp)
        .await
        .map_err(CtrlError::meta)?;

    Ok(Json(run))
}

#[derive(Deserialize, Default)]
struct CompareExperimentsQuery {
    baseline: Option<String>,
    candidate: Option<String>,
}

async fn handle_compare_experiments(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<CompareExperimentsQuery>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let base_id = q
        .baseline
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CtrlError::bad("baseline query parameter is required"))?
        .to_string();
    let cand_id = q
        .candidate
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CtrlError::bad("candidate query parameter is required"))?
        .to_string();

    let base_exp = ctrl
        .meta()
        .get_agent_experiment(&id, &base_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("baseline experiment not found"))?;
    let cand_exp = ctrl
        .meta()
        .get_agent_experiment(&id, &cand_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("candidate experiment not found"))?;

    let base_runs = ctrl
        .meta()
        .list_experiment_runs(&id, &base_id)
        .await
        .map_err(CtrlError::meta)?;
    let cand_runs = ctrl
        .meta()
        .list_experiment_runs(&id, &cand_id)
        .await
        .map_err(CtrlError::meta)?;

    let base_scores: Vec<f64> = base_runs.iter().filter_map(|r| r.avg_score).collect();
    let cand_scores: Vec<f64> = cand_runs.iter().filter_map(|r| r.avg_score).collect();
    let avg_score = compare_continuous_metric(&base_scores, &cand_scores, true);

    let base_lat: Vec<f64> = base_runs
        .iter()
        .map(|r| r.latency_ms.max(0) as f64)
        .collect();
    let cand_lat: Vec<f64> = cand_runs
        .iter()
        .map(|r| r.latency_ms.max(0) as f64)
        .collect();
    let latency_ms = compare_continuous_metric(&base_lat, &cand_lat, false);

    let base_cost: Vec<f64> = base_runs.iter().map(|r| r.cost_usd.max(0.0)).collect();
    let cand_cost: Vec<f64> = cand_runs.iter().map(|r| r.cost_usd.max(0.0)).collect();
    let cost_usd = compare_continuous_metric(&base_cost, &cand_cost, false);

    let base_tok: Vec<f64> = base_runs
        .iter()
        .map(|r| r.total_tokens.max(0) as f64)
        .collect();
    let cand_tok: Vec<f64> = cand_runs
        .iter()
        .map(|r| r.total_tokens.max(0) as f64)
        .collect();
    let total_tokens = compare_continuous_metric(&base_tok, &cand_tok, false);

    let base_pass: Vec<f64> = base_runs
        .iter()
        .filter_map(|r| r.passed.map(|p| if p { 1.0 } else { 0.0 }))
        .collect();
    let cand_pass: Vec<f64> = cand_runs
        .iter()
        .filter_map(|r| r.passed.map(|p| if p { 1.0 } else { 0.0 }))
        .collect();
    let pass_rate = compare_continuous_metric(&base_pass, &cand_pass, true);

    let n_base = base_runs.len();
    let n_cand = cand_runs.len();
    let sufficient_samples =
        n_base >= MIN_SIGNIFICANCE_SAMPLES && n_cand >= MIN_SIGNIFICANCE_SAMPLES;
    let deltas = [
        &avg_score,
        &latency_ms,
        &cost_usd,
        &total_tokens,
        &pass_rate,
    ];
    let overall_verdict = if !sufficient_samples {
        "insufficient_data"
    } else if deltas.iter().any(|d| d.verdict == "significant_regression") {
        "significant_regression"
    } else if deltas
        .iter()
        .any(|d| d.verdict == "significant_improvement")
    {
        "significant_improvement"
    } else {
        "no_significant_change"
    };

    Ok(Json(json!({
        "baseline": base_exp,
        "candidate": cand_exp,
        "baseline_sample_size": n_base,
        "candidate_sample_size": n_cand,
        "min_significance_samples": MIN_SIGNIFICANCE_SAMPLES,
        "sufficient_samples": sufficient_samples,
        "overall_verdict": overall_verdict,
        "avg_score": avg_score,
        "latency_ms": latency_ms,
        "cost_usd": cost_usd,
        "total_tokens": total_tokens,
        "pass_rate": pass_rate,
    })))
}

// ===========================================================================
// Alerts, Webhooks, Privacy, Pricing & Retention
// ===========================================================================

async fn handle_list_agent_alerts(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let rules = ctrl
        .meta()
        .list_agent_alert_rules(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "rules": rules })))
}

#[derive(Deserialize)]
struct CreateAgentAlertReq {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    agent_name: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    metric: String,
    #[serde(default = "default_gt")]
    operator: String,
    threshold: f64,
    #[serde(default = "default_window_min")]
    window_minutes: i32,
    #[serde(default = "default_cooldown_min")]
    cooldown_minutes: i32,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    webhook_url: Option<String>,
}

fn default_gt() -> String {
    "gt".to_string()
}

fn default_window_min() -> i32 {
    15
}

fn default_cooldown_min() -> i32 {
    15
}

async fn handle_upsert_agent_alert(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateAgentAlertReq>,
) -> ApiRes<AgentAlertRule> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let name = req.name.trim().to_string();
    let metric = req.metric.trim().to_string();
    let operator = req.operator.trim().to_string();
    if name.is_empty() {
        return Err(CtrlError::bad("alert rule name is required"));
    }
    if !ALLOWED_ALERT_METRICS.contains(&metric.as_str()) {
        return Err(CtrlError::bad(format!(
            "invalid metric '{metric}': expected one of {}",
            ALLOWED_ALERT_METRICS.join(", ")
        )));
    }
    if !ALERT_OPERATORS.contains(&operator.as_str()) {
        return Err(CtrlError::bad("operator must be one of: gt, gte, lt, lte"));
    }
    if !req.threshold.is_finite() {
        return Err(CtrlError::bad("alert threshold must be finite"));
    }
    let webhook_url = req
        .webhook_url
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if let Some(ref url) = webhook_url {
        validate_outbound_url_ssrf(url)
            .map_err(|e| CtrlError::bad(format!("webhook_url rejected by SSRF policy: {e}")))?;
    }

    let now_ms = Utc::now().timestamp_millis();
    let rule_id = req
        .id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| new_id("aal"));
    let existing = ctrl
        .meta()
        .get_agent_alert_rule(&id, &rule_id)
        .await
        .map_err(CtrlError::meta)?;

    let rule = AgentAlertRule {
        id: rule_id,
        project_id: id,
        name,
        agent_name: req
            .agent_name
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        environment: req
            .environment
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        metric,
        operator,
        threshold: req.threshold,
        window_minutes: req.window_minutes.clamp(1, 10_080),
        cooldown_minutes: req.cooldown_minutes.clamp(0, 10_080),
        enabled: req.enabled,
        state: existing
            .as_ref()
            .map(|e| e.state.clone())
            .unwrap_or_else(|| "ok".to_string()),
        webhook_url,
        last_evaluated_ms: existing.as_ref().and_then(|e| e.last_evaluated_ms),
        last_triggered_ms: existing.as_ref().and_then(|e| e.last_triggered_ms),
        last_resolved_ms: existing.as_ref().and_then(|e| e.last_resolved_ms),
        last_value: existing.as_ref().and_then(|e| e.last_value),
        created_at_ms: existing.map(|e| e.created_at_ms).unwrap_or(now_ms),
    };

    ctrl.meta()
        .upsert_agent_alert_rule(&rule)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(rule))
}

async fn handle_delete_agent_alert(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, rule_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let deleted = ctrl
        .meta()
        .delete_agent_alert_rule(&id, &rule_id)
        .await
        .map_err(CtrlError::meta)?;
    if !deleted {
        return Err(CtrlError::not_found("agent alert rule not found"));
    }
    Ok(Json(json!({ "status": "ok", "id": rule_id })))
}

#[derive(Deserialize, Default)]
struct ListAlertEventsQuery {
    limit: Option<usize>,
}

async fn handle_list_agent_alert_events(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<ListAlertEventsQuery>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let limit = q.limit.unwrap_or(100).clamp(1, 200);
    let events = ctrl
        .meta()
        .list_agent_alert_events(&id, limit)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "events": events })))
}

async fn handle_evaluate_agent_alerts_all(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let (evaluated, events) = ctrl.evaluate_agent_alerts_for_project(&id, None).await?;
    Ok(Json(json!({
        "evaluated": evaluated,
        "events": events,
    })))
}

async fn handle_evaluate_agent_alert_one(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, rule_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let rule = ctrl
        .meta()
        .get_agent_alert_rule(&id, &rule_id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("agent alert rule not found"))?;
    let (evaluated, events) = ctrl
        .evaluate_agent_alerts_for_project(&id, Some(&rule.id))
        .await?;
    Ok(Json(json!({
        "evaluated": evaluated,
        "events": events,
    })))
}

async fn handle_get_privacy(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<AgentPrivacyPolicy> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let policy = ctrl
        .meta()
        .get_agent_privacy_policy(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(policy))
}

#[derive(Deserialize)]
struct UpdatePrivacyReq {
    capture_inputs: Option<bool>,
    capture_outputs: Option<bool>,
    capture_tool_args: Option<bool>,
    capture_tool_results: Option<bool>,
    capture_prompts: Option<bool>,
    capture_responses: Option<bool>,
    capture_user_id: Option<bool>,
    redact_pii: Option<bool>,
    redact_keys: Option<Vec<String>>,
    exclude_attributes: Option<Vec<String>>,
    retention_days: Option<i64>,
    sample_rate: Option<f64>,
}

async fn handle_put_privacy(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UpdatePrivacyReq>,
) -> ApiRes<AgentPrivacyPolicy> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let mut policy = ctrl
        .meta()
        .get_agent_privacy_policy(&id)
        .await
        .map_err(CtrlError::meta)?;
    if let Some(v) = req.capture_inputs {
        policy.capture_inputs = v;
    }
    if let Some(v) = req.capture_outputs {
        policy.capture_outputs = v;
    }
    if let Some(v) = req.capture_tool_args {
        policy.capture_tool_args = v;
    }
    if let Some(v) = req.capture_tool_results {
        policy.capture_tool_results = v;
    }
    if let Some(v) = req.capture_prompts {
        policy.capture_prompts = v;
    }
    if let Some(v) = req.capture_responses {
        policy.capture_responses = v;
    }
    if let Some(v) = req.capture_user_id {
        policy.capture_user_id = v;
    }
    if let Some(v) = req.redact_pii {
        policy.redact_pii = v;
    }
    if let Some(v) = req.redact_keys {
        policy.redact_keys = v;
    }
    if let Some(v) = req.exclude_attributes {
        policy.exclude_attributes = v;
    }
    if let Some(days) = req.retention_days {
        policy.retention_days = days.clamp(1, 3650);
    }
    if let Some(sr) = req.sample_rate {
        if sr.is_finite() {
            policy.sample_rate = sr.clamp(0.0, 1.0);
        }
    }
    policy.project_id = id;
    policy.updated_at_ms = Utc::now().timestamp_millis();

    ctrl.meta()
        .upsert_agent_privacy_policy(&policy)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(policy))
}

async fn handle_list_pricing(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let rules = ctrl
        .meta()
        .list_model_pricing(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({ "rules": rules })))
}

#[derive(Deserialize)]
struct CreatePricingReq {
    #[serde(default)]
    id: Option<String>,
    provider: String,
    model_pattern: String,
    input_per_1m_usd: f64,
    output_per_1m_usd: f64,
    #[serde(default)]
    cached_input_per_1m_usd: f64,
}

async fn handle_upsert_pricing(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreatePricingReq>,
) -> ApiRes<ModelPricingRule> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let provider = req.provider.trim().to_string();
    let model_pattern = req.model_pattern.trim().to_string();
    if provider.is_empty() || model_pattern.is_empty() {
        return Err(CtrlError::bad("provider and model_pattern are required"));
    }
    if !req.input_per_1m_usd.is_finite()
        || req.input_per_1m_usd < 0.0
        || !req.output_per_1m_usd.is_finite()
        || req.output_per_1m_usd < 0.0
        || !req.cached_input_per_1m_usd.is_finite()
        || req.cached_input_per_1m_usd < 0.0
    {
        return Err(CtrlError::bad("pricing rates must be finite and >= 0.0"));
    }
    let rule = ModelPricingRule {
        id: req
            .id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| new_id("prc")),
        project_id: id,
        provider,
        model_pattern,
        input_per_1m_usd: req.input_per_1m_usd,
        output_per_1m_usd: req.output_per_1m_usd,
        cached_input_per_1m_usd: req.cached_input_per_1m_usd,
        updated_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta()
        .upsert_model_pricing(&rule)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(rule))
}

async fn handle_delete_pricing(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, rule_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let deleted = ctrl
        .meta()
        .delete_model_pricing(&id, &rule_id)
        .await
        .map_err(CtrlError::meta)?;
    if !deleted {
        return Err(CtrlError::not_found("pricing rule not found"));
    }
    Ok(Json(json!({ "status": "ok", "id": rule_id })))
}

// ===========================================================================
// ControlPlaneService Alert Evaluation & Retention Pruning
// ===========================================================================

impl ControlPlaneService {
    /// Evaluates all enabled AI agent alert rules across all projects, managing `ok <-> firing`
    /// transitions, cooldown deduplication, `agent_alert_events` history, and HMAC-SHA256 webhooks.
    pub async fn evaluate_agent_alerts(&self) {
        let Ok(projects) = self.meta().list_projects().await else {
            return;
        };
        for proj in projects {
            if let Err(err) = self.evaluate_agent_alerts_for_project(&proj.id, None).await {
                tracing::warn!(project_id = %proj.id, "agent alert evaluation error: {}", err.1);
            }
        }
    }

    pub(crate) async fn evaluate_agent_alerts_for_project(
        &self,
        project_id: &str,
        only_rule_id: Option<&str>,
    ) -> Result<(usize, Vec<AgentAlertEvent>), CtrlError> {
        let now_ms = Utc::now().timestamp_millis();
        let rules = self
            .meta()
            .list_agent_alert_rules(project_id)
            .await
            .map_err(CtrlError::meta)?;
        let active_rules: Vec<AgentAlertRule> = rules
            .into_iter()
            .filter(|r| r.enabled && only_rule_id.is_none_or(|id| id == r.id))
            .collect();
        if active_rules.is_empty() {
            return Ok((0, Vec::new()));
        }

        let tombstones = active_tombstones(self, project_id).await?;
        let mut trace_rollups_cache: Option<HashMap<(EvalTargetType, String), AgentEvalRollup>> =
            None;
        let mut emitted_events = Vec::new();
        let mut evaluated = 0usize;

        for mut rule in active_rules {
            evaluated += 1;
            let window_ms = (rule.window_minutes.max(1) as i64) * 60_000;
            let filter = AnalyticsFilter {
                start_ts_ms: Some(now_ms - window_ms),
                end_ts_ms: Some(now_ms),
                agent_name: rule.agent_name.clone(),
                environment: rule.environment.clone(),
                ..Default::default()
            };

            let mut overview = self
                .agent_store()
                .query_analytics(project_id, &filter, &tombstones)
                .await
                .map_err(map_store_err)?;

            let observed_opt = match rule.metric.as_str() {
                "failure_rate" => {
                    if overview.total_executions > 0 {
                        Some(overview.error_rate)
                    } else {
                        None
                    }
                }
                "p95_latency_ms" => {
                    if overview.total_executions > 0 {
                        Some(overview.latency.p95_ms)
                    } else {
                        None
                    }
                }
                "cost_usd" => {
                    if overview.total_executions > 0 {
                        Some(overview.total_cost_usd)
                    } else {
                        None
                    }
                }
                "loop_rate" => {
                    if overview.total_executions > 0 {
                        Some(overview.loop_warning_count as f64 / overview.total_executions as f64)
                    } else {
                        None
                    }
                }
                "avg_tokens" => {
                    if overview.total_executions > 0 {
                        Some(overview.total_tokens as f64 / overview.total_executions as f64)
                    } else {
                        None
                    }
                }
                "tool_failure_rate" => {
                    let inv: i64 = overview.by_tool.iter().map(|t| t.invocations).sum();
                    if inv > 0 {
                        let fails: i64 = overview.by_tool.iter().map(|t| t.failures).sum();
                        Some(fails as f64 / inv as f64)
                    } else {
                        None
                    }
                }
                "eval_score_drop" => {
                    if overview.avg_eval_score.is_none() {
                        if trace_rollups_cache.is_none() {
                            trace_rollups_cache = Some(
                                self.meta()
                                    .list_agent_eval_rollups(
                                        project_id,
                                        Some(EvalTargetType::Trace),
                                    )
                                    .await
                                    .map_err(CtrlError::meta)?,
                            );
                        }
                        if let Some(ref rollups) = trace_rollups_cache {
                            if !rollups.is_empty() {
                                let traces = collect_filtered_traces_with_rollups(
                                    self,
                                    project_id,
                                    &filter,
                                    rollups,
                                    &tombstones,
                                )
                                .await?;
                                enrich_analytics_overview_from_traces(
                                    &filter,
                                    &traces,
                                    &mut overview,
                                );
                            }
                        }
                    }
                    overview.avg_eval_score
                }
                _ => None,
            };

            // Evaluation bookkeeping never writes `state`: the transitions below are conditional
            // updates, so with several nodes only the one that wins the claim delivers.
            let observed_opt = observed_opt.filter(|v| v.is_finite());
            self.meta()
                .record_agent_alert_evaluation(&rule.id, now_ms, observed_opt)
                .await
                .map_err(CtrlError::meta)?;
            let Some(observed) = observed_opt else {
                continue;
            };

            let condition_met = alert_condition_met(&rule.operator, observed, rule.threshold);
            let in_cooldown = rule.last_triggered_ms.is_some_and(|t| {
                now_ms.saturating_sub(t) < (rule.cooldown_minutes.max(0) as i64) * 60_000
            });

            if condition_met {
                if rule.state != "firing"
                    && !in_cooldown
                    && self
                        .meta()
                        .claim_agent_alert_transition(&rule.id, &rule.state, "firing", now_ms)
                        .await
                        .map_err(CtrlError::meta)?
                {
                    rule.state = "firing".to_string();
                    rule.last_triggered_ms = Some(now_ms);
                    let (delivered, delivery_error) = self
                        .deliver_agent_alert(&rule, "triggered", observed, now_ms)
                        .await;
                    let ev = AgentAlertEvent {
                        id: new_id("aev"),
                        project_id: project_id.to_string(),
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        event_type: "triggered".to_string(),
                        metric: rule.metric.clone(),
                        observed_value: observed,
                        threshold: rule.threshold,
                        delivered,
                        delivery_error,
                        created_at_ms: now_ms,
                    };
                    self.meta()
                        .insert_agent_alert_event(&ev)
                        .await
                        .map_err(CtrlError::meta)?;
                    emitted_events.push(ev);
                }
            } else if rule.state == "firing"
                && self
                    .meta()
                    .claim_agent_alert_transition(&rule.id, "firing", "ok", now_ms)
                    .await
                    .map_err(CtrlError::meta)?
            {
                rule.state = "ok".to_string();
                rule.last_resolved_ms = Some(now_ms);
                let (delivered, delivery_error) = self
                    .deliver_agent_alert(&rule, "resolved", observed, now_ms)
                    .await;
                let ev = AgentAlertEvent {
                    id: new_id("aev"),
                    project_id: project_id.to_string(),
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    event_type: "resolved".to_string(),
                    metric: rule.metric.clone(),
                    observed_value: observed,
                    threshold: rule.threshold,
                    delivered,
                    delivery_error,
                    created_at_ms: now_ms,
                };
                self.meta()
                    .insert_agent_alert_event(&ev)
                    .await
                    .map_err(CtrlError::meta)?;
                emitted_events.push(ev);
            }
        }

        Ok((evaluated, emitted_events))
    }

    async fn deliver_agent_alert(
        &self,
        rule: &AgentAlertRule,
        event_type: &str,
        observed_value: f64,
        now_ms: i64,
    ) -> (bool, Option<String>) {
        let cred = match self
            .meta()
            .get_byok_credential(&rule.project_id, "webhook")
            .await
        {
            Ok(Some(rec)) => rec,
            Ok(None) => {
                return if rule.webhook_url.is_some() {
                    (
                        false,
                        Some("no BYOK webhook credential configured for signing".to_string()),
                    )
                } else {
                    (false, None)
                };
            }
            Err(e) => return (false, Some(e.to_string())),
        };

        let kek = self.vault_kek();
        let cred_url = match unseal_secret(&kek, &cred.sealed_secret_hex) {
            Ok(u) => u,
            Err(e) => return (false, Some(e)),
        };
        let cfg = match unseal_byok_config_json(&kek, &cred.config_json) {
            Ok(v) => v,
            Err(e) => return (false, Some(e)),
        };
        let Some(signing_secret) = webhook_signing_secret(&cfg).map(str::to_string) else {
            return (
                false,
                Some("webhook credential has no signing_secret".to_string()),
            );
        };
        let url = rule.webhook_url.clone().unwrap_or(cred_url);
        let body = json!({
            "event": format!("pulse.agent_alert.{event_type}"),
            "project_id": rule.project_id,
            "rule": {
                "id": rule.id,
                "name": rule.name,
                "metric": rule.metric,
                "operator": rule.operator,
                "threshold": rule.threshold,
                "state": rule.state,
            },
            "observed_value": observed_value,
            "window_minutes": rule.window_minutes,
            "timestamp_ms": now_ms,
        });
        match post_signed_webhook(&url, &signing_secret, &body).await {
            Ok(_) => (true, None),
            Err(err) => (false, Some(err)),
        }
    }

    pub fn notify_agent_erasure(&self) {
        self.agent_erasure_notify.notify_one();
    }

    pub async fn agent_erasure_notified(&self) {
        self.agent_erasure_notify.notified().await;
    }

    /// Physically purges all unpurged `agent_erasure_tombstones` across writable agent storage
    /// and marks confirmed tombstones `physically_purged_at_ms`. Returns the earliest urgent
    /// pending-GC deadline (if any S3 segment is awaiting its pre-expiry deletion grace).
    pub async fn sweep_pending_agent_erasures(&self) -> Option<i64> {
        let store = self.agent_store();
        if store.is_read_only() {
            return None;
        }
        let unpurged = match self.meta().list_unpurged_erasure_tombstones().await {
            Ok(rows) => rows,
            Err(err) => {
                tracing::warn!(error = %err, "failed to load unpurged agent erasure tombstones");
                return None;
            }
        };
        if unpurged.is_empty() {
            return store.next_urgent_gc_deadline_ms().await;
        }

        let mut by_project: HashMap<String, Vec<AgentErasureTombstone>> = HashMap::new();
        for tb in unpurged {
            by_project
                .entry(tb.project_id.clone())
                .or_default()
                .push(tb);
        }

        let mut next_deadline: Option<i64> = None;
        let mut any_concurrent_ingest = false;

        for (pid, tbs) in by_project {
            match store.purge_erased_in_project(&pid, &tbs).await {
                Ok(outcome) => {
                    if let Some(dl) = outcome.next_urgent_gc_deadline_ms {
                        next_deadline = Some(next_deadline.map_or(dl, |m| m.min(dl)));
                    }
                    if outcome.purged_tombstone_ids.is_empty() {
                        continue;
                    }
                    if store
                        .last_ingest_ms(&pid)
                        .is_some_and(|ts| ts >= outcome.pass_started_at_ms)
                    {
                        any_concurrent_ingest = true;
                        continue;
                    }
                    match self
                        .meta()
                        .mark_erasure_tombstones_purged(
                            &outcome.purged_tombstone_ids,
                            outcome.pass_started_at_ms,
                        )
                        .await
                    {
                        Ok(_) => {
                            if store
                                .last_ingest_ms(&pid)
                                .is_some_and(|ts| ts >= outcome.pass_started_at_ms)
                            {
                                let _ = self
                                    .meta()
                                    .reset_erasure_tombstones_purged(&outcome.purged_tombstone_ids)
                                    .await;
                                any_concurrent_ingest = true;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                project_id = %pid,
                                error = %err,
                                "failed to mark agent erasure tombstones physically purged"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        project_id = %pid,
                        error = %err,
                        "failed to physically purge erased agent traces"
                    );
                }
            }
        }

        if any_concurrent_ingest {
            self.notify_agent_erasure();
        }

        next_deadline
    }

    /// Prunes expired daily `.plss` agent segments, expired control-plane evaluations, and
    /// expired 3-level erasure tombstones across writable projects using each project's
    /// `AgentPrivacyPolicy.retention_days`.
    pub async fn prune_expired_agent_data(&self) -> AgentRetentionStats {
        let mut stats = AgentRetentionStats::default();
        let store = self.agent_store();
        if store.is_read_only() {
            return stats;
        }

        let now = Utc::now();
        let now_ms = now.timestamp_millis();

        // 1. Ensure all unpurged tombstones (including any whose `expires_at` passed while offline)
        //    are physically purged BEFORE SQL tombstone pruning can ever run.
        let _ = self.sweep_pending_agent_erasures().await;

        // 2. Unconditionally re-verify ALL expired-and-purged tombstones across storage (never
        //    gating on volatile in-memory `last_ingest_ms` which resets to 0 across restarts) and
        //    ONLY permanently delete tombstone IDs explicitly confirmed in `purged_tombstone_ids`.
        let mut confirmed_prunable_ids: Vec<String> = Vec::new();
        let mut unconfirmed_reopen_ids: Vec<String> = Vec::new();
        if let Ok(expired_purged) = self
            .meta()
            .list_expired_purged_erasure_tombstones(now_ms)
            .await
        {
            let mut by_project: HashMap<String, Vec<AgentErasureTombstone>> = HashMap::new();
            for (tb, _purged_at) in expired_purged {
                by_project
                    .entry(tb.project_id.clone())
                    .or_default()
                    .push(tb);
            }
            for (pid, tbs) in by_project {
                match store.purge_erased_in_project(&pid, &tbs).await {
                    Ok(outcome) => {
                        let concurrent_ingest = store
                            .last_ingest_ms(&pid)
                            .is_some_and(|ts| ts >= outcome.pass_started_at_ms);
                        let confirmed_set: HashSet<String> = if concurrent_ingest {
                            HashSet::new()
                        } else {
                            outcome.purged_tombstone_ids.into_iter().collect()
                        };
                        for tb in tbs {
                            if confirmed_set.contains(&tb.id) {
                                confirmed_prunable_ids.push(tb.id);
                            } else {
                                unconfirmed_reopen_ids.push(tb.id);
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            project_id = %pid,
                            error = %err,
                            "failed to re-verify expired agent erasure tombstones before pruning"
                        );
                    }
                }
            }
        }
        if !unconfirmed_reopen_ids.is_empty() {
            let _ = self
                .meta()
                .reset_erasure_tombstones_purged(&unconfirmed_reopen_ids)
                .await;
            self.notify_agent_erasure();
        }

        // 3. Prune expired `.plss` segments ONLY for known agent projects (never opening event-only projects).
        if let Ok(agent_pids) = store.known_project_ids().await {
            for pid in agent_pids {
                let policy = self
                    .meta()
                    .get_agent_privacy_policy(&pid)
                    .await
                    .unwrap_or_else(|_| AgentPrivacyPolicy::default_for_project(&pid));
                let days = policy.retention_days.clamp(1, 3650);
                let cutoff = now - chrono::Duration::days(days);
                let cutoff_date = cutoff.date_naive();
                match store.prune_expired_segments(&pid, cutoff_date).await {
                    Ok(n) => stats.segments_pruned += n,
                    Err(err) => tracing::warn!(
                        project_id = %pid,
                        error = %err,
                        "failed to prune expired agent segments"
                    ),
                }
            }
        }

        // 4. Prune expired SQL evaluations across projects (pure SQL, 0 S3 calls).
        if let Ok(projects) = self.meta().list_projects().await {
            for proj in projects {
                stats.projects_checked += 1;
                let policy = self
                    .meta()
                    .get_agent_privacy_policy(&proj.id)
                    .await
                    .unwrap_or_else(|_| AgentPrivacyPolicy::default_for_project(&proj.id));
                let days = policy.retention_days.clamp(1, 3650);
                let cutoff = now - chrono::Duration::days(days);
                let cutoff_ms = cutoff.timestamp_millis();

                match self
                    .meta()
                    .prune_expired_agent_evaluations(&proj.id, cutoff_ms)
                    .await
                {
                    Ok(n) => stats.evaluations_pruned += n,
                    Err(err) => tracing::warn!(
                        project_id = %proj.id,
                        error = %err,
                        "failed to prune expired agent evaluations"
                    ),
                }
            }
        }

        if !confirmed_prunable_ids.is_empty() {
            match self
                .meta()
                .prune_erasure_tombstones_by_ids(&confirmed_prunable_ids, now_ms)
                .await
            {
                Ok(n) => stats.tombstones_pruned += n,
                Err(err) => {
                    tracing::warn!(error = %err, "failed to prune expired erasure tombstones")
                }
            }
        }

        stats
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use pulse_agent::{encode_plst_batch, AgentTraceStoreConfig};
    use pulse_exec::{CatalogRegistry, QueryEngine};
    use pulse_identity::IdentityRouter;
    use pulse_storage::{build_from_url, EncryptedObjectStore, StorageConfig, StorageEngine};
    use ring::hmac;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use crate::api::{build_control_router, sign_webhook};
    use crate::db::MetaStore;
    use crate::models::{seal_byok_config_json, seal_secret, ByokCredentialRecord};

    const TEST_ADMIN_TOKEN: &str = "sk_root_test_m2b_agent_token_123456";
    const TEST_INGEST_KEY: &str = "pk_test_m2b_agent_ingest_123456";

    async fn make_test_ctrl() -> (TempDir, Arc<ControlPlaneService>) {
        let dir = TempDir::new().unwrap();
        let ctrl = make_test_ctrl_at(dir.path(), "agent_cache").await;
        (dir, ctrl)
    }

    /// A node with its own object store whose metadata DB lives in `dir`.
    async fn make_test_ctrl_at(dir: &std::path::Path, cache: &str) -> Arc<ControlPlaneService> {
        let meta = MetaStore::connect_auto(dir).await.unwrap();
        let raw_store = build_from_url("memory://").unwrap();
        let enc_store = Arc::new(EncryptedObjectStore::new(raw_store));
        let id_router = Arc::new(IdentityRouter::new(enc_store.as_object_store()));
        let storage = StorageEngine::new_with_encrypted(
            StorageConfig {
                num_buckets: 2,
                l0_flush_row_threshold: 1000,
                l0_flush_max_age: Duration::from_secs(60),
                compaction_min_segments: 4,
            },
            enc_store,
            id_router,
        );
        let qengine = QueryEngine::new(storage, 4 * 1024 * 1024, None, 0, CatalogRegistry::new())
            .await
            .unwrap();
        ControlPlaneService::new_with_admin_with_agent_config(
            meta,
            qengine,
            "proj_default",
            TEST_INGEST_KEY,
            Some(TEST_ADMIN_TOKEN.to_string()),
            AgentTraceStoreConfig {
                cache_dir: Some(dir.join(cache)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    /// Scalability review R-05: two nodes share the metadata DB and both see a failing trace.
    /// The state transition is the claim, so only one records and delivers the trigger.
    #[tokio::test]
    async fn two_nodes_emit_one_agent_alert_event_per_transition() {
        let dir = TempDir::new().unwrap();
        let a = make_test_ctrl_at(dir.path(), "cache_a").await;
        let b = make_test_ctrl_at(dir.path(), "cache_b").await;
        let now_ms = Utc::now().timestamp_millis();
        a.meta()
            .upsert_agent_alert_rule(&AgentAlertRule {
                id: "rule_claim".to_string(),
                project_id: "proj_default".to_string(),
                name: "High Failure Rate".to_string(),
                agent_name: None,
                environment: None,
                metric: "failure_rate".to_string(),
                operator: "gt".to_string(),
                threshold: 0.2,
                window_minutes: 60,
                cooldown_minutes: 10,
                enabled: true,
                state: "ok".to_string(),
                webhook_url: None,
                last_evaluated_ms: None,
                last_triggered_ms: None,
                last_resolved_ms: None,
                last_value: None,
                created_at_ms: now_ms,
            })
            .await
            .unwrap();
        for ctrl in [&a, &b] {
            let trace: AgentTrace = serde_json::from_value(json!({
                "trace_id": "tr_fail_1", "project_id": "proj_default", "agent_name": "bot",
                "agent_version": "1.0.0", "environment": "production", "status": "error",
                "start_ts_ms": now_ms - 500, "end_ts_ms": now_ms - 400, "duration_ms": 100,
            }))
            .unwrap();
            ctrl.agent_store()
                .ingest_batch(
                    "proj_default",
                    TraceIngestBatch {
                        traces: vec![trace],
                        spans: vec![],
                    },
                )
                .await
                .unwrap();
        }

        let (ra, rb) = tokio::join!(
            a.evaluate_agent_alerts_for_project("proj_default", None),
            b.evaluate_agent_alerts_for_project("proj_default", None),
        );
        assert_eq!(ra.unwrap().1.len() + rb.unwrap().1.len(), 1);
        let events = a
            .meta()
            .list_agent_alert_events("proj_default", 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "one triggered event row, not one per node");
        let rule = a
            .meta()
            .get_agent_alert_rule("proj_default", "rule_claim")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rule.state, "firing");
    }

    #[tokio::test]
    async fn ingest_plst_and_otlp_privacy_pricing_and_zero_sql_writes() {
        let (_dir, ctrl) = make_test_ctrl().await;
        let app = build_control_router(ctrl.clone());
        let now_ms = Utc::now().timestamp_millis();

        // Confirm custom pricing overrides apply on ingest
        let pricing_req = Request::builder()
            .method("POST")
            .uri("/v1/projects/proj_default/agent/pricing")
            .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "provider": "openai",
                    "model_pattern": "custom-m2b-",
                    "input_per_1m_usd": 2.0,
                    "output_per_1m_usd": 8.0,
                    "cached_input_per_1m_usd": 1.0
                }))
                .unwrap(),
            ))
            .unwrap();
        let pr = app.clone().oneshot(pricing_req).await.unwrap();
        assert_eq!(pr.status(), StatusCode::OK);

        // Missing key -> 401
        let unauth_req = Request::builder()
            .method("POST")
            .uri("/v1/agent/traces")
            .header("Content-Type", "application/json")
            .body(Body::from(b"{}".as_slice()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(unauth_req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        // Admin sk_ token on public ingest -> 401
        let sk_ingest_req = Request::builder()
            .method("POST")
            .uri("/v1/agent/traces")
            .header("x-pulse-key", TEST_ADMIN_TOKEN)
            .header("Content-Type", "application/json")
            .body(Body::from(b"{}".as_slice()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(sk_ingest_req).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        // PLST binary ingest with identical payloads across two spans (blob dedup) + PII + pricing
        let shared_payload = json!({
            "prompt": "Contact user@example.com",
            "password": "super-secret-password"
        });
        let trace = AgentTrace {
            trace_id: "tr_plst_1".to_string(),
            project_id: String::new(),
            agent_name: "support".to_string(),
            agent_version: "1.0.0".to_string(),
            prompt_version: None,
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".to_string(),
            session_id: Some("sess_plst_1".to_string()),
            user_id: Some("user_secret_id".to_string()),
            workflow_id: None,
            parent_trace_id: None,
            status: TraceStatus::Ok,
            start_ts_ms: now_ms - 500,
            end_ts_ms: Some(now_ms - 100),
            duration_ms: 400,
            input: shared_payload.clone(),
            output: json!({"reply": "ok"}),
            error_type: None,
            error_message: None,
            failure_source: None,
            metadata: Value::Null,
            tags: vec![],
            span_count: 2,
            llm_call_count: 1,
            tool_call_count: 0,
            error_span_count: 0,
            input_tokens: 1000,
            output_tokens: 500,
            cached_tokens: 200,
            total_tokens: 1500,
            total_cost_nanos: 0,
            total_cost_usd: 0.0,
            primary_model: Some("custom-m2b-pro".to_string()),
            primary_provider: Some("openai".to_string()),
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning: false,
        };
        let sp1 = AgentSpan {
            span_id: "sp_1".to_string(),
            trace_id: "tr_plst_1".to_string(),
            project_id: String::new(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: "llm".to_string(),
            span_type: SpanType::Llm,
            status: TraceStatus::Ok,
            start_ts_ms: now_ms - 500,
            end_ts_ms: Some(now_ms - 100),
            duration_ms: 400,
            provider: Some("openai".to_string()),
            model: Some("custom-m2b-pro".to_string()),
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 1000,
            output_tokens: 500,
            cached_tokens: 200,
            total_tokens: 1500,
            cost_nanos: 0,
            cost_usd: 0.0,
            temperature: None,
            finish_reason: None,
            model_params: Value::Null,
            tool_name: None,
            tool_version: None,
            tool_retry_count: 0,
            input: shared_payload.clone(),
            output: json!({"reply": "ok"}),
            attributes: Value::Null,
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        };
        let batch = TraceIngestBatch {
            traces: vec![trace],
            spans: vec![sp1],
        };
        let plst_bytes = encode_plst_batch(&batch, true).unwrap();
        let plst_req = Request::builder()
            .method("POST")
            .uri("/v1/agent/traces")
            .header("x-pulse-key", TEST_INGEST_KEY)
            .header("Content-Type", "application/x-pulse-trace")
            .body(Body::from(plst_bytes))
            .unwrap();
        let plst_resp = app.clone().oneshot(plst_req).await.unwrap();
        assert_eq!(plst_resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(plst_resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let ingest_res: TraceIngestResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(ingest_res.accepted_traces, 1);
        assert_eq!(ingest_res.accepted_spans, 1);
        assert!(ingest_res.redacted_fields >= 2);

        // Verify metrics updated (including intra-batch blob dedup between trace & span payloads)
        assert_eq!(
            ctrl.agent_metrics().traces_ingested.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            ctrl.agent_metrics().spans_ingested.load(Ordering::Relaxed),
            1
        );
        assert!(ctrl.agent_metrics().blobs_deduped.load(Ordering::Relaxed) >= 2);

        // Verify SQLite has ZERO agent_traces / agent_spans tables (zero SQL writes on ingest)
        let MetaStore::Sqlite(pool) = ctrl.meta() else {
            unreachable!();
        };
        let sql_tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('agent_traces', 'agent_spans')",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        assert!(sql_tables.is_empty());

        // Verify trace detail via HTTP GET (default privacy strips user_id, redacts PII/secret, prices cost_nanos)
        let det_req = Request::builder()
            .method("GET")
            .uri("/v1/projects/proj_default/agent/traces/tr_plst_1")
            .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
            .body(Body::empty())
            .unwrap();
        let det_resp = app.clone().oneshot(det_req).await.unwrap();
        assert_eq!(det_resp.status(), StatusCode::OK);
        let det_bytes = axum::body::to_bytes(det_resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let detail: TraceDetail = serde_json::from_slice(&det_bytes).unwrap();
        assert_eq!(detail.trace.user_id, None);
        assert_eq!(detail.trace.input["password"], "[REDACTED]");
        assert_eq!(detail.spans[0].cost_nanos, 5_800_000);
        assert_eq!(detail.trace.total_cost_nanos, 5_800_000);

        // OTLP/JSON ingest
        let otlp_payload = json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": { "stringValue": "otlp-bot" }
                    }]
                },
                "scopeSpans": [{
                    "scope": { "name": "otel", "version": "1.0" },
                    "spans": [{
                        "traceId": "tr_otlp_1",
                        "spanId": "sp_otlp_1",
                        "parentSpanId": "",
                        "name": "chat gpt-4o-mini",
                        "startTimeUnixNano": ((now_ms as u64 - 200) * 1_000_000).to_string(),
                        "endTimeUnixNano": ((now_ms as u64 - 100) * 1_000_000).to_string(),
                        "attributes": [
                            { "key": "gen_ai.system", "value": { "stringValue": "openai" } },
                            { "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4o-mini" } },
                            { "key": "gen_ai.usage.input_tokens", "value": { "intValue": 1000 } },
                            { "key": "gen_ai.usage.output_tokens", "value": { "intValue": 500 } }
                        ]
                    }]
                }]
            }]
        });
        let otlp_http = Request::builder()
            .method("POST")
            .uri("/v1/otel/v1/traces")
            .header("x-pulse-key", TEST_INGEST_KEY)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&otlp_payload).unwrap()))
            .unwrap();
        let otlp_resp = app.clone().oneshot(otlp_http).await.unwrap();
        assert_eq!(otlp_resp.status(), StatusCode::OK);
        assert_eq!(
            ctrl.agent_metrics().traces_ingested.load(Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn agent_alerts_lifecycle_cooldown_hmac_and_retention() {
        let (_dir, ctrl) = make_test_ctrl().await;

        let secret_signing = "super_secret_hmac_key_12345";
        let ts = "1700000000";
        let payload = br#"{"event":"pulse.agent_alert.triggered"}"#;
        let sig_hdr = sign_webhook(secret_signing, ts, payload);
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret_signing.as_bytes());
        let mut ctx = hmac::Context::with_key(&key);
        ctx.update(ts.as_bytes());
        ctx.update(b".");
        ctx.update(payload);
        assert_eq!(
            sig_hdr,
            format!("sha256={}", hex::encode(ctx.sign().as_ref()))
        );

        // Configure BYOK webhook pointing to loopback (SSRF blocks without PULSE_ALLOW_PRIVATE_ENDPOINTS,
        // verifying failure is recorded cleanly in AgentAlertEvent without aborting evaluation).
        let hook_url = "https://127.0.0.1:9/hook".to_string();
        let kek = ctrl.vault_kek();
        ctrl.meta()
            .upsert_byok_credential(&ByokCredentialRecord {
                id: "byok_wh_1".to_string(),
                project_id: "proj_default".to_string(),
                category: "webhook".to_string(),
                provider: "custom".to_string(),
                label: "Test Webhook".to_string(),
                key_prefix_masked: "https://...".to_string(),
                sealed_secret_hex: seal_secret(&kek, &hook_url),
                config_json: seal_byok_config_json(
                    &kek,
                    &json!({ "signing_secret": secret_signing }),
                ),
                status: "active".to_string(),
                last_verified_at_ms: None,
                updated_at_ms: Utc::now().timestamp_millis(),
            })
            .await
            .unwrap();

        let now_ms = Utc::now().timestamp_millis();
        ctrl.meta()
            .upsert_agent_alert_rule(&AgentAlertRule {
                id: "rule_fail".to_string(),
                project_id: "proj_default".to_string(),
                name: "High Failure Rate".to_string(),
                agent_name: None,
                environment: None,
                metric: "failure_rate".to_string(),
                operator: "gt".to_string(),
                threshold: 0.2,
                window_minutes: 60,
                cooldown_minutes: 10,
                enabled: true,
                state: "ok".to_string(),
                webhook_url: Some(hook_url.clone()),
                last_evaluated_ms: None,
                last_triggered_ms: None,
                last_resolved_ms: None,
                last_value: None,
                created_at_ms: now_ms,
            })
            .await
            .unwrap();

        // Ingest failing trace -> failure_rate = 1.0 -> ok -> firing
        ctrl.agent_store()
            .ingest_batch(
                "proj_default",
                TraceIngestBatch {
                    traces: vec![AgentTrace {
                        trace_id: "tr_fail_1".to_string(),
                        project_id: "proj_default".to_string(),
                        agent_name: "bot".to_string(),
                        agent_version: "1.0.0".to_string(),
                        prompt_version: None,
                        model_config_version: None,
                        tool_config_version: None,
                        app_release: None,
                        environment: "production".to_string(),
                        session_id: Some("s1".to_string()),
                        user_id: None,
                        workflow_id: None,
                        parent_trace_id: None,
                        status: TraceStatus::Error,
                        start_ts_ms: now_ms - 500,
                        end_ts_ms: Some(now_ms - 400),
                        duration_ms: 100,
                        input: Value::Null,
                        output: Value::Null,
                        error_type: Some("ToolError".to_string()),
                        error_message: Some("boom".to_string()),
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
                    }],
                    spans: vec![],
                },
            )
            .await
            .unwrap();

        let (_, evs1) = ctrl
            .evaluate_agent_alerts_for_project("proj_default", None)
            .await
            .unwrap();
        assert_eq!(evs1.len(), 1);
        assert_eq!(evs1[0].event_type, "triggered");
        assert!(!evs1[0].delivered);
        assert!(evs1[0].delivery_error.is_some());

        // Second evaluation immediately -> cooldown / already firing suppresses duplicate
        let (_, evs2) = ctrl
            .evaluate_agent_alerts_for_project("proj_default", None)
            .await
            .unwrap();
        assert!(evs2.is_empty());

        // Ingest 9 OK traces -> failure_rate = 0.1 <= 0.2 -> firing -> ok (resolved)
        let ok_traces: Vec<AgentTrace> = (0..9)
            .map(|i| AgentTrace {
                trace_id: format!("tr_ok_{i}"),
                project_id: "proj_default".to_string(),
                agent_name: "bot".to_string(),
                agent_version: "1.0.0".to_string(),
                prompt_version: None,
                model_config_version: None,
                tool_config_version: None,
                app_release: None,
                environment: "production".to_string(),
                session_id: Some("s1".to_string()),
                user_id: None,
                workflow_id: None,
                parent_trace_id: None,
                status: TraceStatus::Ok,
                start_ts_ms: now_ms - 300 + i,
                end_ts_ms: Some(now_ms - 250 + i),
                duration_ms: 50,
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
            })
            .collect();
        ctrl.agent_store()
            .ingest_batch(
                "proj_default",
                TraceIngestBatch {
                    traces: ok_traces,
                    spans: vec![],
                },
            )
            .await
            .unwrap();

        let (_, evs3) = ctrl
            .evaluate_agent_alerts_for_project("proj_default", None)
            .await
            .unwrap();
        assert_eq!(evs3.len(), 1);
        assert_eq!(evs3[0].event_type, "resolved");

        // Verify rule returned to state = "ok"
        let updated_rule = ctrl
            .meta()
            .get_agent_alert_rule("proj_default", "rule_fail")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated_rule.state, "ok");

        // Verify retention pruning removes old segments, evaluations, and expired tombstones
        let mut policy = ctrl
            .meta()
            .get_agent_privacy_policy("proj_default")
            .await
            .unwrap();
        policy.retention_days = 1;
        ctrl.meta()
            .upsert_agent_privacy_policy(&policy)
            .await
            .unwrap();

        let old_ms = now_ms - 10 * 86_400_000;
        ctrl.agent_store()
            .ingest_batch(
                "proj_default",
                TraceIngestBatch {
                    traces: vec![AgentTrace {
                        trace_id: "tr_old_ret".to_string(),
                        project_id: "proj_default".to_string(),
                        agent_name: "bot".to_string(),
                        agent_version: "1.0.0".to_string(),
                        prompt_version: None,
                        model_config_version: None,
                        tool_config_version: None,
                        app_release: None,
                        environment: "production".to_string(),
                        session_id: Some("s_old".to_string()),
                        user_id: None,
                        workflow_id: None,
                        parent_trace_id: None,
                        status: TraceStatus::Ok,
                        start_ts_ms: old_ms,
                        end_ts_ms: Some(old_ms + 10),
                        duration_ms: 10,
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
                    }],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        ctrl.agent_store().flush_all_l0().await.unwrap();

        ctrl.meta()
            .insert_agent_evaluation(&AgentEvaluation {
                id: "ev_old_ret".to_string(),
                project_id: "proj_default".to_string(),
                target_type: EvalTargetType::Trace,
                session_id: "s_old".to_string(),
                trace_id: Some("tr_old_ret".to_string()),
                span_id: None,
                experiment_run_id: None,
                evaluator_id: "cfg".to_string(),
                evaluator_name: "no_errors".to_string(),
                evaluator_type: EvaluatorType::Deterministic,
                evaluator_version: "1.0.0".to_string(),
                score: 1.0,
                score_type: ScoreType::Boolean,
                passed: Some(true),
                label: None,
                reasoning: None,
                execution_ms: 1,
                error: None,
                metadata: Value::Null,
                created_at_ms: old_ms,
            })
            .await
            .unwrap();
        ctrl.meta()
            .insert_erasure_tombstone(&AgentErasureTombstone {
                id: "ers_expired".to_string(),
                project_id: "proj_default".to_string(),
                level: ErasureLevel::Trace,
                target_id: "tr_gone".to_string(),
                created_at: old_ms,
                expires_at: Some(now_ms - 1000),
            })
            .await
            .unwrap();

        let ret_stats = ctrl.prune_expired_agent_data().await;
        assert!(ret_stats.segments_pruned >= 1);
        assert_eq!(ret_stats.evaluations_pruned, 1);
        assert_eq!(ret_stats.tombstones_pruned, 1);
        assert!(ctrl
            .agent_store()
            .get_trace_detail("proj_default", "tr_old_ret", &[])
            .await
            .unwrap()
            .is_none());
        assert!(ctrl
            .agent_store()
            .get_trace_detail("proj_default", "tr_ok_0", &[])
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn sessions_erasure_promote_and_experiments_http_flow() {
        let (_dir, ctrl) = make_test_ctrl().await;
        let app = build_control_router(ctrl.clone());
        let now_ms = Utc::now().timestamp_millis();

        // Enable capture_user_id so User-level erasure can match user_id
        let priv_req = Request::builder()
            .method("PUT")
            .uri("/v1/projects/proj_default/agent/privacy")
            .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({ "capture_user_id": true })).unwrap(),
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(priv_req).await.unwrap().status(),
            StatusCode::OK
        );

        let batch = TraceIngestBatch {
            traces: vec![AgentTrace {
                trace_id: "tr_flow_1".to_string(),
                project_id: String::new(),
                agent_name: "order-bot".to_string(),
                agent_version: "1.0.0".to_string(),
                prompt_version: None,
                model_config_version: None,
                tool_config_version: None,
                app_release: None,
                environment: "production".to_string(),
                session_id: Some("sess_flow_1".to_string()),
                user_id: Some("usr_erase_me".to_string()),
                workflow_id: None,
                parent_trace_id: None,
                status: TraceStatus::Ok,
                start_ts_ms: now_ms - 400,
                end_ts_ms: Some(now_ms - 100),
                duration_ms: 300,
                input: json!({"q": "status of order 42?"}),
                output: json!({"reply": "Shipped"}),
                error_type: None,
                error_message: None,
                failure_source: None,
                metadata: Value::Null,
                tags: vec![],
                span_count: 1,
                llm_call_count: 0,
                tool_call_count: 1,
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
            }],
            spans: vec![AgentSpan {
                span_id: "sp_tool_1".to_string(),
                trace_id: "tr_flow_1".to_string(),
                project_id: String::new(),
                parent_span_id: None,
                linked_span_ids: vec![],
                span_phase: SpanPhase::Final,
                depth: 0,
                name: "order_lookup".to_string(),
                span_type: SpanType::Tool,
                status: TraceStatus::Ok,
                start_ts_ms: now_ms - 350,
                end_ts_ms: Some(now_ms - 150),
                duration_ms: 200,
                provider: None,
                model: None,
                model_id: None,
                model_version: None,
                prompt_template_id: None,
                prompt_template_version: None,
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: 0,
                total_tokens: 0,
                cost_nanos: 0,
                cost_usd: 0.0,
                temperature: None,
                finish_reason: None,
                model_params: Value::Null,
                tool_name: Some("order_lookup".to_string()),
                tool_version: None,
                tool_retry_count: 0,
                input: json!({"order_id": 42}),
                output: json!({"state": "shipped"}),
                attributes: Value::Null,
                events: vec![],
                metadata: Value::Null,
                error_type: None,
                error_message: None,
                error_stack: None,
            }],
        };
        let ingest_req = Request::builder()
            .method("POST")
            .uri("/v1/agent/traces")
            .header("x-pulse-key", TEST_INGEST_KEY)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&batch).unwrap()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(ingest_req).await.unwrap().status(),
            StatusCode::OK
        );

        // Post evaluation with empty error "" -> normalized to None so rollup updates
        let eval_req = Request::builder()
            .method("POST")
            .uri("/v1/projects/proj_default/agent/evaluations")
            .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
            .header("Content-Type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "target_type": "session",
                    "session_id": "sess_flow_1",
                    "evaluator_id": "ext_1",
                    "evaluator_name": "helpfulness",
                    "score": 0.9,
                    "passed": true,
                    "error": "   "
                }))
                .unwrap(),
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(eval_req).await.unwrap().status(),
            StatusCode::OK
        );

        // Session list & detail hydrate session rollup + evaluations
        let sess_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/proj_default/agent/sessions/sess_flow_1")
                    .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sess_resp.status(), StatusCode::OK);
        let sess_detail: SessionDetail = serde_json::from_slice(
            &axum::body::to_bytes(sess_resp.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sess_detail.summary.avg_eval_score, Some(0.9));
        assert_eq!(sess_detail.evaluations.len(), 1);

        // Create dataset & promote trace -> derives expected_output & expected_tools
        let ds_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/proj_default/agent/datasets")
                    .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({ "name": "orders" })).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let ds: AgentDataset = serde_json::from_slice(
            &axum::body::to_bytes(ds_resp.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        let prom_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/proj_default/agent/traces/tr_flow_1/promote")
                    .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({ "dataset_id": ds.id })).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(prom_resp.status(), StatusCode::OK);
        let promoted: AgentDatasetItem = serde_json::from_slice(
            &axum::body::to_bytes(prom_resp.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(promoted.source_trace_id.as_deref(), Some("tr_flow_1"));
        assert_eq!(
            promoted.expected_behavior["expected_tools"],
            json!(["order_lookup"])
        );

        // Experiment & compare insufficient_data guard
        let exp1: AgentExperiment = serde_json::from_slice(
            &axum::body::to_bytes(
                app.clone()
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/v1/projects/proj_default/agent/experiments")
                            .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                            .header("Content-Type", "application/json")
                            .body(Body::from(
                                serde_json::to_vec(&json!({
                                    "name": "exp_a",
                                    "dataset_id": ds.id,
                                    "agent_name": "order-bot",
                                    "agent_version": "1.0"
                                }))
                                .unwrap(),
                            ))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .into_body(),
                1024 * 1024,
            )
            .await
            .unwrap(),
        )
        .unwrap();
        let exp2: AgentExperiment = serde_json::from_slice(
            &axum::body::to_bytes(
                app.clone()
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/v1/projects/proj_default/agent/experiments")
                            .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                            .header("Content-Type", "application/json")
                            .body(Body::from(
                                serde_json::to_vec(&json!({
                                    "name": "exp_b",
                                    "dataset_id": ds.id,
                                    "agent_name": "order-bot",
                                    "agent_version": "2.0"
                                }))
                                .unwrap(),
                            ))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .into_body(),
                1024 * 1024,
            )
            .await
            .unwrap(),
        )
        .unwrap();

        let cmp_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/v1/projects/proj_default/agent/experiments/compare?baseline={}&candidate={}",
                        exp1.id, exp2.id
                    ))
                    .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cmp_resp.status(), StatusCode::OK);
        let cmp_val: Value = serde_json::from_slice(
            &axum::body::to_bytes(cmp_resp.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cmp_val["overall_verdict"], "insufficient_data");

        // User-level erasure hides trace & session AND purges session evaluation
        let erase_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/projects/proj_default/agent/erasure")
                    .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "level": "user",
                            "target_id": "usr_erase_me"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(erase_resp.status(), StatusCode::OK);
        let erase_val: Value = serde_json::from_slice(
            &axum::body::to_bytes(erase_resp.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(erase_val["purged_evaluations"], 1);

        let after_trace = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/projects/proj_default/agent/traces/tr_flow_1")
                    .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(after_trace.status(), StatusCode::NOT_FOUND);
    }
}
