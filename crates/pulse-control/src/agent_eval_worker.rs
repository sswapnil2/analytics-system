//! Asynchronous Multi-Level Evaluation Worker (`Session | Trace | Span`).
//!
//! Consumes lightweight [`EvalJob`]s from a bounded `mpsc` channel (`cap = 2048`),
//! enforces 3-level GDPR/erasure tombstones both pre-hydration and immediately before
//! persistence, lazily hydrates traces/sessions from [`AgentTraceStore`], runs built-in
//! deterministic evaluators plus BYOK `llm_judge` evaluations (capped at
//! [`MAX_CONCURRENT_LLM_JUDGES`] concurrent outbound calls via SSRF-pinned
//! [`crate::copilot::call_external_llm_with_system`]), updates `agent_evaluations` +
//! `agent_eval_rollups`, and drains all buffered jobs on `watch::Receiver<bool>` shutdown.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use pulse_agent::{
    build_llm_judge_prompt_for_target, parse_llm_judge_response_for_target,
    run_deterministic_evaluator_for_target, AgentErasureTombstone, AgentEvaluation, AgentSpan,
    AgentTrace, AgentTraceStore, ErasureLevel, EvalContext, EvalTarget, EvalTargetType,
    EvaluatorConfig, SessionEvalContext, SpanEvalContext, TraceDetail,
};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;

use crate::copilot::{call_external_llm_with_system, llm_model_and_endpoint};
use crate::db::MetaStore;
use crate::models::{unseal_byok_config_json, unseal_secret};

pub const EVAL_QUEUE_CAPACITY: usize = 2048;
pub const MAX_CONCURRENT_LLM_JUDGES: usize = 8;
const MAX_CONCURRENT_EVAL_JOBS: usize = 32;
const MAX_IN_FLIGHT_EVAL_TASKS: usize = EVAL_QUEUE_CAPACITY;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HYDRATED_TRACES_PER_SESSION: usize = 256;

const LLM_JUDGE_SYSTEM_PROMPT: &str =
    "You are an objective AI agent evaluator. Respond ONLY with a valid JSON object.";

#[derive(Debug, Clone)]
pub struct EvalJob {
    pub project_id: String,
    pub session_id: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub evaluator_ids: Option<Vec<String>>,
}

#[derive(Debug, Default)]
pub struct AgentIngestMetrics {
    pub traces_ingested: AtomicU64,
    pub spans_ingested: AtomicU64,
    pub blobs_deduped: AtomicU64,
    /// Number of valid (non-error) evaluations executed and persisted.
    pub evals_executed: AtomicU64,
    pub eval_queue_dropped: AtomicU64,
}

#[derive(Clone)]
struct DecryptedLlmCred {
    provider: String,
    api_key: String,
    default_model: String,
    endpoint: Option<String>,
}

struct PreparedLlmTask {
    cfg: EvaluatorConfig,
    target_type: EvalTargetType,
    session_id: String,
    trace_id: Option<String>,
    span_id: Option<String>,
    prompt: String,
}

/// Runs the background evaluation worker until `shutdown` signals `true` or all senders drop,
/// then drains any remaining buffered [`EvalJob`]s before returning.
pub async fn run_agent_eval_worker(
    meta: MetaStore,
    agent_store: Arc<AgentTraceStore>,
    vault_kek: [u8; 32],
    metrics: Arc<AgentIngestMetrics>,
    mut rx: mpsc::Receiver<EvalJob>,
    mut shutdown: watch::Receiver<bool>,
) {
    let llm_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_LLM_JUDGES));
    let job_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_EVAL_JOBS));
    let mut in_flight: JoinSet<()> = JoinSet::new();

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            maybe_job = rx.recv(), if in_flight.len() < MAX_IN_FLIGHT_EVAL_TASKS => {
                match maybe_job {
                    Some(job) => {
                        spawn_eval_job(
                            &mut in_flight,
                            &meta,
                            &agent_store,
                            vault_kek,
                            &metrics,
                            &llm_sem,
                            &job_sem,
                            job,
                        );
                    }
                    None => break,
                }
            }
            Some(_) = in_flight.join_next(), if !in_flight.is_empty() => {}
        }
    }

    // Close the receiver so no new jobs arrive while keeping already-buffered jobs drainable
    // within the bounded `JoinSet` limit.
    rx.close();
    let drain_res = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, async {
        loop {
            while in_flight.len() < MAX_IN_FLIGHT_EVAL_TASKS {
                match rx.try_recv() {
                    Ok(job) => spawn_eval_job(
                        &mut in_flight,
                        &meta,
                        &agent_store,
                        vault_kek,
                        &metrics,
                        &llm_sem,
                        &job_sem,
                        job,
                    ),
                    Err(_) => break,
                }
            }
            if in_flight.is_empty() {
                break;
            }
            if in_flight.join_next().await.is_none() {
                break;
            }
        }
    })
    .await;
    if drain_res.is_err() {
        tracing::warn!(
            remaining = in_flight.len(),
            "agent eval worker shutdown drain timed out; aborting remaining tasks"
        );
        in_flight.abort_all();
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_eval_job(
    in_flight: &mut JoinSet<()>,
    meta: &MetaStore,
    agent_store: &Arc<AgentTraceStore>,
    vault_kek: [u8; 32],
    metrics: &Arc<AgentIngestMetrics>,
    llm_sem: &Arc<Semaphore>,
    job_sem: &Arc<Semaphore>,
    job: EvalJob,
) {
    let meta = meta.clone();
    let agent_store = agent_store.clone();
    let metrics = metrics.clone();
    let llm_sem = llm_sem.clone();
    let job_sem = job_sem.clone();

    in_flight.spawn(async move {
        if let Err(err) = process_eval_job(
            &meta,
            &agent_store,
            vault_kek,
            &metrics,
            &llm_sem,
            &job_sem,
            job,
        )
        .await
        {
            tracing::warn!(error = %err, "agent eval job failed");
        }
    });
}

fn config_applies_to_job(cfg: &EvaluatorConfig, job: &EvalJob) -> bool {
    if let Some(ref ids) = job.evaluator_ids {
        if !ids.iter().any(|id| id == &cfg.id) {
            return false;
        }
    } else if !cfg.enabled {
        return false;
    }

    if job.span_id.is_some() && cfg.target_type != EvalTargetType::Span {
        return false;
    }
    if job.trace_id.is_none()
        && job.session_id.is_some()
        && cfg.target_type != EvalTargetType::Session
    {
        return false;
    }

    if job.evaluator_ids.is_none() {
        let sr = cfg.sample_rate;
        if sr.is_nan() || sr <= 0.0 {
            return false;
        }
        if sr < 1.0 && rand::random::<f64>() >= sr {
            return false;
        }
    }

    true
}

fn scope_matches_trace(cfg: &EvaluatorConfig, trace: &AgentTrace, explicit: bool) -> bool {
    if explicit {
        return true;
    }
    if let Some(ref agent) = cfg.agent_name {
        if !agent.is_empty() && trace.agent_name != *agent {
            return false;
        }
    }
    if let Some(ref env) = cfg.environment {
        if !env.is_empty() && trace.environment != *env {
            return false;
        }
    }
    true
}

fn is_trace_erased(tombstones: &[AgentErasureTombstone], trace: &AgentTrace) -> bool {
    tombstones.iter().any(|tb| tb.matches_trace(trace))
}

fn is_session_erased(
    tombstones: &[AgentErasureTombstone],
    project_id: &str,
    session_id: &str,
    traces: &[AgentTrace],
) -> bool {
    tombstones.iter().any(|tb| {
        (tb.project_id.is_empty() || tb.project_id == project_id)
            && ((tb.level == ErasureLevel::Session && tb.target_id == session_id)
                || traces.iter().any(|tr| tb.matches_trace(tr)))
    })
}

async fn load_byok_llm_cred(
    meta: &MetaStore,
    project_id: &str,
    vault_kek: &[u8; 32],
) -> Option<DecryptedLlmCred> {
    let rec = match meta.get_byok_credential(project_id, "llm").await {
        Ok(Some(r)) => r,
        Ok(None) => return None,
        Err(err) => {
            tracing::warn!(project_id, error = %err, "failed to load BYOK llm credential");
            return None;
        }
    };
    let api_key = match unseal_secret(vault_kek, &rec.sealed_secret_hex) {
        Ok(k) => k,
        Err(err) => {
            tracing::warn!(project_id, error = %err, "failed to unseal BYOK llm secret");
            return None;
        }
    };
    let cfg_json = match unseal_byok_config_json(vault_kek, &rec.config_json) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(project_id, error = %err, "failed to unseal BYOK llm config");
            return None;
        }
    };
    let (default_model, endpoint) = llm_model_and_endpoint(&rec.provider, &cfg_json);
    Some(DecryptedLlmCred {
        provider: rec.provider,
        api_key,
        default_model,
        endpoint,
    })
}

async fn execute_llm_task(
    task: &PreparedLlmTask,
    project_id: &str,
    cred: &DecryptedLlmCred,
    llm_sem: &Semaphore,
) -> Option<AgentEvaluation> {
    let model = task
        .cfg
        .config
        .get("model")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&cred.default_model);

    let started = Instant::now();
    let Ok(_permit) = llm_sem.acquire().await else {
        return None;
    };
    let llm_res = call_external_llm_with_system(
        &cred.provider,
        model,
        &cred.api_key,
        cred.endpoint.as_deref(),
        LLM_JUDGE_SYSTEM_PROMPT,
        &task.prompt,
        300,
    )
    .await;
    drop(_permit);

    let elapsed_ms = started.elapsed().as_millis() as i64;
    match llm_res {
        Ok(raw_text) => Some(parse_llm_judge_response_for_target(
            &task.cfg,
            project_id,
            task.target_type,
            &task.session_id,
            task.trace_id.clone(),
            task.span_id.clone(),
            &raw_text,
            elapsed_ms,
        )),
        Err(err) => {
            tracing::warn!(
                project_id,
                evaluator_id = %task.cfg.id,
                error = ?err,
                "llm_judge outbound call failed"
            );
            // Record the failure so the evaluator doesn't silently show no result. The raw error
            // stays in the log only: provider URLs can carry the API key (e.g. Gemini `?key=`).
            let mut ev = parse_llm_judge_response_for_target(
                &task.cfg,
                project_id,
                task.target_type,
                &task.session_id,
                task.trace_id.clone(),
                task.span_id.clone(),
                "",
                elapsed_ms,
            );
            ev.label = Some("judge_error".to_string());
            ev.reasoning = Some("The LLM judge call failed; see server logs.".to_string());
            ev.error = Some(format!("llm_judge call to {} failed", cred.provider));
            Some(ev)
        }
    }
}

async fn persist_evals(
    meta: &MetaStore,
    metrics: &AgentIngestMetrics,
    project_id: &str,
    trace_detail: &Option<TraceDetail>,
    session_traces: &[AgentTrace],
    evals: Vec<AgentEvaluation>,
) -> Result<(), String> {
    if evals.is_empty() {
        return Ok(());
    }

    // Re-check 3-level erasure tombstones immediately prior to DB persistence in case
    // an Admin DELETE arrived while evaluations (especially LLM judge HTTP calls) were running.
    let fresh_tombstones = meta
        .list_active_erasure_tombstones(project_id, Utc::now().timestamp_millis())
        .await
        .map_err(|e| e.to_string())?;

    for ev in evals {
        let erased_now = match ev.target_type {
            EvalTargetType::Trace | EvalTargetType::Span => {
                trace_detail
                    .as_ref()
                    .is_some_and(|d| is_trace_erased(&fresh_tombstones, &d.trace))
                    || ev.trace_id.as_deref().is_some_and(|tid| {
                        fresh_tombstones
                            .iter()
                            .any(|tb| tb.level == ErasureLevel::Trace && tb.target_id == tid)
                    })
                    || (!ev.session_id.is_empty()
                        && fresh_tombstones.iter().any(|tb| {
                            tb.level == ErasureLevel::Session && tb.target_id == ev.session_id
                        }))
            }
            EvalTargetType::Session => is_session_erased(
                &fresh_tombstones,
                project_id,
                &ev.session_id,
                session_traces,
            ),
        };
        if erased_now {
            continue;
        }
        meta.insert_agent_evaluation(&ev)
            .await
            .map_err(|e| e.to_string())?;
        if ev.error.is_none() {
            metrics.evals_executed.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

async fn process_eval_job(
    meta: &MetaStore,
    agent_store: &AgentTraceStore,
    vault_kek: [u8; 32],
    metrics: &AgentIngestMetrics,
    llm_sem: &Semaphore,
    job_sem: &Semaphore,
    job: EvalJob,
) -> Result<(), String> {
    if job.project_id.trim().is_empty() {
        return Ok(());
    }
    let explicit_mode = job.evaluator_ids.is_some();
    if let Some(ref ids) = job.evaluator_ids {
        if ids.is_empty() {
            return Ok(());
        }
    }

    // Phase 1: Bounded hydration + deterministic evaluation + LLM prompt preparation.
    // Release `job_sem` before waiting on `llm_sem` or outbound LLM HTTP calls so bursts
    // of >32 slow LLM jobs cannot Head-of-Line starve deterministic evaluation jobs.
    let job_permit = job_sem
        .acquire()
        .await
        .map_err(|_| "eval job semaphore closed".to_string())?;

    let now_ms = Utc::now().timestamp_millis();
    let tombstones = meta
        .list_active_erasure_tombstones(&job.project_id, now_ms)
        .await
        .map_err(|e| e.to_string())?;

    // Pre-hydration erasure check before loading configs or reading AgentTraceStore.
    if let Some(ref sid) = job.session_id {
        if !sid.is_empty()
            && tombstones
                .iter()
                .any(|tb| tb.level == ErasureLevel::Session && tb.target_id == *sid)
        {
            return Ok(());
        }
    }
    let trace_pre_erased = job.trace_id.as_deref().is_some_and(|tid| {
        !tid.is_empty()
            && tombstones
                .iter()
                .any(|tb| tb.level == ErasureLevel::Trace && tb.target_id == tid)
    });
    if trace_pre_erased && job.session_id.as_deref().unwrap_or("").is_empty() {
        return Ok(());
    }

    let all_configs = meta
        .list_evaluator_configs(&job.project_id)
        .await
        .map_err(|e| e.to_string())?;
    let matched: Vec<EvaluatorConfig> = all_configs
        .into_iter()
        .filter(|cfg| config_applies_to_job(cfg, &job))
        .collect();
    if matched.is_empty() {
        return Ok(());
    }

    let needs_session_lookup = job.session_id.as_deref().unwrap_or("").is_empty()
        && matched
            .iter()
            .any(|c| c.target_type == EvalTargetType::Session);
    let needs_trace_detail = !trace_pre_erased
        && job.trace_id.as_deref().is_some_and(|s| !s.is_empty())
        && (needs_session_lookup
            || matched
                .iter()
                .any(|c| matches!(c.target_type, EvalTargetType::Trace | EvalTargetType::Span)));

    let trace_detail: Option<TraceDetail> = if needs_trace_detail {
        let tid = job.trace_id.as_deref().unwrap_or_default();
        agent_store
            .get_trace_detail(&job.project_id, tid, &tombstones)
            .await
            .map_err(|e| e.to_string())?
    } else {
        None
    };

    let resolved_session_id: Option<String> = job
        .session_id
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            trace_detail
                .as_ref()
                .and_then(|d| d.trace.session_id.clone())
                .filter(|s| !s.is_empty())
        });

    let needs_session_detail = resolved_session_id.is_some()
        && matched
            .iter()
            .any(|c| c.target_type == EvalTargetType::Session);

    let mut session_traces: Vec<AgentTrace> = Vec::new();
    let mut session_spans: Vec<AgentSpan> = Vec::new();
    if needs_session_detail {
        if let Some(ref sid) = resolved_session_id {
            if !tombstones
                .iter()
                .any(|tb| tb.level == ErasureLevel::Session && tb.target_id == *sid)
            {
                if let Some(sess_detail) = agent_store
                    .get_session_detail(&job.project_id, sid, &tombstones)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    let mut seen_tids: HashSet<String> = HashSet::new();
                    let mut hydrated_map: HashMap<String, TraceDetail> = HashMap::new();
                    if let Some(ref td) = trace_detail {
                        hydrated_map.insert(td.trace.trace_id.clone(), td.clone());
                    }

                    for tr_summary in sess_detail
                        .traces
                        .iter()
                        .take(MAX_HYDRATED_TRACES_PER_SESSION)
                    {
                        if !seen_tids.insert(tr_summary.trace_id.clone()) {
                            continue;
                        }
                        if let Some(td) = hydrated_map.remove(&tr_summary.trace_id) {
                            session_traces.push(td.trace);
                            session_spans.extend(td.spans);
                        } else if let Some(td) = agent_store
                            .get_trace_detail(&job.project_id, &tr_summary.trace_id, &tombstones)
                            .await
                            .map_err(|e| e.to_string())?
                        {
                            session_traces.push(td.trace);
                            session_spans.extend(td.spans);
                        }
                    }
                }
            }
        }
    }

    let mut det_evals: Vec<AgentEvaluation> = Vec::new();
    let mut llm_tasks: Vec<PreparedLlmTask> = Vec::new();

    // 1. Trace-level and Span-level evaluations
    if let Some(ref detail) = trace_detail {
        if !is_trace_erased(&tombstones, &detail.trace) {
            let sess_str = detail.trace.session_id.as_deref().unwrap_or("");
            for cfg in &matched {
                if !scope_matches_trace(cfg, &detail.trace, explicit_mode) {
                    continue;
                }
                match cfg.target_type {
                    EvalTargetType::Trace => {
                        let target = EvalTarget::Trace(EvalContext {
                            trace: &detail.trace,
                            spans: &detail.spans,
                            expected_output: None,
                            expected_behavior: None,
                        });
                        if cfg.kind == "llm_judge" {
                            llm_tasks.push(PreparedLlmTask {
                                cfg: cfg.clone(),
                                target_type: EvalTargetType::Trace,
                                session_id: sess_str.to_string(),
                                trace_id: Some(detail.trace.trace_id.clone()),
                                span_id: None,
                                prompt: build_llm_judge_prompt_for_target(cfg, &target),
                            });
                        } else if let Some(ev) =
                            run_deterministic_evaluator_for_target(cfg, &target)
                        {
                            det_evals.push(ev);
                        }
                    }
                    EvalTargetType::Span => {
                        let spans_iter: Box<dyn Iterator<Item = &AgentSpan> + Send> =
                            if let Some(ref target_span_id) = job.span_id {
                                Box::new(
                                    detail
                                        .spans
                                        .iter()
                                        .filter(move |s| s.span_id == *target_span_id),
                                )
                            } else {
                                Box::new(detail.spans.iter())
                            };
                        for sp in spans_iter {
                            let target = EvalTarget::Span(SpanEvalContext {
                                span: sp,
                                trace: Some(&detail.trace),
                                expected_output: None,
                                expected_behavior: None,
                            });
                            if cfg.kind == "llm_judge" {
                                llm_tasks.push(PreparedLlmTask {
                                    cfg: cfg.clone(),
                                    target_type: EvalTargetType::Span,
                                    session_id: sess_str.to_string(),
                                    trace_id: Some(detail.trace.trace_id.clone()),
                                    span_id: Some(sp.span_id.clone()),
                                    prompt: build_llm_judge_prompt_for_target(cfg, &target),
                                });
                            } else if let Some(ev) =
                                run_deterministic_evaluator_for_target(cfg, &target)
                            {
                                det_evals.push(ev);
                            }
                        }
                    }
                    EvalTargetType::Session => {}
                }
            }
        }
    }

    // 2. Session-level evaluations
    if !session_traces.is_empty() {
        if let Some(ref sid) = resolved_session_id {
            if !is_session_erased(&tombstones, &job.project_id, sid, &session_traces) {
                let latest_trace = session_traces.iter().max_by_key(|t| t.start_ts_ms);
                for cfg in &matched {
                    if cfg.target_type != EvalTargetType::Session {
                        continue;
                    }
                    if let Some(tr) = latest_trace {
                        if !scope_matches_trace(cfg, tr, explicit_mode) {
                            continue;
                        }
                    }
                    let target = EvalTarget::Session(SessionEvalContext {
                        project_id: &job.project_id,
                        session_id: sid,
                        traces: &session_traces,
                        spans: &session_spans,
                        expected_output: None,
                        expected_behavior: None,
                    });
                    if cfg.kind == "llm_judge" {
                        llm_tasks.push(PreparedLlmTask {
                            cfg: cfg.clone(),
                            target_type: EvalTargetType::Session,
                            session_id: sid.clone(),
                            trace_id: None,
                            span_id: None,
                            prompt: build_llm_judge_prompt_for_target(cfg, &target),
                        });
                    } else if let Some(ev) = run_deterministic_evaluator_for_target(cfg, &target) {
                        det_evals.push(ev);
                    }
                }
            }
        }
    }

    let llm_cred = if llm_tasks.is_empty() {
        None
    } else {
        load_byok_llm_cred(meta, &job.project_id, &vault_kek).await
    };
    if !llm_tasks.is_empty() && llm_cred.is_none() {
        tracing::debug!(
            project_id = %job.project_id,
            "skipping llm_judge evaluators: no active BYOK llm credential"
        );
        llm_tasks.clear();
    }

    persist_evals(
        meta,
        metrics,
        &job.project_id,
        &trace_detail,
        &session_traces,
        det_evals,
    )
    .await?;

    // Release hydration/DB concurrency permit before waiting on `llm_sem` or outbound HTTP.
    drop(job_permit);

    let Some(cred) = llm_cred else {
        return Ok(());
    };
    if llm_tasks.is_empty() {
        return Ok(());
    }

    // Phase 2: Execute outbound LLM-as-judge calls under `llm_sem` without holding `job_sem`.
    let mut llm_evals: Vec<AgentEvaluation> = Vec::with_capacity(llm_tasks.len());
    for task in &llm_tasks {
        if let Some(ev) = execute_llm_task(task, &job.project_id, &cred, llm_sem).await {
            llm_evals.push(ev);
        }
    }

    // Phase 3: Re-acquire `job_sem` briefly to enforce fresh erasure check and persist LLM results.
    if !llm_evals.is_empty() {
        let _persist_permit = job_sem
            .acquire()
            .await
            .map_err(|_| "eval job semaphore closed".to_string())?;
        persist_evals(
            meta,
            metrics,
            &job.project_id,
            &trace_detail,
            &session_traces,
            llm_evals,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use axum::routing::post;
    use axum::{Json, Router};
    use object_store::memory::InMemory;
    use pulse_agent::{
        AgentTraceStoreConfig, EvaluatorType, SpanPhase, SpanType, TraceIngestBatch, TraceStatus,
    };
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    use crate::models::{
        seal_byok_config_json, seal_secret, ByokCredentialRecord, Organization, Project,
    };

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn make_trace(
        project_id: &str,
        trace_id: &str,
        session_id: Option<&str>,
        user_id: Option<&str>,
        status: TraceStatus,
    ) -> AgentTrace {
        AgentTrace {
            trace_id: trace_id.to_string(),
            project_id: project_id.to_string(),
            agent_name: "support-agent".to_string(),
            agent_version: "1.0.0".to_string(),
            prompt_version: None,
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".to_string(),
            session_id: session_id.map(str::to_string),
            user_id: user_id.map(str::to_string),
            workflow_id: None,
            parent_trace_id: None,
            status,
            start_ts_ms: 1_700_000_000_000,
            end_ts_ms: Some(1_700_000_000_120),
            duration_ms: 120,
            input: json!({"prompt": "How do I reset my password?"}),
            output: json!({"answer": "Click Forgot Password on the login screen."}),
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
            total_cost_nanos: 1_000_000,
            total_cost_usd: 0.001,
            primary_model: Some("gpt-4o-mini".to_string()),
            primary_provider: Some("openai".to_string()),
            avg_eval_score: None,
            eval_pass_rate: None,
            has_loop_warning: false,
        }
    }

    fn make_span(
        project_id: &str,
        trace_id: &str,
        span_id: &str,
        span_type: SpanType,
        status: TraceStatus,
        cost_usd: f64,
    ) -> AgentSpan {
        AgentSpan {
            span_id: span_id.to_string(),
            trace_id: trace_id.to_string(),
            project_id: project_id.to_string(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: format!("span_{span_id}"),
            span_type,
            status,
            start_ts_ms: 1_700_000_000_010,
            end_ts_ms: Some(1_700_000_000_110),
            duration_ms: 100,
            provider: Some("openai".to_string()),
            model: Some("gpt-4o-mini".to_string()),
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 50,
            output_tokens: 25,
            cached_tokens: 0,
            total_tokens: 75,
            cost_nanos: (cost_usd * 1_000_000_000.0) as i64,
            cost_usd,
            temperature: None,
            finish_reason: Some("stop".to_string()),
            model_params: Value::Null,
            tool_name: (span_type == SpanType::Tool).then(|| "search_kb".to_string()),
            tool_version: None,
            tool_retry_count: 0,
            input: json!({"q": "reset password"}),
            output: json!({"ok": true}),
            attributes: Value::Null,
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        }
    }

    fn make_evaluator(
        id: &str,
        project_id: &str,
        target_type: EvalTargetType,
        kind: &str,
        sample_rate: f64,
        config: Value,
    ) -> EvaluatorConfig {
        EvaluatorConfig {
            id: id.to_string(),
            project_id: project_id.to_string(),
            name: format!("Eval {id}"),
            description: String::new(),
            target_type,
            evaluator_type: if kind == "llm_judge" {
                EvaluatorType::LlmJudge
            } else {
                EvaluatorType::Deterministic
            },
            kind: kind.to_string(),
            version: "1.0.0".to_string(),
            enabled: true,
            agent_name: None,
            environment: None,
            sample_rate,
            config,
            created_at_ms: 1000,
            updated_at_ms: 1000,
        }
    }

    #[tokio::test]
    async fn deterministic_multi_level_evals_and_rollups() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );

        let tr1 = make_trace("p1", "tr_1", Some("sess_1"), Some("u1"), TraceStatus::Ok);
        let sp1 = make_span("p1", "tr_1", "sp_1", SpanType::Llm, TraceStatus::Ok, 0.002);
        let sp2 = make_span(
            "p1",
            "tr_1",
            "sp_2",
            SpanType::Tool,
            TraceStatus::Error,
            0.001,
        );
        let tr2 = make_trace("p1", "tr_2", Some("sess_1"), Some("u1"), TraceStatus::Ok);
        let sp3 = make_span("p1", "tr_2", "sp_3", SpanType::Llm, TraceStatus::Ok, 0.003);

        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr1, tr2],
                    spans: vec![sp1, sp2, sp3],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        meta.upsert_evaluator_config(&make_evaluator(
            "ev_trace",
            "p1",
            EvalTargetType::Trace,
            "no_errors",
            1.0,
            json!({}),
        ))
        .await
        .unwrap();
        meta.upsert_evaluator_config(&make_evaluator(
            "ev_span",
            "p1",
            EvalTargetType::Span,
            "cost_budget",
            1.0,
            json!({ "max_cost_usd": 0.01 }),
        ))
        .await
        .unwrap();
        meta.upsert_evaluator_config(&make_evaluator(
            "ev_session",
            "p1",
            EvalTargetType::Session,
            "no_loop",
            1.0,
            json!({}),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        let worker = tokio::spawn(run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            [7u8; 32],
            metrics.clone(),
            rx,
            shutdown_rx,
        ));

        tx.send(EvalJob {
            project_id: "p1".to_string(),
            session_id: Some("sess_1".to_string()),
            trace_id: Some("tr_1".to_string()),
            span_id: None,
            evaluator_ids: None,
        })
        .await
        .unwrap();

        // Wait until all 4 evaluations (1 Trace + 2 Spans on tr_1 + 1 Session on sess_1) complete.
        for _ in 0..50 {
            if metrics.evals_executed.load(Ordering::Relaxed) >= 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 4);

        let tr_evals = meta.list_trace_evaluations("p1", "tr_1").await.unwrap();
        assert_eq!(tr_evals.len(), 3); // 1 Trace eval + 2 Span evals
        let sess_evals = meta
            .list_project_evaluations_by_target("p1", Some(EvalTargetType::Session), 20)
            .await
            .unwrap();
        assert_eq!(sess_evals.len(), 1);
        assert_eq!(sess_evals[0].passed, Some(true));

        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert_eq!(
            rollups[&(EvalTargetType::Trace, "tr_1".to_string())].eval_count,
            1
        );
        assert_eq!(
            rollups[&(EvalTargetType::Span, "sp_1".to_string())].eval_count,
            1
        );
        assert_eq!(
            rollups[&(EvalTargetType::Span, "sp_2".to_string())].eval_count,
            1
        );
        assert_eq!(
            rollups[&(EvalTargetType::Session, "sess_1".to_string())].eval_count,
            1
        );

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn llm_judge_byok_semaphore_concurrency_and_ssrf_guard() {
        let _env_guard = ENV_LOCK.lock().await;

        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/v1/chat/completions",
            post({
                let in_flight = in_flight.clone();
                let max_seen = max_seen.clone();
                move |_body: Json<Value>| {
                    let in_flight = in_flight.clone();
                    let max_seen = max_seen.clone();
                    async move {
                        let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(cur, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        Json(json!({
                            "choices": [{
                                "message": {
                                    "content": "{\"score\": 0.9, \"passed\": true, \"reasoning\": \"accurate\"}"
                                }
                            }]
                        }))
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );
        let kek = [9u8; 32];

        // First confirm SSRF rejects metadata IP when private endpoints are NOT allowed.
        std::env::remove_var("PULSE_ALLOW_PRIVATE_ENDPOINTS");
        assert!(crate::copilot::pinned_outbound_client(
            "http://169.254.169.254/latest/meta-data",
            Duration::from_secs(2)
        )
        .await
        .is_err());

        // Now allow loopback for the local mock LLM server.
        std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1");

        meta.ensure_org(&Organization {
            id: "org_1".to_string(),
            name: "Org 1".to_string(),
            slug: "org-1".to_string(),
            created_at_ms: 1000,
        })
        .await
        .unwrap();
        meta.ensure_project(&Project {
            id: "p1".to_string(),
            org_id: "org_1".to_string(),
            name: "Proj 1".to_string(),
            timezone: "UTC".to_string(),
            created_at_ms: 1000,
        })
        .await
        .unwrap();

        let cfg_raw = json!({
            "endpoint": format!("http://{addr}/v1/chat/completions"),
            "model": "gpt-4o-mini"
        });
        meta.upsert_byok_credential(&ByokCredentialRecord {
            id: "byok_llm_1".to_string(),
            project_id: "p1".to_string(),
            category: "llm".to_string(),
            provider: "openai".to_string(),
            label: "Test OpenAI".to_string(),
            key_prefix_masked: "sk-test••••".to_string(),
            sealed_secret_hex: seal_secret(&kek, "sk-test-secret-12345"),
            config_json: seal_byok_config_json(&kek, &cfg_raw),
            status: "verified".to_string(),
            last_verified_at_ms: Some(1000),
            updated_at_ms: 1000,
        })
        .await
        .unwrap();

        let tr = make_trace("p1", "tr_llm", Some("sess_llm"), None, TraceStatus::Ok);
        let sp = make_span(
            "p1",
            "tr_llm",
            "sp_llm",
            SpanType::Llm,
            TraceStatus::Ok,
            0.001,
        );
        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![sp],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        meta.upsert_evaluator_config(&make_evaluator(
            "ev_llm",
            "p1",
            EvalTargetType::Trace,
            "llm_judge",
            1.0,
            json!({ "rubric": "Check clarity and correctness." }),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        let worker = tokio::spawn(run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            kek,
            metrics.clone(),
            rx,
            shutdown_rx,
        ));

        for _ in 0..20 {
            tx.send(EvalJob {
                project_id: "p1".to_string(),
                session_id: None,
                trace_id: Some("tr_llm".to_string()),
                span_id: None,
                evaluator_ids: Some(vec!["ev_llm".to_string()]),
            })
            .await
            .unwrap();
        }

        for _ in 0..100 {
            if metrics.evals_executed.load(Ordering::Relaxed) >= 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        srv.abort();
        std::env::remove_var("PULSE_ALLOW_PRIVATE_ENDPOINTS");

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 20);
        let peak = max_seen.load(Ordering::SeqCst);
        assert!(
            peak <= MAX_CONCURRENT_LLM_JUDGES,
            "exceeded MAX_CONCURRENT_LLM_JUDGES: {peak}"
        );
        assert!(peak > 1, "expected concurrent LLM judge calls, got {peak}");

        let evals = meta.list_trace_evaluations("p1", "tr_llm").await.unwrap();
        assert_eq!(evals.len(), 20);
        assert!((evals[0].score - 0.9).abs() < 1e-6);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn skips_erased_targets_at_all_three_levels() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );

        let tr_trace = make_trace(
            "p1",
            "tr_erased",
            Some("sess_ok1"),
            Some("u_ok"),
            TraceStatus::Ok,
        );
        let tr_sess = make_trace(
            "p1",
            "tr_in_erased_sess",
            Some("sess_erased"),
            Some("u_ok"),
            TraceStatus::Ok,
        );
        let tr_user = make_trace(
            "p1",
            "tr_by_erased_user",
            Some("sess_ok2"),
            Some("user_erased"),
            TraceStatus::Ok,
        );

        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr_trace, tr_sess, tr_user],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        for (id, level, target) in [
            ("tb_tr", ErasureLevel::Trace, "tr_erased"),
            ("tb_se", ErasureLevel::Session, "sess_erased"),
            ("tb_us", ErasureLevel::User, "user_erased"),
        ] {
            meta.insert_erasure_tombstone(&AgentErasureTombstone {
                id: id.to_string(),
                project_id: "p1".to_string(),
                level,
                target_id: target.to_string(),
                created_at: 100,
                expires_at: None,
            })
            .await
            .unwrap();
        }

        meta.upsert_evaluator_config(&make_evaluator(
            "ev_tr",
            "p1",
            EvalTargetType::Trace,
            "no_errors",
            1.0,
            json!({}),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        let worker = tokio::spawn(run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            [1u8; 32],
            metrics.clone(),
            rx,
            shutdown_rx,
        ));

        for (tid, sid) in [
            ("tr_erased", "sess_ok1"),
            ("tr_in_erased_sess", "sess_erased"),
            ("tr_by_erased_user", "sess_ok2"),
        ] {
            tx.send(EvalJob {
                project_id: "p1".to_string(),
                session_id: Some(sid.to_string()),
                trace_id: Some(tid.to_string()),
                span_id: None,
                evaluator_ids: None,
            })
            .await
            .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(120)).await;
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 0);
        assert!(meta
            .list_project_evaluations("p1", 50)
            .await
            .unwrap()
            .is_empty());

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn respects_sample_rate_zero_and_graceful_shutdown_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );

        let tr = make_trace("p1", "tr_drain", Some("sess_drain"), None, TraceStatus::Ok);
        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        // sample_rate = 0.0: skipped on automatic jobs, runs on explicit evaluator_ids
        meta.upsert_evaluator_config(&make_evaluator(
            "ev_zero",
            "p1",
            EvalTargetType::Trace,
            "no_errors",
            0.0,
            json!({}),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        // Pre-fill queue with 25 automatic jobs (skipped) + 25 explicit jobs (executed)
        // while keeping `tx` alive, then immediately trigger shutdown.
        for _ in 0..25 {
            tx.send(EvalJob {
                project_id: "p1".to_string(),
                session_id: None,
                trace_id: Some("tr_drain".to_string()),
                span_id: None,
                evaluator_ids: None,
            })
            .await
            .unwrap();
        }
        for _ in 0..25 {
            tx.send(EvalJob {
                project_id: "p1".to_string(),
                session_id: None,
                trace_id: Some("tr_drain".to_string()),
                span_id: None,
                evaluator_ids: Some(vec!["ev_zero".to_string()]),
            })
            .await
            .unwrap();
        }

        shutdown_tx.send(true).unwrap();
        run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            [3u8; 32],
            metrics.clone(),
            rx,
            shutdown_rx,
        )
        .await;

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 25);
        assert_eq!(
            meta.list_trace_evaluations("p1", "tr_drain")
                .await
                .unwrap()
                .len(),
            25
        );

        // Once worker closed rx, additional try_send fails immediately.
        assert!(tx
            .try_send(EvalJob {
                project_id: "p1".to_string(),
                session_id: None,
                trace_id: Some("tr_drain".to_string()),
                span_id: None,
                evaluator_ids: None,
            })
            .is_err());

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn session_only_evaluator_runs_on_trace_id_only_job() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );

        let tr = make_trace("p1", "tr_1", Some("sess_1"), Some("u1"), TraceStatus::Ok);
        let sp = make_span("p1", "tr_1", "sp_1", SpanType::Llm, TraceStatus::Ok, 0.002);
        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![sp],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        // Only a Session-level evaluator exists; EvalJob only supplies trace_id (session_id: None).
        meta.upsert_evaluator_config(&make_evaluator(
            "ev_sess_only",
            "p1",
            EvalTargetType::Session,
            "no_loop",
            1.0,
            json!({}),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        let worker = tokio::spawn(run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            [5u8; 32],
            metrics.clone(),
            rx,
            shutdown_rx,
        ));

        tx.send(EvalJob {
            project_id: "p1".to_string(),
            session_id: None,
            trace_id: Some("tr_1".to_string()),
            span_id: None,
            evaluator_ids: None,
        })
        .await
        .unwrap();

        for _ in 0..50 {
            if metrics.evals_executed.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 1);
        let sess_evals = meta
            .list_project_evaluations_by_target("p1", Some(EvalTargetType::Session), 20)
            .await
            .unwrap();
        assert_eq!(sess_evals.len(), 1);
        assert_eq!(sess_evals[0].session_id, "sess_1");
        assert_eq!(sess_evals[0].passed, Some(true));

        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        assert_eq!(
            rollups[&(EvalTargetType::Session, "sess_1".to_string())].eval_count,
            1
        );

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_judge_json_does_not_poison_rollups() {
        let _env_guard = ENV_LOCK.lock().await;

        let req_seq = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/v1/chat/completions",
            post({
                let req_seq = req_seq.clone();
                move |_body: Json<Value>| {
                    let req_seq = req_seq.clone();
                    async move {
                        let n = req_seq.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            Json(json!({
                                "choices": [{
                                    "message": {
                                        "content": "{\"score\": 0.9, \"passed\": true, \"reasoning\": \"solid\"}"
                                    }
                                }]
                            }))
                        } else {
                            Json(json!({
                                "choices": [{
                                    "message": {
                                        "content": "non-json"
                                    }
                                }]
                            }))
                        }
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1");

        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );
        let kek = [8u8; 32];

        meta.ensure_org(&Organization {
            id: "org_1".to_string(),
            name: "Org 1".to_string(),
            slug: "org-1".to_string(),
            created_at_ms: 1000,
        })
        .await
        .unwrap();
        meta.ensure_project(&Project {
            id: "p1".to_string(),
            org_id: "org_1".to_string(),
            name: "Proj 1".to_string(),
            timezone: "UTC".to_string(),
            created_at_ms: 1000,
        })
        .await
        .unwrap();

        let cfg_raw = json!({
            "endpoint": format!("http://{addr}/v1/chat/completions"),
            "model": "gpt-4o-mini"
        });
        meta.upsert_byok_credential(&ByokCredentialRecord {
            id: "byok_llm_bad_json".to_string(),
            project_id: "p1".to_string(),
            category: "llm".to_string(),
            provider: "openai".to_string(),
            label: "Test OpenAI".to_string(),
            key_prefix_masked: "sk-test••••".to_string(),
            sealed_secret_hex: seal_secret(&kek, "sk-test-secret-12345"),
            config_json: seal_byok_config_json(&kek, &cfg_raw),
            status: "verified".to_string(),
            last_verified_at_ms: Some(1000),
            updated_at_ms: 1000,
        })
        .await
        .unwrap();

        let tr = make_trace("p1", "tr_bad_json", Some("sess_bad"), None, TraceStatus::Ok);
        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        meta.upsert_evaluator_config(&make_evaluator(
            "ev_llm",
            "p1",
            EvalTargetType::Trace,
            "llm_judge",
            1.0,
            json!({ "rubric": "Score quality." }),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        let worker = tokio::spawn(run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            kek,
            metrics.clone(),
            rx,
            shutdown_rx,
        ));

        // First healthy 0.9 evaluation
        tx.send(EvalJob {
            project_id: "p1".to_string(),
            session_id: None,
            trace_id: Some("tr_bad_json".to_string()),
            span_id: None,
            evaluator_ids: Some(vec!["ev_llm".to_string()]),
        })
        .await
        .unwrap();
        for _ in 0..100 {
            if metrics.evals_executed.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 1);

        // Second HTTP 200 call with malformed non-json judge text
        tx.send(EvalJob {
            project_id: "p1".to_string(),
            session_id: None,
            trace_id: Some("tr_bad_json".to_string()),
            span_id: None,
            evaluator_ids: Some(vec!["ev_llm".to_string()]),
        })
        .await
        .unwrap();
        for _ in 0..100 {
            if meta
                .list_trace_evaluations("p1", "tr_bad_json")
                .await
                .unwrap()
                .len()
                >= 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        srv.abort();
        std::env::remove_var("PULSE_ALLOW_PRIVATE_ENDPOINTS");

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 1);
        let evals = meta
            .list_trace_evaluations("p1", "tr_bad_json")
            .await
            .unwrap();
        assert_eq!(evals.len(), 2);
        assert_eq!(evals.iter().filter(|e| e.error.is_some()).count(), 1);
        assert_eq!(evals.iter().filter(|e| e.error.is_none()).count(), 1);

        let rollups = meta.list_agent_eval_rollups("p1", None).await.unwrap();
        let tr_rollup = &rollups[&(EvalTargetType::Trace, "tr_bad_json".to_string())];
        assert!((tr_rollup.avg_score - 0.9).abs() < 1e-9);
        assert_eq!(tr_rollup.pass_rate, Some(1.0));
        assert_eq!(tr_rollup.eval_count, 1);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn deterministic_jobs_not_starved_by_burst_of_slow_llm_jobs() {
        let _env_guard = ENV_LOCK.lock().await;

        let llm_entered = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = watch::channel(false);
        let app = Router::new().route(
            "/v1/chat/completions",
            post({
                let llm_entered = llm_entered.clone();
                move |_body: Json<Value>| {
                    let llm_entered = llm_entered.clone();
                    let mut rx = release_rx.clone();
                    async move {
                        llm_entered.fetch_add(1, Ordering::SeqCst);
                        while !*rx.borrow() {
                            if rx.changed().await.is_err() {
                                break;
                            }
                        }
                        Json(json!({
                            "choices": [{
                                "message": {
                                    "content": "{\"score\": 0.85, \"passed\": true, \"reasoning\": \"ok\"}"
                                }
                            }]
                        }))
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        std::env::set_var("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1");

        let tmp = tempfile::tempdir().unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let store = Arc::new(
            AgentTraceStore::open(Arc::new(InMemory::new()), AgentTraceStoreConfig::default())
                .await
                .unwrap(),
        );
        let kek = [6u8; 32];

        meta.ensure_org(&Organization {
            id: "org_1".to_string(),
            name: "Org 1".to_string(),
            slug: "org-1".to_string(),
            created_at_ms: 1000,
        })
        .await
        .unwrap();
        meta.ensure_project(&Project {
            id: "p1".to_string(),
            org_id: "org_1".to_string(),
            name: "Proj 1".to_string(),
            timezone: "UTC".to_string(),
            created_at_ms: 1000,
        })
        .await
        .unwrap();

        let cfg_raw = json!({
            "endpoint": format!("http://{addr}/v1/chat/completions"),
            "model": "gpt-4o-mini"
        });
        meta.upsert_byok_credential(&ByokCredentialRecord {
            id: "byok_llm_burst".to_string(),
            project_id: "p1".to_string(),
            category: "llm".to_string(),
            provider: "openai".to_string(),
            label: "Test OpenAI".to_string(),
            key_prefix_masked: "sk-test••••".to_string(),
            sealed_secret_hex: seal_secret(&kek, "sk-test-secret-12345"),
            config_json: seal_byok_config_json(&kek, &cfg_raw),
            status: "verified".to_string(),
            last_verified_at_ms: Some(1000),
            updated_at_ms: 1000,
        })
        .await
        .unwrap();

        let tr_slow = make_trace("p1", "tr_slow", Some("sess_slow"), None, TraceStatus::Ok);
        let tr_fast = make_trace("p1", "tr_fast", Some("sess_fast"), None, TraceStatus::Ok);
        store
            .ingest_batch(
                "p1",
                TraceIngestBatch {
                    traces: vec![tr_slow, tr_fast],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        store.flush_all_l0().await.unwrap();

        meta.upsert_evaluator_config(&make_evaluator(
            "ev_llm",
            "p1",
            EvalTargetType::Trace,
            "llm_judge",
            1.0,
            json!({ "rubric": "Check quality." }),
        ))
        .await
        .unwrap();
        meta.upsert_evaluator_config(&make_evaluator(
            "ev_det",
            "p1",
            EvalTargetType::Trace,
            "no_errors",
            1.0,
            json!({}),
        ))
        .await
        .unwrap();

        let (tx, rx) = mpsc::channel(EVAL_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = Arc::new(AgentIngestMetrics::default());

        let worker = tokio::spawn(run_agent_eval_worker(
            meta.clone(),
            store.clone(),
            kek,
            metrics.clone(),
            rx,
            shutdown_rx,
        ));

        // Burst 40 (> MAX_CONCURRENT_EVAL_JOBS = 32) slow LLM jobs that block in the mock server.
        for _ in 0..40 {
            tx.send(EvalJob {
                project_id: "p1".to_string(),
                session_id: None,
                trace_id: Some("tr_slow".to_string()),
                span_id: None,
                evaluator_ids: Some(vec!["ev_llm".to_string()]),
            })
            .await
            .unwrap();
        }

        // Wait until all 8 MAX_CONCURRENT_LLM_JUDGES slots are occupied and blocked.
        for _ in 0..100 {
            if llm_entered.load(Ordering::SeqCst) >= MAX_CONCURRENT_LLM_JUDGES {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            llm_entered.load(Ordering::SeqCst),
            MAX_CONCURRENT_LLM_JUDGES
        );

        // Now enqueue a deterministic-only job AFTER the 40 blocked LLM jobs.
        tx.send(EvalJob {
            project_id: "p1".to_string(),
            session_id: None,
            trace_id: Some("tr_fast".to_string()),
            span_id: None,
            evaluator_ids: Some(vec!["ev_det".to_string()]),
        })
        .await
        .unwrap();

        // Deterministic job MUST finish while all 40 slow LLM jobs are still blocked (0 LLM evals persisted).
        let mut fast_done = false;
        for _ in 0..100 {
            if !meta
                .list_trace_evaluations("p1", "tr_fast")
                .await
                .unwrap()
                .is_empty()
            {
                fast_done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            fast_done,
            "deterministic job was Head-of-Line starved behind >32 blocked LLM jobs"
        );
        assert!(
            meta.list_trace_evaluations("p1", "tr_slow")
                .await
                .unwrap()
                .is_empty(),
            "expected deterministic job to complete before any blocked LLM job finished"
        );

        // Release the blocked LLM calls and verify all 40 + 1 evaluations drain cleanly.
        release_tx.send(true).unwrap();
        for _ in 0..200 {
            if metrics.evals_executed.load(Ordering::Relaxed) >= 41 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap();
        srv.abort();
        std::env::remove_var("PULSE_ALLOW_PRIVATE_ENDPOINTS");

        assert_eq!(metrics.evals_executed.load(Ordering::Relaxed), 41);
        assert_eq!(
            meta.list_trace_evaluations("p1", "tr_slow")
                .await
                .unwrap()
                .len(),
            40
        );
        assert_eq!(
            meta.list_trace_evaluations("p1", "tr_fast")
                .await
                .unwrap()
                .len(),
            1
        );

        store.close().await.unwrap();
    }
}
