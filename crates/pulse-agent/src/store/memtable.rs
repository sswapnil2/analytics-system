use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate};
use parking_lot::RwLock;
use serde_json::Value;
use xxhash_rust::xxh3::xxh3_64;

use crate::model::{
    AgentErasureTombstone, AgentSpan, AgentTrace, ErasureLevel, FailureSource, SpanEvent, SpanType,
    TraceIngestBatch, TraceStatus,
};
use crate::pricing::{cost_nanos_to_usd, sync_span_cost, sync_trace_cost};
use crate::stats::{classify_failure_source, detect_execution_loop, sum_cost_nanos};
use crate::store::error::StoreResult;
use crate::MAX_TAGS_PER_TRACE;

pub const DEFAULT_AGENT_SHARDS: usize = 8;
pub const MAX_AGENT_SHARDS: usize = 64;

#[inline]
pub fn shard_id_for_trace(trace_id: &str, shard_count: u16) -> u16 {
    let sc = (shard_count as u64).max(1);
    (xxh3_64(trace_id.as_bytes()) % sc) as u16
}

#[inline]
pub fn trace_date_from_ts(start_ts_ms: i64) -> NaiveDate {
    DateTime::from_timestamp_millis(start_ts_ms)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
}

/// Deterministic CRDT status precedence (`Running < Ok < Cancelled < Timeout < Error`).
/// Do NOT compare `TraceStatus::to_u8()` directly because `Ok = 0` and `Running = 1`.
#[inline]
pub(crate) fn status_rank(status: TraceStatus) -> u8 {
    match status {
        TraceStatus::Running => 0,
        TraceStatus::Ok => 1,
        TraceStatus::Cancelled => 2,
        TraceStatus::Timeout => 3,
        TraceStatus::Error => 4,
    }
}

#[inline]
fn agent_name_rank(s: &str) -> u8 {
    if s.is_empty() {
        0
    } else if s == "unknown" {
        1
    } else {
        2
    }
}

#[inline]
fn agent_version_rank(s: &str) -> u8 {
    if s.is_empty() {
        0
    } else if s == "unversioned" {
        1
    } else {
        2
    }
}

#[inline]
pub(crate) fn env_rank(s: &str) -> u8 {
    if s.is_empty() {
        0
    } else if s == "production" {
        1
    } else {
        2
    }
}

#[inline]
pub(crate) fn failure_source_rank(fs: Option<FailureSource>) -> u8 {
    match fs {
        None => 0,
        Some(FailureSource::Unknown) => 1,
        Some(FailureSource::Application) => 2,
        Some(FailureSource::Prompt) => 3,
        Some(FailureSource::External) => 4,
        Some(FailureSource::Tool) => 5,
        Some(FailureSource::Model) => 6,
    }
}

fn pick_ranked_str(dst: &mut String, src: String, rank_fn: fn(&str) -> u8) {
    if (rank_fn(&src), src.as_str()) > (rank_fn(dst), dst.as_str()) {
        *dst = src;
    }
}

pub(crate) fn merge_opt_str_max(dst: &mut Option<String>, src: Option<String>) {
    if dst.as_ref().is_some_and(|d| d.is_empty()) {
        *dst = None;
    }
    if let Some(s) = src.filter(|v| !v.is_empty()) {
        if dst.as_ref().is_none_or(|d| s.as_str() > d.as_str()) {
            *dst = Some(s);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct TraceFieldRanks {
    pub output_rank: u8,
    pub metadata_rank: u8,
    pub error_type_rank: u8,
    pub error_message_rank: u8,
}

impl TraceFieldRanks {
    pub(crate) fn from_trace(tr: &AgentTrace) -> Self {
        let r = status_rank(tr.status);
        Self {
            output_rank: if tr.output.is_null() { 0 } else { r },
            metadata_rank: if tr.metadata.is_null() { 0 } else { r },
            error_type_rank: if tr.error_type.as_deref().is_some_and(|s| !s.is_empty()) {
                r
            } else {
                0
            },
            error_message_rank: if tr.error_message.as_deref().is_some_and(|s| !s.is_empty()) {
                r
            } else {
                0
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceCrdtMeta {
    pub field_ranks: TraceFieldRanks,
    pub explicit_status: TraceStatus,
    pub explicit_failure_source: Option<FailureSource>,
    pub explicit_has_loop_warning: bool,
    pub explicit_primary_model: Option<String>,
    pub explicit_primary_provider: Option<String>,
}

pub(crate) type RankedSpan = (AgentSpan, SpanFieldRanks);
pub(crate) type RankedTraceEntry = (AgentTrace, TraceCrdtMeta, Vec<RankedSpan>);

impl Default for TraceCrdtMeta {
    fn default() -> Self {
        Self {
            field_ranks: TraceFieldRanks::default(),
            explicit_status: TraceStatus::Running,
            explicit_failure_source: None,
            explicit_has_loop_warning: false,
            explicit_primary_model: None,
            explicit_primary_provider: None,
        }
    }
}

impl TraceCrdtMeta {
    pub(crate) fn from_trace(tr: &AgentTrace) -> Self {
        Self {
            field_ranks: TraceFieldRanks::from_trace(tr),
            explicit_status: tr.status,
            explicit_failure_source: tr.failure_source,
            explicit_has_loop_warning: tr.has_loop_warning,
            explicit_primary_model: tr.primary_model.clone().filter(|s| !s.is_empty()),
            explicit_primary_provider: tr.primary_provider.clone().filter(|s| !s.is_empty()),
        }
    }
}

fn merge_opt_str_ranked_with_rank(
    dst: &mut Option<String>,
    dst_rank: &mut u8,
    src: Option<String>,
    src_rank: u8,
) {
    if dst.as_ref().is_some_and(|d| d.is_empty()) {
        *dst = None;
        *dst_rank = 0;
    }
    if let Some(s) = src.filter(|v| !v.is_empty()) {
        let take_src = match dst.as_deref() {
            None => true,
            Some(d) => src_rank > *dst_rank || (src_rank == *dst_rank && s.as_str() > d),
        };
        if take_src {
            *dst = Some(s);
            *dst_rank = src_rank;
        }
    }
}

fn json_type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

pub(crate) fn cmp_json_value(a: &Value, b: &Value) -> std::cmp::Ordering {
    let ra = json_type_rank(a);
    let rb = json_type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => x.to_string().cmp(&y.to_string()),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let o = cmp_json_value(xi, yi);
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            let mut xk: Vec<(&String, &Value)> = x.iter().collect();
            let mut yk: Vec<(&String, &Value)> = y.iter().collect();
            xk.sort_by(|a, b| a.0.cmp(b.0));
            yk.sort_by(|a, b| a.0.cmp(b.0));
            for ((ak, av), (bk, bv)) in xk.iter().zip(yk.iter()) {
                let o = ak.cmp(bk).then_with(|| cmp_json_value(av, bv));
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            xk.len().cmp(&yk.len())
        }
        _ => std::cmp::Ordering::Equal,
    }
}

fn merge_json_val_ranked_with_rank(dst: &mut Value, dst_rank: &mut u8, src: Value, src_rank: u8) {
    if src.is_null() {
        return;
    }
    if dst.is_null()
        || src_rank > *dst_rank
        || (src_rank == *dst_rank && cmp_json_value(&src, dst) == std::cmp::Ordering::Greater)
    {
        *dst = src;
        *dst_rank = src_rank;
    }
}

fn merge_json_val_max(dst: &mut Value, src: Value) {
    if src.is_null() {
        return;
    }
    if dst.is_null() || cmp_json_value(&src, dst) == std::cmp::Ordering::Greater {
        *dst = src;
    }
}

fn merge_opt_f64(dst: &mut Option<f64>, src: Option<f64>) {
    *dst = match (*dst, src) {
        (Some(a), Some(b)) if a.is_finite() && b.is_finite() => {
            Some(if a.total_cmp(&b).is_ge() { a } else { b })
        }
        (Some(a), _) if a.is_finite() => Some(a),
        (_, Some(b)) if b.is_finite() => Some(b),
        _ => None,
    };
}

pub(crate) fn merge_trace_headers_ranked(
    dst: &mut AgentTrace,
    dst_ranks: &mut TraceFieldRanks,
    mut src: AgentTrace,
    src_ranks: TraceFieldRanks,
) {
    sync_trace_cost(&mut src);
    sync_trace_cost(dst);
    if dst.project_id.is_empty() && !src.project_id.is_empty() {
        dst.project_id = src.project_id;
    }

    let dst_rank = status_rank(dst.status);
    let src_rank = status_rank(src.status);
    if src_rank > dst_rank {
        dst.status = src.status;
    }

    match (dst.start_ts_ms > 0, src.start_ts_ms > 0) {
        (false, true) => dst.start_ts_ms = src.start_ts_ms,
        (true, true) => dst.start_ts_ms = dst.start_ts_ms.min(src.start_ts_ms),
        (false, false) => dst.start_ts_ms = dst.start_ts_ms.max(src.start_ts_ms),
        (true, false) => {}
    }
    match (dst.end_ts_ms, src.end_ts_ms) {
        (None, Some(e)) => dst.end_ts_ms = Some(e),
        (Some(a), Some(b)) => dst.end_ts_ms = Some(a.max(b)),
        _ => {}
    }
    dst.duration_ms = dst.duration_ms.max(src.duration_ms).max(0);
    if let Some(end) = dst.end_ts_ms {
        if dst.start_ts_ms > 0 {
            dst.duration_ms = dst
                .duration_ms
                .max(end.saturating_sub(dst.start_ts_ms).max(0));
        }
    }

    pick_ranked_str(&mut dst.agent_name, src.agent_name, agent_name_rank);
    pick_ranked_str(
        &mut dst.agent_version,
        src.agent_version,
        agent_version_rank,
    );
    pick_ranked_str(&mut dst.environment, src.environment, env_rank);

    merge_opt_str_max(&mut dst.session_id, src.session_id);
    merge_opt_str_max(&mut dst.user_id, src.user_id);
    merge_opt_str_max(&mut dst.workflow_id, src.workflow_id);
    merge_opt_str_max(&mut dst.parent_trace_id, src.parent_trace_id);

    merge_opt_str_max(&mut dst.prompt_version, src.prompt_version);
    merge_opt_str_max(&mut dst.model_config_version, src.model_config_version);
    merge_opt_str_max(&mut dst.tool_config_version, src.tool_config_version);
    merge_opt_str_max(&mut dst.app_release, src.app_release);
    merge_opt_str_max(&mut dst.primary_model, src.primary_model);
    merge_opt_str_max(&mut dst.primary_provider, src.primary_provider);

    merge_opt_str_ranked_with_rank(
        &mut dst.error_type,
        &mut dst_ranks.error_type_rank,
        src.error_type,
        src_ranks.error_type_rank,
    );
    merge_opt_str_ranked_with_rank(
        &mut dst.error_message,
        &mut dst_ranks.error_message_rank,
        src.error_message,
        src_ranks.error_message_rank,
    );

    if failure_source_rank(src.failure_source) > failure_source_rank(dst.failure_source) {
        dst.failure_source = src.failure_source;
    }

    let mut merged_tags: BTreeSet<String> = dst.tags.drain(..).filter(|t| !t.is_empty()).collect();
    merged_tags.extend(src.tags.into_iter().filter(|t| !t.is_empty()));
    dst.tags = merged_tags.into_iter().take(MAX_TAGS_PER_TRACE).collect();

    merge_json_val_max(&mut dst.input, src.input);
    merge_json_val_ranked_with_rank(
        &mut dst.output,
        &mut dst_ranks.output_rank,
        src.output,
        src_ranks.output_rank,
    );
    merge_json_val_ranked_with_rank(
        &mut dst.metadata,
        &mut dst_ranks.metadata_rank,
        src.metadata,
        src_ranks.metadata_rank,
    );

    dst.span_count = dst.span_count.max(src.span_count);
    dst.llm_call_count = dst.llm_call_count.max(src.llm_call_count);
    dst.tool_call_count = dst.tool_call_count.max(src.tool_call_count);
    dst.error_span_count = dst.error_span_count.max(src.error_span_count);
    dst.input_tokens = dst.input_tokens.max(src.input_tokens).max(0);
    dst.output_tokens = dst.output_tokens.max(src.output_tokens).max(0);
    dst.cached_tokens = dst.cached_tokens.max(src.cached_tokens).max(0);
    dst.total_tokens = dst.total_tokens.max(src.total_tokens).max(0);
    dst.total_cost_nanos = dst.total_cost_nanos.max(src.total_cost_nanos).max(0);
    dst.has_loop_warning |= src.has_loop_warning;
    merge_opt_f64(&mut dst.avg_eval_score, src.avg_eval_score);
    merge_opt_f64(&mut dst.eval_pass_rate, src.eval_pass_rate);
    sync_trace_cost(dst);
}

pub(crate) fn merge_trace_headers_with_meta(
    dst: &mut AgentTrace,
    dst_meta: &mut TraceCrdtMeta,
    mut src: AgentTrace,
    src_meta: TraceCrdtMeta,
) {
    if status_rank(src_meta.explicit_status) > status_rank(dst_meta.explicit_status) {
        dst_meta.explicit_status = src_meta.explicit_status;
    }
    if failure_source_rank(src_meta.explicit_failure_source)
        > failure_source_rank(dst_meta.explicit_failure_source)
    {
        dst_meta.explicit_failure_source = src_meta.explicit_failure_source;
    }
    dst_meta.explicit_has_loop_warning |= src_meta.explicit_has_loop_warning;
    merge_opt_str_max(
        &mut dst_meta.explicit_primary_model,
        src_meta.explicit_primary_model,
    );
    merge_opt_str_max(
        &mut dst_meta.explicit_primary_provider,
        src_meta.explicit_primary_provider,
    );
    dst.primary_model = dst_meta.explicit_primary_model.clone();
    dst.primary_provider = dst_meta.explicit_primary_provider.clone();
    src.primary_model = None;
    src.primary_provider = None;
    merge_trace_headers_ranked(dst, &mut dst_meta.field_ranks, src, src_meta.field_ranks);
}

#[allow(dead_code)]
pub(crate) fn merge_trace_headers(dst: &mut AgentTrace, src: AgentTrace) {
    let mut dst_ranks = TraceFieldRanks::from_trace(dst);
    let src_ranks = TraceFieldRanks::from_trace(&src);
    merge_trace_headers_ranked(dst, &mut dst_ranks, src, src_ranks);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SpanFieldRanks {
    pub output_rank: Option<(u8, i64)>,
    pub attributes_rank: Option<(u8, i64)>,
    pub metadata_rank: Option<(u8, i64)>,
    pub model_params_rank: Option<(u8, i64)>,
    pub finish_reason_rank: Option<(u8, i64)>,
    pub error_type_rank: Option<(u8, i64)>,
    pub error_message_rank: Option<(u8, i64)>,
    pub error_stack_rank: Option<(u8, i64)>,
    pub tool_name_rank: Option<(u8, i64)>,
}

impl SpanFieldRanks {
    pub(crate) fn from_span(sp: &AgentSpan) -> Self {
        let r = (sp.span_phase.priority(), sp.start_ts_ms);
        let opt_r = |o: &Option<String>| -> Option<(u8, i64)> {
            o.as_deref().is_some_and(|s| !s.is_empty()).then_some(r)
        };
        let val_r = |v: &Value| -> Option<(u8, i64)> { (!v.is_null()).then_some(r) };
        Self {
            output_rank: val_r(&sp.output),
            attributes_rank: val_r(&sp.attributes),
            metadata_rank: val_r(&sp.metadata),
            model_params_rank: val_r(&sp.model_params),
            finish_reason_rank: opt_r(&sp.finish_reason),
            error_type_rank: opt_r(&sp.error_type),
            error_message_rank: opt_r(&sp.error_message),
            error_stack_rank: opt_r(&sp.error_stack),
            tool_name_rank: sp.tool_name.as_ref().map(|_| r),
        }
    }
}

pub(crate) fn merge_opt_str_phase_ranked_preserve_empty(
    dst: &mut Option<String>,
    dst_rank: &mut Option<(u8, i64)>,
    src: Option<String>,
    src_rank: Option<(u8, i64)>,
) {
    if let Some(s) = src {
        let take_src = match dst.as_deref() {
            None => true,
            Some(d) => src_rank > *dst_rank || (src_rank == *dst_rank && s.as_str() > d),
        };
        if take_src {
            *dst = Some(s);
            *dst_rank = src_rank;
        }
    }
}

fn merge_opt_str_phase_ranked(
    dst: &mut Option<String>,
    dst_rank: &mut Option<(u8, i64)>,
    src: Option<String>,
    src_rank: Option<(u8, i64)>,
) {
    if dst.as_ref().is_some_and(|d| d.is_empty()) {
        *dst = None;
        *dst_rank = None;
    }
    if let Some(s) = src.filter(|v| !v.is_empty()) {
        let take_src = match dst.as_deref() {
            None => true,
            Some(d) => src_rank > *dst_rank || (src_rank == *dst_rank && s.as_str() > d),
        };
        if take_src {
            *dst = Some(s);
            *dst_rank = src_rank;
        }
    }
}

fn merge_json_val_phase_ranked(
    dst: &mut Value,
    dst_rank: &mut Option<(u8, i64)>,
    src: Value,
    src_rank: Option<(u8, i64)>,
) {
    if src.is_null() {
        return;
    }
    if dst.is_null()
        || src_rank > *dst_rank
        || (src_rank == *dst_rank && cmp_json_value(&src, dst) == std::cmp::Ordering::Greater)
    {
        *dst = src;
        *dst_rank = src_rank;
    }
}

fn union_linked_span_ids(dst: &mut Vec<String>, src: &[String]) {
    if src.is_empty() && dst.is_empty() {
        return;
    }
    let mut set: BTreeSet<String> = dst.drain(..).filter(|s| !s.is_empty()).collect();
    set.extend(src.iter().filter(|s| !s.is_empty()).cloned());
    *dst = set.into_iter().collect();
}

fn union_span_events(dst: &mut Vec<SpanEvent>, src: &[SpanEvent]) {
    if src.is_empty() && dst.is_empty() {
        return;
    }
    let mut all: Vec<SpanEvent> = Vec::with_capacity(dst.len() + src.len());
    all.append(dst);
    all.extend(src.iter().cloned());
    all.sort_by(|a, b| {
        a.timestamp_ms
            .cmp(&b.timestamp_ms)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| cmp_json_value(&a.attributes, &b.attributes))
    });
    all.dedup();
    *dst = all;
}

#[inline]
fn span_type_rank(st: SpanType) -> u8 {
    match st {
        SpanType::Custom => 0,
        other => other.to_u8().saturating_add(1),
    }
}

fn merge_span_pair(
    dst: &mut AgentSpan,
    dst_ranks: &mut SpanFieldRanks,
    mut src: AgentSpan,
    src_ranks: SpanFieldRanks,
) {
    sync_span_cost(&mut src);
    sync_span_cost(dst);

    let new_rank = (src.span_phase.priority(), src.start_ts_ms);
    let old_rank = (dst.span_phase.priority(), dst.start_ts_ms);

    // Phase-independent static fields & collections (merged commutatively & associatively)
    let merge_static_and_ranked = |target: &mut AgentSpan,
                                   target_ranks: &mut SpanFieldRanks,
                                   other: &mut AgentSpan,
                                   other_ranks: SpanFieldRanks| {
        merge_opt_str_max(&mut target.parent_span_id, other.parent_span_id.take());
        merge_opt_str_max(&mut target.provider, other.provider.take());
        merge_opt_str_max(&mut target.model, other.model.take());
        merge_opt_str_max(&mut target.model_id, other.model_id.take());
        merge_opt_str_max(&mut target.model_version, other.model_version.take());
        merge_opt_str_max(
            &mut target.prompt_template_id,
            other.prompt_template_id.take(),
        );
        merge_opt_str_max(
            &mut target.prompt_template_version,
            other.prompt_template_version.take(),
        );
        merge_opt_f64(&mut target.temperature, other.temperature);
        merge_opt_str_phase_ranked_preserve_empty(
            &mut target.tool_name,
            &mut target_ranks.tool_name_rank,
            other.tool_name.take(),
            other_ranks.tool_name_rank,
        );
        merge_opt_str_max(&mut target.tool_version, other.tool_version.take());
        merge_json_val_max(&mut target.input, std::mem::take(&mut other.input));
        union_linked_span_ids(&mut target.linked_span_ids, &other.linked_span_ids);
        union_span_events(&mut target.events, &other.events);

        merge_json_val_phase_ranked(
            &mut target.output,
            &mut target_ranks.output_rank,
            std::mem::take(&mut other.output),
            other_ranks.output_rank,
        );
        merge_json_val_phase_ranked(
            &mut target.attributes,
            &mut target_ranks.attributes_rank,
            std::mem::take(&mut other.attributes),
            other_ranks.attributes_rank,
        );
        merge_json_val_phase_ranked(
            &mut target.metadata,
            &mut target_ranks.metadata_rank,
            std::mem::take(&mut other.metadata),
            other_ranks.metadata_rank,
        );
        merge_json_val_phase_ranked(
            &mut target.model_params,
            &mut target_ranks.model_params_rank,
            std::mem::take(&mut other.model_params),
            other_ranks.model_params_rank,
        );
        merge_opt_str_phase_ranked(
            &mut target.finish_reason,
            &mut target_ranks.finish_reason_rank,
            other.finish_reason.take(),
            other_ranks.finish_reason_rank,
        );
        merge_opt_str_phase_ranked(
            &mut target.error_type,
            &mut target_ranks.error_type_rank,
            other.error_type.take(),
            other_ranks.error_type_rank,
        );
        merge_opt_str_phase_ranked(
            &mut target.error_message,
            &mut target_ranks.error_message_rank,
            other.error_message.take(),
            other_ranks.error_message_rank,
        );
        merge_opt_str_phase_ranked(
            &mut target.error_stack,
            &mut target_ranks.error_stack_rank,
            other.error_stack.take(),
            other_ranks.error_stack_rank,
        );
    };

    if new_rank > old_rank {
        if src.project_id.is_empty() && !dst.project_id.is_empty() {
            src.project_id = std::mem::take(&mut dst.project_id);
        }
        if status_rank(dst.status) > status_rank(src.status) && src.span_phase == dst.span_phase {
            src.status = dst.status;
        }
        merge_static_and_ranked(dst, dst_ranks, &mut src, src_ranks);
        // Copy src's winning envelope/metric fields while keeping dst's merged static/ranked fields
        src.parent_span_id = dst.parent_span_id.take();
        src.provider = dst.provider.take();
        src.model = dst.model.take();
        src.model_id = dst.model_id.take();
        src.model_version = dst.model_version.take();
        src.prompt_template_id = dst.prompt_template_id.take();
        src.prompt_template_version = dst.prompt_template_version.take();
        src.temperature = dst.temperature;
        src.tool_name = dst.tool_name.take();
        src.tool_version = dst.tool_version.take();
        src.input = std::mem::take(&mut dst.input);
        src.linked_span_ids = std::mem::take(&mut dst.linked_span_ids);
        src.events = std::mem::take(&mut dst.events);
        src.output = std::mem::take(&mut dst.output);
        src.attributes = std::mem::take(&mut dst.attributes);
        src.metadata = std::mem::take(&mut dst.metadata);
        src.model_params = std::mem::take(&mut dst.model_params);
        src.finish_reason = dst.finish_reason.take();
        src.error_type = dst.error_type.take();
        src.error_message = dst.error_message.take();
        src.error_stack = dst.error_stack.take();
        *dst = src;
    } else if new_rank < old_rank {
        if dst.project_id.is_empty() && !src.project_id.is_empty() {
            dst.project_id = std::mem::take(&mut src.project_id);
        }
        if status_rank(src.status) > status_rank(dst.status) && src.span_phase == dst.span_phase {
            dst.status = src.status;
        }
        merge_static_and_ranked(dst, dst_ranks, &mut src, src_ranks);
    } else {
        if dst.project_id.is_empty() && !src.project_id.is_empty() {
            dst.project_id = std::mem::take(&mut src.project_id);
        }
        if status_rank(src.status) > status_rank(dst.status) {
            dst.status = src.status;
        }
        match (dst.end_ts_ms, src.end_ts_ms) {
            (None, Some(e)) => dst.end_ts_ms = Some(e),
            (Some(a), Some(b)) => dst.end_ts_ms = Some(a.max(b)),
            _ => {}
        }
        dst.duration_ms = dst.duration_ms.max(src.duration_ms).max(0);
        dst.depth = dst.depth.max(src.depth);
        if span_type_rank(src.span_type) > span_type_rank(dst.span_type) {
            dst.span_type = src.span_type;
        }
        if dst.name.is_empty() || (!src.name.is_empty() && src.name > dst.name) {
            dst.name = std::mem::take(&mut src.name);
        }

        dst.input_tokens = dst.input_tokens.max(src.input_tokens).max(0);
        dst.output_tokens = dst.output_tokens.max(src.output_tokens).max(0);
        dst.cached_tokens = dst.cached_tokens.max(src.cached_tokens).max(0);
        dst.total_tokens = dst.total_tokens.max(src.total_tokens).max(0);
        dst.cost_nanos = dst.cost_nanos.max(src.cost_nanos).max(0);
        dst.tool_retry_count = dst.tool_retry_count.max(src.tool_retry_count).max(0);
        sync_span_cost(dst);

        merge_static_and_ranked(dst, dst_ranks, &mut src, src_ranks);
    }
}

pub(crate) fn merge_spans_crdt_with_ranks(
    map: &mut HashMap<String, (AgentSpan, SpanFieldRanks)>,
    spans: impl IntoIterator<Item = (AgentSpan, SpanFieldRanks)>,
) {
    for (mut sp, sp_ranks) in spans {
        sync_span_cost(&mut sp);
        if let Some((existing, ex_ranks)) = map.get_mut(&sp.span_id) {
            merge_span_pair(existing, ex_ranks, sp, sp_ranks);
        } else {
            map.insert(sp.span_id.clone(), (sp, sp_ranks));
        }
    }
}

pub(crate) fn merge_spans_crdt_ranked(
    map: &mut HashMap<String, (AgentSpan, SpanFieldRanks)>,
    spans: impl IntoIterator<Item = AgentSpan>,
) {
    merge_spans_crdt_with_ranks(
        map,
        spans.into_iter().map(|sp| {
            let ranks = SpanFieldRanks::from_span(&sp);
            (sp, ranks)
        }),
    );
}

/// CRDT span merge keyed by `span_id`, keeping highest `(span_phase.priority(), start_ts_ms)`
/// (`Final > Update > Start`) while preserving non-null payloads/metadata commutatively
/// regardless of phase arrival order.
#[allow(dead_code)]
pub(crate) fn merge_spans_crdt(
    map: &mut HashMap<String, AgentSpan>,
    spans: impl IntoIterator<Item = AgentSpan>,
) {
    let mut ranked: HashMap<String, (AgentSpan, SpanFieldRanks)> = map
        .drain()
        .map(|(k, v)| {
            let r = SpanFieldRanks::from_span(&v);
            (k, (v, r))
        })
        .collect();
    merge_spans_crdt_ranked(&mut ranked, spans);
    for (k, (v, _)) in ranked {
        map.insert(k, v);
    }
}

pub(crate) fn recompute_trace_from_spans_with_explicit(
    trace: &mut AgentTrace,
    explicit_status: TraceStatus,
    explicit_failure_source: Option<FailureSource>,
    explicit_has_loop_warning: bool,
    explicit_primary_model: Option<String>,
    explicit_primary_provider: Option<String>,
    spans: &[AgentSpan],
) {
    trace.status = explicit_status;
    trace.failure_source = explicit_failure_source;
    trace.has_loop_warning = explicit_has_loop_warning;
    trace.primary_model = explicit_primary_model.filter(|s| !s.is_empty());
    trace.primary_provider = explicit_primary_provider.filter(|s| !s.is_empty());
    recompute_trace_from_spans(trace, spans);
}

/// Deduplicates spans by `span_id` (`Final > Update > Start`) and recomputes trace rollups.
pub(crate) fn recompute_trace_from_spans(trace: &mut AgentTrace, spans: &[AgentSpan]) {
    if spans.is_empty() {
        trace.input_tokens = trace.input_tokens.max(0);
        trace.output_tokens = trace.output_tokens.max(0);
        trace.cached_tokens = trace.cached_tokens.max(0);
        trace.total_tokens = trace.total_tokens.max(0);
        trace.duration_ms = trace.duration_ms.max(0);
        sync_trace_cost(trace);
        return;
    }

    let mut dedup_map: HashMap<String, (AgentSpan, SpanFieldRanks)> =
        HashMap::with_capacity(spans.len());
    merge_spans_crdt_ranked(&mut dedup_map, spans.iter().cloned());

    let mut deduped: Vec<AgentSpan> = dedup_map.into_values().map(|(sp, _)| sp).collect();
    deduped.sort_by(|a, b| {
        a.start_ts_ms
            .cmp(&b.start_ts_ms)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });

    let mut llm_calls = 0i32;
    let mut tool_calls = 0i32;
    let mut err_spans = 0i32;
    let mut max_span_status: Option<TraceStatus> = None;
    let mut in_tok = 0i64;
    let mut out_tok = 0i64;
    let mut cached_tok = 0i64;
    let mut tot_tok = 0i64;
    let mut min_start = trace.start_ts_ms;
    let mut max_end = trace.end_ts_ms;
    let mut span_model: Option<String> = None;
    let mut span_provider: Option<String> = None;

    for sp in &deduped {
        match sp.span_type {
            SpanType::Llm => llm_calls = llm_calls.saturating_add(1),
            SpanType::Tool => tool_calls = tool_calls.saturating_add(1),
            _ => {}
        }
        if max_span_status.is_none_or(|cur| status_rank(sp.status) > status_rank(cur)) {
            max_span_status = Some(sp.status);
        }
        if matches!(sp.status, TraceStatus::Error | TraceStatus::Timeout) {
            err_spans = err_spans.saturating_add(1);
        }
        let s_in = sp.input_tokens.max(0);
        let s_out = sp.output_tokens.max(0);
        let s_cached = sp.cached_tokens.max(0);
        let s_tot = sp.total_tokens.max(s_in.saturating_add(s_out)).max(0);
        in_tok = in_tok.saturating_add(s_in);
        out_tok = out_tok.saturating_add(s_out);
        cached_tok = cached_tok.saturating_add(s_cached);
        tot_tok = tot_tok.saturating_add(s_tot);

        if sp.start_ts_ms > 0 && (min_start <= 0 || sp.start_ts_ms < min_start) {
            min_start = sp.start_ts_ms;
        }
        if let Some(end) = sp.end_ts_ms {
            max_end = Some(max_end.map_or(end, |m| m.max(end)));
        }
        if sp.span_type == SpanType::Llm {
            merge_opt_str_max(&mut span_model, sp.model.clone());
            merge_opt_str_max(&mut span_provider, sp.provider.clone());
        }
    }

    let cost_sum = sum_cost_nanos(deduped.iter().map(|s| s.cost_nanos));
    trace.span_count = i32::try_from(deduped.len()).unwrap_or(i32::MAX);
    trace.llm_call_count = llm_calls;
    trace.tool_call_count = tool_calls;
    trace.error_span_count = err_spans;
    trace.input_tokens = in_tok;
    trace.output_tokens = out_tok;
    trace.cached_tokens = cached_tok;
    trace.total_tokens = tot_tok.max(trace.total_tokens.max(0));
    if cost_sum > 0 {
        trace.total_cost_nanos = cost_sum;
        trace.total_cost_usd = cost_nanos_to_usd(cost_sum);
    } else {
        sync_trace_cost(trace);
    }

    if min_start > 0 {
        trace.start_ts_ms = min_start;
    }
    trace.end_ts_ms = max_end;
    if let Some(end) = max_end {
        if trace.start_ts_ms > 0 {
            trace.duration_ms = trace
                .duration_ms
                .max(end.saturating_sub(trace.start_ts_ms).max(0));
        }
    }
    trace.duration_ms = trace.duration_ms.max(0);
    merge_opt_str_max(&mut trace.primary_model, span_model);
    merge_opt_str_max(&mut trace.primary_provider, span_provider);

    if let Some(ms) = max_span_status {
        if status_rank(ms) > status_rank(trace.status) {
            trace.status = ms;
        }
    }
    if err_spans > 0 || matches!(trace.status, TraceStatus::Error | TraceStatus::Timeout) {
        let span_fs = classify_failure_source(&deduped);
        if failure_source_rank(Some(span_fs)) > failure_source_rank(trace.failure_source) {
            trace.failure_source = Some(span_fs);
        }
    }
    if detect_execution_loop(&deduped).is_some() {
        trace.has_loop_warning = true;
    }
}

/// Heap bytes a JSON value occupies, roughly: content plus per-node overhead. Small objects
/// are mostly overhead, so counting serialised length alone under-reports memory several times.
fn value_bytes(v: &Value) -> usize {
    32 + match v {
        Value::String(s) => s.len(),
        Value::Array(a) => a.iter().map(value_bytes).sum(),
        // serde_json's map is a B-tree: one ~640-byte node for up to 11 entries.
        Value::Object(m) => {
            640.max(m.len() * 80)
                + m.iter()
                    .map(|(k, v)| k.len() + value_bytes(v))
                    .sum::<usize>()
        }
        _ => 0,
    }
}

fn opt_len(s: &Option<String>) -> usize {
    s.as_ref().map_or(0, String::len)
}

fn approx_span_bytes(sp: &AgentSpan) -> usize {
    // Struct, its slot in the span map (keyed by a second copy of the id), strings, payloads.
    std::mem::size_of::<AgentSpan>()
        + 96
        + 2 * sp.span_id.len()
        + sp.trace_id.len()
        + sp.project_id.len()
        + sp.name.len()
        + opt_len(&sp.parent_span_id)
        + opt_len(&sp.error_message)
        + opt_len(&sp.error_stack)
        + value_bytes(&sp.input)
        + value_bytes(&sp.output)
        + value_bytes(&sp.attributes)
        + value_bytes(&sp.metadata)
        + value_bytes(&sp.model_params)
        + sp.events
            .iter()
            .map(|e| 64 + e.name.len() + value_bytes(&e.attributes))
            .sum::<usize>()
}

/// Estimated resident bytes of one memtable entry. The header is held twice (as sent, and
/// with span rollups applied).
fn approx_trace_bytes(
    trace: &AgentTrace,
    spans: &HashMap<String, (AgentSpan, SpanFieldRanks)>,
) -> usize {
    let header = std::mem::size_of::<AgentTrace>()
        + trace.trace_id.len()
        + trace.agent_name.len()
        + opt_len(&trace.error_message)
        + value_bytes(&trace.input)
        + value_bytes(&trace.output)
        + value_bytes(&trace.metadata);
    256 + 2 * header
        + spans
            .values()
            .map(|(sp, _)| approx_span_bytes(sp))
            .sum::<usize>()
}

struct MemTraceEntry {
    explicit_trace: AgentTrace,
    field_ranks: TraceFieldRanks,
    trace: AgentTrace,
    has_explicit_header: bool,
    spans: HashMap<String, (AgentSpan, SpanFieldRanks)>,
    last_l0_seq: u64,
    approx_bytes: usize,
    unflushed_wal_seqs: HashMap<String, BTreeSet<u64>>,
}

impl MemTraceEntry {
    fn crdt_meta(&self) -> TraceCrdtMeta {
        TraceCrdtMeta {
            field_ranks: self.field_ranks,
            explicit_status: self.explicit_trace.status,
            explicit_failure_source: self.explicit_trace.failure_source,
            explicit_has_loop_warning: self.explicit_trace.has_loop_warning,
            explicit_primary_model: self
                .explicit_trace
                .primary_model
                .clone()
                .filter(|s| !s.is_empty()),
            explicit_primary_provider: self
                .explicit_trace
                .primary_provider
                .clone()
                .filter(|s| !s.is_empty()),
        }
    }
}

#[derive(Default)]
struct MemShard {
    traces: HashMap<String, MemTraceEntry>,
    oldest_unflushed_at: Option<Instant>,
    approx_bytes: usize,
    l0_seq: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RankedL0Snapshot {
    pub entries: Vec<(AgentTrace, Vec<AgentSpan>)>,
    pub trace_meta: HashMap<String, TraceCrdtMeta>,
    pub span_ranks: HashMap<(String, String), SpanFieldRanks>,
}

#[derive(Debug)]
pub(crate) struct StagedShardFlush {
    pub date: NaiveDate,
    pub shard_id: u16,
    pub snapshot_l0_seq: u64,
    pub entries: Vec<RankedTraceEntry>,
    pub staged_wal_seqs: HashMap<String, HashMap<String, BTreeSet<u64>>>,
    pub flushed_writer_max_seq: BTreeMap<String, u64>,
}

struct MemTableInner {
    shards: HashMap<(NaiveDate, u16), MemShard>,
    trace_route: HashMap<String, (NaiveDate, u16)>,
    max_inserted_wal_seq: BTreeMap<String, u64>,
    blocked_erased_wal_seq: BTreeMap<String, u64>,
    has_purged_wal: bool,
}

/// Sharded in-memory L0 MemTable keyed by `(trace_date, xxh3_64(trace_id) % shard_count)`.
pub struct ShardedMemTable {
    project_id: String,
    shard_count: u16,
    inner: RwLock<MemTableInner>,
}

impl ShardedMemTable {
    pub fn new(project_id: impl Into<String>, shard_count: u16) -> Self {
        let sc = shard_count.clamp(1, MAX_AGENT_SHARDS as u16);
        Self {
            project_id: project_id.into(),
            shard_count: sc,
            inner: RwLock::new(MemTableInner {
                shards: HashMap::new(),
                trace_route: HashMap::new(),
                max_inserted_wal_seq: BTreeMap::new(),
                blocked_erased_wal_seq: BTreeMap::new(),
                has_purged_wal: false,
            }),
        }
    }

    pub fn shard_count(&self) -> u16 {
        self.shard_count
    }

    pub fn insert_batch(
        &self,
        writer_id: &str,
        wal_seq: u64,
        batch: TraceIngestBatch,
    ) -> StoreResult<usize> {
        if batch.traces.is_empty() && batch.spans.is_empty() {
            return Ok(0);
        }

        let mut spans_by_trace: HashMap<String, Vec<AgentSpan>> = HashMap::new();
        for mut sp in batch.spans {
            if sp.project_id.is_empty() {
                sp.project_id = self.project_id.clone();
            }
            sync_span_cost(&mut sp);
            spans_by_trace
                .entry(sp.trace_id.clone())
                .or_default()
                .push(sp);
        }

        let mut traces_by_id: HashMap<String, (AgentTrace, TraceFieldRanks)> = HashMap::new();
        for mut tr in batch.traces {
            if tr.project_id.is_empty() {
                tr.project_id = self.project_id.clone();
            }
            sync_trace_cost(&mut tr);
            let ranks = TraceFieldRanks::from_trace(&tr);
            traces_by_id
                .entry(tr.trace_id.clone())
                .and_modify(|(existing, existing_ranks)| {
                    merge_trace_headers_ranked(existing, existing_ranks, tr.clone(), ranks)
                })
                .or_insert((tr, ranks));
        }

        let all_trace_ids: BTreeSet<String> = traces_by_id
            .keys()
            .cloned()
            .chain(spans_by_trace.keys().cloned())
            .collect();

        let now = Instant::now();
        let mut inner = self.inner.write();
        if wal_seq > 0 && !writer_id.is_empty() {
            inner
                .max_inserted_wal_seq
                .entry(writer_id.to_string())
                .and_modify(|m| *m = (*m).max(wal_seq))
                .or_insert(wal_seq);
        }
        let mut touched = 0usize;

        for tid in all_trace_ids {
            let trace_opt = traces_by_id.remove(&tid);
            let new_spans = spans_by_trace.remove(&tid).unwrap_or_default();

            let route = if let Some(&existing_route) = inner.trace_route.get(&tid) {
                existing_route
            } else {
                let ts = trace_opt
                    .as_ref()
                    .map(|(t, _)| t.start_ts_ms)
                    .or_else(|| new_spans.iter().map(|s| s.start_ts_ms).min())
                    .unwrap_or(0);
                let r = (
                    trace_date_from_ts(ts),
                    shard_id_for_trace(&tid, self.shard_count),
                );
                inner.trace_route.insert(tid.clone(), r);
                r
            };

            let shard = inner.shards.entry(route).or_default();
            if shard.oldest_unflushed_at.is_none() {
                shard.oldest_unflushed_at = Some(now);
            }
            shard.l0_seq = shard.l0_seq.saturating_add(1);
            let cur_l0_seq = shard.l0_seq;

            if let Some(entry) = shard.traces.get_mut(&tid) {
                let old_bytes = entry.approx_bytes;
                if let Some((tr, tr_ranks)) = trace_opt {
                    if entry.has_explicit_header {
                        merge_trace_headers_ranked(
                            &mut entry.explicit_trace,
                            &mut entry.field_ranks,
                            tr,
                            tr_ranks,
                        );
                    } else {
                        entry.explicit_trace = tr;
                        entry.field_ranks = tr_ranks;
                        entry.has_explicit_header = true;
                    }
                }
                merge_spans_crdt_ranked(&mut entry.spans, new_spans);
                let span_vec: Vec<AgentSpan> =
                    entry.spans.values().map(|(sp, _)| sp.clone()).collect();
                entry.trace = entry.explicit_trace.clone();
                recompute_trace_from_spans(&mut entry.trace, &span_vec);
                entry.last_l0_seq = cur_l0_seq;
                if wal_seq > 0 {
                    entry
                        .unflushed_wal_seqs
                        .entry(writer_id.to_string())
                        .or_default()
                        .insert(wal_seq);
                }
                entry.approx_bytes = approx_trace_bytes(&entry.trace, &entry.spans);
                shard.approx_bytes = shard
                    .approx_bytes
                    .saturating_sub(old_bytes)
                    .saturating_add(entry.approx_bytes);
            } else {
                let (explicit_trace, field_ranks, has_explicit_header) = match trace_opt {
                    Some((tr, ranks)) => (tr, ranks, true),
                    None => (
                        AgentTrace {
                            trace_id: tid.clone(),
                            project_id: self.project_id.clone(),
                            agent_name: "unknown".to_string(),
                            agent_version: "unversioned".to_string(),
                            prompt_version: None,
                            model_config_version: None,
                            tool_config_version: None,
                            app_release: None,
                            environment: "production".to_string(),
                            session_id: None,
                            user_id: None,
                            workflow_id: None,
                            parent_trace_id: None,
                            status: TraceStatus::Running,
                            start_ts_ms: 0,
                            end_ts_ms: None,
                            duration_ms: 0,
                            input: Value::Null,
                            output: Value::Null,
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
                        },
                        TraceFieldRanks::default(),
                        false,
                    ),
                };
                let mut spans = HashMap::new();
                merge_spans_crdt_ranked(&mut spans, new_spans);
                let span_vec: Vec<AgentSpan> = spans.values().map(|(sp, _)| sp.clone()).collect();
                let mut trace = explicit_trace.clone();
                recompute_trace_from_spans(&mut trace, &span_vec);
                let approx = approx_trace_bytes(&trace, &spans);
                let mut wal_map = HashMap::new();
                if wal_seq > 0 {
                    let mut s = BTreeSet::new();
                    s.insert(wal_seq);
                    wal_map.insert(writer_id.to_string(), s);
                }
                shard.approx_bytes = shard.approx_bytes.saturating_add(approx);
                shard.traces.insert(
                    tid,
                    MemTraceEntry {
                        explicit_trace,
                        field_ranks,
                        trace,
                        has_explicit_header,
                        spans,
                        last_l0_seq: cur_l0_seq,
                        approx_bytes: approx,
                        unflushed_wal_seqs: wal_map,
                    },
                );
            }
            touched += 1;
        }

        Ok(touched)
    }

    /// `(total bytes, largest shard's bytes)` of unflushed traces, by the same estimate the
    /// flush thresholds use.
    pub fn byte_stats(&self) -> (usize, usize) {
        let inner = self.inner.read();
        inner.shards.values().fold((0, 0), |(total, max), s| {
            (total + s.approx_bytes, max.max(s.approx_bytes))
        })
    }

    pub fn eligible_shards(
        &self,
        max_age: Duration,
        max_traces: usize,
        max_bytes: usize,
        force_all: bool,
    ) -> Vec<(NaiveDate, u16)> {
        let now = Instant::now();
        let inner = self.inner.read();
        let mut out = Vec::new();
        for (&key, shard) in &inner.shards {
            if shard.traces.is_empty() {
                continue;
            }
            if force_all
                || shard.traces.len() >= max_traces
                || shard.approx_bytes >= max_bytes
                || shard
                    .oldest_unflushed_at
                    .is_some_and(|t| now.saturating_duration_since(t) >= max_age)
            {
                out.push(key);
            }
        }
        out.sort();
        out
    }

    pub(crate) fn stage_shard(&self, date: NaiveDate, shard_id: u16) -> Option<StagedShardFlush> {
        let inner = self.inner.read();
        let shard = inner.shards.get(&(date, shard_id))?;
        if shard.traces.is_empty() {
            return None;
        }
        let mut entries = Vec::with_capacity(shard.traces.len());
        let mut staged_wal_seqs: HashMap<String, HashMap<String, BTreeSet<u64>>> =
            HashMap::with_capacity(shard.traces.len());
        let mut flushed_writer_max_seq: BTreeMap<String, u64> = BTreeMap::new();

        for (tid, entry) in &shard.traces {
            let mut spans: Vec<(AgentSpan, SpanFieldRanks)> = entry
                .spans
                .values()
                .map(|(sp, sr)| (sp.clone(), *sr))
                .collect();
            spans.sort_by(|a, b| {
                a.0.start_ts_ms
                    .cmp(&b.0.start_ts_ms)
                    .then_with(|| a.0.span_id.cmp(&b.0.span_id))
            });
            entries.push((entry.trace.clone(), entry.crdt_meta(), spans));
            if !entry.unflushed_wal_seqs.is_empty() {
                staged_wal_seqs.insert(tid.clone(), entry.unflushed_wal_seqs.clone());
                for (wid, seqs) in &entry.unflushed_wal_seqs {
                    if let Some(&max_s) = seqs.iter().next_back() {
                        flushed_writer_max_seq
                            .entry(wid.clone())
                            .and_modify(|m| *m = (*m).max(max_s))
                            .or_insert(max_s);
                    }
                }
            }
        }

        entries.sort_by(|a, b| {
            a.0.start_ts_ms
                .cmp(&b.0.start_ts_ms)
                .then_with(|| a.0.trace_id.cmp(&b.0.trace_id))
        });

        Some(StagedShardFlush {
            date,
            shard_id,
            snapshot_l0_seq: shard.l0_seq,
            entries,
            staged_wal_seqs,
            flushed_writer_max_seq,
        })
    }

    /// Computes safe cross-shard `committed_wal_seq` watermark assuming `staged_flushes`
    /// have durably landed in `.plss` segments and will be completed in L0.
    ///
    /// Foreign writers (`wid != self_writer_id`) may ONLY advance via sequences actually
    /// flushed in `staged_flushes` and are capped below any WAL sequence whose entries were
    /// dropped/purged due to erasure or date-pruning (`blocked_erased_wal_seq`).
    pub(crate) fn compute_safe_committed_wal_seq(
        &self,
        prev_committed: &BTreeMap<String, u64>,
        staged_flushes: &[StagedShardFlush],
        self_writer_id: &str,
    ) -> BTreeMap<String, u64> {
        let mut staged_by_shard: HashMap<(NaiveDate, u16), &StagedShardFlush> =
            HashMap::with_capacity(staged_flushes.len());
        let mut flushed_max: BTreeMap<String, u64> = prev_committed.clone();
        for sf in staged_flushes {
            staged_by_shard.insert((sf.date, sf.shard_id), sf);
            for (wid, &max_s) in &sf.flushed_writer_max_seq {
                flushed_max
                    .entry(wid.clone())
                    .and_modify(|m| *m = (*m).max(max_s))
                    .or_insert(max_s);
            }
        }

        let inner = self.inner.read();
        if !self_writer_id.is_empty() {
            if let Some(&max_s) = inner.max_inserted_wal_seq.get(self_writer_id) {
                flushed_max
                    .entry(self_writer_id.to_string())
                    .and_modify(|m| *m = (*m).max(max_s))
                    .or_insert(max_s);
            }
        }
        let mut min_remaining: HashMap<String, u64> = HashMap::new();
        for (&shard_key, shard) in &inner.shards {
            let staged_opt = staged_by_shard.get(&shard_key).copied();
            for (tid, entry) in &shard.traces {
                if let Some(staged) = staged_opt {
                    if entry.last_l0_seq <= staged.snapshot_l0_seq {
                        continue; // This entry is completely flushed by `staged`.
                    }
                }
                let staged_for_tid = staged_opt.and_then(|s| s.staged_wal_seqs.get(tid));
                for (wid, seqs) in &entry.unflushed_wal_seqs {
                    let staged_set = staged_for_tid.and_then(|m| m.get(wid));
                    for &seq in seqs {
                        if staged_set.is_some_and(|s| s.contains(&seq)) {
                            continue;
                        }
                        min_remaining
                            .entry(wid.clone())
                            .and_modify(|m| *m = (*m).min(seq))
                            .or_insert(seq);
                    }
                }
            }
        }

        let mut out = BTreeMap::new();
        for (wid, &fmax) in &flushed_max {
            let prev = prev_committed.get(wid).copied().unwrap_or(0);
            let mut safe = match min_remaining.get(wid) {
                Some(&min_rem) => min_rem.saturating_sub(1).max(prev),
                None => fmax.max(prev),
            };
            if wid != self_writer_id {
                if let Some(&blocked_seq) = inner.blocked_erased_wal_seq.get(wid) {
                    safe = safe.min(blocked_seq.saturating_sub(1)).max(prev);
                }
            }
            if safe > 0 {
                out.insert(wid.clone(), safe);
            }
        }
        out
    }

    pub(crate) fn complete_shard_flush(&self, staged: &StagedShardFlush) {
        let mut inner = self.inner.write();
        let shard_key = (staged.date, staged.shard_id);
        let mut empty_shard = false;
        let mut removed_tids = Vec::new();

        if let Some(shard) = inner.shards.get_mut(&shard_key) {
            for (tid, staged_seqs) in &staged.staged_wal_seqs {
                let remove_entry = if let Some(entry) = shard.traces.get_mut(tid) {
                    if entry.last_l0_seq <= staged.snapshot_l0_seq {
                        true
                    } else {
                        for (wid, seqs) in staged_seqs {
                            if let Some(cur_set) = entry.unflushed_wal_seqs.get_mut(wid) {
                                for s in seqs {
                                    cur_set.remove(s);
                                }
                                if cur_set.is_empty() {
                                    entry.unflushed_wal_seqs.remove(wid);
                                }
                            }
                        }
                        false
                    }
                } else {
                    false
                };
                if remove_entry {
                    shard.traces.remove(tid);
                    removed_tids.push(tid.clone());
                }
            }
            // Also remove any entries without wal_seqs that were snapshotted
            shard.traces.retain(|tid, entry| {
                if entry.last_l0_seq <= staged.snapshot_l0_seq
                    && entry.unflushed_wal_seqs.is_empty()
                {
                    removed_tids.push(tid.clone());
                    false
                } else {
                    true
                }
            });

            shard.approx_bytes = shard.traces.values().map(|e| e.approx_bytes).sum();
            if shard.traces.is_empty() {
                shard.oldest_unflushed_at = None;
                empty_shard = true;
            } else {
                shard.oldest_unflushed_at = Some(Instant::now());
            }
        }

        if empty_shard {
            inner.shards.remove(&shard_key);
        }
        for tid in removed_tids {
            if inner.trace_route.get(&tid) == Some(&shard_key) {
                inner.trace_route.remove(&tid);
            }
        }
    }

    /// Evicts L0 entries whose contributing WAL sequences have all been committed to segments
    /// (used by read-only query nodes on `refresh_from_wal` after manifest advances).
    pub fn evict_committed_wal(&self, committed_wal_seq: &BTreeMap<String, u64>) {
        if committed_wal_seq.is_empty() {
            return;
        }
        let mut inner = self.inner.write();
        let mut empty_shards = Vec::new();
        let mut removed_routes = Vec::new();

        for (&shard_key, shard) in &mut inner.shards {
            let mut to_remove = Vec::new();
            for (tid, entry) in &mut shard.traces {
                let mut empty_writers = Vec::new();
                for (wid, seqs) in &mut entry.unflushed_wal_seqs {
                    if let Some(&committed) = committed_wal_seq.get(wid) {
                        seqs.retain(|&s| s > committed);
                        if seqs.is_empty() {
                            empty_writers.push(wid.clone());
                        }
                    }
                }
                for wid in empty_writers {
                    entry.unflushed_wal_seqs.remove(&wid);
                }
                if entry.unflushed_wal_seqs.is_empty() {
                    to_remove.push(tid.clone());
                }
            }
            for tid in to_remove {
                shard.traces.remove(&tid);
                removed_routes.push((tid, shard_key));
            }
            shard.approx_bytes = shard.traces.values().map(|e| e.approx_bytes).sum();
            if shard.traces.is_empty() {
                shard.oldest_unflushed_at = None;
                empty_shards.push(shard_key);
            }
        }

        for k in empty_shards {
            inner.shards.remove(&k);
        }
        for (tid, sk) in removed_routes {
            if inner.trace_route.get(&tid) == Some(&sk) {
                inner.trace_route.remove(&tid);
            }
        }
        inner
            .blocked_erased_wal_seq
            .retain(|wid, bseq| committed_wal_seq.get(wid).is_none_or(|&c| *bseq > c));
    }

    pub fn snapshot_all(&self) -> Vec<(AgentTrace, Vec<AgentSpan>)> {
        self.snapshot_all_ranked().entries
    }

    pub(crate) fn snapshot_all_ranked(&self) -> RankedL0Snapshot {
        let inner = self.inner.read();
        let mut entries = Vec::new();
        let mut trace_meta = HashMap::new();
        let mut span_ranks = HashMap::new();
        for shard in inner.shards.values() {
            for entry in shard.traces.values() {
                let mut spans: Vec<AgentSpan> = Vec::with_capacity(entry.spans.len());
                for (sp, sr) in entry.spans.values() {
                    span_ranks.insert((entry.trace.trace_id.clone(), sp.span_id.clone()), *sr);
                    spans.push(sp.clone());
                }
                spans.sort_by(|a, b| {
                    a.start_ts_ms
                        .cmp(&b.start_ts_ms)
                        .then_with(|| a.span_id.cmp(&b.span_id))
                });
                trace_meta.insert(entry.trace.trace_id.clone(), entry.crdt_meta());
                entries.push((entry.trace.clone(), spans));
            }
        }
        RankedL0Snapshot {
            entries,
            trace_meta,
            span_ranks,
        }
    }

    pub fn snapshot_for_trace(&self, trace_id: &str) -> Option<(AgentTrace, Vec<AgentSpan>)> {
        let inner = self.inner.read();
        let key = inner.trace_route.get(trace_id)?;
        let shard = inner.shards.get(key)?;
        let entry = shard.traces.get(trace_id)?;
        let mut spans: Vec<AgentSpan> = entry.spans.values().map(|(sp, _)| sp.clone()).collect();
        spans.sort_by(|a, b| {
            a.start_ts_ms
                .cmp(&b.start_ts_ms)
                .then_with(|| a.span_id.cmp(&b.span_id))
        });
        Some((entry.trace.clone(), spans))
    }

    pub fn snapshot_for_session(&self, session_id: &str) -> Vec<(AgentTrace, Vec<AgentSpan>)> {
        let inner = self.inner.read();
        let mut out = Vec::new();
        for shard in inner.shards.values() {
            for entry in shard.traces.values() {
                if entry.trace.session_id.as_deref() == Some(session_id) {
                    let mut spans: Vec<AgentSpan> =
                        entry.spans.values().map(|(sp, _)| sp.clone()).collect();
                    spans.sort_by(|a, b| {
                        a.start_ts_ms
                            .cmp(&b.start_ts_ms)
                            .then_with(|| a.span_id.cmp(&b.span_id))
                    });
                    out.push((entry.trace.clone(), spans));
                }
            }
        }
        out
    }

    pub fn purge_erased(&self, tombstones: &[AgentErasureTombstone]) -> usize {
        self.purge_erased_with_ids(tombstones).len()
    }

    pub(crate) fn purge_erased_with_ids(
        &self,
        tombstones: &[AgentErasureTombstone],
    ) -> Vec<String> {
        if tombstones.is_empty() {
            return Vec::new();
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        let active: Vec<&AgentErasureTombstone> = tombstones
            .iter()
            .filter(|t| !t.is_expired(now_ms))
            .collect();
        if active.is_empty() {
            return Vec::new();
        }

        let mut inner = self.inner.write();
        let inner_ref = &mut *inner;
        let mut removed_ids = Vec::new();
        let mut empty_shards = Vec::new();
        let mut removed_routes = Vec::new();

        for (&shard_key, shard) in &mut inner_ref.shards {
            let mut to_remove = Vec::new();
            for (tid, entry) in &shard.traces {
                let matches = active.iter().any(|tb| {
                    tb.matches_trace(&entry.trace)
                        || ((tb.project_id.is_empty() || tb.project_id == self.project_id)
                            && tb.level == ErasureLevel::Trace
                            && tb.target_id == *tid)
                });
                if matches {
                    to_remove.push(tid.clone());
                }
            }
            for tid in to_remove {
                if let Some(removed_entry) = shard.traces.remove(&tid) {
                    record_entry_blocked_wal(&mut inner_ref.blocked_erased_wal_seq, &removed_entry);
                }
                removed_ids.push(tid.clone());
                removed_routes.push((tid, shard_key));
            }
            shard.approx_bytes = shard.traces.values().map(|e| e.approx_bytes).sum();
            if shard.traces.is_empty() {
                shard.oldest_unflushed_at = None;
                empty_shards.push(shard_key);
            }
        }

        for k in empty_shards {
            inner.shards.remove(&k);
        }
        for (tid, sk) in removed_routes {
            if inner.trace_route.get(&tid) == Some(&sk) {
                inner.trace_route.remove(&tid);
            }
        }
        if !removed_ids.is_empty() {
            inner.has_purged_wal = true;
        }
        removed_ids
    }

    pub(crate) fn record_accounted_wal_seq(&self, writer_id: &str, wal_seq: u64) {
        if wal_seq == 0 || writer_id.is_empty() {
            return;
        }
        let mut inner = self.inner.write();
        inner
            .max_inserted_wal_seq
            .entry(writer_id.to_string())
            .and_modify(|m| *m = (*m).max(wal_seq))
            .or_insert(wal_seq);
        inner.has_purged_wal = true;
    }

    pub(crate) fn record_blocked_foreign_wal_seq(&self, writer_id: &str, wal_seq: u64) {
        if wal_seq == 0 || writer_id.is_empty() {
            return;
        }
        let mut inner = self.inner.write();
        inner
            .blocked_erased_wal_seq
            .entry(writer_id.to_string())
            .and_modify(|m| *m = (*m).min(wal_seq))
            .or_insert(wal_seq);
        inner.has_purged_wal = true;
    }

    pub(crate) fn has_purged_wal(&self) -> bool {
        self.inner.read().has_purged_wal
    }

    pub(crate) fn clear_purged_wal_flag(&self) {
        self.inner.write().has_purged_wal = false;
    }

    pub(crate) fn purge_erased_ids(&self, erased_ids: &BTreeMap<String, i64>) -> usize {
        if erased_ids.is_empty() {
            return 0;
        }
        let mut inner = self.inner.write();
        let inner_ref = &mut *inner;
        let mut removed = 0usize;
        let mut empty_shards = Vec::new();
        let mut removed_routes = Vec::new();

        for (&shard_key, shard) in &mut inner_ref.shards {
            let before = shard.traces.len();
            shard.traces.retain(|tid, entry| {
                if erased_ids.contains_key(tid) {
                    record_entry_blocked_wal(&mut inner_ref.blocked_erased_wal_seq, entry);
                    removed_routes.push((tid.clone(), shard_key));
                    false
                } else {
                    true
                }
            });
            let diff = before.saturating_sub(shard.traces.len());
            if diff > 0 {
                removed += diff;
                shard.approx_bytes = shard.traces.values().map(|e| e.approx_bytes).sum();
                if shard.traces.is_empty() {
                    shard.oldest_unflushed_at = None;
                    empty_shards.push(shard_key);
                }
            }
        }

        for k in empty_shards {
            inner.shards.remove(&k);
        }
        for (tid, sk) in removed_routes {
            if inner.trace_route.get(&tid) == Some(&sk) {
                inner.trace_route.remove(&tid);
            }
        }
        if removed > 0 {
            inner.has_purged_wal = true;
        }
        removed
    }

    pub(crate) fn purge_erased_id_set(&self, erased_ids: &HashSet<String>) -> usize {
        if erased_ids.is_empty() {
            return 0;
        }
        let mut inner = self.inner.write();
        let inner_ref = &mut *inner;
        let mut removed = 0usize;
        let mut empty_shards = Vec::new();
        let mut removed_routes = Vec::new();

        for (&shard_key, shard) in &mut inner_ref.shards {
            let before = shard.traces.len();
            shard.traces.retain(|tid, entry| {
                if erased_ids.contains(tid) {
                    record_entry_blocked_wal(&mut inner_ref.blocked_erased_wal_seq, entry);
                    removed_routes.push((tid.clone(), shard_key));
                    false
                } else {
                    true
                }
            });
            let diff = before.saturating_sub(shard.traces.len());
            if diff > 0 {
                removed += diff;
                shard.approx_bytes = shard.traces.values().map(|e| e.approx_bytes).sum();
                if shard.traces.is_empty() {
                    shard.oldest_unflushed_at = None;
                    empty_shards.push(shard_key);
                }
            }
        }

        for k in empty_shards {
            inner.shards.remove(&k);
        }
        for (tid, sk) in removed_routes {
            if inner.trace_route.get(&tid) == Some(&sk) {
                inner.trace_route.remove(&tid);
            }
        }
        if removed > 0 {
            inner.has_purged_wal = true;
        }
        removed
    }

    pub(crate) fn prune_before_date(&self, cutoff_date: NaiveDate) -> usize {
        let mut inner = self.inner.write();
        let inner_ref = &mut *inner;
        let expired_keys: Vec<(NaiveDate, u16)> = inner_ref
            .shards
            .keys()
            .copied()
            .filter(|(date, _)| *date < cutoff_date)
            .collect();
        if expired_keys.is_empty() {
            return 0;
        }
        let mut removed = 0usize;
        let mut removed_tids = Vec::new();
        for key in expired_keys {
            if let Some(shard) = inner_ref.shards.remove(&key) {
                removed += shard.traces.len();
                for (tid, entry) in shard.traces {
                    record_entry_blocked_wal(&mut inner_ref.blocked_erased_wal_seq, &entry);
                    removed_tids.push((tid, key));
                }
            }
        }
        for (tid, key) in removed_tids {
            if inner_ref.trace_route.get(&tid) == Some(&key) {
                inner_ref.trace_route.remove(&tid);
            }
        }
        removed
    }

    pub fn total_traces(&self) -> usize {
        self.inner
            .read()
            .shards
            .values()
            .map(|s| s.traces.len())
            .sum()
    }
}

fn record_entry_blocked_wal(blocked: &mut BTreeMap<String, u64>, entry: &MemTraceEntry) {
    for (wid, seqs) in &entry.unflushed_wal_seqs {
        if let Some(&min_s) = seqs.iter().next() {
            if min_s > 0 && !wid.is_empty() {
                blocked
                    .entry(wid.clone())
                    .and_modify(|m| *m = (*m).min(min_s))
                    .or_insert(min_s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SpanPhase;

    fn mk_trace(id: &str, status: TraceStatus, ts: i64) -> AgentTrace {
        AgentTrace {
            trace_id: id.to_string(),
            project_id: "p1".to_string(),
            agent_name: "bot".to_string(),
            agent_version: "v1".to_string(),
            prompt_version: None,
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".to_string(),
            session_id: Some("sess_1".to_string()),
            user_id: Some("user_1".to_string()),
            workflow_id: None,
            parent_trace_id: None,
            status,
            start_ts_ms: ts,
            end_ts_ms: Some(ts + 100),
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
        }
    }

    fn mk_span(id: &str, trace_id: &str, phase: SpanPhase, status: TraceStatus) -> AgentSpan {
        AgentSpan {
            span_id: id.to_string(),
            trace_id: trace_id.to_string(),
            project_id: "p1".to_string(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: phase,
            depth: 0,
            name: "call".to_string(),
            span_type: SpanType::Llm,
            status,
            start_ts_ms: 1_700_000_000_000,
            end_ts_ms: Some(1_700_000_000_050),
            duration_ms: 50,
            provider: Some("openai".to_string()),
            model: Some("gpt-4o".to_string()),
            model_id: None,
            model_version: None,
            prompt_template_id: None,
            prompt_template_version: None,
            input_tokens: 10,
            output_tokens: 5,
            cached_tokens: 0,
            total_tokens: 15,
            cost_nanos: 2_000_000,
            cost_usd: 0.002,
            temperature: None,
            finish_reason: None,
            model_params: Value::Null,
            tool_name: None,
            tool_version: None,
            tool_retry_count: 0,
            input: Value::Null,
            output: Value::Null,
            attributes: Value::Null,
            events: vec![],
            metadata: Value::Null,
            error_type: None,
            error_message: None,
            error_stack: None,
        }
    }

    #[test]
    fn sharded_memtable_routes_deterministically_and_flushes_on_age() {
        assert!(status_rank(TraceStatus::Running) < status_rank(TraceStatus::Ok));
        assert!(status_rank(TraceStatus::Ok) < status_rank(TraceStatus::Cancelled));
        assert!(status_rank(TraceStatus::Cancelled) < status_rank(TraceStatus::Timeout));
        assert!(status_rank(TraceStatus::Timeout) < status_rank(TraceStatus::Error));

        let mt = ShardedMemTable::new("p1", 8);
        let ts = 1_700_000_000_000i64;
        mt.insert_batch(
            "w1",
            1,
            TraceIngestBatch {
                traces: vec![mk_trace("tr_a", TraceStatus::Running, ts)],
                spans: vec![mk_span(
                    "sp_1",
                    "tr_a",
                    SpanPhase::Start,
                    TraceStatus::Running,
                )],
            },
        )
        .unwrap();

        // Age = 10s -> not eligible yet; Age = 0ns -> eligible
        assert!(mt
            .eligible_shards(Duration::from_secs(10), 4096, 32 * 1024 * 1024, false)
            .is_empty());
        let elig = mt.eligible_shards(Duration::ZERO, 4096, 32 * 1024 * 1024, false);
        assert_eq!(elig.len(), 1);
        let (date, sid) = elig[0];
        let staged = mt.stage_shard(date, sid).unwrap();

        // Concurrent update while staged: Final span + Ok trace arrives with seq 2
        mt.insert_batch(
            "w1",
            2,
            TraceIngestBatch {
                traces: vec![mk_trace("tr_a", TraceStatus::Ok, ts)],
                spans: vec![mk_span("sp_1", "tr_a", SpanPhase::Final, TraceStatus::Ok)],
            },
        )
        .unwrap();

        // Watermark keeps seq 2 because concurrent update has l0_seq > snapshot_l0_seq
        let prev = BTreeMap::new();
        let safe = mt.compute_safe_committed_wal_seq(&prev, std::slice::from_ref(&staged), "w1");
        assert_eq!(safe.get("w1").copied(), Some(1));

        mt.complete_shard_flush(&staged);
        let (tr, spans) = mt.snapshot_for_trace("tr_a").unwrap();
        assert_eq!(tr.status, TraceStatus::Ok);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].span_phase, SpanPhase::Final);
    }

    #[test]
    fn cross_shard_wal_watermark_prevents_premature_wal_purge() {
        let mt = ShardedMemTable::new("p1", 8);
        let day1 = 1_700_000_000_000i64;
        let day2 = day1 + 86_400_000;

        // Single WAL seq 1 touches 2 different shards (different dates)
        mt.insert_batch(
            "w1",
            1,
            TraceIngestBatch {
                traces: vec![
                    mk_trace("tr_1", TraceStatus::Ok, day1),
                    mk_trace("tr_2", TraceStatus::Ok, day2),
                ],
                spans: vec![],
            },
        )
        .unwrap();

        let elig = mt.eligible_shards(Duration::ZERO, 100, 1024 * 1024, true);
        assert_eq!(elig.len(), 2);

        // Flush only first shard -> seq 1 is still in second shard, so watermark MUST stay 0!
        let staged0 = mt.stage_shard(elig[0].0, elig[0].1).unwrap();
        let prev = BTreeMap::new();
        let safe0 = mt.compute_safe_committed_wal_seq(&prev, std::slice::from_ref(&staged0), "w1");
        assert!(!safe0.contains_key("w1"));
        mt.complete_shard_flush(&staged0);

        // Flush second shard -> now watermark advances to 1!
        let staged1 = mt.stage_shard(elig[1].0, elig[1].1).unwrap();
        let safe1 = mt.compute_safe_committed_wal_seq(&prev, std::slice::from_ref(&staged1), "w1");
        assert_eq!(safe1.get("w1").copied(), Some(1));
        mt.complete_shard_flush(&staged1);
    }

    #[test]
    fn merge_spans_crdt_is_commutative_across_span_phases_and_preserves_timeout() {
        use crate::model::SpanEvent;
        use serde_json::json;

        let ts = 1_700_000_000_000i64;
        let mut start = mk_span("sp_comm", "tr_comm", SpanPhase::Start, TraceStatus::Running);
        start.parent_span_id = Some("sp_root".to_string());
        start.provider = Some("anthropic".to_string());
        start.model = None;
        start.input = json!({"prompt": "hello"});
        start.attributes = json!({"phase": "start"});
        start.metadata = Value::Null;
        start.events = vec![];

        let mut update = mk_span(
            "sp_comm",
            "tr_comm",
            SpanPhase::Update,
            TraceStatus::Running,
        );
        update.parent_span_id = None;
        update.provider = None;
        update.model = Some("claude-sonnet-4".to_string());
        update.model_version = Some("20250514".to_string());
        update.linked_span_ids = vec!["sp_linked".to_string()];
        update.metadata = json!({"stream_chunks": 4});
        update.events = vec![SpanEvent {
            name: "first_token".to_string(),
            timestamp_ms: ts + 10,
            attributes: json!({"tok": "hi"}),
        }];

        let mut fin = mk_span("sp_comm", "tr_comm", SpanPhase::Final, TraceStatus::Ok);
        fin.parent_span_id = None;
        fin.provider = None;
        fin.model = None;
        fin.temperature = Some(0.3);
        fin.finish_reason = Some("end_turn".to_string());
        fin.model_params = json!({"max_tokens": 256});
        fin.output = json!({"answer": "world"});
        fin.metadata = Value::Null;
        fin.events = vec![];

        let mut in_order = HashMap::new();
        merge_spans_crdt(&mut in_order, [start.clone(), update.clone(), fin.clone()]);

        let mut out_of_order = HashMap::new();
        merge_spans_crdt(
            &mut out_of_order,
            [fin.clone(), update.clone(), start.clone()],
        );

        assert_eq!(in_order["sp_comm"], out_of_order["sp_comm"]);
        let merged = &out_of_order["sp_comm"];
        assert_eq!(merged.span_phase, SpanPhase::Final);
        assert_eq!(merged.status, TraceStatus::Ok);
        assert_eq!(merged.input, json!({"prompt": "hello"}));
        assert_eq!(merged.output, json!({"answer": "world"}));
        assert_eq!(merged.attributes, json!({"phase": "start"}));
        assert_eq!(merged.metadata, json!({"stream_chunks": 4}));
        assert_eq!(merged.model_params, json!({"max_tokens": 256}));
        assert_eq!(merged.events.len(), 1);
        assert_eq!(merged.events[0].name, "first_token");
        assert_eq!(merged.linked_span_ids, vec!["sp_linked".to_string()]);
        assert_eq!(merged.parent_span_id.as_deref(), Some("sp_root"));
        assert_eq!(merged.provider.as_deref(), Some("anthropic"));
        assert_eq!(merged.model.as_deref(), Some("claude-sonnet-4"));
        assert_eq!(merged.model_version.as_deref(), Some("20250514"));
        assert_eq!(merged.temperature, Some(0.3));
        assert_eq!(merged.finish_reason.as_deref(), Some("end_turn"));

        // Timeout span (without Error span) preserves TraceStatus::Timeout on trace
        let mut tr_timeout = mk_trace("tr_to", TraceStatus::Ok, ts);
        let sp_to = mk_span("sp_to", "tr_to", SpanPhase::Final, TraceStatus::Timeout);
        recompute_trace_from_spans(&mut tr_timeout, &[sp_to.clone()]);
        assert_eq!(tr_timeout.status, TraceStatus::Timeout);
        assert_eq!(tr_timeout.error_span_count, 1);

        // Timeout + Error span promotes trace to Error
        let mut tr_err = mk_trace("tr_err", TraceStatus::Ok, ts);
        let sp_err = mk_span("sp_err", "tr_err", SpanPhase::Final, TraceStatus::Error);
        recompute_trace_from_spans(&mut tr_err, &[sp_to, sp_err]);
        assert_eq!(tr_err.status, TraceStatus::Error);
        assert_eq!(tr_err.error_span_count, 2);
    }

    #[test]
    fn merge_trace_headers_and_spans_are_commutative_and_associative() {
        use serde_json::json;

        let ts = 1_700_000_000_000i64;
        let mut a = mk_trace("tr_3way", TraceStatus::Running, ts);
        a.environment = "staging".to_string();
        a.failure_source = Some(FailureSource::Application);
        a.session_id = Some("sess_a".to_string());
        a.user_id = Some("user_a".to_string());
        a.workflow_id = Some("wf_a".to_string());
        a.parent_trace_id = Some("pt_a".to_string());
        a.output = json!({"step": 1});
        a.tags = (0..70).map(|i| format!("tag_{i:03}")).collect();
        a.avg_eval_score = Some(0.4);
        a.eval_pass_rate = Some(0.5);

        let mut b = mk_trace("tr_3way", TraceStatus::Ok, ts + 200);
        b.environment = "production".to_string();
        b.failure_source = Some(FailureSource::Model);
        b.session_id = Some("sess_b".to_string());
        b.user_id = None;
        b.output = json!({"step": 2});
        b.tags = vec!["tag_000".to_string(), "aaa_first".to_string()];
        b.avg_eval_score = Some(0.9);
        b.eval_pass_rate = Some(1.0);

        let mut c = mk_trace("tr_3way", TraceStatus::Ok, ts + 100);
        c.environment = "canary".to_string();
        c.failure_source = Some(FailureSource::Tool);
        c.output = json!({"step": 3});
        c.metadata = json!({"m": "c"});

        let merge2 = |x: AgentTrace, y: AgentTrace| -> AgentTrace {
            let mut acc = x;
            merge_trace_headers(&mut acc, y);
            acc
        };

        // 2-way commutativity
        assert_eq!(
            merge2(a.clone(), b.clone()),
            merge2(b.clone(), a.clone()),
            "2-way merge(A, B) != merge(B, A)"
        );
        assert_eq!(
            merge2(b.clone(), c.clone()),
            merge2(c.clone(), b.clone()),
            "equal-rank 2-way merge(B, C) != merge(C, B)"
        );

        // 3-way associativity & all permutations
        let ab_c = merge2(merge2(a.clone(), b.clone()), c.clone());
        let a_bc = merge2(a.clone(), merge2(b.clone(), c.clone()));
        let cb_a = merge2(merge2(c.clone(), b.clone()), a.clone());
        assert_eq!(ab_c, a_bc, "3-way associativity failed: (A⊔B)⊔C != A⊔(B⊔C)");
        assert_eq!(ab_c, cb_a, "3-way permutation failed");
        assert_eq!(ab_c.environment, "staging");
        assert_eq!(ab_c.failure_source, Some(FailureSource::Model));
        assert_eq!(ab_c.tags[0], "aaa_first");
        assert_eq!(ab_c.tags.len(), MAX_TAGS_PER_TRACE);
        assert_eq!(ab_c.avg_eval_score, Some(0.9));
        assert_eq!(ab_c.eval_pass_rate, Some(1.0));

        // Negative/zero start_ts_ms commutativity
        let mut z = mk_trace("tr_z", TraceStatus::Ok, 0);
        let mut neg = mk_trace("tr_z", TraceStatus::Ok, -5);
        z.project_id = String::new();
        let zn = merge2(z.clone(), neg.clone());
        let nz = merge2(neg.clone(), z.clone());
        assert_eq!(zn, nz);
        assert_eq!(zn.start_ts_ms, 0);
        assert_eq!(zn.project_id, "p1");
        neg.start_ts_ms = -5;
        let mut pos = mk_trace("tr_z", TraceStatus::Ok, ts);
        pos.project_id = "p1".to_string();
        assert_eq!(merge2(neg.clone(), pos.clone()), merge2(pos, neg));
    }
}
