use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use regex::Regex;
use serde_json::{json, Map, Value};

use crate::model::{AgentPrivacyPolicy, AgentSpan, AgentTrace, SpanType, TraceIngestBatch};
use crate::pricing::{cost_nanos_to_usd, sync_span_cost, sync_trace_cost};
use crate::{
    MAX_ATTRIBUTES_PER_SPAN, MAX_EVENTS_PER_SPAN, MAX_LINKED_SPANS_PER_SPAN, MAX_PAYLOAD_BYTES,
    MAX_SPANS_PER_TRACE, MAX_SPAN_DEPTH, MAX_TAGS_PER_TRACE,
};

pub const REDACTED_MARKER: &str = "[REDACTED]";
const MAX_SPAN_ID_BYTES: usize = 256;

struct PiiRegexes {
    email: Regex,
    api_key: Regex,
    ssn: Regex,
    credit_card: Regex,
}

fn pii_regexes() -> &'static PiiRegexes {
    static RE: OnceLock<PiiRegexes> = OnceLock::new();
    RE.get_or_init(|| PiiRegexes {
        email: Regex::new(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b").unwrap(),
        api_key: Regex::new(
            r"\b(?:sk-[A-Za-z0-9_-]{16,}|sk_live_[A-Za-z0-9_-]{12,}|sk_root_[A-Za-z0-9_-]{12,}|pk_live_[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{20,}|AIza[0-9A-Za-z_-]{30,}|xox[baprs]-[0-9A-Za-z-]{10,})\b",
        )
        .unwrap(),
        ssn: Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap(),
        credit_card: Regex::new(r"\b(?:\d[ -]*?){13,19}\b").unwrap(),
    })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SanitizeStats {
    pub redacted_fields: usize,
    pub truncated_payloads: usize,
    pub dropped_spans: usize,
}

impl SanitizeStats {
    fn merge(&mut self, other: Self) {
        self.redacted_fields += other.redacted_fields;
        self.truncated_payloads += other.truncated_payloads;
        self.dropped_spans += other.dropped_spans;
    }
}

/// Checks whether a string contains PII patterns (used both by redaction and the `pii_leakage` evaluator).
pub fn detect_pii_patterns(text: &str) -> Vec<&'static str> {
    let re = pii_regexes();
    let mut hits = Vec::new();
    if re.email.is_match(text) {
        hits.push("email");
    }
    if re.api_key.is_match(text) {
        hits.push("api_key_or_secret");
    }
    if re.ssn.is_match(text) {
        hits.push("ssn");
    }
    if re.credit_card.is_match(text) && looks_like_luhn_card(text) {
        hits.push("credit_card");
    }
    hits
}

fn looks_like_luhn_card(text: &str) -> bool {
    let re = pii_regexes();
    for m in re.credit_card.find_iter(text) {
        let digits: Vec<u32> = m.as_str().chars().filter_map(|c| c.to_digit(10)).collect();
        if digits.len() < 13 || digits.len() > 19 {
            continue;
        }
        let mut sum = 0u32;
        let mut alt = false;
        for &d in digits.iter().rev() {
            let mut n = d;
            if alt {
                n *= 2;
                if n > 9 {
                    n -= 9;
                }
            }
            sum += n;
            alt = !alt;
        }
        if sum % 10 == 0 {
            return true;
        }
    }
    false
}

pub fn redact_pii_str(s: &str, redacted_count: &mut usize) -> String {
    let re = pii_regexes();
    let mut out = s.to_string();
    for pat in [&re.api_key, &re.email, &re.ssn] {
        if pat.is_match(&out) {
            *redacted_count += 1;
            out = pat.replace_all(&out, REDACTED_MARKER).into_owned();
        }
    }
    if re.credit_card.is_match(&out) && looks_like_luhn_card(&out) {
        *redacted_count += 1;
        out = re
            .credit_card
            .replace_all(&out, REDACTED_MARKER)
            .into_owned();
    }
    out
}

pub fn redact_value(
    val: &mut Value,
    redact_pii: bool,
    redact_keys_lower: &[String],
    stats: &mut SanitizeStats,
) {
    match val {
        Value::String(s) => {
            if redact_pii {
                let cleaned = redact_pii_str(s, &mut stats.redacted_fields);
                *s = cleaned;
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                redact_value(item, redact_pii, redact_keys_lower, stats);
            }
        }
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let kl = k.to_ascii_lowercase();
                if redact_keys_lower.iter().any(|rk| kl.contains(rk)) {
                    *v = Value::String(REDACTED_MARKER.to_string());
                    stats.redacted_fields += 1;
                } else {
                    redact_value(v, redact_pii, redact_keys_lower, stats);
                }
            }
        }
        _ => {}
    }
}

fn floor_char_boundary(s: &str, max_bytes: usize) -> usize {
    let mut cut = max_bytes.min(s.len());
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    cut
}

/// Enforces `max_bytes` on a serialized JSON value with deterministic UTF-8-safe truncation.
/// Accounts for JSON string escaping (`\"`, `\\`, `\uXXXX`) so the resulting JSON never exceeds `max_bytes`.
pub fn cap_payload(val: &mut Value, max_bytes: usize, stats: &mut SanitizeStats) {
    if val.is_null() {
        return;
    }
    let Ok(serialized) = serde_json::to_string(val) else {
        return;
    };
    if serialized.len() <= max_bytes {
        return;
    }
    stats.truncated_payloads += 1;

    if max_bytes < 64 {
        let minimal = json!({ "_truncated": true });
        if serde_json::to_string(&minimal).is_ok_and(|s| s.len() <= max_bytes) {
            *val = minimal;
        } else {
            *val = Value::Null;
        }
        return;
    }

    let mut cut = floor_char_boundary(&serialized, max_bytes.saturating_sub(96));
    loop {
        let preview = &serialized[..cut];
        let candidate = json!({
            "_truncated": true,
            "_original_bytes": serialized.len(),
            "_preview": preview,
        });
        if let Ok(encoded) = serde_json::to_string(&candidate) {
            if encoded.len() <= max_bytes {
                *val = candidate;
                return;
            }
            if cut == 0 {
                break;
            }
            let excess = encoded.len() - max_bytes;
            let step = excess.max(1).min(cut.div_ceil(2));
            cut = floor_char_boundary(&serialized, cut.saturating_sub(step));
        } else {
            break;
        }
    }

    let fallback = json!({
        "_truncated": true,
        "_original_bytes": serialized.len(),
    });
    if serde_json::to_string(&fallback).is_ok_and(|s| s.len() <= max_bytes) {
        *val = fallback;
    } else {
        *val = json!({ "_truncated": true });
    }
}

/// Applies privacy capture controls, key/PII redaction, and hard size bounds to a trace.
pub fn sanitize_trace(trace: &mut AgentTrace, policy: &AgentPrivacyPolicy) -> SanitizeStats {
    let mut stats = SanitizeStats::default();
    let redact_keys: Vec<String> = policy
        .redact_keys
        .iter()
        .map(|k| k.trim().to_ascii_lowercase())
        .filter(|k| !k.is_empty())
        .collect();

    if !policy.capture_user_id {
        if trace.user_id.is_some() {
            stats.redacted_fields += 1;
        }
        trace.user_id = None;
    }
    if !policy.capture_inputs {
        if !trace.input.is_null() {
            stats.redacted_fields += 1;
        }
        trace.input = Value::Null;
    } else {
        redact_value(
            &mut trace.input,
            policy.redact_pii,
            &redact_keys,
            &mut stats,
        );
        cap_payload(&mut trace.input, MAX_PAYLOAD_BYTES, &mut stats);
    }

    if !policy.capture_outputs {
        if !trace.output.is_null() {
            stats.redacted_fields += 1;
        }
        trace.output = Value::Null;
    } else {
        redact_value(
            &mut trace.output,
            policy.redact_pii,
            &redact_keys,
            &mut stats,
        );
        cap_payload(&mut trace.output, MAX_PAYLOAD_BYTES, &mut stats);
    }

    redact_value(
        &mut trace.metadata,
        policy.redact_pii,
        &redact_keys,
        &mut stats,
    );
    cap_payload(&mut trace.metadata, MAX_PAYLOAD_BYTES, &mut stats);

    if let Some(msg) = &mut trace.error_message {
        if policy.redact_pii {
            *msg = redact_pii_str(msg, &mut stats.redacted_fields);
        }
    }

    if trace.tags.len() > MAX_TAGS_PER_TRACE {
        trace.tags.truncate(MAX_TAGS_PER_TRACE);
    }
    trace.input_tokens = trace.input_tokens.max(0);
    trace.output_tokens = trace.output_tokens.max(0);
    trace.cached_tokens = trace.cached_tokens.max(0);
    trace.total_tokens = trace.total_tokens.max(0);
    sync_trace_cost(trace);
    stats
}

fn sanitize_linked_span_ids(span_id: &str, linked: &mut Vec<String>) {
    let mut cleaned = Vec::with_capacity(linked.len().min(MAX_LINKED_SPANS_PER_SPAN));
    let mut seen = HashSet::new();
    for raw in linked.drain(..) {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed == span_id {
            continue;
        }
        let bounded = &trimmed[..floor_char_boundary(trimmed, MAX_SPAN_ID_BYTES)];
        if !bounded.is_empty() && seen.insert(bounded.to_string()) {
            cleaned.push(bounded.to_string());
            if cleaned.len() >= MAX_LINKED_SPANS_PER_SPAN {
                break;
            }
        }
    }
    *linked = cleaned;
}

/// Applies privacy capture controls, key/PII redaction, attribute exclusions, and hard bounds to a span.
pub fn sanitize_span(span: &mut AgentSpan, policy: &AgentPrivacyPolicy) -> SanitizeStats {
    let mut stats = SanitizeStats::default();
    let redact_keys: Vec<String> = policy
        .redact_keys
        .iter()
        .map(|k| k.trim().to_ascii_lowercase())
        .filter(|k| !k.is_empty())
        .collect();
    let excluded_attrs: HashSet<String> = policy
        .exclude_attributes
        .iter()
        .map(|k| k.trim().to_ascii_lowercase())
        .filter(|k| !k.is_empty())
        .collect();

    let allow_input = match span.span_type {
        SpanType::Llm => policy.capture_prompts && policy.capture_inputs,
        SpanType::Tool => policy.capture_tool_args && policy.capture_inputs,
        _ => policy.capture_inputs,
    };
    let allow_output = match span.span_type {
        SpanType::Llm => policy.capture_responses && policy.capture_outputs,
        SpanType::Tool => policy.capture_tool_results && policy.capture_outputs,
        _ => policy.capture_outputs,
    };

    if !allow_input {
        if !span.input.is_null() {
            stats.redacted_fields += 1;
        }
        span.input = Value::Null;
    } else {
        redact_value(&mut span.input, policy.redact_pii, &redact_keys, &mut stats);
        cap_payload(&mut span.input, MAX_PAYLOAD_BYTES, &mut stats);
    }

    if !allow_output {
        if !span.output.is_null() {
            stats.redacted_fields += 1;
        }
        span.output = Value::Null;
    } else {
        redact_value(
            &mut span.output,
            policy.redact_pii,
            &redact_keys,
            &mut stats,
        );
        cap_payload(&mut span.output, MAX_PAYLOAD_BYTES, &mut stats);
    }

    if let Value::Object(map) = &mut span.attributes {
        if !excluded_attrs.is_empty() {
            map.retain(|k, _| !excluded_attrs.contains(&k.to_ascii_lowercase()));
        }
        if map.len() > MAX_ATTRIBUTES_PER_SPAN {
            let trimmed: Map<String, Value> = map
                .iter()
                .take(MAX_ATTRIBUTES_PER_SPAN)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            *map = trimmed;
        }
    }
    redact_value(
        &mut span.attributes,
        policy.redact_pii,
        &redact_keys,
        &mut stats,
    );
    cap_payload(&mut span.attributes, MAX_PAYLOAD_BYTES, &mut stats);

    redact_value(
        &mut span.metadata,
        policy.redact_pii,
        &redact_keys,
        &mut stats,
    );
    cap_payload(&mut span.metadata, MAX_PAYLOAD_BYTES, &mut stats);

    if span.events.len() > MAX_EVENTS_PER_SPAN {
        span.events.truncate(MAX_EVENTS_PER_SPAN);
    }
    for ev in &mut span.events {
        redact_value(
            &mut ev.attributes,
            policy.redact_pii,
            &redact_keys,
            &mut stats,
        );
        cap_payload(&mut ev.attributes, MAX_PAYLOAD_BYTES, &mut stats);
    }

    if let Some(msg) = &mut span.error_message {
        if policy.redact_pii {
            *msg = redact_pii_str(msg, &mut stats.redacted_fields);
        }
    }
    if let Some(stack) = &mut span.error_stack {
        if policy.redact_pii {
            *stack = redact_pii_str(stack, &mut stats.redacted_fields);
        }
    }

    sanitize_linked_span_ids(&span.span_id, &mut span.linked_span_ids);
    span.input_tokens = span.input_tokens.max(0);
    span.output_tokens = span.output_tokens.max(0);
    span.cached_tokens = span.cached_tokens.max(0);
    span.total_tokens = span.total_tokens.max(0);
    sync_span_cost(span);
    stats
}

/// Computes `depth` for every span in a trace, breaking cycles without orphaning
/// non-cyclic descendants (`C -> A <-> B` keeps `C -> A -> B`), and clamping to `MAX_SPAN_DEPTH`.
pub fn compute_span_depths(spans: &mut [AgentSpan]) {
    let indices: Vec<usize> = (0..spans.len()).collect();
    compute_span_depths_for_indices(spans, &indices);
}

fn compute_span_depths_for_indices(spans: &mut [AgentSpan], indices: &[usize]) {
    let mut parent_map: HashMap<String, Option<String>> = HashMap::with_capacity(indices.len());
    for &idx in indices {
        let sp = &spans[idx];
        parent_map.insert(sp.span_id.clone(), sp.parent_span_id.clone());
    }

    // Pass 1: detect and break cycle-closing edges in `parent_map` so non-cyclic descendants remain intact.
    let mut broken: HashSet<String> = HashSet::new();
    for &idx in indices {
        let start_id = spans[idx].span_id.clone();
        let mut seen = HashSet::new();
        seen.insert(start_id.clone());
        let mut prev_id = start_id;
        let mut cur = parent_map.get(&prev_id).and_then(|p| p.clone());
        while let Some(pid) = cur {
            if !seen.insert(pid.clone()) {
                // `prev_id -> pid` closes a cycle; break `prev_id`'s parent link.
                parent_map.insert(prev_id.clone(), None);
                broken.insert(prev_id);
                break;
            }
            prev_id = pid.clone();
            cur = parent_map.get(&pid).and_then(|p| p.clone());
        }
    }

    // Pass 2: apply broken links and compute acyclic depths clamped to `MAX_SPAN_DEPTH`.
    for &idx in indices {
        let span = &mut spans[idx];
        if broken.contains(&span.span_id) {
            span.parent_span_id = None;
        }
        let mut depth = 0usize;
        let mut cur = parent_map.get(&span.span_id).and_then(|p| p.clone());
        let mut steps = 0usize;
        while let Some(pid) = cur {
            depth += 1;
            steps += 1;
            if depth >= MAX_SPAN_DEPTH || steps > indices.len() {
                break;
            }
            cur = parent_map.get(&pid).and_then(|p| p.clone());
        }
        span.depth = depth.min(MAX_SPAN_DEPTH) as i32;
    }
}

/// Sanitizes an entire `TraceIngestBatch`:
/// - Enforces `MAX_SPANS_PER_TRACE` (512) per `trace_id`, dropping excess spans.
/// - Sanitizes every retained span and trace via `policy`.
/// - Computes span depths & breaks cycles per `trace_id` (<= `MAX_SPAN_DEPTH`).
/// - Aggregates span `cost_nanos` into trace `total_cost_nanos` / `total_cost_usd` when spans are present.
pub fn sanitize_batch(batch: &mut TraceIngestBatch, policy: &AgentPrivacyPolicy) -> SanitizeStats {
    let mut stats = SanitizeStats::default();

    // 1. Enforce MAX_SPANS_PER_TRACE per trace_id while preserving order.
    let mut counts_per_trace: HashMap<String, usize> = HashMap::new();
    let mut retained_spans = Vec::with_capacity(batch.spans.len());
    for mut span in batch.spans.drain(..) {
        let count = counts_per_trace.entry(span.trace_id.clone()).or_insert(0);
        if *count >= MAX_SPANS_PER_TRACE {
            stats.dropped_spans += 1;
            continue;
        }
        *count += 1;
        stats.merge(sanitize_span(&mut span, policy));
        retained_spans.push(span);
    }
    batch.spans = retained_spans;

    // 2. Group span indices per trace_id to compute depths & aggregate costs without cross-trace ID collisions.
    let mut indices_by_trace: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, span) in batch.spans.iter().enumerate() {
        indices_by_trace
            .entry(span.trace_id.clone())
            .or_default()
            .push(idx);
    }

    let mut cost_by_trace: HashMap<String, i64> = HashMap::with_capacity(indices_by_trace.len());
    for (trace_id, indices) in &indices_by_trace {
        compute_span_depths_for_indices(&mut batch.spans, indices);
        let sum_nanos = indices
            .iter()
            .map(|&i| batch.spans[i].cost_nanos.max(0))
            .fold(0i64, |acc, c| acc.saturating_add(c));
        cost_by_trace.insert(trace_id.clone(), sum_nanos);
    }

    // 3. Sanitize traces and roll up span cost_nanos when positive.
    for trace in &mut batch.traces {
        stats.merge(sanitize_trace(trace, policy));
        if let Some(&span_cost_sum) = cost_by_trace.get(&trace.trace_id) {
            if span_cost_sum > 0 {
                trace.total_cost_nanos = span_cost_sum;
                trace.total_cost_usd = cost_nanos_to_usd(span_cost_sum);
            }
        }
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{SpanPhase, TraceStatus};

    fn dummy_span(span_id: &str, trace_id: &str, parent_span_id: Option<&str>) -> AgentSpan {
        AgentSpan {
            span_id: span_id.into(),
            trace_id: trace_id.into(),
            project_id: "p1".into(),
            parent_span_id: parent_span_id.map(str::to_string),
            linked_span_ids: vec![],
            span_phase: SpanPhase::Final,
            depth: 0,
            name: span_id.into(),
            span_type: SpanType::Llm,
            status: TraceStatus::Ok,
            start_ts_ms: 1000,
            end_ts_ms: Some(1100),
            duration_ms: 100,
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

    fn dummy_trace(trace_id: &str) -> AgentTrace {
        AgentTrace {
            trace_id: trace_id.into(),
            project_id: "p1".into(),
            agent_name: "agent".into(),
            agent_version: "1.0.0".into(),
            prompt_version: None,
            model_config_version: None,
            tool_config_version: None,
            app_release: None,
            environment: "production".into(),
            session_id: None,
            user_id: None,
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
        }
    }

    #[test]
    fn redacts_pii_and_sensitive_keys() {
        let mut val = json!({
            "user_email": "alice@example.com",
            "api_key": "super_secret_value",
            "prompt": "Call with sk-1234567890abcdef12345678 and ssn 123-45-6789"
        });
        let mut stats = SanitizeStats::default();
        redact_value(&mut val, true, &["api_key".to_string()], &mut stats);
        assert_eq!(val["api_key"], REDACTED_MARKER);
        assert_eq!(val["user_email"], REDACTED_MARKER);
        assert!(!val["prompt"].as_str().unwrap().contains("sk-1234567890"));
        assert!(!val["prompt"].as_str().unwrap().contains("123-45-6789"));
        assert!(stats.redacted_fields >= 3);
    }

    #[test]
    fn truncates_oversized_payload_safely_even_with_quotes_and_utf8() {
        let big = "a".repeat(MAX_PAYLOAD_BYTES + 5000);
        let mut val = json!({ "text": big });
        let mut stats = SanitizeStats::default();
        cap_payload(&mut val, MAX_PAYLOAD_BYTES, &mut stats);
        assert_eq!(stats.truncated_payloads, 1);
        assert_eq!(val["_truncated"], true);
        assert!(serde_json::to_string(&val).unwrap().len() <= MAX_PAYLOAD_BYTES);

        // Quote-heavy + multi-byte UTF-8 payload with tight budget
        let quote_heavy = "\"\\🚀".repeat(500);
        let mut qval = json!({ "text": quote_heavy });
        let mut qstats = SanitizeStats::default();
        cap_payload(&mut qval, 256, &mut qstats);
        let encoded = serde_json::to_string(&qval).unwrap();
        assert_eq!(qstats.truncated_payloads, 1);
        assert_eq!(qval["_truncated"], true);
        assert!(
            encoded.len() <= 256,
            "encoded length {} exceeded 256",
            encoded.len()
        );
    }

    #[test]
    fn breaks_indirect_cycle_without_orphaning_non_cyclic_descendants() {
        // C -> A <-> B (C listed first)
        let mut spans = vec![
            dummy_span("C", "tr_1", Some("A")),
            dummy_span("A", "tr_1", Some("B")),
            dummy_span("B", "tr_1", Some("A")),
        ];
        compute_span_depths(&mut spans);
        assert_eq!(spans[0].parent_span_id.as_deref(), Some("A"));
        assert_eq!(spans[1].parent_span_id.as_deref(), Some("B"));
        assert_eq!(spans[2].parent_span_id, None);
        assert_eq!(spans[2].depth, 0);
        assert_eq!(spans[1].depth, 1);
        assert_eq!(spans[0].depth, 2);
    }

    #[test]
    fn sanitize_batch_enforces_span_cap_links_stack_and_cost_rollup() {
        let policy = AgentPrivacyPolicy::default_for_project("p1");
        let mut spans = Vec::with_capacity(600);
        for i in 0..520 {
            let mut sp = dummy_span(&format!("sp_{i}"), "tr_cap", None);
            sp.cost_nanos = 10_000;
            sp.span_phase = if i == 0 {
                SpanPhase::Start
            } else {
                SpanPhase::Final
            };
            if i == 0 {
                sp.error_stack = Some("Panic for alice@example.com".into());
                sp.linked_span_ids = (0..100).map(|k| format!("lnk_{k}")).collect();
                sp.linked_span_ids.push("sp_0".into()); // self-link
                sp.linked_span_ids.push("  ".into()); // empty
                sp.linked_span_ids.push("lnk_1".into()); // duplicate
            }
            spans.push(sp);
        }

        let mut batch = TraceIngestBatch {
            traces: vec![dummy_trace("tr_cap")],
            spans,
        };
        let stats = sanitize_batch(&mut batch, &policy);
        assert_eq!(batch.spans.len(), MAX_SPANS_PER_TRACE);
        assert_eq!(stats.dropped_spans, 8);
        assert_eq!(batch.spans[0].span_phase, SpanPhase::Start);
        assert_eq!(
            batch.spans[0].linked_span_ids.len(),
            MAX_LINKED_SPANS_PER_SPAN
        );
        assert!(!batch.spans[0].linked_span_ids.contains(&"sp_0".to_string()));
        assert!(!batch.spans[0]
            .error_stack
            .as_deref()
            .unwrap()
            .contains("alice@example.com"));
        assert_eq!(
            batch.traces[0].total_cost_nanos,
            MAX_SPANS_PER_TRACE as i64 * 10_000
        );
    }
}
