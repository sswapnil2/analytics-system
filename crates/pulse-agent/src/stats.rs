use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::{AgentSpan, FailureSource, LatencyPercentiles, SpanType, TraceStatus};
use crate::pricing::cost_nanos_to_usd;
use crate::MIN_SIGNIFICANCE_SAMPLES;

/// Computes exact linear-interpolated percentiles over a slice of durations (ms).
pub fn compute_latency_percentiles(samples: &[i64]) -> LatencyPercentiles {
    if samples.is_empty() {
        return LatencyPercentiles {
            avg_ms: 0.0,
            p50_ms: 0.0,
            p90_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
        };
    }
    let mut sorted: Vec<f64> = samples.iter().map(|&v| v.max(0) as f64).collect();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let sum: f64 = sorted.iter().sum();
    let avg_ms = sum / sorted.len() as f64;
    LatencyPercentiles {
        avg_ms: round2(avg_ms),
        p50_ms: round2(percentile_sorted(&sorted, 0.50)),
        p90_ms: round2(percentile_sorted(&sorted, 0.90)),
        p95_ms: round2(percentile_sorted(&sorted, 0.95)),
        p99_ms: round2(percentile_sorted(&sorted, 0.99)),
    }
}

pub fn percentile_sorted(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = p.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = idx - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// Sums non-negative integer `cost_nanos` values using saturating arithmetic.
pub fn sum_cost_nanos(nanos: impl IntoIterator<Item = i64>) -> i64 {
    nanos
        .into_iter()
        .fold(0i64, |acc, n| acc.saturating_add(n.max(0)))
}

#[inline]
fn loop_span_type_rank(st: SpanType) -> u8 {
    match st {
        SpanType::Custom => 0,
        other => other.to_u8().saturating_add(1),
    }
}

/// Detects potential agent execution loops from a trace's span list:
/// - Deduplicates `span_id` across streaming `SpanPhase` (`Final > Update > Start`) first,
///   merging `tool_name` with phase-ranked CRDT semantics (higher phase overrides lower phase
///   when `Some(...)`, backfilling `None` from lower phase, preserving `Some("")`).
/// - Same tool invoked >= 4 times consecutively, OR
/// - Same tool invoked >= 6 times total in a single trace, OR
/// - >= 12 LLM generation calls in a single trace.
pub fn detect_execution_loop(spans: &[AgentSpan]) -> Option<String> {
    struct LoopSpan<'a> {
        span_id: &'a str,
        span_type: SpanType,
        span_phase_prio: u8,
        start_ts_ms: i64,
        tool_name: Option<&'a str>,
        tool_name_rank: Option<(u8, i64)>,
        name: &'a str,
    }

    let mut deduped: HashMap<&str, LoopSpan<'_>> = HashMap::with_capacity(spans.len());
    let mut unkeyed: Vec<LoopSpan<'_>> = Vec::new();
    for sp in spans {
        let sp_rank = (sp.span_phase.priority(), sp.start_ts_ms);
        let sp_tool_rank = sp.tool_name.as_ref().map(|_| sp_rank);
        let entry_val = LoopSpan {
            span_id: sp.span_id.as_str(),
            span_type: sp.span_type,
            span_phase_prio: sp.span_phase.priority(),
            start_ts_ms: sp.start_ts_ms,
            tool_name: sp.tool_name.as_deref(),
            tool_name_rank: sp_tool_rank,
            name: sp.name.as_str(),
        };
        if sp.span_id.is_empty() {
            unkeyed.push(entry_val);
        } else {
            deduped
                .entry(sp.span_id.as_str())
                .and_modify(|cur| {
                    let old_rank = (cur.span_phase_prio, cur.start_ts_ms);
                    if let Some(tn) = entry_val.tool_name {
                        let take_tool = match cur.tool_name {
                            None => true,
                            Some(cur_tn) => {
                                entry_val.tool_name_rank > cur.tool_name_rank
                                    || (entry_val.tool_name_rank == cur.tool_name_rank
                                        && tn > cur_tn)
                            }
                        };
                        if take_tool {
                            cur.tool_name = Some(tn);
                            cur.tool_name_rank = entry_val.tool_name_rank;
                        }
                    }
                    if sp_rank > old_rank {
                        cur.span_type = entry_val.span_type;
                        cur.span_phase_prio = entry_val.span_phase_prio;
                        cur.start_ts_ms = entry_val.start_ts_ms;
                        cur.name = entry_val.name;
                    } else if sp_rank == old_rank {
                        if loop_span_type_rank(entry_val.span_type)
                            > loop_span_type_rank(cur.span_type)
                        {
                            cur.span_type = entry_val.span_type;
                        }
                        if cur.name.is_empty()
                            || (!entry_val.name.is_empty() && entry_val.name > cur.name)
                        {
                            cur.name = entry_val.name;
                        }
                    }
                })
                .or_insert(entry_val);
        }
    }

    let mut ordered: Vec<LoopSpan<'_>> = deduped.into_values().chain(unkeyed).collect();
    ordered.sort_by(|a, b| {
        a.start_ts_ms
            .cmp(&b.start_ts_ms)
            .then_with(|| a.span_id.cmp(b.span_id))
            .then_with(|| a.tool_name.cmp(&b.tool_name))
            .then_with(|| a.name.cmp(b.name))
    });

    let mut llm_count = 0usize;
    let mut tool_counts: HashMap<&str, usize> = HashMap::new();
    let mut last_tool: Option<&str> = None;
    let mut consecutive = 0usize;

    for sp in ordered {
        match sp.span_type {
            SpanType::Llm => llm_count += 1,
            SpanType::Tool => {
                let name = sp.tool_name.unwrap_or(sp.name);
                let total = tool_counts.entry(name).or_insert(0);
                *total += 1;
                if last_tool == Some(name) {
                    consecutive += 1;
                } else {
                    last_tool = Some(name);
                    consecutive = 1;
                }
                if consecutive >= 4 {
                    return Some(format!(
                        "Tool '{name}' was invoked {consecutive} times consecutively"
                    ));
                }
                if *total >= 6 {
                    return Some(format!(
                        "Tool '{name}' was invoked {total} times in a single trace"
                    ));
                }
            }
            _ => {}
        }
    }
    if llm_count >= 12 {
        return Some(format!(
            "Trace executed {llm_count} LLM generation calls (threshold: 12)"
        ));
    }
    None
}

/// Infers the most likely root-cause failure source from failed spans (deepest/earliest error span).
pub fn classify_failure_source(spans: &[AgentSpan]) -> FailureSource {
    let mut err_spans: Vec<&AgentSpan> = spans
        .iter()
        .filter(|s| matches!(s.status, TraceStatus::Error | TraceStatus::Timeout))
        .collect();
    if err_spans.is_empty() {
        return FailureSource::Unknown;
    }
    // Prefer deepest span first (root cause), then earliest timestamp, then deterministic span_id tie-break
    err_spans.sort_by(|a, b| {
        b.depth
            .cmp(&a.depth)
            .then_with(|| a.start_ts_ms.cmp(&b.start_ts_ms))
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    let root = err_spans[0];
    match root.span_type {
        SpanType::Llm | SpanType::Embedding => FailureSource::Model,
        SpanType::Tool => FailureSource::Tool,
        SpanType::Http | SpanType::Db | SpanType::Retrieval => FailureSource::External,
        SpanType::Agent | SpanType::Workflow | SpanType::Guardrail | SpanType::Custom => {
            FailureSource::Application
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDelta {
    pub baseline: f64,
    pub candidate: f64,
    pub absolute_delta: f64,
    pub relative_delta_pct: Option<f64>,
    pub sample_size_baseline: usize,
    pub sample_size_candidate: usize,
    /// `significant_improvement | significant_regression | no_significant_change | insufficient_data`
    pub verdict: String,
    pub p_value: Option<f64>,
    pub note: String,
}

/// Compares two continuous metric distributions using Welch's t-test.
/// Filters non-finite (`NaN`/`Inf`) samples and never claims statistical significance when either
/// group has fewer than `MIN_SIGNIFICANCE_SAMPLES` valid finite samples.
pub fn compare_continuous_metric(
    baseline: &[f64],
    candidate: &[f64],
    higher_is_better: bool,
) -> MetricDelta {
    let b_clean: Vec<f64> = baseline.iter().copied().filter(|v| v.is_finite()).collect();
    let c_clean: Vec<f64> = candidate
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();

    let n1 = b_clean.len();
    let n2 = c_clean.len();
    let m1 = mean(&b_clean);
    let m2 = mean(&c_clean);
    let abs_delta = m2 - m1;
    let rel_pct = if m1.abs() > 1e-12 && abs_delta.is_finite() {
        Some((abs_delta / m1) * 100.0)
    } else {
        None
    };

    if n1 < MIN_SIGNIFICANCE_SAMPLES || n2 < MIN_SIGNIFICANCE_SAMPLES {
        return MetricDelta {
            baseline: m1,
            candidate: m2,
            absolute_delta: abs_delta,
            relative_delta_pct: rel_pct,
            sample_size_baseline: n1,
            sample_size_candidate: n2,
            verdict: "insufficient_data".to_string(),
            p_value: None,
            note: format!(
                "Need at least {MIN_SIGNIFICANCE_SAMPLES} finite samples per group for significance testing (have {n1} vs {n2})."
            ),
        };
    }

    let v1 = sample_variance(&b_clean, m1);
    let v2 = sample_variance(&c_clean, m2);
    let se = (v1 / n1 as f64 + v2 / n2 as f64).sqrt();
    if !se.is_finite() || !m1.is_finite() || !m2.is_finite() || !abs_delta.is_finite() {
        return MetricDelta {
            baseline: if m1.is_finite() { m1 } else { 0.0 },
            candidate: if m2.is_finite() { m2 } else { 0.0 },
            absolute_delta: 0.0,
            relative_delta_pct: None,
            sample_size_baseline: n1,
            sample_size_candidate: n2,
            verdict: "insufficient_data".to_string(),
            p_value: None,
            note: "Non-finite intermediate statistics prevented significance testing.".to_string(),
        };
    }

    if se <= 1e-12 {
        let verdict = if abs_delta.abs() <= 1e-12 {
            "no_significant_change"
        } else if (higher_is_better && abs_delta > 0.0) || (!higher_is_better && abs_delta < 0.0) {
            "significant_improvement"
        } else {
            "significant_regression"
        };
        return MetricDelta {
            baseline: m1,
            candidate: m2,
            absolute_delta: abs_delta,
            relative_delta_pct: rel_pct,
            sample_size_baseline: n1,
            sample_size_candidate: n2,
            verdict: verdict.to_string(),
            p_value: Some(if abs_delta.abs() <= 1e-12 { 1.0 } else { 0.0 }),
            note: "Zero variance in both groups.".to_string(),
        };
    }

    let z = (abs_delta / se).abs();
    // Normal approximation to two-tailed Welch's t (accurate for n >= 20)
    let p_value = erfc_approx(z / std::f64::consts::SQRT_2).clamp(0.0, 1.0);
    if !p_value.is_finite() {
        return MetricDelta {
            baseline: m1,
            candidate: m2,
            absolute_delta: abs_delta,
            relative_delta_pct: rel_pct,
            sample_size_baseline: n1,
            sample_size_candidate: n2,
            verdict: "insufficient_data".to_string(),
            p_value: None,
            note: "Non-finite p-value.".to_string(),
        };
    }

    let verdict = if p_value >= 0.05 {
        "no_significant_change"
    } else if (higher_is_better && abs_delta > 0.0) || (!higher_is_better && abs_delta < 0.0) {
        "significant_improvement"
    } else {
        "significant_regression"
    };

    MetricDelta {
        baseline: m1,
        candidate: m2,
        absolute_delta: abs_delta,
        relative_delta_pct: rel_pct,
        sample_size_baseline: n1,
        sample_size_candidate: n2,
        verdict: verdict.to_string(),
        p_value: Some((p_value * 10_000.0).round() / 10_000.0),
        note: format!("Two-tailed Welch test (alpha=0.05, p={p_value:.4})."),
    }
}

/// Compares baseline vs candidate `cost_nanos` slices (lower cost is better).
pub fn compare_cost_nanos_metric(baseline_nanos: &[i64], candidate_nanos: &[i64]) -> MetricDelta {
    let b: Vec<f64> = baseline_nanos
        .iter()
        .map(|&n| cost_nanos_to_usd(n.max(0)))
        .collect();
    let c: Vec<f64> = candidate_nanos
        .iter()
        .map(|&n| cost_nanos_to_usd(n.max(0)))
        .collect();
    compare_continuous_metric(&b, &c, false)
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

fn sample_variance(xs: &[f64], mean: f64) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (xs.len() - 1) as f64
}

/// Abramowitz & Stegun 7.1.26 complementary error function approximation (max error 1.5e-7).
fn erfc_approx(x: f64) -> f64 {
    let z = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * z);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let r = poly * (-z * z).exp();
    if x >= 0.0 {
        r
    } else {
        2.0 - r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SpanPhase;
    use serde_json::Value;

    fn make_span(
        id: &str,
        span_type: SpanType,
        phase: SpanPhase,
        status: TraceStatus,
    ) -> AgentSpan {
        AgentSpan {
            span_id: id.into(),
            trace_id: "tr_1".into(),
            project_id: "p1".into(),
            parent_span_id: None,
            linked_span_ids: vec![],
            span_phase: phase,
            depth: 0,
            name: id.into(),
            span_type,
            status,
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

    #[test]
    fn guards_significance_on_small_sample_sizes_and_nan() {
        let delta = compare_continuous_metric(&[1.0, 2.0, 3.0], &[10.0, 11.0, 12.0], true);
        assert_eq!(delta.verdict, "insufficient_data");
        assert!(delta.p_value.is_none());

        let nan_base = vec![f64::NAN; 25];
        let cand = vec![1.0; 25];
        let nan_delta = compare_continuous_metric(&nan_base, &cand, false);
        assert_eq!(nan_delta.verdict, "insufficient_data");
        assert!(nan_delta.p_value.is_none());
    }

    #[test]
    fn detects_real_regression_on_sufficient_samples() {
        let base: Vec<f64> = (0..30).map(|i| 100.0 + (i % 5) as f64).collect();
        let cand: Vec<f64> = (0..30).map(|i| 250.0 + (i % 5) as f64).collect();
        let delta = compare_continuous_metric(&base, &cand, false);
        assert_eq!(delta.verdict, "significant_regression");
        assert!(delta.p_value.unwrap() < 0.001);
    }

    #[test]
    fn classifies_guardrail_as_application_failure_and_dedups_span_phases_in_loop_check() {
        let guard = make_span(
            "g1",
            SpanType::Guardrail,
            SpanPhase::Final,
            TraceStatus::Error,
        );
        assert_eq!(
            classify_failure_source(&[guard]),
            FailureSource::Application
        );

        // 10 LLM spans emitted across Start + Update + Final = 30 records, but only 10 unique span_ids
        let mut spans = Vec::new();
        for i in 0..10 {
            for phase in [SpanPhase::Start, SpanPhase::Update, SpanPhase::Final] {
                let mut sp = make_span(&format!("llm_{i}"), SpanType::Llm, phase, TraceStatus::Ok);
                sp.start_ts_ms = 1000 + i * 10;
                spans.push(sp);
            }
        }
        assert!(
            detect_execution_loop(&spans).is_none(),
            "streaming phases for same span_id must be deduplicated"
        );
    }

    #[test]
    fn sums_and_compares_cost_nanos() {
        assert_eq!(sum_cost_nanos([100, -50, 200]), 300);
        let base = vec![1_000_000i64; 25];
        let cand = vec![500_000i64; 25];
        let delta = compare_cost_nanos_metric(&base, &cand);
        assert_eq!(delta.verdict, "significant_improvement");
    }
}
