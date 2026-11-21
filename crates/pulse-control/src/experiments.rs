use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use pulse_exec::{
    ExecError, ExperimentMetricEvent, ExperimentMetricQuery, MetricMoments, MetricValue,
    PropPredicate, QueryEngine, TimeRange,
};
use pulse_wire::{cohort_member_hash, evaluate_flag_rule, CohortGate, PropValue};
use xxhash_rust::xxh3::xxh3_64;

pub mod stats;

pub use stats::{
    compare_estimates, compare_means, compute_estimate_table, compute_lift_table,
    compute_sample_ratio_mismatch, compute_statistical_lift_table, estimates_from_moments,
    normal_quantile, sequential_rho, Comparison, Estimate, EstimateKind, LiftOptions, Moments,
};

use crate::models::{
    EdgeFlagBundle, EdgeFlagRule, Experiment, ExperimentMetricSpec, ExperimentResults, FeatureFlag,
    GuardrailResult, SegmentResult, TimeseriesPoint, TimeseriesVariant, VariantLiftRow,
};

/// Evaluates an `EdgeFlagRule` deterministically via the shared `pulse_wire::evaluate_flag_rule`
/// (`xxh3_64` bucketing + optional hashed cohort gate) (`P2-02`, `P2-03`).
pub fn evaluate_edge_flag(rule: &EdgeFlagRule, distinct_id: &str) -> Option<String> {
    // A targeted rule with no hashes matches no one (fail closed).
    let gate = rule
        .targeting_cohort_id
        .as_deref()
        .map(|cohort_id| CohortGate {
            cohort_id,
            member_hashes: rule.cohort_member_hashes.as_deref().unwrap_or_default(),
        });
    evaluate_flag_rule(
        &rule.key,
        rule.enabled,
        rule.rollout_bps,
        gate,
        rule.variants.iter().map(|v| (v.key.as_str(), v.weight_bps)),
        distinct_id,
    )
}

/// Builds an `EdgeFlagBundle` without a `QueryEngine` (for non-cohort unit callers).
/// Any flag carrying a `targeting_cohort_id` fails closed (`Some(vec![])`) rather than failing open to 100% of users.
pub fn build_edge_flag_bundle(
    project_id: &str,
    flags: &[FeatureFlag],
    now_ms: i64,
) -> EdgeFlagBundle {
    let rules: Vec<EdgeFlagRule> = flags
        .iter()
        .map(|f| {
            let cohort_id = f
                .targeting_cohort_id
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.to_string());
            let empty = cohort_id.as_ref().map(|_| Vec::new());
            EdgeFlagRule {
                key: f.key.clone(),
                enabled: f.enabled,
                rollout_bps: ((f.rollout_pct.clamp(0.0, 100.0) * 100.0).round()) as u32,
                targeting_cohort_id: cohort_id,
                cohort_members: empty.clone(),
                cohort_member_hashes: empty,
                variants: f.variants.clone(),
            }
        })
        .collect();
    finalize_bundle(project_id, rules, now_ms)
}

/// Builds an `EdgeFlagBundle` evaluating any `targeting_cohort_id` through `QueryEngine` into exact
/// hashed member lists; raw ids never leave the server.
pub async fn build_edge_flag_bundle_with_cohorts(
    project_id: &str,
    flags: &[FeatureFlag],
    qengine: &QueryEngine,
    now_ms: i64,
) -> Result<EdgeFlagBundle, ExecError> {
    let mut cohort_cache: HashMap<String, Vec<String>> = HashMap::new();
    let mut rules = Vec::with_capacity(flags.len());

    for f in flags {
        let cohort_id = f
            .targeting_cohort_id
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string());

        let cohort_member_hashes = if let Some(cid) = &cohort_id {
            if let Some(cached) = cohort_cache.get(cid) {
                Some(cached.clone())
            } else {
                let members = qengine
                    .evaluate_saved_cohort_external_ids(project_id, cid)
                    .await?;
                let mut hashes: Vec<String> =
                    members.iter().map(|m| cohort_member_hash(cid, m)).collect();
                hashes.sort_unstable();
                hashes.dedup();
                cohort_cache.insert(cid.clone(), hashes.clone());
                Some(hashes)
            }
        } else {
            None
        };

        rules.push(EdgeFlagRule {
            key: f.key.clone(),
            enabled: f.enabled,
            rollout_bps: ((f.rollout_pct.clamp(0.0, 100.0) * 100.0).round()) as u32,
            cohort_members: cohort_id.as_ref().map(|_| Vec::new()),
            targeting_cohort_id: cohort_id,
            cohort_member_hashes,
            variants: f.variants.clone(),
        });
    }

    Ok(finalize_bundle(project_id, rules, now_ms))
}

fn finalize_bundle(project_id: &str, rules: Vec<EdgeFlagRule>, now_ms: i64) -> EdgeFlagBundle {
    let json_bytes = serde_json::to_vec(&rules).unwrap_or_default();
    let etag = format!("W/\"{:016x}\"", xxh3_64(&json_bytes));
    EdgeFlagBundle {
        project_id: project_id.to_string(),
        etag,
        generated_at_ms: now_ms,
        flags: rules,
    }
}

/// Evaluates an experiment's primary metric and guardrails across variants: per-user metric
/// values after each user's first exposure (`QueryEngine::query_experiment_metric`), a Chi-square
/// SRM check, and the lift table configured by `exp.analysis` (fixed-horizon or sequential,
/// optional CUPED, Holm-Bonferroni across treatments). `segment_by` adds exploratory
/// per-segment results for the primary metric.
pub async fn evaluate_experiment_results(
    exp: &Experiment,
    flag: Option<&FeatureFlag>,
    qengine: &QueryEngine,
    segment_by: Option<&str>,
) -> Result<ExperimentResults, ExecError> {
    let t0 = Instant::now();

    let expected_weights: BTreeMap<String, u32> = match flag {
        Some(f) if !f.variants.is_empty() => f
            .variants
            .iter()
            .map(|v| (v.key.clone(), v.weight_bps))
            .collect(),
        _ => BTreeMap::from([
            ("control".to_string(), 5000),
            ("treatment".to_string(), 5000),
        ]),
    };
    let variant_keys: Vec<String> = expected_weights.keys().cloned().collect();

    let opts = lift_options(exp, variant_keys.len());
    let mut primary = evaluate_metric(
        exp,
        &exp.primary_metric,
        &variant_keys,
        opts,
        segment_by,
        qengine,
    )
    .await?;

    let observed: BTreeMap<String, (u64, u64)> = primary
        .moments
        .iter()
        .map(|(k, m)| (k.clone(), (m.n, 0)))
        .collect();
    let (srm_p_value, sample_ratio_mismatch) =
        compute_sample_ratio_mismatch(&observed, &expected_weights);
    if sample_ratio_mismatch {
        for r in &mut primary.rows {
            r.statistically_significant = false;
        }
    }

    let mut guardrails = Vec::with_capacity(exp.guardrail_metrics.len());
    for gm in &exp.guardrail_metrics {
        let mut gm_rows = evaluate_metric(exp, gm, &variant_keys, opts, None, qengine)
            .await?
            .rows;
        if sample_ratio_mismatch {
            for r in &mut gm_rows {
                r.statistically_significant = false;
            }
        }
        let increase_is_bad = gm.direction == "increase_is_bad";
        let regressed_variants = gm_rows
            .iter()
            .filter(|r| {
                !r.is_control
                    && r.statistically_significant
                    && (r.abs_diff > 0.0) == increase_is_bad
                    && r.abs_diff != 0.0
            })
            .map(|r| r.variant_key.clone())
            .collect();
        guardrails.push(GuardrailResult {
            metric_name: gm.name.clone(),
            event_name: gm.event_name.clone(),
            direction: gm.direction.clone(),
            regressed_variants,
            metric_type: normalized_metric_type(gm).to_string(),
            variants: gm_rows,
        });
    }

    let cuped_applied = primary.variance_reduction_pct.is_some();
    Ok(ExperimentResults {
        experiment_id: exp.id.clone(),
        experiment_name: exp.name.clone(),
        flag_key: exp.flag_key.clone(),
        status: exp.status.clone(),
        hypothesis: exp.hypothesis.clone(),
        metric_name: exp.primary_metric.name.clone(),
        exec_time_us: t0.elapsed().as_micros() as u64,
        srm_p_value,
        sample_ratio_mismatch,
        statistical_methodology_note: methodology_note(opts, cuped_applied),
        sequential: opts.sequential_rho.is_some(),
        confidence_level: 1.0 - opts.alpha,
        variants: primary.rows,
        guardrails,
        metric_type: normalized_metric_type(&exp.primary_metric).to_string(),
        cuped_applied,
        variance_reduction_pct: primary.variance_reduction_pct.unwrap_or(0.0),
        multi_variant_users: primary.multi_variant_users,
        segment_by: segment_by.map(str::to_string),
        segments: primary.segments,
        timeseries: primary.timeseries,
    })
}

/// Sequential tests are tuned to the planned sample (10,000 users per variant when unset).
fn lift_options(exp: &Experiment, variants: usize) -> LiftOptions {
    let alpha = if exp.analysis.alpha > 0.0 && exp.analysis.alpha < 0.5 {
        exp.analysis.alpha
    } else {
        0.05
    };
    let planned_total =
        exp.analysis.planned_users_per_variant.unwrap_or(10_000) * variants.max(2) as u64;
    LiftOptions {
        alpha,
        sequential_rho: exp
            .analysis
            .sequential
            .then(|| sequential_rho(alpha, planned_total)),
    }
}

fn methodology_note(opts: LiftOptions, cuped: bool) -> String {
    let level = format!("{:.0}%", (1.0 - opts.alpha) * 100.0);
    let cuped = if cuped {
        "; CUPED variance reduction using each user's pre-exposure value of the metric"
    } else {
        ""
    };
    match opts.sequential_rho {
        Some(_) => format!(
            "Sequential: always-valid p-values and {level} confidence sequences (Waudby-Smith et al. asymptotic CS), safe to check at any time; Holm-Bonferroni adjusted across treatment variants{cuped}; Chi-square SRM check at alpha=0.001"
        ),
        None => format!(
            "Fixed-horizon two-sided test (not peeking-corrected: read results once, at the planned end); Holm-Bonferroni adjusted across treatment variants; {level} intervals{cuped}; Chi-square SRM check at alpha=0.001"
        ),
    }
}

/// `conversion`, `count`, `sum` or `ratio`; older `continuous` rows map to `sum` or `count`.
pub fn normalized_metric_type(m: &ExperimentMetricSpec) -> &'static str {
    match m.metric_type.as_str() {
        "count" => "count",
        "sum" => "sum",
        "ratio" => "ratio",
        "continuous" if m.numeric_prop.is_some() => "sum",
        "continuous" => "count",
        _ => "conversion",
    }
}

/// Checks a metric definition is complete for its type.
pub fn validate_metric_spec(m: &ExperimentMetricSpec) -> Result<(), String> {
    if !matches!(
        m.metric_type.as_str(),
        "conversion" | "count" | "sum" | "ratio" | "continuous"
    ) {
        return Err("metric_type must be conversion, count, sum or ratio".into());
    }
    if !matches!(m.direction.as_str(), "decrease_is_bad" | "increase_is_bad") {
        return Err("metric direction must be decrease_is_bad or increase_is_bad".into());
    }
    let has_prop = m
        .numeric_prop
        .as_deref()
        .is_some_and(|p| !p.trim().is_empty());
    if m.metric_type == "sum" && !has_prop {
        return Err(format!(
            "metric '{}' is a sum and needs numeric_prop",
            m.name
        ));
    }
    if m.metric_type == "ratio"
        && m.denominator
            .as_ref()
            .is_none_or(|d| d.event_name.trim().is_empty())
    {
        return Err(format!(
            "metric '{}' is a ratio and needs a denominator",
            m.name
        ));
    }
    Ok(())
}

fn metric_event(
    event_name: &str,
    filters: &[PropPredicate],
    prop: Option<&str>,
    binary: bool,
) -> ExperimentMetricEvent {
    ExperimentMetricEvent {
        event_name: event_name.to_string(),
        filters: filters.to_vec(),
        value: match prop.filter(|p| !p.trim().is_empty()) {
            _ if binary => MetricValue::Binary,
            Some(p) => MetricValue::Sum(p.to_string()),
            None => MetricValue::Count,
        },
    }
}

struct MetricEval {
    rows: Vec<VariantLiftRow>,
    moments: BTreeMap<String, MetricMoments>,
    variance_reduction_pct: Option<f64>,
    multi_variant_users: u64,
    segments: Vec<SegmentResult>,
    timeseries: Vec<TimeseriesPoint>,
}

async fn evaluate_metric(
    exp: &Experiment,
    metric: &ExperimentMetricSpec,
    expected_variants: &[String],
    opts: LiftOptions,
    segment_by: Option<&str>,
    qengine: &QueryEngine,
) -> Result<MetricEval, ExecError> {
    let kind = normalized_metric_type(metric);
    let ratio = kind == "ratio";
    let estimate_kind = if ratio {
        EstimateKind::Ratio
    } else {
        EstimateKind::Mean
    };
    let cuped = exp.analysis.cuped && !ratio;
    // Respect the exact configured window, including sub-24h windows.
    let window_ms = if metric.conversion_window_ms > 0 {
        metric.conversion_window_ms
    } else {
        7 * 86_400_000
    };

    let q = ExperimentMetricQuery {
        time_range: TimeRange {
            start_ts_ms: exp.start_ts_ms,
            end_ts_ms: exp.end_ts_ms,
        },
        exposure_event: "$experiment_started".to_string(),
        exposure_filters: vec![PropPredicate {
            prop_name: "flag_key".to_string(),
            filter: pulse_exec::PropFilterOp::Eq(PropValue::Str(exp.flag_key.clone())),
        }],
        variant_prop: "variant".to_string(),
        segment_by: segment_by.map(str::to_string),
        metric: metric_event(
            &metric.event_name,
            &metric.filters,
            metric.numeric_prop.as_deref(),
            kind == "conversion",
        ),
        denominator: metric
            .denominator
            .as_ref()
            .filter(|_| ratio)
            .map(|d| metric_event(&d.event_name, &[], d.numeric_prop.as_deref(), false)),
        window_ms,
        pre_window_ms: if cuped {
            exp.analysis.cuped_pre_window_ms.max(0)
        } else {
            0
        },
    };
    let res = qengine.query_experiment_metric(&exp.project_id, &q).await?;

    // Every configured variant shows, even before anyone sees it.
    let mut moments: BTreeMap<String, MetricMoments> = expected_variants
        .iter()
        .map(|k| (k.clone(), MetricMoments::default()))
        .collect();
    moments.extend(res.variants);

    let (estimates, reduction) = estimates_from_moments(&moments, estimate_kind, cuped);
    // Plain conversion metrics keep the exact Fieller / Beta-Binomial table.
    let rows = if kind == "conversion" && reduction.is_none() {
        let counts = moments
            .iter()
            .map(|(k, m)| (k.clone(), (m.n, m.sum_y.round() as u64)))
            .collect();
        compute_lift_table(&counts, opts)
    } else {
        compute_estimate_table(&moments, &estimates, opts)
    };

    let segments = res
        .segments
        .into_iter()
        .map(|g| {
            let (est, _) = estimates_from_moments(&g.variants, estimate_kind, cuped);
            SegmentResult {
                segment: g.segment,
                variants: compute_estimate_table(&g.variants, &est, opts),
            }
        })
        .collect();

    let timeseries = res
        .daily
        .into_iter()
        .map(|p| {
            let (est, _) = estimates_from_moments(&p.cumulative, estimate_kind, cuped);
            TimeseriesPoint {
                day_start_ms: p.day_start_ms,
                variants: compute_estimate_table(&p.cumulative, &est, opts)
                    .into_iter()
                    .map(|r| TimeseriesVariant {
                        variant_key: r.variant_key,
                        is_control: r.is_control,
                        users: r.sample_size,
                        mean_value: r.mean_value,
                        relative_lift_pct: r.relative_lift_pct,
                        ci_low_pct: r.ci95_low_pct,
                        ci_high_pct: r.ci95_high_pct,
                    })
                    .collect(),
            }
        })
        .collect();

    Ok(MetricEval {
        rows,
        moments,
        variance_reduction_pct: reduction,
        multi_variant_users: res.multi_variant_users,
        segments,
        timeseries,
    })
}
