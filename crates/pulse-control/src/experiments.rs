use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use pulse_exec::{
    ExecError, FunnelOrderMode, FunnelQuery, FunnelStepSpec, PropPredicate, QueryEngine, TimeRange,
};
use pulse_wire::{cohort_member_hash, evaluate_flag_rule, CohortGate, PropValue};
use xxhash_rust::xxh3::xxh3_64;

pub mod stats;

pub use stats::{
    compare_means, compute_lift_table, compute_sample_ratio_mismatch,
    compute_statistical_lift_table, normal_quantile, sequential_rho, Comparison, LiftOptions,
    Moments,
};

use crate::models::{
    EdgeFlagBundle, EdgeFlagRule, Experiment, ExperimentMetricSpec, ExperimentResults, FeatureFlag,
    GuardrailResult, VariantLiftRow,
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

/// Evaluates an A/B experiment's exposure & conversion lift across variants using `QueryEngine`
/// plus Chi-square SRM check and the lift table configured by `exp.analysis`
/// (fixed-horizon or sequential, Holm-Bonferroni across treatments, Bayesian P(beat control)).
pub async fn evaluate_experiment_results(
    exp: &Experiment,
    flag: Option<&FeatureFlag>,
    qengine: &QueryEngine,
) -> Result<ExperimentResults, ExecError> {
    let t0 = Instant::now();

    let (variant_keys, expected_weights): (Vec<String>, BTreeMap<String, u32>) =
        if let Some(f) = flag {
            if f.variants.is_empty() {
                let mut m = BTreeMap::new();
                m.insert("control".to_string(), 5000);
                m.insert("treatment".to_string(), 5000);
                (vec!["control".to_string(), "treatment".to_string()], m)
            } else {
                let keys = f.variants.iter().map(|v| v.key.clone()).collect();
                let weights = f
                    .variants
                    .iter()
                    .map(|v| (v.key.clone(), v.weight_bps))
                    .collect();
                (keys, weights)
            }
        } else {
            let mut m = BTreeMap::new();
            m.insert("control".to_string(), 5000);
            m.insert("treatment".to_string(), 5000);
            (vec!["control".to_string(), "treatment".to_string()], m)
        };

    let opts = lift_options(exp, variant_keys.len());
    let (mut primary_rows, observed_counts) =
        evaluate_metric_for_experiment(exp, &exp.primary_metric, &variant_keys, opts, qengine)
            .await?;

    let (srm_p_value, sample_ratio_mismatch) =
        compute_sample_ratio_mismatch(&observed_counts, &expected_weights);
    if sample_ratio_mismatch {
        for r in &mut primary_rows {
            r.statistically_significant = false;
        }
    }

    let mut guardrails = Vec::with_capacity(exp.guardrail_metrics.len());
    for gm in &exp.guardrail_metrics {
        let (mut gm_rows, _) =
            evaluate_metric_for_experiment(exp, gm, &variant_keys, opts, qengine).await?;
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
            variants: gm_rows,
        });
    }

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
        statistical_methodology_note: methodology_note(opts),
        sequential: opts.sequential_rho.is_some(),
        confidence_level: 1.0 - opts.alpha,
        variants: primary_rows,
        guardrails,
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

fn methodology_note(opts: LiftOptions) -> String {
    let level = format!("{:.0}%", (1.0 - opts.alpha) * 100.0);
    match opts.sequential_rho {
        Some(_) => format!(
            "Sequential: always-valid p-values and {level} confidence sequences (Waudby-Smith et al. asymptotic CS), safe to check at any time; Holm-Bonferroni adjusted across treatment variants; Chi-square SRM check at alpha=0.001"
        ),
        None => format!(
            "Fixed-horizon two-sided Z-test (not peeking-corrected: read results once, at the planned end); Holm-Bonferroni adjusted across treatment variants; Fieller relative lift {level} CI; Chi-square SRM check at alpha=0.001"
        ),
    }
}

async fn evaluate_metric_for_experiment(
    exp: &Experiment,
    metric: &ExperimentMetricSpec,
    expected_variants: &[String],
    opts: LiftOptions,
    qengine: &QueryEngine,
) -> Result<(Vec<VariantLiftRow>, BTreeMap<String, (u64, u64)>), ExecError> {
    let range = TimeRange {
        start_ts_ms: exp.start_ts_ms,
        end_ts_ms: exp.end_ts_ms,
    };
    // Respect exact configured conversion window, including sub-24h windows (`P2-09f`)!
    let window_ms = if metric.conversion_window_ms > 0 {
        metric.conversion_window_ms
    } else {
        7 * 86_400_000
    };

    let funnel_from_exposure = FunnelQuery {
        time_range: range,
        steps: vec![
            FunnelStepSpec {
                event_name: "$experiment_started".to_string(),
                filters: vec![PropPredicate {
                    prop_name: "flag_key".to_string(),
                    filter: pulse_exec::PropFilterOp::Eq(PropValue::Str(exp.flag_key.clone())),
                }],
                first_time_ever: false,
            },
            FunnelStepSpec {
                event_name: metric.event_name.clone(),
                filters: Vec::new(),
                first_time_ever: false,
            },
        ],
        order_mode: FunnelOrderMode::LooseOrder,
        conversion_window_ms: window_ms,
        exclusions: Vec::new(),
        hold_constant_props: Vec::new(),
        breakdown_by: Some("variant".to_string()),
        global_filter: pulse_exec::GlobalFilter::default(),
        sampling: pulse_exec::SamplingMode::Exact,
    };

    let mut counts_by_variant: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for vk in expected_variants {
        counts_by_variant.insert(vk.clone(), (0, 0));
    }

    let res1 = qengine
        .query_funnel(&exp.project_id, &funnel_from_exposure)
        .await?;
    for bg in &res1.breakdowns {
        if bg.breakdown_value != "(none)" && !bg.steps.is_empty() {
            let n = bg.steps[0].entered_users;
            let conv = bg.steps.get(1).map(|s| s.entered_users).unwrap_or(0);
            counts_by_variant.insert(bg.breakdown_value.clone(), (n, conv));
        }
    }

    // Exposures come only from `$experiment_started`; with none, every variant reports 0 users
    // and the Studio says how to send them.
    let rows = compute_lift_table(&counts_by_variant, opts);
    Ok((rows, counts_by_variant))
}
