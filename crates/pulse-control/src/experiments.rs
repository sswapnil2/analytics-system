use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use pulse_exec::{
    ExecError, ExperimentMetricEvent, ExperimentMetricQuery, MetricMoments, MetricValue,
    PropPredicate, QueryEngine, TimeRange,
};
use pulse_wire::{cohort_member_hash, CohortGate, PropValue};
use xxhash_rust::xxh3::xxh3_64;

pub mod bandit;
pub mod stats;

pub use stats::{
    compare_estimates, compare_means, compute_estimate_table, compute_lift_table,
    compute_sample_ratio_mismatch, compute_statistical_lift_table, estimates_from_moments,
    normal_quantile, sequential_rho, Comparison, Estimate, EstimateKind, LiftOptions, Moments,
};

use crate::models::{
    BanditArm, BanditResults, EdgeFlagBundle, EdgeFlagRule, Experiment, ExperimentMetricSpec,
    ExperimentResults, FeatureFlag, GuardrailResult, SegmentResult, TimeseriesPoint,
    TimeseriesVariant, VariantLiftRow, WeightSnapshot,
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
    pulse_wire::evaluate_placed_flag_rule(
        &rule.key,
        rule.enabled,
        rule.rollout_bps,
        gate,
        rule.placement(),
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
            edge_rule(f, cohort_id, empty)
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

        rules.push(edge_rule(f, cohort_id, cohort_member_hashes));
    }

    Ok(finalize_bundle(project_id, rules, now_ms))
}

fn edge_rule(
    f: &FeatureFlag,
    targeting_cohort_id: Option<String>,
    cohort_member_hashes: Option<Vec<String>>,
) -> EdgeFlagRule {
    EdgeFlagRule {
        key: f.key.clone(),
        enabled: f.enabled,
        rollout_bps: ((f.rollout_pct.clamp(0.0, 100.0) * 100.0).round()) as u32,
        cohort_members: targeting_cohort_id.as_ref().map(|_| Vec::new()),
        targeting_cohort_id,
        cohort_member_hashes,
        variants: f.variants.clone(),
        holdout_id: f.holdout.as_ref().map(|h| h.id.clone()),
        holdout_bps: f.holdout.as_ref().map(|h| h.bps),
        layer_id: f.layer.as_ref().map(|l| l.id.clone()),
        layer_start_bps: f.layer.as_ref().map(|l| l.start_bps),
        layer_end_bps: f.layer.as_ref().map(|l| l.end_bps),
    }
}

fn finalize_bundle(project_id: &str, rules: Vec<EdgeFlagRule>, now_ms: i64) -> EdgeFlagBundle {
    let json_bytes = serde_json::to_vec(&rules).unwrap_or_default();
    let etag = format!("W/\"{:016x}\"", xxh3_64(&json_bytes));
    EdgeFlagBundle {
        schema_version: 2,
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
        kind: exp.kind.clone(),
        bandit: None,
    })
}

/// Bandit results: per-arm means stratified by exposure day, `P(best)` and expected loss from
/// Thompson sampling, the weight history, and an SRM check against the weights each day's users
/// were assigned under. No p-values: the weights moved toward the leader, so a fixed-split test
/// doesn't apply.
pub async fn evaluate_bandit_results(
    exp: &Experiment,
    flag: Option<&FeatureFlag>,
    history: &[WeightSnapshot],
    qengine: &QueryEngine,
) -> Result<ExperimentResults, ExecError> {
    let t0 = Instant::now();
    let current: BTreeMap<String, u32> = flag
        .map(|f| {
            f.variants
                .iter()
                .map(|v| (v.key.clone(), v.weight_bps))
                .collect()
        })
        .unwrap_or_default();
    let keys: Vec<String> = current.keys().cloned().collect();
    let metric = &exp.primary_metric;
    let kind = if normalized_metric_type(metric) == "ratio" {
        EstimateKind::Ratio
    } else {
        EstimateKind::Mean
    };
    let res = qengine
        .query_experiment_metric(&exp.project_id, &metric_query(exp, metric, None, false))
        .await?;

    let days: Vec<BTreeMap<String, MetricMoments>> =
        res.daily.iter().map(|p| p.day.clone()).collect();
    let est = bandit::stratified_estimates(&days, &keys, kind);
    let odds = bandit::prob_best_and_loss(&est, metric.direction != "increase_is_bad");
    let cfg = exp.bandit.clone().unwrap_or_default();
    let burn_in_complete = !keys.is_empty() && est.values().all(|e| e.n >= cfg.burn_in_users);

    // Expected users per arm = Σ over days of that day's users × the weights in effect then.
    let mut expected: BTreeMap<String, f64> = keys.iter().map(|k| (k.clone(), 0.0)).collect();
    for p in &res.daily {
        let n: u64 = p.day.values().map(|m| m.n).sum();
        let w = bandit::weights_at(history, p.day_start_ms).unwrap_or(&current);
        let w_total: u32 = w.values().sum();
        for (k, e) in expected.iter_mut() {
            *e += n as f64 * *w.get(k).unwrap_or(&0) as f64 / w_total.max(1) as f64;
        }
    }
    let exp_total: f64 = expected.values().sum();
    let expected_bps: BTreeMap<String, u32> = expected
        .iter()
        .map(|(k, e)| {
            let bps = if exp_total > 0.0 {
                (e / exp_total * 10_000.0).round()
            } else {
                0.0
            };
            (k.clone(), bps as u32)
        })
        .collect();
    let observed: BTreeMap<String, (u64, u64)> =
        est.iter().map(|(k, e)| (k.clone(), (e.n, 0))).collect();
    let (srm_p_value, sample_ratio_mismatch) = if exp_total > 0.0 {
        compute_sample_ratio_mismatch(&observed, &expected_bps)
    } else {
        (1.0, false)
    };

    let alpha = if exp.analysis.alpha > 0.0 && exp.analysis.alpha < 0.5 {
        exp.analysis.alpha
    } else {
        0.05
    };
    let z = normal_quantile(1.0 - alpha / 2.0);
    let arms: Vec<BanditArm> = keys
        .iter()
        .map(|k| {
            let e = est[k];
            let (prob_best, expected_loss) = odds[k];
            let half = z * e.var_of_mean.sqrt();
            BanditArm {
                variant_key: k.clone(),
                users: e.n,
                mean_value: e.mean,
                ci_low: e.mean - half,
                ci_high: e.mean + half,
                prob_best,
                expected_loss,
                weight_bps: current[k],
            }
        })
        .collect();
    let best = arms
        .iter()
        .max_by(|a, b| a.prob_best.total_cmp(&b.prob_best));
    let ready_to_ship = burn_in_complete
        && !sample_ratio_mismatch
        && best.is_some_and(|b| {
            b.mean_value.abs() > 1e-12 && b.expected_loss < 0.01 * b.mean_value.abs()
        });

    Ok(ExperimentResults {
        experiment_id: exp.id.clone(),
        experiment_name: exp.name.clone(),
        flag_key: exp.flag_key.clone(),
        status: exp.status.clone(),
        hypothesis: exp.hypothesis.clone(),
        metric_name: metric.name.clone(),
        exec_time_us: t0.elapsed().as_micros() as u64,
        srm_p_value,
        sample_ratio_mismatch,
        statistical_methodology_note: format!(
            "Bandit: Thompson sampling on normal posteriors; arm means stratified by exposure day and weighted by each day's share of users, so changing weights don't bias them; weights update daily with a {:.0}% floor per arm; {:.0}% intervals; no p-values; SRM checked against each day's weights",
            cfg.min_weight_bps as f64 / 100.0,
            (1.0 - alpha) * 100.0
        ),
        sequential: false,
        confidence_level: 1.0 - alpha,
        variants: Vec::new(),
        guardrails: Vec::new(),
        metric_type: normalized_metric_type(metric).to_string(),
        cuped_applied: false,
        variance_reduction_pct: 0.0,
        multi_variant_users: res.multi_variant_users,
        segment_by: None,
        segments: Vec::new(),
        timeseries: Vec::new(),
        kind: exp.kind.clone(),
        bandit: Some(BanditResults {
            best_variant: best.map(|b| b.variant_key.clone()),
            arms,
            weight_history: history.to_vec(),
            burn_in_complete,
            ready_to_ship,
        }),
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

/// The engine query for one metric of `exp`: first exposure to its flag, then `metric` within
/// the window (and the CUPED pre-window when `cuped`).
fn metric_query(
    exp: &Experiment,
    metric: &ExperimentMetricSpec,
    segment_by: Option<&str>,
    cuped: bool,
) -> ExperimentMetricQuery {
    let kind = normalized_metric_type(metric);
    let ratio = kind == "ratio";
    // Respect the exact configured window, including sub-24h windows.
    let window_ms = if metric.conversion_window_ms > 0 {
        metric.conversion_window_ms
    } else {
        7 * 86_400_000
    };

    // A holdout's "experiment" is keyed by holdout id, and its groups are "holdout" / "exposed".
    let (exposure_event, id_prop, variant_prop) = if exp.kind == "holdout" {
        ("$holdout_assigned", "holdout_id", "group")
    } else {
        ("$experiment_started", "flag_key", "variant")
    };
    ExperimentMetricQuery {
        time_range: TimeRange {
            start_ts_ms: exp.start_ts_ms,
            end_ts_ms: exp.end_ts_ms,
        },
        exposure_event: exposure_event.to_string(),
        exposure_filters: vec![PropPredicate {
            prop_name: id_prop.to_string(),
            filter: pulse_exec::PropFilterOp::Eq(PropValue::Str(exp.flag_key.clone())),
        }],
        variant_prop: variant_prop.to_string(),
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
    let q = metric_query(exp, metric, segment_by, cuped);
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
