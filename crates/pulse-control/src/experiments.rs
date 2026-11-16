use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use pulse_exec::{
    ExecError, FunnelOrderMode, FunnelQuery, FunnelStepSpec, PropPredicate, QueryEngine, TimeRange,
};
use pulse_wire::PropValue;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use xxhash_rust::xxh3::xxh3_64;

use crate::models::{
    EdgeFlagBundle, EdgeFlagRule, Experiment, ExperimentMetricSpec, ExperimentResults, FeatureFlag,
    GuardrailResult, VariantLiftRow,
};

/// Evaluates an `EdgeFlagRule` deterministically in < 1 µs using `xxh3_64("{flag_key}:{distinct_id}") % 10_000`.
pub fn evaluate_edge_flag(rule: &EdgeFlagRule, distinct_id: &str) -> Option<String> {
    if !rule.enabled || rule.rollout_bps == 0 {
        return None;
    }
    let seed = format!("{}:{distinct_id}", rule.key);
    let hash = xxh3_64(seed.as_bytes());
    let rollout_slot = (hash % 10_000) as u32;
    if rollout_slot >= rule.rollout_bps {
        return None;
    }
    if rule.variants.is_empty() {
        return Some("on".to_string());
    }
    let variant_hash = xxh3_64(format!("var:{}:{distinct_id}", rule.key).as_bytes());
    let total_weight: u32 = rule.variants.iter().map(|v| v.weight_bps).sum();
    if total_weight == 0 {
        return rule.variants.first().map(|v| v.key.clone());
    }
    let v_slot = (variant_hash % (total_weight as u64)) as u32;
    let mut cumulative = 0u32;
    for v in &rule.variants {
        cumulative += v.weight_bps;
        if v_slot < cumulative {
            return Some(v.key.clone());
        }
    }
    rule.variants.last().map(|v| v.key.clone())
}

pub fn build_edge_flag_bundle(
    project_id: &str,
    flags: &[FeatureFlag],
    now_ms: i64,
) -> EdgeFlagBundle {
    let rules: Vec<EdgeFlagRule> = flags
        .iter()
        .map(|f| EdgeFlagRule {
            key: f.key.clone(),
            enabled: f.enabled,
            rollout_bps: ((f.rollout_pct.clamp(0.0, 100.0) * 100.0).round()) as u32,
            variants: f.variants.clone(),
        })
        .collect();
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
/// plus Frequentist Z-test and 20,000-draw Monte Carlo Bayesian Beta-Binomial simulation.
pub async fn evaluate_experiment_results(
    exp: &Experiment,
    flag: Option<&FeatureFlag>,
    qengine: &QueryEngine,
) -> Result<ExperimentResults, ExecError> {
    let t0 = Instant::now();

    let variant_keys: Vec<String> = if let Some(f) = flag {
        if f.variants.is_empty() {
            vec!["control".to_string(), "treatment".to_string()]
        } else {
            f.variants.iter().map(|v| v.key.clone()).collect()
        }
    } else {
        vec!["control".to_string(), "treatment".to_string()]
    };

    let primary_rows =
        evaluate_metric_for_experiment(exp, &exp.primary_metric, &variant_keys, qengine).await?;

    let mut guardrails = Vec::with_capacity(exp.guardrail_metrics.len());
    for gm in &exp.guardrail_metrics {
        let gm_rows = evaluate_metric_for_experiment(exp, gm, &variant_keys, qengine).await?;
        guardrails.push(GuardrailResult {
            metric_name: gm.name.clone(),
            event_name: gm.event_name.clone(),
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
        variants: primary_rows,
        guardrails,
    })
}

async fn evaluate_metric_for_experiment(
    exp: &Experiment,
    metric: &ExperimentMetricSpec,
    expected_variants: &[String],
    qengine: &QueryEngine,
) -> Result<Vec<VariantLiftRow>, ExecError> {
    let exp_prop = format!("$exp_{}", exp.flag_key);
    let range = TimeRange {
        start_ts_ms: exp.start_ts_ms,
        end_ts_ms: exp.end_ts_ms,
    };

    // 1. Primary path: 2-step Funnel from `$experiment_started` (filtered by `flag_key` or broken down by `variant` / `$exp_<flag_key>`) -> `metric.event_name`
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
        conversion_window_ms: metric.conversion_window_ms.max(86_400_000),
        exclusions: Vec::new(),
        hold_constant_props: Vec::new(),
        breakdown_by: Some("variant".to_string()),
        global_filter: pulse_exec::GlobalFilter::default(),
    };

    let mut counts_by_variant: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for vk in expected_variants {
        counts_by_variant.insert(vk.clone(), (0, 0));
    }

    let res1 = qengine
        .query_funnel(&exp.project_id, &funnel_from_exposure)
        .await?;
    let mut total_exposed = 0u64;
    for bg in &res1.breakdowns {
        if bg.breakdown_value != "(none)" && !bg.steps.is_empty() {
            let n = bg.steps[0].entered_users;
            let conv = bg.steps.get(1).map(|s| s.entered_users).unwrap_or(0);
            total_exposed += n;
            counts_by_variant.insert(bg.breakdown_value.clone(), (n, conv));
        }
    }

    // 2. Fallback session-property attribution (`$exp_<flag_key>` on entry events like `App_Open`) if `$experiment_started` wasn't emitted explicitly
    if total_exposed == 0 {
        let fallback_q = FunnelQuery {
            time_range: range,
            steps: vec![
                FunnelStepSpec {
                    event_name: "App_Open".to_string(),
                    filters: vec![PropPredicate {
                        prop_name: exp_prop.clone(),
                        filter: pulse_exec::PropFilterOp::IsSet,
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
            conversion_window_ms: metric.conversion_window_ms.max(86_400_000),
            exclusions: Vec::new(),
            hold_constant_props: Vec::new(),
            breakdown_by: Some(exp_prop),
            global_filter: pulse_exec::GlobalFilter::default(),
        };
        let res2 = qengine.query_funnel(&exp.project_id, &fallback_q).await?;
        for bg in &res2.breakdowns {
            if bg.breakdown_value != "(none)" && !bg.steps.is_empty() {
                let n = bg.steps[0].entered_users;
                let conv = bg.steps.get(1).map(|s| s.entered_users).unwrap_or(0);
                counts_by_variant.insert(bg.breakdown_value.clone(), (n, conv));
            }
        }
    }

    Ok(compute_statistical_lift_table(&counts_by_variant))
}

/// Computes Frequentist Two-Proportion Z-test (`p_value`, 95% CI) and 20,000-draw Monte Carlo Bayesian
/// Beta-Binomial `P(Treatment > Control)` in < 2ms!
pub fn compute_statistical_lift_table(
    counts_by_variant: &BTreeMap<String, (u64, u64)>,
) -> Vec<VariantLiftRow> {
    let control_key = if counts_by_variant.contains_key("control") {
        "control".to_string()
    } else {
        counts_by_variant
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "control".to_string())
    };

    let (n_c, x_c) = counts_by_variant
        .get(&control_key)
        .copied()
        .unwrap_or((0, 0));
    let p_c = if n_c == 0 {
        0.0
    } else {
        x_c as f64 / n_c as f64
    };

    const MC_DRAWS: usize = 20_000;
    let mut rng = StdRng::seed_from_u64(0x5055_4C53_4541_4254);

    // Pre-draw 20,000 Beta(1 + x_c, 1 + n_c - x_c) samples for control once
    let alpha_c = 1.0 + x_c as f64;
    let beta_c = 1.0 + n_c.saturating_sub(x_c) as f64;
    let control_draws: Vec<f64> = (0..MC_DRAWS)
        .map(|_| sample_beta(&mut rng, alpha_c, beta_c))
        .collect();

    let mut ordered_keys: Vec<String> = Vec::with_capacity(counts_by_variant.len());
    if counts_by_variant.contains_key(&control_key) {
        ordered_keys.push(control_key.clone());
    }
    for k in counts_by_variant.keys() {
        if k != &control_key {
            ordered_keys.push(k.clone());
        }
    }

    let mut rows = Vec::with_capacity(ordered_keys.len());
    let mut key_order_map: HashMap<&str, usize> = HashMap::new();
    for (idx, k) in ordered_keys.iter().enumerate() {
        key_order_map.insert(k.as_str(), idx);
        let (n_t, x_t) = counts_by_variant.get(k).copied().unwrap_or((0, 0));
        let is_control = k == &control_key;
        let p_t = if n_t == 0 {
            0.0
        } else {
            x_t as f64 / n_t as f64
        };
        let var_t = p_t * (1.0 - p_t);

        if is_control || n_c == 0 || n_t == 0 {
            rows.push(VariantLiftRow {
                variant_key: k.clone(),
                is_control,
                sample_size: n_t,
                conversions: x_t,
                conversion_rate: p_t,
                mean_value: p_t,
                variance: var_t,
                relative_lift_pct: 0.0,
                ci95_low_pct: 0.0,
                ci95_high_pct: 0.0,
                p_value: 1.0,
                bayesian_prob_to_beat_control: if is_control { 0.5 } else { 0.0 },
                statistically_significant: false,
            });
            continue;
        }

        let diff = p_t - p_c;
        let relative_lift_pct = if p_c > 1e-12 {
            (diff / p_c) * 100.0
        } else {
            0.0
        };

        // Unpooled SE for 95% CI
        let se_unpooled =
            ((p_c * (1.0 - p_c)) / (n_c as f64) + (p_t * (1.0 - p_t)) / (n_t as f64)).sqrt();
        let margin = 1.959_964 * se_unpooled;
        let (ci95_low_pct, ci95_high_pct) = if p_c > 1e-12 {
            (
                ((diff - margin) / p_c) * 100.0,
                ((diff + margin) / p_c) * 100.0,
            )
        } else {
            (0.0, 0.0)
        };

        // Pooled two-proportion Z-test for two-sided p-value
        let p_pool = (x_c + x_t) as f64 / (n_c + n_t) as f64;
        let se_pool = (p_pool * (1.0 - p_pool) * (1.0 / (n_c as f64) + 1.0 / (n_t as f64))).sqrt();
        let p_value = if se_pool > 1e-15 {
            let z = (diff / se_pool).abs();
            two_sided_normal_p_value(z)
        } else {
            1.0
        };

        // 20,000-draw Monte Carlo Beta-Binomial Bayesian probability to beat control
        let alpha_t = 1.0 + x_t as f64;
        let beta_t = 1.0 + n_t.saturating_sub(x_t) as f64;
        let mut wins = 0usize;
        for &c_draw in &control_draws {
            let t_draw = sample_beta(&mut rng, alpha_t, beta_t);
            if t_draw > c_draw {
                wins += 1;
            }
        }
        let prob_beat = wins as f64 / (MC_DRAWS as f64);

        rows.push(VariantLiftRow {
            variant_key: k.clone(),
            is_control: false,
            sample_size: n_t,
            conversions: x_t,
            conversion_rate: p_t,
            mean_value: p_t,
            variance: var_t,
            relative_lift_pct,
            ci95_low_pct,
            ci95_high_pct,
            p_value,
            bayesian_prob_to_beat_control: prob_beat,
            statistically_significant: p_value < 0.05,
        });
    }

    let _ = key_order_map.len();
    rows
}

/// Marsaglia-Tsang fast Gamma(shape, 1.0) sampler used to draw exact Beta(a, b) variates.
#[inline]
fn sample_gamma<R: Rng>(rng: &mut R, shape: f64) -> f64 {
    if shape < 1.0 {
        let u: f64 = rng.gen::<f64>().clamp(1e-15, 1.0);
        return sample_gamma(rng, shape + 1.0) * u.powf(1.0 / shape);
    }
    let d = shape - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();
    loop {
        let z = sample_standard_normal(rng);
        let v_base = 1.0 + c * z;
        if v_base <= 0.0 {
            continue;
        }
        let v = v_base * v_base * v_base;
        let u: f64 = rng.gen::<f64>().clamp(1e-15, 1.0);
        if u < 1.0 - 0.0331 * (z * z) * (z * z) {
            return d * v;
        }
        if u.ln() < 0.5 * z * z + d * (1.0 - v + v.ln()) {
            return d * v;
        }
    }
}

#[inline]
fn sample_standard_normal<R: Rng>(rng: &mut R) -> f64 {
    let u1 = rng.gen::<f64>().clamp(1e-15, 1.0);
    let u2 = rng.gen::<f64>();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

#[inline]
fn sample_beta<R: Rng>(rng: &mut R, alpha: f64, beta: f64) -> f64 {
    let x = sample_gamma(rng, alpha);
    let y = sample_gamma(rng, beta);
    if x + y <= 0.0 {
        0.5
    } else {
        x / (x + y)
    }
}

/// Abramowitz & Stegun 7.1.26 high-precision `erfc(|z| / sqrt(2))` two-sided Z-test p-value (< 1.5e-7 max error).
#[inline]
fn two_sided_normal_p_value(abs_z: f64) -> f64 {
    let x = abs_z / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    (poly * (-x * x).exp()).clamp(0.0, 1.0)
}
