use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use pulse_exec::{
    ExecError, FunnelOrderMode, FunnelQuery, FunnelStepSpec, PropPredicate, QueryEngine, TimeRange,
};
use pulse_wire::{cohort_member_hash, evaluate_flag_rule, CohortGate, PropValue};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use xxhash_rust::xxh3::xxh3_64;

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
/// plus Chi-square SRM check, Holm-Bonferroni multi-variant Z-test, Fieller/Delta relative lift CI,
/// and 20,000-draw Monte Carlo Bayesian Beta-Binomial simulation (`P2-09f`, `P2-10`).
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

    let (mut primary_rows, observed_counts) =
        evaluate_metric_for_experiment(exp, &exp.primary_metric, &variant_keys, qengine).await?;

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
            evaluate_metric_for_experiment(exp, gm, &variant_keys, qengine).await?;
        if sample_ratio_mismatch {
            for r in &mut gm_rows {
                r.statistically_significant = false;
            }
        }
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
        srm_p_value,
        sample_ratio_mismatch,
        statistical_methodology_note:
            "Fixed-horizon two-sided Z-test (not peeking-corrected; Holm-Bonferroni adjusted across treatment variants; Fieller/Delta relative lift 95% CI; Chi-square SRM check at alpha=0.001)"
                .to_string(),
        variants: primary_rows,
        guardrails,
    })
}

async fn evaluate_metric_for_experiment(
    exp: &Experiment,
    metric: &ExperimentMetricSpec,
    expected_variants: &[String],
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
    let rows = compute_statistical_lift_table(&counts_by_variant);
    Ok((rows, counts_by_variant))
}

/// Pearson's Chi-square Sample Ratio Mismatch test against configured variant `weight_bps`.
/// Flags `sample_ratio_mismatch = true` when `total_n >= 50` and `p < 0.001`.
pub fn compute_sample_ratio_mismatch(
    counts_by_variant: &BTreeMap<String, (u64, u64)>,
    expected_weights: &BTreeMap<String, u32>,
) -> (f64, bool) {
    let mut pairs: Vec<(f64, f64)> = Vec::with_capacity(expected_weights.len());
    let mut total_obs = 0u64;
    let mut total_weight = 0u64;

    for (k, &w) in expected_weights {
        if w == 0 {
            continue;
        }
        let obs = counts_by_variant.get(k).map(|&(n, _)| n).unwrap_or(0);
        total_obs += obs;
        total_weight += u64::from(w);
        pairs.push((obs as f64, f64::from(w)));
    }

    if pairs.len() < 2 || total_obs == 0 || total_weight == 0 {
        return (1.0, false);
    }

    let n_f = total_obs as f64;
    let w_f = total_weight as f64;
    let mut chi2 = 0.0;
    for (obs, w) in &pairs {
        let exp_n = n_f * (*w / w_f);
        if exp_n > 0.0 {
            let d = *obs - exp_n;
            chi2 += (d * d) / exp_n;
        }
    }

    let df = (pairs.len() - 1) as f64;
    let p_val = chi_square_survival(chi2, df);
    let srm = total_obs >= 50 && p_val < 0.001;
    (p_val, srm)
}

/// Computes Frequentist Two-Proportion Z-test (`p_value` with Holm-Bonferroni multi-variant adjustment),
/// Fieller/Delta 95% relative lift CI (accounting for control `p_c` variance), and 20,000-draw Monte Carlo
/// Bayesian Beta-Binomial `P(Treatment > Control)`.
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
    let mut treatment_indices: Vec<usize> = Vec::new();

    for k in &ordered_keys {
        let (n_t, x_t) = counts_by_variant.get(k).copied().unwrap_or((0, 0));
        let is_control = k == &control_key;
        let p_t = if n_t == 0 {
            0.0
        } else {
            x_t as f64 / n_t as f64
        };
        let var_t = p_t * (1.0 - p_t);

        if is_control || n_c == 0 || n_t == 0 {
            if !is_control {
                treatment_indices.push(rows.len());
            }
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

        // Fieller's theorem / Delta-method 95% CI on relative lift `(p_t / p_c - 1)` accounting for uncertainty in BOTH `p_t` and `p_c`.
        let (ci95_low_pct, ci95_high_pct) = relative_lift_ci95_pct(p_c, n_c, p_t, n_t);

        // Pooled two-proportion Z-test for two-sided p-value
        let p_pool = (x_c + x_t) as f64 / (n_c + n_t) as f64;
        let se_pool = (p_pool * (1.0 - p_pool) * (1.0 / (n_c as f64) + 1.0 / (n_t as f64))).sqrt();
        let raw_p = if se_pool > 1e-15 {
            let z = (diff / se_pool).abs();
            two_sided_normal_p_value(z)
        } else {
            1.0
        };

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

        treatment_indices.push(rows.len());
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
            p_value: raw_p,
            bayesian_prob_to_beat_control: prob_beat,
            statistically_significant: false,
        });
    }

    // Apply Holm-Bonferroni step-down family-wise error correction across all `m` treatment variants.
    let m = treatment_indices.len();
    if m > 0 {
        treatment_indices.sort_by(|&a, &b| {
            rows[a]
                .p_value
                .partial_cmp(&rows[b].p_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut running_max: f64 = 0.0;
        for (rank, &row_idx) in treatment_indices.iter().enumerate() {
            let factor = (m - rank) as f64;
            let adj = (rows[row_idx].p_value * factor).clamp(0.0, 1.0);
            running_max = running_max.max(adj);
            rows[row_idx].p_value = running_max;
            rows[row_idx].statistically_significant = running_max < 0.05;
        }
    }

    rows
}

/// Fieller's exact 95% confidence interval for relative lift `(p_t / p_c - 1) * 100%`
/// (with Delta-method fallback when `g >= 0.99`), propagating variance from BOTH `p_t` and `p_c`.
fn relative_lift_ci95_pct(p_c: f64, n_c: u64, p_t: f64, n_t: u64) -> (f64, f64) {
    if p_c <= 1e-12 || n_c == 0 || n_t == 0 {
        return (0.0, 0.0);
    }
    const Z: f64 = 1.959_964;
    let v_c = (p_c * (1.0 - p_c)) / (n_c as f64);
    let v_t = (p_t * (1.0 - p_t)) / (n_t as f64);
    let ratio = p_t / p_c;
    let g = (Z * Z * v_c) / (p_c * p_c);

    if g < 0.99 {
        let disc = (v_t + ratio * ratio * v_c - g * v_t).max(0.0).sqrt();
        let half = (Z / p_c) * disc;
        let lo = ((ratio - half) / (1.0 - g) - 1.0) * 100.0;
        let hi = ((ratio + half) / (1.0 - g) - 1.0) * 100.0;
        (lo, hi)
    } else {
        // Delta-method variance: Var(p_t / p_c) = v_t / p_c^2 + (p_t^2 / p_c^4) * v_c
        let se_ratio = (v_t / (p_c * p_c) + (ratio * ratio * v_c) / (p_c * p_c)).sqrt();
        let lo = (ratio - Z * se_ratio - 1.0) * 100.0;
        let hi = (ratio + Z * se_ratio - 1.0) * 100.0;
        (lo, hi)
    }
}

/// Survival function `P(ChiSq_df >= x) = Q(df / 2, x / 2)` via regularized upper incomplete gamma.
fn chi_square_survival(chi2: f64, df: f64) -> f64 {
    if !chi2.is_finite() || chi2 <= 0.0 || df <= 0.0 {
        return 1.0;
    }
    regularized_gamma_q(0.5 * df, 0.5 * chi2)
}

/// Regularized upper incomplete gamma `Q(a, x) = 1 - P(a, x)` using series for `x < a + 1`
/// and Lentz's continued fraction for `x >= a + 1`.
fn regularized_gamma_q(a: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 1.0;
    }
    let gln = ln_gamma(a);
    if x < a + 1.0 {
        let mut ap = a;
        let mut sum = 1.0 / a;
        let mut del = sum;
        for _ in 0..200 {
            ap += 1.0;
            del *= x / ap;
            sum += del;
            if del.abs() < sum.abs() * 1e-14 {
                break;
            }
        }
        let p = sum * (-x + a * x.ln() - gln).exp();
        (1.0 - p).clamp(0.0, 1.0)
    } else {
        let fpmin = 1e-300_f64;
        let mut b = x + 1.0 - a;
        let mut c = 1.0 / fpmin;
        let mut d = 1.0 / b;
        let mut h = d;
        for i in 1..=200 {
            let i_f = i as f64;
            let an = -i_f * (i_f - a);
            b += 2.0;
            d = an * d + b;
            if d.abs() < fpmin {
                d = fpmin;
            }
            c = b + an / c;
            if c.abs() < fpmin {
                c = fpmin;
            }
            d = 1.0 / d;
            let del = d * c;
            h *= del;
            if (del - 1.0).abs() < 1e-14 {
                break;
            }
        }
        ((-x + a * x.ln() - gln).exp() * h).clamp(0.0, 1.0)
    }
}

/// Lanczos 7-term `ln(Gamma(z))` approximation accurate to < 2e-10 across `z > 0`.
fn ln_gamma(z: f64) -> f64 {
    const COEFFS: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    let x_in = z - 1.0;
    let mut sum = COEFFS[0];
    for (i, &c) in COEFFS.iter().enumerate().skip(1) {
        sum += c / (x_in + (i as f64));
    }
    let t = x_in + 7.5;
    0.5 * std::f64::consts::TAU.ln() + (x_in + 0.5) * t.ln() - t + sum.ln()
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
