//! Pure experiment statistics: SRM, lift tables, mean comparisons and sequential tests.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use pulse_exec::MetricMoments;

use crate::models::VariantLiftRow;

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

/// How a lift table is analysed.
#[derive(Debug, Clone, Copy)]
pub struct LiftOptions {
    /// Two-sided significance level; CIs are `1 - alpha`.
    pub alpha: f64,
    /// `Some(rho)` switches to always-valid (sequential) p-values and confidence sequences,
    /// tuned by [`sequential_rho`]. `None` is the fixed-horizon test.
    pub sequential_rho: Option<f64>,
}

impl Default for LiftOptions {
    fn default() -> Self {
        Self {
            alpha: 0.05,
            sequential_rho: None,
        }
    }
}

/// Per-variant sufficient statistics of a per-user metric (`y`).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Moments {
    pub n: u64,
    pub sum: f64,
    pub sum_sq: f64,
}

impl Moments {
    /// A binary metric: each converter contributes `1` to both `sum` and `sum_sq`.
    pub fn from_conversions(n: u64, conversions: u64) -> Self {
        Self {
            n,
            sum: conversions as f64,
            sum_sq: conversions as f64,
        }
    }

    pub fn mean(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.sum / self.n as f64
        }
    }

    /// Sample variance (`n - 1` denominator); `0` below two users.
    pub fn variance(&self) -> f64 {
        if self.n < 2 {
            return 0.0;
        }
        let n = self.n as f64;
        ((self.sum_sq - self.sum * self.sum / n) / (n - 1.0)).max(0.0)
    }
}

/// Treatment vs control on one metric.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Comparison {
    pub abs_diff: f64,
    pub abs_ci: (f64, f64),
    pub rel_lift_pct: f64,
    pub rel_ci_pct: (f64, f64),
    pub p_value: f64,
}

/// Rho for the asymptotic confidence sequence of Waudby-Smith et al. (2023, Thm 2.2), chosen so
/// the sequence is tightest at `planned_total_n` users (summed across both arms).
pub fn sequential_rho(alpha: f64, planned_total_n: u64) -> f64 {
    let n_star = planned_total_n.max(2) as f64;
    let l = -2.0 * alpha.ln();
    ((l + (l + 1.0).ln()) / n_star).sqrt()
}

/// Half-width of a two-sided asymptotic `1 - alpha` confidence sequence after `n` users whose
/// per-user variance of the estimate is `s2` (estimator variance times `n`).
fn cs_half_width(s2: f64, n: f64, rho: f64, alpha: f64) -> f64 {
    let r2n = rho * rho * n + 1.0;
    (s2 * (2.0 * r2n / (n * n * rho * rho)) * (r2n.sqrt() / alpha).ln()).sqrt()
}

/// Always-valid p-value: the smallest alpha at which the confidence sequence excludes 0.
fn cs_p_value(diff: f64, s2: f64, n: f64, rho: f64) -> f64 {
    if s2 <= 0.0 {
        return if diff == 0.0 { 1.0 } else { 0.0 };
    }
    let r2n = rho * rho * n + 1.0;
    let ln_p = 0.5 * r2n.ln() - diff * diff * n * n * rho * rho / (2.0 * s2 * r2n);
    ln_p.exp().clamp(0.0, 1.0)
}

/// One arm's estimate: a mean (or ratio) and the variance of that estimate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Estimate {
    pub n: u64,
    pub mean: f64,
    pub var_of_mean: f64,
}

impl Moments {
    pub fn estimate(&self) -> Estimate {
        Estimate {
            n: self.n,
            mean: self.mean(),
            var_of_mean: if self.n == 0 {
                0.0
            } else {
                self.variance() / self.n as f64
            },
        }
    }
}

/// Welch (unpooled, normal-approximation) comparison of two per-user means, with a delta-method
/// relative-lift CI. With `opts.sequential_rho` the p-value and both CIs are always valid, so the
/// result can be checked any number of times. `None` below two users per arm.
pub fn compare_means(c: &Moments, t: &Moments, opts: LiftOptions) -> Option<Comparison> {
    compare_estimates(&c.estimate(), &t.estimate(), opts)
}

/// [`compare_means`] over any two arm estimates (plain, CUPED-adjusted or ratio).
pub fn compare_estimates(c: &Estimate, t: &Estimate, opts: LiftOptions) -> Option<Comparison> {
    if c.n < 2 || t.n < 2 {
        return None;
    }
    let (m_c, m_t) = (c.mean, t.mean);
    let diff = m_t - m_c;
    let var_diff = c.var_of_mean + t.var_of_mean;

    // Delta method for `m_t / m_c - 1`.
    let rel = (m_c.abs() > 1e-12).then(|| {
        let var_rel = t.var_of_mean / (m_c * m_c) + (m_t * m_t * c.var_of_mean) / m_c.powi(4);
        (m_t / m_c - 1.0, var_rel)
    });

    let (abs_half, rel_half, p_value) = match opts.sequential_rho {
        Some(rho) => {
            let n = (c.n + t.n) as f64;
            (
                cs_half_width(var_diff * n, n, rho, opts.alpha),
                rel.map(|(_, v)| cs_half_width(v * n, n, rho, opts.alpha)),
                cs_p_value(diff, var_diff * n, n, rho),
            )
        }
        None => {
            let z = normal_quantile(1.0 - opts.alpha / 2.0);
            let se = var_diff.sqrt();
            let p = if se > 1e-15 {
                two_sided_normal_p_value((diff / se).abs())
            } else if diff == 0.0 {
                1.0
            } else {
                0.0
            };
            (z * se, rel.map(|(_, v)| z * v.sqrt()), p)
        }
    };

    let (rel_lift_pct, rel_ci_pct) = match (rel, rel_half) {
        (Some((r, _)), Some(h)) => (r * 100.0, ((r - h) * 100.0, (r + h) * 100.0)),
        _ => (0.0, (0.0, 0.0)),
    };
    Some(Comparison {
        abs_diff: diff,
        abs_ci: (diff - abs_half, diff + abs_half),
        rel_lift_pct,
        rel_ci_pct,
        p_value,
    })
}

/// How a metric's per-user moments turn into one estimate per arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateKind {
    /// Mean of `y` per user.
    Mean,
    /// `Σy / Σd` (delta method).
    Ratio,
}

fn sample_var(n: f64, sum: f64, sum_sq: f64) -> f64 {
    if n < 2.0 {
        0.0
    } else {
        ((sum_sq - sum * sum / n) / (n - 1.0)).max(0.0)
    }
}

fn sample_cov(n: f64, sum_a: f64, sum_b: f64, sum_ab: f64) -> f64 {
    if n < 2.0 {
        0.0
    } else {
        (sum_ab - sum_a * sum_b / n) / (n - 1.0)
    }
}

/// Per-arm estimates from engine moments, with optional CUPED on mean metrics.
///
/// CUPED: `theta = Cov(x, y) / Var(x)` pooled over all arms; each arm's mean becomes
/// `ȳ - theta (x̄ - x̄_all)` with per-user variance `Var(y) + theta² Var(x) - 2 theta Cov(x, y)`.
/// Returns the estimates and the pooled variance reduction (`None` when CUPED didn't apply:
/// asked off, a ratio metric, or no pre-period variation).
pub fn estimates_from_moments(
    groups: &BTreeMap<String, MetricMoments>,
    kind: EstimateKind,
    cuped: bool,
) -> (BTreeMap<String, Estimate>, Option<f64>) {
    let all = groups
        .values()
        .fold(MetricMoments::default(), |mut acc, m| {
            acc.merge(m);
            acc
        });
    let n_all = all.n as f64;
    let theta = (cuped && kind == EstimateKind::Mean && all.n >= 2)
        .then(|| {
            let var_x = sample_var(n_all, all.sum_x, all.sum_x2);
            (var_x > 1e-12).then(|| sample_cov(n_all, all.sum_x, all.sum_y, all.sum_xy) / var_x)
        })
        .flatten();
    let x_bar_all = if all.n > 0 { all.sum_x / n_all } else { 0.0 };

    let est = groups
        .iter()
        .map(|(k, m)| {
            let n = m.n as f64;
            let e = if m.n == 0 {
                Estimate {
                    n: 0,
                    mean: 0.0,
                    var_of_mean: 0.0,
                }
            } else {
                match (kind, theta) {
                    (EstimateKind::Ratio, _) => {
                        let d_bar = m.sum_d / n;
                        let r = if m.sum_d.abs() > 1e-12 {
                            m.sum_y / m.sum_d
                        } else {
                            0.0
                        };
                        let var = if d_bar.abs() > 1e-12 {
                            let s_yy = sample_var(n, m.sum_y, m.sum_y2);
                            let s_dd = sample_var(n, m.sum_d, m.sum_d2);
                            let s_yd = sample_cov(n, m.sum_y, m.sum_d, m.sum_yd);
                            ((s_yy - 2.0 * r * s_yd + r * r * s_dd) / (n * d_bar * d_bar)).max(0.0)
                        } else {
                            0.0
                        };
                        Estimate {
                            n: m.n,
                            mean: r,
                            var_of_mean: var,
                        }
                    }
                    (EstimateKind::Mean, Some(th)) => {
                        let s_yy = sample_var(n, m.sum_y, m.sum_y2);
                        let s_xx = sample_var(n, m.sum_x, m.sum_x2);
                        let s_xy = sample_cov(n, m.sum_x, m.sum_y, m.sum_xy);
                        Estimate {
                            n: m.n,
                            mean: m.sum_y / n - th * (m.sum_x / n - x_bar_all),
                            var_of_mean: ((s_yy + th * th * s_xx - 2.0 * th * s_xy) / n).max(0.0),
                        }
                    }
                    (EstimateKind::Mean, None) => Estimate {
                        n: m.n,
                        mean: m.sum_y / n,
                        var_of_mean: sample_var(n, m.sum_y, m.sum_y2) / n,
                    },
                }
            };
            (k.clone(), e)
        })
        .collect();

    let reduction = theta.map(|th| {
        let var_y = sample_var(n_all, all.sum_y, all.sum_y2);
        let var_x = sample_var(n_all, all.sum_x, all.sum_x2);
        let cov = sample_cov(n_all, all.sum_x, all.sum_y, all.sum_xy);
        if var_y > 1e-12 {
            ((th * th * var_x - 2.0 * th * cov) / -var_y).clamp(0.0, 1.0) * 100.0
        } else {
            0.0
        }
    });
    (est, reduction)
}

/// Lift table over per-arm estimates (any metric type). `P(beat control)` uses the normal
/// approximation; p-values are Holm-Bonferroni adjusted across treatments.
pub fn compute_estimate_table(
    groups: &BTreeMap<String, MetricMoments>,
    estimates: &BTreeMap<String, Estimate>,
    opts: LiftOptions,
) -> Vec<VariantLiftRow> {
    let control_key = if estimates.contains_key("control") {
        "control".to_string()
    } else {
        estimates.keys().next().cloned().unwrap_or_default()
    };
    let control = estimates.get(&control_key).copied();
    let mut keys: Vec<&String> = estimates.keys().filter(|k| **k != control_key).collect();
    if control.is_some() {
        keys.insert(0, &control_key);
    }

    let mut rows = Vec::with_capacity(keys.len());
    let mut treatments = Vec::new();
    for k in keys {
        let e = estimates[k];
        let m = groups.get(k).copied().unwrap_or_default();
        let is_control = *k == control_key;
        let mut row = VariantLiftRow {
            variant_key: k.clone(),
            is_control,
            sample_size: e.n,
            conversions: 0,
            conversion_rate: e.mean,
            mean_value: e.mean,
            variance: e.var_of_mean * e.n as f64,
            relative_lift_pct: 0.0,
            ci95_low_pct: 0.0,
            ci95_high_pct: 0.0,
            p_value: 1.0,
            bayesian_prob_to_beat_control: if is_control { 0.5 } else { 0.0 },
            statistically_significant: false,
            abs_diff: 0.0,
            abs_ci_low: 0.0,
            abs_ci_high: 0.0,
            sequential: opts.sequential_rho.is_some(),
        };
        row.conversions = m.sum_y.round().max(0.0) as u64;
        if !is_control {
            treatments.push(rows.len());
            if let Some(cmp) = control.and_then(|c| compare_estimates(&c, &e, opts)) {
                let c = control.unwrap_or(e);
                let sd = (c.var_of_mean + e.var_of_mean).sqrt();
                row.relative_lift_pct = cmp.rel_lift_pct;
                row.ci95_low_pct = cmp.rel_ci_pct.0;
                row.ci95_high_pct = cmp.rel_ci_pct.1;
                row.p_value = cmp.p_value;
                row.abs_diff = cmp.abs_diff;
                row.abs_ci_low = cmp.abs_ci.0;
                row.abs_ci_high = cmp.abs_ci.1;
                row.bayesian_prob_to_beat_control = if sd > 1e-15 {
                    standard_normal_cdf(cmp.abs_diff / sd)
                } else {
                    0.5
                };
            }
        }
        rows.push(row);
    }
    holm_adjust(&mut rows, &treatments, opts.alpha);
    rows
}

/// `Φ(z)`, from the two-sided p-value helper.
fn standard_normal_cdf(z: f64) -> f64 {
    let tail = 0.5 * two_sided_normal_p_value(z.abs());
    if z >= 0.0 {
        1.0 - tail
    } else {
        tail
    }
}

/// Fixed-horizon conversion lift table at the default 5% level. See [`compute_lift_table`].
pub fn compute_statistical_lift_table(
    counts_by_variant: &BTreeMap<String, (u64, u64)>,
) -> Vec<VariantLiftRow> {
    compute_lift_table(counts_by_variant, LiftOptions::default())
}

/// Conversion lift table over `(exposed, converted)` per variant. Treatments are compared with
/// `control` (or the first key):
/// - fixed horizon: pooled two-proportion Z-test and Fieller relative-lift CI;
/// - sequential: Welch always-valid p-value and confidence sequences ([`compare_means`]).
///
/// p-values are Holm-Bonferroni adjusted across treatments. `P(beat control)` comes from 20,000
/// Beta-Binomial Monte Carlo draws.
pub fn compute_lift_table(
    counts_by_variant: &BTreeMap<String, (u64, u64)>,
    opts: LiftOptions,
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
    let z_crit = normal_quantile(1.0 - opts.alpha / 2.0);

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
        let blank = VariantLiftRow {
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
            abs_diff: 0.0,
            abs_ci_low: 0.0,
            abs_ci_high: 0.0,
            sequential: opts.sequential_rho.is_some(),
        };

        if is_control || n_c == 0 || n_t == 0 {
            if !is_control {
                treatment_indices.push(rows.len());
            }
            rows.push(blank);
            continue;
        }

        let diff = p_t - p_c;
        let (relative_lift_pct, (ci_low, ci_high), (abs_low, abs_high), raw_p) = match opts
            .sequential_rho
        {
            Some(_) => match compare_means(
                &Moments::from_conversions(n_c, x_c),
                &Moments::from_conversions(n_t, x_t),
                opts,
            ) {
                Some(cmp) => (cmp.rel_lift_pct, cmp.rel_ci_pct, cmp.abs_ci, cmp.p_value),
                None => (0.0, (0.0, 0.0), (diff, diff), 1.0),
            },
            None => {
                let rel = if p_c > 1e-12 {
                    (diff / p_c) * 100.0
                } else {
                    0.0
                };
                let se_unpooled = (p_c * (1.0 - p_c) / n_c as f64 + var_t / n_t as f64).sqrt();
                // Pooled two-proportion Z-test for the two-sided p-value.
                let p_pool = (x_c + x_t) as f64 / (n_c + n_t) as f64;
                let se_pool =
                    (p_pool * (1.0 - p_pool) * (1.0 / (n_c as f64) + 1.0 / (n_t as f64))).sqrt();
                let p = if se_pool > 1e-15 {
                    two_sided_normal_p_value((diff / se_pool).abs())
                } else {
                    1.0
                };
                (
                    rel,
                    relative_lift_ci_pct(p_c, n_c, p_t, n_t, z_crit),
                    (diff - z_crit * se_unpooled, diff + z_crit * se_unpooled),
                    p,
                )
            }
        };

        let alpha_t = 1.0 + x_t as f64;
        let beta_t = 1.0 + n_t.saturating_sub(x_t) as f64;
        let mut wins = 0usize;
        for &c_draw in &control_draws {
            if sample_beta(&mut rng, alpha_t, beta_t) > c_draw {
                wins += 1;
            }
        }

        treatment_indices.push(rows.len());
        rows.push(VariantLiftRow {
            relative_lift_pct,
            ci95_low_pct: ci_low,
            ci95_high_pct: ci_high,
            p_value: raw_p,
            bayesian_prob_to_beat_control: wins as f64 / (MC_DRAWS as f64),
            abs_diff: diff,
            abs_ci_low: abs_low,
            abs_ci_high: abs_high,
            ..blank
        });
    }

    holm_adjust(&mut rows, &treatment_indices, opts.alpha);
    rows
}

/// Holm-Bonferroni step-down family-wise correction across the `m` treatment rows, in place.
fn holm_adjust(rows: &mut [VariantLiftRow], treatment_indices: &[usize], alpha: f64) {
    let mut idx = treatment_indices.to_vec();
    let m = idx.len();
    idx.sort_by(|&a, &b| {
        rows[a]
            .p_value
            .partial_cmp(&rows[b].p_value)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut running_max: f64 = 0.0;
    for (rank, &row_idx) in idx.iter().enumerate() {
        let adj = (rows[row_idx].p_value * (m - rank) as f64).clamp(0.0, 1.0);
        running_max = running_max.max(adj);
        rows[row_idx].p_value = running_max;
        rows[row_idx].statistically_significant = running_max < alpha;
    }
}

/// Fieller confidence interval at critical value `z` for relative lift `(p_t / p_c - 1) * 100%`
/// (with Delta-method fallback when `g >= 0.99`), propagating variance from BOTH `p_t` and `p_c`.
fn relative_lift_ci_pct(p_c: f64, n_c: u64, p_t: f64, n_t: u64, z: f64) -> (f64, f64) {
    if p_c <= 1e-12 || n_c == 0 || n_t == 0 {
        return (0.0, 0.0);
    }
    let v_c = (p_c * (1.0 - p_c)) / (n_c as f64);
    let v_t = (p_t * (1.0 - p_t)) / (n_t as f64);
    let ratio = p_t / p_c;
    let g = (z * z * v_c) / (p_c * p_c);

    if g < 0.99 {
        let disc = (v_t + ratio * ratio * v_c - g * v_t).max(0.0).sqrt();
        let half = (z / p_c) * disc;
        let lo = ((ratio - half) / (1.0 - g) - 1.0) * 100.0;
        let hi = ((ratio + half) / (1.0 - g) - 1.0) * 100.0;
        (lo, hi)
    } else {
        // Delta-method variance: Var(p_t / p_c) = v_t / p_c^2 + (p_t^2 / p_c^4) * v_c
        let se_ratio = (v_t / (p_c * p_c) + (ratio * ratio * v_c) / (p_c * p_c)).sqrt();
        let lo = (ratio - z * se_ratio - 1.0) * 100.0;
        let hi = (ratio + z * se_ratio - 1.0) * 100.0;
        (lo, hi)
    }
}

/// Standard normal quantile (Acklam's rational approximation, relative error < 1.2e-9).
pub fn normal_quantile(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let p = p.clamp(1e-300, 1.0 - 1e-16);
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    if p < 0.02425 {
        tail((-2.0 * p.ln()).sqrt())
    } else if p > 1.0 - 0.02425 {
        -tail((-2.0 * (1.0 - p).ln()).sqrt())
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
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
pub(super) fn sample_standard_normal<R: Rng>(rng: &mut R) -> f64 {
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
