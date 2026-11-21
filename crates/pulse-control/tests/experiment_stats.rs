//! Statistics checks for experiment analysis: known reference values, and an A/A simulation
//! proving the sequential test stays at its false-positive rate under repeated peeking.

use std::collections::BTreeMap;

use pulse_control::experiments::normal_quantile;
use pulse_control::{
    compare_means, compute_lift_table, sequential_rho, Experiment, LiftOptions, Moments,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn moments(n: u64, mean: f64, var: f64) -> Moments {
    let sum = mean * n as f64;
    Moments {
        n,
        sum,
        sum_sq: var * (n - 1) as f64 + sum * sum / n as f64,
    }
}

#[test]
fn normal_quantile_matches_tables() {
    assert!((normal_quantile(0.975) - 1.959_964).abs() < 1e-6);
    assert!((normal_quantile(0.995) - 2.575_829).abs() < 1e-6);
    assert!((normal_quantile(0.5)).abs() < 1e-12);
    assert!((normal_quantile(0.001) + 3.090_232).abs() < 1e-6);
}

#[test]
fn welch_fixed_horizon_matches_hand_computed_reference() {
    // Means 10 vs 10.3, variance 4, 1000 users each: se = sqrt(0.008), z = 3.3541.
    let c = moments(1000, 10.0, 4.0);
    let t = moments(1000, 10.3, 4.0);
    assert!((c.variance() - 4.0).abs() < 1e-9);
    let cmp = compare_means(&c, &t, LiftOptions::default()).unwrap();
    assert!((cmp.abs_diff - 0.3).abs() < 1e-9);
    assert!((cmp.p_value - 7.96e-4).abs() < 2e-5, "p = {}", cmp.p_value);
    assert!((cmp.abs_ci.0 - 0.124_695).abs() < 1e-4, "{:?}", cmp.abs_ci);
    assert!((cmp.abs_ci.1 - 0.475_305).abs() < 1e-4, "{:?}", cmp.abs_ci);
    assert!((cmp.rel_lift_pct - 3.0).abs() < 1e-9);
    assert!(cmp.rel_ci_pct.0 > 0.0 && cmp.rel_ci_pct.1 < 6.0);
}

#[test]
fn sequential_is_wider_and_more_conservative_than_fixed() {
    let c = moments(5000, 0.10, 0.09);
    let t = moments(5000, 0.11, 0.0979);
    let fixed = compare_means(&c, &t, LiftOptions::default()).unwrap();
    let seq = compare_means(
        &c,
        &t,
        LiftOptions {
            alpha: 0.05,
            sequential_rho: Some(sequential_rho(0.05, 10_000)),
        },
    )
    .unwrap();
    assert!(seq.p_value >= fixed.p_value);
    assert!(seq.abs_ci.1 - seq.abs_ci.0 > fixed.abs_ci.1 - fixed.abs_ci.0);
    // The p-value and the CI agree: significant exactly when the CI excludes 0.
    assert_eq!(seq.p_value < 0.05, seq.abs_ci.0 > 0.0 || seq.abs_ci.1 < 0.0);
}

#[test]
fn lift_table_sequential_path_uses_compare_means_and_alpha() {
    let mut counts = BTreeMap::new();
    counts.insert("control".to_string(), (4000u64, 400u64));
    counts.insert("treatment".to_string(), (4000u64, 470u64));
    let opts = LiftOptions {
        alpha: 0.05,
        sequential_rho: Some(sequential_rho(0.05, 8000)),
    };
    let rows = compute_lift_table(&counts, opts);
    let t = rows.iter().find(|r| !r.is_control).unwrap();
    let cmp = compare_means(
        &Moments::from_conversions(4000, 400),
        &Moments::from_conversions(4000, 470),
        opts,
    )
    .unwrap();
    assert!(t.sequential);
    assert!((t.p_value - cmp.p_value).abs() < 1e-12);
    assert!((t.ci95_low_pct - cmp.rel_ci_pct.0).abs() < 1e-9);
    assert!((t.abs_diff - 0.0175).abs() < 1e-12);
    assert_eq!(t.statistically_significant, t.p_value < 0.05);

    // A stricter alpha can only remove significance.
    let strict = compute_lift_table(
        &counts,
        LiftOptions {
            alpha: 0.001,
            sequential_rho: None,
        },
    );
    let loose = compute_lift_table(&counts, LiftOptions::default());
    let s = strict.iter().find(|r| !r.is_control).unwrap();
    let l = loose.iter().find(|r| !r.is_control).unwrap();
    assert!(l.statistically_significant && !s.statistically_significant);
    assert!(s.ci95_high_pct - s.ci95_low_pct > l.ci95_high_pct - l.ci95_low_pct);
}

/// 2,000 A/A tests at a 10% base rate, each checked 20 times as users arrive. Checking a
/// fixed-horizon test that often inflates false positives well past 5%; the sequential test must
/// stay at or under its 5% level.
#[test]
fn aa_peeking_simulation_holds_sequential_false_positive_rate() {
    const SIMS: usize = 2_000;
    const PEEKS: u64 = 20;
    const USERS_PER_PEEK: u64 = 200;
    let rho = sequential_rho(0.05, 2 * PEEKS * USERS_PER_PEEK);
    let seq_opts = LiftOptions {
        alpha: 0.05,
        sequential_rho: Some(rho),
    };
    let mut rng = StdRng::seed_from_u64(42);
    let (mut fixed_fp, mut seq_fp) = (0usize, 0usize);

    for _ in 0..SIMS {
        let (mut x_c, mut x_t) = (0u64, 0u64);
        let (mut fixed_hit, mut seq_hit) = (false, false);
        for peek in 1..=PEEKS {
            for _ in 0..USERS_PER_PEEK {
                x_c += u64::from(rng.gen::<f64>() < 0.1);
                x_t += u64::from(rng.gen::<f64>() < 0.1);
            }
            let n = peek * USERS_PER_PEEK;
            let (c, t) = (
                Moments::from_conversions(n, x_c),
                Moments::from_conversions(n, x_t),
            );
            fixed_hit |=
                compare_means(&c, &t, LiftOptions::default()).is_some_and(|r| r.p_value < 0.05);
            seq_hit |= compare_means(&c, &t, seq_opts).is_some_and(|r| r.p_value < 0.05);
        }
        fixed_fp += usize::from(fixed_hit);
        seq_fp += usize::from(seq_hit);
    }
    let fixed_rate = fixed_fp as f64 / SIMS as f64;
    let seq_rate = seq_fp as f64 / SIMS as f64;
    assert!(fixed_rate > 0.12, "peeking fixed-horizon rate {fixed_rate}");
    assert!(
        seq_rate <= 0.055,
        "sequential false-positive rate {seq_rate}"
    );
}

#[test]
fn experiments_stored_before_analysis_config_still_parse() {
    let legacy = r#"{
        "id": "exp_1", "project_id": "p", "name": "n", "flag_id": "f", "flag_key": "k",
        "hypothesis": "h", "status": "running", "start_ts_ms": 0, "end_ts_ms": 1,
        "primary_metric": {"name": "m", "event_name": "e"},
        "guardrail_metrics": [{"name": "g", "event_name": "x", "metric_type": "conversion"}],
        "created_at_ms": 0
    }"#;
    let exp: Experiment = serde_json::from_str(legacy).unwrap();
    assert!(!exp.analysis.sequential);
    assert!((exp.analysis.alpha - 0.05).abs() < 1e-12);
    assert_eq!(exp.guardrail_metrics[0].direction, "decrease_is_bad");
}

fn add_user(m: &mut pulse_exec::MetricMoments, y: f64, x: f64, d: f64) {
    m.n += 1;
    m.sum_y += y;
    m.sum_y2 += y * y;
    m.sum_x += x;
    m.sum_x2 += x * x;
    m.sum_xy += x * y;
    m.sum_d += d;
    m.sum_d2 += d * d;
    m.sum_yd += y * d;
}

fn normal(rng: &mut StdRng) -> f64 {
    let u1: f64 = rng.gen::<f64>().max(1e-12);
    let u2: f64 = rng.gen();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// 1,000 simulated tests with a true +2 effect and a pre-period covariate (correlation ~0.9):
/// CUPED stays unbiased and its estimates vary far less than the plain difference in means.
#[test]
fn cuped_is_unbiased_and_cuts_variance_in_simulation() {
    use pulse_control::experiments::{estimates_from_moments, EstimateKind};
    let mut rng = StdRng::seed_from_u64(7);
    let (mut plain, mut adj) = (Vec::new(), Vec::new());
    for _ in 0..1_000 {
        let mut groups: BTreeMap<String, pulse_exec::MetricMoments> = BTreeMap::new();
        for (arm, effect) in [("control", 0.0), ("treatment", 2.0)] {
            let m = groups.entry(arm.to_string()).or_default();
            for _ in 0..400 {
                let level = 50.0 + 10.0 * normal(&mut rng);
                let x = level + 4.0 * normal(&mut rng);
                let y = level + effect + 4.0 * normal(&mut rng);
                add_user(m, y, x, 0.0);
            }
        }
        let (p, _) = estimates_from_moments(&groups, EstimateKind::Mean, false);
        let (c, red) = estimates_from_moments(&groups, EstimateKind::Mean, true);
        assert!(red.unwrap() > 50.0);
        plain.push(p["treatment"].mean - p["control"].mean);
        adj.push(c["treatment"].mean - c["control"].mean);
    }
    let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    let var = |v: &[f64]| {
        let m = mean(v);
        v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64
    };
    assert!(
        (mean(&adj) - 2.0).abs() < 0.05,
        "CUPED bias: mean {}",
        mean(&adj)
    );
    assert!(
        (mean(&plain) - 2.0).abs() < 0.15,
        "plain mean {}",
        mean(&plain)
    );
    assert!(
        var(&adj) < 0.4 * var(&plain),
        "CUPED var {} vs plain {}",
        var(&adj),
        var(&plain)
    );
}

/// Ratio metrics (spend per visit) under A/A: the delta-method test keeps its 5% level.
#[test]
fn ratio_metric_aa_false_positive_rate() {
    use pulse_control::experiments::{
        compute_estimate_table, estimates_from_moments, EstimateKind,
    };
    let mut rng = StdRng::seed_from_u64(3);
    let sims = 1_000;
    let mut hits = 0;
    for _ in 0..sims {
        let mut groups: BTreeMap<String, pulse_exec::MetricMoments> = BTreeMap::new();
        for arm in ["control", "treatment"] {
            let m = groups.entry(arm.to_string()).or_default();
            for _ in 0..300 {
                let visits = f64::from(rng.gen_range(1..=6u8));
                let spend = visits * (10.0 + 3.0 * normal(&mut rng)).max(0.0);
                add_user(m, spend, 0.0, visits);
            }
        }
        let (est, _) = estimates_from_moments(&groups, EstimateKind::Ratio, false);
        let rows = compute_estimate_table(&groups, &est, LiftOptions::default());
        hits += usize::from(rows.iter().any(|r| r.statistically_significant));
    }
    let rate = hits as f64 / sims as f64;
    assert!(
        (0.025..=0.075).contains(&rate),
        "ratio A/A false-positive rate {rate}"
    );
}
