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

/// Seen on a real node: p = 0.016 and an absolute CI clear of 0, yet a symmetric delta-method
/// relative CI of -21.9% to +218%. The relative CI must exclude 0 whenever the absolute one does.
#[test]
fn sequential_relative_ci_agrees_with_absolute_ci() {
    let mut counts = BTreeMap::new();
    counts.insert("control".to_string(), (615u64, 70u64));
    counts.insert("treatment".to_string(), (585u64, 132u64));
    let seq = LiftOptions {
        alpha: 0.05,
        sequential_rho: Some(sequential_rho(0.05, 20_000)),
    };
    for opts in [seq, LiftOptions::default()] {
        let rows = compute_lift_table(&counts, opts);
        let t = rows.iter().find(|r| !r.is_control).unwrap();
        assert!(t.abs_ci_low > 0.0, "{t:?}");
        assert!(t.statistically_significant, "{t:?}");
        assert!(t.ci95_low_pct > 0.0, "relative CI must exclude 0: {t:?}");
        assert!(t.ci95_low_pct < t.relative_lift_pct && t.relative_lift_pct < t.ci95_high_pct);
    }

    // Agreement holds across a sweep of effect sizes, both directions, fixed and sequential.
    let c = Moments::from_conversions(615, 70);
    for x_t in 20..200 {
        let t = Moments::from_conversions(585, x_t);
        for opts in [seq, LiftOptions::default()] {
            let cmp = compare_means(&c, &t, opts).unwrap();
            let abs_sig = cmp.abs_ci.0 > 0.0 || cmp.abs_ci.1 < 0.0;
            let rel_sig = cmp.rel_ci_pct.0 > 0.0 || cmp.rel_ci_pct.1 < 0.0;
            assert_eq!(abs_sig, rel_sig, "x_t={x_t} {cmp:?}");
        }
    }
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
