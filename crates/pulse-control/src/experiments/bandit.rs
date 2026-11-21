//! Multi-armed bandits: Thompson sampling over each arm's posterior, with weights that move once
//! a day. Arms are estimated per exposure day and then averaged with each day's share of all
//! users, so an arm that got more traffic on high-converting days isn't flattered by it
//! (Simpson's paradox under changing weights).

use std::collections::BTreeMap;

use pulse_exec::MetricMoments;
use rand::rngs::StdRng;
use rand::SeedableRng;

use super::stats::{estimates_from_moments, sample_standard_normal, Estimate, EstimateKind};

/// Per-arm estimate stratified by exposure day: `Σ_d W_d · mean_{a,d}` with `W_d` the day's share
/// of all users. A day where the arm has fewer than two users is left out of that arm's average.
pub fn stratified_estimates(
    days: &[BTreeMap<String, MetricMoments>],
    arms: &[String],
    kind: EstimateKind,
) -> BTreeMap<String, Estimate> {
    let per_day: Vec<(f64, BTreeMap<String, Estimate>)> = days
        .iter()
        .map(|d| {
            let n: u64 = d.values().map(|m| m.n).sum();
            (n as f64, estimates_from_moments(d, kind, false).0)
        })
        .collect();
    arms.iter()
        .map(|a| {
            let (mut w_sum, mut mean, mut var, mut n) = (0.0, 0.0, 0.0, 0u64);
            for (day_n, est) in &per_day {
                let Some(e) = est.get(a) else { continue };
                n += e.n;
                if e.n < 2 {
                    continue;
                }
                w_sum += day_n;
                mean += day_n * e.mean;
                var += day_n * day_n * e.var_of_mean;
            }
            let e = if w_sum > 0.0 {
                Estimate {
                    n,
                    mean: mean / w_sum,
                    var_of_mean: var / (w_sum * w_sum),
                }
            } else {
                Estimate {
                    n,
                    mean: 0.0,
                    var_of_mean: 0.0,
                }
            };
            (a.clone(), e)
        })
        .collect()
}

/// `(P(best), expected loss)` per arm from normal posteriors, by Monte Carlo. Loss is how much
/// worse than the best arm this one is expected to be, in metric units. Seeded, so the same
/// data gives the same answer on every refresh.
pub fn prob_best_and_loss(
    est: &BTreeMap<String, Estimate>,
    higher_is_better: bool,
) -> BTreeMap<String, (f64, f64)> {
    const DRAWS: usize = 20_000;
    let arms: Vec<(&String, &Estimate)> = est.iter().collect();
    let sign = if higher_is_better { 1.0 } else { -1.0 };
    let mut wins = vec![0.0f64; arms.len()];
    let mut loss = vec![0.0f64; arms.len()];
    let mut draw = vec![0.0f64; arms.len()];
    let mut rng = StdRng::seed_from_u64(0x5eed_ba4d);
    for _ in 0..DRAWS {
        for (i, (_, e)) in arms.iter().enumerate() {
            draw[i] = sign * (e.mean + e.var_of_mean.sqrt() * sample_standard_normal(&mut rng));
        }
        let best = draw.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let tied = draw.iter().filter(|&&d| d == best).count() as f64;
        for i in 0..arms.len() {
            if draw[i] == best {
                wins[i] += 1.0 / tied;
            }
            loss[i] += best - draw[i];
        }
    }
    arms.iter()
        .enumerate()
        .map(|(i, (k, _))| {
            (
                (*k).clone(),
                (wins[i] / DRAWS as f64, loss[i] / DRAWS as f64),
            )
        })
        .collect()
}

/// Thompson weights: each arm gets `min_bps` plus its `P(best)` share of the rest, summing to
/// exactly 10,000 (rounding goes to the most likely arm).
pub fn weights_from_prob_best(
    prob_best: &BTreeMap<String, f64>,
    min_bps: u32,
) -> BTreeMap<String, u32> {
    let k = prob_best.len() as u32;
    let min_bps = min_bps.min(10_000 / k.max(1));
    let free = 10_000 - min_bps * k;
    let mut w: BTreeMap<String, u32> = prob_best
        .iter()
        .map(|(a, p)| {
            (
                a.clone(),
                min_bps + (p.clamp(0.0, 1.0) * free as f64).floor() as u32,
            )
        })
        .collect();
    let short = 10_000 - w.values().sum::<u32>();
    if let Some(top) = prob_best
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(a, _)| a.clone())
    {
        *w.get_mut(&top).unwrap() += short;
    }
    w
}

/// The weights users exposed on `day_start_ms` were assigned under: the last snapshot that took
/// effect by then, else the first one.
pub fn weights_at(
    history: &[crate::models::WeightSnapshot],
    day_start_ms: i64,
) -> Option<&BTreeMap<String, u32>> {
    history
        .iter()
        .rev()
        .find(|s| s.effective_from_ms <= day_start_ms)
        .or(history.first())
        .map(|s| &s.weights)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact moments for `n` users converting at rate `p` (no sampling noise).
    fn conv(n: u64, p: f64) -> MetricMoments {
        let c = (n as f64 * p).round();
        MetricMoments {
            n,
            sum_y: c,
            sum_y2: c,
            ..Default::default()
        }
    }

    fn arms(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn weights_respect_the_floor_and_sum_to_10000() {
        let p = BTreeMap::from([
            ("a".to_string(), 1.0),
            ("b".to_string(), 0.0),
            ("c".to_string(), 0.0),
        ]);
        let w = weights_from_prob_best(&p, 100);
        assert_eq!(w["a"], 9800);
        assert_eq!(w["b"], 100);
        assert_eq!(w["c"], 100);
        let p = BTreeMap::from([("a".to_string(), 1.0 / 3.0), ("b".to_string(), 2.0 / 3.0)]);
        let w = weights_from_prob_best(&p, 500);
        assert_eq!(w.values().sum::<u32>(), 10_000);
        assert!(w.values().all(|&v| v >= 500));
    }

    /// The base rate climbs from 5% to 30% while arm b's share climbs from 10% to 90%. Both arms
    /// convert the same on every day, so pooling says b wins; stratifying says they're equal.
    #[test]
    fn stratified_estimate_survives_drift_that_fools_pooling() {
        let days: Vec<BTreeMap<String, MetricMoments>> = (0..9)
            .map(|d| {
                let rate = 0.05 + 0.03125 * d as f64;
                let share_b = 0.1 + 0.1 * d as f64;
                BTreeMap::from([
                    (
                        "a".to_string(),
                        conv((10_000.0 * (1.0 - share_b)) as u64, rate),
                    ),
                    ("b".to_string(), conv((10_000.0 * share_b) as u64, rate)),
                ])
            })
            .collect();
        let pooled = |k: &str| {
            let (n, c) = days
                .iter()
                .fold((0.0, 0.0), |(n, c), d| (n + d[k].n as f64, c + d[k].sum_y));
            c / n
        };
        assert!(pooled("b") - pooled("a") > 0.05, "pooling is fooled");
        let est = stratified_estimates(&days, &arms(&["a", "b"]), EstimateKind::Mean);
        assert!((est["a"].mean - est["b"].mean).abs() < 1e-3, "{est:?}");
        let pb = prob_best_and_loss(&est, true);
        assert!((pb["a"].0 - 0.5).abs() < 0.15, "{pb:?}");
    }

    /// Three arms at 10%, 12% and 15%: after a month of daily updates the 15% arm has most of the
    /// traffic and the others keep their floor.
    #[test]
    fn simulated_bandit_converges_to_the_best_arm() {
        let rates = BTreeMap::from([
            ("a".to_string(), 0.10),
            ("b".to_string(), 0.12),
            ("c".to_string(), 0.15),
        ]);
        let keys = arms(&["a", "b", "c"]);
        let mut weights: BTreeMap<String, u32> =
            BTreeMap::from([("a".into(), 3334), ("b".into(), 3333), ("c".into(), 3333)]);
        let mut days = Vec::new();
        for _ in 0..30 {
            days.push(
                rates
                    .iter()
                    .map(|(k, r)| (k.clone(), conv(2_000 * weights[k] as u64 / 10_000, *r)))
                    .collect(),
            );
            let est = stratified_estimates(&days, &keys, EstimateKind::Mean);
            let pb = prob_best_and_loss(&est, true)
                .into_iter()
                .map(|(k, (p, _))| (k, p))
                .collect();
            weights = weights_from_prob_best(&pb, 100);
            assert!(weights.values().all(|&w| w >= 100), "{weights:?}");
        }
        assert!(weights["c"] > 9_000, "{weights:?}");
        let est = stratified_estimates(&days, &keys, EstimateKind::Mean);
        assert!((est["c"].mean - 0.15).abs() < 0.01);
        assert!(
            (est["a"].mean - 0.10).abs() < 0.02,
            "the floor keeps a measured"
        );
    }

    #[test]
    fn lower_is_better_flips_the_winner() {
        let est = BTreeMap::from([
            (
                "a".to_string(),
                Estimate {
                    n: 1000,
                    mean: 1.0,
                    var_of_mean: 0.0001,
                },
            ),
            (
                "b".to_string(),
                Estimate {
                    n: 1000,
                    mean: 2.0,
                    var_of_mean: 0.0001,
                },
            ),
        ]);
        assert!(prob_best_and_loss(&est, true)["b"].0 > 0.99);
        let low = prob_best_and_loss(&est, false);
        assert!(low["a"].0 > 0.99);
        assert!((low["b"].1 - 1.0).abs() < 0.01, "b is a full unit worse");
    }
}
