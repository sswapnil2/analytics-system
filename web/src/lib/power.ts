/**
 * Sample-size planning for experiments. Mirrors the server's analysis
 * (crates/pulse-control/src/experiments/stats.rs): Welch comparison of means,
 * Bonferroni across treatments for planning, and the same sequential rho.
 */

export interface PowerOptions {
  /** Two-sided significance level. */
  alpha?: number;
  /** Chance of detecting a true effect of the planned size. */
  power?: number;
  /** Variants including control. */
  variants?: number;
  /** Plan for the always-valid (sequential) test, which needs more users. */
  sequential?: boolean;
}

/** Standard normal quantile (Acklam's approximation, relative error < 1.2e-9). */
export function normalQuantile(p: number): number {
  const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239];
  const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
  const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
  const tail = (q: number) =>
    (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  if (p < 0.02425) return tail(Math.sqrt(-2 * Math.log(p)));
  if (p > 1 - 0.02425) return -tail(Math.sqrt(-2 * Math.log(1 - p)));
  const q = p - 0.5;
  const r = q * q;
  return (
    ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q) /
    (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
  );
}

/** Same as the server's `sequential_rho`: tightest at `plannedTotal` users across all arms. */
export function sequentialRho(alpha: number, plannedTotal: number): number {
  const l = -2 * Math.log(alpha);
  return Math.sqrt((l + Math.log(l + 1)) / Math.max(2, plannedTotal));
}

/**
 * Users per variant to detect an absolute difference `delta` between two arms whose
 * per-user variances sum to `varSum`.
 */
export function usersPerVariantForDiff(varSum: number, delta: number, opts: PowerOptions = {}): number {
  const { alpha = 0.05, power = 0.8, variants = 2, sequential = false } = opts;
  if (!(varSum > 0) || !(delta > 0)) return Infinity;
  const a = alpha / Math.max(1, variants - 1);
  const zPow = normalQuantile(power);
  const fixed = Math.ceil(((normalQuantile(1 - a / 2) + zPow) ** 2 * varSum) / delta ** 2);
  if (!sequential) return fixed;

  // Smallest n where the confidence sequence (tuned to this plan) clears `delta` with `power`.
  const enough = (n: number) => {
    const total = n * variants;
    const pair = 2 * n;
    const rho = sequentialRho(a, total);
    const s2 = (varSum / n) * pair;
    const r2n = rho * rho * pair + 1;
    const half = Math.sqrt(s2 * ((2 * r2n) / (pair * pair * rho * rho)) * Math.log(Math.sqrt(r2n) / a));
    return half + zPow * Math.sqrt(varSum / n) <= delta;
  };
  let lo = fixed;
  let hi = fixed * 2;
  while (!enough(hi)) {
    hi *= 2;
    if (hi > 1e12) return Infinity;
  }
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (enough(mid)) hi = mid;
    else lo = mid;
  }
  return hi;
}

/** Users per variant to detect a relative change `mde` on conversion rate `p`. */
export function usersPerVariant(p: number, mde: number, opts: PowerOptions = {}): number {
  if (p <= 0 || p >= 1 || mde <= 0) return Infinity;
  return usersPerVariantForDiff(2 * p * (1 - p), p * mde, opts);
}

/** Users per variant to detect a relative change `mde` on a per-user mean with standard deviation `sd`. */
export function usersPerVariantForMean(mean: number, sd: number, mde: number, opts: PowerOptions = {}): number {
  if (mean === 0 || !(sd > 0) || mde <= 0) return Infinity;
  return usersPerVariantForDiff(2 * sd * sd, Math.abs(mean) * mde, opts);
}
