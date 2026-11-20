use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// HyperLogLog precision (`p = 10` -> `m = 1024` registers, standard error ~3.25%).
pub const HLL_PRECISION: u32 = 10;
pub const HLL_REGISTERS: usize = 1 << HLL_PRECISION;
/// Below this many non-zero registers we store sparse `(register_idx, rho)` pairs so small
/// segments add only a few dozen bytes to `SegmentFooterMeta`.
const SPARSE_THRESHOLD: usize = 96;

/// Compact, mergeable HyperLogLog sketch (`p = 10`, `m = 1024`) stored in `SegmentFooterMeta`
/// for zero-I/O approximate `Uniques` / `DAU` / `WAU` / `MAU` over long time ranges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "k", content = "d")]
pub enum CompactHll {
    /// Sparse list of `(register_idx, rho)` pairs (for small cardinalities).
    #[serde(rename = "s")]
    Sparse(Vec<(u16, u8)>),
    /// Dense 1024-byte register array (compresses to ~250-350 B inside zstd manifests).
    #[serde(rename = "d")]
    Dense(Vec<u8>),
}

/// Mutable dense HLL builder/merger used during segment creation and query aggregation.
#[derive(Debug, Clone)]
pub struct HllSketch {
    registers: [u8; HLL_REGISTERS],
}

impl Default for HllSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketch {
    pub fn new() -> Self {
        Self {
            registers: [0u8; HLL_REGISTERS],
        }
    }

    #[inline]
    pub fn observe_uid(&mut self, uid: u32) {
        let h = xxh3_64(&uid.to_le_bytes());
        let idx = (h >> (64 - HLL_PRECISION)) as usize;
        let w = h << HLL_PRECISION;
        // `leading_zeros` on remaining `64 - HLL_PRECISION` bits + 1, capped to `64 - P + 1`.
        let max_zeros = 64 - HLL_PRECISION;
        let rho = (w.leading_zeros().min(max_zeros) as u8) + 1;
        if rho > self.registers[idx] {
            self.registers[idx] = rho;
        }
    }

    #[inline]
    pub fn merge_sketch(&mut self, other: &Self) {
        for (dst, &src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if src > *dst {
                *dst = src;
            }
        }
    }

    pub fn merge_compact(&mut self, compact: &CompactHll) {
        match compact {
            CompactHll::Sparse(pairs) => {
                for &(idx, rho) in pairs {
                    let i = idx as usize;
                    if i < HLL_REGISTERS && rho > self.registers[i] {
                        self.registers[i] = rho;
                    }
                }
            }
            CompactHll::Dense(bytes) => {
                for (dst, &src) in self.registers.iter_mut().zip(bytes.iter()) {
                    if src > *dst {
                        *dst = src;
                    }
                }
            }
        }
    }

    pub fn to_compact(&self) -> CompactHll {
        let non_zero = self.registers.iter().filter(|&&r| r > 0).count();
        if non_zero <= SPARSE_THRESHOLD {
            let mut pairs = Vec::with_capacity(non_zero);
            for (idx, &r) in self.registers.iter().enumerate() {
                if r > 0 {
                    pairs.push((idx as u16, r));
                }
            }
            CompactHll::Sparse(pairs)
        } else {
            CompactHll::Dense(self.registers.to_vec())
        }
    }

    /// Standard Flajolet / HyperLogLog cardinality estimator with LinearCounting small-range correction.
    pub fn estimate(&self) -> f64 {
        let m = HLL_REGISTERS as f64;
        let alpha_m = 0.7213 / (1.0 + 1.079 / m);
        let mut inv_sum = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            if r == 0 {
                zeros += 1;
                inv_sum += 1.0;
            } else {
                inv_sum += 2.0f64.powi(-(r as i32));
            }
        }
        let raw = alpha_m * m * m / inv_sum;
        let est = if raw <= 2.5 * m && zeros > 0 {
            m * (m / (zeros as f64)).ln()
        } else {
            raw
        };
        est.max(0.0).round()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_accuracy_sparse_dense_and_merge() {
        // Small cardinality -> sparse encoding + LinearCounting accuracy
        let mut small = HllSketch::new();
        for u in 0..40u32 {
            small.observe_uid(u);
        }
        let c_small = small.to_compact();
        assert!(matches!(c_small, CompactHll::Sparse(_)));
        let est_small = small.estimate();
        assert!(
            (est_small - 40.0).abs() <= 4.0,
            "small HLL estimate {est_small} too far from 40"
        );

        // Large cardinality across two disjoint halves -> dense + lossless merge
        let mut a = HllSketch::new();
        let mut b = HllSketch::new();
        for u in 0..50_000u32 {
            a.observe_uid(u);
        }
        for u in 50_000..100_000u32 {
            b.observe_uid(u);
        }
        let ca = a.to_compact();
        let cb = b.to_compact();
        assert!(matches!(ca, CompactHll::Dense(_)));

        let mut merged = HllSketch::new();
        merged.merge_compact(&ca);
        merged.merge_compact(&cb);
        let est = merged.estimate();
        let rel_err = (est - 100_000.0).abs() / 100_000.0;
        assert!(
            rel_err < 0.08,
            "merged HLL estimate {est} had relative error {rel_err:.4} (>8%)"
        );
    }
}
