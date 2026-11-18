use xxhash_rust::xxh3::xxh3_64;

/// Shared deterministic flag rollout slot in `0..10_000` (`xxh3_64("{flag_key}:{distinct_id}") % 10_000`) (`P2-03`).
#[inline]
pub fn flag_rollout_slot(flag_key: &str, distinct_id: &str) -> u32 {
    let mut buf = Vec::with_capacity(flag_key.len() + 1 + distinct_id.len());
    buf.extend_from_slice(flag_key.as_bytes());
    buf.push(b':');
    buf.extend_from_slice(distinct_id.as_bytes());
    (xxh3_64(&buf) % 10_000) as u32
}

/// Shared deterministic variant allocation slot in `0..total_weight` (`xxh3_64("var:{flag_key}:{distinct_id}") % total_weight`) (`P2-03`).
#[inline]
pub fn flag_variant_slot(flag_key: &str, distinct_id: &str, total_weight: u32) -> u32 {
    if total_weight == 0 {
        return 0;
    }
    let mut buf = Vec::with_capacity(4 + flag_key.len() + 1 + distinct_id.len());
    buf.extend_from_slice(b"var:");
    buf.extend_from_slice(flag_key.as_bytes());
    buf.push(b':');
    buf.extend_from_slice(distinct_id.as_bytes());
    (xxh3_64(&buf) % u64::from(total_weight)) as u32
}

/// How a cohort member travels in the public flag bundle: lowercase 16-hex
/// `xxh3_64("cohort:{cohort_id}:{distinct_id}")`. The bundle is readable with a browser-safe key,
/// so it must not list customers' user ids; a client can still check its own id.
pub fn cohort_member_hash(cohort_id: &str, distinct_id: &str) -> String {
    let mut buf = Vec::with_capacity(7 + cohort_id.len() + 1 + distinct_id.len());
    buf.extend_from_slice(b"cohort:");
    buf.extend_from_slice(cohort_id.as_bytes());
    buf.push(b':');
    buf.extend_from_slice(distinct_id.as_bytes());
    format!("{:016x}", xxh3_64(&buf))
}

/// A cohort-targeted flag's audience: `member_hashes` from [`cohort_member_hash`], sorted.
#[derive(Debug, Clone, Copy)]
pub struct CohortGate<'a> {
    pub cohort_id: &'a str,
    pub member_hashes: &'a [String],
}

impl CohortGate<'_> {
    pub fn contains(&self, distinct_id: &str) -> bool {
        let h = cohort_member_hash(self.cohort_id, distinct_id);
        self.member_hashes
            .binary_search_by(|cand| cand.as_str().cmp(&h))
            .is_ok()
    }
}

/// Evaluates a feature flag rule deterministically across Control Plane and SDKs (`P2-02`, `P2-03`).
///
/// With `cohort: Some(gate)`, the flag only matches users in the gate (`P2-02`).
pub fn evaluate_flag_rule<'a, I>(
    flag_key: &str,
    enabled: bool,
    rollout_bps: u32,
    cohort: Option<CohortGate<'_>>,
    variants: I,
    distinct_id: &str,
) -> Option<String>
where
    I: IntoIterator<Item = (&'a str, u32)>,
{
    if !enabled || rollout_bps == 0 {
        return None;
    }
    if let Some(gate) = cohort {
        if !gate.contains(distinct_id) {
            return None;
        }
    }

    let slot = flag_rollout_slot(flag_key, distinct_id);
    if slot >= rollout_bps {
        return None;
    }

    let vec_vars: Vec<(&'a str, u32)> = variants.into_iter().collect();
    if vec_vars.is_empty() {
        return Some("on".to_string());
    }
    let total_weight: u32 = vec_vars.iter().map(|(_, w)| *w).sum();
    if total_weight == 0 {
        return vec_vars.first().map(|(k, _)| (*k).to_string());
    }

    let v_slot = flag_variant_slot(flag_key, distinct_id, total_weight);
    let mut cumulative = 0u32;
    for (k, w) in &vec_vars {
        cumulative = cumulative.saturating_add(*w);
        if v_slot < cumulative {
            return Some((*k).to_string());
        }
    }
    vec_vars.last().map(|(k, _)| (*k).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cohort_gate_matches_hashed_members_only() {
        // Pinned: sdks/ts-web/test/sdk.test.mjs asserts the same bytes from the web SDK.
        assert_eq!(
            cohort_member_hash("coh_beta", "beta_user_1"),
            "4270f71e9f59a803"
        );
        let mut hashes: Vec<String> = ["beta_user_1", "beta_user_2"]
            .iter()
            .map(|id| cohort_member_hash("coh_beta", id))
            .collect();
        hashes.sort();
        let gate = CohortGate {
            cohort_id: "coh_beta",
            member_hashes: &hashes,
        };
        let eval = |id| evaluate_flag_rule("f", true, 10_000, Some(gate), [], id);
        assert_eq!(eval("beta_user_1").as_deref(), Some("on"));
        assert_eq!(eval("someone_else"), None);
        // Salted by cohort: the same user's hash differs per cohort.
        assert_ne!(cohort_member_hash("coh_other", "beta_user_1"), hashes[0]);
    }
}
