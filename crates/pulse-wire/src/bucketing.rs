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

/// Evaluates a feature flag rule deterministically across Control Plane and SDKs (`P2-02`, `P2-03`).
///
/// If `cohort_members` is `Some(sorted_allowlist)`, the flag ONLY matches users whose `distinct_id`
/// appears in `sorted_allowlist` (`P2-02`).
pub fn evaluate_flag_rule<'a, I>(
    flag_key: &str,
    enabled: bool,
    rollout_bps: u32,
    cohort_members: Option<&[String]>,
    variants: I,
    distinct_id: &str,
) -> Option<String>
where
    I: IntoIterator<Item = (&'a str, u32)>,
{
    if !enabled || rollout_bps == 0 {
        return None;
    }
    if let Some(allowlist) = cohort_members {
        let is_member = allowlist
            .binary_search_by(|cand| cand.as_str().cmp(distinct_id))
            .is_ok()
            || allowlist.iter().any(|cand| cand == distinct_id);
        if !is_member {
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
