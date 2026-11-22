use xxhash_rust::xxh3::xxh3_64;

/// Shared deterministic flag rollout slot in `0..10_000` (`xxh3_64("{flag_key}:{distinct_id}") % 10_000`).
#[inline]
pub fn flag_rollout_slot(flag_key: &str, distinct_id: &str) -> u32 {
    let mut buf = Vec::with_capacity(flag_key.len() + 1 + distinct_id.len());
    buf.extend_from_slice(flag_key.as_bytes());
    buf.push(b':');
    buf.extend_from_slice(distinct_id.as_bytes());
    (xxh3_64(&buf) % 10_000) as u32
}

/// Shared deterministic variant allocation slot in `0..total_weight` (`xxh3_64("var:{flag_key}:{distinct_id}") % total_weight`).
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

/// `xxh3_64("{domain}:{id}:{distinct_id}") % 10_000`: a slot independent of every flag's own slots.
fn domain_slot(domain: &str, id: &str, distinct_id: &str) -> u32 {
    let raw = format!("{domain}:{id}:{distinct_id}");
    (xxh3_64(raw.as_bytes()) % 10_000) as u32
}

/// A user is in holdout `holdout_id` when this is below the holdout's `bps`.
pub fn holdout_slot(holdout_id: &str, distinct_id: &str) -> u32 {
    domain_slot("holdout", holdout_id, distinct_id)
}

/// A user belongs to the layer experiment whose `[start_bps, end_bps)` range holds this.
pub fn layer_slot(layer_id: &str, distinct_id: &str) -> u32 {
    domain_slot("layer", layer_id, distinct_id)
}

/// Users in the first `bps` of a holdout get no flag attached to it.
#[derive(Debug, Clone, Copy)]
pub struct HoldoutGate<'a> {
    pub id: &'a str,
    pub bps: u32,
}

impl HoldoutGate<'_> {
    pub fn holds_out(&self, distinct_id: &str) -> bool {
        holdout_slot(self.id, distinct_id) < self.bps
    }
}

/// A flag in a layer only reaches users whose layer slot is in `[start_bps, end_bps)`; flags in
/// the same layer own disjoint ranges, so no user is in two of their experiments.
#[derive(Debug, Clone, Copy)]
pub struct LayerGate<'a> {
    pub id: &'a str,
    pub start_bps: u32,
    pub end_bps: u32,
}

/// Where a flag sits: an optional holdout and an optional layer range.
#[derive(Debug, Clone, Copy, Default)]
pub struct Placement<'a> {
    pub holdout: Option<HoldoutGate<'a>>,
    pub layer: Option<LayerGate<'a>>,
}

/// Evaluates a feature flag rule deterministically across Control Plane and SDKs (`P2-02`, `P2-03`).
///
/// With `cohort: Some(gate)`, the flag only matches users in the gate.
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
    evaluate_placed_flag_rule(
        flag_key,
        enabled,
        rollout_bps,
        cohort,
        Placement::default(),
        variants,
        distinct_id,
    )
}

/// [`evaluate_flag_rule`] with a holdout and layer. Order: enabled, holdout, layer, cohort,
/// rollout, variant. With an empty `placement` the result is exactly `evaluate_flag_rule`'s.
pub fn evaluate_placed_flag_rule<'a, I>(
    flag_key: &str,
    enabled: bool,
    rollout_bps: u32,
    cohort: Option<CohortGate<'_>>,
    placement: Placement<'_>,
    variants: I,
    distinct_id: &str,
) -> Option<String>
where
    I: IntoIterator<Item = (&'a str, u32)>,
{
    if !enabled || rollout_bps == 0 {
        return None;
    }
    if placement.holdout.is_some_and(|h| h.holds_out(distinct_id)) {
        return None;
    }
    if let Some(l) = placement.layer {
        let slot = layer_slot(l.id, distinct_id);
        if slot < l.start_bps || slot >= l.end_bps {
            return None;
        }
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

    #[test]
    fn holdout_and_layer_gates() {
        let eval = |p: Placement, id: &str| {
            evaluate_placed_flag_rule("f", true, 10_000, None, p, [("a", 5_000), ("b", 5_000)], id)
        };
        let holdout = HoldoutGate {
            id: "h",
            bps: 1_000,
        };
        let (mut held, mut layer_a, mut layer_b) = (0, 0, 0);
        for i in 0..20_000 {
            let id = format!("u{i}");
            let plain = eval(Placement::default(), &id);
            assert!(plain.is_some());
            match eval(
                Placement {
                    holdout: Some(holdout),
                    layer: None,
                },
                &id,
            ) {
                None => held += 1,
                v => assert_eq!(v, plain, "outside the holdout, assignment is unchanged"),
            }
            let a = eval(
                Placement {
                    holdout: None,
                    layer: Some(LayerGate {
                        id: "l",
                        start_bps: 0,
                        end_bps: 3_000,
                    }),
                },
                &id,
            );
            let b = eval(
                Placement {
                    holdout: None,
                    layer: Some(LayerGate {
                        id: "l",
                        start_bps: 3_000,
                        end_bps: 10_000,
                    }),
                },
                &id,
            );
            assert!(
                a.is_none() || b.is_none(),
                "{id} is in two experiments of one layer"
            );
            assert!(a.is_some() || b.is_some(), "the two ranges cover the layer");
            layer_a += usize::from(a.is_some());
            layer_b += usize::from(b.is_some());
        }
        assert!(
            (1_800..2_200).contains(&held),
            "10% holdout held {held} of 20000"
        );
        assert!(
            (5_700..6_300).contains(&layer_a),
            "30% layer range got {layer_a}"
        );
        assert_eq!(layer_a + layer_b, 20_000);
    }
}
