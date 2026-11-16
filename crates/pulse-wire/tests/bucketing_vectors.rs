use std::path::PathBuf;

use pulse_wire::{evaluate_flag_rule, flag_rollout_slot, flag_variant_slot};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BucketingVector {
    flag_key: String,
    distinct_id: String,
    xxh3_rollout_hex: String,
    rollout_slot: u32,
    variant_slot_10000: u32,
    variant_50_50: String,
}

fn vectors_file_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sdks/bucketing_vectors.json")
}

#[test]
fn p2_03_shared_bucketing_test_vectors_match_rust_xxh3_64() {
    let inputs: Vec<(&str, String)> = vec![
        ("one_tap_upi", "".to_string()),
        ("a", "b".to_string()),
        ("flag", "u1".to_string()),
        ("one_tap_upi_mandate", "anon_839201abcdef".to_string()),
        ("one_tap_upi_mandate", "+919876543210".to_string()),
        ("kyc_video_v2", "user_bengaluru_00042".to_string()),
        ("hindi_utf8", "उपयोगकर्ता_भारत_🚀_12345".to_string()),
        ("len_16_exact", "0123".to_string()),
        ("len_64_mid", "x".repeat(53)),
        ("len_160_mid2", "m".repeat(147)),
        ("len_320_long", "L".repeat(307)),
        ("len_1200_multiblock", "pulse_analytics_india_".repeat(55)),
    ];

    let computed: Vec<BucketingVector> = inputs
        .into_iter()
        .map(|(fk, did)| {
            let raw = format!("{fk}:{did}");
            let h = xxh3_64(raw.as_bytes());
            let rollout_slot = flag_rollout_slot(fk, &did);
            let variant_slot_10000 = flag_variant_slot(fk, &did, 10_000);
            let variant_50_50 = evaluate_flag_rule(
                fk,
                true,
                10_000,
                None,
                [("control", 5_000u32), ("treatment", 5_000u32)],
                &did,
            )
            .unwrap();
            BucketingVector {
                flag_key: fk.to_string(),
                distinct_id: did,
                xxh3_rollout_hex: format!("{h:016x}"),
                rollout_slot,
                variant_slot_10000,
                variant_50_50,
            }
        })
        .collect();

    let path = vectors_file_path();
    if !path.exists() {
        let json = serde_json::to_string_pretty(&computed).unwrap();
        std::fs::write(&path, format!("{json}\n")).unwrap();
    }

    let on_disk: Vec<BucketingVector> =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        on_disk, computed,
        "sdks/bucketing_vectors.json drifted from canonical Rust xxh3_64 bucketing!"
    );
}
