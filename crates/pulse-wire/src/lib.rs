pub mod batch;
pub mod bucketing;
pub mod error;
pub mod event;
pub mod replay;

pub use batch::{
    decode_zstd_bounded, DictionaryRegistry, WireBatch, BATCH_MAGIC, DEFAULT_DICT_ID,
    FLAG_DICT_ZSTD, FLAG_RAW_ZSTD, MAX_DECOMPRESSED_BYTES, WIRE_VERSION,
};
pub use bucketing::{evaluate_flag_rule, flag_rollout_slot, flag_variant_slot};
pub use error::{Result, WireError};
pub use event::{PropValue, WireEvent};
pub use replay::{
    mask_input_value, InteractionKind, NodeKind, ReplayChunk, WireNode, WireframeOp,
    FIXED_MASK_PLACEHOLDER, MASK_CHAR, REPLAY_MAGIC,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_polymorphic_batch(count: usize) -> WireBatch {
        let event_names = [
            "App_Open",
            "KYC_PAN_Submitted",
            "Video_KYC_Completed",
            "UPI_Autopay_Mandate_Approved",
            "First_Disbursal_Success",
        ];
        let cities = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Pune"];
        let platforms = ["Android", "iOS", "Web"];

        let mut events = Vec::with_capacity(count);
        let base_ts: i64 = 1_760_000_000_000;

        for i in 0..count {
            let user_idx = i % 800;
            let routing_seed = format!("dev_install_{user_idx:05}");
            let distinct_id = if i % 3 == 0 {
                format!("anon_{user_idx:05}")
            } else {
                format!("+9198450{user_idx:05}")
            };
            let session_id = format!("sess_{user_idx:05}_{}", i / 20);
            let event_name = event_names[i % event_names.len()].to_string();
            let ts = base_ts + (i as i64) * 350;

            // Exercise all 5 PropValue variants including polymorphic collisions on "amount"
            let amount_prop = match i % 4 {
                0 => PropValue::Int(499 + (i as i64 % 5000)),
                1 => PropValue::Float(499.75 + (i as f64 * 0.25)),
                2 => PropValue::Str(format!("{}", 499 + (i % 5000))),
                _ => PropValue::Null,
            };

            let props = vec![
                ("amount".to_string(), amount_prop),
                (
                    "city".to_string(),
                    PropValue::Str(cities[i % cities.len()].to_string()),
                ),
                (
                    "platform".to_string(),
                    PropValue::Str(platforms[i % platforms.len()].to_string()),
                ),
                ("kyc_verified".to_string(), PropValue::Bool(i % 2 == 0)),
                ("attempt_seq".to_string(), PropValue::Int((i % 7) as i64)),
            ];

            events.push(WireEvent {
                event_uuid: WireEvent::deterministic_uuid(
                    &distinct_id,
                    &session_id,
                    &event_name,
                    ts,
                    i as u64,
                ),
                routing_seed,
                distinct_id,
                session_id,
                event_name,
                client_event_ts_ms: ts,
                client_sent_ts_ms: ts + 120,
                props,
                replay_offset_kb: (i % 128) as u32,
            });
        }

        WireBatch::new(events)
    }

    #[test]
    fn gate1_wire_batch_10k_polymorphic_roundtrip_and_compression_ratio() {
        let batch = sample_polymorphic_batch(10_000);
        let json_bytes = batch.encode_json().expect("encode json");
        let bin_raw = batch.encode_binary(3).expect("encode binary raw zstd");

        let registry = DictionaryRegistry::default();
        let bin_dict = batch
            .encode_binary_with_dict(DEFAULT_DICT_ID, &registry, 3)
            .expect("encode binary dict zstd");

        let decoded_raw = WireBatch::decode_binary(&bin_raw).expect("decode raw zstd");
        assert_eq!(decoded_raw.len(), 10_000);
        assert_eq!(decoded_raw, batch);

        let decoded_dict =
            WireBatch::decode_binary_with_registry(&bin_dict, &registry).expect("decode dict zstd");
        assert_eq!(decoded_dict, batch);

        let savings_ratio = 1.0 - (bin_raw.len() as f64 / json_bytes.len() as f64);
        assert!(
            savings_ratio >= 0.65,
            "expected >= 65% compression vs JSON, got {:.2}% (json={}B, bin={}B)",
            savings_ratio * 100.0,
            json_bytes.len(),
            bin_raw.len()
        );
    }

    #[test]
    fn json_fallback_and_auto_detection_works() {
        let json_payload = br#"{
            "events": [
                {
                    "distinct_id": "anon_42",
                    "routing_seed": "dev_42",
                    "session_id": "sess_1",
                    "event_name": "KYC_PAN_Submitted",
                    "client_event_ts_ms": 1760000001000,
                    "props": {
                        "amount": 1500,
                        "fee": 27.5,
                        "instant": true,
                        "coupon": null,
                        "bank": "HDFC"
                    }
                }
            ]
        }"#;

        let reg = DictionaryRegistry::default();
        let decoded =
            WireBatch::decode_auto(json_payload, Some("application/json"), &reg).expect("json");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.events[0].event_name, "KYC_PAN_Submitted");
        assert_eq!(
            decoded.events[0].get_prop("amount"),
            Some(&PropValue::Int(1500))
        );
        assert_eq!(
            decoded.events[0].get_prop("fee"),
            Some(&PropValue::Float(27.5))
        );
        assert_eq!(
            decoded.events[0].get_prop("bank"),
            Some(&PropValue::Str("HDFC".to_string()))
        );
    }

    #[test]
    fn wireframe_replay_stream_and_pii_masking_roundtrip() {
        let chunk1 = ReplayChunk {
            project_id: "proj_fintech".to_string(),
            session_id: "sess_frustrated_01".to_string(),
            chunk_start_ts_ms: 1_760_000_100_000,
            ops: vec![
                WireframeOp::FullSnapshot {
                    width: 390,
                    height: 844,
                    nodes: vec![
                        WireNode {
                            node_id: 1,
                            parent_id: 0,
                            kind: NodeKind::Screen,
                            x: 0,
                            y: 0,
                            w: 390,
                            h: 844,
                            bg_rgba: 0x090D_16FF,
                            masked_label: "KYC Verification".to_string(),
                        },
                        WireNode {
                            node_id: 2,
                            parent_id: 1,
                            kind: NodeKind::MaskedInput,
                            x: 24,
                            y: 180,
                            w: 342,
                            h: 52,
                            bg_rgba: 0x161F_30FF,
                            // Even if unmasked text is passed accidentally, WireNode::write_to masks it!
                            masked_label: "PAN: ABCDE1234F".to_string(),
                        },
                        WireNode {
                            node_id: 3,
                            parent_id: 1,
                            kind: NodeKind::Button,
                            x: 24,
                            y: 260,
                            w: 342,
                            h: 54,
                            bg_rgba: 0x3B82_F6FF,
                            masked_label: "Verify PAN".to_string(),
                        },
                    ],
                },
                WireframeOp::PointerInteraction {
                    ts_offset_ms: 850,
                    x: 195,
                    y: 285,
                    kind: InteractionKind::RageTap { count: 4 },
                },
                WireframeOp::NetworkBreadcrumb {
                    ts_offset_ms: 1200,
                    method: "POST".to_string(),
                    path: "/api/kyc/nsdl-verify".to_string(),
                    status_code: 504,
                    duration_ms: 5012,
                },
            ],
        };

        let chunk2 = ReplayChunk {
            project_id: "proj_fintech".to_string(),
            session_id: "sess_frustrated_01".to_string(),
            chunk_start_ts_ms: 1_760_000_105_000,
            ops: vec![WireframeOp::NodeUpsert(WireNode {
                node_id: 4,
                parent_id: 1,
                kind: NodeKind::ErrorBanner,
                x: 24,
                y: 340,
                w: 342,
                h: 48,
                bg_rgba: 0xEF44_44FF,
                masked_label: "NSDL Gateway 504 Timeout".to_string(),
            })],
        };

        let bin1 = chunk1.encode_binary(3).expect("encode chunk1");
        let bin2 = chunk2.encode_binary(3).expect("encode chunk2");

        let mut stream = Vec::new();
        stream.extend_from_slice(&bin1);
        let offset2 = stream.len();
        stream.extend_from_slice(&bin2);

        let all_chunks = ReplayChunk::decode_stream(&stream).expect("decode full stream");
        assert_eq!(all_chunks.len(), 2);
        assert!(all_chunks[0].has_rage_tap());
        assert!(all_chunks[0].has_error());

        if let WireframeOp::FullSnapshot { nodes, .. } = &all_chunks[0].ops[0] {
            assert_eq!(nodes[1].masked_label, "████");
            assert!(!nodes[1].masked_label.contains("ABCDE1234F"));
        } else {
            panic!("expected FullSnapshot");
        }

        // Byte-range slice starting straight at chunk2's offset
        let tail_chunks =
            ReplayChunk::decode_stream(&stream[offset2..]).expect("decode range slice");
        assert_eq!(tail_chunks.len(), 1);
        assert_eq!(tail_chunks[0], chunk2);
    }
}
