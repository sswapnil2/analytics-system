pub mod dedup;
pub mod governance;
pub mod http;
pub mod quic;
pub mod service;

pub use dedup::{normalize_event_timestamp, DedupRegistry, DEDUP_WINDOW_MS};
pub use governance::{
    DiscoveredEventSchema, GovernanceProvider, InMemoryGovernance, ProjectLexiconRules,
};
pub use http::build_ingest_router;
pub use quic::{
    send_quic_ingest_batch, spawn_quic_listener, QuicIngestHandle, QUIC_OP_IDENTIFY,
    QUIC_OP_INGEST, QUIC_OP_REPLAY,
};
pub use service::{
    IdentifyRequest, IdentifyResponse, IngestError, IngestMetrics, IngestResponse, IngestService,
    ReplayIngestResponse, Result,
};

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use pulse_identity::IdentityRouter;
    use pulse_storage::{SessionHeader, StorageConfig, StorageEngine};
    use pulse_wire::{PropValue, WireBatch, WireEvent};

    use super::*;

    #[tokio::test]
    async fn gate2_500k_events_dedup_polymorphic_wal_recovery_compaction_and_session_pruning() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));

        let base_ts: i64 = 1_762_000_000_000; // Fixed calendar day in UTC
        let num_unique: usize = 500_000;
        let batch_size: usize = 10_000;

        // Config with 16 buckets and high L0 threshold so all 500k remain in L0 + WAL until abrupt crash!
        let pre_crash_cfg = StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 1_000_000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 4,
        };

        let mut retry_sample_batch: Option<WireBatch> = None;

        // === PHASE 1: Ingest 500k unique + 100k (20%) duplicates with 15% out-of-order & polymorphic `amount` ===
        {
            let id_router = Arc::new(IdentityRouter::new(store.clone()));
            let storage = StorageEngine::new(pre_crash_cfg.clone(), store.clone(), id_router);
            let gov = InMemoryGovernance::new();
            gov.register_api_key("pk_gate2", "proj_gate2");
            let ingest = IngestService::new(storage.clone(), gov);

            let mut total_accepted = 0usize;
            let mut total_deduped = 0usize;

            for chunk_idx in 0..(num_unique / batch_size) {
                let start = chunk_idx * batch_size;
                let mut events = Vec::with_capacity(batch_size + 2_000);

                for offset in 0..batch_size {
                    let i = start + offset;
                    let user_idx = i % 5_000;
                    let sess_num = i / 50; // 10,000 total distinct sessions (50 events/session)
                    let session_id = format!("sess_{sess_num:06}");
                    let distinct_id = format!("user_{user_idx:05}");
                    let routing_seed = format!("dev_{user_idx:05}");

                    // Rare target event ("Rare_Needle_Alert") appears in ONLY ~3% of sessions (sess_num % 40 == 0)
                    let event_name = if sess_num % 40 == 0 && offset % 50 == 7 {
                        "Rare_Needle_Alert".to_string()
                    } else {
                        format!("Common_Step_{}", i % 8)
                    };

                    // 15% late out-of-order timestamps (3 hours earlier on same UTC day)
                    let mut event_ts = base_ts + ((i % 20_000) as i64) * 500;
                    if i % 20 < 3 {
                        event_ts -= 3 * 3600 * 1000 - 1_000_000;
                    }
                    let sent_ts = base_ts + 25_000_000;

                    // Polymorphic collision on `amount`: Int, Float, and Str in the exact same batch!
                    let amount_val = match i % 3 {
                        0 => PropValue::Int(100 + (i % 900) as i64),
                        1 => PropValue::Float(100.50 + (i % 900) as f64),
                        _ => PropValue::Str(format!("{}", 100 + (i % 900))),
                    };

                    let mut props = vec![
                        ("amount".to_string(), amount_val),
                        (
                            "network".to_string(),
                            PropValue::Str(if i % 2 == 0 { "Jio_4G" } else { "Airtel_5G" }.into()),
                        ),
                    ];
                    // Also inject >512 distinct sparse property keys occasionally to exercise Tier C + 64-bit Bloom!
                    if i % 250 == 0 {
                        props.push((
                            format!("overflow_attr_{}", i % 620),
                            PropValue::Str("rare_val".to_string()),
                        ));
                    }

                    let ev = WireEvent {
                        event_uuid: WireEvent::deterministic_uuid(
                            &distinct_id,
                            &session_id,
                            &event_name,
                            base_ts,
                            i as u64,
                        ),
                        routing_seed,
                        distinct_id,
                        session_id,
                        event_name,
                        client_event_ts_ms: event_ts,
                        client_sent_ts_ms: sent_ts,
                        props,
                        replay_offset_kb: 0,
                    };
                    events.push(ev);
                }

                // Append 20% duplicate retries (first 2,000 events of this batch resent)
                for dup_idx in 0..2_000 {
                    events.push(events[dup_idx].clone());
                }

                let wire_batch = WireBatch::new(events);
                if chunk_idx == 0 {
                    retry_sample_batch = Some(WireBatch::new(wire_batch.events[0..1_000].to_vec()));
                }

                let bin = wire_batch.encode_binary(1).expect("encode PLSB");
                let resp = ingest
                    .ingest_raw("pk_gate2", Some("application/x-pulse-frame"), &bin)
                    .await
                    .expect("ingest batch");

                total_accepted += resp.accepted;
                total_deduped += resp.deduped;
            }

            assert_eq!(total_accepted, 500_000);
            assert_eq!(total_deduped, 100_000);

            let ps = storage.get_or_open_project("proj_gate2").await.unwrap();
            assert_eq!(ps.total_l0_rows(), 500_000);
            assert_eq!(ps.snapshot().manifest.total_segments(), 0);
            // Simulate ABRUPT NODE CRASH here: drop `ingest` and `storage` WITHOUT flushing L0 to L1!
        }

        // === PHASE 2: Abrupt Restart WAL Recovery + Post-Crash Retry Idempotency + Compaction + SessionIndex Pruning ===
        let post_crash_cfg = StorageConfig {
            num_buckets: 16,
            l0_flush_row_threshold: 250_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        };
        let id_router2 = Arc::new(IdentityRouter::new(store.clone()));
        let storage2 = StorageEngine::new(post_crash_cfg, store.clone(), id_router2.clone());
        let gov2 = InMemoryGovernance::new();
        gov2.register_api_key("pk_gate2", "proj_gate2");
        let ingest2 = IngestService::new(storage2.clone(), gov2);

        // Verify WAL replay restored 100% of all 500,000 acknowledged events (RPO = 0)!
        let ps2 = storage2
            .get_or_open_project("proj_gate2")
            .await
            .expect("recover project");
        assert_eq!(
            ps2.total_l0_rows(),
            500_000,
            "WAL recovery must restore 100% of 500,000 acknowledged events"
        );

        // Retry a pre-crash batch AFTER restart and assert 100% deduplicated (0 duplicate inflation!)
        let retry_bin = retry_sample_batch.unwrap().encode_binary(1).unwrap();
        let retry_resp = ingest2
            .ingest_raw("pk_gate2", Some("application/x-pulse-frame"), &retry_bin)
            .await
            .unwrap();
        assert_eq!(retry_resp.accepted, 0);
        assert_eq!(retry_resp.deduped, 1_000);

        // Flush L0 -> L1 Vortex segments
        let segs_written = storage2.flush_all_projects().await.expect("flush L0");
        assert!(segs_written >= 16);
        assert_eq!(ps2.total_l0_rows(), 0);
        assert_eq!(ps2.snapshot().manifest.total_l1_rows(), 500_000);

        // Now send 3 additional small late-arriving offline waves into bucket 0's partition and flush each
        // so `(date, bucket)` accumulates >= 4 overlapping L1 segments, then trigger Compactor!
        for wave in 0..3u64 {
            let mut late_events = Vec::new();
            for j in 0..200u64 {
                let uid_str = format!("late_user_{wave}_{j}");
                late_events.push(WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        &uid_str,
                        &format!("late_sess_{wave}_{j}"),
                        "Common_Step_1",
                        base_ts + 1_000,
                        9_000_000 + wave * 1000 + j,
                    ),
                    routing_seed: "seed_forcing_same_partition".to_string(),
                    distinct_id: uid_str,
                    session_id: format!("late_sess_{wave}_{j}"),
                    event_name: "Common_Step_1".to_string(),
                    client_event_ts_ms: base_ts + 5_000 + (j as i64),
                    client_sent_ts_ms: base_ts + 10_000,
                    props: vec![("amount".to_string(), PropValue::Int(777))],
                    replay_offset_kb: 0,
                });
            }
            let b = WireBatch::new(late_events).encode_binary(1).unwrap();
            let r = ingest2
                .ingest_raw("pk_gate2", Some("application/x-pulse-frame"), &b)
                .await
                .unwrap();
            assert_eq!(r.accepted, 200);
            storage2.flush_all_projects().await.unwrap();
        }

        let pre_compact_segs = ps2.snapshot().manifest.total_segments();
        let compacted_partitions = storage2
            .compact_all_projects()
            .await
            .expect("run compactor");
        assert!(
            compacted_partitions >= 1,
            "expected at least 1 partition with >=4 overlapping segments to compact"
        );
        let post_snap = ps2.snapshot();
        assert!(post_snap.manifest.total_segments() < pre_compact_segs);
        assert_eq!(post_snap.manifest.total_l1_rows(), 500_600);

        // Verify polymorphic Tier B columns (`p_<amount>_i64`, `p_<amount>_f64`, `p_<amount>_str`) coexist cleanly!
        let amount_pk = ps2.dictionary().lookup_prop_key("amount").unwrap();
        let any_seg = post_snap
            .manifest
            .active_segments
            .values()
            .next()
            .unwrap()
            .first()
            .unwrap();
        let amount_meta = any_seg
            .footer
            .tier_b_columns
            .iter()
            .find(|c| c.prop_key_id == amount_pk)
            .expect("amount promoted to Tier B");
        assert!(amount_meta.has_i64 && amount_meta.has_f64 && amount_meta.has_str);

        // === Verify SessionIndex 64-bit Bloom bitmask skips >90% of non-matching sessions ===
        let needle_event_id = ps2
            .dictionary()
            .lookup_event("Rare_Needle_Alert")
            .expect("Rare_Needle_Alert interned");
        let needle_mask = SessionHeader::event_bit(needle_event_id);

        let reader = storage2.segment_reader();
        let mut total_sessions = 0usize;
        let mut skipped_sessions = 0usize;

        for seg in post_snap.manifest.active_segments.values().flatten() {
            let sidx = reader.read_session_index(seg).await.unwrap();
            total_sessions += sidx.headers.len();
            let matched = sidx.matching_session_ranges(|h| h.may_contain_any_event(needle_mask));
            skipped_sessions += sidx.headers.len() - matched.len();
        }

        let skip_ratio = skipped_sessions as f64 / total_sessions as f64;
        assert!(
            skip_ratio > 0.90,
            "expected SessionIndex bitmask to skip >90% of sessions, skipped {:.2}% ({}/{})",
            skip_ratio * 100.0,
            skipped_sessions,
            total_sessions
        );

        // Also verify that scanning for an unseen event bit skips 100% of sessions with ZERO Tier A/B reads!
        let reader_before = reader.tier_ab_reads.load(Ordering::Relaxed);
        let unseen_mask = SessionHeader::event_bit(61); // event_id 61 never emitted
        for seg in post_snap.manifest.active_segments.values().flatten() {
            let (_skipped, matched, maybe_batch) = reader
                .scan_sessions_pruned(seg, unseen_mask, 0)
                .await
                .unwrap();
            assert!(matched.is_empty());
            assert!(maybe_batch.is_none());
        }
        let reader_after = reader.tier_ab_reads.load(Ordering::Relaxed);
        assert_eq!(
            reader_before, reader_after,
            "SessionIndex bitmask pruning must not read Tier A/B Vortex arrays when 0 sessions match"
        );

        id_router2.close_all().await.unwrap();
    }

    #[tokio::test]
    async fn quic_listener_and_lexicon_governance_work_end_to_end() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp_dir.path()).expect("local fs"));
        let id_router = Arc::new(IdentityRouter::new(store.clone()));
        let storage = StorageEngine::new(StorageConfig::default(), store, id_router.clone());

        let gov = InMemoryGovernance::new();
        gov.register_api_key("pk_quic", "proj_quic");
        let mut rules = ProjectLexiconRules::default();
        rules.dropped_events.insert("Noisy_Debug_Ping".to_string());
        rules.pii_masked_properties.insert("pan_number".to_string());
        gov.set_rules("proj_quic", rules);

        let service = IngestService::new(storage.clone(), gov);
        let quic = spawn_quic_listener("127.0.0.1:0".parse().unwrap(), service.clone())
            .expect("bind quic");

        let batch = WireBatch::new(vec![
            WireEvent {
                event_uuid: [1u8; 16],
                routing_seed: "d1".into(),
                distinct_id: "u1".into(),
                session_id: "s1".into(),
                event_name: "Noisy_Debug_Ping".into(),
                client_event_ts_ms: 1_762_000_000_000,
                client_sent_ts_ms: 1_762_000_000_100,
                props: vec![],
                replay_offset_kb: 0,
            },
            WireEvent {
                event_uuid: [2u8; 16],
                routing_seed: "d1".into(),
                distinct_id: "u1".into(),
                session_id: "s1".into(),
                event_name: "KYC_PAN_Submitted".into(),
                client_event_ts_ms: 1_762_000_001_000,
                client_sent_ts_ms: 1_762_000_001_100,
                props: vec![("pan_number".into(), PropValue::Str("ABCDE1234F".into()))],
                replay_offset_kb: 0,
            },
        ]);

        let payload = batch.encode_binary(3).unwrap();
        let resp = send_quic_ingest_batch(quic.local_addr, &quic.cert_der, "pk_quic", &payload)
            .await
            .expect("quic ingest");
        assert_eq!(resp.accepted, 1);
        assert_eq!(resp.dropped, 1);

        let ps = storage.get_or_open_project("proj_quic").await.unwrap();
        let snap = ps.snapshot();
        let mut found_masked = false;
        for b in 0..ps.config().num_buckets {
            for row in ps.snapshot_l0_bucket(&snap, b as u8) {
                for (_, pv) in row.props {
                    if let PropValue::Str(s) = pv {
                        assert!(!s.contains("ABCDE1234F"));
                        found_masked = true;
                    }
                }
            }
        }
        assert!(found_masked);
        quic.endpoint.close(0u32.into(), b"shutdown");
        id_router.close_all().await.unwrap();
    }

    #[tokio::test]
    async fn p0_07_quic_authenticates_header_before_body_and_caps_streams() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let id_router = Arc::new(IdentityRouter::new(store.clone()));
        let storage = StorageEngine::new(
            StorageConfig {
                num_buckets: 4,
                l0_flush_row_threshold: 10_000,
                l0_flush_max_age: Duration::from_secs(60),
                compaction_min_segments: 4,
            },
            store,
            id_router.clone(),
        );
        let gov = InMemoryGovernance::new();
        gov.register_api_key("pk_valid", "proj_quic");
        let ingest = IngestService::new(storage, gov);

        let quic = spawn_quic_listener("127.0.0.1:0".parse().unwrap(), ingest).expect("bind quic");

        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(
                quic.cert_der.as_slice(),
            ))
            .unwrap();
        let client_cfg = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_cfg);

        let conn = endpoint
            .connect(quic.local_addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();

        // Send ONLY the auth header claiming a 16 MiB body WITHOUT sending the body or closing `send`!
        // A server that buffers `read_to_end` before checking auth will stall here instead of rejecting immediately.
        let bad_key = b"pk_attacker_bad";
        let mut header = Vec::new();
        header.push(QUIC_OP_INGEST);
        header.extend_from_slice(&(bad_key.len() as u16).to_le_bytes());
        header.extend_from_slice(bad_key);
        header.extend_from_slice(&(16_000_000u32).to_le_bytes());
        send.write_all(&header).await.unwrap();

        let reply = tokio::time::timeout(Duration::from_millis(500), recv.read_to_end(4096))
            .await
            .expect("QUIC server must reject unauthenticated stream before waiting for body")
            .unwrap();
        let reply_str = String::from_utf8_lossy(&reply);
        assert!(
            reply_str.contains("unauthorized"),
            "expected unauthorized error, got: {reply_str}"
        );

        quic.endpoint.close(0u32.into(), b"shutdown");
        id_router.close_all().await.unwrap();
    }
}
