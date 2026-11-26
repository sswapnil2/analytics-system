//! M4a verification tests: W1 graceful shutdown drain, bounded crash loss,
//! Feature #23 dead-letter reason storage + CLI erasure filtering (with BYOK + live writer),
//! and Feature #24 Prometheus metrics + alert rules.

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use pulse_node::{
    inspect_dead_letters, start_pulse_node, start_pulse_node_with_store, NodeConfig, NodeRole,
    RunningNode,
};
use pulse_wire::{PropValue, WireBatch, WireEvent};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const ADMIN: &str = "sk_root_m4a_test_token_00000000000000000000000000";
const TEST_BYOK_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, val: Option<&str>) -> Self {
        let prev = std::env::var(key).ok();
        match val {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

fn node_config(dir: &Path, pid: &str) -> NodeConfig {
    NodeConfig {
        role: NodeRole::All,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.join("node"),
        web_dist_dir: dir.join("no_web"),
        default_project_id: pid.into(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN.into()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 4,
    }
}

async fn post_events(node: &RunningNode, key: &str, users: &[&str], ts_ms: i64) -> u16 {
    let events: Vec<Value> = users
        .iter()
        .map(|u| {
            json!({
                "distinct_id": u,
                "event_name": "App_Open",
                "client_event_ts_ms": ts_ms,
            })
        })
        .collect();
    http_client()
        .post(format!("http://{}/v1/ingest", node.http_addr))
        .header("x-pulse-key", key)
        .json(&json!({ "events": events }))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[test]
fn w1_graceful_shutdown_drains_all_and_abrupt_crash_bounds_loss() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _ttl = EnvGuard::set("PULSE_WRITER_LEASE_TTL_SECS", Some("3"));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // 1) Graceful shutdown drains 100% of W1 buffered events even with a 60s flush timer.
    {
        let _flush_ms = EnvGuard::set("PULSE_WRITER_FLUSH_MS", Some("60000"));
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        rt.block_on(async {
            let node =
                start_pulse_node_with_store(node_config(tmp.path(), "proj_shut"), store.clone())
                    .await
                    .unwrap();
            let key = node
                .bootstrap_credentials
                .generated_ingest_key
                .clone()
                .unwrap();
            assert_eq!(
                post_events(&node, &key, &["u1", "u2", "u3", "u4"], 1_790_000_000_000).await,
                200
            );
            // Still buffered in memory (60s flush interval).
            assert_eq!(node.ingest.buffer_stats().buffer_events, 4);
            node.shutdown().await;

            // Restart and confirm 100% of buffered events survived shutdown.
            let node2 = start_pulse_node_with_store(node_config(tmp.path(), "proj_shut"), store)
                .await
                .unwrap();
            let ps2 = node2.storage.open_for_read("proj_shut").await.unwrap();
            let rows = ps2.snapshot().manifest.total_l1_rows() as usize + ps2.total_l0_rows();
            assert_eq!(rows, 4, "graceful shutdown must drain 100% of W1 buffer");
            node2.shutdown().await;
        });
    }

    // 2) Abrupt crash (`mem::forget` + runtime teardown): events older than the 20ms flush window
    //    are durable in WAL; at most the most recent un-WALed batch (<= 20ms) is lost.
    let tmp2 = tempfile::tempdir().unwrap();
    let dir2 = tmp2.path().to_path_buf();
    let store2 = Arc::new(InMemory::new());
    let s2_clone = store2.clone();
    std::thread::spawn(move || {
        let rt_c = tokio::runtime::Runtime::new().unwrap();
        rt_c.block_on(async move {
            let node = start_pulse_node_with_store(node_config(&dir2, "proj_crash"), s2_clone)
                .await
                .unwrap();
            let key = node
                .bootstrap_credentials
                .generated_ingest_key
                .clone()
                .unwrap();
            // Wave A: wait >20ms so it flushes to WAL.
            assert_eq!(
                post_events(&node, &key, &["w1", "w2", "w3"], 1_790_000_000_000).await,
                200
            );
            tokio::time::sleep(Duration::from_millis(80)).await;
            // Wave B: immediately crash (lost or flushed depending on timing, strictly <= 1 batch).
            assert_eq!(
                post_events(&node, &key, &["w4"], 1_790_000_001_000).await,
                200
            );
            std::mem::forget(node);
        });
        rt_c.shutdown_background();
    })
    .join()
    .unwrap();

    rt.block_on(async {
        let node = start_pulse_node_with_store(node_config(tmp2.path(), "proj_crash"), store2)
            .await
            .unwrap();
        let mut ps_opt = None;
        for _ in 0..40 {
            if let Ok(ps) = node.storage.open_for_read("proj_crash").await {
                ps_opt = Some(ps);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let ps = ps_opt.expect("restarted node must open project after lease expires");
        let rows = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
        assert!(
            (3..=4).contains(&rows),
            "expected all 3 pre-window events durable and at most 1 in-window lost, got {rows}"
        );
        node.shutdown().await;
    });
}

#[test]
fn dead_letters_cli_lists_plsd_reasons_with_byok_and_filters_erased_without_fencing() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // 1) Inspecting a missing data_dir must NOT create directories, hot_store_mode, or pulse_meta.db.
        let missing_tmp = tempfile::tempdir().unwrap();
        let missing_dir = missing_tmp.path().join("nonexistent_data_dir");
        let empty = inspect_dead_letters(&missing_dir, "proj_missing", None, None)
            .await
            .unwrap();
        assert!(empty.is_empty());
        assert!(
            !missing_dir.exists(),
            "inspect_dead_letters must not create missing data_dir"
        );

        // 2) Live BYOK-encrypted node with key rotation, aliases, erasures, PLSD + legacy PLSB.
        let tmp = tempfile::tempdir().unwrap();
        let pid = "proj_dl_cli";
        let node = start_pulse_node(node_config(tmp.path(), pid)).await.unwrap();
        let data_dir = tmp.path().join("node");
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        // Rotate BYOK encryption key to confirm inspect_dead_letters hydrates system_keyring!
        let rot_resp = http_client()
            .post(format!(
                "http://{}/v1/system/encryption/rotate",
                node.http_addr
            ))
            .bearer_auth(ADMIN)
            .json(&json!({
                "secret": "1111111111111111111111111111111111111111111111111111111111111111"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(rot_resp.status(), 200);

        // Create users & alias: `anon_aliased` -> `user_erased_alias`, plus `user_erased` and `user_kept`.
        let t0 = 1_790_000_000_000i64;
        assert_eq!(
            post_events(
                &node,
                &key,
                &["user_kept", "user_erased", "anon_aliased"],
                t0
            )
            .await,
            200
        );
        node.ingest.flush_all().await.unwrap();
        let id_resp = http_client()
            .post(format!("http://{}/v1/identify", node.http_addr))
            .header("x-pulse-key", &key)
            .json(&json!({ "anon_id": "anon_aliased", "user_id": "user_erased_alias" }))
            .send()
            .await
            .unwrap();
        assert_eq!(id_resp.status(), 200);

        // Flush identity graph so DbReader sees all interned UIDs & aliases.
        node.storage.identity_router().flush_all().await.unwrap();

        // Erase `user_erased` and `user_erased_alias` (which hides `anon_aliased` too).
        for uid in ["user_erased", "user_erased_alias"] {
            let del = http_client()
                .delete(format!(
                    "http://{}/v1/projects/{pid}/users/{uid}",
                    node.http_addr
                ))
                .bearer_auth(ADMIN)
                .send()
                .await
                .unwrap();
            assert_eq!(del.status(), 200);
        }

        let ps = node.storage.open_for_read(pid).await.unwrap();
        let make_ev = |distinct_id: &str, name: &str, ts_ms: i64, salt: u64| WireEvent {
            event_uuid: WireEvent::deterministic_uuid(distinct_id, "s", name, ts_ms, salt),
            routing_seed: distinct_id.into(),
            distinct_id: distinct_id.into(),
            session_id: "s".into(),
            event_name: name.into(),
            client_event_ts_ms: ts_ms,
            client_sent_ts_ms: 0,
            props: vec![("k".into(), PropValue::Str("v".into()))],
            replay_offset_kb: 0,
        };

        // Write PLSD dead letters via ProjectStorage (encrypted under rotated BYOK key!).
        let old_ts = t0; // <= erased_at_ms
        let future_ts = chrono::Utc::now().timestamp_millis() + 60_000; // > erased_at_ms (re-signup)
        ps.dead_letter(
            0,
            10,
            make_ev("user_kept", "Kept_Event", old_ts, 1),
            11,
            &"project schema cap reached",
        )
        .await
        .unwrap();
        ps.dead_letter(
            0,
            11,
            make_ev("user_erased", "Erased_Direct", old_ts, 2),
            12,
            &"project schema cap reached",
        )
        .await
        .unwrap();
        ps.dead_letter(
            1,
            12,
            make_ev("anon_aliased", "Erased_Alias", old_ts, 3),
            13,
            &"project schema cap reached",
        )
        .await
        .unwrap();
        ps.dead_letter(
            1,
            13,
            make_ev("user_erased", "Resignup_Event", future_ts, 4),
            14,
            &"dictionary capacity exceeded",
        )
        .await
        .unwrap();
        ps.dead_letter(
            2,
            14,
            make_ev("unknown_never_interned", "Unknown_User_Event", old_ts, 5),
            15,
            &"project schema cap reached",
        )
        .await
        .unwrap();

        // Also write a legacy `.plsb` dead-letter file via node's EncryptedObjectStore.
        let legacy_ev = make_ev("user_kept", "Legacy_Plsb_Event", old_ts, 6);
        let legacy_bytes = WireBatch::new(vec![legacy_ev]).encode_binary(1).unwrap();
        node.storage
            .object_store()
            .put(
                &ObjectPath::from(format!(
                    "wal_dead/{pid}/bucket_003/seq_000000000015_0000000000000016.plsb"
                )),
                legacy_bytes.into(),
            )
            .await
            .unwrap();

        // Inspect WHILE live writer node is still running (must NOT fence SlateDB writer!).
        let listed = inspect_dead_letters(&data_dir, pid, None, None)
            .await
            .unwrap();
        let names: Vec<&str> = listed.iter().map(|i| i.event.event_name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "Kept_Event",
                "Resignup_Event",
                "Unknown_User_Event",
                "Legacy_Plsb_Event"
            ],
            "erased direct and alias-merged users <= erased_at_ms must be filtered out; got {names:?}"
        );
        assert_eq!(listed[0].reason, "project schema cap reached");
        assert_eq!(listed[1].reason, "dictionary capacity exceeded");
        assert_eq!(listed[3].reason, "unknown (legacy PLSB)");

        // Verify `--bucket` and `--limit` filters.
        let b1 = inspect_dead_letters(&data_dir, pid, Some(1), None)
            .await
            .unwrap();
        assert_eq!(b1.len(), 1);
        assert_eq!(b1[0].event.event_name, "Resignup_Event");
        let limited = inspect_dead_letters(&data_dir, pid, None, Some(2))
            .await
            .unwrap();
        assert_eq!(limited.len(), 2);

        // Verify the live running node was NOT fenced by inspect_dead_letters!
        assert_eq!(
            post_events(&node, &key, &["after_inspect"], future_ts).await,
            200
        );
        let eng_ok = http_client()
            .post(format!("http://{}/v1/engage", node.http_addr))
            .header("x-pulse-key", &key)
            .json(&json!({"distinct_id": "user_kept", "$set": {"plan": "pro"}}))
            .send()
            .await
            .unwrap();
        assert_eq!(eng_ok.status(), 200);

        // 3) Run the actual `pulse-node dead-letters --json` binary CLI.
        let bin = env!("CARGO_BIN_EXE_pulse-node");
        let out = std::process::Command::new(bin)
            .args([
                "dead-letters",
                "--project-id",
                pid,
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--json",
            ])
            .output()
            .expect("run pulse-node dead-letters");
        assert!(
            out.status.success(),
            "dead-letters CLI failed: stderr={}",
            String::from_utf8_lossy(&out.stderr)
        );
        let cli_items: Vec<Value> = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(cli_items.len(), 4);
        assert_eq!(cli_items[0]["reason"], "project schema cap reached");

        node.shutdown().await;
    });
}

#[test]
fn metrics_endpoint_exposes_w1_wal_lag_and_queue_metrics_and_alerts_yaml_is_present() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let node = start_pulse_node(node_config(tmp.path(), "proj_metrics"))
            .await
            .unwrap();
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        let ts = chrono::Utc::now().timestamp_millis();
        assert_eq!(post_events(&node, &key, &["m1", "m2"], ts).await, 200);
        // Send duplicate so deduped_total increments too.
        let dup_payload = json!({
            "events": [{
                "event_uuid": "11111111-2222-3333-4444-555555555599",
                "distinct_id": "m_dup",
                "event_name": "App_Open",
                "client_event_ts_ms": ts,
            }]
        });
        for _ in 0..2 {
            let resp = http_client()
                .post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&dup_payload)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
        }
        node.ingest.flush_all().await.unwrap();

        let metrics_text = http_client()
            .get(format!("http://{}/metrics", node.http_addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        let required = [
            "pulse_writer_flush_bound_ms",
            "pulse_writer_flush_bound_bytes",
            "pulse_writer_flushes_total{reason=\"timer\"}",
            "pulse_writer_flushes_total{reason=\"bytes\"}",
            "pulse_writer_flushes_total{reason=\"drain\"}",
            "pulse_writer_buffer_bytes",
            "pulse_writer_buffer_capacity_bytes",
            "pulse_writer_buffer_events",
            "pulse_writer_buffer_batches",
            "pulse_wal_bytes",
            "pulse_ingest_log_lag_bytes",
            "pulse_ingest_log_lag_limit_bytes",
            "pulse_ingest_apply_lag_seconds",
            "pulse_ingest_apply_queue_records",
            "pulse_ingest_apply_queue_events",
            "pulse_ingest_events_deduped_total 1",
            "pulse_ingest_dead_lettered_events_total",
        ];
        for needle in required {
            assert!(
                metrics_text.contains(needle),
                "missing `{needle}` in /metrics output:\n{metrics_text}"
            );
        }
        assert!(
            !metrics_text.contains("project_id="),
            "metrics cardinality violated: found project_id label in:\n{metrics_text}"
        );

        node.shutdown().await;
    });

    // Validate deploy/prometheus-alerts.yml contains all expected alert rules.
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let alerts_path = manifest_dir.join("../../deploy/prometheus-alerts.yml");
    let raw = std::fs::read_to_string(&alerts_path).expect("read deploy/prometheus-alerts.yml");
    for expected in [
        "alert: PulseIngestApplyLagHigh",
        "alert: PulseIngestApplyLagApproachingDedupWindow",
        "alert: PulseIngestLogLagHigh",
        "alert: PulseWriterBufferSaturation",
        "alert: PulseHotSegmentBackpressureRisk",
        "alert: PulseDeadLetteredEventsDetected",
        "alert: PulseMaintenanceErrorsDetected",
    ] {
        assert!(
            raw.contains(expected),
            "missing `{expected}` in deploy/prometheus-alerts.yml"
        );
    }
}

#[test]
fn quic_writer_async_flush_bound_and_mixed_batch_atomicity() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _ttl = EnvGuard::set("PULSE_WRITER_LEASE_TTL_SECS", Some("2"));
    let _upload = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Part 1: QUIC against NodeRole::All with 60s flush bound + abrupt crash loses buffered batch;
    // with 20ms flush bound + 120ms wait, abrupt crash preserves 100% of QUIC events in WAL.
    {
        let _flush = EnvGuard::set("PULSE_WRITER_FLUSH_MS", Some("60000"));
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let s3 = Arc::new(InMemory::new());
        let s3_c = s3.clone();
        std::thread::spawn(move || {
            let rt_c = tokio::runtime::Runtime::new().unwrap();
            rt_c.block_on(async move {
                let mut cfg = node_config(&dir, "proj_quic_60s");
                cfg.quic_addr = Some("127.0.0.1:0".parse().unwrap());
                let node = start_pulse_node_with_store(cfg, s3_c).await.unwrap();
                let key = node
                    .bootstrap_credentials
                    .generated_ingest_key
                    .clone()
                    .unwrap();
                let quic = node.quic.as_ref().unwrap();
                let ts = 1_790_000_000_000i64;
                let batch = WireBatch::new(vec![WireEvent {
                    event_uuid: WireEvent::deterministic_uuid("qu1", "qs", "Quic60s", ts, 1),
                    routing_seed: "qu1".into(),
                    distinct_id: "qu1".into(),
                    session_id: "qs".into(),
                    event_name: "Quic60s".into(),
                    client_event_ts_ms: ts,
                    client_sent_ts_ms: ts,
                    props: Vec::new(),
                    replay_offset_kb: 0,
                }]);
                let plsb = batch.encode_binary(1).unwrap();
                let resp = pulse_ingest::quic::send_quic_ingest_batch(
                    quic.local_addr,
                    &quic.cert_der,
                    &key,
                    &plsb,
                )
                .await
                .unwrap();
                assert_eq!(resp.accepted, 1);
                assert_eq!(resp.deduped, 0);
                std::mem::forget(node);
            });
            rt_c.shutdown_background();
        })
        .join()
        .unwrap();

        rt.block_on(async {
            let node = start_pulse_node_with_store(node_config(tmp.path(), "proj_quic_60s"), s3)
                .await
                .unwrap();
            let mut rows = 0usize;
            for _ in 0..40 {
                if let Ok(ps) = node.storage.open_for_read("proj_quic_60s").await {
                    rows = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            assert_eq!(
                rows, 0,
                "60s flush window must not yet be durable before flush"
            );
            node.shutdown().await;
        });
    }

    {
        let _flush = EnvGuard::set("PULSE_WRITER_FLUSH_MS", Some("20"));
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let s3 = Arc::new(InMemory::new());
        let s3_c = s3.clone();
        std::thread::spawn(move || {
            let rt_c = tokio::runtime::Runtime::new().unwrap();
            rt_c.block_on(async move {
                let mut cfg = node_config(&dir, "proj_quic_20ms");
                cfg.quic_addr = Some("127.0.0.1:0".parse().unwrap());
                let node = start_pulse_node_with_store(cfg, s3_c).await.unwrap();
                let key = node
                    .bootstrap_credentials
                    .generated_ingest_key
                    .clone()
                    .unwrap();
                let quic = node.quic.as_ref().unwrap();
                let ts = 1_790_000_000_000i64;
                let batch = WireBatch::new(vec![WireEvent {
                    event_uuid: WireEvent::deterministic_uuid("qu2", "qs", "Quic20ms", ts, 2),
                    routing_seed: "qu2".into(),
                    distinct_id: "qu2".into(),
                    session_id: "qs".into(),
                    event_name: "Quic20ms".into(),
                    client_event_ts_ms: ts,
                    client_sent_ts_ms: ts,
                    props: Vec::new(),
                    replay_offset_kb: 0,
                }]);
                let plsb = batch.encode_binary(1).unwrap();
                let resp = pulse_ingest::quic::send_quic_ingest_batch(
                    quic.local_addr,
                    &quic.cert_der,
                    &key,
                    &plsb,
                )
                .await
                .unwrap();
                assert_eq!(resp.accepted, 1);
                tokio::time::sleep(Duration::from_millis(120)).await;
                std::mem::forget(node);
            });
            rt_c.shutdown_background();
        })
        .join()
        .unwrap();

        rt.block_on(async {
            let node = start_pulse_node_with_store(node_config(tmp.path(), "proj_quic_20ms"), s3)
                .await
                .unwrap();
            let mut ps_opt = None;
            for _ in 0..40 {
                if let Ok(ps) = node.storage.open_for_read("proj_quic_20ms").await {
                    ps_opt = Some(ps);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            let ps = ps_opt.expect("20ms QUIC node must reopen cleanly");
            let rows = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
            assert_eq!(rows, 1, "20ms flushed QUIC event must survive abrupt crash");
            node.shutdown().await;
        });
    }

    // Part 2: NodeRole::Writer QUIC mixed-batch identify failure (returns QUIC error + 0 buffered
    // events) and QUIC 429 writer_log_backpressure without cross-project HOL.
    {
        let tmp = tempfile::tempdir().unwrap();
        let meta_db = tmp.path().join("w3_quic.db");
        let meta_url = format!("sqlite://{}", meta_db.display());
        let _db = EnvGuard::set("DATABASE_URL", Some(&meta_url));
        let _flush = EnvGuard::set("PULSE_WRITER_FLUSH_MS", Some("10"));
        let _cap = EnvGuard::set("PULSE_WRITER_LOG_PROJECT_MAX_BYTES", Some("1024"));
        let _cache = EnvGuard::set("PULSE_OWNER_CACHE_TTL_MS", Some("60000"));

        rt.block_on(async {
            let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut owner_cfg = node_config(&tmp.path().join("owner"), "proj_q_default");
            owner_cfg.role = NodeRole::All;
            let owner = pulse_node::start_pulse_node_with_store_and_identity(
                owner_cfg,
                s3.clone(),
                pulse_node::NodeIdentity::new("ing-quic", None),
            )
            .await
            .unwrap();
            let owner_url = format!("http://{}", owner.http_addr);

            let mut w_cfg = node_config(&tmp.path().join("writer"), "proj_q_default");
            w_cfg.role = NodeRole::Writer;
            w_cfg.quic_addr = Some("127.0.0.1:0".parse().unwrap());
            let writer = pulse_node::start_pulse_node_with_store_and_identity(
                w_cfg,
                Arc::new(InMemory::new()),
                pulse_node::NodeIdentity::new("writer-quic", None),
            )
            .await
            .unwrap();
            let quic = writer.quic.as_ref().unwrap();

            let create_proj = |name: &str| {
                let url = owner_url.clone();
                let name = name.to_string();
                async move {
                    let resp: Value = http_client()
                        .post(format!("{url}/v1/projects"))
                        .bearer_auth(ADMIN)
                        .json(&json!({ "name": name }))
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    (
                        resp["project"]["id"].as_str().unwrap().to_string(),
                        resp["api_key"]["plaintext_key"]
                            .as_str()
                            .unwrap()
                            .to_string(),
                    )
                }
            };
            let (p1, pk1) = create_proj("quic-p1").await;
            let (p2, pk2) = create_proj("quic-p2").await;
            let now_ms = chrono::Utc::now().timestamp_millis();
            owner
                .control
                .meta()
                .set_project_owner(&p1, "ing-quic", &owner_url, now_ms)
                .await
                .unwrap();
            owner
                .control
                .meta()
                .set_project_owner(&p2, "ing-quic", &owner_url, now_ms)
                .await
                .unwrap();

            // Pre-link `anon_conflict` -> `canon_1`, then try mixed batch identifying `anon_conflict`
            // -> `canon_2` + track event; sync identify conflict must fail and buffer 0 events.
            assert_eq!(
                http_client()
                    .post(format!("{owner_url}/v1/identify"))
                    .header("x-pulse-key", &pk1)
                    .json(&json!({ "anon_id": "anon_conflict", "user_id": "canon_1" }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            let mixed_bad = serde_json::to_vec(&json!({
                "batch": [
                    { "type": "identify", "anonymousId": "anon_conflict", "userId": "canon_2" },
                    { "type": "track", "userId": "u_should_not_buffer", "event": "Mixed_Fail_Ev", "messageId": "mf_1", "timestamp": now_ms }
                ]
            }))
            .unwrap();
            let err = pulse_ingest::quic::send_quic_ingest_batch(
                quic.local_addr,
                &quic.cert_der,
                &pk1,
                &mixed_bad,
            )
            .await
            .expect_err("mixed-batch identify conflict over QUIC must fail");
            let err_s = err.to_string();
            assert!(
                err_s.contains("conflict") || err_s.contains("error"),
                "expected conflict QUIC error, got {err_s}"
            );
            assert_eq!(writer.ingest.buffer_stats().buffer_events, 0);

            // Shutdown owner to induce writer_log backpressure on P1 over QUIC, while P2 succeeds!
            owner.shutdown().await;
            let mut hit_quic_429 = false;
            for i in 0..20usize {
                let batch = WireBatch::new(vec![WireEvent {
                    event_uuid: WireEvent::deterministic_uuid(
                        "qp1",
                        "s",
                        "Q429",
                        now_ms + i as i64,
                        i as u64,
                    ),
                    routing_seed: "qp1".into(),
                    distinct_id: format!("qp1_{i}"),
                    session_id: "s".into(),
                    event_name: "Q429".into(),
                    client_event_ts_ms: now_ms + i as i64,
                    client_sent_ts_ms: now_ms + i as i64,
                    props: vec![("pad".into(), PropValue::Str("z".repeat(600)))],
                    replay_offset_kb: 0,
                }]);
                let plsb = batch.encode_binary(1).unwrap();
                match pulse_ingest::quic::send_quic_ingest_batch(
                    quic.local_addr,
                    &quic.cert_der,
                    &pk1,
                    &plsb,
                )
                .await
                {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(e) => {
                        let s = e.to_string();
                        assert!(
                            s.contains("writer_log_backpressure"),
                            "expected writer_log_backpressure over QUIC, got {s}"
                        );
                        hit_quic_429 = true;
                        break;
                    }
                }
            }
            assert!(hit_quic_429, "P1 must hit writer_log_backpressure over QUIC");

            // P2 on the same Writer QUIC listener still succeeds 200 without HOL blocking!
            let p2_batch = WireBatch::new(vec![WireEvent {
                event_uuid: WireEvent::deterministic_uuid("qp2", "s", "QOk", now_ms + 100, 99),
                routing_seed: "qp2".into(),
                distinct_id: "qp2".into(),
                session_id: "s".into(),
                event_name: "QOk".into(),
                client_event_ts_ms: now_ms + 100,
                client_sent_ts_ms: now_ms + 100,
                props: Vec::new(),
                replay_offset_kb: 0,
            }]);
            let p2_resp = pulse_ingest::quic::send_quic_ingest_batch(
                quic.local_addr,
                &quic.cert_der,
                &pk2,
                &p2_batch.encode_binary(1).unwrap(),
            )
            .await
            .expect("P2 QUIC ingest must succeed while P1 is 429'd");
            assert_eq!(p2_resp.accepted, 1);

            writer.shutdown().await;
        });
    }
}
