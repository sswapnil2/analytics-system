//! M6c (W3 Part 3) Multi-Node Integration & Fault-Injection Tests.
//!
//! Covers all 7 W3 exit criteria from `docs/history/TIERED_STORAGE_DESIGN.md` §7:
//! 1. `NodeRole::Writer` accepts `/v1/ingest` (JSON/PLSB), `/v1/agent/traces` (JSON/PLST), and
//!    `/v1/otel/v1/traces`, stores zero local SlateDB/hot segments, and forwards async to the
//!    owning `NodeRole::Ingest`/`All` node (recording `writer_offsets` in `AgentManifest`).
//! 2. Real OS subprocess `SIGKILL` of `NodeRole::Writer` after `200` loses zero WALed events or
//!    agent traces; clean shutdown drains `AgentWriterBuffer` + event buffer to `writer_log/`.
//! 3. `writer_log` per-project `429` (`writer_log_backpressure`) isolation across projects on both
//!    `/v1/ingest` and `/v1/agent/traces`, owner outage recovery, and
//!    `restore_from_published_with_store_and_meta_for_test` preserving `writer_offsets`.
//! 4. Owner `AgentTraceStore::ingest_forwarded_batch` idempotency across in-L0 resend, flush +
//!    WAL purge + publish + restart, physical erasure (zero resurrection), `writer_log/` wipe,
//!    and `7d` expiry boundaries (`6d < 7d` retained, `8d > 7d` GC'd).
//! 5. Cached `project_owner` routing & forwarding for sync profile/replay calls (including mixed
//!    batches and empty `POST /v1/groups`), `/v1/projects/:id/*` + non-`:id` project routes from
//!    `Writer` and non-owner `All` to `Ingest` (while public `GET /v1/status` stays `404` on
//!    `Ingest`), `NodeRole::Query` S3 reads vs 4 forwarded write routes, instant stale-cache
//!    invalidation on ownership transfer, fast `503` on dead owner, and `503 forward_loop_detected`.
//! 6. Alert evaluation webhook fires **once** per trigger when `Ingest`, `Writer`, and `Query`
//!    coexist.

use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use chrono::Utc;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use pulse_agent::model::TraceIngestBatch;
use pulse_agent::protocol::encode_plst_batch;
use pulse_agent::PublishedAgentManifestStore;
use pulse_node::{
    restore_from_published_with_store_and_meta_for_test, start_pulse_node_with_store_and_identity,
    NodeConfig, NodeIdentity, NodeRole,
};
use pulse_wire::{WireBatch, WireEvent};
use serde_json::{json, Value};

const ADMIN_TOKEN: &str = "sk_root_m6c_multi_node_w3_test_token_0000000000";
const TEST_VAULT_KEK: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let mut saved = Vec::with_capacity(pairs.len());
        for &(k, v) in pairs {
            saved.push((k, std::env::var(k).ok()));
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, prev) in self.saved.drain(..).rev() {
            match prev {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

struct ChildGuard(Option<std::process::Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn node_cfg(data_dir: &Path, default_project_id: &str, role: NodeRole) -> NodeConfig {
    NodeConfig {
        role,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: data_dir.to_path_buf(),
        web_dist_dir: data_dir.join("no_web"),
        default_project_id: default_project_id.to_string(),
        default_api_key: format!("pk_live_{default_project_id}_{}", role.as_str()),
        admin_token: Some(ADMIN_TOKEN.to_string()),
        allowed_origins: vec!["http://localhost:3000".to_string()],
        auto_seed: false,
        num_buckets: pulse_storage::DEFAULT_NUM_BUCKETS,
    }
}

fn make_trace_batch(
    trace_id: &str,
    session_id: &str,
    user_id: &str,
    now_ms: i64,
    agent: &str,
) -> Value {
    json!({
        "traces": [{
            "trace_id": trace_id,
            "agent_name": agent,
            "agent_version": "1.0.0",
            "environment": "prod",
            "session_id": session_id,
            "user_id": user_id,
            "status": "ok",
            "start_ts_ms": now_ms,
            "duration_ms": 25,
            "input": format!("prompt for {trace_id}"),
            "output": format!("answer for {trace_id}")
        }],
        "spans": [{
            "trace_id": trace_id,
            "span_id": format!("sp_{trace_id}"),
            "name": "llm.completion",
            "span_type": "llm",
            "status": "ok",
            "start_ts_ms": now_ms + 1,
            "duration_ms": 20,
            "model": "gpt-4o-mini",
            "input_tokens": 100,
            "output_tokens": 50,
            "cost_nanos": 1_500_000
        }]
    })
}

fn make_otel_payload(trace_hex: &str, span_hex: &str, now_ms: i64, agent: &str) -> Value {
    json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": agent } }
                ]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": trace_hex,
                    "spanId": span_hex,
                    "name": "gen_ai.chat",
                    "startTimeUnixNano": (now_ms as u64) * 1_000_000,
                    "endTimeUnixNano": ((now_ms + 15) as u64) * 1_000_000,
                    "attributes": [
                        { "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4o-mini" } },
                        { "key": "gen_ai.usage.input_tokens", "value": { "intValue": "40" } },
                        { "key": "gen_ai.usage.output_tokens", "value": { "intValue": "10" } }
                    ]
                }]
            }]
        }]
    })
}

async fn create_project_on(http: &reqwest::Client, base_url: &str, name: &str) -> (String, String) {
    let proj_resp: Value = http
        .post(format!("{base_url}/v1/projects"))
        .bearer_auth(ADMIN_TOKEN)
        .json(&json!({ "name": name }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let pid = proj_resp["project"]["id"].as_str().unwrap().to_string();
    let pk = proj_resp["api_key"]["plaintext_key"]
        .as_str()
        .unwrap()
        .to_string();
    (pid, pk)
}

async fn insights_total(
    http: &reqwest::Client,
    base_url: &str,
    pid: &str,
    event_name: &str,
    now_ms: i64,
) -> i64 {
    let resp = http
        .post(format!("{base_url}/v1/projects/{pid}/query/insights"))
        .bearer_auth(ADMIN_TOKEN)
        .json(&json!({
            "time_range": { "start_ts_ms": now_ms - 600_000, "end_ts_ms": now_ms + 600_000 },
            "interval": "day",
            "series": [{ "label": "A", "event_name": event_name, "metric": "totals" }]
        }))
        .send()
        .await
        .unwrap();
    if !resp.status().is_success() {
        return -1;
    }
    let body: Value = resp.json().await.unwrap();
    body["series"][0]["overall_value"]
        .as_f64()
        .unwrap_or(0.0)
        .round() as i64
}

async fn list_agent_trace_ids(http: &reqwest::Client, base_url: &str, pid: &str) -> Vec<String> {
    let resp = http
        .get(format!(
            "{base_url}/v1/projects/{pid}/agent/traces?limit=100"
        ))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap();
    if !resp.status().is_success() {
        return Vec::new();
    }
    let body: Value = resp.json().await.unwrap();
    body["traces"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["trace_id"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

async fn poll_until<F, Fut>(timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for condition"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn spawn_writer_subprocess(
    bin: &str,
    port: u16,
    data_dir: &Path,
    s3_url: &str,
    meta_url: &str,
    node_name: &str,
    log_prefix: &Path,
) -> ChildGuard {
    let stdout_path = log_prefix.with_extension("out");
    let stderr_path = log_prefix.with_extension("err");
    ChildGuard(Some(
        std::process::Command::new(bin)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--no-quic",
                "--no-seed",
                "--role",
                "writer",
                "--node-name",
                node_name,
                "--advertise-addr",
                &format!("http://127.0.0.1:{port}"),
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--web-dir",
                data_dir.join("no_web").to_str().unwrap(),
            ])
            .env("DATABASE_URL", meta_url)
            .env("PULSE_OBJECT_STORE_URL", s3_url)
            .env("PULSE_ADMIN_TOKEN", ADMIN_TOKEN)
            .env("PULSE_VAULT_KEK", TEST_VAULT_KEK)
            .env("PULSE_HOT_STORE", "local")
            .env("PULSE_UPLOAD_INTERVAL_SECS", "300")
            .env("PULSE_WRITER_FLUSH_MS", "15")
            .stdout(Stdio::from(std::fs::File::create(stdout_path).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(stderr_path).unwrap()))
            .spawn()
            .unwrap(),
    ))
}

#[test]
fn w3_exit_1_and_2_writer_async_ingest_survives_writer_sigkill_and_owner_restart() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let s3_dir = tmp.path().join("s3");
    let dir_owner = tmp.path().join("owner");
    let dir_writer = tmp.path().join("writer");
    std::fs::create_dir_all(&s3_dir).unwrap();
    let meta_db = tmp.path().join("meta.db");
    let meta_url = format!("sqlite://{}", meta_db.display());
    let s3_url = reqwest::Url::from_directory_path(&s3_dir)
        .unwrap()
        .to_string();

    let _env = EnvGuard::set(&[
        ("DATABASE_URL", Some(&meta_url)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_FLUSH_MS", Some("15")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("2")),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let bin = env!("CARGO_BIN_EXE_pulse-node");
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    rt.block_on(async {
        let s3: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let owner = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_owner, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-a", None),
        )
        .await
        .unwrap();
        let owner_url = format!("http://{}", owner.http_addr);

        let writer_port = free_port();
        let writer_url = format!("http://127.0.0.1:{writer_port}");
        let mut writer_proc = spawn_writer_subprocess(
            bin,
            writer_port,
            &dir_writer,
            &s3_url,
            &meta_url,
            "writer-sub-1",
            &tmp.path().join("w1"),
        );

        poll_until(Duration::from_secs(15), || async {
            http.get(format!("{writer_url}/readyz"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        })
        .await;

        let (pid, pk) = create_project_on(&http, &owner_url, "w3-sigkill").await;
        let now_ms = Utc::now().timestamp_millis();

        // 1. Send events (JSON + PLSB) and agent traces (JSON + PLST + OTLP) to NodeRole::Writer.
        let r_ev_json = http
            .post(format!("{writer_url}/v1/ingest"))
            .header("x-pulse-key", &pk)
            .json(&json!({
                "events": [
                    { "event": "W3_Ev", "userId": "u1", "messageId": "m_json_1", "timestamp": now_ms },
                    { "event": "W3_Ev", "userId": "u2", "messageId": "m_json_2", "timestamp": now_ms + 1 },
                    { "event": "W3_Ev", "userId": "u3", "messageId": "m_json_3", "timestamp": now_ms + 2 }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_ev_json.status(), 200);

        let plsb_batch = WireBatch {
            events: vec![
                WireEvent {
                    event_uuid: WireEvent::deterministic_uuid("u4", "s4", "W3_Ev", now_ms + 3, 1),
                    routing_seed: "u4".into(),
                    distinct_id: "u4".into(),
                    session_id: "s4".into(),
                    event_name: "W3_Ev".into(),
                    client_event_ts_ms: now_ms + 3,
                    client_sent_ts_ms: now_ms + 3,
                    props: Vec::new(),
                    replay_offset_kb: 0,
                },
                WireEvent {
                    event_uuid: WireEvent::deterministic_uuid("u5", "s5", "W3_Ev", now_ms + 4, 2),
                    routing_seed: "u5".into(),
                    distinct_id: "u5".into(),
                    session_id: "s5".into(),
                    event_name: "W3_Ev".into(),
                    client_event_ts_ms: now_ms + 4,
                    client_sent_ts_ms: now_ms + 4,
                    props: Vec::new(),
                    replay_offset_kb: 0,
                },
            ],
            rejected: Vec::new(),
            profile_calls: Vec::new(),
        };
        let plsb_bytes = plsb_batch.encode_binary(0).unwrap();
        let r_ev_plsb = http
            .post(format!("{writer_url}/v1/ingest"))
            .header("x-pulse-key", &pk)
            .header("content-type", "application/x-pulse-frame")
            .body(plsb_bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(r_ev_plsb.status(), 200);

        let r_ag_json = http
            .post(format!("{writer_url}/v1/agent/traces"))
            .header("x-pulse-key", &pk)
            .json(&make_trace_batch("tr_w3_json", "s1", "u1", now_ms, "bot"))
            .send()
            .await
            .unwrap();
        assert_eq!(r_ag_json.status(), 200);

        let plst_batch: TraceIngestBatch =
            serde_json::from_value(make_trace_batch("tr_w3_plst", "s2", "u2", now_ms + 5, "bot"))
                .unwrap();
        let plst_bytes = encode_plst_batch(&plst_batch, true).unwrap();
        let r_ag_plst = http
            .post(format!("{writer_url}/v1/agent/traces"))
            .header("x-pulse-key", &pk)
            .header("content-type", "application/x-pulse-trace")
            .body(plst_bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(r_ag_plst.status(), 200);

        let otel_trace_hex = "11223344556677889900aabbccddeeff";
        let r_ag_otel = http
            .post(format!("{writer_url}/v1/otel/v1/traces"))
            .header("x-pulse-key", &pk)
            .json(&make_otel_payload(
                otel_trace_hex,
                "0102030405060708",
                now_ms + 10,
                "bot",
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(r_ag_otel.status(), 200);

        poll_until(Duration::from_secs(10), || async {
            let ev_cnt = insights_total(&http, &owner_url, &pid, "W3_Ev", now_ms).await;
            let tr_ids = list_agent_trace_ids(&http, &owner_url, &pid).await;
            ev_cnt == 5 && tr_ids.len() == 3
        })
        .await;

        // Verify writer never opened local SlateDB or local event/agent WAL/segment dirs.
        assert!(!dir_writer.join("hot/identity").exists());
        assert!(!dir_writer.join("hot/profiles").exists());
        assert!(!dir_writer.join("hot/segments").exists());
        assert!(!dir_writer.join("hot/agent_segments").exists());
        assert!(!dir_writer.join("wal/wal").exists());
        assert!(!dir_writer.join("wal/agent_wal").exists());

        // 2. Stop owner briefly so writer queues unforwarded PWLF v2 records on disk,
        // accept HTTP 200 on writer, flush to writer_log/, then SIGKILL the writer!
        owner.shutdown().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let r_out_ev = http
            .post(format!("{writer_url}/v1/ingest"))
            .header("x-pulse-key", &pk)
            .json(&json!({
                "events": [
                    { "event": "W3_Ev", "userId": "u6", "messageId": "m_sigkill_1", "timestamp": now_ms + 20 }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_out_ev.status(), 200);

        let r_out_ag = http
            .post(format!("{writer_url}/v1/agent/traces"))
            .header("x-pulse-key", &pk)
            .json(&make_trace_batch("tr_sigkill_ag", "s3", "u3", now_ms + 21, "bot"))
            .send()
            .await
            .unwrap();
        assert_eq!(r_out_ag.status(), 200);

        let r_out_otel = http
            .post(format!("{writer_url}/v1/otel/v1/traces"))
            .header("x-pulse-key", &pk)
            .json(&make_otel_payload(
                "ffeeddccbbaa00998877665544332211",
                "8877665544332211",
                now_ms + 22,
                "bot",
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(r_out_otel.status(), 200);

        // Wait past the 15ms flush windows so PWLF v2 segments are fsynced to writer_log/.
        tokio::time::sleep(Duration::from_millis(120)).await;

        // Real OS SIGKILL!
        if let Some(mut child) = writer_proc.0.take() {
            child.kill().unwrap();
            let _ = child.wait();
        }

        // Wait past lease TTL (2s) so restarted owner reacquires lease immediately.
        tokio::time::sleep(Duration::from_millis(2_300)).await;

        let owner2 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_owner, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-a", None),
        )
        .await
        .unwrap();
        let owner2_url = format!("http://{}", owner2.http_addr);

        // Restart writer subprocess on same dir_writer.
        let writer_port2 = free_port();
        let writer_proc2 = spawn_writer_subprocess(
            bin,
            writer_port2,
            &dir_writer,
            &s3_url,
            &meta_url,
            "writer-sub-1",
            &tmp.path().join("w2"),
        );

        poll_until(Duration::from_secs(15), || async {
            let ev_cnt = insights_total(&http, &owner2_url, &pid, "W3_Ev", now_ms).await;
            let tr_ids = list_agent_trace_ids(&http, &owner2_url, &pid).await;
            ev_cnt == 6 && tr_ids.len() == 5
        })
        .await;

        let tr_ids = list_agent_trace_ids(&http, &owner2_url, &pid).await;
        assert!(tr_ids.contains(&"tr_w3_json".to_string()), "{tr_ids:?}");
        assert!(tr_ids.contains(&"tr_w3_plst".to_string()), "{tr_ids:?}");
        assert!(tr_ids.contains(&otel_trace_hex.to_string()), "{tr_ids:?}");
        assert!(tr_ids.contains(&"tr_sigkill_ag".to_string()), "{tr_ids:?}");
        assert!(
            tr_ids.contains(&"ffeeddccbbaa00998877665544332211".to_string()),
            "{tr_ids:?}"
        );

        drop(writer_proc2);

        // 3. Also verify clean in-process NodeRole::Writer shutdown drains AgentWriterBuffer.
        let dir_writer_clean = tmp.path().join("writer_clean");
        let w_clean = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer_clean, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-clean", None),
        )
        .await
        .unwrap();
        let w_clean_url = format!("http://{}", w_clean.http_addr);
        assert_eq!(
            http.post(format!("{w_clean_url}/v1/agent/traces"))
                .header("x-pulse-key", &pk)
                .json(&make_trace_batch("tr_clean_drain", "s4", "u7", now_ms + 30, "bot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        w_clean.shutdown().await;

        let w_clean2 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer_clean, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-clean", None),
        )
        .await
        .unwrap();
        poll_until(Duration::from_secs(10), || async {
            list_agent_trace_ids(&http, &owner2_url, &pid)
                .await
                .contains(&"tr_clean_drain".to_string())
        })
        .await;

        w_clean2.shutdown().await;
        owner2.shutdown().await;
    });
}

#[test]
fn w3_exit_3_writer_log_per_project_429_isolation_outage_and_restore_from_published() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let dir_owner = tmp.path().join("owner");
    let dir_writer = tmp.path().join("writer");
    let meta_db = tmp.path().join("meta.db");
    let meta_url = format!("sqlite://{}", meta_db.display());

    let _env = EnvGuard::set(&[
        ("DATABASE_URL", Some(&meta_url)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_FLUSH_MS", Some("10")),
        ("PULSE_WRITER_LOG_PROJECT_MAX_MB", Some("1")),
        ("PULSE_WRITER_LOG_MAX_MB", Some("8")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("2")),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let http = reqwest::Client::new();

    rt.block_on(async {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_owner, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-cap", None),
        )
        .await
        .unwrap();
        let owner_url = format!("http://{}", owner.http_addr);

        let writer = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-cap", None),
        )
        .await
        .unwrap();
        let writer_url = format!("http://{}", writer.http_addr);

        let (pid_a, pk_a) = create_project_on(&http, &owner_url, "proj-a").await;
        let (pid_b, pk_b) = create_project_on(&http, &owner_url, "proj-b").await;
        let now_ms = Utc::now().timestamp_millis();

        // 1. Pre-publish batch on proj_a via writer -> arrives on owner -> flush + publish to S3.
        assert_eq!(
            http.post(format!("{writer_url}/v1/ingest"))
                .header("x-pulse-key", &pk_a)
                .json(&json!({
                    "events": [
                        { "event": "Cap_Pre", "userId": "ua_0", "messageId": "m_pre_pub", "timestamp": now_ms }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{writer_url}/v1/agent/traces"))
                .header("x-pulse-key", &pk_a)
                .json(&make_trace_batch("tr_pre_pub", "sa_0", "ua_0", now_ms, "bot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        poll_until(Duration::from_secs(10), || async {
            insights_total(&http, &owner_url, &pid_a, "Cap_Pre", now_ms).await == 1
                && list_agent_trace_ids(&http, &owner_url, &pid_a)
                    .await
                    .contains(&"tr_pre_pub".to_string())
        })
        .await;

        owner.control.agent_store().flush_all_l0().await.unwrap();
        assert_eq!(
            owner.control.agent_store().publish_all_projects().await.unwrap(),
            1
        );
        owner.storage.flush_all_projects().await.unwrap();
        owner.storage.publish_all_projects().await.unwrap();

        let pub_store =
            PublishedAgentManifestStore::new(&pid_a, owner.storage.encrypted_store().clone())
                .unwrap();
        let pub_man = pub_store
            .load_latest()
            .await
            .unwrap()
            .expect("published agent manifest");
        assert!(
            !pub_man.writer_offsets.is_empty(),
            "PublishedAgentManifest must include writer_offsets"
        );

        // 2. Owner outage -> fill proj_a writer_log past 1 MiB to trigger 429 writer_log_backpressure.
        let meta_conn = owner.control.meta().clone();
        owner.shutdown().await;

        let incompressible_chunk = |mut state: u64, len: usize| -> String {
            const ALPH: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&*+-=?@^_~";
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                s.push(ALPH[(state as usize) % ALPH.len()] as char);
            }
            s
        };
        let mut hit_ev_429 = false;
        let mut hit_ag_429 = false;
        let mut accepted_ev_outage = 0i64;
        for i in 0..16u64 {
            let mut batch_events = Vec::with_capacity(40);
            for j in 0..40u64 {
                let s1 = incompressible_chunk(0x9e37_79b9_7f4a_7c15 ^ (i * 1000 + j * 2 + 1), 4000);
                let s2 = incompressible_chunk(0xbf58_476d_1ce4_e5b9 ^ (i * 1000 + j * 2 + 2), 4000);
                batch_events.push(json!({
                    "event": "Cap_Outage",
                    "userId": format!("ua_{i}_{j}"),
                    "messageId": format!("m_out_{i}_{j}"),
                    "timestamp": now_ms + (i as i64) + 1,
                    "properties": { "p1": s1, "p2": s2 }
                }));
            }
            let resp = http
                .post(format!("{writer_url}/v1/ingest"))
                .header("x-pulse-key", &pk_a)
                .json(&json!({ "events": batch_events }))
                .send()
                .await
                .unwrap();
            if resp.status().as_u16() == 429 {
                let b: Value = resp.json().await.unwrap();
                assert_eq!(b["code"], "writer_log_backpressure", "{b}");
                hit_ev_429 = true;
                break;
            }
            assert_eq!(resp.status(), 200);
            accepted_ev_outage += 40;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        assert!(hit_ev_429, "proj_a /v1/ingest should hit 429 writer_log_backpressure");

        let big_ag_spans: Vec<Value> = (0..6u64)
            .map(|k| {
                json!({
                    "trace_id": "tr_over_cap",
                    "span_id": format!("sp_cap_{k}"),
                    "name": "llm.completion",
                    "span_type": "llm",
                    "status": "ok",
                    "start_ts_ms": now_ms + 50 + k as i64,
                    "duration_ms": 10,
                    "output": incompressible_chunk(0xdead_beef_cafe_0001 ^ k, 50_000)
                })
            })
            .collect();
        let resp_ag_429 = http
            .post(format!("{writer_url}/v1/agent/traces"))
            .header("x-pulse-key", &pk_a)
            .json(&json!({
                "traces": [{
                    "trace_id": "tr_over_cap",
                    "agent_name": "bot",
                    "agent_version": "1.0.0",
                    "environment": "prod",
                    "session_id": "sa_cap",
                    "user_id": "ua_cap",
                    "status": "ok",
                    "start_ts_ms": now_ms + 50,
                    "duration_ms": 20
                }],
                "spans": big_ag_spans
            }))
            .send()
            .await
            .unwrap();
        if resp_ag_429.status().as_u16() == 429 {
            let b: Value = resp_ag_429.json().await.unwrap();
            assert_eq!(b["code"], "writer_log_backpressure", "{b}");
            hit_ag_429 = true;
        }
        assert!(hit_ag_429, "proj_a /v1/agent/traces must also enforce writer_log 429");

        // 3. While proj_a is 429'd, proj_b STILL accepts 200 on both /v1/ingest and /v1/agent/traces!
        assert_eq!(
            http.post(format!("{writer_url}/v1/ingest"))
                .header("x-pulse-key", &pk_b)
                .json(&json!({
                    "events": [{
                        "event": "ProjB_Isolated",
                        "userId": "ub_1",
                        "messageId": "mb_1",
                        "timestamp": now_ms + 100
                    }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{writer_url}/v1/agent/traces"))
                .header("x-pulse-key", &pk_b)
                .json(&make_trace_batch("tr_b_isolated", "sb_1", "ub_1", now_ms + 101, "bot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        // 4. Wipe dir_owner and run restore --from-published, then restart owner.
        std::fs::remove_dir_all(&dir_owner).unwrap();
        restore_from_published_with_store_and_meta_for_test(&dir_owner, None, s3.clone(), &meta_conn)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(2_300)).await;

        let owner2 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_owner, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-cap", None),
        )
        .await
        .unwrap();
        let owner2_url = format!("http://{}", owner2.http_addr);

        poll_until(Duration::from_secs(15), || async {
            insights_total(&http, &owner2_url, &pid_a, "Cap_Outage", now_ms).await
                == accepted_ev_outage
                && insights_total(&http, &owner2_url, &pid_b, "ProjB_Isolated", now_ms).await == 1
                && list_agent_trace_ids(&http, &owner2_url, &pid_b)
                    .await
                    .contains(&"tr_b_isolated".to_string())
        })
        .await;

        // Pre-publish trace was NOT lost and NOT duplicated.
        let tr_a = list_agent_trace_ids(&http, &owner2_url, &pid_a).await;
        assert_eq!(
            tr_a.iter().filter(|id| *id == "tr_pre_pub").count(),
            1,
            "{tr_a:?}"
        );

        // proj_a accepts new requests again once writer_log drains.
        poll_until(Duration::from_secs(5), || async {
            http.post(format!("{writer_url}/v1/agent/traces"))
                .header("x-pulse-key", &pk_a)
                .json(&make_trace_batch("tr_post_recover", "sa_r", "ua_r", now_ms + 200, "bot"))
                .send()
                .await
                .is_ok_and(|r| r.status() == 200)
        })
        .await;

        writer.shutdown().await;
        owner2.shutdown().await;
    });
}

#[test]
fn w3_exit_4_agent_forward_idempotency_erasure_wipe_and_7d_expiry() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let dir_owner = tmp.path().join("owner");
    let dir_writer = tmp.path().join("writer");
    let meta_db = tmp.path().join("meta.db");
    let meta_url = format!("sqlite://{}", meta_db.display());

    let _env = EnvGuard::set(&[
        ("DATABASE_URL", Some(&meta_url)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_FLUSH_MS", Some("10")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("2")),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let http = reqwest::Client::new();

    rt.block_on(async {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_owner, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-idem", None),
        )
        .await
        .unwrap();
        let owner_url = format!("http://{}", owner.http_addr);
        let (pid, pk) = create_project_on(&http, &owner_url, "proj-idem").await;
        let now_ms = Utc::now().timestamp_millis();

        let batch1: TraceIngestBatch =
            serde_json::from_value(make_trace_batch("tr_idem_1", "s_i1", "u_i1", now_ms, "bot"))
                .unwrap();

        // 1. Direct forwarded batch at offset 0.
        let r1 = owner
            .control
            .agent_store()
            .ingest_forwarded_batch("w_manual", &pid, 0, batch1.clone(), now_ms)
            .await
            .unwrap();
        assert_eq!(r1.accepted_traces, 1);

        // Resend same offset 0 while still in L0 -> 0 accepted.
        let r_dup1 = owner
            .control
            .agent_store()
            .ingest_forwarded_batch("w_manual", &pid, 0, batch1.clone(), now_ms)
            .await
            .unwrap();
        assert_eq!(r_dup1.accepted_traces, 0);

        // Flush L0, purge agent_wal, publish -> clean restart -> resend -> STILL 0 accepted!
        owner.control.agent_store().flush_all_l0().await.unwrap();
        assert_eq!(
            owner
                .control
                .agent_store()
                .publish_all_projects()
                .await
                .unwrap(),
            1
        );
        owner.shutdown().await;
        tokio::time::sleep(Duration::from_millis(2_300)).await;

        let owner2 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_owner, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-idem", None),
        )
        .await
        .unwrap();
        let owner2_url = format!("http://{}", owner2.http_addr);

        let r_dup2 = owner2
            .control
            .agent_store()
            .ingest_forwarded_batch("w_manual", &pid, 0, batch1.clone(), now_ms)
            .await
            .unwrap();
        assert_eq!(r_dup2.accepted_traces, 0);

        // 2. Erase tr_idem_1 and physically purge -> resend offset 0 -> zero resurrection!
        assert_eq!(
            http.delete(format!(
                "{owner2_url}/v1/projects/{pid}/agent/traces/tr_idem_1"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
            200
        );
        let _ = owner2.control.sweep_pending_agent_erasures().await;
        let r_erased = owner2
            .control
            .agent_store()
            .ingest_forwarded_batch("w_manual", &pid, 0, batch1, now_ms)
            .await
            .unwrap();
        assert_eq!(r_erased.accepted_traces, 0);
        assert!(
            !list_agent_trace_ids(&http, &owner2_url, &pid)
                .await
                .contains(&"tr_idem_1".to_string()),
            "erased trace must not resurrect on forwarded replay"
        );

        // 3. Start writer, ingest trace, shut down, wipe writer_log/, restart -> new writer_id at offset 0 works!
        let writer1 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-wipe", None),
        )
        .await
        .unwrap();
        let w1_url = format!("http://{}", writer1.http_addr);
        assert_eq!(
            http.post(format!("{w1_url}/v1/agent/traces"))
                .header("x-pulse-key", &pk)
                .json(&make_trace_batch(
                    "tr_before_wipe",
                    "sw1",
                    "uw1",
                    now_ms + 10,
                    "bot"
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        poll_until(Duration::from_secs(10), || async {
            list_agent_trace_ids(&http, &owner2_url, &pid)
                .await
                .contains(&"tr_before_wipe".to_string())
        })
        .await;
        writer1.shutdown().await;

        std::fs::remove_dir_all(dir_writer.join("writer_log")).unwrap();
        let writer2 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-wipe", None),
        )
        .await
        .unwrap();
        let w2_url = format!("http://{}", writer2.http_addr);
        assert_eq!(
            http.post(format!("{w2_url}/v1/agent/traces"))
                .header("x-pulse-key", &pk)
                .json(&make_trace_batch(
                    "tr_after_wipe",
                    "sw2",
                    "uw2",
                    now_ms + 20,
                    "bot"
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        poll_until(Duration::from_secs(10), || async {
            list_agent_trace_ids(&http, &owner2_url, &pid)
                .await
                .contains(&"tr_after_wipe".to_string())
        })
        .await;

        // 4. Verify analytics sees exact 2 traces (tr_before_wipe + tr_after_wipe minus erased tr_idem_1).
        let analytics: Value = http
            .get(format!("{owner2_url}/v1/projects/{pid}/agent/analytics"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(analytics["total_executions"], 2, "{analytics}");
        assert_eq!(analytics["total_tokens"], 300, "{analytics}");

        // 5. Verify 6d (< 7d) entry stays active across flush + GC + publish while stale 8d (> 7d) expires.
        let trace_6d: TraceIngestBatch = serde_json::from_value(make_trace_batch(
            "tr_6d",
            "s_6d",
            "u_6d",
            now_ms - 6 * 86_400_000,
            "bot",
        ))
        .unwrap();
        let r_6d = owner2
            .control
            .agent_store()
            .ingest_forwarded_batch("w_age_check", &pid, 0, trace_6d, now_ms - 6 * 86_400_000)
            .await
            .unwrap();
        assert_eq!(r_6d.accepted_traces, 1);

        let trace_8d: TraceIngestBatch = serde_json::from_value(make_trace_batch(
            "tr_8d",
            "s_8d",
            "u_8d",
            now_ms - 8 * 86_400_000,
            "bot",
        ))
        .unwrap();
        let r_8d = owner2
            .control
            .agent_store()
            .ingest_forwarded_batch("w_age_8d", &pid, 0, trace_8d, now_ms - 8 * 86_400_000)
            .await
            .unwrap();
        assert_eq!(r_8d.accepted_traces, 1);

        owner2.control.agent_store().flush_all_l0().await.unwrap();
        owner2
            .control
            .agent_store()
            .compact_and_gc(Some(&pid), &[])
            .await
            .unwrap();
        owner2
            .control
            .agent_store()
            .publish_all_projects()
            .await
            .unwrap();

        let pub_store =
            PublishedAgentManifestStore::new(&pid, owner2.storage.encrypted_store().clone())
                .unwrap();
        let pub_man = pub_store
            .load_latest()
            .await
            .unwrap()
            .expect("published agent manifest after 6d/8d expiry check");
        assert!(
            pub_man.writer_offsets.contains_key("w_age_check"),
            "6d (< 7d) writer_offsets entry must remain active: {:?}",
            pub_man.writer_offsets
        );
        assert!(
            !pub_man.writer_offsets.contains_key("w_age_8d"),
            "8d (> 7d) writer_offsets entry must be pruned across compact_and_gc + publish: {:?}",
            pub_man.writer_offsets
        );

        let r_6d_dup = owner2
            .control
            .agent_store()
            .ingest_forwarded_batch("w_age_check", &pid, 0, TraceIngestBatch::default(), now_ms)
            .await
            .unwrap();
        assert_eq!(r_6d_dup.accepted_traces, 0);

        // Because w_age_8d's >7d offset expired and was pruned, offset 0 is accepted again.
        let trace_8d_recheck: TraceIngestBatch = serde_json::from_value(make_trace_batch(
            "tr_8d_recheck",
            "s_8d_r",
            "u_8d_r",
            now_ms,
            "bot",
        ))
        .unwrap();
        let r_8d_after_expire = owner2
            .control
            .agent_store()
            .ingest_forwarded_batch("w_age_8d", &pid, 0, trace_8d_recheck, now_ms)
            .await
            .unwrap();
        assert_eq!(r_8d_after_expire.accepted_traces, 1);

        writer2.shutdown().await;
        owner2.shutdown().await;
    });
}

#[test]
fn w3_exit_5_and_6_routing_sync_profiles_queries_cache_invalidation_and_query_writes() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let dir_all = tmp.path().join("all_a");
    let dir_ing = tmp.path().join("ingest_b");
    let dir_writer = tmp.path().join("writer");
    let dir_query = tmp.path().join("query");
    let meta_db = tmp.path().join("meta.db");
    let meta_url = format!("sqlite://{}", meta_db.display());

    let _env = EnvGuard::set(&[
        ("DATABASE_URL", Some(&meta_url)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_FLUSH_MS", Some("10")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("2")),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    rt.block_on(async {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_a = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_all, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("node-a", None),
        )
        .await
        .unwrap();
        let node_b = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_ing, "proj_default", NodeRole::Ingest),
            s3.clone(),
            NodeIdentity::new("node-b", None),
        )
        .await
        .unwrap();
        let writer = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-r", None),
        )
        .await
        .unwrap();
        let query = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_query, "proj_default", NodeRole::Query),
            s3.clone(),
            NodeIdentity::new("query-r", None),
        )
        .await
        .unwrap();

        let url_a = format!("http://{}", node_a.http_addr);
        let url_b = format!("http://{}", node_b.http_addr);
        let url_w = format!("http://{}", writer.http_addr);
        let url_q = format!("http://{}", query.http_addr);

        // Public GET /v1/status on NodeRole::Ingest MUST stay 404 (ha_02 invariant).
        assert_eq!(
            http.get(format!("{url_b}/v1/status"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            404
        );

        let (pid, pk) = create_project_on(&http, &url_a, "proj-route").await;
        // Reassign ownership of `pid` explicitly to `node-b` (`NodeRole::Ingest`).
        node_b
            .control
            .meta()
            .set_project_owner(&pid, "node-b", &url_b, Utc::now().timestamp_millis())
            .await
            .unwrap();

        let now_ms = Utc::now().timestamp_millis();

        // 1. Sync profile calls + empty POST /v1/groups + replay/chunk via Writer & non-owner All.
        for (label, target_url, uid, gid) in [
            ("writer", &url_w, "u_via_w", "g_via_w"),
            ("all_non_owner", &url_a, "u_via_a", "g_via_a"),
        ] {
            let r_id = http
                .post(format!("{target_url}/v1/identify"))
                .header("x-pulse-key", &pk)
                .json(&json!({ "anon_id": format!("anon_{uid}"), "user_id": uid }))
                .send()
                .await
                .unwrap();
            assert_eq!(r_id.status(), 200, "{label}");
            let b_id: Value = r_id.json().await.unwrap();
            assert!(b_id["canonical_uid"].as_u64().is_some(), "{b_id}");

            let r_eng = http
                .post(format!("{target_url}/v1/engage"))
                .header("x-pulse-key", &pk)
                .json(&json!({ "distinct_id": uid, "$set": { "via": label } }))
                .send()
                .await
                .unwrap();
            assert_eq!(r_eng.status(), 200, "{label}");

            let r_grp = http
                .post(format!("{target_url}/v1/groups"))
                .header("x-pulse-key", &pk)
                .json(&json!({ "group_type": "org", "group_id": gid, "$set": { "tier": "ent" } }))
                .send()
                .await
                .unwrap();
            assert_eq!(r_grp.status(), 200, "{label}");

            // M6a Caveat #2: empty /v1/groups ([], {}) must forward and succeed when owner is up.
            for empty_body in [json!([]), json!({})] {
                let r_empty = http
                    .post(format!("{target_url}/v1/groups"))
                    .header("x-pulse-key", &pk)
                    .json(&empty_body)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(r_empty.status(), 200, "{label} {empty_body}");
            }

            let r_rep = http
                .post(format!("{target_url}/v1/replay/chunk"))
                .header("x-pulse-key", &pk)
                .json(&json!({
                    "project_id": pid,
                    "session_id": format!("sess_{uid}"),
                    "chunk_start_ts_ms": now_ms,
                    "ops": [
                        { "op": "full_snapshot", "ts_offset_ms": 0, "width": 390, "height": 844, "nodes": [] }
                    ]
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(r_rep.status(), 200, "{label}");
        }

        // Mixed /v1/ingest batch on Writer: valid identify+event succeeds, 409 identify conflict aborts atomically.
        let r_mix_ok = http
            .post(format!("{url_w}/v1/ingest"))
            .header("x-pulse-key", &pk)
            .json(&json!({
                "batch": [
                    { "type": "identify", "userId": "u_mix_1", "anonymousId": "a_mix_1", "traits": { "plan": "pro" } },
                    { "type": "track", "userId": "u_mix_1", "event": "Mix_Event", "messageId": "mm1", "timestamp": now_ms }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_mix_ok.status(), 200);
        let b_mix: Value = r_mix_ok.json().await.unwrap();
        assert_eq!(b_mix["profiles_updated"], 1, "{b_mix}");

        assert_eq!(
            http.post(format!("{url_w}/v1/identify"))
                .header("x-pulse-key", &pk)
                .json(&json!({ "anon_id": "a_mix_2", "user_id": "u_mix_2" }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let r_mix_conflict = http
            .post(format!("{url_w}/v1/ingest"))
            .header("x-pulse-key", &pk)
            .json(&json!({
                "batch": [
                    { "type": "identify", "userId": "u_mix_1", "anonymousId": "u_mix_2" },
                    { "type": "track", "userId": "u_mix_1", "event": "Mix_Conflict_Should_Not_Land", "messageId": "mm_bad", "timestamp": now_ms }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_mix_conflict.status(), 409);

        assert_eq!(
            http.post(format!("{url_w}/v1/agent/traces"))
                .header("x-pulse-key", &pk)
                .json(&make_trace_batch("tr_route_1", "sr1", "u_via_w", now_ms, "bot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_w}/v1/agent/traces"))
                .header("x-pulse-key", &pk)
                .json(&make_trace_batch("tr_route_del", "sr2", "u_via_a", now_ms + 1, "bot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        poll_until(Duration::from_secs(10), || async {
            insights_total(&http, &url_w, &pid, "Mix_Event", now_ms).await == 1
                && list_agent_trace_ids(&http, &url_w, &pid).await.len() == 2
        })
        .await;
        assert_eq!(
            insights_total(&http, &url_w, &pid, "Mix_Conflict_Should_Not_Land", now_ms).await,
            0
        );

        // 2. Verify Writer & non-owner All route all query/project/non-:id endpoints cleanly.
        let r_user_w = http
            .get(format!("{url_w}/v1/projects/{pid}/users/u_via_w"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(r_user_w.status(), 200);

        let r_replay_range = http
            .get(format!("{url_w}/v1/projects/{pid}/replay/sess_u_via_w"))
            .bearer_auth(ADMIN_TOKEN)
            .header("range", "bytes=0-15")
            .send()
            .await
            .unwrap();
        assert_eq!(r_replay_range.status(), 206);

        let r_flag_create = http
            .post(format!("{url_w}/v1/projects/{pid}/flags"))
            .bearer_auth(ADMIN_TOKEN)
            .header("origin", format!("http://{}", writer.http_addr))
            .json(&json!({ "key": "w3_flag", "enabled": true, "rollout_pct": 100.0 }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_flag_create.status(), 200);

        // Non-:id routes on non-owner node-a (`All`): status, query, sessions, replay, mcp, flags.
        assert_eq!(
            http.get(format!("{url_a}/v1/status?project_id={pid}"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_a}/v1/query"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "kind": "insights",
                    "project_id": pid,
                    "query": {
                        "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                        "interval": "day",
                        "series": [{ "label": "A", "event_name": "Mix_Event", "metric": "totals" }]
                    }
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.get(format!("{url_a}/v1/sessions?project_id={pid}"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.get(format!("{url_a}/v1/replay/sess_u_via_w?project_id={pid}"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_a}/v1/mcp"))
                .bearer_auth(ADMIN_TOKEN)
                .header("x-pulse-project", &pid)
                .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.get(format!("{url_a}/v1/flags/{pk}"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        // 3. Publish on node-b and test NodeRole::Query S3 reads + 4 forwarded storage writes.
        node_b.storage.flush_all_projects().await.unwrap();
        node_b.storage.publish_all_projects().await.unwrap();
        node_b.control.agent_store().flush_all_l0().await.unwrap();
        node_b.control.agent_store().publish_all_projects().await.unwrap();
        query.run_query_refresh_tick().await;

        assert_eq!(insights_total(&http, &url_q, &pid, "Mix_Event", now_ms).await, 1);
        assert_eq!(list_agent_trace_ids(&http, &url_q, &pid).await.len(), 2);

        // Forwarded 4 write mutations via NodeRole::Query:
        assert_eq!(
            http.post(format!("{url_q}/v1/projects/{pid}/import"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "events": [{ "event": "Imp_Via_Q", "userId": "u_imp", "messageId": "miq1", "timestamp": now_ms }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.delete(format!("{url_q}/v1/projects/{pid}/users/u_via_a"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.delete(format!("{url_q}/v1/projects/{pid}/agent/traces/tr_route_del"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_q}/v1/projects/{pid}/agent/erasure"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({ "level": "trace", "target_id": "tr_route_1" }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        // Non-owner node-a and writer NEVER opened local SlateDB for pid!
        assert!(!dir_all.join("hot/identity").join(&pid).exists());
        assert!(!dir_all.join("hot/profiles").join(&pid).exists());
        assert!(!dir_writer.join("hot/identity").exists());
        assert!(!dir_writer.join("hot/profiles").exists());

        // 4. Stale cache invalidation & dead owner 503 & forward loop guard.
        let r_loop = http
            .post(format!("{url_w}/v1/projects/{pid}/query/insights"))
            .bearer_auth(ADMIN_TOKEN)
            .header("x-pulse-forwarded-hop", "2")
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "Mix_Event", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_loop.status(), 503);
        let loop_body: Value = r_loop.json().await.unwrap();
        assert_eq!(loop_body["code"], "forward_loop_detected", "{loop_body}");

        // Live ownership transfer stale-cache test: create a fresh project on node-b, warm writer's
        // cache via /v1/projects/{pid_xfer}/flags, reassign to node-a while node-b stays up,
        // and verify writer handles 503 not_owner by refreshing & retrying against node-a -> 200!
        let (pid_xfer, pk_xfer) = create_project_on(&http, &url_a, "proj-xfer").await;
        node_b
            .control
            .meta()
            .set_project_owner(&pid_xfer, "node-b", &url_b, Utc::now().timestamp_millis())
            .await
            .unwrap();
        assert_eq!(
            http.get(format!("{url_w}/v1/projects/{pid_xfer}/flags"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node_a
            .control
            .meta()
            .set_project_owner(&pid_xfer, "node-a", &url_a, Utc::now().timestamp_millis())
            .await
            .unwrap();
        let r_live_xfer_sync = http
            .post(format!("{url_w}/v1/engage"))
            .header("x-pulse-key", &pk_xfer)
            .json(&json!({ "distinct_id": "u_xfer", "$set": { "live_xfer": true } }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_live_xfer_sync.status(), 200);
        let r_live_xfer_q = http
            .post(format!("{url_w}/v1/projects/{pid_xfer}/query/insights"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "Mix_Event", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_live_xfer_q.status(), 200);

        // Dead-owner transport-error refresh test: restart node-b on dir_ing with a new port;
        // writer's stale cache for `pid` points to dead url_b -> transport error refresh -> url_b2 -> 200!
        node_b.shutdown().await;
        tokio::time::sleep(Duration::from_millis(2_300)).await;
        let node_b2 = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_ing, "proj_default", NodeRole::Ingest),
            s3.clone(),
            NodeIdentity::new("node-b", None),
        )
        .await
        .unwrap();

        let r_stale_retry = http
            .post(format!("{url_w}/v1/engage"))
            .header("x-pulse-key", &pk)
            .json(&json!({ "distinct_id": "u_via_w", "$set": { "failover": true } }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_stale_retry.status(), 200);

        let r_stale_q = http
            .get(format!("{url_w}/v1/projects/{pid}/users/u_via_w"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(r_stale_q.status(), 200);

        // Shutdown node-b2 and node-a too -> writer returns immediate 503 on sync & query routes (never hangs!).
        node_b2.shutdown().await;
        node_a.shutdown().await;
        let r_dead_owner = http
            .post(format!("{url_w}/v1/engage"))
            .header("x-pulse-key", &pk)
            .json(&json!({ "distinct_id": "u_via_w", "$set": { "dead": true } }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_dead_owner.status(), 503);

        let r_dead_q = http
            .post(format!("{url_w}/v1/projects/{pid}/query/insights"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "Mix_Event", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_dead_q.status(), 503);

        query.shutdown().await;
        writer.shutdown().await;
    });
}

#[test]
fn w3_exit_7_alerts_fire_once_on_owner_when_split_role_and_writer_present() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let dir_ing = tmp.path().join("ingest");
    let dir_writer = tmp.path().join("writer");
    let dir_query = tmp.path().join("query");
    let meta_db = tmp.path().join("meta.db");
    let meta_url = format!("sqlite://{}", meta_db.display());

    let _env = EnvGuard::set(&[
        ("DATABASE_URL", Some(&meta_url)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_FLUSH_MS", Some("10")),
        ("PULSE_ALLOW_PRIVATE_ENDPOINTS", Some("1")),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let http = reqwest::Client::new();

    rt.block_on(async {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = hits.clone();
        let webhook_app = Router::new().route(
            "/hook",
            post(move || {
                let h = hits_clone.clone();
                async move {
                    h.fetch_add(1, Ordering::AcqRel);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let hook_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hook_addr = hook_listener.local_addr().unwrap();
        let hook_url = format!("http://{hook_addr}/hook");
        let hook_task = tokio::spawn(async move {
            let _ = axum::serve(hook_listener, webhook_app).await;
        });

        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingest = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_ing, "proj_default", NodeRole::Ingest),
            s3.clone(),
            NodeIdentity::new("ingest-al", None),
        )
        .await
        .unwrap();
        let writer = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_writer, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-al", None),
        )
        .await
        .unwrap();
        let query = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_query, "proj_default", NodeRole::Query),
            s3.clone(),
            NodeIdentity::new("query-al", None),
        )
        .await
        .unwrap();

        let url_i = format!("http://{}", ingest.http_addr);
        let url_w = format!("http://{}", writer.http_addr);
        let url_q = format!("http://{}", query.http_addr);

        let (pid, pk) = create_project_on(&http, &url_q, "proj-alert").await;
        ingest
            .control
            .meta()
            .set_project_owner(&pid, "ingest-al", &url_i, Utc::now().timestamp_millis())
            .await
            .unwrap();
        query.run_query_refresh_tick().await;

        // Configure BYOK webhook + event alert rule via writer (forwards to ingest owner!).
        assert_eq!(
            http.put(format!("{url_w}/v1/projects/{pid}/byok"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "category": "webhook",
                    "provider": "webhook",
                    "secret": hook_url,
                    "config_json": {
                        "signing_secret": "w3_webhook_secret_123456"
                    }
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_w}/v1/projects/{pid}/alerts"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "name": "payment_failure_spike",
                    "event_name": "Payment_Failed",
                    "operator": "gte",
                    "threshold": 2,
                    "webhook_url": hook_url
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        let now_ms = Utc::now().timestamp_millis();
        assert_eq!(
            http.post(format!("{url_w}/v1/ingest"))
                .header("x-pulse-key", &pk)
                .json(&json!({
                    "events": [
                        { "event": "Payment_Failed", "userId": "u1", "messageId": "pf_1", "timestamp": now_ms },
                        { "event": "Payment_Failed", "userId": "u2", "messageId": "pf_2", "timestamp": now_ms + 1 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        poll_until(Duration::from_secs(10), || async {
            insights_total(&http, &url_w, &pid, "Payment_Failed", now_ms).await == 2
        })
        .await;

        ingest.storage.flush_all_projects().await.unwrap();
        ingest.storage.publish_all_projects().await.unwrap();
        query.run_query_refresh_tick().await;

        // Trigger alert checks concurrently on Ingest and Query — owner + alert_claims ensures 1 webhook!
        tokio::join!(
            ingest.control.evaluate_alerts(),
            query.control.evaluate_alerts()
        );
        assert_eq!(
            hits.load(Ordering::Acquire),
            1,
            "event alert must fire exactly once across Ingest + Query + Writer"
        );

        // Second evaluation within 24h cooldown does not re-fire.
        ingest.control.evaluate_alerts().await;
        query.control.evaluate_alerts().await;
        assert_eq!(hits.load(Ordering::Acquire), 1);

        hook_task.abort();
        query.shutdown().await;
        writer.shutdown().await;
        ingest.shutdown().await;
    });
}

#[test]
fn w3_live_ownership_migration_prevents_split_brain_and_stale_lease_errors() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let dir_a = tmp.path().join("owner_a");
    let dir_b = tmp.path().join("node_b");
    let dir_w = tmp.path().join("writer");
    let meta_db = tmp.path().join("meta.db");
    let meta_url = format!("sqlite://{}", meta_db.display());

    let _env = EnvGuard::set(&[
        ("DATABASE_URL", Some(&meta_url)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_FLUSH_MS", Some("10")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("15")),
        ("PULSE_OWNER_CACHE_TTL_MS", Some("60000")),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    rt.block_on(async {
        let s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner_a = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_a, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("owner-a", None),
        )
        .await
        .unwrap();
        let node_b = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_b, "proj_default", NodeRole::All),
            s3.clone(),
            NodeIdentity::new("node-b", None),
        )
        .await
        .unwrap();
        let writer = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_w, "proj_default", NodeRole::Writer),
            Arc::new(InMemory::new()),
            NodeIdentity::new("writer-mig", None),
        )
        .await
        .unwrap();

        let url_a = format!("http://{}", owner_a.http_addr);
        let url_b = format!("http://{}", node_b.http_addr);
        let url_w = format!("http://{}", writer.http_addr);

        let (pid, pk) = create_project_on(&http, &url_a, "proj-live-mig").await;
        owner_a
            .control
            .meta()
            .set_project_owner(&pid, "owner-a", &url_a, Utc::now().timestamp_millis())
            .await
            .unwrap();

        let now_ms = Utc::now().timestamp_millis();

        // Warm ProjectStorage + AgentTraceStore + writer_leases on owner-a AND warm writer-1's
        // and owner-a's 60s MetaOwnerDirectory caches.
        assert_eq!(
            http.post(format!("{url_w}/v1/identify"))
                .header("x-pulse-key", &pk)
                .json(&json!({ "anon_id": "anon_warm", "user_id": "u_warm" }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_w}/v1/ingest"))
                .header("x-pulse-key", &pk)
                .json(&json!({
                    "events": [{
                        "event": "Warm_Ev",
                        "userId": "u_warm",
                        "messageId": "m_warm_1",
                        "timestamp": now_ms
                    }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{url_w}/v1/agent/traces"))
                .header("x-pulse-key", &pk)
                .json(&make_trace_batch("tr_warm", "s_warm", "u_warm", now_ms, "bot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        poll_until(Duration::from_secs(10), || async {
            insights_total(&http, &url_a, &pid, "Warm_Ev", now_ms).await == 1
                && list_agent_trace_ids(&http, &url_a, &pid)
                    .await
                    .contains(&"tr_warm".to_string())
        })
        .await;
        assert!(owner_a.storage.get_project(&pid).is_some());

        // Warm owner-a's MetaOwnerDirectory cache for /v1/mcp and /v1/query.
        let r_mcp_warm = http
            .post(format!("{url_a}/v1/mcp"))
            .bearer_auth(ADMIN_TOKEN)
            .header("x-pulse-project", &pid)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "query_insights",
                    "arguments": { "event_name": "Warm_Ev", "metric": "totals" }
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_mcp_warm.status(), 200);

        // Migrate project_owner to node-b WITHOUT restarting owner-a and WITHOUT calling reconcile_ownership()!
        node_b
            .control
            .meta()
            .set_project_owner(&pid, "node-b", &url_b, Utc::now().timestamp_millis())
            .await
            .unwrap();

        // Immediately exercise /v1/identify, /v1/engage, /v1/replay/chunk, /v1/ingest,
        // /v1/agent/traces, /v1/projects/:id/query/insights, and /v1/mcp!
        let r_id = http
            .post(format!("{url_w}/v1/identify"))
            .header("x-pulse-key", &pk)
            .json(&json!({ "anon_id": "anon_mig", "user_id": "u_mig" }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_id.status(), 200, "{}", r_id.text().await.unwrap());

        let r_eng = http
            .post(format!("{url_w}/v1/engage"))
            .header("x-pulse-key", &pk)
            .json(&json!({ "distinct_id": "u_mig", "$set": { "migrated": true } }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_eng.status(), 200, "{}", r_eng.text().await.unwrap());

        let r_rep = http
            .post(format!("{url_w}/v1/replay/chunk"))
            .header("x-pulse-key", &pk)
            .json(&json!({
                "project_id": pid,
                "session_id": "sess_mig",
                "chunk_start_ts_ms": now_ms,
                "ops": [
                    { "op": "full_snapshot", "ts_offset_ms": 0, "width": 390, "height": 844, "nodes": [] }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_rep.status(), 200, "{}", r_rep.text().await.unwrap());

        let r_ing = http
            .post(format!("{url_w}/v1/ingest"))
            .header("x-pulse-key", &pk)
            .json(&json!({
                "events": [{
                    "event": "Migrated_Ev",
                    "userId": "u_mig",
                    "messageId": "m_mig_1",
                    "timestamp": now_ms + 5
                }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_ing.status(), 200);

        let r_ag = http
            .post(format!("{url_w}/v1/agent/traces"))
            .header("x-pulse-key", &pk)
            .json(&make_trace_batch("tr_migrated", "s_mig", "u_mig", now_ms + 6, "bot"))
            .send()
            .await
            .unwrap();
        assert_eq!(r_ag.status(), 200);

        // Immediately query via writer (tests no 503 not_writer on new owner node-b).
        let r_q_imm = http
            .post(format!("{url_w}/v1/projects/{pid}/query/insights"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "Migrated_Ev", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_q_imm.status(), 200, "{}", r_q_imm.text().await.unwrap());

        // Wait until forwarded async events & agent traces land on node-b.
        poll_until(Duration::from_secs(10), || async {
            insights_total(&http, &url_b, &pid, "Migrated_Ev", now_ms).await == 1
                && list_agent_trace_ids(&http, &url_b, &pid)
                    .await
                    .contains(&"tr_migrated".to_string())
        })
        .await;

        // Stale-cache POST /v1/mcp on deposed owner-a must detect NotOwner and forward to node-b -> 200!
        let r_mcp_mig = http
            .post(format!("{url_a}/v1/mcp"))
            .bearer_auth(ADMIN_TOKEN)
            .header("x-pulse-project", &pid)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "query_insights",
                    "arguments": { "event_name": "Migrated_Ev", "metric": "totals" }
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_mcp_mig.status(), 200);
        let mcp_body: Value = r_mcp_mig.json().await.unwrap();
        assert_ne!(mcp_body["result"]["isError"], json!(true), "{mcp_body}");
        assert_eq!(
            mcp_body["result"]["structuredContent"]["series"][0]["overall_value"],
            json!(1.0),
            "{mcp_body}"
        );

        // Verify identify/engage/replay landed ONLY on node-b (zero split-brain writes on owner-a!).
        let r_user_b = http
            .get(format!("{url_b}/v1/projects/{pid}/users/u_mig"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(r_user_b.status(), 200);
        let user_b: Value = r_user_b.json().await.unwrap();
        assert_eq!(
            user_b["profile"]["props"]["migrated"],
            json!(true),
            "{user_b}"
        );

        let r_rep_b = http
            .get(format!("{url_b}/v1/projects/{pid}/replay/sess_mig"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(r_rep_b.status(), 200);

        assert!(
            owner_a.storage.get_project(&pid).is_none(),
            "deposed owner-a must fence and evict cached ProjectStorage handle"
        );
        assert!(
            !dir_a.join("hot/hot/replay").join(&pid).join("sess_mig").exists(),
            "post-migration replay must not write to deposed owner-a"
        );
        assert!(
            dir_b.join("hot/hot/replay").join(&pid).join("sess_mig").exists(),
            "post-migration replay must land on new owner node-b"
        );

        writer.shutdown().await;
        node_b.shutdown().await;
        owner_a.shutdown().await;
    });
}
