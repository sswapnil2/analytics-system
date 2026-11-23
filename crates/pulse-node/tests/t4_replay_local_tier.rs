//! T4 integration & fault-injection tests for Feature #27a (Replay Local Hot Tier & Cycle Upload):
//! 1. Zero S3 calls during `/v1/replay/chunk` writes & idle cycles under `PULSE_HOT_STORE=local`
//! 2. Cycle upload to S3 `replays/`, local `hot/replay/` cleanup, `RunningNode::shutdown` flush,
//!    and monotonic post-upload `seq` + `replay_offset_kb` continuation across restarts
//! 3. Crash recovery: `SIGKILL` pre-upload (real `pulse-node` process) + crash mid-session upload
//!    (partial upload + `AlreadyExists` idempotent completion) + S3-unreachable `200 OK` resilience
//! 4. Hybrid reads across S3 `replays/` + local `hot/replay/`, HTTP `206 Range` across boundary,
//!    `?format=binary`, concurrent reader vs upload+delete race, and concurrent chunk appends
//! 5. `NodeRole::Query` reading uploaded `replays/` with `PULSE_BYOK_MASTER_KEY` (`PLSEAGM2` reseal),
//!    dynamic key rotation, `only_with_replay` / funnel visibility, and 0 idle S3 calls
//! 6. `PULSE_UPLOAD_INTERVAL_SECS=0` eager upload, `PULSE_HOT_STORE=object` compatibility &
//!    mode-switch refusal on leftover `hot/replay/`, local disk failure -> `503`, DPDP
//!    `delete_session` purging both tiers, and `restore --from-published` cursor seeding

use std::collections::BTreeMap;
use std::fmt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_control::MetaStore;
use pulse_node::{
    restore_from_published_with_store_and_meta_for_test, start_pulse_node_with_store, NodeConfig,
    NodeRole,
};
use pulse_storage::with_hot_store;
use serde_json::{json, Value};

const ADMIN_TOKEN: &str = "sk_root_t4_replay_local_tier_test_token_000000000";
const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST_VAULT_KEK: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

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

#[derive(Debug)]
struct FaultAndCountS3 {
    inner: Arc<dyn ObjectStore>,
    calls: Mutex<BTreeMap<(String, &'static str), u64>>,
    fail_replay_puts_after: Mutex<Option<usize>>,
    replay_put_hits: AtomicUsize,
}

impl FaultAndCountS3 {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            calls: Mutex::new(BTreeMap::new()),
            fail_replay_puts_after: Mutex::new(None),
            replay_put_hits: AtomicUsize::new(0),
        }
    }

    fn count(&self, op: &'static str, p: Option<&ObjectPath>) {
        let prefix = p
            .and_then(|p| p.parts().next())
            .map_or("(root)".to_string(), |s| s.as_ref().to_string());
        *self.calls.lock().unwrap().entry((prefix, op)).or_default() += 1;
    }

    fn take_calls(&self) -> BTreeMap<(String, &'static str), u64> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }

    fn fail_replay_puts_after(&self, hits_before_fail: Option<usize>) {
        self.replay_put_hits.store(0, Ordering::SeqCst);
        *self.fail_replay_puts_after.lock().unwrap() = hits_before_fail;
    }

    fn maybe_fail_replay_put(&self, l: &ObjectPath) -> OsResult<()> {
        if l.as_ref().starts_with("replays/") {
            if let Some(limit) = *self.fail_replay_puts_after.lock().unwrap() {
                let hit = self.replay_put_hits.fetch_add(1, Ordering::SeqCst);
                if hit >= limit {
                    return Err(object_store::Error::Generic {
                        store: "FaultAndCountS3",
                        source: Box::new(std::io::Error::other("injected S3 replay PUT failure")),
                    });
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for FaultAndCountS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultAndCountS3({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultAndCountS3 {
    async fn put_opts(&self, l: &ObjectPath, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
        self.count("PUT", Some(l));
        self.maybe_fail_replay_put(l)?;
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.count("PUT", Some(l));
        self.maybe_fail_replay_put(l)?;
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
        self.count("GET", Some(l));
        self.inner.get_opts(l, o).await
    }
    async fn get_range(&self, l: &ObjectPath, r: std::ops::Range<u64>) -> OsResult<Bytes> {
        self.count("GET", Some(l));
        self.inner.get_range(l, r).await
    }
    async fn head(&self, l: &ObjectPath) -> OsResult<ObjectMeta> {
        self.count("HEAD", Some(l));
        self.inner.head(l).await
    }
    async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
        self.count("DELETE", Some(l));
        self.inner.delete(l).await
    }
    fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.count("LIST", p);
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.count("LIST", p);
        self.inner.list_with_delimiter(p).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.count("PUT", Some(to));
        self.maybe_fail_replay_put(to)?;
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.count("PUT", Some(to));
        self.maybe_fail_replay_put(to)?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn sum_all_calls(calls: &BTreeMap<(String, &'static str), u64>) -> u64 {
    calls.values().sum()
}

fn sum_op(calls: &BTreeMap<(String, &'static str), u64>, op: &str) -> u64 {
    calls
        .iter()
        .filter(|((_, o), _)| *o == op)
        .map(|(_, n)| *n)
        .sum()
}

fn node_cfg(dir: &Path, pid: &str, role: NodeRole) -> NodeConfig {
    NodeConfig {
        role,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.to_path_buf(),
        web_dist_dir: dir.join("no_web"),
        default_project_id: pid.to_string(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN_TOKEN.to_string()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 4,
    }
}

fn replay_chunk_json(sid: &str, start_ts_ms: i64, ts_offset_ms: u32, label: &str) -> Value {
    json!({
        "project_id": "",
        "session_id": sid,
        "chunk_start_ts_ms": start_ts_ms,
        "ops": [{
            "op": "full_snapshot",
            "ts_offset_ms": ts_offset_ms,
            "width": 390,
            "height": 844,
            "nodes": [{
                "node_id": 1,
                "parent_id": 0,
                "kind": "screen",
                "x": 0,
                "y": 0,
                "w": 390,
                "h": 844,
                "bg_rgba": 255,
                "masked_label": label
            }]
        }]
    })
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(files_under(&p));
        } else {
            out.push(p);
        }
    }
    out.sort();
    out
}

#[test]
fn t4_27a_replay_zero_s3_writes_upload_continuation_and_concurrent_appends() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("node_a");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_rep_zero_s3";

        let node = start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Warm open project once.
        assert_eq!(
            http.post(format!("{base}/v1/replay/chunk"))
                .header("x-pulse-key", &key)
                .json(&replay_chunk_json("sess_warm", now_ms, 0, "Warm"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        s3.take_calls();

        // 1. Write 12 replay chunks sequentially to `sess_seq` (with large labels to push > 1 KiB).
        let big_label = "X".repeat(600);
        let mut last_byte_offset = 0u64;
        let mut total_bytes = 0u64;
        for i in 0..12u32 {
            let resp = http
                .post(format!("{base}/v1/replay/chunk"))
                .header("x-pulse-key", &key)
                .json(&replay_chunk_json(
                    "sess_seq",
                    now_ms + i as i64 * 100,
                    i * 100,
                    &big_label,
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body: Value = resp.json().await.unwrap();
            let bo = body["byte_offset"].as_u64().unwrap();
            let cb = body["chunk_bytes"].as_u64().unwrap();
            assert_eq!(bo, total_bytes);
            assert_eq!(body["replay_offset_kb"].as_u64().unwrap(), bo / 1024);
            last_byte_offset = bo;
            total_bytes += cb;
        }
        assert!(last_byte_offset > 0);
        assert_eq!(
            sum_all_calls(&s3.take_calls()),
            0,
            "/v1/replay/chunk writes must make 0 S3 calls in local mode"
        );
        assert!(
            !files_under(&data_dir.join("hot/hot/replay")).is_empty(),
            "local replay chunks must exist under hot/hot/replay"
        );
        assert!(
            files_under(&s3_dir.join("replays")).is_empty(),
            "S3 replays/ must be empty prior to upload cycle"
        );

        // 2. Concurrent /v1/replay/chunk appends on same session (`sess_conc`).
        let mut futs = Vec::new();
        for i in 0..16u32 {
            let http = http.clone();
            let base = base.clone();
            let key = key.clone();
            futs.push(async move {
                let resp = http
                    .post(format!("{base}/v1/replay/chunk"))
                    .header("x-pulse-key", &key)
                    .json(&replay_chunk_json(
                        "sess_conc",
                        now_ms + i as i64 * 50,
                        i * 50,
                        &format!("Conc_{i}"),
                    ))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
                resp.json::<Value>().await.unwrap()
            });
        }
        let conc_res = futures::future::join_all(futs).await;
        let mut offsets: Vec<u64> = conc_res
            .iter()
            .map(|v| v["byte_offset"].as_u64().unwrap())
            .collect();
        offsets.sort_unstable();
        offsets.dedup();
        assert_eq!(
            offsets.len(),
            16,
            "concurrent chunk byte_offsets must be unique"
        );
        assert_eq!(
            sum_all_calls(&s3.take_calls()),
            0,
            "concurrent /v1/replay/chunk writes must make 0 S3 calls"
        );

        // 3. Cycle upload publishes chunks to S3 `replays/` with PLSEAGM2 and cleans `hot/replay/`.
        node.storage.publish_all_projects().await.unwrap();
        let upload_calls = s3.take_calls();
        assert_eq!(
            sum_op(&upload_calls, "PUT"),
            1 + 12 + 16,
            "cycle upload must PUT all 29 pending chunks"
        );
        assert!(
            files_under(&data_dir.join("hot/hot/replay")).is_empty(),
            "local hot/replay/ must be cleaned up after cycle upload"
        );
        let s3_rep_0 = s3_fs
            .get_range(
                &ObjectPath::from(format!("replays/{pid}/sess_seq/0000000000.plsr")),
                0..8,
            )
            .await
            .unwrap();
        assert_eq!(s3_rep_0.as_ref(), b"PLSEAGM2");

        // 4. Idle publish cycle makes 0 S3 calls.
        node.storage.publish_all_projects().await.unwrap();
        assert_eq!(
            sum_all_calls(&s3.take_calls()),
            0,
            "idle upload cycle must make 0 S3 calls"
        );

        // 5. Shutdown node cleanly, restart on same data_dir, append chunk 12 to `sess_seq`:
        //    must continue at seq=12 and byte_offset=total_bytes with 0 S3 calls!
        node.shutdown().await;
        s3.take_calls();
        let node2 =
            start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3.clone())
                .await
                .unwrap();
        s3.take_calls();

        let cont_resp = http
            .post(format!("http://{}/v1/replay/chunk", node2.http_addr))
            .header("x-pulse-key", &key)
            .json(&replay_chunk_json(
                "sess_seq",
                now_ms + 9_999,
                9_999,
                "PostRestart",
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(cont_resp.status(), 200);
        let cont_body: Value = cont_resp.json().await.unwrap();
        assert_eq!(cont_body["byte_offset"].as_u64(), Some(total_bytes));
        assert_eq!(
            cont_body["object_path"].as_str(),
            Some(format!("hot/replay/{pid}/sess_seq/0000000012.plsr").as_str())
        );

        // 6. Hybrid read across S3 (0..12) + local (12), including HTTP 206 Range across boundary
        //    and `?format=binary`!
        let base2 = format!("http://{}", node2.http_addr);
        let full_bin = http
            .get(format!(
                "{base2}/v1/projects/{pid}/replay/sess_seq?format=binary"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(full_bin.status(), 200);
        let full_bytes = full_bin.bytes().await.unwrap();
        let cont_len = cont_body["chunk_bytes"].as_u64().unwrap();
        assert_eq!(full_bytes.len() as u64, total_bytes + cont_len);

        // Range spanning end of S3 chunk 11 into local chunk 12:
        let range_start = total_bytes.saturating_sub(32);
        let range_end = total_bytes + cont_len.min(32) - 1;
        let range_resp = http
            .get(format!("{base2}/v1/projects/{pid}/replay/sess_seq"))
            .bearer_auth(ADMIN_TOKEN)
            .header("Range", format!("bytes={range_start}-{range_end}"))
            .send()
            .await
            .unwrap();
        assert_eq!(range_resp.status(), 206);
        let range_bytes = range_resp.bytes().await.unwrap();
        assert_eq!(
            range_bytes.as_ref(),
            &full_bytes[range_start as usize..=range_end as usize]
        );

        // 7. RunningNode::shutdown flushes pending local chunk 12 to S3 before exit.
        node2.shutdown().await;
        assert!(
            s3_fs
                .head(&ObjectPath::from(format!(
                    "replays/{pid}/sess_seq/0000000012.plsr"
                )))
                .await
                .is_ok(),
            "RunningNode::shutdown must upload pending local replay chunk to S3"
        );
    });
}

async fn wait_for_healthz(
    http: &reqwest::Client,
    base: &str,
    child: &mut std::process::Child,
    stdout_path: &Path,
    stderr_path: &Path,
    label: &str,
) {
    for _ in 0..300 {
        if let Some(status) = child.try_wait().unwrap() {
            let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
            let stderr = std::fs::read_to_string(stderr_path).unwrap_or_default();
            panic!(
                "{label} exited unexpectedly with {status} before /healthz became ready\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
        if let Ok(r) = http.get(format!("{base}/healthz")).send().await {
            if r.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
    let stderr = std::fs::read_to_string(stderr_path).unwrap_or_default();
    panic!(
        "{label} /healthz did not become ready within 30s\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn t4_27a_crash_recovery_mid_upload_and_sigkill_pre_upload() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Part A: Mid-session upload failure + simulated pre-cursor S3 copy + restart recovery.
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("node_crash");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_crash_rep";

        let hot_routed = with_hot_store(s3.clone(), &data_dir).await.unwrap();
        let enc = Arc::new(pulse_storage::EncryptedObjectStore::new(hot_routed));
        let id_router = Arc::new(pulse_identity::IdentityRouter::new(enc.as_object_store()));
        let engine1 = pulse_storage::StorageEngine::new_with_encrypted(
            pulse_storage::StorageConfig::default(),
            enc,
            id_router,
        );

        // Fail ALL S3 replay PUTs during ingest -> `append_replay_chunk` still succeeds locally!
        s3.fail_replay_puts_after(Some(0));
        for i in 0..3u32 {
            let chunk = pulse_wire::ReplayChunk {
                project_id: pid.to_string(),
                session_id: "sess_mid".to_string(),
                chunk_start_ts_ms: 1_762_000_000_000 + i as i64 * 100,
                ops: vec![pulse_wire::WireframeOp::PointerInteraction {
                    ts_offset_ms: i * 100,
                    x: 10,
                    y: 20,
                    kind: pulse_wire::InteractionKind::Tap,
                }],
            };
            engine1.append_replay_chunk(&chunk).await.unwrap();
        }

        // Allow chunk 0 to succeed, fail chunk 1 mid-upload.
        s3.fail_replay_puts_after(Some(1));
        assert!(engine1.publish_all_projects().await.is_err());
        assert!(s3_fs
            .head(&ObjectPath::from(format!(
                "replays/{pid}/sess_mid/0000000000.plsr"
            )))
            .await
            .is_ok());

        // Inject crash-style state: copy chunk 1 directly to S3 without advancing cursor, then drop engine1!
        s3.fail_replay_puts_after(None);
        let hot_1 = data_dir.join(format!("hot/hot/replay/{pid}/sess_mid/0000000001.plsr"));
        let bytes_1 = std::fs::read(&hot_1).unwrap();
        s3_fs
            .put(
                &ObjectPath::from(format!("replays/{pid}/sess_mid/0000000001.plsr")),
                PutPayload::from_bytes(Bytes::from(bytes_1)),
            )
            .await
            .unwrap();
        engine1.identity_router().close_all().await.unwrap();
        engine1.profile_router().close_all().await.unwrap();
        drop(engine1);

        // Restart node on same data_dir.
        let node2 =
            start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3.clone())
                .await
                .unwrap();
        // Upload cycle recovers pending session, handles AlreadyExists on chunk 1, uploads chunk 2,
        // cleans local hot/replay/, and decodes all 3 chunks cleanly!
        node2.storage.publish_all_projects().await.unwrap();
        assert!(files_under(&data_dir.join("hot/hot/replay")).is_empty());
        let http = reqwest::Client::new();
        let got: Value = http
            .get(format!(
                "http://{}/v1/projects/{pid}/replay/sess_mid",
                node2.http_addr
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got["chunks"].as_array().map(|a| a.len()), Some(3));
        node2.shutdown().await;
    });

    // Part B: Real `pulse-node` subprocess SIGKILL pre-upload -> restart -> cycle upload via HTTP/admin.
    let bin = env!("CARGO_BIN_EXE_pulse-node");
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("proc_data");
    let s3_dir = tmp.path().join("proc_s3");
    let web_dir = tmp.path().join("no_web");
    std::fs::create_dir_all(&s3_dir).unwrap();
    let s3_url = reqwest::Url::from_directory_path(&s3_dir)
        .unwrap()
        .to_string();

    let free_port = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let pid = "proj_sigkill_rep";
    let port1 = free_port();
    let base1 = format!("http://127.0.0.1:{port1}");

    struct ChildGuard(Option<std::process::Child>);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if let Some(mut c) = self.0.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }

    let c1_stdout = tmp.path().join("c1.stdout.log");
    let c1_stderr = tmp.path().join("c1.stderr.log");
    let mut c1 = ChildGuard(Some(
        std::process::Command::new(bin)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port1.to_string(),
                "--no-quic",
                "--no-seed",
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--web-dir",
                web_dir.to_str().unwrap(),
            ])
            .env("PULSE_ADMIN_TOKEN", ADMIN_TOKEN)
            .env("PULSE_DEFAULT_PROJECT", pid)
            .env("PULSE_OBJECT_STORE_URL", &s3_url)
            .env("PULSE_HOT_STORE", "local")
            .env("PULSE_UPLOAD_INTERVAL_SECS", "300")
            .env("PULSE_WRITER_LEASE_TTL_SECS", "2")
            .stdout(std::process::Stdio::from(
                std::fs::File::create(&c1_stdout).unwrap(),
            ))
            .stderr(std::process::Stdio::from(
                std::fs::File::create(&c1_stderr).unwrap(),
            ))
            .spawn()
            .unwrap(),
    ));

    rt.block_on(async {
        let http = reqwest::Client::new();
        wait_for_healthz(
            &http,
            &base1,
            c1.0.as_mut().unwrap(),
            &c1_stdout,
            &c1_stderr,
            "pulse-node (c1)",
        )
        .await;
        let key_resp: Value = http
            .post(format!("{base1}/v1/projects/{pid}/api-keys"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({"label": "rep", "role": "ingest_and_flags"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ingest_key = key_resp["plaintext_key"].as_str().unwrap().to_string();
        let now_ms = chrono::Utc::now().timestamp_millis();

        for i in 0..2u32 {
            let r = http
                .post(format!("{base1}/v1/replay/chunk"))
                .header("x-pulse-key", &ingest_key)
                .json(&replay_chunk_json(
                    "sess_kill",
                    now_ms + i as i64 * 100,
                    i * 100,
                    &format!("K{i}"),
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 200);
        }
        assert!(files_under(&s3_dir.join("replays")).is_empty());

        // Abrupt SIGKILL before any upload cycle!
        let mut proc1 = c1.0.take().unwrap();
        proc1.kill().unwrap();
        let _ = proc1.wait();

        let port2 = free_port();
        let base2 = format!("http://127.0.0.1:{port2}");
        let c2_stdout = tmp.path().join("c2.stdout.log");
        let c2_stderr = tmp.path().join("c2.stderr.log");
        let mut c2 = ChildGuard(Some(
            std::process::Command::new(bin)
                .args([
                    "--host",
                    "127.0.0.1",
                    "--port",
                    &port2.to_string(),
                    "--no-quic",
                    "--no-seed",
                    "--data-dir",
                    data_dir.to_str().unwrap(),
                    "--web-dir",
                    web_dir.to_str().unwrap(),
                ])
                .env("PULSE_ADMIN_TOKEN", ADMIN_TOKEN)
                .env("PULSE_DEFAULT_PROJECT", pid)
                .env("PULSE_OBJECT_STORE_URL", &s3_url)
                .env("PULSE_HOT_STORE", "local")
                .env("PULSE_UPLOAD_INTERVAL_SECS", "0")
                .env("PULSE_WRITER_LEASE_TTL_SECS", "2")
                .stdout(std::process::Stdio::from(
                    std::fs::File::create(&c2_stdout).unwrap(),
                ))
                .stderr(std::process::Stdio::from(
                    std::fs::File::create(&c2_stderr).unwrap(),
                ))
                .spawn()
                .unwrap(),
        ));
        wait_for_healthz(
            &http,
            &base2,
            c2.0.as_mut().unwrap(),
            &c2_stdout,
            &c2_stderr,
            "pulse-node (c2)",
        )
        .await;
        // Wait for eager 5s maintenance ticker + lease expiry to upload recovered chunks to S3!
        let mut uploaded_ok = false;
        for _ in 0..150 {
            if let Some(status) = c2.0.as_mut().unwrap().try_wait().unwrap() {
                let stdout = std::fs::read_to_string(&c2_stdout).unwrap_or_default();
                let stderr = std::fs::read_to_string(&c2_stderr).unwrap_or_default();
                panic!(
                    "pulse-node (c2) exited unexpectedly with {status} while awaiting replay upload\nstdout:\n{stdout}\nstderr:\n{stderr}"
                );
            }
            if files_under(&s3_dir.join(format!("replays/{pid}/sess_kill"))).len() == 2 {
                uploaded_ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(
            uploaded_ok,
            "restarted pulse-node must upload SIGKILL-recovered local replay chunks"
        );
        let got: Value = http
            .get(format!("{base2}/v1/projects/{pid}/replay/sess_kill"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got["chunks"].as_array().map(|a| a.len()), Some(2));
        drop(c2);
    });
}

#[test]
fn t4_27a_query_node_byok_rotation_reader_pin_and_visibility() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _qtick = EnvGuard::set("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));
    let _kek = EnvGuard::set("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let ingest_dir = tmp.path().join("ingest");
        let query_dir = tmp.path().join("query");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db = EnvGuard::set("DATABASE_URL", Some(&format!("sqlite://{}", meta_db.display())));

        let s3_fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs));
        let pid = "proj_rep_query";

        let ingest = start_pulse_node_with_store(
            node_cfg(&ingest_dir, pid, NodeRole::Ingest),
            s3.clone(),
        )
        .await
        .unwrap();
        let ingest_key = ingest
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let query = start_pulse_node_with_store(
            node_cfg(&query_dir, pid, NodeRole::Query),
            s3.clone(),
        )
        .await
        .unwrap();

        let http = reqwest::Client::new();
        let i_url = format!("http://{}", ingest.http_addr);
        let q_url = format!("http://{}", query.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // 1. Ingest events + publish pub_0 BEFORE any replay exists, so Query caches `warmed_projects`!
        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_rep", "session_id": "sess_q1", "event_name": "View", "client_event_ts_ms": now_ms - 5_000 },
                        { "distinct_id": "u_rep", "session_id": "sess_q1", "event_name": "Checkout", "client_event_ts_ms": now_ms - 3_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest.ingest.flush_all().await.unwrap();
        ingest.storage.flush_all_projects().await.unwrap();
        assert_eq!(ingest.storage.publish_all_projects().await.unwrap(), 1);
        assert_eq!(query.run_query_refresh_tick().await, 1);

        // Query sessions with only_with_replay=true -> 0 initially.
        let pre_sess: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/sessions?only_with_replay=true"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pre_sess["sessions"].as_array().unwrap().len(), 0);

        // Idle Query tick makes 0 PUT/DELETE/GET S3 calls.
        s3.take_calls();
        assert_eq!(query.run_query_refresh_tick().await, 0);
        let idle_q = s3.take_calls();
        assert_eq!(sum_op(&idle_q, "PUT"), 0);
        assert_eq!(sum_op(&idle_q, "DELETE"), 0);
        assert_eq!(sum_op(&idle_q, "GET"), 0);

        // 2. Write replay chunk 0 on Ingest, rotate BYOK key, write replay chunk 1, upload!
        assert_eq!(
            http.post(format!("{i_url}/v1/replay/chunk"))
                .header("x-pulse-key", &ingest_key)
                .json(&replay_chunk_json("sess_q1", now_ms - 5_000, 0, "Key1"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let key2_secret = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let rot = http
            .post(format!("{q_url}/v1/system/encryption/rotate"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({ "secret": key2_secret }))
            .send()
            .await
            .unwrap();
        assert_eq!(rot.status(), 200);
        ingest.control.reload_caches_from_db().await.unwrap();

        assert_eq!(
            http.post(format!("{i_url}/v1/replay/chunk"))
                .header("x-pulse-key", &ingest_key)
                .json(&replay_chunk_json("sess_q1", now_ms - 4_000, 1000, "Key2"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        // Upload only replays (no new PublishedManifest!).
        ingest.storage.publish_all_projects().await.unwrap();
        query.run_query_refresh_tick().await;

        // 3. Query must decrypt both K1 and K2 PLSEAGM2 chunks from S3 `replays/`, discover
        //    `only_with_replay=true`, and show `has_replay: true` in funnel!
        let post_sess: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/sessions?only_with_replay=true"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(post_sess["sessions"].as_array().unwrap().len(), 1);
        assert_eq!(post_sess["sessions"][0]["has_replay"], true);

        let q_rep: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/replay/sess_q1"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q_rep["chunks"].as_array().map(|a| a.len()), Some(2));

        let q_range = http
            .get(format!("{q_url}/v1/projects/{pid}/replay/sess_q1"))
            .bearer_auth(ADMIN_TOKEN)
            .header("Range", "bytes=0-15")
            .send()
            .await
            .unwrap();
        assert_eq!(q_range.status(), 206);

        // 4. Concurrent reader vs upload+delete race on Ingest (with NodeRole::All):
        //    write 20 chunks while a reader continuously fetches binary + JSON.
        let rstore = ingest.storage.replay_store().clone();
        let reader_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_c = reader_stop.clone();
        let rstore_c = rstore.clone();
        let reader_task = tokio::spawn(async move {
            while !stop_c.load(Ordering::Relaxed) {
                let res = rstore_c.read_chunks(pid, "sess_race", 0).await.unwrap();
                let _ = rstore_c.read_bytes_from(pid, "sess_race", 0, None).await.unwrap();
                let _ = res.len();
                tokio::task::yield_now().await;
            }
        });
        for i in 0..10u32 {
            assert_eq!(
                http.post(format!("{i_url}/v1/replay/chunk"))
                    .header("x-pulse-key", &ingest_key)
                    .json(&replay_chunk_json("sess_race", now_ms + i as i64 * 100, i * 100, &format!("R{i}")))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            if i % 3 == 0 {
                ingest.storage.publish_all_projects().await.unwrap();
            }
        }
        ingest.storage.publish_all_projects().await.unwrap();
        ingest.storage.run_gc_all_projects().await.unwrap();
        reader_stop.store(true, Ordering::Relaxed);
        reader_task.await.unwrap();
        assert_eq!(rstore.read_chunks(pid, "sess_race", 0).await.unwrap().len(), 10);

        query.shutdown().await;
        ingest.shutdown().await;
    });
}

#[test]
fn t4_27a_eager_object_mode_disk_503_dpdp_and_restore_cursor_seeding() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // 1. PULSE_UPLOAD_INTERVAL_SECS=0 eager upload + local disk failure -> 503 + DPDP both tiers.
    rt.block_on(async {
        let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
        let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("0"));
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("eager_node");
        let s3_dir = tmp.path().join("eager_s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_eager_dpdp";

        let node = start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Write event + replay for `u_dpdp` / `sess_dpdp`; eager mode uploads replay right away.
        assert_eq!(
            http.post(format!("{base}/v1/ingest"))
                .header("x-pulse-key", &key)
                .json(&json!({
                    "events": [{ "distinct_id": "u_dpdp", "session_id": "sess_dpdp", "event_name": "Click", "client_event_ts_ms": now_ms - 1000 }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.ingest.flush_all().await.unwrap();
        assert_eq!(
            http.post(format!("{base}/v1/replay/chunk"))
                .header("x-pulse-key", &key)
                .json(&replay_chunk_json("sess_dpdp", now_ms - 1000, 0, "Eager"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert!(
            s3_fs
                .head(&ObjectPath::from(format!(
                    "replays/{pid}/sess_dpdp/0000000000.plsr"
                )))
                .await
                .is_ok(),
            "PULSE_UPLOAD_INTERVAL_SECS=0 must eagerly upload replay chunk to S3"
        );

        // Also stage a local-only chunk by temporarily toggling interval, so DPDP purges BOTH tiers!
        node.storage.set_upload_interval(Duration::from_secs(300));
        assert_eq!(
            http.post(format!("{base}/v1/replay/chunk"))
                .header("x-pulse-key", &key)
                .json(&replay_chunk_json("sess_dpdp", now_ms, 500, "HotAndCold"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(files_under(&data_dir.join(format!("hot/hot/replay/{pid}/sess_dpdp"))).len(), 1);

        // DPDP DELETE user -> purges BOTH S3 `replays/` and local `hot/replay/` + `hot/replay_cursors/`!
        let del_resp = http
            .delete(format!("{base}/v1/projects/{pid}/users/u_dpdp"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(del_resp.status(), 200);
        let del_json: Value = del_resp.json().await.unwrap();
        assert_eq!(del_json["replays_purged"], 1);
        assert!(files_under(&s3_dir.join(format!("replays/{pid}/sess_dpdp"))).is_empty());
        assert!(files_under(&data_dir.join(format!("hot/hot/replay/{pid}/sess_dpdp"))).is_empty());
        assert!(files_under(&data_dir.join(format!("hot/hot/replay_cursors/{pid}"))).is_empty());

        // Subsequent upload cycle must not resurrect anything and makes 0 replay PUTs.
        s3.take_calls();
        node.storage.publish_all_projects().await.unwrap();
        let post_del_calls = s3.take_calls();
        assert_eq!(
            post_del_calls
                .get(&("replays".to_string(), "PUT"))
                .copied()
                .unwrap_or(0),
            0
        );

        // Local disk failure on `hot/hot/replay/{pid}` -> HTTP 503 on `/v1/replay/chunk`!
        let replay_proj_root = data_dir.join(format!("hot/hot/replay/{pid}"));
        std::fs::create_dir_all(&replay_proj_root).unwrap();
        struct RestorePerms(PathBuf);
        impl Drop for RestorePerms {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
            }
        }
        std::fs::set_permissions(&replay_proj_root, std::fs::Permissions::from_mode(0o555)).unwrap();
        let _restore = RestorePerms(replay_proj_root.clone());
        let fail_resp = http
            .post(format!("{base}/v1/replay/chunk"))
            .header("x-pulse-key", &key)
            .json(&replay_chunk_json("sess_disk_fail", now_ms, 0, "Fail"))
            .send()
            .await
            .unwrap();
        assert_eq!(fail_resp.status(), 503);
        drop(_restore);

        node.shutdown().await;
    });

    // 2. Mode-switch refusal (`local` with leftover `hot/replay/` -> `object` refused) +
    //    `restore --from-published` cursor seeding!
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("vol_1");
        let restored_dir = tmp.path().join("vol_restored");
        let s3_dir = tmp.path().join("s3_rest");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let meta_db = tmp.path().join("ext_meta.db");
        let _db = EnvGuard::set("DATABASE_URL", Some(&format!("sqlite://{}", meta_db.display())));
        let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
        let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
        let pid = "proj_restore_rep";

        let node = start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3_fs.clone())
            .await
            .unwrap();
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        assert_eq!(
            http.post(format!("{base}/v1/ingest"))
                .header("x-pulse-key", &key)
                .json(&json!({ "events": [{ "distinct_id": "u_r", "event_name": "E", "client_event_ts_ms": now_ms }] }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.ingest.flush_all().await.unwrap();
        node.storage.flush_all_projects().await.unwrap();

        let r0: Value = http
            .post(format!("{base}/v1/replay/chunk"))
            .header("x-pulse-key", &key)
            .json(&replay_chunk_json("sess_rest", now_ms, 0, "R0"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let r0_bytes = r0["chunk_bytes"].as_u64().unwrap();
        node.storage.publish_all_projects().await.unwrap();

        // Write a second local chunk NOT yet uploaded, then verify local->object switch refuses!
        assert_eq!(
            http.post(format!("{base}/v1/replay/chunk"))
                .header("x-pulse-key", &key)
                .json(&replay_chunk_json("sess_rest", now_ms + 100, 100, "R1"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        // Drop node without uploading R1 (use shutdown? No, shutdown uploads! Drop node & wait lease or test with_hot_store directly on a separate dir).
        node.shutdown().await;

        // Verify `with_hot_store` refuses switching `local -> object` when `hot/replay/` has data.
        {
            let switch_tmp = tempfile::tempdir().unwrap();
            let st = with_hot_store(Arc::new(object_store::memory::InMemory::new()), switch_tmp.path())
                .await
                .unwrap();
            st.put(
                &ObjectPath::from("hot/replay/p1/s1/0000000000.plsr"),
                PutPayload::from_static(b"x"),
            )
            .await
            .unwrap();
            let _obj_env = EnvGuard::set("PULSE_HOT_STORE", Some("object"));
            let err = with_hot_store(Arc::new(object_store::memory::InMemory::new()), switch_tmp.path())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("PULSE_HOT_STORE=local"), "{err}");
        }

        // Restore into brand-new `restored_dir`: must seed cursor from S3 `replays/` (seq=2, total_bytes)!
        let meta = MetaStore::connect_auto(&data_dir).await.unwrap();
        let reports = restore_from_published_with_store_and_meta_for_test(
            &restored_dir,
            Some(pid),
            s3_fs.clone(),
            &meta,
        )
        .await
        .unwrap();
        assert_eq!(reports.len(), 1);

        let _lease = EnvGuard::set("PULSE_WRITER_LEASE_TTL_SECS", Some("1"));
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let restored_node =
            start_pulse_node_with_store(node_cfg(&restored_dir, pid, NodeRole::All), s3_fs.clone())
                .await
                .unwrap();
        let cont: Value = http
            .post(format!(
                "http://{}/v1/replay/chunk",
                restored_node.http_addr
            ))
            .header("x-pulse-key", &key)
            .json(&replay_chunk_json("sess_rest", now_ms + 200, 200, "R2_AfterRestore"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            cont["object_path"].as_str(),
            Some(format!("hot/replay/{pid}/sess_rest/0000000002.plsr").as_str()),
            "post-restore append must continue at seq 2 without colliding on S3 seq 0/1"
        );
        assert!(
            cont["byte_offset"].as_u64().unwrap() > r0_bytes,
            "post-restore byte_offset must include pre-restore uploaded S3 chunks"
        );
        restored_node.shutdown().await;
    });
}

#[test]
fn t4_27a_query_node_dpdp_erasure_interleaved_before_refresh_tick() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _qtick = EnvGuard::set("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300"));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let ingest_dir = tmp.path().join("ingest");
        let query_dir = tmp.path().join("query");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db = EnvGuard::set("DATABASE_URL", Some(&format!("sqlite://{}", meta_db.display())));

        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs));
        let pid = "proj_rep_dpdp_interleaved";

        let ingest =
            start_pulse_node_with_store(node_cfg(&ingest_dir, pid, NodeRole::All), s3.clone())
                .await
                .unwrap();
        let ingest_key = ingest
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let query =
            start_pulse_node_with_store(node_cfg(&query_dir, pid, NodeRole::Query), s3.clone())
                .await
                .unwrap();

        let http = reqwest::Client::new();
        let i_url = format!("http://{}", ingest.http_addr);
        let q_url = format!("http://{}", query.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // 1. Ingest `sess_shared` with events from `u_del` and `u_keep` + 1 replay chunk, upload & publish `pub_0`.
        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_del", "session_id": "sess_shared", "event_name": "View", "client_event_ts_ms": now_ms - 5_000 },
                        { "distinct_id": "u_keep", "session_id": "sess_shared", "event_name": "View", "client_event_ts_ms": now_ms - 4_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/replay/chunk"))
                .header("x-pulse-key", &ingest_key)
                .json(&replay_chunk_json("sess_shared", now_ms - 5_000, 0, "Shared"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest.ingest.flush_all().await.unwrap();
        ingest.storage.flush_all_projects().await.unwrap();
        assert_eq!(ingest.storage.publish_all_projects().await.unwrap(), 1);

        // 2. Query refreshes `pub_0` and warms ReplayStore discovery caches via `/sessions`.
        assert_eq!(query.run_query_refresh_tick().await, 1);
        let pre_all: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/sessions"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let pre_arr = pre_all["sessions"].as_array().unwrap();
        assert_eq!(pre_arr.len(), 1);
        assert_eq!(pre_arr[0]["session_id"], "sess_shared");
        assert_eq!(pre_arr[0]["has_replay"], true);

        let pre_only: Value = http
            .get(format!(
                "{q_url}/v1/projects/{pid}/sessions?only_with_replay=true"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pre_only["sessions"].as_array().unwrap().len(), 1);

        // 3. Ingest DPDP-erases `u_del` (purges S3 replay + records `erased_uids`) WITHOUT publishing a new manifest.
        let del_resp = http
            .delete(format!("{i_url}/v1/projects/{pid}/users/u_del"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(del_resp.status(), 200);
        let del_json: Value = del_resp.json().await.unwrap();
        assert_eq!(del_json["replays_purged"], 1);

        // 4. Query receives GET /v1/projects/{pid}/sessions BEFORE `run_query_refresh_tick()`.
        //    Must immediately reflect `has_replay: false` (no stale ReplayStore cache hit) AND must
        //    not suppress ReplayStore invalidation across subsequent `run_query_refresh_tick()`.
        let early_all: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/sessions"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let early_arr = early_all["sessions"].as_array().unwrap();
        assert_eq!(early_arr.len(), 1);
        assert_eq!(early_arr[0]["session_id"], "sess_shared");
        assert_eq!(early_arr[0]["distinct_id"], "u_keep");
        assert_eq!(
            early_arr[0]["has_replay"], false,
            "Query must return has_replay=false immediately before refresh tick: {early_all}"
        );

        // 5. Run `run_query_refresh_tick()` and re-query: must still report `has_replay: false` and 0 replay sessions.
        assert_eq!(query.run_query_refresh_tick().await, 0);
        let post_all: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/sessions"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let post_arr = post_all["sessions"].as_array().unwrap();
        assert_eq!(post_arr.len(), 1);
        assert_eq!(post_arr[0]["session_id"], "sess_shared");
        assert_eq!(post_arr[0]["distinct_id"], "u_keep");
        assert_eq!(
            post_arr[0]["has_replay"], false,
            "Query must return has_replay=false after refresh tick: {post_all}"
        );

        let post_only: Value = http
            .get(format!(
                "{q_url}/v1/projects/{pid}/sessions?only_with_replay=true"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(post_only["sessions"].as_array().unwrap().len(), 0);

        // 6. Subsequent steady-state refresh ticks must make 0 S3 PUT/DELETE/GET/`replays/` calls.
        s3.take_calls();
        assert_eq!(query.run_query_refresh_tick().await, 0);
        assert_eq!(query.run_query_refresh_tick().await, 0);
        let idle_q = s3.take_calls();
        assert_eq!(sum_op(&idle_q, "PUT"), 0, "idle calls: {idle_q:?}");
        assert_eq!(sum_op(&idle_q, "DELETE"), 0, "idle calls: {idle_q:?}");
        assert_eq!(sum_op(&idle_q, "GET"), 0, "idle calls: {idle_q:?}");
        let replay_calls: u64 = idle_q
            .iter()
            .filter(|((prefix, _), _)| prefix == "replays")
            .map(|(_, count)| *count)
            .sum();
        assert_eq!(replay_calls, 0, "idle replay S3 calls: {idle_q:?}");

        query.shutdown().await;
        ingest.shutdown().await;
    });
}

#[test]
fn t4_27a_same_bucket_dpdp_erasure_preserves_survivor_sessions_and_purges_subsequent_replays() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _qtick = EnvGuard::set("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300"));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let ingest_dir = tmp.path().join("ingest");
        let query_dir = tmp.path().join("query");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db = EnvGuard::set("DATABASE_URL", Some(&format!("sqlite://{}", meta_db.display())));

        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs));
        let pid = "proj_rep_same_bucket_dpdp";

        // Force all users into bucket 0 so L1 segment compaction on `erased` triggers for every user.
        let mut icfg = node_cfg(&ingest_dir, pid, NodeRole::All);
        icfg.num_buckets = 1;
        let mut qcfg = node_cfg(&query_dir, pid, NodeRole::Query);
        qcfg.num_buckets = 1;

        let ingest = start_pulse_node_with_store(icfg, s3.clone()).await.unwrap();
        let ingest_key = ingest
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let query = start_pulse_node_with_store(qcfg, s3.clone()).await.unwrap();

        let http = reqwest::Client::new();
        let i_url = format!("http://{}", ingest.http_addr);
        let q_url = format!("http://{}", query.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // 1. Ingest 3 users (`u1`, `u2`, `u3`) colliding in bucket 0, each with events + replay chunk.
        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u1", "session_id": "sess_u1", "event_name": "Step_A", "client_event_ts_ms": now_ms - 6_000 },
                        { "distinct_id": "u2", "session_id": "sess_u2", "event_name": "Step_A", "client_event_ts_ms": now_ms - 5_000 },
                        { "distinct_id": "u3", "session_id": "sess_u3", "event_name": "Step_A", "client_event_ts_ms": now_ms - 4_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        for (sid, label, ts) in [
            ("sess_u1", "Screen_U1", now_ms - 6_000),
            ("sess_u2", "Screen_U2", now_ms - 5_000),
            ("sess_u3", "Screen_U3", now_ms - 4_000),
        ] {
            assert_eq!(
                http.post(format!("{i_url}/v1/replay/chunk"))
                    .header("x-pulse-key", &ingest_key)
                    .json(&replay_chunk_json(sid, ts, 0, label))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
        }

        ingest.ingest.flush_all().await.unwrap();
        ingest.storage.flush_all_projects().await.unwrap();
        assert_eq!(ingest.storage.publish_all_projects().await.unwrap(), 1);

        // Append a second local-hot chunk for `u2` after `pub_0` so `u2` spans both S3 and local hot.
        assert_eq!(
            http.post(format!("{i_url}/v1/replay/chunk"))
                .header("x-pulse-key", &ingest_key)
                .json(&replay_chunk_json("sess_u2", now_ms - 5_000, 150, "Screen_U2_Hot"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert!(
            !files_under(&ingest_dir.join(format!("hot/hot/replay/{pid}/sess_u2"))).is_empty(),
            "u2 must have un-uploaded local hot chunk"
        );
        assert_eq!(query.run_query_refresh_tick().await, 1);

        // 2. Erase `u1` (`DELETE /v1/projects/{pid}/users/u1`) -> `replays_purged == 1`.
        let del1 = http
            .delete(format!("{i_url}/v1/projects/{pid}/users/u1"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(del1.status(), 200);
        let del1_json: Value = del1.json().await.unwrap();
        assert_eq!(del1_json["replays_purged"], 1, "del1: {del1_json}");

        // 3. Verify surviving user `u3` keeps its real `session_id`, `replay_sessions`, and funnel
        //    `has_replay: true` on BOTH `Ingest` and `Query` even though `u1` was compacted out
        //    of the shared bucket-0 L1 segment.
        for (role_label, base_url) in [("ingest", &i_url), ("query", &q_url)] {
            let u3_detail: Value = http
                .get(format!("{base_url}/v1/projects/{pid}/users/u3"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                u3_detail["events"][0]["session_id"], "sess_u3",
                "{role_label} u3 detail event session_id: {u3_detail}"
            );
            assert_eq!(
                u3_detail["replay_sessions"],
                json!(["sess_u3"]),
                "{role_label} u3 replay_sessions: {u3_detail}"
            );

            let funnel: Value = http
                .post(format!("{base_url}/v1/projects/{pid}/query/funnel"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                    "steps": [
                        { "event_name": "Step_A" },
                        { "event_name": "Step_B" }
                    ],
                    "conversion_window_ms": 60_000
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let drops = funnel["dropped_sample_sessions"].as_array().unwrap();
            let u3_drop = drops
                .iter()
                .find(|d| d["distinct_id"] == "u3")
                .unwrap_or_else(|| panic!("{role_label} missing u3 in funnel drops: {funnel}"));
            assert_eq!(
                u3_drop["session_id"], "sess_u3",
                "{role_label} u3 drop: {u3_drop}"
            );
            assert_eq!(
                u3_drop["has_replay"], true,
                "{role_label} u3 drop has_replay: {u3_drop}"
            );
        }

        // 4. Erase `u2` (`DELETE /v1/projects/{pid}/users/u2`) -> must still discover `"sess_u2"`
        //    from the compacted bucket-0 segment, return `replays_purged == 1`, and completely purge
        //    `u2`'s `.plsr` files from BOTH S3 `replays/` and local `hot/replay/`.
        let del2 = http
            .delete(format!("{i_url}/v1/projects/{pid}/users/u2"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(del2.status(), 200);
        let del2_json: Value = del2.json().await.unwrap();
        assert_eq!(del2_json["replays_purged"], 1, "del2: {del2_json}");

        assert!(
            files_under(&s3_dir.join(format!("replays/{pid}/sess_u2"))).is_empty(),
            "u2 S3 .plsr files must be purged: {:?}",
            files_under(&s3_dir.join(format!("replays/{pid}/sess_u2")))
        );
        assert!(
            files_under(&ingest_dir.join(format!("hot/hot/replay/{pid}/sess_u2"))).is_empty(),
            "u2 local hot .plsr files must be purged: {:?}",
            files_under(&ingest_dir.join(format!("hot/hot/replay/{pid}/sess_u2")))
        );
        assert!(
            !files_under(&s3_dir.join(format!("replays/{pid}/sess_u3"))).is_empty(),
            "unrelated survivor u3 replay files must remain in S3"
        );

        // Verify `Query` reflects `u2` erasure immediately and preserves `u3`.
        let q_only: Value = http
            .get(format!(
                "{q_url}/v1/projects/{pid}/sessions?only_with_replay=true"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let q_sids: Vec<&str> = q_only["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["session_id"].as_str().unwrap())
            .collect();
        assert_eq!(q_sids, vec!["sess_u3"], "q_only: {q_only}");

        query.shutdown().await;
        ingest.shutdown().await;
    });
}
