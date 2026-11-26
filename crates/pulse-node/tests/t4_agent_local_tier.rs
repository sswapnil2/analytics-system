//! T4 integration & fault-injection tests for Features #27b (Agent Local Hot Tier &
//! Cycle Upload/Publish) and #27c (Physical Agent Erasure Before Tombstone Expiry):
//!
//! 1. `PULSE_HOT_STORE=local` -> 0 S3 calls during `/v1/agent/traces` JSON+PLST ingest, L0 flush,
//!    local compaction, idle ticks (active project, event-only project, permanent tombstone);
//!    cycle upload to `agent_segments/` + `agent_published/`, local `hot/agent_segments/` cleanup,
//!    `RunningNode::shutdown` flush+upload, and `PULSE_UPLOAD_INTERVAL_SECS=0` eager upload.
//! 2. `NodeRole::Query` reading `agent_published/` on demand (0 boot S3 calls, 0 idle S3 calls,
//!    `GET .../agent/traces`, detail, analytics, sessions, MCP `list_agent_traces`), dynamic
//!    BYOK (`PLSEAGM2`) key rotation, `NotFound` manifest refresh race, and `Query` isolation
//!    from pruning tombstones.
//! 3. Crash recovery & fencing: real `pulse-node` `SIGKILL` pre-upload -> restart recovery;
//!    `AgentUploadIntent` leftover + T2 step-6 self-crash recovery; `StaleWriter` fencing;
//!    local disk failure -> HTTP `503`; mode-switch refusals (`local <-> object` and wiped
//!    `<data_dir>` with `agent_published/`); `restore --from-published` agent + mixed restore.
//! 4. Feature #27c physical erasure before `expires_at_ms` across L0, `.ptwl` WAL, local
//!    `hot/agent_segments/`, and S3 `agent_segments/` + `pending_s3_deletes` at all 3 levels
//!    (`trace`, `session`, `user`), expired-but-unpurged tombstone safety gate, post-expiry
//!    tombstone pruning with zero resurrection, and split-role (`Ingest` + `Query`) erasure.

use std::collections::BTreeMap;
use std::fmt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Bytes;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_agent::model::{AgentErasureTombstone, ErasureLevel, TraceIngestBatch};
use pulse_agent::protocol::encode_plst_batch;
use pulse_agent::store::manifest::{
    AgentUploadIntent, AgentUploadIntentBatch, AgentUploadIntentStore, ManifestStore,
    AGENT_GC_GRACE_MS,
};
use pulse_agent::{PublishedAgentManifestStore, StoreError};
use pulse_control::MetaStore;
use pulse_node::{
    restore_from_published_with_store_and_meta_for_test, start_pulse_node_with_store, NodeConfig,
    NodeRole,
};
use serde_json::{json, Value};

const ADMIN_TOKEN: &str = "sk_root_t4_agent_local_tier_test_token_00000000";
const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST_BYOK_KEY_2: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
const TEST_VAULT_KEK: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

static ENV_LOCK: Mutex<()> = Mutex::new(());

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

#[derive(Debug)]
struct FaultAndCountS3 {
    inner: Arc<dyn ObjectStore>,
    calls: Mutex<BTreeMap<(String, &'static str), u64>>,
    fail_agent_seg_puts_after: Mutex<Option<usize>>,
    agent_seg_put_hits: AtomicUsize,
}

impl FaultAndCountS3 {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            calls: Mutex::new(BTreeMap::new()),
            fail_agent_seg_puts_after: Mutex::new(None),
            agent_seg_put_hits: AtomicUsize::new(0),
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

    fn fail_agent_seg_puts_after(&self, limit: Option<usize>) {
        self.agent_seg_put_hits.store(0, Ordering::SeqCst);
        *self.fail_agent_seg_puts_after.lock().unwrap() = limit;
    }

    fn maybe_fail_agent_seg_put(&self, l: &ObjectPath) -> OsResult<()> {
        if l.as_ref().starts_with("agent_segments/") {
            if let Some(limit) = *self.fail_agent_seg_puts_after.lock().unwrap() {
                let hit = self.agent_seg_put_hits.fetch_add(1, Ordering::SeqCst);
                if hit >= limit {
                    return Err(object_store::Error::Generic {
                        store: "FaultAndCountS3",
                        source: Box::new(std::io::Error::other(
                            "injected S3 agent_segments PUT failure",
                        )),
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
        self.maybe_fail_agent_seg_put(l)?;
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.count("PUT", Some(l));
        self.maybe_fail_agent_seg_put(l)?;
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
        self.maybe_fail_agent_seg_put(to)?;
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.count("PUT", Some(to));
        self.maybe_fail_agent_seg_put(to)?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn sum_all_calls(calls: &BTreeMap<(String, &'static str), u64>) -> u64 {
    calls.values().sum()
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

fn trace_json(
    trace_id: &str,
    session_id: &str,
    user_id: &str,
    ts_ms: i64,
    prompt_tag: &str,
) -> Value {
    json!({
        "traces": [{
            "trace_id": trace_id,
            "session_id": session_id,
            "user_id": user_id,
            "agent_name": "planner",
            "agent_version": "v2",
            "environment": "production",
            "status": "ok",
            "start_ts_ms": ts_ms,
            "duration_ms": 42,
            "input": {"prompt": prompt_tag},
            "output": {"answer": format!("ans_{trace_id}")},
            "metadata": {},
            "tags": ["t4"]
        }],
        "spans": [{
            "span_id": format!("sp_{trace_id}"),
            "trace_id": trace_id,
            "name": "llm_step",
            "span_type": "llm",
            "status": "ok",
            "start_ts_ms": ts_ms,
            "duration_ms": 42,
            "provider": "openai",
            "model": "gpt-4o",
            "input_tokens": 12,
            "output_tokens": 18,
            "total_tokens": 30
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

async fn wait_for_healthz(
    http: &reqwest::Client,
    base: &str,
    child: &mut std::process::Child,
    stdout_path: &Path,
    stderr_path: &Path,
    label: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
            let stderr = std::fs::read_to_string(stderr_path).unwrap_or_default();
            panic!(
                "{label} exited prematurely with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
        if let Ok(r) = http.get(format!("{base}/healthz")).send().await {
            if r.status().is_success() {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{label} timed out waiting for /healthz"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn t4_27b_agent_local_tier_cycle_upload_zero_idle_byok_query_shutdown_restore_and_refusals() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300")),
        ("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("DATABASE_URL", None),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let writer_dir = tmp.path().join("writer");
        let query_dir = tmp.path().join("query");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db = EnvGuard::set(&[(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        )]);

        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_ag_loc";

        let node = start_pulse_node_with_store(node_cfg(&writer_dir, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        let ingest_key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Create a second event-only project + permanent tombstone so idle loops exercise both.
        let ev_proj: Value = http
            .post(format!("{base}/v1/projects"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({"name": "EventOnly"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ev_pid = ev_proj["project"]["id"].as_str().unwrap().to_string();
        let ev_key = ev_proj["api_key"]["plaintext_key"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            http.post(format!("{base}/v1/ingest"))
                .header("x-pulse-key", &ev_key)
                .json(&json!({
                    "events": [{"event": "EvOnly", "userId": "u_ev", "timestamp": now_ms}]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.control
            .meta()
            .insert_erasure_tombstone(&AgentErasureTombstone {
                id: "tb_perm_ev".to_string(),
                project_id: ev_pid.clone(),
                level: ErasureLevel::Trace,
                target_id: "tr_never_existed".to_string(),
                created_at: now_ms,
                expires_at: None,
            })
            .await
            .unwrap();
        let _ = node.control.sweep_pending_agent_erasures().await;

        s3.take_calls();

        // 1. JSON + PLST ingest + L0 flush + local compact + idle must make ZERO S3 calls!
        assert_eq!(
            http.post(format!("{base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_k1_json", "sess_ag_1", "u_ag_1", now_ms, "hello_k1"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let batch_plst: TraceIngestBatch = serde_json::from_value(trace_json(
            "tr_k1_plst",
            "sess_ag_1",
            "u_ag_1",
            now_ms + 50,
            "hello_plst",
        ))
        .unwrap();
        let plst_bytes = encode_plst_batch(&batch_plst, true).unwrap();
        assert_eq!(
            http.post(format!("{base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .header("content-type", "application/x-pulse-plst")
                .body(plst_bytes)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{base}/v1/admin/flush"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let calls_ingest_flush = s3.take_calls();
        assert_eq!(
            sum_all_calls(&calls_ingest_flush),
            0,
            "ingest + flush in local mode must make 0 S3 calls: {calls_ingest_flush:?}"
        );

        let local_seg_files = files_under(&writer_dir.join("hot/hot/agent_segments"));
        assert!(
            !local_seg_files.is_empty(),
            "local L0 flush must write encrypted segments under hot/hot/agent_segments/"
        );
        assert!(
            !files_under(&writer_dir.join("hot/agent_manifests")).is_empty(),
            "local L0 flush must write local manifest under hot/agent_manifests/"
        );
        let s3_agent_objs: Vec<_> = s3_fs
            .list(Some(&ObjectPath::from("agent_segments")))
            .try_collect()
            .await
            .unwrap();
        assert!(s3_agent_objs.is_empty(), "S3 agent_segments must be empty pre-upload");

        // Idle sweeps (compact, publish before upload_interval, erasure sweep, prune) -> 0 S3 calls.
        node.control.agent_store().compact_and_gc(None, &[]).await.unwrap();
        let _ = node.control.sweep_pending_agent_erasures().await;
        let _ = node.control.prune_expired_agent_data().await;
        let idle_calls = s3.take_calls();
        assert_eq!(
            sum_all_calls(&idle_calls),
            0,
            "idle compact/erasure/prune in local mode must make 0 S3 calls: {idle_calls:?}"
        );

        // 2. Cycle upload/publish -> S3 agent_segments/ + agent_published/, local hot segments deleted.
        let uploaded_cnt = node.control.agent_store().publish_all_projects().await.unwrap();
        assert_eq!(uploaded_cnt, 1);
        let s3_seg_objs: Vec<_> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_segments/{pid}"))))
            .try_collect()
            .await
            .unwrap();
        assert!(!s3_seg_objs.is_empty());
        // Verify BYOK encryption on S3 segment (`PLSEAGM2` header).
        let raw_s3_seg = s3_fs.get(&s3_seg_objs[0].location).await.unwrap().bytes().await.unwrap();
        assert!(raw_s3_seg.starts_with(b"PLSEAGM2"));
        assert!(
            files_under(&writer_dir.join(format!("hot/hot/agent_segments/{pid}"))).is_empty(),
            "local hot segments must be cleaned up after S3 publish"
        );
        let s3_pub_objs: Vec<_> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_published/{pid}"))))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(s3_pub_objs.len(), 1);

        // Clean publish after upload -> 0 S3 calls.
        s3.take_calls();
        assert_eq!(node.control.agent_store().publish_all_projects().await.unwrap(), 0);
        assert_eq!(sum_all_calls(&s3.take_calls()), 0);

        // 3. NodeRole::Query: 0 S3 calls at boot, reads agent_published/ on demand, BYOK rotation, MCP, 0 idle calls.
        s3.take_calls();
        let query = start_pulse_node_with_store(node_cfg(&query_dir, pid, NodeRole::Query), s3.clone())
            .await
            .unwrap();
        let q_url = format!("http://{}", query.http_addr);
        // Boot should not scan agent_published/ across projects eagerly.
        let q_boot_calls = s3.take_calls();
        assert_eq!(
            q_boot_calls
                .iter()
                .filter(|((p, _), _)| p == "agent_published" || p == "agent_segments")
                .map(|(_, n)| *n)
                .sum::<u64>(),
            0,
            "NodeRole::Query boot must make 0 agent S3 calls: {q_boot_calls:?}"
        );

        let q_list: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q_list["traces"].as_array().map(|a| a.len()), Some(2));

        let q_detail: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/traces/tr_k1_json"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q_detail["trace"]["trace_id"], "tr_k1_json");

        let q_ana: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/analytics"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q_ana["total_executions"], 2);

        let q_sess: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/sessions"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q_sess["sessions"].as_array().map(|a| a.len()), Some(1));

        let mcp_resp: Value = http
            .post(format!("{q_url}/v1/mcp"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": { "name": "list_agent_traces", "arguments": { "limit": 10 } }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            mcp_resp["result"].to_string().contains("tr_k1_json"),
            "MCP list_agent_traces must return uploaded trace: {mcp_resp}"
        );

        // Query idle refresh tick -> 0 agent_segments S3 calls (and Query cannot prune tombstones).
        s3.take_calls();
        query.run_query_refresh_tick().await;
        let q_idle_calls = s3.take_calls();
        assert_eq!(
            q_idle_calls
                .iter()
                .filter(|((p, _), _)| p == "agent_segments")
                .map(|(_, n)| *n)
                .sum::<u64>(),
            0
        );

        // Rotate BYOK key on Query, reload caches on writer, ingest tr_k2, shutdown -> auto flushes & publishes!
        assert_eq!(
            http.post(format!("{q_url}/v1/system/encryption/rotate"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({"secret": TEST_BYOK_KEY_2}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.control.reload_caches_from_db().await.unwrap();
        assert_eq!(
            http.post(format!("{base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_k2_shutdown", "sess_ag_2", "u_ag_2", now_ms + 200, "hello_k2"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        // Shutdown must flush L0 & upload local segments to S3 `agent_published/` (`pub_000000000001`).
        node.shutdown().await;
        let s3_pub_after_sd: Vec<_> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_published/{pid}"))))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(s3_pub_after_sd.len(), 2);

        // Query reads both K1 and K2 traces across key rotation!
        let q_list2: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q_list2["traces"].as_array().map(|a| a.len()), Some(3));

        // Mode-switch refusals (`object` over `local` agent state, and wiped `<data_dir>` with `agent_published/`).
        let wiped_dir = tmp.path().join("wiped");
        {
            let _obj = EnvGuard::set(&[("PULSE_HOT_STORE", Some("object"))]);
            let err_obj =
                start_pulse_node_with_store(node_cfg(&writer_dir, pid, NodeRole::All), s3.clone())
                    .await
                    .err()
                    .expect("starting object mode over local agent_manifests/ must fail");
            assert!(err_obj.to_string().contains("local"), "{err_obj}");
        }
        {
            let err_wiped =
                start_pulse_node_with_store(node_cfg(&wiped_dir, pid, NodeRole::All), s3.clone())
                    .await
                    .err()
                    .expect("wiped data_dir with agent_published/ in S3 must refuse startup");
            assert!(
                err_wiped.to_string().contains("restore --from-published"),
                "{err_wiped}"
            );
        }

        // 5. `restore --from-published` restores both event & agent state across the bucket & bumps writer_generation.
        let ext_meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let reports = restore_from_published_with_store_and_meta_for_test(
            &wiped_dir,
            None,
            s3_fs.clone(),
            &ext_meta,
        )
        .await
        .unwrap();
        assert!(
            reports.iter().any(|r| r.project_id == pid),
            "restore must report restored project {pid}: {reports:?}"
        );
        assert!(
            reports.iter().any(|r| r.project_id == ev_pid),
            "restore must report restored event project {ev_pid}: {reports:?}"
        );
        assert!(
            !files_under(&wiped_dir.join(format!("hot/agent_manifests/{pid}"))).is_empty(),
            "restore must install local agent manifest for {pid}"
        );
        // Shutdown query before launching restored writer on same meta_db.
        query.shutdown().await;
        drop(ext_meta);

        // Eager upload (`PULSE_UPLOAD_INTERVAL_SECS=0`) on restored node: ingest + flush immediately publishes S3!
        let _eager = EnvGuard::set(&[("PULSE_UPLOAD_INTERVAL_SECS", Some("0"))]);
        let restored = start_pulse_node_with_store(
            node_cfg(&wiped_dir, pid, NodeRole::All),
            s3.clone(),
        )
        .await
        .unwrap();
        let r_base = format!("http://{}", restored.http_addr);
        assert_eq!(
            http.post(format!("{r_base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_restored_eager", "sess_ag_3", "u_ag_3", now_ms + 300, "eager"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{r_base}/v1/admin/flush"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let pub_after_restore: Vec<_> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_published/{pid}"))))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            pub_after_restore.len(),
            3,
            "PULSE_UPLOAD_INTERVAL_SECS=0 flush after restore must publish pub_000000000002 immediately"
        );
        let restored_list: Value = http
            .get(format!("{r_base}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            restored_list["traces"].as_array().map(|a| a.len()),
            Some(4),
            "restored node must read pre-restore K1+K2 traces + new trace: {restored_list}"
        );

        // 7. Local WAL disk failure -> HTTP 503 on `/v1/agent/traces`.
        let wal_proj_dir = wiped_dir.join(format!("wal/agent_wal/{pid}"));
        std::fs::create_dir_all(&wal_proj_dir).unwrap();
        let mut locked_dirs = vec![(
            wal_proj_dir.clone(),
            std::fs::metadata(&wal_proj_dir).unwrap().permissions(),
        )];
        for entry in std::fs::read_dir(&wal_proj_dir).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                locked_dirs.push((p.clone(), std::fs::metadata(&p).unwrap().permissions()));
            }
        }
        for (p, _) in locked_dirs.iter().rev() {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o555)).unwrap();
        }
        let err_status = http
            .post(format!("{r_base}/v1/agent/traces"))
            .header("x-pulse-key", &ingest_key)
            .json(&trace_json("tr_disk_fail", "sess_fail", "u_fail", now_ms + 400, "fail"))
            .send()
            .await
            .unwrap()
            .status();
        for (p, perms) in locked_dirs {
            std::fs::set_permissions(p, perms).unwrap();
        }
        assert_eq!(
            err_status.as_u16(),
            503,
            "read-only/unwritable local agent_wal dir must return 503, got {err_status}"
        );

        restored.shutdown().await;
    });
}

#[test]
fn t4_27b_sigkill_crash_recovery_mid_upload_intent_step6_and_fencing() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("2")),
        ("DATABASE_URL", None),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Part A: Real `pulse-node` subprocess SIGKILL pre-upload -> restart -> WAL replay & publish.
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
    let pid = "proj_sigkill_ag";
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
            .stdout(Stdio::from(std::fs::File::create(&c1_stdout).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(&c1_stderr).unwrap()))
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
            .json(&json!({"label": "ag", "role": "ingest_and_flags"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ingest_key = key_resp["plaintext_key"].as_str().unwrap().to_string();
        let now_ms = chrono::Utc::now().timestamp_millis();
        assert_eq!(
            http.post(format!("{base1}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json(
                    "tr_sigkill_1",
                    "sess_sk",
                    "u_sk",
                    now_ms,
                    "sigkill_msg"
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        // Abrupt SIGKILL before flush or upload!
        c1.0.as_mut().unwrap().kill().unwrap();
        c1.0.take().unwrap().wait().unwrap();
        tokio::time::sleep(Duration::from_millis(2500)).await;

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
                .env("PULSE_UPLOAD_INTERVAL_SECS", "1")
                .env("PULSE_WRITER_LEASE_TTL_SECS", "2")
                .stdout(Stdio::from(std::fs::File::create(&c2_stdout).unwrap()))
                .stderr(Stdio::from(std::fs::File::create(&c2_stderr).unwrap()))
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
        let recovered: Value = http
            .get(format!("{base2}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(recovered["traces"].as_array().map(|a| a.len()), Some(1));
        assert_eq!(recovered["traces"][0]["trace_id"], "tr_sigkill_1");

        // Flush + wait up to 5s for background upload cycle (`PULSE_UPLOAD_INTERVAL_SECS=1`) to publish S3!
        assert_eq!(
            http.post(format!("{base2}/v1/admin/flush"))
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
        while tokio::time::Instant::now() < deadline {
            if !files_under(&s3_dir.join(format!("agent_published/{pid}"))).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        assert!(
            !files_under(&s3_dir.join(format!("agent_published/{pid}"))).is_empty(),
            "restarted pulse-node must upload SIGKILL-recovered agent trace to S3"
        );
        drop(c2);
    });

    // Part B: Mid-upload S3 failure recovery + UploadIntent orphan cleanup + StaleWriter fencing.
    rt.block_on(async {
        let tmp2 = tempfile::tempdir().unwrap();
        let data = tmp2.path().join("data");
        let s3_dir2 = tmp2.path().join("s3");
        std::fs::create_dir_all(&s3_dir2).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir2).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_mid_crash";

        let node = start_pulse_node_with_store(node_cfg(&data, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        let ingest_key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        assert_eq!(
            http.post(format!("{base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_mid_1", "sess_mid", "u_mid", now_ms, "mid"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.control.agent_store().flush_all_l0().await.unwrap();

        // Inject S3 segment PUT failure mid-upload -> ingest remains 200 OK, upload returns Err, retry succeeds!
        s3.fail_agent_seg_puts_after(Some(0));
        assert!(node
            .control
            .agent_store()
            .publish_all_projects()
            .await
            .is_err());
        s3.fail_agent_seg_puts_after(None);
        assert_eq!(
            node.control
                .agent_store()
                .publish_all_projects()
                .await
                .unwrap(),
            1
        );

        // Leave a dangling AgentUploadIntent for a non-existent epoch/target -> next open cleans orphan intent.
        node.shutdown().await;
        let routed = pulse_storage::with_hot_store(s3_fs.clone(), &data)
            .await
            .unwrap();
        let intent_store = AgentUploadIntentStore::new(pid, routed.clone()).unwrap();
        intent_store
            .save(&AgentUploadIntent {
                intent_id: "orphan_intent".to_string(),
                writer_generation: 0,
                base_local_epoch: 1,
                target_publish_seq: 99,
                created_at_ms: now_ms,
                batches: vec![AgentUploadIntentBatch {
                    date: "2026-01-01".to_string(),
                    shard_id: 0,
                    source_segment_ids: vec![],
                    source_object_paths: vec![],
                    target_segment_prefix: "seg_orphan_up".to_string(),
                }],
            })
            .await
            .unwrap();

        let node2 = start_pulse_node_with_store(node_cfg(&data, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        assert!(
            intent_store.load().await.unwrap().is_none(),
            "startup reconciliation must clear dangling AgentUploadIntent"
        );
        node2.shutdown().await;

        // Remote writer bumps writer_generation in agent_published/ -> reopening local writer fails cleanly.
        let pub_store = PublishedAgentManifestStore::new(pid, routed.clone()).unwrap();
        let mut latest = pub_store.load_latest().await.unwrap().unwrap();
        latest.publish_seq += 1;
        latest.writer_generation += 1;
        pub_store.publish_next(latest).await.unwrap();
        let stale_res =
            start_pulse_node_with_store(node_cfg(&data, pid, NodeRole::All), s3.clone()).await;
        assert!(
            stale_res.is_err(),
            "stale writer generation must fail startup"
        );
        let _ = (
            ManifestStore::new(pid, routed, AGENT_GC_GRACE_MS),
            StoreError::Shutdown,
        );
    });
}

#[test]
fn t4_27c_physical_agent_erasure_before_ttl_expiry_and_split_role() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300")),
        ("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("DATABASE_URL", None),
    ]);

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
        let _db = EnvGuard::set(&[(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        )]);

        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_erase_27c";

        let ingest =
            start_pulse_node_with_store(node_cfg(&ingest_dir, pid, NodeRole::Ingest), s3.clone())
                .await
                .unwrap();
        let query =
            start_pulse_node_with_store(node_cfg(&query_dir, pid, NodeRole::Query), s3.clone())
                .await
                .unwrap();
        let ingest_key = ingest
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let i_url = format!("http://{}", ingest.http_addr);
        let q_url = format!("http://{}", query.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Enable `capture_user_id` so User-level erasure matches `user_id` on ingested traces.
        assert_eq!(
            http.put(format!("{q_url}/v1/projects/{pid}/agent/privacy"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({ "capture_user_id": true }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest.control.reload_caches_from_db().await.unwrap();

        // Layer 1 (S3 published): ingest trace A (trace erase), B (session erase), C (user erase), D (survives) -> publish to S3!
        for (tid, sid, uid, tag) in [
            ("tr_e_trace_s3", "sess_keep_a", "u_keep_a", "TAG_ERASE_TRACE_S3"),
            ("tr_e_sess_s3", "sess_erase_target", "u_keep_b", "TAG_ERASE_SESS_S3"),
            ("tr_e_user_s3", "sess_keep_c", "u_erase_target", "TAG_ERASE_USER_S3"),
            ("tr_survive_1", "sess_survive", "u_survive", "TAG_SURVIVE_1"),
        ] {
            assert_eq!(
                http.post(format!("{i_url}/v1/agent/traces"))
                    .header("x-pulse-key", &ingest_key)
                    .json(&trace_json(tid, sid, uid, now_ms, tag))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
        }
        ingest.control.agent_store().flush_all_l0().await.unwrap();
        assert_eq!(ingest.control.agent_store().publish_all_projects().await.unwrap(), 1);
        let old_s3_paths: Vec<ObjectPath> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_segments/{pid}"))))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location)
            .collect();
        assert!(!old_s3_paths.is_empty());

        // Layer 2 (Local hot/agent_segments/): flush E into local segment (NOT yet uploaded to S3).
        assert_eq!(
            http.post(format!("{i_url}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json(
                    "tr_e_local",
                    "sess_erase_target",
                    "u_keep_x",
                    now_ms + 10,
                    "TAG_ERASE_LOCAL",
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest.control.agent_store().flush_all_l0().await.unwrap();

        // Layer 3 (L0 + active .ptwl WAL): ingest F (trace in erased user) & G (surviving) without flushing.
        assert_eq!(
            http.post(format!("{i_url}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json(
                    "tr_e_l0_wal",
                    "sess_keep_y",
                    "u_erase_target",
                    now_ms + 20,
                    "TAG_ERASE_WAL",
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json(
                    "tr_survive_2",
                    "sess_survive",
                    "u_survive",
                    now_ms + 30,
                    "TAG_SURVIVE_2",
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        // Pre-warm Query cache on S3 segment so we also test cache invalidation & query update.
        let pre_q: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pre_q["traces"].as_array().map(|a| a.len()), Some(4));

        // Issue 3-level erasure tombstones via Query/Admin node with SHORT expires_at = now + 6_000ms (< 15m default grace & 300s upload!).
        let short_expires_ms = chrono::Utc::now().timestamp_millis() + 6_000;
        for (level, target) in [
            ("trace", "tr_e_trace_s3"),
            ("session", "sess_erase_target"),
            ("user", "u_erase_target"),
        ] {
            let er = http
                .post(format!("{q_url}/v1/projects/{pid}/agent/erasure"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "level": level,
                    "target_id": target,
                    "expires_at": short_expires_ms
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(er.status(), 200);
        }

        // Sweep unpurged erasures on Ingest BEFORE expires_at passes!
        // Note: urgent pre-expiry grace schedules S3 old segment deletion before short_expires_ms.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(5_800);
        loop {
            let _ = ingest.control.sweep_pending_agent_erasures().await;
            let unpurged = ingest
                .control
                .meta()
                .list_unpurged_erasure_tombstones()
                .await
                .unwrap();
            if unpurged.is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("erasure tombstones not physically purged before short expires_at: {unpurged:?}");
            }
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
        assert!(
            chrono::Utc::now().timestamp_millis() <= short_expires_ms + 500,
            "physical purge + S3 old segment delete must finish before/by tombstone expiry"
        );

        // Superseded S3 segment(s) containing erased traces must already be deleted from S3!
        let mut byok_bytes = [0u8; 32];
        for (idx, b) in byok_bytes.iter_mut().enumerate() {
            *b = u8::from_str_radix(&TEST_BYOK_KEY[idx * 2..idx * 2 + 2], 16).unwrap();
        }
        let enc_probe = Arc::new(pulse_storage::EncryptedObjectStore::new(s3_fs.clone()));
        enc_probe.set_master_key(Some(byok_bytes));
        let latest_pub = PublishedAgentManifestStore::new(pid, enc_probe.as_object_store())
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .unwrap();
        let active_s3_set: std::collections::HashSet<String> = latest_pub
            .segments
            .iter()
            .map(|s| s.object_path.clone())
            .collect();
        let superseded_paths: Vec<&ObjectPath> = old_s3_paths
            .iter()
            .filter(|p| !active_s3_set.contains(p.as_ref()))
            .collect();
        assert!(
            !superseded_paths.is_empty(),
            "expected superseded S3 segments after erasure: old={old_s3_paths:?} active={active_s3_set:?}"
        );
        for p in superseded_paths {
            assert!(
                s3_fs.head(p).await.is_err(),
                "old S3 segment {p} containing erased traces must be deleted before tombstone expires"
            );
        }
        let current_s3_files: Vec<String> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_segments/{pid}"))))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        for cur in &current_s3_files {
            assert!(
                active_s3_set.contains(cur),
                "unexpected un-deleted S3 segment {cur} outside active manifest {active_s3_set:?}"
            );
        }

        // Now wait until well PAST short_expires_ms, then run prune_expired_agent_data().
        while chrono::Utc::now().timestamp_millis() <= short_expires_ms + 150 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Query role MUST NOT prune tombstones or segments.
        let q_prune = query.control.prune_expired_agent_data().await;
        assert_eq!(q_prune.tombstones_pruned, 0);

        // Ingest role prunes the already-purged expired tombstones.
        let i_prune = ingest.control.prune_expired_agent_data().await;
        assert!(
            i_prune.tombstones_pruned >= 3,
            "expected >= 3 expired purged tombstones pruned: {i_prune:?}"
        );
        assert!(ingest
            .control
            .meta()
            .list_active_erasure_tombstones(pid, chrono::Utc::now().timestamp_millis())
            .await
            .unwrap()
            .is_empty());

        // With tombstones table COMPLETELY EMPTY, Query MUST see ONLY the 2 surviving traces (0 erased traces)!
        let post_q: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ids: Vec<&str> = post_q["traces"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["trace_id"].as_str())
            .collect();
        assert_eq!(ids.len(), 2, "must contain only tr_survive_1 & tr_survive_2: {ids:?}");
        assert!(ids.contains(&"tr_survive_1"));
        assert!(ids.contains(&"tr_survive_2"));

        // Safety gate: an EXPIRED-but-UNPURGED tombstone (`physically_purged_at_ms IS NULL`) MUST STILL
        // block reads & MUST NOT be pruned until physical purge completes!
        let past_ms = chrono::Utc::now().timestamp_millis() - 5_000;
        let safety_tb = AgentErasureTombstone {
            id: "tb_unpurged_expired".to_string(),
            project_id: pid.to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_survive_2".to_string(),
            created_at: past_ms - 1_000,
            expires_at: Some(past_ms),
        };
        ingest
            .control
            .meta()
            .insert_erasure_tombstone(&safety_tb)
            .await
            .unwrap();
        let active_now = ingest
            .control
            .meta()
            .list_active_erasure_tombstones(pid, chrono::Utc::now().timestamp_millis())
            .await
            .unwrap();
        assert!(
            active_now
                .iter()
                .any(|tb| tb.id == "tb_unpurged_expired" && tb.expires_at.is_none()),
            "expired-but-unpurged tombstone must still be active with expires_at=None: {active_now:?}"
        );
        // Directly test SQL prune refusal while unpurged.
        let sql_pruned = ingest
            .control
            .meta()
            .prune_expired_erasure_tombstones(Some(pid), chrono::Utc::now().timestamp_millis())
            .await
            .unwrap();
        assert_eq!(sql_pruned, 0, "must never prune unpurged tombstone");
        // Querying tr_survive_2 while unpurged must return 404!
        let hidden_resp = http
            .get(format!("{q_url}/v1/projects/{pid}/agent/traces/tr_survive_2"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(hidden_resp.status(), 404);

        let _ = AtomicBool::new(false);
        query.shutdown().await;
        ingest.shutdown().await;
    });
}

#[test]
fn t4_27bc_fenced_ingest_503_post_purge_pre_expiry_reingest_restart_and_restore_s3_cleanup() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300")),
        ("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY)),
        ("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK)),
        ("DATABASE_URL", None),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Part 1: Live fenced writer rejects `/v1/agent/traces` & `/v1/otel/v1/traces` with HTTP 503.
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("data");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_fence_503";

        let node = start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        let ingest_key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        assert_eq!(
            http.post(format!("{base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_f_0", "sess_f", "u_f", now_ms, "f0"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.control.agent_store().flush_all_l0().await.unwrap();
        assert_eq!(node.control.agent_store().publish_all_projects().await.unwrap(), 1);

        // Simulate a newer writer taking over `pub_000000000001` in S3.
        let routed = pulse_storage::with_hot_store_for_role(s3_fs.clone(), &data_dir, false)
            .await
            .unwrap();
        let enc = Arc::new(pulse_storage::EncryptedObjectStore::new(routed));
        let mut byok_bytes = [0u8; 32];
        for (idx, b) in byok_bytes.iter_mut().enumerate() {
            *b = u8::from_str_radix(&TEST_BYOK_KEY[idx * 2..idx * 2 + 2], 16).unwrap();
        }
        enc.set_master_key(Some(byok_bytes));
        let pub_store = PublishedAgentManifestStore::new(pid, enc.as_object_store()).unwrap();
        let mut hijacked = pub_store.load_latest().await.unwrap().unwrap();
        hijacked.publish_seq += 1;
        hijacked.writer_generation += 1;
        pub_store.publish_next(hijacked).await.unwrap();

        // Trigger publish on live node -> fences writer.
        assert_eq!(
            http.post(format!("{base}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_f_1", "sess_f", "u_f", now_ms + 10, "f1"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.control.agent_store().flush_all_l0().await.unwrap();
        let fence_err = node.control.agent_store().publish_all_projects().await.unwrap_err();
        assert!(matches!(fence_err, StoreError::NotWriter { .. }), "{fence_err:?}");

        // Live fenced writer must immediately return HTTP 503 on both `/v1/agent/traces` and `/v1/otel/v1/traces`!
        let status_json = http
            .post(format!("{base}/v1/agent/traces"))
            .header("x-pulse-key", &ingest_key)
            .json(&trace_json("tr_f_2", "sess_f", "u_f", now_ms + 20, "f2"))
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(
            status_json.as_u16(),
            503,
            "fenced live writer must return 503 on /v1/agent/traces"
        );

        let otel_payload = json!({
            "resourceSpans": [{
                "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "ag" } }] },
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "0102030405060708090a0b0c0d0e0f22",
                        "spanId": "1112131415161722",
                        "name": "llm_step",
                        "startTimeUnixNano": ((now_ms as u64 + 30) * 1_000_000).to_string(),
                        "endTimeUnixNano": ((now_ms as u64 + 40) * 1_000_000).to_string(),
                        "status": { "code": 1 }
                    }]
                }]
            }]
        });
        let status_otel = http
            .post(format!("{base}/v1/otel/v1/traces"))
            .header("x-pulse-key", &ingest_key)
            .json(&otel_payload)
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(
            status_otel.as_u16(),
            503,
            "fenced live writer must return 503 on /v1/otel/v1/traces"
        );

        node.shutdown().await;
    });

    // Part 2: Post-purge pre-expiry re-ingest resurrection & restart survival (`4d-i` & `4d-ii`).
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let ingest_dir = tmp.path().join("ingest");
        let query_dir = tmp.path().join("query");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db = EnvGuard::set(&[(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        )]);
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_4d_reingest";

        let ingest =
            start_pulse_node_with_store(node_cfg(&ingest_dir, pid, NodeRole::Ingest), s3.clone())
                .await
                .unwrap();
        let query =
            start_pulse_node_with_store(node_cfg(&query_dir, pid, NodeRole::Query), s3.clone())
                .await
                .unwrap();
        let ingest_key = ingest
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let i_url = format!("http://{}", ingest.http_addr);
        let q_url = format!("http://{}", query.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        assert_eq!(
            http.post(format!("{i_url}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json("tr_4d_init", "sess_4d", "u_4d", now_ms, "init"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest.control.agent_store().flush_all_l0().await.unwrap();
        assert_eq!(
            ingest
                .control
                .agent_store()
                .publish_all_projects()
                .await
                .unwrap(),
            1
        );

        let exp_ms = chrono::Utc::now().timestamp_millis() + 3_000;
        assert_eq!(
            http.post(format!("{q_url}/v1/projects/{pid}/agent/erasure"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "level": "session",
                    "target_id": "sess_4d",
                    "expires_at": exp_ms
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let deadline = tokio::time::Instant::now() + Duration::from_millis(2_500);
        loop {
            let _ = ingest.control.sweep_pending_agent_erasures().await;
            if ingest
                .control
                .meta()
                .list_unpurged_erasure_tombstones()
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "initial purge timed out"
            );
            tokio::time::sleep(Duration::from_millis(80)).await;
        }

        // Unrelated trace ingest must NOT reopen `physically_purged_at_ms`.
        assert_eq!(
            http.post(format!("{i_url}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json(
                    "tr_4d_unrel",
                    "sess_other",
                    "u_other",
                    now_ms + 50,
                    "keep"
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert!(
            ingest
                .control
                .meta()
                .list_unpurged_erasure_tombstones()
                .await
                .unwrap()
                .is_empty(),
            "unrelated trace ingest must not reopen purged finite tombstone"
        );

        // Pre-expiry re-ingest matching `sess_4d` MUST reopen `physically_purged_at_ms = NULL`.
        assert_eq!(
            http.post(format!("{i_url}/v1/agent/traces"))
                .header("x-pulse-key", &ingest_key)
                .json(&trace_json(
                    "tr_reingest_late",
                    "sess_4d",
                    "u_4d",
                    now_ms + 100,
                    "late"
                ))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert!(
            !ingest
                .control
                .meta()
                .list_unpurged_erasure_tombstones()
                .await
                .unwrap()
                .is_empty(),
            "matching pre-expiry re-ingest must reopen purged finite tombstone"
        );
        ingest.control.agent_store().flush_all_l0().await.unwrap();
        ingest
            .control
            .agent_store()
            .publish_all_projects()
            .await
            .unwrap();

        // Wait past `exp_ms` WITHOUT calling `prune_expired_agent_data()` -> Probe 4d-i: Query MUST return 404!
        while chrono::Utc::now().timestamp_millis() <= exp_ms + 100 {
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        query.run_query_refresh_tick().await;
        let q_res_4d_i = http
            .get(format!(
                "{q_url}/v1/projects/{pid}/agent/traces/tr_reingest_late"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(
            q_res_4d_i.status().as_u16(),
            404,
            "4d-i: pre-expiry re-ingest must NEVER resurrect on expiry before prune"
        );

        // Probe 4d-ii: Simulate restart of Ingest node (wiping in-memory `last_ingest_ms`).
        // Even if `physically_purged_at_ms` had somehow stayed set before restart, `prune_expired_agent_data()`
        // MUST unconditionally verify `purge_erased_in_project` and purge `tr_reingest_late` before pruning!
        ingest.shutdown().await;
        let ingest2 =
            start_pulse_node_with_store(node_cfg(&ingest_dir, pid, NodeRole::Ingest), s3.clone())
                .await
                .unwrap();
        // Force-mark `physically_purged_at_ms` via SQL to directly prove `prune_expired_agent_data`
        // does NOT skip verification when `last_ingest_ms` is `None` after restart!
        let active_tbs = ingest2
            .control
            .meta()
            .list_unpurged_erasure_tombstones()
            .await
            .unwrap();
        let tb_ids: Vec<String> = active_tbs.iter().map(|t| t.id.clone()).collect();
        if !tb_ids.is_empty() {
            ingest2
                .control
                .meta()
                .mark_erasure_tombstones_purged(&tb_ids, now_ms + 50)
                .await
                .unwrap();
        }

        // First prune pass may schedule urgent S3 delete (grace=0 -> deletes on first pass or second);
        // run until tombstone is confirmed absent and pruned.
        for _ in 0..5 {
            let _ = ingest2.control.prune_expired_agent_data().await;
            if ingest2
                .control
                .meta()
                .list_active_erasure_tombstones(pid, chrono::Utc::now().timestamp_millis())
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            ingest2
                .control
                .meta()
                .list_active_erasure_tombstones(pid, chrono::Utc::now().timestamp_millis())
                .await
                .unwrap()
                .is_empty(),
            "4d-ii: expired tombstone should be pruned only after physical purge completes"
        );
        query.run_query_refresh_tick().await;
        let q_res_4d_ii = http
            .get(format!(
                "{q_url}/v1/projects/{pid}/agent/traces/tr_reingest_late"
            ))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(
            q_res_4d_ii.status().as_u16(),
            404,
            "4d-ii: tr_reingest_late must remain physically purged after restart + prune"
        );

        query.shutdown().await;
        ingest2.shutdown().await;
    });

    // Part 3: `restore --from-published` before `physically_purged_at_ms` reconciles & deletes orphaned S3 `agent_segments/` (`4e`).
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("orig");
        let restored_dir = tmp.path().join("restored");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db = EnvGuard::set(&[(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        )]);
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3 = Arc::new(FaultAndCountS3::new(s3_fs.clone()));
        let pid = "proj_4e_restore";

        let node = start_pulse_node_with_store(node_cfg(&data_dir, pid, NodeRole::All), s3.clone())
            .await
            .unwrap();
        let ingest_key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let base = format!("http://{}", node.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        for (tid, tag) in [("tr_r_erase", "erase"), ("tr_r_keep", "keep")] {
            assert_eq!(
                http.post(format!("{base}/v1/agent/traces"))
                    .header("x-pulse-key", &ingest_key)
                    .json(&trace_json(tid, "sess_r", "u_r", now_ms, tag))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
        }
        node.control.agent_store().flush_all_l0().await.unwrap();
        assert_eq!(
            node.control
                .agent_store()
                .publish_all_projects()
                .await
                .unwrap(),
            1
        );
        let orig_s3_segs: Vec<ObjectPath> = s3_fs
            .list(Some(&ObjectPath::from(format!("agent_segments/{pid}"))))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location)
            .collect();
        assert!(!orig_s3_segs.is_empty());

        // Issue erasure with 5s TTL and run 1 sweep -> compacts & publishes pub_1 without the
        // superseded S3 segment, leaving it in `pending_s3_deletes` and tombstone still unpurged (`NULL`).
        let exp_ms = chrono::Utc::now().timestamp_millis() + 5_000;
        assert_eq!(
            http.post(format!("{base}/v1/projects/{pid}/agent/erasure"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "level": "trace",
                    "target_id": "tr_r_erase",
                    "expires_at": exp_ms
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let _ = node.control.sweep_pending_agent_erasures().await;
        let routed_orig = pulse_storage::with_hot_store_for_role(s3_fs.clone(), &data_dir, false)
            .await
            .unwrap();
        let enc_orig = Arc::new(pulse_storage::EncryptedObjectStore::new(routed_orig));
        let mut byok_bytes = [0u8; 32];
        for (idx, b) in byok_bytes.iter_mut().enumerate() {
            *b = u8::from_str_radix(&TEST_BYOK_KEY[idx * 2..idx * 2 + 2], 16).unwrap();
        }
        enc_orig.set_master_key(Some(byok_bytes));
        let pub1 = PublishedAgentManifestStore::new(pid, enc_orig.as_object_store())
            .unwrap()
            .load_latest()
            .await
            .unwrap()
            .unwrap();
        let active_pub1: std::collections::HashSet<String> = pub1
            .segments
            .iter()
            .map(|s| s.object_path.clone())
            .collect();
        let superseded_segs: Vec<ObjectPath> = orig_s3_segs
            .into_iter()
            .filter(|p| !active_pub1.contains(p.as_ref()))
            .collect();
        assert!(!superseded_segs.is_empty());
        for seg in &superseded_segs {
            assert!(
                s3_fs.head(seg).await.is_ok(),
                "superseded seg {seg} should still be in S3 grace window before volume loss"
            );
        }
        assert!(
            !node
                .control
                .meta()
                .list_unpurged_erasure_tombstones()
                .await
                .unwrap()
                .is_empty(),
            "tombstone must remain unpurged while superseded S3 seg is still in S3"
        );

        // Volume loss + restore into `restored_dir`.
        node.shutdown().await;
        let ext_meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        // Connect directly to shared_meta.db.
        drop(ext_meta);
        let shared_meta = MetaStore::connect_url(&format!("sqlite://{}", meta_db.display()))
            .await
            .unwrap();
        restore_from_published_with_store_and_meta_for_test(
            &restored_dir,
            None,
            s3_fs.clone(),
            &shared_meta,
        )
        .await
        .unwrap();
        drop(shared_meta);

        let restored =
            start_pulse_node_with_store(node_cfg(&restored_dir, pid, NodeRole::All), s3.clone())
                .await
                .unwrap();
        let r_base = format!("http://{}", restored.http_addr);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(6_000);
        loop {
            let _ = restored.control.sweep_pending_agent_erasures().await;
            if restored
                .control
                .meta()
                .list_unpurged_erasure_tombstones()
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "4e: restored node failed to purge and delete orphaned S3 segment before expiry"
            );
            tokio::time::sleep(Duration::from_millis(120)).await;
        }

        for seg in &superseded_segs {
            assert!(
                s3_fs.head(seg).await.is_err(),
                "4e: orphaned superseded S3 segment {seg} must be deleted after restore + sweep"
            );
        }
        let list_after: Value = http
            .get(format!("{r_base}/v1/projects/{pid}/agent/traces"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ids: Vec<&str> = list_after["traces"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["trace_id"].as_str())
            .collect();
        assert_eq!(ids, vec!["tr_r_keep"]);

        restored.shutdown().await;
    });
}

#[test]
fn t4_adversarial_agent_store_close_fenced_project_does_not_skip_siblings() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_UPLOAD_INTERVAL_SECS", Some("300")),
        ("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY)),
    ]);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("node");
        let s3_dir = tmp.path().join("s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let routed = pulse_storage::with_hot_store_for_role(s3_fs.clone(), &data_dir, false)
            .await
            .unwrap();
        let enc = Arc::new(pulse_storage::EncryptedObjectStore::new(routed));
        let mut byok_bytes = [0u8; 32];
        for (idx, b) in byok_bytes.iter_mut().enumerate() {
            *b = u8::from_str_radix(&TEST_BYOK_KEY[idx * 2..idx * 2 + 2], 16).unwrap();
        }
        enc.set_master_key(Some(byok_bytes));

        let store = pulse_agent::AgentTraceStore::open_with_encrypted(
            enc.as_object_store(),
            Some(enc.clone()),
            pulse_agent::AgentTraceStoreConfig::default(),
        )
        .await
        .unwrap();

        let now_ms = chrono::Utc::now().timestamp_millis();
        for idx in 0..8usize {
            let pid = format!("proj_{idx:02}");
            let batch: TraceIngestBatch = serde_json::from_value(trace_json(
                &format!("tr_{idx:02}"),
                &format!("sess_{idx:02}"),
                &format!("u_{idx:02}"),
                now_ms + idx as i64,
                "prompt",
            ))
            .unwrap();
            store.ingest_batch(&pid, batch).await.unwrap();
        }

        // Seed Step 4 conflict (`pub_000000000000.json`) for half the projects so `close()` hits
        // `StoreError::NotWriter` during `upload_project_state`.
        for idx in 0..4usize {
            let pid = format!("proj_{idx:02}");
            let pub_store = PublishedAgentManifestStore::new(&pid, enc.as_object_store()).unwrap();
            let hijacked = pulse_agent::store::manifest::PublishedAgentManifest {
                publish_seq: 0,
                writer_generation: 99,
                source_local_epoch: 1,
                published_at_ms: now_ms,
                committed_wal_seq: BTreeMap::new(),
                segments: Vec::new(),
                erased_traces: BTreeMap::new(),
                intent_id: Some("hijack".to_string()),
                writer_offsets: BTreeMap::new(),
            };
            pub_store.publish_next(hijacked).await.unwrap();
        }

        // `close()` must gracefully ignore `NotWriter` on fenced projects, continue flushing and
        // publishing all remaining unfenced projects, and cleanly close Foyer cache (`cache.close()`).
        store.close().await.unwrap();
        // Idempotent second close must also return Ok(()).
        store.close().await.unwrap();

        let mut unfenced_published = 0usize;
        for idx in 4..8usize {
            let pid = format!("proj_{idx:02}");
            let pub_store = PublishedAgentManifestStore::new(&pid, enc.as_object_store()).unwrap();
            let latest = pub_store.load_latest().await.unwrap();
            assert!(
                latest.is_some(),
                "unfenced project {pid} must be published during close() despite fenced siblings"
            );
            assert_eq!(latest.unwrap().segments.len(), 1);
            unfenced_published += 1;
        }
        assert_eq!(unfenced_published, 4);

        // Remove the intentionally fenced projects' local manifest dirs so reopening `store2`
        // opens only the 4 unfenced projects and verifies the Foyer disk cache closed cleanly.
        for idx in 0..4usize {
            let pid = format!("proj_{idx:02}");
            let _ = std::fs::remove_dir_all(data_dir.join("hot/agent_manifests").join(&pid));
        }
        let store2 = pulse_agent::AgentTraceStore::open_with_encrypted(
            enc.as_object_store(),
            Some(enc.clone()),
            pulse_agent::AgentTraceStoreConfig::default(),
        )
        .await
        .unwrap();
        for idx in 4..8usize {
            let pid = format!("proj_{idx:02}");
            let got = store2
                .get_trace_detail(&pid, &format!("tr_{idx:02}"), &[])
                .await
                .unwrap();
            assert!(got.is_some(), "reopened store must read {pid} trace");
        }
        store2.close().await.unwrap();
    });
}
