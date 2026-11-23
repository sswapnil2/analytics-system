//! T3 integration & fault-injection tests for Features #18–#21:
//! - #18: Local segment tier (`hot/segments/`), 0 S3 PUTs on L0 flush, count-based compaction disabled unless `PULSE_UPLOAD_INTERVAL_SECS=0`
//! - #19: Full 7-step upload cycle, 300s/1h/>4MiB timers, 64 MiB/partition merge cap, create-only S3 `segments/`, 0 idle S3 calls
//! - #20: `UploadIntent` persistence & replay across crash points 1..=7, local `hot/segments/` sweep, and fail-closed `503` dedup seeding
//! - #21: Per-project & per-node `hot/segments/` byte bounds (`1x` upload wake with failure cooldown, `2x` HTTP/QUIC `429`), isolation & recovery

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use pulse_identity::IdentityRouter;
use pulse_ingest::quic::send_quic_ingest_batch;
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole};
use pulse_storage::{
    is_hot_routed_store, with_hot_store, EncryptedObjectStore, PublishedManifestStore,
    StorageConfig, StorageEngine, UploadIntentStore,
};
use pulse_wire::{WireBatch, WireEvent};
use serde_json::{json, Value};

const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ADMIN_TOKEN: &str = "sk_root_t3_local_segment_tier_test_token_000000000";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultOp {
    Put,
    Get,
    Delete,
    AllS3,
}

#[derive(Debug)]
struct FaultRule {
    op: FaultOp,
    substring: String,
    fail_after_hits: usize,
    hits: usize,
}

#[derive(Debug)]
struct FaultAndCountStore {
    inner: Arc<dyn ObjectStore>,
    calls: Mutex<BTreeMap<(String, &'static str), u64>>,
    put_modes: Mutex<Vec<(String, String)>>,
    rule: Mutex<Option<FaultRule>>,
    fail_all_puts: AtomicBool,
    put_attempts: AtomicU64,
    triggered: AtomicBool,
}

impl FaultAndCountStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            calls: Mutex::new(BTreeMap::new()),
            put_modes: Mutex::new(Vec::new()),
            rule: Mutex::new(None),
            fail_all_puts: AtomicBool::new(false),
            put_attempts: AtomicU64::new(0),
            triggered: AtomicBool::new(false),
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

    fn take_put_modes(&self) -> Vec<(String, String)> {
        std::mem::take(&mut *self.put_modes.lock().unwrap())
    }

    fn set_rule(&self, op: FaultOp, substring: &str, fail_after_hits: usize) {
        *self.rule.lock().unwrap() = Some(FaultRule {
            op,
            substring: substring.to_string(),
            fail_after_hits,
            hits: 0,
        });
        self.triggered.store(false, Ordering::Release);
    }

    fn clear_rule(&self) {
        *self.rule.lock().unwrap() = None;
        self.fail_all_puts.store(false, Ordering::Release);
        self.triggered.store(false, Ordering::Release);
    }

    fn check_fault(&self, op: FaultOp, path: &ObjectPath) -> OsResult<()> {
        if op == FaultOp::Put {
            self.put_attempts.fetch_add(1, Ordering::Relaxed);
            if self.fail_all_puts.load(Ordering::Acquire) {
                self.triggered.store(true, Ordering::Release);
                return Err(object_store::Error::Generic {
                    store: "FaultAndCountStore",
                    source: Box::new(std::io::Error::other(format!("S3 unreachable on {path}"))),
                });
            }
        }
        let mut guard = self.rule.lock().unwrap();
        let Some(ref mut rule) = *guard else {
            return Ok(());
        };
        let op_matches = rule.op == op || rule.op == FaultOp::AllS3;
        if op_matches && path.as_ref().contains(&rule.substring) {
            if rule.hits >= rule.fail_after_hits {
                self.triggered.store(true, Ordering::Release);
                return Err(object_store::Error::Generic {
                    store: "FaultAndCountStore",
                    source: Box::new(std::io::Error::other(format!(
                        "injected fault ({op:?}) on {path}"
                    ))),
                });
            }
            rule.hits += 1;
        }
        Ok(())
    }
}

impl fmt::Display for FaultAndCountStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultAndCountStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultAndCountStore {
    async fn put_opts(&self, l: &ObjectPath, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
        self.count("PUT", Some(l));
        self.check_fault(FaultOp::Put, l)?;
        let mode_str = match o.mode {
            object_store::PutMode::Overwrite => "Overwrite",
            object_store::PutMode::Create => "Create",
            object_store::PutMode::Update(_) => "Update",
        };
        self.put_modes
            .lock()
            .unwrap()
            .push((l.as_ref().to_string(), mode_str.to_string()));
        self.inner.put_opts(l, p, o).await
    }

    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.count("PUT", Some(l));
        self.check_fault(FaultOp::Put, l)?;
        self.inner.put_multipart_opts(l, o).await
    }

    async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
        self.count("GET", Some(l));
        self.check_fault(FaultOp::Get, l)?;
        self.inner.get_opts(l, o).await
    }

    async fn get_range(&self, l: &ObjectPath, r: Range<u64>) -> OsResult<Bytes> {
        self.count("GET", Some(l));
        self.check_fault(FaultOp::Get, l)?;
        self.inner.get_range(l, r).await
    }

    async fn head(&self, l: &ObjectPath) -> OsResult<ObjectMeta> {
        self.count("HEAD", Some(l));
        self.check_fault(FaultOp::Get, l)?;
        self.inner.head(l).await
    }

    async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
        self.count("DELETE", Some(l));
        self.check_fault(FaultOp::Delete, l)?;
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
        self.check_fault(FaultOp::Put, to)?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.count("PUT", Some(to));
        self.check_fault(FaultOp::Put, to)?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn sum_prefix_ops(calls: &BTreeMap<(String, &'static str), u64>, prefix: &str, op: &str) -> u64 {
    calls
        .iter()
        .filter(|((p, o), _)| p == prefix && *o == op)
        .map(|(_, n)| *n)
        .sum()
}

fn storage_engine_s3_calls(calls: &BTreeMap<(String, &'static str), u64>) -> u64 {
    calls
        .iter()
        .filter(|((p, _), _)| {
            matches!(
                p.as_str(),
                "segments" | "published" | "manifests" | "identity" | "profiles" | "wal" | "hot"
            )
        })
        .map(|(_, n)| *n)
        .sum()
}

fn node_cfg(dir: &Path, pid: &str, role: NodeRole, with_quic: bool) -> NodeConfig {
    NodeConfig {
        role,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: with_quic.then(|| "127.0.0.1:0".parse().unwrap()),
        data_dir: dir.to_path_buf(),
        web_dist_dir: dir.join("no_web"),
        default_project_id: pid.to_string(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN_TOKEN.to_string()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 2,
    }
}

fn make_event(did: &str, event_name: &str, ts_ms: i64, seq: u64) -> WireEvent {
    WireEvent {
        event_uuid: WireEvent::deterministic_uuid(did, "sess", event_name, ts_ms, seq),
        routing_seed: did.into(),
        distinct_id: did.into(),
        session_id: "sess".into(),
        event_name: event_name.into(),
        client_event_ts_ms: ts_ms,
        client_sent_ts_ms: 0,
        props: vec![("plan".into(), pulse_wire::PropValue::Str("pro".into()))],
        replay_offset_kb: 0,
    }
}

#[test]
fn t3_1_local_segment_tier_query_equivalence_and_zero_idle_s3_calls() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let s3_dir = tmp.path().join("s3");
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs));

        let node = start_pulse_node_with_store(
            node_cfg(&data_dir, "proj_t3_1", NodeRole::All, false),
            s3_fault.clone(),
        )
        .await
        .unwrap();
        let pid = "proj_t3_1";
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Warm up project open (reads S3 published/ once at open), then verify 0 S3 calls on ingest + flushes!
        let _ps_warm = node.storage.get_or_open_project(pid).await.unwrap();
        s3_fault.take_calls();
        assert_eq!(
            http.post(format!("http://{}/v1/identify", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!({ "anon_id": "anon_0", "user_id": "user_0" }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("http://{}/v1/engage", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!([{ "$distinct_id": "user_0", "$set": { "plan": "pro" } }]))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        for i in 0..4u64 {
            assert_eq!(
                http.post(format!("http://{}/v1/ingest", node.http_addr))
                    .header("x-pulse-key", &key)
                    .json(&json!({
                        "events": [
                            { "distinct_id": "anon_0", "event_name": "Signup", "client_event_ts_ms": now_ms - 10_000 + i as i64 * 1000 },
                            { "distinct_id": "user_0", "event_name": "Activated", "client_event_ts_ms": now_ms - 5_000 + i as i64 * 1000 },
                        ]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            node.storage.flush_all_projects().await.unwrap();
        }
        let flush_calls = s3_fault.take_calls();
        assert_eq!(storage_engine_s3_calls(&flush_calls), 0, "{flush_calls:?}");
        assert!(data_dir.join("hot/hot/segments").join(pid).is_dir());

        // Count-based compaction is disabled for local segment tier when upload_interval > 0.
        let ps = node.storage.get_or_open_project(pid).await.unwrap();
        assert_eq!(
            ps.compact_overdue(node.storage.segment_reader())
                .await
                .unwrap(),
            0
        );
        assert_eq!(ps.snapshot().manifest.total_segments(), 8);
        assert!(ps.hot_segment_bytes() > 0);

        let metrics_before = http
            .get(format!("http://{}/metrics", node.http_addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            metrics_before.contains("pulse_hot_segment_bytes ")
                && !metrics_before.contains("pulse_hot_segment_bytes 0\n"),
            "metrics must export non-zero pulse_hot_segment_bytes before upload: {metrics_before}"
        );

        let query_req = json!({
            "kind": "funnel",
            "project_id": pid,
            "query": {
                "steps": [{ "event_name": "Signup" }, { "event_name": "Activated" }],
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "order_mode": "LooseOrder",
                "conversion_window_ms": 3_600_000
            }
        });
        let funnel_before: Value = http
            .post(format!("http://{}/v1/query", node.http_addr))
            .bearer_auth(ADMIN_TOKEN)
            .json(&query_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            funnel_before["overall_steps"][0]["entered_users"].as_u64(),
            Some(1),
            "{funnel_before}"
        );
        assert_eq!(
            funnel_before["overall_steps"][1]["entered_users"].as_u64(),
            Some(1),
            "{funnel_before}"
        );

        // Run full 7-step upload cycle!
        s3_fault.take_calls();
        s3_fault.take_put_modes();
        assert_eq!(node.storage.publish_all_projects().await.unwrap(), 1);
        let upload_calls = s3_fault.take_calls();
        let put_modes = s3_fault.take_put_modes();

        assert_eq!(sum_prefix_ops(&upload_calls, "segments", "GET"), 0);
        assert_eq!(sum_prefix_ops(&upload_calls, "segments", "LIST"), 0);
        assert_eq!(sum_prefix_ops(&upload_calls, "segments", "PUT"), 2);
        assert_eq!(ps.snapshot().manifest.total_segments(), 2);
        assert_eq!(sum_prefix_ops(&upload_calls, "manifests", "PUT"), 0);
        assert_eq!(sum_prefix_ops(&upload_calls, "published", "PUT"), 3); // profiles + identity + pub_0
        for (path, mode) in &put_modes {
            if path.starts_with("segments/") || path.starts_with("published/") {
                assert_eq!(mode, "Create", "S3 object {path} must use PutMode::Create");
            }
        }
        assert_eq!(ps.hot_segment_bytes(), 0);
        assert_eq!(node.storage.hot_segment_bytes(), 0);

        // Query results identical after upload!
        let funnel_after: Value = http
            .post(format!("http://{}/v1/query", node.http_addr))
            .bearer_auth(ADMIN_TOKEN)
            .json(&query_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(funnel_after["overall_steps"], funnel_before["overall_steps"]);
        assert_eq!(funnel_after["sampling"], funnel_before["sampling"]);
        assert_eq!(
            funnel_after["time_to_convert"],
            funnel_before["time_to_convert"]
        );
        assert!(
            funnel_after["snapshot_epoch"].as_u64().unwrap()
                > funnel_before["snapshot_epoch"].as_u64().unwrap()
        );

        // Flush a closed-date late event (`now_ms - 2 days`, <= 4 MiB) for existing user_0.
        let old_ts = now_ms - 2 * 86_400_000;
        assert_eq!(
            http.post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!({
                    "events": [{ "distinct_id": "user_0", "event_name": "Activated", "client_event_ts_ms": old_ts }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.storage.flush_all_projects().await.unwrap();

        // Idle ticks + upload cycle with only deferred closed-date segment -> 0 S3 calls!
        s3_fault.take_calls();
        assert_eq!(node.storage.publish_all_projects().await.unwrap(), 0);
        assert_eq!(node.storage.run_gc_all_projects().await.unwrap(), 0);
        assert_eq!(node.storage.compact_all_projects().await.unwrap(), 0);
        tokio::time::sleep(Duration::from_secs(6)).await;
        let idle_calls = s3_fault.take_calls();
        assert_eq!(
            storage_engine_s3_calls(&idle_calls),
            0,
            "idle + deferred closed-date project must make 0 S3 calls on StorageEngine prefixes: {idle_calls:?}"
        );

        node.shutdown().await;
    });
}

#[test]
fn t3_2_crash_recovery_all_steps_1_through_7_intent_sweep_and_s3_unreachable() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };

        // Test crash points:
        // 1 = fail on Step 2 profile snapshot (Step 1 intent already on disk)
        // 2 = fail on Step 3 identity snapshot (Step 2 profile snapshot already in S3)
        // 3 = fail on Step 4 S3 segment PUT after 1st S3 segment? Wait: test 1 partition -> fail on Step 4, AND 2 partitions -> fail on 2nd S3 segment!
        // 4 = fail on Step 5 local manifest commit (S3 snapshots + S3 segment already written!)
        // 5 = fail on Step 6 published/ PUT (local manifest committed S3 segments, S3 snapshots written!)
        // 6 = Step 6 self-crash (published/{pid}/pub_ written in S3, fail on post-Step-6 local manifest update!)
        // 7 = fail on Step 7 local segment delete (post-Step-6 local manifest updated, hot/segments/ still on disk + intent present!)
        #[derive(Clone, Copy)]
        enum CrashStage {
            Step1BeforeSnapshots,
            Step2AfterProfileSnap,
            Step3AfterIdentitySnap,
            Step4AfterFirstS3Segment,
            Step5AfterLocalManifestCommit,
            Step6SelfCrashAfterPubCreate,
            Step7PartialLocalDelete,
        }

        let stages = [
            CrashStage::Step1BeforeSnapshots,
            CrashStage::Step2AfterProfileSnap,
            CrashStage::Step3AfterIdentitySnap,
            CrashStage::Step4AfterFirstS3Segment,
            CrashStage::Step5AfterLocalManifestCommit,
            CrashStage::Step6SelfCrashAfterPubCreate,
            CrashStage::Step7PartialLocalDelete,
        ];

        for (idx, stage) in stages.into_iter().enumerate() {
            let pid = format!("proj_crash_{idx}");
            let tmp = tempfile::tempdir().unwrap();
            let s3_dir = tmp.path().join("s3");
            let data_dir = tmp.path().join("node");
            std::fs::create_dir_all(&s3_dir).unwrap();

            let s3_fs: Arc<dyn ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
            let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs.clone()));
            let routed = with_hot_store(s3_fault.clone(), &data_dir).await.unwrap();
            assert!(is_hot_routed_store(routed.as_ref()));
            let local_fault = Arc::new(FaultAndCountStore::new(routed));

            let enc = Arc::new(EncryptedObjectStore::new(local_fault.clone()));
            let engine = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc.clone(),
                Arc::new(IdentityRouter::new(enc.as_object_store())),
            );
            let ps = engine.get_or_open_project(&pid).await.unwrap();
            let ig = engine.identity_graph(&pid, true).await.unwrap();
            let pstore = engine.profile_store(&pid, true).await.unwrap();

            let now_ms = chrono::Utc::now().timestamp_millis();
            let u0 = ig.get_or_intern("u0", 0);
            pstore
                .upsert(&pulse_storage::StoredUserProfile {
                    project_id: pid.clone(),
                    distinct_id: "u0".into(),
                    canonical_uid: u0,
                    props: BTreeMap::from([("tier".into(), json!("gold"))]),
                    total_revenue: 10.0,
                    updated_at_ms: now_ms,
                })
                .await
                .unwrap();

            // Write 2 dates so Step 4 has 2 S3 PUTs when testing mid-Step-4 crash.
            ps.append_bucket_events(
                0,
                vec![
                    (make_event("u0", "click", now_ms, 1), u0),
                    (make_event("u0", "click", now_ms + 86_400_000, 2), u0),
                ],
            )
            .await
            .unwrap();
            ps.flush_bucket(0).await.unwrap();

            // Also plant an orphan `.pvtx` file from a simulated crashed L0 flush!
            let orphan_l0 = ObjectPath::from(format!(
                "hot/segments/{pid}/20261123/bucket_000/seg_orphan_l0.pvtx"
            ));
            enc.as_object_store()
                .put(&orphan_l0, PutPayload::from_static(b"orphan"))
                .await
                .unwrap();

            let _pinned_step7 = if matches!(stage, CrashStage::Step7PartialLocalDelete) {
                Some(ps.snapshot())
            } else {
                None
            };

            match stage {
                CrashStage::Step1BeforeSnapshots => {
                    s3_fault.set_rule(FaultOp::Put, "snapshots/profiles_", 0);
                }
                CrashStage::Step2AfterProfileSnap => {
                    s3_fault.set_rule(FaultOp::Put, "snapshots/identity_", 0);
                }
                CrashStage::Step3AfterIdentitySnap => {
                    s3_fault.set_rule(FaultOp::Put, "segments/", 0);
                }
                CrashStage::Step4AfterFirstS3Segment => {
                    s3_fault.set_rule(FaultOp::Put, "segments/", 1);
                }
                CrashStage::Step5AfterLocalManifestCommit => {
                    // Fail Step 6 `published/{pid}/pub_` AFTER Step 5 local manifest commit!
                    s3_fault.set_rule(FaultOp::Put, "/pub_000000000000.json", 0);
                }
                CrashStage::Step6SelfCrashAfterPubCreate => {
                    // `pub_0` succeeds on S3; post-Step-6 local manifest commit (`epoch_000000000003.json`) fails!
                    local_fault.set_rule(FaultOp::Put, "epoch_000000000003.json", 0);
                }
                CrashStage::Step7PartialLocalDelete => {
                    // Local segments remain on disk (pinned) and intent clear fails at Step 7!
                    local_fault.set_rule(FaultOp::Delete, "upload_intent.json", 0);
                }
            }

            let err = ps
                .upload_if_dirty_at(now_ms, false)
                .await
                .expect_err("injected crash must fail upload cycle");
            drop(_pinned_step7);
            assert!(
                s3_fault.triggered.load(Ordering::Acquire)
                    || local_fault.triggered.load(Ordering::Acquire),
                "crash stage failed without fault trigger: {err}"
            );

            // UploadIntent must still be present on disk!
            assert!(
                UploadIntentStore::new(&pid, enc.as_object_store())
                    .load()
                    .await
                    .unwrap()
                    .is_some(),
                "UploadIntent must persist after crash"
            );

            pstore.close().await.unwrap();
            ig.close().await.unwrap();
            drop(ps);
            drop(engine);

            // Test S3 unreachable during restart intent replay on Step 4 crash!
            if matches!(stage, CrashStage::Step4AfterFirstS3Segment) {
                s3_fault.clear_rule();
                s3_fault.set_rule(FaultOp::Delete, "segments/", 0);
                let routed_unreach = with_hot_store(s3_fault.clone(), &data_dir).await.unwrap();
                let enc_unreach = Arc::new(EncryptedObjectStore::new(routed_unreach));
                let eng_unreach = StorageEngine::new_with_encrypted(
                    cfg.clone(),
                    enc_unreach.clone(),
                    Arc::new(IdentityRouter::new(enc_unreach.as_object_store())),
                );
                assert!(
                    eng_unreach.get_or_open_project(&pid).await.is_err(),
                    "S3 unreachable during intent replay must fail open"
                );
                assert!(
                    UploadIntentStore::new(&pid, enc_unreach.as_object_store())
                        .load()
                        .await
                        .unwrap()
                        .is_some(),
                    "intent must NOT be cleared when S3 delete fails"
                );
            }

            // Clear faults and restart cleanly!
            s3_fault.clear_rule();
            local_fault.clear_rule();

            let routed2 = with_hot_store(s3_fault.clone(), &data_dir).await.unwrap();
            let enc2 = Arc::new(EncryptedObjectStore::new(routed2));
            let engine2 = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc2.clone(),
                Arc::new(IdentityRouter::new(enc2.as_object_store())),
            );
            let ps2 = engine2.get_or_open_project(&pid).await.unwrap();

            // Intent cleared, orphan L0 segment swept!
            assert!(
                UploadIntentStore::new(&pid, enc2.as_object_store())
                    .load()
                    .await
                    .unwrap()
                    .is_none(),
                "startup intent replay must clear UploadIntent"
            );
            assert!(
                enc2.as_object_store().head(&orphan_l0).await.is_err(),
                "startup sweep must delete unreferenced L0 hot/segments/ orphan"
            );

            // Finish any pending upload (for Steps 1-5) and assert exact state!
            let _ = ps2.upload_if_dirty_at(now_ms, false).await.unwrap();

            assert_eq!(ps2.last_published_seq(), Some(0));
            assert_eq!(ps2.snapshot().manifest.total_l1_rows(), 2);
            assert_eq!(ps2.hot_segment_bytes(), 0);

            // Verify 0 leftover hot/segments/ files on local disk!
            let hot_leftovers: Vec<_> = enc2
                .as_object_store()
                .list(Some(&ObjectPath::from(format!("hot/segments/{pid}"))))
                .try_collect()
                .await
                .unwrap();
            assert!(
                hot_leftovers.is_empty(),
                "expected 0 leftover hot/segments/ after recovery+upload, found {hot_leftovers:?}"
            );

            // Verify S3 has ONLY the referenced segments (2) and snapshots (2) + pub_0 (1) — ZERO leaked orphans!
            let s3_segs: Vec<_> = s3_fs
                .list(Some(&ObjectPath::from(format!("segments/{pid}"))))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                s3_segs.len(),
                2,
                "expected 2 S3 segments (0 leaked orphans)"
            );
            let s3_snaps: Vec<_> = s3_fs
                .list(Some(&ObjectPath::from(format!(
                    "published/{pid}/snapshots"
                ))))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                s3_snaps.len(),
                2,
                "expected 2 S3 snapshot blobs (1 identity + 1 profiles, 0 leaked orphans)"
            );
            let latest_pub = PublishedManifestStore::new(&pid, enc2.as_object_store())
                .load_latest()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(latest_pub.publish_seq, 0);
            assert_eq!(latest_pub.high_water_mark, 1);

            engine2
                .identity_graph(&pid, true)
                .await
                .unwrap()
                .close()
                .await
                .unwrap();
        }
    });
}

#[test]
fn t3_3_dedup_seeding_503_on_s3_unreachable_no_duplicate_on_retry() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let s3_dir = tmp.path().join("s3");
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs));

        let pid = "proj_t3_3";
        let now_ms = chrono::Utc::now().timestamp_millis();
        let ev_uuid = format!("{:032x}", 0xdead_beef_u128);

        let key = {
            let node = start_pulse_node_with_store(
                node_cfg(&data_dir, pid, NodeRole::All, false),
                s3_fault.clone(),
            )
            .await
            .unwrap();
            let k = node
                .bootstrap_credentials
                .generated_ingest_key
                .clone()
                .unwrap();
            let http = reqwest::Client::new();
            let resp = http
                .post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &k)
                .json(&json!({
                    "events": [{
                        "event_uuid": ev_uuid,
                        "distinct_id": "u0",
                        "event_name": "Purchase",
                        "client_event_ts_ms": now_ms - 1_000,
                    }]
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            node.storage.flush_all_projects().await.unwrap();
            assert_eq!(node.storage.publish_all_projects().await.unwrap(), 1);
            node.shutdown().await;
            k
        };

        // Restart node while S3 `segments/` reads fail during dedup seeding (`engine.rs:815`).
        s3_fault.set_rule(FaultOp::Get, "segments/", 0);
        let node2 = start_pulse_node_with_store(
            node_cfg(&data_dir, pid, NodeRole::All, false),
            s3_fault.clone(),
        )
        .await
        .unwrap();
        let http = reqwest::Client::new();

        let retry_payload = json!({
            "events": [{
                "event_uuid": ev_uuid,
                "distinct_id": "u0",
                "event_name": "Purchase",
                "client_event_ts_ms": now_ms - 1_000,
            }]
        });
        let fail_resp = http
            .post(format!("http://{}/v1/ingest", node2.http_addr))
            .header("x-pulse-key", &key)
            .json(&retry_payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            fail_resp.status(),
            503,
            "S3 unreachable during dedup seeding must surface 503, not accept with empty dedup"
        );
        assert!(node2.storage.get_project(pid).is_none());

        // S3 recovers -> retry deduplicates cleanly and total events stays 1!
        s3_fault.clear_rule();
        let ok_resp: Value = http
            .post(format!("http://{}/v1/ingest", node2.http_addr))
            .header("x-pulse-key", &key)
            .json(&retry_payload)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(ok_resp["deduped"].as_u64(), Some(1));
        assert_eq!(ok_resp["accepted"].as_u64(), Some(0));

        let ps2 = node2.storage.get_project(pid).unwrap();
        assert_eq!(ps2.snapshot().manifest.total_l1_rows(), 1);
        assert_eq!(ps2.total_l0_rows(), 0);

        node2.shutdown().await;
    });
}

#[test]
fn t3_4_hot_segment_byte_bounds_1x_wake_2x_429_http_quic_and_recovery() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let s3_dir = tmp.path().join("s3");
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs));

        let node = start_pulse_node_with_store(
            node_cfg(&data_dir, "proj_a", NodeRole::All, true),
            s3_fault.clone(),
        )
        .await
        .unwrap();
        let key_a = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();

        // Create Project B + ingest key.
        let created_b: Value = http
            .post(format!("http://{}/v1/projects", node.http_addr))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({ "name": "proj_b" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let pid_b = created_b["project"]["id"].as_str().unwrap().to_string();
        let key_b_resp: Value = http
            .post(format!("http://{}/v1/projects/{pid_b}/api-keys", node.http_addr))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({ "label": "ingest_b", "role": "ingest_and_flags" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let key_b = key_b_resp["plaintext_key"].as_str().unwrap().to_string();

        // 1. Verify 1x triggers immediate background upload WITHOUT waiting 300s!
        node.storage.set_hot_segment_byte_limits(400, 100_000_000);
        let now_ms = chrono::Utc::now().timestamp_millis();
        assert_eq!(
            http.post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u1", "event_name": "Ev", "client_event_ts_ms": now_ms, "props": { "pad": "x".repeat(256) } }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.storage.flush_all_projects().await.unwrap();
        // Wait up to 4s (far less than 300s!) for upload_notify to drain hot_segment_bytes to 0.
        tokio::time::timeout(Duration::from_secs(4), async {
            while node.storage.hot_segment_bytes() > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("1x crossing must wake background upload immediately");

        // 2. Make S3 unreachable -> 1x does NOT busy-spin S3, ingest continues up to 2x, then 429!
        node.storage.set_hot_segment_byte_limits(1_500, 100_000_000); // 2x = 3_000
        s3_fault.fail_all_puts.store(true, Ordering::Release);
        s3_fault.put_attempts.store(0, Ordering::Release);

        let mut seq = 1u64;
        let mut got_429 = false;
        for _ in 0..20 {
            let resp = http
                .post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!({
                    "events": [{
                        "distinct_id": format!("u_{seq}"),
                        "event_name": "BigEv",
                        "client_event_ts_ms": now_ms + seq as i64,
                        "props": { "pad": "y".repeat(600) }
                    }]
                }))
                .send()
                .await
                .unwrap();
            seq += 1;
            if resp.status() == 429 {
                assert_eq!(
                    resp.headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok()),
                    Some("1")
                );
                let body: Value = resp.json().await.unwrap();
                assert_eq!(body["code"].as_str(), Some("hot_segment_backpressure"));
                got_429 = true;
                break;
            }
            assert_eq!(resp.status(), 200);
            node.storage.flush_all_projects().await.unwrap();
        }
        assert!(got_429, "Project A must hit 429 hot_segment_backpressure past 2x");

        // Send burst of 429 requests over 2.5s and verify upload failure cooldown prevents S3 busy-spin!
        let attempts_before = s3_fault.put_attempts.load(Ordering::Relaxed);
        for _ in 0..10 {
            let r = http
                .post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!({
                    "events": [{ "distinct_id": "u_spin", "event_name": "Ev", "client_event_ts_ms": now_ms }]
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 429);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let attempts_after = s3_fault.put_attempts.load(Ordering::Relaxed);
        assert!(
            attempts_after - attempts_before <= 3,
            "upload failure cooldown must prevent busy-spinning S3 (attempts={})",
            attempts_after - attempts_before
        );

        // QUIC ingest also enforces hot_segment_backpressure on Project A!
        if let Some(ref quic) = node.quic {
            let batch = WireBatch::new(vec![make_event("u_quic", "Ev", now_ms, 999)]);
            let encoded = batch.encode_binary(1).unwrap();
            let err = send_quic_ingest_batch(quic.local_addr, &quic.cert_der, &key_a, &encoded)
                .await
                .expect_err("QUIC ingest must reject when Project A >= 2x");
            assert!(
                err.to_string().contains("hot_segment_backpressure")
                    || err.to_string().contains("hot segment backpressure"),
                "{err}"
            );
        }

        // Synchronous endpoints on Project A remain unaffected!
        assert_eq!(
            http.post(format!("http://{}/v1/identify", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!({ "anon_id": "a_sync", "user_id": "u_sync" }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("http://{}/v1/engage", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!([{ "$distinct_id": "u_sync", "$set": { "ok": true } }]))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("http://{}/v1/groups", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!({ "$group_type": "org", "$group_key": "g1", "$set": { "tier": "pro" } }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let replay_chunk = pulse_wire::ReplayChunk {
            project_id: "proj_a".into(),
            session_id: "sess_sync".into(),
            chunk_start_ts_ms: now_ms,
            ops: vec![],
        };
        let replay_resp_s3_down = http
            .post(format!("http://{}/v1/replay/chunk", node.http_addr))
            .header("x-pulse-key", &key_a)
            .json(&replay_chunk)
            .send()
            .await
            .unwrap()
            .status();
        assert_ne!(
            replay_resp_s3_down, 429,
            "/v1/replay/chunk must never return hot_segment_backpressure 429"
        );

        // Unrelated Project B stays 200 OK while node total < 2 * node_1x!
        assert_eq!(
            http.post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key_b)
                .json(&json!({
                    "events": [{ "distinct_id": "ub", "event_name": "Ev", "client_event_ts_ms": now_ms }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        // Node-wide 2x blocks even Project B!
        node.storage.set_hot_segment_byte_limits(100_000_000, 1_500);
        let b_blocked = http
            .post(format!("http://{}/v1/ingest", node.http_addr))
            .header("x-pulse-key", &key_b)
            .json(&json!({
                "events": [{ "distinct_id": "ub2", "event_name": "Ev", "client_event_ts_ms": now_ms }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(b_blocked.status(), 429);

        // Unblock S3 -> `/v1/replay/chunk` succeeds even before 2x drains, and after failure cooldown
        // expires, 1x pressure wakes background upload, drains hot/segments/ to 0, and Project A resumes 200 OK!
        node.storage.set_hot_segment_byte_limits(1_500, 100_000_000);
        s3_fault.fail_all_puts.store(false, Ordering::Release);
        assert_eq!(
            http.post(format!("http://{}/v1/replay/chunk", node.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&replay_chunk)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let r = http
                    .post(format!("http://{}/v1/ingest", node.http_addr))
                    .header("x-pulse-key", &key_a)
                    .json(&json!({
                        "events": [{ "distinct_id": "u_resumed", "event_name": "Ev", "client_event_ts_ms": now_ms }]
                    }))
                    .send()
                    .await
                    .unwrap();
                if r.status() == 200 && node.storage.hot_segment_bytes() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("hot_segment_bytes must drain and Project A resume 200 OK after S3 recovers");

        node.shutdown().await;
    });
}

fn count_files_recursive(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0usize;
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            count += count_files_recursive(&p);
        } else {
            count += 1;
        }
    }
    count
}

#[test]
fn t3_5_live_step5_to_step6_s3_failure_recovery_without_restart() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let s3_dir = tmp.path().join("s3");
        let data_dir = tmp.path().join("node");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs));

        let pid = "proj_t3_5";
        let node = start_pulse_node_with_store(
            node_cfg(&data_dir, pid, NodeRole::All, false),
            s3_fault.clone(),
        )
        .await
        .unwrap();
        let key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let now_ms = chrono::Utc::now().timestamp_millis();

        // Ingest and flush a local hot/segments/ file while 1x is still high.
        assert_eq!(
            http.post(format!("http://{}/v1/ingest", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!({
                    "events": [{
                        "distinct_id": "u0",
                        "event_name": "Purchase",
                        "client_event_ts_ms": now_ms,
                        "props": { "pad": "z".repeat(512) }
                    }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.storage.flush_all_projects().await.unwrap();
        let ps = node.storage.get_project(pid).unwrap();
        assert!(ps.hot_segment_bytes() >= 200);
        assert_eq!(
            count_files_recursive(&data_dir.join("hot/hot/segments").join(pid)),
            1
        );

        // Pin snapshot during Step 6 fault so background upload/drain cannot race ahead of our assertions,
        // inject Step 6 PUT failure on `pub_000000000000.json`, and lower 1x/2x bounds so project hits 429.
        let pin = ps.snapshot();
        s3_fault.set_rule(FaultOp::Put, "/pub_000000000000.json", 0);
        node.storage.set_hot_segment_byte_limits(100, 100_000_000);

        let r429 = http
            .post(format!("http://{}/v1/ingest", node.http_addr))
            .header("x-pulse-key", &key)
            .json(&json!({
                "events": [{ "distinct_id": "u1", "event_name": "Ev", "client_event_ts_ms": now_ms }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r429.status(), 429);
        assert!(ps.check_hot_segment_backpressure().is_err());

        // Step 5 commits local manifest and Step 6 fails on `pub_000000000000.json` without restart.
        let err = ps
            .upload_if_dirty_at(now_ms, false)
            .await
            .expect_err("Step 6 pub_0 PUT failure must fail upload_if_dirty_at");
        assert!(
            s3_fault.triggered.load(Ordering::Acquire),
            "expected Step 6 fault trigger, got: {err}"
        );
        assert_eq!(ps.last_published_seq(), None);

        // Release pin, clear S3 fault WITHOUT restarting PulseNode/StorageEngine, and retry upload.
        drop(pin);
        s3_fault.clear_rule();
        if let Some(pub0) = ps.upload_if_dirty_at(now_ms + 5_000, false).await.unwrap() {
            assert_eq!(pub0.publish_seq, 0);
        }
        ps.drain_pending_local_deletes().await;

        assert_eq!(ps.last_published_seq(), Some(0));
        assert_eq!(ps.hot_segment_bytes(), 0);
        assert_eq!(node.storage.hot_segment_bytes(), 0);
        assert!(ps.check_hot_segment_backpressure().is_ok());
        assert_eq!(
            count_files_recursive(&data_dir.join("hot/hot/segments").join(pid)),
            0,
            "expected 0 leftover hot/segments/ files on disk after live recovery"
        );

        let ok_resp = http
            .post(format!("http://{}/v1/ingest", node.http_addr))
            .header("x-pulse-key", &key)
            .json(&json!({
                "events": [{
                    "distinct_id": "u_after_recovery",
                    "event_name": "Ev",
                    "client_event_ts_ms": now_ms + 6_000
                }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(ok_resp.status(), 200);

        node.shutdown().await;
    });
}
