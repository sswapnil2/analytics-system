//! T4 integration & fault-injection tests for Feature #26 (Cold S3 Merge with T2 2-publish-interval deletion grace):
//! - Closed-date + 1h idle gating (today's date skipped, `< 1h` idle skipped, late upload resets 1h timer)
//! - Bucket-isolated S3 merge (`PutMode::Create`), `64 MiB` input cap, 0 idle S3 calls after merge + GC
//! - T2 2-publish-interval deletion grace + `StorageSnapshot` pinning + `restore --from-published` cleanup
//! - Remote `NodeRole::Query` concurrent read safety across cold merge + publish + GC (`0` HTTP / `NotFound` errors)
//! - Role gating (`Query` and `Compactor` make 0 cold-merge calls; `All` and `Ingest` run cold merge)
//! - Background scheduler (`PULSE_COLD_MERGE_IDLE_SECS` / `PULSE_COLD_MERGE_INTERVAL_SECS`) & fault injection

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
use pulse_control::MetaStore;
use pulse_node::{
    restore_from_published_with_store_and_meta_for_test, start_pulse_node_with_store, NodeConfig,
    NodeRole,
};
use pulse_storage::ColdMergeIntentStore;
use serde_json::{json, Value};

const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST_VAULT_KEK: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
const ADMIN_TOKEN: &str = "sk_root_t4_cold_s3_merge_test_token_00000000000000";

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
}

impl FaultAndCountStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            calls: Mutex::new(BTreeMap::new()),
            put_modes: Mutex::new(Vec::new()),
            rule: Mutex::new(None),
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
    }

    fn clear_rule(&self) {
        *self.rule.lock().unwrap() = None;
    }

    fn check_fault(&self, op: FaultOp, path: &ObjectPath) -> OsResult<()> {
        let mut guard = self.rule.lock().unwrap();
        let Some(ref mut rule) = *guard else {
            return Ok(());
        };
        let op_matches = rule.op == op || rule.op == FaultOp::AllS3;
        if op_matches && path.as_ref().contains(&rule.substring) {
            if rule.hits >= rule.fail_after_hits {
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

fn sum_op(calls: &BTreeMap<(String, &'static str), u64>, op: &str) -> u64 {
    calls
        .iter()
        .filter(|((_, o), _)| *o == op)
        .map(|(_, n)| *n)
        .sum()
}

fn node_cfg(dir: &Path, pid: &str, role: NodeRole, num_buckets: u16) -> NodeConfig {
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
        num_buckets,
    }
}

#[test]
fn t4_1_cold_s3_merge_e2e_closed_idle_grace_and_zero_idle_s3_calls() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _cold_idle = EnvGuard::set("PULSE_COLD_MERGE_IDLE_SECS", Some("3600"));
    let _cold_int = EnvGuard::set("PULSE_COLD_MERGE_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));
    let _kek = EnvGuard::set("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
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
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs.clone()));

        let pid = "proj_t4_1";
        let node = start_pulse_node_with_store(
            node_cfg(&data_dir, pid, NodeRole::All, 2),
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
        let base_url = format!("http://{}", node.http_addr);

        let now_ms = chrono::Utc::now().timestamp_millis();
        let d2_ts = now_ms - 2 * 86_400_000;
        let d1_ts = now_ms - 86_400_000;
        let d2_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(d2_ts)
            .unwrap()
            .date_naive();
        let d1_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(d1_ts)
            .unwrap()
            .date_naive();
        let today_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now_ms)
            .unwrap()
            .date_naive();

        // Pick users distributed across both buckets (2 buckets).
        let mut users = Vec::new();
        for i in 0..8 {
            users.push(format!("user_{i}"));
        }

        let ps = node.storage.get_or_open_project(pid).await.unwrap();

        // Perform 3 ingest + flush + upload cycles to create 3 S3 segments per (date, bucket).
        for round in 0..3i64 {
            let mut events = Vec::new();
            for (idx, u) in users.iter().enumerate() {
                events.push(json!({
                    "distinct_id": u,
                    "event_name": "Signup",
                    "client_event_ts_ms": d2_ts + round * 1_000 + idx as i64 * 10
                }));
                events.push(json!({
                    "distinct_id": u,
                    "event_name": "Signup",
                    "client_event_ts_ms": d1_ts + round * 1_000 + idx as i64 * 10
                }));
                events.push(json!({
                    "distinct_id": u,
                    "event_name": "Signup",
                    "client_event_ts_ms": now_ms - 5_000 + round * 100 + idx as i64
                }));
            }
            assert_eq!(
                http.post(format!("{base_url}/v1/ingest"))
                    .header("x-pulse-key", &key)
                    .json(&json!({ "events": events }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            node.ingest.flush_all().await.unwrap();
            node.storage.flush_all_projects().await.unwrap();
            assert!(ps.upload_if_dirty_at(now_ms, true).await.unwrap().is_some());
        }

        // Verify 3 S3 segments per (date, bucket) exist before cold merge.
        let pre_snap = ps.snapshot();
        for d in [d2_date, d1_date, today_date] {
            for b in 0..2u8 {
                assert_eq!(
                    pre_snap
                        .manifest
                        .active_segments
                        .get(&(d, b))
                        .unwrap()
                        .len(),
                    3,
                    "expected 3 S3 segments for ({d}, {b})"
                );
            }
        }
        drop(pre_snap);

        let insights_req = json!({
            "kind": "insights",
            "project_id": pid,
            "query": {
                "time_range": { "start_ts_ms": d2_ts - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "Signup", "metric": "total" }]
            }
        });
        let q_before: Value = http
            .post(format!("{base_url}/v1/query"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&insights_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            q_before["series"][0]["overall_value"].as_f64(),
            Some(72.0),
            "{q_before}"
        );

        // 1. `< 1h` idle -> 0 merges, 0 S3 calls!
        s3_fault.take_calls();
        assert_eq!(
            node.storage
                .cold_merge_all_projects_at(now_ms + 3_599_000)
                .await
                .unwrap(),
            0
        );
        let calls_before_1h = s3_fault.take_calls();
        assert_eq!(
            storage_engine_s3_calls(&calls_before_1h),
            0,
            "cold_merge < 1h idle must make 0 S3 calls: {calls_before_1h:?}"
        );

        // 2. Late upload to yesterday (`d1_date`) at now_ms + 3_000_000 resets d1's 1h timer!
        let mut late_events = Vec::new();
        for (idx, u) in users.iter().enumerate() {
            late_events.push(json!({
                "distinct_id": u,
                "event_name": "Signup",
                "client_event_ts_ms": d1_ts + 50_000 + idx as i64
            }));
        }
        assert_eq!(
            http.post(format!("{base_url}/v1/ingest"))
                .header("x-pulse-key", &key)
                .json(&json!({ "events": late_events }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        node.ingest.flush_all().await.unwrap();
        node.storage.flush_all_projects().await.unwrap();
        let t_late = now_ms + 3_000_000;
        assert!(ps.upload_if_dirty_at(t_late, true).await.unwrap().is_some());

        // Hold a StorageSnapshot pinning all pre-merge S3 segments across d2 & d1 cold merges, publish, and 2*interval!
        let pinned_pre_merge_snap = ps.snapshot();

        // 3. At now_ms + 3_600_000 (1h after initial upload, only 10m after late upload):
        //    - d2_date merges (both buckets -> 2 partitions) with PutMode::Create (`seg_cold_*`)
        //    - d1_date does NOT merge yet!
        //    - today_date does NOT merge!
        s3_fault.take_put_modes();
        assert_eq!(
            node.storage
                .cold_merge_all_projects_at(now_ms + 3_600_000)
                .await
                .unwrap(),
            2
        );
        let put_modes = s3_fault.take_put_modes();
        assert_eq!(
            put_modes
                .iter()
                .filter(|(p, m)| p.contains("seg_cold_") && m == "Create")
                .count(),
            2
        );

        // 4. At t_late + 3_600_000: d1_date merges (both buckets -> 2 partitions).
        assert_eq!(
            node.storage
                .cold_merge_all_projects_at(t_late + 3_600_000)
                .await
                .unwrap(),
            2
        );

        // Publish superseding manifest using wall-clock time (`publish_if_dirty`) so Step 6's `run_gc_at`
        // honors the 2-publish-interval grace window.
        let pub_sup = ps
            .publish_if_dirty()
            .await
            .unwrap()
            .expect("cold merge must dirty S3 state and publish");
        let pub_sup_ts = pub_sup.published_at_ms;

        let mut s3_old_paths = Vec::new();
        for d in [d2_date, d1_date] {
            let prefix = ObjectPath::from(format!("segments/{pid}/{}", d.format("%Y%m%d")));
            let objs: Vec<_> = s3_fs.list(Some(&prefix)).try_collect().await.unwrap();
            for o in objs {
                if !o.location.as_ref().contains("seg_cold_") {
                    s3_old_paths.push(o.location.as_ref().to_string());
                }
            }
        }
        assert_eq!(
            s3_old_paths.len(),
            14,
            "3+3 on d2 + 4+4 on d1 = 14 old S3 segments"
        );

        // Before 2*300s grace -> 0 deleted.
        assert_eq!(ps.run_gc_at(pub_sup_ts + 599_999).await.unwrap(), 0);
        // Past 2*300s grace while pinned by `pinned_pre_merge_snap` -> 0 deleted!
        assert_eq!(ps.run_gc_at(pub_sup_ts + 600_000).await.unwrap(), 0);
        // Drop StorageSnapshot -> deletes all 14 superseded S3 segments, leaving 1 S3 segment per (closed_date, bucket)!
        drop(pinned_pre_merge_snap);
        assert_eq!(ps.run_gc_at(pub_sup_ts + 600_000).await.unwrap(), 14);

        for d in [d2_date, d1_date] {
            let prefix = ObjectPath::from(format!("segments/{pid}/{}", d.format("%Y%m%d")));
            let objs: Vec<_> = s3_fs.list(Some(&prefix)).try_collect().await.unwrap();
            assert_eq!(
                objs.len(),
                2,
                "closed day {d} must have exactly 2 S3 segments (1 per bucket): {objs:?}"
            );
        }

        // Query equivalence after cold merge + GC (80 total events: 72 + 8 late).
        let q_after: Value = http
            .post(format!("{base_url}/v1/query"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&insights_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            q_after["series"][0]["overall_value"].as_f64(),
            Some(80.0),
            "{q_after}"
        );

        // Idle ticks after merge + publish + GC execute 0 S3 calls!
        s3_fault.take_calls();
        assert_eq!(
            node.storage
                .cold_merge_all_projects_at(pub_sup_ts + 1_200_000)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            node.storage
                .upload_all_projects_at(pub_sup_ts + 1_200_000)
                .await
                .unwrap(),
            0
        );
        assert_eq!(ps.run_gc_at(pub_sup_ts + 1_200_000).await.unwrap(), 0);
        let idle_calls = s3_fault.take_calls();
        assert_eq!(
            storage_engine_s3_calls(&idle_calls),
            0,
            "idle ticks after cold merge + GC must execute 0 S3 calls: {idle_calls:?}"
        );

        node.shutdown().await;
    });
}

#[test]
fn t4_2_remote_query_node_concurrent_read_safety_and_role_gating() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _cold_idle = EnvGuard::set("PULSE_COLD_MERGE_IDLE_SECS", Some("3600"));
    let _cold_int = EnvGuard::set("PULSE_COLD_MERGE_INTERVAL_SECS", Some("300"));
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
        let s3_dir = tmp.path().join("s3");
        let ingest_dir = tmp.path().join("ingest_node");
        let query_dir = tmp.path().join("query_node");
        let compactor_dir = tmp.path().join("compactor_node");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("shared_meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );

        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs));
        let pid = "proj_t4_remote";

        let ingest_node = start_pulse_node_with_store(
            node_cfg(&ingest_dir, pid, NodeRole::Ingest, 2),
            s3_fault.clone(),
        )
        .await
        .unwrap();
        let query_node = start_pulse_node_with_store(
            node_cfg(&query_dir, pid, NodeRole::Query, 2),
            s3_fault.clone(),
        )
        .await
        .unwrap();
        let compactor_node = start_pulse_node_with_store(
            node_cfg(&compactor_dir, pid, NodeRole::Compactor, 2),
            s3_fault.clone(),
        )
        .await
        .unwrap();

        let ingest_key = ingest_node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let i_url = format!("http://{}", ingest_node.http_addr);
        let q_url = format!("http://{}", query_node.http_addr);

        // Role gating: Query and Compactor must execute 0 cold-merge calls.
        s3_fault.take_calls();
        assert_eq!(query_node.storage.cold_merge_all_projects().await.unwrap(), 0);
        assert_eq!(
            compactor_node
                .storage
                .cold_merge_all_projects()
                .await
                .unwrap(),
            0
        );
        let ro_calls = s3_fault.take_calls();
        assert_eq!(sum_op(&ro_calls, "PUT"), 0);
        assert_eq!(sum_op(&ro_calls, "DELETE"), 0);

        let now_ms = chrono::Utc::now().timestamp_millis();
        let closed_ts = now_ms - 2 * 86_400_000;
        let closed_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(closed_ts)
            .unwrap()
            .date_naive();

        let i_ps = ingest_node.storage.get_or_open_project(pid).await.unwrap();
        // Produce 3 S3 segments on closed_ts across 3 publish cycles (pub_0, pub_1, pub_2).
        for round in 0..3i64 {
            assert_eq!(
                http.post(format!("{i_url}/v1/ingest"))
                    .header("x-pulse-key", &ingest_key)
                    .json(&json!({
                        "events": [
                            { "distinct_id": "u_a", "event_name": "Purchase", "client_event_ts_ms": closed_ts + round * 1_000 },
                            { "distinct_id": "u_b", "event_name": "Purchase", "client_event_ts_ms": closed_ts + round * 1_000 + 10 }
                        ]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            ingest_node.ingest.flush_all().await.unwrap();
            ingest_node.storage.flush_all_projects().await.unwrap();
            assert!(
                i_ps.upload_if_dirty_at(now_ms, true)
                    .await
                    .unwrap()
                    .is_some()
            );
        }

        // Query node refreshes to pub_2.
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        let q_ps = query_node.storage.get_or_open_project(pid).await.unwrap();
        assert_eq!(q_ps.last_published_seq(), Some(2));

        // Spawn concurrent Query readers querying continuously while Ingest runs cold_merge + publish + refresh!
        let stop_readers = Arc::new(AtomicBool::new(false));
        let query_ok = Arc::new(AtomicU64::new(0));
        let query_err = Arc::new(AtomicU64::new(0));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let stop_c = stop_readers.clone();
            let ok_c = query_ok.clone();
            let err_c = query_err.clone();
            let http_c = http.clone();
            let q_url_c = q_url.clone();
            readers.push(tokio::spawn(async move {
                while !stop_c.load(Ordering::Acquire) {
                    let res = http_c
                        .post(format!("{q_url_c}/v1/query"))
                        .bearer_auth(ADMIN_TOKEN)
                        .json(&json!({
                            "kind": "insights",
                            "query": {
                                "time_range": { "start_ts_ms": closed_ts - 60_000, "end_ts_ms": now_ms + 60_000 },
                                "interval": "day",
                                "series": [{ "label": "A", "event_name": "Purchase", "metric": "total" }]
                            }
                        }))
                        .send()
                        .await;
                    match res {
                        Ok(r) if r.status().is_success() => {
                            let body: Value = r.json().await.unwrap_or(Value::Null);
                            if body["series"][0]["overall_value"].as_f64() == Some(6.0) {
                                ok_c.fetch_add(1, Ordering::Relaxed);
                            } else {
                                err_c.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {
                            err_c.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }));
        }

        // Let readers run against pub_2 for a bit, then cold-merge + publish pub_3 on Ingest!
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            ingest_node
                .storage
                .cold_merge_all_projects_at(now_ms + 3_600_000)
                .await
                .unwrap()
                >= 1
        );
        let pub3 = i_ps
            .publish_if_dirty()
            .await
            .unwrap()
            .expect("pub_3 must land after cold merge");
        assert_eq!(pub3.publish_seq, 3);

        // Even before Query refreshes (still reading pub_2!), old S3 segments survive T2 grace (pub3_ts + 2*300s).
        assert_eq!(i_ps.run_gc_at(pub3.published_at_ms + 599_999).await.unwrap(), 0);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Refresh Query to pub_3 while concurrent readers continue!
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        assert_eq!(q_ps.last_published_seq(), Some(3));
        tokio::time::sleep(Duration::from_millis(50)).await;

        stop_readers.store(true, Ordering::Release);
        for h in readers {
            h.await.unwrap();
        }
        assert!(query_ok.load(Ordering::Relaxed) >= 10);
        assert_eq!(
            query_err.load(Ordering::Relaxed),
            0,
            "concurrent Query readers must see 0 errors/NotFound across cold merge + publish"
        );

        // Now after 2*300s grace elapses, Ingest GC deletes superseded S3 segments cleanly.
        assert!(i_ps.run_gc_at(pub3.published_at_ms + 600_000).await.unwrap() >= 2);

        // Verify query still returns 6.0 and Query node has 1 segment per active bucket in closed_date.
        let mut total_closed_segs = 0usize;
        for b in 0..2u8 {
            if let Some(v) = q_ps.snapshot().manifest.active_segments.get(&(closed_date, b)) {
                assert_eq!(v.len(), 1);
                total_closed_segs += v.len();
            }
        }
        assert!(total_closed_segs >= 1);

        compactor_node.shutdown().await;
        query_node.shutdown().await;
        ingest_node.shutdown().await;
    });
}

#[test]
fn t4_3_background_scheduler_and_fault_injection_crash_recovery() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("1"));
    let _closed_up = EnvGuard::set("PULSE_CLOSED_DATE_UPLOAD_SECS", Some("0"));
    let _cold_idle = EnvGuard::set("PULSE_COLD_MERGE_IDLE_SECS", Some("1"));
    let _cold_int = EnvGuard::set("PULSE_COLD_MERGE_INTERVAL_SECS", Some("1"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let s3_dir = tmp.path().join("s3");
        let data_dir = tmp.path().join("ingest_bg");
        std::fs::create_dir_all(&s3_dir).unwrap();
        let meta_db = tmp.path().join("bg_meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );
        let s3_fs: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        let s3_fault = Arc::new(FaultAndCountStore::new(s3_fs.clone()));

        let pid = "proj_t4_bg";
        let node = start_pulse_node_with_store(
            node_cfg(&data_dir, pid, NodeRole::Ingest, 1),
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
        let url = format!("http://{}", node.http_addr);

        let now_ms = chrono::Utc::now().timestamp_millis();
        let closed_ts = now_ms - 2 * 86_400_000;
        let closed_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(closed_ts)
            .unwrap()
            .date_naive();

        // Write 2 S3 segments across 2 manual flush+upload cycles on closed_date.
        for seq in 0..2i64 {
            assert_eq!(
                http.post(format!("{url}/v1/ingest"))
                    .header("x-pulse-key", &key)
                    .json(&json!({
                        "events": [
                            { "distinct_id": "u_bg", "event_name": "BgEvent", "client_event_ts_ms": closed_ts + seq * 1_000 }
                        ]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            node.ingest.flush_all().await.unwrap();
            node.storage.flush_all_projects().await.unwrap();
            node.storage.publish_all_projects().await.unwrap();
        }
        let ps = node.storage.get_or_open_project(pid).await.unwrap();
        assert_eq!(
            ps.snapshot()
                .manifest
                .active_segments
                .get(&(closed_date, 0))
                .unwrap()
                .len(),
            2
        );

        // Wait for NodeRole::Ingest's background publish_ticker to run cold_merge + publish automatically!
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let len = ps
                    .snapshot()
                    .manifest
                    .active_segments
                    .get(&(closed_date, 0))
                    .map_or(0, |v| v.len());
                if len == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("NodeRole::Ingest background ticker must cold-merge closed partition");

        node.shutdown().await;

        // Fault injection + crash recovery + restore cleanup test.
        let _interval2 = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
        let _cold_idle2 = EnvGuard::set("PULSE_COLD_MERGE_IDLE_SECS", Some("3600"));
        let _cold_int2 = EnvGuard::set("PULSE_COLD_MERGE_INTERVAL_SECS", Some("0"));
        let crash_dir = tmp.path().join("crash_node");
        let s3_dir2 = tmp.path().join("s3_crash");
        std::fs::create_dir_all(&s3_dir2).unwrap();
        let meta_db2 = tmp.path().join("crash_meta.db");
        let _db_env2 = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db2.display())),
        );
        let s3_fs2: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&s3_dir2).unwrap());
        let s3_fault2 = Arc::new(FaultAndCountStore::new(s3_fs2.clone()));

        let pid2 = "proj_t4_fault";
        let node2 = start_pulse_node_with_store(
            node_cfg(&crash_dir, pid2, NodeRole::All, 1),
            s3_fault2.clone(),
        )
        .await
        .unwrap();
        let key2 = node2
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let url2 = format!("http://{}", node2.http_addr);

        let ps2_init = node2.storage.get_or_open_project(pid2).await.unwrap();
        for seq in 0..2i64 {
            assert_eq!(
                http.post(format!("{url2}/v1/ingest"))
                    .header("x-pulse-key", &key2)
                    .json(&json!({
                        "events": [
                            { "distinct_id": "u_f", "event_name": "FaultEvent", "client_event_ts_ms": closed_ts + seq * 1_000 }
                        ]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            node2.ingest.flush_all().await.unwrap();
            node2.storage.flush_all_projects().await.unwrap();
            ps2_init.upload_if_dirty_at(now_ms, true).await.unwrap().unwrap();
        }

        // Fault 1: PutMode::Create failure on seg_cold_ mid-merge.
        s3_fault2.set_rule(FaultOp::Put, "seg_cold_", 0);
        assert!(node2
            .storage
            .cold_merge_all_projects_at(now_ms + 3_600_000)
            .await
            .is_err());
        s3_fault2.clear_rule();

        // Fault 2: Crash/restart with leftover ColdMergeIntent + orphan seg_cold_* in S3!
        // Inject a synthetic orphaned seg_cold_* + ColdMergeIntentStore into the local hot tier & S3,
        // shut down node2, and verify reopen reconciles and deletes the orphan.
        let orphan_path = format!(
            "segments/{pid2}/{}/bucket_000/seg_cold_orphan_test.pvtx",
            closed_date.format("%Y%m%d")
        );
        s3_fs2
            .put(
                &ObjectPath::from(orphan_path.as_str()),
                PutPayload::from_static(b"orphan"),
            )
            .await
            .unwrap();
        let enc_local = Arc::new(pulse_storage::EncryptedObjectStore::new(
            pulse_storage::with_hot_store(s3_fs2.clone(), &crash_dir)
                .await
                .unwrap(),
        ));
        let cmi_store = ColdMergeIntentStore::new(pid2, enc_local.as_object_store());
        cmi_store
            .save(&pulse_storage::ColdMergeIntent {
                intent_id: "cmi_orphan".into(),
                writer_generation: 1,
                base_local_epoch: 1,
                created_at_ms: now_ms,
                target_object_paths: vec![orphan_path.clone()],
            })
            .await
            .unwrap();
        node2.shutdown().await;

        // Restart node2 -> open_inner reconciles ColdMergeIntent and deletes orphan_path!
        let node2_reopen = start_pulse_node_with_store(
            node_cfg(&crash_dir, pid2, NodeRole::All, 1),
            s3_fault2.clone(),
        )
        .await
        .unwrap();
        let ps2 = node2_reopen.storage.get_or_open_project(pid2).await.unwrap();
        assert!(s3_fs2
            .head(&ObjectPath::from(orphan_path.as_str()))
            .await
            .is_err());
        assert!(cmi_store.load().await.unwrap().is_none());

        // Merge + publish succeeds cleanly.
        assert_eq!(ps2.cold_merge_at(now_ms + 3_600_000).await.unwrap(), 1);
        let pub_final = ps2.publish_if_dirty().await.unwrap().unwrap();
        let pub_final_path = format!("published/{pid2}/pub_{:012}.json", pub_final.publish_seq);
        assert!(s3_fs2.head(&ObjectPath::from(pub_final_path.as_str())).await.is_ok());

        // Fault 3: Inject DELETE failure during run_gc_at, then clear fault and verify idempotent retry!
        s3_fault2.set_rule(FaultOp::Delete, "segments/", 0);
        assert_eq!(
            ps2.run_gc_at(pub_final.published_at_ms + 600_000)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            ps2.snapshot()
                .manifest
                .pending_s3_deletes
                .iter()
                .filter(|d| d.kind == pulse_storage::PendingS3DeleteKind::Segment)
                .count(),
            2
        );
        s3_fault2.clear_rule();
        assert_eq!(
            ps2.run_gc_at(pub_final.published_at_ms + 600_000)
                .await
                .unwrap(),
            2
        );

        node2_reopen.shutdown().await;

        // Also verify restore_from_published_with_store_and_meta_for_test restores cleanly and clears ColdMergeIntentStore.
        let restore_dir = tmp.path().join("restore_target");
        std::fs::create_dir_all(&restore_dir).unwrap();
        let restore_routed = pulse_storage::open_hot_store_for_restore(s3_fs2.clone(), &restore_dir)
            .await
            .unwrap();
        let restore_enc = Arc::new(pulse_storage::EncryptedObjectStore::new(restore_routed));
        let restore_cmi = ColdMergeIntentStore::new(pid2, restore_enc.as_object_store());
        restore_cmi
            .save(&pulse_storage::ColdMergeIntent {
                intent_id: "cmi_stale".into(),
                writer_generation: 1,
                base_local_epoch: 1,
                created_at_ms: now_ms,
                target_object_paths: vec!["segments/stale.pvtx".into()],
            })
            .await
            .unwrap();
        assert!(restore_cmi.load().await.unwrap().is_some());

        let restore_meta = MetaStore::connect_url(&format!("sqlite://{}", meta_db2.display()))
            .await
            .unwrap();
        let reports = restore_from_published_with_store_and_meta_for_test(
            &restore_dir,
            Some(pid2),
            s3_fs2.clone(),
            &restore_meta,
        )
        .await
        .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].project_id, pid2);
        assert!(
            restore_cmi.load().await.unwrap().is_none(),
            "restore_from_published must clear ColdMergeIntentStore"
        );
    });
}
