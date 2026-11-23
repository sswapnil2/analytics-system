//! T2 integration tests for Feature #17: remote `NodeRole::Query` reading published S3 state:
//! - Cold-start with a brand-new empty `<data_dir>` (no shared local volume) + BYOK `PLSEAGM2`
//! - 0 S3 `PUT` / `DELETE` calls and 0 `writer_leases` rows on `NodeRole::Query`
//! - Clean `None -> pub_0 -> pub_1` published state transitions
//! - `ColumnarProfileIndex` consistency + live cross-node `erased_uids` hiding without republish

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_control::MetaStore;
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole};
use serde_json::{json, Value};

const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST_VAULT_KEK: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
const ADMIN_TOKEN: &str = "sk_root_t2_remote_query_node_test_token_0000000000";

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

#[derive(Debug, Default)]
struct CountingS3 {
    inner: InMemory,
    calls: Mutex<BTreeMap<(String, &'static str), u64>>,
}

impl CountingS3 {
    fn count(&self, op: &'static str, p: Option<&ObjectPath>) {
        let prefix = p
            .and_then(|p| p.parts().next())
            .map_or("(root)".to_string(), |s| s.as_ref().to_string());
        *self.calls.lock().unwrap().entry((prefix, op)).or_default() += 1;
    }

    fn take(&self) -> BTreeMap<(String, &'static str), u64> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
}

impl fmt::Display for CountingS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingS3")
    }
}

#[async_trait]
impl ObjectStore for CountingS3 {
    async fn put_opts(&self, l: &ObjectPath, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
        self.count("PUT", Some(l));
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.count("PUT", Some(l));
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
        self.count("GET", Some(l));
        self.inner.get_opts(l, o).await
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
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.count("PUT", Some(to));
        self.inner.copy_if_not_exists(from, to).await
    }
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

#[test]
fn t2_17_remote_query_node_reads_published_state_with_byok_and_live_erasure() {
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
        let ingest_dir = tmp.path().join("ingest_node");
        let query_dir = tmp.path().join("remote_query_node");
        let meta_db = tmp.path().join("shared_meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );

        let s3_counting = Arc::new(CountingS3::default());
        let s3_raw: Arc<dyn ObjectStore> = s3_counting.clone();
        let pid = "proj_remote_q";

        let ingest_node = start_pulse_node_with_store(
            node_cfg(&ingest_dir, pid, NodeRole::Ingest),
            s3_raw.clone(),
        )
        .await
        .unwrap();
        let ingest_key = ingest_node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        // 1. Start remote NodeRole::Query on a completely separate, empty <data_dir> BEFORE any publish.
        s3_counting.take();
        let query_node = start_pulse_node_with_store(
            node_cfg(&query_dir, pid, NodeRole::Query),
            s3_raw.clone(),
        )
        .await
        .unwrap();
        let q_boot_calls = s3_counting.take();
        assert_eq!(sum_op(&q_boot_calls, "PUT"), 0, "Query node must never PUT");
        assert_eq!(
            sum_op(&q_boot_calls, "DELETE"),
            0,
            "Query node must never DELETE"
        );
        assert!(query_dir.join("query_hot").is_dir());

        let http = reqwest::Client::new();
        let q_url = format!("http://{}", query_node.http_addr);
        let i_url = format!("http://{}", ingest_node.http_addr);

        // Before pub_0: Query answers 200 OK with 0 rows.
        let pre_status: Value = http
            .get(format!("{q_url}/v1/status"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pre_status["l1_rows"].as_u64(), Some(0));

        // 2. Writer ingests events + identify + engage, then flushes + publishes pub_0.
        let now_ms = chrono::Utc::now().timestamp_millis();
        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "anon_0", "event_name": "App_Open", "client_event_ts_ms": now_ms - 5_000 },
                        { "distinct_id": "anon_0", "event_name": "KYC_Submitted", "client_event_ts_ms": now_ms - 3_000 },
                        { "distinct_id": "u_erase", "event_name": "App_Open", "client_event_ts_ms": now_ms - 4_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/identify"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({ "anon_id": "anon_0", "user_id": "user_0" }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "user_0", "event_name": "Activated", "client_event_ts_ms": now_ms - 1_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/engage"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!([
                    { "$distinct_id": "user_0", "$set": { "plan": "gold" }, "$track_charge": { "$amount": 100.0 } },
                    { "$distinct_id": "u_erase", "$set": { "plan": "gold" }, "$track_charge": { "$amount": 50.0 } }
                ]))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest_node.ingest.flush_all().await.unwrap();
        ingest_node.storage.flush_all_projects().await.unwrap();
        assert_eq!(ingest_node.storage.publish_all_projects().await.unwrap(), 1);

        // Verify S3 has 0 identity/, profiles/, or manifests/ objects (local hot tier!).
        for local_prefix in ["identity", "profiles", "manifests"] {
            let objs: Vec<_> = s3_raw
                .list(Some(&ObjectPath::from(local_prefix)))
                .try_collect()
                .await
                .unwrap();
            assert!(objs.is_empty(), "S3 must have 0 {local_prefix}/ objects");
        }

        // 3. Refresh Query node -> discovers pub_0, restores encrypted Candidate B snapshots to query_hot.
        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        let q_refresh_calls = s3_counting.take();
        assert_eq!(sum_op(&q_refresh_calls, "PUT"), 0);
        assert_eq!(sum_op(&q_refresh_calls, "DELETE"), 0);

        // Query status, funnel, user.plan filter (ColumnarProfileIndex), and users list.
        let post_status: Value = http
            .get(format!("{q_url}/v1/status"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(post_status["l1_rows"].as_u64(), Some(4));

        let funnel: Value = http
            .post(format!("{q_url}/v1/query"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "kind": "funnel",
                "query": {
                    "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                    "steps": [
                        { "event_name": "App_Open" },
                        { "event_name": "KYC_Submitted" },
                        { "event_name": "Activated" }
                    ],
                    "order_mode": "LooseOrder",
                    "conversion_window_ms": 86_400_000
                }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(funnel["overall_steps"][0]["entered_users"].as_u64(), Some(2));
        assert_eq!(funnel["overall_steps"][2]["entered_users"].as_u64(), Some(1));

        let gold_query = || async {
            let v: Value = http
                .post(format!("{q_url}/v1/query"))
                .bearer_auth(ADMIN_TOKEN)
                .json(&json!({
                    "kind": "insights",
                    "query": {
                        "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                        "interval": "day",
                        "series": [{
                            "label": "A",
                            "event_name": "App_Open",
                            "metric": "uniques",
                            "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "gold" } }]
                        }]
                    }
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v["series"][0]["overall_value"].as_f64().unwrap_or(0.0)
        };
        assert_eq!(gold_query().await, 2.0);

        // Idle refresh on Query makes 0 GETs / 0 PUTs / 0 DELETEs.
        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 0);
        let q_idle_calls = s3_counting.take();
        assert_eq!(sum_op(&q_idle_calls, "PUT"), 0);
        assert_eq!(sum_op(&q_idle_calls, "DELETE"), 0);
        assert_eq!(sum_op(&q_idle_calls, "GET"), 0);

        // 4. Transition pub_0 -> pub_1: hold an in-flight read-only ProjectProfileStore handle across
        //    the refresh to verify `pub_0` is NOT clobbered while in use, and IS cleaned up once dropped.
        fn count_files_recursive(dir: &Path) -> usize {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return 0;
            };
            let mut count = 0;
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
        let pub0_hot_dir = query_dir.join(format!("query_hot/profiles/{pid}/pub_0"));
        assert!(count_files_recursive(&pub0_hot_dir) > 0);
        let held_pub0_store = query_node.storage.profile_store(pid, false).await.unwrap();

        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_new", "event_name": "App_Open", "client_event_ts_ms": now_ms + 1_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/engage"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({ "distinct_id": "u_new", "$set": { "plan": "gold" } }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        ingest_node.ingest.flush_all().await.unwrap();
        ingest_node.storage.flush_all_projects().await.unwrap();
        assert_eq!(ingest_node.storage.publish_all_projects().await.unwrap(), 1);

        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        assert_eq!(gold_query().await, 3.0);

        // While `held_pub0_store` is still alive, `pub_0` files must remain intact and readable.
        assert!(count_files_recursive(&pub0_hot_dir) > 0);
        assert!(held_pub0_store
            .get_by_distinct_id("user_0")
            .await
            .unwrap()
            .is_some());
        drop(held_pub0_store);

        // Once the in-flight handle drops, the next refresh cleans up superseded `pub_0` files
        // and prunes the empty `pub_0/` directory stub while keeping `profiles/{pid}`.
        assert_eq!(query_node.run_query_refresh_tick().await, 0);
        assert_eq!(
            count_files_recursive(&pub0_hot_dir),
            0,
            "superseded query_hot/profiles/{pid}/pub_0 files must be deleted after reader drains"
        );
        assert!(
            !pub0_hot_dir.exists(),
            "empty pub_0 dir stub must be pruned after reader drains"
        );
        assert!(query_dir.join(format!("query_hot/profiles/{pid}")).is_dir());
        let pub1_hot_dir = query_dir.join(format!("query_hot/profiles/{pid}/pub_1"));
        assert!(count_files_recursive(&pub1_hot_dir) > 0);

        // 5. Cross-node erasure WITHOUT republishing pub_2:
        //    `u_erase` is erased in shared MetaStore; Query node hides `u_erase` across queries,
        //    ColumnarProfileIndex (`user.plan == "gold"`), user detail, users list, and revenue.
        let uid_erase = ingest_node
            .storage
            .identity_graph(pid, false)
            .await
            .unwrap()
            .lookup_raw("u_erase")
            .expect("u_erase must exist in writer identity graph");
        ingest_node
            .control
            .meta()
            .insert_erased_uids(pid, &[uid_erase], now_ms + 10_000)
            .await
            .unwrap();

        assert_eq!(query_node.run_query_refresh_tick().await, 0);
        assert_eq!(gold_query().await, 2.0);

        let users_resp: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/users"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let dids: Vec<&str> = users_resp
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|u| u["distinct_id"].as_str())
            .collect();
        assert!(!dids.contains(&"u_erase"), "erased user must be hidden: {dids:?}");
        assert!(dids.contains(&"user_0"));
        assert!(dids.contains(&"u_new"));

        let detail_resp = http
            .get(format!("{q_url}/v1/projects/{pid}/users/u_erase"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(detail_resp.status(), 200);
        let detail_json: Value = detail_resp.json().await.unwrap();
        assert!(
            detail_json["profile"].is_null(),
            "erased user profile must be null: {detail_json}"
        );
        assert!(
            detail_json["events"].as_array().unwrap().is_empty(),
            "erased user events must be empty: {detail_json}"
        );

        let rev: Value = http
            .get(format!("{q_url}/v1/projects/{pid}/revenue"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rev["total_revenue"].as_f64(), Some(100.0));

        // 6. Verify Query node never took a writer lease and Ingest node was never fenced.
        let meta = MetaStore::connect_auto(&query_dir).await.unwrap();
        let probe_lease = meta
            .acquire_writer_lease(
                pid,
                "probe_non_owner",
                chrono::Utc::now().timestamp_millis(),
                chrono::Utc::now().timestamp_millis() + 10_000,
            )
            .await
            .unwrap();
        assert!(
            probe_lease.is_none(),
            "ingest node must still hold the active writer lease"
        );
        assert_eq!(
            http.post(format!("{i_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [{ "distinct_id": "u_after", "event_name": "App_Open", "client_event_ts_ms": now_ms + 2_000 }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        // 7. Also verify a second brand-new Query node starting AFTER pub_1 discovers pub_1 on first open!
        let query_dir2 = tmp.path().join("remote_query_node_2");
        let query_node2 = start_pulse_node_with_store(
            node_cfg(&query_dir2, pid, NodeRole::Query),
            s3_raw.clone(),
        )
        .await
        .unwrap();
        let q2_url = format!("http://{}", query_node2.http_addr);
        let q2_status: Value = http
            .get(format!("{q2_url}/v1/status"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q2_status["l1_rows"].as_u64(), Some(5));
        let q2_gold: Value = http
            .post(format!("{q2_url}/v1/query"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "kind": "insights",
                "query": {
                    "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                    "interval": "day",
                    "series": [{
                        "label": "A",
                        "event_name": "App_Open",
                        "metric": "uniques",
                        "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "gold" } }]
                    }]
                }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(q2_gold["series"][0]["overall_value"].as_f64(), Some(2.0));

        query_node2.shutdown().await;
        query_node.shutdown().await;
        ingest_node.shutdown().await;
    });
}

#[test]
fn t2_17_remote_query_node_dynamic_byok_rotation_and_profile_index_single_tick() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const TEST_BYOK_KEY_2: &str =
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _qtick = EnvGuard::set("PULSE_QUERY_REFRESH_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", None);
    let _kek = EnvGuard::set("PULSE_VAULT_KEK", Some(TEST_VAULT_KEK));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let writer_dir = tmp.path().join("writer_node");
        let query_dir = tmp.path().join("query_node");
        let ingest_peer_dir = tmp.path().join("ingest_peer_node");
        let meta_db = tmp.path().join("shared_meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );

        let s3_counting = Arc::new(CountingS3::default());
        let s3_raw: Arc<dyn ObjectStore> = s3_counting.clone();
        let pid = "proj_dyn_byok";

        let writer_node = start_pulse_node_with_store(
            node_cfg(&writer_dir, pid, NodeRole::All),
            s3_raw.clone(),
        )
        .await
        .unwrap();
        let ingest_key = writer_node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        let query_node = start_pulse_node_with_store(
            node_cfg(&query_dir, pid, NodeRole::Query),
            s3_raw.clone(),
        )
        .await
        .unwrap();

        // Also boot a split-role Ingest peer to verify reload_caches_from_db() picks up
        // rotated BYOK keys on non-Query roles.
        let ingest_peer = start_pulse_node_with_store(
            node_cfg(&ingest_peer_dir, pid, NodeRole::Ingest),
            s3_raw.clone(),
        )
        .await
        .unwrap();

        assert!(!query_node.storage.byok_encryption_status().enabled);
        assert!(!ingest_peer.storage.byok_encryption_status().enabled);

        let http = reqwest::Client::new();
        let w_url = format!("http://{}", writer_node.http_addr);
        let q_url = format!("http://{}", query_node.http_addr);

        let pre_status: Value = http
            .get(format!("{q_url}/v1/status?project_id={pid}"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pre_status["l1_rows"].as_u64(), Some(0));

        // 1. Dynamically rotate BYOK key on Writer via API (PULSE_BYOK_MASTER_KEY is unset).
        let rot_resp = http
            .post(format!("{w_url}/v1/system/encryption/rotate"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({ "secret": TEST_BYOK_KEY }))
            .send()
            .await
            .unwrap();
        assert_eq!(rot_resp.status(), 200);
        let rot_json: Value = rot_resp.json().await.unwrap();
        assert_eq!(rot_json["status"]["enabled"].as_bool(), Some(true));

        // Verify non-published-reader node (Ingest) also picks up the rotated key via reload_caches_from_db().
        ingest_peer.control.reload_caches_from_db().await.unwrap();
        assert!(
            ingest_peer.storage.byok_encryption_status().enabled,
            "non-Query role must also pick up rotated BYOK key from MetaStore"
        );

        let now_ms = chrono::Utc::now().timestamp_millis();
        assert_eq!(
            http.post(format!("{w_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_rot_1", "event_name": "Signup", "client_event_ts_ms": now_ms - 5_000 },
                        { "distinct_id": "u_rot_2", "event_name": "Signup", "client_event_ts_ms": now_ms - 4_000 },
                        { "distinct_id": "u_free_0", "event_name": "Signup", "client_event_ts_ms": now_ms - 3_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{w_url}/v1/engage"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!([
                    { "$distinct_id": "u_rot_1", "$set": { "plan": "enterprise" } },
                    { "$distinct_id": "u_rot_2", "$set": { "plan": "enterprise" } },
                    { "$distinct_id": "u_free_0", "$set": { "plan": "free" } }
                ]))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        writer_node.ingest.flush_all().await.unwrap();
        writer_node.storage.flush_all_projects().await.unwrap();
        assert_eq!(writer_node.storage.publish_all_projects().await.unwrap(), 1);

        let pub0_bytes = s3_raw
            .get(&ObjectPath::from(format!(
                "published/{pid}/pub_000000000000.json"
            )))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            pub0_bytes.starts_with(b"PLSEAGM2"),
            "published manifest must be encrypted with rotated BYOK key"
        );

        // 2. Run ONE Query tick and assert immediate BYOK decrypt + ColumnarProfileIndex consistency.
        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        let q_calls_pub0 = s3_counting.take();
        assert_eq!(sum_op(&q_calls_pub0, "PUT"), 0);
        assert_eq!(sum_op(&q_calls_pub0, "DELETE"), 0);

        assert_eq!(
            query_node
                .storage
                .maintenance_errors()
                .manifest_refresh
                .load(Ordering::Relaxed),
            0
        );
        let metrics_text = http
            .get(format!("{q_url}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            metrics_text.contains("pulse_maintenance_errors_total{kind=\"manifest_refresh\"} 0"),
            "metrics must report 0 manifest_refresh errors: {metrics_text}"
        );

        let status_pub0: Value = http
            .get(format!("{q_url}/v1/status?project_id={pid}"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status_pub0["l1_rows"].as_u64(), Some(3));

        let insights_series = |target_pid: &str, series: Value| {
            let http = http.clone();
            let q_url = q_url.clone();
            let target_pid = target_pid.to_string();
            async move {
                let resp = http
                    .post(format!("{q_url}/v1/projects/{target_pid}/query/insights"))
                    .bearer_auth(ADMIN_TOKEN)
                    .json(&json!({
                        "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                        "interval": "day",
                        "series": series
                    }))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
                resp.json::<Value>().await.unwrap()
            }
        };

        let pub0_res = insights_series(
            pid,
            json!([
                { "label": "ALL", "event_name": "Signup", "metric": "uniques", "filters": [] },
                { "label": "ENT", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "enterprise" } }] },
                { "label": "FREE", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "free" } }] }
            ]),
        )
        .await;
        assert_eq!(pub0_res["series"][0]["overall_value"].as_f64(), Some(3.0));
        assert_eq!(pub0_res["series"][1]["overall_value"].as_f64(), Some(2.0));
        assert_eq!(pub0_res["series"][2]["overall_value"].as_f64(), Some(1.0));

        // 3. Dynamic new project discovery on Query node via a single tick.
        let create_proj = http
            .post(format!("{w_url}/v1/projects"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({ "name": "Dynamic Project 2" }))
            .send()
            .await
            .unwrap();
        assert_eq!(create_proj.status(), 200);
        let create_json: Value = create_proj.json().await.unwrap();
        let pid2 = create_json["project"]["id"].as_str().unwrap().to_string();
        let ingest_key_2 = create_json["api_key"]["plaintext_key"]
            .as_str()
            .unwrap()
            .to_string();

        assert_eq!(
            http.post(format!("{w_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key_2)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_p2_1", "event_name": "Signup", "client_event_ts_ms": now_ms }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{w_url}/v1/engage"))
                .header("x-pulse-key", &ingest_key_2)
                .json(&json!({ "distinct_id": "u_p2_1", "$set": { "plan": "pro" } }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        writer_node.ingest.flush_all().await.unwrap();
        writer_node.storage.flush_all_projects().await.unwrap();
        assert_eq!(writer_node.storage.publish_all_projects().await.unwrap(), 1);

        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        let q_calls_p2 = s3_counting.take();
        assert_eq!(sum_op(&q_calls_p2, "PUT"), 0);
        assert_eq!(sum_op(&q_calls_p2, "DELETE"), 0);
        let status_p2: Value = http
            .get(format!("{q_url}/v1/status?project_id={pid2}"))
            .bearer_auth(ADMIN_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status_p2["l1_rows"].as_u64(), Some(1));
        let p2_res = insights_series(
            &pid2,
            json!([
                { "label": "PRO", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "pro" } }] }
            ]),
        )
        .await;
        assert_eq!(p2_res["series"][0]["overall_value"].as_f64(), Some(1.0));

        // 4. Multi-thread concurrent insights + GET /users stress across pub_0 -> pub_1 -> pub_2
        //    with a 2nd BYOK key rotation before pub_1, asserting 0 skew hits and 0 HTTP errors.
        let stop_hammer = Arc::new(AtomicBool::new(false));
        let total_ok = Arc::new(AtomicU64::new(0));
        let http_errors = Arc::new(AtomicU64::new(0));
        let skew_hits = Arc::new(AtomicU64::new(0));

        let mut workers = Vec::new();
        for tid in 0..6usize {
            let http = http.clone();
            let q_url = q_url.clone();
            let stop = stop_hammer.clone();
            let total_ok = total_ok.clone();
            let http_errors = http_errors.clone();
            let skew_hits = skew_hits.clone();
            workers.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if tid % 3 == 0 {
                        match http
                            .get(format!("{q_url}/v1/projects/{pid}/users"))
                            .bearer_auth(ADMIN_TOKEN)
                            .send()
                            .await
                        {
                            Ok(r) if r.status() == 200 => {
                                if r.json::<Value>().await.is_ok() {
                                    total_ok.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    http_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            _ => {
                                http_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        let resp = http
                            .post(format!("{q_url}/v1/projects/{pid}/query/insights"))
                            .bearer_auth(ADMIN_TOKEN)
                            .json(&json!({
                                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                                "interval": "day",
                                "series": [
                                    { "label": "ALL", "event_name": "Signup", "metric": "uniques", "filters": [] },
                                    { "label": "ENT", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "enterprise" } }] },
                                    { "label": "FREE", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "free" } }] }
                                ]
                            }))
                            .send()
                            .await;
                        match resp {
                            Ok(r) if r.status() == 200 => {
                                if let Ok(v) = r.json::<Value>().await {
                                    let all = v["series"][0]["overall_value"].as_f64().unwrap_or(-1.0);
                                    let ent = v["series"][1]["overall_value"].as_f64().unwrap_or(-1.0);
                                    let free = v["series"][2]["overall_value"].as_f64().unwrap_or(-1.0);
                                    if (ent + free - all).abs() > 1e-6 || !(3.0..=5.0).contains(&all) {
                                        skew_hits.fetch_add(1, Ordering::Relaxed);
                                    }
                                    total_ok.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    http_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            _ => {
                                http_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }));
        }

        // Rotate 2nd BYOK key and publish pub_1 while concurrent queries hammer Query node.
        let rot2 = http
            .post(format!("{w_url}/v1/system/encryption/rotate"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({ "secret": TEST_BYOK_KEY_2 }))
            .send()
            .await
            .unwrap();
        assert_eq!(rot2.status(), 200);
        assert_eq!(
            http.post(format!("{w_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_rot_3", "event_name": "Signup", "client_event_ts_ms": now_ms + 1_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{w_url}/v1/engage"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({ "distinct_id": "u_rot_3", "$set": { "plan": "enterprise" } }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        writer_node.ingest.flush_all().await.unwrap();
        writer_node.storage.flush_all_projects().await.unwrap();
        assert_eq!(writer_node.storage.publish_all_projects().await.unwrap(), 1);
        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        let q_calls_pub1 = s3_counting.take();
        assert_eq!(sum_op(&q_calls_pub1, "PUT"), 0);
        assert_eq!(sum_op(&q_calls_pub1, "DELETE"), 0);

        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        // Publish pub_2 while concurrent queries continue.
        assert_eq!(
            http.post(format!("{w_url}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_rot_4", "event_name": "Signup", "client_event_ts_ms": now_ms + 2_000 }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            http.post(format!("{w_url}/v1/engage"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({ "distinct_id": "u_rot_4", "$set": { "plan": "enterprise" } }))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        writer_node.ingest.flush_all().await.unwrap();
        writer_node.storage.flush_all_projects().await.unwrap();
        assert_eq!(writer_node.storage.publish_all_projects().await.unwrap(), 1);
        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 1);
        let q_calls_pub1 = s3_counting.take();
        assert_eq!(sum_op(&q_calls_pub1, "PUT"), 0);
        assert_eq!(sum_op(&q_calls_pub1, "DELETE"), 0);

        tokio::time::sleep(std::time::Duration::from_millis(25)).await;

        stop_hammer.store(true, Ordering::Relaxed);
        for w in workers {
            w.await.unwrap();
        }

        assert_eq!(http_errors.load(Ordering::Relaxed), 0);
        assert_eq!(
            skew_hits.load(Ordering::Relaxed),
            0,
            "ColumnarProfileIndex must never lag StorageEngine across pub_0 -> pub_1 -> pub_2"
        );
        assert!(total_ok.load(Ordering::Relaxed) > 0);

        // Final consistency & cleanup check on pub_2.
        s3_counting.take();
        assert_eq!(query_node.run_query_refresh_tick().await, 0);
        let q_calls_final = s3_counting.take();
        assert_eq!(sum_op(&q_calls_final, "PUT"), 0);
        assert_eq!(sum_op(&q_calls_final, "DELETE"), 0);
        let pub2_res = insights_series(
            pid,
            json!([
                { "label": "ALL", "event_name": "Signup", "metric": "uniques", "filters": [] },
                { "label": "ENT", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "enterprise" } }] },
                { "label": "FREE", "event_name": "Signup", "metric": "uniques", "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "free" } }] }
            ]),
        )
        .await;
        assert_eq!(pub2_res["series"][0]["overall_value"].as_f64(), Some(5.0));
        assert_eq!(pub2_res["series"][1]["overall_value"].as_f64(), Some(4.0));
        assert_eq!(pub2_res["series"][2]["overall_value"].as_f64(), Some(1.0));
        assert_eq!(
            query_node
                .storage
                .maintenance_errors()
                .manifest_refresh
                .load(Ordering::Relaxed),
            0
        );
        assert!(!query_dir.join(format!("query_hot/profiles/{pid}/pub_0")).exists());
        assert!(!query_dir.join(format!("query_hot/profiles/{pid}/pub_1")).exists());
        assert!(query_dir.join(format!("query_hot/profiles/{pid}/pub_2")).is_dir());

        ingest_peer.shutdown().await;
        query_node.shutdown().await;
        writer_node.shutdown().await;
    });
}
