//! P0-7 (T0a + T0b): which segment-store calls a request makes before it is acknowledged, and what
//! an idle node costs. Counts calls by top-level key prefix in each window and prints a table;
//! asserts that with WAL, identity, and profiles on local disk (`PULSE_HOT_STORE=local`), no
//! acknowledged request writes to or reads from the segment store.
//! Own test binary: it sets `PULSE_HOT_STORE`.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole, RunningNode};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const ADMIN: &str = "sk_root_storecallstest0000000000000000000000000000";
type Calls = BTreeMap<(String, &'static str), u64>;

const HOT_PREFIXES: [&str; 8] = [
    "wal",
    "wal_corrupt",
    "wal_dead",
    "agent_wal",
    "agent_wal_corrupt",
    "identity",
    "profiles",
    "manifests",
];

struct HotStoreEnvGuard {
    prev: Option<String>,
}

impl HotStoreEnvGuard {
    fn set(val: &str) -> Self {
        let prev = std::env::var("PULSE_HOT_STORE").ok();
        std::env::set_var("PULSE_HOT_STORE", val);
        Self { prev }
    }
}

impl Drop for HotStoreEnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("PULSE_HOT_STORE", v),
            None => std::env::remove_var("PULSE_HOT_STORE"),
        }
    }
}

#[derive(Debug, Default)]
struct Counting {
    inner: InMemory,
    calls: Mutex<Calls>,
}

impl Counting {
    fn count(&self, op: &'static str, p: Option<&ObjectPath>) {
        let prefix = p
            .and_then(|p| p.parts().next())
            .map_or("(root)".to_string(), |s| s.as_ref().to_string());
        *self.calls.lock().unwrap().entry((prefix, op)).or_default() += 1;
    }
    fn take(&self) -> Calls {
        std::mem::take(&mut self.calls.lock().unwrap())
    }
}

impl fmt::Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counting")
    }
}

#[async_trait]
impl ObjectStore for Counting {
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

fn print(label: &str, calls: &Calls) {
    eprintln!("--- {label}");
    for ((prefix, op), n) in calls {
        eprintln!("    {prefix:<18} {op:<6} {n}");
    }
}

fn prefix_puts(calls: &Calls, target_prefix: &str) -> u64 {
    calls
        .iter()
        .filter(|((prefix, op), _)| *op == "PUT" && prefix == target_prefix)
        .map(|(_, n)| n)
        .sum()
}

fn hot_calls(calls: &Calls) -> u64 {
    calls
        .iter()
        .filter(|((prefix, _), _)| HOT_PREFIXES.contains(&prefix.as_str()))
        .map(|(_, n)| n)
        .sum()
}

fn config(dir: &Path, role: NodeRole) -> NodeConfig {
    NodeConfig {
        role,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.join("node"),
        web_dist_dir: dir.join("no_web"),
        default_project_id: "proj_default".into(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN.into()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 8,
    }
}

fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

async fn post(node: &RunningNode, path: &str, key: &str, body: Value) -> u16 {
    http_client()
        .post(format!("http://{}{path}", node.http_addr))
        .header("x-pulse-key", key)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn admin(node: &RunningNode, method: reqwest::Method, path: &str) -> u16 {
    http_client()
        .request(method, format!("http://{}{path}", node.http_addr))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

fn event(i: usize, now: i64) -> Value {
    json!({"events": [{"event": "e", "userId": format!("u_{i}"), "timestamp": now}]})
}

fn trace(i: usize, now: i64) -> Value {
    json!({"traces": [{"trace_id": format!("tr_{i}"), "agent_name": "bot", "agent_version": "v1",
        "environment": "production", "status": "ok", "start_ts_ms": now, "duration_ms": 5,
        "input": {}, "output": {}, "metadata": {}, "tags": []}],
        "spans": [{"span_id": format!("sp_{i}"), "trace_id": format!("tr_{i}"), "name": "llm",
        "start_ts_ms": now}]})
}

struct RunCalls {
    events: Calls,
    traces: Calls,
    identifies: Calls,
    engages: Calls,
    idle_l0: Calls,
    idle_flushed: Calls,
    user_detail: Calls,
    idle_ingest: Calls,
}

/// Segment-store calls across ingest/identify/engage/trace windows, user-detail reads, and idle.
async fn run(mode: &str, n: usize, idle: Duration) -> RunCalls {
    let _hot_store = HotStoreEnvGuard::set(mode);
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(Counting::default());
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::All), store.clone())
        .await
        .unwrap();
    let created: Value = reqwest::Client::new()
        .post(format!("http://{}/v1/projects", node.http_addr))
        .bearer_auth(ADMIN)
        .json(&json!({"name": "Calls"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let pid = created["project"]["id"].as_str().unwrap().to_string();
    let key = created["api_key"]["plaintext_key"]
        .as_str()
        .unwrap()
        .to_string();
    let now = chrono::Utc::now().timestamp_millis();
    // Warm-up opens the project's manifest, identity, profile and agent stores.
    assert_eq!(
        post(&node, "/v1/ingest", &key, event(usize::MAX, now)).await,
        200
    );
    assert_eq!(
        post(&node, "/v1/agent/traces", &key, trace(usize::MAX, now)).await,
        200
    );
    assert_eq!(
        post(
            &node,
            "/v1/identify",
            &key,
            json!({"anon_id": "warm_anon", "user_id": "warm_user"})
        )
        .await,
        200
    );
    assert_eq!(
        post(
            &node,
            "/v1/engage",
            &key,
            json!({"distinct_id": "warm_user", "$set": {"plan": "starter"}})
        )
        .await,
        200
    );
    print(&format!("{mode}: startup + warm-up"), &store.take());

    for i in 0..n {
        assert_eq!(post(&node, "/v1/ingest", &key, event(i, now)).await, 200);
    }
    let events = store.take();
    print(
        &format!("{mode}: {n} event requests, one event each"),
        &events,
    );
    for i in 0..n {
        assert_eq!(
            post(&node, "/v1/agent/traces", &key, trace(i, now)).await,
            200
        );
    }
    let traces = store.take();
    print(
        &format!("{mode}: {n} trace requests, one trace each"),
        &traces,
    );
    for i in 0..20 {
        assert_eq!(
            post(
                &node,
                "/v1/identify",
                &key,
                json!({"anon_id": format!("anon_{i}"), "user_id": format!("user_{i}")})
            )
            .await,
            200
        );
    }
    let identifies = store.take();
    print(&format!("{mode}: 20 identify requests"), &identifies);
    for i in 0..20 {
        assert_eq!(
            post(
                &node,
                "/v1/engage",
                &key,
                json!({"distinct_id": format!("user_{i}"), "$set": {"tier": "pro", "seq": i}})
            )
            .await,
            200
        );
    }
    let engages = store.take();
    print(&format!("{mode}: 20 engage requests"), &engages);

    tokio::time::sleep(idle).await;
    let idle_l0 = store.take();
    print(&format!("{mode}: idle {idle:?}, data in L0"), &idle_l0);
    assert_eq!(
        admin(&node, reqwest::Method::POST, "/v1/admin/flush").await,
        200
    );
    print(&format!("{mode}: POST /v1/admin/flush"), &store.take());
    tokio::time::sleep(idle).await;
    let idle_flushed = store.take();
    print(
        &format!("{mode}: idle {idle:?}, after flush"),
        &idle_flushed,
    );
    node.shutdown().await;
    print(&format!("{mode}: shutdown"), &store.take());

    // The same project on a restarted node that has not opened it: what reads of it cost.
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::All), store.clone())
        .await
        .unwrap();
    print(&format!("{mode}: restart (all)"), &store.take());
    for _ in 0..10 {
        assert_eq!(admin(&node, reqwest::Method::GET, "/v1/status").await, 200);
    }
    print(&format!("{mode}: 10 x GET /v1/status"), &store.take());
    for _ in 0..10 {
        let st = admin(
            &node,
            reqwest::Method::GET,
            &format!("/v1/projects/{pid}/users/u_1"),
        )
        .await;
        assert_eq!(st, 200);
    }
    let user_detail = store.take();
    print(&format!("{mode}: 10 x GET user detail"), &user_detail);
    node.shutdown().await;
    store.take();

    // An ingest-role node reloads its caches every 5 s, reading every project it has not opened.
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::Ingest), store.clone())
        .await
        .unwrap();
    store.take();
    tokio::time::sleep(idle).await;
    let idle_ingest = store.take();
    print(
        &format!("{mode}: ingest role, idle {idle:?}, project not open"),
        &idle_ingest,
    );
    node.shutdown().await;
    RunCalls {
        events,
        traces,
        identifies,
        engages,
        idle_l0,
        idle_flushed,
        user_detail,
        idle_ingest,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_requests_make_zero_hot_tier_calls_in_local_mode() {
    let idle = Duration::from_secs(30);
    let obj = run("object", 1000, idle).await;
    assert!(
        prefix_puts(&obj.events, "wal") > 0 && prefix_puts(&obj.events, "identity") > 0,
        "object mode writes wal and identity to the segment store: {:?}",
        obj.events
    );
    assert!(
        prefix_puts(&obj.traces, "agent_wal") > 0,
        "object mode writes agent_wal to the segment store: {:?}",
        obj.traces
    );
    assert!(
        prefix_puts(&obj.identifies, "identity") > 0,
        "object mode writes identity on /v1/identify: {:?}",
        obj.identifies
    );
    assert!(
        prefix_puts(&obj.engages, "profiles") > 0,
        "object mode writes profiles on /v1/engage: {:?}",
        obj.engages
    );

    let loc = run("local", 1000, idle).await;
    assert_eq!(
        hot_calls(&loc.events),
        0,
        "local mode: zero hot-tier calls during /v1/ingest new-user acks: {:?}",
        loc.events
    );
    assert_eq!(
        hot_calls(&loc.traces),
        0,
        "local mode: zero hot-tier calls during /v1/agent/traces acks: {:?}",
        loc.traces
    );
    assert_eq!(
        hot_calls(&loc.identifies),
        0,
        "local mode: zero hot-tier calls during /v1/identify acks: {:?}",
        loc.identifies
    );
    assert_eq!(
        hot_calls(&loc.engages),
        0,
        "local mode: zero hot-tier calls during /v1/engage acks: {:?}",
        loc.engages
    );
    assert_eq!(
        hot_calls(&loc.user_detail),
        0,
        "local mode: zero hot-tier calls during GET user detail: {:?}",
        loc.user_detail
    );
    assert_eq!(
        (
            hot_calls(&loc.idle_l0),
            hot_calls(&loc.idle_flushed),
            hot_calls(&loc.idle_ingest),
        ),
        (0, 0, 0),
        "local mode: zero hot-tier calls during idle"
    );
}
