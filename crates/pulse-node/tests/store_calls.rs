//! P0-7: which segment-store calls a request makes before it is acknowledged, and what an idle
//! node costs. Counts calls by top-level key prefix in each window and prints a table; asserts
//! that with the WAL on local disk no acknowledged request writes to the segment store.
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ADMIN: &str = "sk_root_storecallstest0000000000000000000000000000";
type Calls = BTreeMap<(String, &'static str), u64>;

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

/// PUTs to the WAL prefixes. SlateDB identity PUTs are reported, not asserted: they are not
/// awaited before the ack and are a separate decision (see the PR).
fn puts(calls: &Calls) -> u64 {
    calls
        .iter()
        .filter(|((prefix, op), _)| *op == "PUT" && matches!(prefix.as_str(), "wal" | "agent_wal"))
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

async fn post(node: &RunningNode, path: &str, key: &str, body: Value) -> u16 {
    reqwest::Client::new()
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
    reqwest::Client::new()
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

/// Segment-store PUTs while `n` event and `n` trace requests were acknowledged.
async fn run(mode: &str, n: usize, idle: Duration) -> (u64, u64) {
    std::env::set_var("PULSE_HOT_STORE", mode);
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

    tokio::time::sleep(idle).await;
    print(&format!("{mode}: idle {idle:?}, data in L0"), &store.take());
    assert_eq!(
        admin(&node, reqwest::Method::POST, "/v1/admin/flush").await,
        200
    );
    print(&format!("{mode}: POST /v1/admin/flush"), &store.take());
    tokio::time::sleep(idle).await;
    print(
        &format!("{mode}: idle {idle:?}, after flush"),
        &store.take(),
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
    print(&format!("{mode}: 10 x GET user detail"), &store.take());
    node.shutdown().await;
    store.take();

    // An ingest-role node reloads its caches every 5 s, reading every project it has not opened.
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::Ingest), store.clone())
        .await
        .unwrap();
    store.take();
    tokio::time::sleep(idle).await;
    print(
        &format!("{mode}: ingest role, idle {idle:?}, project not open"),
        &store.take(),
    );
    node.shutdown().await;
    (puts(&events), puts(&traces))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_requests_write_nothing_to_the_segment_store_with_a_local_wal() {
    let idle = Duration::from_secs(30);
    let (events, traces) = run("object", 1000, idle).await;
    assert!(
        events > 0 && traces > 0,
        "object mode keeps the WAL in the segment store"
    );
    let (events, traces) = run("local", 1000, idle).await;
    assert_eq!(
        (events, traces),
        (0, 0),
        "local mode: WAL PUTs to the segment store while (events, traces) were acknowledged"
    );
}
