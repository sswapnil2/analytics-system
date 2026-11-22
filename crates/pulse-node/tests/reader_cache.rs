//! A node that has not opened a project for writing reads its identity and profile stores
//! through cached read-only handles. Without the cache, every 5 s cache reload wrote two
//! SlateDB checkpoint objects per project, and every user-detail call reloaded the whole
//! identity graph. Counts object-store calls under the node to check both.

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

const ADMIN: &str = "sk_root_readercachetest00000000000000000000000000";
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

fn sum(calls: &Calls, prefix: &str, op: &str) -> u64 {
    calls
        .iter()
        .filter(|((p, o), _)| (prefix == "*" || p == prefix) && *o == op)
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
        num_buckets: 4,
    }
}

async fn user_detail(node: &RunningNode, pid: &str) -> u16 {
    reqwest::Client::new()
        .get(format!(
            "http://{}/v1/projects/{pid}/users/u_1",
            node.http_addr
        ))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_project_this_node_has_not_opened_is_read_through_cached_handles() {
    let _hot_store = HotStoreEnvGuard::set("object");
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(Counting::default());

    // A project with an identity graph and a profile, written and cleanly shut down.
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::All), store.clone())
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let created: Value = http
        .post(format!("http://{}/v1/projects", node.http_addr))
        .bearer_auth(ADMIN)
        .json(&json!({"name": "Idle"}))
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
    for (path, body) in [
        (
            "/v1/ingest",
            json!({"events": (0..50).map(|i| json!({"event": "e", "userId": format!("u_{i}"), "timestamp": now})).collect::<Vec<_>>()}),
        ),
        (
            "/v1/engage",
            json!({"distinct_id": "u_1", "$set": {"plan": "gold"}}),
        ),
    ] {
        let st = http
            .post(format!("http://{}{path}", node.http_addr))
            .header("x-pulse-key", &key)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(st.as_u16(), 200, "{path}");
    }
    node.shutdown().await;

    // An ingest node reloads its caches every 5 s, which reads every project's profiles.
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::Ingest), store.clone())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await; // first reload opens the reader
    store.take();
    tokio::time::sleep(Duration::from_secs(30)).await; // six more reloads
    let idle = store.take();
    eprintln!("ingest role, 30 s idle, project not open: {idle:?}");
    assert_eq!(sum(&idle, "*", "PUT"), 0, "PUTs while idle: {idle:?}");
    assert_eq!(sum(&idle, "*", "DELETE"), 0, "DELETEs while idle: {idle:?}");
    // The cached profile reader polls SlateDB's manifest every 10 s: a few reads, not per reload.
    let reads = sum(&idle, "*", "GET") + sum(&idle, "*", "LIST");
    assert!(reads <= 30, "reads while idle: {idle:?}");
    node.shutdown().await;
    store.take();

    // User detail on a node that has not opened the project loads the graph once.
    let node = start_pulse_node_with_store(config(tmp.path(), NodeRole::All), store.clone())
        .await
        .unwrap();
    store.take();
    assert_eq!(user_detail(&node, &pid).await, 200);
    let first = sum(&store.take(), "identity", "GET");
    for _ in 0..9 {
        assert_eq!(user_detail(&node, &pid).await, 200);
    }
    let rest = store.take();
    eprintln!("user detail: first call {first} identity GETs; next 9 calls: {rest:?}");
    // On an `all` node the first call's event export also opens the project for writing, so
    // later calls use the writer's graph; either way none of them loads the graph again.
    assert!(first > 0, "the first call loads the graph");
    let again = sum(&rest, "identity", "GET");
    assert!(
        again * 4 < first,
        "the next 9 calls reload the graph: {rest:?}"
    );
    node.shutdown().await;
}
