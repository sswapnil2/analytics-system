//! P0-7: switching `PULSE_HOT_STORE` between `object` and `local`. Every existing deployment
//! upgrades from an object-mode run, so a clean object-mode shutdown must leave nothing for the
//! local-mode guard, events and traces both; and an object-mode node must not start over a WAL
//! a local-mode run left on disk. Own test binary: it sets `PULSE_HOT_STORE`.

use object_store::memory::InMemory;
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole, RunningNode};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::{Arc, Mutex};

const PID: &str = "proj_switch";
const ADMIN: &str = "sk_root_hot_store_switch_test_0000000000000000";
/// `PULSE_HOT_STORE` is process-wide: one test at a time.
static ENV: Mutex<()> = Mutex::new(());

fn config(dir: &Path) -> NodeConfig {
    NodeConfig {
        role: NodeRole::All,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.join("node"),
        web_dist_dir: dir.join("no_web"),
        default_project_id: PID.into(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN.into()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 4,
    }
}

async fn start(mode: &str, dir: &Path, store: &Arc<InMemory>) -> anyhow::Result<RunningNode> {
    std::env::set_var("PULSE_HOT_STORE", mode);
    start_pulse_node_with_store(config(dir), store.clone()).await
}

fn key(node: &RunningNode) -> String {
    node.bootstrap_credentials
        .generated_ingest_key
        .clone()
        .unwrap()
}

async fn post(node: &RunningNode, key: &str, path: &str, body: Value) {
    let st = reqwest::Client::new()
        .post(format!("http://{}{path}", node.http_addr))
        .header("x-pulse-key", key)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(st.as_u16(), 200, "{path}");
}

async fn ingest(node: &RunningNode, events: usize, traces: usize) {
    let key = key(node);
    let now = chrono::Utc::now().timestamp_millis();
    for i in 0..events {
        let ev = json!({"events": [{"distinct_id": format!("u{i}"), "event_name": "Page_Viewed",
            "client_event_ts_ms": now}]});
        post(node, &key, "/v1/ingest", ev).await;
    }
    for i in 0..traces {
        let tr = json!({"traces": [{"trace_id": format!("tr_{i}"), "agent_name": "bot",
            "agent_version": "v1", "environment": "production", "status": "ok",
            "start_ts_ms": now, "duration_ms": 1, "input": {}, "output": {}, "metadata": {},
            "tags": []}], "spans": []});
        post(node, &key, "/v1/agent/traces", tr).await;
    }
}

async fn counts(node: &RunningNode) -> (usize, i64) {
    let ps = node.storage.get_or_open_project(PID).await.unwrap();
    let rows = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
    let traces: Value = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/projects/{PID}/agent/traces?limit=500",
            node.http_addr
        ))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (rows, traces["total"].as_i64().unwrap_or(-1))
}

#[test]
fn a_clean_object_mode_shutdown_upgrades_to_local_with_every_event_and_trace() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let node = start("object", tmp.path(), &store).await.unwrap();
        ingest(&node, 40, 60).await;
        node.shutdown().await;

        let node = start("local", tmp.path(), &store)
            .await
            .expect("a clean object-mode shutdown leaves no WAL behind");
        assert_eq!(
            counts(&node).await,
            (40, 60),
            "(events, traces) after upgrade"
        );
        node.shutdown().await;
    });
}

#[test]
fn an_object_mode_node_refuses_to_start_over_a_local_wal() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (dir, first) = (tmp.path().to_path_buf(), store.clone());
    // A local-mode run killed before it flushed: no shutdown, the way kill -9 leaves things.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let node = start("local", &dir, &first).await.unwrap();
            ingest(&node, 5, 5).await;
            std::mem::forget(node);
        });
        rt.shutdown_background();
    })
    .join()
    .unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let err = start("object", tmp.path(), &store)
            .await
            .err()
            .expect("must refuse to start over the local WAL");
        assert!(err.to_string().contains("PULSE_HOT_STORE=local"), "{err}");
    });
}
