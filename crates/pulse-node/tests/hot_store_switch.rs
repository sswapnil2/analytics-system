//! P0-7 (T0b): hot-store mode marker & startup refusal rules.
//! - Same-mode restarts (`local` -> `local`, `object` -> `object`) preserve events, traces,
//!   identity, and profiles.
//! - Mode mismatches (`object` -> `local` and `local` -> `object`) refuse startup via
//!   `<data_dir>/hot_store_mode`.
//! - A `local` marker with missing or wiped local `identity/` or `profiles/` refuses startup.
//! - Starting `local` over leftover object-mode `identity/` or `profiles/` refuses startup.
//!
//! Own test binary: it sets `PULSE_HOT_STORE`.

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole, RunningNode};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::{Arc, Mutex};

const PID: &str = "proj_switch";
const ADMIN: &str = "sk_root_hot_store_switch_test_0000000000000000";
/// `PULSE_HOT_STORE` is process-wide: one test at a time.
static ENV: Mutex<()> = Mutex::new(());

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
    let _guard = HotStoreEnvGuard::set(mode);
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
    if events > 0 {
        post(
            node,
            &key,
            "/v1/identify",
            json!({"anon_id": "u0", "user_id": "canon_u0"}),
        )
        .await;
        post(
            node,
            &key,
            "/v1/engage",
            json!({"distinct_id": "canon_u0", "$set": {"plan": "pro"}}),
        )
        .await;
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

async fn user_plan(node: &RunningNode, did: &str) -> Option<String> {
    let detail: Value = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/projects/{PID}/users/{did}",
            node.http_addr
        ))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    detail["profile"]["props"]["plan"]
        .as_str()
        .map(str::to_string)
}

#[test]
fn same_mode_restarts_preserve_events_traces_identity_and_profiles() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for mode in ["local", "object"] {
            let tmp = tempfile::tempdir().unwrap();
            let store = Arc::new(InMemory::new());
            let node = start(mode, tmp.path(), &store).await.unwrap();
            ingest(&node, 20, 30).await;
            node.shutdown().await;

            let node = start(mode, tmp.path(), &store)
                .await
                .unwrap_or_else(|e| panic!("{mode} restart failed: {e}"));
            assert_eq!(counts(&node).await, (20, 30), "{mode} counts");
            assert_eq!(
                user_plan(&node, "u0").await.as_deref(),
                Some("pro"),
                "{mode} identity+profile"
            );
            node.shutdown().await;
        }
    });
}

#[test]
fn mode_mismatches_between_object_and_local_refuse_startup() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // object -> clean shutdown -> local refused
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let node = start("object", tmp.path(), &store).await.unwrap();
        ingest(&node, 5, 5).await;
        node.shutdown().await;

        let err = start("local", tmp.path(), &store)
            .await
            .err()
            .expect("object -> local must refuse startup");
        assert!(err.to_string().contains("PULSE_HOT_STORE=object"), "{err}");

        // local -> clean shutdown -> object refused
        let tmp2 = tempfile::tempdir().unwrap();
        let store2 = Arc::new(InMemory::new());
        let node = start("local", tmp2.path(), &store2).await.unwrap();
        ingest(&node, 5, 5).await;
        node.shutdown().await;

        let err = start("object", tmp2.path(), &store2)
            .await
            .err()
            .expect("local -> object must refuse startup");
        assert!(err.to_string().contains("PULSE_HOT_STORE=local"), "{err}");
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

#[test]
fn local_mode_refuses_to_start_when_local_identity_or_profiles_are_missing_or_wiped() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let node = start("local", tmp.path(), &store).await.unwrap();
        ingest(&node, 5, 0).await;
        node.shutdown().await;

        let hot_id = tmp.path().join("node/hot/identity");
        let hot_prof = tmp.path().join("node/hot/profiles");

        // 1. Deleted hot/identity directory -> refused.
        let id_backup = tmp.path().join("id_backup");
        std::fs::rename(&hot_id, &id_backup).unwrap();
        let err = start("local", tmp.path(), &store)
            .await
            .err()
            .expect("missing hot/identity must refuse startup");
        assert!(
            err.to_string().contains("restore --from-published"),
            "{err}"
        );

        // 2. Empty hot/identity directory (wiped contents while dir exists & segments/profiles exist) -> refused.
        std::fs::create_dir_all(&hot_id).unwrap();
        let err = start("local", tmp.path(), &store)
            .await
            .err()
            .expect("wiped hot/identity/* must refuse startup");
        assert!(
            err.to_string().contains("restore --from-published"),
            "{err}"
        );

        // 3. Restore hot/identity, remove hot/profiles -> refused.
        std::fs::remove_dir_all(&hot_id).unwrap();
        std::fs::rename(&id_backup, &hot_id).unwrap();
        std::fs::remove_dir_all(&hot_prof).unwrap();
        let err = start("local", tmp.path(), &store)
            .await
            .err()
            .expect("missing hot/profiles must refuse startup");
        assert!(
            err.to_string().contains("restore --from-published"),
            "{err}"
        );
    });
}

#[test]
fn local_mode_refuses_to_start_over_leftover_object_store_identity_or_profiles() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for key in [
            "identity/proj_switch/manifest/00000000000000000001.manifest",
            "profiles/proj_switch/manifest/00000000000000000001.manifest",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let store = Arc::new(InMemory::new());
            store
                .put(&ObjectPath::from(key), PutPayload::from_static(b"x"))
                .await
                .unwrap();
            let err = start("local", tmp.path(), &store)
                .await
                .err()
                .expect("leftover object-mode hot state must refuse local startup");
            assert!(err.to_string().contains("PULSE_HOT_STORE=local"), "{err}");
        }
    });
}

#[test]
fn local_mode_refuses_to_start_when_entire_data_dir_is_wiped_and_published_exists() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemory::new());
        let node = start("local", tmp.path(), &store).await.unwrap();
        ingest(&node, 10, 0).await;
        node.shutdown().await;

        // Wipe the entire `<data_dir>` (including `hot_store_mode` marker and `hot/`).
        let node_dir = tmp.path().join("node");
        std::fs::remove_dir_all(&node_dir).unwrap();
        assert!(!node_dir.exists());

        let err = start("local", tmp.path(), &store)
            .await
            .err()
            .expect("wiped <data_dir> with S3 published/ must refuse startup");
        assert!(
            err.to_string().contains("restore --from-published"),
            "{err}"
        );
        assert!(
            !node_dir.join("hot_store_mode").exists(),
            "refusal must not write hot_store_mode marker"
        );
        assert!(
            !node_dir.join("hot").exists(),
            "refusal must not initialize hot/ directories"
        );

        // Fresh directory without any `published/` starts cleanly.
        let clean_tmp = tempfile::tempdir().unwrap();
        let clean_store = Arc::new(InMemory::new());
        let clean_node = start("local", clean_tmp.path(), &clean_store)
            .await
            .expect("clean fresh init without published/ must succeed");
        clean_node.shutdown().await;
    });
}
