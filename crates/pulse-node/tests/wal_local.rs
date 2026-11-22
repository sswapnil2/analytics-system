//! P0-7: the WAL on local disk (`PULSE_HOT_STORE` unset means local). A crash loses nothing
//! acknowledged, a clean shutdown leaves the local WAL empty, an unwritable WAL directory
//! refuses requests with 503 instead of acknowledging them, and a node refuses to start over
//! unflushed WAL left in the object store. Own test binary: it sets
//! `PULSE_WRITER_LEASE_TTL_SECS`.

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole, RunningNode};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const PID: &str = "proj_wal";

fn config(dir: &Path) -> NodeConfig {
    NodeConfig {
        role: NodeRole::All,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.join("node"),
        web_dist_dir: dir.join("no_web"),
        default_project_id: PID.into(),
        default_api_key: String::new(),
        admin_token: Some("sk_root_wal_local_test_token_00000000000000000".into()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 4,
    }
}

async fn start(dir: &Path, store: &Arc<InMemory>) -> (RunningNode, String) {
    std::env::set_var("PULSE_WRITER_LEASE_TTL_SECS", "3");
    let node = start_pulse_node_with_store(config(dir), store.clone())
        .await
        .unwrap();
    let key = node
        .bootstrap_credentials
        .generated_ingest_key
        .clone()
        .unwrap_or_default();
    (node, key)
}

async fn post_event(node: &RunningNode, key: &str, user: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("http://{}/v1/ingest", node.http_addr))
        .header("x-pulse-key", key)
        .json(&json!({ "events": [{
            "distinct_id": user,
            "event_name": "App_Open",
            "client_event_ts_ms": chrono::Utc::now().timestamp_millis(),
        }]}))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn rows(node: &RunningNode) -> usize {
    let ps = node.storage.get_or_open_project(PID).await.unwrap();
    ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows()
}

fn files_under(dir: &Path) -> Vec<String> {
    let mut out = vec![];
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(files_under(&p));
        } else {
            out.push(p.display().to_string());
        }
    }
    out
}

fn set_mode_recursive(dir: &Path, mode: u32) {
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        if e.path().is_dir() {
            set_mode_recursive(&e.path(), mode);
        }
    }
}

#[test]
fn a_crash_loses_no_acknowledged_event_with_the_wal_on_local_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (dir, first_store) = (tmp.path().to_path_buf(), store.clone());
    // A torn-down runtime: no flush, no lease release, the way kill -9 leaves things.
    let key = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let key = rt.block_on(async {
            let (node, key) = start(&dir, &first_store).await;
            for i in 0..50 {
                assert_eq!(post_event(&node, &key, &format!("u{i}")).await, 200);
            }
            std::mem::forget(node);
            key
        });
        rt.shutdown_background();
        key
    })
    .join()
    .unwrap();
    assert!(!key.is_empty());

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (node, _) = start(tmp.path(), &store).await;
        // `rows` opens the project, which waits out the dead node's lease.
        let mut found = 0;
        for _ in 0..40 {
            if let Ok(ps) = node.storage.get_or_open_project(PID).await {
                found = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert_eq!(
            found, 50,
            "every acknowledged event was replayed from the local WAL"
        );
        node.shutdown().await;
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_shutdown_leaves_the_local_wal_empty_and_the_object_store_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (node, key) = start(tmp.path(), &store).await;
    for i in 0..20 {
        assert_eq!(post_event(&node, &key, &format!("u{i}")).await, 200);
    }
    let wal_dir = tmp.path().join("node").join("wal");
    assert!(
        !files_under(&wal_dir).is_empty(),
        "the WAL is written locally"
    );
    node.shutdown().await;
    assert_eq!(
        files_under(&wal_dir),
        Vec::<String>::new(),
        "local WAL after shutdown"
    );

    // Everything is in the object store: a node on a fresh disk sees all 20.
    let fresh = tempfile::tempdir().unwrap();
    let (node, _) = start(fresh.path(), &store).await;
    assert_eq!(rows(&node).await, 20);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unwritable_wal_directory_answers_503_and_acknowledges_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (node, key) = start(tmp.path(), &store).await;
    assert_eq!(post_event(&node, &key, "ok").await, 200);
    let wal_dir = tmp.path().join("node").join("wal");
    set_mode_recursive(&wal_dir, 0o555);
    let refused = post_event(&node, &key, "refused").await;
    let trace = reqwest::Client::new()
        .post(format!("http://{}/v1/agent/traces", node.http_addr))
        .header("x-pulse-key", &key)
        .json(
            &json!({"traces": [{"trace_id": "tr_1", "agent_name": "bot", "agent_version": "v1",
            "environment": "production", "status": "ok", "start_ts_ms": 1, "duration_ms": 1,
            "input": {}, "output": {}, "metadata": {}, "tags": []}], "spans": []}),
        )
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    set_mode_recursive(&wal_dir, 0o755);
    assert_eq!(
        (refused, trace),
        (503, 503),
        "(event, trace) with an unwritable WAL"
    );
    assert_eq!(rows(&node).await, 1, "the refused event was not applied");
    assert_eq!(post_event(&node, &key, "again").await, 200);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_wal_node_refuses_to_start_over_unflushed_wal_in_the_object_store() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let left = ObjectPath::from("wal/proj_wal/bucket_000/epoch_0000000001_seq_000000000001.wal");
    store.put(&left, b"x".to_vec().into()).await.unwrap();
    let err = start_pulse_node_with_store(config(tmp.path()), store.clone())
        .await
        .err()
        .expect("must refuse to start");
    assert!(err.to_string().contains("unflushed WAL"), "{err}");
}
