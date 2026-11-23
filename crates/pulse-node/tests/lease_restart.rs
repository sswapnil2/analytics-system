//! A node killed without a clean shutdown keeps its writer lease until it expires. The same node
//! restarted on the same data directory gets a new node id, so it must wait that lease out
//! instead of failing to start. Own test binary: it sets `PULSE_WRITER_LEASE_TTL_SECS`.

use pulse_node::{start_pulse_node, NodeConfig, NodeRole};
use serde_json::json;
use std::path::Path;
use std::time::{Duration, Instant};

fn config(dir: &Path, auto_seed: bool) -> NodeConfig {
    NodeConfig {
        role: NodeRole::All,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.join("node"),
        web_dist_dir: dir.join("no_web"),
        default_project_id: "proj_restart".into(),
        default_api_key: String::new(),
        admin_token: Some("sk_root_lease_restart_test_token_000000000000".into()),
        allowed_origins: vec![],
        auto_seed,
        num_buckets: 1,
    }
}

async fn post_event(addr: std::net::SocketAddr, key: &str, user: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("http://{addr}/v1/ingest"))
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
}

#[test]
fn node_restarted_after_a_crash_waits_out_its_old_lease_and_starts() {
    std::env::set_var("PULSE_WRITER_LEASE_TTL_SECS", "3");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    // The first node writes one event, then its runtime is torn down: no flush, no lease release,
    // and its renewal task dies with it, the way kill -9 would leave things.
    let first_dir = dir.clone();
    let key = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let key = rt.block_on(async {
            let node = start_pulse_node(config(&first_dir, false)).await.unwrap();
            let key = node
                .bootstrap_credentials
                .generated_ingest_key
                .clone()
                .unwrap();
            assert_eq!(post_event(node.http_addr, &key, "before").await, 200);
            node.ingest.flush_all().await.unwrap();
            std::mem::forget(node);
            key
        });
        rt.shutdown_background();
        key
    })
    .join()
    .unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let started = Instant::now();
        // With seeding on, startup opens the default project, which needs the lease.
        let node = start_pulse_node(config(&dir, true))
            .await
            .expect("a restarted node starts once its old lease expires");
        assert_eq!(post_event(node.http_addr, &key, "after").await, 200);
        node.ingest.flush_all().await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(3 + 2),
            "took {:?}",
            started.elapsed()
        );
        let ps = node.storage.open_for_read("proj_restart").await.unwrap();
        let rows = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
        assert_eq!(
            rows, 2,
            "the event acknowledged before the crash was replayed"
        );
        node.shutdown().await;
    });
}
