//! P0-7: the WAL on local disk (`PULSE_HOT_STORE` unset means local). A crash loses nothing
//! acknowledged, a clean shutdown leaves the local WAL empty, an unwritable WAL directory
//! refuses requests with 503 instead of acknowledging them, and a node refuses to start over
//! unflushed WAL left in the object store. Own test binary: it sets
//! `PULSE_WRITER_LEASE_TTL_SECS`.

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_agent::store::wal::{
    encode_ptwl_frame, wal_corrupt_path, wal_object_path, AgentWalStore,
};
use pulse_agent::TraceIngestBatch;
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole, RunningNode};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    let ps = node.storage.open_for_read(PID).await.unwrap();
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
            node.ingest.flush_all().await.unwrap();
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
    node.ingest.flush_all().await.unwrap();
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

    // All 20 rows and a published manifest are in the object store, while manifests/ stays local.
    let enc: Arc<dyn ObjectStore> =
        Arc::new(pulse_storage::EncryptedObjectStore::new(store.clone()));
    assert_eq!(
        pulse_storage::ManifestStore::new(PID, enc.clone())
            .load_latest()
            .await
            .unwrap()
            .epoch,
        0,
        "local manifests/ must not be written to S3"
    );
    let pub_m = pulse_storage::PublishedManifestStore::new(PID, enc)
        .load_latest()
        .await
        .unwrap()
        .expect("published manifest must exist after clean shutdown");
    let pub_rows: u64 = pub_m
        .partitions
        .iter()
        .flat_map(|p| p.segments.iter())
        .map(|s| s.footer.row_count as u64)
        .sum();
    assert_eq!(pub_rows, 20);

    // Starting on a fresh/wiped <data_dir> while S3 `published/` exists must refuse startup.
    let fresh = tempfile::tempdir().unwrap();
    let err = start_pulse_node_with_store(config(fresh.path()), store.clone())
        .await
        .err()
        .expect("fresh data_dir with S3 published/ must refuse startup");
    assert!(
        err.to_string().contains("restore --from-published"),
        "{err}"
    );

    // Restarting on the original `tmp.path()` sees all 20 rows.
    let (node, _) = start(tmp.path(), &store).await;
    assert_eq!(rows(&node).await, 20);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unwritable_wal_directory_answers_503_and_acknowledges_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (node, key) = start(tmp.path(), &store).await;
    assert_eq!(post_event(&node, &key, "ok").await, 200);
    node.ingest.flush_all().await.unwrap();
    assert_eq!(rows(&node).await, 1);

    let wal_dir = tmp.path().join("node").join("wal");
    set_mode_recursive(&wal_dir, 0o555);
    // First async event queues in W1 buffer and trips the sticky wal_error when the flusher hits EACCES.
    assert_eq!(post_event(&node, &key, "buffered_before_error").await, 200);
    let mut tripped = false;
    for _ in 0..20 {
        let status = post_event(&node, &key, "refused").await;
        if status == 503 {
            tripped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(tripped, "expected 503 once sticky wal_error tripped");
    let refused = post_event(&node, &key, "refused_2").await;
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
    // Once WAL permissions recover, the previously 200-acked batch drains cleanly without loss,
    // while the 503-refused batches were never queued.
    node.ingest.flush_all().await.unwrap();
    assert_eq!(
        rows(&node).await,
        2,
        "only the 200-acked events (initial + pre-error buffered) were applied; 503 requests were refused"
    );
    assert_eq!(post_event(&node, &key, "again").await, 200);
    node.ingest.flush_all().await.unwrap();
    assert_eq!(rows(&node).await, 3);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_wal_corrupt_routes_locally_with_zero_segment_store_calls() {
    #[derive(Debug, Default)]
    struct CountingSegments {
        inner: InMemory,
        calls: AtomicUsize,
    }
    impl std::fmt::Display for CountingSegments {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingSegments")
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for CountingSegments {
        async fn put_opts(
            &self,
            l: &ObjectPath,
            p: PutPayload,
            o: PutOptions,
        ) -> OsResult<PutResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjectPath,
            o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(l, o).await
        }
        async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(l).await
        }
        fn list(
            &self,
            p: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, OsResult<ObjectMeta>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let seg = Arc::new(CountingSegments::default());
    let os = pulse_storage::with_hot_store(seg.clone(), tmp.path())
        .await
        .unwrap();
    seg.calls.store(0, Ordering::SeqCst);

    let wal = AgentWalStore::new(PID, os.clone()).unwrap();
    let wid = "writer_corrupt_test";
    let mut corrupt = encode_ptwl_frame(
        1,
        1_000,
        &TraceIngestBatch {
            traces: vec![],
            spans: vec![],
        },
    )
    .unwrap();
    corrupt.truncate(corrupt.len().saturating_sub(8));
    os.put(
        &wal_object_path(PID, wid, 1).unwrap(),
        PutPayload::from_bytes(corrupt.into()),
    )
    .await
    .unwrap();

    let mut applied = HashMap::new();
    let replayed = wal
        .replay_uncommitted(&BTreeMap::new(), &mut applied, true)
        .await
        .unwrap();
    assert!(replayed.is_empty());
    assert_eq!(seg.calls.load(Ordering::SeqCst), 0);
    let qpath = wal_corrupt_path(PID, wid, "seq_000000000001.ptwl").unwrap();
    assert!(os.head(&qpath).await.is_ok());
    assert!(tmp
        .path()
        .join("wal/agent_wal_corrupt")
        .join(PID)
        .join(wid)
        .join("seq_000000000001.ptwl")
        .is_file());
}

#[test]
fn crash_recovery_preserves_local_identity_and_profiles() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (dir, first_store) = (tmp.path().to_path_buf(), store.clone());
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (node, key) = start(&dir, &first_store).await;
            assert_eq!(post_event(&node, &key, "anon_crash_1").await, 200);
            let http = reqwest::Client::new();
            let id_res = http
                .post(format!("http://{}/v1/identify", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!({"anon_id": "anon_crash_1", "user_id": "user_crash_1"}))
                .send()
                .await
                .unwrap();
            assert_eq!(id_res.status().as_u16(), 200);
            let eng_res = http
                .post(format!("http://{}/v1/engage", node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!({"distinct_id": "user_crash_1", "$set": {"tier": "gold"}}))
                .send()
                .await
                .unwrap();
            assert_eq!(eng_res.status().as_u16(), 200);
            std::mem::forget(node);
        });
        rt.shutdown_background();
    })
    .join()
    .unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (node, _) = start(tmp.path(), &store).await;
        for _ in 0..40 {
            if node.storage.get_or_open_project(PID).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert_eq!(rows(&node).await, 1);
        let detail: Value = reqwest::Client::new()
            .get(format!(
                "http://{}/v1/projects/{PID}/users/anon_crash_1",
                node.http_addr
            ))
            .bearer_auth("sk_root_wal_local_test_token_00000000000000000")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            detail["profile"]["props"]["tier"].as_str(),
            Some("gold"),
            "identity alias and profile survive crash recovery locally: {detail}"
        );
        node.shutdown().await;
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unwritable_hot_directory_answers_503_on_sync_profile_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (node, key) = start(tmp.path(), &store).await;
    assert_eq!(post_event(&node, &key, "seed_user").await, 200);
    node.ingest.flush_all().await.unwrap();

    let hot_dir = tmp.path().join("node").join("hot");
    set_mode_recursive(&hot_dir, 0o555);
    let http = reqwest::Client::new();
    // Under PR #98 + W1, /v1/ingest writes only to wal/ (still writable) and returns 200,
    // while synchronous /v1/identify and /v1/engage touch hot/identity and hot/profiles and return 503.
    let id_resp = http
        .post(format!("http://{}/v1/identify", node.http_addr))
        .header("x-pulse-key", &key)
        .json(&json!({"anon_id": "anon_unwritable", "user_id": "user_unwritable"}))
        .send()
        .await
        .unwrap();
    let id_status = id_resp.status().as_u16();
    let id_body = id_resp.text().await.unwrap_or_default();

    let eng_resp = http
        .post(format!("http://{}/v1/engage", node.http_addr))
        .header("x-pulse-key", &key)
        .json(&json!({"distinct_id": "seed_user", "$set": {"tier": "gold"}}))
        .send()
        .await
        .unwrap();
    let eng_status = eng_resp.status().as_u16();
    let eng_body = eng_resp.text().await.unwrap_or_default();
    set_mode_recursive(&hot_dir, 0o755);

    assert_eq!(
        id_status, 503,
        "unwritable local hot/ directory must answer 503 on /v1/identify; body={id_body}"
    );
    assert_eq!(
        eng_status, 503,
        "unwritable local hot/ directory must answer 503 on /v1/engage; body={eng_body}"
    );
    assert_eq!(rows(&node).await, 1);
    assert_eq!(post_event(&node, &key, "recovered_user").await, 200);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_unwritable_profiles_directory_answers_503_on_engage_and_identify() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let (node, key) = start(tmp.path(), &store).await;
    let http = reqwest::Client::new();

    // Seed and flush an existing user so `hot/identity/` already exists and is writable.
    assert_eq!(post_event(&node, &key, "prof_seed_user").await, 200);
    node.ingest.flush_all().await.unwrap();
    let seed_eng = http
        .post(format!("http://{}/v1/engage", node.http_addr))
        .header("x-pulse-key", &key)
        .json(&json!({"distinct_id": "prof_seed_user", "$set": {"plan": "free"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(seed_eng.status().as_u16(), 200);

    // Chmod ONLY `hot/profiles/` to 0o555 while keeping `hot/identity/` writable.
    let profiles_dir = tmp.path().join("node").join("hot").join("profiles");
    assert!(
        profiles_dir.is_dir(),
        "expected hot/profiles directory at {}",
        profiles_dir.display()
    );
    set_mode_recursive(&profiles_dir, 0o555);

    // Force ProfileStore write (already-flushed user, so SlateDB must open/flush WAL into profiles/).
    // Note: an open SlateDB Db handle may need a new WAL SST write (which fails on 0o555 dir) -> 503.
    let eng_err_resp = http
        .post(format!("http://{}/v1/engage", node.http_addr))
        .header("x-pulse-key", &key)
        .json(&json!({"distinct_id": "prof_seed_user", "$set": {"plan": "blocked"}}))
        .send()
        .await
        .unwrap();
    let eng_status = eng_err_resp.status().as_u16();
    let eng_body = eng_err_resp.text().await.unwrap_or_default();

    // Also test /v1/identify where `alias_durable` succeeds in `hot/identity/` and `on_identify` hits `hot/profiles/`.
    let id_err_resp = http
        .post(format!("http://{}/v1/identify", node.http_addr))
        .header("x-pulse-key", &key)
        .json(&json!({"anon_id": "anon_prof_err", "user_id": "prof_seed_user"}))
        .send()
        .await
        .unwrap();
    let id_status = id_err_resp.status().as_u16();
    let id_body = id_err_resp.text().await.unwrap_or_default();

    set_mode_recursive(&profiles_dir, 0o755);

    assert_eq!(
        eng_status, 503,
        "isolated unwritable hot/profiles/ must answer 503 (never 200 or 500) on /v1/engage; body={eng_body}"
    );
    assert_eq!(
        id_status, 503,
        "isolated unwritable hot/profiles/ must answer 503 (never 200 or 500) on /v1/identify; body={id_body}"
    );

    // After restoring permissions, /v1/engage recovers cleanly and returns canonical_uid.
    let rec_resp = http
        .post(format!("http://{}/v1/engage", node.http_addr))
        .header("x-pulse-key", &key)
        .json(&json!({"distinct_id": "prof_seed_user", "$set": {"plan": "recovered"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(rec_resp.status().as_u16(), 200);
    let rec_json: Value = rec_resp.json().await.unwrap();
    assert!(rec_json["canonical_uid"].as_u64().is_some(), "{rec_json}");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w2_sync_profiles_readback_and_sigkill_durability() {
    struct ChildGuard(Option<std::process::Child>);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if let Some(mut c) = self.0.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }

    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    async fn wait_healthy(http: &reqwest::Client, base: &str, child: &mut std::process::Child) {
        for _ in 0..160 {
            if let Ok(r) = http.get(format!("{base}/healthz")).send().await {
                if r.status().as_u16() == 200 {
                    return;
                }
            }
            if let Some(st) = child.try_wait().unwrap() {
                panic!("pulse-node child exited prematurely with {st}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("pulse-node did not become healthy at {base}");
    }

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    let web_dir = tmp.path().join("no_web");
    let bin = env!("CARGO_BIN_EXE_pulse-node");
    let admin_tok = "sk_root_w2_sigkill_test_token_000000000000000000";
    let pid = "proj_w2_e2e";
    let http = reqwest::Client::new();

    let port1 = free_port();
    let base1 = format!("http://127.0.0.1:{port1}");
    let mut child1 = ChildGuard(Some(
        std::process::Command::new(bin)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port1.to_string(),
                "--no-quic",
                "--no-seed",
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--web-dir",
                web_dir.to_str().unwrap(),
            ])
            .env("PULSE_ADMIN_TOKEN", admin_tok)
            .env("PULSE_DEFAULT_PROJECT", pid)
            .env("PULSE_WRITER_LEASE_TTL_SECS", "2")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn pulse-node"),
    ));
    wait_healthy(&http, &base1, child1.0.as_mut().unwrap()).await;

    // Create ingest API key.
    let key_resp: Value = http
        .post(format!("{base1}/v1/projects/{pid}/api-keys"))
        .bearer_auth(admin_tok)
        .json(&json!({"label": "w2_e2e", "role": "ingest_and_flags"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ingest_key = key_resp["plaintext_key"].as_str().unwrap().to_string();

    // 1) POST /v1/engage -> returns canonical_uid & canonical_uids, immediately readable on next request.
    let eng_resp = http
        .post(format!("{base1}/v1/engage"))
        .header("x-pulse-key", &ingest_key)
        .json(&json!({"distinct_id": "w2_user", "$set": {"plan": "pro", "tier": "gold"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(eng_resp.status().as_u16(), 200);
    let eng_json: Value = eng_resp.json().await.unwrap();
    assert_eq!(eng_json["status"], "ok");
    assert_eq!(eng_json["updated"], 1);
    let uid = eng_json["canonical_uid"]
        .as_u64()
        .expect("/v1/engage must return canonical_uid");
    assert_eq!(eng_json["canonical_uids"], json!([uid]));

    let users_list: Vec<Value> = http
        .get(format!("{base1}/v1/projects/{pid}/users"))
        .bearer_auth(admin_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(users_list.len(), 1);
    assert_eq!(users_list[0]["canonical_uid"].as_u64(), Some(uid));
    assert_eq!(users_list[0]["props"]["plan"], "pro");

    let detail_1: Value = http
        .get(format!("{base1}/v1/projects/{pid}/users/w2_user"))
        .bearer_auth(admin_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail_1["canonical_uid"].as_u64(), Some(uid));
    assert_eq!(detail_1["profile"]["props"]["tier"], "gold");

    // 2) POST /v1/identify -> returns same canonical_uid, immediately readable via anon_id too.
    let id_resp = http
        .post(format!("{base1}/v1/identify"))
        .header("x-pulse-key", &ingest_key)
        .json(&json!({
            "anon_id": "anon_w2",
            "user_id": "w2_user",
            "user_properties": {"city": "Bengaluru"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(id_resp.status().as_u16(), 200);
    let id_json: Value = id_resp.json().await.unwrap();
    assert_eq!(id_json["canonical_uid"].as_u64(), Some(uid));

    let detail_anon: Value = http
        .get(format!("{base1}/v1/projects/{pid}/users/anon_w2"))
        .bearer_auth(admin_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail_anon["canonical_uid"].as_u64(), Some(uid));
    assert_eq!(detail_anon["profile"]["props"]["city"], "Bengaluru");
    assert_eq!(detail_anon["profile"]["props"]["plan"], "pro");

    // 3) POST /v1/groups + POST /v1/replay/chunk -> 200 and immediately readable.
    let grp_resp = http
        .post(format!("{base1}/v1/groups"))
        .header("x-pulse-key", &ingest_key)
        .json(&json!({"group_type": "merchant", "group_id": "m_w2", "$set": {"seats": 42}}))
        .send()
        .await
        .unwrap();
    assert_eq!(grp_resp.status().as_u16(), 200);
    let grp_json: Value = grp_resp.json().await.unwrap();
    assert_eq!(grp_json["updated"], 1);

    let rep_resp = http
        .post(format!("{base1}/v1/replay/chunk"))
        .header("x-pulse-key", &ingest_key)
        .json(&json!({
            "project_id": "",
            "session_id": "sess_w2_e2e",
            "chunk_start_ts_ms": chrono::Utc::now().timestamp_millis(),
            "ops": [{
                "op": "full_snapshot",
                "ts_offset_ms": 0,
                "width": 390,
                "height": 844,
                "nodes": [{
                    "node_id": 1,
                    "parent_id": 0,
                    "kind": "screen",
                    "x": 0,
                    "y": 0,
                    "w": 390,
                    "h": 844,
                    "bg_rgba": 255,
                    "masked_label": "Checkout"
                }]
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(rep_resp.status().as_u16(), 200);
    let rep_json: Value = rep_resp.json().await.unwrap();
    assert!(
        rep_json["replay_offset_kb"].as_u64().is_some(),
        "{rep_json}"
    );

    let rep_get_1 = http
        .get(format!("{base1}/v1/projects/{pid}/replay/sess_w2_e2e"))
        .bearer_auth(admin_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(rep_get_1.status().as_u16(), 200);

    // 4) Abrupt SIGKILL immediately after 200 OK (no graceful shutdown).
    let mut c1 = child1.0.take().unwrap();
    c1.kill().expect("SIGKILL pulse-node");
    let _ = c1.wait();

    // 5) Restart pulse-node on the same `--data-dir` and verify profile + canonical_uid + replay survive.
    let port2 = free_port();
    let base2 = format!("http://127.0.0.1:{port2}");
    let mut child2 = ChildGuard(Some(
        std::process::Command::new(bin)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port2.to_string(),
                "--no-quic",
                "--no-seed",
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--web-dir",
                web_dir.to_str().unwrap(),
            ])
            .env("PULSE_ADMIN_TOKEN", admin_tok)
            .env("PULSE_DEFAULT_PROJECT", pid)
            .env("PULSE_WRITER_LEASE_TTL_SECS", "2")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("respawn pulse-node"),
    ));
    wait_healthy(&http, &base2, child2.0.as_mut().unwrap()).await;

    let users_after: Vec<Value> = http
        .get(format!("{base2}/v1/projects/{pid}/users"))
        .bearer_auth(admin_tok)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(users_after.len(), 1);
    assert_eq!(users_after[0]["canonical_uid"].as_u64(), Some(uid));
    assert_eq!(users_after[0]["props"]["plan"], "pro");
    assert_eq!(users_after[0]["props"]["tier"], "gold");
    assert_eq!(users_after[0]["props"]["city"], "Bengaluru");

    // Wait briefly if dead process's 2s lease hasn't quite expired for export_events in GET /users/:did.
    let mut detail_after = Value::Null;
    for _ in 0..40 {
        let r = http
            .get(format!("{base2}/v1/projects/{pid}/users/w2_user"))
            .bearer_auth(admin_tok)
            .send()
            .await
            .unwrap();
        if r.status().as_u16() == 200 {
            detail_after = r.json().await.unwrap();
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert_eq!(
        detail_after["canonical_uid"].as_u64(),
        Some(uid),
        "{detail_after}"
    );
    assert_eq!(detail_after["profile"]["props"]["plan"], "pro");
    assert_eq!(detail_after["profile"]["props"]["city"], "Bengaluru");

    let rep_get_after = http
        .get(format!("{base2}/v1/projects/{pid}/replay/sess_w2_e2e"))
        .bearer_auth(admin_tok)
        .send()
        .await
        .unwrap();
    assert_eq!(rep_get_after.status().as_u16(), 200);
    let rep_after_json: Value = rep_get_after.json().await.unwrap();
    assert_eq!(
        rep_after_json["chunks"].as_array().map(|a| a.len()),
        Some(1)
    );
}
