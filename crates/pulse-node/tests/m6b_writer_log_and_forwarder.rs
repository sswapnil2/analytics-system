use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use pulse_node::{start_pulse_node_with_store_and_identity, NodeConfig, NodeIdentity, NodeRole};
use serde_json::{json, Value};

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

#[derive(Debug)]
struct CountingStore {
    inner: Arc<InMemory>,
    calls: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<InMemory>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            calls: AtomicUsize::new(0),
        })
    }

    fn total_calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn node_cfg(dir: &Path, project_id: &str, role: NodeRole, admin_token: &str) -> NodeConfig {
    NodeConfig {
        role,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: dir.to_path_buf(),
        web_dist_dir: dir.join("no_web"),
        default_project_id: project_id.to_string(),
        default_api_key: String::new(),
        admin_token: Some(admin_token.to_string()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 2,
    }
}

async fn count_events_on_storage(
    storage: &pulse_storage::StorageEngine,
    project_id: &str,
    event_name: &str,
) -> usize {
    let ps = storage.open_for_read(project_id).await.unwrap();
    let snap = ps.snapshot();
    let mut total = 0usize;
    let target_eid = ps.dictionary().lookup_event(event_name);
    for b in 0..ps.config().num_buckets {
        for row in ps.snapshot_l0_bucket(&snap, b as u8) {
            if Some(row.event_id) == target_eid {
                total += 1;
            }
        }
    }
    for seg in snap.manifest.active_segments.values().flatten() {
        for row in storage.segment_reader().read_all_rows(seg).await.unwrap() {
            if Some(row.event_id) == target_eid {
                total += 1;
            }
        }
    }
    total
}

#[test]
fn stateless_writer_isolation_pii_masking_mixed_batch_and_metrics() {
    let _env_lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let meta_db = tmp.path().join("meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );
        let _hot_env = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
        let _flush_env = EnvGuard::set("PULSE_WRITER_FLUSH_MS", Some("10"));
        let _role_env = EnvGuard::set("PULSE_NODE_ROLE", Some("writer"));
        assert_eq!(NodeConfig::default().role, NodeRole::Writer);

        let admin_token = "sk_root_m6b_writer_isolation_1234";
        let pid = "proj_w_iso";
        let s3_shared: Arc<InMemory> = Arc::new(InMemory::new());

        // Start owning storage node (Ingest) + Query node? Use All for storage+query inspection.
        let owner = start_pulse_node_with_store_and_identity(
            node_cfg(&tmp.path().join("owner"), pid, NodeRole::All, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("storage-1", None),
        )
        .await
        .unwrap();
        let ingest_key = owner
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        // Configure Lexicon rules (drop Noisy_Drop, PII-mask secret_pan).
        let http = reqwest::Client::new();
        let owner_url = format!("http://{}", owner.http_addr);
        assert_eq!(
            http.put(format!("{owner_url}/v1/projects/{pid}/lexicon/events/Noisy_Drop"))
                .bearer_auth(admin_token)
                .json(&json!({ "status": "dropped" }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        assert_eq!(
            http.put(format!("{owner_url}/v1/projects/{pid}/lexicon/properties/secret_pan"))
                .bearer_auth(admin_token)
                .json(&json!({ "pii_masked": true }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );

        // Start stateless Writer with CountingStore to assert ZERO S3 calls and zero local wal/hot files!
        let writer_counting = CountingStore::new(s3_shared.clone());
        let writer_dir = tmp.path().join("writer");
        let writer = start_pulse_node_with_store_and_identity(
            node_cfg(&writer_dir, pid, NodeRole::Writer, admin_token),
            writer_counting.clone(),
            NodeIdentity::new("writer-1", None),
        )
        .await
        .unwrap();

        assert_eq!(
            writer_counting.total_calls(),
            0,
            "NodeRole::Writer must make 0 S3 calls on boot"
        );
        assert!(!writer_dir.join("wal").exists());
        assert!(!writer_dir.join("hot").exists());
        assert!(!writer_dir.join("hot_store_mode").exists());
        assert!(writer_dir.join("writer_log/writer_id").is_file());

        let healthy = owner
            .control
            .meta()
            .list_healthy_writer_nodes(Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap();
        assert!(
            healthy.iter().all(|n| n.node_name != "writer-1"),
            "NodeRole::Writer must NOT register in writer_nodes"
        );

        let writer_url = format!("http://{}", writer.http_addr);
        assert_eq!(
            http.get(format!("{writer_url}/readyz"))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );

        // Reload caches on writer so it picks up the lexicon rules.
        writer.control.reload_caches_from_db().await.unwrap();

        // Pause forwarding by temporarily pointing project_owner to an unreachable node while we inspect writer_log disk bytes.
        let saved_owner = owner.control.meta().get_project_owner(pid).await.unwrap().unwrap();
        owner
            .control
            .meta()
            .set_project_owner(pid, "dead-node", "http://127.0.0.1:1", Utc::now().timestamp_millis())
            .await
            .unwrap();
        writer.control.reload_caches_from_db().await.unwrap();

        let now_ms = Utc::now().timestamp_millis();
        // Send event with Noisy_Drop + raw PII secret_pan -> PII must be masked BEFORE hitting writer_log!
        let r_pii = http
            .post(format!("{writer_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "events": [
                    { "distinct_id": "u1", "event_name": "Noisy_Drop", "client_event_ts_ms": now_ms },
                    { "distinct_id": "u1", "event_name": "Masked_Ev", "client_event_ts_ms": now_ms, "properties": { "secret_pan": "RAW_PAN_SECRET_999" } }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_pii.status(), reqwest::StatusCode::OK);
        let body_pii: Value = r_pii.json().await.unwrap();
        assert_eq!(body_pii["accepted"], 1);
        assert_eq!(body_pii["dropped"], 1);
        assert_eq!(body_pii["deduped"], 0);

        writer.ingest.flush_all().await.unwrap();
        let mut found_seg_bytes = Vec::new();
        for entry in std::fs::read_dir(writer_dir.join("writer_log").join(pid)).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().is_some_and(|e| e == "log") {
                found_seg_bytes.extend(std::fs::read(&p).unwrap());
            }
        }
        assert!(!found_seg_bytes.is_empty(), "expected writer_log segment file");
        assert!(
            !found_seg_bytes.windows(b"RAW_PAN_SECRET_999".len()).any(|w| w == b"RAW_PAN_SECRET_999"),
            "raw unmasked PII must never be persisted in writer_log!"
        );

        // Mixed batch when owner is unreachable -> 503 and ZERO events added to writer_log.
        let unforwarded_before = writer.ingest.writer_log().unwrap().stats().unforwarded_events;
        let r_mixed_err = http
            .post(format!("{writer_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "batch": [
                    { "type": "identify", "userId": "u_mix", "anonymousId": "a_mix" },
                    { "type": "track", "userId": "u_mix", "event": "Mixed_Should_Not_Queue" }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_mixed_err.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        writer.ingest.flush_all().await.unwrap();
        assert_eq!(
            writer.ingest.writer_log().unwrap().stats().unforwarded_events,
            unforwarded_before,
            "failed mixed-batch sync call must queue zero events"
        );

        // Restore real project_owner -> forwarder drains Masked_Ev to owner!
        owner
            .control
            .meta()
            .set_project_owner(
                pid,
                &saved_owner.node_name,
                &saved_owner.address,
                Utc::now().timestamp_millis(),
            )
            .await
            .unwrap();
        writer.control.reload_caches_from_db().await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if count_events_on_storage(owner.qengine.storage(), pid, "Masked_Ev").await == 1 {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "Masked_Ev never forwarded");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Mixed batch with identify 409 conflict (re-aliasing already-identified anon_id to different user) -> 409 & zero queued events!
        assert_eq!(
            http.post(format!("{owner_url}/v1/identify"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({ "anon_id": "anon_conflict", "user_id": "user_first" }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        let r_conflict = http
            .post(format!("{writer_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "batch": [
                    { "type": "identify", "userId": "user_second", "anonymousId": "anon_conflict" },
                    { "type": "track", "userId": "user_second", "event": "Conflict_Should_Not_Queue" }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_conflict.status(), reqwest::StatusCode::CONFLICT);
        writer.ingest.flush_all().await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            count_events_on_storage(owner.qengine.storage(), pid, "Conflict_Should_Not_Queue").await,
            0
        );

        // Verify /metrics exports all required writer_log & forwarder metrics with ZERO project_id= labels.
        let metrics_text = http
            .get(format!("{writer_url}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!metrics_text.contains("project_id="), "{metrics_text}");
        for metric in [
            "pulse_writer_log_bytes",
            "pulse_writer_log_capacity_bytes",
            "pulse_writer_log_unforwarded_batches",
            "pulse_writer_log_unforwarded_events",
            "pulse_writer_forwarding_lag_seconds",
            "pulse_writer_log_open_handles",
            "pulse_writer_log_expired_batches_dropped_total",
            "pulse_writer_log_expired_events_dropped_total",
            "pulse_writer_forward_batches_total{result=\"ok\"}",
            "pulse_writer_forward_retries_total",
        ] {
            assert!(metrics_text.contains(metric), "missing {metric} in:\n{metrics_text}");
        }

        // Verify deploy/prometheus-alerts.yml contains all 4 new alert rules.
        let alerts_raw = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/prometheus-alerts.yml"),
        )
        .unwrap();
        for alert in [
            "alert: PulseWriterForwardingLagHigh",
            "alert: PulseWriterForwardingLagApproachingDedupWindow",
            "alert: PulseWriterLogDiskSaturation",
            "alert: PulseWriterLogExpiredBatchesDropped",
        ] {
            assert!(alerts_raw.contains(alert), "missing {alert}");
        }

        writer.shutdown().await;
        owner.shutdown().await;
    });
}

#[test]
fn two_writers_two_storage_nodes_forwarding_outage_reassign_wipe_and_fastpath() {
    let _env_lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let meta_db = tmp.path().join("meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );
        let _hot_env = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
        let _flush_env = EnvGuard::set("PULSE_WRITER_FLUSH_MS", Some("10"));
        let _backoff_env = EnvGuard::set("PULSE_FORWARDER_MAX_BACKOFF_MS", Some("100"));
        let _owner_ttl_env = EnvGuard::set("PULSE_OWNER_CACHE_TTL_MS", Some("60000"));

        let admin_token = "sk_root_m6b_multinode_1234";
        let s3_shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Start 2 storage nodes (Ingest) and 2 stateless writers (Writer).
        let s1_dir = tmp.path().join("storage_1");
        let s2_dir = tmp.path().join("storage_2");
        let w1_dir = tmp.path().join("writer_1");
        let w2_dir = tmp.path().join("writer_2");

        let storage_1 = start_pulse_node_with_store_and_identity(
            node_cfg(&s1_dir, "proj_a", NodeRole::Ingest, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("storage-1", None),
        )
        .await
        .unwrap();
        let storage_2 = start_pulse_node_with_store_and_identity(
            node_cfg(&s2_dir, "proj_a", NodeRole::Ingest, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("storage-2", None),
        )
        .await
        .unwrap();

        // Create proj_b and assign proj_a -> storage-1, proj_b -> storage-2.
        let meta = storage_1.control.meta().clone();
        let org_id = meta.get_project("proj_a").await.unwrap().unwrap().org_id;
        let now_ms = Utc::now().timestamp_millis();
        meta.insert_project(&pulse_control::Project {
            id: "proj_b".to_string(),
            org_id,
            name: "Project B".to_string(),
            timezone: "UTC".to_string(),
            created_at_ms: now_ms,
        })
        .await
        .unwrap();
        let (key_b_raw, key_b_hash, key_b_prefix) =
            pulse_control::generate_random_api_key(pulse_control::KEY_ROLE_INGEST_AND_FLAGS);
        meta.insert_api_key(&pulse_control::ApiKeyRecord {
            id: "key_proj_b".to_string(),
            key_hash: key_b_hash,
            key_prefix: key_b_prefix,
            project_id: "proj_b".to_string(),
            label: "Test Key B".to_string(),
            role: pulse_control::KEY_ROLE_INGEST_AND_FLAGS.to_string(),
            created_at_ms: now_ms,
            revoked: false,
        })
        .await
        .unwrap();
        let key_a_raw = storage_1
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        meta.set_project_owner(
            "proj_a",
            "storage-1",
            &format!("http://{}", storage_1.http_addr),
            now_ms,
        )
        .await
        .unwrap();
        meta.set_project_owner(
            "proj_b",
            "storage-2",
            &format!("http://{}", storage_2.http_addr),
            now_ms,
        )
        .await
        .unwrap();

        let writer_1 = start_pulse_node_with_store_and_identity(
            node_cfg(&w1_dir, "proj_a", NodeRole::Writer, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("writer-1", None),
        )
        .await
        .unwrap();
        let writer_2 = start_pulse_node_with_store_and_identity(
            node_cfg(&w2_dir, "proj_a", NodeRole::Writer, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("writer-2", None),
        )
        .await
        .unwrap();

        let http = reqwest::Client::new();
        let w1_url = format!("http://{}", writer_1.http_addr);
        let w2_url = format!("http://{}", writer_2.http_addr);
        let s2_url = format!("http://{}", storage_2.http_addr);

        // Internal endpoint auth & ForwardLoopGuard checks on storage-2.
        assert_eq!(
            http.post(format!("{s2_url}/internal/v1/forward/events"))
                .body(vec![1, 2, 3])
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        let tok = meta.resolve_or_create_internal_token().await.unwrap();
        let r_loop = http
            .post(format!("{s2_url}/internal/v1/forward/events"))
            .header("x-pulse-internal-token", &tok)
            .header("x-pulse-forwarded-hop", "2")
            .header("x-pulse-forwarded-by", "writer-1")
            .body(vec![1, 2, 3])
            .send()
            .await
            .unwrap();
        assert_eq!(r_loop.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let loop_json: Value = r_loop.json().await.unwrap();
        assert_eq!(loop_json["code"], "forward_loop_detected");

        // Send events via BOTH writers to BOTH projects; include large device skew (>5s) to verify
        // server timestamping is NOT double-shifted on the owner!
        let skewed_sent = now_ms - 600_000;
        let skewed_event = now_ms - 605_000;
        for i in 0..4 {
            let url = if i % 2 == 0 { &w1_url } else { &w2_url };
            assert_eq!(
                http.post(format!("{url}/v1/ingest"))
                    .header("x-pulse-key", &key_a_raw)
                    .json(&json!({
                        "events": [{
                            "distinct_id": format!("ua_{i}"),
                            "event_name": "Multi_A",
                            "client_event_ts_ms": skewed_event,
                            "client_sent_ts_ms": skewed_sent
                        }]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                reqwest::StatusCode::OK
            );
            assert_eq!(
                http.post(format!("{url}/v1/ingest"))
                    .header("x-pulse-key", &key_b_raw)
                    .json(&json!({
                        "events": [{
                            "distinct_id": format!("ub_{i}"),
                            "event_name": "Multi_B",
                            "client_event_ts_ms": now_ms
                        }]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                reqwest::StatusCode::OK
            );
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let ca = count_events_on_storage(storage_1.qengine.storage(), "proj_a", "Multi_A").await;
            let cb = count_events_on_storage(storage_2.qengine.storage(), "proj_b", "Multi_B").await;
            if ca == 4 && cb == 4 {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "ca={ca}, cb={cb}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Verify storage-1 timestamp was normalized ONCE (near now_ms - 5_000, not shifted twice).
        let ps_a = storage_1.qengine.storage().open_for_read("proj_a").await.unwrap();
        let snap_a = ps_a.snapshot();
        for b in 0..ps_a.config().num_buckets {
            for row in ps_a.snapshot_l0_bucket(&snap_a, b as u8) {
                assert!(
                    (row.event_ts_ms - (now_ms - 5_000)).abs() < 15_000,
                    "event_ts_ms shifted unexpectedly: {} vs {}",
                    row.event_ts_ms,
                    now_ms - 5_000
                );
            }
        }
        drop(snap_a);

        // Stop storage-1: proj_a on writer-1 hits per-project 429 disk cap without blocking proj_b -> storage-2!
        storage_1.shutdown().await;
        writer_1
            .ingest
            .writer_log()
            .unwrap()
            .set_disk_limits_for_test(400, 1024 * 1024);

        let mut hit_429 = false;
        for i in 0..20 {
            let resp = http
                .post(format!("{w1_url}/v1/ingest"))
                .header("x-pulse-key", &key_a_raw)
                .json(&json!({
                    "events": [{
                        "distinct_id": format!("u_hold_{i}"),
                        "event_name": "Outage_Hold",
                        "client_event_ts_ms": now_ms,
                        "properties": { "pad": "0123456789abcdef0123456789abcdef" }
                    }]
                }))
                .send()
                .await
                .unwrap();
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let b: Value = resp.json().await.unwrap();
                assert_eq!(b["code"], "writer_log_backpressure", "{b}");
                hit_429 = true;
                break;
            }
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
            writer_1.ingest.flush_all().await.unwrap();
        }
        assert!(hit_429, "expected proj_a to hit 429 writer_log_backpressure while storage-1 down");

        // proj_b MUST still ingest & forward cleanly to storage-2 (zero cross-project HOL blocking)!
        assert_eq!(
            http.post(format!("{w1_url}/v1/ingest"))
                .header("x-pulse-key", &key_b_raw)
                .json(&json!({
                    "events": [{
                        "distinct_id": "ub_no_hol",
                        "event_name": "Multi_B_NoHol",
                        "client_event_ts_ms": now_ms
                    }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if count_events_on_storage(storage_2.qengine.storage(), "proj_b", "Multi_B_NoHol").await
                == 1
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "proj_b blocked by proj_a!");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Restart storage-1: writer-1's forwarder drains buffered Outage_Hold batches with 0 duplicates!
        let queued_a_events = writer_1.ingest.writer_log().unwrap().stats().unforwarded_events;
        assert!(queued_a_events > 0);
        let storage_1_restarted = start_pulse_node_with_store_and_identity(
            node_cfg(&s1_dir, "proj_a", NodeRole::Ingest, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("storage-1", None),
        )
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let drained = count_events_on_storage(
                storage_1_restarted.qengine.storage(),
                "proj_a",
                "Outage_Hold",
            )
            .await;
            if drained == queued_a_events
                && writer_1.ingest.writer_log().unwrap().stats().unforwarded_events == 0
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expected {queued_a_events}, got {drained}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Reassign proj_a to storage-3 after publishing & restoring proj_a onto storage-3's volume
        // (verifying 503 not_owner refresh + restored writer_offsets + zero duplicate replay).
        storage_1_restarted.storage.publish_all_projects().await.unwrap();
        let s3_dir = tmp.path().join("storage_3");
        {
            let routed_r = pulse_storage::open_hot_store_for_restore(s3_shared.clone(), &s3_dir)
                .await
                .unwrap();
            let enc_r = Arc::new(pulse_storage::EncryptedObjectStore::new(routed_r));
            let pub_m = pulse_storage::PublishedManifestStore::new("proj_a", enc_r.as_object_store())
                .load_latest()
                .await
                .unwrap()
                .expect("published manifest for proj_a");
            let restored = pulse_storage::restore_project_from_published(
                "proj_a",
                &pub_m,
                enc_r.as_object_store(),
            )
            .await
            .unwrap();
            restored.identity_graph.close().await.unwrap();
            if let Some(p) = restored.profile_store {
                p.close().await.unwrap();
            }
            pulse_storage::finalize_local_hot_store_marker(&s3_dir)
                .await
                .unwrap();
        }
        let storage_3 = start_pulse_node_with_store_and_identity(
            node_cfg(&s3_dir, "proj_a", NodeRole::Ingest, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("storage-3", None),
        )
        .await
        .unwrap();

        meta.set_project_owner(
            "proj_a",
            "storage-3",
            &format!("http://{}", storage_3.http_addr),
            Utc::now().timestamp_millis(),
        )
        .await
        .unwrap();
        storage_1_restarted.storage.reconcile_ownership().await;
        // Writer-1 still holds storage_1_restarted in its owner cache -> forwarder hits 503 not_owner
        // and refreshes to storage-3!
        assert_eq!(
            http.post(format!("{w1_url}/v1/ingest"))
                .header("x-pulse-key", &key_a_raw)
                .json(&json!({
                    "events": [{
                        "distinct_id": "ua_moved",
                        "event_name": "Moved_To_S3",
                        "client_event_ts_ms": now_ms
                    }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if writer_1.ingest.forwarder().unwrap().stats().batches_not_owner > 0 {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "expected 503 not_owner retry");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Gracefully stop storage_1_restarted so its writer lease releases cleanly for storage-3.
        storage_1_restarted.shutdown().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if count_events_on_storage(storage_3.qengine.storage(), "proj_a", "Moved_To_S3").await
                == 1
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "Moved_To_S3 not delivered to storage-3");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Wipe writer-1's disk -> restart -> new writer_id starts at offset 0 and is NOT dropped as repeat!
        let old_writer_id = writer_1.ingest.writer_log().unwrap().writer_id().to_string();
        writer_1.shutdown().await;
        std::fs::remove_dir_all(w1_dir.join("writer_log")).unwrap();
        let writer_1_wiped = start_pulse_node_with_store_and_identity(
            node_cfg(&w1_dir, "proj_a", NodeRole::Writer, admin_token),
            s3_shared.clone(),
            NodeIdentity::new("writer-1", None),
        )
        .await
        .unwrap();
        assert_ne!(
            writer_1_wiped.ingest.writer_log().unwrap().writer_id(),
            old_writer_id
        );
        assert_eq!(
            http.post(format!("http://{}/v1/ingest", writer_1_wiped.http_addr))
                .header("x-pulse-key", &key_a_raw)
                .json(&json!({
                    "events": [{
                        "distinct_id": "ua_wiped",
                        "event_name": "After_Wipe",
                        "client_event_ts_ms": now_ms
                    }]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if count_events_on_storage(storage_3.qengine.storage(), "proj_a", "After_Wipe").await == 1
            {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "After_Wipe dropped as duplicate!");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Verify NodeRole::All single-node §6.5 fast-path creates ZERO writer_log/ directory.
        let single_dir = tmp.path().join("single_all");
        let single_db = tmp.path().join("single_meta.db");
        let _single_db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", single_db.display())),
        );
        let single = start_pulse_node_with_store_and_identity(
            node_cfg(&single_dir, "proj_single", NodeRole::All, admin_token),
            Arc::new(InMemory::new()),
            None,
        )
        .await
        .unwrap();
        let single_key = single.bootstrap_credentials.generated_ingest_key.clone().unwrap();
        assert_eq!(
            http.post(format!("http://{}/v1/ingest", single.http_addr))
                .header("x-pulse-key", &single_key)
                .json(&json!({ "events": [{ "distinct_id": "u1", "event_name": "FastPath", "client_event_ts_ms": now_ms }] }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        single.ingest.flush_all().await.unwrap();
        assert!(
            !single_dir.join("writer_log").exists(),
            "NodeRole::All fast-path must never create writer_log/"
        );
        assert!(single.ingest.writer_log().is_none());

        single.shutdown().await;
        writer_1_wiped.shutdown().await;
        writer_2.shutdown().await;
        storage_2.shutdown().await;
        storage_3.shutdown().await;
    });
}
