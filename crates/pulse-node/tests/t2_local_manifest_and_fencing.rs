//! T2 integration & fault-injection tests for Features #14, #15, #16, and M2 Caveats #1 & #2:
//! - #14: Local-authoritative `manifests/` (`<data_dir>/hot/manifests`), `PULSE_UPLOAD_INTERVAL_SECS`
//!   (300s default vs `0` eager), 0 S3 `manifests/`/`published/` calls on flush/compaction/idle,
//!   and `publish_enabled` check in `compact_min_segments`.
//! - #15: Dense `publish_seq` fencing on write-open (`StaleWriter` / HTTP `503`), step-6 self-crash
//!   recovery on `pub_0` and `pub_{P+1}`, wiped-`manifests/` refusal, behind-writer refusal, and
//!   live/restarting deposed-writer fencing even with higher `local.epoch` and higher `lease_gen`.
//! - #16 & M2 Caveat #1: Persisted `pending_s3_deletes` with 2-publish-interval grace for BOTH
//!   superseded S3 segments and superseded S3 snapshot blobs across restarts, pin holds, and crashes.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_control::MetaStore;
use pulse_identity::IdentityRouter;
use pulse_node::{start_pulse_node_with_store, NodeConfig, NodeRole};
use pulse_storage::{
    finalize_local_hot_store_marker, open_hot_store_for_restore, parse_byok_master_key,
    restore_project_from_published, with_hot_store, EncryptedObjectStore, IdentitySnapshotRef,
    ManifestStore, PublishedManifest, PublishedManifestStore, SnapshotRef, StorageConfig,
    StorageEngine, StorageError, StoredUserProfile, WriterLeases,
};
use pulse_wire::WireEvent;
use serde_json::json;

const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ADMIN_TOKEN: &str = "sk_root_t2_local_manifest_fencing_test_token_00000000";

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

fn sum_prefix_ops(calls: &BTreeMap<(String, &'static str), u64>, prefix: &str, op: &str) -> u64 {
    calls
        .iter()
        .filter(|((p, o), _)| p == prefix && *o == op)
        .map(|(_, n)| *n)
        .sum()
}

fn total_calls(calls: &BTreeMap<(String, &'static str), u64>) -> u64 {
    calls.values().sum()
}

fn make_event(did: &str, ts_ms: i64, seq: u64) -> WireEvent {
    WireEvent {
        event_uuid: WireEvent::deterministic_uuid(did, "sess", "click", ts_ms, seq),
        routing_seed: did.into(),
        distinct_id: did.into(),
        session_id: "sess".into(),
        event_name: "click".into(),
        client_event_ts_ms: ts_ms,
        client_sent_ts_ms: 0,
        props: vec![],
        replay_offset_kb: 0,
    }
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
        num_buckets: 1,
    }
}

#[derive(Default)]
struct FixedLeases {
    gens: Mutex<HashMap<String, u64>>,
}

impl FixedLeases {
    fn set_gen(&self, project_id: &str, gen: u64) {
        self.gens
            .lock()
            .unwrap()
            .insert(project_id.to_string(), gen);
    }
}

#[async_trait]
impl WriterLeases for FixedLeases {
    async fn acquire(&self, project_id: &str) -> Result<Option<u64>, String> {
        Ok(Some(
            *self.gens.lock().unwrap().get(project_id).unwrap_or(&1),
        ))
    }
    fn ttl(&self) -> Duration {
        Duration::from_secs(30)
    }
}

#[test]
fn t2_14_local_manifests_zero_s3_flush_eager_zero_and_idle_zero_calls() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", None);
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("node");
        let s3_counting = Arc::new(CountingS3::default());
        let s3_raw: Arc<dyn ObjectStore> = s3_counting.clone();

        let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(routed));
        let id_router = Arc::new(IdentityRouter::new(enc.as_object_store()));
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 100,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 4,
        };
        let engine = StorageEngine::new_with_encrypted(cfg, enc.clone(), id_router);
        assert_eq!(engine.upload_interval(), Duration::from_secs(300));

        let pid = "proj_t2_14";
        let ps = engine.get_or_open_project(pid).await.unwrap();
        let ig = engine.identity_graph(pid, true).await.unwrap();

        // 4 separate flushes -> 0 S3 segments/, manifests/, or published/ calls;
        // segments written locally under data_dir/hot/hot/segments and manifests under data_dir/hot/manifests.
        s3_counting.take();
        for i in 0..4u64 {
            let uid = ig.get_or_intern(&format!("u_{i}"), 0);
            ps.append_bucket_events(
                0,
                vec![(
                    make_event(&format!("u_{i}"), 1_790_000_000_000 + i as i64 * 1000, i),
                    uid,
                )],
            )
            .await
            .unwrap();
            ps.flush_bucket(0).await.unwrap();
        }
        let flush_calls = s3_counting.take();
        assert_eq!(sum_prefix_ops(&flush_calls, "manifests", "PUT"), 0);
        assert_eq!(sum_prefix_ops(&flush_calls, "published", "PUT"), 0);
        assert_eq!(sum_prefix_ops(&flush_calls, "segments", "PUT"), 0);
        assert!(ps.last_published_seq().is_none());
        assert!(data_dir.join("hot/manifests").join(pid).is_dir());
        assert!(data_dir.join("hot/hot/segments").join(pid).is_dir());

        // Count-based compaction is disabled for the local segment tier when PULSE_UPLOAD_INTERVAL_SECS > 0.
        s3_counting.take();
        let compacted = ps.compact_overdue(engine.segment_reader()).await.unwrap();
        assert_eq!(compacted, 0);
        assert_eq!(total_calls(&s3_counting.take()), 0);
        assert_eq!(ps.snapshot().manifest.total_segments(), 4);

        // Publish cycle uploads local segments (merging 4 -> 1 S3 segment) and writes pub_0 to S3,
        // persisting last_published_seq = Some(0) locally and cleaning up hot/segments/.
        s3_counting.take();
        let pub_count = engine
            .upload_all_projects_at(
                chrono::DateTime::parse_from_rfc3339("2026-09-21T12:00:00Z")
                    .unwrap()
                    .timestamp_millis(),
            )
            .await
            .unwrap();
        assert_eq!(pub_count, 1);
        let pub_calls = s3_counting.take();
        assert_eq!(sum_prefix_ops(&pub_calls, "manifests", "PUT"), 0);
        assert_eq!(sum_prefix_ops(&pub_calls, "segments", "PUT"), 1);
        assert_eq!(sum_prefix_ops(&pub_calls, "published", "PUT"), 2); // identity + pub_0 (no profiles yet)
        assert_eq!(ps.last_published_seq(), Some(0));
        assert_eq!(ps.snapshot().manifest.total_segments(), 1);
        assert_eq!(ps.hot_segment_bytes(), 0);

        let local_mf = ManifestStore::new(pid, enc.as_object_store())
            .load_latest()
            .await
            .unwrap();
        assert_eq!(local_mf.last_published_seq, Some(0));

        // Idle publish cycle makes ZERO S3 calls.
        s3_counting.take();
        assert_eq!(engine.publish_all_projects().await.unwrap(), 0);
        assert_eq!(total_calls(&s3_counting.take()), 0);

        // M2 Caveat #2: with upload_interval=0 and publish_enabled=false, compact_min_segments does NOT publish.
        engine.set_upload_interval(Duration::ZERO);
        ps.set_publish_enabled(false);
        for i in 4..8u64 {
            let uid = ig.get_or_intern(&format!("u_{i}"), 0);
            ps.append_bucket_events(
                0,
                vec![(
                    make_event(&format!("u_{i}"), 1_790_000_010_000 + i as i64 * 1000, i),
                    uid,
                )],
            )
            .await
            .unwrap();
            ps.flush_bucket(0).await.unwrap();
        }
        assert_eq!(ps.last_published_seq(), Some(0));
        let compacted2 = ps
            .compact_min_segments(engine.segment_reader(), 4)
            .await
            .unwrap();
        assert_eq!(compacted2, 1);
        assert_eq!(
            ps.last_published_seq(),
            Some(0),
            "publish_enabled=false must gate compact publish"
        );

        // Re-enable publish with PULSE_UPLOAD_INTERVAL_SECS=0 -> flush publishes eagerly while keeping manifests/ local.
        ps.set_publish_enabled(true);
        s3_counting.take();
        let uid8 = ig.get_or_intern("u_8", 0);
        ps.append_bucket_events(0, vec![(make_event("u_8", 1_790_000_020_000, 8), uid8)])
            .await
            .unwrap();
        ps.flush_bucket(0).await.unwrap();
        let eager_calls = s3_counting.take();
        assert_eq!(sum_prefix_ops(&eager_calls, "manifests", "PUT"), 0);
        assert_eq!(sum_prefix_ops(&eager_calls, "published", "PUT"), 2); // identity + pub_1
        assert_eq!(ps.last_published_seq(), Some(1));
    });
}

#[test]
fn t2_15_dense_publish_seq_fencing_and_step6_self_crash_recovery() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("300"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 100,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 10,
        };

        // ------------------------------------------------------------------
        // A. Step-6 self-crash on `pub_0` (local_last_pub_seq == None) and
        //    wiped-manifests refusal (`local_epoch == 0`).
        // ------------------------------------------------------------------
        {
            let pid = "proj_step6_pub0";
            let tmp = tempfile::tempdir().unwrap();
            let data_dir = tmp.path().join("node");
            let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let leases = Arc::new(FixedLeases::default());
            leases.set_gen(pid, 1);

            let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc = Arc::new(EncryptedObjectStore::new(routed));
            let engine = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc.clone(),
                Arc::new(IdentityRouter::new(enc.as_object_store())),
            );
            engine.set_writer_leases(leases.clone());

            let ps = engine.get_or_open_project(pid).await.unwrap();
            let ig = engine.identity_graph(pid, true).await.unwrap();
            let u0 = ig.get_or_intern("u0", 0);
            ps.append_bucket_events(0, vec![(make_event("u0", 1_790_000_000_000, 0), u0)])
                .await
                .unwrap();
            ps.flush_bucket(0).await.unwrap(); // local epoch >= 1, last_published_seq = None

            // Simulate step-6 crash right after pub_0 lands in S3 BEFORE local manifest records it.
            let snap = ps.snapshot();
            let id_exp = ig.export_snapshot().await.unwrap();
            let id_path = format!("published/{pid}/snapshots/identity_000000000000_crash.pzst");
            enc.as_object_store()
                .put(
                    &ObjectPath::from(id_path.as_str()),
                    PutPayload::from(id_exp.payload),
                )
                .await
                .unwrap();
            PublishedManifestStore::new(pid, enc.as_object_store())
                .publish_next(PublishedManifest {
                    publish_seq: 0,
                    writer_generation: snap.manifest.writer_generation,
                    source_local_epoch: snap.manifest.epoch,
                    high_water_mark: id_exp.user_count,
                    published_at_ms: chrono::Utc::now().timestamp_millis(),
                    partitions: snap.manifest.to_sorted_partitions(),
                    dict_snapshot: snap.manifest.dict_snapshot.clone(),
                    committed_wal_seq: snap.manifest.committed_wal_seq.clone(),
                    committed_l0_seq: snap.manifest.committed_l0_seq.clone(),
                    snapshot_ref: SnapshotRef {
                        identity: IdentitySnapshotRef {
                            object_path: id_path,
                            user_count: id_exp.user_count,
                            mutation_epoch: id_exp.mutation_epoch,
                        },
                        profiles: None,
                    },
                })
                .await
                .unwrap();
            drop(snap);
            ig.close().await.unwrap();
            drop(ps);
            drop(engine);

            // Reopen with same local volume -> recovers pub_0 and persists last_published_seq = Some(0)!
            let routed2 = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc2 = Arc::new(EncryptedObjectStore::new(routed2));
            let engine2 = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc2.clone(),
                Arc::new(IdentityRouter::new(enc2.as_object_store())),
            );
            engine2.set_writer_leases(leases.clone());
            let ps2 = engine2.get_or_open_project(pid).await.unwrap();
            assert_eq!(ps2.last_published_seq(), Some(0));
            let disk_mf = ManifestStore::new(pid, enc2.as_object_store())
                .load_latest()
                .await
                .unwrap();
            assert_eq!(disk_mf.last_published_seq, Some(0));

            // Next publish writes pub_1 cleanly.
            let ig2 = engine2.identity_graph(pid, true).await.unwrap();
            let u1 = ig2.get_or_intern("u1", 0);
            ps2.append_bucket_events(0, vec![(make_event("u1", 1_790_000_001_000, 1), u1)])
                .await
                .unwrap();
            ps2.flush_bucket(0).await.unwrap();
            assert_eq!(engine2.publish_all_projects().await.unwrap(), 1);
            assert_eq!(ps2.last_published_seq(), Some(1));
            ig2.close().await.unwrap();
            drop(ps2);
            drop(engine2);

            // Behind-writer check: S3 jumps to pub_2 & pub_3 with same writer_generation=1 while
            // local disk has last_published_seq = Some(1) -> must refuse with StaleWriter!
            let pms_check = PublishedManifestStore::new(pid, enc2.as_object_store());
            let mut pub_ahead = pms_check.load_latest().await.unwrap().unwrap();
            pub_ahead.publish_seq = 2;
            pms_check.publish_next(pub_ahead.clone()).await.unwrap();
            pub_ahead.publish_seq = 3;
            pms_check.publish_next(pub_ahead).await.unwrap();

            let routed_behind = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc_behind = Arc::new(EncryptedObjectStore::new(routed_behind));
            let engine_behind = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc_behind.clone(),
                Arc::new(IdentityRouter::new(enc_behind.as_object_store())),
            );
            engine_behind.set_writer_leases(leases.clone());
            let err_behind = engine_behind
                .get_or_open_project(pid)
                .await
                .map(|_| ())
                .expect_err("behind local last_published_seq by >= 2 must fail");
            assert!(matches!(err_behind, StorageError::StaleWriter { .. }), "{err_behind}");
            drop(engine_behind);

            // Wiped manifests/ (`local_epoch == 0`) while published/ exists in S3 MUST refuse open!
            let mf_dir = data_dir.join("hot/manifests").join(pid);
            std::fs::remove_dir_all(&mf_dir).unwrap();
            std::fs::create_dir_all(&mf_dir).unwrap();
            let routed3 = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc3 = Arc::new(EncryptedObjectStore::new(routed3));
            let engine3 = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc3.clone(),
                Arc::new(IdentityRouter::new(enc3.as_object_store())),
            );
            engine3.set_writer_leases(leases);
            let err = engine3
                .get_or_open_project(pid)
                .await
                .map(|_| ())
                .expect_err("wiped local manifests with S3 published/ must fail");
            assert!(matches!(err, StorageError::StaleWriter { .. }), "{err}");
        }

        // ------------------------------------------------------------------
        // B. Deposed writer fencing (live higher local.epoch + restarting
        //    with higher lease_gen) + behind-writer refusal + HTTP 503.
        // ------------------------------------------------------------------
        {
            let pid = "proj_fence_deposed";
            let tmp = tempfile::tempdir().unwrap();
            let dir_a = tmp.path().join("node_a");
            let dir_b = tmp.path().join("node_b");
            let meta_db = tmp.path().join("shared_meta.db");
            let _db_env = EnvGuard::set(
                "DATABASE_URL",
                Some(&format!("sqlite://{}", meta_db.display())),
            );
            let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

            // Writer A starts via full node (real DbWriterLeases), ingests, publishes pub_0,
            // and runs extra local flushes so Writer A's local.epoch (e.g. 6) > Writer B's!
            let node_a = start_pulse_node_with_store(
                node_cfg(&dir_a, pid, NodeRole::All),
                s3_raw.clone(),
            )
            .await
            .unwrap();
            let key_a = node_a
                .bootstrap_credentials
                .generated_ingest_key
                .clone()
                .unwrap();
            let http = reqwest::Client::new();
            let now_ms = chrono::Utc::now().timestamp_millis();
            assert_eq!(
                http.post(format!("http://{}/v1/ingest", node_a.http_addr))
                    .header("x-pulse-key", &key_a)
                    .json(&json!({ "events": [{ "distinct_id": "ua_0", "event_name": "e", "client_event_ts_ms": now_ms }] }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            node_a.ingest.flush_all().await.unwrap();
            node_a.storage.flush_all_projects().await.unwrap();
            node_a.storage.publish_all_projects().await.unwrap();

            // Extra local flushes on Writer A so local.epoch jumps ahead without publishing.
            for k in 1..=4 {
                assert_eq!(
                    http.post(format!("http://{}/v1/ingest", node_a.http_addr))
                        .header("x-pulse-key", &key_a)
                        .json(&json!({ "events": [{ "distinct_id": format!("ua_{k}"), "event_name": "e", "client_event_ts_ms": now_ms + k }] }))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    200
                );
                node_a.ingest.flush_all().await.unwrap();
            node_a.storage.flush_all_projects().await.unwrap();
            }
            let ps_a = node_a.storage.get_or_open_project(pid).await.unwrap();
            let a_local_epoch = ps_a.snapshot().epoch();
            assert!(a_local_epoch >= 6);

            // Simulate Writer A partitioned from MetaStore / lease expires -> Writer B restores pub_0 and takes over!
            let meta = MetaStore::connect_auto(&dir_b).await.unwrap();
            let b_gen = meta
                .acquire_writer_lease(pid, "node_b_takeover", i64::MAX / 2, 0)
                .await
                .unwrap()
                .expect("takeover lease in MetaStore must bump generation");
            assert_eq!(b_gen, 2);
            let leases_b = Arc::new(FixedLeases::default());
            leases_b.set_gen(pid, b_gen);

            let enc_s3 = Arc::new(EncryptedObjectStore::new(s3_raw.clone()));
            enc_s3
                .register_master_key(
                    &pulse_storage::make_key_id(&parse_byok_master_key(TEST_BYOK_KEY).unwrap()),
                    parse_byok_master_key(TEST_BYOK_KEY).unwrap(),
                    true,
                )
                .unwrap();
            let pub0 = PublishedManifestStore::new(pid, enc_s3.as_object_store())
                .load_latest()
                .await
                .unwrap()
                .unwrap();

            let r_routed = open_hot_store_for_restore(s3_raw.clone(), &dir_b)
                .await
                .unwrap();
            let r_enc = Arc::new(EncryptedObjectStore::new(r_routed));
            let restored =
                restore_project_from_published(pid, &pub0, r_enc.as_object_store())
                    .await
                    .unwrap();
            restored.identity_graph.close().await.unwrap();
            if let Some(p) = restored.profile_store {
                p.close().await.unwrap();
            }
            finalize_local_hot_store_marker(&dir_b).await.unwrap();

            let routed_b = with_hot_store(s3_raw.clone(), &dir_b).await.unwrap();
            let enc_b = Arc::new(EncryptedObjectStore::new(routed_b));
            let engine_b = StorageEngine::new_with_encrypted(
                cfg.clone(),
                enc_b.clone(),
                Arc::new(IdentityRouter::new(enc_b.as_object_store())),
            );
            engine_b.set_writer_leases(leases_b);
            let ps_b = engine_b.get_or_open_project(pid).await.unwrap();
            let ig_b = engine_b.identity_graph(pid, true).await.unwrap();
            let ub = ig_b.get_or_intern("ub_1", 0);
            ps_b.append_bucket_events(0, vec![(make_event("ub_1", now_ms + 100, 10), ub)])
                .await
                .unwrap();
            ps_b.flush_bucket(0).await.unwrap();
            assert_eq!(engine_b.publish_all_projects().await.unwrap(), 1);
            assert_eq!(ps_b.last_published_seq(), Some(1));
            let pub1 = PublishedManifestStore::new(pid, enc_s3.as_object_store())
                .load_latest()
                .await
                .unwrap()
                .unwrap();
            assert!(pub1.writer_generation > pub0.writer_generation);
            assert!(pub1.source_local_epoch < a_local_epoch);

            // Live deposed Writer A (even with higher local.epoch!) tries to publish -> fenced (NotWriter)!
            let fence_err = ps_a
                .publish_if_dirty()
                .await
                .expect_err("live deposed writer must fail publish_next");
            assert!(matches!(fence_err, StorageError::NotWriter { .. }), "{fence_err}");

            // Shut down Writer A cleanly and Writer B so we can test
            // deposed Writer A restarting with an even HIGHER lease generation!
            drop(ps_a);
            node_a.shutdown().await;
            ig_b.close().await.unwrap();
            drop(ps_b);
            drop(engine_b);

            // Deposed Writer A restarts on `dir_a` (gets lease_gen = 3 > pub1.writer_generation = 2,
            // and has local.epoch >= 6 > pub1.source_local_epoch): MUST still refuse open with StaleWriter -> HTTP 503!
            let node_a2 = start_pulse_node_with_store(
                node_cfg(&dir_a, pid, NodeRole::All),
                s3_raw.clone(),
            )
            .await
            .unwrap();
            let resp = http
                .post(format!("http://{}/v1/ingest", node_a2.http_addr))
                .header("x-pulse-key", &key_a)
                .json(&json!({ "events": [{ "distinct_id": "ua_post", "event_name": "e", "client_event_ts_ms": now_ms + 200 }] }))
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "restarting deposed writer must answer 503"
            );
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["code"].as_str(), Some("stale_writer"));

            node_a2.shutdown().await;
        }
    });
}

#[test]
fn t2_16_s3_deletion_grace_segments_and_snapshot_blobs_across_restart() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot = EnvGuard::set("PULSE_HOT_STORE", Some("local"));
    let _interval = EnvGuard::set("PULSE_UPLOAD_INTERVAL_SECS", Some("60"));
    let _byok = EnvGuard::set("PULSE_BYOK_MASTER_KEY", Some(TEST_BYOK_KEY));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("node");
        let s3_counting = Arc::new(CountingS3::default());
        let s3_raw: Arc<dyn ObjectStore> = s3_counting.clone();

        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 100,
            l0_flush_max_age: Duration::from_secs(3600),
            compaction_min_segments: 2,
        };
        let pid = "proj_t2_16";

        // 1. Publish pub_0 with 2 segments [S1, S2], identity snapshot id_0, profile snapshot prof_0.
        let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
        let enc = Arc::new(EncryptedObjectStore::new(routed));
        let engine = StorageEngine::new_with_encrypted(
            cfg.clone(),
            enc.clone(),
            Arc::new(IdentityRouter::new(enc.as_object_store())),
        );
        // Exercise T2 S3-side compaction + 2-interval deletion grace directly on S3 segments.
        engine.set_local_segment_tier(false);
        let ps = engine.get_or_open_project(pid).await.unwrap();
        let ig = engine.identity_graph(pid, true).await.unwrap();
        let pstore = engine.profile_store(pid, true).await.unwrap();

        let u0 = ig.get_or_intern("u0", 0);
        pstore
            .upsert(&StoredUserProfile {
                project_id: pid.into(),
                distinct_id: "u0".into(),
                canonical_uid: u0,
                props: BTreeMap::from([("plan".into(), json!("starter"))]),
                total_revenue: 10.0,
                updated_at_ms: 1000,
            })
            .await
            .unwrap();
        ps.append_bucket_events(0, vec![(make_event("u0", 1_790_000_000_000, 0), u0)])
            .await
            .unwrap();
        let seg1 = ps.flush_bucket(0).await.unwrap().pop().unwrap();
        ps.append_bucket_events(0, vec![(make_event("u0", 1_790_000_001_000, 1), u0)])
            .await
            .unwrap();
        let seg2 = ps.flush_bucket(0).await.unwrap().pop().unwrap();

        let pub0 = ps.publish_if_dirty().await.unwrap().unwrap();
        assert_eq!(pub0.publish_seq, 0);
        let id0_path = pub0.snapshot_ref.identity.object_path.clone();
        let prof0_path = pub0
            .snapshot_ref
            .profiles
            .as_ref()
            .unwrap()
            .object_path
            .clone();

        // Idle run_gc makes 0 S3 calls.
        s3_counting.take();
        assert_eq!(ps.run_gc().await.unwrap(), 0);
        assert_eq!(total_calls(&s3_counting.take()), 0);

        // 2. Compact [S1, S2] -> S_cmp locally WITHOUT publishing yet.
        assert_eq!(
            ps.compact_overdue(engine.segment_reader()).await.unwrap(),
            1
        );
        // S1 & S2 must remain in S3 because pub_1 hasn't landed yet!
        assert!(s3_raw
            .head(&ObjectPath::from(seg1.object_path.as_str()))
            .await
            .is_ok());
        assert!(s3_raw
            .head(&ObjectPath::from(seg2.object_path.as_str()))
            .await
            .is_ok());
        assert_eq!(ps.run_gc_at(i64::MAX / 2).await.unwrap(), 0);

        // 3. Simulate restart BEFORE pub_1 lands -> pending_s3_deletes AND last_snapshot_ref MUST survive!
        pstore.close().await.unwrap();
        ig.close().await.unwrap();
        drop(ps);
        drop(engine);

        let routed2 = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
        let enc2 = Arc::new(EncryptedObjectStore::new(routed2));
        let engine2 = StorageEngine::new_with_encrypted(
            cfg.clone(),
            enc2.clone(),
            Arc::new(IdentityRouter::new(enc2.as_object_store())),
        );
        engine2.set_local_segment_tier(false);
        let ps2 = engine2.get_or_open_project(pid).await.unwrap();
        let ig2 = engine2.identity_graph(pid, true).await.unwrap();
        let pstore2 = engine2.profile_store(pid, true).await.unwrap();

        // Still not published -> S1, S2 protected even at far-future clock.
        assert_eq!(ps2.run_gc_at(i64::MAX / 2).await.unwrap(), 0);

        // Mutate identity + profiles after restart so pub_1 writes new id_1 + prof_1, superseding id_0 + prof_0!
        let u1 = ig2.get_or_intern("u1", 0);
        pstore2
            .upsert(&StoredUserProfile {
                project_id: pid.into(),
                distinct_id: "u1".into(),
                canonical_uid: u1,
                props: BTreeMap::from([("plan".into(), json!("pro"))]),
                total_revenue: 50.0,
                updated_at_ms: 2000,
            })
            .await
            .unwrap();

        let pub1 = ps2.publish_if_dirty().await.unwrap().unwrap();
        assert_eq!(pub1.publish_seq, 1);
        let t1 = pub1.published_at_ms;
        let two_intervals_ms = 2 * 60 * 1_000i64;

        // Immediately after pub_1 and 1ms before 2-interval grace expires: ALL 4 superseded objects
        // (seg1, seg2, id_0, prof_0) MUST still exist in S3, and run_gc_at makes 0 S3 calls!
        assert!(s3_raw
            .head(&ObjectPath::from(seg1.object_path.as_str()))
            .await
            .is_ok());
        assert!(s3_raw
            .head(&ObjectPath::from(seg2.object_path.as_str()))
            .await
            .is_ok());
        assert!(s3_raw
            .head(&ObjectPath::from(id0_path.as_str()))
            .await
            .is_ok());
        assert!(s3_raw
            .head(&ObjectPath::from(prof0_path.as_str()))
            .await
            .is_ok());

        s3_counting.take();
        assert_eq!(ps2.run_gc_at(t1 + two_intervals_ms - 1).await.unwrap(), 0);
        assert_eq!(total_calls(&s3_counting.take()), 0);

        // 4. Crash mid-drain (`NotFound` idempotency): delete `seg2` externally before `run_gc_at`.
        //    (`InMemory::delete` returns `Ok(())` on missing keys, while `LocalFileSystem` returns `NotFound`;
        //    both are treated as resolved and removed from `pending_s3_deletes`.)
        s3_raw
            .delete(&ObjectPath::from(seg2.object_path.as_str()))
            .await
            .unwrap();

        let deleted = ps2.run_gc_at(t1 + two_intervals_ms).await.unwrap();
        assert!((3..=4).contains(&deleted), "deleted={deleted}");
        assert!(s3_raw
            .head(&ObjectPath::from(seg1.object_path.as_str()))
            .await
            .is_err());
        assert!(s3_raw
            .head(&ObjectPath::from(id0_path.as_str()))
            .await
            .is_err());
        assert!(s3_raw
            .head(&ObjectPath::from(prof0_path.as_str()))
            .await
            .is_err());

        let mf_after = ManifestStore::new(pid, enc2.as_object_store())
            .load_latest()
            .await
            .unwrap();
        assert!(mf_after.pending_s3_deletes.is_empty());

        // Subsequent run_gc_at makes 0 S3 calls.
        s3_counting.take();
        assert_eq!(
            ps2.run_gc_at(t1 + two_intervals_ms + 10_000).await.unwrap(),
            0
        );
        assert_eq!(total_calls(&s3_counting.take()), 0);

        // 5. Verify ReaderPinTracker prevents deletion even past grace until StorageSnapshot drops.
        ps2.append_bucket_events(0, vec![(make_event("u1", 1_790_000_002_000, 2), u1)])
            .await
            .unwrap();
        let seg3 = ps2.flush_bucket(0).await.unwrap().pop().unwrap();
        ps2.append_bucket_events(0, vec![(make_event("u1", 1_790_000_003_000, 3), u1)])
            .await
            .unwrap();
        let seg4 = ps2.flush_bucket(0).await.unwrap().pop().unwrap();

        // Acquire StorageSnapshot BEFORE compaction so seg3 & seg4 are pinned in ReaderPinTracker.
        let pinned_snap = ps2.snapshot();
        assert_eq!(
            ps2.compact_overdue(engine2.segment_reader()).await.unwrap(),
            1
        );
        let pub2 = ps2.publish_if_dirty().await.unwrap().unwrap();
        let t2 = pub2.published_at_ms;
        // Even past 2-interval grace, pinned_snap blocks deletion of seg3 & seg4!
        assert_eq!(
            ps2.run_gc_at(t2 + two_intervals_ms + 1000).await.unwrap(),
            0
        );
        assert!(s3_raw
            .head(&ObjectPath::from(seg3.object_path.as_str()))
            .await
            .is_ok());
        assert!(s3_raw
            .head(&ObjectPath::from(seg4.object_path.as_str()))
            .await
            .is_ok());

        // Drop StorageSnapshot -> seg3 & seg4 (plus id1 snapshot) delete cleanly!
        drop(pinned_snap);
        let drained = ps2.run_gc_at(t2 + two_intervals_ms + 1000).await.unwrap();
        assert!(drained >= 2);
        assert!(s3_raw
            .head(&ObjectPath::from(seg3.object_path.as_str()))
            .await
            .is_err());
        assert!(s3_raw
            .head(&ObjectPath::from(seg4.object_path.as_str()))
            .await
            .is_err());

        // 6. Verify PULSE_UPLOAD_INTERVAL_SECS=0 retains superseded blobs for 2 publish cycles (`superseding_pub_seq + 2`).
        engine2.set_upload_interval(Duration::ZERO);
        let u2 = ig2.get_or_intern("u2", 0);
        ps2.append_bucket_events(0, vec![(make_event("u2", 1_790_000_004_000, 4), u2)])
            .await
            .unwrap();
        ps2.flush_bucket(0).await.unwrap(); // pub_3 (supersedes pub_2's id2 at superseding_pub_seq = 3)
        let id2_path = pub2.snapshot_ref.identity.object_path.clone();
        assert_eq!(ps2.run_gc().await.unwrap(), 0);
        assert!(s3_raw
            .head(&ObjectPath::from(id2_path.as_str()))
            .await
            .is_ok());

        let u3 = ig2.get_or_intern("u3", 0);
        ps2.append_bucket_events(0, vec![(make_event("u3", 1_790_000_005_000, 5), u3)])
            .await
            .unwrap();
        ps2.flush_bucket(0).await.unwrap(); // pub_4 (still < 3 + 2 = 5)
        assert_eq!(ps2.run_gc().await.unwrap(), 0);
        assert!(s3_raw
            .head(&ObjectPath::from(id2_path.as_str()))
            .await
            .is_ok());

        let u4 = ig2.get_or_intern("u4", 0);
        ps2.append_bucket_events(0, vec![(make_event("u4", 1_790_000_006_000, 6), u4)])
            .await
            .unwrap();
        ps2.flush_bucket(0).await.unwrap(); // pub_5 == 3 + 2 -> eligible (drained by Step 6 run_gc_at)!
        assert_eq!(ps2.run_gc().await.unwrap(), 0);
        assert!(s3_raw
            .head(&ObjectPath::from(id2_path.as_str()))
            .await
            .is_err());

        pstore2.close().await.unwrap();
        ig2.close().await.unwrap();

        // 7. Verify true `Error::NotFound` idempotency using LocalFileSystem S3 backend:
        let fs_tmp = tempfile::tempdir().unwrap();
        let fs_s3_dir = fs_tmp.path().join("s3");
        std::fs::create_dir_all(&fs_s3_dir).unwrap();
        let fs_s3: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&fs_s3_dir).unwrap());
        let fs_data = fs_tmp.path().join("node");
        let fs_routed = with_hot_store(fs_s3.clone(), &fs_data).await.unwrap();
        let fs_enc = Arc::new(EncryptedObjectStore::new(fs_routed));
        let fs_engine = StorageEngine::new_with_encrypted(
            cfg,
            fs_enc.clone(),
            Arc::new(IdentityRouter::new(fs_enc.as_object_store())),
        );
        fs_engine.set_local_segment_tier(false);
        let fs_ps = fs_engine
            .get_or_open_project("proj_fs_notfound")
            .await
            .unwrap();
        let fs_ig = fs_engine
            .identity_graph("proj_fs_notfound", true)
            .await
            .unwrap();
        let u_fs = fs_ig.get_or_intern("u_fs", 0);
        fs_ps
            .append_bucket_events(0, vec![(make_event("u_fs", 1_790_000_000_000, 1), u_fs)])
            .await
            .unwrap();
        let fs_seg1 = fs_ps.flush_bucket(0).await.unwrap().pop().unwrap();
        fs_ps
            .append_bucket_events(0, vec![(make_event("u_fs", 1_790_000_001_000, 2), u_fs)])
            .await
            .unwrap();
        let fs_seg2 = fs_ps.flush_bucket(0).await.unwrap().pop().unwrap();
        assert_eq!(
            fs_ps
                .compact_overdue(fs_engine.segment_reader())
                .await
                .unwrap(),
            1
        );
        let fs_pub = fs_ps.publish_if_dirty().await.unwrap().unwrap();
        // Delete fs_seg2 on LocalFileSystem so run_gc_at hits genuine Error::NotFound for fs_seg2!
        fs_s3
            .delete(&ObjectPath::from(fs_seg2.object_path.as_str()))
            .await
            .unwrap();
        let fs_deleted = fs_ps
            .run_gc_at(fs_pub.published_at_ms + two_intervals_ms)
            .await
            .unwrap();
        assert_eq!(
            fs_deleted, 1,
            "only fs_seg1 actually deleted; fs_seg2 returned NotFound"
        );
        let fs_mf = ManifestStore::new("proj_fs_notfound", fs_enc.as_object_store())
            .load_latest()
            .await
            .unwrap();
        assert!(fs_mf.pending_s3_deletes.is_empty());
        assert!(fs_s3
            .head(&ObjectPath::from(fs_seg1.object_path.as_str()))
            .await
            .is_err());
        fs_ig.close().await.unwrap();
    });
}
