//! Empirical T1 Snapshot Benchmark (Candidate A Checkpoint Copy vs Candidate B Logical Export),
//! Empty-Dir BYOK Restore Verification, and 4-Point Publish Crash Tests.
//!
//! Run with `--nocapture` to inspect the full comparison table:
//! `cargo test -p pulse-node --test t1_snapshot_bench --locked -- --nocapture`

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult, UploadPart,
};
use pulse_identity::{IdentityRouter, ProjectIdentityGraph};
use pulse_storage::snapshot::{
    restore_slatedb_checkpoint, sync_slatedb_checkpoint, CandidateACheckpointFiles,
    SlateDbCheckpointSyncState,
};
use pulse_storage::{
    finalize_local_hot_store_marker, install_restored_manifest, open_hot_store_for_restore,
    restore_project_from_published, with_hot_store, EncryptedObjectStore, IdentitySnapshotRef,
    ManifestStore, ProfileSnapshotRef, ProjectProfileStore, ProjectStorage, PublishedManifest,
    PublishedManifestStore, SnapshotRef, StorageConfig, StorageEngine, StoredUserProfile,
    VortexSegmentReader,
};
use pulse_wire::WireEvent;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, val);
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

const TEST_BYOK_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const WRONG_BYOK_KEY: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

#[derive(Debug, Default, Clone)]
struct OpCounters {
    put_opts: u64,
    put_opts_bytes: u64,
    put_multipart_opts: u64,
    put_multipart_bytes: u64,
    copy: u64,
    copy_bytes: u64,
    get: u64,
    get_bytes: u64,
    list: u64,
    delete: u64,
    head: u64,
}

impl OpCounters {
    fn total_puts(&self) -> u64 {
        self.put_opts + self.put_multipart_opts + self.copy
    }

    fn total_put_bytes(&self) -> u64 {
        self.put_opts_bytes + self.put_multipart_bytes + self.copy_bytes
    }

    fn add(&mut self, other: &OpCounters) {
        self.put_opts += other.put_opts;
        self.put_opts_bytes += other.put_opts_bytes;
        self.put_multipart_opts += other.put_multipart_opts;
        self.put_multipart_bytes += other.put_multipart_bytes;
        self.copy += other.copy;
        self.copy_bytes += other.copy_bytes;
        self.get += other.get;
        self.get_bytes += other.get_bytes;
        self.list += other.list;
        self.delete += other.delete;
        self.head += other.head;
    }
}

type PrefixStats = BTreeMap<String, OpCounters>;

#[derive(Debug)]
struct CountingMultipart {
    inner: Box<dyn MultipartUpload>,
    prefix: String,
    bytes: u64,
    stats: Arc<Mutex<PrefixStats>>,
}

#[async_trait]
impl MultipartUpload for CountingMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.bytes += data.content_length() as u64;
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let res = self.inner.complete().await?;
        let mut guard = self.stats.lock().unwrap();
        let entry = guard.entry(self.prefix.clone()).or_default();
        entry.put_multipart_opts += 1;
        entry.put_multipart_bytes += self.bytes;
        Ok(res)
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.inner.abort().await
    }
}

/// Counting `ObjectStore` wrapper tracking exact operation counts and byte volumes per prefix.
#[derive(Debug, Default)]
struct Counting {
    inner: InMemory,
    stats: Arc<Mutex<PrefixStats>>,
}

impl Counting {
    fn prefix_of(p: Option<&ObjectPath>) -> String {
        p.and_then(|p| p.parts().next())
            .map_or("(root)".to_string(), |s| s.as_ref().to_string())
    }

    fn take(&self) -> PrefixStats {
        std::mem::take(&mut *self.stats.lock().unwrap())
    }

    fn sum_all(stats: &PrefixStats) -> OpCounters {
        let mut total = OpCounters::default();
        for c in stats.values() {
            total.add(c);
        }
        total
    }

    fn sum_prefix(stats: &PrefixStats, prefix: &str) -> OpCounters {
        stats.get(prefix).cloned().unwrap_or_default()
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
        let len = p.content_length() as u64;
        let res = self.inner.put_opts(l, p, o).await?;
        let mut guard = self.stats.lock().unwrap();
        let entry = guard.entry(Self::prefix_of(Some(l))).or_default();
        entry.put_opts += 1;
        entry.put_opts_bytes += len;
        Ok(res)
    }

    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(l, o).await?;
        Ok(Box::new(CountingMultipart {
            inner,
            prefix: Self::prefix_of(Some(l)),
            bytes: 0,
            stats: self.stats.clone(),
        }))
    }

    async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
        let res = self.inner.get_opts(l, o).await?;
        {
            let mut guard = self.stats.lock().unwrap();
            let entry = guard.entry(Self::prefix_of(Some(l))).or_default();
            entry.get += 1;
            entry.get_bytes += res.range.end.saturating_sub(res.range.start);
        }
        Ok(res)
    }

    async fn head(&self, l: &ObjectPath) -> OsResult<ObjectMeta> {
        {
            let mut guard = self.stats.lock().unwrap();
            guard.entry(Self::prefix_of(Some(l))).or_default().head += 1;
        }
        self.inner.head(l).await
    }

    async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
        {
            let mut guard = self.stats.lock().unwrap();
            guard.entry(Self::prefix_of(Some(l))).or_default().delete += 1;
        }
        self.inner.delete(l).await
    }

    fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        {
            let mut guard = self.stats.lock().unwrap();
            guard.entry(Self::prefix_of(p)).or_default().list += 1;
        }
        self.inner.list(p)
    }

    async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
        {
            let mut guard = self.stats.lock().unwrap();
            guard.entry(Self::prefix_of(p)).or_default().list += 1;
        }
        self.inner.list_with_delimiter(p).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        let sz = self.inner.head(from).await.map(|m| m.size).unwrap_or(0);
        self.inner.copy(from, to).await?;
        let mut guard = self.stats.lock().unwrap();
        let entry = guard.entry(Self::prefix_of(Some(to))).or_default();
        entry.copy += 1;
        entry.copy_bytes += sz;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        let sz = self.inner.head(from).await.map(|m| m.size).unwrap_or(0);
        self.inner.copy_if_not_exists(from, to).await?;
        let mut guard = self.stats.lock().unwrap();
        let entry = guard.entry(Self::prefix_of(Some(to))).or_default();
        entry.copy += 1;
        entry.copy_bytes += sz;
        Ok(())
    }
}

fn make_event(distinct_id: &str, seq: u64) -> WireEvent {
    let ts = 1_790_000_000_000 + (seq as i64) * 1_000;
    WireEvent {
        event_uuid: WireEvent::deterministic_uuid(distinct_id, "s0", "App_Open", ts, seq),
        routing_seed: distinct_id.into(),
        distinct_id: distinct_id.into(),
        session_id: "s0".into(),
        event_name: "App_Open".into(),
        client_event_ts_ms: ts,
        client_sent_ts_ms: ts + 5,
        props: vec![],
        replay_offset_kb: 0,
    }
}

#[derive(Debug, Clone)]
struct CycleMetrics {
    cycle_name: &'static str,
    snap_puts: u64,
    snap_bytes: u64,
    snap_deletes: u64,
    snap_lists: u64,
    total_s3_puts: u64,
    total_s3_bytes: u64,
    snap_latency_ms: f64,
}

#[derive(Debug, Clone)]
struct BenchSummary {
    candidate: &'static str,
    cycles: Vec<CycleMetrics>,
    total_snap_puts: u64,
    total_snap_bytes: u64,
    total_snap_deletes: u64,
    total_snap_lists: u64,
    total_s3_puts: u64,
    total_s3_bytes: u64,
    avg_active_snap_puts_per_cycle: f64,
    t2_puts_per_hr_12c: f64,
    t1_puts_per_hr_20c: f64,
    s3_snapshot_objects: usize,
    s3_total_published_objects: usize,
    restore_latency_ms: f64,
    verified_rows: usize,
    verified_profiles: usize,
}

#[derive(Default)]
struct WorkloadExpectations {
    profiles: HashMap<String, (u32, String)>,
    uid_to_did: HashMap<u32, String>,
    seq_counter: u64,
}

async fn apply_cycle_workload(
    cycle: usize,
    pid: &str,
    ps: &ProjectStorage,
    id_graph: &ProjectIdentityGraph,
    pstore: &ProjectProfileStore,
    exp: &mut WorkloadExpectations,
) {
    let bucket = 0u8;
    match cycle {
        1..=3 => {
            // Active cycle: 500 new users, 50 alias_durable merges, 100 profile updates.
            let mut batch_events = Vec::with_capacity(500);
            for i in 0..500 {
                let did = format!("c{cycle}_user_{i:04}");
                let uid = id_graph.get_or_intern(&did, bucket);
                exp.uid_to_did.insert(uid, did.clone());
                exp.seq_counter += 1;
                batch_events.push((make_event(&did, exp.seq_counter), uid));
            }
            id_graph.flush().await.unwrap();
            {
                let g = ps.lock_bucket_append(bucket).await;
                ps.append_bucket_events_locked(&g, bucket, &batch_events)
                    .await
                    .unwrap();
            }

            for i in 0..50 {
                let anon = format!("c{cycle}_anon_{i:04}");
                let user = format!("c{cycle}_user_{i:04}");
                let canon = id_graph
                    .alias_durable(&anon, &user, bucket, bucket)
                    .await
                    .unwrap();
                let anon_uid = id_graph.lookup_raw(&anon).unwrap();
                exp.uid_to_did.insert(anon_uid, anon);
                exp.uid_to_did.insert(canon, user.clone());
            }

            for i in 0..100 {
                let did = format!("c{cycle}_user_{i:04}");
                let canon = id_graph.lookup_canonical(&did).unwrap();
                let plan = format!("tier_{cycle}_{}", i % 3);
                pstore
                    .upsert(&StoredUserProfile {
                        project_id: pid.to_string(),
                        distinct_id: did.clone(),
                        canonical_uid: canon,
                        props: BTreeMap::from([("plan".into(), serde_json::json!(plan))]),
                        total_revenue: (i as f64) * 10.0,
                        updated_at_ms: 1_790_000_000_000 + (exp.seq_counter as i64) + (i as i64),
                    })
                    .await
                    .unwrap();
                exp.profiles.insert(did, (canon, plan));
            }
        }
        4 => {
            // Idle cycle: events for existing users ONLY (0 new users, 0 merges, 0 profile updates).
            let mut batch_events = Vec::with_capacity(200);
            for i in 0..200 {
                let did = format!("c1_user_{i:04}");
                let uid = id_graph.get_or_intern(&did, bucket);
                exp.seq_counter += 1;
                batch_events.push((make_event(&did, exp.seq_counter), uid));
            }
            id_graph.flush().await.unwrap();
            {
                let g = ps.lock_bucket_append(bucket).await;
                ps.append_bucket_events_locked(&g, bucket, &batch_events)
                    .await
                    .unwrap();
            }
        }
        5 => {
            // Heavy profile + merge cycle: 200 new users, 100 merges, 300 profile updates.
            let mut batch_events = Vec::with_capacity(200);
            for i in 0..200 {
                let did = format!("c5_user_{i:04}");
                let uid = id_graph.get_or_intern(&did, bucket);
                exp.uid_to_did.insert(uid, did.clone());
                exp.seq_counter += 1;
                batch_events.push((make_event(&did, exp.seq_counter), uid));
            }
            id_graph.flush().await.unwrap();
            {
                let g = ps.lock_bucket_append(bucket).await;
                ps.append_bucket_events_locked(&g, bucket, &batch_events)
                    .await
                    .unwrap();
            }

            for i in 0..100 {
                let anon = format!("c5_anon_{i:04}");
                let user = format!("c5_user_{i:04}");
                let canon = id_graph
                    .alias_durable(&anon, &user, bucket, bucket)
                    .await
                    .unwrap();
                let anon_uid = id_graph.lookup_raw(&anon).unwrap();
                exp.uid_to_did.insert(anon_uid, anon);
                exp.uid_to_did.insert(canon, user.clone());
            }

            for i in 0..300 {
                let did = if i < 200 {
                    format!("c5_user_{i:04}")
                } else {
                    format!("c2_user_{:04}", i - 200)
                };
                let canon = id_graph.lookup_canonical(&did).unwrap();
                let plan = format!("enterprise_{}", i % 5);
                pstore
                    .upsert(&StoredUserProfile {
                        project_id: pid.to_string(),
                        distinct_id: did.clone(),
                        canonical_uid: canon,
                        props: BTreeMap::from([("plan".into(), serde_json::json!(plan))]),
                        total_revenue: (i as f64) * 25.0,
                        updated_at_ms: 1_790_000_500_000 + (i as i64),
                    })
                    .await
                    .unwrap();
                exp.profiles.insert(did, (canon, plan));
            }
        }
        _ => unreachable!(),
    }
}

async fn copy_s3_prefix(src: &Arc<dyn ObjectStore>, dst: &Arc<dyn ObjectStore>, prefix: &str) {
    let metas: Vec<_> = src
        .list(Some(&ObjectPath::from(prefix)))
        .try_collect()
        .await
        .unwrap();
    for m in metas {
        let bytes = src.get(&m.location).await.unwrap().bytes().await.unwrap();
        dst.put(&m.location, bytes.into()).await.unwrap();
    }
}

struct VerifyRestoreInput<'a> {
    pid: &'a str,
    s3_raw: &'a Arc<Counting>,
    published: &'a PublishedManifest,
    sample_snapshot_key: &'a str,
    restored_id: &'a Arc<ProjectIdentityGraph>,
    restored_prof: &'a Arc<ProjectProfileStore>,
    restored_enc_store: &'a Arc<dyn ObjectStore>,
    exp: &'a WorkloadExpectations,
}

async fn verify_byok_security_and_restored_state(v: VerifyRestoreInput<'_>) -> (usize, usize) {
    let user_count = v.restored_id.user_count() as u64;
    assert!(
        user_count >= v.published.high_water_mark,
        "identity user_count ({user_count}) must be >= high_water_mark ({})",
        v.published.high_water_mark
    );

    // 1. Open EVERY published Vortex segment and resolve EVERY uid_internal.
    let seg_reader = VortexSegmentReader::new(v.restored_enc_store.clone());
    let id_snap = v.restored_id.snapshot();
    let mut verified_rows = 0usize;
    for part in &v.published.partitions {
        for seg in &part.segments {
            let rows = seg_reader.read_all_rows(seg).await.unwrap();
            for row in rows {
                let raw_ext = v
                    .restored_id
                    .external_id(row.uid_internal)
                    .expect("every segment uid_internal must resolve to external_id");
                assert_eq!(
                    v.exp.uid_to_did.get(&row.uid_internal),
                    Some(&raw_ext),
                    "external_id mismatch for uid {}",
                    row.uid_internal
                );
                let canon_uid = id_snap.resolve(row.uid_internal);
                assert!(
                    v.restored_id.external_id(canon_uid).is_some(),
                    "canonical uid {canon_uid} must exist"
                );
                verified_rows += 1;
            }
        }
    }
    assert!(verified_rows > 0);

    // 2. Verify EVERY profile row in ProjectProfileStore.
    let all_profs = v.restored_prof.list_updated_since(0).await.unwrap();
    assert_eq!(all_profs.len(), v.exp.profiles.len());
    for prof in &all_profs {
        let (exp_canon, exp_plan) = v
            .exp
            .profiles
            .get(&prof.distinct_id)
            .expect("unexpected profile");
        assert_eq!(prof.canonical_uid, *exp_canon);
        assert_eq!(
            v.restored_id.lookup_canonical(&prof.distinct_id),
            Some(*exp_canon)
        );
        assert_eq!(
            prof.props.get("plan").and_then(|val| val.as_str()),
            Some(exp_plan.as_str())
        );
    }

    // 3. Verify interning a new user yields uid >= high_water_mark.
    let new_uid = v
        .restored_id
        .get_or_intern("post_restore_brand_new_user", 0);
    assert!(
        (new_uid as u64) >= v.published.high_water_mark,
        "new uid ({new_uid}) must be >= high_water_mark ({})",
        v.published.high_water_mark
    );

    // 4. Verify BYOK path binding & wrong-key refusal:
    // (a) Raw ciphertext copied from S3 snapshot path to local `identity/{pid}/...` fails AEAD unwrap.
    let raw_cipher = v
        .s3_raw
        .get(&ObjectPath::from(v.sample_snapshot_key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let tamper_inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wrong_path = ObjectPath::from(format!("identity/{}/tampered.bin", v.pid));
    tamper_inner
        .put(&wrong_path, raw_cipher.into())
        .await
        .unwrap();
    let tamper_enc = EncryptedObjectStore::new(tamper_inner);
    assert!(
        tamper_enc.get(&wrong_path).await.is_err(),
        "raw ciphertext copied to a different path must fail PLSEAGM2 AEAD unwrap"
    );

    // (b) Opening S3 snapshot object with WRONG BYOK key fails AEAD unwrap.
    {
        let _wrong_guard = EnvGuard::set("PULSE_BYOK_MASTER_KEY", WRONG_BYOK_KEY);
        let wrong_key_enc = EncryptedObjectStore::new(Arc::new(InMemory::new()));
        let wrong_raw = v
            .s3_raw
            .get(&ObjectPath::from(v.sample_snapshot_key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let p = ObjectPath::from(v.sample_snapshot_key);
        wrong_key_enc
            .raw_inner()
            .put(&p, wrong_raw.into())
            .await
            .unwrap();
        assert!(
            wrong_key_enc.get(&p).await.is_err(),
            "reading snapshot with wrong BYOK master key must fail AEAD unwrap"
        );
    }

    (verified_rows, all_profs.len())
}

async fn run_candidate_a_benchmark() -> BenchSummary {
    let pid = "proj_bench_a";
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("node");
    let s3_counting = Arc::new(Counting::default());
    let s3_raw: Arc<dyn ObjectStore> = s3_counting.clone();

    let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
    let enc_store = Arc::new(EncryptedObjectStore::new(routed));
    let store: Arc<dyn ObjectStore> = enc_store.as_object_store();

    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let cfg = StorageConfig {
        num_buckets: 1,
        l0_flush_row_threshold: 100_000,
        l0_flush_max_age: std::time::Duration::from_secs(3600),
        compaction_min_segments: 100,
    };
    let engine = StorageEngine::new_with_encrypted(cfg, enc_store.clone(), id_router);
    let ps = engine.get_or_open_project(pid).await.unwrap();
    ps.set_publish_enabled(false);
    let id_graph = engine.identity_graph(pid, true).await.unwrap();
    let pstore = engine.profile_store(pid, true).await.unwrap();
    let pub_store = PublishedManifestStore::new(pid, store.clone());

    let mut exp = WorkloadExpectations::default();

    let mut id_sync = SlateDbCheckpointSyncState::default();
    let mut prof_sync = SlateDbCheckpointSyncState::default();
    let mut last_id_epoch: Option<u64> = None;
    let mut last_prof_dirty: Option<u64> = None;
    let mut last_id_files: Option<CandidateACheckpointFiles> = None;
    let mut last_prof_files: Option<CandidateACheckpointFiles> = None;

    let mut cycles_metrics = Vec::new();
    let mut last_published_manifest: Option<PublishedManifest> = None;

    let id_snap_root = format!("published/{pid}/snapshots/cand_a_identity");
    let prof_snap_root = format!("published/{pid}/snapshots/cand_a_profiles");

    s3_counting.take();

    for cycle in 1..=5usize {
        apply_cycle_workload(cycle, pid, &ps, &id_graph, &pstore, &mut exp).await;

        s3_counting.take();
        let t0 = Instant::now();
        // Freeze & flush L0 segments (same segment writes as T1).
        ps.flush_bucket(0).await.unwrap();
        let before_snap_stats = s3_counting.take();

        // Candidate A snapshot: Profiles (T_prof >= T_freeze), then Identity (T_id >= T_prof).
        let cur_prof_dirty = pstore.dirty_seq();
        if last_prof_dirty != Some(cur_prof_dirty) {
            let cp = pstore.create_slatedb_checkpoint().await.unwrap();
            let files = sync_slatedb_checkpoint(
                &store,
                &format!("profiles/{pid}"),
                &prof_snap_root,
                &cp,
                &mut prof_sync,
            )
            .await
            .unwrap();
            last_prof_dirty = Some(cur_prof_dirty);
            last_prof_files = Some(files);
        }

        id_graph.flush().await.unwrap();
        let cur_id_epoch = id_graph.mutation_epoch();
        if last_id_epoch != Some(cur_id_epoch) || id_graph.flushed_mutation_epoch() != cur_id_epoch
        {
            let cp = id_graph.create_slatedb_checkpoint().await.unwrap();
            let files = sync_slatedb_checkpoint(
                &store,
                &format!("identity/{pid}"),
                &id_snap_root,
                &cp,
                &mut id_sync,
            )
            .await
            .unwrap();
            last_id_epoch = Some(id_graph.mutation_epoch());
            last_id_files = Some(files);
        }

        let snap_stats = s3_counting.take();

        // Publish manifest pointer for Candidate A (counted under `published/`).
        let cur_manifest = ps.snapshot().manifest.clone();
        let hwm = id_graph.flushed_user_count();
        let pub_m = pub_store
            .publish_next(PublishedManifest {
                publish_seq: (cycle - 1) as u64,
                writer_generation: cur_manifest.writer_generation,
                source_local_epoch: cur_manifest.epoch,
                high_water_mark: hwm,
                published_at_ms: Utc::now().timestamp_millis(),
                partitions: cur_manifest.to_sorted_partitions(),
                dict_snapshot: cur_manifest.dict_snapshot.clone(),
                committed_wal_seq: cur_manifest.committed_wal_seq.clone(),
                committed_l0_seq: cur_manifest.committed_l0_seq.clone(),
                snapshot_ref: SnapshotRef {
                    identity: IdentitySnapshotRef {
                        object_path: format!(
                            "{id_snap_root}/{}",
                            last_id_files.as_ref().unwrap().manifest_file
                        ),
                        user_count: hwm,
                        mutation_epoch: cur_id_epoch,
                    },
                    profiles: last_prof_files.as_ref().map(|f| ProfileSnapshotRef {
                        object_path: format!("{prof_snap_root}/{}", f.manifest_file),
                        dirty_seq: cur_prof_dirty,
                    }),
                },
            })
            .await
            .unwrap();
        let snap_latency_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let pub_stats = s3_counting.take();
        last_published_manifest = Some(pub_m);

        let snap_c = Counting::sum_prefix(&snap_stats, "published");
        let seg_c = Counting::sum_all(&before_snap_stats);
        let pub_c = Counting::sum_all(&pub_stats);

        let cycle_name = match cycle {
            1 => "Cycle 1 (active)",
            2 => "Cycle 2 (active)",
            3 => "Cycle 3 (active)",
            4 => "Cycle 4 (idle)",
            5 => "Cycle 5 (heavy)",
            _ => unreachable!(),
        };
        cycles_metrics.push(CycleMetrics {
            cycle_name,
            snap_puts: snap_c.total_puts(),
            snap_bytes: snap_c.total_put_bytes(),
            snap_deletes: snap_c.delete,
            snap_lists: snap_c.list,
            total_s3_puts: snap_c.total_puts() + seg_c.total_puts() + pub_c.total_puts(),
            total_s3_bytes: snap_c.total_put_bytes()
                + seg_c.total_put_bytes()
                + pub_c.total_put_bytes(),
            snap_latency_ms,
        });
    }

    let snap_objs: Vec<_> = s3_counting
        .list(Some(&ObjectPath::from(format!(
            "published/{pid}/snapshots"
        ))))
        .try_collect()
        .await
        .unwrap();
    let all_pub_objs: Vec<_> = s3_counting
        .list(Some(&ObjectPath::from(format!("published/{pid}"))))
        .try_collect()
        .await
        .unwrap();
    let s3_snapshot_objects = snap_objs.len();
    let s3_total_published_objects = all_pub_objs.len();

    pstore.close().await.unwrap();
    id_graph.close().await.unwrap();
    drop(ps);
    drop(engine);

    // Empty-dir BYOK restore verification for Candidate A.
    let restore_tmp = tempfile::tempdir().unwrap();
    let restore_data_dir = restore_tmp.path().join("node");
    let restore_s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    copy_s3_prefix(&s3_raw, &restore_s3, "segments").await;
    copy_s3_prefix(&s3_raw, &restore_s3, "manifests").await;
    copy_s3_prefix(&s3_raw, &restore_s3, "published").await;

    let restore_routed = open_hot_store_for_restore(restore_s3.clone(), &restore_data_dir)
        .await
        .unwrap();
    let restore_enc = Arc::new(EncryptedObjectStore::new(restore_routed));
    let restore_store: Arc<dyn ObjectStore> = restore_enc.as_object_store();

    let t_restore = Instant::now();
    restore_slatedb_checkpoint(
        &restore_store,
        &format!("identity/{pid}"),
        &id_snap_root,
        last_id_files.as_ref().unwrap(),
    )
    .await
    .unwrap();
    restore_slatedb_checkpoint(
        &restore_store,
        &format!("profiles/{pid}"),
        &prof_snap_root,
        last_prof_files.as_ref().unwrap(),
    )
    .await
    .unwrap();
    let pub_m = last_published_manifest.unwrap();
    let _installed = install_restored_manifest(pid, restore_store.clone(), &pub_m)
        .await
        .unwrap();
    let restored_id = ProjectIdentityGraph::open(pid, restore_store.clone())
        .await
        .unwrap();
    let restored_prof = ProjectProfileStore::open(pid, restore_store.clone())
        .await
        .unwrap();
    finalize_local_hot_store_marker(&restore_data_dir)
        .await
        .unwrap();
    let restore_latency_ms = t_restore.elapsed().as_secs_f64() * 1000.0;

    let sample_key = format!(
        "{id_snap_root}/{}",
        last_id_files.as_ref().unwrap().manifest_file
    );
    let (verified_rows, verified_profiles) =
        verify_byok_security_and_restored_state(VerifyRestoreInput {
            pid,
            s3_raw: &s3_counting,
            published: &pub_m,
            sample_snapshot_key: &sample_key,
            restored_id: &restored_id,
            restored_prof: &restored_prof,
            restored_enc_store: &restore_store,
            exp: &exp,
        })
        .await;
    restored_id.close().await.unwrap();
    restored_prof.close().await.unwrap();

    let total_snap_puts: u64 = cycles_metrics.iter().map(|c| c.snap_puts).sum();
    let total_snap_bytes: u64 = cycles_metrics.iter().map(|c| c.snap_bytes).sum();
    let total_snap_deletes: u64 = cycles_metrics.iter().map(|c| c.snap_deletes).sum();
    let total_snap_lists: u64 = cycles_metrics.iter().map(|c| c.snap_lists).sum();
    let total_s3_puts: u64 = cycles_metrics.iter().map(|c| c.total_s3_puts).sum();
    let total_s3_bytes: u64 = cycles_metrics.iter().map(|c| c.total_s3_bytes).sum();
    let active_puts: u64 = cycles_metrics
        .iter()
        .filter(|c| !c.cycle_name.contains("idle"))
        .map(|c| c.snap_puts)
        .sum();
    let avg_active = active_puts as f64 / 4.0;

    BenchSummary {
        candidate: "Candidate A (Checkpoint Copy)",
        cycles: cycles_metrics,
        total_snap_puts,
        total_snap_bytes,
        total_snap_deletes,
        total_snap_lists,
        total_s3_puts,
        total_s3_bytes,
        avg_active_snap_puts_per_cycle: avg_active,
        t2_puts_per_hr_12c: avg_active * 12.0,
        t1_puts_per_hr_20c: avg_active * 20.0,
        s3_snapshot_objects,
        s3_total_published_objects,
        restore_latency_ms,
        verified_rows,
        verified_profiles,
    }
}

async fn run_candidate_b_benchmark() -> BenchSummary {
    let pid = "proj_bench_b";
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("node");
    let s3_counting = Arc::new(Counting::default());
    let s3_raw: Arc<dyn ObjectStore> = s3_counting.clone();

    let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
    let enc_store = Arc::new(EncryptedObjectStore::new(routed));

    let cfg = StorageConfig {
        num_buckets: 1,
        l0_flush_row_threshold: 100_000,
        l0_flush_max_age: std::time::Duration::from_secs(3600),
        compaction_min_segments: 100,
    };
    let id_router = Arc::new(IdentityRouter::new(enc_store.as_object_store()));
    let engine = StorageEngine::new_with_encrypted(cfg, enc_store.clone(), id_router);

    let ps = engine.get_or_open_project(pid).await.unwrap();
    let id_graph = engine.identity_graph(pid, true).await.unwrap();
    let pstore = engine.profile_store(pid, true).await.unwrap();

    let mut exp = WorkloadExpectations::default();
    let mut cycles_metrics = Vec::new();

    s3_counting.take();

    for cycle in 1..=5usize {
        apply_cycle_workload(cycle, pid, &ps, &id_graph, &pstore, &mut exp).await;

        s3_counting.take();
        let t0 = Instant::now();
        // `flush_bucket` now runs the full T1 pipeline: freeze segments, commit manifest,
        // T_prof -> T_id logical export, and create-only PublishedManifest!
        ps.flush_bucket(0).await.unwrap();
        let snap_latency_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let stats = s3_counting.take();

        let pub_c = Counting::sum_prefix(&stats, "published");
        let seg_c = Counting::sum_prefix(&stats, "segments");
        let mf_c = Counting::sum_prefix(&stats, "manifests");
        // Exclude the 1 PublishedManifest PUT from snapshot PUTs for direct apples-to-apples
        // comparison of snapshot overhead (and also report total S3 PUTs).
        let snap_puts = pub_c.total_puts().saturating_sub(1);
        let pub_m = PublishedManifestStore::new(pid, enc_store.as_object_store())
            .load_latest()
            .await
            .unwrap()
            .unwrap();
        let pub_obj_bytes = s3_counting
            .head(
                &PublishedManifestStore::new(pid, enc_store.as_object_store())
                    .pub_path(pub_m.publish_seq),
            )
            .await
            .map(|m| m.size)
            .unwrap_or(0);
        let snap_bytes = pub_c.total_put_bytes().saturating_sub(pub_obj_bytes);

        let cycle_name = match cycle {
            1 => "Cycle 1 (active)",
            2 => "Cycle 2 (active)",
            3 => "Cycle 3 (active)",
            4 => "Cycle 4 (idle)",
            5 => "Cycle 5 (heavy)",
            _ => unreachable!(),
        };
        cycles_metrics.push(CycleMetrics {
            cycle_name,
            snap_puts,
            snap_bytes,
            snap_deletes: pub_c.delete,
            snap_lists: pub_c.list,
            total_s3_puts: pub_c.total_puts() + seg_c.total_puts() + mf_c.total_puts(),
            total_s3_bytes: pub_c.total_put_bytes()
                + seg_c.total_put_bytes()
                + mf_c.total_put_bytes(),
            snap_latency_ms,
        });
    }

    let snap_objs: Vec<_> = s3_counting
        .list(Some(&ObjectPath::from(format!(
            "published/{pid}/snapshots"
        ))))
        .try_collect()
        .await
        .unwrap();
    let all_pub_objs: Vec<_> = s3_counting
        .list(Some(&ObjectPath::from(format!("published/{pid}"))))
        .try_collect()
        .await
        .unwrap();
    let s3_snapshot_objects = snap_objs.len();
    let s3_total_published_objects = all_pub_objs.len();

    let pub_m = ps.latest_published_manifest().await.unwrap().unwrap();
    pstore.close().await.unwrap();
    id_graph.close().await.unwrap();
    drop(ps);
    drop(engine);

    // Empty-dir BYOK restore verification for Candidate B.
    let restore_tmp = tempfile::tempdir().unwrap();
    let restore_data_dir = restore_tmp.path().join("node");
    let restore_s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    copy_s3_prefix(&s3_raw, &restore_s3, "segments").await;
    copy_s3_prefix(&s3_raw, &restore_s3, "manifests").await;
    copy_s3_prefix(&s3_raw, &restore_s3, "published").await;

    let restore_routed = open_hot_store_for_restore(restore_s3.clone(), &restore_data_dir)
        .await
        .unwrap();
    let restore_enc = Arc::new(EncryptedObjectStore::new(restore_routed));
    let restore_store: Arc<dyn ObjectStore> = restore_enc.as_object_store();

    let t_restore = Instant::now();
    let restored = restore_project_from_published(pid, &pub_m, restore_store.clone())
        .await
        .unwrap();
    finalize_local_hot_store_marker(&restore_data_dir)
        .await
        .unwrap();
    let restore_latency_ms = t_restore.elapsed().as_secs_f64() * 1000.0;

    let restored_prof = restored.profile_store.unwrap();
    let (verified_rows, verified_profiles) =
        verify_byok_security_and_restored_state(VerifyRestoreInput {
            pid,
            s3_raw: &s3_counting,
            published: &pub_m,
            sample_snapshot_key: &pub_m.snapshot_ref.identity.object_path,
            restored_id: &restored.identity_graph,
            restored_prof: &restored_prof,
            restored_enc_store: &restore_store,
            exp: &exp,
        })
        .await;
    restored.identity_graph.close().await.unwrap();
    restored_prof.close().await.unwrap();

    // Verify `with_hot_store` now opens cleanly on restored dir.
    let _post_restore = with_hot_store(restore_s3, &restore_data_dir).await.unwrap();

    let total_snap_puts: u64 = cycles_metrics.iter().map(|c| c.snap_puts).sum();
    let total_snap_bytes: u64 = cycles_metrics.iter().map(|c| c.snap_bytes).sum();
    let total_snap_deletes: u64 = cycles_metrics.iter().map(|c| c.snap_deletes).sum();
    let total_snap_lists: u64 = cycles_metrics.iter().map(|c| c.snap_lists).sum();
    let total_s3_puts: u64 = cycles_metrics.iter().map(|c| c.total_s3_puts).sum();
    let total_s3_bytes: u64 = cycles_metrics.iter().map(|c| c.total_s3_bytes).sum();
    let active_puts: u64 = cycles_metrics
        .iter()
        .filter(|c| !c.cycle_name.contains("idle"))
        .map(|c| c.snap_puts)
        .sum();
    let avg_active = active_puts as f64 / 4.0;

    BenchSummary {
        candidate: "Candidate B (Logical Export)",
        cycles: cycles_metrics,
        total_snap_puts,
        total_snap_bytes,
        total_snap_deletes,
        total_snap_lists,
        total_s3_puts,
        total_s3_bytes,
        avg_active_snap_puts_per_cycle: avg_active,
        t2_puts_per_hr_12c: avg_active * 12.0,
        t1_puts_per_hr_20c: avg_active * 20.0,
        s3_snapshot_objects,
        s3_total_published_objects,
        restore_latency_ms,
        verified_rows,
        verified_profiles,
    }
}

fn print_summary(s: &BenchSummary) {
    eprintln!("==================================================================");
    eprintln!("  {}", s.candidate);
    eprintln!("==================================================================");
    for c in &s.cycles {
        eprintln!(
            "  {:<18} | snap_puts={:<4} | snap_bytes={:<8} | snap_del={:<4} | snap_list={:<2} | total_s3_puts={:<4} | latency={:.2}ms",
            c.cycle_name, c.snap_puts, c.snap_bytes, c.snap_deletes, c.snap_lists, c.total_s3_puts, c.snap_latency_ms
        );
    }
    eprintln!("------------------------------------------------------------------");
    eprintln!(
        "  TOTAL snapshot PUTs        : {} ({} bytes)",
        s.total_snap_puts, s.total_snap_bytes
    );
    eprintln!(
        "  TOTAL snapshot DELETE/LIST : {} deletes / {} lists",
        s.total_snap_deletes, s.total_snap_lists
    );
    eprintln!(
        "  TOTAL S3 PUTs (all prefix) : {} ({} bytes)",
        s.total_s3_puts, s.total_s3_bytes
    );
    eprintln!(
        "  Active avg PUTs/cycle      : {:.2}",
        s.avg_active_snap_puts_per_cycle
    );
    eprintln!(
        "  Extrapolated T2 (12 cy/hr) : {:.1} snapshot PUTs/hr",
        s.t2_puts_per_hr_12c
    );
    eprintln!(
        "  Extrapolated T1 (20 fl/hr) : {:.1} snapshot PUTs/hr",
        s.t1_puts_per_hr_20c
    );
    eprintln!(
        "  S3 objects (snap / pub)    : {} / {}",
        s.s3_snapshot_objects, s.s3_total_published_objects
    );
    eprintln!(
        "  Empty-dir BYOK restore     : {:.2}ms (verified {} rows, {} profiles)",
        s.restore_latency_ms, s.verified_rows, s.verified_profiles
    );
}

#[test]
fn empirical_t1_snapshot_benchmark_candidate_a_vs_candidate_b() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot_guard = EnvGuard::set("PULSE_HOT_STORE", "local");
    let _byok_guard = EnvGuard::set("PULSE_BYOK_MASTER_KEY", TEST_BYOK_KEY);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (sum_a, sum_b) = rt.block_on(async {
        let a = run_candidate_a_benchmark().await;
        let b = run_candidate_b_benchmark().await;
        (a, b)
    });

    print_summary(&sum_a);
    print_summary(&sum_b);

    // Core empirical assertions:
    // 1. Both candidates produce 0 snapshot PUTs on the idle cycle (Cycle 4).
    assert_eq!(sum_a.cycles[3].snap_puts, 0, "Candidate A idle cycle PUTs");
    assert_eq!(sum_b.cycles[3].snap_puts, 0, "Candidate B idle cycle PUTs");
    // 2. Candidate B uses dramatically fewer snapshot PUTs than Candidate A across active cycles.
    assert!(
        sum_b.total_snap_puts < sum_a.total_snap_puts,
        "Candidate B ({}) should have far fewer PUTs than Candidate A ({})",
        sum_b.total_snap_puts,
        sum_a.total_snap_puts
    );
    assert_eq!(sum_b.total_snap_puts, 8); // 2 PUTs * 4 active cycles
    assert_eq!(sum_b.s3_snapshot_objects, 2); // 1 identity + 1 profiles
    assert_eq!(sum_a.verified_rows, 1900);
    assert_eq!(sum_b.verified_rows, 1900);
    assert_eq!(sum_a.verified_profiles, 500);
    assert_eq!(sum_b.verified_profiles, 500);
}

#[derive(Debug)]
struct FaultInjectingStore {
    inner: InMemory,
    fail_on_substring: Mutex<Option<String>>,
    triggered: AtomicBool,
}

impl FaultInjectingStore {
    fn new(fail_on: Option<&str>) -> Self {
        Self {
            inner: InMemory::new(),
            fail_on_substring: Mutex::new(fail_on.map(str::to_string)),
            triggered: AtomicBool::new(false),
        }
    }

    fn set_fail_on(&self, fail_on: Option<&str>) {
        *self.fail_on_substring.lock().unwrap() = fail_on.map(str::to_string);
        self.triggered.store(false, AtomicOrdering::Release);
    }

    fn maybe_fail(&self, path: &ObjectPath) -> OsResult<()> {
        let needle = self.fail_on_substring.lock().unwrap().clone();
        if let Some(sub) = needle {
            if path.as_ref().contains(&sub) {
                self.triggered.store(true, AtomicOrdering::Release);
                return Err(object_store::Error::Generic {
                    store: "FaultInjectingStore",
                    source: Box::new(std::io::Error::other(format!("injected crash on {path}"))),
                });
            }
        }
        Ok(())
    }
}

impl fmt::Display for FaultInjectingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultInjectingStore")
    }
}

#[async_trait]
impl ObjectStore for FaultInjectingStore {
    async fn put_opts(&self, l: &ObjectPath, p: PutPayload, o: PutOptions) -> OsResult<PutResult> {
        self.maybe_fail(l)?;
        self.inner.put_opts(l, p, o).await
    }
    async fn put_multipart_opts(
        &self,
        l: &ObjectPath,
        o: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.maybe_fail(l)?;
        self.inner.put_multipart_opts(l, o).await
    }
    async fn get_opts(&self, l: &ObjectPath, o: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(l, o).await
    }
    async fn head(&self, l: &ObjectPath) -> OsResult<ObjectMeta> {
        self.inner.head(l).await
    }
    async fn delete(&self, l: &ObjectPath) -> OsResult<()> {
        self.inner.delete(l).await
    }
    fn list(&self, p: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.maybe_fail(to)?;
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.maybe_fail(to)?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[test]
fn t1_publish_all_four_crash_points_restore_cleanly() {
    let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _hot_guard = EnvGuard::set("PULSE_HOT_STORE", "local");
    let _byok_guard = EnvGuard::set("PULSE_BYOK_MASTER_KEY", TEST_BYOK_KEY);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let cfg = StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 100_000,
            l0_flush_max_age: std::time::Duration::from_secs(3600),
            compaction_min_segments: 100,
        };

        // Test crash points (1), (2), and (3):
        // (1) after freeze before profile snapshot -> fail on `snapshots/profiles_000000000001`
        // (2) after profile snapshot before identity snapshot -> fail on `snapshots/identity_000000000001`
        // (3) after identity snapshot before PublishedManifest create -> fail on `pub_000000000001.json`
        for (case_idx, fail_pattern) in [
            (1, "snapshots/profiles_000000000001"),
            (2, "snapshots/identity_000000000001"),
            (3, "pub_000000000001.json"),
        ] {
            let pid = format!("proj_crash_{case_idx}");
            let tmp = tempfile::tempdir().unwrap();
            let data_dir = tmp.path().join("node");
            let fault_s3 = Arc::new(FaultInjectingStore::new(None));
            let s3_raw: Arc<dyn ObjectStore> = fault_s3.clone();

            let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc = Arc::new(EncryptedObjectStore::new(routed));
            let id_router = Arc::new(IdentityRouter::new(enc.as_object_store()));
            let engine = StorageEngine::new_with_encrypted(cfg.clone(), enc.clone(), id_router);

            let ps = engine.get_or_open_project(&pid).await.unwrap();
            let id_graph = engine.identity_graph(&pid, true).await.unwrap();
            let pstore = engine.profile_store(&pid, true).await.unwrap();

            // Publish pub_0 cleanly (user "base_u0", profile "base_u0").
            let u0 = id_graph.get_or_intern("base_u0", 0);
            id_graph.flush().await.unwrap();
            pstore
                .upsert(&StoredUserProfile {
                    project_id: pid.clone(),
                    distinct_id: "base_u0".into(),
                    canonical_uid: u0,
                    props: BTreeMap::from([("plan".into(), serde_json::json!("starter"))]),
                    total_revenue: 10.0,
                    updated_at_ms: 1000,
                })
                .await
                .unwrap();
            ps.append_bucket_events(0, vec![(make_event("base_u0", 1), u0)])
                .await
                .unwrap();
            ps.flush_bucket(0).await.unwrap();
            assert_eq!(ps.last_published_seq(), Some(0));

            // Inject crash on pub_1 at `fail_pattern`.
            let u1 = id_graph.get_or_intern("crash_u1", 0);
            id_graph.flush().await.unwrap();
            pstore
                .upsert(&StoredUserProfile {
                    project_id: pid.clone(),
                    distinct_id: "crash_u1".into(),
                    canonical_uid: u1,
                    props: BTreeMap::from([("plan".into(), serde_json::json!("pro"))]),
                    total_revenue: 50.0,
                    updated_at_ms: 2000,
                })
                .await
                .unwrap();
            ps.append_bucket_events(0, vec![(make_event("crash_u1", 2), u1)])
                .await
                .unwrap();
            fault_s3.set_fail_on(Some(fail_pattern));
            let err = ps
                .flush_bucket(0)
                .await
                .expect_err("injected crash must fail flush");
            assert!(fault_s3.triggered.load(AtomicOrdering::Acquire), "{err}");

            // Latest published in S3 must still be pub_0!
            fault_s3.set_fail_on(None);
            let latest = PublishedManifestStore::new(&pid, enc.as_object_store())
                .load_latest()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(latest.publish_seq, 0, "crash point {case_idx}");
            assert_eq!(latest.high_water_mark, 1, "crash point {case_idx}");

            pstore.close().await.unwrap();
            id_graph.close().await.unwrap();
            drop(ps);
            drop(engine);

            // Empty-dir BYOK restore recovers pub_0 cleanly!
            let r_tmp = tempfile::tempdir().unwrap();
            let r_data = r_tmp.path().join("node");
            let r_s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            copy_s3_prefix(&s3_raw, &r_s3, "segments").await;
            copy_s3_prefix(&s3_raw, &r_s3, "published").await;

            let r_routed = open_hot_store_for_restore(r_s3.clone(), &r_data)
                .await
                .unwrap();
            let r_enc = Arc::new(EncryptedObjectStore::new(r_routed));
            let restored = restore_project_from_published(&pid, &latest, r_enc.as_object_store())
                .await
                .unwrap();
            assert_eq!(restored.identity_user_count, 1);
            let r_reader = VortexSegmentReader::new(r_enc.as_object_store());
            let mut row_count = 0usize;
            for part in &latest.partitions {
                for seg in &part.segments {
                    for row in r_reader.read_all_rows(seg).await.unwrap() {
                        assert_eq!(
                            restored
                                .identity_graph
                                .external_id(row.uid_internal)
                                .as_deref(),
                            Some("base_u0")
                        );
                        row_count += 1;
                    }
                }
            }
            assert_eq!(row_count, 1);
            let r_prof = restored.profile_store.unwrap();
            assert_eq!(r_prof.count().await.unwrap(), 1);
            assert!(r_prof
                .get_by_distinct_id("base_u0")
                .await
                .unwrap()
                .is_some());
            restored.identity_graph.close().await.unwrap();
            r_prof.close().await.unwrap();
        }

        // Test crash point (4): after PublishedManifest create (pub_1 written to S3)
        // BEFORE local manifest persists `last_published_seq = Some(1)`.
        {
            let pid = "proj_crash_4";
            let tmp = tempfile::tempdir().unwrap();
            let data_dir = tmp.path().join("node");
            let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

            let routed = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc = Arc::new(EncryptedObjectStore::new(routed));
            let id_router = Arc::new(IdentityRouter::new(enc.as_object_store()));
            let engine = StorageEngine::new_with_encrypted(cfg.clone(), enc.clone(), id_router);

            let ps = engine.get_or_open_project(pid).await.unwrap();
            let id_graph = engine.identity_graph(pid, true).await.unwrap();
            let pstore = engine.profile_store(pid, true).await.unwrap();

            let u0 = id_graph.get_or_intern("u0", 0);
            id_graph.flush().await.unwrap();
            pstore
                .upsert(&StoredUserProfile {
                    project_id: pid.into(),
                    distinct_id: "u0".into(),
                    canonical_uid: u0,
                    props: BTreeMap::new(),
                    total_revenue: 0.0,
                    updated_at_ms: 1000,
                })
                .await
                .unwrap();
            ps.append_bucket_events(0, vec![(make_event("u0", 1), u0)])
                .await
                .unwrap();
            ps.flush_bucket(0).await.unwrap(); // commits epoch 1 + publishes pub_0

            let u1 = id_graph.get_or_intern("u1", 0);
            id_graph.flush().await.unwrap();
            pstore
                .upsert(&StoredUserProfile {
                    project_id: pid.into(),
                    distinct_id: "u1".into(),
                    canonical_uid: u1,
                    props: BTreeMap::new(),
                    total_revenue: 0.0,
                    updated_at_ms: 2000,
                })
                .await
                .unwrap();
            ps.append_bucket_events(0, vec![(make_event("u1", 2), u1)])
                .await
                .unwrap();
            ps.flush_bucket(0).await.unwrap(); // commits epoch 2 + publishes pub_1 (local manifest on disk has last_published_seq=Some(0)!)

            // Verify on-disk manifest still has last_published_seq == Some(0) while S3 has pub_1!
            let on_disk_mf = ManifestStore::new(pid, enc.as_object_store())
                .load_latest()
                .await
                .unwrap();
            assert_eq!(on_disk_mf.last_published_seq, Some(0));
            let latest_pub = PublishedManifestStore::new(pid, enc.as_object_store())
                .load_latest()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(latest_pub.publish_seq, 1);

            pstore.close().await.unwrap();
            id_graph.close().await.unwrap();
            drop(ps);
            drop(engine);

            // (4a) Empty-dir BYOK restore recovers pub_1 cleanly.
            let r_tmp = tempfile::tempdir().unwrap();
            let r_data = r_tmp.path().join("node");
            let r_s3: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            copy_s3_prefix(&s3_raw, &r_s3, "segments").await;
            copy_s3_prefix(&s3_raw, &r_s3, "published").await;
            let r_routed = open_hot_store_for_restore(r_s3.clone(), &r_data)
                .await
                .unwrap();
            let r_enc = Arc::new(EncryptedObjectStore::new(r_routed));
            let restored =
                restore_project_from_published(pid, &latest_pub, r_enc.as_object_store())
                    .await
                    .unwrap();
            assert_eq!(restored.identity_user_count, 2);
            assert_eq!(
                restored
                    .profile_store
                    .as_ref()
                    .unwrap()
                    .count()
                    .await
                    .unwrap(),
                2
            );
            restored.identity_graph.close().await.unwrap();
            restored.profile_store.unwrap().close().await.unwrap();

            // (4b) Same-dir restart reconciles last_published_seq = Some(1) and next flush writes pub_2!
            let routed2 = with_hot_store(s3_raw.clone(), &data_dir).await.unwrap();
            let enc2 = Arc::new(EncryptedObjectStore::new(routed2));
            let id_router2 = Arc::new(IdentityRouter::new(enc2.as_object_store()));
            let engine2 = StorageEngine::new_with_encrypted(cfg, enc2, id_router2);
            let ps2 = engine2.get_or_open_project(pid).await.unwrap();
            assert_eq!(ps2.last_published_seq(), Some(1));
            let id2 = engine2.identity_graph(pid, true).await.unwrap();
            let u2 = id2.get_or_intern("u2", 0);
            ps2.append_bucket_events(0, vec![(make_event("u2", 3), u2)])
                .await
                .unwrap();
            ps2.flush_bucket(0).await.unwrap();
            assert_eq!(ps2.last_published_seq(), Some(2));
        }
    });
}
