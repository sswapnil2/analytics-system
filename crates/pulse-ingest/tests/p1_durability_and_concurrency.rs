use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_identity::IdentityRouter;
use pulse_ingest::{normalize_event_timestamp, InMemoryGovernance, IngestService};
use pulse_storage::{
    build_object_store, decode_wal_frame, write_vortex_segment, BucketWal, ProjectDictSnapshot,
    ProjectDictionary, ReaderPinTracker, ReplayStore, StorageConfig, StorageEngine, StoredRow,
    VortexSegmentReader,
};
use pulse_wire::{NodeKind, PropValue, ReplayChunk, WireBatch, WireEvent, WireNode, WireframeOp};

#[derive(Debug)]
struct HookStore {
    inner: InMemory,
    fail_wal_puts: AtomicBool,
    fail_bucket1_wal: AtomicBool,
    delay_first_wal_ms: AtomicUsize,
    delay_segment_put_ms: AtomicUsize,
    wal_put_counter: AtomicUsize,
    get_delay_ms: AtomicUsize,
}

impl HookStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemory::new(),
            fail_wal_puts: AtomicBool::new(false),
            fail_bucket1_wal: AtomicBool::new(false),
            delay_first_wal_ms: AtomicUsize::new(0),
            delay_segment_put_ms: AtomicUsize::new(0),
            wal_put_counter: AtomicUsize::new(0),
            get_delay_ms: AtomicUsize::new(0),
        })
    }
}

impl Display for HookStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HookStore")
    }
}

#[async_trait]
impl ObjectStore for HookStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let p = location.as_ref();
        if p.starts_with("wal/") {
            if self.fail_wal_puts.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "HookStore",
                    source: Box::new(std::io::Error::other("injected wal put error")),
                });
            }
            if self.fail_bucket1_wal.load(Ordering::SeqCst) && p.contains("/bucket_001/") {
                return Err(object_store::Error::Generic {
                    store: "HookStore",
                    source: Box::new(std::io::Error::other("injected bucket_001 wal error")),
                });
            }
            let idx = self.wal_put_counter.fetch_add(1, Ordering::SeqCst);
            let delay = self.delay_first_wal_ms.load(Ordering::SeqCst);
            if idx == 0 && delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
        } else if p.starts_with("segments/") {
            let delay = self.delay_segment_put_ms.load(Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        let d = self.get_delay_ms.load(Ordering::SeqCst);
        if d > 0 && location.as_ref().starts_with("segments/") {
            tokio::time::sleep(Duration::from_millis(d as u64)).await;
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectPath) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn make_event(uuid_byte: u8, seed: &str, uid: &str, ts: i64) -> WireEvent {
    WireEvent {
        event_uuid: [uuid_byte; 16],
        routing_seed: seed.to_string(),
        distinct_id: uid.to_string(),
        session_id: "sess_1".to_string(),
        event_name: "Purchase".to_string(),
        client_event_ts_ms: ts,
        client_sent_ts_ms: ts,
        props: vec![("amount".to_string(), PropValue::Int(100))],
        replay_offset_kb: 0,
    }
}

#[tokio::test]
async fn p1_01_wal_put_failure_does_not_poison_dedup_on_retry() {
    let hook = HookStore::new();
    let store: Arc<dyn ObjectStore> = hook.clone();
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router,
    );
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_test", "proj_p1_01");
    let ingest = IngestService::new(storage.clone(), gov);

    let batch = WireBatch::new(vec![make_event(0xAA, "seed1", "u1", 1_762_000_000_000)]);
    let raw = batch.encode_binary(1).unwrap();

    // First attempt fails at WAL put
    hook.fail_wal_puts.store(true, Ordering::SeqCst);
    assert!(ingest.ingest_raw("pk_test", None, &raw).await.is_err());

    // Client retries exact same batch after S3 recovers — MUST be accepted, NOT deduped!
    hook.fail_wal_puts.store(false, Ordering::SeqCst);
    let retry = ingest.ingest_raw("pk_test", None, &raw).await.unwrap();
    assert_eq!(
        retry.accepted, 1,
        "retry after WAL error was falsely deduped!"
    );
    assert_eq!(retry.deduped, 0);
}

#[tokio::test]
async fn p1_02_out_of_order_wal_puts_never_lose_acknowledged_rows() {
    let hook = HookStore::new();
    hook.delay_first_wal_ms.store(80, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = hook.clone();

    // Phase 1 runs on its own runtime, which is shut down afterwards: that kills every task
    // it spawned, including a background L0 flush still in flight, the way a process crash
    // would. (Dropping the engine alone doesn't: the flush task holds its own reference.)
    let crash_store = store.clone();
    let phase1 = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let store = crash_store;
            let id_router = Arc::new(IdentityRouter::new(store.clone()));
            let storage = StorageEngine::new(
                StorageConfig {
                    num_buckets: 1,            // Force both concurrent requests onto bucket 0
                    l0_flush_row_threshold: 2, // Request B (2 rows) triggers flush; Request A (1 row) stays in WAL!
                    l0_flush_max_age: Duration::from_secs(60),
                    compaction_min_segments: 4,
                },
                store.clone(),
                id_router.clone(),
            );
            let gov = InMemoryGovernance::new();
            gov.register_api_key("pk_test", "proj_p1_02");
            let ingest = IngestService::new(storage.clone(), gov);
            // Warm up project + identity graph first so task_a deterministically hits WAL seq 1
            let _ = storage.get_or_open_project("proj_p1_02").await.unwrap();
            let _ = id_router.get_or_open("proj_p1_02").await.unwrap();

            let ing_a = ingest.clone();
            let ing_b = ingest.clone();

            let task_a = tokio::spawn(async move {
                ing_a
                    .ingest_for_project(
                        "proj_p1_02",
                        WireBatch::new(vec![make_event(1, "s", "u_slow", 1_762_000_001_000)]),
                        1_762_000_001_000,
                    )
                    .await
                    .unwrap()
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            let task_b = tokio::spawn(async move {
                ing_b
                    .ingest_for_project(
                        "proj_p1_02",
                        WireBatch::new(vec![
                            make_event(2, "s", "u_fast_1", 1_762_000_002_000),
                            make_event(3, "s", "u_fast_2", 1_762_000_003_000),
                        ]),
                        1_762_000_003_000,
                    )
                    .await
                    .unwrap()
            });

            let (ra, rb) = tokio::join!(task_a, task_b);
            assert_eq!(ra.unwrap().accepted, 1);
            assert_eq!(rb.unwrap().accepted, 2);
            id_router.close_all().await.unwrap();
        });
        // Abrupt process crash: no graceful flush_all, in-flight tasks die mid-write.
        rt.shutdown_background();
    });
    phase1.join().unwrap();

    // Recover from object store: ALL 3 acknowledged events MUST be present!
    let id_router2 = Arc::new(IdentityRouter::new(store.clone()));
    let storage2 = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router2,
    );
    let ps = storage2.get_or_open_project("proj_p1_02").await.unwrap();
    let snap = ps.snapshot();
    let total = ps.snapshot_l0_bucket(&snap, 0).len() as u64 + snap.manifest.total_l1_rows();
    assert_eq!(
        total, 3,
        "acknowledged batch was lost due to out-of-order WAL/L0 seq!"
    );
}

#[tokio::test]
async fn p1_03_default_local_object_store_uses_fsync_wrapper() {
    let tmp = tempfile::tempdir().unwrap();
    let store = build_object_store(tmp.path()).unwrap();
    let desc = format!("{store}");
    assert!(
        desc.contains("DurableLocalObjectStore"),
        "expected fsync-backed DurableLocalObjectStore for default local backend, got {desc}"
    );
}

#[tokio::test]
async fn p1_04_corrupt_wal_frame_is_quarantined_and_header_seq_is_crc_protected() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // 1. Write seq 1 (valid), seq 2 (will be corrupted on object store), seq 3 (valid)
    let wal = BucketWal::new("proj_p1_04", 0, 0, 0, store.clone());
    let b1 = WireBatch::new(vec![make_event(10, "s", "u1", 1_762_000_000_000)]);
    let b2 = WireBatch::new(vec![make_event(20, "s", "u2", 1_762_000_002_000)]);
    let b3 = WireBatch::new(vec![make_event(30, "s", "u3", 1_762_000_005_000)]);
    let s1 = wal.append(&b1).await.unwrap();
    let s2 = wal.append(&b2).await.unwrap();
    let s3 = wal.append(&b3).await.unwrap();
    assert_eq!((s1, s2, s3), (1, 2, 3));

    // Corrupt seq 2 on object store so recovery actually encounters a broken mid-stream WAL frame!
    let corrupt_path =
        ObjectPath::from("wal/proj_p1_04/bucket_000/epoch_0000000000_seq_000000000002.wal");
    store
        .put(
            &corrupt_path,
            PutPayload::from_static(b"PWALcorrupt_garbage_bytes_here!!"),
        )
        .await
        .unwrap();

    // Verify tampering with header `epoch`/`seq` bytes is caught by `decode_wal_frame`
    let seq1_path =
        ObjectPath::from("wal/proj_p1_04/bucket_000/epoch_0000000000_seq_000000000001.wal");
    let mut raw_frame = store
        .get(&seq1_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    // Flip a bit in header byte 5 (inside epoch/seq header)
    raw_frame[5] ^= 0xFF;
    assert!(
        decode_wal_frame(&raw_frame).is_err(),
        "WAL CRC must cover header epoch/seq/len fields!"
    );

    // Project must STILL open cleanly by quarantining the corrupt WAL frame to `wal_corrupt/`!
    let recovered = wal
        .recover(0)
        .await
        .expect("recover should quarantine bad frame and continue");
    assert_eq!(
        recovered.len(),
        2,
        "valid frames (seq 1 and seq 3) around corrupt seq 2 must be recovered"
    );
    let quarantined =
        ObjectPath::from("wal_corrupt/proj_p1_04/bucket_000/epoch_0000000000_seq_000000000002.wal");
    assert!(
        store.head(&quarantined).await.is_ok(),
        "corrupt WAL frame must be moved to wal_corrupt/"
    );
    assert!(
        store.head(&corrupt_path).await.is_err(),
        "corrupt WAL frame must be removed from active wal/ prefix"
    );
}

#[test]
fn p1_05_reader_pin_tracker_atomic_remove_if_never_drops_concurrent_pin() {
    let tracker = Arc::new(ReaderPinTracker::default());
    let seg = vec!["segments/p/20261116/bucket_000/seg_hot.vortex".to_string()];

    // Race 1->0 unpin against 0->1 pin across 8 threads (no permanent pin, so count hits 0 constantly!)
    let mut handles = Vec::new();
    for _ in 0..8 {
        let t = tracker.clone();
        let s = seg.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..25_000 {
                t.pin_all(&s);
                std::hint::spin_loop();
                assert!(
                    t.is_pinned(&s[0]),
                    "active pin was erased by concurrent unpin_all!"
                );
                t.unpin_all(&s);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(!tracker.is_pinned(&seg[0]));
}

#[tokio::test]
async fn p1_06_concurrent_compactions_do_not_duplicate_rows() {
    let hook = HookStore::new();
    let store: Arc<dyn ObjectStore> = hook.clone();
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store.clone(),
        id_router,
    );
    let ps = storage.get_or_open_project("proj_p1_06").await.unwrap();

    // Create 4 separate L1 segments in the same (date, bucket 0) partition (1 row each = 4 total rows)
    for i in 0..4u8 {
        ps.append_bucket_events(
            0,
            vec![(
                make_event(
                    40 + i,
                    "s",
                    &format!("u{i}"),
                    1_762_000_000_000 + (i as i64) * 1000,
                ),
                i as u32,
            )],
        )
        .await
        .unwrap();
        ps.flush_all().await.unwrap();
    }
    assert_eq!(ps.snapshot().manifest.total_l1_rows(), 4);

    // Slow down segment reads slightly so two concurrent `compact_overdue` calls overlap
    hook.get_delay_ms.store(25, Ordering::SeqCst);
    let reader = VortexSegmentReader::new(store);
    let (r1, r2) = tokio::join!(ps.compact_overdue(&reader), ps.compact_overdue(&reader));
    r1.unwrap();
    r2.unwrap();

    assert_eq!(
        ps.snapshot().manifest.total_l1_rows(),
        4,
        "concurrent compactions duplicated rows!"
    );
}

#[tokio::test]
async fn p1_07_snapshot_taken_before_flush_never_misses_evicted_l0_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router,
    );
    let ps = storage.get_or_open_project("proj_p1_07").await.unwrap();
    ps.append_bucket_events(
        0,
        vec![
            (make_event(71, "s", "u1", 1_762_000_000_000), 0),
            (make_event(72, "s", "u2", 1_762_000_001_000), 1),
        ],
    )
    .await
    .unwrap();

    // Reader acquires snapshot at Epoch 0 (2 rows in L0, 0 in L1)
    let snap = ps.snapshot();

    // Concurrent flush completes BEFORE reader scans `snapshot_l0_bucket`!
    ps.flush_all().await.unwrap();

    let visible_rows = ps.snapshot_l0_bucket(&snap, 0).len() as u64 + snap.manifest.total_l1_rows();
    assert_eq!(
        visible_rows, 2,
        "reader missed rows that were flushed after snapshot() acquisition!"
    );
}

#[tokio::test]
async fn p1_10_interned_ids_are_durable_before_inline_l1_flush() {
    let hook = HookStore::new();
    // Bucket 1 WAL will fail, while Bucket 0 succeeds and triggers inline L1 flush (`threshold = 1`)!
    hook.fail_bucket1_wal.store(true, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = hook.clone();

    {
        let id_router = Arc::new(IdentityRouter::new(store.clone()));
        let storage = StorageEngine::new(
            StorageConfig {
                num_buckets: 2,
                l0_flush_row_threshold: 1,
                l0_flush_max_age: Duration::from_secs(60),
                compaction_min_segments: 4,
            },
            store.clone(),
            id_router,
        );
        let gov = InMemoryGovernance::new();
        gov.register_api_key("pk_test", "proj_p1_10");
        let ingest = IngestService::new(storage, gov);

        // Find one seed routing to bucket 0 and one routing to bucket 1
        let mut s_b0 = String::new();
        let mut s_b1 = String::new();
        for i in 0..100 {
            let cand = format!("seed_{i}");
            let b = pulse_storage::route_bucket(&cand, 2);
            if b == 0 && s_b0.is_empty() {
                s_b0 = cand;
            } else if b == 1 && s_b1.is_empty() {
                s_b1 = cand;
            }
        }

        let batch = WireBatch::new(vec![
            make_event(81, &s_b0, "user_b0_flushed", 1_762_000_000_000),
            make_event(82, &s_b1, "user_b1_failed", 1_762_000_000_000),
        ]);
        // `ingest_for_project` fails on bucket 1 AFTER bucket 0 may have flushed to L1!
        let _ = ingest
            .ingest_for_project("proj_p1_10", batch, 1_762_000_000_000)
            .await;
    }

    // Reopen identity & storage: if L1 has rows, their interned `u32` IDs MUST exist in reopened IdentityGraph!
    let id_router2 = Arc::new(IdentityRouter::new(store.clone()));
    let storage2 = StorageEngine::new(
        StorageConfig {
            num_buckets: 2,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router2.clone(),
    );
    let ps = storage2.get_or_open_project("proj_p1_10").await.unwrap();
    let l1_rows = ps.snapshot().manifest.total_l1_rows();
    if l1_rows > 0 {
        let g = id_router2.get_or_open("proj_p1_10").await.unwrap();
        assert!(
            g.lookup_raw("user_b0_flushed").is_some(),
            "L1 segment contains uid whose identity was never flushed to SlateDB!"
        );
    }
}

#[test]
fn p1_12_dictionary_does_not_silently_alias_at_u16_max() {
    let keys: Vec<String> = (0..=u16::MAX as usize).map(|i| format!("k_{i}")).collect();
    let dict = ProjectDictionary::from_snapshot(ProjectDictSnapshot {
        events: Vec::new(),
        prop_keys: keys,
    });
    // Dictionary already holds all 65,536 u16 slots (0..=65535); a 65,537th distinct key must NOT return 65535!
    let before = dict.prop_key_name(u16::MAX).unwrap();
    let overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dict.get_or_intern_prop_key("k_overflow_65536")
    }));
    if let Ok(id) = overflow {
        assert_ne!(
            dict.prop_key_name(id).as_deref(),
            Some(before.as_str()),
            "dictionary silently aliased k_overflow_65536 onto slot 65535!"
        );
    }
}

#[tokio::test]
async fn p1_13_and_p1_14_json_backfill_timestamps_and_distinct_prop_uuids_preserved() {
    // Check overflow safety on extreme i64 timestamps
    let _ = normalize_event_timestamp(i64::MIN, i64::MAX, 0);

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router,
    );
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_test", "proj_p1_13_14");
    let ingest = IngestService::new(storage.clone(), gov);

    let historical_ts = 1_700_000_000_000i64; // Backfilled historical timestamp (no `client_sent_ts_ms`)
    let json_body = br#"[
        {"distinct_id":"u1","session_id":"s1","event_name":"add_to_cart","client_event_ts_ms":1700000000000,"props":{"sku":"apple"}},
        {"distinct_id":"u1","session_id":"s1","event_name":"add_to_cart","client_event_ts_ms":1700000000000,"props":{"sku":"banana"}}
    ]"#;

    let resp = ingest
        .ingest_raw("pk_test", Some("application/json"), json_body)
        .await
        .unwrap();
    assert_eq!(
        resp.accepted, 2,
        "two add_to_cart events with different SKUs collided on synthetic UUID!"
    );
    assert_eq!(resp.deduped, 0);

    let ps = storage.get_or_open_project("proj_p1_13_14").await.unwrap();
    let snap = ps.snapshot();
    let rows = ps.snapshot_l0_bucket(&snap, 0);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].event_ts_ms, historical_ts,
        "clock-skew normalizer overwrote historical JSON backfill timestamp!"
    );
}

#[tokio::test]
async fn p1_15_bool_true_and_str_true_do_not_collide_in_inverted_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let date = chrono::NaiveDate::from_ymd_opt(2026, 11, 16).unwrap();

    let rows = vec![
        StoredRow {
            l0_seq: 1,
            wal_seq: 1,
            event_uuid_fp: 1,
            uid_internal: 1,
            session_hash: 10,
            session_id: "s1".into(),
            event_id: 0,
            event_ts_ms: 1000,
            replay_offset_kb: 0,
            session_flags: 0,
            props: vec![(0, PropValue::Bool(true))],
        },
        StoredRow {
            l0_seq: 2,
            wal_seq: 2,
            event_uuid_fp: 2,
            uid_internal: 2,
            session_hash: 20,
            session_id: "s2".into(),
            event_id: 0,
            event_ts_ms: 2000,
            replay_offset_kb: 0,
            session_flags: 0,
            props: vec![(0, PropValue::Str("true".into()))],
        },
    ];

    let meta = write_vortex_segment(&store, "proj_p1_15", date, 0, "seg_1", rows)
        .await
        .unwrap()
        .unwrap();
    let reader = VortexSegmentReader::new(store);
    let idx = reader.read_inverted_index(&meta).await.unwrap();

    // Row 0 (Bool true) and Row 1 (Str "true") must NOT overwrite each other's bitmap!
    assert!(
        idx.bitmaps.len() >= 2,
        "Bool(true) and Str(\"true\") shared one inverted-index key and overwrote each other!"
    );
}

#[tokio::test]
async fn p1_16_replay_store_immutable_chunks_survive_midnight_restart_and_reject_traversal() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let day1_23_59 = 1_763_251_140_000i64; // 2025-11-15 23:59:00 UTC
    let day2_00_02 = day1_23_59 + 180_000; // 2025-11-16 00:02:00 UTC (next UTC date!)

    {
        let rstore1 = ReplayStore::new(store.clone());
        // Path traversal session_id MUST be rejected
        let bad = ReplayChunk {
            project_id: "proj_r".into(),
            session_id: "../other_sess".into(),
            chunk_start_ts_ms: day1_23_59,
            ops: vec![],
        };
        assert!(
            rstore1.append_chunk(&bad).await.is_err(),
            "ReplayStore must reject session_id containing '/'"
        );

        let c1 = ReplayChunk {
            project_id: "proj_r".into(),
            session_id: "sess_midnight_01".into(),
            chunk_start_ts_ms: day1_23_59,
            ops: vec![WireframeOp::FullSnapshot {
                ts_offset_ms: 0,
                width: 390,
                height: 844,
                nodes: vec![WireNode {
                    node_id: 1,
                    parent_id: 0,
                    kind: NodeKind::Screen,
                    x: 0,
                    y: 0,
                    w: 390,
                    h: 844,
                    bg_rgba: 0xFF,
                    masked_label: "Before Midnight".into(),
                }],
            }],
        };
        rstore1.append_chunk(&c1).await.unwrap();
    }

    // Simulate process restart past midnight (in-memory `session_paths` cache is empty!)
    let rstore2 = ReplayStore::new(store);
    let c2 = ReplayChunk {
        project_id: "proj_r".into(),
        session_id: "sess_midnight_01".into(),
        chunk_start_ts_ms: day2_00_02,
        ops: vec![WireframeOp::NodeRemove { node_id: 1 }],
    };
    let append2 = rstore2.append_chunk(&c2).await.unwrap();
    assert!(
        append2.byte_offset > 0,
        "second chunk past midnight started at offset 0 (new file)!"
    );

    let all_chunks = rstore2
        .read_chunks("proj_r", "sess_midnight_01", 0)
        .await
        .unwrap();
    assert_eq!(
        all_chunks.len(),
        2,
        "session chunks split across midnight after restart were lost!"
    );
}

#[tokio::test]
async fn ha_01_ingest_does_not_block_on_slow_l1_segment_flush_and_enforces_l0_backpressure() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::build_ingest_router;
    use pulse_storage::StorageError;
    use tower::ServiceExt;

    let hook = HookStore::new();
    hook.delay_segment_put_ms.store(250, Ordering::SeqCst);
    let store: Arc<dyn ObjectStore> = hook.clone();
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 2,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router.clone(),
    );
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_ha_01", "proj_ha_01");
    let ingest = IngestService::new(storage.clone(), gov);

    let ps = storage.get_or_open_project("proj_ha_01").await.unwrap();

    // 1. Ingesting at the L0 flush threshold must NOT block waiting on the 250ms L1 segment upload!
    let t0 = std::time::Instant::now();
    let resp = ingest
        .ingest_for_project(
            "proj_ha_01",
            WireBatch::new(vec![
                make_event(1, "s", "u1", 1_762_000_000_000),
                make_event(2, "s", "u2", 1_762_000_001_000),
            ]),
            1_762_000_001_000,
        )
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(resp.accepted, 2);
    assert!(
        elapsed < Duration::from_millis(150),
        "ingest blocked on background L1 flush: took {elapsed:?}"
    );

    // Wait for the background L1 segment flush to finish
    let mut flushed = false;
    for _ in 0..40 {
        if ps.snapshot().manifest.total_l1_rows() == 2 && ps.total_l0_rows() == 0 {
            flushed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(flushed, "background L0->L1 flush did not complete in time");

    // 2. Enforce L0 high-water mark backpressure (HTTP 429 + Retry-After: 1)
    ps.set_max_l0_rows_per_bucket(2);
    hook.delay_segment_put_ms.store(300, Ordering::SeqCst);

    let r2 = ingest
        .ingest_for_project(
            "proj_ha_01",
            WireBatch::new(vec![
                make_event(3, "s", "u3", 1_762_000_002_000),
                make_event(4, "s", "u4", 1_762_000_003_000),
            ]),
            1_762_000_003_000,
        )
        .await
        .unwrap();
    assert_eq!(r2.accepted, 2);

    // Direct service call must reject with StorageError::L0Backpressure while L0 is at high-water mark
    let err = ingest
        .ingest_for_project(
            "proj_ha_01",
            WireBatch::new(vec![make_event(5, "s", "u5", 1_762_000_004_000)]),
            1_762_000_004_000,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            pulse_ingest::IngestError::Storage(StorageError::L0Backpressure { .. })
        ),
        "expected L0Backpressure, got {err:?}"
    );

    // HTTP router must return 429 Too Many Requests with Retry-After: 1
    let app = build_ingest_router(ingest.clone());
    let payload_json = serde_json::json!({
        "events": [{
            "distinct_id": "u6",
            "event_name": "Purchase",
            "client_event_ts_ms": 1_762_000_005_000i64
        }]
    })
    .to_string();
    let http_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/ingest")
                .header("x-pulse-key", "pk_ha_01")
                .header("content-type", "application/json")
                .body(Body::from(payload_json.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(http_resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        http_resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );

    // Once background flush drains L0, subsequent ingest succeeds with 200 OK
    let mut drained = false;
    for _ in 0..40 {
        if ps.total_l0_rows() == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        drained,
        "background flush did not drain L0 after backpressure"
    );

    let ok_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/ingest")
                .header("x-pulse-key", "pk_ha_01")
                .header("content-type", "application/json")
                .body(Body::from(payload_json))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), StatusCode::OK);

    id_router.close_all().await.unwrap();
}

#[tokio::test]
async fn ingest_rate_limit_is_per_project_and_returns_429_with_retry_after() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::rate_limit::RateLimiter;
    use pulse_ingest::{build_ingest_router, IngestError};
    use tower::ServiceExt;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 10_000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store,
        id_router,
    );
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_a", "proj_rl_a");
    gov.register_api_key("pk_a2", "proj_rl_a"); // same tenant, second key
    gov.register_api_key("pk_b", "proj_rl_b");
    // 10 events/s, burst 10.
    let ingest = IngestService::with_rate_limit(storage, gov, RateLimiter::new(10.0, 1.0));
    let ts = 1_762_000_000_000i64;
    let batch = |n: u8| {
        WireBatch::new(
            (0..n)
                .map(|i| make_event(i, "s", &format!("u{i}"), ts + i64::from(i)))
                .collect(),
        )
        .encode_binary(1)
        .unwrap()
    };

    assert!(ingest.ingest_raw("pk_a", None, &batch(8)).await.is_ok());
    // 2 left: a 5-event batch is still admitted and overdraws to -3.
    assert!(ingest.ingest_raw("pk_a2", None, &batch(5)).await.is_ok());
    // Both keys share the tenant's quota.
    assert!(matches!(
        ingest.ingest_raw("pk_a", None, &batch(1)).await,
        Err(IngestError::RateLimited {
            retry_after_secs: 1
        })
    ));
    // Another tenant is unaffected.
    assert!(ingest.ingest_raw("pk_b", None, &batch(3)).await.is_ok());

    let resp = build_ingest_router(ingest.clone())
        .oneshot(
            Request::post("/v1/ingest")
                .header("x-pulse-key", "pk_a")
                .body(Body::from(batch(1)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(resp.headers()["retry-after"], "1");
}
