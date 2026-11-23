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
    delay_every_wal_ms: AtomicUsize,
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
            delay_every_wal_ms: AtomicUsize::new(0),
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
            let every = self.delay_every_wal_ms.load(Ordering::SeqCst);
            if every > 0 {
                tokio::time::sleep(Duration::from_millis(every as u64)).await;
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
async fn wal_put_failure_does_not_poison_dedup_on_retry() {
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

    // First attempt fails at WAL put (synchronous WAL test)
    hook.fail_wal_puts.store(true, Ordering::SeqCst);
    assert!(ingest
        .ingest_for_project("proj_p1_01", batch.clone(), 1_762_000_000_000)
        .await
        .is_err());

    // Client retries exact same batch after S3 recovers — MUST be accepted, NOT deduped!
    hook.fail_wal_puts.store(false, Ordering::SeqCst);
    let retry = ingest
        .ingest_for_project("proj_p1_01", batch, 1_762_000_000_000)
        .await
        .unwrap();
    assert_eq!(
        retry.accepted, 1,
        "retry after WAL error was falsely deduped!"
    );
    assert_eq!(retry.deduped, 0);
}

#[tokio::test]
async fn out_of_order_wal_puts_never_lose_acknowledged_rows() {
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
async fn default_local_object_store_uses_fsync_wrapper() {
    let tmp = tempfile::tempdir().unwrap();
    let store = build_object_store(tmp.path()).unwrap();
    let desc = format!("{store}");
    assert!(
        desc.contains("DurableLocalObjectStore"),
        "expected fsync-backed DurableLocalObjectStore for default local backend, got {desc}"
    );
}

#[tokio::test]
async fn corrupt_wal_frame_is_quarantined_and_header_seq_is_crc_protected() {
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
fn reader_pin_tracker_atomic_remove_if_never_drops_concurrent_pin() {
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
async fn concurrent_compactions_do_not_duplicate_rows() {
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
async fn snapshot_taken_before_flush_never_misses_evicted_l0_rows() {
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
async fn interned_ids_are_durable_before_inline_l1_flush() {
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
fn dictionary_does_not_silently_alias_at_u16_max() {
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
async fn json_backfill_timestamps_and_distinct_prop_uuids_preserved() {
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

    ingest.flush_all().await.unwrap();
    let ps = storage.open_for_read("proj_p1_13_14").await.unwrap();
    let snap = ps.snapshot();
    let rows = ps.snapshot_l0_bucket(&snap, 0);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].event_ts_ms, historical_ts,
        "clock-skew normalizer overwrote historical JSON backfill timestamp!"
    );
}

#[tokio::test]
async fn bool_true_and_str_true_do_not_collide_in_inverted_index() {
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
async fn replay_store_immutable_chunks_survive_midnight_restart_and_reject_traversal() {
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
        ops: vec![WireframeOp::NodeRemove {
            ts_offset_ms: 0,
            node_id: 1,
        }],
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
    ps.wait_applied(Duration::from_secs(5)).await;

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

fn group_commit_fixture() -> (Arc<HookStore>, Arc<StorageEngine>, Arc<IngestService>) {
    let hook = HookStore::new();
    let store: Arc<dyn ObjectStore> = hook.clone();
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 1,
            l0_flush_row_threshold: 100_000,
            l0_flush_max_age: Duration::from_secs(600),
            compaction_min_segments: 4,
        },
        store.clone(),
        Arc::new(IdentityRouter::new(store)),
    );
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_test", "proj_group");
    let ingest = IngestService::new(storage.clone(), gov);
    (hook, storage, ingest)
}

/// Requests that arrive while a WAL write is in flight used to queue for one durable PUT each.
/// They now share the next one, and each is still acknowledged only after it is durable.
#[tokio::test]
async fn concurrent_requests_on_a_bucket_share_one_wal_write() {
    let (hook, storage, ingest) = group_commit_fixture();
    // Warm the project so the delayed PUT below is the first of the concurrent wave.
    let warm = WireBatch::new(vec![make_event(0xF0, "s", "warm", 1_762_000_000_000)]);
    ingest
        .ingest_for_project("proj_group", warm, 1_762_000_000_000)
        .await
        .unwrap();
    hook.wal_put_counter.store(0, Ordering::SeqCst);
    hook.delay_first_wal_ms.store(200, Ordering::SeqCst);

    // 32 requests with one distinct event each, plus two that repeat request 0's event.
    let batches: Vec<WireBatch> = (0..34u8)
        .map(|i| {
            let id = if i >= 32 { 0 } else { i };
            WireBatch::new(vec![make_event(
                id,
                "s",
                &format!("u{id}"),
                1_762_000_000_000 + i64::from(id),
            )])
        })
        .collect();
    let results = futures::future::join_all(
        batches
            .into_iter()
            .map(|b| ingest.ingest_for_project("proj_group", b, 1_762_000_000_000)),
    )
    .await;

    let (mut accepted, mut deduped) = (0, 0);
    for r in results {
        let r = r.expect("every request is acknowledged");
        accepted += r.accepted;
        deduped += r.deduped;
    }
    assert_eq!(accepted, 32, "each distinct event is stored exactly once");
    assert_eq!(deduped, 2, "a repeat inside the same group is deduplicated");
    let puts = hook.wal_put_counter.load(Ordering::SeqCst);
    assert!(puts <= 3, "34 concurrent requests made {puts} WAL writes");

    // What was acknowledged is in the WAL: a second engine on the same store replays all of it.
    let ps = storage.open_for_read("proj_group").await.unwrap();
    assert_eq!(ps.total_l0_rows(), 33);
    let store: Arc<dyn ObjectStore> = hook.clone();
    let recovered = StorageEngine::new(
        ps.config().clone(),
        store.clone(),
        Arc::new(IdentityRouter::new(store)),
    )
    .get_or_open_project("proj_group")
    .await
    .unwrap();
    assert_eq!(recovered.total_l0_rows(), 33);
}

/// The request holding the bucket lock writes for everyone queued behind it. If that request is
/// dropped mid-write (client disconnect, timeout), the others must still be written and answered.
#[tokio::test]
async fn dropping_the_request_that_writes_a_group_does_not_fail_the_others() {
    let (hook, storage, ingest) = group_commit_fixture();
    let batch = |i: u8| {
        WireBatch::new(vec![make_event(
            i,
            "s",
            &format!("u{i}"),
            1_762_000_000_000,
        )])
    };
    let send = |i: u8| {
        let (ingest, b) = (ingest.clone(), batch(i));
        tokio::spawn(async move {
            ingest
                .ingest_for_project("proj_group", b, 1_762_000_000_000)
                .await
        })
    };
    send(0xF0).await.unwrap().unwrap();
    hook.delay_every_wal_ms.store(300, Ordering::SeqCst);

    // A holds the lock for one slow PUT. B queues first behind it, then seven more, so B writes
    // one record for all eight once A is done.
    let a = send(0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let b = send(1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let others: Vec<_> = (2..9u8).map(send).collect();
    // A's PUT ends at ~300 ms; B's starts then. Drop B in the middle of it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    b.abort();

    for (i, r) in futures::future::join_all(others)
        .await
        .into_iter()
        .enumerate()
    {
        let r = r.unwrap();
        assert!(
            r.is_ok(),
            "request {} failed after the writer was dropped: {r:?}",
            i + 2
        );
        assert_eq!(r.unwrap().accepted, 1);
    }
    a.await.unwrap().unwrap();
    // warm + A + seven others, and B's own event if its record completed: each at most once.
    let ps = storage.open_for_read("proj_group").await.unwrap();
    let rows = ps.total_l0_rows();
    assert!((9..=10).contains(&rows), "{rows} rows");
}

/// A failed group write fails or retries every request in it; none is acknowledged, none is
/// remembered by dedup, and a client retry is stored.
#[tokio::test]
async fn failed_group_write_acknowledges_nothing_and_retries_are_stored() {
    let (hook, storage, ingest) = group_commit_fixture();
    let batches: Vec<WireBatch> = (0..8u8)
        .map(|i| {
            WireBatch::new(vec![make_event(
                i,
                "s",
                &format!("u{i}"),
                1_762_000_000_000,
            )])
        })
        .collect();
    let send_all = || {
        futures::future::join_all(
            batches
                .iter()
                .cloned()
                .map(|b| ingest.ingest_for_project("proj_group", b, 1_762_000_000_000)),
        )
    };
    hook.fail_wal_puts.store(true, Ordering::SeqCst);
    assert!(send_all().await.iter().all(|r| r.is_err()));
    let ps = storage.open_for_read("proj_group").await.unwrap();
    assert_eq!(ps.total_l0_rows(), 0);

    hook.fail_wal_puts.store(false, Ordering::SeqCst);
    let retried = send_all().await;
    assert!(retried.iter().all(|r| r.as_ref().unwrap().accepted == 1));
    ps.wait_applied(Duration::from_secs(5)).await;
    assert_eq!(ps.total_l0_rows(), 8);
}

#[tokio::test]
async fn w1_async_ack_flushes_on_20ms_timer_and_counts_dedup_in_metrics() {
    let (_hook, storage, ingest) = group_commit_fixture();
    let batch = WireBatch::new(vec![make_event(1, "s1", "u1", 1_762_000_000_000)]);
    let raw = batch.encode_binary(1).unwrap();

    let r1 = ingest.ingest_raw("pk_test", None, &raw).await.unwrap();
    let r2 = ingest.ingest_raw("pk_test", None, &raw).await.unwrap();
    assert_eq!(r1.accepted, 1);
    assert_eq!(r1.deduped, 0);
    assert_eq!(r2.accepted, 1);
    assert_eq!(r2.deduped, 0, "W1 async ack always returns deduped=0");

    // Without manual flush, the 20ms timer flushes to WAL and counts exact duplicates in metrics.
    let mut flushed = false;
    for _ in 0..50 {
        if ingest.buffer_stats().buffer_bytes == 0
            && ingest.metrics().deduped_total.load(Ordering::Relaxed) == 1
        {
            flushed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(flushed, "20ms timer flusher did not flush & dedup in time");
    assert_eq!(ingest.metrics().accepted_total.load(Ordering::Relaxed), 1);
    assert!(ingest.buffer_stats().flushes_timer >= 1);

    let ps = storage.open_for_read("proj_group").await.unwrap();
    assert_eq!(ps.total_l0_rows(), 1);
}

#[tokio::test]
async fn w1_async_buffer_flushes_on_byte_threshold_and_enforces_429_backpressure() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::build_ingest_router;
    use tower::ServiceExt;

    let (hook, _storage, ingest) = group_commit_fixture();
    // 1) Byte-threshold trigger (`flushes_bytes >= 1`).
    ingest.set_buffer_limits_for_test(64 * 1024, 64 * 1024, 60_000, 300);
    let batch = WireBatch::new(vec![
        make_event(1, "s1", "u1", 1_762_000_000_000),
        make_event(2, "s1", "u2", 1_762_000_001_000),
    ]);
    ingest
        .ingest_raw("pk_test", None, &batch.encode_binary(1).unwrap())
        .await
        .unwrap();
    let mut byte_flushed = false;
    for _ in 0..50 {
        if ingest.buffer_stats().flushes_bytes >= 1 && ingest.buffer_stats().buffer_bytes == 0 {
            byte_flushed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(byte_flushed, "byte threshold did not trigger flush");

    // 2) Buffer saturation returns HTTP 429 `ingest_buffer_backpressure`.
    hook.delay_every_wal_ms.store(300, Ordering::SeqCst);
    ingest.set_buffer_limits_for_test(512, 512, 1, 256);
    let app = build_ingest_router(ingest.clone());
    let b1 = WireBatch::new(vec![make_event(10, "s1", "u10", 1_762_000_002_000)])
        .encode_binary(1)
        .unwrap();
    let r1 = app
        .clone()
        .oneshot(
            Request::post("/v1/ingest")
                .header("x-pulse-key", "pk_test")
                .body(Body::from(b1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);

    // Fill past 512 bytes while the slow WAL write keeps the buffer occupied.
    let mut got_429 = false;
    for i in 11..20u8 {
        let bi = WireBatch::new(vec![make_event(
            i,
            "s1",
            &format!("u{i}"),
            1_762_000_003_000,
        )])
        .encode_binary(1)
        .unwrap();
        let resp = app
            .clone()
            .oneshot(
                Request::post("/v1/ingest")
                    .header("x-pulse-key", "pk_test")
                    .body(Body::from(bi))
                    .unwrap(),
            )
            .await
            .unwrap();
        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            let body_bytes = axum::body::to_bytes(resp.into_body(), 16 * 1024)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
            assert_eq!(v["code"], "ingest_buffer_backpressure");
            got_429 = true;
            break;
        }
    }
    assert!(
        got_429,
        "expected 429 ingest_buffer_backpressure when buffer full"
    );
    hook.delay_every_wal_ms.store(0, Ordering::SeqCst);
    ingest.flush_all().await.unwrap();
}

#[tokio::test]
async fn w1_async_wal_error_circuit_breaker_returns_503_before_429_and_recovers_zero_loss() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::build_ingest_router;
    use tower::ServiceExt;

    let (hook, storage, ingest) = group_commit_fixture();
    // Tight buffer cap (600 B): normally 2nd/3rd event would hit 429, but wal_error MUST take
    // precedence and return 503!
    ingest.set_buffer_limits_for_test(600, 600, 5, 128);
    let app = build_ingest_router(ingest.clone());

    hook.fail_wal_puts.store(true, Ordering::SeqCst);
    let b1 = WireBatch::new(vec![make_event(1, "s1", "u1", 1_762_000_000_000)])
        .encode_binary(1)
        .unwrap();
    let r1 = app
        .clone()
        .oneshot(
            Request::post("/v1/ingest")
                .header("x-pulse-key", "pk_test")
                .body(Body::from(b1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);

    // Wait for flusher to attempt WAL write and trip sticky wal_error.
    let mut got_503 = false;
    for i in 2..15u8 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let bi = WireBatch::new(vec![make_event(
            i,
            "s1",
            &format!("u{i}"),
            1_762_000_001_000,
        )])
        .encode_binary(1)
        .unwrap();
        let resp = app
            .clone()
            .oneshot(
                Request::post("/v1/ingest")
                    .header("x-pulse-key", "pk_test")
                    .body(Body::from(bi))
                    .unwrap(),
            )
            .await
            .unwrap();
        if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
            got_503 = true;
            break;
        }
    }
    assert!(got_503, "expected 503 once sticky wal_error tripped");

    // Once WAL recovers, all previously 200-acked events drain with zero data loss.
    hook.fail_wal_puts.store(false, Ordering::SeqCst);
    let expected_accepted = ingest.buffer_stats().buffer_events;
    assert!(expected_accepted >= 1);
    let mut recovered = false;
    for _ in 0..80 {
        if ingest.buffer_stats().buffer_bytes == 0 {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        recovered,
        "sticky wal_error did not drain after WAL recovery"
    );
    let ps = storage.open_for_read("proj_group").await.unwrap();
    assert_eq!(ps.total_l0_rows(), expected_accepted);
}

#[tokio::test]
async fn w1_async_mixed_batch_identify_failure_is_atomic_with_zero_events_queued() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::{build_ingest_router, IdentifyRequest};
    use tower::ServiceExt;

    let (_hook, storage, ingest) = group_commit_fixture();
    // Create two already-identified users so aliasing them triggers 409 IdentifiedMergeConflict.
    ingest
        .identify_for_project(
            "proj_group",
            IdentifyRequest {
                anon_id: "a1".into(),
                user_id: "real_user_1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    ingest
        .identify_for_project(
            "proj_group",
            IdentifyRequest {
                anon_id: "a2".into(),
                user_id: "real_user_2".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let app = build_ingest_router(ingest.clone());
    let mixed = serde_json::json!({
        "batch": [
            { "type": "track", "userId": "real_user_1", "event": "Checkout", "messageId": "m_mix_1" },
            { "type": "identify", "userId": "real_user_1", "anonymousId": "real_user_2" }
        ]
    });
    let resp = app
        .oneshot(
            Request::post("/v1/ingest")
                .header("x-pulse-key", "pk_test")
                .header("content-type", "application/json")
                .body(Body::from(mixed.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    assert_eq!(ingest.buffer_stats().buffer_bytes, 0);
    assert_eq!(ingest.buffer_stats().buffer_events, 0);
    ingest.flush_all().await.unwrap();
    let ps = storage.open_for_read("proj_group").await.unwrap();
    assert_eq!(ps.total_l0_rows(), 0, "mixed-batch failure leaked events");
}

#[tokio::test]
async fn w1_async_drops_per_batch_422_and_dead_letters_project_cap_with_reason() {
    let (hook, storage, ingest) = group_commit_fixture();
    let store: Arc<dyn ObjectStore> = hook.clone();

    // 1) > MAX_NEW_EVENTS_PER_BATCH (128) in a single batch succeeds on async /v1/ingest (no 422).
    let many_events: Vec<WireEvent> = (0..150u64)
        .map(|i| WireEvent {
            event_uuid: WireEvent::deterministic_uuid("u", "s", &format!("ev_{i}"), 100, i),
            routing_seed: "u".into(),
            distinct_id: "u".into(),
            session_id: "s".into(),
            event_name: format!("ev_{i}"),
            client_event_ts_ms: 1_762_000_000_000 + i as i64,
            client_sent_ts_ms: 0,
            props: vec![],
            replay_offset_kb: 0,
        })
        .collect();
    let raw_many = WireBatch::new(many_events).encode_binary(1).unwrap();
    let resp = ingest.ingest_raw("pk_test", None, &raw_many).await.unwrap();
    assert_eq!(resp.accepted, 150);
    ingest.flush_all().await.unwrap();

    // 2) Fill dictionary to MAX_EVENTS_PER_PROJECT (8192), then send one more new event via ingest_raw.
    let ps = storage.open_for_read("proj_group").await.unwrap();
    let mut idx = 0usize;
    while ps.dictionary().event_count() < pulse_ingest::service::MAX_EVENTS_PER_PROJECT {
        let _ = ps
            .dictionary()
            .try_get_or_intern_event(&format!("fill_cap_{idx}"));
        idx += 1;
    }
    let over_cap = WireBatch::new(vec![WireEvent {
        event_uuid: WireEvent::deterministic_uuid("u_dl", "s_dl", "over_cap_ev", 999, 999),
        routing_seed: "u_dl".into(),
        distinct_id: "u_dl".into(),
        session_id: "s_dl".into(),
        event_name: "over_cap_ev".into(),
        client_event_ts_ms: 1_762_000_009_000,
        client_sent_ts_ms: 0,
        props: vec![],
        replay_offset_kb: 0,
    }])
    .encode_binary(1)
    .unwrap();
    let dl_resp = ingest.ingest_raw("pk_test", None, &over_cap).await.unwrap();
    assert_eq!(dl_resp.accepted, 1);
    ingest.flush_all().await.unwrap();

    let dead = pulse_storage::list_dead_letters(&store, "proj_group", None)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].event.event_name, "over_cap_ev");
    assert_eq!(
        dead[0].reason.as_deref(),
        Some("project schema cap reached (event names or property keys)")
    );
}

#[derive(Debug)]
struct ToggleLeases {
    ttl: Duration,
    available: AtomicBool,
    fenced_away: AtomicBool,
    generation: u64,
}

#[async_trait]
impl pulse_storage::WriterLeases for ToggleLeases {
    async fn acquire(&self, _project_id: &str) -> std::result::Result<Option<u64>, String> {
        if self.fenced_away.load(Ordering::SeqCst) {
            return Ok(None);
        }
        if self.available.load(Ordering::SeqCst) {
            Ok(Some(self.generation))
        } else {
            Err("transient metastore outage".to_string())
        }
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }
}

#[tokio::test]
async fn w1_shutdown_returns_promptly_on_persistent_wal_io_error() {
    let (hook, _storage, ingest) = group_commit_fixture();
    ingest.set_buffer_limits_for_test(64 * 1024, 64 * 1024, 5, 128);
    hook.fail_wal_puts.store(true, Ordering::SeqCst);

    let b1 = WireBatch::new(vec![make_event(1, "s1", "u1", 1_762_000_000_000)])
        .encode_binary(1)
        .unwrap();
    let r1 = ingest.ingest_raw("pk_test", None, &b1).await.unwrap();
    assert_eq!(r1.accepted, 1);

    // Wait until the background flusher trips sticky `wal_error`.
    let mut tripped = false;
    for i in 2..20u8 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let bi = WireBatch::new(vec![make_event(
            i,
            "s1",
            &format!("u{i}"),
            1_762_000_001_000,
        )])
        .encode_binary(1)
        .unwrap();
        if ingest.ingest_raw("pk_test", None, &bi).await.is_err() {
            tripped = true;
            break;
        }
    }
    assert!(tripped, "expected sticky wal_error to trip");
    assert!(ingest.buffer_stats().buffer_batches >= 1);

    // Shutdown MUST return promptly without spinning indefinitely in `project_flusher_loop`.
    tokio::time::timeout(Duration::from_secs(2), ingest.shutdown())
        .await
        .expect("IngestService::shutdown hung on persistent WAL error");

    let stats = ingest.buffer_stats();
    assert_eq!(stats.buffer_bytes, 0);
    assert_eq!(stats.buffer_events, 0);
    assert_eq!(stats.buffer_batches, 0);
}

#[tokio::test]
async fn w1_shutdown_returns_promptly_when_writer_fenced() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    let leases = Arc::new(ToggleLeases {
        ttl: Duration::from_millis(90),
        available: AtomicBool::new(true),
        fenced_away: AtomicBool::new(false),
        generation: 1,
    });
    storage.set_writer_leases(leases.clone());
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_test", "proj_fence_sd");
    let ingest = IngestService::new(storage.clone(), gov);
    ingest.set_buffer_limits_for_test(64 * 1024, 64 * 1024, 5, 128);

    // Open project while lease is held.
    let b1 = WireBatch::new(vec![make_event(1, "s1", "u1", 1_762_000_000_000)])
        .encode_binary(1)
        .unwrap();
    ingest.ingest_raw("pk_test", None, &b1).await.unwrap();
    ingest.flush_all().await.unwrap();

    // Queue another batch with slow flush_ms, then fence the writer before the flusher runs.
    ingest.set_buffer_limits_for_test(64 * 1024, 64 * 1024, 500, 64 * 1024);
    let b2 = WireBatch::new(vec![make_event(2, "s1", "u2", 1_762_000_001_000)])
        .encode_binary(1)
        .unwrap();
    ingest.ingest_raw("pk_test", None, &b2).await.unwrap();

    leases.fenced_away.store(true, Ordering::SeqCst);
    storage.renew_writer_leases().await;
    assert!(ingest.buffer_stats().buffer_batches >= 1);

    tokio::time::timeout(Duration::from_secs(2), ingest.shutdown())
        .await
        .expect("IngestService::shutdown hung on fenced NotWriter");

    let stats = ingest.buffer_stats();
    assert_eq!(stats.buffer_bytes, 0);
    assert_eq!(stats.buffer_events, 0);
    assert_eq!(stats.buffer_batches, 0);
}

#[tokio::test]
async fn w1_transient_not_writer_recovers_after_lease_renewal() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::build_ingest_router;
    use tower::ServiceExt;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    let leases = Arc::new(ToggleLeases {
        ttl: Duration::from_millis(90), // local_lease_validity = 60ms
        available: AtomicBool::new(true),
        fenced_away: AtomicBool::new(false),
        generation: 1,
    });
    storage.set_writer_leases(leases.clone());

    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_test", "proj_lease_recover");
    let ingest = IngestService::new(storage.clone(), gov);
    ingest.set_buffer_limits_for_test(64 * 1024, 64 * 1024, 5, 128);
    let app = build_ingest_router(ingest.clone());

    // 1) Initial batch succeeds while lease is valid.
    let b1 = WireBatch::new(vec![make_event(1, "s1", "u1", 1_762_000_000_000)])
        .encode_binary(1)
        .unwrap();
    let r1 = app
        .clone()
        .oneshot(
            Request::post("/v1/ingest")
                .header("x-pulse-key", "pk_test")
                .body(Body::from(b1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    ingest.flush_all().await.unwrap();

    // 2) Simulate transient MetaStore outage exceeding local_lease_validity (60ms) without fencing.
    leases.available.store(false, Ordering::SeqCst);
    storage.renew_writer_leases().await;
    tokio::time::sleep(Duration::from_millis(90)).await;

    // Queue a batch and poll until the flusher hits `NotWriter` and trips sticky `wal_error` (503).
    let mut got_503_not_writer = false;
    for i in 2..20u8 {
        let bi = WireBatch::new(vec![make_event(
            i,
            "s1",
            &format!("u{i}"),
            1_762_000_001_000,
        )])
        .encode_binary(1)
        .unwrap();
        let resp = app
            .clone()
            .oneshot(
                Request::post("/v1/ingest")
                    .header("x-pulse-key", "pk_test")
                    .body(Body::from(bi))
                    .unwrap(),
            )
            .await
            .unwrap();
        if resp.status() == StatusCode::SERVICE_UNAVAILABLE {
            let body_bytes = axum::body::to_bytes(resp.into_body(), 16 * 1024)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
            assert_eq!(v["code"], "not_writer");
            got_503_not_writer = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(
        got_503_not_writer,
        "expected 503 not_writer once local lease validity lapsed"
    );
    let buffered_during_outage = ingest.buffer_stats().buffer_events;
    assert!(buffered_during_outage >= 1);

    // 3) MetaStore recovers and renews the lease on the same ProjectStorage.
    leases.available.store(true, Ordering::SeqCst);
    storage.renew_writer_leases().await;

    // Background flusher must retry with backoff, drain buffered slices, and clear `wal_error`.
    let mut drained = false;
    for _ in 0..80 {
        if ingest.buffer_stats().buffer_bytes == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        drained,
        "transient NotWriter wedged flusher after lease renewal"
    );

    // 4) Subsequent /v1/ingest resumes 200 OK (no permanent 503 wedge).
    let b_after = WireBatch::new(vec![make_event(99, "s1", "u_after", 1_762_000_002_000)])
        .encode_binary(1)
        .unwrap();
    let r_after = app
        .oneshot(
            Request::post("/v1/ingest")
                .header("x-pulse-key", "pk_test")
                .body(Body::from(b_after))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r_after.status(), StatusCode::OK);

    ingest.flush_all().await.unwrap();
    let ps = storage.open_for_read("proj_lease_recover").await.unwrap();
    assert_eq!(ps.total_l0_rows(), 1 + buffered_during_outage + 1);

    tokio::time::timeout(Duration::from_secs(2), ingest.shutdown())
        .await
        .expect("IngestService::shutdown hung after lease recovery");
}

#[tokio::test]
async fn w2_engage_returns_canonical_uid_and_flushes_identity_before_apply_engage() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::{
        build_ingest_router, DiscoveredEventSchema, EngageResponse, GovernanceProvider,
        IngestError, ProjectLexiconRules,
    };
    use tower::ServiceExt;

    struct VerifyingGovernance {
        store: Arc<dyn ObjectStore>,
        verified_durable: AtomicUsize,
    }

    #[async_trait]
    impl GovernanceProvider for VerifyingGovernance {
        async fn authenticate_api_key(&self, api_key: &str) -> Option<String> {
            (api_key == "pk_w2").then(|| "proj_w2".to_string())
        }

        fn lexicon_rules(&self, _project_id: &str) -> Arc<ProjectLexiconRules> {
            Arc::new(ProjectLexiconRules::default())
        }

        fn record_discovery(&self, _items: Vec<DiscoveredEventSchema>) {}

        async fn apply_engage(
            &self,
            project_id: &str,
            distinct_id: &str,
            canonical_uid: u32,
            _ops: &serde_json::Value,
        ) -> pulse_ingest::Result<u32> {
            // Re-open identity graph read-only from the backing store to verify `id_graph.flush()`
            // already made the mapping durable BEFORE `apply_engage` ran.
            let ro = pulse_identity::ProjectIdentityGraph::open_read_only(
                project_id,
                self.store.clone(),
            )
            .await
            .map_err(|e| IngestError::Governance(e.to_string()))?;
            let persisted = ro.lookup_canonical(distinct_id);
            ro.close()
                .await
                .map_err(|e| IngestError::Governance(e.to_string()))?;
            if persisted != Some(canonical_uid) {
                return Err(IngestError::Governance(format!(
                    "expected durable identity {canonical_uid} before apply_engage, got {persisted:?}"
                )));
            }
            self.verified_durable.fetch_add(1, Ordering::SeqCst);
            Ok(canonical_uid)
        }
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store.clone(),
        id_router,
    );
    let gov = Arc::new(VerifyingGovernance {
        store: store.clone(),
        verified_durable: AtomicUsize::new(0),
    });
    let ingest = IngestService::new(storage.clone(), gov.clone());
    let app = build_ingest_router(ingest.clone());

    // Single profile object -> returns both `canonical_uid` and `canonical_uids: [uid]`.
    let resp1 = app
        .clone()
        .oneshot(
            Request::post("/v1/engage")
                .header("x-pulse-key", "pk_w2")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"distinct_id": "u_single", "$set": {"plan": "pro"}})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let b1 = axum::body::to_bytes(resp1.into_body(), 64 * 1024)
        .await
        .unwrap();
    let e1: EngageResponse = serde_json::from_slice(&b1).unwrap();
    assert_eq!(e1.status, "ok");
    assert_eq!(e1.updated, 1);
    let uid1 = e1
        .canonical_uid
        .expect("single /v1/engage must return canonical_uid");
    assert_eq!(e1.canonical_uids, vec![uid1]);

    // Same-user batch -> shares one UID, so `canonical_uid` is Some(uid1) and `canonical_uids` has [uid1, uid1].
    let resp_same = app
        .clone()
        .oneshot(
            Request::post("/v1/engage")
                .header("x-pulse-key", "pk_w2")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!([
                        {"distinct_id": "u_single", "$set": {"a": 1}},
                        {"distinct_id": "u_single", "$set": {"b": 2}}
                    ])
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_same.status(), StatusCode::OK);
    let b_same = axum::body::to_bytes(resp_same.into_body(), 64 * 1024)
        .await
        .unwrap();
    let e_same: EngageResponse = serde_json::from_slice(&b_same).unwrap();
    assert_eq!(e_same.updated, 2);
    assert_eq!(e_same.canonical_uid, Some(uid1));
    assert_eq!(e_same.canonical_uids, vec![uid1, uid1]);

    // Multi-user batch -> omits `canonical_uid`, returns `canonical_uids: [uid_a, uid_b]`.
    let resp_multi = app
        .oneshot(
            Request::post("/v1/engage")
                .header("x-pulse-key", "pk_w2")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "profiles": [
                            {"distinct_id": "u_multi_a", "$set": {"tier": "a"}},
                            {"distinct_id": "u_multi_b", "$set": {"tier": "b"}}
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_multi.status(), StatusCode::OK);
    let b_multi = axum::body::to_bytes(resp_multi.into_body(), 64 * 1024)
        .await
        .unwrap();
    let v_multi: serde_json::Value = serde_json::from_slice(&b_multi).unwrap();
    assert_eq!(v_multi["updated"], 2);
    assert!(v_multi.get("canonical_uid").is_none());
    let uids = v_multi["canonical_uids"].as_array().unwrap();
    assert_eq!(uids.len(), 2);
    assert_ne!(uids[0], uids[1]);
    assert_eq!(gov.verified_durable.load(Ordering::SeqCst), 5);

    ingest.shutdown().await;
}

#[tokio::test]
async fn w2_governance_errors_propagate_across_http_and_mixed_batches() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::{
        build_ingest_router, DiscoveredEventSchema, GovernanceProvider, IngestError,
        ProjectLexiconRules,
    };
    use pulse_storage::StorageError;
    use tower::ServiceExt;

    #[derive(Clone, Copy)]
    enum Mode {
        Conflict,
        Unavailable,
        NotWriter,
        Internal,
    }

    struct FailingGovernance {
        mode: parking_lot::Mutex<Mode>,
    }

    impl FailingGovernance {
        fn make_err(&self) -> IngestError {
            match *self.mode.lock() {
                Mode::Conflict => IngestError::Conflict("meta conflict".into()),
                Mode::Unavailable => {
                    IngestError::Storage(StorageError::Unavailable("profile slatedb down".into()))
                }
                Mode::NotWriter => IngestError::Storage(StorageError::NotWriter {
                    project_id: "proj_fail".into(),
                }),
                Mode::Internal => IngestError::Governance("db syntax boom".into()),
            }
        }
    }

    #[async_trait]
    impl GovernanceProvider for FailingGovernance {
        async fn authenticate_api_key(&self, api_key: &str) -> Option<String> {
            (api_key == "pk_fail").then(|| "proj_fail".to_string())
        }

        fn lexicon_rules(&self, _project_id: &str) -> Arc<ProjectLexiconRules> {
            Arc::new(ProjectLexiconRules::default())
        }

        fn record_discovery(&self, _items: Vec<DiscoveredEventSchema>) {}

        async fn apply_engage(
            &self,
            _project_id: &str,
            _distinct_id: &str,
            _canonical_uid: u32,
            _ops: &serde_json::Value,
        ) -> pulse_ingest::Result<u32> {
            Err(self.make_err())
        }

        async fn on_identify(
            &self,
            _project_id: &str,
            _anon_id: &str,
            _user_id: &str,
            _canonical_uid: u32,
        ) -> pulse_ingest::Result<u32> {
            Err(self.make_err())
        }

        async fn apply_group_engage(
            &self,
            _project_id: &str,
            _group_type: &str,
            _group_id: &str,
            _ops: &serde_json::Value,
        ) -> pulse_ingest::Result<()> {
            Err(self.make_err())
        }
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    let gov = Arc::new(FailingGovernance {
        mode: parking_lot::Mutex::new(Mode::Unavailable),
    });
    let ingest = IngestService::new(storage.clone(), gov.clone());
    let app = build_ingest_router(ingest.clone());

    for (mode, expected_status) in [
        (Mode::Conflict, StatusCode::CONFLICT),
        (Mode::Unavailable, StatusCode::SERVICE_UNAVAILABLE),
        (Mode::NotWriter, StatusCode::SERVICE_UNAVAILABLE),
        (Mode::Internal, StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        *gov.mode.lock() = mode;

        let r_eng = app
            .clone()
            .oneshot(
                Request::post("/v1/engage")
                    .header("x-pulse-key", "pk_fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"distinct_id": "u1", "$set": {"a": 1}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            r_eng.status(),
            expected_status,
            "/v1/engage status mismatch"
        );

        let r_id = app
            .clone()
            .oneshot(
                Request::post("/v1/identify")
                    .header("x-pulse-key", "pk_fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"anon_id": "a1", "user_id": "u1"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            r_id.status(),
            expected_status,
            "/v1/identify status mismatch"
        );

        let r_grp = app
            .clone()
            .oneshot(
                Request::post("/v1/groups")
                    .header("x-pulse-key", "pk_fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"group_type": "org", "group_id": "g1", "$set": {"plan": "ent"}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            r_grp.status(),
            expected_status,
            "/v1/groups status mismatch"
        );

        let r_mix = app
            .clone()
            .oneshot(
                Request::post("/v1/ingest")
                    .header("x-pulse-key", "pk_fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "batch": [
                                {"type": "track", "userId": "u1", "event": "Paid", "messageId": "m_fail"},
                                {"type": "identify", "userId": "u1", "anonymousId": "a_mix", "traits": {"plan": "pro"}}
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            r_mix.status(),
            expected_status,
            "mixed /v1/ingest status mismatch"
        );
        assert_eq!(ingest.buffer_stats().buffer_bytes, 0);
        assert_eq!(ingest.buffer_stats().buffer_events, 0);
    }

    ingest.flush_all().await.unwrap();
    let ps = storage.open_for_read("proj_fail").await.unwrap();
    assert_eq!(ps.total_l0_rows(), 0, "failing mixed batches queued events");
    ingest.shutdown().await;
}

#[tokio::test]
async fn w2_replay_chunk_http_routes_return_offset_400_and_503() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use pulse_ingest::{build_ingest_router, ReplayIngestResponse};
    use tower::ServiceExt;

    #[derive(Debug)]
    struct ToggleReplayStore {
        inner: InMemory,
        fail_replay: AtomicBool,
    }
    impl Display for ToggleReplayStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "ToggleReplayStore")
        }
    }
    #[async_trait]
    impl ObjectStore for ToggleReplayStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            if self.fail_replay.load(Ordering::SeqCst) && location.as_ref().starts_with("replays/")
            {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: Box::new(std::io::Error::other("replay disk full")),
                });
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
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[Range<u64>],
        ) -> OsResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
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

    let store = Arc::new(ToggleReplayStore {
        inner: InMemory::new(),
        fail_replay: AtomicBool::new(false),
    });
    let id_router = Arc::new(IdentityRouter::new(store.clone()));
    let storage = StorageEngine::new(
        StorageConfig {
            num_buckets: 4,
            l0_flush_row_threshold: 1000,
            l0_flush_max_age: Duration::from_secs(60),
            compaction_min_segments: 4,
        },
        store.clone(),
        id_router,
    );
    let gov = InMemoryGovernance::new();
    gov.register_api_key("pk_rep", "proj_rep");
    let ingest = IngestService::new(storage, gov);
    let app = build_ingest_router(ingest.clone());

    let valid_chunk = ReplayChunk {
        project_id: String::new(),
        session_id: "sess_w2_rep".into(),
        chunk_start_ts_ms: 1_762_000_000_000i64,
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
                bg_rgba: 255,
                masked_label: "Home".into(),
            }],
        }],
    };
    let valid_chunk_json = serde_json::to_vec(&valid_chunk).unwrap();
    let ok_resp = app
        .clone()
        .oneshot(
            Request::post("/v1/replay/chunk")
                .header("x-pulse-key", "pk_rep")
                .header("content-type", "application/json")
                .body(Body::from(valid_chunk_json.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_resp.status(), StatusCode::OK);
    let ok_bytes = axum::body::to_bytes(ok_resp.into_body(), 64 * 1024)
        .await
        .unwrap();
    let rep: ReplayIngestResponse = serde_json::from_slice(&ok_bytes).unwrap();
    assert_eq!(rep.status, "ok");
    assert!(rep.chunk_bytes > 0);
    assert_eq!(rep.byte_offset, 0);
    assert_eq!(rep.replay_offset_kb, 0);

    // Invalid session_id -> 400 BAD_REQUEST
    let bad_chunk = serde_json::json!({
        "project_id": "",
        "session_id": "../escape",
        "chunk_start_ts_ms": 1_762_000_000_000i64,
        "ops": []
    });
    let bad_resp = app
        .clone()
        .oneshot(
            Request::post("/v1/replay/chunk")
                .header("x-pulse-key", "pk_rep")
                .header("content-type", "application/json")
                .body(Body::from(bad_chunk.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);

    // Failing object store -> 503 SERVICE_UNAVAILABLE
    store.fail_replay.store(true, Ordering::SeqCst);
    let err_resp = app
        .oneshot(
            Request::post("/v1/replay/chunk")
                .header("x-pulse-key", "pk_rep")
                .header("content-type", "application/json")
                .body(Body::from(valid_chunk_json))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(err_resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    ingest.shutdown().await;
}

#[tokio::test]
async fn w2_quic_error_codes_for_identify_and_replay() {
    use pulse_ingest::{
        spawn_quic_listener, DiscoveredEventSchema, GovernanceProvider, IngestError,
        ProjectLexiconRules, QUIC_OP_IDENTIFY, QUIC_OP_REPLAY,
    };
    use pulse_storage::StorageError;

    struct QuicErrGov {
        err_kind: parking_lot::Mutex<u8>,
    }

    #[async_trait]
    impl GovernanceProvider for QuicErrGov {
        async fn authenticate_api_key(&self, api_key: &str) -> Option<String> {
            (api_key == "pk_q").then(|| "proj_q".to_string())
        }
        fn lexicon_rules(&self, _project_id: &str) -> Arc<ProjectLexiconRules> {
            Arc::new(ProjectLexiconRules::default())
        }
        fn record_discovery(&self, _items: Vec<DiscoveredEventSchema>) {}
        async fn on_identify(
            &self,
            _project_id: &str,
            _anon_id: &str,
            _user_id: &str,
            _canonical_uid: u32,
        ) -> pulse_ingest::Result<u32> {
            match *self.err_kind.lock() {
                0 => Err(IngestError::Conflict("merge conflict".into())),
                1 => Err(IngestError::Storage(StorageError::Unavailable(
                    "down".into(),
                ))),
                _ => Err(IngestError::Storage(StorageError::NotWriter {
                    project_id: "proj_q".into(),
                })),
            }
        }
    }

    let _ = rustls::crypto::ring::default_provider().install_default();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    let gov = Arc::new(QuicErrGov {
        err_kind: parking_lot::Mutex::new(0),
    });
    let ingest = IngestService::new(storage, gov.clone());
    let quic = spawn_quic_listener("127.0.0.1:0".parse().unwrap(), ingest.clone()).unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(
            quic.cert_der.as_slice(),
        ))
        .unwrap();
    let client_cfg = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_cfg);
    let conn = endpoint
        .connect(quic.local_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    async fn send_op(
        conn: &quinn::Connection,
        op: u8,
        key: &str,
        body: &[u8],
    ) -> serde_json::Value {
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        let mut hdr = Vec::with_capacity(7 + key.len());
        hdr.push(op);
        hdr.extend_from_slice(&(key.len() as u16).to_le_bytes());
        hdr.extend_from_slice(key.as_bytes());
        hdr.extend_from_slice(&(body.len() as u32).to_le_bytes());
        send.write_all(&hdr).await.unwrap();
        send.write_all(body).await.unwrap();
        send.finish().unwrap();
        let resp = recv.read_to_end(64 * 1024).await.unwrap();
        serde_json::from_slice(&resp).unwrap()
    }

    let id_body = serde_json::json!({"anon_id": "a1", "user_id": "u1"}).to_string();
    *gov.err_kind.lock() = 0;
    let r0 = send_op(&conn, QUIC_OP_IDENTIFY, "pk_q", id_body.as_bytes()).await;
    assert_eq!(r0["code"], "conflict", "{r0}");

    *gov.err_kind.lock() = 1;
    let r1 = send_op(&conn, QUIC_OP_IDENTIFY, "pk_q", id_body.as_bytes()).await;
    assert_eq!(r1["code"], "unavailable", "{r1}");

    *gov.err_kind.lock() = 2;
    let r2 = send_op(&conn, QUIC_OP_IDENTIFY, "pk_q", id_body.as_bytes()).await;
    assert_eq!(r2["code"], "not_writer", "{r2}");

    // Also test QUIC_OP_REPLAY returns JSON cleanly (or error without code on invalid JSON).
    let r_rep = send_op(&conn, QUIC_OP_REPLAY, "pk_q", b"not-json").await;
    assert_eq!(r_rep["status"], "error", "{r_rep}");

    drop(conn);
    endpoint.wait_idle().await;
    ingest.shutdown().await;
}
