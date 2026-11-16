use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use pulse_identity::ProjectIdentityGraph;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};

#[derive(Debug)]
struct FailWriteStore {
    inner: InMemory,
    fail_puts: AtomicBool,
}

impl FailWriteStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemory::new(),
            fail_puts: AtomicBool::new(false),
        })
    }
}

impl Display for FailWriteStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailWriteStore")
    }
}

#[async_trait]
impl ObjectStore for FailWriteStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if self.fail_puts.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "FailWriteStore",
                source: Box::new(std::io::Error::other("injected object_store put failure")),
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

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
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

#[tokio::test]
async fn p1_08_alias_refuses_to_merge_two_already_identified_users() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let graph = ProjectIdentityGraph::open("proj_p1_08", store)
        .await
        .unwrap();

    // Shared device: anon_kiosk logs in as Alice
    graph
        .alias_durable("anon_kiosk", "user_alice", 1, 1)
        .await
        .expect("first identify should succeed");
    assert_eq!(
        graph.lookup_canonical("anon_kiosk"),
        graph.lookup_canonical("user_alice")
    );

    // Later on the same kiosk without SDK reset(), Bob logs in — MUST NOT merge Alice into Bob!
    let _ = graph.alias_durable("anon_kiosk", "user_bob", 1, 2).await;
    let _ = graph.get_or_intern("user_bob", 2);

    assert_ne!(
        graph.lookup_canonical("user_alice"),
        graph.lookup_canonical("user_bob"),
        "Alice and Bob are distinct identified users and must NEVER be merged together!"
    );

    // Direct attacker call `/v1/identify {anon_id: "user_alice", user_id: "user_mallory"}` must also fail to merge
    let _ = graph
        .alias_durable("user_alice", "user_mallory", 1, 3)
        .await;
    let _ = graph.get_or_intern("user_mallory", 3);
    assert_ne!(
        graph.lookup_canonical("user_alice"),
        graph.lookup_canonical("user_mallory"),
        "already-identified user_alice must not be hijacked into user_mallory"
    );

    graph.close().await.unwrap();
}

#[tokio::test]
async fn p1_09_flush_failure_preserves_mutations_until_retry_succeeds() {
    let fail_store = FailWriteStore::new();
    let store: Arc<dyn ObjectStore> = fail_store.clone();

    {
        let graph = ProjectIdentityGraph::open("proj_p1_09", store.clone())
            .await
            .unwrap();
        for i in 0..40u32 {
            graph.get_or_intern(&format!("u_{i}"), (i % 8) as u8);
        }

        // Inject object-store write failure during flush
        fail_store.fail_puts.store(true, Ordering::SeqCst);
        let err_res = graph.flush().await;
        assert!(
            err_res.is_err(),
            "expected flush to fail while fail_puts=true"
        );

        // Restore object-store writes and retry flush + close
        fail_store.fail_puts.store(false, Ordering::SeqCst);
        graph
            .close()
            .await
            .expect("retry flush on close should succeed");
    }

    // Reopen from SlateDB: all 40 interned IDs must have survived!
    let reopened = ProjectIdentityGraph::open("proj_p1_09", store)
        .await
        .unwrap();
    assert_eq!(
        reopened.user_count(),
        40,
        "flush error dropped pending identity mutations!"
    );
    for i in 0..40u32 {
        assert_eq!(reopened.lookup_raw(&format!("u_{i}")), Some(i));
    }
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn p1_11_missing_chunk_or_dangling_alias_fails_loudly_on_replay() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Write a metadata header claiming `chunk_count = 1` with NO `log/chunk_000000000000` present
    {
        let db = Db::open(
            ObjectPath::from("identity/proj_missing_chunk"),
            store.clone(),
        )
        .await
        .unwrap();
        let mut wb = WriteBatch::new();
        wb.put(b"meta/chunk_count", 1u64.to_le_bytes());
        db.write_with_options(
            wb,
            &WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    let missing_res = ProjectIdentityGraph::open("proj_missing_chunk", store.clone()).await;
    assert!(
        missing_res.is_err(),
        "replay must fail loudly when a mutation log chunk is missing instead of silently skipping"
    );

    // Write a chunk containing a dangling Alias referencing uid 99 before any Intern
    {
        let db = Db::open(
            ObjectPath::from("identity/proj_dangling_alias"),
            store.clone(),
        )
        .await
        .unwrap();
        let mut bad_chunk = Vec::new();
        bad_chunk.extend_from_slice(&1u32.to_le_bytes()); // 1 entry
        bad_chunk.push(3u8); // OP_ALIAS
        bad_chunk.extend_from_slice(&99u32.to_le_bytes());
        bad_chunk.extend_from_slice(&100u32.to_le_bytes());
        bad_chunk.push(0u8);
        bad_chunk.push(0u8);

        let mut wb = WriteBatch::new();
        wb.put(b"log/chunk_000000000000", &bad_chunk);
        wb.put(b"meta/chunk_count", 1u64.to_le_bytes());
        db.write_with_options(
            wb,
            &WriteOptions {
                await_durable: true,
            },
        )
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    let dangling_res = ProjectIdentityGraph::open("proj_dangling_alias", store).await;
    assert!(
        dangling_res.is_err(),
        "replay must fail loudly on out-of-order / dangling Alias record"
    );
}
