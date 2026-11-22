//! M2b integration tests for `pulse-node restore --from-published`, SQLite startup warning,
//! erasure reconciliation, `uid >= HWM` tombstone deletion, `deletion_audit_log` retention,
//! `HighWaterMarkViolation` refusal, and post-restore restart + ingest.
//!
//! Own test binary: mutates process-global `PULSE_HOT_STORE`, `PULSE_BYOK_MASTER_KEY`,
//! `PULSE_WRITER_LEASE_TTL_SECS`, and `DATABASE_URL`.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use pulse_control::{DeletionAuditRecord, MetaStore};
use pulse_node::{
    restore_from_published_with_store_and_meta_for_test, start_pulse_node_with_store, NodeConfig,
    NodeRole, RunningNode,
};
use pulse_storage::{PublishedManifestStore, StoredUserProfile};
use serde_json::{json, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

const ADMIN: &str = "sk_root_restore_test_secret_00000000000000000";
const BYOK_HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

static ENV: Mutex<()> = Mutex::new(());

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
        let mut saved = Vec::with_capacity(pairs.len());
        for &(k, v) in pairs {
            saved.push((k, std::env::var(k).ok()));
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, prev) in self.saved.drain(..).rev() {
            match prev {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }
}

#[derive(Clone, Default)]
struct WarnCapture {
    messages: Arc<Mutex<Vec<String>>>,
}

struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }
}

impl<S: Subscriber> Layer<S> for WarnCapture {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() <= tracing::Level::WARN {
            let mut v = MessageVisitor(String::new());
            event.record(&mut v);
            if !v.0.is_empty() {
                self.messages.lock().unwrap().push(v.0);
            }
        }
    }
}

fn config(data_dir: &Path, project_id: &str) -> NodeConfig {
    NodeConfig {
        role: NodeRole::All,
        http_addr: "127.0.0.1:0".parse().unwrap(),
        quic_addr: None,
        data_dir: data_dir.to_path_buf(),
        web_dist_dir: data_dir.join("no_web"),
        default_project_id: project_id.to_string(),
        default_api_key: String::new(),
        admin_token: Some(ADMIN.to_string()),
        allowed_origins: vec![],
        auto_seed: false,
        num_buckets: 4,
    }
}

async fn post_ingest(node: &RunningNode, key: &str, path: &str, body: Value) {
    let status = reqwest::Client::new()
        .post(format!("http://{}{path}", node.http_addr))
        .header("x-pulse-key", key)
        .json(&body)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status.as_u16(), 200, "{path}");
}

#[test]
fn cli_restore_refuses_sqlite_when_database_url_unset_or_sqlite() {
    let bin = env!("CARGO_BIN_EXE_pulse-node");

    let help = Command::new(bin).arg("--help").output().unwrap();
    assert!(help.status.success());
    let help_out = String::from_utf8_lossy(&help.stdout);
    assert!(
        help_out.contains("restore --from-published"),
        "help missing restore subcommand: {help_out}"
    );

    // 1. Unset DATABASE_URL -> refuses before touching data_dir or creating pulse_meta.db.
    let tmp1 = tempfile::tempdir().unwrap();
    let data1 = tmp1.path().join("wiped_node");
    let out1 = Command::new(bin)
        .args(["restore", "--from-published", "--data-dir"])
        .arg(&data1)
        .env_remove("DATABASE_URL")
        .output()
        .unwrap();
    assert!(!out1.status.success(), "must fail when DATABASE_URL unset");
    let err1 = format!(
        "{}{}",
        String::from_utf8_lossy(&out1.stdout),
        String::from_utf8_lossy(&out1.stderr)
    );
    assert!(err1.contains("DATABASE_URL"), "{err1}");
    assert!(err1.contains("SQLite"), "{err1}");
    assert!(
        !data1.join("pulse_meta.db").exists(),
        "must not create pulse_meta.db on refusal"
    );

    // 2. Explicit sqlite:// DATABASE_URL -> refuses before touching data_dir.
    let tmp2 = tempfile::tempdir().unwrap();
    let data2 = tmp2.path().join("wiped_node2");
    let sqlite_file = tmp2.path().join("explicit.db");
    let out2 = Command::new(bin)
        .args(["restore", "--from-published", "--data-dir"])
        .arg(&data2)
        .env(
            "DATABASE_URL",
            format!("sqlite://{}", sqlite_file.display()),
        )
        .output()
        .unwrap();
    assert!(
        !out2.status.success(),
        "must fail on sqlite:// DATABASE_URL"
    );
    let err2 = format!(
        "{}{}",
        String::from_utf8_lossy(&out2.stdout),
        String::from_utf8_lossy(&out2.stderr)
    );
    assert!(err2.contains("DATABASE_URL"), "{err2}");
    assert!(err2.contains("SQLite"), "{err2}");
    assert!(!data2.join("pulse_meta.db").exists());
    assert!(!sqlite_file.exists());
}

#[test]
fn startup_emits_sqlite_warning_under_local_and_silent_under_object() {
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("3")),
        ("DATABASE_URL", None),
    ]);
    let _lock = ENV.lock().unwrap_or_else(|e| e.into_inner());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // local + SQLite -> emits prominent warning.
        let tmp_local = tempfile::tempdir().unwrap();
        let store_local = Arc::new(object_store::memory::InMemory::new());
        let cap_local = WarnCapture::default();
        let sub_local = tracing_subscriber::registry().with(cap_local.clone());
        let _guard = tracing::subscriber::set_default(sub_local);
        let node_local =
            start_pulse_node_with_store(config(tmp_local.path(), "proj_warn_l"), store_local)
                .await
                .unwrap();
        node_local.shutdown().await;
        drop(_guard);
        let warns_local = cap_local.messages.lock().unwrap().clone();
        assert!(
            warns_local.iter().any(|m| {
                m.contains("PULSE_HOT_STORE=local")
                    && m.contains("SQLite")
                    && m.contains("DATABASE_URL")
                    && m.contains("restore --from-published")
            }),
            "expected SQLite startup warning under local mode, got: {warns_local:?}"
        );

        // object + SQLite -> no local-mode SQLite warning.
        std::env::set_var("PULSE_HOT_STORE", "object");
        let tmp_obj = tempfile::tempdir().unwrap();
        let store_obj = Arc::new(object_store::memory::InMemory::new());
        let cap_obj = WarnCapture::default();
        let sub_obj = tracing_subscriber::registry().with(cap_obj.clone());
        let _guard2 = tracing::subscriber::set_default(sub_obj);
        let node_obj =
            start_pulse_node_with_store(config(tmp_obj.path(), "proj_warn_o"), store_obj)
                .await
                .unwrap();
        node_obj.shutdown().await;
        drop(_guard2);
        let warns_obj = cap_obj.messages.lock().unwrap().clone();
        assert!(
            !warns_obj
                .iter()
                .any(|m| m.contains("PULSE_HOT_STORE=local") && m.contains("SQLite")),
            "did not expect local-mode SQLite warning under object mode: {warns_obj:?}"
        );
    });
}

#[test]
fn full_restore_from_published_with_byok_erasure_reconciliation_hwm_and_restart() {
    let _lock = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvGuard::set(&[
        ("PULSE_HOT_STORE", Some("local")),
        ("PULSE_BYOK_MASTER_KEY", Some(BYOK_HEX)),
        ("PULSE_WRITER_LEASE_TTL_SECS", Some("3")),
        ("DATABASE_URL", None),
    ]);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let s3_dir = tempfile::tempdir().unwrap();
        let s3_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(s3_dir.path()).unwrap());

        // External MetaStore living outside <data_dir> so it survives local volume loss.
        let ext_meta_dir = tempfile::tempdir().unwrap();
        let ext_db_url = format!("sqlite://{}", ext_meta_dir.path().join("pulse_meta.db").display());
        std::env::set_var("DATABASE_URL", &ext_db_url);
        let ext_meta = MetaStore::connect_auto(ext_meta_dir.path()).await.unwrap();

        let data1_dir = tempfile::tempdir().unwrap();
        let node1 = start_pulse_node_with_store(config(data1_dir.path(), "proj_a"), s3_store.clone())
            .await
            .unwrap();
        let key_a = node1
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        let now_ms = chrono::Utc::now().timestamp_millis();
        // Pre-publish users:
        // - anon_0 (uid 0) + user_0 (uid 1) merged via identify, revenue 250
        // - user_keep (uid 2), revenue 40
        // - user_resign (uid 3), erased at (now_ms - 5000) then re-updated at (now_ms + 5000) before publish
        post_ingest(
            &node1,
            &key_a,
            "/v1/ingest",
            json!({
                "events": [
                    { "event": "App_Open", "distinct_id": "anon_0", "timestamp": now_ms - 10_000 },
                    { "event": "App_Open", "distinct_id": "user_0", "timestamp": now_ms - 9_000 },
                    { "event": "App_Open", "distinct_id": "user_keep", "timestamp": now_ms - 8_000 },
                    { "event": "App_Open", "distinct_id": "user_resign", "timestamp": now_ms - 7_000 }
                ]
            }),
        )
        .await;
        post_ingest(
            &node1,
            &key_a,
            "/v1/identify",
            json!({ "anon_id": "anon_0", "user_id": "user_0" }),
        )
        .await;
        post_ingest(
            &node1,
            &key_a,
            "/v1/engage",
            json!([
                { "$distinct_id": "anon_0", "$set": { "plan": "gold" }, "$track_charge": { "$amount": 100.0 } },
                { "$distinct_id": "user_0", "$set": { "tier": "pro" }, "$track_charge": { "$amount": 150.0 } },
                { "$distinct_id": "user_keep", "$set": { "plan": "silver" }, "$track_charge": { "$amount": 40.0 } }
            ]),
        )
        .await;

        // Upsert user_resign with a timestamp strictly after `t_erase_old` so it survives restore.
        let t_erase_old = now_ms;
        let ps1 = node1.storage.profile_store("proj_a", true).await.unwrap();
        let ig1 = node1.storage.identity_graph("proj_a", false).await.unwrap();
        let uid_resign = ig1.lookup_canonical("user_resign").unwrap();
        ps1.upsert(&StoredUserProfile {
            project_id: "proj_a".into(),
            distinct_id: "user_resign".into(),
            canonical_uid: uid_resign,
            props: BTreeMap::from([("plan".into(), json!("resigned_pro"))]),
            total_revenue: 60.0,
            updated_at_ms: t_erase_old + 5_000,
        })
        .await
        .unwrap();

        // Flush -> writes encrypted PLSEAGM2 snapshots & PublishedManifest with HWM = 4.
        node1.storage.flush_all_projects().await.unwrap();
        let pub_m = PublishedManifestStore::new(
            "proj_a",
            Arc::new(pulse_storage::EncryptedObjectStore::new(s3_store.clone()))
                .tap_byok(BYOK_HEX)
                .as_object_store(),
        )
        .load_latest()
        .await
        .unwrap()
        .expect("published manifest must exist");
        let hwm = pub_m.high_water_mark;
        assert_eq!(hwm, 4);

        node1.shutdown().await;

        // Simulate post-publish mutations recorded in external MetaStore before local disk loss:
        // 1. Pre-publish erasure of raw UID 0 (`anon_0`) at `t_erase_post` (covers merged `user_0` too).
        // 2. Old erasure of `user_resign` at `t_erase_old` (older than `updated_at_ms` so preserved).
        // 3. Post-publish erasure of UIDs `hwm` and `hwm + 2` (must be deleted by restore).
        // 4. `deletion_audit_log` entry (must remain untouched).
        let t_erase_post = t_erase_old + 10_000;
        ext_meta
            .insert_erased_uids("proj_a", &[0], t_erase_post)
            .await
            .unwrap();
        ext_meta
            .insert_erased_uids("proj_a", &[uid_resign], t_erase_old)
            .await
            .unwrap();
        ext_meta
            .insert_erased_uids("proj_a", &[hwm as u32, (hwm + 2) as u32], t_erase_post)
            .await
            .unwrap();
        ext_meta
            .insert_deletion_audit(&DeletionAuditRecord {
                id: "del_audit_keep".into(),
                project_id: "proj_a".into(),
                distinct_id_masked: "a***".into(),
                canonical_uid: 1,
                events_purged: 2,
                replays_purged: 0,
                requested_by: "admin".into(),
                deleted_at_ms: t_erase_post,
            })
            .await
            .unwrap();

        // Wiped <data_dir>: starting pulse-node without restore must REFUSE startup.
        let wiped_dir = tempfile::tempdir().unwrap();
        let err = start_pulse_node_with_store(config(wiped_dir.path(), "proj_a"), s3_store.clone())
            .await
            .err()
            .expect("wiped data_dir with S3 published/ must refuse startup");
        assert!(err.to_string().contains("restore --from-published"), "{err}");
        assert!(!wiped_dir.path().join("hot_store_mode").exists());

        // HighWaterMarkViolation check: publish a bogus manifest with HWM=999 on a copy store and
        // verify restore refuses and does NOT write hot_store_mode.
        {
            let bad_s3_dir = tempfile::tempdir().unwrap();
            let bad_s3: Arc<dyn ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(bad_s3_dir.path()).unwrap());
            let mut stream = s3_store.list(None);
            while let Some(meta) = futures::StreamExt::next(&mut stream).await {
                let m = meta.unwrap();
                let bytes = s3_store.get(&m.location).await.unwrap().bytes().await.unwrap();
                bad_s3.put(&m.location, bytes.into()).await.unwrap();
            }
            let bad_enc = Arc::new(pulse_storage::EncryptedObjectStore::new(bad_s3.clone()))
                .tap_byok(BYOK_HEX);
            let bad_pub_store = PublishedManifestStore::new("proj_a", bad_enc.as_object_store());
            let mut bad_pub = bad_pub_store.load_latest().await.unwrap().unwrap();
            bad_pub.publish_seq += 1;
            bad_pub.high_water_mark = 999;
            bad_pub_store.publish_next(bad_pub).await.unwrap();

            let hwm_fail_dir = tempfile::tempdir().unwrap();
            let hwm_err = restore_from_published_with_store_and_meta_for_test(
                hwm_fail_dir.path(),
                Some("proj_a"),
                bad_s3,
                &ext_meta,
            )
            .await
            .unwrap_err();
            assert!(
                hwm_err.to_string().contains("high-water mark")
                    || hwm_err.to_string().contains("HighWaterMark")
                    || hwm_err.to_string().contains("999"),
                "{hwm_err}"
            );
            assert!(!hwm_fail_dir.path().join("hot_store_mode").exists());
        }

        // Partial <data_dir> with stale local WAL: restore must clear stale wal/{project_id},
        // purge erased profiles, preserve re-signup user_resign, delete uid >= HWM tombstones,
        // keep uid < HWM tombstones and deletion_audit_log, and write hot_store_mode.
        let partial_dir = tempfile::tempdir().unwrap();
        let stale_wal_store =
            pulse_storage::DurableLocalObjectStore::new_at(&partial_dir.path().join("wal"))
                .unwrap();
        stale_wal_store
            .put(
                &ObjectPath::from("wal/proj_a/bucket_000/seq_0000000000000099.wal"),
                PutPayload::from_static(b"stale_wal_bytes"),
            )
            .await
            .unwrap();

        let reports = restore_from_published_with_store_and_meta_for_test(
            partial_dir.path(),
            None,
            s3_store.clone(),
            &ext_meta,
        )
        .await
        .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].project_id, "proj_a");
        assert_eq!(reports[0].high_water_mark, 4);
        assert_eq!(reports[0].identity_user_count, 4);
        assert_eq!(
            reports[0].erased_profiles_purged, 2,
            "must purge both anon_0 and merged user_0, keeping user_keep & user_resign"
        );
        assert_eq!(
            reports[0].erased_tombstones_deleted, 2,
            "must delete uid >= HWM (4 and 6)"
        );

        assert_eq!(
            std::fs::read_to_string(partial_dir.path().join("hot_store_mode")).unwrap(),
            "local\n"
        );
        assert!(
            futures::StreamExt::next(&mut stale_wal_store.list(Some(&ObjectPath::from("wal/proj_a"))))
                .await
                .is_none(),
            "stale local wal/proj_a must be cleared during restore"
        );

        let mut remaining_erased = ext_meta.list_erased_uids("proj_a").await.unwrap();
        remaining_erased.sort_by_key(|(u, _)| *u);
        assert_eq!(
            remaining_erased,
            vec![(0, t_erase_post), (uid_resign, t_erase_old)]
        );
        assert_eq!(ext_meta.list_deletion_audit("proj_a").await.unwrap().len(), 1);

        // Start Node 2 on the restored <data_dir> + external MetaStore.
        let node2 = start_pulse_node_with_store(config(partial_dir.path(), "proj_a"), s3_store)
            .await
            .unwrap();

        // Check restored profiles via HTTP API.
        let client = reqwest::Client::new();
        let users: Vec<Value> = client
            .get(format!("http://{}/v1/projects/proj_a/users", node2.http_addr))
            .bearer_auth(ADMIN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let dids: Vec<&str> = users
            .iter()
            .map(|u| u["distinct_id"].as_str().unwrap())
            .collect();
        assert!(!dids.contains(&"anon_0"), "{dids:?}");
        assert!(!dids.contains(&"user_0"), "{dids:?}");
        assert!(dids.contains(&"user_keep"), "{dids:?}");
        assert!(dids.contains(&"user_resign"), "{dids:?}");

        // Ingest a brand-new user after restore -> UID must be >= HWM (4), no UID collision.
        post_ingest(
            &node2,
            &key_a,
            "/v1/ingest",
            json!({
                "events": [
                    { "event": "App_Open", "distinct_id": "brand_new_after_restore", "timestamp": now_ms + 20_000 }
                ]
            }),
        )
        .await;
        let ig2 = node2.storage.identity_graph("proj_a", false).await.unwrap();
        let new_uid = ig2.lookup_canonical("brand_new_after_restore").unwrap();
        assert!(
            (new_uid as u64) >= hwm,
            "post-restore user UID {new_uid} collided below HWM {hwm}"
        );

        // New user is visible in users/events and does NOT hit any deleted post-HWM tombstone.
        let detail_new: Value = client
            .get(format!(
                "http://{}/v1/projects/proj_a/users/brand_new_after_restore",
                node2.http_addr
            ))
            .bearer_auth(ADMIN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(detail_new["events"].as_array().unwrap().len(), 1);

        node2.shutdown().await;
    });
}

trait TapByok {
    fn tap_byok(self, hex_key: &str) -> Self;
}

impl TapByok for Arc<pulse_storage::EncryptedObjectStore> {
    fn tap_byok(self, hex_key: &str) -> Self {
        let key = pulse_storage::parse_byok_master_key(hex_key).unwrap();
        self.set_master_key(Some(key));
        self
    }
}
