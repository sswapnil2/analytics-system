use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use pulse_agent::{AgentAlertRule, AgentErasureTombstone, AgentPrivacyPolicy, ErasureLevel};
use pulse_control::AlertRule;
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

#[test]
fn multi_node_ownership_enforces_not_owner_scopes_loops_places_projects_and_reloads_erased_uids() {
    let _env_lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let dir_a = tmp.path().join("node_a");
        let dir_b = tmp.path().join("node_b");
        let meta_db = tmp.path().join("shared_meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );
        let _hot_env = EnvGuard::set("PULSE_HOT_STORE", Some("local"));

        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pid = "proj_multi";
        let admin_token = "sk_root_m6a_multinode_token_1234";

        // 1. Start node-a first: registers in writer_nodes and receives placement for `proj_multi`.
        let node_a = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_a, pid, NodeRole::All, admin_token),
            s3_raw.clone(),
            NodeIdentity::new("node-a", None),
        )
        .await
        .unwrap();
        let ingest_key = node_a
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();

        // 2. Start node-b sharing `shared_meta.db` and `s3_raw`, but with separate `dir_b`.
        let node_b = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_b, pid, NodeRole::All, admin_token),
            s3_raw.clone(),
            NodeIdentity::new("node-b", None),
        )
        .await
        .unwrap();

        let meta = node_a.control.meta().clone();
        let owner = meta.get_project_owner(pid).await.unwrap().unwrap();
        assert_eq!(owner.node_name, "node-a");
        assert_eq!(owner.address, format!("http://{}", node_a.http_addr));

        // 3. Ingest + engage + agent traces on owner node-a succeed.
        let http = reqwest::Client::new();
        let url_a = format!("http://{}", node_a.http_addr);
        let url_b = format!("http://{}", node_b.http_addr);
        let now_ms = chrono::Utc::now().timestamp_millis();

        assert_eq!(
            http.post(format!("{url_a}/v1/ingest"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "events": [
                        { "distinct_id": "u_keep", "event_name": "App_Open", "client_event_ts_ms": now_ms },
                        { "distinct_id": "u_erase_ext", "event_name": "App_Open", "client_event_ts_ms": now_ms }
                    ]
                }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        assert_eq!(
            http.post(format!("{url_a}/v1/engage"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({ "distinct_id": "u_erase_ext", "$set": { "tier": "vip" } }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        assert_eq!(
            http.post(format!("{url_a}/v1/groups"))
                .header("x-pulse-key", &ingest_key)
                .json(&json!({
                    "group_type": "company",
                    "group_id": "acme",
                    "$set": { "plan": "enterprise" }
                }))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
        node_a.ingest.flush_all().await.unwrap();

        // 4. Non-owner node-b MUST still reject unforwarded async ingest (/v1/ingest, /v1/agent/traces,
        // /v1/otel/v1/traces) with 503 not_owner, while W3 owner-routing forwards sync profile/replay
        // and query/status endpoints transparently to node-a (returning 503 not_owner when hop=1).
        let assert_not_owner = |status: reqwest::StatusCode, body: Value| {
            assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE, "{body}");
            assert_eq!(body["code"], "not_owner", "{body}");
            assert_eq!(body["project_id"], pid, "{body}");
            assert_eq!(body["owner_node"], "node-a", "{body}");
            assert_eq!(body["owner_address"], format!("http://{}", node_a.http_addr), "{body}");
        };
        let assert_forward_loop = |status: reqwest::StatusCode, body: Value| {
            assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE, "{body}");
            assert_eq!(body["code"], "forward_loop_detected", "{body}");
        };

        let r_ingest = http
            .post(format!("{url_b}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({ "events": [{ "distinct_id": "u_x", "event_name": "App_Open", "client_event_ts_ms": now_ms }] }))
            .send()
            .await
            .unwrap();
        assert_not_owner(r_ingest.status(), r_ingest.json().await.unwrap());

        let r_identify_fwd = http
            .post(format!("{url_b}/v1/identify"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({ "anon_id": "a_fwd_x", "user_id": "u_fwd_x" }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_identify_fwd.status(), reqwest::StatusCode::OK);
        let identify_body: Value = r_identify_fwd.json().await.unwrap();
        assert!(identify_body["canonical_uid"].as_u64().is_some(), "{identify_body}");

        let r_identify_hop = http
            .post(format!("{url_b}/v1/identify"))
            .header("x-pulse-key", &ingest_key)
            .header("x-pulse-forwarded-hop", "1")
            .json(&json!({ "anon_id": "a_x", "user_id": "u_x" }))
            .send()
            .await
            .unwrap();
        assert_forward_loop(r_identify_hop.status(), r_identify_hop.json().await.unwrap());

        let r_engage_fwd = http
            .post(format!("{url_b}/v1/engage"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({ "distinct_id": "u_fwd_x", "$set": { "plan": "pro" } }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_engage_fwd.status(), reqwest::StatusCode::OK);
        let engage_body: Value = r_engage_fwd.json().await.unwrap();
        assert_eq!(engage_body["updated"], 1, "{engage_body}");

        let r_engage_hop = http
            .post(format!("{url_b}/v1/engage"))
            .header("x-pulse-key", &ingest_key)
            .header("x-pulse-forwarded-hop", "1")
            .json(&json!({ "distinct_id": "u_x", "$set": { "plan": "pro" } }))
            .send()
            .await
            .unwrap();
        assert_forward_loop(r_engage_hop.status(), r_engage_hop.json().await.unwrap());

        let r_groups_fwd = http
            .post(format!("{url_b}/v1/groups"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "group_type": "company",
                "group_id": "acme_fwd",
                "$set": { "plan": "pro" }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_groups_fwd.status(), reqwest::StatusCode::OK);
        let groups_body: Value = r_groups_fwd.json().await.unwrap();
        assert_eq!(groups_body["updated"], 1, "{groups_body}");

        let r_groups_hop = http
            .post(format!("{url_b}/v1/groups"))
            .header("x-pulse-key", &ingest_key)
            .header("x-pulse-forwarded-hop", "1")
            .json(&json!({
                "group_type": "company",
                "group_id": "acme",
                "$set": { "plan": "pro" }
            }))
            .send()
            .await
            .unwrap();
        assert_forward_loop(r_groups_hop.status(), r_groups_hop.json().await.unwrap());

        let r_replay_fwd = http
            .post(format!("{url_b}/v1/replay/chunk"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "project_id": pid,
                "session_id": "sess_fwd_1",
                "chunk_start_ts_ms": now_ms,
                "ops": []
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_replay_fwd.status(), reqwest::StatusCode::OK);

        let r_replay_hop = http
            .post(format!("{url_b}/v1/replay/chunk"))
            .header("x-pulse-key", &ingest_key)
            .header("x-pulse-forwarded-hop", "1")
            .json(&json!({
                "project_id": pid,
                "session_id": "sess_1",
                "chunk_start_ts_ms": now_ms,
                "ops": []
            }))
            .send()
            .await
            .unwrap();
        assert_forward_loop(r_replay_hop.status(), r_replay_hop.json().await.unwrap());

        let r_query_fwd = http
            .post(format!("{url_b}/v1/projects/{pid}/query/insights"))
            .bearer_auth(admin_token)
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "App_Open", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r_query_fwd.status(), reqwest::StatusCode::OK);

        let r_query_hop = http
            .post(format!("{url_b}/v1/projects/{pid}/query/insights"))
            .bearer_auth(admin_token)
            .header("x-pulse-forwarded-hop", "1")
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "App_Open", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap();
        assert_not_owner(r_query_hop.status(), r_query_hop.json().await.unwrap());

        let r_status_fwd = http
            .get(format!("{url_b}/v1/status?project_id={pid}"))
            .bearer_auth(admin_token)
            .send()
            .await
            .unwrap();
        assert_eq!(r_status_fwd.status(), reqwest::StatusCode::OK);
        let status_body: Value = r_status_fwd.json().await.unwrap();
        assert_eq!(status_body["active_project_id"], pid, "{status_body}");

        let r_status_hop = http
            .get(format!("{url_b}/v1/status?project_id={pid}"))
            .bearer_auth(admin_token)
            .header("x-pulse-forwarded-hop", "1")
            .send()
            .await
            .unwrap();
        assert_not_owner(r_status_hop.status(), r_status_hop.json().await.unwrap());

        let r_agent = http
            .post(format!("{url_b}/v1/agent/traces"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "traces": [{
                    "trace_id": "tr_non_owner",
                    "agent_name": "bot",
                    "agent_version": "1.0.0",
                    "environment": "prod",
                    "status": "ok",
                    "start_ts_ms": now_ms,
                    "duration_ms": 10
                }],
                "spans": []
            }))
            .send()
            .await
            .unwrap();
        assert_not_owner(r_agent.status(), r_agent.json().await.unwrap());

        let r_otel = http
            .post(format!("{url_b}/v1/otel/v1/traces"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "resourceSpans": [{
                    "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "bot" } }] },
                    "scopeSpans": [{
                        "spans": [{
                            "traceId": "0102030405060708090a0b0c0d0e0f10",
                            "spanId": "0102030405060708",
                            "name": "llm_call",
                            "startTimeUnixNano": (now_ms as u64) * 1_000_000,
                            "endTimeUnixNano": ((now_ms + 10) as u64) * 1_000_000
                        }]
                    }]
                }]
            }))
            .send()
            .await
            .unwrap();
        assert_not_owner(r_otel.status(), r_otel.json().await.unwrap());

        // 5. Verify node-b never opened SlateDB for `proj_multi` and node-a stays unfenced.
        assert!(!dir_b.join("hot/identity").join(pid).exists());
        assert!(!dir_b.join("hot/profiles").join(pid).exists());

        // 6. Owner-scoped background loops on node-b skip `proj_multi`!
        meta.upsert_alert_rule(&AlertRule {
            id: "al_evt_1".into(),
            project_id: pid.into(),
            name: "Always fires".into(),
            event_name: "App_Open".into(),
            operator: "gte".into(),
            threshold: 0.0,
            webhook_url: None,
            last_triggered_ms: None,
            created_at_ms: now_ms,
        })
        .await
        .unwrap();
        meta.upsert_agent_alert_rule(&AgentAlertRule {
            id: "al_agent_1".into(),
            project_id: pid.into(),
            name: "Agent alert".into(),
            metric: "failure_rate".into(),
            operator: "gte".into(),
            threshold: 0.0,
            window_minutes: 60,
            agent_name: None,
            environment: None,
            webhook_url: None,
            cooldown_minutes: 15,
            enabled: true,
            state: "ok".into(),
            last_evaluated_ms: None,
            last_triggered_ms: None,
            last_resolved_ms: None,
            last_value: None,
            created_at_ms: now_ms,
        })
        .await
        .unwrap();
        meta.insert_erasure_tombstone(&AgentErasureTombstone {
            id: "tb_multi_1".into(),
            project_id: pid.into(),
            level: ErasureLevel::Trace,
            target_id: "tr_owner_only".into(),
            created_at: now_ms,
            expires_at: Some(now_ms + 60_000),
        })
        .await
        .unwrap();
        meta.upsert_agent_privacy_policy(&AgentPrivacyPolicy::default_for_project(pid))
            .await
            .unwrap();

        node_b.control.evaluate_alerts().await;
        node_b.control.evaluate_agent_alerts().await;
        node_b.control.sweep_pending_agent_erasures().await;
        let prune_stats_b = node_b.control.prune_expired_agent_data().await;
        node_b.control.reload_caches_from_db().await.unwrap();

        // node-b must NOT have triggered alerts, evaluated SQL retention, or purged tombstones for proj_multi!
        let evt_rule = meta
            .list_alert_rules(pid)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(evt_rule.last_triggered_ms.is_none());
        assert_eq!(prune_stats_b.projects_checked, 0);
        assert_eq!(
            meta.list_unpurged_erasure_tombstones().await.unwrap().len(),
            1
        );
        assert!(!dir_b.join("hot/identity").join(pid).exists());
        assert!(!dir_b.join("hot/profiles").join(pid).exists());

        // node-a (the owner) sweeps and marks the tombstone purged.
        node_a.control.sweep_pending_agent_erasures().await;
        assert!(meta
            .list_unpurged_erasure_tombstones()
            .await
            .unwrap()
            .is_empty());

        // 7. POST /v1/projects places a new project on least-loaded node-b (0 projects < 1).
        let create_resp = http
            .post(format!("{url_a}/v1/projects"))
            .bearer_auth(admin_token)
            .json(&json!({ "name": "Second Project" }))
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), reqwest::StatusCode::OK);
        let created: Value = create_resp.json().await.unwrap();
        let pid2 = created["project"]["id"].as_str().unwrap();
        let owner2 = meta.get_project_owner(pid2).await.unwrap().unwrap();
        assert_eq!(owner2.node_name, "node-b");

        // 8. NodeRole::All 5s `reload_caches_from_db` ticker picks up external `erased_uids` rows!
        let id_graph_a = node_a.storage.identity_graph(pid, false).await.unwrap();
        let erased_uid = id_graph_a.lookup_canonical("u_erase_ext").unwrap();
        meta.insert_erased_uids(pid, &[erased_uid], now_ms + 1_000)
            .await
            .unwrap();

        let mut synced = false;
        for _ in 0..40 {
            if node_a.storage.erased_snapshot(pid).contains(erased_uid) {
                synced = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(
            synced,
            "NodeRole::All background ticker did not reload external erased_uids within 8s"
        );

        let users_resp: Value = http
            .get(format!("{url_a}/v1/projects/{pid}/users"))
            .bearer_auth(admin_token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let distinct_ids: Vec<&str> = users_resp
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|u| u["distinct_id"].as_str())
            .collect();
        assert!(
            !distinct_ids.contains(&"u_erase_ext"),
            "erased user still visible in profile catalog: {users_resp}"
        );

        let q_after_erase: Value = http
            .post(format!("{url_a}/v1/projects/{pid}/query/insights"))
            .bearer_auth(admin_token)
            .json(&json!({
                "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                "interval": "day",
                "series": [{ "label": "A", "event_name": "App_Open", "metric": "totals" }]
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            q_after_erase["series"][0]["overall_value"].as_f64(),
            Some(1.0),
            "only u_keep should remain visible after external erasure reload: {q_after_erase}"
        );

        // Clean shutdown deregisters writer_nodes while keeping project_owner rows intact.
        node_b.shutdown().await;
        node_a.shutdown().await;
        assert!(meta
            .list_healthy_writer_nodes(chrono::Utc::now().timestamp_millis(), 60_000)
            .await
            .unwrap()
            .is_empty());
        assert!(meta.get_project_owner(pid).await.unwrap().is_some());
    });
}

#[test]
fn compactor_role_enforces_reconcile_ownership_and_skips_reassigned_projects() {
    let _env_lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let dir_a = tmp.path().join("node_a");
        let meta_db = tmp.path().join("shared_meta.db");
        let _db_env = EnvGuard::set(
            "DATABASE_URL",
            Some(&format!("sqlite://{}", meta_db.display())),
        );
        let _hot_env = EnvGuard::set("PULSE_HOT_STORE", Some("object"));

        let s3_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pid = "proj_cmp_m6a";
        let admin_token = "sk_root_m6a_compactor_token_1234";

        // Writer node-a owns `proj_cmp_m6a` and produces 4 segments so compaction is overdue.
        let writer = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_a, pid, NodeRole::All, admin_token),
            s3_raw.clone(),
            NodeIdentity::new("node-a", None),
        )
        .await
        .unwrap();
        // Start a NodeRole::Compactor sharing node-a identity and object store BEFORE flushing
        // segments so its initial t=0 tick sees 0 segments and does not race the test.
        let dir_cmp = tmp.path().join("node_cmp");
        let compactor = start_pulse_node_with_store_and_identity(
            node_cfg(&dir_cmp, pid, NodeRole::Compactor, admin_token),
            s3_raw.clone(),
            NodeIdentity::new("node-a", None),
        )
        .await
        .unwrap();
        assert!(!compactor.storage.is_published_reader());

        let cmp_ps = compactor.storage.get_or_open_project(pid).await.unwrap();

        let ingest_key = writer
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let now_ms = chrono::Utc::now().timestamp_millis();
        for i in 0..4i64 {
            assert_eq!(
                http.post(format!("http://{}/v1/ingest", writer.http_addr))
                    .header("x-pulse-key", &ingest_key)
                    .json(&json!({
                        "events": [{
                            "distinct_id": "u_cmp",
                            "event_name": "App_Open",
                            "client_event_ts_ms": now_ms - i
                        }]
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                reqwest::StatusCode::OK
            );
            writer.ingest.flush_all().await.unwrap();
            writer.storage.flush_all_projects().await.unwrap();
        }
        let writer_ps = writer.storage.get_or_open_project(pid).await.unwrap();
        assert_eq!(writer_ps.snapshot().manifest.total_segments(), 4);
        cmp_ps.refresh_manifest().await.unwrap();
        assert_eq!(cmp_ps.snapshot().manifest.total_segments(), 4);
        let epoch_before = cmp_ps.snapshot().manifest.epoch;
        let meta = writer.control.meta().clone();

        // Reassign project ownership away from node-a to node-b.
        assert!(meta
            .reassign_project_owner(
                pid,
                "node-b",
                "http://127.0.0.1:19999",
                Some("node-a"),
                now_ms + 1_000,
            )
            .await
            .unwrap());

        compactor.storage.reconcile_ownership().await;
        assert!(cmp_ps.is_fenced());
        assert!(compactor.storage.get_project(pid).is_none());
        assert!(matches!(
            cmp_ps
                .compact_overdue(compactor.storage.segment_reader())
                .await,
            Err(pulse_storage::StorageError::NotWriter { .. })
        ));
        assert_eq!(compactor.storage.compact_all_projects().await.unwrap(), 0);
        assert_eq!(cmp_ps.snapshot().manifest.epoch, epoch_before);
        assert_eq!(cmp_ps.snapshot().manifest.total_segments(), 4);
        assert!(matches!(
            compactor.storage.get_or_open_project(pid).await,
            Err(pulse_storage::StorageError::NotOwner {
                owner_node: Some(ref n),
                ..
            }) if n == "node-b"
        ));

        compactor.shutdown().await;
        writer.shutdown().await;
    });
}
