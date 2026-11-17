pub mod seeder;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue, Method};
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use pulse_control::{
    build_admin_control_router, build_public_flag_router, BootstrapCredentials,
    ControlPlaneService, MetaStore,
};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, spawn_quic_listener, IngestService, QuicIngestHandle};
use pulse_storage::{build_object_store, StorageConfig, StorageEngine, DEFAULT_NUM_BUCKETS};
use tokio::sync::{oneshot, watch};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::compression::predicate::{NotForContentType, Predicate};
use tower_http::compression::{CompressionLayer, DefaultPredicate};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeader;
use tracing::info;

pub use seeder::{seed_indian_fintech_dataset, SeedSummary};

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub http_addr: SocketAddr,
    pub quic_addr: Option<SocketAddr>,
    pub data_dir: PathBuf,
    pub web_dist_dir: PathBuf,
    pub default_project_id: String,
    /// Optional explicit initial ingest key (used in deterministic tests); empty in production so a random key is generated once (`P0-02`).
    pub default_api_key: String,
    /// Optional explicit root admin bearer token; when `None` and `PULSE_ADMIN_TOKEN` is unset, a random `sk_root_...` token is generated once (`P0-01`, `P0-02`).
    pub admin_token: Option<String>,
    /// Allowed browser origins for admin/query CORS (`P0-01`).
    pub allowed_origins: Vec<String>,
    /// Opt-in synthetic demo dataset seeder (`false` by default, enabled via `--seed` or `PULSE_SEED=1`, Section 6).
    pub auto_seed: bool,
    pub num_buckets: u16,
}

impl Default for NodeConfig {
    fn default() -> Self {
        let host: IpAddr = std::env::var("PULSE_HOST")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let port: u16 = std::env::var("PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8080);
        let quic_addr = std::env::var("PULSE_QUIC_ADDR")
            .ok()
            .and_then(|s| s.parse().ok());
        let data_dir = std::env::var("PULSE_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./pulse_data"));
        let web_dist_dir = std::env::var("PULSE_WEB_DIST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./web/dist"));
        let seed_enabled =
            std::env::var("PULSE_SEED").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
        let allowed_origins = std::env::var("PULSE_CORS_ORIGINS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                vec![
                    format!("http://127.0.0.1:{port}"),
                    format!("http://localhost:{port}"),
                    "http://127.0.0.1:5173".to_string(),
                    "http://localhost:5173".to_string(),
                ]
            });

        Self {
            http_addr: SocketAddr::new(host, port),
            quic_addr,
            data_dir,
            web_dist_dir,
            default_project_id: std::env::var("PULSE_DEFAULT_PROJECT")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "proj_default".to_string()),
            default_api_key: String::new(),
            admin_token: std::env::var("PULSE_ADMIN_TOKEN")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            allowed_origins,
            auto_seed: seed_enabled,
            num_buckets: std::env::var("PULSE_NUM_BUCKETS")
                .ok()
                .and_then(|v| v.trim().parse::<u16>().ok())
                .map(|n| n.clamp(1, 256))
                .unwrap_or(DEFAULT_NUM_BUCKETS),
        }
    }
}

pub struct RunningNode {
    pub http_addr: SocketAddr,
    pub quic: Option<QuicIngestHandle>,
    pub control: Arc<ControlPlaneService>,
    pub ingest: Arc<IngestService>,
    pub storage: Arc<StorageEngine>,
    pub qengine: Arc<QueryEngine>,
    pub bootstrap_credentials: BootstrapCredentials,
    shutdown_bg_tx: watch::Sender<bool>,
    shutdown_http_tx: Option<oneshot::Sender<()>>,
    http_task: tokio::task::JoinHandle<()>,
}

impl RunningNode {
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_bg_tx.send(true);
        if let Some(q) = &self.quic {
            q.shutdown().await;
        }
        if let Some(tx) = self.shutdown_http_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.http_task.await;
        let _ = self.storage.flush_all_projects().await;
        let _ = self.storage.identity_router().close_all().await;
    }
}

fn build_admin_cors_layer(origins: &[String]) -> CorsLayer {
    let parsed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| HeaderValue::from_str(o).ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::RANGE,
            header::IF_NONE_MATCH,
            header::HeaderName::from_static("x-pulse-admin-key"),
            header::HeaderName::from_static("x-pulse-project"),
        ])
}

/// Boots the unified single-binary Pulse Analytics Node.
pub async fn start_pulse_node(config: NodeConfig) -> anyhow::Result<RunningNode> {
    std::fs::create_dir_all(&config.data_dir)?;

    let object_store = build_object_store(&config.data_dir)?;
    let identity_router = Arc::new(IdentityRouter::new(object_store.clone()));

    let storage_cfg = StorageConfig {
        num_buckets: config.num_buckets,
        l0_flush_row_threshold: 250_000,
        l0_flush_max_age: Duration::from_secs(60),
        compaction_min_segments: 4,
    };
    let storage = StorageEngine::new(storage_cfg, object_store, identity_router);

    let catalogs = CatalogRegistry::new();
    let foyer_dir = config.data_dir.join("foyer_cache");
    let qengine = QueryEngine::new(
        storage.clone(),
        128 * 1024 * 1024,
        Some(&foyer_dir),
        512 * 1024 * 1024,
        catalogs,
    )
    .await?;

    let meta = MetaStore::connect_auto(&config.data_dir).await?;
    let control = ControlPlaneService::new_with_admin(
        meta,
        qengine.clone(),
        &config.default_project_id,
        &config.default_api_key,
        config.admin_token.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    let bootstrap_credentials = control.take_bootstrap_credentials();
    if let Some(tok) = &bootstrap_credentials.generated_admin_token {
        info!("Generated one-time Root Admin Bearer Token (save now, never stored in plaintext): {tok}");
    }
    if let Some(pk) = &bootstrap_credentials.generated_ingest_key {
        info!(
            project_id = %config.default_project_id,
            "Generated one-time Public Ingest Key (save now, never stored in plaintext): {pk}"
        );
    }

    let ingest = IngestService::new(storage.clone(), control.clone());
    control.attach_ingest(ingest.clone()).await;

    if config.auto_seed {
        let ps = storage
            .get_or_open_project(&config.default_project_id)
            .await?;
        let existing_rows = ps.total_l0_rows() as u64 + ps.snapshot().manifest.total_l1_rows();
        if existing_rows == 0 {
            seed_indian_fintech_dataset(&control, &ingest, &config.default_project_id).await?;
        }
    }

    let (shutdown_bg_tx, shutdown_bg_rx) = watch::channel(false);
    let _bg_handle = storage.spawn_background_maintenance(shutdown_bg_rx);

    let quic = if let Some(qaddr) = config.quic_addr {
        match spawn_quic_listener(qaddr, ingest.clone()) {
            Ok(h) => {
                info!(quic_udp = %h.local_addr, "Pulse QUIC UDP edge gateway listening");
                Some(h)
            }
            Err(e) => {
                tracing::warn!("QUIC listener bind skipped ({qaddr}): {e}");
                None
            }
        }
    } else {
        None
    };

    // Public SDK routes (permissive CORS) vs Protected Admin/Query routes (restricted CORS + Bearer auth, `P0-01`)
    let public_router = Router::new()
        .merge(build_ingest_router(ingest.clone()))
        .merge(build_public_flag_router(control.clone()))
        .layer(CorsLayer::permissive());

    let admin_router = build_admin_control_router(control.clone())
        .layer(build_admin_cors_layer(&config.allowed_origins));

    let api_router = Router::new()
        .merge(public_router)
        .merge(admin_router)
        .route("/healthz", get(|| async { "ok" }));

    let index_file = config.web_dist_dir.join("index.html");
    let app = if index_file.exists() {
        let serve_dir =
            ServeDir::new(&config.web_dist_dir).not_found_service(ServeFile::new(index_file));
        // ponytail: everything in dist except index.html is under content-hashed assets/, so
        // non-HTML can be cached forever. Add a path check if un-hashed files land in dist/.
        let cached_dir = SetResponseHeader::overriding(
            serve_dir,
            header::CACHE_CONTROL,
            |res: &axum::http::Response<_>| {
                let html = res
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .is_some_and(|v| v.as_bytes().starts_with(b"text/html"));
                Some(HeaderValue::from_static(if html {
                    "no-cache"
                } else {
                    "public, max-age=31536000, immutable"
                }))
            },
        );
        api_router.fallback_service(cached_dir)
    } else {
        api_router.fallback(get(|| async {
            Html(
                "<!doctype html><html><head><title>Pulse Analytics</title></head>\
                 <body style='background:#090d16;color:#e2e8f0;font-family:sans-serif;padding:2rem'>\
                 <h1>Pulse Analytics Node Running</h1>\
                 <p>API &amp; Query Engine online. Run <code>npm --prefix web run build</code> to bundle Analytics Studio.</p>\
                 </body></html>",
            )
        }))
    };
    // gzip/br for the web app and JSON. Replay frames are served by byte range, and
    // compressing a range response would corrupt it, so they stay as they are.
    let app = app.layer(CompressionLayer::new().compress_when(
        DefaultPredicate::new().and(NotForContentType::const_new("application/x-pulse-frame")),
    ));

    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    let bound_http_addr = listener.local_addr()?;
    info!(http = %bound_http_addr, "Pulse Analytics unified HTTP/2 + Studio server listening");

    let (shutdown_http_tx, shutdown_http_rx) = oneshot::channel::<()>();
    let http_task = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_http_rx.await;
            })
            .await;
    });

    Ok(RunningNode {
        http_addr: bound_http_addr,
        quic,
        control,
        ingest,
        storage,
        qengine,
        bootstrap_credentials,
        shutdown_bg_tx,
        shutdown_http_tx: Some(shutdown_http_tx),
        http_task,
    })
}

#[cfg(test)]
mod tests {
    use pulse_sdk_core::{PulseClient, SdkConfig, SdkWireNode};
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn p0_01_and_p0_07_secure_node_config_defaults() {
        let cfg = NodeConfig::default();
        assert!(
            cfg.http_addr.ip().is_loopback(),
            "default HTTP bind must be loopback 127.0.0.1, got {}",
            cfg.http_addr
        );
        assert!(
            cfg.quic_addr.is_none(),
            "QUIC listener must be disabled by default"
        );
        assert!(!cfg.auto_seed, "demo seeding must be opt-in (--seed)");
        assert!(
            cfg.default_api_key.is_empty(),
            "default_api_key must not be a hardcoded secret"
        );
    }

    #[tokio::test]
    async fn gate4_full_node_sdk_offline_wireframe_lexicon_ab_lift_and_mcp() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let sdk_dir = tmp_dir.path().join("sdk_offline_wal");

        // 1. Spin up `pulse-node` on ephemeral loopback ports with random first-boot credentials and `--seed` enabled
        let node = start_pulse_node(NodeConfig {
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: tmp_dir.path().join("node_data"),
            web_dist_dir: tmp_dir.path().join("no_web"),
            default_project_id: "proj_fintech_in".to_string(),
            default_api_key: String::new(),
            admin_token: None,
            allowed_origins: vec!["http://127.0.0.1:8080".to_string()],
            auto_seed: true,
            num_buckets: 32,
        })
        .await
        .expect("start pulse-node");

        let admin_token = node
            .bootstrap_credentials
            .generated_admin_token
            .clone()
            .expect("random admin token generated");
        let ingest_key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .expect("random ingest key generated");

        let base_url = format!("http://{}", node.http_addr);
        let http = reqwest::Client::new();

        // Unauthenticated `/v1/status` must return 401
        let unauth = http
            .get(format!("{base_url}/v1/status"))
            .send()
            .await
            .unwrap();
        assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

        // Pick a deterministic phone ID enrolled in the 50% `one_tap_upi_mandate` rollout
        let sdk_user_id = (0..32)
            .map(|i| format!("+9199000999{i:02}"))
            .find(|id| pulse_wire::flag_rollout_slot("one_tap_upi_mandate", id) < 5_000)
            .expect("deterministic phone id inside 50% rollout");

        // 2. Offline SDK phase using the freshly minted random `pk_live_...` key
        {
            let offline_sdk = PulseClient::new(SdkConfig {
                endpoint_url: "http://127.0.0.1:9".to_string(),
                api_key: ingest_key.clone(),
                storage_dir: sdk_dir.to_string_lossy().to_string(),
                flush_batch_size: 50,
                flush_interval_ms: 0,
            })
            .unwrap();

            offline_sdk
                .record_wireframe_snapshot(
                    390,
                    844,
                    vec![
                        SdkWireNode {
                            node_id: 1,
                            parent_id: 0,
                            kind: "screen".into(),
                            x: 0,
                            y: 0,
                            w: 390,
                            h: 844,
                            bg_rgba: 0x090D_16FF,
                            label: "SDK Checkout".into(),
                            unmasked: false,
                        },
                        SdkWireNode {
                            node_id: 2,
                            parent_id: 1,
                            kind: "masked_input".into(),
                            x: 20,
                            y: 160,
                            w: 350,
                            h: 48,
                            bg_rgba: 0x1E29_3BFF,
                            label: "PAN: ZZZZZ9999Z".into(),
                            unmasked: false,
                        },
                    ],
                )
                .unwrap();

            let now = chrono::Utc::now().timestamp_millis();
            offline_sdk.record_touch(200, 200, true, now).unwrap();
            offline_sdk.record_touch(202, 201, true, now + 150).unwrap();
            let tap3 = offline_sdk.record_touch(199, 202, true, now + 300).unwrap();
            assert_eq!(tap3, "rage_tap");

            offline_sdk
                .track(
                    "SDK_Cart_Viewed".into(),
                    r#"{"amount": 2499, "city": "Bengaluru"}"#.into(),
                )
                .unwrap();
            offline_sdk.identify(sdk_user_id.clone()).unwrap();
            offline_sdk
                .track(
                    "SDK_Order_Completed".into(),
                    r#"{"amount": 2499, "city": "Bengaluru"}"#.into(),
                )
                .unwrap();

            assert!(offline_sdk.flush().await.is_err());
            assert!(offline_sdk.queued_packet_count() >= 3);
        }

        // Verify 100,000+ auto-seeded events exist in authenticated `/v1/status`
        let status_json: Value = http
            .get(format!("{base_url}/v1/status"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(status_json.get("default_api_key").is_none());
        let total_seeded = status_json["l1_rows"].as_u64().unwrap_or(0)
            + status_json["l0_rows"].as_u64().unwrap_or(0);
        assert!(
            total_seeded >= 100_000,
            "expected >= 100,000 seeded events, got {total_seeded}"
        );

        // 3. Re-open SDK pointing at live `pulse-node` -> flush offline disk WAL + evaluate edge flag
        let live_sdk = PulseClient::new(SdkConfig {
            endpoint_url: base_url.clone(),
            api_key: ingest_key.clone(),
            storage_dir: sdk_dir.to_string_lossy().to_string(),
            flush_batch_size: 50,
            flush_interval_ms: 0,
        })
        .unwrap();
        let sdk_session_id = live_sdk.session_id();
        let flushed_packets = live_sdk.flush().await.expect("flush offline queue to node");
        assert!(flushed_packets >= 3);
        assert_eq!(live_sdk.queued_packet_count(), 0);

        let variant = live_sdk
            .evaluate_flag("one_tap_upi_mandate".into())
            .await
            .expect("evaluate flag");
        assert!(
            matches!(variant.as_deref(), Some("control") | Some("one_tap_sheet")),
            "unexpected flag variant: {variant:?}"
        );

        // Verify SDK wireframe replay stored masked PAN (`ZZZZZ9999Z` must NEVER appear!)
        let replay_http = http
            .get(format!("{base_url}/v1/replay/{sdk_session_id}"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap();
        assert_eq!(
            replay_http.status(),
            reqwest::StatusCode::OK,
            "expected HTTP 200 for /v1/replay/{sdk_session_id}"
        );
        let replay_resp: Value = replay_http.json().await.unwrap();
        let replay_text = replay_resp.to_string();
        assert!(!replay_text.contains("ZZZZZ9999Z"));
        assert!(replay_text.contains("████"));

        // 4. Blacklist an event via Lexicon API and assert live ingestion drops it
        let put_lex = http
            .put(format!(
                "{base_url}/v1/projects/proj_fintech_in/lexicon/events/Noisy_Debug_Spam"
            ))
            .bearer_auth(&admin_token)
            .json(&json!({
                "status": "dropped",
                "description": "Dropped noisy debug heartbeat"
            }))
            .send()
            .await
            .unwrap();
        assert!(put_lex.status().is_success());

        let ingest_check: Value = http
            .post(format!("{base_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "events": [
                    {
                        "distinct_id": &sdk_user_id,
                        "event_name": "Noisy_Debug_Spam",
                        "client_event_ts_ms": chrono::Utc::now().timestamp_millis()
                    },
                    {
                        "distinct_id": &sdk_user_id,
                        "event_name": "Valid_Post_Lexicon_Event",
                        "client_event_ts_ms": chrono::Utc::now().timestamp_millis()
                    }
                ]
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(ingest_check["dropped"].as_u64(), Some(1));
        assert_eq!(ingest_check["accepted"].as_u64(), Some(1));

        // 5. Evaluate active A/B Experiment `exp_one_tap_upi_mandate`
        let exp_res: Value = http
            .get(format!(
                "{base_url}/v1/projects/proj_fintech_in/experiments/exp_one_tap_upi_mandate/results"
            ))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let variants = exp_res["variants"].as_array().expect("variants array");
        let treatment = variants
            .iter()
            .find(|v| v["variant_key"] == "one_tap_sheet")
            .expect("one_tap_sheet row");
        let lift = treatment["relative_lift_pct"].as_f64().unwrap();
        let p_val = treatment["p_value"].as_f64().unwrap();
        let bayes = treatment["bayesian_prob_to_beat_control"].as_f64().unwrap();
        assert!(
            lift > 12.0 && lift < 17.0,
            "expected ~+14.5% lift, got {lift:.2}%"
        );
        assert!(p_val < 0.01, "expected p < 0.01, got {p_val}");
        assert!(bayes > 0.99, "expected Bayesian > 0.99, got {bayes}");

        // 6. Invoke MCP JSON-RPC 2.0 `tools/list` and `tools/call` over authenticated HTTP
        let mcp_list: Value = http
            .post(format!("{base_url}/v1/mcp"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list"
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            mcp_list["result"]["tools"].as_array().map(|a| a.len()),
            Some(5)
        );

        let mcp_funnel: Value = http
            .post(format!("{base_url}/v1/mcp"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "query_funnel",
                    "arguments": {
                        "steps": ["App_Open", "OTP_Verified", "KYC_PAN_Submitted", "UPI_Autopay_Mandate_Approved", "First_Disbursal_Success"],
                        "days": 14
                    }
                }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let step0_entered = mcp_funnel["result"]["structuredContent"]["overall_steps"][0]
            ["entered_users"]
            .as_u64()
            .unwrap_or(0);
        assert!(
            step0_entered >= 8_000,
            "expected >= 8000 unique users at Funnel Step 0, got {step0_entered}"
        );

        node.shutdown().await;
    }
}
