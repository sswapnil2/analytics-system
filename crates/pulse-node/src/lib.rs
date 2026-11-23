pub mod metrics;
pub mod seeder;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use pulse_control::{
    build_admin_control_router, build_agent_ingest_router, build_public_flag_router,
    BootstrapCredentials, ControlPlaneService, MetaStore,
};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_ingest::{build_ingest_router, spawn_quic_listener, IngestService, QuicIngestHandle};
use pulse_storage::{
    build_object_store, with_hot_store_for_role, DurableLocalObjectStore, MaintenanceKind,
    StorageConfig, StorageEngine, DEFAULT_NUM_BUCKETS,
};
use tokio::sync::{oneshot, watch};
use tower_http::compression::predicate::{NotForContentType, Predicate};
use tower_http::compression::{CompressionLayer, DefaultPredicate};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeader;
use tower_http::trace::TraceLayer;
use tracing::info;

pub use seeder::{seed_indian_fintech_dataset, SeedSummary};

/// Horizontal scaling role for a `pulse-node` process (`--role` / `PULSE_NODE_ROLE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    #[default]
    All,
    Ingest,
    Query,
    Compactor,
}

impl NodeRole {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "all" => Some(Self::All),
            "ingest" => Some(Self::Ingest),
            "query" => Some(Self::Query),
            "compactor" => Some(Self::Compactor),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Ingest => "ingest",
            Self::Query => "query",
            Self::Compactor => "compactor",
        }
    }

    #[inline]
    pub fn serves_ingest(&self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }

    #[inline]
    pub fn serves_query_and_admin(&self) -> bool {
        matches!(self, Self::All | Self::Query)
    }

    #[inline]
    pub fn runs_l0_flusher(&self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }

    #[inline]
    pub fn runs_compactor(&self) -> bool {
        matches!(self, Self::All | Self::Compactor)
    }
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub role: NodeRole,
    pub http_addr: SocketAddr,
    pub quic_addr: Option<SocketAddr>,
    pub data_dir: PathBuf,
    pub web_dist_dir: PathBuf,
    pub default_project_id: String,
    /// Optional explicit initial ingest key (used in deterministic tests); empty in production so a random key is generated once.
    pub default_api_key: String,
    /// Optional explicit root admin bearer token; when `None` and `PULSE_ADMIN_TOKEN` is unset, a random `sk_root_...` token is generated once (`P0-01`, `P0-02`).
    pub admin_token: Option<String>,
    /// Allowed browser origins for admin/query CORS.
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

        let role = std::env::var("PULSE_NODE_ROLE")
            .ok()
            .and_then(|s| NodeRole::parse(&s))
            .unwrap_or(NodeRole::All);

        Self {
            role,
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
    writer_leases: Option<Arc<pulse_control::DbWriterLeases>>,
    shutdown_bg_tx: watch::Sender<bool>,
    shutdown_http_tx: Option<oneshot::Sender<()>>,
    http_task: tokio::task::JoinHandle<()>,
    ready: Arc<AtomicBool>,
}

impl RunningNode {
    /// Executes one `NodeRole::Query` refresh cycle (keyring, control caches, published
    /// manifests, `erased_uids`, and `ColumnarProfileIndex` atomic swap).
    pub async fn run_query_refresh_tick(&self) -> usize {
        run_query_refresh_tick(&self.control, &self.storage).await
    }

    pub async fn shutdown(mut self) {
        // Fail `/readyz` first so load balancers stop routing here, then give them
        // `PULSE_SHUTDOWN_DRAIN_MS` (default 0) to notice before the listener closes.
        self.ready.store(false, Ordering::Relaxed);
        let drain_ms = std::env::var("PULSE_SHUTDOWN_DRAIN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        if drain_ms > 0 {
            tokio::time::sleep(Duration::from_millis(drain_ms)).await;
        }
        let _ = self.shutdown_bg_tx.send(true);
        if let Some(q) = &self.quic {
            q.shutdown().await;
        }
        if let Some(tx) = self.shutdown_http_tx.take() {
            let _ = tx.send(());
        }
        if tokio::time::timeout(Duration::from_secs(15), &mut self.http_task)
            .await
            .is_err()
        {
            tracing::warn!("HTTP server did not stop within 15s during shutdown; aborting listener to proceed with final flush");
            self.http_task.abort();
        }
        self.qengine.cache().close().await;
        // Drains the agent WAL group commit and flushes agent L0 (no-op flush when read-only).
        if let Err(e) = self.control.agent_store().close().await {
            tracing::error!(error = %e, "closing agent trace store on shutdown failed");
        }
        if !self.storage.identity_router().is_read_only() {
            // Drain W1 ingest buffers to WAL and finish applying queued WAL records before final L0 flush.
            self.ingest.shutdown().await;
            // Losing these would lose acknowledged data from memory: say so loudly.
            if let Err(e) = self.storage.flush_all_projects().await {
                tracing::error!(error = %e, "final flush on shutdown failed");
            }
            if let Err(e) = self.storage.publish_all_projects().await {
                tracing::error!(error = %e, "final publish on shutdown failed");
            }
            if let Err(e) = self.storage.identity_router().close_all().await {
                tracing::error!(error = %e, "closing identity stores on shutdown failed");
            }
            if let Err(e) = self.storage.profile_router().close_all().await {
                tracing::error!(error = %e, "closing profile stores on shutdown failed");
            }
            // Last: everything acknowledged is flushed, so a standby can take over at once.
            if let Some(leases) = &self.writer_leases {
                if let Err(e) = leases.release_all().await {
                    tracing::warn!(error = %e, "releasing writer leases on shutdown failed");
                }
            }
        } else {
            // Read-only profile stores hold SlateDB readers; close them so their pollers stop.
            let _ = self.storage.profile_router().close_all().await;
        }
    }
}

/// Content-Security-Policy for Pulse Studio (`web/dist`) and admin/control API routes.
/// The SHA-256 hash matches the inline theme-init script in `web/index.html` (which prevents FOUC);
/// once that script is extracted to `/theme-init.js`, `script-src` can be tightened to `'self'`.
const STUDIO_CSP: &str = concat!(
    "default-src 'self'; ",
    "script-src 'self' 'sha256-CHRePUA+2hu8+7Fc1MQN0Ox3SoQuGEleoBo20ZdmAyU='; ",
    "style-src 'self' 'unsafe-inline'; ",
    "img-src 'self' data:; ",
    "font-src 'self'; ",
    "connect-src 'self'; ",
    "frame-ancestors 'none'; ",
    "base-uri 'self'; ",
    "form-action 'self'"
);

async fn security_headers_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let is_https = req.uri().scheme_str() == Some("https")
        || req
            .headers()
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .is_some_and(|first| first.trim().eq_ignore_ascii_case("https"));
    let mut res = next.run(req).await;
    let headers = res.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(STUDIO_CSP),
    );
    if is_https {
        headers.insert(
            header::STRICT_TRANSPORT_SECURITY,
            HeaderValue::from_static("max-age=31536000; includeSubDomains"),
        );
    }
    res
}

/// Refuses ingest requests beyond the in-flight cap with `503` + `Retry-After`, which every
/// SDK retries with backoff. Without it, requests queue on the bucket append locks with their
/// decoded batches in memory, and latency and memory grow without bound under overload.
async fn shed_ingest_when_saturated(
    axum::extract::State(slots): axum::extract::State<Arc<tokio::sync::Semaphore>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Ok(_slot) = slots.try_acquire() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            axum::Json(serde_json::json!({
                "status": "error",
                "code": "ingest_saturated",
                "message": "too many ingest requests in flight; retry shortly",
            })),
        )
            .into_response();
    };
    next.run(req).await
}

/// `PULSE_INGEST_MAX_INFLIGHT`: concurrent ingest requests per node (default 256, `0` = no cap).
/// Each can hold an 8 MiB body plus its decoded batch, so this is also the memory bound.
fn ingest_inflight_cap() -> Option<usize> {
    let cap = std::env::var("PULSE_INGEST_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(256);
    (cap > 0).then_some(cap)
}

fn build_admin_cors_layer(origins: &[String]) -> CorsLayer {
    let parsed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| HeaderValue::from_str(o).ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_credentials(true)
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

/// Executes one `NodeRole::Query` refresh cycle:
/// 1. Reloads the BYOK decryption keyring from `MetaStore`.
/// 2. Reloads control-plane metadata, discovers new projects, and atomically refreshes published
///    manifests, `erased_uids`, and `ColumnarProfileIndex` under each project's
///    `query_refresh_gate`.
pub async fn run_query_refresh_tick(
    control: &ControlPlaneService,
    storage: &StorageEngine,
) -> usize {
    if let Err(e) = control.refresh_encryption_keyring().await {
        tracing::warn!(error = %e, "query node failed to refresh BYOK keyring");
    }
    let (control_res, manifest_res) = control.refresh_query_caches_and_manifests().await;
    if let Err(e) = control_res {
        tracing::warn!(error = %e, "query node failed to reload control state");
    }
    let refreshed = manifest_res.as_ref().copied().unwrap_or(0);
    storage
        .maintenance_errors()
        .check(MaintenanceKind::ManifestRefresh, manifest_res);
    refreshed
}

/// Boots the Pulse Analytics Node (`All`, `Ingest`, `Query`, or `Compactor` role).
pub async fn start_pulse_node(config: NodeConfig) -> anyhow::Result<RunningNode> {
    std::fs::create_dir_all(&config.data_dir)?;
    let segment_store = build_object_store(&config.data_dir)?;
    start_pulse_node_with_store(config, segment_store).await
}

/// [`start_pulse_node`] on a given segment store (tests count or share it).
pub async fn start_pulse_node_with_store(
    config: NodeConfig,
    raw_store: Arc<dyn object_store::ObjectStore>,
) -> anyhow::Result<RunningNode> {
    std::fs::create_dir_all(&config.data_dir)?;
    let raw_store =
        with_hot_store_for_role(raw_store, &config.data_dir, config.role == NodeRole::Query)
            .await?;
    let enc_store = Arc::new(pulse_storage::EncryptedObjectStore::new(raw_store));
    let identity_router = Arc::new(if config.role.serves_ingest() {
        IdentityRouter::new(enc_store.as_object_store())
    } else {
        IdentityRouter::new_read_only(enc_store.as_object_store())
    });

    let storage_cfg = StorageConfig {
        num_buckets: config.num_buckets,
        l0_flush_row_threshold: 250_000,
        l0_flush_max_age: Duration::from_secs(60),
        compaction_min_segments: 4,
    };
    let storage =
        StorageEngine::new_with_encrypted(storage_cfg, enc_store.clone(), identity_router);
    if config.role == NodeRole::Query {
        let query_hot_dir = config.data_dir.join("query_hot");
        let query_hot_raw: Arc<dyn object_store::ObjectStore> = Arc::new(
            DurableLocalObjectStore::new_at(&query_hot_dir)?.with_prune_empty_below_depth(2),
        );
        let query_hot_enc = enc_store.with_shared_keyring(query_hot_raw);
        storage.enable_published_reader(query_hot_enc.as_object_store());
    }

    let catalogs = CatalogRegistry::new();
    let foyer_dir = config.data_dir.join("foyer_cache");
    let disk_cache_dir = config
        .role
        .serves_query_and_admin()
        .then_some(foyer_dir.as_path());
    let cache_mem_bytes = std::env::var("PULSE_CACHE_MEMORY_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&mb| mb > 0)
        .map(|mb| mb.saturating_mul(1024 * 1024))
        .unwrap_or(128 * 1024 * 1024);
    let qengine = QueryEngine::new(
        storage.clone(),
        cache_mem_bytes,
        disk_cache_dir,
        512 * 1024 * 1024,
        catalogs,
    )
    .await?;

    let meta = MetaStore::connect_auto(&config.data_dir).await?;
    if meta.is_sqlite() && pulse_storage::is_local_hot_store_mode().unwrap_or(true) {
        tracing::warn!(
            data_dir = %config.data_dir.display(),
            "PULSE_HOT_STORE=local is using SQLite (pulse_meta.db); \
             losing this volume will lose accounts, projects, API keys, and erased_uids tombstones, \
             and `pulse-node restore --from-published` refuses SQLite. \
             Set DATABASE_URL to an external PostgreSQL database for recoverable deployments."
        );
    }
    // Set before anything can open a project: only the lease holder may write one.
    let writer_leases = config
        .role
        .serves_ingest()
        .then(|| Arc::new(pulse_control::DbWriterLeases::from_env(meta.clone())));
    let (shutdown_bg_tx, mut shutdown_bg_rx) = watch::channel(false);
    if let Some(leases) = &writer_leases {
        use pulse_storage::WriterLeases;
        storage.set_writer_leases(leases.clone());
        // Renewal starts now: seeding below holds a lease for longer than one TTL.
        let lease_storage = storage.clone();
        let mut lease_shutdown = shutdown_bg_rx.clone();
        let every = leases.ttl() / 3;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => lease_storage.renew_writer_leases().await,
                    changed = lease_shutdown.changed() => {
                        if changed.is_err() || *lease_shutdown.borrow() { break; }
                    }
                }
            }
        });
    }
    let control = ControlPlaneService::new_with_admin(
        meta,
        qengine.clone(),
        &config.default_project_id,
        &config.default_api_key,
        config.admin_token.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    control.set_outbox_path(config.data_dir.join("outbox"));
    let bootstrap_credentials = control.take_bootstrap_credentials();
    if let Some(tok) = &bootstrap_credentials.generated_admin_token {
        // Logs get shipped and shared, so the root token goes to an owner-only file instead.
        let path = config.data_dir.join("root_admin_token");
        write_owner_only(&path, tok)?;
        info!(
            "Generated the root admin token and wrote it to {} (owner-only). Save it somewhere safe, then delete the file.",
            path.display()
        );
    }
    if let Some(pk) = &bootstrap_credentials.generated_ingest_key {
        info!(
            project_id = %config.default_project_id,
            "Generated the public ingest key for client SDKs: {pk}"
        );
    }

    let ingest = IngestService::new(storage.clone(), control.clone());
    control.attach_ingest(ingest.clone()).await;

    if config.auto_seed && config.role.serves_ingest() {
        // Each process gets a new node id, so after a crash this node waits out the lease its
        // previous run left behind. A standby that never gets the lease skips seeding.
        let wait = writer_leases.as_ref().map_or(Duration::ZERO, |l| {
            use pulse_storage::WriterLeases;
            l.ttl() + Duration::from_secs(2)
        });
        let deadline = std::time::Instant::now() + wait;
        let opened = loop {
            match storage
                .get_or_open_project(&config.default_project_id)
                .await
            {
                Err(pulse_storage::StorageError::NotWriter { .. })
                    if std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                other => break other,
            }
        };
        match opened {
            Ok(ps) => {
                let existing_rows =
                    ps.total_l0_rows() as u64 + ps.snapshot().manifest.total_l1_rows();
                if existing_rows == 0 {
                    seed_indian_fintech_dataset(&control, &ingest, &config.default_project_id)
                        .await?;
                    seeder::seed_agent_demo(&control, &config.default_project_id).await?;
                }
            }
            Err(pulse_storage::StorageError::NotWriter { .. }) => tracing::warn!(
                project_id = %config.default_project_id,
                "another node holds the writer lease; not seeding"
            ),
            Err(e) => return Err(e.into()),
        }
    }

    if config.role.serves_query_and_admin() {
        let alerts_control = control.clone();
        let mut alerts_shutdown = shutdown_bg_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        alerts_control.evaluate_alerts().await;
                        alerts_control.evaluate_agent_alerts().await;
                        alerts_control.prune_expired_agent_data().await;
                    }
                    changed = alerts_shutdown.changed() => {
                        if changed.is_err() || *alerts_shutdown.borrow() { break; }
                    }
                }
            }
        });
    }
    if config.role != NodeRole::Compactor {
        control.spawn_agent_eval_worker(shutdown_bg_rx.clone());
    }
    if config.role.serves_ingest() {
        // The agent trace store only flushes and compacts when asked; writers own both.
        // Flush runs on its own task: acknowledged spans pile up in memory until it does, so
        // a long compaction pass must never hold it back.
        let flush_control = control.clone();
        let mut flush_shutdown = shutdown_bg_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    // The timer, or the append path asking early because a shard or the
                    // memtable as a whole passed its byte threshold.
                    _ = async {
                        let store = flush_control.agent_store();
                        tokio::select! {
                            _ = ticker.tick() => {}
                            _ = store.flush_requested() => {}
                        }
                    } => {
                        if let Err(e) = flush_control.agent_store().flush_l0().await {
                            tracing::warn!(error = %e, "agent L0 flush failed");
                        }
                    }
                    changed = flush_shutdown.changed() => {
                        if changed.is_err() || *flush_shutdown.borrow() { break; }
                    }
                }
            }
        });
        let agent_control = control.clone();
        let mut agent_shutdown = shutdown_bg_rx.clone();
        // On `all` the alerts loop above already prunes; a query node's store is read-only,
        // so with split roles the ingest node is the only one that can delete expired segments.
        let prunes_here = config.role == NodeRole::Ingest;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ticks: u64 = 0;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // ponytail: compaction every minute on each writer; the manifest CAS
                        // settles races between ingest nodes. Elect one compactor if that churns.
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        match agent_control.meta().list_all_active_erasure_tombstones(now_ms).await {
                            Ok(tombstones) => {
                                if let Err(e) = agent_control.agent_store().compact_and_gc(None, &tombstones).await {
                                    tracing::warn!(error = %e, "agent compaction failed");
                                }
                            }
                            Err(e) => tracing::warn!(error = %e, "loading agent erasure tombstones failed"),
                        }
                        ticks += 1;
                        if prunes_here && ticks % 5 == 0 {
                            agent_control.prune_expired_agent_data().await;
                        }
                    }
                    changed = agent_shutdown.changed() => {
                        if changed.is_err() || *agent_shutdown.borrow() { break; }
                    }
                }
            }
        });
    }
    let _bg_handle = if config.role == NodeRole::All {
        let keyring_control = control.clone();
        let mut keyring_shutdown = shutdown_bg_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = keyring_control.refresh_encryption_keyring().await {
                            tracing::warn!(error = %e, "failed to refresh BYOK keyring");
                        }
                    }
                    changed = keyring_shutdown.changed() => {
                        if changed.is_err() || *keyring_shutdown.borrow() { break; }
                    }
                }
            }
        });
        storage.spawn_background_maintenance(shutdown_bg_rx)
    } else {
        let bg_storage = storage.clone();
        let bg_control = control.clone();
        let role = config.role;
        tokio::spawn(async move {
            let mut ticker = if role == NodeRole::Query {
                if let Some(secs) = std::env::var("PULSE_QUERY_REFRESH_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                {
                    let dur = Duration::from_secs(secs.max(1));
                    tokio::time::interval_at(tokio::time::Instant::now() + dur, dur)
                } else {
                    tokio::time::interval(Duration::from_secs(5))
                }
            } else {
                tokio::time::interval(Duration::from_secs(5))
            };
            let interval = bg_storage.upload_interval();
            let cold_interval = bg_storage.cold_merge_interval();
            let cold_on_publish =
                role == NodeRole::Ingest && !interval.is_zero() && cold_interval == interval;
            let mut publish_ticker = (role == NodeRole::Ingest && !interval.is_zero()).then(|| {
                let mut t =
                    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
                t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                t
            });
            let mut cold_merge_ticker = (role == NodeRole::Ingest
                && !cold_interval.is_zero()
                && !cold_on_publish)
                .then(|| {
                    let mut t = tokio::time::interval_at(
                        tokio::time::Instant::now() + cold_interval,
                        cold_interval,
                    );
                    t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    t
                });
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match role {
                            NodeRole::All => {}
                            NodeRole::Ingest => {
                                let errs = bg_storage.maintenance_errors();
                                errs.check(MaintenanceKind::IdentityFlush, bg_storage.identity_router().flush_all().await);
                                for p in bg_storage.open_projects() {
                                    errs.check(MaintenanceKind::Flush, p.flush_due_buckets().await);
                                }
                                errs.check(MaintenanceKind::Compaction, bg_storage.run_gc_all_projects().await);
                                if let Err(e) = bg_control.reload_caches_from_db().await {
                                    tracing::warn!(error = %e, "reloading API key cache failed");
                                }
                            }
                            NodeRole::Compactor => {
                                if let Err(e) = bg_control.refresh_encryption_keyring().await {
                                    tracing::warn!(error = %e, "compactor failed to refresh BYOK keyring");
                                }
                                let errs = bg_storage.maintenance_errors();
                                errs.check(MaintenanceKind::ManifestRefresh, bg_storage.refresh_all_manifests().await);
                                // Erasure tombstones decide which rows compaction drops.
                                if let Err(e) = bg_control.reload_caches_from_db().await {
                                    tracing::warn!(error = %e, "compactor failed to reload control state");
                                }
                                // Compaction walks open projects, and nothing else on this
                                // role opens one.
                                // ponytail: every project stays open on the compactor. Shard
                                // projects across compactors when that no longer fits in RAM.
                                match bg_control.meta().list_projects().await {
                                    Ok(projects) => {
                                        for p in projects {
                                            if let Err(e) = bg_storage.get_or_open_project(&p.id).await {
                                                tracing::warn!(project_id = %p.id, error = %e, "compactor failed to open project");
                                            }
                                        }
                                    }
                                    Err(e) => tracing::warn!(error = %e, "compactor failed to list projects"),
                                }
                                errs.check(MaintenanceKind::Compaction, bg_storage.compact_all_projects().await);
                            }
                            NodeRole::Query => {
                                run_query_refresh_tick(&bg_control, &bg_storage).await;
                            }
                        }
                    }
                    _ = async {
                        match publish_ticker.as_mut() {
                            Some(t) => t.tick().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if cold_on_publish {
                            bg_storage.maintenance_errors().check(
                                MaintenanceKind::Compaction,
                                bg_storage.cold_merge_all_projects().await,
                            );
                        }
                        bg_storage
                            .maintenance_errors()
                            .check(MaintenanceKind::Publish, bg_storage.publish_all_projects().await);
                    }
                    _ = async {
                        match cold_merge_ticker.as_mut() {
                            Some(t) => t.tick().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        bg_storage.maintenance_errors().check(
                            MaintenanceKind::Compaction,
                            bg_storage.cold_merge_all_projects().await,
                        );
                    }
                    _ = bg_storage.upload_requested(), if role == NodeRole::Ingest => {
                        bg_storage
                            .maintenance_errors()
                            .check(MaintenanceKind::Publish, bg_storage.publish_all_projects().await);
                    }
                    changed = shutdown_bg_rx.changed() => {
                        if changed.is_err() || *shutdown_bg_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    };

    let quic = if config.role.serves_ingest() {
        if let Some(qaddr) = config.quic_addr {
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
        }
    } else {
        None
    };

    // Public SDK routes (permissive CORS) vs Protected Admin/Query routes (restricted CORS + Bearer auth, `P0-01`)
    let ready = Arc::new(AtomicBool::new(true));
    let http_metrics = Arc::new(metrics::HttpMetrics::default());
    let mut api_router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(metrics::router(metrics::Probe {
            http: http_metrics.clone(),
            ready: ready.clone(),
            control: control.clone(),
            ingest: ingest.clone(),
            storage: storage.clone(),
            qengine: qengine.clone(),
            token: std::env::var("PULSE_METRICS_TOKEN")
                .ok()
                .filter(|t| !t.trim().is_empty()),
        }));
    if config.role.serves_ingest() {
        let mut ingest_routes = Router::new()
            .merge(build_ingest_router(ingest.clone()))
            .merge(build_agent_ingest_router(control.clone()));
        if let Some(cap) = ingest_inflight_cap() {
            ingest_routes = ingest_routes.layer(axum::middleware::from_fn_with_state(
                Arc::new(tokio::sync::Semaphore::new(cap)),
                shed_ingest_when_saturated,
            ));
        }
        let ingest_public = ingest_routes
            .merge(build_public_flag_router(control.clone()))
            .layer(CorsLayer::permissive());
        api_router = api_router.merge(ingest_public);
    } else if config.role.serves_query_and_admin() {
        let flag_public = build_public_flag_router(control.clone()).layer(CorsLayer::permissive());
        api_router = api_router.merge(flag_public);
    }
    if config.role.serves_query_and_admin() {
        let admin_router = build_admin_control_router(control.clone())
            .layer(axum::middleware::from_fn(security_headers_middleware))
            .layer(build_admin_cors_layer(&config.allowed_origins));
        api_router = api_router.merge(admin_router);
    }

    let index_file = config.web_dist_dir.join("index.html");
    let app = if config.role.serves_query_and_admin() {
        let spa_router = if index_file.exists() {
            let serve_dir =
                ServeDir::new(&config.web_dist_dir).fallback(ServeFile::new(index_file));
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
            Router::new().fallback_service(cached_dir)
        } else {
            Router::new().fallback(get(|| async {
                Html(
                    "<!doctype html><html><head><title>Pulse Analytics</title></head>\
                     <body style='background:#090d16;color:#e2e8f0;font-family:sans-serif;padding:2rem'>\
                     <h1>Pulse Analytics Node Running</h1>\
                     <p>API &amp; Query Engine online. Run <code>npm --prefix web run build</code> to bundle Analytics Studio.</p>\
                     </body></html>",
                )
            }))
        };
        let spa_router = spa_router
            .layer(axum::middleware::from_fn(security_headers_middleware))
            .layer(axum::middleware::from_fn(
                |req: axum::extract::Request, next: axum::middleware::Next| async move {
                    if req.uri().path().starts_with("/v1/") {
                        return StatusCode::NOT_FOUND.into_response();
                    }
                    next.run(req).await
                },
            ));
        api_router.fallback_service(spa_router)
    } else {
        api_router
    };
    // gzip/br for the web app and JSON. Replay frames are served by byte range, and
    // compressing a range response would corrupt it, so they stay as they are.
    let app = app
        .layer(CompressionLayer::new().compress_when(
            DefaultPredicate::new().and(NotForContentType::const_new("application/x-pulse-frame")),
        ))
        .layer(axum::middleware::from_fn_with_state(
            http_metrics,
            metrics::track,
        ))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    let bound_http_addr = listener.local_addr()?;
    info!(
        role = config.role.as_str(),
        http = %bound_http_addr,
        "Pulse Analytics HTTP/2 server listening"
    );

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
        writer_leases,
        shutdown_bg_tx,
        shutdown_http_tx: Some(shutdown_http_tx),
        http_task,
        ready,
    })
}

/// Summary of one project restored by [`restore_from_published`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredProjectReport {
    pub project_id: String,
    pub publish_seq: u64,
    pub published_at_ms: i64,
    pub high_water_mark: u64,
    pub identity_user_count: u64,
    pub erased_profiles_purged: u64,
    pub erased_tombstones_deleted: u64,
}

/// Explicitly restores local hot-tier state (`identity/`, `profiles/`, `manifests/`, and
/// `<data_dir>/hot_store_mode`) from the newest `published/` manifest per project in the object
/// store, purges erased profiles from restored snapshots, and deletes post-publish `erased_uids`
/// (`uid >= high_water_mark`) while keeping `deletion_audit_log` intact.
///
/// Refuses SQLite metadata databases before touching `data_dir`.
pub async fn restore_from_published(
    data_dir: &std::path::Path,
    project_id_filter: Option<&str>,
) -> anyhow::Result<Vec<RestoredProjectReport>> {
    let db_url = std::env::var("DATABASE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "restore --from-published refuses SQLite metadata DB: DATABASE_URL is not set. \
                 Accounts, projects, and erased_uids tombstones on SQLite are lost with the volume; \
                 set DATABASE_URL to your external PostgreSQL database (`postgres://...`)."
            )
        })?;
    if !(db_url.starts_with("postgres://") || db_url.starts_with("postgresql://")) {
        anyhow::bail!(
            "restore --from-published refuses SQLite or non-PostgreSQL metadata DB: \
             set DATABASE_URL to a `postgres://` or `postgresql://` connection string"
        );
    }
    let meta = MetaStore::connect_url(&db_url).await?;
    if meta.is_sqlite() {
        anyhow::bail!(
            "restore --from-published refuses SQLite metadata DB; set DATABASE_URL to PostgreSQL"
        );
    }

    std::fs::create_dir_all(data_dir)?;
    let raw_s3 = build_object_store(data_dir)?;
    restore_from_published_inner(data_dir, project_id_filter, raw_s3, &meta).await
}

#[doc(hidden)]
pub async fn restore_from_published_with_store_and_meta_for_test(
    data_dir: &std::path::Path,
    project_id_filter: Option<&str>,
    raw_s3: Arc<dyn object_store::ObjectStore>,
    meta: &MetaStore,
) -> anyhow::Result<Vec<RestoredProjectReport>> {
    std::fs::create_dir_all(data_dir)?;
    restore_from_published_inner(data_dir, project_id_filter, raw_s3, meta).await
}

async fn restore_from_published_inner(
    data_dir: &std::path::Path,
    project_id_filter: Option<&str>,
    raw_s3: Arc<dyn object_store::ObjectStore>,
    meta: &MetaStore,
) -> anyhow::Result<Vec<RestoredProjectReport>> {
    let routed = pulse_storage::open_hot_store_for_restore(raw_s3, data_dir).await?;
    let enc = Arc::new(pulse_storage::EncryptedObjectStore::new(routed));
    if !meta.list_system_keyring().await?.is_empty() {
        pulse_control::restore_encryption_keyring_to_store(meta, &enc)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    let enc_store = enc.as_object_store();

    let project_ids: Vec<String> = if let Some(pid) = project_id_filter {
        vec![pid.to_string()]
    } else {
        let listing = enc_store
            .list_with_delimiter(Some(&object_store::path::Path::from("published")))
            .await?;
        let mut ids: Vec<String> = listing
            .common_prefixes
            .into_iter()
            .filter_map(|p| p.parts().nth(1).map(|seg| seg.as_ref().to_string()))
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };

    let mut reports = Vec::new();
    for project_id in project_ids {
        let Some(pub_m) =
            pulse_storage::PublishedManifestStore::new(&project_id, enc_store.clone())
                .load_latest()
                .await?
        else {
            if project_id_filter.is_some() {
                anyhow::bail!("no published manifest found for project `{project_id}`");
            }
            continue;
        };

        // Clear any stale local WAL, hot segments, and upload intent from an earlier partial volume before restoring.
        pulse_storage::delete_objects_with_prefix(&enc_store, &format!("wal/{project_id}")).await?;
        pulse_storage::delete_objects_with_prefix(
            &enc_store,
            &format!("hot/segments/{project_id}"),
        )
        .await?;
        pulse_storage::UploadIntentStore::new(&project_id, enc_store.clone())
            .clear()
            .await?;
        pulse_storage::ColdMergeIntentStore::new(&project_id, enc_store.clone())
            .clear()
            .await?;

        let restored =
            pulse_storage::restore_project_from_published(&project_id, &pub_m, enc_store.clone())
                .await?;

        let erased: pulse_storage::ErasedUids = meta
            .list_erased_uids(&project_id)
            .await?
            .into_iter()
            .collect();
        let mut erased_profiles_purged = 0u64;
        if !erased.is_empty() {
            let expanded =
                pulse_storage::expand_erased_with_identity(&erased, Some(&restored.identity_graph));
            if let Some(ref pstore) = restored.profile_store {
                let all_profiles = pstore.list_updated_since(0).await?;
                let to_delete: Vec<String> = all_profiles
                    .into_iter()
                    .filter(|prof| {
                        pulse_storage::is_profile_erased(
                            prof,
                            &expanded,
                            Some(&restored.identity_graph),
                        )
                    })
                    .map(|prof| prof.distinct_id)
                    .collect();
                for chunk in to_delete.chunks(10_000) {
                    let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
                    erased_profiles_purged += pstore.delete_for_ids(&refs).await?;
                    let _ = meta
                        .delete_user_profiles_for_ids(&project_id, chunk)
                        .await?;
                }
            }
        }

        if let Some(pstore) = restored.profile_store {
            pstore.close().await?;
        }
        restored.identity_graph.close().await?;

        let erased_tombstones_deleted = meta
            .delete_erased_uids_from_hwm(&project_id, pub_m.high_water_mark)
            .await?;

        reports.push(RestoredProjectReport {
            project_id,
            publish_seq: pub_m.publish_seq,
            published_at_ms: pub_m.published_at_ms,
            high_water_mark: pub_m.high_water_mark,
            identity_user_count: restored.identity_user_count,
            erased_profiles_purged,
            erased_tombstones_deleted,
        });
    }

    if reports.is_empty() {
        anyhow::bail!("no published manifests found under `published/` in object store");
    }

    pulse_storage::finalize_local_hot_store_marker(data_dir).await?;
    Ok(reports)
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeadLetterItem {
    pub object_path: String,
    pub bucket_id: u8,
    pub wal_seq: u64,
    pub recorded_at_ms: i64,
    pub reason: String,
    pub event: pulse_wire::WireEvent,
}

/// Lists dead-lettered events for `project_id` under `wal_dead/{project_id}/` without mutating
/// `<data_dir>` or fencing an active writer, filtering out events belonging to erased users
/// (`client_event_ts_ms <= erased_at_ms`).
pub async fn inspect_dead_letters(
    data_dir: &std::path::Path,
    project_id: &str,
    bucket_filter: Option<u8>,
    limit: Option<usize>,
) -> anyhow::Result<Vec<DeadLetterItem>> {
    let raw_s3 = pulse_storage::build_object_store_for_inspect(data_dir)?;
    let db_url = std::env::var("DATABASE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let meta = if let Some(url) = db_url {
        Some(MetaStore::connect_url(&url).await?)
    } else if data_dir.join("pulse_meta.db").is_file() {
        Some(MetaStore::connect_auto(data_dir).await?)
    } else {
        None
    };
    inspect_dead_letters_inner(
        data_dir,
        project_id,
        bucket_filter,
        limit,
        raw_s3,
        meta.as_ref(),
    )
    .await
}

#[doc(hidden)]
pub async fn inspect_dead_letters_with_store_and_meta_for_test(
    data_dir: &std::path::Path,
    project_id: &str,
    bucket_filter: Option<u8>,
    limit: Option<usize>,
    raw_s3: Arc<dyn object_store::ObjectStore>,
    meta: Option<&MetaStore>,
) -> anyhow::Result<Vec<DeadLetterItem>> {
    inspect_dead_letters_inner(data_dir, project_id, bucket_filter, limit, raw_s3, meta).await
}

async fn inspect_dead_letters_inner(
    data_dir: &std::path::Path,
    project_id: &str,
    bucket_filter: Option<u8>,
    limit: Option<usize>,
    raw_s3: Arc<dyn object_store::ObjectStore>,
    meta: Option<&MetaStore>,
) -> anyhow::Result<Vec<DeadLetterItem>> {
    let routed = pulse_storage::open_hot_store_for_inspect(raw_s3, data_dir).await?;
    let enc = Arc::new(pulse_storage::EncryptedObjectStore::new(routed));
    if let Some(m) = meta {
        if !m.list_system_keyring().await?.is_empty() {
            pulse_control::restore_encryption_keyring_to_store(m, &enc)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }
    }
    let enc_store = enc.as_object_store();

    let entries = pulse_storage::list_dead_letters(&enc_store, project_id, bucket_filter).await?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let erased: pulse_storage::ErasedUids = if let Some(m) = meta {
        m.list_erased_uids(project_id).await?.into_iter().collect()
    } else {
        pulse_storage::ErasedUids::default()
    };

    let (id_graph_opt, expanded) = if !erased.is_empty() {
        let g = pulse_identity::ProjectIdentityGraph::open_read_only(project_id, enc_store.clone())
            .await?;
        let exp = pulse_storage::expand_erased_with_identity(&erased, Some(&g));
        (Some(g), exp)
    } else {
        (None, erased)
    };

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Some(ref g) = id_graph_opt {
            let raw_uid = g.lookup_raw(&entry.event.distinct_id);
            let canon_uid = g.lookup_canonical(&entry.event.distinct_id);
            let hidden = raw_uid.is_some_and(|u| expanded.hides(u, entry.event.client_event_ts_ms))
                || canon_uid.is_some_and(|u| expanded.hides(u, entry.event.client_event_ts_ms));
            if hidden {
                continue;
            }
        }
        out.push(DeadLetterItem {
            object_path: entry.object_path,
            bucket_id: entry.bucket_id,
            wal_seq: entry.wal_seq,
            recorded_at_ms: entry.recorded_at_ms,
            reason: entry
                .reason
                .unwrap_or_else(|| "unknown (legacy PLSB)".to_string()),
            event: entry.event,
        });
        if let Some(lim) = limit {
            if out.len() >= lim {
                break;
            }
        }
    }

    Ok(out)
}

fn write_owner_only(path: &std::path::Path, contents: &str) -> anyhow::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    let mut f = opts.open(path)?;
    f.write_all(contents.as_bytes())?;
    f.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pulse_sdk_core::{PulseClient, SdkConfig, SdkWireNode};
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn secure_node_config_defaults() {
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
            role: NodeRole::All,
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

        // 6. Invoke MCP JSON-RPC 2.0 `initialize`, `tools/list` and `tools/call` over authenticated HTTP
        let mcp_init: Value = http
            .post(format!("{base_url}/v1/mcp"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": { "protocolVersion": "2025-06-18", "capabilities": {} }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(mcp_init["result"]["protocolVersion"], "2025-06-18");
        let notif = http
            .post(format!("{base_url}/v1/mcp"))
            .bearer_auth(&admin_token)
            .json(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
            .send()
            .await
            .unwrap();
        assert_eq!(notif.status(), reqwest::StatusCode::ACCEPTED);
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
        let tool_names: Vec<&str> = mcp_list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert_eq!(
            tool_names,
            [
                "get_lexicon_schema",
                "query_insights",
                "query_funnel",
                "query_retention",
                "inspect_frustrated_sessions",
                "query_flows",
                "get_user",
                "list_property_values",
                "list_cohorts",
                "list_agent_traces",
                "get_agent_trace",
                "query_agent_analytics",
                "list_agent_evaluations"
            ]
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

    #[tokio::test]
    async fn ha_02_split_ingest_and_query_nodes_share_storage_without_fencing() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let shared_data_dir = tmp_dir.path().join("shared_cluster_data");
        let admin_token = "sk_root_ha_split_test_token".to_string();

        let ingest_node = start_pulse_node(NodeConfig {
            role: NodeRole::Ingest,
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: shared_data_dir.clone(),
            web_dist_dir: tmp_dir.path().join("no_web"),
            default_project_id: "proj_ha_split".to_string(),
            default_api_key: String::new(),
            admin_token: Some(admin_token.clone()),
            allowed_origins: vec!["http://127.0.0.1:8080".to_string()],
            auto_seed: false,
            num_buckets: 8,
        })
        .await
        .expect("start ingest node");

        let query_node = start_pulse_node(NodeConfig {
            role: NodeRole::Query,
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: shared_data_dir.clone(),
            web_dist_dir: tmp_dir.path().join("no_web"),
            default_project_id: "proj_ha_split".to_string(),
            default_api_key: String::new(),
            admin_token: Some(admin_token.clone()),
            allowed_origins: vec!["http://127.0.0.1:8080".to_string()],
            auto_seed: false,
            num_buckets: 8,
        })
        .await
        .expect("start query node");

        let ingest_key = ingest_node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .expect("generated ingest key on first boot");
        let ingest_url = format!("http://{}", ingest_node.http_addr);
        let query_url = format!("http://{}", query_node.http_addr);
        let http = reqwest::Client::new();

        // 1. Verify strict role router isolation
        let ingest_status = http
            .get(format!("{ingest_url}/v1/status"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap();
        assert_eq!(ingest_status.status(), reqwest::StatusCode::NOT_FOUND);

        let query_ingest = http
            .post(format!("{query_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({ "events": [] }))
            .send()
            .await
            .unwrap();
        assert_eq!(query_ingest.status(), reqwest::StatusCode::NOT_FOUND);

        // 2. Open project on Query node BEFORE any data is ingested (read-only SlateDB must not error or fence)
        let pre_status: Value = http
            .get(format!("{query_url}/v1/status"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(pre_status["l1_rows"].as_u64(), Some(0));

        // 3. Send events + /v1/identify to Ingest node
        let now_ms = chrono::Utc::now().timestamp_millis();
        let pre_login = http
            .post(format!("{ingest_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "events": [
                    {
                        "distinct_id": "anon_ha_01",
                        "event_name": "App_Open",
                        "client_event_ts_ms": now_ms - 5_000
                    },
                    {
                        "distinct_id": "anon_ha_01",
                        "event_name": "KYC_PAN_Submitted",
                        "client_event_ts_ms": now_ms - 3_000
                    }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(pre_login.status(), reqwest::StatusCode::OK);

        let identify = http
            .post(format!("{ingest_url}/v1/identify"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "anon_id": "anon_ha_01",
                "user_id": "user_ha_01"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(identify.status(), reqwest::StatusCode::OK);

        let post_login = http
            .post(format!("{ingest_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "events": [
                    {
                        "distinct_id": "user_ha_01",
                        "event_name": "First_Disbursal_Success",
                        "client_event_ts_ms": now_ms - 1_000
                    }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(post_login.status(), reqwest::StatusCode::OK);

        // 4. Flush Ingest node and refresh Query node manifests + read-only identity graph
        ingest_node.ingest.flush_all().await.unwrap();
        ingest_node.storage.flush_all_projects().await.unwrap();
        query_node.storage.refresh_all_manifests().await.unwrap();

        let post_status: Value = http
            .get(format!("{query_url}/v1/status"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(post_status["l1_rows"].as_u64(), Some(3));
        assert_eq!(post_status["interned_users"].as_u64(), Some(2));

        // 5. Query stitched 3-step funnel on Query node
        let funnel_resp = http
            .post(format!("{query_url}/v1/query"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "kind": "funnel",
                "query": {
                    "time_range": {
                        "start_ts_ms": now_ms - 60_000,
                        "end_ts_ms": now_ms + 60_000
                    },
                    "steps": [
                        { "event_name": "App_Open" },
                        { "event_name": "KYC_PAN_Submitted" },
                        { "event_name": "First_Disbursal_Success" }
                    ],
                    "order_mode": "LooseOrder",
                    "conversion_window_ms": 86_400_000
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(funnel_resp.status(), reqwest::StatusCode::OK);
        let funnel_json: Value = funnel_resp.json().await.unwrap();
        let steps = funnel_json["overall_steps"]
            .as_array()
            .expect("overall_steps");
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0]["entered_users"].as_u64(), Some(1));
        assert_eq!(steps[2]["entered_users"].as_u64(), Some(1));

        // 6. Verify Ingest node writer was NOT fenced by Query node
        let post_query_ingest = http
            .post(format!("{ingest_url}/v1/ingest"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({
                "events": [
                    {
                        "distinct_id": "user_ha_02",
                        "event_name": "App_Open",
                        "client_event_ts_ms": now_ms
                    }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(post_query_ingest.status(), reqwest::StatusCode::OK);
        ingest_node.ingest.flush_all().await.unwrap();
        ingest_node.storage.flush_all_projects().await.unwrap();

        // 7. A profile written through the ingest node reaches the query node's catalog on its
        //    background reload (B-11: query nodes used to never reload profiles or erasures).
        let engage = http
            .post(format!("{ingest_url}/v1/engage"))
            .header("x-pulse-key", &ingest_key)
            .json(&json!({ "distinct_id": "user_ha_01", "$set": { "plan": "gold" } }))
            .send()
            .await
            .unwrap();
        assert_eq!(engage.status(), reqwest::StatusCode::OK);
        let gold_users = || async {
            let v: Value = http
                .post(format!("{query_url}/v1/query"))
                .bearer_auth(&admin_token)
                .json(&json!({
                    "kind": "insights",
                    "query": {
                        "time_range": { "start_ts_ms": now_ms - 60_000, "end_ts_ms": now_ms + 60_000 },
                        "interval": "day",
                        "series": [{ "label": "A", "event_name": "App_Open", "metric": "uniques",
                            "filters": [{ "prop_name": "user.plan", "filter": { "op": "eq", "val": "gold" } }] }]
                    }
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            v["series"][0]["overall_value"].as_f64().unwrap_or(0.0)
        };
        let mut seen = 0.0;
        for _ in 0..30 {
            seen = gold_users().await;
            if seen == 1.0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(
            seen, 1.0,
            "query node never picked up the profile written via ingest"
        );

        query_node.shutdown().await;
        ingest_node.shutdown().await;
    }

    /// A `compactor` node shares storage with an ingest node and must merge the segments the
    /// ingest node flushes. It only ever looked at projects it had open, and opened none.
    #[tokio::test]
    async fn compactor_role_compacts_projects_written_by_an_ingest_node() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let shared = tmp_dir.path().join("shared");
        let cfg = |role| NodeConfig {
            role,
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: shared.clone(),
            web_dist_dir: tmp_dir.path().join("no_web"),
            default_project_id: "proj_compact".to_string(),
            default_api_key: String::new(),
            admin_token: Some("sk_root_compactor_test_token".to_string()),
            allowed_origins: vec![],
            auto_seed: false,
            num_buckets: 1,
        };
        let ingest_node = start_pulse_node(cfg(NodeRole::Ingest)).await.unwrap();
        // Count-based compaction on S3 segments is active when PULSE_UPLOAD_INTERVAL_SECS=0.
        ingest_node.storage.set_upload_interval(Duration::ZERO);
        let key = ingest_node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let http = reqwest::Client::new();
        let now_ms = chrono::Utc::now().timestamp_millis();
        // Four flushes into the same (day, bucket): enough to be due for compaction.
        for i in 0..4 {
            let resp = http
                .post(format!("http://{}/v1/ingest", ingest_node.http_addr))
                .header("x-pulse-key", &key)
                .json(&json!({ "events": [{
                    "distinct_id": format!("user_{i}"),
                    "event_name": "App_Open",
                    "client_event_ts_ms": now_ms - i,
                }]}))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
            ingest_node.ingest.flush_all().await.unwrap();
            ingest_node.storage.flush_all_projects().await.unwrap();
        }
        let ps = ingest_node
            .storage
            .get_or_open_project("proj_compact")
            .await
            .unwrap();
        assert_eq!(ps.snapshot().manifest.total_segments(), 4);

        let compactor = start_pulse_node(cfg(NodeRole::Compactor)).await.unwrap();
        compactor.storage.set_upload_interval(Duration::ZERO);
        let mut segments = 4;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            ps.refresh_manifest().await.unwrap();
            segments = ps.snapshot().manifest.total_segments();
            if segments == 1 {
                break;
            }
        }
        assert_eq!(segments, 1, "compactor node never merged the 4 segments");
        assert_eq!(ps.snapshot().manifest.total_l1_rows(), 4);

        compactor.shutdown().await;
        ingest_node.shutdown().await;
    }

    /// Two ingest-capable nodes on one data dir used to both acknowledge writes for the same
    /// project and overwrite each other's WAL objects. Only the lease holder may write.
    #[tokio::test]
    async fn second_ingest_node_refuses_writes_until_the_writer_releases_its_lease() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let shared = tmp_dir.path().join("shared");
        let cfg = || NodeConfig {
            role: NodeRole::Ingest,
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: shared.clone(),
            web_dist_dir: tmp_dir.path().join("no_web"),
            default_project_id: "proj_lease".to_string(),
            default_api_key: String::new(),
            admin_token: Some("sk_root_writer_lease_test_token".to_string()),
            allowed_origins: vec![],
            auto_seed: false,
            num_buckets: 1,
        };
        let node_a = start_pulse_node(cfg()).await.unwrap();
        let key = node_a
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .unwrap();
        let node_b = start_pulse_node(cfg()).await.unwrap();
        let http = reqwest::Client::new();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let post = |addr: SocketAddr, user: &'static str| {
            http.post(format!("http://{addr}/v1/ingest"))
                .header("x-pulse-key", &key)
                .json(&json!({ "events": [{
                    "distinct_id": user,
                    "event_name": "App_Open",
                    "client_event_ts_ms": now_ms,
                }]}))
                .send()
        };
        for user in ["a1", "a2", "a3"] {
            let resp = post(node_a.http_addr, user).await.unwrap();
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
        }

        let refused = post(node_b.http_addr, "b1").await.unwrap();
        assert_eq!(refused.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value = refused.json().await.unwrap();
        assert_eq!(body["code"], "not_writer");
        // The writer is unaffected by the refused attempt.
        let resp = post(node_a.http_addr, "a4").await.unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        // ...and by the second node having started: its profile store stays the writer's. SlateDB
        // notices a fenced handle on its next poll (1 s), so give it time to.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let engaged = http
            .post(format!("http://{}/v1/engage", node_a.http_addr))
            .header("x-pulse-key", &key)
            .json(&json!({ "distinct_id": "a1", "$set": { "tier": "gold" } }))
            .send()
            .await
            .unwrap();
        assert_eq!(engaged.status(), reqwest::StatusCode::OK);

        // A clean shutdown flushes and releases the lease, so the standby takes over at once.
        node_a.shutdown().await;
        let resp = post(node_b.http_addr, "b1").await.unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        node_b.ingest.flush_all().await.unwrap();
        let ps = node_b.storage.open_for_read("proj_lease").await.unwrap();
        let rows = ps.snapshot().manifest.total_l1_rows() as usize + ps.total_l0_rows();
        assert_eq!(rows, 5, "takeover lost or duplicated acknowledged events");
        let profile = node_b
            .storage
            .profile_store("proj_lease", false)
            .await
            .unwrap()
            .get_by_distinct_id("a1")
            .await
            .unwrap();
        assert!(
            profile.is_some(),
            "the writer's profile update was lost: the second node took its profile store"
        );
        node_b.shutdown().await;
    }

    #[tokio::test]
    async fn ingest_sheds_with_503_once_inflight_cap_is_reached() {
        use tower::ServiceExt;
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::channel::<()>(1);
        let release = Arc::new(tokio::sync::Notify::new());
        let gate = release.clone();
        let app = Router::new()
            .route(
                "/v1/ingest",
                axum::routing::post(move || {
                    let (entered_tx, gate) = (entered_tx.clone(), gate.clone());
                    async move {
                        let _ = entered_tx.send(()).await;
                        gate.notified().await;
                        "ok"
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(tokio::sync::Semaphore::new(1)),
                shed_ingest_when_saturated,
            ));
        let post = || {
            axum::http::Request::post("/v1/ingest")
                .body(axum::body::Body::empty())
                .unwrap()
        };

        let first = tokio::spawn(app.clone().oneshot(post()));
        entered_rx.recv().await.expect("first request is in flight");
        let shed = app.clone().oneshot(post()).await.unwrap();
        assert_eq!(shed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(shed.headers().get(header::RETRY_AFTER).unwrap(), "1");

        release.notify_one();
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
        // The slot is free again.
        release.notify_one();
        assert_eq!(app.oneshot(post()).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_and_readyz_report_traffic_and_readiness() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let node = start_pulse_node(NodeConfig {
            role: NodeRole::All,
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: tmp_dir.path().join("node_data"),
            web_dist_dir: tmp_dir.path().join("no_web"),
            default_project_id: "proj_metrics".to_string(),
            default_api_key: String::new(),
            admin_token: None,
            allowed_origins: vec![],
            auto_seed: false,
            num_buckets: 4,
        })
        .await
        .expect("start pulse-node");
        let base = format!("http://{}", node.http_addr);
        let http = reqwest::Client::new();

        let ready = http.get(format!("{base}/readyz")).send().await.unwrap();
        assert_eq!(ready.status(), reqwest::StatusCode::OK);
        // A templated route is labelled by its template, not the raw path.
        let _ = http
            .get(format!("{base}/v1/projects/proj_secret_123/insights"))
            .send()
            .await
            .unwrap();
        let _ = http.get(format!("{base}/healthz")).send().await.unwrap();

        let body = http
            .get(format!("{base}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            body.contains(
                r#"pulse_http_requests_total{route="/healthz",method="GET",status="200"} 1"#
            ),
            "{body}"
        );
        assert!(body.contains(r#"route="/readyz""#), "{body}");
        assert!(
            body.contains("pulse_ingest_events_accepted_total 0"),
            "{body}"
        );
        assert!(
            body.contains(r#"pulse_maintenance_errors_total{kind="flush"} 0"#),
            "{body}"
        );
        assert!(body.contains("pulse_ready 1"), "{body}");
        assert!(
            !body.contains("proj_secret_123"),
            "raw paths must not become labels: {body}"
        );

        // Draining: readiness flips before the listener closes.
        node.ready.store(false, Ordering::Relaxed);
        let draining = http.get(format!("{base}/readyz")).send().await.unwrap();
        assert_eq!(draining.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(draining.text().await.unwrap().contains("draining"));

        node.shutdown().await;
    }

    #[tokio::test]
    async fn security_headers_on_spa_and_admin_without_breaking_public_sdk() {
        let tmp_dir = tempfile::tempdir().expect("tempdir");
        let web_dir = tmp_dir.path().join("dist");
        std::fs::create_dir_all(&web_dir).unwrap();
        std::fs::write(
            web_dir.join("index.html"),
            "<!doctype html><html><body>Pulse Studio</body></html>",
        )
        .unwrap();

        let node = start_pulse_node(NodeConfig {
            role: NodeRole::All,
            http_addr: "127.0.0.1:0".parse().unwrap(),
            quic_addr: None,
            data_dir: tmp_dir.path().join("node_data"),
            web_dist_dir: web_dir,
            default_project_id: "proj_sec".to_string(),
            default_api_key: String::new(),
            admin_token: None,
            allowed_origins: vec!["http://localhost:5173".to_string()],
            auto_seed: false,
            num_buckets: 4,
        })
        .await
        .expect("start pulse-node");

        let ingest_key = node
            .bootstrap_credentials
            .generated_ingest_key
            .clone()
            .expect("ingest key");
        let base = format!("http://{}", node.http_addr);
        let http = reqwest::Client::new();

        // 1. Web UI (SPA) responses carry all browser security headers.
        let spa = http.get(format!("{base}/")).send().await.unwrap();
        assert_eq!(spa.status(), reqwest::StatusCode::OK);
        assert_eq!(
            spa.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(spa.headers().get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            spa.headers().get("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
        assert_eq!(
            spa.headers().get("permissions-policy").unwrap(),
            "camera=(), microphone=(), geolocation=()"
        );
        let csp = spa
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(csp.contains("default-src 'self'"), "{csp}");
        assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
        assert!(csp.contains("base-uri 'self'"), "{csp}");
        assert!(csp.contains("form-action 'self'"), "{csp}");
        assert!(
            spa.headers().get("strict-transport-security").is_none(),
            "plain HTTP must not send HSTS"
        );

        // 2. HTTPS (X-Forwarded-Proto: https) adds HSTS.
        let spa_https = http
            .get(format!("{base}/"))
            .header("x-forwarded-proto", "https")
            .send()
            .await
            .unwrap();
        assert_eq!(
            spa_https
                .headers()
                .get("strict-transport-security")
                .unwrap(),
            "max-age=31536000; includeSubDomains"
        );

        // 3. Admin/control API routes carry security headers too.
        let auth_cfg = http
            .get(format!("{base}/v1/auth/config"))
            .send()
            .await
            .unwrap();
        assert_eq!(auth_cfg.status(), reqwest::StatusCode::OK);
        assert_eq!(
            auth_cfg.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(auth_cfg.headers().get("x-frame-options").unwrap(), "DENY");
        assert!(auth_cfg.headers().get("content-security-policy").is_some());

        // 4. Public SDK route keeps permissive CORS and works from any customer origin.
        let flags = http
            .get(format!("{base}/v1/flags/{ingest_key}"))
            .header("origin", "https://customer-app.example")
            .send()
            .await
            .unwrap();
        assert_eq!(flags.status(), reqwest::StatusCode::OK);
        assert_eq!(
            flags.headers().get("access-control-allow-origin").unwrap(),
            "*"
        );

        node.shutdown().await;
    }
}
