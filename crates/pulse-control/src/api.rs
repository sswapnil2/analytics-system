use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path as AxumPath, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, put};
use axum::{Extension, Json, Router};
use chrono::Utc;
use constant_time_eq::constant_time_eq;
use dashmap::{DashMap, DashSet};
use parking_lot::Mutex;
use pulse_exec::{
    validate_cohort_acyclic, BehavioralCohortDef, CustomEventDef, CustomPropertyDef, FlowsQuery,
    FunnelQuery, InsightsQuery, LookupTableDef, QueryEngine, RetentionQuery, SessionSearchQuery,
    TimeRange,
};
use pulse_ingest::{DiscoveredEventSchema, GovernanceProvider, IngestService, ProjectLexiconRules};
use pulse_wire::{PropValue, WireframeOp};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::db::{MetaError, MetaStore};
use crate::experiments::{build_edge_flag_bundle, evaluate_experiment_results};
use crate::mcp::{handle_mcp_request, JsonRpcRequest};
use crate::models::{
    generate_random_api_key, generate_root_admin_token, mask_key_prefix, sha256_bytes, sha256_hex,
    ApiKeyRecord, CohortRecord, CreatedApiKeyResponse, CustomSchemaBundle, Dashboard, Experiment,
    ExperimentMetricSpec, FeatureFlag, LexiconEvent, LexiconProperty, Organization, Project,
    SavedReport, VariantAllocation, KEY_ROLE_ADMIN, KEY_ROLE_INGEST_AND_FLAGS,
};

#[derive(Debug, Clone)]
pub enum AuthPrincipal {
    RootAdmin,
    ProjectAdmin { project_id: String },
}

impl AuthPrincipal {
    fn authorize_project(&self, target_project_id: &str) -> Result<(), CtrlError> {
        match self {
            AuthPrincipal::RootAdmin => Ok(()),
            AuthPrincipal::ProjectAdmin { project_id } => {
                if project_id == target_project_id {
                    Ok(())
                } else {
                    Err(CtrlError::forbidden(
                        "key is not authorized for the requested project",
                    ))
                }
            }
        }
    }

    fn resolve_project(
        &self,
        requested: Option<&str>,
        default_pid: &str,
    ) -> Result<String, CtrlError> {
        match self {
            AuthPrincipal::RootAdmin => Ok(requested
                .filter(|s| !s.is_empty())
                .unwrap_or(default_pid)
                .to_string()),
            AuthPrincipal::ProjectAdmin { project_id } => {
                if let Some(req_pid) = requested.filter(|s| !s.is_empty()) {
                    if req_pid != project_id {
                        return Err(CtrlError::forbidden(
                            "key is not authorized for the requested project",
                        ));
                    }
                }
                Ok(project_id.clone())
            }
        }
    }
}

#[derive(Debug, Clone)]
struct CachedApiKey {
    project_id: String,
    role: String,
}

#[derive(Debug, Clone, Default)]
pub struct BootstrapCredentials {
    pub generated_admin_token: Option<String>,
    pub generated_ingest_key: Option<String>,
}

pub struct ControlPlaneService {
    meta: MetaStore,
    qengine: Arc<QueryEngine>,
    ingest: tokio::sync::RwLock<Option<Arc<IngestService>>>,
    default_project_id: String,
    root_admin_hash: [u8; 32],
    bootstrap_creds: Mutex<BootstrapCredentials>,
    /// Keyed by lowercase SHA-256 hex digest (`P0-02`).
    api_key_cache: DashMap<String, CachedApiKey>,
    known_projects: DashSet<String>,
    lexicon_cache: DashMap<String, Arc<ProjectLexiconRules>>,
    discovery_tx: mpsc::UnboundedSender<Vec<DiscoveredEventSchema>>,
}

impl ControlPlaneService {
    pub async fn new(
        meta: MetaStore,
        qengine: Arc<QueryEngine>,
        default_project_id: impl Into<String>,
        initial_ingest_key: impl Into<String>,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_admin(meta, qengine, default_project_id, initial_ingest_key, None).await
    }

    pub async fn new_with_admin(
        meta: MetaStore,
        qengine: Arc<QueryEngine>,
        default_project_id: impl Into<String>,
        initial_ingest_key: impl Into<String>,
        explicit_admin_token: Option<String>,
    ) -> Result<Arc<Self>, String> {
        let default_project_id = default_project_id.into();
        let initial_ingest_key = initial_ingest_key.into();

        let (admin_token_plain, generated_admin_token) = match explicit_admin_token
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                std::env::var("PULSE_ADMIN_TOKEN")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            }) {
            Some(tok) => (tok, None),
            None => {
                let fresh = generate_root_admin_token();
                (fresh.clone(), Some(fresh))
            }
        };
        let root_admin_hash = sha256_bytes(admin_token_plain.trim());

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<DiscoveredEventSchema>>();
        let svc = Arc::new(Self {
            meta: meta.clone(),
            qengine,
            ingest: tokio::sync::RwLock::new(None),
            default_project_id,
            root_admin_hash,
            bootstrap_creds: Mutex::new(BootstrapCredentials {
                generated_admin_token,
                generated_ingest_key: None,
            }),
            api_key_cache: DashMap::new(),
            known_projects: DashSet::new(),
            lexicon_cache: DashMap::new(),
            discovery_tx: tx,
        });

        let generated_ingest = svc
            .ensure_default_org_and_project(initial_ingest_key.trim())
            .await?;
        svc.bootstrap_creds.lock().generated_ingest_key = generated_ingest;
        svc.reload_caches_from_db().await?;

        let meta_bg = meta;
        tokio::spawn(async move {
            while let Some(items) = rx.recv().await {
                let now_ms = Utc::now().timestamp_millis();
                for item in items {
                    let _ = meta_bg
                        .increment_lexicon_discovery(
                            &item.project_id,
                            &item.event_name,
                            item.count as i64,
                            &item.props,
                            now_ms,
                        )
                        .await;
                }
            }
        });

        Ok(svc)
    }

    /// Takes one-time randomly generated startup credentials so `pulse-node` can log them once and clear memory (`P0-02`).
    pub fn take_bootstrap_credentials(&self) -> BootstrapCredentials {
        std::mem::take(&mut *self.bootstrap_creds.lock())
    }

    pub async fn attach_ingest(&self, ingest: Arc<IngestService>) {
        *self.ingest.write().await = Some(ingest);
    }

    pub fn meta(&self) -> &MetaStore {
        &self.meta
    }

    pub fn qengine(&self) -> &Arc<QueryEngine> {
        &self.qengine
    }

    pub fn default_project_id(&self) -> &str {
        &self.default_project_id
    }

    async fn ensure_default_org_and_project(
        &self,
        initial_ingest_key: &str,
    ) -> Result<Option<String>, String> {
        let now = Utc::now().timestamp_millis();
        self.meta
            .ensure_org(&Organization {
                id: "org_default".to_string(),
                name: "Default Organization".to_string(),
                slug: "default".to_string(),
                created_at_ms: now,
            })
            .await
            .map_err(|e| e.to_string())?;

        self.meta
            .ensure_project(&Project {
                id: self.default_project_id.clone(),
                org_id: "org_default".to_string(),
                name: "Default Project".to_string(),
                timezone: "UTC".to_string(),
                created_at_ms: now,
            })
            .await
            .map_err(|e| e.to_string())?;

        if !initial_ingest_key.is_empty() {
            let key_hash = sha256_hex(initial_ingest_key);
            let rec = ApiKeyRecord {
                id: format!("key_{}", &key_hash[..16]),
                key_hash,
                key_prefix: mask_key_prefix(initial_ingest_key),
                project_id: self.default_project_id.clone(),
                label: "Configured Ingest Key".to_string(),
                role: KEY_ROLE_INGEST_AND_FLAGS.to_string(),
                created_at_ms: now,
                revoked: false,
            };
            self.meta
                .insert_api_key(&rec)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(None);
        }

        let existing = self
            .meta
            .list_api_keys(Some(&self.default_project_id))
            .await
            .map_err(|e| e.to_string())?;
        if existing.is_empty() {
            let (plaintext, key_hash, key_prefix) =
                generate_random_api_key(KEY_ROLE_INGEST_AND_FLAGS);
            let rec = ApiKeyRecord {
                id: format!("key_{}", &Uuid::new_v4().simple().to_string()[..12]),
                key_hash,
                key_prefix,
                project_id: self.default_project_id.clone(),
                label: "Initial Project Ingest Key".to_string(),
                role: KEY_ROLE_INGEST_AND_FLAGS.to_string(),
                created_at_ms: now,
                revoked: false,
            };
            self.meta
                .insert_api_key(&rec)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(Some(plaintext));
        }

        Ok(None)
    }

    pub async fn reload_caches_from_db(&self) -> Result<(), String> {
        let keys = self
            .meta
            .list_api_keys(None)
            .await
            .map_err(|e| e.to_string())?;
        self.api_key_cache.clear();
        for k in keys {
            if !k.revoked {
                self.api_key_cache.insert(
                    k.key_hash,
                    CachedApiKey {
                        project_id: k.project_id,
                        role: k.role,
                    },
                );
            }
        }

        let projects = self.meta.list_projects().await.map_err(|e| e.to_string())?;
        self.known_projects.clear();
        for proj in projects {
            self.known_projects.insert(proj.id.clone());
            self.refresh_project_lexicon_cache(&proj.id).await?;
            self.refresh_project_virtual_catalog(&proj.id).await?;
        }
        Ok(())
    }

    async fn ensure_known_project(&self, project_id: &str) -> Result<(), CtrlError> {
        if self.known_projects.contains(project_id) {
            return Ok(());
        }
        match self
            .meta
            .get_project(project_id)
            .await
            .map_err(CtrlError::internal)?
        {
            Some(p) => {
                self.known_projects.insert(p.id);
                Ok(())
            }
            None => Err(CtrlError::not_found(format!(
                "project '{project_id}' not found"
            ))),
        }
    }

    async fn authorize_and_check(
        &self,
        principal: &AuthPrincipal,
        project_id: &str,
    ) -> Result<(), CtrlError> {
        principal.authorize_project(project_id)?;
        self.ensure_known_project(project_id).await
    }

    fn verify_admin_token(&self, token: &str) -> Result<AuthPrincipal, CtrlError> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(CtrlError::unauthorized("missing admin bearer token"));
        }
        let digest_bytes = sha256_bytes(trimmed);
        if constant_time_eq(&self.root_admin_hash, &digest_bytes) {
            return Ok(AuthPrincipal::RootAdmin);
        }

        let hash_hex = hex::encode(digest_bytes);
        if let Some(entry) = self.api_key_cache.get(&hash_hex) {
            if entry.role == KEY_ROLE_ADMIN {
                return Ok(AuthPrincipal::ProjectAdmin {
                    project_id: entry.project_id.clone(),
                });
            }
            return Err(CtrlError::forbidden(
                "public pk_ ingest keys cannot access admin or query routes",
            ));
        }

        Err(CtrlError::unauthorized("invalid admin bearer token"))
    }

    pub async fn refresh_project_lexicon_cache(&self, project_id: &str) -> Result<(), String> {
        let evs = self
            .meta
            .list_lexicon_events(project_id)
            .await
            .map_err(|e| e.to_string())?;
        let props = self
            .meta
            .list_lexicon_properties(project_id)
            .await
            .map_err(|e| e.to_string())?;

        let mut rules = ProjectLexiconRules::default();
        for e in evs {
            if e.status == "dropped" {
                rules.dropped_events.insert(e.name);
            }
        }
        for p in props {
            if p.status == "dropped" {
                rules.dropped_properties.insert(p.name.clone());
            }
            if p.pii_masked {
                rules.pii_masked_properties.insert(p.name);
            }
        }
        self.lexicon_cache
            .insert(project_id.to_string(), Arc::new(rules));
        Ok(())
    }

    pub async fn refresh_project_virtual_catalog(&self, project_id: &str) -> Result<(), String> {
        let cat = self.qengine.catalogs().for_project(project_id);

        let ces = self
            .meta
            .list_custom_events(project_id)
            .await
            .map_err(|e| e.to_string())?;
        cat.custom_events.clear();
        for ce in ces {
            cat.custom_events.insert(ce.name.clone(), ce);
        }

        let cps = self
            .meta
            .list_custom_properties(project_id)
            .await
            .map_err(|e| e.to_string())?;
        cat.custom_properties.clear();
        for cp in cps {
            cat.custom_properties.insert(cp.name.clone(), cp);
        }

        let lts = self
            .meta
            .list_lookup_tables(project_id)
            .await
            .map_err(|e| e.to_string())?;
        cat.lookup_tables.clear();
        for (_, def) in lts {
            cat.lookup_tables.insert(def.name.clone(), def);
        }

        let cohorts = self
            .meta
            .list_cohorts(project_id)
            .await
            .map_err(|e| e.to_string())?;
        cat.saved_cohorts.clear();
        for c in cohorts {
            cat.saved_cohorts.insert(c.id.clone(), c.definition);
        }

        Ok(())
    }
}

#[async_trait]
impl GovernanceProvider for ControlPlaneService {
    async fn authenticate_api_key(&self, api_key: &str) -> Option<String> {
        let key_hash = sha256_hex(api_key.trim());
        self.api_key_cache.get(&key_hash).and_then(|entry| {
            if entry.role == KEY_ROLE_INGEST_AND_FLAGS {
                Some(entry.project_id.clone())
            } else {
                None
            }
        })
    }

    fn lexicon_rules(&self, project_id: &str) -> Arc<ProjectLexiconRules> {
        self.lexicon_cache
            .get(project_id)
            .map(|r| r.clone())
            .unwrap_or_else(|| Arc::new(ProjectLexiconRules::default()))
    }

    fn record_discovery(&self, items: Vec<DiscoveredEventSchema>) {
        let _ = self.discovery_tx.send(items);
    }
}

fn extract_admin_bearer(headers: &HeaderMap) -> Option<&str> {
    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(tok) = auth
            .strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "))
        {
            if !tok.trim().is_empty() {
                return Some(tok.trim());
            }
        }
    }
    if let Some(val) = headers
        .get("x-pulse-admin-key")
        .and_then(|v| v.to_str().ok())
    {
        if !val.trim().is_empty() {
            return Some(val.trim());
        }
    }
    None
}

async fn require_admin_auth(
    State(ctrl): State<Arc<ControlPlaneService>>,
    mut req: Request,
    next: Next,
) -> std::result::Result<Response, CtrlError> {
    let Some(token) = extract_admin_bearer(req.headers()) else {
        return Err(CtrlError::unauthorized(
            "missing Authorization: Bearer <sk_...> header",
        ));
    };
    let principal = ctrl.verify_admin_token(token)?;
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

/// Builds the public SDK Flag Bundle router (`GET /v1/flags/:project_api_key`), safe for permissive CORS.
pub fn build_public_flag_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .route("/v1/flags/:project_api_key", get(handle_edge_flag_bundle))
        .with_state(ctrl)
}

/// Builds the authenticated Control Plane + Query + Replay + MCP router (`P0-01`).
pub fn build_admin_control_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .route("/v1/status", get(handle_status))
        .route("/v1/admin/flush", post(handle_admin_flush))
        .route(
            "/v1/projects",
            get(handle_list_projects).post(handle_create_project),
        )
        .route(
            "/v1/projects/:id/api-keys",
            get(handle_list_keys).post(handle_create_key),
        )
        // Unified & per-project Query endpoints
        .route("/v1/query", post(handle_unified_query))
        .route(
            "/v1/projects/:id/query/insights",
            post(handle_query_insights),
        )
        .route("/v1/projects/:id/query/funnel", post(handle_query_funnel))
        .route(
            "/v1/projects/:id/query/retention",
            post(handle_query_retention),
        )
        .route("/v1/projects/:id/query/flows", post(handle_query_flows))
        // Sessions & Byte-Range Wireframe Replay
        .route(
            "/v1/sessions",
            get(handle_get_sessions_default).post(handle_post_sessions_default),
        )
        .route(
            "/v1/projects/:id/sessions",
            get(handle_get_sessions).post(handle_post_sessions),
        )
        .route("/v1/replay/:session_id", get(handle_get_replay_default))
        .route(
            "/v1/projects/:id/replay/:session_id",
            get(handle_get_replay),
        )
        // Lexicon Governance
        .route("/v1/projects/:id/lexicon", get(handle_get_lexicon))
        .route(
            "/v1/projects/:id/lexicon/events/:name",
            put(handle_put_lexicon_event),
        )
        .route(
            "/v1/projects/:id/lexicon/properties/:name",
            put(handle_put_lexicon_prop),
        )
        // Cohorts & Schema Extensions
        .route(
            "/v1/projects/:id/cohorts",
            get(handle_list_cohorts).post(handle_create_cohort),
        )
        .route(
            "/v1/projects/:id/cohorts/preview",
            post(handle_preview_cohort),
        )
        .route(
            "/v1/projects/:id/schema-extensions",
            get(handle_get_schema_extensions),
        )
        .route(
            "/v1/projects/:id/lookup-tables",
            get(handle_list_lookup_tables).post(handle_upload_lookup_table),
        )
        .route(
            "/v1/projects/:id/custom-events",
            post(handle_create_custom_event),
        )
        .route(
            "/v1/projects/:id/custom-properties",
            post(handle_create_custom_property),
        )
        // Saved Reports & Dashboards
        .route(
            "/v1/projects/:id/saved-reports",
            get(handle_list_reports).post(handle_save_report),
        )
        .route(
            "/v1/projects/:id/dashboards",
            get(handle_list_dashboards).post(handle_save_dashboard),
        )
        // Feature Flags & Experimentation
        .route(
            "/v1/projects/:id/flags",
            get(handle_list_flags).post(handle_upsert_flag),
        )
        .route("/v1/projects/:id/flags/:flag_id", patch(handle_patch_flag))
        .route(
            "/v1/projects/:id/experiments",
            get(handle_list_experiments).post(handle_create_experiment),
        )
        .route(
            "/v1/projects/:id/experiments/:exp_id/results",
            get(handle_experiment_results_scoped),
        )
        // Model Context Protocol (MCP) JSON-RPC 2.0
        .route("/v1/mcp", post(handle_mcp))
        .route_layer(middleware::from_fn_with_state(
            ctrl.clone(),
            require_admin_auth,
        ))
        .with_state(ctrl)
}

/// Combined Control Plane router (public edge flag bundle + bearer-protected admin/query routes).
pub fn build_control_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .merge(build_public_flag_router(ctrl.clone()))
        .merge(build_admin_control_router(ctrl))
}

type ApiRes<T> = std::result::Result<Json<T>, CtrlError>;

struct CtrlError(StatusCode, String);

impl CtrlError {
    fn bad(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    fn unauthorized(msg: impl Into<String>) -> Self {
        Self(StatusCode::UNAUTHORIZED, msg.into())
    }
    fn forbidden(msg: impl Into<String>) -> Self {
        Self(StatusCode::FORBIDDEN, msg.into())
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
    }
    fn conflict(msg: impl Into<String>) -> Self {
        Self(StatusCode::CONFLICT, msg.into())
    }
    fn meta(err: MetaError) -> Self {
        match err {
            MetaError::Conflict(msg) => Self::conflict(msg),
            other => Self::internal(other),
        }
    }
    fn internal(msg: impl std::fmt::Display) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, msg.to_string())
    }
}

impl IntoResponse for CtrlError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({ "status": "error", "message": self.1 })),
        )
            .into_response()
    }
}

async fn handle_status(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiRes<Value> {
    let pid = principal.resolve_project(
        q.get("project_id").map(|s| s.as_str()),
        &ctrl.default_project_id,
    )?;
    ctrl.ensure_known_project(&pid).await?;

    let ps = ctrl
        .qengine
        .storage()
        .get_or_open_project(&pid)
        .await
        .map_err(CtrlError::internal)?;
    let snap = ps.snapshot();
    let id_graph = ctrl
        .qengine
        .storage()
        .identity_router()
        .get_or_open(&pid)
        .await
        .map_err(CtrlError::internal)?;

    let (live_eps, accepted_total, deduped_total, dropped_total) =
        if let Some(ing) = ctrl.ingest.read().await.as_ref() {
            let m = ing.metrics();
            (
                m.current_eps(),
                m.accepted_total.load(std::sync::atomic::Ordering::Relaxed),
                m.deduped_total.load(std::sync::atomic::Ordering::Relaxed),
                m.dropped_total.load(std::sync::atomic::Ordering::Relaxed),
            )
        } else {
            (0, 0, 0, 0)
        };

    let all_projects = ctrl.meta.list_projects().await.map_err(CtrlError::meta)?;
    let projects: Vec<Project> = match &principal {
        AuthPrincipal::RootAdmin => all_projects,
        AuthPrincipal::ProjectAdmin { project_id } => all_projects
            .into_iter()
            .filter(|p| &p.id == project_id)
            .collect(),
    };

    Ok(Json(json!({
        "status": "ok",
        "default_project_id": ctrl.default_project_id,
        "active_project_id": pid,
        "live_eps": live_eps,
        "accepted_events_total": accepted_total,
        "deduped_events_total": deduped_total,
        "dropped_events_total": dropped_total,
        "last_query_latency_us": ctrl.qengine.last_query_latency_us(),
        "queries_executed": ctrl.qengine.queries_executed(),
        "snapshot_epoch": snap.epoch(),
        "l0_rows": ps.total_l0_rows(),
        "l1_rows": snap.manifest.total_l1_rows(),
        "segment_count": snap.manifest.total_segments(),
        "interned_users": id_graph.user_count(),
        "projects": projects,
    })))
}

async fn handle_admin_flush(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<Value> {
    if !matches!(principal, AuthPrincipal::RootAdmin) {
        return Err(CtrlError::forbidden(
            "cluster flush requires root admin token",
        ));
    }
    let flushed = ctrl
        .qengine
        .storage()
        .flush_all_projects()
        .await
        .map_err(CtrlError::internal)?;
    let compacted = ctrl
        .qengine
        .storage()
        .compact_all_projects()
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(json!({
        "status": "ok",
        "segments_flushed": flushed,
        "partitions_compacted": compacted
    })))
}

async fn handle_list_projects(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<Vec<Project>> {
    let list = ctrl.meta.list_projects().await.map_err(CtrlError::meta)?;
    let scoped = match principal {
        AuthPrincipal::RootAdmin => list,
        AuthPrincipal::ProjectAdmin { project_id } => {
            list.into_iter().filter(|p| p.id == project_id).collect()
        }
    };
    Ok(Json(scoped))
}

#[derive(Deserialize)]
struct CreateProjectReq {
    id: Option<String>,
    name: String,
    timezone: Option<String>,
}

async fn handle_create_project(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Json(req): Json<CreateProjectReq>,
) -> ApiRes<Value> {
    if !matches!(principal, AuthPrincipal::RootAdmin) {
        return Err(CtrlError::forbidden(
            "creating projects requires root admin token",
        ));
    }
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "project id is generated by the server and must not be client-supplied",
        ));
    }
    let name = req.name.trim();
    if name.is_empty() {
        return Err(CtrlError::bad("project name must not be empty"));
    }

    let now = Utc::now().timestamp_millis();
    let id = format!("proj_{}", &Uuid::new_v4().simple().to_string()[..12]);
    let proj = Project {
        id: id.clone(),
        org_id: "org_default".to_string(),
        name: name.to_string(),
        timezone: req.timezone.unwrap_or_else(|| "UTC".to_string()),
        created_at_ms: now,
    };
    ctrl.meta
        .insert_project(&proj)
        .await
        .map_err(CtrlError::meta)?;

    let (plaintext_key, key_hash, key_prefix) = generate_random_api_key(KEY_ROLE_INGEST_AND_FLAGS);
    let key = ApiKeyRecord {
        id: format!("key_{}", &Uuid::new_v4().simple().to_string()[..12]),
        key_hash: key_hash.clone(),
        key_prefix,
        project_id: id.clone(),
        label: "Primary Ingest Key".to_string(),
        role: KEY_ROLE_INGEST_AND_FLAGS.to_string(),
        created_at_ms: now,
        revoked: false,
    };
    ctrl.meta
        .insert_api_key(&key)
        .await
        .map_err(CtrlError::meta)?;

    // Incremental single-project cache registration (Section 6)
    ctrl.known_projects.insert(id.clone());
    ctrl.api_key_cache.insert(
        key_hash,
        CachedApiKey {
            project_id: id.clone(),
            role: key.role.clone(),
        },
    );
    ctrl.refresh_project_lexicon_cache(&id)
        .await
        .map_err(CtrlError::internal)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;

    Ok(Json(json!({
        "project": proj,
        "api_key": CreatedApiKeyResponse {
            record: key,
            plaintext_key,
        }
    })))
}

async fn handle_list_keys(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<ApiKeyRecord>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let keys = ctrl
        .meta
        .list_api_keys(Some(&id))
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(keys))
}

#[derive(Deserialize)]
struct CreateKeyReq {
    label: String,
    #[serde(default = "default_ingest_role")]
    role: String,
}

fn default_ingest_role() -> String {
    KEY_ROLE_INGEST_AND_FLAGS.to_string()
}

async fn handle_create_key(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateKeyReq>,
) -> ApiRes<CreatedApiKeyResponse> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let role = match req.role.as_str() {
        KEY_ROLE_INGEST_AND_FLAGS | KEY_ROLE_ADMIN => req.role,
        other => {
            return Err(CtrlError::bad(format!(
                "invalid key role '{other}'; expected '{KEY_ROLE_INGEST_AND_FLAGS}' or '{KEY_ROLE_ADMIN}'"
            )))
        }
    };
    let (plaintext_key, key_hash, key_prefix) = generate_random_api_key(&role);
    let record = ApiKeyRecord {
        id: format!("key_{}", &Uuid::new_v4().simple().to_string()[..12]),
        key_hash: key_hash.clone(),
        key_prefix,
        project_id: id.clone(),
        label: req.label,
        role: role.clone(),
        created_at_ms: Utc::now().timestamp_millis(),
        revoked: false,
    };
    ctrl.meta
        .insert_api_key(&record)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.api_key_cache.insert(
        key_hash,
        CachedApiKey {
            project_id: id,
            role,
        },
    );
    Ok(Json(CreatedApiKeyResponse {
        record,
        plaintext_key,
    }))
}

// ------------------- Queries -------------------

async fn handle_unified_query(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Json(payload): Json<Value>,
) -> ApiRes<Value> {
    let req_pid = payload.get("project_id").and_then(|v| v.as_str());
    let pid = principal.resolve_project(req_pid, &ctrl.default_project_id)?;
    ctrl.ensure_known_project(&pid).await?;

    let kind = payload
        .get("kind")
        .or_else(|| payload.get("report_type"))
        .and_then(|v| v.as_str())
        .unwrap_or("insights");

    let inner = payload.get("query").cloned().unwrap_or(payload.clone());
    match kind {
        "insights" => {
            let q: InsightsQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_insights(&pid, &q)
                .await
                .map_err(|e| CtrlError::bad(e.to_string()))?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "funnel" | "funnels" => {
            let q: FunnelQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_funnel(&pid, &q)
                .await
                .map_err(|e| CtrlError::bad(e.to_string()))?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "retention" => {
            let q: RetentionQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_retention(&pid, &q)
                .await
                .map_err(|e| CtrlError::bad(e.to_string()))?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "flows" | "flow" => {
            let q: FlowsQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_flows(&pid, &q)
                .await
                .map_err(|e| CtrlError::bad(e.to_string()))?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        other => Err(CtrlError::bad(format!("unknown query kind: {other}"))),
    }
}

async fn handle_query_insights(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<InsightsQuery>,
) -> ApiRes<pulse_exec::InsightsResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let res = ctrl
        .qengine
        .query_insights(&id, &q)
        .await
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    Ok(Json(res))
}

async fn handle_query_funnel(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<FunnelQuery>,
) -> ApiRes<pulse_exec::FunnelResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let res = ctrl
        .qengine
        .query_funnel(&id, &q)
        .await
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    Ok(Json(res))
}

async fn handle_query_retention(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<RetentionQuery>,
) -> ApiRes<pulse_exec::RetentionResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let res = ctrl
        .qengine
        .query_retention(&id, &q)
        .await
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    Ok(Json(res))
}

async fn handle_query_flows(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<FlowsQuery>,
) -> ApiRes<pulse_exec::FlowsResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let res = ctrl
        .qengine
        .query_flows(&id, &q)
        .await
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    Ok(Json(res))
}

// ------------------- Sessions & Byte-Range Wireframe Replay -------------------

fn session_query_from_params(params: &HashMap<String, String>) -> SessionSearchQuery {
    SessionSearchQuery {
        time_range: match (
            params.get("start_ts_ms").and_then(|s| s.parse().ok()),
            params.get("end_ts_ms").and_then(|s| s.parse().ok()),
        ) {
            (Some(s), Some(e)) => Some(TimeRange {
                start_ts_ms: s,
                end_ts_ms: e,
            }),
            _ => None,
        },
        event_name: params
            .get("event")
            .or_else(|| params.get("event_name"))
            .cloned(),
        only_rage_tap: params
            .get("rage_tap")
            .or_else(|| params.get("only_rage_tap"))
            .is_some_and(|v| v == "true" || v == "1"),
        only_dead_tap: params
            .get("dead_tap")
            .is_some_and(|v| v == "true" || v == "1"),
        only_errors: params
            .get("errors")
            .or_else(|| params.get("only_errors"))
            .is_some_and(|v| v == "true" || v == "1"),
        limit: params.get("limit").and_then(|s| s.parse().ok()),
    }
}

async fn handle_get_sessions_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let pid = principal.resolve_project(
        params.get("project_id").map(|s| s.as_str()),
        &ctrl.default_project_id,
    )?;
    ctrl.ensure_known_project(&pid).await?;
    let sq = session_query_from_params(&params);
    let res = ctrl
        .qengine
        .search_sessions(&pid, &sq)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_post_sessions_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Json(sq): Json<SessionSearchQuery>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let pid = principal.resolve_project(None, &ctrl.default_project_id)?;
    ctrl.ensure_known_project(&pid).await?;
    let res = ctrl
        .qengine
        .search_sessions(&pid, &sq)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_get_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let sq = session_query_from_params(&params);
    let res = ctrl
        .qengine
        .search_sessions(&id, &sq)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_post_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(sq): Json<SessionSearchQuery>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let res = ctrl
        .qengine
        .search_sessions(&id, &sq)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_get_replay_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> std::result::Result<Response, CtrlError> {
    let pid = principal.resolve_project(
        params.get("project_id").map(|s| s.as_str()),
        &ctrl.default_project_id,
    )?;
    ctrl.ensure_known_project(&pid).await?;
    serve_replay(&ctrl, &pid, &session_id, &headers, &params).await
}

async fn handle_get_replay(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, session_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> std::result::Result<Response, CtrlError> {
    ctrl.authorize_and_check(&principal, &id).await?;
    serve_replay(&ctrl, &id, &session_id, &headers, &params).await
}

/// Validates RFC 9110 single byte-range header (`bytes=start-end`, `bytes=start-`, `bytes=-suffix`) (`P3-05`).
fn resolve_http_byte_range(
    spec: &str,
    total_len: usize,
) -> std::result::Result<(usize, usize), ()> {
    if total_len == 0 {
        return Err(());
    }
    let (start_s, end_s) = spec.split_once('-').ok_or(())?;
    if start_s.is_empty() {
        // Suffix range: `bytes=-N`
        let suffix: usize = end_s.parse().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        let start = total_len.saturating_sub(suffix);
        Ok((start, total_len - 1))
    } else {
        let start: usize = start_s.parse().map_err(|_| ())?;
        if start >= total_len {
            return Err(());
        }
        if end_s.is_empty() {
            Ok((start, total_len - 1))
        } else {
            let end: usize = end_s.parse().map_err(|_| ())?;
            if end < start {
                return Err(());
            }
            Ok((start, end.min(total_len - 1)))
        }
    }
}

async fn serve_replay(
    ctrl: &ControlPlaneService,
    project_id: &str,
    session_id: &str,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> std::result::Result<Response, CtrlError> {
    let rstore = ctrl.qengine.storage().replay_store();
    let map_replay_err = |err: pulse_storage::StorageError| match err {
        pulse_storage::StorageError::InvalidIdentifier(msg) => CtrlError::bad(msg),
        other => CtrlError::internal(other),
    };

    if let Some(range_hdr) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        let Some(spec) = range_hdr.strip_prefix("bytes=") else {
            return Ok((StatusCode::RANGE_NOT_SATISFIABLE, "invalid range unit").into_response());
        };
        let Some(total_u64) = rstore
            .total_session_bytes(project_id, session_id)
            .await
            .map_err(map_replay_err)?
        else {
            return Err(CtrlError::not_found("replay session not found"));
        };
        let total = total_u64 as usize;
        let Ok((start, end)) = resolve_http_byte_range(spec.trim(), total) else {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{total}"))],
                "",
            )
                .into_response());
        };
        let len = (end - start + 1) as u64;
        let slice = rstore
            .read_bytes_from(project_id, session_id, start as u64, Some(len))
            .await
            .map_err(map_replay_err)?
            .unwrap_or_default();
        return Ok((
            StatusCode::PARTIAL_CONTENT,
            [
                (
                    header::CONTENT_TYPE,
                    "application/x-pulse-frame".to_string(),
                ),
                (
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{total}"),
                ),
            ],
            slice,
        )
            .into_response());
    }

    let offset_kb: u64 = params
        .get("offset_kb")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let byte_offset = params
        .get("byte_offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(offset_kb.saturating_mul(1024));

    if params.get("format").is_some_and(|f| f == "binary") {
        let Some(bytes) = rstore
            .read_bytes_from(project_id, session_id, byte_offset, None)
            .await
            .map_err(CtrlError::internal)?
        else {
            return Err(CtrlError::not_found("replay session not found"));
        };
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-pulse-frame")],
            bytes,
        )
            .into_response());
    }

    let mut chunks = rstore
        .read_chunks(project_id, session_id, byte_offset)
        .await
        .map_err(CtrlError::internal)?;

    if byte_offset > 0
        && !chunks.iter().any(|c| {
            c.ops
                .iter()
                .any(|op| matches!(op, WireframeOp::FullSnapshot { .. }))
        })
    {
        if let Ok(head_chunks) = rstore.read_chunks(project_id, session_id, 0).await {
            if let Some(first) = head_chunks.into_iter().next() {
                chunks.insert(0, first);
            }
        }
    }

    if chunks.is_empty() {
        return Err(CtrlError::not_found("replay session not found"));
    }

    Ok(Json(json!({
        "project_id": project_id,
        "session_id": session_id,
        "start_byte_offset": byte_offset,
        "chunks": chunks
    }))
    .into_response())
}

// ------------------- Lexicon Governance -------------------

async fn handle_get_lexicon(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let events = ctrl
        .meta
        .list_lexicon_events(&id)
        .await
        .map_err(CtrlError::meta)?;
    let properties = ctrl
        .meta
        .list_lexicon_properties(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({
        "events": events,
        "properties": properties
    })))
}

#[derive(Deserialize)]
struct UpdateLexiconEventReq {
    display_name: Option<String>,
    description: Option<String>,
    status: String,
    owner: Option<String>,
}

async fn handle_put_lexicon_event(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, name)): AxumPath<(String, String)>,
    Json(req): Json<UpdateLexiconEventReq>,
) -> ApiRes<LexiconEvent> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let ev = LexiconEvent {
        project_id: id.clone(),
        display_name: req.display_name.unwrap_or_else(|| name.clone()),
        name,
        description: req.description.unwrap_or_default(),
        status: req.status,
        owner: req.owner.unwrap_or_else(|| "product".to_string()),
        volume_24h: 0,
        updated_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_lexicon_event(&ev)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_lexicon_cache(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(ev))
}

#[derive(Deserialize)]
struct UpdateLexiconPropReq {
    prop_scope: Option<String>,
    inferred_types: Option<String>,
    status: Option<String>,
    pii_masked: bool,
}

async fn handle_put_lexicon_prop(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, name)): AxumPath<(String, String)>,
    Json(req): Json<UpdateLexiconPropReq>,
) -> ApiRes<LexiconProperty> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let prop = LexiconProperty {
        project_id: id.clone(),
        name,
        prop_scope: req.prop_scope.unwrap_or_else(|| "event".to_string()),
        inferred_types: req.inferred_types.unwrap_or_else(|| "string".to_string()),
        status: req.status.unwrap_or_else(|| "active".to_string()),
        pii_masked: req.pii_masked,
        updated_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_lexicon_property(&prop)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_lexicon_cache(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(prop))
}

// ------------------- Cohorts & Virtual Schema Extensions (`P0-03`) -------------------

async fn handle_list_cohorts(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<CohortRecord>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl.meta.list_cohorts(&id).await.map_err(CtrlError::meta)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct CreateCohortReq {
    id: Option<String>,
    name: String,
    #[serde(default)]
    description: String,
    definition: BehavioralCohortDef,
}

async fn handle_create_cohort(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateCohortReq>,
) -> ApiRes<CohortRecord> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "cohort id is generated by the server and must not be client-supplied",
        ));
    }
    let cohort_id = format!("coh_{}", &Uuid::new_v4().simple().to_string()[..12]);
    let cat = ctrl.qengine.catalogs().for_project(&id);
    validate_cohort_acyclic(Some(&cohort_id), &req.definition, &cat)
        .map_err(|e| CtrlError::bad(e.to_string()))?;

    let rec = CohortRecord {
        id: cohort_id,
        project_id: id.clone(),
        name: req.name,
        description: req.description,
        definition: req.definition,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_cohort(&rec)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(rec))
}

#[derive(Deserialize)]
struct PreviewCohortReq {
    definition: BehavioralCohortDef,
    time_range: Option<TimeRange>,
}

async fn handle_preview_cohort(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<PreviewCohortReq>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let cat = ctrl.qengine.catalogs().for_project(&id);
    validate_cohort_acyclic(None, &req.definition, &cat)
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    let (user_count, exec_time_us) = ctrl
        .qengine
        .preview_cohort_count(&id, &req.definition, req.time_range)
        .await
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    Ok(Json(json!({
        "user_count": user_count,
        "exec_time_us": exec_time_us
    })))
}

async fn handle_get_schema_extensions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<CustomSchemaBundle> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let custom_events = ctrl
        .meta
        .list_custom_events(&id)
        .await
        .map_err(CtrlError::meta)?;
    let custom_properties = ctrl
        .meta
        .list_custom_properties(&id)
        .await
        .map_err(CtrlError::meta)?;
    let lookup_tables = ctrl
        .meta
        .list_lookup_tables(&id)
        .await
        .map_err(CtrlError::meta)?
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    Ok(Json(CustomSchemaBundle {
        custom_events,
        custom_properties,
        lookup_tables,
    }))
}

async fn handle_list_lookup_tables(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<crate::models::LookupTableSummary>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl
        .meta
        .list_lookup_tables(&id)
        .await
        .map_err(CtrlError::meta)?
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    Ok(Json(list))
}

#[derive(Deserialize)]
struct UploadLookupReq {
    name: String,
    join_prop: String,
    csv_text: Option<String>,
    entries: Option<HashMap<String, HashMap<String, PropValue>>>,
}

async fn handle_upload_lookup_table(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UploadLookupReq>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let def = if let Some(csv_str) = req.csv_text {
        LookupTableDef::from_csv_bytes(&req.name, &req.join_prop, csv_str.as_bytes())
            .map_err(CtrlError::bad)?
    } else if let Some(entries) = req.entries {
        LookupTableDef {
            name: req.name.clone(),
            join_prop: req.join_prop.clone(),
            entries,
        }
    } else {
        return Err(CtrlError::bad(
            "provide either `csv_text` or `entries` JSON map",
        ));
    };

    let lt_id = format!("lt_{}_{}", id, def.name);
    let now = Utc::now().timestamp_millis();
    let row_count = def.entries.len();
    ctrl.meta
        .upsert_lookup_table(&lt_id, &id, &def, now)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;

    Ok(Json(json!({
        "status": "ok",
        "id": lt_id,
        "name": def.name,
        "join_prop": def.join_prop,
        "row_count": row_count
    })))
}

async fn handle_create_custom_event(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(def): Json<CustomEventDef>,
) -> ApiRes<CustomEventDef> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let ce_id = format!("ce_{}_{}", id, def.name);
    ctrl.meta
        .upsert_custom_event(&ce_id, &id, &def, Utc::now().timestamp_millis())
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(def))
}

async fn handle_create_custom_property(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(def): Json<CustomPropertyDef>,
) -> ApiRes<CustomPropertyDef> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let cp_id = format!("cp_{}_{}", id, def.name);
    ctrl.meta
        .upsert_custom_property(&cp_id, &id, &def, Utc::now().timestamp_millis())
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(def))
}

// ------------------- Saved Reports & Dashboards -------------------

async fn handle_list_reports(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<SavedReport>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl
        .meta
        .list_saved_reports(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct SaveReportReq {
    id: Option<String>,
    name: String,
    report_kind: String,
    query_json: Value,
}

async fn handle_save_report(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<SaveReportReq>,
) -> ApiRes<SavedReport> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "report id is generated by the server and must not be client-supplied",
        ));
    }
    let rep = SavedReport {
        id: format!("rep_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id,
        name: req.name,
        report_kind: req.report_kind,
        query_json: req.query_json,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_saved_report(&rep)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(rep))
}

async fn handle_list_dashboards(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<Dashboard>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl
        .meta
        .list_dashboards(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct SaveDashboardReq {
    id: Option<String>,
    name: String,
    report_ids: Vec<String>,
}

async fn handle_save_dashboard(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<SaveDashboardReq>,
) -> ApiRes<Dashboard> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "dashboard id is generated by the server and must not be client-supplied",
        ));
    }
    let dash = Dashboard {
        id: format!("dash_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id,
        name: req.name,
        report_ids: req.report_ids,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_dashboard(&dash)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(dash))
}

// ------------------- Feature Flags & Experiments (`P0-03`, `P0-04`, `P2-10`) -------------------

async fn handle_list_flags(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<FeatureFlag>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl
        .meta
        .list_feature_flags(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct UpsertFlagReq {
    id: Option<String>,
    key: String,
    #[serde(default)]
    description: String,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_100")]
    rollout_pct: f64,
    targeting_cohort_id: Option<String>,
    #[serde(default)]
    variants: Vec<VariantAllocation>,
}

fn default_true() -> bool {
    true
}
fn default_100() -> f64 {
    100.0
}

async fn handle_upsert_flag(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UpsertFlagReq>,
) -> ApiRes<FeatureFlag> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "flag id is generated by the server and must not be client-supplied",
        ));
    }
    let variants = if req.variants.is_empty() {
        vec![
            VariantAllocation {
                key: "control".to_string(),
                weight_bps: 5000,
                payload: Value::Null,
            },
            VariantAllocation {
                key: "treatment".to_string(),
                weight_bps: 5000,
                payload: Value::Null,
            },
        ]
    } else {
        let weight_sum: u64 = req.variants.iter().map(|v| v.weight_bps as u64).sum();
        if weight_sum != 10_000 {
            return Err(CtrlError::bad(format!(
                "variant weight_bps must sum to 10000, got {weight_sum}"
            )));
        }
        req.variants
    };

    if let Some(coh_id) = &req.targeting_cohort_id {
        let cohorts = ctrl.meta.list_cohorts(&id).await.map_err(CtrlError::meta)?;
        if !cohorts.iter().any(|c| &c.id == coh_id) {
            return Err(CtrlError::bad(format!(
                "targeting_cohort_id '{coh_id}' does not exist in project '{id}'"
            )));
        }
    }

    let flag = FeatureFlag {
        id: format!("flg_{}_{}", id, req.key),
        project_id: id,
        key: req.key,
        description: req.description,
        enabled: req.enabled,
        rollout_pct: req.rollout_pct.clamp(0.0, 100.0),
        targeting_cohort_id: req.targeting_cohort_id,
        variants,
        updated_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_feature_flag(&flag)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(flag))
}

#[derive(Deserialize)]
struct PatchFlagReq {
    enabled: Option<bool>,
    rollout_pct: Option<f64>,
    description: Option<String>,
}

async fn handle_patch_flag(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, flag_id)): AxumPath<(String, String)>,
    Json(req): Json<PatchFlagReq>,
) -> ApiRes<FeatureFlag> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let flags = ctrl
        .meta
        .list_feature_flags(&id)
        .await
        .map_err(CtrlError::meta)?;
    let Some(mut found) = flags
        .into_iter()
        .find(|f| f.id == flag_id || f.key == flag_id)
    else {
        return Err(CtrlError::not_found("flag not found"));
    };
    if let Some(en) = req.enabled {
        found.enabled = en;
    }
    if let Some(pct) = req.rollout_pct {
        found.rollout_pct = pct.clamp(0.0, 100.0);
    }
    if let Some(desc) = req.description {
        found.description = desc;
    }
    found.updated_at_ms = Utc::now().timestamp_millis();
    ctrl.meta
        .upsert_feature_flag(&found)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(found))
}

async fn handle_edge_flag_bundle(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(api_key): AxumPath<String>,
    headers: HeaderMap,
) -> std::result::Result<Response, CtrlError> {
    let Some(project_id) = ctrl.authenticate_api_key(&api_key).await else {
        return Err(CtrlError::unauthorized("invalid project api key"));
    };
    let flags = ctrl
        .meta
        .list_feature_flags(&project_id)
        .await
        .map_err(CtrlError::meta)?;
    let bundle = build_edge_flag_bundle(&project_id, &flags, Utc::now().timestamp_millis());

    if let Some(inm) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        if inm.trim() == bundle.etag {
            return Ok(
                (StatusCode::NOT_MODIFIED, [(header::ETAG, bundle.etag)], "").into_response(),
            );
        }
    }

    Ok((
        StatusCode::OK,
        [(header::ETAG, bundle.etag.clone())],
        Json(bundle),
    )
        .into_response())
}

async fn handle_list_experiments(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<Experiment>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl
        .meta
        .list_experiments(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct CreateExpReq {
    id: Option<String>,
    name: String,
    flag_id: Option<String>,
    flag_key: String,
    hypothesis: String,
    status: Option<String>,
    start_ts_ms: Option<i64>,
    end_ts_ms: Option<i64>,
    primary_metric: ExperimentMetricSpec,
    #[serde(default)]
    guardrail_metrics: Vec<ExperimentMetricSpec>,
}

async fn handle_create_experiment(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateExpReq>,
) -> ApiRes<Experiment> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "experiment id is generated by the server and must not be client-supplied",
        ));
    }
    let now = Utc::now().timestamp_millis();
    let exp = Experiment {
        id: format!("exp_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id.clone(),
        name: req.name,
        flag_id: req
            .flag_id
            .unwrap_or_else(|| format!("flg_{}_{}", id, req.flag_key)),
        flag_key: req.flag_key,
        hypothesis: req.hypothesis,
        status: req.status.unwrap_or_else(|| "running".to_string()),
        start_ts_ms: req.start_ts_ms.unwrap_or(now - 14 * 86_400_000),
        end_ts_ms: req.end_ts_ms.unwrap_or(now + 14 * 86_400_000),
        primary_metric: req.primary_metric,
        guardrail_metrics: req.guardrail_metrics,
        created_at_ms: now,
    };
    ctrl.meta
        .upsert_experiment(&exp)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(exp))
}

async fn handle_experiment_results_scoped(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((project_id, exp_id)): AxumPath<(String, String)>,
) -> ApiRes<crate::models::ExperimentResults> {
    ctrl.authorize_and_check(&principal, &project_id).await?;
    let Some(exp) = ctrl
        .meta
        .get_experiment(&project_id, &exp_id)
        .await
        .map_err(CtrlError::meta)?
    else {
        return Err(CtrlError::not_found("experiment not found"));
    };
    let flags = ctrl
        .meta
        .list_feature_flags(&project_id)
        .await
        .map_err(CtrlError::meta)?;
    let flag = flags
        .iter()
        .find(|f| f.id == exp.flag_id || f.key == exp.flag_key);
    let results = evaluate_experiment_results(&exp, flag, &ctrl.qengine)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(results))
}

// ------------------- MCP JSON-RPC 2.0 (`P0-04`) -------------------

async fn handle_mcp(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> std::result::Result<Json<crate::mcp::JsonRpcResponse>, CtrlError> {
    let header_pid = headers.get("x-pulse-project").and_then(|v| v.to_str().ok());
    let bound_pid = principal.resolve_project(header_pid, &ctrl.default_project_id)?;
    ctrl.ensure_known_project(&bound_pid).await?;
    let resp = handle_mcp_request(req, &bound_pid, &ctrl.meta, &ctrl.qengine).await;
    Ok(Json(resp))
}
