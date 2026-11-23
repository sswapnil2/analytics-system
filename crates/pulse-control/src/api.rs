use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{MatchedPath, Path as AxumPath, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Extension, Json, Router};
use chrono::Utc;
use constant_time_eq::constant_time_eq;
use dashmap::DashMap;
use object_store::{path::Path as ObjectPath, ObjectStore, PutPayload};
use parking_lot::Mutex;
use pulse_agent::{AgentTraceStore, AgentTraceStoreConfig, StoreError as AgentStoreError};
use pulse_exec::{
    validate_cohort_acyclic, BehavioralCohortDef, CustomEventDef, CustomPropertyDef, FlowsQuery,
    FunnelQuery, InsightsQuery, LookupTableDef, QueryEngine, RetentionQuery, SessionSearchQuery,
    TimeRange,
};
use pulse_ingest::{
    rate_limit::RateLimiter, DiscoveredEventSchema, GovernanceProvider, IngestError, IngestService,
    ProjectLexiconRules,
};
use pulse_storage::{
    build_customer_object_store, make_key_id, parse_or_derive_master_key,
    validate_object_store_connection, validate_outbound_url_ssrf,
};
use pulse_wire::{mask_user_identifier, PropValue, WireBatch, WireEvent, WireframeOp};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use roaring::RoaringBitmap;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

mod accounts;
mod auth;
pub use auth::AuthConfig;

use crate::agent_eval_worker::{
    run_agent_eval_worker, AgentIngestMetrics, EvalJob, EVAL_QUEUE_CAPACITY,
};

use crate::copilot::{run_byok_copilot, CopilotError, CopilotRequest};
use crate::db::{MetaError, MetaStore};
use crate::experiments::{build_edge_flag_bundle_with_cohorts, evaluate_experiment_results};
use crate::mcp::JsonRpcRequest;
use crate::models::{
    apply_profile_ops, generate_random_api_key, generate_root_admin_token, mask_byok_secret,
    mask_key_prefix, seal_byok_config_json, seal_secret, sha256_bytes, sha256_hex,
    unseal_byok_config_json, unseal_secret, AlertRule, Annotation, ApiKeyRecord,
    ByokCredentialRecord, ByokCredentialView, CohortRecord, CreatedApiKeyResponse,
    CustomSchemaBundle, Dashboard, DashboardCard, DeletionAuditRecord, EnterpriseAuditRecord,
    Experiment, ExperimentMetricSpec, FeatureFlag, GroupProfileRecord, LexiconEvent,
    LexiconProperty, Organization, Project, Role, SavedReport, SystemKeyringRecord,
    UserProfileRecord, VariantAllocation, BYOK_CATEGORIES, KEY_ROLE_ADMIN,
    KEY_ROLE_INGEST_AND_FLAGS, REPORT_KINDS,
};

#[derive(Debug, Clone)]
pub enum AuthPrincipal {
    RootAdmin,
    ProjectAdmin {
        project_id: String,
    },
    /// Dashboard user signed in with a `ps_` session token. Sees only its organisation's projects.
    User {
        user_id: String,
        org_id: String,
        role: Role,
    },
}

impl AuthPrincipal {
    /// The root token acts as an owner everywhere; a project admin key as an admin of its project.
    fn role(&self) -> Role {
        match self {
            AuthPrincipal::RootAdmin => Role::Owner,
            AuthPrincipal::ProjectAdmin { .. } => Role::Admin,
            AuthPrincipal::User { role, .. } => *role,
        }
    }

    fn authorize_project(&self, target_project_id: &str) -> Result<(), CtrlError> {
        match self {
            AuthPrincipal::RootAdmin | AuthPrincipal::User { .. } => Ok(()),
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
            AuthPrincipal::RootAdmin | AuthPrincipal::User { .. } => Ok(requested
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

    pub(crate) fn actor_label(&self) -> String {
        match self {
            AuthPrincipal::RootAdmin => "root_admin".to_string(),
            AuthPrincipal::User { user_id, .. } => format!("user:{user_id}"),
            AuthPrincipal::ProjectAdmin { project_id } => format!("project_admin:{project_id}"),
        }
    }

    /// Encryption keys and the audit log: root admin token only. Studio users (any self-registered
    /// account in an allowed domain) and project keys are refused.
    fn require_cluster_admin(&self) -> Result<String, CtrlError> {
        match self {
            AuthPrincipal::RootAdmin => Ok(self.actor_label()),
            AuthPrincipal::User { .. } | AuthPrincipal::ProjectAdmin { .. } => Err(
                CtrlError::forbidden("cluster administration requires the root admin token"),
            ),
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
    agent_store: parking_lot::RwLock<Arc<AgentTraceStore>>,
    eval_tx: mpsc::Sender<EvalJob>,
    eval_rx: parking_lot::Mutex<Option<mpsc::Receiver<EvalJob>>>,
    agent_metrics: Arc<AgentIngestMetrics>,
    default_project_id: String,
    root_admin_hash: [u8; 32],
    vault_kek: [u8; 32],
    has_durable_kek: bool,
    bootstrap_creds: Mutex<BootstrapCredentials>,
    /// Keyed by lowercase SHA-256 hex digest.
    api_key_cache: DashMap<String, CachedApiKey>,
    /// Project id → owning org id.
    known_projects: DashMap<String, String>,
    lexicon_cache: DashMap<String, Arc<ProjectLexiconRules>>,
    /// Per project: the update time up to which user profiles are loaded into the catalog.
    profiles_synced_ms: DashMap<String, i64>,
    /// Per project: `publish_seq` last loaded into `ColumnarProfileIndex` on `NodeRole::Query`.
    synced_published_seq: DashMap<String, u64>,
    /// Per project: `canonical_uid -> merged updated_at_ms` currently stored in `ColumnarProfileIndex`.
    profile_catalog_ts: DashMap<String, HashMap<u32, i64>>,
    discovery_tx: mpsc::UnboundedSender<Vec<DiscoveredEventSchema>>,
    auth_cfg: parking_lot::RwLock<AuthConfig>,
    auth_state: auth::AuthState,
    /// Owner-only file that sign-up, reset and invite links go to (Pulse has no mailer).
    outbox_path: parking_lot::RwLock<Option<std::path::PathBuf>>,
    agent_rate_limit: RateLimiter,
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
        Self::new_with_options(
            meta,
            qengine,
            default_project_id,
            initial_ingest_key,
            explicit_admin_token,
            None,
        )
        .await
    }

    pub async fn new_with_admin_with_agent_config(
        meta: MetaStore,
        qengine: Arc<QueryEngine>,
        default_project_id: impl Into<String>,
        initial_ingest_key: impl Into<String>,
        explicit_admin_token: Option<String>,
        agent_store_config: AgentTraceStoreConfig,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_options(
            meta,
            qengine,
            default_project_id,
            initial_ingest_key,
            explicit_admin_token,
            Some(agent_store_config),
        )
        .await
    }

    async fn new_with_options(
        meta: MetaStore,
        qengine: Arc<QueryEngine>,
        default_project_id: impl Into<String>,
        initial_ingest_key: impl Into<String>,
        explicit_admin_token: Option<String>,
        agent_config: Option<AgentTraceStoreConfig>,
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
        let (vault_kek, has_durable_kek) = match meta.resolve_or_create_cluster_kek().await? {
            Some(kek) => (kek, true),
            None => {
                let mut eph = [0u8; 32];
                SystemRandom::new()
                    .fill(&mut eph)
                    .map_err(|_| "CSPRNG failure generating ephemeral cluster KEK".to_string())?;
                (eph, false)
            }
        };

        // Restore the BYOK encryption keyring BEFORE opening AgentTraceStore so
        // encrypted manifests and WAL segments decrypt cleanly on restart.
        Self::restore_encryption_keyring_inner(&meta, &vault_kek, &qengine).await?;

        let store_cfg = agent_config.unwrap_or_else(|| AgentTraceStoreConfig {
            read_only: qengine.storage().identity_router().is_read_only(),
            ..Default::default()
        });
        let agent_store = Arc::new(
            AgentTraceStore::open_with_encrypted(
                qengine.storage().object_store().clone(),
                Some(qengine.storage().encrypted_store().clone()),
                store_cfg,
            )
            .await
            .map_err(|e| format!("failed to open AgentTraceStore: {e}"))?,
        );
        let (eval_tx, eval_rx) = mpsc::channel::<EvalJob>(EVAL_QUEUE_CAPACITY);
        let agent_metrics = Arc::new(AgentIngestMetrics::default());

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<DiscoveredEventSchema>>();
        let svc = Arc::new(Self {
            meta: meta.clone(),
            qengine,
            ingest: tokio::sync::RwLock::new(None),
            agent_store: parking_lot::RwLock::new(agent_store),
            eval_tx,
            eval_rx: parking_lot::Mutex::new(Some(eval_rx)),
            agent_metrics,
            default_project_id,
            root_admin_hash,
            vault_kek,
            has_durable_kek,
            bootstrap_creds: Mutex::new(BootstrapCredentials {
                generated_admin_token,
                generated_ingest_key: None,
            }),
            api_key_cache: DashMap::new(),
            known_projects: DashMap::new(),
            lexicon_cache: DashMap::new(),
            profiles_synced_ms: DashMap::new(),
            synced_published_seq: DashMap::new(),
            profile_catalog_ts: DashMap::new(),
            discovery_tx: tx,
            auth_cfg: parking_lot::RwLock::new(AuthConfig::from_env()),
            auth_state: auth::AuthState::default(),
            outbox_path: parking_lot::RwLock::new(None),
            agent_rate_limit: RateLimiter::from_env(),
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

    /// Takes one-time randomly generated startup credentials so `pulse-node` can log them once and clear memory.
    pub fn take_bootstrap_credentials(&self) -> BootstrapCredentials {
        std::mem::take(&mut *self.bootstrap_creds.lock())
    }

    pub async fn attach_ingest(&self, ingest: Arc<IngestService>) {
        *self.ingest.write().await = Some(ingest);
    }

    pub fn agent_store(&self) -> Arc<AgentTraceStore> {
        self.agent_store.read().clone()
    }

    /// Replaces the active `AgentTraceStore`. Call before `spawn_agent_eval_worker`.
    pub fn attach_agent_store(&self, store: Arc<AgentTraceStore>) {
        *self.agent_store.write() = store;
    }

    /// Swaps the active `AgentTraceStore` and closes the previous instance.
    pub async fn replace_agent_store(
        &self,
        store: Arc<AgentTraceStore>,
    ) -> Result<(), AgentStoreError> {
        let prev = {
            let mut guard = self.agent_store.write();
            std::mem::replace(&mut *guard, store)
        };
        prev.close().await
    }

    pub fn agent_metrics(&self) -> &Arc<AgentIngestMetrics> {
        &self.agent_metrics
    }

    pub fn eval_tx(&self) -> &mpsc::Sender<EvalJob> {
        &self.eval_tx
    }

    /// Non-blocking enqueue onto the bounded eval queue; increments `eval_queue_dropped` if full/closed.
    pub fn try_enqueue_eval(&self, job: EvalJob) -> bool {
        match self.eval_tx.try_send(job) {
            Ok(()) => true,
            Err(_) => {
                self.agent_metrics
                    .eval_queue_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }
        }
    }

    pub fn take_eval_rx(&self) -> Option<mpsc::Receiver<EvalJob>> {
        self.eval_rx.lock().take()
    }

    /// Spawns the background evaluation worker once (returns `None` if already taken/spawned).
    pub fn spawn_agent_eval_worker(
        &self,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let rx = self.take_eval_rx()?;
        let meta = self.meta.clone();
        let store = self.agent_store();
        let vault_kek = self.vault_kek;
        let metrics = self.agent_metrics.clone();
        Some(tokio::spawn(async move {
            run_agent_eval_worker(meta, store, vault_kek, metrics, rx, shutdown_rx).await;
        }))
    }

    #[allow(dead_code)]
    pub(crate) fn vault_kek(&self) -> [u8; 32] {
        self.vault_kek
    }

    pub(crate) fn agent_rate_limit(&self) -> &RateLimiter {
        &self.agent_rate_limit
    }

    pub(crate) async fn authenticate_api_key_str(&self, api_key: &str) -> Option<String> {
        <Self as GovernanceProvider>::authenticate_api_key(self, api_key).await
    }

    pub fn meta(&self) -> &MetaStore {
        &self.meta
    }

    /// Checks the metadata database answers, for `/readyz`.
    pub async fn ping_metadata(&self) -> Result<(), String> {
        self.meta.ping().await.map_err(|e| e.to_string())
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

    async fn refresh_api_key_cache(&self) -> Result<(), String> {
        let keys = self
            .meta
            .list_api_keys(None)
            .await
            .map_err(|e| e.to_string())?;
        // Upsert, then drop what is gone. Clearing first would 401 every ingest and flag
        // request that looks a key up mid-reload: the cache has no DB fallback.
        let mut live: HashSet<String> = HashSet::with_capacity(keys.len());
        for k in keys {
            if !k.revoked {
                live.insert(k.key_hash.clone());
                self.api_key_cache.insert(
                    k.key_hash,
                    CachedApiKey {
                        project_id: k.project_id,
                        role: k.role,
                    },
                );
            }
        }
        self.api_key_cache.retain(|hash, _| live.contains(hash));
        Ok(())
    }

    pub async fn reload_caches_from_db(&self) -> Result<(), String> {
        if let Err(e) = self.refresh_encryption_keyring().await {
            tracing::warn!(error = %e, "failed to refresh BYOK keyring");
        }
        self.refresh_api_key_cache().await?;

        let projects = self.meta.list_projects().await.map_err(|e| e.to_string())?;
        let live_projects: HashSet<&str> = projects.iter().map(|p| p.id.as_str()).collect();
        self.known_projects
            .retain(|id, _| live_projects.contains(id.as_str()));
        // One project's broken catalog must not stop the others from reloading.
        let mut first_err = None;
        for proj in &projects {
            self.known_projects
                .insert(proj.id.clone(), proj.org_id.clone());
            let res = async {
                self.refresh_project_lexicon_cache(&proj.id).await?;
                self.refresh_project_virtual_catalog(&proj.id).await
            }
            .await;
            if let Err(e) = res {
                first_err.get_or_insert(format!("project {}: {e}", proj.id));
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// The project's org id; 404 when the project doesn't exist.
    async fn project_org(&self, project_id: &str) -> Result<String, CtrlError> {
        if let Some(org) = self.known_projects.get(project_id) {
            return Ok(org.clone());
        }
        match self
            .meta
            .get_project(project_id)
            .await
            .map_err(CtrlError::internal)?
        {
            Some(p) => {
                self.known_projects.insert(p.id, p.org_id.clone());
                Ok(p.org_id)
            }
            None => Err(CtrlError::not_found(format!(
                "project '{project_id}' not found"
            ))),
        }
    }

    /// The project a request targets: the one it names, or else the caller's own default (for a
    /// user, the first project in their org). Then checks the caller may use it.
    async fn resolve_and_check(
        &self,
        principal: &AuthPrincipal,
        requested: Option<&str>,
    ) -> Result<String, CtrlError> {
        let requested = requested.filter(|s| !s.is_empty());
        let pid = match (principal, requested) {
            (AuthPrincipal::User { org_id, .. }, None) => self
                .meta
                .list_projects()
                .await
                .map_err(CtrlError::meta)?
                .into_iter()
                .find(|p| &p.org_id == org_id)
                .map(|p| p.id)
                .ok_or_else(|| CtrlError::not_found("your organisation has no projects yet"))?,
            _ => principal.resolve_project(requested, &self.default_project_id)?,
        };
        self.authorize_and_check(principal, &pid).await?;
        Ok(pid)
    }

    /// Projects the caller can see.
    async fn visible_projects(&self, principal: &AuthPrincipal) -> Result<Vec<Project>, CtrlError> {
        let all = self.meta.list_projects().await.map_err(CtrlError::meta)?;
        Ok(match principal {
            AuthPrincipal::RootAdmin => all,
            AuthPrincipal::User { org_id, .. } => {
                all.into_iter().filter(|p| &p.org_id == org_id).collect()
            }
            AuthPrincipal::ProjectAdmin { project_id } => {
                all.into_iter().filter(|p| &p.id == project_id).collect()
            }
        })
    }

    pub(crate) async fn authorize_and_check(
        &self,
        principal: &AuthPrincipal,
        project_id: &str,
    ) -> Result<(), CtrlError> {
        principal.authorize_project(project_id)?;
        let org = self.project_org(project_id).await?;
        if let AuthPrincipal::User { org_id, .. } = principal {
            if &org != org_id {
                // Same answer as a missing project, so other orgs' project ids don't leak.
                return Err(CtrlError::not_found(format!(
                    "project '{project_id}' not found"
                )));
            }
        }
        Ok(())
    }

    /// Restores the encryption keyring across restarts and key rotations, failing closed if sealed secrets cannot decrypt.
    async fn restore_encryption_keyring_inner(
        meta: &MetaStore,
        vault_kek: &[u8; 32],
        qengine: &QueryEngine,
    ) -> Result<(), String> {
        let keyring_rows = meta
            .list_system_keyring()
            .await
            .map_err(|e| e.to_string())?;
        if keyring_rows.is_empty() {
            return Ok(());
        }
        let enc_store = qengine.storage().encrypted_store();
        for row in &keyring_rows {
            if enc_store.has_master_key(&row.key_id) {
                if row.is_active {
                    enc_store.activate_master_key(&row.key_id);
                }
                continue;
            }
            let secret = unseal_secret(vault_kek, &row.sealed_key_hex).map_err(|e| {
                format!(
                    "failed to unseal cluster encryption key '{}': {e}",
                    row.key_id
                )
            })?;
            let salt_bytes = hex::decode(row.salt_hex.trim())
                .map_err(|e| format!("corrupted salt_hex for keyring '{}': {e}", row.key_id))?;
            if salt_bytes.len() != 16 {
                return Err(format!("keyring '{}' salt must be 16 bytes", row.key_id));
            }
            let mut salt = [0u8; 16];
            salt.copy_from_slice(&salt_bytes);
            let master = parse_or_derive_master_key(&secret, &salt)
                .map_err(|e| format!("failed to derive master key for '{}': {e}", row.key_id))?;
            qengine
                .storage()
                .register_byok_master_key(&row.key_id, master, row.is_active)
                .map_err(|e| e.to_string())?;
        }
        if keyring_rows.iter().all(|r| !r.is_active) {
            qengine.storage().disable_new_encryption();
        }
        Ok(())
    }

    pub async fn refresh_encryption_keyring(&self) -> Result<(), String> {
        Self::restore_encryption_keyring_inner(&self.meta, &self.vault_kek, &self.qengine).await
    }

    /// Anything sealed under an ephemeral KEK is unreadable after a restart, so refuse to seal.
    fn require_durable_kek(&self) -> Result<(), CtrlError> {
        let allow_ephemeral = matches!(
            std::env::var("PULSE_ALLOW_EPHEMERAL_KEK").as_deref(),
            Ok("1" | "true")
        );
        if !self.has_durable_kek && !allow_ephemeral {
            return Err(CtrlError::bad(
                "cannot store secrets without a durable cluster KEK (set PULSE_VAULT_KEK, or use file-backed SQLite)",
            ));
        }
        Ok(())
    }

    pub(crate) async fn record_enterprise_audit(
        &self,
        actor: &str,
        action: &str,
        resource: &str,
        details_json: Value,
    ) {
        let rec = EnterpriseAuditRecord {
            id: format!("aud_{}", &Uuid::new_v4().simple().to_string()[..12]),
            actor: actor.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            details_json,
            created_at_ms: Utc::now().timestamp_millis(),
        };
        let _ = self.meta.insert_enterprise_audit(&rec).await;
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

    /// Loads user profiles into the query catalog: every profile on the first call for a
    pub(crate) fn from_stored_profile(p: pulse_storage::StoredUserProfile) -> UserProfileRecord {
        UserProfileRecord {
            project_id: p.project_id,
            distinct_id: p.distinct_id,
            canonical_uid: p.canonical_uid,
            props: p.props,
            total_revenue: p.total_revenue,
            updated_at_ms: p.updated_at_ms,
        }
    }

    pub(crate) fn to_stored_profile(p: &UserProfileRecord) -> pulse_storage::StoredUserProfile {
        pulse_storage::StoredUserProfile {
            project_id: p.project_id.clone(),
            distinct_id: p.distinct_id.clone(),
            canonical_uid: p.canonical_uid,
            props: p.props.clone(),
            total_revenue: p.total_revenue,
            updated_at_ms: p.updated_at_ms,
        }
    }

    fn evict_erased_from_catalog(
        &self,
        project_id: &str,
        expanded: &pulse_storage::ErasedUids,
        id_graph: Option<&pulse_identity::ProjectIdentityGraph>,
    ) {
        if !expanded.is_empty() {
            let mut hidden_uids = Vec::new();
            if let Some(mut ts_map) = self.profile_catalog_ts.get_mut(project_id) {
                let id_snap = id_graph.map(|ig| ig.snapshot());
                ts_map.retain(|&cat_uid, &mut prof_ts| {
                    let resolved = id_snap.as_ref().map_or(cat_uid, |s| s.resolve(cat_uid));
                    let hidden =
                        expanded.hides(cat_uid, prof_ts) || expanded.hides(resolved, prof_ts);
                    if hidden {
                        hidden_uids.push(cat_uid);
                    }
                    !hidden
                });
            }
            if !hidden_uids.is_empty() {
                if let Some(cat) = self.qengine.catalogs().get_project(project_id) {
                    for uid in hidden_uids {
                        cat.remove_user_profile(uid);
                    }
                }
            }
        }
    }

    async fn load_erased_for_project_ingest(
        &self,
        project_id: &str,
    ) -> std::result::Result<
        (
            pulse_storage::ErasedUids,
            Option<Arc<pulse_identity::ProjectIdentityGraph>>,
        ),
        IngestError,
    > {
        let (expanded, id_graph) =
            crate::mcp::sync_erased_for_project_typed(project_id, &self.meta, &self.qengine)
                .await
                .map_err(|e| match e {
                    crate::mcp::SyncErasedError::Meta(me) => meta_err_to_ingest(me),
                    crate::mcp::SyncErasedError::Storage(se) => storage_err_to_ingest(se),
                })?;
        self.evict_erased_from_catalog(project_id, &expanded, id_graph.as_deref());
        Ok((expanded, id_graph))
    }

    async fn load_erased_for_project_ctrl(
        &self,
        project_id: &str,
    ) -> Result<
        (
            pulse_storage::ErasedUids,
            Option<Arc<pulse_identity::ProjectIdentityGraph>>,
        ),
        CtrlError,
    > {
        self.load_erased_for_project_ingest(project_id)
            .await
            .map_err(CtrlError::ingest)
    }

    /// Loads `erased_uids` from `MetaStore`, opens the project's read-only identity graph when
    /// non-empty, expands merged raw->canonical mappings, syncs `StorageEngine::set_erased`, and
    /// evicts any hidden entries from `ColumnarProfileIndex` (`cat.user_profiles` and `profile_catalog_ts`).
    async fn load_erased_for_project(
        &self,
        project_id: &str,
    ) -> Result<
        (
            pulse_storage::ErasedUids,
            Option<Arc<pulse_identity::ProjectIdentityGraph>>,
        ),
        String,
    > {
        self.load_erased_for_project_ingest(project_id)
            .await
            .map_err(|e| e.to_string())
    }

    /// Syncs the project's `ColumnarProfileIndex` from the object-store-backed SlateDB `ProfileStore`.
    /// On the first call for a project, migrates any legacy SQL `user_profiles` rows automatically.
    async fn refresh_user_profiles(
        &self,
        project_id: &str,
        expanded: &pulse_storage::ErasedUids,
        id_graph: Option<&pulse_identity::ProjectIdentityGraph>,
    ) -> Result<(), String> {
        let mut published_seq_opt = self
            .qengine
            .storage()
            .published_profile_snapshot_key(project_id)
            .map(|(seq, _)| seq);
        if let Some(pub_seq) = published_seq_opt {
            if self
                .synced_published_seq
                .get(project_id)
                .is_some_and(|v| *v == pub_seq)
            {
                return Ok(());
            }
        }

        let cat = self.qengine.catalogs().for_project(project_id);
        let pstore = self
            .qengine
            .storage()
            .profile_store(project_id, false)
            .await
            .map_err(|e| e.to_string())?;
        if published_seq_opt.is_none() {
            published_seq_opt = self
                .qengine
                .storage()
                .published_profile_snapshot_key(project_id)
                .map(|(seq, _)| seq);
            if let Some(pub_seq) = published_seq_opt {
                if self
                    .synced_published_seq
                    .get(project_id)
                    .is_some_and(|v| *v == pub_seq)
                {
                    return Ok(());
                }
            }
        }
        let started_ms = Utc::now().timestamp_millis();
        let since = if published_seq_opt.is_some() {
            None
        } else {
            self.profiles_synced_ms.get(project_id).map(|v| *v)
        };
        match since {
            None => {
                let mut stored_rows = pstore
                    .list_updated_since(0)
                    .await
                    .map_err(|e| e.to_string())?;
                if stored_rows.is_empty() && !self.qengine.storage().profile_router().is_read_only()
                {
                    if let Ok(legacy) = self
                        .meta
                        .list_user_profiles_updated_since(project_id, 0)
                        .await
                    {
                        if !legacy.is_empty() {
                            let filtered_stored: Vec<_> = legacy
                                .into_iter()
                                .map(|r| Self::to_stored_profile(&r))
                                .filter(|s| {
                                    !pulse_storage::is_profile_erased(s, expanded, id_graph)
                                })
                                .collect();
                            if !filtered_stored.is_empty() {
                                let _ = pstore.upsert_batch(&filtered_stored).await;
                            }
                            stored_rows = filtered_stored;
                        }
                    }
                }
                let rows: Vec<UserProfileRecord> = stored_rows
                    .into_iter()
                    .filter(|s| !pulse_storage::is_profile_erased(s, expanded, id_graph))
                    .map(Self::from_stored_profile)
                    .collect();
                let merged_map = merge_profile_rows(rows);
                let ts_map: HashMap<u32, i64> = merged_map
                    .iter()
                    .map(|(&uid, p)| (uid, p.updated_at_ms))
                    .collect();
                self.profile_catalog_ts
                    .insert(project_id.to_string(), ts_map.clone());
                let merged = merged_map
                    .into_iter()
                    .map(|(uid, p)| (uid, profile_to_catalog_props(&p)))
                    .collect();
                cat.set_user_profiles_with_ts(merged, ts_map);
            }
            Some(since) => {
                let rows = pstore
                    .list_updated_since(since)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut uids: std::collections::BTreeSet<u32> =
                    rows.iter().map(|r| r.canonical_uid).collect();
                // Evict profiles newly hidden by `erased_uids` (e.g. erased on another node) even
                // when `pstore` was not modified after `since`.
                if !expanded.is_empty() {
                    if let Some(ts_map) = self.profile_catalog_ts.get(project_id) {
                        let id_snap = id_graph.map(|ig| ig.snapshot());
                        for (&cat_uid, &prof_ts) in ts_map.value() {
                            let resolved = id_snap.as_ref().map_or(cat_uid, |s| s.resolve(cat_uid));
                            if expanded.hides(cat_uid, prof_ts) || expanded.hides(resolved, prof_ts)
                            {
                                uids.insert(cat_uid);
                            }
                        }
                    }
                }
                for uid in uids {
                    self.rebuild_user_profile_with_erased(project_id, uid, expanded, id_graph)
                        .await?;
                }
            }
        }
        // Re-read a little overlap next time so a write that committed with an older
        // timestamp while this load ran is not missed; rebuilding a user is idempotent.
        self.profiles_synced_ms
            .insert(project_id.to_string(), started_ms - PROFILE_SYNC_OVERLAP_MS);
        if let Some(pub_seq) = published_seq_opt {
            self.synced_published_seq
                .insert(project_id.to_string(), pub_seq);
        }
        Ok(())
    }

    async fn rebuild_user_profile_with_erased(
        &self,
        project_id: &str,
        canonical_uid: u32,
        expanded_erased: &pulse_storage::ErasedUids,
        id_graph: Option<&pulse_identity::ProjectIdentityGraph>,
    ) -> Result<(), String> {
        self.rebuild_user_profile_with_erased_ingest(
            project_id,
            canonical_uid,
            expanded_erased,
            id_graph,
        )
        .await
        .map_err(|e| e.to_string())
    }

    async fn rebuild_user_profile_with_erased_ingest(
        &self,
        project_id: &str,
        canonical_uid: u32,
        expanded_erased: &pulse_storage::ErasedUids,
        id_graph: Option<&pulse_identity::ProjectIdentityGraph>,
    ) -> std::result::Result<(), IngestError> {
        // A read: it rebuilds the in-memory index. The periodic refresh calls it on every node.
        let pstore = self
            .qengine
            .storage()
            .profile_store(project_id, false)
            .await
            .map_err(storage_err_to_ingest)?;
        let rows: Vec<UserProfileRecord> = pstore
            .list_for_canonical(canonical_uid)
            .await
            .map_err(storage_err_to_ingest)?
            .into_iter()
            .filter(|s| !pulse_storage::is_profile_erased(s, expanded_erased, id_graph))
            .map(Self::from_stored_profile)
            .collect();
        let cat = self.qengine.catalogs().for_project(project_id);
        match merge_profile_rows(rows).remove(&canonical_uid) {
            Some(p) => {
                self.profile_catalog_ts
                    .entry(project_id.to_string())
                    .or_default()
                    .insert(canonical_uid, p.updated_at_ms);
                cat.upsert_user_profile_with_ts(
                    canonical_uid,
                    profile_to_catalog_props(&p),
                    p.updated_at_ms,
                );
            }
            None => {
                if let Some(mut map) = self.profile_catalog_ts.get_mut(project_id) {
                    map.remove(&canonical_uid);
                }
                cat.remove_user_profile(canonical_uid);
            }
        }
        Ok(())
    }

    async fn refresh_project_virtual_catalog_metadata(
        &self,
        project_id: &str,
    ) -> Result<(), String> {
        let cat = self.qengine.catalogs().for_project(project_id);

        if let Some(proj) = self
            .meta
            .get_project(project_id)
            .await
            .map_err(|e| e.to_string())?
        {
            cat.set_timezone(&proj.timezone);
        }

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
            cat.insert_lookup_table(def);
        }

        let cohorts = self
            .meta
            .list_cohorts(project_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut active_cohort_ids = HashSet::with_capacity(cohorts.len());
        for c in cohorts {
            active_cohort_ids.insert(c.id.clone());
            cat.saved_cohorts.insert(c.id, c.definition);
        }
        cat.saved_cohorts
            .retain(|id, _| active_cohort_ids.contains(id));

        Ok(())
    }

    pub async fn refresh_project_virtual_catalog(&self, project_id: &str) -> Result<(), String> {
        self.refresh_project_virtual_catalog_metadata(project_id)
            .await?;

        // Sync expanded `erased_uids` into StorageEngine BEFORE refreshing user profiles so
        // initial loads and remote readers always observe fresh erasure tombstones.
        let (expanded, id_graph) = self.load_erased_for_project(project_id).await?;
        self.refresh_user_profiles(project_id, &expanded, id_graph.as_deref())
            .await?;

        Ok(())
    }

    async fn sync_published_project_atomic(&self, project_id: &str) -> Result<bool, String> {
        let storage = self.qengine.storage();
        if !storage.is_published_reader() {
            let (expanded, id_graph) = self.load_erased_for_project(project_id).await?;
            self.refresh_user_profiles(project_id, &expanded, id_graph.as_deref())
                .await?;
            return Ok(false);
        }

        let was_open = storage.get_project(project_id).is_some();
        let ps = storage
            .get_or_open_project(project_id)
            .await
            .map_err(|e| e.to_string())?;
        let _refresh_guard = ps.lock_refresh().await;

        let prepared = ps
            .prepare_published_refresh()
            .await
            .map_err(|e| e.to_string())?;
        let target_pub_seq = prepared
            .as_ref()
            .map(|p| p.publish_seq())
            .or_else(|| ps.last_published_seq());

        let need_profile_reload = target_pub_seq.is_some_and(|seq| {
            self.synced_published_seq
                .get(project_id)
                .is_none_or(|v| *v != seq)
        });

        let cat = self.qengine.catalogs().for_project(project_id);
        let lock_timeout = self
            .qengine
            .query_timeout()
            .min(std::time::Duration::from_secs(10))
            .max(std::time::Duration::from_millis(500));

        if prepared.is_some() || need_profile_reload {
            let started_ms = Utc::now().timestamp_millis();
            let erased: pulse_storage::ErasedUids = self
                .meta
                .list_erased_uids(project_id)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .collect();

            let target_id_graph = prepared
                .as_ref()
                .and_then(|p| p.id_graph().cloned())
                .unwrap_or_else(|| ps.id_graph());
            let expanded =
                pulse_storage::expand_erased_with_identity(&erased, Some(&target_id_graph));

            let target_pstore = match prepared.as_ref().and_then(|p| p.prof_store().cloned()) {
                Some(p) => p,
                None => storage
                    .profile_store(project_id, false)
                    .await
                    .map_err(|e| e.to_string())?,
            };
            let stored_rows = target_pstore
                .list_updated_since(0)
                .await
                .map_err(|e| e.to_string())?;
            let rows: Vec<UserProfileRecord> = stored_rows
                .into_iter()
                .filter(|s| !pulse_storage::is_profile_erased(s, &expanded, Some(&target_id_graph)))
                .map(Self::from_stored_profile)
                .collect();
            let merged_map = merge_profile_rows(rows);
            let ts_map: HashMap<u32, i64> = merged_map
                .iter()
                .map(|(&uid, p)| (uid, p.updated_at_ms))
                .collect();
            let merged = merged_map
                .into_iter()
                .map(|(uid, p)| (uid, profile_to_catalog_props(&p)))
                .collect();
            let prepared_profiles =
                pulse_exec::VirtualCatalog::prepare_user_profiles(merged, ts_map.clone());

            let Ok(gate_guard) =
                tokio::time::timeout(lock_timeout, cat.acquire_refresh_write()).await
            else {
                tracing::warn!(
                    project_id,
                    "timed out waiting for in-flight queries before atomic published refresh; retrying next tick"
                );
                return Ok(false);
            };

            let applied = if let Some(prep) = prepared {
                ps.apply_prepared_refresh(prep)
            } else {
                !was_open && target_pub_seq.is_some()
            };
            storage.set_erased(project_id, expanded);
            self.profile_catalog_ts
                .insert(project_id.to_string(), ts_map);
            cat.install_prepared_user_profiles(prepared_profiles);
            self.profiles_synced_ms
                .insert(project_id.to_string(), started_ms - PROFILE_SYNC_OVERLAP_MS);
            if let Some(pub_seq) = target_pub_seq {
                self.synced_published_seq
                    .insert(project_id.to_string(), pub_seq);
            }
            drop(gate_guard);

            ps.cleanup_retired_stores().await;
            return Ok(applied);
        }

        // No published manifest yet (e.g. shared-disk pre-pub_0 deployment): fall back to
        // local manifest/identity refresh + incremental profile sync.
        if target_pub_seq.is_none() {
            drop(_refresh_guard);
            let local_refreshed = ps.refresh_manifest().await.map_err(|e| e.to_string())?;
            let (expanded, id_graph) = self.load_erased_for_project(project_id).await?;
            self.refresh_user_profiles(project_id, &expanded, id_graph.as_deref())
                .await?;
            ps.cleanup_retired_stores().await;
            return Ok(local_refreshed);
        }

        // No new publish: still refresh `erased_uids` and drain any retired `query_hot` stores.
        let erased: pulse_storage::ErasedUids = self
            .meta
            .list_erased_uids(project_id)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .collect();
        let id_graph = (!erased.is_empty()).then(|| ps.id_graph());
        let expanded = pulse_storage::expand_erased_with_identity(&erased, id_graph.as_deref());
        if let Ok(gate_guard) =
            tokio::time::timeout(lock_timeout, cat.acquire_refresh_write()).await
        {
            storage.set_erased(project_id, expanded.clone());
            if !expanded.is_empty() {
                let id_snap = id_graph.as_deref().map(|ig| ig.snapshot());
                let evicted = cat.evict_erased_profiles(&expanded, id_snap.as_ref());
                if !evicted.is_empty() {
                    if let Some(mut ts_map) = self.profile_catalog_ts.get_mut(project_id) {
                        for uid in evicted {
                            ts_map.remove(&uid);
                        }
                    }
                }
            }
            drop(gate_guard);
        } else {
            storage.set_erased(project_id, expanded);
        }
        ps.cleanup_retired_stores().await;
        Ok(false)
    }

    /// Atomically refreshes control-plane caches, discovers newly created projects, and syncs
    /// published manifests + `erased_uids` + `ColumnarProfileIndex` for `NodeRole::Query`.
    pub async fn refresh_query_caches_and_manifests(
        &self,
    ) -> (Result<(), String>, Result<usize, String>) {
        let mut control_err: Option<String> = None;
        if let Err(e) = self.refresh_api_key_cache().await {
            control_err.get_or_insert(e);
        }

        let mut refreshed = 0usize;
        let mut manifest_err: Option<String> = None;
        let mut covered_projects: HashSet<String> = HashSet::new();

        match self.meta.list_projects().await {
            Ok(projects) => {
                let live_projects: HashSet<&str> = projects.iter().map(|p| p.id.as_str()).collect();
                self.known_projects
                    .retain(|id, _| live_projects.contains(id.as_str()));
                for proj in &projects {
                    covered_projects.insert(proj.id.clone());
                    if let Err(e) = self.refresh_project_lexicon_cache(&proj.id).await {
                        control_err.get_or_insert(format!("project {}: {e}", proj.id));
                    }
                    if let Err(e) = self
                        .refresh_project_virtual_catalog_metadata(&proj.id)
                        .await
                    {
                        control_err.get_or_insert(format!("project {}: {e}", proj.id));
                    }
                    match self.sync_published_project_atomic(&proj.id).await {
                        Ok(true) => refreshed += 1,
                        Ok(false) => {}
                        Err(e) => {
                            manifest_err.get_or_insert(format!("project {}: {e}", proj.id));
                        }
                    }
                    self.known_projects
                        .insert(proj.id.clone(), proj.org_id.clone());
                }
            }
            Err(e) => {
                control_err.get_or_insert(e.to_string());
            }
        }

        for ps in self.qengine.storage().open_projects() {
            let pid = ps.project_id().to_string();
            if covered_projects.contains(&pid) {
                continue;
            }
            match self.sync_published_project_atomic(&pid).await {
                Ok(true) => refreshed += 1,
                Ok(false) => {}
                Err(e) => {
                    manifest_err.get_or_insert(format!("project {pid}: {e}"));
                }
            }
        }

        let control_res = control_err.map_or(Ok(()), Err);
        let manifest_res = manifest_err.map_or(Ok(refreshed), Err);
        (control_res, manifest_res)
    }

    async fn erase_user_dpdp(
        &self,
        project_id: &str,
        distinct_id: &str,
        requested_by: &str,
    ) -> Result<DeletionAuditRecord, CtrlError> {
        let id_graph = self
            .qengine
            .storage()
            .identity_graph(project_id, true)
            .await
            .map_err(CtrlError::storage)?;

        let pstore = self
            .qengine
            .storage()
            .profile_store(project_id, true)
            .await
            .map_err(CtrlError::storage)?;
        let existing_prof = pstore
            .get_by_distinct_id(distinct_id)
            .await
            .map_err(CtrlError::storage)?
            .map(Self::from_stored_profile);

        let mut uids_bm = RoaringBitmap::new();
        let mut canon_uid = 0u32;
        if let Some(c) = id_graph
            .lookup_canonical(distinct_id)
            .or_else(|| id_graph.lookup_raw(distinct_id))
            .or_else(|| existing_prof.as_ref().map(|p| p.canonical_uid))
        {
            canon_uid = c;
            uids_bm = id_graph.all_raw_uids_for_canonical_root(canon_uid);
        } else if let Some(digits) = distinct_id.strip_prefix("uid_") {
            if let Ok(parsed) = digits.parse::<u32>() {
                canon_uid = parsed;
                uids_bm = id_graph.all_raw_uids_for_canonical_root(canon_uid);
            }
        }

        let mut all_ids = id_graph.external_ids_for_canonical_roots(&uids_bm);
        if !all_ids.iter().any(|x| x == distinct_id) {
            all_ids.push(distinct_id.to_string());
        }

        let mut events_purged = 0u64;
        let mut sessions_to_delete: Vec<String> = Vec::new();
        if !uids_bm.is_empty() {
            if let Ok(evs) = self
                .qengine
                .export_events(project_id, None, Some(canon_uid), 50_000, false)
                .await
            {
                events_purged = evs.len() as u64;
                for ev in evs {
                    if !ev.session_id.is_empty() {
                        sessions_to_delete.push(ev.session_id);
                    }
                }
            }
            if let Ok(ps) = self.qengine.storage().get_or_open_project(project_id).await {
                let (l0_purged, mut l0_sids) = ps.purge_user_l0(&uids_bm);
                if events_purged == 0 {
                    events_purged = l0_purged;
                }
                sessions_to_delete.append(&mut l0_sids);
            }
        }

        sessions_to_delete.sort_unstable();
        sessions_to_delete.dedup();
        let mut replays_purged = 0u64;
        let rstore = self.qengine.storage().replay_store();
        for sid in &sessions_to_delete {
            if rstore
                .delete_session(project_id, sid)
                .await
                .unwrap_or(false)
            {
                replays_purged += 1;
            }
        }

        let id_refs: Vec<&str> = all_ids.iter().map(String::as_str).collect();
        let _ = pstore
            .delete_for_ids(&id_refs)
            .await
            .map_err(CtrlError::storage)?;
        let _ = self
            .meta
            .delete_user_profiles_for_ids(project_id, &all_ids)
            .await;

        let uid_vec: Vec<u32> = uids_bm.iter().collect();
        let erased_at_ms = Utc::now().timestamp_millis();
        self.meta
            .insert_erased_uids(project_id, &uid_vec, erased_at_ms)
            .await
            .map_err(CtrlError::meta)?;

        let storage = self.qengine.storage();
        let mut erased = (*storage.erased_snapshot(project_id)).clone();
        for &u in &uid_vec {
            erased.insert(u, erased_at_ms);
        }
        storage.set_erased(project_id, erased);
        if let Some(mut map) = self.profile_catalog_ts.get_mut(project_id) {
            map.remove(&canon_uid);
        }
        let cat = self.qengine.catalogs().for_project(project_id);
        cat.remove_user_profile(canon_uid);

        let rec = DeletionAuditRecord {
            id: format!("del_{}", &Uuid::new_v4().simple().to_string()[..12]),
            project_id: project_id.to_string(),
            distinct_id_masked: mask_user_identifier(distinct_id),
            canonical_uid: canon_uid,
            events_purged,
            replays_purged,
            requested_by: requested_by.to_string(),
            deleted_at_ms: Utc::now().timestamp_millis(),
        };
        self.meta
            .insert_deletion_audit(&rec)
            .await
            .map_err(CtrlError::meta)?;
        Ok(rec)
    }
}

/// Restores sealed BYOK encryption keys from `system_keyring` into `enc_store` (used by
/// `pulse-node restore --from-published`). Skips KEK resolution when `system_keyring` is empty.
pub async fn restore_encryption_keyring_to_store(
    meta: &MetaStore,
    enc_store: &pulse_storage::EncryptedObjectStore,
) -> Result<(), String> {
    let keyring_rows = meta
        .list_system_keyring()
        .await
        .map_err(|e| e.to_string())?;
    if keyring_rows.is_empty() {
        return Ok(());
    }
    let vault_kek = meta.resolve_or_create_cluster_kek().await?.ok_or_else(|| {
        "system_keyring contains sealed encryption keys, but PULSE_VAULT_KEK is not set".to_string()
    })?;
    for row in keyring_rows {
        let secret = unseal_secret(&vault_kek, &row.sealed_key_hex).map_err(|e| {
            format!(
                "failed to unseal cluster encryption key '{}': {e}",
                row.key_id
            )
        })?;
        let salt_bytes = hex::decode(row.salt_hex.trim())
            .map_err(|e| format!("corrupted salt_hex for keyring '{}': {e}", row.key_id))?;
        if salt_bytes.len() != 16 {
            return Err(format!("keyring '{}' salt must be 16 bytes", row.key_id));
        }
        let mut salt = [0u8; 16];
        salt.copy_from_slice(&salt_bytes);
        let master = parse_or_derive_master_key(&secret, &salt)
            .map_err(|e| format!("failed to derive master key for '{}': {e}", row.key_id))?;
        enc_store
            .register_master_key(&row.key_id, master, row.is_active)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// How far back each incremental profile reload re-reads, to cover writes that were in flight.
const PROFILE_SYNC_OVERLAP_MS: i64 = 60_000;

/// Merges profile rows (oldest first) into one record per canonical user: for each property
/// the most recently written value wins, and revenue adds up across the user's ids.
pub(crate) fn merge_profile_rows(
    rows: Vec<UserProfileRecord>,
) -> std::collections::BTreeMap<u32, UserProfileRecord> {
    let mut out = std::collections::BTreeMap::<u32, UserProfileRecord>::new();
    for row in rows {
        match out.get_mut(&row.canonical_uid) {
            Some(m) => {
                m.props.extend(row.props);
                m.total_revenue += row.total_revenue;
                if row.updated_at_ms >= m.updated_at_ms {
                    m.updated_at_ms = row.updated_at_ms;
                    m.distinct_id = row.distinct_id;
                }
            }
            None => {
                out.insert(row.canonical_uid, row);
            }
        }
    }
    out
}

/// Catalog view of a merged profile; `total_revenue` is queryable as `user.total_revenue`.
fn profile_to_catalog_props(p: &UserProfileRecord) -> HashMap<String, PropValue> {
    let mut m: HashMap<String, PropValue> = p
        .props
        .iter()
        .map(|(k, v)| (k.clone(), json_value_to_prop_value(v)))
        .collect();
    m.insert(
        "total_revenue".to_string(),
        PropValue::Float(p.total_revenue),
    );
    m
}

fn json_value_to_prop_value(v: &Value) -> PropValue {
    match v {
        Value::Null => PropValue::Null,
        Value::Bool(b) => PropValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PropValue::Float(f)
            } else {
                PropValue::Null
            }
        }
        Value::String(s) => PropValue::Str(s.clone()),
        Value::Array(_) | Value::Object(_) => {
            PropValue::Str(serde_json::to_string(v).unwrap_or_default())
        }
    }
}

fn is_sqlx_unavailable(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) => true,
        sqlx::Error::Database(db_err) => {
            let code = db_err.code();
            if matches!(
                code.as_deref(),
                Some(
                    "5" | "6"
                        | "517"
                        | "262"
                        | "57P01"
                        | "57P02"
                        | "57P03"
                        | "08000"
                        | "08003"
                        | "08006"
                        | "53300"
                )
            ) {
                return true;
            }
            let msg = db_err.message().to_ascii_lowercase();
            msg.contains("database is locked") || msg.contains("database is busy")
        }
        _ => false,
    }
}

fn meta_err_to_ingest(err: MetaError) -> IngestError {
    match err {
        MetaError::Conflict(msg) => IngestError::Conflict(msg),
        MetaError::Sqlx(ref e) if is_sqlx_unavailable(e) => {
            IngestError::Unavailable(err.to_string())
        }
        other => IngestError::Governance(other.to_string()),
    }
}

fn storage_err_to_ingest(err: pulse_storage::StorageError) -> IngestError {
    IngestError::Storage(err)
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

    async fn apply_engage(
        &self,
        project_id: &str,
        distinct_id: &str,
        canonical_uid: u32,
        ops: &Value,
    ) -> std::result::Result<u32, IngestError> {
        let pstore = self
            .qengine
            .storage()
            .profile_store(project_id, true)
            .await
            .map_err(storage_err_to_ingest)?;
        let (expanded, id_graph) = self.load_erased_for_project_ingest(project_id).await?;
        let existing = pstore
            .get_by_distinct_id(distinct_id)
            .await
            .map_err(storage_err_to_ingest)?
            .filter(|p| !pulse_storage::is_profile_erased(p, &expanded, id_graph.as_deref()));
        let mut props = existing
            .as_ref()
            .map(|p| p.props.clone())
            .unwrap_or_default();
        let mut total_revenue = existing.as_ref().map(|p| p.total_revenue).unwrap_or(0.0);

        let out = apply_profile_ops(&mut props, &mut total_revenue, ops);
        if out.delete_requested {
            // This path is authenticated only by the public ingest key that ships in client code,
            // so it must never erase data. Erasure goes through the admin-authenticated
            // DELETE /v1/projects/:id/users/:distinct_id.
            tracing::warn!(
                project_id,
                "ignored $delete_user from ingest; erase users with DELETE /v1/projects/:id/users/:distinct_id"
            );
            return Ok(canonical_uid);
        }
        if !out.modified && existing.is_some() {
            return Ok(canonical_uid);
        }

        let rec = pulse_storage::StoredUserProfile {
            project_id: project_id.to_string(),
            distinct_id: distinct_id.to_string(),
            canonical_uid,
            props,
            total_revenue,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        pstore.upsert(&rec).await.map_err(storage_err_to_ingest)?;
        self.rebuild_user_profile_with_erased_ingest(
            project_id,
            canonical_uid,
            &expanded,
            id_graph.as_deref(),
        )
        .await?;
        Ok(canonical_uid)
    }

    async fn on_identify(
        &self,
        project_id: &str,
        anon_id: &str,
        user_id: &str,
        canonical_uid: u32,
    ) -> std::result::Result<u32, IngestError> {
        let pstore = self
            .qengine
            .storage()
            .profile_store(project_id, true)
            .await
            .map_err(storage_err_to_ingest)?;
        let (expanded, id_graph) = self.load_erased_for_project_ingest(project_id).await?;
        let mut to_rekey = Vec::with_capacity(2);
        let mut to_purge = Vec::new();
        if !expanded.is_empty() {
            for did in [anon_id, user_id] {
                if let Some(prof) = pstore
                    .get_by_distinct_id(did)
                    .await
                    .map_err(storage_err_to_ingest)?
                {
                    if pulse_storage::is_profile_erased(&prof, &expanded, id_graph.as_deref()) {
                        to_purge.push(did);
                        continue;
                    }
                }
                to_rekey.push(did);
            }
            if !to_purge.is_empty() {
                pstore
                    .delete_for_ids(&to_purge)
                    .await
                    .map_err(storage_err_to_ingest)?;
            }
        } else {
            to_rekey.push(anon_id);
            to_rekey.push(user_id);
        }
        // Rows written before the merge carry the pre-merge uid; point both ids at the
        // canonical user so the anonymous traits show on the identified user.
        if !to_rekey.is_empty() {
            pstore
                .rekey(&to_rekey, canonical_uid, Utc::now().timestamp_millis())
                .await
                .map_err(storage_err_to_ingest)?;
        }
        self.rebuild_user_profile_with_erased_ingest(
            project_id,
            canonical_uid,
            &expanded,
            id_graph.as_deref(),
        )
        .await?;
        Ok(canonical_uid)
    }

    async fn apply_group_engage(
        &self,
        project_id: &str,
        group_type: &str,
        group_id: &str,
        ops: &Value,
    ) -> std::result::Result<(), IngestError> {
        let existing = self
            .meta
            .get_group_profile(project_id, group_type, group_id)
            .await
            .map_err(meta_err_to_ingest)?;
        let mut props = existing
            .as_ref()
            .map(|g| g.props.clone())
            .unwrap_or_default();
        let mut rev = 0.0;
        let out = apply_profile_ops(&mut props, &mut rev, ops);
        if !out.modified && existing.is_some() {
            return Ok(());
        }
        let rec = GroupProfileRecord {
            project_id: project_id.to_string(),
            group_type: group_type.to_string(),
            group_id: group_id.to_string(),
            props,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        self.meta
            .upsert_group_profile(&rec)
            .await
            .map_err(meta_err_to_ingest)?;
        Ok(())
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum AuthSource {
    Header,
    Cookie,
}

fn is_state_changing_method(method: &axum::http::Method) -> bool {
    matches!(
        *method,
        axum::http::Method::POST
            | axum::http::Method::PUT
            | axum::http::Method::PATCH
            | axum::http::Method::DELETE
    )
}

fn origin_matches_request(
    origin: &str,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    public_url: &str,
) -> bool {
    let origin = origin.trim().trim_end_matches('/');
    if origin.is_empty() || origin.eq_ignore_ascii_case("null") {
        return false;
    }
    let pub_trimmed = public_url.trim().trim_end_matches('/');
    if !pub_trimmed.is_empty() && origin.eq_ignore_ascii_case(pub_trimmed) {
        return true;
    }
    let scheme = if auth::is_https_request(headers, Some(uri)) {
        "https"
    } else {
        "http"
    };
    let host_candidates = [
        headers
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim),
        headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(str::trim),
    ];
    for host in host_candidates.into_iter().flatten() {
        if host.is_empty() {
            continue;
        }
        if origin.eq_ignore_ascii_case(&format!("{scheme}://{host}")) {
            return true;
        }
        if let Some(stripped) = host.strip_suffix(if scheme == "https" { ":443" } else { ":80" }) {
            if origin.eq_ignore_ascii_case(&format!("{scheme}://{stripped}")) {
                return true;
            }
        }
    }
    false
}

fn verify_cookie_csrf(
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    public_url: &str,
) -> std::result::Result<(), CtrlError> {
    if let Some(sfs) = headers
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if !sfs.eq_ignore_ascii_case("same-origin") {
            return Err(CtrlError::forbidden(
                "cross-site cookie-authenticated mutation rejected",
            ));
        }
        if let Some(origin) = headers
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if !origin_matches_request(origin, headers, uri, public_url) {
                return Err(CtrlError::forbidden(
                    "cross-site cookie-authenticated mutation rejected",
                ));
            }
        }
        return Ok(());
    }

    if let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if origin_matches_request(origin, headers, uri, public_url) {
            return Ok(());
        }
        return Err(CtrlError::forbidden(
            "cross-site cookie-authenticated mutation rejected",
        ));
    }

    Err(CtrlError::forbidden(
        "missing Sec-Fetch-Site or Origin header for cookie-authenticated mutation",
    ))
}

async fn require_admin_auth(
    State(ctrl): State<Arc<ControlPlaneService>>,
    mut req: Request,
    next: Next,
) -> std::result::Result<Response, CtrlError> {
    let (token, auth_source) = if let Some(tok) = extract_admin_bearer(req.headers()) {
        (tok, AuthSource::Header)
    } else if let Some(tok) = auth::extract_session_cookie(req.headers()) {
        (tok, AuthSource::Cookie)
    } else {
        return Err(CtrlError::unauthorized(
            "missing Authorization: Bearer <sk_...> header",
        ));
    };
    let session_user = if token.starts_with("ps_") {
        ctrl.meta
            .get_session_user(&sha256_hex(token), Utc::now().timestamp_millis())
            .await
            .map_err(CtrlError::meta)?
    } else {
        None
    };
    let now = Utc::now().timestamp_millis();
    let principal = match (auth_source, session_user) {
        (_, Some(su)) => {
            // Last-seen for the active-sessions list, written at most once a minute per session.
            if now - su.last_seen_ms > 60_000 {
                let _ = ctrl.meta.touch_session(&sha256_hex(token), now).await;
            }
            AuthPrincipal::User {
                user_id: su.user_id,
                org_id: su.org_id,
                role: su.role,
            }
        }
        (AuthSource::Cookie, None) => {
            return Err(CtrlError::unauthorized("invalid or expired session cookie"));
        }
        (AuthSource::Header, None) => ctrl.verify_admin_token(token)?,
    };
    if auth_source == AuthSource::Cookie && is_state_changing_method(req.method()) {
        let public_url = ctrl.auth_cfg.read().public_url.clone();
        verify_cookie_csrf(req.headers(), req.uri(), &public_url)?;
    }
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default();
    let needed = accounts::required_role(req.method(), &path);
    if principal.role() < needed {
        return Err(CtrlError::forbidden(format!(
            "your role can't do this; it needs {} or above",
            needed.as_str()
        )));
    }
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

/// Builds the public SDK Flag Bundle router (`GET /v1/flags/:project_api_key`), safe for permissive CORS.
pub fn build_public_flag_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .route("/v1/flags/:project_api_key", get(handle_edge_flag_bundle))
        .with_state(ctrl)
}

/// Builds the authenticated Control Plane + Query + Replay + MCP router.
pub fn build_admin_control_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .route("/v1/status", get(handle_status))
        .route("/v1/admin/flush", post(handle_admin_flush))
        .route(
            "/v1/projects",
            get(handle_list_projects).post(handle_create_project),
        )
        .route("/v1/projects/:id", patch(handle_patch_project))
        .route(
            "/v1/projects/:id/api-keys",
            get(handle_list_keys).post(handle_create_key),
        )
        .route(
            "/v1/projects/:id/api-keys/:key_id",
            delete(handle_revoke_key),
        )
        // Unified & per-project Query endpoints
        .route("/v1/query", post(handle_unified_query))
        .route(
            "/v1/projects/:id/query/insights",
            post(handle_query_insights),
        )
        .route("/v1/projects/:id/query/funnel", post(handle_query_funnel))
        .route(
            "/v1/projects/:id/query/funnel/dropped",
            post(handle_query_funnel_dropped),
        )
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
            "/v1/projects/:id/cohorts/from-funnel-dropped",
            post(handle_cohort_from_funnel_dropped),
        )
        .route(
            "/v1/projects/:id/cohorts/:cohort_id",
            put(handle_update_cohort).delete(handle_delete_cohort),
        )
        .route(
            "/v1/projects/:id/cohorts/:cohort_id/export",
            get(handle_export_cohort),
        )
        // User Profiles, DPDP Erasure, Batch Import/Export, Revenue & Alerts (`Gaps 1, 4, 5, 7, 14, 15, 16`)
        .route("/v1/projects/:id/users", get(handle_list_user_profiles))
        .route(
            "/v1/projects/:id/users/:distinct_id",
            get(handle_get_user_detail).delete(handle_delete_user_profile),
        )
        .route("/v1/projects/:id/deletions", get(handle_list_deletions))
        .route("/v1/projects/:id/import", post(handle_import_events))
        .route("/v1/projects/:id/export", get(handle_export_events))
        .route("/v1/projects/:id/revenue", get(handle_get_revenue_summary))
        .route(
            "/v1/projects/:id/alerts",
            get(handle_list_alerts).post(handle_create_alert),
        )
        .route(
            "/v1/projects/:id/alerts/:alert_id",
            axum::routing::delete(handle_delete_alert),
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
        .route(
            "/v1/projects/:id/annotations",
            get(handle_list_annotations).post(handle_create_annotation),
        )
        .route(
            "/v1/projects/:id/annotations/:ann_id",
            axum::routing::delete(handle_delete_annotation),
        )
        // Saved Reports & Dashboards
        .route(
            "/v1/projects/:id/saved-reports",
            get(handle_list_reports).post(handle_save_report),
        )
        .route(
            "/v1/projects/:id/saved-reports/:rep_id",
            get(handle_get_report)
                .put(handle_update_report)
                .delete(handle_delete_report),
        )
        .route(
            "/v1/projects/:id/dashboards",
            get(handle_list_dashboards).post(handle_save_dashboard),
        )
        .route(
            "/v1/projects/:id/dashboards/:dash_id",
            get(handle_get_dashboard)
                .put(handle_replace_dashboard)
                .delete(handle_delete_dashboard),
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
            "/v1/projects/:id/experiments/:exp_id",
            patch(handle_patch_experiment),
        )
        .route(
            "/v1/projects/:id/experiments/:exp_id/results",
            get(handle_experiment_results_scoped),
        )
        // Model Context Protocol (MCP) JSON-RPC 2.0
        .route("/v1/mcp", post(handle_mcp))
        // Encryption at rest, audit log, provider credentials, warehouse export & copilot
        .route("/v1/system/encryption", get(handle_get_encryption))
        .route(
            "/v1/system/encryption/rotate",
            post(handle_rotate_encryption),
        )
        .route("/v1/audit-logs", get(handle_list_audit_logs))
        .route(
            "/v1/projects/:id/byok",
            get(handle_list_byok).put(handle_upsert_byok),
        )
        .route(
            "/v1/projects/:id/byok/:category",
            delete(handle_delete_byok),
        )
        .route("/v1/projects/:id/byok-verify", post(handle_verify_byok))
        .route(
            "/v1/projects/:id/export/warehouse",
            post(handle_warehouse_export),
        )
        .route("/v1/projects/:id/ai/copilot", post(handle_byok_copilot))
        .route(
            "/v1/auth/me",
            get(auth::handle_me).patch(accounts::handle_update_me),
        )
        .route("/v1/auth/logout", post(auth::handle_logout))
        .merge(crate::api_agent::agent_admin_routes())
        .merge(accounts::account_router())
        .route_layer(middleware::from_fn_with_state(
            ctrl.clone(),
            require_admin_auth,
        ))
        // Sign-in routes are merged after `route_layer`, so they stay outside the bearer middleware
        // but share the admin CORS policy.
        .merge(auth::public_auth_router())
        .merge(accounts::public_router())
        .with_state(ctrl)
}

/// Combined Control Plane router (public edge flag bundle + public agent ingest + bearer-protected admin/query routes).
pub fn build_control_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .merge(build_public_flag_router(ctrl.clone()))
        .merge(crate::api_agent::build_agent_ingest_router(ctrl.clone()))
        .merge(build_admin_control_router(ctrl))
}

pub(crate) type ApiRes<T> = std::result::Result<Json<T>, CtrlError>;

#[derive(Debug)]
pub(crate) struct CtrlError(pub(crate) StatusCode, pub(crate) String);

impl CtrlError {
    pub(crate) fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self(status, msg.into())
    }
    pub(crate) fn bad(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    pub(crate) fn unauthorized(msg: impl Into<String>) -> Self {
        Self(StatusCode::UNAUTHORIZED, msg.into())
    }
    pub(crate) fn forbidden(msg: impl Into<String>) -> Self {
        Self(StatusCode::FORBIDDEN, msg.into())
    }
    pub(crate) fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
    }
    pub(crate) fn conflict(msg: impl Into<String>) -> Self {
        Self(StatusCode::CONFLICT, msg.into())
    }
    pub(crate) fn bad_gateway(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_GATEWAY, msg.into())
    }
    pub(crate) fn meta(err: MetaError) -> Self {
        match err {
            MetaError::Conflict(msg) => Self::conflict(msg),
            MetaError::Sqlx(ref e) if is_sqlx_unavailable(e) => {
                Self(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
            }
            other => Self::internal(other),
        }
    }
    pub(crate) fn storage(err: pulse_storage::StorageError) -> Self {
        if err.is_unavailable() {
            Self(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
        } else if matches!(
            err,
            pulse_storage::StorageError::Identity(
                pulse_identity::IdentityError::IdentifiedMergeConflict
            )
        ) {
            Self::conflict(err.to_string())
        } else if matches!(err, pulse_storage::StorageError::InvalidIdentifier(_)) {
            Self::bad(err.to_string())
        } else if matches!(err, pulse_storage::StorageError::DictionaryFull { .. }) {
            Self(StatusCode::UNPROCESSABLE_ENTITY, err.to_string())
        } else {
            Self::internal(err)
        }
    }
    pub(crate) fn ingest(err: IngestError) -> Self {
        match err {
            IngestError::Unauthorized => Self::unauthorized(err.to_string()),
            IngestError::Wire(_) | IngestError::Json(_) => Self::bad(err.to_string()),
            IngestError::SchemaGovernanceLimit(msg) => Self(StatusCode::UNPROCESSABLE_ENTITY, msg),
            IngestError::RateLimited { .. } => Self(StatusCode::TOO_MANY_REQUESTS, err.to_string()),
            IngestError::Conflict(msg) => Self::conflict(msg),
            IngestError::Identity(pulse_identity::IdentityError::IdentifiedMergeConflict) => {
                Self::conflict(err.to_string())
            }
            IngestError::Unavailable(msg) => Self(StatusCode::SERVICE_UNAVAILABLE, msg),
            IngestError::Identity(ref e) if e.is_store_unavailable() => {
                Self(StatusCode::SERVICE_UNAVAILABLE, err.to_string())
            }
            IngestError::Storage(se) => Self::storage(se),
            other => Self::internal(other),
        }
    }
    #[allow(dead_code)]
    pub(crate) fn exec(err: pulse_exec::ExecError) -> Self {
        match err {
            pulse_exec::ExecError::QueryTimeout(ms) => Self(
                StatusCode::GATEWAY_TIMEOUT,
                format!("query exceeded deadline ({ms} ms)"),
            ),
            e @ pulse_exec::ExecError::QueryTooLarge { .. }
            | e @ pulse_exec::ExecError::QueryMemoryExceeded { .. } => {
                Self(StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
            }
            e @ pulse_exec::ExecError::Overloaded(_) => {
                Self(StatusCode::TOO_MANY_REQUESTS, e.to_string())
            }
            pulse_exec::ExecError::InvalidQuery(msg) | pulse_exec::ExecError::Expression(msg) => {
                Self::bad(msg)
            }
            other => Self::internal(other),
        }
    }
    pub(crate) fn internal(msg: impl std::fmt::Display) -> Self {
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
    let pid = ctrl
        .resolve_and_check(&principal, q.get("project_id").map(|s| s.as_str()))
        .await?;

    let (snap, l0_rows, interned_users) = {
        let refresh_cat = ctrl
            .qengine
            .storage()
            .is_published_reader()
            .then(|| ctrl.qengine.catalogs().for_project(&pid));
        let _refresh_guard = match refresh_cat.as_ref() {
            Some(cat) => Some(cat.acquire_query_read().await),
            None => None,
        };
        let ps = ctrl
            .qengine
            .storage()
            .open_for_read(&pid)
            .await
            .map_err(CtrlError::internal)?;
        let snap = ps.snapshot();
        let l0_rows = ps.total_l0_rows();
        let id_graph = ctrl
            .qengine
            .storage()
            .identity_graph(&pid, false)
            .await
            .map_err(CtrlError::internal)?;
        (snap, l0_rows, id_graph.user_count())
    };

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

    let projects = ctrl.visible_projects(&principal).await?;

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
        "l0_rows": l0_rows,
        "l1_rows": snap.manifest.total_l1_rows(),
        "segment_count": snap.manifest.total_segments(),
        "interned_users": interned_users,
        "projects": projects,
    })))
}

async fn handle_admin_flush(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<Value> {
    if !matches!(
        principal,
        AuthPrincipal::RootAdmin | AuthPrincipal::User { .. }
    ) {
        return Err(CtrlError::forbidden(
            "cluster flush requires root admin token",
        ));
    }
    if let Some(ing) = ctrl.ingest.read().await.clone() {
        ing.flush_all().await.map_err(CtrlError::ingest)?;
    }
    let flushed = ctrl
        .qengine
        .storage()
        .flush_all_projects()
        .await
        .map_err(CtrlError::storage)?;
    let compacted = ctrl
        .qengine
        .storage()
        .compact_all_projects()
        .await
        .map_err(CtrlError::storage)?;
    ctrl.agent_store()
        .flush_all_l0()
        .await
        .map_err(CtrlError::internal)?;
    let agent_retention = ctrl.prune_expired_agent_data().await;
    Ok(Json(json!({
        "status": "ok",
        "segments_flushed": flushed,
        "partitions_compacted": compacted,
        "agent_segments_pruned": agent_retention.segments_pruned,
        "agent_evaluations_pruned": agent_retention.evaluations_pruned,
        "agent_tombstones_pruned": agent_retention.tombstones_pruned,
    })))
}

async fn handle_list_projects(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<Vec<Project>> {
    Ok(Json(ctrl.visible_projects(&principal).await?))
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
    let org_id = match &principal {
        AuthPrincipal::RootAdmin => "org_default".to_string(),
        AuthPrincipal::User { org_id, .. } => org_id.clone(),
        AuthPrincipal::ProjectAdmin { .. } => {
            return Err(CtrlError::forbidden(
                "creating projects requires root admin token",
            ))
        }
    };
    if req.id.is_some() {
        return Err(CtrlError::bad(
            "project id is generated by the server and must not be client-supplied",
        ));
    }
    let (proj, api_key) = ctrl
        .create_project_in_org(&org_id, &req.name, req.timezone)
        .await?;
    Ok(Json(json!({ "project": proj, "api_key": api_key })))
}

/// Trims `tz`; `Ok(None)` when blank, an error when it isn't an IANA zone.
fn valid_timezone(tz: &str) -> Result<Option<String>, CtrlError> {
    let tz = tz.trim();
    if tz.is_empty() {
        return Ok(None);
    }
    if tz.parse::<chrono_tz::Tz>().is_err() {
        return Err(CtrlError::bad(format!(
            "unknown time zone '{tz}': use an IANA name such as 'UTC', 'America/New_York' or 'Asia/Kolkata'"
        )));
    }
    Ok(Some(tz.to_string()))
}

#[derive(Deserialize)]
struct PatchProjectReq {
    name: Option<String>,
    timezone: Option<String>,
}

/// Renames a project and/or changes the time zone its reports bucket days in.
async fn handle_patch_project(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<PatchProjectReq>,
) -> ApiRes<Project> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if let Some(name) = req.name.as_deref().map(str::trim) {
        if name.is_empty() {
            return Err(CtrlError::bad("project name must not be empty"));
        }
        let name: String = name.chars().take(80).collect();
        ctrl.meta
            .rename_project(&id, &name)
            .await
            .map_err(CtrlError::meta)?;
    }
    if let Some(tz) = req
        .timezone
        .as_deref()
        .map(valid_timezone)
        .transpose()?
        .flatten()
    {
        ctrl.meta
            .set_project_timezone(&id, &tz)
            .await
            .map_err(CtrlError::meta)?;
        ctrl.refresh_project_virtual_catalog(&id)
            .await
            .map_err(CtrlError::internal)?;
    }
    let proj = ctrl
        .meta
        .get_project(&id)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| CtrlError::not_found("project not found"))?;
    Ok(Json(proj))
}

impl ControlPlaneService {
    /// Creates a project in `org_id` with its first ingest key (plaintext returned once).
    async fn create_project_in_org(
        &self,
        org_id: &str,
        name: &str,
        timezone: Option<String>,
    ) -> Result<(Project, CreatedApiKeyResponse), CtrlError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(CtrlError::bad("project name must not be empty"));
        }
        let timezone = timezone
            .as_deref()
            .map(valid_timezone)
            .transpose()?
            .flatten();
        let now = Utc::now().timestamp_millis();
        let id = format!("proj_{}", &Uuid::new_v4().simple().to_string()[..12]);
        let proj = Project {
            id: id.clone(),
            org_id: org_id.to_string(),
            name: name.to_string(),
            timezone: timezone.unwrap_or_else(|| "UTC".to_string()),
            created_at_ms: now,
        };
        self.meta
            .insert_project(&proj)
            .await
            .map_err(CtrlError::meta)?;

        let (plaintext_key, key_hash, key_prefix) =
            generate_random_api_key(KEY_ROLE_INGEST_AND_FLAGS);
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
        self.meta
            .insert_api_key(&key)
            .await
            .map_err(CtrlError::meta)?;

        self.known_projects.insert(id.clone(), proj.org_id.clone());
        self.api_key_cache.insert(
            key_hash,
            CachedApiKey {
                project_id: id.clone(),
                role: key.role.clone(),
            },
        );
        self.refresh_project_lexicon_cache(&id)
            .await
            .map_err(CtrlError::internal)?;
        self.refresh_project_virtual_catalog(&id)
            .await
            .map_err(CtrlError::internal)?;
        Ok((
            proj,
            CreatedApiKeyResponse {
                record: key,
                plaintext_key,
            },
        ))
    }
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

async fn handle_revoke_key(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, key_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let key = ctrl
        .meta
        .list_api_keys(Some(&id))
        .await
        .map_err(CtrlError::meta)?
        .into_iter()
        .find(|k| k.id == key_id)
        .ok_or_else(|| CtrlError::not_found("api key not found"))?;
    ctrl.meta
        .revoke_api_key(&id, &key_id)
        .await
        .map_err(CtrlError::meta)?;
    // Other nodes drop it on their next cache reload.
    ctrl.api_key_cache.remove(&key.key_hash);
    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "api_key.revoked",
        &format!("project:{id}"),
        json!({ "key_id": key_id, "label": key.label }),
    )
    .await;
    Ok(Json(json!({"id": key_id, "revoked": true})))
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
            project_id: id.clone(),
            role: role.clone(),
        },
    );
    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "api_key.created",
        &format!("project:{id}"),
        json!({ "key_id": record.id, "label": record.label, "role": role }),
    )
    .await;
    Ok(Json(CreatedApiKeyResponse {
        record,
        plaintext_key,
    }))
}

// ------------------- Queries -------------------

fn parse_json_body<T: serde::de::DeserializeOwned>(
    label: &str,
    bytes: &[u8],
) -> Result<T, CtrlError> {
    serde_json::from_slice(bytes).map_err(|e| {
        CtrlError::bad(format!(
            "invalid {label} request: {}",
            pulse_wire::sanitize_json_error_message(&e.to_string())
        ))
    })
}

fn parse_query_value<T: serde::de::DeserializeOwned>(
    label: &str,
    value: Value,
) -> Result<T, CtrlError> {
    serde_json::from_value(value).map_err(|e| {
        CtrlError::bad(format!(
            "invalid {label} query: {}",
            pulse_wire::sanitize_json_error_message(&e.to_string())
        ))
    })
}

async fn handle_unified_query(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    body: Bytes,
) -> ApiRes<Value> {
    let payload: Value = parse_json_body("query", &body)?;
    let req_pid = payload.get("project_id").and_then(|v| v.as_str());
    let pid = ctrl.resolve_and_check(&principal, req_pid).await?;

    let kind = payload
        .get("kind")
        .or_else(|| payload.get("report_type"))
        .and_then(|v| v.as_str())
        .unwrap_or("insights");

    let inner = payload.get("query").cloned().unwrap_or(payload.clone());
    ctrl.load_erased_for_project(&pid)
        .await
        .map_err(CtrlError::internal)?;
    match kind {
        "insights" => {
            let q: InsightsQuery = parse_query_value("insights", inner)?;
            let res = ctrl
                .qengine
                .query_insights(&pid, &q)
                .await
                .map_err(CtrlError::exec)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "funnel" | "funnels" => {
            let q: FunnelQuery = parse_query_value("funnel", inner)?;
            let res = ctrl
                .qengine
                .query_funnel(&pid, &q)
                .await
                .map_err(CtrlError::exec)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "retention" => {
            let q: RetentionQuery = parse_query_value("retention", inner)?;
            let res = ctrl
                .qengine
                .query_retention(&pid, &q)
                .await
                .map_err(CtrlError::exec)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "flows" | "flow" => {
            let q: FlowsQuery = parse_query_value("flows", inner)?;
            let res = ctrl
                .qengine
                .query_flows(&pid, &q)
                .await
                .map_err(CtrlError::exec)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        other => Err(CtrlError::bad(format!("unknown query kind: {other}"))),
    }
}

async fn handle_query_insights(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> ApiRes<pulse_exec::InsightsResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let q: InsightsQuery = parse_json_body("insights query", &body)?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .query_insights(&id, &q)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

async fn handle_query_funnel(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> ApiRes<pulse_exec::FunnelResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let q: FunnelQuery = parse_json_body("funnel query", &body)?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .query_funnel(&id, &q)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

/// Every user who dropped off before `?step=N` of the funnel in the body, as CSV
/// (`distinct_id` per line) or JSON with `?format=json`.
async fn handle_query_funnel_dropped(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response, CtrlError> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let q: FunnelQuery = parse_json_body("funnel query", &body)?;
    let step = params
        .get("step")
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| CtrlError::bad("?step=N is required: the step users didn't reach"))?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let users = ctrl
        .qengine
        .query_funnel_dropped_users(&id, &q, step)
        .await
        .map_err(CtrlError::exec)?;
    if params.get("format").map(String::as_str) == Some("json") {
        return Ok(Json(json!({ "step": step, "users": users })).into_response());
    }
    let mut csv = String::from("distinct_id\n");
    for u in &users {
        csv.push_str(&csv_escape(u));
        csv.push('\n');
    }
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"dropped_step_{step}.csv\""),
            ),
        ],
        csv,
    )
        .into_response())
}

async fn handle_query_retention(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> ApiRes<pulse_exec::RetentionResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let q: RetentionQuery = parse_json_body("retention query", &body)?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .query_retention(&id, &q)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

async fn handle_query_flows(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> ApiRes<pulse_exec::FlowsResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let q: FlowsQuery = parse_json_body("flows query", &body)?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .query_flows(&id, &q)
        .await
        .map_err(CtrlError::exec)?;
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
        only_with_replay: params
            .get("only_with_replay")
            .is_some_and(|v| v == "true" || v == "1"),
        limit: params.get("limit").and_then(|s| s.parse().ok()),
    }
}

async fn handle_get_sessions_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let pid = ctrl
        .resolve_and_check(&principal, params.get("project_id").map(|s| s.as_str()))
        .await?;
    let sq = session_query_from_params(&params);
    ctrl.load_erased_for_project(&pid)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .search_sessions(&pid, &sq)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

async fn handle_post_sessions_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Json(sq): Json<SessionSearchQuery>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let pid = ctrl.resolve_and_check(&principal, None).await?;
    ctrl.load_erased_for_project(&pid)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .search_sessions(&pid, &sq)
        .await
        .map_err(CtrlError::exec)?;
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
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .search_sessions(&id, &sq)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

async fn handle_post_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(sq): Json<SessionSearchQuery>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let res = ctrl
        .qengine
        .search_sessions(&id, &sq)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

async fn handle_get_replay_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> std::result::Result<Response, CtrlError> {
    let pid = ctrl
        .resolve_and_check(&principal, params.get("project_id").map(|s| s.as_str()))
        .await?;
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

/// Validates RFC 9110 single byte-range header (`bytes=start-end`, `bytes=start-`, `bytes=-suffix`).
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
    let map_replay_err = CtrlError::storage;

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
            .map_err(CtrlError::storage)?
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
        .map_err(CtrlError::storage)?;

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

// ------------------- Cohorts & Virtual Schema Extensions -------------------

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
struct UpdateCohortReq {
    name: String,
    #[serde(default)]
    description: String,
    definition: BehavioralCohortDef,
}

async fn find_cohort(
    ctrl: &ControlPlaneService,
    project_id: &str,
    cohort_id: &str,
) -> Result<CohortRecord, CtrlError> {
    ctrl.meta
        .list_cohorts(project_id)
        .await
        .map_err(CtrlError::meta)?
        .into_iter()
        .find(|c| c.id == cohort_id)
        .ok_or_else(|| CtrlError::not_found(format!("cohort '{cohort_id}' not found")))
}

async fn handle_update_cohort(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, cohort_id)): AxumPath<(String, String)>,
    Json(req): Json<UpdateCohortReq>,
) -> ApiRes<CohortRecord> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let mut rec = find_cohort(&ctrl, &id, &cohort_id).await?;
    let cat = ctrl.qengine.catalogs().for_project(&id);
    validate_cohort_acyclic(Some(&cohort_id), &req.definition, &cat)
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    rec.name = validate_name(&req.name)?;
    rec.description = req.description;
    rec.definition = req.definition;
    ctrl.meta
        .upsert_cohort(&rec)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(rec))
}

fn cohort_def_refs(def: &BehavioralCohortDef, cohort_id: &str) -> bool {
    match def {
        BehavioralCohortDef::CohortRef { cohort_id: r } => r == cohort_id,
        BehavioralCohortDef::And { clauses } | BehavioralCohortDef::Or { clauses } => {
            clauses.iter().any(|c| cohort_def_refs(c, cohort_id))
        }
        _ => false,
    }
}

/// Refuses (409) while another cohort, a flag, a saved report or a dashboard still uses it.
async fn handle_delete_cohort(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, cohort_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let cohorts = ctrl.meta.list_cohorts(&id).await.map_err(CtrlError::meta)?;
    if !cohorts.iter().any(|c| c.id == cohort_id) {
        return Err(CtrlError::not_found(format!(
            "cohort '{cohort_id}' not found"
        )));
    }
    let mut users: Vec<String> = cohorts
        .iter()
        .filter(|c| c.id != cohort_id && cohort_def_refs(&c.definition, &cohort_id))
        .map(|c| format!("cohort '{}'", c.name))
        .collect();
    for f in ctrl
        .meta
        .list_feature_flags(&id)
        .await
        .map_err(CtrlError::meta)?
    {
        if f.targeting_cohort_id.as_deref() == Some(cohort_id.as_str()) {
            users.push(format!("flag '{}'", f.key));
        }
    }
    // ponytail: reports/dashboards hold free-form JSON, so match the quoted id anywhere in it.
    let quoted = format!("\"{cohort_id}\"");
    for r in ctrl
        .meta
        .list_saved_reports(&id)
        .await
        .map_err(CtrlError::meta)?
    {
        if r.query_json.to_string().contains(&quoted) {
            users.push(format!("report '{}'", r.name));
        }
    }
    for d in ctrl
        .meta
        .list_dashboards(&id)
        .await
        .map_err(CtrlError::meta)?
    {
        if serde_json::to_string(&d.cards)
            .unwrap_or_default()
            .contains(&quoted)
        {
            users.push(format!("dashboard '{}'", d.name));
        }
    }
    if !users.is_empty() {
        return Err(CtrlError::conflict(format!(
            "cohort is still used by {}; remove it there first",
            users.join(", ")
        )));
    }
    ctrl.meta
        .delete_cohort(&id, &cohort_id)
        .await
        .map_err(CtrlError::meta)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(json!({ "deleted": cohort_id })))
}

#[derive(Deserialize)]
struct CohortFromDroppedReq {
    name: String,
    #[serde(default)]
    description: String,
    query: FunnelQuery,
}

/// Saves exactly the users the drop-off list shows (`?step=N`), with every step
/// filter, window and order applied, as a fixed cohort.
async fn handle_cohort_from_funnel_dropped(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(req): Json<CohortFromDroppedReq>,
) -> ApiRes<CohortRecord> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let step = params
        .get("step")
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| CtrlError::bad("?step=N is required: the step users didn't reach"))?;
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let uids = ctrl
        .qengine
        .query_funnel_dropped_uids(&id, &req.query, step)
        .await
        .map_err(CtrlError::exec)?;
    let rec = CohortRecord {
        id: format!("coh_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id.clone(),
        name: req.name,
        description: req.description,
        definition: BehavioralCohortDef::Users {
            canonical_uids: uids.iter().collect(),
        },
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
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let (user_count, exec_time_us) = ctrl
        .qengine
        .preview_cohort_count(&id, &req.definition, req.time_range)
        .await
        .map_err(CtrlError::exec)?;
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
    let user_properties = ctrl.qengine.catalogs().for_project(&id).user_prop_keys();
    Ok(Json(CustomSchemaBundle {
        custom_events,
        custom_properties,
        lookup_tables,
        user_properties,
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
        LookupTableDef::new(req.name.clone(), req.join_prop.clone(), entries)
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

const MAX_DASHBOARD_CARDS: usize = 60;
const MAX_TEXT_CARD_CHARS: usize = 5000;
const MAX_DESCRIPTION_CHARS: usize = 1000;

fn validate_name(raw: &str) -> Result<String, CtrlError> {
    let name = raw.trim();
    if !(1..=120).contains(&name.chars().count()) {
        return Err(CtrlError::bad("name must be between 1 and 120 characters"));
    }
    Ok(name.to_string())
}

fn new_card_id() -> String {
    format!("card_{}", &Uuid::new_v4().simple().to_string()[..12])
}

impl ControlPlaneService {
    /// Who to record as `created_by`: the user's email for sessions, otherwise `"admin token"`.
    async fn principal_label(&self, principal: &AuthPrincipal) -> Result<String, CtrlError> {
        if let AuthPrincipal::User { user_id, .. } = principal {
            if let Some(u) = self
                .meta
                .get_user_by_id(user_id)
                .await
                .map_err(CtrlError::meta)?
            {
                return Ok(u.email);
            }
        }
        Ok("admin token".to_string())
    }

    async fn validate_cards(
        &self,
        project_id: &str,
        cards: Vec<DashboardCard>,
    ) -> Result<Vec<DashboardCard>, CtrlError> {
        if cards.len() > MAX_DASHBOARD_CARDS {
            return Err(CtrlError::bad(format!(
                "a dashboard can hold at most {MAX_DASHBOARD_CARDS} cards"
            )));
        }
        let report_ids: std::collections::HashSet<String> = self
            .meta
            .list_saved_reports(project_id)
            .await
            .map_err(CtrlError::meta)?
            .into_iter()
            .map(|r| r.id)
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::with_capacity(cards.len());
        for mut card in cards {
            if card.id.trim().is_empty() {
                card.id = new_card_id();
            }
            if !seen.insert(card.id.clone()) {
                return Err(CtrlError::bad(format!("duplicate card id '{}'", card.id)));
            }
            if card.width != 1 && card.width != 2 {
                return Err(CtrlError::bad(
                    "card width must be 1 (half row) or 2 (full row)",
                ));
            }
            match card.kind.as_str() {
                "report" => {
                    let rid = card.report_id.as_deref().unwrap_or_default();
                    if !report_ids.contains(rid) {
                        return Err(CtrlError::bad(format!(
                            "report card '{}' references unknown report '{rid}'",
                            card.id
                        )));
                    }
                    card.text = None;
                }
                "text" => {
                    let text = card.text.as_deref().unwrap_or_default();
                    if text.chars().count() > MAX_TEXT_CARD_CHARS {
                        return Err(CtrlError::bad(format!(
                            "text cards are limited to {MAX_TEXT_CARD_CHARS} characters"
                        )));
                    }
                    card.text = Some(text.to_string());
                    card.report_id = None;
                }
                _ => return Err(CtrlError::bad("card kind must be 'report' or 'text'")),
            }
            out.push(card);
        }
        Ok(out)
    }

    async fn dashboard_or_404(
        &self,
        project_id: &str,
        dash_id: &str,
    ) -> Result<Dashboard, CtrlError> {
        self.meta
            .get_dashboard(project_id, dash_id)
            .await
            .map_err(CtrlError::meta)?
            .ok_or_else(|| CtrlError::not_found(format!("dashboard '{dash_id}' not found")))
    }

    async fn report_or_404(
        &self,
        project_id: &str,
        rep_id: &str,
    ) -> Result<SavedReport, CtrlError> {
        self.meta
            .get_saved_report(project_id, rep_id)
            .await
            .map_err(CtrlError::meta)?
            .ok_or_else(|| CtrlError::not_found(format!("report '{rep_id}' not found")))
    }
}

fn validate_description(raw: Option<String>) -> Result<Option<String>, CtrlError> {
    let desc = raw.map(|d| d.trim().to_string()).filter(|d| !d.is_empty());
    if desc
        .as_ref()
        .is_some_and(|d| d.chars().count() > MAX_DESCRIPTION_CHARS)
    {
        return Err(CtrlError::bad(format!(
            "description must be at most {MAX_DESCRIPTION_CHARS} characters"
        )));
    }
    Ok(desc)
}

async fn handle_list_annotations(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiRes<Vec<Annotation>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let num = |k: &str| q.get(k).and_then(|s| s.parse::<i64>().ok());
    let list = ctrl
        .meta
        .list_annotations(
            &id,
            num("start_ts_ms").unwrap_or(i64::MIN),
            num("end_ts_ms").unwrap_or(i64::MAX),
        )
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct CreateAnnotationReq {
    ts_ms: i64,
    text: String,
}

async fn handle_create_annotation(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateAnnotationReq>,
) -> ApiRes<Annotation> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let text = req.text.trim();
    if text.is_empty() || text.chars().count() > 280 {
        return Err(CtrlError::bad(
            "annotation text must be 1 to 280 characters",
        ));
    }
    if req.ts_ms <= 0 {
        return Err(CtrlError::bad("ts_ms must be a Unix time in milliseconds"));
    }
    let a = Annotation {
        id: format!("ann_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id,
        ts_ms: req.ts_ms,
        text: text.to_string(),
        created_by: Some(ctrl.principal_label(&principal).await?),
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .insert_annotation(&a)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(a))
}

async fn handle_delete_annotation(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, ann_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if !ctrl
        .meta
        .delete_annotation(&id, &ann_id)
        .await
        .map_err(CtrlError::meta)?
    {
        return Err(CtrlError::not_found(format!(
            "annotation '{ann_id}' not found"
        )));
    }
    Ok(Json(json!({ "deleted": ann_id })))
}

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
    if !REPORT_KINDS.contains(&req.report_kind.as_str()) {
        return Err(CtrlError::bad(
            "report_kind must be one of funnel, insights, retention, flows",
        ));
    }
    let now = Utc::now().timestamp_millis();
    let rep = SavedReport {
        id: format!("rep_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id,
        name: validate_name(&req.name)?,
        report_kind: req.report_kind,
        query_json: req.query_json,
        created_at_ms: now,
        updated_at_ms: now,
    };
    ctrl.meta
        .upsert_saved_report(&rep)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(rep))
}

async fn handle_get_report(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, rep_id)): AxumPath<(String, String)>,
) -> ApiRes<SavedReport> {
    ctrl.authorize_and_check(&principal, &id).await?;
    Ok(Json(ctrl.report_or_404(&id, &rep_id).await?))
}

#[derive(Deserialize)]
struct UpdateReportReq {
    name: String,
    query_json: Value,
}

async fn handle_update_report(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, rep_id)): AxumPath<(String, String)>,
    Json(req): Json<UpdateReportReq>,
) -> ApiRes<SavedReport> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let mut rep = ctrl.report_or_404(&id, &rep_id).await?;
    rep.name = validate_name(&req.name)?;
    rep.query_json = req.query_json;
    rep.updated_at_ms = Utc::now().timestamp_millis();
    ctrl.meta
        .upsert_saved_report(&rep)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(rep))
}

async fn handle_delete_report(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, rep_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if !ctrl
        .meta
        .delete_saved_report(&id, &rep_id)
        .await
        .map_err(CtrlError::meta)?
    {
        return Err(CtrlError::not_found(format!("report '{rep_id}' not found")));
    }
    // ponytail: not transactional with the delete; a crash in between leaves dangling report
    // cards, which the UI must already tolerate. Wrap in a transaction if that ever matters.
    let now = Utc::now().timestamp_millis();
    let mut dashboards_updated = 0;
    for mut dash in ctrl
        .meta
        .list_dashboards(&id)
        .await
        .map_err(CtrlError::meta)?
    {
        let before = dash.cards.len();
        dash.cards
            .retain(|c| c.report_id.as_deref() != Some(rep_id.as_str()));
        if dash.cards.len() != before {
            dash.updated_at_ms = now;
            ctrl.meta
                .upsert_dashboard(&dash)
                .await
                .map_err(CtrlError::meta)?;
            dashboards_updated += 1;
        }
    }
    Ok(Json(
        json!({ "status": "ok", "dashboards_updated": dashboards_updated }),
    ))
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
    description: Option<String>,
    cards: Option<Vec<DashboardCard>>,
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
    let name = validate_name(&req.name)?;
    let description = validate_description(req.description)?;
    let cards = ctrl
        .validate_cards(&id, req.cards.unwrap_or_default())
        .await?;
    let now = Utc::now().timestamp_millis();
    let dash = Dashboard {
        id: format!("dash_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id,
        name,
        description,
        cards,
        created_by: Some(ctrl.principal_label(&principal).await?),
        created_at_ms: now,
        updated_at_ms: now,
    };
    ctrl.meta
        .upsert_dashboard(&dash)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(dash))
}

async fn handle_get_dashboard(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dash_id)): AxumPath<(String, String)>,
) -> ApiRes<Dashboard> {
    ctrl.authorize_and_check(&principal, &id).await?;
    Ok(Json(ctrl.dashboard_or_404(&id, &dash_id).await?))
}

#[derive(Deserialize)]
struct ReplaceDashboardReq {
    name: String,
    description: Option<String>,
    cards: Vec<DashboardCard>,
}

async fn handle_replace_dashboard(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dash_id)): AxumPath<(String, String)>,
    Json(req): Json<ReplaceDashboardReq>,
) -> ApiRes<Dashboard> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let mut dash = ctrl.dashboard_or_404(&id, &dash_id).await?;
    dash.name = validate_name(&req.name)?;
    dash.description = validate_description(req.description)?;
    dash.cards = ctrl.validate_cards(&id, req.cards).await?;
    dash.updated_at_ms = Utc::now().timestamp_millis();
    ctrl.meta
        .upsert_dashboard(&dash)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(dash))
}

async fn handle_delete_dashboard(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, dash_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if !ctrl
        .meta
        .delete_dashboard(&id, &dash_id)
        .await
        .map_err(CtrlError::meta)?
    {
        return Err(CtrlError::not_found(format!(
            "dashboard '{dash_id}' not found"
        )));
    }
    Ok(Json(json!({ "status": "ok" })))
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
    /// A cohort id to target, or `""` to target everyone again.
    targeting_cohort_id: Option<String>,
    /// Replaces the variant split (e.g. shipping the winner at 10000 bps).
    variants: Option<Vec<VariantAllocation>>,
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
    if let Some(coh_id) = req.targeting_cohort_id {
        if coh_id.is_empty() {
            found.targeting_cohort_id = None;
        } else {
            let cohorts = ctrl.meta.list_cohorts(&id).await.map_err(CtrlError::meta)?;
            if !cohorts.iter().any(|c| c.id == coh_id) {
                return Err(CtrlError::bad(format!(
                    "targeting_cohort_id '{coh_id}' does not exist in project '{id}'"
                )));
            }
            found.targeting_cohort_id = Some(coh_id);
        }
    }
    if let Some(variants) = req.variants {
        let weight_sum: u64 = variants.iter().map(|v| v.weight_bps as u64).sum();
        if variants.is_empty() || weight_sum != 10_000 {
            return Err(CtrlError::bad(format!(
                "variant weight_bps must sum to 10000, got {weight_sum}"
            )));
        }
        found.variants = variants;
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
    ctrl.load_erased_for_project(&project_id)
        .await
        .map_err(CtrlError::internal)?;
    let bundle = build_edge_flag_bundle_with_cohorts(
        &project_id,
        &flags,
        &ctrl.qengine,
        Utc::now().timestamp_millis(),
    )
    .await
    .map_err(CtrlError::internal)?;

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

#[derive(Deserialize)]
struct PatchExpReq {
    status: Option<String>,
    end_ts_ms: Option<i64>,
}

/// Stop, pause or resume an experiment. Shipping a winner is a flag patch plus `status: "concluded"`.
async fn handle_patch_experiment(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, exp_id)): AxumPath<(String, String)>,
    Json(req): Json<PatchExpReq>,
) -> ApiRes<Experiment> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let Some(mut exp) = ctrl
        .meta
        .get_experiment(&id, &exp_id)
        .await
        .map_err(CtrlError::meta)?
    else {
        return Err(CtrlError::not_found("experiment not found"));
    };
    if let Some(status) = req.status {
        if !matches!(status.as_str(), "running" | "paused" | "concluded") {
            return Err(CtrlError::bad(
                "status must be one of running, paused, concluded",
            ));
        }
        exp.status = status;
    }
    if let Some(end) = req.end_ts_ms {
        exp.end_ts_ms = end;
    }
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
    ctrl.load_erased_for_project(&project_id)
        .await
        .map_err(CtrlError::internal)?;
    let results = evaluate_experiment_results(&exp, flag, &ctrl.qengine)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(results))
}

// ------------------- MCP JSON-RPC 2.0 -------------------

async fn handle_mcp(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> std::result::Result<Response, CtrlError> {
    let header_pid = headers.get("x-pulse-project").and_then(|v| v.to_str().ok());
    let bound_pid = ctrl.resolve_and_check(&principal, header_pid).await?;
    // JSON-RPC notifications (no `id`, e.g. `notifications/initialized`) get no response body;
    // MCP Streamable HTTP requires `202 Accepted`.
    if req.id.is_null() {
        return Ok(StatusCode::ACCEPTED.into_response());
    }
    ctrl.load_erased_for_project(&bound_pid)
        .await
        .map_err(CtrlError::internal)?;
    let resp = crate::mcp::handle_mcp_request_with_agent_store(
        req,
        &bound_pid,
        &ctrl.meta,
        &ctrl.qengine,
        Some(ctrl.agent_store().as_ref()),
    )
    .await;
    Ok(Json(resp).into_response())
}

// ------------------- User Profiles, DPDP Erasure, Import/Export, Revenue & Alerts -------------------

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cur.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' {
            in_quotes = true;
        } else if c == ',' {
            fields.push(cur.trim().to_string());
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    fields.push(cur.trim().to_string());
    fields
}

async fn handle_list_user_profiles(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiRes<Vec<Value>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let limit = q
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let offset = q
        .get("offset")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let (erased, id_graph) = ctrl.load_erased_for_project_ctrl(&id).await?;
    let pstore = ctrl
        .qengine
        .storage()
        .profile_store(&id, false)
        .await
        .map_err(CtrlError::storage)?;
    let profiles = pstore
        .list_paginated_filtered(
            q.get("search").map(String::as_str),
            limit,
            offset,
            &erased,
            id_graph.as_deref(),
        )
        .await
        .map_err(CtrlError::storage)?;

    let out: Vec<Value> = profiles
        .into_iter()
        .map(|p| {
            json!({
                "project_id": p.project_id,
                "distinct_id": p.distinct_id,
                "distinct_id_masked": mask_user_identifier(&p.distinct_id),
                "canonical_uid": p.canonical_uid,
                "props": p.props,
                "total_revenue": p.total_revenue,
                "updated_at_ms": p.updated_at_ms,
            })
        })
        .collect();
    Ok(Json(out))
}

#[derive(Deserialize)]
struct UserDetailQuery {
    /// Look the user up by canonical uid. Query results only carry masked ids
    /// (`+91 •••• 655`), which can't be resolved back to the user.
    uid: Option<u32>,
}

async fn handle_get_user_detail(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, distinct_id)): AxumPath<(String, String)>,
    Query(q): Query<UserDetailQuery>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let (erased, _) = ctrl.load_erased_for_project_ctrl(&id).await?;
    let id_graph = ctrl
        .qengine
        .storage()
        .identity_graph(&id, false)
        .await
        .map_err(CtrlError::storage)?;
    let expanded = pulse_storage::expand_erased_with_identity(&erased, Some(&id_graph));

    let pstore = ctrl
        .qengine
        .storage()
        .profile_store(&id, false)
        .await
        .map_err(CtrlError::storage)?;
    let profile = pstore
        .get_by_distinct_id(&distinct_id)
        .await
        .map_err(CtrlError::storage)?
        .filter(|p| !pulse_storage::is_profile_erased(p, &expanded, Some(&id_graph)))
        .map(ControlPlaneService::from_stored_profile);

    let resolved_canon = q
        .uid
        .or_else(|| id_graph.lookup_canonical(&distinct_id))
        .or_else(|| id_graph.lookup_raw(&distinct_id))
        .or_else(|| profile.as_ref().map(|p| p.canonical_uid));
    let has_user = resolved_canon.is_some();
    let canon_uid = resolved_canon.unwrap_or(0);

    let events = if has_user {
        ctrl.qengine
            .export_events(&id, None, Some(canon_uid), 200, true)
            .await
            .map_err(CtrlError::exec)?
    } else {
        Vec::new()
    };

    let rstore = ctrl.qengine.storage().replay_store();
    let mut replay_sessions: Vec<String> = Vec::new();
    for ev in &events {
        if !ev.session_id.is_empty()
            && !replay_sessions.contains(&ev.session_id)
            && rstore.has_session(&id, &ev.session_id).await
        {
            replay_sessions.push(ev.session_id.clone());
        }
    }

    // Show what every id merged into this user set, not just this id's row.
    let profile = if has_user {
        let rows: Vec<UserProfileRecord> = pstore
            .list_for_canonical(canon_uid)
            .await
            .map_err(CtrlError::storage)?
            .into_iter()
            .filter(|p| !pulse_storage::is_profile_erased(p, &expanded, Some(&id_graph)))
            .map(ControlPlaneService::from_stored_profile)
            .collect();
        merge_profile_rows(rows).remove(&canon_uid).or(profile)
    } else {
        profile
    };

    Ok(Json(json!({
        "distinct_id": distinct_id,
        "distinct_id_masked": mask_user_identifier(&distinct_id),
        "canonical_uid": canon_uid,
        "profile": profile,
        "events": events,
        "replay_sessions": replay_sessions,
    })))
}

async fn handle_delete_user_profile(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, distinct_id)): AxumPath<(String, String)>,
) -> ApiRes<DeletionAuditRecord> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let who = match &principal {
        AuthPrincipal::RootAdmin => "root_admin",
        AuthPrincipal::ProjectAdmin { .. } => "project_admin",
        AuthPrincipal::User { user_id, .. } => user_id.as_str(),
    };
    let rec = ctrl.erase_user_dpdp(&id, &distinct_id, who).await?;
    Ok(Json(rec))
}

async fn handle_list_deletions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<DeletionAuditRecord>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let list = ctrl
        .meta
        .list_deletion_audit(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(list))
}

/// How long `/import` remembers an event id. Re-importing the same file within this window adds
/// nothing. ponytail: one global window; make it per project if someone backfills older reruns.
const IMPORT_DEDUP_RETENTION_MS: i64 = 30 * 24 * 3600 * 1000;

async fn handle_import_events(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let ingest_guard = ctrl.ingest.read().await;
    let Some(ingest) = ingest_guard.as_ref() else {
        return Err(CtrlError::internal("ingest service unavailable"));
    };

    let ctype = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    let first_non_ws = body.iter().copied().find(|b| !b.is_ascii_whitespace());
    // NDJSON (the usual analytics export format) also starts with `{`, so a `{` body that
    // isn't one JSON value is read line by line.
    let line_based = ctype.contains("csv")
        || ctype.contains("ndjson")
        || ctype.contains("jsonl")
        || ctype.contains("ldjson")
        || ctype.contains("text/plain")
        || (first_non_ws != Some(b'{') && first_non_ws != Some(b'['))
        || (first_non_ws == Some(b'{')
            && serde_json::from_slice::<serde::de::IgnoredAny>(&body).is_err());
    let batch = if line_based {
        let text = std::str::from_utf8(&body)
            .map_err(|_| CtrlError::bad("CSV/NDJSON import must be valid UTF-8"))?;
        let trimmed = text.trim();
        let mut rejected = Vec::new();
        if trimmed.starts_with('{') {
            // NDJSON lines
            let mut events = Vec::new();
            for (idx, line) in trimmed.lines().enumerate() {
                let l = line.trim();
                if l.is_empty() {
                    continue;
                }
                match serde_json::from_str::<WireEvent>(l) {
                    Ok(ev) => events.push(ev),
                    Err(e) => rejected.push(pulse_wire::RejectedEvent {
                        index: idx,
                        error: format!(
                            "NDJSON line {}: {}",
                            idx + 1,
                            pulse_wire::sanitize_json_error_message(&e.to_string())
                        ),
                    }),
                }
            }
            WireBatch {
                events,
                rejected,
                profile_calls: Vec::new(),
            }
        } else {
            // CSV with header
            let mut lines = trimmed.lines().filter(|l| !l.trim().is_empty());
            let Some(header_line) = lines.next() else {
                return Err(CtrlError::bad("CSV import body is empty"));
            };
            let cols = parse_csv_line(header_line);
            let mut events = Vec::new();
            for (row_idx, line) in lines.enumerate() {
                let vals = parse_csv_line(line);
                let mut map = serde_json::Map::new();
                let mut props_map = serde_json::Map::new();
                for (c_idx, col_name) in cols.iter().enumerate() {
                    let raw_v = vals.get(c_idx).map(String::as_str).unwrap_or("");
                    if raw_v.is_empty() {
                        continue;
                    }
                    match col_name.as_str() {
                        "event" | "event_name" | "distinct_id" | "user_id" | "userId"
                        | "anonymous_id" | "session_id" | "timestamp" | "client_event_ts_ms"
                        | "insert_id" | "messageId" => {
                            map.insert(col_name.clone(), Value::String(raw_v.to_string()));
                        }
                        other => {
                            if let Ok(i) = raw_v.parse::<i64>() {
                                props_map.insert(other.to_string(), Value::from(i));
                            } else if let Ok(f) = raw_v.parse::<f64>() {
                                props_map.insert(other.to_string(), Value::from(f));
                            } else if raw_v == "true" || raw_v == "false" {
                                props_map.insert(other.to_string(), Value::Bool(raw_v == "true"));
                            } else {
                                props_map
                                    .insert(other.to_string(), Value::String(raw_v.to_string()));
                            }
                        }
                    }
                }
                if !props_map.is_empty() {
                    map.insert("properties".to_string(), Value::Object(props_map));
                }
                match serde_json::from_value::<WireEvent>(Value::Object(map)) {
                    Ok(ev) => events.push(ev),
                    Err(e) => rejected.push(pulse_wire::RejectedEvent {
                        index: row_idx,
                        error: format!(
                            "CSV row {}: {}",
                            row_idx + 1,
                            pulse_wire::sanitize_json_error_message(&e.to_string())
                        ),
                    }),
                }
            }
            WireBatch {
                events,
                rejected,
                profile_calls: Vec::new(),
            }
        }
    } else {
        WireBatch::decode_json(&body).map_err(|e| CtrlError::bad(e.to_string()))?
    };

    if batch.events.is_empty() && batch.profile_calls.is_empty() {
        if let Some(first) = batch.rejected.first() {
            return Err(CtrlError::bad(first.error.clone()));
        }
    }
    // An import is a backfill: `sentAt` is when the source system sent the event long ago,
    // not a live device clock, so the mobile clock-skew correction must not restamp it.
    let mut batch = batch;
    for ev in &mut batch.events {
        ev.client_sent_ts_ms = 0;
    }
    let now_ms = Utc::now().timestamp_millis();

    // Re-running a backfill must add nothing, long after the in-memory dedup window has passed.
    // Claim every event id first; only ids not seen before go through.
    let ids: Vec<String> = batch
        .events
        .iter()
        .map(|e| Uuid::from_bytes(e.event_uuid).simple().to_string())
        .collect();
    let fresh = ctrl
        .meta
        .claim_import_uuids(&id, &ids, now_ms)
        .await
        .map_err(CtrlError::meta)?;
    let claimed: Vec<String> = fresh.iter().cloned().collect();
    let mut taken = HashSet::new();
    let before = batch.events.len();
    let mut keep = ids
        .iter()
        .map(|u| fresh.contains(u) && taken.insert(u.clone()));
    batch.events.retain(|_| keep.next().unwrap_or(false));
    let import_dupes = before - batch.events.len();

    let res = match ingest.ingest_for_project(&id, batch, now_ms).await {
        Ok(r) => r,
        Err(e) => {
            if let Err(re) = ctrl.meta.release_import_uuids(&id, &claimed).await {
                tracing::warn!(error = %re, "failed to release import ids after a failed import");
            }
            return Err(CtrlError::ingest(e));
        }
    };
    let _ = ctrl.qengine.storage().flush_all_projects().await;
    if let Err(e) = ctrl
        .meta
        .prune_import_uuids(now_ms - IMPORT_DEDUP_RETENTION_MS)
        .await
    {
        tracing::warn!(error = %e, "failed to prune old import ids");
    }

    Ok(Json(json!({
        "status": res.status,
        "accepted": res.accepted,
        "deduped": res.deduped + import_dupes,
        "dropped": res.dropped,
        "server_ts_ms": res.server_ts_ms,
        "rejected": res.rejected,
        "profiles_updated": res.profiles_updated,
    })))
}

async fn handle_export_events(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response, CtrlError> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let time_range = match (
        q.get("start_ts_ms").and_then(|s| s.parse::<i64>().ok()),
        q.get("end_ts_ms").and_then(|s| s.parse::<i64>().ok()),
    ) {
        (Some(s), Some(e)) => Some(TimeRange {
            start_ts_ms: s,
            end_ts_ms: e,
        }),
        _ => None,
    };
    let limit = q
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5_000)
        .clamp(1, 50_000);
    let format = q
        .get("format")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "ndjson".to_string());

    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let rows = ctrl
        .qengine
        .export_events(&id, time_range, None, limit, true)
        .await
        .map_err(CtrlError::exec)?;

    if format == "json" {
        return Ok(Json(json!({ "events": rows, "count": rows.len() })).into_response());
    }
    if format == "csv" {
        let mut out =
            String::from("event_ts_ms,event_name,distinct_id,session_id,properties_json\n");
        for r in &rows {
            let pjson = serde_json::to_string(&r.props).unwrap_or_else(|_| "{}".into());
            out.push_str(&format!(
                "{},{},{},{},{}\n",
                r.event_ts_ms,
                csv_escape(&r.event_name),
                csv_escape(&r.distinct_id),
                csv_escape(&r.session_id),
                csv_escape(&pjson),
            ));
        }
        return Ok((
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/csv; charset=utf-8"),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"pulse_events_export.csv\"",
                ),
            ],
            out,
        )
            .into_response());
    }

    let mut ndjson = String::with_capacity(rows.len() * 160);
    for r in &rows {
        if let Ok(line) = serde_json::to_string(r) {
            ndjson.push_str(&line);
            ndjson.push('\n');
        }
    }
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-ndjson; charset=utf-8")],
        ndjson,
    )
        .into_response())
}

async fn handle_export_cohort(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, cohort_id)): AxumPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response, CtrlError> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let (expanded, id_graph) = ctrl.load_erased_for_project_ctrl(&id).await?;
    let members = ctrl
        .qengine
        .evaluate_saved_cohort_uids(&id, &cohort_id)
        .await
        .map_err(CtrlError::exec)?;
    let pstore = ctrl
        .qengine
        .storage()
        .profile_store(&id, false)
        .await
        .map_err(CtrlError::storage)?;
    let mut by_uid: HashMap<u32, UserProfileRecord> = HashMap::with_capacity(members.len());
    for &(uid, _) in &members {
        let rows: Vec<UserProfileRecord> = pstore
            .list_for_canonical(uid)
            .await
            .map_err(CtrlError::storage)?
            .into_iter()
            .filter(|p| !pulse_storage::is_profile_erased(p, &expanded, id_graph.as_deref()))
            .map(ControlPlaneService::from_stored_profile)
            .collect();
        if let Some(merged) = merge_profile_rows(rows).remove(&uid) {
            by_uid.insert(uid, merged);
        }
    }

    let format = q
        .get("format")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "csv".to_string());

    if format == "json" {
        let users: Vec<Value> = members
            .iter()
            .map(|(uid, masked_did)| {
                let prof = by_uid.get(uid);
                json!({
                    "canonical_uid": uid,
                    "distinct_id": masked_did,
                    "props": prof.map(|p| &p.props),
                    "total_revenue": prof.map(|p| p.total_revenue).unwrap_or(0.0),
                })
            })
            .collect();
        return Ok(
            Json(json!({ "cohort_id": cohort_id, "count": users.len(), "users": users }))
                .into_response(),
        );
    }

    let mut csv = String::from("canonical_uid,distinct_id,plan,utm_source,total_revenue\n");
    for (uid, masked_did) in &members {
        let prof = by_uid.get(uid);
        let plan = prof
            .and_then(|p| p.props.get("plan").and_then(|v| v.as_str()))
            .unwrap_or("");
        let utm = prof
            .and_then(|p| p.props.get("utm_source").and_then(|v| v.as_str()))
            .unwrap_or("");
        let rev = prof.map(|p| p.total_revenue).unwrap_or(0.0);
        csv.push_str(&format!(
            "{},{},{},{},{:.2}\n",
            uid,
            csv_escape(masked_did),
            csv_escape(plan),
            csv_escape(utm),
            rev,
        ));
    }
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"cohort_{cohort_id}.csv\""),
            ),
        ],
        csv,
    )
        .into_response())
}

async fn handle_get_revenue_summary(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    // The SDKs' trackRevenue sends `$revenue` with `amount`; `?event=` covers apps that log purchases under their own event.
    let revenue_event = params
        .get("event")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("$revenue")
        .to_string();
    let (erased, id_graph) = ctrl.load_erased_for_project_ctrl(&id).await?;
    let pstore = ctrl
        .qengine
        .storage()
        .profile_store(&id, false)
        .await
        .map_err(CtrlError::storage)?;
    let (total_profiles, paying_users, total_revenue) = pstore
        .revenue_summary_filtered(&erased, id_graph.as_deref())
        .await
        .map_err(CtrlError::storage)?;

    let now_ms = Utc::now().timestamp_millis();
    let iq = InsightsQuery {
        time_range: TimeRange {
            start_ts_ms: now_ms - 14 * 86_400_000,
            end_ts_ms: now_ms,
        },
        interval: pulse_exec::TimeInterval::Day,
        series: vec![
            pulse_exec::InsightsSeriesSpec {
                label: "Revenue".to_string(),
                event_name: revenue_event.clone(),
                metric: pulse_exec::InsightsMetric::PropSum {
                    prop: "amount".to_string(),
                },
                filters: Vec::new(),
            },
            pulse_exec::InsightsSeriesSpec {
                label: "Paying Users".to_string(),
                event_name: revenue_event.clone(),
                metric: pulse_exec::InsightsMetric::Uniques,
                filters: Vec::new(),
            },
        ],
        breakdown_by: Vec::new(),
        global_filter: Default::default(),
        compare_previous_period: false,
        formula: None,
        sampling: Default::default(),
    };
    let ins_opt = ctrl.qengine.query_insights(&id, &iq).await.ok();

    let arpu = if total_profiles > 0 {
        total_revenue / (total_profiles as f64)
    } else {
        0.0
    };
    let arppu = if paying_users > 0 {
        total_revenue / (paying_users as f64)
    } else {
        0.0
    };

    Ok(Json(json!({
        "total_revenue": total_revenue,
        "paying_users": paying_users,
        "total_profiles": total_profiles,
        "arpu": arpu,
        "arppu": arppu,
        "revenue_event": revenue_event,
        "daily_series": ins_opt.map(|r| r.series).unwrap_or_default(),
    })))
}

pub(crate) const ALERT_OPERATORS: [&str; 4] = ["gt", "gte", "lt", "lte"];
/// An alert looks at the last 24 hours and, while its condition holds, fires at most once per window.
const ALERT_WINDOW_MS: i64 = 86_400_000;

pub(crate) fn alert_condition_met(operator: &str, value: f64, threshold: f64) -> bool {
    match operator {
        "gt" => value > threshold,
        "gte" => value >= threshold,
        "lt" => value < threshold,
        "lte" => value <= threshold,
        _ => false,
    }
}

impl ControlPlaneService {
    /// Count of the alert's event over the last 24 hours.
    async fn alert_value(&self, a: &AlertRule, now_ms: i64) -> f64 {
        let iq = InsightsQuery {
            time_range: TimeRange {
                start_ts_ms: now_ms - ALERT_WINDOW_MS,
                end_ts_ms: now_ms,
            },
            interval: pulse_exec::TimeInterval::Day,
            series: vec![pulse_exec::InsightsSeriesSpec {
                label: a.name.clone(),
                event_name: a.event_name.clone(),
                metric: pulse_exec::InsightsMetric::Totals,
                filters: Vec::new(),
            }],
            breakdown_by: Vec::new(),
            global_filter: Default::default(),
            compare_previous_period: false,
            formula: None,
            sampling: Default::default(),
        };
        self.qengine
            .query_insights(&a.project_id, &iq)
            .await
            .ok()
            .and_then(|r| r.series.first().and_then(|s| s.overall_value))
            .unwrap_or(0.0)
    }

    /// Checks every alert rule, records when one fires, and posts a signed webhook for it.
    /// `pulse-node` calls this on a timer.
    /// Every query-serving node runs this; the conditional update on `last_triggered_ms` is the
    /// claim, so only one node delivers each trigger.
    pub async fn evaluate_alerts(&self) {
        let now_ms = Utc::now().timestamp_millis();
        let Ok(projects) = self.meta.list_projects().await else {
            return;
        };
        for proj in projects {
            let Ok(rules) = self.meta.list_alert_rules(&proj.id).await else {
                continue;
            };
            if !rules.is_empty() {
                let _ = self.load_erased_for_project(&proj.id).await;
            }
            for mut a in rules {
                let prev_triggered_ms = a.last_triggered_ms;
                if a.last_triggered_ms
                    .is_some_and(|t| now_ms - t < ALERT_WINDOW_MS)
                {
                    continue;
                }
                let value = self.alert_value(&a, now_ms).await;
                if !alert_condition_met(&a.operator, value, a.threshold) {
                    continue;
                }
                a.last_triggered_ms = Some(now_ms);
                match self
                    .meta
                    .claim_alert_trigger(&a.id, prev_triggered_ms, now_ms)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => continue, // another node claimed this trigger
                    Err(e) => {
                        tracing::warn!(alert_id = %a.id, "couldn't record alert trigger: {e}");
                        continue;
                    }
                }
                match self.deliver_alert(&a, value, now_ms).await {
                    Ok(Some(status)) => {
                        tracing::info!(alert_id = %a.id, %status, "alert fired, webhook delivered")
                    }
                    Ok(None) => {
                        tracing::info!(alert_id = %a.id, "alert fired; no webhook configured")
                    }
                    Err(e) => tracing::warn!(alert_id = %a.id, "alert fired, webhook failed: {e}"),
                }
            }
        }
    }

    /// Posts the alert to its `webhook_url`, or to the project's webhook credential URL, signed with
    /// that credential's secret. Returns `Ok(None)` when the project has no webhook credential.
    async fn deliver_alert(
        &self,
        a: &AlertRule,
        value: f64,
        now_ms: i64,
    ) -> Result<Option<reqwest::StatusCode>, String> {
        let Some(rec) = self
            .meta
            .get_byok_credential(&a.project_id, "webhook")
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        let cred_url = unseal_secret(&self.vault_kek, &rec.sealed_secret_hex)?;
        let cfg = unseal_byok_config_json(&self.vault_kek, &rec.config_json)?;
        let signing_secret = webhook_signing_secret(&cfg)
            .ok_or("webhook credential has no signing_secret")?
            .to_string();
        let url = a.webhook_url.clone().unwrap_or(cred_url);
        let body = json!({
            "event": "pulse.alert.triggered",
            "project_id": a.project_id,
            "alert": {
                "id": a.id,
                "name": a.name,
                "event_name": a.event_name,
                "operator": a.operator,
                "threshold": a.threshold,
            },
            "value": value,
            "window_ms": ALERT_WINDOW_MS,
            "timestamp_ms": now_ms,
        });
        post_signed_webhook(&url, &signing_secret, &body)
            .await
            .map(Some)
    }
}

/// POSTs `body` with `X-Pulse-Timestamp` and `X-Pulse-Signature-256` through the SSRF-pinned client.
pub(crate) async fn post_signed_webhook(
    url: &str,
    signing_secret: &str,
    body: &Value,
) -> Result<reqwest::StatusCode, String> {
    let (parsed, client) =
        crate::copilot::pinned_outbound_client(url, std::time::Duration::from_secs(10))
            .await
            .map_err(|e| format!("webhook URL blocked by SSRF policy: {e}"))?;
    let body_bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
    let ts = Utc::now().timestamp().to_string();
    let sig_hdr = sign_webhook(signing_secret, &ts, &body_bytes);
    let resp = client
        .post(parsed.as_str())
        .header("Content-Type", "application/json")
        .header("X-Pulse-Timestamp", ts)
        .header("X-Pulse-Signature-256", sig_hdr)
        .body(body_bytes)
        .send()
        .await
        .map_err(|e| format!("webhook delivery failed: {}", e.without_url()))?;
    if !resp.status().is_success() {
        return Err(format!("webhook endpoint returned HTTP {}", resp.status()));
    }
    Ok(resp.status())
}

async fn handle_list_alerts(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<Value>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let alerts = ctrl
        .meta
        .list_alert_rules(&id)
        .await
        .map_err(CtrlError::meta)?;

    let now_ms = Utc::now().timestamp_millis();
    let mut out = Vec::with_capacity(alerts.len());
    for a in alerts {
        let current_value = ctrl.alert_value(&a, now_ms).await;
        let triggered = alert_condition_met(&a.operator, current_value, a.threshold);
        out.push(json!({
            "id": a.id,
            "project_id": a.project_id,
            "name": a.name,
            "event_name": a.event_name,
            "operator": a.operator,
            "threshold": a.threshold,
            "webhook_url": a.webhook_url,
            "last_triggered_ms": a.last_triggered_ms,
            "created_at_ms": a.created_at_ms,
            "current_value": current_value,
            "triggered": triggered,
        }));
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
struct CreateAlertReq {
    name: String,
    event_name: String,
    #[serde(default = "default_gt_op")]
    operator: String,
    threshold: f64,
    #[serde(default)]
    webhook_url: Option<String>,
}

fn default_gt_op() -> String {
    "gt".to_string()
}

async fn handle_create_alert(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateAlertReq>,
) -> ApiRes<AlertRule> {
    ctrl.authorize_and_check(&principal, &id).await?;
    if req.name.trim().is_empty() || req.event_name.trim().is_empty() {
        return Err(CtrlError::bad("alert name and event_name are required"));
    }
    if !ALERT_OPERATORS.contains(&req.operator.as_str()) {
        return Err(CtrlError::bad("operator must be one of: gt, gte, lt, lte"));
    }
    let webhook_url = req
        .webhook_url
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if let Some(u) = &webhook_url {
        validate_outbound_url_ssrf(u)
            .map_err(|e| CtrlError::bad(format!("webhook_url rejected by SSRF policy: {e}")))?;
    }
    let rec = AlertRule {
        id: format!("alr_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id,
        name: req.name.trim().to_string(),
        event_name: req.event_name.trim().to_string(),
        operator: req.operator,
        threshold: req.threshold,
        webhook_url,
        last_triggered_ms: None,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_alert_rule(&rec)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(rec))
}

async fn handle_delete_alert(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, alert_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let deleted = ctrl
        .meta
        .delete_alert_rule(&id, &alert_id)
        .await
        .map_err(CtrlError::meta)?;
    if deleted == 0 {
        return Err(CtrlError::not_found("alert rule not found"));
    }
    Ok(Json(json!({ "status": "ok", "id": alert_id })))
}

// ======================= ENCRYPTION, PROVIDER CREDENTIALS & COPILOT =======================

struct StorageCfg<'a> {
    url: Option<&'a str>,
    access_key: Option<&'a str>,
    region: Option<&'a str>,
    endpoint: Option<&'a str>,
    prefix: Option<&'a str>,
}

fn storage_cfg_fields(cfg: &Value) -> StorageCfg<'_> {
    let url = cfg
        .get("url")
        .or_else(|| cfg.get("bucket_url"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let access_key = cfg
        .get("access_key")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let region = cfg
        .get("region")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let endpoint = cfg
        .get("endpoint")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let prefix = cfg
        .get("prefix")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().trim_matches('/'))
        .filter(|s| !s.is_empty());
    StorageCfg {
        url,
        access_key,
        region,
        endpoint,
        prefix,
    }
}

pub(crate) fn webhook_signing_secret(cfg: &Value) -> Option<&str> {
    cfg.get("signing_secret")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| s.len() >= 16)
}

/// `sha256=<hex HMAC of "{timestamp}.{body}">`; receivers reject stale timestamps to stop replays.
pub(crate) fn sign_webhook(signing_secret: &str, ts: &str, body: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_secret.as_bytes());
    let mut ctx = hmac::Context::with_key(&key);
    ctx.update(ts.as_bytes());
    ctx.update(b".");
    ctx.update(body);
    format!("sha256={}", hex::encode(ctx.sign().as_ref()))
}

fn validate_byok_inputs(category: &str, secret: &str, cfg: &Value) -> Result<(), CtrlError> {
    match category {
        "webhook" => {
            validate_outbound_url_ssrf(secret)
                .map_err(|e| CtrlError::bad(format!("webhook URL rejected by SSRF policy: {e}")))?;
            if webhook_signing_secret(cfg).is_none() {
                return Err(CtrlError::bad(
                    "webhook requires config_json.signing_secret of at least 16 characters",
                ));
            }
        }
        "storage" => {
            let sc = storage_cfg_fields(cfg);
            let url = sc.url.ok_or_else(|| {
                CtrlError::bad("storage credential requires config_json.url (e.g. s3://bucket)")
            })?;
            build_customer_object_store(url, sc.access_key, Some(secret), sc.region, sc.endpoint)
                .map_err(|e| CtrlError::bad(format!("invalid or blocked storage destination: {e}")))?;
        }
        _ => {
            if secret.len() < 8 {
                return Err(CtrlError::bad(
                    "LLM API key appears too short (expected >= 8 characters)",
                ));
            }
            if let Some(ep) = cfg
                .get("endpoint")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                validate_outbound_url_ssrf(ep).map_err(|e| {
                    CtrlError::bad(format!("LLM endpoint rejected by SSRF policy: {e}"))
                })?;
            }
        }
    }
    Ok(())
}

async fn handle_get_encryption(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<Value> {
    let _ = principal.require_cluster_admin()?;
    let keyring = ctrl
        .meta
        .list_system_keyring()
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({
        "status": ctrl.qengine.storage().byok_encryption_status(),
        "keyring": keyring,
        "has_durable_kek": ctrl.has_durable_kek,
    })))
}

#[derive(Deserialize)]
struct RotateEncryptionReq {
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    disable_new_writes: bool,
}

async fn handle_rotate_encryption(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Json(req): Json<RotateEncryptionReq>,
) -> ApiRes<Value> {
    let actor = principal.require_cluster_admin()?;

    if req.disable_new_writes {
        ctrl.meta
            .deactivate_all_keyring_writes()
            .await
            .map_err(CtrlError::meta)?;
        ctrl.qengine.storage().disable_new_encryption();
        ctrl.record_enterprise_audit(
            &actor,
            "encryption.disable_new_writes",
            "cluster_keyring",
            json!({}),
        )
        .await;
        let keyring = ctrl
            .meta
            .list_system_keyring()
            .await
            .map_err(CtrlError::meta)?;
        return Ok(Json(json!({
            "status": ctrl.qengine.storage().byok_encryption_status(),
            "keyring": keyring,
            "has_durable_kek": ctrl.has_durable_kek,
        })));
    }

    let secret = req
        .secret
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CtrlError::bad("secret or disable_new_writes=true is required"))?;

    ctrl.require_durable_kek()?;

    let mut salt = [0u8; 16];
    SystemRandom::new()
        .fill(&mut salt)
        .map_err(|_| CtrlError::internal("CSPRNG failure generating key derivation salt"))?;
    let master =
        parse_or_derive_master_key(secret, &salt).map_err(|e| CtrlError::bad(e.to_string()))?;
    let key_id = make_key_id(&master);

    ctrl.qengine
        .storage()
        .register_byok_master_key(&key_id, master, true)
        .map_err(CtrlError::internal)?;

    ctrl.meta
        .upsert_system_keyring_key(&SystemKeyringRecord {
            key_id: key_id.clone(),
            salt_hex: hex::encode(salt),
            sealed_key_hex: seal_secret(&ctrl.vault_kek, secret),
            is_active: true,
            created_at_ms: Utc::now().timestamp_millis(),
        })
        .await
        .map_err(CtrlError::meta)?;

    ctrl.record_enterprise_audit(
        &actor,
        "encryption.rotate_key",
        "cluster_keyring",
        json!({ "key_id": key_id }),
    )
    .await;

    let keyring = ctrl
        .meta
        .list_system_keyring()
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(json!({
        "status": ctrl.qengine.storage().byok_encryption_status(),
        "keyring": keyring,
        "has_durable_kek": ctrl.has_durable_kek,
    })))
}

#[derive(Deserialize)]
struct AuditLogQuery {
    limit: Option<i64>,
}

async fn handle_list_audit_logs(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Query(query): Query<AuditLogQuery>,
) -> ApiRes<Vec<EnterpriseAuditRecord>> {
    let _ = principal.require_cluster_admin()?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let logs = ctrl
        .meta
        .list_enterprise_audit(limit)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(logs))
}

async fn handle_list_byok(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<ByokCredentialView>> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let recs = ctrl
        .meta
        .list_byok_credentials(&id)
        .await
        .map_err(CtrlError::meta)?;
    Ok(Json(recs.iter().map(ByokCredentialView::from).collect()))
}

#[derive(Deserialize)]
struct UpsertByokReq {
    category: String,
    provider: String,
    #[serde(default)]
    label: Option<String>,
    secret: String,
    #[serde(default)]
    config_json: Option<Value>,
}

async fn handle_upsert_byok(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UpsertByokReq>,
) -> ApiRes<ByokCredentialView> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let category = req.category.trim().to_ascii_lowercase();
    if !BYOK_CATEGORIES.contains(&category.as_str()) {
        return Err(CtrlError::bad(
            "category must be one of: llm, storage, webhook",
        ));
    }

    let secret = req.secret.trim();
    if secret.is_empty() {
        return Err(CtrlError::bad("secret cannot be empty"));
    }
    let provider = req.provider.trim().to_string();
    if provider.is_empty() {
        return Err(CtrlError::bad("provider is required"));
    }
    let config_json = req.config_json.unwrap_or_else(|| json!({}));
    validate_byok_inputs(&category, secret, &config_json)?;
    ctrl.require_durable_kek()?;

    let now_ms = Utc::now().timestamp_millis();
    let rec = ByokCredentialRecord {
        id: format!("byok_{}", &Uuid::new_v4().simple().to_string()[..12]),
        project_id: id.clone(),
        category: category.clone(),
        provider: provider.clone(),
        label: req
            .label
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("BYOK {provider}")),
        key_prefix_masked: mask_byok_secret(secret),
        sealed_secret_hex: seal_secret(&ctrl.vault_kek, secret),
        config_json: seal_byok_config_json(&ctrl.vault_kek, &config_json),
        status: "configured".to_string(),
        last_verified_at_ms: None,
        updated_at_ms: now_ms,
    };

    ctrl.meta
        .upsert_byok_credential(&rec)
        .await
        .map_err(CtrlError::meta)?;

    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "byok.upsert",
        &format!("project:{id}/byok:{category}"),
        json!({ "provider": provider, "label": rec.label }),
    )
    .await;

    Ok(Json(ByokCredentialView::from(&rec)))
}

async fn handle_delete_byok(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, category)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let cat = category.trim().to_ascii_lowercase();
    let affected = ctrl
        .meta
        .delete_byok_credential(&id, &cat)
        .await
        .map_err(CtrlError::meta)?;
    if affected == 0 {
        return Err(CtrlError::not_found("BYOK credential not found"));
    }
    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "byok.delete",
        &format!("project:{id}/byok:{cat}"),
        json!({}),
    )
    .await;
    Ok(Json(json!({ "status": "ok", "category": cat })))
}

#[derive(Deserialize)]
struct VerifyByokReq {
    category: String,
}

async fn handle_verify_byok(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<VerifyByokReq>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let cat = req.category.trim().to_ascii_lowercase();
    if !BYOK_CATEGORIES.contains(&cat.as_str()) {
        return Err(CtrlError::bad(
            "category must be one of: llm, storage, webhook",
        ));
    }

    let rec = ctrl
        .meta
        .get_byok_credential(&id, &cat)
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| {
            CtrlError::not_found(format!("no BYOK credential configured for '{cat}'"))
        })?;
    let secret =
        unseal_secret(&ctrl.vault_kek, &rec.sealed_secret_hex).map_err(CtrlError::internal)?;
    let cfg =
        unseal_byok_config_json(&ctrl.vault_kek, &rec.config_json).map_err(CtrlError::internal)?;

    let details = match cat.as_str() {
        "storage" => {
            let sc = storage_cfg_fields(&cfg);
            let url = sc
                .url
                .ok_or_else(|| CtrlError::bad("storage credential missing config_json.url"))?;
            let entries = validate_object_store_connection(
                url,
                sc.access_key,
                Some(secret.as_str()),
                sc.region,
                sc.endpoint,
            )
            .await
            .map_err(|e| CtrlError::bad(format!("object store verification failed: {e}")))?;
            format!("Verified connectivity to {url} ({entries} root entries visible)")
        }
        "webhook" => {
            let probe_body = json!({
                "event": "pulse.webhook.verify",
                "project_id": id,
                "timestamp_ms": Utc::now().timestamp_millis(),
            });
            let signing_secret = webhook_signing_secret(&cfg).ok_or_else(|| {
                CtrlError::bad(
                    "webhook credential needs config_json.signing_secret (>= 16 chars); save it again",
                )
            })?;
            let status = post_signed_webhook(&secret, signing_secret, &probe_body)
                .await
                .map_err(CtrlError::bad_gateway)?;
            let host = reqwest::Url::parse(&secret)
                .ok()
                .and_then(|u| u.host_str().map(str::to_string))
                .unwrap_or_else(|| "webhook".to_string());
            format!(
                "Delivered signed probe (HTTP {}) to {host}",
                status.as_u16()
            )
        }
        _ => {
            // A real round trip: the same request the Copilot makes, with a one-line prompt.
            let (model, endpoint) = crate::copilot::llm_model_and_endpoint(&rec.provider, &cfg);
            crate::copilot::call_external_llm(
                &rec.provider,
                &model,
                &secret,
                endpoint.as_deref(),
                "Reply with the single word OK.",
                &json!({}),
                &[],
            )
            .await
            .map_err(|e| match e {
                crate::copilot::CopilotError::BadRequest(m) => CtrlError::bad(m),
                crate::copilot::CopilotError::Upstream(m) => CtrlError::bad_gateway(format!(
                    "{} rejected the key or request: {m}",
                    rec.provider
                )),
            })?;
            format!(
                "{} answered a test request with model {model}",
                rec.provider
            )
        }
    };

    let now_ms = Utc::now().timestamp_millis();
    ctrl.meta
        .touch_byok_verified(&id, &cat, "verified", now_ms)
        .await
        .map_err(CtrlError::meta)?;

    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "byok.verify",
        &format!("project:{id}/byok:{cat}"),
        json!({ "provider": rec.provider, "details": details }),
    )
    .await;

    Ok(Json(json!({
        "verified": true,
        "category": cat,
        "provider": rec.provider,
        "details": details,
        "verified_at_ms": now_ms,
    })))
}

#[derive(Deserialize, Default)]
struct WarehouseExportReq {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    start_ts_ms: Option<i64>,
    #[serde(default)]
    end_ts_ms: Option<i64>,
}

async fn handle_warehouse_export(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    body: Option<Json<WarehouseExportReq>>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let req = body.map(|Json(r)| r).unwrap_or_default();

    let storage_rec = ctrl
        .meta
        .get_byok_credential(&id, "storage")
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| {
            CtrlError::bad(
                "No BYOK cloud storage destination configured for this project. Save a 'storage' credential first.",
            )
        })?;
    let secret_key = unseal_secret(&ctrl.vault_kek, &storage_rec.sealed_secret_hex)
        .map_err(CtrlError::internal)?;
    let storage_cfg = unseal_byok_config_json(&ctrl.vault_kek, &storage_rec.config_json)
        .map_err(CtrlError::internal)?;
    let sc = storage_cfg_fields(&storage_cfg);
    let url = sc.url.ok_or_else(|| {
        CtrlError::bad("storage credential requires config_json.url (e.g. s3://bucket)")
    })?;

    let dest_store = build_customer_object_store(
        url,
        sc.access_key,
        Some(secret_key.as_str()),
        sc.region,
        sc.endpoint,
    )
    .map_err(|e| CtrlError::bad(format!("cannot open customer warehouse destination: {e}")))?;

    let time_range = match (req.start_ts_ms, req.end_ts_ms) {
        (Some(start_ts_ms), Some(end_ts_ms)) => {
            if start_ts_ms > end_ts_ms {
                return Err(CtrlError::bad("start_ts_ms must be <= end_ts_ms"));
            }
            Some(TimeRange {
                start_ts_ms,
                end_ts_ms,
            })
        }
        _ => None,
    };
    let limit = req.limit.unwrap_or(10_000).clamp(1, 50_000);
    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let rows = ctrl
        .qengine
        .export_events(&id, time_range, None, limit, true)
        .await
        .map_err(CtrlError::exec)?;

    let mut ndjson = String::new();
    for row in &rows {
        let line = serde_json::to_string(row).map_err(CtrlError::internal)?;
        ndjson.push_str(&line);
        ndjson.push('\n');
    }

    let now_ms = Utc::now().timestamp_millis();
    let object_key = match sc.prefix {
        Some(pfx) => format!("{pfx}/exports/{id}/events_{now_ms}.ndjson"),
        None => format!("exports/{id}/events_{now_ms}.ndjson"),
    };
    let byte_len = ndjson.len();
    dest_store
        .put(
            &ObjectPath::from(object_key.as_str()),
            PutPayload::from_bytes(Bytes::from(ndjson)),
        )
        .await
        .map_err(|e| CtrlError::bad_gateway(format!("warehouse upload failed: {e}")))?;

    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "warehouse.export",
        &format!("project:{id}"),
        json!({
            "destination_url": url,
            "object_key": object_key,
            "events_exported": rows.len(),
            "bytes_written": byte_len,
        }),
    )
    .await;

    Ok(Json(json!({
        "status": "ok",
        "destination_url": url,
        "object_key": object_key,
        "events_exported": rows.len(),
        "bytes_written": byte_len,
    })))
}

async fn handle_byok_copilot(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CopilotRequest>,
) -> ApiRes<crate::copilot::CopilotResponse> {
    ctrl.authorize_and_check(&principal, &id).await?;

    let llm_cred = ctrl
        .meta
        .get_byok_credential(&id, "llm")
        .await
        .map_err(CtrlError::meta)?
        .ok_or_else(|| {
            CtrlError::bad(
                "No BYOK LLM API key configured for this project. Add one under BYOK Vault first.",
            )
        })?;
    let llm_api_key =
        unseal_secret(&ctrl.vault_kek, &llm_cred.sealed_secret_hex).map_err(CtrlError::internal)?;

    ctrl.load_erased_for_project(&id)
        .await
        .map_err(CtrlError::internal)?;
    let resp = run_byok_copilot(
        &ctrl.qengine,
        &ctrl.meta,
        &id,
        &llm_cred,
        &llm_api_key,
        &req,
    )
    .await
    .map_err(|err| match err {
        CopilotError::BadRequest(msg) => CtrlError::bad(msg),
        CopilotError::Upstream(msg) => CtrlError::bad_gateway(msg),
    })?;

    ctrl.record_enterprise_audit(
        &principal.actor_label(),
        "copilot.query",
        &format!("project:{id}"),
        json!({
            "provider": resp.provider,
            "model": resp.model,
            "tools_executed": resp.tools_executed,
        }),
    )
    .await;

    Ok(Json(resp))
}

#[cfg(test)]
mod cluster_admin_tests {
    use super::AuthPrincipal;

    #[test]
    fn only_root_token_is_cluster_admin() {
        assert!(AuthPrincipal::RootAdmin.require_cluster_admin().is_ok());
        let user = AuthPrincipal::User {
            user_id: "u1".into(),
            org_id: "org_default".into(),
            role: crate::models::Role::Owner,
        };
        assert!(user.require_cluster_admin().is_err());
        let proj = AuthPrincipal::ProjectAdmin {
            project_id: "p".into(),
        };
        assert!(proj.require_cluster_admin().is_err());
    }
}

#[cfg(test)]
mod agent_service_wiring_tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use pulse_agent::{
        AgentTrace, EvalTargetType, EvaluatorConfig, EvaluatorType, TraceIngestBatch, TraceStatus,
    };
    use pulse_exec::CatalogRegistry;
    use pulse_identity::IdentityRouter;
    use pulse_storage::{build_from_url, EncryptedObjectStore, StorageConfig, StorageEngine};

    async fn make_test_service(tmp: &std::path::Path) -> Arc<ControlPlaneService> {
        let raw_store = build_from_url("memory://").unwrap();
        let enc_store = Arc::new(EncryptedObjectStore::new(raw_store));
        let id_router = Arc::new(IdentityRouter::new(enc_store.as_object_store()));
        let storage = StorageEngine::new_with_encrypted(
            StorageConfig {
                num_buckets: 2,
                l0_flush_row_threshold: 1000,
                l0_flush_max_age: Duration::from_secs(60),
                compaction_min_segments: 4,
            },
            enc_store,
            id_router,
        );
        let qengine = QueryEngine::new(storage, 4 * 1024 * 1024, None, 0, CatalogRegistry::new())
            .await
            .unwrap();
        let meta = MetaStore::connect_auto(tmp).await.unwrap();
        ControlPlaneService::new_with_admin_with_agent_config(
            meta,
            qengine,
            "proj_wiring",
            "pk_wiring_test_1234567890",
            Some("sk_root_wiring_test_1234567890".to_string()),
            AgentTraceStoreConfig::default(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn eval_queue_drop_counter_and_worker_shutdown_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = make_test_service(tmp.path()).await;

        let job = |tid: &str| EvalJob {
            project_id: "proj_wiring".to_string(),
            session_id: None,
            trace_id: Some(tid.to_string()),
            span_id: None,
            evaluator_ids: None,
        };

        // Fill the bounded queue to capacity while receiver is not yet spawned.
        for i in 0..EVAL_QUEUE_CAPACITY {
            assert!(
                svc.try_enqueue_eval(job(&format!("tr_fill_{i}"))),
                "expected enqueue {i} to succeed"
            );
        }
        assert_eq!(
            svc.agent_metrics()
                .eval_queue_dropped
                .load(Ordering::Relaxed),
            0
        );

        // Next enqueue must fail non-blockingly and increment eval_queue_dropped.
        assert!(!svc.try_enqueue_eval(job("tr_overflow")));
        assert_eq!(
            svc.agent_metrics()
                .eval_queue_dropped
                .load(Ordering::Relaxed),
            1
        );

        // Drain the test queue before spawning worker with a real trace job.
        {
            let mut rx = svc.take_eval_rx().expect("eval_rx available");
            while rx.try_recv().is_ok() {}
            drop(rx);
        }

        // Re-test spawning & shutdown drain on a fresh service instance.
        let tmp2 = tempfile::tempdir().unwrap();
        let svc2 = make_test_service(tmp2.path()).await;

        let replacement_store = Arc::new(
            AgentTraceStore::open(
                build_from_url("memory://").unwrap(),
                AgentTraceStoreConfig::default(),
            )
            .await
            .unwrap(),
        );
        svc2.replace_agent_store(replacement_store.clone())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&svc2.agent_store(), &replacement_store));

        svc2.agent_store()
            .ingest_batch(
                "proj_wiring",
                TraceIngestBatch {
                    traces: vec![AgentTrace {
                        trace_id: "tr_svc_1".to_string(),
                        project_id: "proj_wiring".to_string(),
                        agent_name: "wiring-bot".to_string(),
                        agent_version: "1.0.0".to_string(),
                        prompt_version: None,
                        model_config_version: None,
                        tool_config_version: None,
                        app_release: None,
                        environment: "production".to_string(),
                        session_id: Some("sess_svc".to_string()),
                        user_id: None,
                        workflow_id: None,
                        parent_trace_id: None,
                        status: TraceStatus::Ok,
                        start_ts_ms: 1000,
                        end_ts_ms: Some(1100),
                        duration_ms: 100,
                        input: serde_json::json!({"q": "hi"}),
                        output: serde_json::json!({"a": "hello"}),
                        error_type: None,
                        error_message: None,
                        failure_source: None,
                        metadata: serde_json::Value::Null,
                        tags: vec![],
                        span_count: 0,
                        llm_call_count: 0,
                        tool_call_count: 0,
                        error_span_count: 0,
                        input_tokens: 0,
                        output_tokens: 0,
                        cached_tokens: 0,
                        total_tokens: 0,
                        total_cost_nanos: 0,
                        total_cost_usd: 0.0,
                        primary_model: None,
                        primary_provider: None,
                        avg_eval_score: None,
                        eval_pass_rate: None,
                        has_loop_warning: false,
                    }],
                    spans: vec![],
                },
            )
            .await
            .unwrap();
        svc2.agent_store().flush_all_l0().await.unwrap();

        svc2.meta()
            .upsert_evaluator_config(&EvaluatorConfig {
                id: "ev_wiring".to_string(),
                project_id: "proj_wiring".to_string(),
                name: "No Errors".to_string(),
                description: String::new(),
                target_type: EvalTargetType::Trace,
                evaluator_type: EvaluatorType::Deterministic,
                kind: "no_errors".to_string(),
                version: "1.0.0".to_string(),
                enabled: true,
                agent_name: None,
                environment: None,
                sample_rate: 1.0,
                config: serde_json::json!({}),
                created_at_ms: 1000,
                updated_at_ms: 1000,
            })
            .await
            .unwrap();

        assert!(svc2.try_enqueue_eval(EvalJob {
            project_id: "proj_wiring".to_string(),
            session_id: None,
            trace_id: Some("tr_svc_1".to_string()),
            span_id: None,
            evaluator_ids: None,
        }));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = svc2
            .spawn_agent_eval_worker(shutdown_rx)
            .expect("worker should spawn once");
        assert!(
            svc2.spawn_agent_eval_worker(watch::channel(false).1)
                .is_none(),
            "second spawn should return None"
        );

        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();

        assert_eq!(
            svc2.agent_metrics().evals_executed.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            svc2.meta()
                .list_trace_evaluations("proj_wiring", "tr_svc_1")
                .await
                .unwrap()
                .len(),
            1
        );

        svc2.agent_store().close().await.unwrap();
    }

    #[test]
    fn ctrl_error_maps_dictionary_full_to_422() {
        let direct =
            CtrlError::storage(pulse_storage::StorageError::DictionaryFull { kind: "events" });
        assert_eq!(direct.0, StatusCode::UNPROCESSABLE_ENTITY);

        let via_ingest = CtrlError::ingest(IngestError::Storage(
            pulse_storage::StorageError::DictionaryFull { kind: "prop_keys" },
        ));
        assert_eq!(via_ingest.0, StatusCode::UNPROCESSABLE_ENTITY);
    }
}
