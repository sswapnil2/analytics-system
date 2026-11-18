use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
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
use pulse_exec::{
    validate_cohort_acyclic, BehavioralCohortDef, CustomEventDef, CustomPropertyDef, FlowsQuery,
    FunnelQuery, InsightsQuery, LookupTableDef, QueryEngine, RetentionQuery, SessionSearchQuery,
    TimeRange,
};
use pulse_ingest::{DiscoveredEventSchema, GovernanceProvider, IngestService, ProjectLexiconRules};
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
use tokio::sync::mpsc;
use uuid::Uuid;

mod accounts;
mod auth;
pub use auth::AuthConfig;

use crate::copilot::{run_byok_copilot, CopilotError, CopilotRequest};
use crate::db::{MetaError, MetaStore};
use crate::experiments::{build_edge_flag_bundle_with_cohorts, evaluate_experiment_results};
use crate::license::{
    evaluate_license_health, verify_license_signature, CommercialFeature, EditionTier,
    LicenseClaims, LicenseHealth, LicenseStatusResponse, SystemLicenseRecord,
    LICENSE_GRACE_PERIOD_MS,
};
use crate::mcp::{handle_mcp_request, JsonRpcRequest};
use crate::models::{
    apply_profile_ops, generate_random_api_key, generate_root_admin_token, mask_byok_secret,
    mask_key_prefix, seal_byok_config_json, seal_secret, sha256_bytes, sha256_hex,
    unseal_byok_config_json, unseal_secret, AlertRule, ApiKeyRecord, ByokCredentialRecord,
    ByokCredentialView, CohortRecord, CreatedApiKeyResponse, CustomSchemaBundle, Dashboard,
    DashboardCard, DeletionAuditRecord, EnterpriseAuditRecord, Experiment, ExperimentMetricSpec,
    FeatureFlag, GroupProfileRecord, LexiconEvent, LexiconProperty, Organization, Project, Role,
    SavedReport, SystemKeyringRecord, UserProfileRecord, VariantAllocation, BYOK_CATEGORIES,
    KEY_ROLE_ADMIN, KEY_ROLE_INGEST_AND_FLAGS, REPORT_KINDS,
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

    fn actor_label(&self) -> String {
        match self {
            AuthPrincipal::RootAdmin => "root_admin".to_string(),
            AuthPrincipal::User { user_id, .. } => format!("user:{user_id}"),
            AuthPrincipal::ProjectAdmin { project_id } => format!("project_admin:{project_id}"),
        }
    }

    /// Encryption keys and the licence: root admin token only. Studio users (any self-registered
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
    default_project_id: String,
    root_admin_hash: [u8; 32],
    vault_kek: [u8; 32],
    has_durable_kek: bool,
    bootstrap_creds: Mutex<BootstrapCredentials>,
    /// Keyed by lowercase SHA-256 hex digest (`P0-02`).
    api_key_cache: DashMap<String, CachedApiKey>,
    /// Project id → owning org id.
    known_projects: DashMap<String, String>,
    lexicon_cache: DashMap<String, Arc<ProjectLexiconRules>>,
    /// Per project: the update time up to which user profiles are loaded into the catalog.
    profiles_synced_ms: DashMap<String, i64>,
    license_state: parking_lot::RwLock<Option<LicenseClaims>>,
    max_seen_ts_ms: AtomicI64,
    discovery_tx: mpsc::UnboundedSender<Vec<DiscoveredEventSchema>>,
    auth_cfg: parking_lot::RwLock<AuthConfig>,
    auth_state: auth::AuthState,
    /// Owner-only file that sign-up, reset and invite links go to (Pulse has no mailer).
    outbox_path: parking_lot::RwLock<Option<std::path::PathBuf>>,
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

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<DiscoveredEventSchema>>();
        let svc = Arc::new(Self {
            meta: meta.clone(),
            qengine,
            ingest: tokio::sync::RwLock::new(None),
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
            license_state: parking_lot::RwLock::new(None),
            max_seen_ts_ms: AtomicI64::new(0),
            discovery_tx: tx,
            auth_cfg: parking_lot::RwLock::new(AuthConfig::from_env()),
            auth_state: auth::AuthState::default(),
            outbox_path: parking_lot::RwLock::new(None),
        });

        let generated_ingest = svc
            .ensure_default_org_and_project(initial_ingest_key.trim())
            .await?;
        svc.bootstrap_creds.lock().generated_ingest_key = generated_ingest;
        svc.reload_caches_from_db().await?;
        svc.restore_license_and_byok().await?;

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
            self.known_projects
                .insert(proj.id.clone(), proj.org_id.clone());
            self.refresh_project_lexicon_cache(&proj.id).await?;
            self.refresh_project_virtual_catalog(&proj.id).await?;
        }
        Ok(())
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

    async fn authorize_and_check(
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

    async fn restore_license_and_byok(&self) -> Result<(), String> {
        let wall_now = Utc::now().timestamp_millis();
        let db_lic = self
            .meta
            .get_system_license()
            .await
            .map_err(|e| e.to_string())?;
        let mut max_seen = db_lic
            .as_ref()
            .map(|r| r.max_seen_ts_ms)
            .unwrap_or(0)
            .max(wall_now);
        self.max_seen_ts_ms.store(max_seen, Ordering::Relaxed);

        if let Ok(env_key) = std::env::var("PULSE_LICENSE_KEY") {
            let trimmed = env_key.trim();
            if !trimmed.is_empty() {
                let claims = verify_license_signature(trimmed)?;
                *self.license_state.write() = Some(claims.clone());
                max_seen = max_seen.max(Utc::now().timestamp_millis());
                self.max_seen_ts_ms.store(max_seen, Ordering::Relaxed);
                let _ = self
                    .meta
                    .upsert_system_license(&SystemLicenseRecord {
                        license_key: trimmed.to_string(),
                        customer_org: claims.customer_org,
                        edition: claims.edition.as_str().to_string(),
                        features: claims.features,
                        expires_at_ms: claims.expires_at_ms,
                        activated_at_ms: wall_now,
                        max_seen_ts_ms: max_seen,
                    })
                    .await;
            }
        }
        if self.license_state.read().is_none() {
            if let Some(rec) = db_lic {
                let claims = verify_license_signature(&rec.license_key).map_err(|e| {
                    format!("stored system_license failed Ed25519 verification: {e}")
                })?;
                *self.license_state.write() = Some(claims);
                let _ = self.meta.update_license_max_seen_ts(max_seen).await;
            }
        }

        // Open-Source encryption keyring is ALWAYS restored across restarts regardless of license
        // tier or expiration (`C3`/`C4`), failing closed if sealed secrets cannot decrypt.
        let keyring_rows = self
            .meta
            .list_system_keyring()
            .await
            .map_err(|e| e.to_string())?;
        for row in keyring_rows {
            let secret = unseal_secret(&self.vault_kek, &row.sealed_key_hex).map_err(|e| {
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
            self.qengine
                .storage()
                .register_byok_master_key(&row.key_id, master, row.is_active)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn current_license_with_health(&self) -> (Option<LicenseClaims>, LicenseHealth) {
        let guard = self.license_state.read();
        let Some(claims) = guard.as_ref() else {
            return (None, LicenseHealth::Expired);
        };
        let wall_now = Utc::now().timestamp_millis();
        let prev = self.max_seen_ts_ms.fetch_max(wall_now, Ordering::AcqRel);
        let max_seen = prev.max(wall_now);
        if wall_now > prev && (wall_now - prev) >= 60_000 {
            let meta = self.meta.clone();
            tokio::spawn(async move {
                let _ = meta.update_license_max_seen_ts(max_seen).await;
            });
        }
        let health = evaluate_license_health(claims, wall_now, max_seen);
        (Some(claims.clone()), health)
    }

    fn build_license_status(&self) -> LicenseStatusResponse {
        let enc = self.qengine.storage().byok_encryption_status();
        let (claims_opt, health) = self.current_license_with_health();
        match (claims_opt, health) {
            (Some(claims), LicenseHealth::Active) => LicenseStatusResponse {
                edition: claims.edition.as_str().to_string(),
                health: "active".to_string(),
                is_commercial: true,
                in_grace_period: false,
                customer_org: Some(claims.customer_org),
                license_id: Some(claims.license_id),
                expires_at_ms: Some(claims.expires_at_ms),
                grace_ends_at_ms: Some(
                    claims.expires_at_ms.saturating_add(LICENSE_GRACE_PERIOD_MS),
                ),
                features: claims.features,
                encryption: enc,
            },
            (Some(claims), LicenseHealth::GracePeriod) => LicenseStatusResponse {
                edition: claims.edition.as_str().to_string(),
                health: "grace_period".to_string(),
                is_commercial: true,
                in_grace_period: true,
                customer_org: Some(claims.customer_org),
                license_id: Some(claims.license_id),
                expires_at_ms: Some(claims.expires_at_ms),
                grace_ends_at_ms: Some(
                    claims.expires_at_ms.saturating_add(LICENSE_GRACE_PERIOD_MS),
                ),
                features: claims.features,
                encryption: enc,
            },
            (Some(claims), LicenseHealth::Expired) => LicenseStatusResponse {
                edition: EditionTier::Community.as_str().to_string(),
                health: "expired".to_string(),
                is_commercial: false,
                in_grace_period: false,
                customer_org: Some(claims.customer_org),
                license_id: Some(claims.license_id),
                expires_at_ms: Some(claims.expires_at_ms),
                grace_ends_at_ms: Some(
                    claims.expires_at_ms.saturating_add(LICENSE_GRACE_PERIOD_MS),
                ),
                features: Vec::new(),
                encryption: enc,
            },
            (None, _) => LicenseStatusResponse {
                edition: EditionTier::Community.as_str().to_string(),
                health: "community".to_string(),
                is_commercial: false,
                in_grace_period: false,
                customer_org: None,
                license_id: None,
                expires_at_ms: None,
                grace_ends_at_ms: None,
                features: Vec::new(),
                encryption: enc,
            },
        }
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

    fn require_commercial_feature(&self, feature: CommercialFeature) -> Result<(), CtrlError> {
        let (claims_opt, health) = self.current_license_with_health();
        match (claims_opt, health) {
            (Some(claims), LicenseHealth::Active | LicenseHealth::GracePeriod) => {
                if claims.features.iter().any(|f| f == feature.as_str()) {
                    Ok(())
                } else {
                    Err(CtrlError::payment_required(
                        feature.as_str(),
                        format!("License does not include feature '{}'", feature.as_str()),
                    ))
                }
            }
            (Some(_), LicenseHealth::Expired) => Err(CtrlError::payment_required(
                feature.as_str(),
                "Commercial license and 14-day grace period have expired.",
            )),
            (None, _) => Err(CtrlError::payment_required(
                feature.as_str(),
                "This capability requires a Commercial BYOK license key (PULSE_LICENSE_KEY or POST /v1/license).",
            )),
        }
    }

    async fn record_enterprise_audit(
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
    /// project, then only the users whose rows changed since the previous call.
    // ponytail: all profiles live in RAM (one map entry per user, as the live path already
    // grows to); move to an on-disk KV keyed by canonical uid if projects pass ~10M users.
    async fn refresh_user_profiles(&self, project_id: &str) -> Result<(), String> {
        let cat = self.qengine.catalogs().for_project(project_id);
        let started_ms = Utc::now().timestamp_millis();
        let since = self.profiles_synced_ms.get(project_id).map(|v| *v);
        match since {
            None => {
                let rows = self
                    .meta
                    .list_user_profiles_updated_since(project_id, 0)
                    .await
                    .map_err(|e| e.to_string())?;
                let merged = merge_profile_rows(rows)
                    .into_iter()
                    .map(|(uid, p)| (uid, profile_to_catalog_props(&p)))
                    .collect();
                cat.set_user_profiles(merged);
            }
            Some(since) => {
                let rows = self
                    .meta
                    .list_user_profiles_updated_since(project_id, since)
                    .await
                    .map_err(|e| e.to_string())?;
                let uids: std::collections::BTreeSet<u32> =
                    rows.iter().map(|r| r.canonical_uid).collect();
                for uid in uids {
                    self.rebuild_user_profile(project_id, uid).await?;
                }
            }
        }
        // Re-read a little overlap next time so a write that committed with an older
        // timestamp while this load ran is not missed; rebuilding a user is idempotent.
        self.profiles_synced_ms
            .insert(project_id.to_string(), started_ms - PROFILE_SYNC_OVERLAP_MS);
        Ok(())
    }

    /// Recomputes one canonical user's merged properties from all of their profile rows.
    async fn rebuild_user_profile(
        &self,
        project_id: &str,
        canonical_uid: u32,
    ) -> Result<(), String> {
        let rows = self
            .meta
            .list_user_profiles_for_canonical(project_id, canonical_uid)
            .await
            .map_err(|e| e.to_string())?;
        let cat = self.qengine.catalogs().for_project(project_id);
        match merge_profile_rows(rows).remove(&canonical_uid) {
            Some(p) => cat.upsert_user_profile(canonical_uid, profile_to_catalog_props(&p)),
            None => cat.remove_user_profile(canonical_uid),
        }
        Ok(())
    }

    pub async fn refresh_project_virtual_catalog(&self, project_id: &str) -> Result<(), String> {
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
        cat.saved_cohorts.clear();
        for c in cohorts {
            cat.saved_cohorts.insert(c.id.clone(), c.definition);
        }

        self.refresh_user_profiles(project_id).await?;

        let erased = self
            .meta
            .list_erased_uids(project_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut erased_bm = RoaringBitmap::new();
        for u in erased {
            erased_bm.insert(u);
        }
        cat.set_deleted_uids(erased_bm);

        Ok(())
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
            .identity_router()
            .get_or_open(project_id)
            .await
            .map_err(|e| CtrlError::internal(e.to_string()))?;

        let existing_prof = self
            .meta
            .get_user_profile(project_id, distinct_id)
            .await
            .map_err(CtrlError::meta)?;

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

        let _ = self
            .meta
            .delete_user_profiles_for_ids(project_id, &all_ids)
            .await
            .map_err(CtrlError::meta)?;

        let uid_vec: Vec<u32> = uids_bm.iter().collect();
        self.meta
            .insert_erased_uids(project_id, &uid_vec)
            .await
            .map_err(CtrlError::meta)?;

        let cat = self.qengine.catalogs().for_project(project_id);
        cat.mark_deleted_uids(&uids_bm);
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

/// How far back each incremental profile reload re-reads, to cover writes that were in flight.
const PROFILE_SYNC_OVERLAP_MS: i64 = 60_000;

/// Merges profile rows (oldest first) into one record per canonical user: for each property
/// the most recently written value wins, and revenue adds up across the user's ids.
fn merge_profile_rows(
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
    ) {
        let existing = self
            .meta
            .get_user_profile(project_id, distinct_id)
            .await
            .ok()
            .flatten();
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
            return;
        }
        if !out.modified && existing.is_some() {
            return;
        }

        let rec = UserProfileRecord {
            project_id: project_id.to_string(),
            distinct_id: distinct_id.to_string(),
            canonical_uid,
            props: props.clone(),
            total_revenue,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        if let Err(e) = self.meta.upsert_user_profile(&rec).await {
            tracing::error!(project_id, error = %e, "failed to store user profile");
            return;
        }
        if let Err(e) = self.rebuild_user_profile(project_id, canonical_uid).await {
            tracing::error!(project_id, error = %e, "failed to refresh user profile in catalog");
        }
    }

    async fn on_identify(
        &self,
        project_id: &str,
        anon_id: &str,
        user_id: &str,
        canonical_uid: u32,
    ) {
        // Rows written before the merge carry the pre-merge uid; point both ids at the
        // canonical user so the anonymous traits show on the identified user.
        if let Err(e) = self
            .meta
            .rekey_user_profiles(project_id, &[anon_id, user_id], canonical_uid)
            .await
        {
            tracing::error!(project_id, error = %e, "failed to re-key profiles after identify");
            return;
        }
        if let Err(e) = self.rebuild_user_profile(project_id, canonical_uid).await {
            tracing::error!(project_id, error = %e, "failed to refresh user profile in catalog");
        }
    }

    async fn apply_group_engage(
        &self,
        project_id: &str,
        group_type: &str,
        group_id: &str,
        ops: &Value,
    ) {
        let existing = self
            .meta
            .get_group_profile(project_id, group_type, group_id)
            .await
            .ok()
            .flatten();
        let mut props = existing
            .as_ref()
            .map(|g| g.props.clone())
            .unwrap_or_default();
        let mut rev = 0.0;
        let out = apply_profile_ops(&mut props, &mut rev, ops);
        if !out.modified && existing.is_some() {
            return;
        }
        let rec = GroupProfileRecord {
            project_id: project_id.to_string(),
            group_type: group_type.to_string(),
            group_id: group_id.to_string(),
            props,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        let _ = self.meta.upsert_group_profile(&rec).await;
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
    let session_user = if token.starts_with("ps_") {
        ctrl.meta
            .get_session_user(&sha256_hex(token), Utc::now().timestamp_millis())
            .await
            .map_err(CtrlError::meta)?
    } else {
        None
    };
    let now = Utc::now().timestamp_millis();
    let principal = match session_user {
        Some(su) => {
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
        None => ctrl.verify_admin_token(token)?,
    };
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
        // Open-Source Encryption at Rest + Commercial BYOK License, Audit, Vault, Export & Copilot
        .route("/v1/system/encryption", get(handle_get_encryption))
        .route(
            "/v1/system/encryption/rotate",
            post(handle_rotate_encryption),
        )
        .route(
            "/v1/license",
            get(handle_get_license)
                .post(handle_activate_license)
                .delete(handle_deactivate_license),
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
    fn bad_gateway(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_GATEWAY, msg.into())
    }
    fn payment_required(feature: &str, msg: impl Into<String>) -> Self {
        Self(
            StatusCode::PAYMENT_REQUIRED,
            format!("commercial_license_required ({feature}): {}", msg.into()),
        )
    }
    fn meta(err: MetaError) -> Self {
        match err {
            MetaError::Conflict(msg) => Self::conflict(msg),
            other => Self::internal(other),
        }
    }
    fn exec(err: pulse_exec::ExecError) -> Self {
        match err {
            pulse_exec::ExecError::QueryTimeout(ms) => Self(
                StatusCode::GATEWAY_TIMEOUT,
                format!("query exceeded deadline ({ms} ms)"),
            ),
            e @ pulse_exec::ExecError::QueryTooLarge { .. } => {
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
    let pid = ctrl
        .resolve_and_check(&principal, q.get("project_id").map(|s| s.as_str()))
        .await?;

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
    if !matches!(
        principal,
        AuthPrincipal::RootAdmin | AuthPrincipal::User { .. }
    ) {
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
        let timezone = match timezone
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
        {
            Some(tz) => {
                if tz.parse::<chrono_tz::Tz>().is_err() {
                    return Err(CtrlError::bad(format!(
                        "unknown time zone '{tz}': use an IANA name such as 'UTC', 'America/New_York' or 'Asia/Kolkata'"
                    )));
                }
                Some(tz)
            }
            None => None,
        };
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
    let res = ctrl
        .qengine
        .query_funnel(&id, &q)
        .await
        .map_err(CtrlError::exec)?;
    Ok(Json(res))
}

async fn handle_query_retention(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> ApiRes<pulse_exec::RetentionResult> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let q: RetentionQuery = parse_json_body("retention query", &body)?;
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
) -> std::result::Result<Response, CtrlError> {
    let header_pid = headers.get("x-pulse-project").and_then(|v| v.to_str().ok());
    let bound_pid = ctrl.resolve_and_check(&principal, header_pid).await?;
    // JSON-RPC notifications (no `id`, e.g. `notifications/initialized`) get no response body;
    // MCP Streamable HTTP requires `202 Accepted`.
    if req.id.is_null() {
        return Ok(StatusCode::ACCEPTED.into_response());
    }
    let resp = handle_mcp_request(req, &bound_pid, &ctrl.meta, &ctrl.qengine).await;
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
    let profiles = ctrl
        .meta
        .list_user_profiles(&id, q.get("search").map(String::as_str), limit, offset)
        .await
        .map_err(CtrlError::meta)?;

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

async fn handle_get_user_detail(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    AxumPath((id, distinct_id)): AxumPath<(String, String)>,
) -> ApiRes<Value> {
    ctrl.authorize_and_check(&principal, &id).await?;
    let id_graph = ctrl
        .qengine
        .storage()
        .identity_router()
        .get_or_open(&id)
        .await
        .map_err(|e| CtrlError::internal(e.to_string()))?;

    let profile = ctrl
        .meta
        .get_user_profile(&id, &distinct_id)
        .await
        .map_err(CtrlError::meta)?;

    let canon_uid = id_graph
        .lookup_canonical(&distinct_id)
        .or_else(|| id_graph.lookup_raw(&distinct_id))
        .or_else(|| profile.as_ref().map(|p| p.canonical_uid))
        .unwrap_or(0);

    let events = if canon_uid > 0 || id_graph.lookup_raw(&distinct_id).is_some() {
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
    let batch = if ctype.contains("csv")
        || (first_non_ws != Some(b'{') && first_non_ws != Some(b'['))
    {
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
            WireBatch { events, rejected }
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
            WireBatch { events, rejected }
        }
    } else {
        WireBatch::decode_json(&body).map_err(|e| CtrlError::bad(e.to_string()))?
    };

    if batch.events.is_empty() {
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
    let res = ingest
        .ingest_for_project(&id, batch, now_ms)
        .await
        .map_err(|e| CtrlError::bad(e.to_string()))?;
    let _ = ctrl.qengine.storage().flush_all_projects().await;

    Ok(Json(json!({
        "status": res.status,
        "accepted": res.accepted,
        "deduped": res.deduped,
        "dropped": res.dropped,
        "server_ts_ms": res.server_ts_ms,
        "rejected": res.rejected,
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
    let members = ctrl
        .qengine
        .evaluate_saved_cohort_uids(&id, &cohort_id)
        .await
        .map_err(CtrlError::exec)?;
    let profiles = ctrl
        .meta
        .list_user_profiles_updated_since(&id, 0)
        .await
        .map_err(CtrlError::meta)?;
    let by_uid = merge_profile_rows(profiles);

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
    let (total_profiles, paying_users, total_revenue) = ctrl
        .meta
        .user_revenue_summary(&id)
        .await
        .map_err(CtrlError::meta)?;

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

const ALERT_OPERATORS: [&str; 4] = ["gt", "gte", "lt", "lte"];
/// An alert looks at the last 24 hours and, while its condition holds, fires at most once per window.
const ALERT_WINDOW_MS: i64 = 86_400_000;

fn alert_condition_met(operator: &str, value: f64, threshold: f64) -> bool {
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
    // ponytail: every query-serving node runs this, so several query nodes each deliver; add a
    // leader lease if you run more than one.
    pub async fn evaluate_alerts(&self) {
        let now_ms = Utc::now().timestamp_millis();
        let Ok(projects) = self.meta.list_projects().await else {
            return;
        };
        for proj in projects {
            let Ok(rules) = self.meta.list_alert_rules(&proj.id).await else {
                continue;
            };
            for mut a in rules {
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
                if let Err(e) = self.meta.upsert_alert_rule(&a).await {
                    tracing::warn!(alert_id = %a.id, "couldn't record alert trigger: {e}");
                    continue;
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
    /// that credential's secret. Needs the `webhook_alerts` licence feature and a webhook credential;
    /// returns `Ok(None)` when either is missing.
    async fn deliver_alert(
        &self,
        a: &AlertRule,
        value: f64,
        now_ms: i64,
    ) -> Result<Option<reqwest::StatusCode>, String> {
        if self
            .require_commercial_feature(CommercialFeature::WebhookAlerts)
            .is_err()
        {
            return Ok(None);
        }
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
async fn post_signed_webhook(
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

// ======================= OPEN-SOURCE ENCRYPTION, COMMERCIAL LICENSE, BYOK & COPILOT =======================

fn byok_feature_for_category(category: &str) -> Result<CommercialFeature, CtrlError> {
    match category {
        "llm" => Ok(CommercialFeature::ByokAiCopilot),
        "storage" => Ok(CommercialFeature::WarehouseExport),
        "webhook" => Ok(CommercialFeature::WebhookAlerts),
        _ => Err(CtrlError::bad(
            "category must be one of: llm, storage, webhook",
        )),
    }
}

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

fn webhook_signing_secret(cfg: &Value) -> Option<&str> {
    cfg.get("signing_secret")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| s.len() >= 16)
}

/// `sha256=<hex HMAC of "{timestamp}.{body}">`; receivers reject stale timestamps to stop replays.
fn sign_webhook(signing_secret: &str, ts: &str, body: &[u8]) -> String {
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

async fn handle_get_license(
    State(ctrl): State<Arc<ControlPlaneService>>,
) -> ApiRes<LicenseStatusResponse> {
    Ok(Json(ctrl.build_license_status()))
}

#[derive(Deserialize)]
struct ActivateLicenseReq {
    #[serde(default)]
    license_key: Option<String>,
    #[serde(default)]
    trial: bool,
}

async fn handle_activate_license(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
    Json(req): Json<ActivateLicenseReq>,
) -> ApiRes<LicenseStatusResponse> {
    let actor = principal.require_cluster_admin()?;
    if req.trial {
        return Err(CtrlError::bad(
            "self-serve trial key minting is disabled; supply an Ed25519-signed license_key",
        ));
    }
    let key_str = req
        .license_key
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CtrlError::bad("license_key is required"))?;

    let claims = verify_license_signature(&key_str).map_err(CtrlError::bad)?;
    let now_ms = Utc::now().timestamp_millis();
    let max_seen = ctrl
        .max_seen_ts_ms
        .fetch_max(now_ms, Ordering::AcqRel)
        .max(now_ms);
    if matches!(
        evaluate_license_health(&claims, now_ms, max_seen),
        LicenseHealth::Expired
    ) {
        return Err(CtrlError::bad(
            "license key and 14-day grace period have expired",
        ));
    }

    ctrl.meta
        .upsert_system_license(&SystemLicenseRecord {
            license_key: key_str,
            customer_org: claims.customer_org.clone(),
            edition: claims.edition.as_str().to_string(),
            features: claims.features.clone(),
            expires_at_ms: claims.expires_at_ms,
            activated_at_ms: now_ms,
            max_seen_ts_ms: max_seen,
        })
        .await
        .map_err(CtrlError::meta)?;

    *ctrl.license_state.write() = Some(claims.clone());
    ctrl.record_enterprise_audit(
        &actor,
        "license.activate",
        "system_license",
        json!({ "license_id": claims.license_id, "customer_org": claims.customer_org }),
    )
    .await;

    Ok(Json(ctrl.build_license_status()))
}

async fn handle_deactivate_license(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiRes<LicenseStatusResponse> {
    let actor = principal.require_cluster_admin()?;
    ctrl.meta
        .delete_system_license()
        .await
        .map_err(CtrlError::meta)?;
    *ctrl.license_state.write() = None;

    // Preserves cluster encryption keyring so existing encrypted objects stay readable (`C3`).
    ctrl.record_enterprise_audit(&actor, "license.deactivate", "system_license", json!({}))
        .await;

    Ok(Json(ctrl.build_license_status()))
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
    ctrl.require_commercial_feature(CommercialFeature::EnterpriseAuditLog)?;
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
    let feature = byok_feature_for_category(&category)?;
    ctrl.require_commercial_feature(feature)?;

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
    let feature = byok_feature_for_category(&cat)?;
    ctrl.require_commercial_feature(feature)?;

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
    ctrl.require_commercial_feature(CommercialFeature::WarehouseExport)?;
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
    ctrl.require_commercial_feature(CommercialFeature::ByokAiCopilot)?;

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
