use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, put};
use axum::{Json, Router};
use chrono::Utc;
use dashmap::DashMap;
use pulse_exec::{
    validate_cohort_acyclic, BehavioralCohortDef, CustomEventDef, CustomPropertyDef, FlowsQuery,
    FunnelQuery, InsightsQuery, LookupTableDef, QueryEngine, RetentionQuery, SessionSearchQuery,
    TimeRange,
};
use pulse_ingest::{DiscoveredEventSchema, GovernanceProvider, IngestService, ProjectLexiconRules};
use pulse_wire::{
    InteractionKind, NodeKind, PropValue, ReplayChunk, WireBatch, WireEvent, WireNode, WireframeOp,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::db::MetaStore;
use crate::experiments::{build_edge_flag_bundle, evaluate_experiment_results};
use crate::mcp::{handle_mcp_request, JsonRpcRequest};
use crate::models::{
    ApiKeyRecord, CohortRecord, CustomSchemaBundle, Dashboard, Experiment, ExperimentMetricSpec,
    FeatureFlag, LexiconEvent, LexiconProperty, Organization, Project, SavedReport,
    VariantAllocation,
};

pub struct ControlPlaneService {
    meta: MetaStore,
    qengine: Arc<QueryEngine>,
    ingest: tokio::sync::RwLock<Option<Arc<IngestService>>>,
    default_project_id: String,
    default_api_key: String,
    api_key_cache: DashMap<String, String>,
    lexicon_cache: DashMap<String, Arc<ProjectLexiconRules>>,
    discovery_tx: mpsc::UnboundedSender<Vec<DiscoveredEventSchema>>,
}

impl ControlPlaneService {
    pub async fn new(
        meta: MetaStore,
        qengine: Arc<QueryEngine>,
        default_project_id: impl Into<String>,
        default_api_key: impl Into<String>,
    ) -> Result<Arc<Self>, String> {
        let default_project_id = default_project_id.into();
        let default_api_key = default_api_key.into();

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<DiscoveredEventSchema>>();
        let svc = Arc::new(Self {
            meta: meta.clone(),
            qengine,
            ingest: tokio::sync::RwLock::new(None),
            default_project_id,
            default_api_key,
            api_key_cache: DashMap::new(),
            lexicon_cache: DashMap::new(),
            discovery_tx: tx,
        });

        svc.ensure_default_org_and_project().await?;
        svc.reload_caches_from_db().await?;

        // Background schema discovery persistence loop
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

    pub fn default_api_key(&self) -> &str {
        &self.default_api_key
    }

    async fn ensure_default_org_and_project(&self) -> Result<(), String> {
        let now = Utc::now().timestamp_millis();
        self.meta
            .upsert_org(&Organization {
                id: "org_pulse_india".to_string(),
                name: "Pulse Fintech & Quick-Commerce".to_string(),
                slug: "pulse-india".to_string(),
                created_at_ms: now,
            })
            .await
            .map_err(|e| e.to_string())?;

        self.meta
            .upsert_project(&Project {
                id: self.default_project_id.clone(),
                org_id: "org_pulse_india".to_string(),
                name: "Pulse SuperApp (IN)".to_string(),
                timezone: "Asia/Kolkata".to_string(),
                created_at_ms: now,
            })
            .await
            .map_err(|e| e.to_string())?;

        self.meta
            .upsert_api_key(&ApiKeyRecord {
                key_secret: self.default_api_key.clone(),
                project_id: self.default_project_id.clone(),
                label: "Default Live Mobile/Web Key".to_string(),
                role: "ingest_and_flags".to_string(),
                created_at_ms: now,
                revoked: false,
            })
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
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
                self.api_key_cache.insert(k.key_secret, k.project_id);
            }
        }

        let projects = self.meta.list_projects().await.map_err(|e| e.to_string())?;
        for proj in projects {
            self.refresh_project_lexicon_cache(&proj.id).await?;
            self.refresh_project_virtual_catalog(&proj.id).await?;
        }
        Ok(())
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
        self.api_key_cache.get(api_key).map(|v| v.clone())
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

/// Builds the full Control Plane + Query + Replay + MCP Axum Router.
pub fn build_control_router(ctrl: Arc<ControlPlaneService>) -> Router {
    Router::new()
        .route("/v1/status", get(handle_status))
        .route("/v1/admin/flush", post(handle_admin_flush))
        .route("/v1/admin/simulate-burst", post(handle_simulate_burst))
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
        // Cohorts & Schema Extensions (Lookup Tables, Custom Events, Custom Properties)
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
        .route("/v1/flags/:project_api_key", get(handle_edge_flag_bundle))
        .route(
            "/v1/projects/:id/experiments",
            get(handle_list_experiments).post(handle_create_experiment),
        )
        .route(
            "/v1/projects/:id/experiments/:exp_id/results",
            get(handle_experiment_results_scoped),
        )
        .route(
            "/v1/experiments/:exp_id/results",
            get(handle_experiment_results_global),
        )
        // Model Context Protocol (MCP) JSON-RPC 2.0
        .route("/v1/mcp", post(handle_mcp))
        .with_state(ctrl)
}

type ApiRes<T> = std::result::Result<Json<T>, CtrlError>;

struct CtrlError(StatusCode, String);

impl CtrlError {
    fn bad(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
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
    Query(q): Query<HashMap<String, String>>,
) -> ApiRes<Value> {
    let pid = q
        .get("project_id")
        .cloned()
        .unwrap_or_else(|| ctrl.default_project_id.clone());
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

    let projects = ctrl
        .meta
        .list_projects()
        .await
        .map_err(CtrlError::internal)?;

    Ok(Json(json!({
        "status": "ok",
        "default_project_id": ctrl.default_project_id,
        "default_api_key": ctrl.default_api_key,
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

async fn handle_admin_flush(State(ctrl): State<Arc<ControlPlaneService>>) -> ApiRes<Value> {
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

#[derive(Deserialize, Default)]
struct BurstReq {
    project_id: Option<String>,
    count: Option<usize>,
}

async fn handle_simulate_burst(
    State(ctrl): State<Arc<ControlPlaneService>>,
    body: Option<Json<BurstReq>>,
) -> ApiRes<Value> {
    let req = body.map(|b| b.0).unwrap_or_default();
    let pid = req
        .project_id
        .unwrap_or_else(|| ctrl.default_project_id.clone());
    let count = req.count.unwrap_or(1_200).clamp(50, 25_000);

    let Some(ingest) = ctrl.ingest.read().await.clone() else {
        return Err(CtrlError::bad("ingest service not attached"));
    };

    let now_ms = Utc::now().timestamp_millis();
    let steps = [
        "App_Open",
        "KYC_PAN_Submitted",
        "Video_KYC_Completed",
        "UPI_Autopay_Mandate_Approved",
        "First_Disbursal_Success",
    ];
    let cities = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Pune"];
    let mut events = Vec::with_capacity(count + count / 5);

    let burst_tag = Uuid::new_v4().simple().to_string();
    let rage_session_id = format!("sess_burst_rage_{}", &burst_tag[..8]);

    // Emit a live frustrated Wireframe Replay chunk with masked PAN + Rage Taps
    let replay_chunk = ReplayChunk {
        project_id: pid.clone(),
        session_id: rage_session_id.clone(),
        chunk_start_ts_ms: now_ms - 5_000,
        ops: vec![
            WireframeOp::FullSnapshot {
                width: 390,
                height: 844,
                nodes: vec![
                    WireNode {
                        node_id: 1,
                        parent_id: 0,
                        kind: NodeKind::Screen,
                        x: 0,
                        y: 0,
                        w: 390,
                        h: 844,
                        bg_rgba: 0x090D_16FF,
                        masked_label: "Instant Credit Line KYC".to_string(),
                    },
                    WireNode {
                        node_id: 2,
                        parent_id: 1,
                        kind: NodeKind::MaskedInput,
                        x: 24,
                        y: 190,
                        w: 342,
                        h: 52,
                        bg_rgba: 0x1620_33FF,
                        masked_label: "PAN: ██████████".to_string(),
                    },
                    WireNode {
                        node_id: 3,
                        parent_id: 1,
                        kind: NodeKind::Button,
                        x: 24,
                        y: 268,
                        w: 342,
                        h: 52,
                        bg_rgba: 0x3B82_F6FF,
                        masked_label: "Verify PAN".to_string(),
                    },
                    WireNode {
                        node_id: 4,
                        parent_id: 1,
                        kind: NodeKind::ErrorBanner,
                        x: 24,
                        y: 340,
                        w: 342,
                        h: 48,
                        bg_rgba: 0xEF44_44FF,
                        masked_label: "NSDL Gateway 504 Timeout".to_string(),
                    },
                ],
            },
            WireframeOp::PointerInteraction {
                ts_offset_ms: 1_100,
                x: 195,
                y: 294,
                kind: InteractionKind::RageTap { count: 4 },
            },
            WireframeOp::NetworkBreadcrumb {
                ts_offset_ms: 1_450,
                method: "POST".to_string(),
                path: "/v2/kyc/nsdl-pan-verify".to_string(),
                status_code: 504,
                duration_ms: 5_020,
            },
        ],
    };
    let _ = ctrl
        .qengine
        .storage()
        .append_replay_chunk(&replay_chunk)
        .await;

    for i in 0..count {
        let u = i % 250;
        let distinct_id = format!("+9198801{u:05}");
        let session_id = if i < 6 {
            rage_session_id.clone()
        } else {
            format!("sess_burst_{}_{u:04}", &burst_tag[..6])
        };
        let ev_name = if i == 3 {
            "NSDL_Gateway_504_Timeout"
        } else {
            steps[i % steps.len()]
        };
        let ts = if i % 10 == 0 {
            // 10% simulated 3-hour offline metro delay
            now_ms - 3 * 3_600_000 + (i as i64) * 10
        } else {
            now_ms - 30_000 + (i as i64) * 20
        };

        events.push(WireEvent {
            event_uuid: WireEvent::deterministic_uuid(
                &distinct_id,
                &session_id,
                ev_name,
                ts,
                (now_ms as u64).wrapping_add(i as u64),
            ),
            routing_seed: format!("dev_burst_{u:05}"),
            distinct_id,
            session_id,
            event_name: ev_name.to_string(),
            client_event_ts_ms: ts,
            client_sent_ts_ms: now_ms,
            props: vec![
                (
                    "city".to_string(),
                    PropValue::Str(cities[i % cities.len()].to_string()),
                ),
                (
                    "platform".to_string(),
                    PropValue::Str(if i % 3 == 0 { "iOS" } else { "Android" }.into()),
                ),
                (
                    "amount".to_string(),
                    PropValue::Int(500 + ((i % 25) as i64) * 250),
                ),
                (
                    "payment_method".to_string(),
                    PropValue::Str(if i % 4 == 0 { "card" } else { "upi" }.into()),
                ),
            ],
            replay_offset_kb: 0,
        });
    }

    // Add 15% simulated flaky 4G duplicate retries
    let dup_n = count * 15 / 100;
    for idx in 0..dup_n {
        events.push(events[idx].clone());
    }

    let resp = ingest
        .ingest_for_project(&pid, WireBatch::new(events), now_ms)
        .await
        .map_err(CtrlError::internal)?;

    Ok(Json(json!({
        "status": "ok",
        "accepted": resp.accepted,
        "deduped": resp.deduped,
        "dropped": resp.dropped,
        "frustrated_session_id": rage_session_id
    })))
}

// ------------------- Projects & Keys -------------------

async fn handle_list_projects(
    State(ctrl): State<Arc<ControlPlaneService>>,
) -> ApiRes<Vec<Project>> {
    let list = ctrl
        .meta
        .list_projects()
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(list))
}

#[derive(Deserialize)]
struct CreateProjectReq {
    id: Option<String>,
    name: String,
    timezone: Option<String>,
}

async fn handle_create_project(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Json(req): Json<CreateProjectReq>,
) -> ApiRes<Value> {
    let now = Utc::now().timestamp_millis();
    let id = req
        .id
        .unwrap_or_else(|| format!("proj_{}", &Uuid::new_v4().simple().to_string()[..8]));
    let proj = Project {
        id: id.clone(),
        org_id: "org_pulse_india".to_string(),
        name: req.name,
        timezone: req.timezone.unwrap_or_else(|| "Asia/Kolkata".to_string()),
        created_at_ms: now,
    };
    ctrl.meta
        .upsert_project(&proj)
        .await
        .map_err(CtrlError::internal)?;
    let key = ApiKeyRecord {
        key_secret: format!("pk_live_{}", &Uuid::new_v4().simple().to_string()[..16]),
        project_id: id,
        label: "Primary Key".to_string(),
        role: "ingest_and_flags".to_string(),
        created_at_ms: now,
        revoked: false,
    };
    ctrl.meta
        .upsert_api_key(&key)
        .await
        .map_err(CtrlError::internal)?;
    ctrl.reload_caches_from_db()
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(json!({ "project": proj, "api_key": key })))
}

async fn handle_list_keys(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<ApiKeyRecord>> {
    let keys = ctrl
        .meta
        .list_api_keys(Some(&id))
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(keys))
}

#[derive(Deserialize)]
struct CreateKeyReq {
    label: String,
}

async fn handle_create_key(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateKeyReq>,
) -> ApiRes<ApiKeyRecord> {
    let key = ApiKeyRecord {
        key_secret: format!("pk_live_{}", &Uuid::new_v4().simple().to_string()[..18]),
        project_id: id,
        label: req.label,
        role: "ingest_and_flags".to_string(),
        created_at_ms: Utc::now().timestamp_millis(),
        revoked: false,
    };
    ctrl.meta
        .upsert_api_key(&key)
        .await
        .map_err(CtrlError::internal)?;
    ctrl.api_key_cache
        .insert(key.key_secret.clone(), key.project_id.clone());
    Ok(Json(key))
}

// ------------------- Queries -------------------

async fn handle_unified_query(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Json(payload): Json<Value>,
) -> ApiRes<Value> {
    let pid = payload
        .get("project_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(&ctrl.default_project_id);
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
                .query_insights(pid, &q)
                .await
                .map_err(CtrlError::internal)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "funnel" | "funnels" => {
            let q: FunnelQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_funnel(pid, &q)
                .await
                .map_err(CtrlError::internal)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "retention" => {
            let q: RetentionQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_retention(pid, &q)
                .await
                .map_err(CtrlError::internal)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        "flows" | "flow" => {
            let q: FlowsQuery =
                serde_json::from_value(inner).map_err(|e| CtrlError::bad(e.to_string()))?;
            let res = ctrl
                .qengine
                .query_flows(pid, &q)
                .await
                .map_err(CtrlError::internal)?;
            Ok(Json(serde_json::to_value(res).unwrap()))
        }
        other => Err(CtrlError::bad(format!("unknown query kind: {other}"))),
    }
}

async fn handle_query_insights(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<InsightsQuery>,
) -> ApiRes<pulse_exec::InsightsResult> {
    let res = ctrl
        .qengine
        .query_insights(&id, &q)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_query_funnel(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<FunnelQuery>,
) -> ApiRes<pulse_exec::FunnelResult> {
    let res = ctrl
        .qengine
        .query_funnel(&id, &q)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_query_retention(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<RetentionQuery>,
) -> ApiRes<pulse_exec::RetentionResult> {
    let res = ctrl
        .qengine
        .query_retention(&id, &q)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_query_flows(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Json(q): Json<FlowsQuery>,
) -> ApiRes<pulse_exec::FlowsResult> {
    let res = ctrl
        .qengine
        .query_flows(&id, &q)
        .await
        .map_err(CtrlError::internal)?;
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
    Query(params): Query<HashMap<String, String>>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let pid = params
        .get("project_id")
        .cloned()
        .unwrap_or_else(|| ctrl.default_project_id.clone());
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
    Json(sq): Json<SessionSearchQuery>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let res = ctrl
        .qengine
        .search_sessions(&ctrl.default_project_id, &sq)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_get_sessions(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
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
    AxumPath(id): AxumPath<String>,
    Json(sq): Json<SessionSearchQuery>,
) -> ApiRes<pulse_exec::SessionSearchResult> {
    let res = ctrl
        .qengine
        .search_sessions(&id, &sq)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(res))
}

async fn handle_get_replay_default(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> std::result::Result<Response, CtrlError> {
    let pid = params
        .get("project_id")
        .cloned()
        .unwrap_or_else(|| ctrl.default_project_id.clone());
    serve_replay(&ctrl, &pid, &session_id, &headers, &params).await
}

async fn handle_get_replay(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath((id, session_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> std::result::Result<Response, CtrlError> {
    serve_replay(&ctrl, &id, &session_id, &headers, &params).await
}

async fn serve_replay(
    ctrl: &ControlPlaneService,
    project_id: &str,
    session_id: &str,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> std::result::Result<Response, CtrlError> {
    let rstore = ctrl.qengine.storage().replay_store();

    // 1. Support standard HTTP `Range: bytes=start-end` byte-range streaming (`206 Partial Content`)
    if let Some(range_hdr) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(spec) = range_hdr.strip_prefix("bytes=") {
            let (start_s, end_s) = spec.split_once('-').unwrap_or((spec, ""));
            let start: u64 = start_s.parse().unwrap_or(0);
            let max_len = end_s
                .parse::<u64>()
                .ok()
                .map(|end| end.saturating_sub(start) + 1);
            let Some(bytes) = rstore
                .read_bytes_from(project_id, session_id, start, max_len)
                .await
                .map_err(CtrlError::internal)?
            else {
                return Err(CtrlError::not_found("replay session not found"));
            };
            let end_actual = start + (bytes.len() as u64).saturating_sub(1);
            return Ok((
                StatusCode::PARTIAL_CONTENT,
                [
                    (
                        header::CONTENT_TYPE,
                        "application/x-pulse-frame".to_string(),
                    ),
                    (
                        header::CONTENT_RANGE,
                        format!("bytes {start}-{end_actual}/*"),
                    ),
                ],
                bytes,
            )
                .into_response());
        }
    }

    let offset_kb: u64 = params
        .get("offset_kb")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let byte_offset = params
        .get("byte_offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(offset_kb * 1024);

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

    // If jumping to a non-zero offset and chunk[0] lacks `FullSnapshot`, prepend initial `FullSnapshot` from offset 0
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
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Value> {
    let events = ctrl
        .meta
        .list_lexicon_events(&id)
        .await
        .map_err(CtrlError::internal)?;
    let properties = ctrl
        .meta
        .list_lexicon_properties(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath((id, name)): AxumPath<(String, String)>,
    Json(req): Json<UpdateLexiconEventReq>,
) -> ApiRes<LexiconEvent> {
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
        .map_err(CtrlError::internal)?;
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
    AxumPath((id, name)): AxumPath<(String, String)>,
    Json(req): Json<UpdateLexiconPropReq>,
) -> ApiRes<LexiconProperty> {
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
        .map_err(CtrlError::internal)?;
    ctrl.refresh_project_lexicon_cache(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(prop))
}

// ------------------- Cohorts & Virtual Schema Extensions -------------------

async fn handle_list_cohorts(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<CohortRecord>> {
    let list = ctrl
        .meta
        .list_cohorts(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateCohortReq>,
) -> ApiRes<CohortRecord> {
    let cohort_id = req
        .id
        .unwrap_or_else(|| format!("coh_{}", &Uuid::new_v4().simple().to_string()[..8]));
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
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<PreviewCohortReq>,
) -> ApiRes<Value> {
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
    AxumPath(id): AxumPath<String>,
) -> ApiRes<CustomSchemaBundle> {
    let custom_events = ctrl
        .meta
        .list_custom_events(&id)
        .await
        .map_err(CtrlError::internal)?;
    let custom_properties = ctrl
        .meta
        .list_custom_properties(&id)
        .await
        .map_err(CtrlError::internal)?;
    let lookup_tables = ctrl
        .meta
        .list_lookup_tables(&id)
        .await
        .map_err(CtrlError::internal)?
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
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<crate::models::LookupTableSummary>> {
    let list = ctrl
        .meta
        .list_lookup_tables(&id)
        .await
        .map_err(CtrlError::internal)?
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UploadLookupReq>,
) -> ApiRes<Value> {
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
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(def): Json<CustomEventDef>,
) -> ApiRes<CustomEventDef> {
    let ce_id = format!("ce_{}_{}", id, def.name);
    ctrl.meta
        .upsert_custom_event(&ce_id, &id, &def, Utc::now().timestamp_millis())
        .await
        .map_err(CtrlError::internal)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(def))
}

async fn handle_create_custom_property(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
    Json(def): Json<CustomPropertyDef>,
) -> ApiRes<CustomPropertyDef> {
    let cp_id = format!("cp_{}_{}", id, def.name);
    ctrl.meta
        .upsert_custom_property(&cp_id, &id, &def, Utc::now().timestamp_millis())
        .await
        .map_err(CtrlError::internal)?;
    ctrl.refresh_project_virtual_catalog(&id)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(def))
}

// ------------------- Saved Reports & Dashboards -------------------

async fn handle_list_reports(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<SavedReport>> {
    let list = ctrl
        .meta
        .list_saved_reports(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<SaveReportReq>,
) -> ApiRes<SavedReport> {
    let rep = SavedReport {
        id: req
            .id
            .unwrap_or_else(|| format!("rep_{}", &Uuid::new_v4().simple().to_string()[..8])),
        project_id: id,
        name: req.name,
        report_kind: req.report_kind,
        query_json: req.query_json,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_saved_report(&rep)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(rep))
}

async fn handle_list_dashboards(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<Dashboard>> {
    let list = ctrl
        .meta
        .list_dashboards(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<SaveDashboardReq>,
) -> ApiRes<Dashboard> {
    let dash = Dashboard {
        id: req
            .id
            .unwrap_or_else(|| format!("dash_{}", &Uuid::new_v4().simple().to_string()[..8])),
        project_id: id,
        name: req.name,
        report_ids: req.report_ids,
        created_at_ms: Utc::now().timestamp_millis(),
    };
    ctrl.meta
        .upsert_dashboard(&dash)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(dash))
}

// ------------------- Feature Flags & Experiments -------------------

async fn handle_list_flags(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<FeatureFlag>> {
    let list = ctrl
        .meta
        .list_feature_flags(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<UpsertFlagReq>,
) -> ApiRes<FeatureFlag> {
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
        req.variants
    };
    let flag = FeatureFlag {
        id: req.id.unwrap_or_else(|| format!("flg_{}_{}", id, req.key)),
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
        .map_err(CtrlError::internal)?;
    Ok(Json(flag))
}

#[derive(Deserialize)]
struct PatchFlagReq {
    enabled: Option<bool>,
    rollout_pct: Option<f64>,
}

async fn handle_patch_flag(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath((id, flag_id)): AxumPath<(String, String)>,
    Json(req): Json<PatchFlagReq>,
) -> ApiRes<FeatureFlag> {
    let flags = ctrl
        .meta
        .list_feature_flags(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    found.updated_at_ms = Utc::now().timestamp_millis();
    ctrl.meta
        .upsert_feature_flag(&found)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(found))
}

async fn handle_edge_flag_bundle(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(api_key): AxumPath<String>,
    headers: HeaderMap,
) -> std::result::Result<Response, CtrlError> {
    let Some(project_id) = ctrl.authenticate_api_key(&api_key).await else {
        return Err(CtrlError(
            StatusCode::UNAUTHORIZED,
            "invalid project api key".to_string(),
        ));
    };
    let flags = ctrl
        .meta
        .list_feature_flags(&project_id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
) -> ApiRes<Vec<Experiment>> {
    let list = ctrl
        .meta
        .list_experiments(&id)
        .await
        .map_err(CtrlError::internal)?;
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
    AxumPath(id): AxumPath<String>,
    Json(req): Json<CreateExpReq>,
) -> ApiRes<Experiment> {
    let now = Utc::now().timestamp_millis();
    let exp = Experiment {
        id: req
            .id
            .unwrap_or_else(|| format!("exp_{}", &Uuid::new_v4().simple().to_string()[..8])),
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
        .map_err(CtrlError::internal)?;
    Ok(Json(exp))
}

async fn handle_experiment_results_scoped(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath((_id, exp_id)): AxumPath<(String, String)>,
) -> ApiRes<crate::models::ExperimentResults> {
    compute_exp_results(&ctrl, &exp_id).await
}

async fn handle_experiment_results_global(
    State(ctrl): State<Arc<ControlPlaneService>>,
    AxumPath(exp_id): AxumPath<String>,
) -> ApiRes<crate::models::ExperimentResults> {
    compute_exp_results(&ctrl, &exp_id).await
}

async fn compute_exp_results(
    ctrl: &ControlPlaneService,
    exp_id: &str,
) -> ApiRes<crate::models::ExperimentResults> {
    let Some(exp) = ctrl
        .meta
        .get_experiment_by_id(exp_id)
        .await
        .map_err(CtrlError::internal)?
    else {
        return Err(CtrlError::not_found("experiment not found"));
    };
    let flags = ctrl
        .meta
        .list_feature_flags(&exp.project_id)
        .await
        .map_err(CtrlError::internal)?;
    let flag = flags
        .iter()
        .find(|f| f.id == exp.flag_id || f.key == exp.flag_key);
    let results = evaluate_experiment_results(&exp, flag, &ctrl.qengine)
        .await
        .map_err(CtrlError::internal)?;
    Ok(Json(results))
}

// ------------------- MCP JSON-RPC 2.0 -------------------

async fn handle_mcp(
    State(ctrl): State<Arc<ControlPlaneService>>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<crate::mcp::JsonRpcResponse> {
    let resp = handle_mcp_request(req, &ctrl.default_project_id, &ctrl.meta, &ctrl.qengine).await;
    Json(resp)
}
