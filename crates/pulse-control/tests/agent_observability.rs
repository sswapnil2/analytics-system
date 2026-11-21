use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use prost::Message;
use pulse_agent::otlp::pb::{
    pb_any_value, PbAnyValue, PbExportTraceServiceRequest, PbInstrumentationScope, PbKeyValue,
    PbResource, PbResourceSpans, PbScopeSpans, PbSpan, PbSpanEvent, PbSpanLink, PbStatus,
};
use pulse_agent::store::manifest::ManifestStore;
use pulse_agent::{
    encode_plst_batch, AgentErasureTombstone, AgentEvaluation, AgentSpan, AgentTrace,
    AgentTraceStore, AgentTraceStoreConfig, ErasureLevel, EvalTargetType, EvaluatorType,
    FailureSource, ScoreType, SpanEvent, SpanPhase, SpanType, TraceIngestBatch, TraceStatus,
    MAX_PLST_UNCOMP_BYTES, PLST_MAGIC, PLST_VERSION,
};
use pulse_control::{
    build_control_router, ControlPlaneService, MetaStore, EVAL_QUEUE_CAPACITY,
    MAX_CONCURRENT_LLM_JUDGES,
};
use pulse_exec::{CatalogRegistry, QueryEngine};
use pulse_identity::IdentityRouter;
use pulse_storage::{
    validate_outbound_url_ssrf, EncryptedObjectStore, StorageConfig, StorageEngine,
};
use ring::hmac;
use serde_json::{json, Value};
use sqlx::Row;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tower::ServiceExt;

const ROOT: &str = "sk_root_test_agent_obs_0123456789abcdef";
const INGEST_KEY: &str = "pk_test_agent_obs_ingest_0123456789ab";
const DEFAULT_PROJECT: &str = "proj_alpha";
const PW: &str = "correct horse battery staple";

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let mut saved = Vec::with_capacity(vars.len());
        for &(k, v) in vars {
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

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicUsize,
    get_ranges: AtomicUsize,
    puts: AtomicUsize,
    deletes: AtomicUsize,
    plss_gets: AtomicUsize,
    plss_get_ranges: AtomicUsize,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
            get_ranges: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            plss_gets: AtomicUsize::new(0),
            plss_get_ranges: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.gets.store(0, Ordering::SeqCst);
        self.get_ranges.store(0, Ordering::SeqCst);
        self.puts.store(0, Ordering::SeqCst);
        self.deletes.store(0, Ordering::SeqCst);
        self.plss_gets.store(0, Ordering::SeqCst);
        self.plss_get_ranges.store(0, Ordering::SeqCst);
    }

    fn plss_gets(&self) -> usize {
        self.plss_gets.load(Ordering::SeqCst)
    }

    fn plss_get_ranges(&self) -> usize {
        self.plss_get_ranges.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if location.as_ref().ends_with(".plss") {
            self.plss_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &ObjectPath,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        self.get_ranges.fetch_add(1, Ordering::SeqCst);
        if location.as_ref().ends_with(".plss") {
            self.plss_get_ranges.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

struct TestHarness {
    tmp: TempDir,
    raw_store: Arc<dyn ObjectStore>,
    counting_store: Arc<CountingObjectStore>,
    enc_store: Arc<EncryptedObjectStore>,
    ctrl: Arc<ControlPlaneService>,
    meta: MetaStore,
    app: Router,
}

impl TestHarness {
    async fn new(gc_grace_secs: i64) -> Self {
        let _env_guard = ENV_LOCK.lock().await;
        Self::new_with_env_locked(gc_grace_secs).await
    }

    async fn new_with_env_locked(gc_grace_secs: i64) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = AgentTraceStoreConfig {
            cache_dir: Some(tmp.path().join("agent_foyer_cache")),
            l0_shards: 1,
            gc_grace_secs,
            wal_group_commit_ms: 5,
            ..Default::default()
        };
        Self::with_tempdir_and_config(tmp, cfg).await
    }

    async fn with_tempdir_and_config(tmp: TempDir, agent_cfg: AgentTraceStoreConfig) -> Self {
        let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counting_store = Arc::new(CountingObjectStore::new(raw_store.clone()));
        let enc_store = Arc::new(EncryptedObjectStore::new(counting_store.clone()));
        let id_router = Arc::new(IdentityRouter::new(enc_store.as_object_store()));
        let storage = StorageEngine::new_with_encrypted(
            StorageConfig {
                num_buckets: 4,
                l0_flush_row_threshold: 10_000,
                l0_flush_max_age: Duration::from_secs(60),
                compaction_min_segments: 4,
            },
            enc_store.clone(),
            id_router,
        );
        let qengine = QueryEngine::new(storage, 16 * 1024 * 1024, None, 0, CatalogRegistry::new())
            .await
            .unwrap();
        let meta = MetaStore::connect_auto(tmp.path()).await.unwrap();
        let ctrl = ControlPlaneService::new_with_admin_with_agent_config(
            meta.clone(),
            qengine,
            DEFAULT_PROJECT,
            INGEST_KEY,
            Some(ROOT.to_string()),
            agent_cfg,
        )
        .await
        .unwrap();
        ctrl.set_outbox_path(tmp.path().join("outbox"));
        let app = build_control_router(ctrl.clone());
        Self {
            tmp,
            raw_store,
            counting_store,
            enc_store,
            ctrl,
            meta,
            app,
        }
    }

    async fn send_raw(
        &self,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut builder = Request::builder().method(method).uri(uri);
        for &(k, v) in headers {
            builder = builder.header(k, v);
        }
        let resp = self
            .app
            .clone()
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let resp_headers = resp.headers().clone();
        let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
        (status, resp_headers, bytes)
    }

    async fn send_json(
        &self,
        method: &str,
        uri: &str,
        bearer: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(tok) = bearer {
            builder = builder.header("authorization", format!("Bearer {tok}"));
        }
        let req_body = match body {
            Some(v) => {
                builder = builder.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&v).unwrap())
            }
            None => Body::empty(),
        };
        let resp = self
            .app
            .clone()
            .oneshot(builder.body(req_body).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
        let val = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
        (status, val)
    }

    async fn ingest_plst(&self, key: &str, batch: &TraceIngestBatch) -> (StatusCode, Value) {
        let payload = encode_plst_batch(batch, true).unwrap();
        let (st, _, bytes) = self
            .send_raw(
                "POST",
                "/v1/agent/traces",
                &[
                    ("x-pulse-key", key),
                    ("content-type", "application/x-pulse-trace"),
                ],
                payload,
            )
            .await;
        let val = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (st, val)
    }

    async fn signup_owner(&self, email: &str) -> String {
        let (st, v) = self
            .send_json(
                "POST",
                "/v1/auth/signup",
                None,
                Some(json!({
                    "email": email,
                    "password": PW,
                    "name": "Owner",
                    "setup_code": ROOT,
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "signup owner failed: {v}");
        v["token"].as_str().unwrap().to_string()
    }

    async fn invite_and_accept(&self, by_token: &str, email: &str, role: &str) -> String {
        let (st, v) = self
            .send_json(
                "POST",
                "/v1/org/invites",
                Some(by_token),
                Some(json!({ "email": email, "role": role })),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "invite failed: {v}");
        let url = v["invite_url"].as_str().unwrap();
        let token = url.rsplit('/').next().unwrap();
        let (st, accepted) = self
            .send_json(
                "POST",
                "/v1/auth/invites/accept",
                None,
                Some(json!({ "token": token, "password": PW, "name": role })),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "accept invite failed: {accepted}");
        accepted["token"].as_str().unwrap().to_string()
    }
}

async fn list_paths(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let p = ObjectPath::from(prefix);
    let mut metas: Vec<ObjectMeta> = store.list(Some(&p)).try_collect().await.unwrap();
    metas.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
    metas
        .into_iter()
        .map(|m| m.location.as_ref().to_string())
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn make_sample_trace(
    trace_id: &str,
    session_id: Option<&str>,
    user_id: Option<&str>,
    agent_name: &str,
    agent_version: &str,
    start_ts_ms: i64,
    duration_ms: i64,
    status: TraceStatus,
) -> (AgentTrace, Vec<AgentSpan>) {
    let end_ts_ms = start_ts_ms + duration_ms;
    let span_llm_id = format!("{trace_id}_sp_llm");
    let span_tool_id = format!("{trace_id}_sp_tool");
    let span_guard_id = format!("{trace_id}_sp_guard");

    let trace = AgentTrace {
        trace_id: trace_id.to_string(),
        project_id: String::new(),
        agent_name: agent_name.to_string(),
        agent_version: agent_version.to_string(),
        prompt_version: Some("p_v1".to_string()),
        model_config_version: None,
        tool_config_version: None,
        app_release: None,
        environment: "production".to_string(),
        session_id: session_id.map(str::to_string),
        user_id: user_id.map(str::to_string),
        workflow_id: Some("wf_support".to_string()),
        parent_trace_id: None,
        status,
        start_ts_ms,
        end_ts_ms: Some(end_ts_ms),
        duration_ms,
        input: json!({"prompt": "Help me reset my password"}),
        output: json!({"answer": "Here are the reset steps"}),
        error_type: if status == TraceStatus::Error {
            Some("upstream_error".to_string())
        } else {
            None
        },
        error_message: if status == TraceStatus::Error {
            Some("model timeout".to_string())
        } else {
            None
        },
        failure_source: if status == TraceStatus::Error {
            Some(FailureSource::Model)
        } else {
            None
        },
        metadata: json!({"channel": "web"}),
        tags: vec!["support".to_string()],
        span_count: 3,
        llm_call_count: 1,
        tool_call_count: 1,
        error_span_count: if status == TraceStatus::Error { 1 } else { 0 },
        input_tokens: 120,
        output_tokens: 45,
        cached_tokens: 20,
        total_tokens: 165,
        total_cost_nanos: 0,
        total_cost_usd: 0.0,
        primary_model: Some("gpt-4o".to_string()),
        primary_provider: Some("openai".to_string()),
        avg_eval_score: None,
        eval_pass_rate: None,
        has_loop_warning: false,
    };

    let s_llm = AgentSpan {
        span_id: span_llm_id.clone(),
        trace_id: trace_id.to_string(),
        project_id: String::new(),
        parent_span_id: None,
        linked_span_ids: vec![],
        span_phase: SpanPhase::Final,
        depth: 0,
        name: "llm.chat".to_string(),
        span_type: SpanType::Llm,
        status,
        start_ts_ms,
        end_ts_ms: Some(start_ts_ms + duration_ms / 2),
        duration_ms: duration_ms / 2,
        provider: Some("openai".to_string()),
        model: Some("gpt-4o".to_string()),
        model_id: Some("gpt-4o-2024-08-06".to_string()),
        model_version: None,
        prompt_template_id: None,
        prompt_template_version: None,
        input_tokens: 120,
        output_tokens: 45,
        cached_tokens: 20,
        total_tokens: 165,
        cost_nanos: 0,
        cost_usd: 0.0,
        temperature: Some(0.2),
        finish_reason: Some("stop".to_string()),
        model_params: json!({}),
        tool_name: None,
        tool_version: None,
        tool_retry_count: 0,
        input: json!({"messages": [{"role": "user", "content": "Help me reset my password"}]}),
        output: json!({"content": "Calling lookup tool and returning answer."}),
        attributes: json!({"step": 1}),
        events: vec![SpanEvent {
            name: "first_token".to_string(),
            timestamp_ms: start_ts_ms + 10,
            attributes: json!({"latency_ms": 10}),
        }],
        metadata: json!({}),
        error_type: if status == TraceStatus::Error {
            Some("upstream_error".to_string())
        } else {
            None
        },
        error_message: if status == TraceStatus::Error {
            Some("model timeout".to_string())
        } else {
            None
        },
        error_stack: None,
    };

    let s_tool = AgentSpan {
        span_id: span_tool_id.clone(),
        trace_id: trace_id.to_string(),
        project_id: String::new(),
        parent_span_id: Some(span_llm_id.clone()),
        linked_span_ids: vec![span_llm_id.clone()],
        span_phase: SpanPhase::Final,
        depth: 1,
        name: "tool.lookup_kb".to_string(),
        span_type: SpanType::Tool,
        status: TraceStatus::Ok,
        start_ts_ms: start_ts_ms + duration_ms / 2,
        end_ts_ms: Some(start_ts_ms + (duration_ms * 3) / 4),
        duration_ms: duration_ms / 4,
        provider: None,
        model: None,
        model_id: None,
        model_version: None,
        prompt_template_id: None,
        prompt_template_version: None,
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        total_tokens: 0,
        cost_nanos: 0,
        cost_usd: 0.0,
        temperature: None,
        finish_reason: None,
        model_params: json!({}),
        tool_name: Some("lookup_kb".to_string()),
        tool_version: Some("1.0".to_string()),
        tool_retry_count: 0,
        input: json!({"query": "password reset"}),
        output: json!({"articles": ["kb-101"]}),
        attributes: json!({}),
        events: vec![],
        metadata: json!({}),
        error_type: None,
        error_message: None,
        error_stack: None,
    };

    let s_guard = AgentSpan {
        span_id: span_guard_id,
        trace_id: trace_id.to_string(),
        project_id: String::new(),
        parent_span_id: Some(span_llm_id),
        linked_span_ids: vec![span_tool_id],
        span_phase: SpanPhase::Final,
        depth: 1,
        name: "guardrail.pii_check".to_string(),
        span_type: SpanType::Guardrail,
        status: TraceStatus::Ok,
        start_ts_ms: start_ts_ms + (duration_ms * 3) / 4,
        end_ts_ms: Some(end_ts_ms),
        duration_ms: duration_ms / 4,
        provider: None,
        model: None,
        model_id: None,
        model_version: None,
        prompt_template_id: None,
        prompt_template_version: None,
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        total_tokens: 0,
        cost_nanos: 0,
        cost_usd: 0.0,
        temperature: None,
        finish_reason: None,
        model_params: json!({}),
        tool_name: None,
        tool_version: None,
        tool_retry_count: 0,
        input: json!({"text": "Here are the reset steps"}),
        output: json!({"allowed": true}),
        attributes: json!({"policy": "pii_v1"}),
        events: vec![],
        metadata: json!({}),
        error_type: None,
        error_message: None,
        error_stack: None,
    };

    (trace, vec![s_llm, s_tool, s_guard])
}

async fn snapshot_sqlite_table_counts(
    meta: &MetaStore,
    db_path: &Path,
) -> (BTreeMap<String, i64>, u64) {
    let MetaStore::Sqlite(pool) = meta else {
        panic!("expected sqlite MetaStore");
    };
    let rows = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let table_names: Vec<String> = rows.into_iter().map(|r| r.get::<String, _>(0)).collect();
    assert!(
        !table_names
            .iter()
            .any(|n| n == "agent_traces" || n == "agent_spans"),
        "agent_traces/agent_spans must NOT exist in SQLite schema: {table_names:?}"
    );

    let mut counts = BTreeMap::new();
    for t in table_names {
        let q = format!("SELECT COUNT(*) FROM \"{t}\"");
        let cnt: i64 = sqlx::query_scalar(&q).fetch_one(pool).await.unwrap();
        counts.insert(t, cnt);
    }

    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(pool)
        .await
        .unwrap();
    let wal_path = db_path.with_file_name("pulse_meta.db-wal");
    let wal_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    (counts, wal_len)
}

fn kv_str(k: &str, v: &str) -> PbKeyValue {
    PbKeyValue {
        key: k.to_string(),
        value: Some(PbAnyValue {
            value: Some(pb_any_value::Value::StringValue(v.to_string())),
        }),
    }
}

fn kv_int(k: &str, v: i64) -> PbKeyValue {
    PbKeyValue {
        key: k.to_string(),
        value: Some(PbAnyValue {
            value: Some(pb_any_value::Value::IntValue(v)),
        }),
    }
}

// ===========================================================================
// 1 & 2. Ingestion Formats, Auth, CORS, Limits, Privacy, Pricing & Zero SQL Writes
// ===========================================================================

#[tokio::test]
async fn test_01_ingest_formats_auth_cors_limits_privacy_pricing_and_zero_sql_writes() {
    let h = TestHarness::new(900).await;
    let now_ms = Utc::now().timestamp_millis();

    // 1. CORS preflight on /v1/agent/traces
    let (opt_st, opt_hdrs, _) = h
        .send_raw(
            "OPTIONS",
            "/v1/agent/traces",
            &[
                ("origin", "https://app.example.com"),
                ("access-control-request-method", "POST"),
            ],
            vec![],
        )
        .await;
    assert!(
        opt_st == StatusCode::OK || opt_st == StatusCode::NO_CONTENT,
        "OPTIONS preflight failed: {opt_st}"
    );
    assert_eq!(
        opt_hdrs
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );

    // 2. Auth failures: missing key, sk_ admin key on ingest, query-string key rejected
    let (tr, sps) = make_sample_trace(
        "tr_auth_check",
        Some("sess_auth"),
        Some("user_auth"),
        "support_bot",
        "v1",
        now_ms,
        100,
        TraceStatus::Ok,
    );
    let sample_batch = TraceIngestBatch {
        traces: vec![tr.clone()],
        spans: sps.clone(),
    };
    let sample_plst = encode_plst_batch(&sample_batch, true).unwrap();

    let (st, _, _) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[("content-type", "application/x-pulse-trace")],
            sample_plst.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _, _) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("x-pulse-key", ROOT),
                ("content-type", "application/x-pulse-trace"),
            ],
            sample_plst.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _, _) = h
        .send_raw(
            "POST",
            &format!("/v1/agent/traces?api_key={INGEST_KEY}"),
            &[("content-type", "application/x-pulse-trace")],
            sample_plst.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // Bearer pk_... accepted
    let (st, resp_hdrs, _) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("authorization", &format!("Bearer {INGEST_KEY}")),
                ("content-type", "application/x-pulse-trace"),
            ],
            sample_plst,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        resp_hdrs
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );

    // 3. Body size cap (8 MiB -> 413) + PLST UncompLen > 64 MiB -> 413 + malformed -> 400
    let oversized_body = vec![b'a'; 8 * 1024 * 1024 + 1];
    let (st, _, _) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/json"),
            ],
            oversized_body,
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE);

    // Crafted PLST header claiming UncompLen > 64 MiB
    let mut bad_len_plst = Vec::with_capacity(24);
    bad_len_plst.extend_from_slice(&PLST_MAGIC);
    bad_len_plst.push(PLST_VERSION);
    bad_len_plst.push(0x01); // RAW_ZSTD (dict_id must be 0)
    bad_len_plst.extend_from_slice(&0u16.to_le_bytes());
    bad_len_plst.extend_from_slice(&((MAX_PLST_UNCOMP_BYTES as u32) + 1).to_le_bytes());
    bad_len_plst.extend_from_slice(&4u32.to_le_bytes());
    bad_len_plst.extend_from_slice(&0u32.to_le_bytes());
    bad_len_plst.extend_from_slice(&[1, 2, 3, 4]);
    let (st, _, _) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/x-pulse-trace"),
            ],
            bad_len_plst,
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE);

    let mut corrupt_plst = Vec::with_capacity(24);
    corrupt_plst.extend_from_slice(&PLST_MAGIC);
    corrupt_plst.push(PLST_VERSION);
    corrupt_plst.push(0x01); // RAW_ZSTD (dict_id = 0)
    corrupt_plst.extend_from_slice(&0u16.to_le_bytes());
    corrupt_plst.extend_from_slice(&16u32.to_le_bytes()); // valid small uncomp_len
    corrupt_plst.extend_from_slice(&4u32.to_le_bytes()); // comp_len
    corrupt_plst.extend_from_slice(&0u32.to_le_bytes()); // wrong CRC32
    corrupt_plst.extend_from_slice(&[1, 2, 3, 4]);
    let (st, _, _) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/x-pulse-trace"),
            ],
            corrupt_plst,
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // 4. Custom pricing + Privacy Policy (PII redaction + user_id stripping + sampling)
    let (st, prc_val) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing"),
            Some(ROOT),
            Some(json!({
                "provider": "custom_ai",
                "model_pattern": "custom-model-",
                "input_per_1m_usd": 10.0,
                "output_per_1m_usd": 30.0,
                "cached_input_per_1m_usd": 2.0
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "upsert pricing failed: {prc_val}");

    // Default privacy policy: capture_user_id=false, redact_pii=true
    let (mut pii_tr, mut pii_sps) = make_sample_trace(
        "tr_pii_default",
        Some("sess_pii"),
        Some("user_should_strip"),
        "support_bot",
        "v1",
        now_ms + 1,
        150,
        TraceStatus::Ok,
    );
    pii_tr.input = json!({
        "email": "alice@example.com",
        "password": "super-secret-password",
        "api_key": "sk_live_secret_value"
    });
    pii_sps[0].provider = Some("custom_ai".to_string());
    pii_sps[0].model = Some("custom-model-pro".to_string());
    pii_sps[0].input_tokens = 1_000_000;
    pii_sps[0].output_tokens = 1_000_000;
    pii_sps[0].cached_tokens = 0;
    pii_sps[0].total_tokens = 2_000_000;
    pii_sps[0].cost_nanos = 0;
    pii_sps[0].cost_usd = 0.0;
    pii_tr.total_cost_nanos = 0;
    pii_tr.total_cost_usd = 0.0;

    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![pii_tr],
                spans: pii_sps,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, pii_detail) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_pii_default"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{pii_detail}");
    assert!(
        pii_detail["trace"]["user_id"].is_null(),
        "default capture_user_id=false must strip user_id"
    );
    assert_eq!(
        pii_detail["trace"]["input"]["password"], "[REDACTED]",
        "password key must be redacted"
    );
    assert_eq!(
        pii_detail["trace"]["input"]["api_key"], "[REDACTED]",
        "api_key must be redacted"
    );
    assert_ne!(
        pii_detail["trace"]["input"]["email"], "alice@example.com",
        "email PII must be redacted"
    );
    // Custom pricing: 1M * $10 + 1M * $30 = $40 = 40_000_000_000 nanos
    let cost_nanos = pii_detail["trace"]["total_cost_nanos"].as_i64().unwrap();
    assert_eq!(
        cost_nanos, 40_000_000_000,
        "expected custom pricing cost_nanos=40_000_000_000, got {cost_nanos}"
    );

    // Update privacy to sample_rate=0.0 -> drops trace
    let (st, _) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(ROOT),
            Some(json!({
                "capture_user_id": true,
                "redact_pii": true,
                "sample_rate": 0.0
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (drop_tr, drop_sps) = make_sample_trace(
        "tr_sampled_out",
        Some("sess_drop"),
        Some("user_kept"),
        "support_bot",
        "v1",
        now_ms + 2,
        100,
        TraceStatus::Ok,
    );
    let (st, drop_resp) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![drop_tr],
                spans: drop_sps,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(drop_resp["accepted_traces"], 0);

    // Restore sample_rate = 1.0 & capture_user_id = true
    let (st, _) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(ROOT),
            Some(json!({
                "capture_user_id": true,
                "redact_pii": true,
                "sample_rate": 1.0
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // 5. ZERO SQL WRITES on trace/span ingest across ALL 4 formats:
    //    PLST binary, Native JSON, OTLP/JSON, and OTLP/Protobuf.
    let db_path = h.tmp.path().join("pulse_meta.db");
    let (counts_before, wal_before) = snapshot_sqlite_table_counts(&h.meta, &db_path).await;
    assert_eq!(wal_before, 0);

    let metrics_before_traces = h
        .ctrl
        .agent_metrics()
        .traces_ingested
        .load(Ordering::Relaxed);
    let metrics_before_spans = h
        .ctrl
        .agent_metrics()
        .spans_ingested
        .load(Ordering::Relaxed);

    // 5a. PLST binary with duplicate blobs (tests blobs_deduped)
    let (mut plst_tr, mut plst_sps) = make_sample_trace(
        "tr_fmt_plst",
        Some("sess_fmt"),
        Some("user_fmt"),
        "support_bot",
        "v1",
        now_ms + 10,
        200,
        TraceStatus::Ok,
    );
    let shared_blob = json!({"long_context": "repeated context payload ".repeat(16)});
    plst_tr.input = shared_blob.clone();
    plst_sps[0].input = shared_blob.clone();
    plst_sps[1].input = shared_blob;
    let (st, r) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![plst_tr],
                spans: plst_sps,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{r}");
    assert_eq!(r["accepted_traces"], 1);
    assert_eq!(r["accepted_spans"], 3);

    // 5b. Native JSON
    let (json_tr, json_sps) = make_sample_trace(
        "tr_fmt_json",
        Some("sess_fmt"),
        Some("user_fmt"),
        "support_bot",
        "v1",
        now_ms + 20,
        220,
        TraceStatus::Ok,
    );
    let json_payload = serde_json::to_vec(&TraceIngestBatch {
        traces: vec![json_tr],
        spans: json_sps,
    })
    .unwrap();
    let (st, _, raw) = h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/json"),
            ],
            json_payload,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{}", String::from_utf8_lossy(&raw));

    // 5c. OTLP/JSON
    let start_ns = (now_ms as u64 + 30) * 1_000_000;
    let end_ns = start_ns + 180_000_000;
    let otlp_json = json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    { "key": "service.name", "value": { "stringValue": "otlp_agent" } },
                    { "key": "service.version", "value": { "stringValue": "2.0" } },
                    { "key": "deployment.environment", "value": { "stringValue": "production" } }
                ]
            },
            "scopeSpans": [{
                "spans": [{
                    "traceId": "0102030405060708090a0b0c0d0e0f10",
                    "spanId": "1112131415161718",
                    "name": "guardrail.otlp_json",
                    "startTimeUnixNano": start_ns.to_string(),
                    "endTimeUnixNano": end_ns.to_string(),
                    "attributes": [
                        { "key": "pulse.span_type", "value": { "stringValue": "guardrail" } },
                        { "key": "pulse.span_phase", "value": { "stringValue": "final" } },
                        { "key": "pulse.session_id", "value": { "stringValue": "sess_fmt" } },
                        { "key": "pulse.parent_trace_id", "value": { "stringValue": "tr_fmt_plst" } },
                        { "key": "gen_ai.system", "value": { "stringValue": "openai" } },
                        { "key": "gen_ai.request.model", "value": { "stringValue": "gpt-4o" } },
                        { "key": "gen_ai.usage.input_tokens", "value": { "intValue": 50 } },
                        { "key": "gen_ai.usage.output_tokens", "value": { "intValue": 25 } }
                    ],
                    "status": { "code": 1 }
                }]
            }]
        }]
    });
    let (st, _, raw) = h
        .send_raw(
            "POST",
            "/v1/otel/v1/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/json"),
            ],
            serde_json::to_vec(&otlp_json).unwrap(),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{}", String::from_utf8_lossy(&raw));

    // 5d. OTLP/Protobuf (using PbExportTraceServiceRequest::encode_to_vec)
    let pb_req = PbExportTraceServiceRequest {
        resource_spans: vec![PbResourceSpans {
            resource: Some(PbResource {
                attributes: vec![
                    kv_str("service.name", "otlp_pb_agent"),
                    kv_str("service.version", "3.0"),
                    kv_str("deployment.environment", "staging"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![PbScopeSpans {
                scope: Some(PbInstrumentationScope {
                    name: "pulse-test".to_string(),
                    version: "1.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans: vec![PbSpan {
                    trace_id: vec![0xaa; 16],
                    span_id: vec![0xbb; 8],
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    name: "guardrail.otlp_pb".to_string(),
                    kind: 1,
                    start_time_unix_nano: start_ns + 5_000_000,
                    end_time_unix_nano: end_ns + 5_000_000,
                    attributes: vec![
                        kv_str("pulse.span_type", "guardrail"),
                        kv_str("pulse.span_phase", "final"),
                        kv_str("pulse.session_id", "sess_fmt"),
                        kv_str("pulse.parent_trace_id", "tr_fmt_json"),
                        kv_str("gen_ai.system", "anthropic"),
                        kv_str("gen_ai.request.model", "claude-3-5-sonnet"),
                        kv_int("gen_ai.usage.input_tokens", 80),
                        kv_int("gen_ai.usage.output_tokens", 30),
                    ],
                    dropped_attributes_count: 0,
                    events: vec![PbSpanEvent {
                        time_unix_nano: start_ns + 6_000_000,
                        name: "pb_event".to_string(),
                        attributes: vec![kv_str("ok", "true")],
                        dropped_attributes_count: 0,
                    }],
                    dropped_events_count: 0,
                    links: vec![PbSpanLink {
                        trace_id: vec![0x01; 16],
                        span_id: vec![0x02; 8],
                        trace_state: String::new(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                    }],
                    dropped_links_count: 0,
                    status: Some(PbStatus {
                        message: String::new(),
                        code: 1,
                    }),
                    flags: 0,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let (st, _, raw) = h
        .send_raw(
            "POST",
            "/v1/otel/v1/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/x-protobuf"),
            ],
            pb_req.encode_to_vec(),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{}", String::from_utf8_lossy(&raw));

    // Verify ZERO SQL writes occurred across ALL 4 ingest requests!
    let wal_file = db_path.with_file_name("pulse_meta.db-wal");
    let wal_len_during = std::fs::metadata(&wal_file).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        wal_len_during, 0,
        "SQLite WAL must remain 0 bytes during trace/span ingest across all 4 formats"
    );
    let (counts_after, wal_after) = snapshot_sqlite_table_counts(&h.meta, &db_path).await;
    assert_eq!(wal_after, 0);
    assert_eq!(
        counts_before, counts_after,
        "SQLite control-plane row counts must NOT change on trace/span ingest"
    );

    // Verify AgentIngestMetrics incremented
    assert!(
        h.ctrl
            .agent_metrics()
            .traces_ingested
            .load(Ordering::Relaxed)
            >= metrics_before_traces + 4
    );
    assert!(
        h.ctrl
            .agent_metrics()
            .spans_ingested
            .load(Ordering::Relaxed)
            >= metrics_before_spans + 8
    );
    assert!(h.ctrl.agent_metrics().blobs_deduped.load(Ordering::Relaxed) >= 1);

    // Verify OTLP/Protobuf trace preserved Guardrail, SpanPhase, linked_span_ids, parent_trace_id
    let pb_trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (st, pb_detail) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/{pb_trace_id}"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{pb_detail}");
    assert_eq!(pb_detail["trace"]["parent_trace_id"], "tr_fmt_json");
    assert_eq!(pb_detail["spans"][0]["span_type"], "guardrail");
    assert_eq!(pb_detail["spans"][0]["span_phase"], "final");
    assert_eq!(
        pb_detail["spans"][0]["linked_span_ids"][0],
        "0202020202020202"
    );

    // 6. Rate limiting (429 + Retry-After) under ENV_LOCK
    drop(h);
    let _env_guard = ENV_LOCK.lock().await;
    let _vars = EnvGuard::set(&[
        ("PULSE_INGEST_EVENTS_PER_SEC", Some("2")),
        ("PULSE_INGEST_BURST_SECS", Some("1")),
    ]);
    let rl_h = TestHarness::new_with_env_locked(900).await;
    drop(_vars);
    drop(_env_guard);

    // Create a second project proj_beta to verify per-project isolation of rate limit buckets
    let (st, beta_proj) = rl_h
        .send_json(
            "POST",
            "/v1/projects",
            Some(ROOT),
            Some(json!({ "name": "Beta Rate Project" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{beta_proj}");
    let beta_id = beta_proj["project"]["id"].as_str().unwrap();
    let (st, beta_key_resp) = rl_h
        .send_json(
            "POST",
            &format!("/v1/projects/{beta_id}/api-keys"),
            Some(ROOT),
            Some(json!({ "label": "Beta Ingest", "role": "ingest_and_flags" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{beta_key_resp}");
    let beta_pk = beta_key_resp["plaintext_key"].as_str().unwrap();

    // First batch (4 units: 1 trace + 3 spans) overdrafts bucket (cap=2 -> -2) and succeeds
    let (tr1, sps1) = make_sample_trace(
        "tr_rl_1",
        None,
        None,
        "bot",
        "v1",
        now_ms + 40,
        50,
        TraceStatus::Ok,
    );
    let (st, _) = rl_h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![tr1],
                spans: sps1,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Immediate second batch on proj_alpha -> 429 + Retry-After
    let (tr2, sps2) = make_sample_trace(
        "tr_rl_2",
        None,
        None,
        "bot",
        "v1",
        now_ms + 41,
        50,
        TraceStatus::Ok,
    );
    let payload2 = encode_plst_batch(
        &TraceIngestBatch {
            traces: vec![tr2],
            spans: sps2,
        },
        true,
    )
    .unwrap();
    let (st, hdrs, _) = rl_h
        .send_raw(
            "POST",
            "/v1/agent/traces",
            &[
                ("x-pulse-key", INGEST_KEY),
                ("content-type", "application/x-pulse-trace"),
            ],
            payload2,
        )
        .await;
    assert_eq!(st, StatusCode::TOO_MANY_REQUESTS);
    assert!(hdrs.get(header::RETRY_AFTER).is_some());

    // Beta project is unaffected -> 200 OK
    let (tr_b, sps_b) = make_sample_trace(
        "tr_rl_beta",
        None,
        None,
        "bot",
        "v1",
        now_ms + 42,
        50,
        TraceStatus::Ok,
    );
    let (st, _) = rl_h
        .ingest_plst(
            beta_pk,
            &TraceIngestBatch {
                traces: vec![tr_b],
                spans: sps_b,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);
}

// ===========================================================================
// 3. PTWL WAL, L0 Flush, PLSS Compaction, 15m GC Grace, BYOK & Coalesced get_range
// ===========================================================================

#[tokio::test]
async fn test_02_wal_l0_plss_compaction_15m_gc_grace_byok_and_coalesced_get_range() {
    let h = TestHarness::new(900).await;
    let now_ms = Utc::now().timestamp_millis();

    // 1. Rotate BYOK cluster encryption key -> verify PLSEAGM2 on .ptwl and .plss
    let (st, rot) = h
        .send_json(
            "POST",
            "/v1/system/encryption/rotate",
            Some(ROOT),
            Some(json!({ "secret": "test-byok-master-passphrase-2026" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "rotate encryption failed: {rot}");
    assert_eq!(rot["status"]["enabled"], true);

    let (tr1, sps1) = make_sample_trace(
        "tr_wal_1",
        Some("sess_wal"),
        None,
        "wal_bot",
        "v1",
        now_ms,
        120,
        TraceStatus::Ok,
    );
    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![tr1],
                spans: sps1,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // .ptwl WAL file exists and starts with PLSEAGM2
    let wal_paths = list_paths(&h.raw_store, &format!("agent_wal/{DEFAULT_PROJECT}/")).await;
    assert!(
        !wal_paths.is_empty(),
        "expected at least 1 .ptwl WAL object before flush"
    );
    let wal_bytes = h
        .raw_store
        .get(&ObjectPath::from(wal_paths[0].as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        wal_bytes.starts_with(b"PLSEAGM2"),
        "expected encrypted BYOK WAL object starting with PLSEAGM2"
    );

    // Flush via POST /v1/admin/flush -> .ptwl purged + .plss created with PLSEAGM2
    let (st, flush_val) = h
        .send_json("POST", "/v1/admin/flush", Some(ROOT), None)
        .await;
    assert_eq!(st, StatusCode::OK, "{flush_val}");

    let wal_after = list_paths(&h.raw_store, &format!("agent_wal/{DEFAULT_PROJECT}/")).await;
    assert!(wal_after.is_empty(), "expected .ptwl purged after flush");
    let segs_after_1 =
        list_paths(&h.raw_store, &format!("agent_segments/{DEFAULT_PROJECT}/")).await;
    assert_eq!(segs_after_1.len(), 1);
    let seg_bytes = h
        .raw_store
        .get(&ObjectPath::from(segs_after_1[0].as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        seg_bytes.starts_with(b"PLSEAGM2"),
        "expected encrypted BYOK .plss segment starting with PLSEAGM2"
    );
    let enc_status = h.enc_store.status();
    assert!(enc_status.enabled);

    // 2. Ingest & flush 3 more batches -> compact_and_gc with 900s (15m) grace period
    for i in 2..=4 {
        let (tr, sps) = make_sample_trace(
            &format!("tr_wal_{i}"),
            Some("sess_wal"),
            None,
            "wal_bot",
            "v1",
            now_ms + (i as i64) * 100,
            100 + (i as i64) * 10,
            TraceStatus::Ok,
        );
        let (st, _) = h
            .ingest_plst(
                INGEST_KEY,
                &TraceIngestBatch {
                    traces: vec![tr],
                    spans: sps,
                },
            )
            .await;
        assert_eq!(st, StatusCode::OK);
        h.ctrl.agent_store().flush_all_l0().await.unwrap();
    }

    let pre_compact_segs =
        list_paths(&h.raw_store, &format!("agent_segments/{DEFAULT_PROJECT}/")).await;
    assert!(pre_compact_segs.len() >= 4);

    h.ctrl
        .agent_store()
        .compact_and_gc(Some(DEFAULT_PROJECT), &[])
        .await
        .unwrap();

    let manifest_store =
        ManifestStore::new(DEFAULT_PROJECT, h.enc_store.as_object_store(), 900_000).unwrap();
    let manifest = manifest_store.load_latest().await.unwrap();
    assert!(
        !manifest.pending_gc.is_empty(),
        "expected superseded .plss segments in manifest.pending_gc"
    );
    for entry in &manifest.pending_gc {
        assert_eq!(
            entry.eligible_after_ms - entry.superseded_at_ms,
            900_000,
            "15-minute GC grace period (900_000 ms) must be enforced"
        );
        assert!(
            h.raw_store
                .head(&ObjectPath::from(entry.object_path.as_str()))
                .await
                .is_ok(),
            "superseded .plss {} must NOT be deleted before 15m grace expires",
            entry.object_path
        );
    }

    // 3. Cold-cache coalesced get_range hydration test (via replace_agent_store over CountingObjectStore)
    let cold_counter = Arc::new(CountingObjectStore::new(h.enc_store.as_object_store()));
    let cold_store = Arc::new(
        AgentTraceStore::open_with_encrypted(
            cold_counter.clone(),
            None,
            AgentTraceStoreConfig {
                cache_dir: None,
                l0_shards: 1,
                gc_grace_secs: 900,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    h.ctrl.replace_agent_store(cold_store).await.unwrap();
    cold_counter.reset();

    // First cold GET: 0 full .plss get(), <= 2 coalesced get_range() calls on AgentTraceStore
    let (st, detail1) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_wal_3"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{detail1}");
    assert_eq!(detail1["trace"]["trace_id"], "tr_wal_3");
    assert_eq!(detail1["spans"].as_array().unwrap().len(), 3);
    assert_eq!(
        cold_counter.plss_gets(),
        0,
        "must never call full object_store::get() on .plss"
    );
    assert_eq!(
        h.counting_store.plss_gets(),
        0,
        "underlying store must never see full .plss get()"
    );
    let cold_ranges = cold_counter.plss_get_ranges();
    assert!(
        (1..=4).contains(&cold_ranges),
        "expected <= 4 coalesced get_range calls on cold .plss hydration, got {cold_ranges}"
    );

    // Second warm GET: 0 get_range() calls
    cold_counter.reset();
    let (st, detail2) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_wal_3"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{detail2}");
    assert_eq!(cold_counter.plss_gets(), 0);
    assert_eq!(
        cold_counter.plss_get_ranges(),
        0,
        "warm trace hydration must hit cache with 0 get_range calls"
    );

    h.ctrl.agent_store().close().await.unwrap();

    // 4. Verify post-grace physical deletion using gc_grace_secs = 1
    let h_short = TestHarness::new(1).await;
    for i in 1..=2 {
        let (tr, sps) = make_sample_trace(
            &format!("tr_gc_{i}"),
            Some("sess_gc"),
            None,
            "gc_bot",
            "v1",
            now_ms + i * 50,
            80,
            TraceStatus::Ok,
        );
        let (st, _) = h_short
            .ingest_plst(
                INGEST_KEY,
                &TraceIngestBatch {
                    traces: vec![tr],
                    spans: sps,
                },
            )
            .await;
        assert_eq!(st, StatusCode::OK);
        h_short.ctrl.agent_store().flush_all_l0().await.unwrap();
    }
    h_short
        .ctrl
        .agent_store()
        .compact_and_gc(Some(DEFAULT_PROJECT), &[])
        .await
        .unwrap();

    let ms_short =
        ManifestStore::new(DEFAULT_PROJECT, h_short.enc_store.as_object_store(), 1_000).unwrap();
    let m_before = ms_short.load_latest().await.unwrap();
    assert!(!m_before.pending_gc.is_empty());
    let old_path = m_before.pending_gc[0].object_path.clone();
    assert!(
        h_short
            .raw_store
            .head(&ObjectPath::from(old_path.as_str()))
            .await
            .is_ok(),
        "superseded segment must remain before 1s grace expires"
    );

    tokio::time::sleep(Duration::from_millis(1150)).await;
    h_short
        .ctrl
        .agent_store()
        .compact_and_gc(Some(DEFAULT_PROJECT), &[])
        .await
        .unwrap();
    let m_after = ms_short.load_latest().await.unwrap();
    assert!(
        m_after.pending_gc.is_empty(),
        "pending_gc should be empty after grace period expires: {:?}",
        m_after.pending_gc
    );
    assert!(
        h_short
            .raw_store
            .head(&ObjectPath::from(old_path.as_str()))
            .await
            .is_err(),
        "superseded segment must be deleted after grace expires"
    );
    h_short.ctrl.agent_store().close().await.unwrap();
}

// ===========================================================================
// 4. Multi-level Evaluations, Rollups & Async Eval Worker
// ===========================================================================

#[derive(Clone)]
struct MockLlmState {
    in_flight: Arc<AtomicUsize>,
    peak_in_flight: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    malformed_mode: Arc<std::sync::atomic::AtomicBool>,
}

async fn mock_openai_handler(State(st): State<MockLlmState>, _body: Bytes) -> impl IntoResponse {
    let cur = st.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    st.peak_in_flight.fetch_max(cur, Ordering::SeqCst);
    st.calls.fetch_add(1, Ordering::SeqCst);

    tokio::time::sleep(Duration::from_millis(30)).await;
    st.in_flight.fetch_sub(1, Ordering::SeqCst);

    let content = if st.malformed_mode.load(Ordering::SeqCst) {
        "NOT VALID JSON AT ALL {{{".to_string()
    } else {
        json!({
            "score": 0.9,
            "passed": true,
            "label": "good",
            "reasoning": "Mock LLM judge approved"
        })
        .to_string()
    };

    axum::Json(json!({
        "id": "chatcmpl-mock",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content }
        }]
    }))
}

#[tokio::test]
async fn test_03_eval_worker_multi_level_llm_judge_ssrf_concurrency_queue_and_shutdown() {
    // Part A: Bounded queue cap = 2,048 drop counter + 503 on POST .../evaluate
    {
        let h_q = TestHarness::new(900).await;
        let now_ms = Utc::now().timestamp_millis();
        // Do NOT spawn eval worker so rx fills deterministically to EVAL_QUEUE_CAPACITY (2048).
        let mut traces = Vec::with_capacity(EVAL_QUEUE_CAPACITY);
        let mut spans = Vec::with_capacity(EVAL_QUEUE_CAPACITY);
        for i in 0..EVAL_QUEUE_CAPACITY {
            let tid = format!("tr_q_{i:04}");
            let (mut tr, mut sps) = make_sample_trace(
                &tid,
                Some("sess_q"),
                None,
                "q_bot",
                "v1",
                now_ms + i as i64,
                50,
                TraceStatus::Ok,
            );
            tr.span_count = 1;
            let sp = sps.remove(0);
            traces.push(tr);
            spans.push(sp);
        }
        let (st, _) = h_q
            .ingest_plst(INGEST_KEY, &TraceIngestBatch { traces, spans })
            .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            h_q.ctrl
                .agent_metrics()
                .eval_queue_dropped
                .load(Ordering::Relaxed),
            0
        );

        // 2049th trace increments eval_queue_dropped without failing ingestion
        let (tr_extra, sps_extra) = make_sample_trace(
            "tr_q_overflow",
            Some("sess_q"),
            None,
            "q_bot",
            "v1",
            now_ms + 5000,
            50,
            TraceStatus::Ok,
        );
        let (st, _) = h_q
            .ingest_plst(
                INGEST_KEY,
                &TraceIngestBatch {
                    traces: vec![tr_extra],
                    spans: sps_extra,
                },
            )
            .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            h_q.ctrl
                .agent_metrics()
                .eval_queue_dropped
                .load(Ordering::Relaxed),
            1
        );

        // Explicit POST .../evaluate returns 503 when bounded queue is full
        let (st, _) = h_q
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluate"),
                Some(ROOT),
                Some(json!({ "target_type": "trace", "trace_id": "tr_q_0000" })),
            )
            .await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    }

    // Part B & C: Hold ENV_LOCK across SSRF tests and loopback mock LLM tests
    let _env_guard = ENV_LOCK.lock().await;
    let _clear_priv = EnvGuard::set(&[("PULSE_ALLOW_PRIVATE_ENDPOINTS", None)]);

    // Verify SSRF blocking when PULSE_ALLOW_PRIVATE_ENDPOINTS is unset
    assert!(validate_outbound_url_ssrf("http://127.0.0.1:8080/v1").is_err());
    assert!(validate_outbound_url_ssrf("https://169.254.169.254/latest/meta-data").is_err());
    assert!(validate_outbound_url_ssrf("https://10.0.0.1/v1/chat/completions").is_err());

    let h = TestHarness::new_with_env_locked(900).await;
    let now_ms = Utc::now().timestamp_millis();

    // BYOK endpoint validation rejects private IPs with 400
    for bad_ep in [
        "http://127.0.0.1:8080/v1",
        "https://169.254.169.254/latest",
        "https://10.0.0.1/v1",
    ] {
        let (st, _) = h
            .send_json(
                "PUT",
                &format!("/v1/projects/{DEFAULT_PROJECT}/byok"),
                Some(ROOT),
                Some(json!({
                    "category": "llm",
                    "provider": "openai",
                    "secret": "sk-test-1234567890",
                    "config_json": { "endpoint": bad_ep, "model": "gpt-4o" }
                })),
            )
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "SSRF must block {bad_ep}");
    }

    // End-to-end worker SSRF block: configure private endpoint while allowed, then unset
    // PULSE_ALLOW_PRIVATE_ENDPOINTS before worker executes the LLM judge job.
    {
        let h_ssrf = TestHarness::new_with_env_locked(900).await;
        {
            let _temp_priv = EnvGuard::set(&[("PULSE_ALLOW_PRIVATE_ENDPOINTS", Some("1"))]);
            let (st, _) = h_ssrf
                .send_json(
                    "PUT",
                    &format!("/v1/projects/{DEFAULT_PROJECT}/byok"),
                    Some(ROOT),
                    Some(json!({
                        "category": "llm",
                        "provider": "openai",
                        "secret": "sk-ssrf-test-123456",
                        "config_json": { "endpoint": "https://127.0.0.1:9/v1/chat/completions", "model": "gpt-4o" }
                    })),
                )
                .await;
            assert_eq!(st, StatusCode::OK);
        }
        // Now PULSE_ALLOW_PRIVATE_ENDPOINTS is unset again!
        let (st, _) = h_ssrf
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluators"),
                Some(ROOT),
                Some(json!({
                    "id": "ev_ssrf_llm",
                    "name": "SSRF LLM",
                    "target_type": "trace",
                    "evaluator_type": "llm_judge",
                    "kind": "llm_judge",
                    "enabled": true,
                    "config": { "prompt": "Rate it" }
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
        let (tr_s, sps_s) = make_sample_trace(
            "tr_ssrf_1",
            Some("sess_ssrf"),
            None,
            "ssrf_bot",
            "v1",
            now_ms,
            80,
            TraceStatus::Ok,
        );
        let (st, _) = h_ssrf
            .ingest_plst(
                INGEST_KEY,
                &TraceIngestBatch {
                    traces: vec![tr_s],
                    spans: sps_s,
                },
            )
            .await;
        assert_eq!(st, StatusCode::OK);

        let (ssrf_tx, ssrf_rx) = watch::channel(false);
        let ssrf_worker = h_ssrf.ctrl.spawn_agent_eval_worker(ssrf_rx).unwrap();
        let deadline_ssrf = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let evals = h_ssrf
                .meta
                .list_trace_evaluations(DEFAULT_PROJECT, "tr_ssrf_1")
                .await
                .unwrap();
            if !evals.is_empty() {
                assert!(
                    evals[0].error.is_some(),
                    "worker must record SSRF error when PULSE_ALLOW_PRIVATE_ENDPOINTS is unset"
                );
                break;
            }
            assert!(tokio::time::Instant::now() < deadline_ssrf);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            h_ssrf
                .ctrl
                .agent_metrics()
                .evals_executed
                .load(Ordering::Relaxed),
            0
        );
        let rollups = h_ssrf
            .meta
            .list_agent_eval_rollups(DEFAULT_PROJECT, Some(EvalTargetType::Trace))
            .await
            .unwrap();
        assert!(
            rollups.is_empty(),
            "errored SSRF eval must NOT enter agent_eval_rollups"
        );
        ssrf_tx.send(true).unwrap();
        ssrf_worker.await.unwrap();
    }

    // Spawn loopback mock OpenAI server
    let mock_state = MockLlmState {
        in_flight: Arc::new(AtomicUsize::new(0)),
        peak_in_flight: Arc::new(AtomicUsize::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
        malformed_mode: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    let mock_app = Router::new()
        .route("/v1/chat/completions", post(mock_openai_handler))
        .with_state(mock_state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{mock_addr}/v1/chat/completions");
    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, mock_app).await;
    });

    // Enable loopback private endpoints for mock LLM judge
    let _allow_priv = EnvGuard::set(&[("PULSE_ALLOW_PRIVATE_ENDPOINTS", Some("1"))]);

    let (st, byok_res) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/byok"),
            Some(ROOT),
            Some(json!({
                "category": "llm",
                "provider": "openai",
                "secret": "sk-mock-llm-key-12345",
                "config_json": { "endpoint": mock_url, "model": "gpt-4o-mini" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "PUT byok failed: {byok_res}");

    // Create deterministic + LLM-judge evaluators across Session, Trace, and Span
    for ev_req in [
        json!({
            "id": "ev_sess_det",
            "name": "Session No Errors",
            "target_type": "session",
            "evaluator_type": "deterministic",
            "kind": "no_errors",
            "enabled": true
        }),
        json!({
            "id": "ev_trace_det",
            "name": "Trace No Errors",
            "target_type": "trace",
            "evaluator_type": "deterministic",
            "kind": "no_errors",
            "enabled": true
        }),
        json!({
            "id": "ev_span_regex",
            "name": "Span Output Match",
            "target_type": "span",
            "evaluator_type": "deterministic",
            "kind": "regex_match",
            "enabled": true,
            "config": { "pattern": "returning answer" }
        }),
        json!({
            "id": "ev_trace_llm",
            "name": "Trace LLM Quality",
            "target_type": "trace",
            "evaluator_type": "llm_judge",
            "kind": "llm_judge",
            "enabled": true,
            "config": { "prompt": "Score helpfulness 0..1" }
        }),
    ] {
        let (st, v) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluators"),
                Some(ROOT),
                Some(ev_req),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "create evaluator failed: {v}");
    }

    // Spawn worker with graceful watch::channel shutdown
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = h
        .ctrl
        .spawn_agent_eval_worker(shutdown_rx)
        .expect("eval worker should spawn once");

    // Ingest 16 traces -> triggers deterministic + LLM judge evaluations concurrently
    let mut batch = TraceIngestBatch::default();
    for i in 0..16 {
        let (tr, sps) = make_sample_trace(
            &format!("tr_eval_{i:02}"),
            Some("sess_eval_1"),
            None,
            "eval_bot",
            "v1",
            now_ms + i * 10,
            100,
            TraceStatus::Ok,
        );
        batch.traces.push(tr);
        batch.spans.extend(sps);
    }
    let (st, _) = h.ingest_plst(INGEST_KEY, &batch).await;
    assert_eq!(st, StatusCode::OK);

    // Also explicitly trigger Session-level and Span-level evaluations
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluate"),
            Some(ROOT),
            Some(json!({
                "target_type": "session",
                "session_id": "sess_eval_1",
                "evaluator_ids": ["ev_sess_det"]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluate"),
            Some(ROOT),
            Some(json!({
                "target_type": "span",
                "session_id": "sess_eval_1",
                "trace_id": "tr_eval_00",
                "span_id": "tr_eval_00_sp_llm",
                "evaluator_ids": ["ev_span_regex"]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    // Wait for all 16 LLM calls + deterministic evaluations + tr_eval_00's 2 valid trace evals to settle
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if mock_state.calls.load(Ordering::SeqCst) >= 16 {
            let sess_evals = h
                .meta
                .list_session_evaluations(DEFAULT_PROJECT, "sess_eval_1")
                .await
                .unwrap();
            let tr0_evals = h
                .meta
                .list_trace_evaluations(DEFAULT_PROJECT, "tr_eval_00")
                .await
                .unwrap();
            let has_sess = sess_evals.iter().any(|e| {
                e.target_type == EvalTargetType::Session && e.evaluator_id == "ev_sess_det"
            });
            let has_span = sess_evals.iter().any(|e| {
                e.target_type == EvalTargetType::Span && e.evaluator_id == "ev_span_regex"
            });
            let tr0_valid = tr0_evals
                .iter()
                .filter(|e| e.target_type == EvalTargetType::Trace && e.error.is_none())
                .count();
            if has_sess && has_span && tr0_valid >= 2 && sess_evals.len() >= 34 {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for eval worker; llm calls={}",
            mock_state.calls.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let peak = mock_state.peak_in_flight.load(Ordering::SeqCst);
    assert!(
        (2..=MAX_CONCURRENT_LLM_JUDGES).contains(&peak),
        "expected concurrent LLM calls in [2, {MAX_CONCURRENT_LLM_JUDGES}], got {peak}"
    );

    // Trigger a malformed LLM judge response -> persisted with error.is_some(), excluded from rollup
    mock_state.malformed_mode.store(true, Ordering::SeqCst);
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluate"),
            Some(ROOT),
            Some(json!({
                "target_type": "trace",
                "trace_id": "tr_eval_00",
                "evaluator_ids": ["ev_trace_llm"]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let tr0_evals = h
            .meta
            .list_trace_evaluations(DEFAULT_PROJECT, "tr_eval_00")
            .await
            .unwrap();
        if tr0_evals.iter().any(|e| e.error.is_some()) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline2);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Verify agent_eval_rollups ignored the error evaluation (AND error IS NULL):
    // tr_eval_00 has ev_trace_det (1.0) + valid ev_trace_llm (0.9) => avg = 0.95, count = 2
    let rollups = h
        .meta
        .get_eval_rollups_batch(
            DEFAULT_PROJECT,
            EvalTargetType::Trace,
            &["tr_eval_00".to_string()],
        )
        .await
        .unwrap();
    let r0 = rollups
        .get(&(EvalTargetType::Trace, "tr_eval_00".to_string()))
        .unwrap();
    assert_eq!(
        r0.eval_count, 2,
        "errored LLM eval must be excluded from agent_eval_rollups"
    );
    assert!((r0.avg_score - 0.95).abs() < 1e-6);

    // Enqueue one final job right before watch::channel shutdown to verify drain on shutdown
    mock_state.malformed_mode.store(false, Ordering::SeqCst);
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_eval_01/evaluate"),
            Some(ROOT),
            Some(json!({ "evaluator_ids": ["ev_trace_llm"] })),
        )
        .await;
    assert_eq!(st, StatusCode::ACCEPTED);

    let calls_before_shutdown = mock_state.calls.load(Ordering::SeqCst);
    shutdown_tx.send(true).unwrap();
    worker_handle.await.unwrap();
    assert!(
        mock_state.calls.load(Ordering::SeqCst) > calls_before_shutdown,
        "eval worker must drain buffered jobs on watch::channel shutdown"
    );

    server_handle.abort();
}

// ===========================================================================
// 5, 6 & 12. First-Class Sessions, 3-Level Erasure, Compaction Stripping & Retention
// ===========================================================================

#[tokio::test]
async fn test_04_sessions_three_level_erasure_compaction_stripping_and_retention() {
    let h = TestHarness::new(0).await;
    let now_ms = Utc::now().timestamp_millis();

    // Enable capture_user_id so user-level erasure can match user_id
    let (st, _) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(ROOT),
            Some(json!({ "capture_user_id": true, "redact_pii": true, "retention_days": 30 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Ingest: user_a has sess_1 (tr_1a in PLSS, tr_1b in L0) and sess_2 (tr_2a in PLSS);
    // user_b has sess_3 (tr_3a in PLSS).
    let (tr_1a, sps_1a) = make_sample_trace(
        "tr_1a",
        Some("sess_1"),
        Some("user_a"),
        "sess_bot",
        "v1",
        now_ms,
        100,
        TraceStatus::Ok,
    );
    let (tr_2a, sps_2a) = make_sample_trace(
        "tr_2a",
        Some("sess_2"),
        Some("user_a"),
        "sess_bot",
        "v1",
        now_ms + 10,
        150,
        TraceStatus::Error,
    );
    let (tr_3a, sps_3a) = make_sample_trace(
        "tr_3a",
        Some("sess_3"),
        Some("user_b"),
        "sess_bot",
        "v1",
        now_ms + 20,
        200,
        TraceStatus::Ok,
    );
    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![tr_1a, tr_2a, tr_3a],
                spans: [sps_1a, sps_2a, sps_3a].concat(),
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    h.ctrl.agent_store().flush_all_l0().await.unwrap();

    // Leave tr_1b in L0 memtable (testing hybrid L0 + PLSS session hydration)
    let (tr_1b, sps_1b) = make_sample_trace(
        "tr_1b",
        Some("sess_1"),
        Some("user_a"),
        "sess_bot",
        "v1",
        now_ms + 30,
        120,
        TraceStatus::Error,
    );
    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![tr_1b],
                spans: sps_1b,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Insert SQL evaluations at Session, Trace, and Span levels
    for ev_body in [
        json!({
            "id": "ev_s1_sess",
            "target_type": "session",
            "session_id": "sess_1",
            "evaluator_id": "manual",
            "evaluator_name": "Session Quality",
            "score": 0.85,
            "passed": true
        }),
        json!({
            "id": "ev_tr1a",
            "target_type": "trace",
            "session_id": "sess_1",
            "trace_id": "tr_1a",
            "evaluator_id": "manual",
            "evaluator_name": "Trace Quality",
            "score": 0.9,
            "passed": true
        }),
        json!({
            "id": "ev_tr1b",
            "target_type": "trace",
            "session_id": "sess_1",
            "trace_id": "tr_1b",
            "evaluator_id": "manual",
            "evaluator_name": "Trace Quality",
            "score": 0.5,
            "passed": false
        }),
        json!({
            "id": "ev_tr2a",
            "target_type": "trace",
            "session_id": "sess_2",
            "trace_id": "tr_2a",
            "evaluator_id": "manual",
            "evaluator_name": "Trace Quality",
            "score": 0.4,
            "passed": false
        }),
        json!({
            "id": "ev_tr3a",
            "target_type": "trace",
            "session_id": "sess_3",
            "trace_id": "tr_3a",
            "evaluator_id": "manual",
            "evaluator_name": "Trace Quality",
            "score": 1.0,
            "passed": true
        }),
    ] {
        let (st, v) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluations"),
                Some(ROOT),
                Some(ev_body),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "create eval failed: {v}");
    }

    // 1. First-class Sessions list + filter + cursor pagination + detail
    let (st, page1) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/sessions?limit=2&user_id=user_a"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{page1}");
    assert_eq!(page1["total"], 2);
    assert_eq!(page1["sessions"].as_array().unwrap().len(), 2);

    let (st, sess1) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/sessions/sess_1"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{sess1}");
    assert_eq!(sess1["summary"]["trace_count"], 2);
    assert_eq!(sess1["summary"]["error_count"], 1);
    assert!((sess1["summary"]["avg_eval_score"].as_f64().unwrap() - 0.85).abs() < 1e-6);
    assert_eq!(sess1["traces"].as_array().unwrap().len(), 2);
    assert_eq!(sess1["evaluations"].as_array().unwrap().len(), 3);

    // 2. Level 1 Erasure: Trace (`DELETE .../traces/tr_1a`)
    let (st, del_tr) = h
        .send_json(
            "DELETE",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_1a"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{del_tr}");
    assert_eq!(del_tr["tombstone"]["level"], "trace");
    assert_eq!(del_tr["purged_evaluations"], 1);

    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_1a"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // tr_1b in sess_1 still visible
    let (st, sess1_after_tr) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/sessions/sess_1"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(sess1_after_tr["traces"].as_array().unwrap().len(), 1);
    assert_eq!(sess1_after_tr["traces"][0]["trace_id"], "tr_1b");

    // 3. Level 2 Erasure: Session (`DELETE .../traces/any?level=session&target_id=sess_1`)
    let (st, del_sess) = h
        .send_json(
            "DELETE",
            &format!(
                "/v1/projects/{DEFAULT_PROJECT}/agent/traces/any?level=session&target_id=sess_1"
            ),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{del_sess}");
    assert_eq!(del_sess["tombstone"]["level"], "session");
    assert_eq!(del_sess["purged_evaluations"], 2); // ev_s1_sess + ev_tr1b

    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/sessions/sess_1"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_1b"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // 4. Level 3 Erasure: User (`POST .../erasure` level="user", target_id="user_a")
    let (st, del_user) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/erasure"),
            Some(ROOT),
            Some(json!({ "level": "user", "target_id": "user_a" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{del_user}");
    assert_eq!(del_user["tombstone"]["level"], "user");
    assert_eq!(del_user["purged_evaluations"], 1); // ev_tr2a

    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_2a"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // User B (tr_3a / sess_3) remains completely intact
    let (st, tr3) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_3a"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{tr3}");

    // 5. Compaction strips erased traces permanently while keeping user_b intact
    h.ctrl.agent_store().flush_all_l0().await.unwrap();
    let tombstones = h
        .meta
        .list_active_erasure_tombstones(DEFAULT_PROJECT, Utc::now().timestamp_millis())
        .await
        .unwrap();
    h.ctrl
        .agent_store()
        .compact_and_gc(Some(DEFAULT_PROJECT), &tombstones)
        .await
        .unwrap();

    // Query directly with EMPTY tombstones slice -> erased traces must already be physically absent
    assert!(h
        .ctrl
        .agent_store()
        .get_trace_detail(DEFAULT_PROJECT, "tr_1a", &[])
        .await
        .unwrap()
        .is_none());
    assert!(h
        .ctrl
        .agent_store()
        .get_trace_detail(DEFAULT_PROJECT, "tr_2a", &[])
        .await
        .unwrap()
        .is_none());
    assert!(h
        .ctrl
        .agent_store()
        .get_trace_detail(DEFAULT_PROJECT, "tr_3a", &[])
        .await
        .unwrap()
        .is_some());

    // 6. Retention pruning (`ControlPlaneService::prune_expired_agent_data()`)
    let (st, _) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(ROOT),
            Some(json!({ "capture_user_id": true, "redact_pii": true, "retention_days": 2 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let ten_days_ago_ms = (Utc::now() - chrono::Duration::days(10)).timestamp_millis();
    let (old_tr, old_sps) = make_sample_trace(
        "tr_old_ret",
        Some("sess_old_ret"),
        Some("user_b"),
        "ret_bot",
        "v1",
        ten_days_ago_ms,
        90,
        TraceStatus::Ok,
    );
    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![old_tr],
                spans: old_sps,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    h.ctrl.agent_store().flush_all_l0().await.unwrap();

    // Insert old SQL evaluation + expired erasure tombstone directly via MetaStore
    h.meta
        .insert_agent_evaluation(&AgentEvaluation {
            id: "ev_old_expired".to_string(),
            project_id: DEFAULT_PROJECT.to_string(),
            target_type: EvalTargetType::Trace,
            session_id: "sess_old_ret".to_string(),
            trace_id: Some("tr_old_ret".to_string()),
            span_id: None,
            experiment_run_id: None,
            evaluator_id: "manual".to_string(),
            evaluator_name: "Manual".to_string(),
            evaluator_type: EvaluatorType::Custom,
            evaluator_version: "1.0.0".to_string(),
            score: 0.7,
            score_type: ScoreType::Numeric,
            passed: Some(true),
            label: None,
            reasoning: None,
            execution_ms: 0,
            error: None,
            metadata: json!({}),
            created_at_ms: ten_days_ago_ms,
        })
        .await
        .unwrap();

    h.meta
        .insert_erasure_tombstone(&AgentErasureTombstone {
            id: "ers_expired".to_string(),
            project_id: DEFAULT_PROJECT.to_string(),
            level: ErasureLevel::Trace,
            target_id: "tr_never_existed".to_string(),
            created_at: ten_days_ago_ms,
            expires_at: Some(now_ms - 5_000),
        })
        .await
        .unwrap();

    let stats = h.ctrl.prune_expired_agent_data().await;
    assert!(stats.segments_pruned >= 1);
    assert_eq!(stats.evaluations_pruned, 1);
    assert!(stats.tombstones_pruned >= 1);

    // Old trace gone, current trace preserved
    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_old_ret"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_3a"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
}

// ===========================================================================
// 7, 8 & 13. 4-Tier RBAC, Tenant Isolation & 4 Project-Bound MCP Tools
// ===========================================================================

#[tokio::test]
async fn test_05_rbac_four_tier_tenant_isolation_and_mcp_tools() {
    let h = TestHarness::new(900).await;
    let now_ms = Utc::now().timestamp_millis();

    let owner = h.signup_owner("owner@acme.test").await;
    let admin = h
        .invite_and_accept(&owner, "admin@acme.test", "admin")
        .await;
    let member = h
        .invite_and_accept(&owner, "member@acme.test", "member")
        .await;
    let viewer = h
        .invite_and_accept(&owner, "viewer@acme.test", "viewer")
        .await;

    // Seed a trace + evaluation in proj_alpha
    let (tr, sps) = make_sample_trace(
        "tr_rbac_1",
        Some("sess_rbac_1"),
        None,
        "rbac_bot",
        "v1",
        now_ms,
        110,
        TraceStatus::Ok,
    );
    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![tr],
                spans: sps,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // 1. Viewer: 200 on all agent GETs, 403 on all agent mutations
    for get_uri in [
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/sessions"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/sessions/sess_rbac_1"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_rbac_1"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/analytics"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/compare"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluators"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluations"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/experiments"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts/events"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
        format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing"),
    ] {
        let (st, v) = h.send_json("GET", &get_uri, Some(&viewer), None).await;
        assert_eq!(st, StatusCode::OK, "Viewer GET {get_uri} failed: {v}");
    }

    for (method, uri, body) in [
        (
            "POST",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluators"),
            Some(json!({"name": "x", "kind": "no_errors"})),
        ),
        (
            "POST",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets"),
            Some(json!({"name": "ds"})),
        ),
        (
            "DELETE",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_rbac_1"),
            None,
        ),
        (
            "PUT",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(json!({"capture_user_id": false})),
        ),
        (
            "POST",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing"),
            Some(
                json!({"provider": "openai", "model_pattern": "x", "input_per_1m_usd": 1.0, "output_per_1m_usd": 2.0}),
            ),
        ),
    ] {
        let (st, _) = h.send_json(method, &uri, Some(&viewer), body).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "Viewer {method} {uri} must 403");
    }

    // 2. Member: succeeds on Member writes, 403 on Admin-only routes
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluators"),
            Some(&member),
            Some(json!({ "id": "ev_m", "name": "Member Eval", "kind": "no_errors" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets"),
            Some(&member),
            Some(json!({ "id": "ds_a", "name": "DS A" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_rbac_1/promote"),
            Some(&member),
            Some(json!({ "dataset_id": "ds_a" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    for (method, uri, body) in [
        (
            "DELETE",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_rbac_1"),
            None,
        ),
        (
            "POST",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/erasure"),
            Some(json!({"level": "trace", "target_id": "tr_rbac_1"})),
        ),
        (
            "PUT",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(json!({"capture_user_id": false})),
        ),
        (
            "POST",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing"),
            Some(
                json!({"provider": "openai", "model_pattern": "x", "input_per_1m_usd": 1.0, "output_per_1m_usd": 2.0}),
            ),
        ),
        (
            "DELETE",
            format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing/prc_nonexistent"),
            None,
        ),
    ] {
        let (st, _) = h.send_json(method, &uri, Some(&member), body).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "Member {method} {uri} must 403");
    }

    // 3. Admin & Owner succeed on Admin-only routes
    let (st, _) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/privacy"),
            Some(&admin),
            Some(json!({ "capture_user_id": false, "redact_pii": true })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, prc) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing"),
            Some(&owner),
            Some(json!({
                "id": "prc_rbac",
                "provider": "openai",
                "model_pattern": "gpt-4o*",
                "input_per_1m_usd": 2.5,
                "output_per_1m_usd": 10.0
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{prc}");
    let (st, _) = h
        .send_json(
            "DELETE",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/pricing/prc_rbac"),
            Some(&admin),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // 4. Tenant Isolation + Cross-Dataset Item Isolation (Caveats 2 & 3)
    let (st, proj_b_resp) = h
        .send_json(
            "POST",
            "/v1/projects",
            Some(ROOT),
            Some(json!({ "name": "Beta Tenant" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{proj_b_resp}");
    let proj_b = proj_b_resp["project"]["id"].as_str().unwrap().to_string();

    let (st, alpha_sk_resp) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/api-keys"),
            Some(ROOT),
            Some(json!({ "label": "Alpha Admin", "role": "admin" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{alpha_sk_resp}");
    let alpha_sk = alpha_sk_resp["plaintext_key"].as_str().unwrap();
    assert!(alpha_sk.starts_with("sk_"));

    // Cross-project sk_live_... -> 403 Forbidden
    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{proj_b}/agent/traces"),
            Some(alpha_sk),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Cross-project dataset/experiment IDOR -> 404 Not Found
    let (st, _) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{proj_b}/agent/datasets/ds_a/items"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Cross-dataset item delete & upsert within same project (Caveats 2 & 3 polish fixes)
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets"),
            Some(ROOT),
            Some(json!({ "id": "ds_b", "name": "DS B" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets/ds_b/items"),
            Some(ROOT),
            Some(json!({ "id": "item_in_b", "name": "B1", "input": {"q": "b"} })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Deleting item_in_b via ds_a -> 404 Not Found (item in ds_b untouched)
    let (st, _) = h
        .send_json(
            "DELETE",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets/ds_a/items/item_in_b"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Upserting item_in_b via ds_a -> 409 Conflict (item in ds_b untouched)
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets/ds_a/items"),
            Some(ROOT),
            Some(json!({ "id": "item_in_b", "name": "Overwrite!", "input": {"q": "hijack"} })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);

    let (st, ds_b_items) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets/ds_b/items"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ds_b_items["items"][0]["name"], "B1");

    // 5. 4 Project-Bound MCP Tools on POST /v1/mcp
    let (st, tools_list) = h
        .send_json(
            "POST",
            "/v1/mcp",
            Some(alpha_sk),
            Some(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {}
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let tools = tools_list["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 13, "expected 9 analytics + 4 agent MCP tools");
    let tool_names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    for expected in [
        "list_agent_traces",
        "get_agent_trace",
        "query_agent_analytics",
        "list_agent_evaluations",
    ] {
        assert!(
            tool_names.contains(&expected),
            "missing MCP tool {expected}"
        );
    }

    // Create an evaluation on tr_rbac_1 for list_agent_evaluations
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluations"),
            Some(ROOT),
            Some(json!({
                "id": "ev_mcp_1",
                "target_type": "trace",
                "session_id": "sess_rbac_1",
                "trace_id": "tr_rbac_1",
                "evaluator_id": "ev_m",
                "evaluator_name": "Member Eval",
                "score": 0.92,
                "passed": true
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Call all 4 agent MCP tools
    for (id, name, args) in [
        (2, "list_agent_traces", json!({ "limit": 10 })),
        (3, "get_agent_trace", json!({ "trace_id": "tr_rbac_1" })),
        (4, "query_agent_analytics", json!({ "days": 7 })),
        (
            5,
            "list_agent_evaluations",
            json!({ "trace_id": "tr_rbac_1", "target_type": "trace" }),
        ),
    ] {
        let (st, resp) = h
            .send_json(
                "POST",
                "/v1/mcp",
                Some(alpha_sk),
                Some(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "tools/call",
                    "params": { "name": name, "arguments": args }
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
        assert!(
            resp["result"]["isError"].is_null(),
            "MCP {name} failed: {resp}"
        );
        assert!(resp["result"]["structuredContent"].is_object());
    }

    // Cross-project override via arguments.project_id -> -32602
    let (st, cross_mcp) = h
        .send_json(
            "POST",
            "/v1/mcp",
            Some(alpha_sk),
            Some(json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": { "name": "list_agent_traces", "arguments": { "project_id": proj_b } }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(cross_mcp["error"]["code"], -32602);

    // Strict target_type validation in list_agent_evaluations -> -32602 (Caveat 4 polish fix)
    let (st, bad_tt) = h
        .send_json(
            "POST",
            "/v1/mcp",
            Some(alpha_sk),
            Some(json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "list_agent_evaluations",
                    "arguments": { "trace_id": "tr_rbac_1", "target_type": "bogus" }
                }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        bad_tt["error"]["code"], -32602,
        "invalid target_type must return -32602, got {bad_tt}"
    );

    // Existing analytics tool error stays `isError: true` (not -32602)
    let (st, bad_insights) = h
        .send_json(
            "POST",
            "/v1/mcp",
            Some(alpha_sk),
            Some(json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": { "name": "query_insights", "arguments": { "metric": "bogus_metric" } }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(bad_insights["result"]["isError"], true);

    // Erasure hides trace from MCP tools
    let (st, _) = h
        .send_json(
            "DELETE",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_rbac_1"),
            Some(&admin),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, after_erase_mcp) = h
        .send_json(
            "POST",
            "/v1/mcp",
            Some(alpha_sk),
            Some(json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": { "name": "get_agent_trace", "arguments": { "trace_id": "tr_rbac_1" } }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(after_erase_mcp["result"]["isError"], true);
}

// ===========================================================================
// 9, 10 & 11. Analytics, Compare, Evaluators, Datasets, Experiments & Alerts
// ===========================================================================

#[derive(Clone)]
struct WebhookCaptureState {
    received: Arc<tokio::sync::Mutex<Vec<(String, String, Value)>>>,
}

async fn webhook_capture_handler(
    State(st): State<WebhookCaptureState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let ts = headers
        .get("x-pulse-timestamp")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let sig = headers
        .get("x-pulse-signature-256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let val: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    st.received.lock().await.push((ts, sig, val));
    StatusCode::OK
}

#[tokio::test]
async fn test_06_analytics_compare_evaluators_datasets_experiments_and_alerts() {
    let h = TestHarness::new(900).await;
    let now_ms = Utc::now().timestamp_millis();

    // 1. Ingest n=25 baseline (v1), n=25 candidate (v2, slower & costlier), n=5 (v3)
    let mut batch = TraceIngestBatch::default();
    for i in 0..25 {
        let (tr, sps) = make_sample_trace(
            &format!("tr_v1_{i:02}"),
            Some(&format!("sess_v1_{i:02}")),
            None,
            "cmp_bot",
            "v1",
            now_ms - 10_000 + i * 10,
            100 + (i % 5) * 10,
            TraceStatus::Ok,
        );
        batch.traces.push(tr);
        batch.spans.extend(sps);
    }
    for i in 0..25 {
        let (mut tr, mut sps) = make_sample_trace(
            &format!("tr_v2_{i:02}"),
            Some(&format!("sess_v2_{i:02}")),
            None,
            "cmp_bot",
            "v2",
            now_ms - 5_000 + i * 10,
            800 + (i % 5) * 50,
            TraceStatus::Ok,
        );
        sps[0].input_tokens = 2000;
        sps[0].output_tokens = 1000;
        sps[0].total_tokens = 3000;
        tr.input_tokens = 2000;
        tr.output_tokens = 1000;
        tr.total_tokens = 3000;
        batch.traces.push(tr);
        batch.spans.extend(sps);
    }
    for i in 0..5 {
        let (tr, sps) = make_sample_trace(
            &format!("tr_v3_{i:02}"),
            Some("sess_v3"),
            None,
            "cmp_bot",
            "v3",
            now_ms - 1_000 + i * 10,
            120,
            TraceStatus::Ok,
        );
        batch.traces.push(tr);
        batch.spans.extend(sps);
    }
    let (st, _) = h.ingest_plst(INGEST_KEY, &batch).await;
    assert_eq!(st, StatusCode::OK);

    // Insert evaluations for v1 (0.95) and v2 (0.55) + test Caveat 2 (409 on duplicate id) + empty error norm
    for i in 0..25 {
        let (st, _) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluations"),
                Some(ROOT),
                Some(json!({
                    "id": format!("ev_v1_{i:02}"),
                    "target_type": "trace",
                    "session_id": format!("sess_v1_{i:02}"),
                    "trace_id": format!("tr_v1_{i:02}"),
                    "evaluator_id": "manual",
                    "evaluator_name": "Manual",
                    "score": 0.95,
                    "passed": true,
                    "error": "   "
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);

        let (st, _) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluations"),
                Some(ROOT),
                Some(json!({
                    "id": format!("ev_v2_{i:02}"),
                    "target_type": "trace",
                    "session_id": format!("sess_v2_{i:02}"),
                    "trace_id": format!("tr_v2_{i:02}"),
                    "evaluator_id": "manual",
                    "evaluator_name": "Manual",
                    "score": 0.55,
                    "passed": false
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
    }

    // Duplicate caller-supplied evaluation id -> 409 Conflict (Caveat 2 polish fix!)
    let (st, dup_resp) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluations"),
            Some(ROOT),
            Some(json!({
                "id": "ev_v1_00",
                "target_type": "trace",
                "session_id": "sess_v1_00",
                "trace_id": "tr_v1_00",
                "evaluator_id": "manual",
                "evaluator_name": "Manual",
                "score": 0.1
            })),
        )
        .await;
    assert_eq!(
        st,
        StatusCode::CONFLICT,
        "duplicate evaluation id must return 409 Conflict, got {dup_resp}"
    );

    // 2. Analytics & Compare with empty ?tool_name= & ?tool_name=%20 (Caveat 1 polish fix!)
    let (st, analytics) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/analytics?tool_name=&agent_name=%20"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{analytics}");
    assert_eq!(analytics["total_executions"], 55);
    assert!(
        analytics["avg_eval_score"].as_f64().is_some(),
        "empty ?tool_name= must NOT skip SQL eval-rollup enrichment: {analytics}"
    );
    assert!(analytics["latency"]["p95_ms"].as_f64().unwrap() > 0.0);
    assert!(!analytics["by_version"].as_array().unwrap().is_empty());
    assert!(!analytics["by_tool"].as_array().unwrap().is_empty());

    // Compare with n < 20 -> insufficient_data
    let (st, cmp_small) = h
        .send_json(
            "GET",
            &format!(
                "/v1/projects/{DEFAULT_PROJECT}/agent/compare?baseline_version=v1&candidate_version=v3"
            ),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{cmp_small}");
    assert_eq!(cmp_small["sufficient_samples"], false);
    assert_eq!(cmp_small["overall_verdict"], "insufficient_data");

    // Compare with n = 25 >= 20 and ?tool_name=%20 -> significant_regression + eval_score populated
    let (st, cmp_sig) = h
        .send_json(
            "GET",
            &format!(
                "/v1/projects/{DEFAULT_PROJECT}/agent/compare?baseline_version=v1&candidate_version=v2&tool_name=%20"
            ),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{cmp_sig}");
    assert_eq!(cmp_sig["sufficient_samples"], true);
    assert_eq!(cmp_sig["overall_verdict"], "significant_regression");
    assert!(cmp_sig["latency_ms"]["p_value"].as_f64().unwrap() < 0.05);
    assert!(cmp_sig["eval_score"]["sample_size_baseline"] == 25);
    assert!(cmp_sig["eval_score"]["sample_size_candidate"] == 25);

    // 3. Evaluator regex validation + Datasets (import, promote) + Experiments (runs, compare)
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/evaluators"),
            Some(ROOT),
            Some(json!({
                "name": "Bad Regex",
                "kind": "regex_match",
                "config": { "pattern": "[unclosed(" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets"),
            Some(ROOT),
            Some(json!({ "id": "ds_golden", "name": "Golden" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, imp_empty) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets/ds_golden/import"),
            Some(ROOT),
            Some(json!({ "items": [] })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(imp_empty["imported"], 0);

    let (st, imp_pop) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/datasets/ds_golden/import"),
            Some(ROOT),
            Some(json!({
                "items": [{ "name": "case1", "input": {"q": "hello"}, "expected_output": {"a": "hi"} }]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(imp_pop["imported"], 1);

    let (st, promoted) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/traces/tr_v1_00/promote"),
            Some(ROOT),
            Some(json!({ "dataset_id": "ds_golden", "name": "Promoted Case" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{promoted}");
    assert_eq!(
        promoted["expected_behavior"]["expected_tools"],
        json!(["lookup_kb"])
    );

    // Experiments + runs + compare (insufficient_data vs n >= 20)
    for (exp_id, ver) in [("exp_base", "v1"), ("exp_cand", "v2"), ("exp_small", "v3")] {
        let (st, _) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/experiments"),
                Some(ROOT),
                Some(json!({
                    "id": exp_id,
                    "name": exp_id,
                    "dataset_id": "ds_golden",
                    "agent_name": "cmp_bot",
                    "agent_version": ver,
                    "total_items": if exp_id == "exp_small" { 3 } else { 20 }
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
    }

    for i in 0..20 {
        let (st, _) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/experiments/exp_base/runs"),
                Some(ROOT),
                Some(json!({
                    "dataset_item_id": format!("it_{i}"),
                    "score": 0.92,
                    "passed": true,
                    "latency_ms": 110 + (i % 4) * 5,
                    "cost_usd": 0.01,
                    "total_tokens": 150
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);

        let (st, _) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/experiments/exp_cand/runs"),
                Some(ROOT),
                Some(json!({
                    "dataset_item_id": format!("it_{i}"),
                    "score": 0.45,
                    "passed": false,
                    "latency_ms": 900 + (i % 4) * 20,
                    "cost_usd": 0.08,
                    "total_tokens": 2500
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
    }
    for i in 0..3 {
        let (st, _) = h
            .send_json(
                "POST",
                &format!("/v1/projects/{DEFAULT_PROJECT}/agent/experiments/exp_small/runs"),
                Some(ROOT),
                Some(json!({
                    "dataset_item_id": format!("it_{i}"),
                    "score": 0.8,
                    "passed": true,
                    "latency_ms": 130
                })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
    }

    let (st, exp_get) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/experiments/exp_base"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_get["experiment"]["status"], "completed");
    assert_eq!(exp_get["experiment"]["completed_items"], 20);
    assert_eq!(exp_get["experiment"]["passed_items"], 20);

    let (st, exp_cmp_small) = h
        .send_json(
            "GET",
            &format!(
                "/v1/projects/{DEFAULT_PROJECT}/agent/experiments/compare?baseline=exp_base&candidate=exp_small"
            ),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_cmp_small["overall_verdict"], "insufficient_data");

    let (st, exp_cmp_sig) = h
        .send_json(
            "GET",
            &format!(
                "/v1/projects/{DEFAULT_PROJECT}/agent/experiments/compare?baseline=exp_base&candidate=exp_cand"
            ),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_cmp_sig["sufficient_samples"], true);
    assert_eq!(exp_cmp_sig["overall_verdict"], "significant_regression");

    // 4. Alerts, Cooldown Dedup, Recovery Event & HMAC Webhook Verification
    let _env_guard = ENV_LOCK.lock().await;
    let _clear_priv = EnvGuard::set(&[("PULSE_ALLOW_PRIVATE_ENDPOINTS", None)]);

    // SSRF rejects private webhook URL when PULSE_ALLOW_PRIVATE_ENDPOINTS is unset
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts"),
            Some(ROOT),
            Some(json!({
                "name": "SSRF Check",
                "metric": "failure_rate",
                "operator": "gt",
                "threshold": 0.2,
                "webhook_url": "http://127.0.0.1:9999/hook"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Spawn loopback webhook receiver
    let wh_state = WebhookCaptureState {
        received: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    };
    let wh_app = Router::new()
        .route("/hook", post(webhook_capture_handler))
        .with_state(wh_state.clone());
    let wh_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wh_url = format!("http://{}/hook", wh_listener.local_addr().unwrap());
    let wh_server = tokio::spawn(async move {
        let _ = axum::serve(wh_listener, wh_app).await;
    });

    let _allow_priv = EnvGuard::set(&[("PULSE_ALLOW_PRIVATE_ENDPOINTS", Some("1"))]);
    let signing_secret = "whsec_test_agent_signing_secret_0123456789";

    let (st, wh_byok) = h
        .send_json(
            "PUT",
            &format!("/v1/projects/{DEFAULT_PROJECT}/byok"),
            Some(ROOT),
            Some(json!({
                "category": "webhook",
                "provider": "custom_webhook",
                "secret": wh_url,
                "config_json": { "signing_secret": signing_secret }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{wh_byok}");

    // Create failure_rate > 0.2 alert scoped to agent_name = "alert_bot"
    let (st, rule_val) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts"),
            Some(ROOT),
            Some(json!({
                "id": "aal_fail",
                "name": "High Failure Rate",
                "agent_name": "alert_bot",
                "metric": "failure_rate",
                "operator": "gt",
                "threshold": 0.2,
                "window_minutes": 15,
                "cooldown_minutes": 10,
                "webhook_url": wh_url
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{rule_val}");

    // Ingest 1 failing trace for alert_bot -> failure_rate = 1.0 -> ok -> firing
    let (f_tr, f_sps) = make_sample_trace(
        "tr_alert_fail",
        None,
        None,
        "alert_bot",
        "v1",
        Utc::now().timestamp_millis(),
        100,
        TraceStatus::Error,
    );
    let (st, _) = h
        .ingest_plst(
            INGEST_KEY,
            &TraceIngestBatch {
                traces: vec![f_tr],
                spans: f_sps,
            },
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    h.ctrl.evaluate_agent_alerts().await;
    let (st, ev1) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts/events"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ev1["events"].as_array().unwrap().len(), 1);

    // Immediate re-evaluation while firing -> 0 new events (deduplicated)
    h.ctrl.evaluate_agent_alerts().await;
    let (st, ev_dup) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts/events"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ev_dup["events"].as_array().unwrap().len(), 1);

    // Ingest 9 passing traces -> failure_rate = 1/10 = 0.1 <= 0.2 -> firing -> ok (resolved event)
    let mut ok_batch = TraceIngestBatch::default();
    let base_ts = Utc::now().timestamp_millis();
    for i in 0..9 {
        let (tr, sps) = make_sample_trace(
            &format!("tr_alert_ok_{i}"),
            None,
            None,
            "alert_bot",
            "v1",
            base_ts + i,
            80,
            TraceStatus::Ok,
        );
        ok_batch.traces.push(tr);
        ok_batch.spans.extend(sps);
    }
    let (st, _) = h.ingest_plst(INGEST_KEY, &ok_batch).await;
    assert_eq!(st, StatusCode::OK);

    h.ctrl.evaluate_agent_alerts().await;

    // Verify both events in SQL + HMAC signatures on loopback receiver
    let (st, events_resp) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts/events"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{events_resp}");
    let events = events_resp["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert!(events
        .iter()
        .any(|e| e["event_type"] == "triggered" && e["delivered"] == true));
    assert!(events
        .iter()
        .any(|e| e["event_type"] == "resolved" && e["delivered"] == true));

    let captured = wh_state.received.lock().await.clone();
    assert_eq!(captured.len(), 2);
    for (ts, sig_hdr, payload) in &captured {
        let body_bytes = serde_json::to_vec(payload).unwrap();
        let key = hmac::Key::new(hmac::HMAC_SHA256, signing_secret.as_bytes());
        let signed_msg = format!("{ts}.{}", String::from_utf8_lossy(&body_bytes));
        let expected_sig = format!(
            "sha256={}",
            hex::encode(hmac::sign(&key, signed_msg.as_bytes()).as_ref())
        );
        assert_eq!(
            sig_hdr, &expected_sig,
            "HMAC-SHA256 webhook signature mismatch"
        );
    }

    // Delivery error tracking when webhook endpoint is unreachable
    let (st, _) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts"),
            Some(ROOT),
            Some(json!({
                "id": "aal_err",
                "name": "Unreachable Hook",
                "agent_name": "alert_bot",
                "metric": "failure_rate",
                "operator": "gt",
                "threshold": 0.05,
                "window_minutes": 15,
                "cooldown_minutes": 0,
                "webhook_url": "http://127.0.0.1:9/unreachable"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let (st, eval_one) = h
        .send_json(
            "POST",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts/aal_err/evaluate"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{eval_one}");
    let (st, events_after_err) = h
        .send_json(
            "GET",
            &format!("/v1/projects/{DEFAULT_PROJECT}/agent/alerts/events?rule_id=aal_err"),
            Some(ROOT),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(events_after_err["events"][0]["delivered"], false);
    assert!(events_after_err["events"][0]["delivery_error"].is_string());

    wh_server.abort();
}
