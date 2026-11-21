//! `/metrics` (Prometheus text format) and `/readyz`.
//!
//! Request metrics are labelled by the route template (`/v1/projects/:id/...`), never the raw path,
//! and carry no project ids, so cardinality stays fixed and nothing tenant-specific is exposed.
//! Set `PULSE_METRICS_TOKEN` to require `Authorization: Bearer <token>` on `/metrics`.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use object_store::path::Path as ObjectPath;
use pulse_control::ControlPlaneService;
use pulse_exec::QueryEngine;
use pulse_ingest::IngestService;
use pulse_storage::StorageEngine;

/// Upper bounds (seconds) of the request-latency histogram.
const BUCKETS_S: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0, 30.0,
];

#[derive(Default)]
struct RouteStats {
    count: AtomicU64,
    sum_us: AtomicU64,
    /// Non-cumulative per-bucket counts; the last slot is `+Inf`.
    buckets: [AtomicU64; BUCKETS_S.len() + 1],
}

type Key = (String, String, u16); // (route template, method, status)

#[derive(Default)]
pub struct HttpMetrics {
    routes: RwLock<HashMap<Key, Arc<RouteStats>>>,
}

impl HttpMetrics {
    fn observe(&self, key: Key, took: Duration) {
        let existing = self.routes.read().unwrap().get(&key).cloned();
        let stats = match existing {
            Some(s) => s,
            None => self.routes.write().unwrap().entry(key).or_default().clone(),
        };
        let secs = took.as_secs_f64();
        let slot = BUCKETS_S
            .iter()
            .position(|&le| secs <= le)
            .unwrap_or(BUCKETS_S.len());
        stats.count.fetch_add(1, Ordering::Relaxed);
        stats
            .sum_us
            .fetch_add(took.as_micros() as u64, Ordering::Relaxed);
        stats.buckets[slot].fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String) {
        out.push_str("# TYPE pulse_http_requests_total counter\n");
        out.push_str("# TYPE pulse_http_request_duration_seconds histogram\n");
        let routes = self.routes.read().unwrap();
        let mut keys: Vec<&Key> = routes.keys().collect();
        keys.sort();
        for key @ (route, method, status) in keys {
            let s = &routes[key];
            let labels = format!(
                "route=\"{}\",method=\"{method}\",status=\"{status}\"",
                escape(route)
            );
            let count = s.count.load(Ordering::Relaxed);
            let _ = writeln!(out, "pulse_http_requests_total{{{labels}}} {count}");
            let mut cum = 0u64;
            for (i, le) in BUCKETS_S.iter().enumerate() {
                cum += s.buckets[i].load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "pulse_http_request_duration_seconds_bucket{{{labels},le=\"{le}\"}} {cum}"
                );
            }
            let _ = writeln!(
                out,
                "pulse_http_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {count}"
            );
            let _ = writeln!(
                out,
                "pulse_http_request_duration_seconds_sum{{{labels}}} {}",
                s.sum_us.load(Ordering::Relaxed) as f64 / 1e6
            );
            let _ = writeln!(
                out,
                "pulse_http_request_duration_seconds_count{{{labels}}} {count}"
            );
        }
    }
}

fn escape(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Records every request by route template; requests no route matched (the web app) are `other`.
pub async fn track(State(m): State<Arc<HttpMetrics>>, req: Request, next: Next) -> Response {
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "other".to_string(), |p| p.as_str().to_string());
    let method = req.method().as_str().to_string();
    let t0 = Instant::now();
    let resp = next.run(req).await;
    m.observe((route, method, resp.status().as_u16()), t0.elapsed());
    resp
}

/// Everything `/metrics` and `/readyz` read.
#[derive(Clone)]
pub struct Probe {
    pub http: Arc<HttpMetrics>,
    pub ready: Arc<AtomicBool>,
    pub control: Arc<ControlPlaneService>,
    pub ingest: Arc<IngestService>,
    pub storage: Arc<StorageEngine>,
    pub qengine: Arc<QueryEngine>,
    pub token: Option<String>,
}

pub fn router(probe: Probe) -> Router {
    Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/readyz", get(handle_readyz))
        .with_state(probe)
}

async fn handle_metrics(State(p): State<Probe>, headers: HeaderMap) -> Response {
    if let Some(token) = &p.token {
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        let ok = presented
            .is_some_and(|t| constant_time_eq::constant_time_eq(t.as_bytes(), token.as_bytes()));
        if !ok {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let mut out = String::with_capacity(8 * 1024);
    p.http.render(&mut out);

    let ing = p.ingest.metrics();
    let mut counter = |name: &str, help: &str, v: u64| {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}"
        );
    };
    counter(
        "pulse_ingest_events_accepted_total",
        "Events stored.",
        ing.accepted_total.load(Ordering::Relaxed),
    );
    counter(
        "pulse_ingest_events_deduped_total",
        "Events dropped as retries of ones already stored.",
        ing.deduped_total.load(Ordering::Relaxed),
    );
    counter(
        "pulse_ingest_events_dropped_total",
        "Events rejected by validation or schema limits.",
        ing.dropped_total.load(Ordering::Relaxed),
    );
    counter(
        "pulse_ingest_batches_total",
        "Ingest batches processed.",
        ing.batches_total.load(Ordering::Relaxed),
    );
    let agent = p.control.agent_metrics();
    for (name, help, v) in [
        (
            "pulse_agent_traces_ingested_total",
            "Agent traces stored.",
            &agent.traces_ingested,
        ),
        (
            "pulse_agent_spans_ingested_total",
            "Agent spans stored.",
            &agent.spans_ingested,
        ),
        (
            "pulse_agent_blobs_deduped_total",
            "Agent payload blobs deduplicated.",
            &agent.blobs_deduped,
        ),
        (
            "pulse_agent_evals_executed_total",
            "Agent evaluations executed.",
            &agent.evals_executed,
        ),
        (
            "pulse_agent_eval_queue_dropped_total",
            "Agent eval jobs dropped because the queue was full.",
            &agent.eval_queue_dropped,
        ),
    ] {
        counter(name, help, v.load(Ordering::Relaxed));
    }
    counter(
        "pulse_queries_executed_total",
        "Analytics queries completed.",
        p.qengine.queries_executed(),
    );

    let errs = p.storage.maintenance_errors();
    out.push_str(
        "# HELP pulse_maintenance_errors_total Background flush/compaction work that failed.\n\
         # TYPE pulse_maintenance_errors_total counter\n",
    );
    for (kind, c) in [
        ("identity_flush", &errs.identity_flush),
        ("flush", &errs.flush),
        ("compaction", &errs.compaction),
        ("manifest_refresh", &errs.manifest_refresh),
    ] {
        let _ = writeln!(
            out,
            "pulse_maintenance_errors_total{{kind=\"{kind}\"}} {}",
            c.load(Ordering::Relaxed)
        );
    }

    let projects = p.storage.open_projects();
    let l0_rows: usize = projects.iter().map(|ps| ps.total_l0_rows()).sum();
    let mut gauge = |name: &str, help: &str, v: f64| {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}");
    };
    gauge(
        "pulse_l0_rows",
        "Rows buffered in memory, not yet flushed to segments.",
        l0_rows as f64,
    );
    gauge(
        "pulse_open_projects",
        "Projects open on this node.",
        projects.len() as f64,
    );
    gauge(
        "pulse_last_query_latency_seconds",
        "Latency of the most recent query.",
        p.qengine.last_query_latency_us() as f64 / 1e6,
    );
    gauge(
        "pulse_ready",
        "1 while the node accepts traffic, 0 while it drains.",
        if p.ready.load(Ordering::Relaxed) {
            1.0
        } else {
            0.0
        },
    );

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}

/// 200 when the node can serve: not draining, metadata DB answers, object store reachable.
/// Load balancers should route on this; `/healthz` only says the process is up.
async fn handle_readyz(State(p): State<Probe>) -> Response {
    const CHECK_TIMEOUT: Duration = Duration::from_secs(2);
    let mut failures: Vec<String> = Vec::new();
    if !p.ready.load(Ordering::Relaxed) {
        failures.push("draining: node is shutting down".into());
    }
    match tokio::time::timeout(CHECK_TIMEOUT, p.control.ping_metadata()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "readyz metadata database check failed");
            failures.push("metadata database: unavailable".into());
        }
        Err(_) => failures.push("metadata database: timed out".into()),
    }
    let probe_path = ObjectPath::from("_pulse_readyz");
    match tokio::time::timeout(CHECK_TIMEOUT, p.storage.object_store().head(&probe_path)).await {
        Ok(Ok(_)) | Ok(Err(object_store::Error::NotFound { .. })) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "readyz object store check failed");
            failures.push("object store: unavailable".into());
        }
        Err(_) => failures.push("object store: timed out".into()),
    }
    if failures.is_empty() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ready": false, "failures": failures })),
        )
            .into_response()
    }
}
