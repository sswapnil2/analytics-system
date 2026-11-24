use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::HeaderMap;
use chrono::Utc;
use dashmap::DashMap;
use pulse_storage::{ProjectOwnerInfo, StorageError};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Notify};

use crate::forward_guard::{apply_outgoing_forward_headers, ForwardLoopGuard};
use crate::service::{
    EngageResponse, IdentifyRequest, IdentifyResponse, IngestError, ReplayIngestResponse, Result,
};
use crate::writer_log::WriterLogManager;

pub const HEADER_INTERNAL_TOKEN: &str = "x-pulse-internal-token";
pub const FORWARD_EVENTS_MAGIC: [u8; 4] = *b"PWFE";
const DEFAULT_FORWARDER_MAX_BACKOFF_MS: u64 = 5_000;

#[async_trait]
pub trait OwnerDirectory: Send + Sync {
    async fn resolve_owner(
        &self,
        project_id: &str,
    ) -> std::result::Result<Option<ProjectOwnerInfo>, String>;
    async fn refresh_owner(
        &self,
        project_id: &str,
    ) -> std::result::Result<Option<ProjectOwnerInfo>, String>;
    async fn refresh_all(&self) -> std::result::Result<(), String>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ForwarderStats {
    pub batches_ok: u64,
    pub batches_not_owner: u64,
    pub batches_retry_error: u64,
    pub retries_total: u64,
}

#[derive(Default)]
pub struct ForwarderMetrics {
    pub batches_ok: AtomicU64,
    pub batches_not_owner: AtomicU64,
    pub batches_retry_error: AtomicU64,
    pub retries_total: AtomicU64,
}

impl ForwarderMetrics {
    pub fn stats(&self) -> ForwarderStats {
        ForwarderStats {
            batches_ok: self.batches_ok.load(Ordering::Relaxed),
            batches_not_owner: self.batches_not_owner.load(Ordering::Relaxed),
            batches_retry_error: self.batches_retry_error.load(Ordering::Relaxed),
            retries_total: self.retries_total.load(Ordering::Relaxed),
        }
    }
}

pub fn encode_forward_events_envelope(
    writer_id: &str,
    project_id: &str,
    batch_offset: u64,
    plsb_bytes: &[u8],
) -> Vec<u8> {
    let wid = writer_id.as_bytes();
    let pid = project_id.as_bytes();
    let mut out = Vec::with_capacity(4 + 4 + wid.len() + 4 + pid.len() + 8 + plsb_bytes.len());
    out.extend_from_slice(&FORWARD_EVENTS_MAGIC);
    out.extend_from_slice(&(wid.len() as u32).to_le_bytes());
    out.extend_from_slice(wid);
    out.extend_from_slice(&(pid.len() as u32).to_le_bytes());
    out.extend_from_slice(pid);
    out.extend_from_slice(&batch_offset.to_le_bytes());
    out.extend_from_slice(plsb_bytes);
    out
}

pub fn decode_forward_events_envelope(
    bytes: &[u8],
) -> std::result::Result<(String, String, u64, &[u8]), String> {
    if bytes.len() < 20 || bytes[0..4] != FORWARD_EVENTS_MAGIC {
        return Err("invalid forward events envelope magic or header".to_string());
    }
    let mut pos = 4usize;
    let wid_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if wid_len == 0 || wid_len > 4096 || pos + wid_len + 12 > bytes.len() {
        return Err("invalid writer_id length in forward events envelope".to_string());
    }
    let writer_id = std::str::from_utf8(&bytes[pos..pos + wid_len])
        .map_err(|e| format!("invalid writer_id utf8: {e}"))?
        .to_string();
    pos += wid_len;
    let pid_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pid_len == 0 || pid_len > 4096 || pos + pid_len + 8 > bytes.len() {
        return Err("invalid project_id length in forward events envelope".to_string());
    }
    let project_id = std::str::from_utf8(&bytes[pos..pos + pid_len])
        .map_err(|e| format!("invalid project_id utf8: {e}"))?
        .to_string();
    pos += pid_len;
    let batch_offset = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
    pos += 8;
    Ok((writer_id, project_id, batch_offset, &bytes[pos..]))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardProfileCallsRequest {
    pub project_id: String,
    pub calls: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardProfileCallsResponse {
    pub status: String,
    pub profiles_updated: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardIdentifyRequest {
    pub project_id: String,
    pub req: IdentifyRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardJsonPayloadRequest {
    pub project_id: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardGroupEngageResponse {
    pub status: String,
    pub updated: usize,
}

struct ProjectForwarderHandle {
    notify: Notify,
}

pub struct WriterForwarder {
    writer_log: Arc<WriterLogManager>,
    owners: Arc<dyn OwnerDirectory>,
    forward_guard: Arc<ForwardLoopGuard>,
    internal_token: Option<String>,
    http_client: reqwest::Client,
    max_backoff_ms: u64,
    projects: DashMap<String, Arc<ProjectForwarderHandle>>,
    shutdown: watch::Receiver<bool>,
    metrics: Arc<ForwarderMetrics>,
    self_weak: std::sync::OnceLock<std::sync::Weak<Self>>,
}

fn max_backoff_ms_from_env() -> u64 {
    std::env::var("PULSE_FORWARDER_MAX_BACKOFF_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms >= 10)
        .unwrap_or(DEFAULT_FORWARDER_MAX_BACKOFF_MS)
}

fn build_forward_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn build_outgoing_headers(
    forward_guard: &ForwardLoopGuard,
    internal_token: Option<&str>,
    fallback_sender: &str,
) -> reqwest::header::HeaderMap {
    let mut axum_headers = HeaderMap::new();
    let sender = forward_guard
        .self_node_name
        .as_deref()
        .or(Some(fallback_sender));
    let _ = apply_outgoing_forward_headers(&mut axum_headers, None, sender, forward_guard.max_hops);
    let mut req_headers = reqwest::header::HeaderMap::new();
    for (k, v) in &axum_headers {
        if let (Ok(rk), Ok(rv)) = (
            reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            req_headers.insert(rk, rv);
        }
    }
    if let Some(tok) = internal_token {
        if let Ok(rv) = reqwest::header::HeaderValue::from_str(tok) {
            req_headers.insert(
                reqwest::header::HeaderName::from_static(HEADER_INTERNAL_TOKEN),
                rv,
            );
        }
    }
    req_headers
}

impl WriterForwarder {
    pub fn start(
        writer_log: Arc<WriterLogManager>,
        owners: Arc<dyn OwnerDirectory>,
        forward_guard: Arc<ForwardLoopGuard>,
        internal_token: Option<String>,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        let active = writer_log.list_active_projects();
        let fwd = Arc::new(Self {
            writer_log,
            owners,
            forward_guard,
            internal_token,
            http_client: build_forward_client(),
            max_backoff_ms: max_backoff_ms_from_env(),
            projects: DashMap::new(),
            shutdown,
            metrics: Arc::new(ForwarderMetrics::default()),
            self_weak: std::sync::OnceLock::new(),
        });
        let _ = fwd.self_weak.set(Arc::downgrade(&fwd));
        for pid in active {
            fwd.notify_project(&pid);
        }
        fwd
    }

    pub fn stats(&self) -> ForwarderStats {
        self.metrics.stats()
    }

    pub fn notify_project(&self, project_id: &str) {
        if *self.shutdown.borrow() {
            return;
        }
        if let Some(existing) = self.projects.get(project_id) {
            existing.notify.notify_one();
            return;
        }
        let mut inserted = false;
        let handle = self
            .projects
            .entry(project_id.to_string())
            .or_insert_with(|| {
                inserted = true;
                Arc::new(ProjectForwarderHandle {
                    notify: Notify::new(),
                })
            })
            .clone();
        if inserted {
            if let Some(weak) = self.self_weak.get().cloned() {
                let pid = project_id.to_string();
                let h = handle.clone();
                let mut shutdown_rx = self.shutdown.clone();
                tokio::spawn(async move {
                    run_project_forwarder_loop(weak, pid, h, &mut shutdown_rx).await;
                });
            }
        }
        handle.notify.notify_one();
    }
}

async fn sleep_or_shutdown(dur: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(dur) => *shutdown.borrow(),
        res = shutdown.changed() => res.is_err() || *shutdown.borrow(),
    }
}

async fn run_project_forwarder_loop(
    weak_fwd: std::sync::Weak<WriterForwarder>,
    project_id: String,
    handle: Arc<ProjectForwarderHandle>,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut backoff_ms = 0u64;
    let mut consecutive_not_owner = 0u32;

    loop {
        if *shutdown.borrow() {
            return;
        }
        let Some(fwd) = weak_fwd.upgrade() else {
            return;
        };

        let notified = handle.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let now_ms = Utc::now().timestamp_millis();
        let next = match fwd.writer_log.peek_next_batch(&project_id, now_ms) {
            Ok(Some(b)) => b,
            Ok(None) => {
                backoff_ms = 0;
                consecutive_not_owner = 0;
                drop(fwd);
                tokio::select! {
                    _ = &mut notified => continue,
                    res = shutdown.changed() => {
                        if res.is_err() || *shutdown.borrow() {
                            return;
                        }
                        continue;
                    }
                }
            }
            Err(e) => {
                tracing::error!(project_id = %project_id, error = %e, "writer_log read error in forwarder");
                drop(fwd);
                if sleep_or_shutdown(Duration::from_millis(500), shutdown).await {
                    return;
                }
                continue;
            }
        };

        let owner_res = if backoff_ms > 0 {
            fwd.owners.refresh_owner(&project_id).await
        } else {
            fwd.owners.resolve_owner(&project_id).await
        };

        let owner = match owner_res {
            Ok(Some(o)) => o,
            Ok(None) => {
                fwd.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                backoff_ms = if backoff_ms == 0 {
                    50
                } else {
                    (backoff_ms * 2).min(fwd.max_backoff_ms)
                };
                let wait = Duration::from_millis(backoff_ms);
                drop(fwd);
                if sleep_or_shutdown(wait, shutdown).await {
                    return;
                }
                continue;
            }
            Err(e) => {
                tracing::warn!(project_id = %project_id, error = %e, "resolving project owner failed");
                fwd.metrics
                    .batches_retry_error
                    .fetch_add(1, Ordering::Relaxed);
                fwd.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                backoff_ms = if backoff_ms == 0 {
                    50
                } else {
                    (backoff_ms * 2).min(fwd.max_backoff_ms)
                };
                let wait = Duration::from_millis(backoff_ms);
                drop(fwd);
                if sleep_or_shutdown(wait, shutdown).await {
                    return;
                }
                continue;
            }
        };

        if fwd.forward_guard.self_node_name.as_deref() == Some(owner.node_name.as_str()) {
            tracing::error!(
                project_id = %project_id,
                owner_node = %owner.node_name,
                "stateless writer resolved self as project_owner; refusing forward loop"
            );
            fwd.metrics
                .batches_retry_error
                .fetch_add(1, Ordering::Relaxed);
            fwd.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
            let wait = Duration::from_millis(fwd.max_backoff_ms.min(1_000));
            drop(fwd);
            if sleep_or_shutdown(wait, shutdown).await {
                return;
            }
            continue;
        }

        let url = format!(
            "{}/internal/v1/forward/events",
            owner.address.trim_end_matches('/')
        );
        let body = encode_forward_events_envelope(
            fwd.writer_log.writer_id(),
            &project_id,
            next.batch_offset,
            &next.plsb_bytes,
        );
        let headers = build_outgoing_headers(
            &fwd.forward_guard,
            fwd.internal_token.as_deref(),
            fwd.writer_log.writer_id(),
        );

        let resp_res = fwd
            .http_client
            .post(&url)
            .headers(headers)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await;

        match resp_res {
            Ok(resp) if resp.status() == StatusCode::OK => {
                if let Err(e) = fwd.writer_log.ack_batch(&project_id, next.batch_offset) {
                    tracing::error!(
                        project_id = %project_id,
                        batch_offset = next.batch_offset,
                        error = %e,
                        "failed to persist writer_log cursor after owner ack"
                    );
                    let wait = Duration::from_millis(100);
                    drop(fwd);
                    if sleep_or_shutdown(wait, shutdown).await {
                        return;
                    }
                    continue;
                }
                fwd.metrics.batches_ok.fetch_add(1, Ordering::Relaxed);
                backoff_ms = 0;
                consecutive_not_owner = 0;
                continue;
            }
            Ok(resp) => {
                let status = resp.status();
                let retry_after_ms = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(|s| s.saturating_mul(1_000).min(fwd.max_backoff_ms));
                let body_bytes = resp.bytes().await.unwrap_or_default();
                let err_json: Option<serde_json::Value> = serde_json::from_slice(&body_bytes).ok();
                let code = err_json
                    .as_ref()
                    .and_then(|v| v.get("code"))
                    .and_then(|c| c.as_str());

                if status == StatusCode::SERVICE_UNAVAILABLE && code == Some("not_owner") {
                    fwd.metrics
                        .batches_not_owner
                        .fetch_add(1, Ordering::Relaxed);
                    fwd.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                    consecutive_not_owner = consecutive_not_owner.saturating_add(1);
                    let _ = fwd.owners.refresh_owner(&project_id).await;
                    let wait_ms = if consecutive_not_owner <= 2 {
                        10
                    } else {
                        backoff_ms = if backoff_ms == 0 {
                            25
                        } else {
                            (backoff_ms * 2).min(fwd.max_backoff_ms)
                        };
                        backoff_ms
                    };
                    let wait = Duration::from_millis(wait_ms);
                    drop(fwd);
                    if sleep_or_shutdown(wait, shutdown).await {
                        return;
                    }
                    continue;
                }

                fwd.metrics
                    .batches_retry_error
                    .fetch_add(1, Ordering::Relaxed);
                fwd.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                consecutive_not_owner = 0;
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let wait_ms = retry_after_ms.unwrap_or_else(|| {
                        backoff_ms = if backoff_ms == 0 {
                            50
                        } else {
                            (backoff_ms * 2).min(fwd.max_backoff_ms)
                        };
                        backoff_ms
                    });
                    let wait = Duration::from_millis(wait_ms);
                    drop(fwd);
                    if sleep_or_shutdown(wait, shutdown).await {
                        return;
                    }
                    continue;
                }

                tracing::warn!(
                    project_id = %project_id,
                    status = %status,
                    code = ?code,
                    "forwarding batch to owner failed; backing off"
                );
                backoff_ms = if backoff_ms == 0 {
                    50
                } else {
                    (backoff_ms * 2).min(fwd.max_backoff_ms)
                };
                let wait = Duration::from_millis(backoff_ms);
                drop(fwd);
                if sleep_or_shutdown(wait, shutdown).await {
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(
                    project_id = %project_id,
                    owner = %owner.address,
                    error = %e,
                    "forwarding batch transport error; backing off"
                );
                fwd.metrics
                    .batches_retry_error
                    .fetch_add(1, Ordering::Relaxed);
                fwd.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                consecutive_not_owner = 0;
                backoff_ms = if backoff_ms == 0 {
                    50
                } else {
                    (backoff_ms * 2).min(fwd.max_backoff_ms)
                };
                let wait = Duration::from_millis(backoff_ms);
                drop(fwd);
                if sleep_or_shutdown(wait, shutdown).await {
                    return;
                }
            }
        }
    }
}

/// Client used by stateless writers to forward synchronous profile/replay calls to the owning storage node.
pub struct OwnerSyncClient {
    writer_id: String,
    owners: Arc<dyn OwnerDirectory>,
    forward_guard: Arc<ForwardLoopGuard>,
    internal_token: Option<String>,
    http_client: reqwest::Client,
}

impl OwnerSyncClient {
    pub fn new(
        writer_id: String,
        owners: Arc<dyn OwnerDirectory>,
        forward_guard: Arc<ForwardLoopGuard>,
        internal_token: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            writer_id,
            owners,
            forward_guard,
            internal_token,
            http_client: build_forward_client(),
        })
    }

    async fn post_json_to_owner<Req: Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        project_id: &str,
        path: &str,
        body: &Req,
    ) -> Result<Resp> {
        let bytes = serde_json::to_vec(body)?;
        let resp_bytes = self
            .post_bytes_to_owner(project_id, path, "application/json", &bytes)
            .await?;
        let parsed: Resp = serde_json::from_slice(&resp_bytes)?;
        Ok(parsed)
    }

    async fn post_bytes_to_owner(
        &self,
        project_id: &str,
        path: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<Vec<u8>> {
        for attempt in 0..3u8 {
            let owner = if attempt == 0 {
                self.owners.resolve_owner(project_id).await
            } else {
                self.owners.refresh_owner(project_id).await
            }
            .map_err(|e| IngestError::Storage(StorageError::Unavailable(e)))?;

            let Some(owner) = owner else {
                return Err(IngestError::Storage(StorageError::NotOwner {
                    project_id: project_id.to_string(),
                    owner_node: None,
                    owner_address: None,
                }));
            };

            if self.forward_guard.self_node_name.as_deref() == Some(owner.node_name.as_str()) {
                return Err(IngestError::Storage(StorageError::Unavailable(
                    "stateless writer resolved self as project_owner".to_string(),
                )));
            }

            let url = format!("{}{}", owner.address.trim_end_matches('/'), path);
            let headers = build_outgoing_headers(
                &self.forward_guard,
                self.internal_token.as_deref(),
                &self.writer_id,
            );
            let resp = self
                .http_client
                .post(&url)
                .headers(headers)
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .body(body.to_vec())
                .send()
                .await
                .map_err(|e| {
                    IngestError::Storage(StorageError::Unavailable(format!(
                        "owner sync request failed: {e}"
                    )))
                })?;

            let status = resp.status();
            let resp_bytes = resp
                .bytes()
                .await
                .map_err(|e| {
                    IngestError::Storage(StorageError::Unavailable(format!(
                        "owner sync response read failed: {e}"
                    )))
                })?
                .to_vec();

            if status.is_success() {
                return Ok(resp_bytes);
            }

            let err_json: Option<serde_json::Value> = serde_json::from_slice(&resp_bytes).ok();
            let code = err_json
                .as_ref()
                .and_then(|v| v.get("code"))
                .and_then(|c| c.as_str());
            let msg = err_json
                .as_ref()
                .and_then(|v| v.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("owner sync request failed")
                .to_string();

            if status == StatusCode::SERVICE_UNAVAILABLE && code == Some("not_owner") && attempt < 2
            {
                continue;
            }
            return Err(map_owner_http_error(
                project_id,
                status,
                code,
                msg,
                err_json.as_ref(),
            ));
        }

        Err(IngestError::Storage(StorageError::NotOwner {
            project_id: project_id.to_string(),
            owner_node: None,
            owner_address: None,
        }))
    }

    pub async fn forward_profile_calls(
        &self,
        project_id: &str,
        calls: Vec<serde_json::Value>,
    ) -> Result<usize> {
        let resp: ForwardProfileCallsResponse = self
            .post_json_to_owner(
                project_id,
                "/internal/v1/forward/profile-calls",
                &ForwardProfileCallsRequest {
                    project_id: project_id.to_string(),
                    calls,
                },
            )
            .await?;
        Ok(resp.profiles_updated)
    }

    pub async fn forward_identify(
        &self,
        project_id: &str,
        req: IdentifyRequest,
    ) -> Result<IdentifyResponse> {
        self.post_json_to_owner(
            project_id,
            "/internal/v1/forward/identify",
            &ForwardIdentifyRequest {
                project_id: project_id.to_string(),
                req,
            },
        )
        .await
    }

    pub async fn forward_engage(
        &self,
        project_id: &str,
        payload: serde_json::Value,
    ) -> Result<EngageResponse> {
        self.post_json_to_owner(
            project_id,
            "/internal/v1/forward/engage",
            &ForwardJsonPayloadRequest {
                project_id: project_id.to_string(),
                payload,
            },
        )
        .await
    }

    pub async fn forward_group_engage(
        &self,
        project_id: &str,
        payload: serde_json::Value,
    ) -> Result<usize> {
        let resp: ForwardGroupEngageResponse = self
            .post_json_to_owner(
                project_id,
                "/internal/v1/forward/groups",
                &ForwardJsonPayloadRequest {
                    project_id: project_id.to_string(),
                    payload,
                },
            )
            .await?;
        Ok(resp.updated)
    }

    pub async fn forward_replay(
        &self,
        project_id: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<ReplayIngestResponse> {
        let path = format!("/internal/v1/forward/replay-chunk?project_id={project_id}");
        let ct = content_type.unwrap_or("application/octet-stream");
        let resp_bytes = self
            .post_bytes_to_owner(project_id, &path, ct, body)
            .await?;
        let resp: ReplayIngestResponse = serde_json::from_slice(&resp_bytes)?;
        Ok(resp)
    }
}

fn map_owner_http_error(
    project_id: &str,
    status: StatusCode,
    code: Option<&str>,
    msg: String,
    json: Option<&serde_json::Value>,
) -> IngestError {
    match status {
        StatusCode::CONFLICT => IngestError::Conflict(msg),
        StatusCode::BAD_REQUEST => IngestError::Wire(pulse_wire::WireError::InvalidPayload(msg)),
        StatusCode::UNPROCESSABLE_ENTITY => IngestError::SchemaGovernanceLimit(msg),
        StatusCode::TOO_MANY_REQUESTS => {
            let what = match code {
                Some("l0_backpressure") => "l0",
                Some("log_backpressure") => "log",
                Some("ingest_buffer_backpressure") => "ingest_buffer",
                Some("writer_log_backpressure") => "writer_log",
                _ => "l0",
            };
            if code == Some("rate_limited") {
                IngestError::RateLimited {
                    retry_after_secs: 1,
                }
            } else if code == Some("hot_segment_backpressure") {
                IngestError::Storage(StorageError::HotSegmentBackpressure {
                    project_id: project_id.to_string(),
                    bytes: 0,
                    max_bytes: 0,
                })
            } else {
                IngestError::Storage(StorageError::Backpressure {
                    project_id: project_id.to_string(),
                    what,
                    bytes: 0,
                    max_bytes: 0,
                })
            }
        }
        StatusCode::SERVICE_UNAVAILABLE => match code {
            Some("not_owner") => IngestError::Storage(StorageError::NotOwner {
                project_id: project_id.to_string(),
                owner_node: json
                    .and_then(|v| v.get("owner_node"))
                    .and_then(|s| s.as_str())
                    .map(str::to_string),
                owner_address: json
                    .and_then(|v| v.get("owner_address"))
                    .and_then(|s| s.as_str())
                    .map(str::to_string),
            }),
            Some("not_writer") => IngestError::Storage(StorageError::NotWriter {
                project_id: project_id.to_string(),
            }),
            _ => IngestError::Storage(StorageError::Unavailable(msg)),
        },
        _ => IngestError::Storage(StorageError::Unavailable(msg)),
    }
}
