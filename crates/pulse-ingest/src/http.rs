use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use pulse_wire::WireBatch;
use serde::Deserialize;

use crate::forward_guard::{forward_loop_middleware, ForwardLoopGuard};
use crate::forwarder::{
    decode_forward_events_envelope, ForwardGroupEngageResponse, ForwardIdentifyRequest,
    ForwardJsonPayloadRequest, ForwardProfileCallsRequest, ForwardProfileCallsResponse,
    HEADER_INTERNAL_TOKEN,
};
use crate::service::{IdentifyRequest, IngestError, IngestService};

/// Builds the Axum router serving `/v1/ingest`, `/v1/identify`, `/v1/engage`, `/v1/groups`, and `/v1/replay/chunk`.
pub fn build_ingest_router(service: Arc<IngestService>) -> Router {
    build_ingest_router_with_guard(service, Arc::new(ForwardLoopGuard::default()))
}

/// Builds the ingest Axum router with a configured [`ForwardLoopGuard`].
pub fn build_ingest_router_with_guard(
    service: Arc<IngestService>,
    guard: Arc<ForwardLoopGuard>,
) -> Router {
    Router::new()
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/identify", post(handle_identify))
        .route("/v1/engage", post(handle_engage))
        .route("/v1/groups", post(handle_groups))
        .route("/v1/replay/chunk", post(handle_replay_chunk))
        // ponytail: one cap for every ingest route; the SDKs send batches and replay chunks far below it.
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            guard,
            forward_loop_middleware,
        ))
        .with_state(service)
}

/// Builds the internal node-to-node forwarding router mounted on storage-owning nodes (`All | Ingest`).
pub fn build_internal_forward_router(
    service: Arc<IngestService>,
    guard: Arc<ForwardLoopGuard>,
    internal_token: Option<String>,
) -> Router {
    let expected_token: Option<Arc<String>> = internal_token
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(Arc::new);
    Router::new()
        .route("/internal/v1/forward/events", post(handle_forward_events))
        .route(
            "/internal/v1/forward/profile-calls",
            post(handle_forward_profile_calls),
        )
        .route(
            "/internal/v1/forward/identify",
            post(handle_forward_identify),
        )
        .route("/internal/v1/forward/engage", post(handle_forward_engage))
        .route("/internal/v1/forward/groups", post(handle_forward_groups))
        .route(
            "/internal/v1/forward/replay-chunk",
            post(handle_forward_replay_chunk),
        )
        .route_layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .route_layer(axum::middleware::from_fn_with_state(
            guard,
            forward_loop_middleware,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            expected_token,
            internal_token_middleware,
        ))
        .with_state(service)
}

async fn internal_token_middleware(
    State(expected): State<Option<Arc<String>>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(ref token) = expected {
        let presented = req
            .headers()
            .get(HEADER_INTERNAL_TOKEN)
            .and_then(|v| v.to_str().ok())
            .map(str::trim);
        let ok = presented
            .is_some_and(|p| constant_time_eq::constant_time_eq(p.as_bytes(), token.as_bytes()));
        if !ok {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "status": "error",
                    "code": "unauthorized_internal",
                    "message": "missing or invalid x-pulse-internal-token",
                })),
            )
                .into_response();
        }
    }
    next.run(req).await
}

#[derive(Debug, Deserialize)]
struct ForwardReplayQuery {
    project_id: String,
}

async fn handle_forward_events(
    State(service): State<Arc<IngestService>>,
    body: Bytes,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let (writer_id, project_id, batch_offset, plsb_bytes) =
        decode_forward_events_envelope(&body)
            .map_err(|msg| IngestError::Wire(pulse_wire::WireError::InvalidPayload(msg)))?;
    let batch = WireBatch::decode_binary(plsb_bytes).map_err(IngestError::Wire)?;
    let accepted = service
        .ingest_forwarded_batch(
            &writer_id,
            &project_id,
            batch_offset,
            batch.events,
            Utc::now().timestamp_millis(),
        )
        .await?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "accepted": accepted,
    })))
}

async fn handle_forward_profile_calls(
    State(service): State<Arc<IngestService>>,
    Json(req): Json<ForwardProfileCallsRequest>,
) -> std::result::Result<Json<ForwardProfileCallsResponse>, ApiError> {
    let profiles_updated = service
        .apply_forwarded_profile_calls(&req.project_id, req.calls)
        .await?;
    Ok(Json(ForwardProfileCallsResponse {
        status: "ok".to_string(),
        profiles_updated,
    }))
}

async fn handle_forward_identify(
    State(service): State<Arc<IngestService>>,
    Json(req): Json<ForwardIdentifyRequest>,
) -> std::result::Result<Json<crate::service::IdentifyResponse>, ApiError> {
    let resp = service
        .identify_for_project(&req.project_id, req.req)
        .await?;
    Ok(Json(resp))
}

async fn handle_forward_engage(
    State(service): State<Arc<IngestService>>,
    Json(req): Json<ForwardJsonPayloadRequest>,
) -> std::result::Result<Json<crate::service::EngageResponse>, ApiError> {
    let resp = service
        .engage_for_project(&req.project_id, req.payload)
        .await?;
    Ok(Json(resp))
}

async fn handle_forward_groups(
    State(service): State<Arc<IngestService>>,
    Json(req): Json<ForwardJsonPayloadRequest>,
) -> std::result::Result<Json<ForwardGroupEngageResponse>, ApiError> {
    let updated = service
        .group_engage_for_project(&req.project_id, req.payload)
        .await?;
    Ok(Json(ForwardGroupEngageResponse {
        status: "ok".to_string(),
        updated,
    }))
}

async fn handle_forward_replay_chunk(
    State(service): State<Arc<IngestService>>,
    Query(q): Query<ForwardReplayQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<crate::service::ReplayIngestResponse>, ApiError> {
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let resp = service
        .ingest_replay_for_project(&q.project_id, content_type, &body)
        .await?;
    Ok(Json(resp))
}

/// Extracts the public `pk_` API key strictly from HTTP headers (never URL query strings, `P3-05`).
fn extract_api_key(headers: &HeaderMap) -> Option<&str> {
    if let Some(val) = headers.get("x-pulse-key").and_then(|v| v.to_str().ok()) {
        if !val.trim().is_empty() {
            return Some(val.trim());
        }
    }
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth
            .strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "))
        {
            if !token.trim().is_empty() {
                return Some(token.trim());
            }
        }
    }
    None
}

async fn handle_ingest(
    State(service): State<Arc<IngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<crate::service::IngestResponse>, ApiError> {
    let api_key = extract_api_key(&headers).ok_or(IngestError::Unauthorized)?;
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let resp = service.ingest_raw(api_key, content_type, &body).await?;
    Ok(Json(resp))
}

async fn handle_identify(
    State(service): State<Arc<IngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<crate::service::IdentifyResponse>, ApiError> {
    let api_key = extract_api_key(&headers).ok_or(IngestError::Unauthorized)?;
    let req: IdentifyRequest = serde_json::from_slice(&body).map_err(IngestError::Json)?;
    let resp = service.identify(api_key, req).await?;
    Ok(Json(resp))
}

async fn handle_engage(
    State(service): State<Arc<IngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<crate::service::EngageResponse>, ApiError> {
    let api_key = extract_api_key(&headers).ok_or(IngestError::Unauthorized)?;
    let resp = service.engage(api_key, &body).await?;
    Ok(Json(resp))
}

async fn handle_groups(
    State(service): State<Arc<IngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let api_key = extract_api_key(&headers).ok_or(IngestError::Unauthorized)?;
    let resp = service.group_engage(api_key, &body).await?;
    Ok(Json(resp))
}

async fn handle_replay_chunk(
    State(service): State<Arc<IngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<crate::service::ReplayIngestResponse>, ApiError> {
    let api_key = extract_api_key(&headers).ok_or(IngestError::Unauthorized)?;
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let resp = service.ingest_replay(api_key, content_type, &body).await?;
    Ok(Json(resp))
}

struct ApiError(IngestError);

impl From<IngestError> for ApiError {
    fn from(err: IngestError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let IngestError::RateLimited { retry_after_secs } = &self.0 {
            let body = serde_json::json!({
                "status": "error",
                "code": "rate_limited",
                "message": self.0.to_string(),
            });
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(
                    axum::http::header::RETRY_AFTER,
                    retry_after_secs.to_string(),
                )],
                Json(body),
            )
                .into_response();
        }
        if let IngestError::Storage(pulse_storage::StorageError::L0Backpressure { .. }) = &self.0 {
            let body = serde_json::json!({
                "status": "error",
                "code": "l0_backpressure",
                "message": self.0.to_string(),
            });
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(axum::http::header::RETRY_AFTER, "1")],
                Json(body),
            )
                .into_response();
        }
        if let IngestError::Storage(pulse_storage::StorageError::Backpressure { what, .. }) =
            &self.0
        {
            // `l0_backpressure`: flushing is behind. `log_backpressure`: processor is behind.
            // `ingest_buffer_backpressure`: writer memory buffer is full.
            let body = serde_json::json!({
                "status": "error",
                "code": format!("{what}_backpressure"),
                "message": self.0.to_string(),
            });
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(axum::http::header::RETRY_AFTER, "1")],
                Json(body),
            )
                .into_response();
        }
        if let IngestError::Storage(pulse_storage::StorageError::HotSegmentBackpressure {
            ..
        }) = &self.0
        {
            // Local hot/segments/ uploads are behind ingest (e.g. S3 unreachable).
            let body = serde_json::json!({
                "status": "error",
                "code": "hot_segment_backpressure",
                "message": self.0.to_string(),
            });
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(axum::http::header::RETRY_AFTER, "1")],
                Json(body),
            )
                .into_response();
        }
        if let IngestError::Storage(pulse_storage::StorageError::NotWriter { .. }) = &self.0 {
            // Another node writes this project (or this node could not renew its lease).
            let body = serde_json::json!({
                "status": "error",
                "code": "not_writer",
                "message": self.0.to_string(),
            });
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::RETRY_AFTER, "1")],
                Json(body),
            )
                .into_response();
        }
        if let IngestError::Storage(pulse_storage::StorageError::NotOwner {
            project_id,
            owner_node,
            owner_address,
        }) = &self.0
        {
            let body = serde_json::json!({
                "status": "error",
                "code": "not_owner",
                "project_id": project_id,
                "owner_node": owner_node,
                "owner_address": owner_address,
                "message": self.0.to_string(),
            });
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::RETRY_AFTER, "1")],
                Json(body),
            )
                .into_response();
        }
        if let IngestError::Storage(pulse_storage::StorageError::StaleWriter { .. }) = &self.0 {
            let body = serde_json::json!({
                "status": "error",
                "code": "stale_writer",
                "message": self.0.to_string(),
            });
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::RETRY_AFTER, "1")],
                Json(body),
            )
                .into_response();
        }
        let status = match &self.0 {
            IngestError::Unauthorized => StatusCode::UNAUTHORIZED,
            IngestError::Wire(_)
            | IngestError::Json(_)
            | IngestError::Storage(pulse_storage::StorageError::InvalidIdentifier(_)) => {
                StatusCode::BAD_REQUEST
            }
            IngestError::SchemaGovernanceLimit(_)
            | IngestError::Storage(pulse_storage::StorageError::DictionaryFull { .. }) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            IngestError::Conflict(_)
            | IngestError::Identity(pulse_identity::IdentityError::IdentifiedMergeConflict)
            | IngestError::Storage(pulse_storage::StorageError::Identity(
                pulse_identity::IdentityError::IdentifiedMergeConflict,
            )) => StatusCode::CONFLICT,
            // The WAL, local identity store, or profile store failed (disk full, unwritable
            // directory, fenced writer, store down): nothing was acknowledged, so retry.
            IngestError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            IngestError::Storage(e) if e.is_unavailable() => StatusCode::SERVICE_UNAVAILABLE,
            IngestError::Identity(e) if e.is_store_unavailable() => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = match &self.0 {
            IngestError::Wire(pulse_wire::WireError::Json(e)) | IngestError::Json(e) => {
                format!(
                    "invalid JSON payload: {}",
                    pulse_wire::sanitize_json_error_message(&e.to_string())
                )
            }
            other => other.to_string(),
        };
        let body = serde_json::json!({
            "status": "error",
            "message": message,
        });
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forward_guard::{HEADER_FORWARDED_BY, HEADER_FORWARDED_HOP};
    use crate::governance::InMemoryGovernance;
    use axum::body::Body;
    use axum::http::{HeaderValue, Method};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use pulse_identity::IdentityRouter;
    use pulse_storage::{StorageConfig, StorageEngine};
    use tower::ServiceExt;

    #[tokio::test]
    async fn internal_router_checks_token_before_forward_loop_guard() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = StorageEngine::new(
            StorageConfig {
                num_buckets: 1,
                ..StorageConfig::default()
            },
            store.clone(),
            Arc::new(IdentityRouter::new(store)),
        );
        let svc = IngestService::new(engine, Arc::new(InMemoryGovernance::default()));
        let guard = Arc::new(ForwardLoopGuard::new(Some("storage-a".into())));
        let app = build_internal_forward_router(svc, guard, Some("secret-token".into()));

        // Unauthenticated request carrying a loop-exceeding hop header must return 401, not 503.
        let unauth_req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/internal/v1/forward/events")
            .header(HEADER_FORWARDED_HOP, HeaderValue::from_static("2"))
            .body(Body::empty())
            .unwrap();
        let unauth_resp = app.clone().oneshot(unauth_req).await.unwrap();
        assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

        // Wrong token carrying a self-loop header must also return 401.
        let bad_tok_req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/internal/v1/forward/events")
            .header(HEADER_INTERNAL_TOKEN, HeaderValue::from_static("wrong"))
            .header(HEADER_FORWARDED_HOP, HeaderValue::from_static("1"))
            .header(HEADER_FORWARDED_BY, HeaderValue::from_static("storage-a"))
            .body(Body::empty())
            .unwrap();
        let bad_tok_resp = app.clone().oneshot(bad_tok_req).await.unwrap();
        assert_eq!(bad_tok_resp.status(), StatusCode::UNAUTHORIZED);

        // Valid token carrying hop=2 reaches forward_loop_middleware and returns 503.
        let auth_loop_req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/internal/v1/forward/events")
            .header(
                HEADER_INTERNAL_TOKEN,
                HeaderValue::from_static("secret-token"),
            )
            .header(HEADER_FORWARDED_HOP, HeaderValue::from_static("2"))
            .body(Body::empty())
            .unwrap();
        let auth_loop_resp = app.oneshot(auth_loop_req).await.unwrap();
        assert_eq!(auth_loop_resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
