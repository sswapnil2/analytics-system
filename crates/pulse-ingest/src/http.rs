use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};

use crate::service::{IdentifyRequest, IngestError, IngestService};

/// Builds the Axum router serving `/v1/ingest`, `/v1/identify`, `/v1/engage`, `/v1/groups`, and `/v1/replay/chunk`.
pub fn build_ingest_router(service: Arc<IngestService>) -> Router {
    Router::new()
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/identify", post(handle_identify))
        .route("/v1/engage", post(handle_engage))
        .route("/v1/groups", post(handle_groups))
        .route("/v1/replay/chunk", post(handle_replay_chunk))
        // ponytail: one cap for every ingest route; the SDKs send batches and replay chunks far below it.
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(service)
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
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
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
        if let IngestError::Storage(pulse_storage::StorageError::Backpressure { .. }) = &self.0 {
            // Flushing is behind ingest.
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
            IngestError::Identity(pulse_identity::IdentityError::IdentifiedMergeConflict) => {
                StatusCode::CONFLICT
            }
            // The WAL or local identity store write failed (disk full, unwritable directory,
            // store down): nothing was acknowledged, so the client may retry.
            IngestError::Storage(
                pulse_storage::StorageError::Io(_) | pulse_storage::StorageError::ObjectStore(_),
            ) => StatusCode::SERVICE_UNAVAILABLE,
            IngestError::Identity(e)
            | IngestError::Storage(pulse_storage::StorageError::Identity(e))
                if e.is_store_unavailable() =>
            {
                StatusCode::SERVICE_UNAVAILABLE
            }
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
