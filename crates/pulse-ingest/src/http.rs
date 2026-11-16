use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};

use crate::service::{IdentifyRequest, IngestError, IngestService};

/// Builds the Axum router serving `/v1/ingest`, `/v1/identify`, and `/v1/replay/chunk`.
pub fn build_ingest_router(service: Arc<IngestService>) -> Router {
    Router::new()
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/identify", post(handle_identify))
        .route("/v1/replay/chunk", post(handle_replay_chunk))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
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
    Json(req): Json<IdentifyRequest>,
) -> std::result::Result<Json<crate::service::IdentifyResponse>, ApiError> {
    let api_key = extract_api_key(&headers).ok_or(IngestError::Unauthorized)?;
    let resp = service.identify(api_key, req).await?;
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
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({
            "status": "error",
            "message": self.0.to_string(),
        });
        (status, Json(body)).into_response()
    }
}
