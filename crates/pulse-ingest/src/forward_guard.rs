use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use thiserror::Error;

pub const HEADER_FORWARDED: &str = "x-pulse-forwarded";
pub const HEADER_FORWARDED_HOP: &str = "x-pulse-forwarded-hop";
pub const HEADER_FORWARDED_BY: &str = "x-pulse-forwarded-by";
pub const DEFAULT_MAX_FORWARD_HOPS: u8 = 1;

#[derive(Debug, Clone)]
pub struct ForwardLoopGuard {
    pub self_node_name: Option<String>,
    pub max_hops: u8,
}

impl Default for ForwardLoopGuard {
    fn default() -> Self {
        Self {
            self_node_name: None,
            max_hops: DEFAULT_MAX_FORWARD_HOPS,
        }
    }
}

impl ForwardLoopGuard {
    pub fn new(self_node_name: Option<String>) -> Self {
        Self {
            self_node_name: self_node_name
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            max_hops: DEFAULT_MAX_FORWARD_HOPS,
        }
    }

    pub fn with_max_hops(mut self, max_hops: u8) -> Self {
        self.max_hops = max_hops;
        self
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ForwardCheckError {
    #[error("request forward hop count ({hop}) exceeds limit ({max_hops})")]
    HopLimitExceeded { hop: u8, max_hops: u8 },
    #[error("forwarding loop detected: node '{node_name}' already handled this request")]
    SelfLoop { node_name: String },
    #[error("malformed x-pulse-forwarded-hop header")]
    InvalidHopHeader,
}

impl IntoResponse for ForwardCheckError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "status": "error",
            "code": "forward_loop_detected",
            "message": self.to_string(),
        });
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    }
}

/// Validates incoming forward-loop headers, returning the current hop count (`0` if unforwarded).
pub fn check_incoming_forward(
    headers: &HeaderMap,
    self_node_name: Option<&str>,
    max_hops: u8,
) -> std::result::Result<u8, ForwardCheckError> {
    let hop = if let Some(raw_hop) = headers.get(HEADER_FORWARDED_HOP) {
        let s = raw_hop
            .to_str()
            .map_err(|_| ForwardCheckError::InvalidHopHeader)?
            .trim();
        if s.is_empty() {
            return Err(ForwardCheckError::InvalidHopHeader);
        }
        s.parse::<u8>()
            .map_err(|_| ForwardCheckError::InvalidHopHeader)?
    } else if let Some(raw_fwd) = headers.get(HEADER_FORWARDED) {
        let s = raw_fwd
            .to_str()
            .map_err(|_| ForwardCheckError::InvalidHopHeader)?
            .trim();
        if s.is_empty() || s == "0" || s.eq_ignore_ascii_case("false") {
            0
        } else {
            1
        }
    } else {
        0
    };

    if hop > max_hops {
        return Err(ForwardCheckError::HopLimitExceeded { hop, max_hops });
    }

    if hop > 0 {
        if let Some(me) = self_node_name.map(str::trim).filter(|s| !s.is_empty()) {
            for val in headers.get_all(HEADER_FORWARDED_BY) {
                if let Ok(s) = val.to_str() {
                    if s.split(',').map(str::trim).any(|entry| entry == me) {
                        return Err(ForwardCheckError::SelfLoop {
                            node_name: me.to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(hop)
}

/// Validates incoming forward state and populates outgoing forward-tracking headers (`out`).
pub fn apply_outgoing_forward_headers(
    out: &mut HeaderMap,
    incoming: Option<&HeaderMap>,
    self_node_name: Option<&str>,
    max_hops: u8,
) -> std::result::Result<(), ForwardCheckError> {
    let cur_hop = match incoming {
        Some(h) => check_incoming_forward(h, self_node_name, max_hops)?,
        None => 0,
    };
    let next_hop = cur_hop
        .checked_add(1)
        .ok_or(ForwardCheckError::HopLimitExceeded {
            hop: u8::MAX,
            max_hops,
        })?;
    if next_hop > max_hops {
        return Err(ForwardCheckError::HopLimitExceeded {
            hop: next_hop,
            max_hops,
        });
    }

    out.insert(
        HeaderName::from_static(HEADER_FORWARDED),
        HeaderValue::from_static("1"),
    );
    if let Ok(val) = HeaderValue::from_str(&next_hop.to_string()) {
        out.insert(HeaderName::from_static(HEADER_FORWARDED_HOP), val);
    }

    let mut nodes: Vec<String> = Vec::new();
    if let Some(h) = incoming {
        for val in h.get_all(HEADER_FORWARDED_BY) {
            if let Ok(s) = val.to_str() {
                for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
                    nodes.push(entry.to_string());
                }
            }
        }
    }
    if let Some(me) = self_node_name.map(str::trim).filter(|s| !s.is_empty()) {
        if !nodes.iter().any(|n| n == me) {
            nodes.push(me.to_string());
        }
    }
    if !nodes.is_empty() {
        if let Ok(val) = HeaderValue::from_str(&nodes.join(",")) {
            out.insert(HeaderName::from_static(HEADER_FORWARDED_BY), val);
        }
    }

    Ok(())
}

/// Axum middleware enforcing `ForwardLoopGuard` on incoming HTTP requests.
pub async fn forward_loop_middleware(
    State(guard): State<Arc<ForwardLoopGuard>>,
    req: Request,
    next: Next,
) -> Response {
    match check_incoming_forward(
        req.headers(),
        guard.self_node_name.as_deref(),
        guard.max_hops,
    ) {
        Ok(_) => next.run(req).await,
        Err(err) => err.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    #[test]
    fn forward_headers_validate_hops_and_self_loops() {
        let empty = HeaderMap::new();
        assert_eq!(
            check_incoming_forward(&empty, Some("node-a"), DEFAULT_MAX_FORWARD_HOPS),
            Ok(0)
        );

        let mut out = HeaderMap::new();
        apply_outgoing_forward_headers(&mut out, Some(&empty), Some("node-a"), 1).unwrap();
        assert_eq!(out.get(HEADER_FORWARDED).unwrap(), "1");
        assert_eq!(out.get(HEADER_FORWARDED_HOP).unwrap(), "1");
        assert_eq!(out.get(HEADER_FORWARDED_BY).unwrap(), "node-a");

        // Valid 1-hop forwarded request arriving at node-b passes.
        assert_eq!(
            check_incoming_forward(&out, Some("node-b"), DEFAULT_MAX_FORWARD_HOPS),
            Ok(1)
        );

        // Node-b trying to re-forward exceeds max_hops=1.
        let mut out2 = HeaderMap::new();
        assert_eq!(
            apply_outgoing_forward_headers(&mut out2, Some(&out), Some("node-b"), 1),
            Err(ForwardCheckError::HopLimitExceeded {
                hop: 2,
                max_hops: 1
            })
        );

        // Self-loop back to node-a at hop=1 is rejected.
        assert_eq!(
            check_incoming_forward(&out, Some("node-a"), DEFAULT_MAX_FORWARD_HOPS),
            Err(ForwardCheckError::SelfLoop {
                node_name: "node-a".into()
            })
        );

        // Multi-node forwarded-by list detects loop.
        let mut multi_by = HeaderMap::new();
        multi_by.insert(HEADER_FORWARDED_HOP, HeaderValue::from_static("1"));
        multi_by.insert(
            HEADER_FORWARDED_BY,
            HeaderValue::from_static("node-b, node-a"),
        );
        assert_eq!(
            check_incoming_forward(&multi_by, Some("node-a"), 2),
            Err(ForwardCheckError::SelfLoop {
                node_name: "node-a".into()
            })
        );

        // Malformed hop header fails closed.
        let mut bad = HeaderMap::new();
        bad.insert(HEADER_FORWARDED_HOP, HeaderValue::from_static("abc"));
        assert_eq!(
            check_incoming_forward(&bad, Some("node-b"), 1),
            Err(ForwardCheckError::InvalidHopHeader)
        );
    }

    #[tokio::test]
    async fn forward_loop_middleware_returns_503_on_loop_or_hop_limit() {
        let guard = Arc::new(ForwardLoopGuard::new(Some("node-a".into())));
        let app = Router::new()
            .route("/ok", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn_with_state(
                guard,
                forward_loop_middleware,
            ));

        // Direct request succeeds.
        let r0 = app
            .clone()
            .oneshot(Request::get("/ok").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r0.status(), StatusCode::OK);

        // Valid 1-hop from node-b succeeds.
        let r1 = app
            .clone()
            .oneshot(
                Request::get("/ok")
                    .header(HEADER_FORWARDED_HOP, "1")
                    .header(HEADER_FORWARDED_BY, "node-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);

        // Self-loop returns 503 forward_loop_detected.
        let r_self = app
            .clone()
            .oneshot(
                Request::get("/ok")
                    .header(HEADER_FORWARDED_HOP, "1")
                    .header(HEADER_FORWARDED_BY, "node-b,node-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r_self.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(r_self.into_body(), 16 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "forward_loop_detected");

        // Hop 2 exceeds limit -> 503 forward_loop_detected.
        let r_hop = app
            .oneshot(
                Request::get("/ok")
                    .header(HEADER_FORWARDED_HOP, "2")
                    .header(HEADER_FORWARDED_BY, "node-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r_hop.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(r_hop.into_body(), 16 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "forward_loop_detected");
    }
}
