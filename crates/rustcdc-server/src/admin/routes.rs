//! The admin router, and the row-shape lookup it serves.

use axum::{
    Json, Router,
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::net::SocketAddr;

use super::auth::{rate_limited_response, unauthorized_response};
use super::rate_limit::AbuseLimitScope;
use super::*;

/// The admin API, and — when `console` is on and the `ui` feature compiled it in — the console
/// at `/ui`. API responses carry `default-src 'none'`: they are data, never a page to render.
pub fn router(state: AdminState, console: bool) -> Router {
    let api = Router::new()
        .route("/healthz", get(healthz))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/status", get(status_authed))
        .route("/config", get(config_authed))
        .route("/metrics", get(metrics))
        .route("/signals", post(signal_action))
        .route("/notifications", get(notifications_authed))
        .route(
            "/notifications/cloudevents",
            get(notifications_cloudevents_authed),
        )
        .route("/notifications/stream", get(notifications_stream_authed))
        .route("/schemas/{id}", get(row_shape_authed))
        .route("/openapi.json", get(openapi_document))
        .with_state(state)
        .layer(axum::middleware::map_response(api_content_security_policy))
        .layer(panic_guard());
    with_console(api, console)
}

async fn api_content_security_policy(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        axum::http::HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
    );
    response
}

#[cfg(feature = "ui")]
fn with_console(api: Router, console: bool) -> Router {
    if console {
        api.merge(super::ui::routes())
    } else {
        api
    }
}

#[cfg(not(feature = "ui"))]
fn with_console(api: Router, _console: bool) -> Router {
    api
}

impl AdminState {
    /// The shapes this process delivered; the delivery path records into the same set.
    pub fn delivered_shapes(&self) -> crate::runtime::row_shapes::DeliveredShapes {
        self.delivered_shapes.clone()
    }
}

/// `GET /schemas/{id}`: a row shape this process has delivered since it started.
///
/// The same rate limit and read-token check as `/status`. The id is validated before the
/// lookup so a malformed one is a 400, not an indistinguishable 404.
async fn row_shape_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("schemas");
    }
    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }
    if !rustcdc::RowShape::is_valid_id(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_shape_id",
                "message": "a row shape id is 64 lowercase hexadecimal characters",
            })),
        )
            .into_response();
    }
    match admin.delivered_shapes.get(&id) {
        Some(shape) => (StatusCode::OK, Json(shape.as_ref())).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "unknown_shape",
                "message": "this process has not delivered a row with that shape since it \
                            started",
            })),
        )
            .into_response(),
    }
}
