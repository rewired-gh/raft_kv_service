use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    http::{HeaderName, StatusCode, header::AUTHORIZATION},
    routing::get,
};
use serde::Serialize;
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

const REQUEST_ID_HEADER: &str = "x-request-id";
const MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    service_name: Arc<str>,
    ready: Arc<AtomicBool>,
}

impl AppState {
    pub fn new(service_name: impl Into<Arc<str>>) -> Self {
        Self {
            service_name: service_name.into(),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
}

pub fn router(state: AppState, request_timeout: Duration) -> Router {
    let request_id_header = HeaderName::from_static(REQUEST_ID_HEADER);
    let middleware = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(SetSensitiveRequestHeadersLayer::new(std::iter::once(
            AUTHORIZATION,
        )))
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, MakeRequestUuid))
        .layer(CatchPanicLayer::new())
        .layer(RequestBodyLimitLayer::new(MAX_REQUEST_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ));

    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .fallback(not_found)
        .with_state(state)
        .layer(middleware)
}

async fn liveness(axum::extract::State(state): axum::extract::State<AppState>) -> Json<Health> {
    Json(Health::up(state.service_name.to_string()))
}

async fn readiness(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> (StatusCode, Json<Health>) {
    if state.ready.load(Ordering::Acquire) {
        (
            StatusCode::OK,
            Json(Health::up(state.service_name.to_string())),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health::down(state.service_name.to_string())),
        )
    }
}

async fn not_found() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::NOT_FOUND,
        Json(ApiError {
            code: "not_found",
            message: "the requested resource does not exist",
        }),
    )
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    service: String,
    version: &'static str,
}

impl Health {
    fn up(service: String) -> Self {
        Self {
            status: "up",
            service,
            version: env!("CARGO_PKG_VERSION"),
        }
    }

    fn down(service: String) -> Self {
        Self {
            status: "down",
            service,
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

#[derive(Serialize)]
struct ApiError {
    code: &'static str,
    message: &'static str,
}
