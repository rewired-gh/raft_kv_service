use std::time::Duration;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use raft_kv_service::http::{AppState, router};
use tower::ServiceExt;

#[tokio::test]
async fn liveness_reports_service_metadata() -> anyhow::Result<()> {
    let state = AppState::new("test-service");
    let response = router(state, Duration::from_secs(1))
        .oneshot(Request::get("/health/live").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await?;
    let body = String::from_utf8(body.to_vec())?;
    assert!(body.contains("\"status\":\"up\""));
    assert!(body.contains("\"service\":\"test-service\""));
    Ok(())
}

#[tokio::test]
async fn readiness_tracks_application_state() -> anyhow::Result<()> {
    let state = AppState::new("test-service");
    let app = router(state.clone(), Duration::from_secs(1));

    let unavailable = app
        .clone()
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);

    state.set_ready(true);
    let ready = app
        .oneshot(Request::get("/health/ready").body(Body::empty())?)
        .await?;
    assert_eq!(ready.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn unknown_routes_return_a_stable_error_shape() -> anyhow::Result<()> {
    let response = router(AppState::new("test-service"), Duration::from_secs(1))
        .oneshot(Request::get("/missing").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(response.into_body(), 4096).await?;
    assert_eq!(
        String::from_utf8(body.to_vec())?,
        r#"{"code":"not_found","message":"the requested resource does not exist"}"#
    );
    Ok(())
}
