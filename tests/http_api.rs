use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{ETAG, LOCATION},
    },
};
use raft_kv_service::{
    http::{AppState, ServiceError, router},
    kv::{Command, KvStateMachine},
};
use tower::ServiceExt;

const CLIENT: &str = "018f3f5e-7b4a-7c15-9e37-79b97f4a7c15";

fn test_app() -> axum::Router {
    let machine = Arc::new(Mutex::new((KvStateMachine::new(32).expect("state"), 0_u64)));
    let state = AppState::new("test-service").with_executor(move |command: Command| {
        let machine = Arc::clone(&machine);
        async move {
            let mut state = machine.lock().map_err(|_| ServiceError::Internal)?;
            state.1 = state.1.checked_add(1).ok_or(ServiceError::Internal)?;
            let index = state.1;
            state
                .0
                .apply(index, &command)
                .map_err(|_| ServiceError::Internal)
        }
    });
    router(state, Duration::from_secs(1))
}

fn mutation_request(
    method: &str,
    uri: &str,
    sequence: u64,
    body: &'static [u8],
) -> Result<Request<Body>> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/octet-stream")
        .header("raft-kv-client-id", CLIENT)
        .header("raft-kv-request-sequence", sequence)
        .header("x-request-id", "test-request")
        .body(Body::from(body))
        .context("build request")
}

#[tokio::test]
async fn conditional_crud_and_retry_follow_the_wire_contract() -> Result<()> {
    let app = test_app();
    let mut create = mutation_request("PUT", "/v1/keys/alpha", 1, b"one")?;
    create.headers_mut().insert("if-none-match", "*".parse()?);
    let created = app.clone().oneshot(create).await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(created.headers()[ETAG], "\"rev-1\"");
    assert_eq!(created.headers()[LOCATION], "/v1/keys/alpha");
    assert_eq!(created.headers()["raft-kv-revision"], "1");
    assert_eq!(created.headers()["cache-control"], "no-store");

    let mut retry = mutation_request("PUT", "/v1/keys/alpha", 1, b"one")?;
    retry.headers_mut().insert("if-none-match", "*".parse()?);
    let retried = app.clone().oneshot(retry).await?;
    assert_eq!(retried.status(), StatusCode::CREATED);
    assert_eq!(retried.headers()["raft-kv-revision"], "1");

    let read = app
        .clone()
        .oneshot(Request::get("/v1/keys/alpha").body(Body::empty())?)
        .await?;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(read.headers()[ETAG], "\"rev-1\"");
    assert_eq!(to_bytes(read.into_body(), 16).await?.as_ref(), b"one");

    let mut update = mutation_request("PUT", "/v1/keys/alpha", 2, b"two")?;
    update
        .headers_mut()
        .insert("if-match", "\"rev-1\"".parse()?);
    let updated = app.clone().oneshot(update).await?;
    assert_eq!(updated.status(), StatusCode::NO_CONTENT);
    assert_eq!(updated.headers()["raft-kv-revision"], "4");

    let mut delete = mutation_request("DELETE", "/v1/keys/alpha", 3, b"")?;
    delete.headers_mut().insert("if-match", "*".parse()?);
    let deleted = app.clone().oneshot(delete).await?;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert_eq!(deleted.headers()["raft-kv-revision"], "5");
    assert!(!deleted.headers().contains_key(ETAG));

    let absent = app
        .oneshot(Request::get("/v1/keys/alpha").body(Body::empty())?)
        .await?;
    assert_eq!(absent.status(), StatusCode::NOT_FOUND);
    assert_eq!(absent.headers()["content-type"], "application/problem+json");
    Ok(())
}

#[tokio::test]
async fn missing_preconditions_are_rejected_before_mutation_identity() -> Result<()> {
    let request = Request::put("/v1/keys/alpha")
        .header("content-type", "application/octet-stream")
        .body(Body::from("value"))?;
    let response = test_app().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
    Ok(())
}

#[tokio::test]
async fn unavailable_executor_returns_retryable_problem() -> Result<()> {
    let response = router(AppState::new("test-service"), Duration::from_secs(1))
        .oneshot(Request::get("/v1/keys/alpha").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["cache-control"], "no-store");
    Ok(())
}
