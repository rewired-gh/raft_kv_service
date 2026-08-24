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
    cluster::{LeadershipTransferError, LeadershipTransferResult, MembershipView, NodeDiagnostics},
    http::{AppState, ServiceError, router},
    kv::{Command, KvStateMachine},
    metrics::Metrics,
    peer::ClusterId,
    raft::{ClusterConfiguration, MembershipEntry, NodeId, Role, Term},
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

fn diagnostics() -> NodeDiagnostics {
    let membership = MembershipEntry::initial(
        ClusterConfiguration::new([(1, "http://node-1:9080".to_owned())]).expect("membership"),
    );
    NodeDiagnostics {
        cluster_id: ClusterId([7; 16]).0,
        node_id: NodeId(1),
        role: Role::Leader,
        term: Term(3),
        leader: Some(NodeId(1)),
        leadership_transfer_target: None,
        commit_index: 9,
        applied_index: 9,
        last_log_index: 9,
        snapshot_index: 4,
        snapshot_term: Term(2),
        active_membership: membership.clone(),
        committed_membership: membership,
        replication: Vec::new(),
        request_queue_depth: 2,
        outbound_queue_depth: 1,
    }
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
async fn failed_precondition_exposes_the_current_revision() -> Result<()> {
    let app = test_app();
    let mut create = mutation_request("PUT", "/v1/keys/alpha", 1, b"one")?;
    create.headers_mut().insert("if-none-match", "*".parse()?);
    assert_eq!(
        app.clone().oneshot(create).await?.status(),
        StatusCode::CREATED
    );

    let mut update = mutation_request("PUT", "/v1/keys/alpha", 2, b"two")?;
    update
        .headers_mut()
        .insert("if-match", "\"rev-999\"".parse()?);
    let response = app.oneshot(update).await?;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(response.headers()["raft-kv-revision"], "1");
    Ok(())
}

#[tokio::test]
async fn configured_command_limits_are_enforced_before_log_admission() -> Result<()> {
    let state = AppState::new("limited")
        .with_command_limits(4, 3)
        .with_executor(|_| async { Err(ServiceError::Internal) });
    let app = router(state, Duration::from_secs(1));

    let oversized_key = app
        .clone()
        .oneshot(Request::get("/v1/keys/abcde").body(Body::empty())?)
        .await?;
    assert_eq!(oversized_key.status(), StatusCode::BAD_REQUEST);

    let mut oversized_value = mutation_request("PUT", "/v1/keys/key", 1, b"four")?;
    oversized_value
        .headers_mut()
        .insert("if-none-match", "*".parse()?);
    let oversized_value = app.oneshot(oversized_value).await?;
    assert_eq!(oversized_value.status(), StatusCode::PAYLOAD_TOO_LARGE);
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

#[tokio::test]
async fn operational_membership_requires_authentication_and_validates_contract() -> Result<()> {
    let state = AppState::new("test-service").with_membership(
        "secret",
        |operation_id, target| async move {
            Ok(MembershipView {
                operation_id,
                members: target
                    .members()
                    .map(|(node, endpoint)| (NodeId(node), endpoint.to_owned()))
                    .collect(),
                commit_index: 9,
            })
        },
        || async {
            Ok(MembershipView {
                operation_id: [1; 16],
                members: vec![(NodeId(1), "http://node-1:9080".to_owned())],
                commit_index: 8,
            })
        },
    );
    let app = router(state, Duration::from_secs(1));
    let unauthorized = app
        .clone()
        .oneshot(Request::get("/operational/v1/membership").body(Body::empty())?)
        .await?;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let request = Request::put("/operational/v1/membership")
        .header("authorization", "Bearer secret")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"operation_id":"018f3f5e-7b4a-7c15-9e37-79b97f4a7c15","members":[{"node_id":1,"peer_endpoint":"http://node-1:9080"}]}"#,
        ))?;
    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(body["commit_index"], 9);
    assert_eq!(body["members"][0]["node_id"], 1);
    Ok(())
}

#[tokio::test]
async fn authenticated_diagnostics_and_metrics_are_bounded_and_versioned() -> Result<()> {
    let metrics = Metrics::new();
    let state = AppState::new("test-service")
        .with_metrics(metrics)
        .with_membership(
            "secret",
            |_, _| async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
            || async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
        )
        .with_diagnostics(|| async { Ok(diagnostics()) });
    let app = router(state, Duration::from_secs(1));

    let status = app
        .clone()
        .oneshot(
            Request::get("/operational/v1/status")
                .header("authorization", "Bearer secret")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(status.status(), StatusCode::OK);
    let status: serde_json::Value =
        serde_json::from_slice(&to_bytes(status.into_body(), 16 * 1024).await?)?;
    assert_eq!(status["role"], "leader");
    assert_eq!(status["commit_index"], 9);
    assert_eq!(status["request_queue_depth"], 2);
    assert_eq!(status["peer_protocol_version"], 4);

    let metrics = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", "Bearer secret")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics = to_bytes(metrics.into_body(), 128 * 1024).await?;
    let metrics = std::str::from_utf8(&metrics)?;
    assert!(metrics.contains("raft_commit_index 9"));
    assert!(metrics.contains("raft_kv_build_info"));
    assert!(!metrics.contains(CLIENT));
    Ok(())
}

#[tokio::test]
async fn leadership_transfer_is_authenticated_and_reports_observed_completion() -> Result<()> {
    let state = AppState::new("test-service")
        .with_membership(
            "secret",
            |_, _| async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
            || async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
        )
        .with_leadership_transfer(|target| async move {
            Ok(LeadershipTransferResult {
                target: NodeId(target),
                term: Term(7),
            })
        });
    let app = router(state, Duration::from_secs(1));
    let unauthorized = app
        .clone()
        .oneshot(
            Request::post("/operational/v1/leadership-transfer")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"target_node_id":2}"#))?,
        )
        .await?;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .oneshot(
            Request::post("/operational/v1/leadership-transfer")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"target_node_id":2}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    assert_eq!(body["target_node_id"], 2);
    assert_eq!(body["term"], 7);
    Ok(())
}

#[tokio::test]
async fn leadership_transfer_errors_are_stable_and_fail_closed() -> Result<()> {
    for (error, expected) in [
        (
            LeadershipTransferError::InvalidTarget,
            StatusCode::BAD_REQUEST,
        ),
        (LeadershipTransferError::Busy, StatusCode::CONFLICT),
        (
            LeadershipTransferError::TimedOut,
            StatusCode::GATEWAY_TIMEOUT,
        ),
        (
            LeadershipTransferError::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            LeadershipTransferError::Overloaded,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            LeadershipTransferError::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let state = AppState::new("test-service")
            .with_membership(
                "secret",
                |_, _| async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
                || async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
            )
            .with_leadership_transfer(move |_| async move { Err(error) });
        let response = router(state, Duration::from_secs(1))
            .oneshot(
                Request::post("/operational/v1/leadership-transfer")
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"target_node_id":2}"#))?,
            )
            .await?;
        assert_eq!(response.status(), expected);
    }

    let state = AppState::new("test-service")
        .with_membership(
            "secret",
            |_, _| async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
            || async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
        )
        .with_leadership_transfer(|_| async {
            Err(raft_kv_service::cluster::LeadershipTransferError::InvalidTarget)
        });
    let response = router(state, Duration::from_secs(1))
        .oneshot(
            Request::post("/operational/v1/leadership-transfer")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"target_node_id":0}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let state = AppState::new("test-service").with_membership(
        "secret",
        |_, _| async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
        || async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
    );
    let response = router(state, Duration::from_secs(1))
        .oneshot(
            Request::post("/operational/v1/leadership-transfer")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"target_node_id":2}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

#[tokio::test]
async fn pre_candidate_is_visible_in_status_and_metrics() -> Result<()> {
    for (role, expected) in [
        (Role::Follower, "follower"),
        (Role::PreCandidate, "pre_candidate"),
        (Role::Candidate, "candidate"),
    ] {
        let mut status = diagnostics();
        status.role = role;
        status.leader = None;
        status.leadership_transfer_target = Some(NodeId(2));
        let state = AppState::new("test-service")
            .with_membership(
                "secret",
                |_, _| async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
                || async { Err(raft_kv_service::cluster::MembershipError::Unavailable) },
            )
            .with_diagnostics(move || {
                let status = status.clone();
                async move { Ok(status) }
            });
        let app = router(state, Duration::from_secs(1));
        let response = app
            .clone()
            .oneshot(
                Request::get("/operational/v1/status")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())?,
            )
            .await?;
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await?)?;
        assert_eq!(body["role"], expected);
        assert_eq!(body["leadership_transfer_target_id"], 2);
        let metrics = app
            .oneshot(
                Request::get("/metrics")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(metrics.status(), StatusCode::OK);
    }
    Ok(())
}
