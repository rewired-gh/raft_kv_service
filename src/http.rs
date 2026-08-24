use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{
            AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH, LOCATION,
        },
    },
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};
use uuid::Uuid;

use crate::kv::{
    ClientId, Command, CommandResult, Mutation, MutationCommand, MutationResult, ReadResult,
    validate_key,
};
use crate::{
    cluster::{MembershipError, MembershipView},
    raft::ClusterConfiguration,
};

const REQUEST_ID_HEADER: &str = "x-request-id";
const CLIENT_ID_HEADER: &str = "raft-kv-client-id";
const REQUEST_SEQUENCE_HEADER: &str = "raft-kv-request-sequence";
const REVISION_HEADER: &str = "raft-kv-revision";
const MAX_VALUE_BYTES: usize = 1024 * 1024;

type ExecutionFuture = Pin<Box<dyn Future<Output = Result<CommandResult, ServiceError>> + Send>>;
type Executor = Arc<dyn Fn(Command) -> ExecutionFuture + Send + Sync>;
type MembershipFuture =
    Pin<Box<dyn Future<Output = Result<MembershipView, MembershipError>> + Send>>;
type MembershipChanger =
    Arc<dyn Fn([u8; 16], ClusterConfiguration) -> MembershipFuture + Send + Sync>;
type MembershipReader = Arc<dyn Fn() -> MembershipFuture + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    service_name: Arc<str>,
    ready: Arc<AtomicBool>,
    executor: Option<Executor>,
    membership_changer: Option<MembershipChanger>,
    membership_reader: Option<MembershipReader>,
    operational_token: Option<Arc<str>>,
}

impl AppState {
    pub fn new(service_name: impl Into<Arc<str>>) -> Self {
        Self {
            service_name: service_name.into(),
            ready: Arc::new(AtomicBool::new(false)),
            executor: None,
            membership_changer: None,
            membership_reader: None,
            operational_token: None,
        }
    }

    pub fn with_membership<C, CFut, R, RFut>(
        mut self,
        token: impl Into<Arc<str>>,
        changer: C,
        reader: R,
    ) -> Self
    where
        C: Fn([u8; 16], ClusterConfiguration) -> CFut + Send + Sync + 'static,
        CFut: Future<Output = Result<MembershipView, MembershipError>> + Send + 'static,
        R: Fn() -> RFut + Send + Sync + 'static,
        RFut: Future<Output = Result<MembershipView, MembershipError>> + Send + 'static,
    {
        self.operational_token = Some(token.into());
        self.membership_changer = Some(Arc::new(move |id, target| Box::pin(changer(id, target))));
        self.membership_reader = Some(Arc::new(move || Box::pin(reader())));
        self
    }

    pub fn with_executor<F, Fut>(mut self, executor: F) -> Self
    where
        F: Fn(Command) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CommandResult, ServiceError>> + Send + 'static,
    {
        self.executor = Some(Arc::new(move |command| Box::pin(executor(command))));
        self
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    async fn execute(&self, command: Command) -> Result<CommandResult, ServiceError> {
        match &self.executor {
            Some(executor) => executor(command).await,
            None => Err(ServiceError::Unavailable),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceError {
    Unavailable,
    Overloaded,
    Internal,
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
        .layer(RequestBodyLimitLayer::new(MAX_VALUE_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ));

    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/v1/keys/{key}", get(read).put(put_key).delete(delete_key))
        .route(
            "/operational/v1/membership",
            get(get_membership).put(put_membership),
        )
        .fallback(not_found)
        .with_state(state)
        .layer(middleware)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MembershipRequestBody {
    operation_id: Uuid,
    members: Vec<MemberBody>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MemberBody {
    node_id: u64,
    peer_endpoint: String,
}

#[derive(Serialize)]
struct MembershipResponseBody {
    operation_id: Uuid,
    members: Vec<MemberBody>,
    commit_index: u64,
}

async fn get_membership(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers);
    if !operational_authorized(&state, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "operational-unauthorized",
            "Unauthorized",
            "a valid operational bearer token is required",
            request_id,
        )
        .into_response();
    }
    let Some(reader) = &state.membership_reader else {
        return service_problem(ServiceError::Unavailable, request_id).into_response();
    };
    membership_response(reader().await, request_id)
}

async fn put_membership(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<MembershipRequestBody>,
) -> Response {
    let request_id = request_id(&headers);
    if !operational_authorized(&state, &headers) {
        return problem(
            StatusCode::UNAUTHORIZED,
            "operational-unauthorized",
            "Unauthorized",
            "a valid operational bearer token is required",
            request_id,
        )
        .into_response();
    }
    if body.members.iter().any(|member| {
        !(member.peer_endpoint.starts_with("http://")
            || member.peer_endpoint.starts_with("https://"))
    }) {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid-membership",
            "Invalid membership",
            "peer endpoints must use an explicit http or https URI",
            request_id,
        )
        .into_response();
    }
    let target = match ClusterConfiguration::new(
        body.members
            .into_iter()
            .map(|member| (member.node_id, member.peer_endpoint)),
    ) {
        Ok(target) => target,
        Err(_) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "invalid-membership",
                "Invalid membership",
                "members must be non-empty, unique, nonzero, and have valid peer endpoints",
                request_id,
            )
            .into_response();
        }
    };
    let Some(changer) = &state.membership_changer else {
        return service_problem(ServiceError::Unavailable, request_id).into_response();
    };
    membership_response(
        changer(*body.operation_id.as_bytes(), target).await,
        request_id,
    )
}

fn operational_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(token) = &state.operational_token else {
        return false;
    };
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| provided == token.as_ref())
}

fn membership_response(
    result: Result<MembershipView, MembershipError>,
    request_id: String,
) -> Response {
    match result {
        Ok(view) => Json(MembershipResponseBody {
            operation_id: Uuid::from_bytes(view.operation_id),
            members: view
                .members
                .into_iter()
                .map(|(node, endpoint)| MemberBody {
                    node_id: node.0,
                    peer_endpoint: endpoint,
                })
                .collect(),
            commit_index: view.commit_index,
        })
        .into_response(),
        Err(error) => {
            let (status, kind, title, detail) = match error {
                MembershipError::Unavailable => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "membership-unavailable",
                    "Membership unavailable",
                    "the leader is unknown or unable to complete the operation",
                ),
                MembershipError::Overloaded => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "membership-overloaded",
                    "Membership overloaded",
                    "the bounded membership request queue is full",
                ),
                MembershipError::Invalid => (
                    StatusCode::BAD_REQUEST,
                    "invalid-membership",
                    "Invalid membership",
                    "the membership request is invalid",
                ),
                MembershipError::Conflict => (
                    StatusCode::CONFLICT,
                    "membership-operation-conflict",
                    "Membership operation conflict",
                    "the operation ID was previously used for a different target",
                ),
                MembershipError::ChangeInProgress => (
                    StatusCode::CONFLICT,
                    "membership-change-in-progress",
                    "Membership change in progress",
                    "another membership operation is in progress",
                ),
                MembershipError::Internal => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal-error",
                    "Internal error",
                    "the membership operation failed internally",
                ),
            };
            problem(status, kind, title, detail, request_id).into_response()
        }
    }
}

async fn liveness(State(state): State<AppState>) -> Json<Health> {
    Json(Health::up(state.service_name.to_string()))
}

async fn readiness(State(state): State<AppState>) -> (StatusCode, Json<Health>) {
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

async fn put_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = request_id(&headers);
    if let Err(problem) = validate_http_key(&key, &request_id) {
        return problem.into_response();
    }
    if body.len() > MAX_VALUE_BYTES {
        return problem(
            StatusCode::PAYLOAD_TOO_LARGE,
            "value-too-large",
            "Value too large",
            "the request value exceeds the configured maximum",
            request_id,
        )
        .into_response();
    }
    let location = format!("/v1/keys/{key}");
    let mutation = match put_precondition(&headers, key, body.to_vec(), &request_id) {
        Ok(mutation) => mutation,
        Err(problem) => return problem.into_response(),
    };
    if headers.get(CONTENT_TYPE).map(HeaderValue::as_bytes) != Some(b"application/octet-stream") {
        return problem(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported-media-type",
            "Unsupported media type",
            "create and update require application/octet-stream",
            request_id,
        )
        .into_response();
    }
    let identity = match mutation_identity(&headers, &request_id) {
        Ok(identity) => identity,
        Err(problem) => return problem.into_response(),
    };
    let location = matches!(mutation, Mutation::Create { .. }).then_some(location);
    mutation_response(
        state,
        MutationCommand {
            client_id: identity.0,
            sequence: identity.1,
            mutation,
        },
        location,
        request_id,
    )
    .await
}

async fn delete_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id(&headers);
    if let Err(problem) = validate_http_key(&key, &request_id) {
        return problem.into_response();
    }
    let expected_revision = match required_if_match(&headers, &request_id) {
        Ok(revision) => revision,
        Err(problem) => return problem.into_response(),
    };
    let identity = match mutation_identity(&headers, &request_id) {
        Ok(identity) => identity,
        Err(problem) => return problem.into_response(),
    };
    mutation_response(
        state,
        MutationCommand {
            client_id: identity.0,
            sequence: identity.1,
            mutation: Mutation::Delete {
                key,
                expected_revision,
            },
        },
        None,
        request_id,
    )
    .await
}

async fn read(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let request_id = request_id(&headers);
    if let Err(problem) = validate_http_key(&key, &request_id) {
        return problem.into_response();
    }
    match state.execute(Command::Read { key }).await {
        Ok(CommandResult::Read(ReadResult::Found { value, revision })) => {
            response_with_revision(StatusCode::OK, revision, Some(value), true)
        }
        Ok(CommandResult::Read(ReadResult::NotFound)) => problem(
            StatusCode::NOT_FOUND,
            "key-not-found",
            "Key not found",
            "the key does not exist at the read linearization point",
            request_id,
        )
        .into_response(),
        Ok(CommandResult::Mutation(_)) => internal_problem(request_id).into_response(),
        Err(error) => service_problem(error, request_id).into_response(),
    }
}

async fn mutation_response(
    state: AppState,
    command: MutationCommand,
    location: Option<String>,
    request_id: String,
) -> Response {
    match state.execute(Command::Mutate(command)).await {
        Ok(CommandResult::Mutation(result)) => {
            mutation_result_response(*unwrap_duplicate(&result), location, request_id)
        }
        Ok(CommandResult::Read(_)) => internal_problem(request_id).into_response(),
        Err(error) => service_problem(error, request_id).into_response(),
    }
}

fn mutation_result_response(
    result: MutationResult,
    location: Option<String>,
    request_id: String,
) -> Response {
    match result {
        MutationResult::Created { revision } => {
            let mut response = response_with_revision(StatusCode::CREATED, revision, None, true);
            if let Some(location) = location.and_then(|value| HeaderValue::from_str(&value).ok()) {
                response.headers_mut().insert(LOCATION, location);
            }
            response
        }
        MutationResult::Updated { revision } => {
            response_with_revision(StatusCode::NO_CONTENT, revision, None, true)
        }
        MutationResult::Deleted { revision } => {
            response_with_revision(StatusCode::NO_CONTENT, revision, None, false)
        }
        MutationResult::PreconditionFailed { .. } => problem(
            StatusCode::PRECONDITION_FAILED,
            "precondition-failed",
            "Precondition failed",
            "the resource precondition was false at the linearization point",
            request_id,
        )
        .into_response(),
        MutationResult::StaleSequence { .. } => conflict_problem(
            "stale-sequence",
            "the request sequence is older than the latest consumed sequence",
            request_id,
        ),
        MutationResult::SequenceGap { .. } => conflict_problem(
            "sequence-gap",
            "the request sequence is not the next expected sequence",
            request_id,
        ),
        MutationResult::IdentityConflict { .. } => conflict_problem(
            "request-identity-conflict",
            "the sequence was already used for different request content",
            request_id,
        ),
        MutationResult::ClientCapacityReached => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "client-capacity-reached",
            "Client capacity reached",
            "the durable client record capacity is full",
            request_id,
        )
        .into_response(),
        MutationResult::Duplicate(result) => {
            mutation_result_response(*result, location, request_id)
        }
    }
}

fn unwrap_duplicate(result: &MutationResult) -> Box<MutationResult> {
    match result {
        MutationResult::Duplicate(result) => unwrap_duplicate(result),
        result => Box::new(result.clone()),
    }
}

fn put_precondition(
    headers: &HeaderMap,
    key: String,
    value: Vec<u8>,
    request_id: &str,
) -> Result<Mutation, Problem> {
    match (headers.get(IF_NONE_MATCH), headers.get(IF_MATCH)) {
        (Some(header), None) if header.as_bytes() == b"*" => Ok(Mutation::Create { key, value }),
        (None, Some(_)) => Ok(Mutation::Update {
            key,
            value,
            expected_revision: required_if_match(headers, request_id)?,
        }),
        (None, None) => Err(problem(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition-required",
            "Precondition required",
            "create requires If-None-Match: *; update requires If-Match",
            request_id.to_owned(),
        )),
        _ => Err(problem(
            StatusCode::BAD_REQUEST,
            "invalid-precondition",
            "Invalid precondition",
            "supply exactly one supported conditional request header",
            request_id.to_owned(),
        )),
    }
}

fn required_if_match(headers: &HeaderMap, request_id: &str) -> Result<Option<u64>, Problem> {
    let Some(value) = headers.get(IF_MATCH) else {
        return Err(problem(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition-required",
            "Precondition required",
            "this operation requires If-Match: * or an exact revision ETag",
            request_id.to_owned(),
        ));
    };
    if value.as_bytes() == b"*" {
        return Ok(None);
    }
    let value = value.to_str().map_err(|_| invalid_etag(request_id))?;
    let revision = value
        .strip_prefix("\"rev-")
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| invalid_etag(request_id))?
        .parse::<u64>()
        .map_err(|_| invalid_etag(request_id))?;
    (revision > 0)
        .then_some(Some(revision))
        .ok_or_else(|| invalid_etag(request_id))
}

fn mutation_identity(headers: &HeaderMap, request_id: &str) -> Result<(ClientId, u64), Problem> {
    let client = headers
        .get(CLIENT_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| {
            invalid_request(
                "invalid-client-id",
                "a valid UUID client ID is required",
                request_id,
            )
        })?;
    let sequence = headers
        .get(REQUEST_SEQUENCE_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|sequence| *sequence > 0)
        .ok_or_else(|| {
            invalid_request(
                "invalid-request-sequence",
                "a positive decimal request sequence is required",
                request_id,
            )
        })?;
    Ok((ClientId(*client.as_bytes()), sequence))
}

fn validate_http_key(key: &str, request_id: &str) -> Result<(), Problem> {
    validate_key(key).map_err(|_| {
        invalid_request(
            "invalid-key",
            "keys must be non-empty URI-unreserved ASCII strings",
            request_id,
        )
    })
}

fn response_with_revision(
    status: StatusCode,
    revision: u64,
    body: Option<Vec<u8>>,
    include_etag: bool,
) -> Response {
    let mut response = (status, body.unwrap_or_default()).into_response();
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(value) = HeaderValue::from_str(&revision.to_string()) {
        headers.insert(HeaderName::from_static(REVISION_HEADER), value);
    }
    if include_etag && let Ok(value) = HeaderValue::from_str(&format!("\"rev-{revision}\"")) {
        headers.insert(ETAG, value);
    }
    if status == StatusCode::OK {
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
    }
    response
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
        .to_owned()
}

async fn not_found(headers: HeaderMap) -> Response {
    problem(
        StatusCode::NOT_FOUND,
        "route-not-found",
        "Route not found",
        "the requested route does not exist",
        request_id(&headers),
    )
    .into_response()
}

fn service_problem(error: ServiceError, request_id: String) -> Problem {
    match error {
        ServiceError::Unavailable => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "leader-unavailable",
            "Leader unavailable",
            "no usable Raft leader is currently available",
            request_id,
        ),
        ServiceError::Overloaded => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            "Service overloaded",
            "the bounded request queue is full",
            request_id,
        ),
        ServiceError::Internal => internal_problem(request_id),
    }
}

fn conflict_problem(kind: &'static str, detail: &'static str, request_id: String) -> Response {
    problem(
        StatusCode::CONFLICT,
        kind,
        "Request sequence conflict",
        detail,
        request_id,
    )
    .into_response()
}

fn invalid_etag(request_id: &str) -> Problem {
    invalid_request(
        "invalid-etag",
        "If-Match must be * or a strong revision ETag",
        request_id,
    )
}

fn invalid_request(kind: &'static str, detail: &'static str, request_id: &str) -> Problem {
    problem(
        StatusCode::BAD_REQUEST,
        kind,
        "Invalid request",
        detail,
        request_id.to_owned(),
    )
}

fn internal_problem(request_id: String) -> Problem {
    problem(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal-error",
        "Internal error",
        "the request could not be completed",
        request_id,
    )
}

fn problem(
    status: StatusCode,
    kind: &'static str,
    title: &'static str,
    detail: &'static str,
    request_id: String,
) -> Problem {
    Problem {
        status,
        kind,
        title,
        detail,
        request_id,
    }
}

struct Problem {
    status: StatusCode,
    kind: &'static str,
    title: &'static str,
    detail: &'static str,
    request_id: String,
}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let status = self.status;
        let mut response = (
            status,
            Json(ProblemBody {
                problem_type: format!("urn:raft-kv:error:{}", self.kind),
                title: self.title,
                status: status.as_u16(),
                detail: self.detail,
                request_id: self.request_id,
            }),
        )
            .into_response();
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

#[derive(Serialize)]
struct ProblemBody {
    #[serde(rename = "type")]
    problem_type: String,
    title: &'static str,
    status: u16,
    detail: &'static str,
    request_id: String,
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
