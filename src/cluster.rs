//! Multi-process static-cluster runtime and gRPC transport adapters.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Channel, ClientTlsConfig, Identity, ServerTlsConfig},
};
use tracing::warn;

use crate::{
    http::ServiceError,
    kv::{Command, CommandResult, MutationResult, encode_command},
    metrics::Metrics,
    peer::{
        ClusterId, PeerContext, PeerLimits, SnapshotChunk, encode_append_entries,
        encode_append_entries_response, encode_forward_command, encode_forward_membership,
        encode_forward_membership_response, encode_forward_response, encode_request_vote,
        encode_request_vote_response, encode_snapshot_chunk, encode_snapshot_response, proto,
    },
    raft::{
        ClusterConfiguration, ConfigurationPhase, DurableRaft, Effect, EntryPayload, Envelope,
        Event, LogEntry, MembershipEntry, Message, NodeId, Role, Term,
    },
    storage::{AppliedEntry, Snapshot, SnapshotStorage, SqliteStorage, StateMachineStorage},
    tls::TlsMaterial,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipView {
    pub operation_id: [u8; 16],
    pub members: Vec<(NodeId, String)>,
    pub commit_index: u64,
}

#[derive(Clone, Debug)]
pub struct NodeDiagnostics {
    pub cluster_id: [u8; 16],
    pub node_id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub active_membership: MembershipEntry,
    pub committed_membership: MembershipEntry,
    pub replication: Vec<(NodeId, crate::raft::ReplicationProgress)>,
    pub request_queue_depth: usize,
    pub outbound_queue_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipError {
    Unavailable,
    Overloaded,
    Invalid,
    Conflict,
    ChangeInProgress,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerAdmission {
    pub certificate_sha256: [u8; 32],
    pub staging_expires_unix_seconds: Option<u64>,
}

struct PendingMembership {
    operation_id: [u8; 16],
    target: ClusterConfiguration,
    phase: MembershipPhase,
    responders: Vec<oneshot::Sender<Result<MembershipView, MembershipError>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MembershipPhase {
    Staging,
    Joint(u64),
    Stable(u64),
}

#[derive(Clone, Debug)]
pub struct StaticNodeConfig {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub voters: Vec<NodeId>,
    pub data_file: PathBuf,
    pub request_queue_capacity: usize,
    pub outbound_queue_capacity: usize,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
    pub max_clients: u64,
    pub snapshot_threshold_entries: u64,
    pub peer_endpoints: BTreeMap<NodeId, String>,
    pub local_peer_endpoint: String,
    pub staging: bool,
    pub peer_tls: Option<TlsMaterial>,
    pub peer_certificates: BTreeMap<NodeId, PeerAdmission>,
    pub metrics: Metrics,
}

pub struct RunningNode {
    pub handle: StaticNodeHandle,
    pub peer_service: PeerRpcService,
    outbound: tokio_mpsc::Receiver<Outbound>,
    endpoints: Arc<RwLock<BTreeMap<NodeId, String>>>,
    rpc_timeout: Duration,
    peer_tls: Option<TlsMaterial>,
}

impl RunningNode {
    pub fn spawn_dispatcher(mut self) -> (StaticNodeHandle, PeerRpcService) {
        let handle = self.handle.clone();
        let response_handle = handle.clone();
        let dispatcher_metrics = self.peer_service.metrics.clone();
        tokio::spawn(async move {
            while let Some(outbound) = self.outbound.recv().await {
                let to = match &outbound {
                    Outbound::Raft(envelope) => envelope.to,
                    Outbound::Snapshot { to, .. } => *to,
                };
                let Some(endpoint) = self
                    .endpoints
                    .read()
                    .ok()
                    .and_then(|endpoints| endpoints.get(&to).cloned())
                else {
                    warn!(peer = to.0, "missing peer endpoint; dropping Raft RPC");
                    continue;
                };
                let response_handle = response_handle.clone();
                let cluster_id = self.peer_service.context.cluster_id;
                let timeout = self.rpc_timeout;
                let peer_tls = self.peer_tls.clone();
                let metrics = dispatcher_metrics.clone();
                tokio::spawn(async move {
                    let _replication_timer = metrics.time_replication();
                    let result = match outbound {
                        Outbound::Raft(envelope) => {
                            dispatch(cluster_id, endpoint, envelope, timeout, peer_tls).await
                        }
                        Outbound::Snapshot {
                            from,
                            to,
                            term,
                            snapshot,
                        } => {
                            dispatch_snapshot(
                                cluster_id,
                                endpoint,
                                (from, to, term),
                                snapshot,
                                timeout,
                                peer_tls,
                            )
                            .await
                        }
                    };
                    match result {
                        Ok(response) => response_handle.receive_response(response),
                        Err(error) => {
                            if error.contains("timed out") {
                                metrics.timed_out();
                            }
                            warn!(%error, "peer RPC failed");
                        }
                    }
                });
            }
        });
        (handle, self.peer_service)
    }
}

enum Outbound {
    Raft(Envelope),
    Snapshot {
        from: NodeId,
        to: NodeId,
        term: Term,
        snapshot: Snapshot,
    },
}

#[derive(Clone)]
pub struct StaticNodeHandle {
    sender: SyncSender<NodeRequest>,
    cluster_id: ClusterId,
    node_id: NodeId,
    endpoints: Arc<RwLock<BTreeMap<NodeId, String>>>,
    rpc_timeout: Duration,
    peer_tls: Option<TlsMaterial>,
    request_queue_depth: Arc<AtomicUsize>,
    metrics: Metrics,
}

#[derive(Clone, Copy)]
enum EnqueueError {
    Full,
    Disconnected,
}

impl StaticNodeHandle {
    fn enqueue(&self, request: NodeRequest) -> Result<(), EnqueueError> {
        self.request_queue_depth.fetch_add(1, Ordering::Relaxed);
        match self.sender.try_send(request) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.request_queue_depth.fetch_sub(1, Ordering::Relaxed);
                Err(match error {
                    mpsc::TrySendError::Full(_) => EnqueueError::Full,
                    mpsc::TrySendError::Disconnected(_) => EnqueueError::Disconnected,
                })
            }
        }
    }
    pub async fn execute(&self, command: Command) -> Result<CommandResult, ServiceError> {
        match self.execute_local(command.clone()).await {
            Err(ServiceError::Unavailable) => self.forward(command).await,
            result => result,
        }
    }

    async fn execute_local(&self, command: Command) -> Result<CommandResult, ServiceError> {
        let (respond, response) = oneshot::channel();
        self.enqueue(NodeRequest::Client { command, respond })
            .map_err(|error| match error {
                EnqueueError::Full => ServiceError::Overloaded,
                EnqueueError::Disconnected => ServiceError::Unavailable,
            })?;
        response.await.map_err(|_| ServiceError::Unavailable)?
    }

    pub async fn change_membership(
        &self,
        operation_id: [u8; 16],
        target: ClusterConfiguration,
    ) -> Result<MembershipView, MembershipError> {
        match self
            .change_membership_local(operation_id, target.clone())
            .await
        {
            Err(MembershipError::Unavailable) => {
                self.forward_membership(Some((operation_id, &target))).await
            }
            result => result,
        }
    }

    async fn change_membership_local(
        &self,
        operation_id: [u8; 16],
        target: ClusterConfiguration,
    ) -> Result<MembershipView, MembershipError> {
        let (respond, response) = oneshot::channel();
        self.enqueue(NodeRequest::MembershipChange {
            operation_id,
            target,
            respond,
        })
        .map_err(|error| match error {
            EnqueueError::Full => MembershipError::Overloaded,
            EnqueueError::Disconnected => MembershipError::Unavailable,
        })?;
        response.await.map_err(|_| MembershipError::Unavailable)?
    }

    pub async fn membership(&self) -> Result<MembershipView, MembershipError> {
        match self.membership_local().await {
            Err(MembershipError::Unavailable) => self.forward_membership(None).await,
            result => result,
        }
    }

    async fn membership_local(&self) -> Result<MembershipView, MembershipError> {
        let (respond, response) = oneshot::channel();
        self.enqueue(NodeRequest::MembershipQuery { respond })
            .map_err(|error| match error {
                EnqueueError::Full => MembershipError::Overloaded,
                EnqueueError::Disconnected => MembershipError::Unavailable,
            })?;
        response.await.map_err(|_| MembershipError::Unavailable)?
    }

    async fn forward_membership(
        &self,
        operation: Option<([u8; 16], &ClusterConfiguration)>,
    ) -> Result<MembershipView, MembershipError> {
        self.metrics.forwarded();
        let (leader, term) = self
            .status()
            .await
            .map_err(|_| MembershipError::Unavailable)?;
        let leader = leader
            .filter(|leader| *leader != self.node_id)
            .ok_or(MembershipError::Unavailable)?;
        let endpoint = self
            .endpoints
            .read()
            .map_err(|_| MembershipError::Internal)?
            .get(&leader)
            .cloned()
            .ok_or(MembershipError::Unavailable)?;
        let channel = connect_peer(endpoint, self.rpc_timeout, self.peer_tls.clone())
            .await
            .map_err(|_| MembershipError::Unavailable)?;
        let request =
            encode_forward_membership(self.cluster_id, self.node_id, leader, term, operation)
                .map_err(|_| MembershipError::Internal)?;
        let response = tokio::time::timeout(
            self.rpc_timeout,
            peer_client(channel).forward_membership(request),
        )
        .await
        .map_err(|_| MembershipError::Unavailable)?
        .map_err(|status| match status.code() {
            tonic::Code::ResourceExhausted => MembershipError::Overloaded,
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                MembershipError::Unavailable
            }
            _ => MembershipError::Internal,
        })?
        .into_inner();
        PeerContext {
            cluster_id: self.cluster_id,
            local_id: self.node_id,
            limits: PeerLimits::default(),
        }
        .decode_forward_membership_response(response)
        .map_err(|_| MembershipError::Internal)?
    }

    async fn status(&self) -> Result<(Option<NodeId>, Term), ServiceError> {
        let (respond, response) = oneshot::channel();
        self.enqueue(NodeRequest::Status { respond })
            .map_err(|error| match error {
                EnqueueError::Full => ServiceError::Overloaded,
                EnqueueError::Disconnected => ServiceError::Unavailable,
            })?;
        response.await.map_err(|_| ServiceError::Unavailable)
    }

    async fn forward(&self, command: Command) -> Result<CommandResult, ServiceError> {
        self.metrics.forwarded();
        let (leader, term) = self.status().await?;
        let leader = leader
            .filter(|leader| *leader != self.node_id)
            .ok_or(ServiceError::Unavailable)?;
        let endpoint = self
            .endpoints
            .read()
            .map_err(|_| ServiceError::Internal)?
            .get(&leader)
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        let channel = connect_peer(endpoint, self.rpc_timeout, self.peer_tls.clone())
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        let mut client = peer_client(channel);
        let request = encode_forward_command(self.cluster_id, self.node_id, leader, term, &command)
            .map_err(|_| ServiceError::Internal)?;
        let response = tokio::time::timeout(self.rpc_timeout, client.forward_command(request))
            .await
            .map_err(|_| ServiceError::Unavailable)?
            .map_err(|status| match status.code() {
                tonic::Code::ResourceExhausted => ServiceError::Overloaded,
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    ServiceError::Unavailable
                }
                _ => ServiceError::Internal,
            })?
            .into_inner();
        PeerContext {
            cluster_id: self.cluster_id,
            local_id: self.node_id,
            limits: PeerLimits::default(),
        }
        .decode_forward_response(response)
        .map_err(|_| ServiceError::Internal)
    }

    fn receive_response(&self, envelope: Envelope) {
        let _ = self.enqueue(NodeRequest::PeerResponse(envelope));
    }

    async fn call(&self, envelope: Envelope) -> Result<Envelope, Status> {
        let (respond, response) = oneshot::channel();
        self.enqueue(NodeRequest::PeerCall { envelope, respond })
            .map_err(|error| match error {
                EnqueueError::Full => Status::resource_exhausted("peer queue is full"),
                EnqueueError::Disconnected => Status::unavailable("Raft worker stopped"),
            })?;
        response
            .await
            .map_err(|_| Status::unavailable("Raft worker stopped"))?
    }

    pub async fn diagnostics(&self) -> Result<NodeDiagnostics, MembershipError> {
        let (respond, response) = oneshot::channel();
        self.enqueue(NodeRequest::Diagnostics { respond })
            .map_err(|error| match error {
                EnqueueError::Full => MembershipError::Overloaded,
                EnqueueError::Disconnected => MembershipError::Unavailable,
            })?;
        response.await.map_err(|_| MembershipError::Unavailable)?
    }
}

pub fn start_static_node(config: StaticNodeConfig) -> Result<RunningNode, String> {
    validate_config(&config)?;
    if let Some(parent) = config.data_file.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let storage = SqliteStorage::open_with_max_clients(&config.data_file, config.max_clients)
        .map_err(|error| error.to_string())?;
    let members = config
        .voters
        .iter()
        .map(|node| {
            let endpoint = if *node == config.node_id {
                config.local_peer_endpoint.clone()
            } else {
                config
                    .peer_endpoints
                    .get(node)
                    .cloned()
                    .ok_or_else(|| format!("missing endpoint for voter {}", node.0))?
            };
            Ok((node.0, endpoint))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let membership = crate::raft::MembershipEntry::initial(
        crate::raft::ClusterConfiguration::new(members).map_err(|error| error.to_string())?,
    );
    let raft = DurableRaft::recover_with_snapshot_membership(
        config.node_id,
        membership,
        config.staging,
        storage,
    )
    .map_err(|error| error.to_string())?;
    let endpoints = Arc::new(RwLock::new(config.peer_endpoints.clone()));
    let request_queue_depth = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = mpsc::sync_channel(config.request_queue_capacity);
    let (outbound_sender, outbound) = tokio_mpsc::channel(config.outbound_queue_capacity);
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let worker_config = config.clone();
    let worker_endpoints = endpoints.clone();
    let worker_queue_depth = request_queue_depth.clone();
    thread::Builder::new()
        .name(format!("raft-node-{}", config.node_id.0))
        .spawn(move || {
            let mut worker = NodeWorker::new(
                raft,
                receiver,
                outbound_sender,
                worker_config,
                worker_endpoints,
                worker_queue_depth,
            );
            let started = worker.bootstrap();
            let can_run = started.is_ok();
            let _ = started_sender.send(started);
            if can_run {
                worker.run();
            }
        })
        .map_err(|error| error.to_string())?;
    started_receiver
        .recv()
        .map_err(|error| error.to_string())??;

    let handle = StaticNodeHandle {
        sender,
        cluster_id: config.cluster_id,
        node_id: config.node_id,
        endpoints: endpoints.clone(),
        rpc_timeout: config.rpc_timeout,
        peer_tls: config.peer_tls.clone(),
        request_queue_depth,
        metrics: config.metrics.clone(),
    };
    Ok(RunningNode {
        peer_service: PeerRpcService {
            context: PeerContext {
                cluster_id: config.cluster_id,
                local_id: config.node_id,
                limits: PeerLimits::default(),
            },
            handle: handle.clone(),
            peer_certificates: Arc::new(config.peer_certificates.clone()),
            require_peer_identity: config.peer_tls.is_some(),
            metrics: config.metrics.clone(),
        },
        handle,
        outbound,
        endpoints,
        rpc_timeout: config.rpc_timeout,
        peer_tls: config.peer_tls,
    })
}

fn validate_config(config: &StaticNodeConfig) -> Result<(), String> {
    if config.node_id.0 == 0
        || config.request_queue_capacity == 0
        || config.outbound_queue_capacity == 0
        || config.election_timeout_min.is_zero()
        || config.election_timeout_min > config.election_timeout_max
        || config.heartbeat_interval.is_zero()
        || config.heartbeat_interval >= config.election_timeout_min
        || config.rpc_timeout.is_zero()
        || config.max_clients == 0
        || config.snapshot_threshold_entries == 0
    {
        return Err("invalid static Raft runtime configuration".to_owned());
    }
    if !config.staging && !config.voters.contains(&config.node_id) {
        return Err("local node is not in the static voter set".to_owned());
    }
    for voter in &config.voters {
        if *voter != config.node_id && !config.peer_endpoints.contains_key(voter) {
            return Err(format!("missing endpoint for voter {}", voter.0));
        }
    }
    if config.peer_tls.is_some()
        && config
            .peer_endpoints
            .keys()
            .any(|node| !config.peer_certificates.contains_key(node))
    {
        return Err("peer TLS requires an admitted certificate fingerprint for every configured remote peer".to_owned());
    }
    Ok(())
}

enum NodeRequest {
    Client {
        command: Command,
        respond: oneshot::Sender<Result<CommandResult, ServiceError>>,
    },
    PeerCall {
        envelope: Envelope,
        respond: oneshot::Sender<Result<Envelope, Status>>,
    },
    PeerResponse(Envelope),
    Status {
        respond: oneshot::Sender<(Option<NodeId>, Term)>,
    },
    SnapshotChunk {
        chunk: SnapshotChunk,
        respond: oneshot::Sender<Result<SnapshotProgress, Status>>,
    },
    MembershipChange {
        operation_id: [u8; 16],
        target: ClusterConfiguration,
        respond: oneshot::Sender<Result<MembershipView, MembershipError>>,
    },
    MembershipQuery {
        respond: oneshot::Sender<Result<MembershipView, MembershipError>>,
    },
    Diagnostics {
        respond: oneshot::Sender<Result<NodeDiagnostics, MembershipError>>,
    },
    AuthorizePeer {
        node_id: NodeId,
        respond: oneshot::Sender<bool>,
    },
}

struct SnapshotProgress {
    snapshot_id: [u8; 32],
    next_offset: usize,
    installed: bool,
    term: Term,
}

struct NodeWorker {
    raft: DurableRaft<SqliteStorage>,
    receiver: Receiver<NodeRequest>,
    outbound: tokio_mpsc::Sender<Outbound>,
    config: StaticNodeConfig,
    pending_clients: HashMap<u64, oneshot::Sender<Result<CommandResult, ServiceError>>>,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    incoming_snapshot: Option<IncomingSnapshot>,
    endpoints: Arc<RwLock<BTreeMap<NodeId, String>>>,
    pending_membership: Option<PendingMembership>,
    request_queue_depth: Arc<AtomicUsize>,
    metrics: Metrics,
}

struct IncomingSnapshot {
    sender: NodeId,
    term: Term,
    format_version: u32,
    last_included: crate::raft::LogPosition,
    voters: Vec<NodeId>,
    membership: crate::raft::MembershipEntry,
    snapshot_id: [u8; 32],
    total_size: usize,
    bytes: Vec<u8>,
}

impl NodeWorker {
    fn new(
        raft: DurableRaft<SqliteStorage>,
        receiver: Receiver<NodeRequest>,
        outbound: tokio_mpsc::Sender<Outbound>,
        config: StaticNodeConfig,
        endpoints: Arc<RwLock<BTreeMap<NodeId, String>>>,
        request_queue_depth: Arc<AtomicUsize>,
    ) -> Self {
        let now = Instant::now();
        Self {
            raft,
            receiver,
            outbound,
            election_deadline: now + random_election_timeout(&config),
            heartbeat_deadline: now + config.heartbeat_interval,
            pending_clients: HashMap::new(),
            incoming_snapshot: None,
            endpoints,
            pending_membership: None,
            request_queue_depth,
            metrics: config.metrics.clone(),
            config,
        }
    }

    fn bootstrap(&mut self) -> Result<(), String> {
        if self.config.voters.len() == 1 {
            let effects = self
                .raft
                .handle(Event::ElectionTimeout)
                .map_err(|error| error.to_string())?;
            self.process(effects, None)?;
        }
        Ok(())
    }

    fn run(&mut self) {
        loop {
            let now = Instant::now();
            let deadline = if self.raft.core().role() == Role::Leader {
                self.heartbeat_deadline
            } else {
                self.election_deadline
            };
            match self
                .receiver
                .recv_timeout(deadline.saturating_duration_since(now))
            {
                Ok(request) => {
                    self.request_queue_depth.fetch_sub(1, Ordering::Relaxed);
                    if let Err(error) = self.handle_request(request) {
                        warn!(%error, node = self.config.node_id.0, "Raft worker request failed");
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(error) = self.on_timer() {
                        warn!(%error, node = self.config.node_id.0, "Raft timer action failed");
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn handle_request(&mut self, request: NodeRequest) -> Result<(), String> {
        match request {
            NodeRequest::Client { command, respond } => self.handle_client(command, respond),
            NodeRequest::PeerCall { envelope, respond } => {
                let was_leader = self.raft.core().role() == Role::Leader;
                let effects = self
                    .raft
                    .handle(Event::Receive(envelope.clone()))
                    .map_err(|error| error.to_string())?;
                let sends = self.process(effects, Some(envelope.from))?;
                let response = sends
                    .into_iter()
                    .find(|candidate| candidate.to == envelope.from)
                    .ok_or_else(|| "core did not produce an RPC response".to_owned());
                let _ = respond.send(response.map_err(Status::internal));
                self.fail_pending_after_stepdown(was_leader);
                Ok(())
            }
            NodeRequest::PeerResponse(envelope) => {
                if matches!(
                    envelope.message,
                    Message::AppendEntriesResponse {
                        accepted: false,
                        ..
                    }
                ) {
                    self.metrics.retried();
                }
                let was_leader = self.raft.core().role() == Role::Leader;
                let effects = self
                    .raft
                    .handle(Event::Receive(envelope))
                    .map_err(|error| error.to_string())?;
                self.process(effects, None)?;
                self.maybe_start_joint()?;
                self.fail_pending_after_stepdown(was_leader);
                Ok(())
            }
            NodeRequest::Status { respond } => {
                let _ = respond.send((
                    self.raft.core().leader(),
                    self.raft.core().hard_state().current_term,
                ));
                Ok(())
            }
            NodeRequest::SnapshotChunk { chunk, respond } => {
                let was_leader = self.raft.core().role() == Role::Leader;
                let result = self.accept_snapshot_chunk(chunk);
                let _ = respond.send(result);
                self.fail_pending_after_stepdown(was_leader);
                Ok(())
            }
            NodeRequest::MembershipChange {
                operation_id,
                target,
                respond,
            } => self.handle_membership_change(operation_id, target, respond),
            NodeRequest::MembershipQuery { respond } => {
                let result = self.membership_view();
                let _ = respond.send(result);
                Ok(())
            }
            NodeRequest::Diagnostics { respond } => {
                let result = self.diagnostics();
                let _ = respond.send(result);
                Ok(())
            }
            NodeRequest::AuthorizePeer { node_id, respond } => {
                let _ = respond.send(self.peer_is_authorized(node_id));
                Ok(())
            }
        }
    }

    fn peer_is_authorized(&self, node_id: NodeId) -> bool {
        let active = self
            .raft
            .core()
            .committed_membership()
            .is_ok_and(|membership| {
                membership.old.node_ids().any(|node| node == node_id.0)
                    || membership.new.node_ids().any(|node| node == node_id.0)
            });
        if active {
            return true;
        }
        let staged = self
            .pending_membership
            .as_ref()
            .is_some_and(|pending| pending.target.node_ids().any(|node| node == node_id.0));
        let not_expired = self
            .config
            .peer_certificates
            .get(&node_id)
            .and_then(|admission| admission.staging_expires_unix_seconds)
            .is_some_and(|expiry| expiry > unix_time_seconds());
        staged && not_expired
    }

    fn handle_client(
        &mut self,
        command: Command,
        respond: oneshot::Sender<Result<CommandResult, ServiceError>>,
    ) -> Result<(), String> {
        if !self.raft.core().leader_ready() {
            let _ = respond.send(Err(ServiceError::Unavailable));
            return Ok(());
        }
        if self.pending_clients.len() >= self.config.request_queue_capacity {
            self.metrics.reject();
            let _ = respond.send(Err(ServiceError::Overloaded));
            return Ok(());
        }
        let encoded = encode_command(&command).map_err(|error| error.to_string())?;
        let (index, effects) = self
            .raft
            .propose(EntryPayload::Command(encoded))
            .map_err(|error| error.to_string())?;
        self.pending_clients.insert(index, respond);
        self.process(effects, None)?;
        Ok(())
    }

    fn membership_view(&self) -> Result<MembershipView, MembershipError> {
        if !self.raft.core().leader_ready() {
            return Err(MembershipError::Unavailable);
        }
        let membership = self
            .raft
            .core()
            .committed_membership()
            .map_err(|_| MembershipError::Internal)?;
        Ok(MembershipView {
            operation_id: membership.operation_id,
            members: membership
                .new
                .members()
                .map(|(node, endpoint)| (NodeId(node), endpoint.to_owned()))
                .collect(),
            commit_index: self.raft.core().last_applied(),
        })
    }

    fn diagnostics(&self) -> Result<NodeDiagnostics, MembershipError> {
        let active_membership = self.raft.core().membership().clone();
        let committed_membership = self
            .raft
            .core()
            .committed_membership()
            .map_err(|_| MembershipError::Internal)?;
        let replication = self
            .raft
            .core()
            .voter_ids()
            .into_iter()
            .filter(|node| *node != self.config.node_id)
            .filter_map(|node| {
                self.raft
                    .core()
                    .replication_progress(node)
                    .map(|progress| (node, progress))
            })
            .collect();
        Ok(NodeDiagnostics {
            cluster_id: self.config.cluster_id.0,
            node_id: self.config.node_id,
            role: self.raft.core().role(),
            term: self.raft.core().hard_state().current_term,
            leader: self.raft.core().leader(),
            commit_index: self.raft.core().commit_index(),
            applied_index: self.raft.core().last_applied(),
            snapshot_index: self.raft.core().snapshot_position().index,
            active_membership,
            committed_membership,
            replication,
            request_queue_depth: self.request_queue_depth.load(Ordering::Relaxed),
            outbound_queue_depth: self
                .outbound
                .max_capacity()
                .saturating_sub(self.outbound.capacity()),
        })
    }

    fn handle_membership_change(
        &mut self,
        operation_id: [u8; 16],
        target: ClusterConfiguration,
        respond: oneshot::Sender<Result<MembershipView, MembershipError>>,
    ) -> Result<(), String> {
        if operation_id == [0; 16] {
            let _ = respond.send(Err(MembershipError::Invalid));
            return Ok(());
        }
        if !self.raft.core().leader_ready() {
            let _ = respond.send(Err(MembershipError::Unavailable));
            return Ok(());
        }
        if let Some(committed) = self
            .raft
            .storage()
            .membership_operation(operation_id)
            .map_err(|error| error.to_string())?
        {
            let result = if committed.new == target {
                self.membership_view()
            } else {
                Err(MembershipError::Conflict)
            };
            let _ = respond.send(result);
            return Ok(());
        }
        if let Some(pending) = self.pending_membership.as_mut() {
            if pending.operation_id == operation_id && pending.target == target {
                if pending.responders.len() >= self.config.request_queue_capacity {
                    self.metrics.reject();
                    let _ = respond.send(Err(MembershipError::Overloaded));
                } else {
                    pending.responders.push(respond);
                }
            } else {
                let error = if pending.operation_id == operation_id {
                    MembershipError::Conflict
                } else {
                    MembershipError::ChangeInProgress
                };
                let _ = respond.send(Err(error));
            }
            return Ok(());
        }
        if self.raft.core().membership_is_joint() {
            let _ = respond.send(Err(MembershipError::ChangeInProgress));
            return Ok(());
        }
        if self.config.peer_tls.is_some() {
            let committed = self
                .raft
                .core()
                .committed_membership()
                .map_err(|error| error.to_string())?;
            let current = committed
                .old
                .node_ids()
                .chain(committed.new.node_ids())
                .map(NodeId)
                .collect::<std::collections::BTreeSet<_>>();
            let invalid = target.node_ids().map(NodeId).any(|node| {
                if node == self.config.node_id {
                    return false;
                }
                let Some(admission) = self.config.peer_certificates.get(&node) else {
                    return true;
                };
                !current.contains(&node)
                    && !admission
                        .staging_expires_unix_seconds
                        .is_some_and(|expiry| expiry > unix_time_seconds())
            });
            if invalid {
                let _ = respond.send(Err(MembershipError::Invalid));
                return Ok(());
            }
        }

        {
            let mut endpoints = self
                .endpoints
                .write()
                .map_err(|_| "peer endpoint lock is poisoned".to_owned())?;
            for (node, endpoint) in target.members() {
                if NodeId(node) != self.config.node_id {
                    endpoints.insert(NodeId(node), endpoint.to_owned());
                }
            }
        }
        self.pending_membership = Some(PendingMembership {
            operation_id,
            target,
            phase: MembershipPhase::Staging,
            responders: vec![respond],
        });

        let committed = self
            .raft
            .core()
            .committed_membership()
            .map_err(|error| error.to_string())?;
        let current = committed
            .new
            .node_ids()
            .map(NodeId)
            .collect::<std::collections::BTreeSet<_>>();
        let prospective = self
            .pending_membership
            .as_ref()
            .expect("pending membership was installed")
            .target
            .node_ids()
            .map(NodeId)
            .filter(|node| !current.contains(node))
            .collect::<Vec<_>>();
        for peer in prospective {
            let effects = self
                .raft
                .stage_peer(peer)
                .map_err(|error| error.to_string())?;
            self.process(effects, None)?;
        }
        self.maybe_start_joint()
    }

    fn maybe_start_joint(&mut self) -> Result<(), String> {
        let Some(pending) = self.pending_membership.as_ref() else {
            return Ok(());
        };
        if pending.phase != MembershipPhase::Staging || !self.raft.core().leader_ready() {
            return Ok(());
        }
        let committed = self
            .raft
            .core()
            .committed_membership()
            .map_err(|error| error.to_string())?;
        let current = committed
            .new
            .node_ids()
            .map(NodeId)
            .collect::<std::collections::BTreeSet<_>>();
        let last_index = self.raft.core().last_log_position().index;
        let caught_up = pending
            .target
            .node_ids()
            .map(NodeId)
            .filter(|node| !current.contains(node))
            .all(|node| {
                self.raft
                    .core()
                    .replication_progress(node)
                    .is_some_and(|progress| progress.match_index >= last_index)
            });
        if !caught_up {
            return Ok(());
        }
        let operation_id = pending.operation_id;
        let target = pending.target.clone();
        let (index, effects) = self
            .raft
            .propose_membership(MembershipEntry {
                operation_id,
                phase: ConfigurationPhase::Joint,
                old: committed.new,
                new: target,
            })
            .map_err(|error| error.to_string())?;
        self.pending_membership
            .as_mut()
            .expect("membership remains pending")
            .phase = MembershipPhase::Joint(index);
        self.process(effects, None).map(|_| ())
    }

    fn membership_entries_applied(&mut self, entries: &[LogEntry]) -> Result<(), String> {
        for entry in entries {
            let EntryPayload::Configuration(configuration) = &entry.payload else {
                continue;
            };
            let Some(pending) = self.pending_membership.as_ref() else {
                continue;
            };
            if pending.operation_id != configuration.operation_id {
                continue;
            }
            match (pending.phase, configuration.phase) {
                (MembershipPhase::Joint(index), ConfigurationPhase::Joint)
                    if index == entry.index =>
                {
                    let target = pending.target.clone();
                    let operation_id = pending.operation_id;
                    let (stable_index, effects) = self
                        .raft
                        .propose_membership(MembershipEntry {
                            operation_id,
                            phase: ConfigurationPhase::Stable,
                            old: target.clone(),
                            new: target,
                        })
                        .map_err(|error| error.to_string())?;
                    self.pending_membership
                        .as_mut()
                        .expect("membership remains pending")
                        .phase = MembershipPhase::Stable(stable_index);
                    self.process(effects, None)?;
                }
                (MembershipPhase::Stable(index), ConfigurationPhase::Stable)
                    if index == entry.index =>
                {
                    let view = Ok(MembershipView {
                        operation_id: configuration.operation_id,
                        members: configuration
                            .new
                            .members()
                            .map(|(node, endpoint)| (NodeId(node), endpoint.to_owned()))
                            .collect(),
                        commit_index: entry.index,
                    });
                    if let Some(pending) = self.pending_membership.take() {
                        for respond in pending.responders {
                            let _ = respond.send(view.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn on_timer(&mut self) -> Result<(), String> {
        let _election_timer =
            (self.raft.core().role() != Role::Leader).then(|| self.metrics.time_election());
        let effects = if self.raft.core().role() == Role::Leader {
            self.heartbeat_deadline = Instant::now() + self.config.heartbeat_interval;
            self.raft.heartbeat().map_err(|error| error.to_string())?
        } else {
            self.metrics.election();
            self.election_deadline = Instant::now() + random_election_timeout(&self.config);
            self.raft
                .handle(Event::ElectionTimeout)
                .map_err(|error| error.to_string())?
        };
        self.process(effects, None)?;
        Ok(())
    }

    fn process(
        &mut self,
        effects: Vec<Effect>,
        direct_peer: Option<NodeId>,
    ) -> Result<Vec<Envelope>, String> {
        let mut pending = VecDeque::from(effects);
        let mut direct = Vec::new();
        while let Some(effect) = pending.pop_front() {
            match effect {
                Effect::ApplyCommitted(entries) => {
                    let membership_entries = entries.clone();
                    let _apply_timer = self.metrics.time_apply();
                    let _storage_timer = self.metrics.time_storage();
                    let batch = self
                        .raft
                        .apply_committed(&entries)
                        .map_err(|error| error.to_string())?;
                    self.complete_clients(batch.entries);
                    pending.extend(batch.effects);
                    self.membership_entries_applied(&membership_entries)?;
                    let compacted = self.raft.core().snapshot_position().index;
                    let applied = self.raft.core().last_applied();
                    if applied.saturating_sub(compacted) >= self.config.snapshot_threshold_entries {
                        let _snapshot_timer = self.metrics.time_snapshot();
                        self.raft
                            .create_snapshot(self.config.cluster_id.0)
                            .map_err(|error| error.to_string())?;
                        self.metrics.snapshot();
                    }
                }
                Effect::Send(envelope)
                    if direct_peer == Some(envelope.to) && is_response(&envelope) =>
                {
                    direct.push(envelope);
                }
                Effect::Send(envelope) => {
                    let _ = self.outbound.try_send(Outbound::Raft(envelope));
                }
                Effect::ResetElectionTimer => {
                    self.election_deadline = Instant::now() + random_election_timeout(&self.config);
                }
                Effect::CommitIndexAdvanced(_) => {}
                Effect::SendSnapshot { to, last_included } => {
                    let snapshot = self
                        .raft
                        .storage()
                        .current_snapshot()
                        .map_err(|error| error.to_string())?
                        .filter(|snapshot| snapshot.last_included == last_included)
                        .ok_or_else(|| "core requested an unavailable snapshot".to_owned())?;
                    let _ = self.outbound.try_send(Outbound::Snapshot {
                        from: self.config.node_id,
                        to,
                        term: self.raft.core().hard_state().current_term,
                        snapshot,
                    });
                }
                Effect::PersistHardState(_) | Effect::PersistLog(_) => {
                    return Err("durable runtime leaked a persistence effect".to_owned());
                }
            }
        }
        if self.raft.core().role() == Role::Leader {
            self.heartbeat_deadline = Instant::now() + self.config.heartbeat_interval;
        }
        Ok(direct)
    }

    fn complete_clients(&mut self, applied: Vec<AppliedEntry>) {
        for applied in applied {
            if matches!(
                &applied.result,
                Some(CommandResult::Mutation(MutationResult::Duplicate(_)))
            ) {
                self.metrics.deduplicated();
            }
            if let Some(respond) = self.pending_clients.remove(&applied.index) {
                let result = applied.result.ok_or(ServiceError::Internal);
                let _ = respond.send(result);
            }
        }
    }

    fn fail_pending_after_stepdown(&mut self, was_leader: bool) {
        if was_leader && self.raft.core().role() != Role::Leader {
            for (_, respond) in self.pending_clients.drain() {
                let _ = respond.send(Err(ServiceError::Unavailable));
            }
            if let Some(pending) = self.pending_membership.take() {
                for respond in pending.responders {
                    let _ = respond.send(Err(MembershipError::Unavailable));
                }
            }
        }
    }

    fn accept_snapshot_chunk(&mut self, chunk: SnapshotChunk) -> Result<SnapshotProgress, Status> {
        let matches_staging = self.incoming_snapshot.as_ref().is_some_and(|staging| {
            staging.sender == chunk.sender
                && staging.term == chunk.term
                && staging.format_version == chunk.format_version
                && staging.last_included == chunk.last_included
                && staging.voters == chunk.voters
                && staging.membership == chunk.membership
                && staging.snapshot_id == chunk.snapshot_id
                && staging.total_size == chunk.total_size
        });
        if !matches_staging {
            if chunk.offset != 0 {
                return Err(Status::failed_precondition(
                    "snapshot transfer must restart at offset zero",
                ));
            }
            let effects = self
                .raft
                .observe_snapshot_leader(chunk.sender, chunk.term)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            self.process(effects, None).map_err(Status::internal)?;
            self.incoming_snapshot = Some(IncomingSnapshot {
                sender: chunk.sender,
                term: chunk.term,
                format_version: chunk.format_version,
                last_included: chunk.last_included,
                voters: chunk.voters.clone(),
                membership: chunk.membership.clone(),
                snapshot_id: chunk.snapshot_id,
                total_size: chunk.total_size,
                bytes: Vec::new(),
            });
        }
        let staging = self
            .incoming_snapshot
            .as_mut()
            .ok_or_else(|| Status::internal("snapshot staging unavailable"))?;
        if chunk.offset < staging.bytes.len() {
            let end = chunk
                .offset
                .checked_add(chunk.bytes.len())
                .ok_or_else(|| Status::invalid_argument("invalid snapshot offset"))?;
            if end > staging.bytes.len() || staging.bytes[chunk.offset..end] != chunk.bytes {
                return Err(Status::invalid_argument("conflicting snapshot chunk"));
            }
        } else if chunk.offset == staging.bytes.len() {
            staging.bytes.extend_from_slice(&chunk.bytes);
        } else {
            return Err(Status::failed_precondition(
                "snapshot chunk is out of order",
            ));
        }
        let next_offset = staging.bytes.len();
        if !chunk.done {
            return Ok(SnapshotProgress {
                snapshot_id: chunk.snapshot_id,
                next_offset,
                installed: false,
                term: self.raft.core().hard_state().current_term,
            });
        }
        if next_offset != staging.total_size {
            return Err(Status::invalid_argument("incomplete final snapshot chunk"));
        }
        let staging = self
            .incoming_snapshot
            .take()
            .ok_or_else(|| Status::internal("snapshot staging unavailable"))?;
        let snapshot = Snapshot {
            format_version: staging.format_version,
            cluster_id: self.config.cluster_id.0,
            last_included: staging.last_included,
            voters: staging.voters,
            membership: staging.membership,
            payload: staging.bytes,
            digest: staging.snapshot_id,
        };
        if snapshot.last_included.index < self.raft.core().last_applied() {
            return Err(Status::failed_precondition(
                "snapshot is older than applied state",
            ));
        }
        if let Some(current) = self
            .raft
            .storage()
            .current_snapshot()
            .map_err(|error| Status::internal(error.to_string()))?
            && current.last_included.index == snapshot.last_included.index
            && current.digest != snapshot.digest
        {
            return Err(Status::invalid_argument(
                "snapshot conflicts at the included index",
            ));
        }
        self.raft
            .storage()
            .validate_snapshot_for_cluster(self.config.cluster_id.0, &snapshot)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let _snapshot_timer = self.metrics.time_snapshot();
        let _storage_timer = self.metrics.time_storage();
        let effects = self
            .raft
            .install_snapshot(
                staging.sender,
                staging.term,
                self.config.cluster_id.0,
                &snapshot,
            )
            .map_err(|error| Status::internal(error.to_string()))?;
        self.metrics.snapshot();
        self.process(effects, None).map_err(Status::internal)?;
        Ok(SnapshotProgress {
            snapshot_id: staging.snapshot_id,
            next_offset,
            installed: true,
            term: self.raft.core().hard_state().current_term,
        })
    }
}

fn random_election_timeout(config: &StaticNodeConfig) -> Duration {
    let min = config.election_timeout_min.as_millis();
    let max = config.election_timeout_max.as_millis();
    let width = max.saturating_sub(min).saturating_add(1);
    let offset = if width > 1 {
        u128::from(fastrand::u64(..)) % width
    } else {
        0
    };
    Duration::from_millis(u64::try_from(min + offset).unwrap_or(u64::MAX))
}

fn unix_time_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn is_response(envelope: &Envelope) -> bool {
    matches!(
        envelope.message,
        Message::RequestVoteResponse { .. } | Message::AppendEntriesResponse { .. }
    )
}

#[derive(Clone)]
pub struct PeerRpcService {
    context: PeerContext,
    handle: StaticNodeHandle,
    peer_certificates: Arc<BTreeMap<NodeId, PeerAdmission>>,
    require_peer_identity: bool,
    metrics: Metrics,
}

impl PeerRpcService {
    fn presented_fingerprint<T>(&self, request: &Request<T>) -> Result<Option<[u8; 32]>, Status> {
        if !self.require_peer_identity {
            return Ok(None);
        }
        let certificate = request
            .peer_certs()
            .and_then(|certificates| certificates.first().cloned())
            .ok_or_else(|| Status::unauthenticated("peer client certificate is required"))?;
        let presented: [u8; 32] = Sha256::digest(certificate.as_ref()).into();
        Ok(Some(presented))
    }

    async fn authorize_sender(
        &self,
        sender: NodeId,
        presented: Option<[u8; 32]>,
    ) -> Result<(), Status> {
        if !self.require_peer_identity {
            return Ok(());
        }
        let presented = presented
            .ok_or_else(|| Status::unauthenticated("peer client certificate is required"))?;
        let expected = self
            .peer_certificates
            .get(&sender)
            .ok_or_else(|| Status::permission_denied("peer identity is not admitted"))?;
        if presented != expected.certificate_sha256 {
            return Err(Status::permission_denied(
                "peer certificate does not match claimed node ID",
            ));
        }
        let (respond, response) = oneshot::channel();
        self.handle
            .enqueue(NodeRequest::AuthorizePeer {
                node_id: sender,
                respond,
            })
            .map_err(|_| Status::unavailable("Raft worker stopped"))?;
        if !response
            .await
            .map_err(|_| Status::unavailable("Raft worker stopped"))?
        {
            return Err(Status::permission_denied(
                "peer identity is outside active membership or staging admission",
            ));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl proto::raft_peer_server::RaftPeer for PeerRpcService {
    async fn forward_membership(
        &self,
        request: Request<proto::ForwardMembershipRequest>,
    ) -> Result<Response<proto::ForwardMembershipResponse>, Status> {
        let _rpc_timer = self.metrics.time_peer_rpc();
        self.metrics.peer_rpc();
        let fingerprint = self.presented_fingerprint(&request)?;
        let (sender, operation) = self
            .context
            .decode_forward_membership(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.authorize_sender(sender, fingerprint).await?;
        let result = match operation {
            Some((operation_id, target)) => {
                self.handle
                    .change_membership_local(operation_id, target)
                    .await
            }
            None => self.handle.membership_local().await,
        };
        let (_, term) = self
            .handle
            .status()
            .await
            .map_err(|_| Status::unavailable("Raft worker stopped"))?;
        encode_forward_membership_response(
            self.context.cluster_id,
            self.context.local_id,
            sender,
            term,
            result,
        )
        .map(Response::new)
        .map_err(|error| Status::internal(error.to_string()))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let _rpc_timer = self.metrics.time_peer_rpc();
        self.metrics.peer_rpc();
        let fingerprint = self.presented_fingerprint(&request)?;
        let chunk = self
            .context
            .decode_snapshot_chunk(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let sender = chunk.sender;
        self.authorize_sender(sender, fingerprint).await?;
        let (respond, response) = oneshot::channel();
        self.handle
            .enqueue(NodeRequest::SnapshotChunk { chunk, respond })
            .map_err(|error| match error {
                EnqueueError::Full => Status::resource_exhausted("peer queue is full"),
                EnqueueError::Disconnected => Status::unavailable("Raft worker stopped"),
            })?;
        let progress = response
            .await
            .map_err(|_| Status::unavailable("Raft worker stopped"))??;
        encode_snapshot_response(
            self.context.cluster_id,
            self.context.local_id,
            sender,
            progress.term,
            progress.snapshot_id,
            progress.next_offset,
            progress.installed,
        )
        .map(Response::new)
        .map_err(|error| Status::internal(error.to_string()))
    }

    async fn forward_command(
        &self,
        request: Request<proto::ForwardCommandRequest>,
    ) -> Result<Response<proto::ForwardCommandResponse>, Status> {
        let _rpc_timer = self.metrics.time_peer_rpc();
        self.metrics.peer_rpc();
        let fingerprint = self.presented_fingerprint(&request)?;
        let (sender, command) = self
            .context
            .decode_forward_command(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.authorize_sender(sender, fingerprint).await?;
        let result = self
            .handle
            .execute_local(command)
            .await
            .map_err(|error| match error {
                ServiceError::Overloaded => Status::resource_exhausted("request queue is full"),
                ServiceError::Unavailable => Status::unavailable("node is not the ready leader"),
                ServiceError::Internal => Status::internal("command execution failed"),
            })?;
        let (_, term) = self
            .handle
            .status()
            .await
            .map_err(|_| Status::unavailable("Raft worker stopped"))?;
        encode_forward_response(
            self.context.cluster_id,
            self.context.local_id,
            sender,
            term,
            &result,
        )
        .map(Response::new)
        .map_err(|error| Status::internal(error.to_string()))
    }

    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let _rpc_timer = self.metrics.time_peer_rpc();
        self.metrics.peer_rpc();
        let fingerprint = self.presented_fingerprint(&request)?;
        let envelope = self
            .context
            .decode_request_vote(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.authorize_sender(envelope.from, fingerprint).await?;
        let response = self.handle.call(envelope).await?;
        encode_request_vote_response(self.context.cluster_id, &response)
            .map(Response::new)
            .map_err(|error| Status::internal(error.to_string()))
    }

    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let _rpc_timer = self.metrics.time_peer_rpc();
        self.metrics.peer_rpc();
        let fingerprint = self.presented_fingerprint(&request)?;
        let envelope = self
            .context
            .decode_append_entries(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.authorize_sender(envelope.from, fingerprint).await?;
        let response = self.handle.call(envelope).await?;
        encode_append_entries_response(self.context.cluster_id, &response)
            .map(Response::new)
            .map_err(|error| Status::internal(error.to_string()))
    }
}

async fn dispatch(
    cluster: ClusterId,
    endpoint: String,
    envelope: Envelope,
    timeout: Duration,
    peer_tls: Option<TlsMaterial>,
) -> Result<Envelope, String> {
    let channel = connect_peer(endpoint, timeout, peer_tls).await?;
    let mut client = peer_client(channel);
    let context = PeerContext {
        cluster_id: cluster,
        local_id: envelope.from,
        limits: PeerLimits::default(),
    };
    match envelope.message {
        Message::RequestVote { .. } => {
            let request =
                encode_request_vote(cluster, &envelope).map_err(|error| error.to_string())?;
            let response = tokio::time::timeout(timeout, client.request_vote(request))
                .await
                .map_err(|_| "RequestVote timed out".to_owned())?
                .map_err(|error| error.to_string())?
                .into_inner();
            context
                .decode_request_vote_response(response)
                .map_err(|error| error.to_string())
        }
        Message::AppendEntries { .. } => {
            let request =
                encode_append_entries(cluster, &envelope).map_err(|error| error.to_string())?;
            let response = tokio::time::timeout(timeout, client.append_entries(request))
                .await
                .map_err(|_| "AppendEntries timed out".to_owned())?
                .map_err(|error| error.to_string())?
                .into_inner();
            context
                .decode_append_entries_response(response)
                .map_err(|error| error.to_string())
        }
        Message::RequestVoteResponse { .. } | Message::AppendEntriesResponse { .. } => {
            Err("attempted to dispatch a response as a request".to_owned())
        }
    }
}

async fn dispatch_snapshot(
    cluster: ClusterId,
    endpoint: String,
    identity: (NodeId, NodeId, Term),
    snapshot: Snapshot,
    timeout: Duration,
    peer_tls: Option<TlsMaterial>,
) -> Result<Envelope, String> {
    let (from, to, term) = identity;
    let channel = connect_peer(endpoint, timeout, peer_tls).await?;
    let mut client = peer_client(channel);
    let context = PeerContext {
        cluster_id: cluster,
        local_id: from,
        limits: PeerLimits::default(),
    };
    let chunk_size = context.limits.snapshot_chunk_bytes;
    let mut offset = 0_usize;
    let mut response_term = term;
    while offset < snapshot.payload.len() {
        let request = encode_snapshot_chunk(cluster, from, to, term, &snapshot, offset, chunk_size)
            .map_err(|error| error.to_string())?;
        let response = tokio::time::timeout(timeout, client.install_snapshot(request))
            .await
            .map_err(|_| "InstallSnapshot chunk timed out".to_owned())?
            .map_err(|error| error.to_string())?
            .into_inner();
        let (snapshot_id, next_offset, installed, observed_term) = context
            .decode_snapshot_response(response)
            .map_err(|error| error.to_string())?;
        if snapshot_id != snapshot.digest
            || next_offset <= offset
            || next_offset > snapshot.payload.len()
            || installed != (next_offset == snapshot.payload.len())
        {
            return Err("invalid InstallSnapshot progress response".to_owned());
        }
        offset = next_offset;
        response_term = observed_term;
    }
    Ok(Envelope {
        from: to,
        to: from,
        message: Message::AppendEntriesResponse {
            term: response_term,
            accepted: true,
            match_index: snapshot.last_included.index,
            request_previous_index: snapshot.last_included.index,
        },
    })
}

async fn connect_peer(
    endpoint: String,
    timeout: Duration,
    peer_tls: Option<TlsMaterial>,
) -> Result<Channel, String> {
    let mut endpoint = Channel::from_shared(endpoint).map_err(|error| error.to_string())?;
    if let Some(tls) = peer_tls {
        let ca = tls
            .client_ca
            .ok_or_else(|| "peer TLS client CA is missing".to_owned())?;
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca))
                    .identity(Identity::from_pem(tls.certificate, tls.private_key)),
            )
            .map_err(|error| error.to_string())?;
    }
    tokio::time::timeout(timeout, endpoint.connect())
        .await
        .map_err(|_| "peer connection timed out".to_owned())?
        .map_err(|error| error.to_string())
}

pub fn peer_server(
    service: PeerRpcService,
) -> proto::raft_peer_server::RaftPeerServer<PeerRpcService> {
    proto::raft_peer_server::RaftPeerServer::new(service)
        .max_decoding_message_size(crate::peer::MAX_PEER_MESSAGE_BYTES)
        .max_encoding_message_size(crate::peer::MAX_PEER_MESSAGE_BYTES)
}

fn peer_client(channel: Channel) -> proto::raft_peer_client::RaftPeerClient<Channel> {
    proto::raft_peer_client::RaftPeerClient::new(channel)
        .max_decoding_message_size(crate::peer::MAX_PEER_MESSAGE_BYTES)
        .max_encoding_message_size(crate::peer::MAX_PEER_MESSAGE_BYTES)
}

pub async fn serve_peer(
    address: SocketAddr,
    service: PeerRpcService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    serve_peer_tls(address, service, None, shutdown).await
}

pub async fn serve_peer_tls(
    address: SocketAddr,
    service: PeerRpcService,
    tls: Option<TlsMaterial>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    let mut server = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(128)
        .timeout(Duration::from_secs(5));
    if let Some(tls) = tls {
        let ca = tls
            .client_ca
            .expect("validated peer TLS material contains a client CA");
        server = server.tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(tls.certificate, tls.private_key))
                .client_ca_root(Certificate::from_pem(ca)),
        )?;
    }
    server
        .add_service(peer_server(service))
        .serve_with_shutdown(address, shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;
    use crate::{
        history::{CheckOptions, Operation, is_linearizable},
        kv::{ClientId, Mutation, MutationCommand},
    };

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn temp_directory() -> PathBuf {
        let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "raft-kv-static-cluster-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp directory");
        path
    }

    fn available_address() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("local address")
    }

    #[tokio::test]
    async fn peer_certificate_identity_is_bound_to_active_membership() {
        let directory = temp_directory();
        let mut endpoints = BTreeMap::new();
        endpoints.insert(NodeId(2), "https://node-2:9080".to_owned());
        let fingerprint = [3; 32];
        let mut admissions = BTreeMap::new();
        admissions.insert(
            NodeId(2),
            PeerAdmission {
                certificate_sha256: fingerprint,
                staging_expires_unix_seconds: None,
            },
        );
        let running = start_static_node(StaticNodeConfig {
            cluster_id: ClusterId([5; 16]),
            node_id: NodeId(1),
            voters: vec![NodeId(1), NodeId(2)],
            data_file: directory.join("peer-auth.sqlite3"),
            request_queue_capacity: 16,
            outbound_queue_capacity: 16,
            election_timeout_min: Duration::from_millis(120),
            election_timeout_max: Duration::from_millis(240),
            heartbeat_interval: Duration::from_millis(30),
            rpc_timeout: Duration::from_millis(100),
            max_clients: 10_000,
            snapshot_threshold_entries: 100,
            peer_endpoints: endpoints,
            local_peer_endpoint: "https://node-1:9080".to_owned(),
            staging: false,
            peer_tls: Some(TlsMaterial {
                certificate: Vec::new(),
                private_key: Vec::new(),
                client_ca: Some(Vec::new()),
            }),
            peer_certificates: admissions,
            metrics: Metrics::new(),
        })
        .expect("start authenticated node");

        assert!(
            running
                .peer_service
                .authorize_sender(NodeId(2), Some(fingerprint))
                .await
                .is_ok()
        );
        assert!(
            running
                .peer_service
                .authorize_sender(NodeId(2), Some([4; 32]))
                .await
                .is_err()
        );
        assert!(
            running
                .peer_service
                .authorize_sender(NodeId(3), Some(fingerprint))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn membership_operation_is_committed_idempotent_and_durable() {
        let directory = temp_directory();
        let database = directory.join("membership.sqlite3");
        let config = StaticNodeConfig {
            cluster_id: ClusterId([6; 16]),
            node_id: NodeId(1),
            voters: vec![NodeId(1)],
            data_file: database.clone(),
            request_queue_capacity: 16,
            outbound_queue_capacity: 16,
            election_timeout_min: Duration::from_millis(120),
            election_timeout_max: Duration::from_millis(240),
            heartbeat_interval: Duration::from_millis(30),
            rpc_timeout: Duration::from_millis(100),
            max_clients: 10_000,
            snapshot_threshold_entries: 100,
            peer_endpoints: BTreeMap::new(),
            local_peer_endpoint: "http://node-1:9080".to_owned(),
            staging: false,
            peer_tls: None,
            peer_certificates: BTreeMap::new(),
            metrics: Metrics::new(),
        };
        let target =
            ClusterConfiguration::new([(1, "http://node-1:9081".to_owned())]).expect("target");
        let operation_id = [4; 16];
        let running = start_static_node(config.clone()).expect("start node");
        let handle = running.handle.clone();
        let first = handle
            .change_membership(operation_id, target.clone())
            .await
            .expect("membership commits");
        assert_eq!(first.operation_id, operation_id);
        assert_eq!(
            first.members,
            vec![(NodeId(1), "http://node-1:9081".to_owned())]
        );
        assert_eq!(
            handle
                .change_membership(operation_id, target.clone())
                .await
                .expect("retry is idempotent"),
            first
        );
        assert_eq!(handle.membership().await.expect("query"), first);
        drop(handle);
        drop(running);
        let mut restarted = None;
        for _ in 0..100 {
            match start_static_node(config.clone()) {
                Ok(node) => {
                    restarted = Some(node);
                    break;
                }
                Err(error) if error.contains("already owned") => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("restart node: {error}"),
            }
        }
        let restarted = restarted.expect("worker releases the data-file lock");
        let retried = restarted
            .handle
            .change_membership(operation_id, target)
            .await
            .expect("durable retry");
        assert_eq!(retried.operation_id, first.operation_id);
        assert_eq!(retried.members, first.members);
        assert!(retried.commit_index >= first.commit_index);
        std::fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn real_cluster_adds_staged_voter_and_removes_leader() {
        let directory = temp_directory();
        let addresses = [
            available_address(),
            available_address(),
            available_address(),
            available_address(),
        ];
        let endpoints = addresses
            .iter()
            .enumerate()
            .map(|(offset, address)| {
                (
                    NodeId(u64::try_from(offset + 1).expect("node id")),
                    format!("http://{address}"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let initial = vec![NodeId(1), NodeId(2), NodeId(3)];
        let mut handles = BTreeMap::new();
        let mut shutdowns = Vec::new();

        for (offset, address) in addresses.iter().copied().enumerate() {
            let node_id = NodeId(u64::try_from(offset + 1).expect("node id"));
            let mut peer_endpoints = endpoints.clone();
            peer_endpoints.remove(&node_id);
            let running = start_static_node(StaticNodeConfig {
                cluster_id: ClusterId([9; 16]),
                node_id,
                voters: initial.clone(),
                data_file: directory.join(format!("dynamic-{}.sqlite3", node_id.0)),
                request_queue_capacity: 128,
                outbound_queue_capacity: 256,
                election_timeout_min: Duration::from_millis(180),
                election_timeout_max: Duration::from_millis(360),
                heartbeat_interval: Duration::from_millis(40),
                rpc_timeout: Duration::from_millis(300),
                max_clients: 10_000,
                snapshot_threshold_entries: 100,
                peer_endpoints,
                local_peer_endpoint: endpoints[&node_id].clone(),
                staging: node_id == NodeId(4),
                peer_tls: None,
                peer_certificates: BTreeMap::new(),
                metrics: Metrics::new(),
            })
            .expect("start dynamic node");
            let (handle, service) = running.spawn_dispatcher();
            let (shutdown, receive_shutdown) = oneshot::channel();
            shutdowns.push(shutdown);
            tokio::spawn(async move {
                serve_peer(address, service, async {
                    let _ = receive_shutdown.await;
                })
                .await
                .expect("peer server");
            });
            handles.insert(node_id, handle);
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let leader = loop {
            let mut leaders = std::collections::BTreeSet::new();
            for handle in handles.values() {
                if let Ok((Some(observed), _)) = handle.status().await {
                    leaders.insert(observed);
                }
            }
            if let Some(leader) = leaders.iter().next().copied()
                && leaders.len() == 1
            {
                break leader;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "leader election timed out"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        };
        let caller = handles
            .iter()
            .find(|(node, _)| **node != leader && **node != NodeId(4))
            .map(|(_, handle)| handle)
            .expect("follower caller");
        let four = ClusterConfiguration::new(
            endpoints
                .iter()
                .map(|(node, endpoint)| (node.0, endpoint.clone())),
        )
        .expect("four-voter target");
        let added = caller
            .change_membership([10; 16], four)
            .await
            .expect("forwarded addition commits");
        assert_eq!(added.members.len(), 4);

        let remaining = ClusterConfiguration::new(
            endpoints
                .iter()
                .filter(|(node, _)| **node != leader)
                .map(|(node, endpoint)| (node.0, endpoint.clone())),
        )
        .expect("leader removal target");
        let removed = caller
            .change_membership([11; 16], remaining)
            .await
            .expect("leader removal commits");
        assert_eq!(removed.members.len(), 3);
        assert!(!removed.members.iter().any(|(node, _)| *node == leader));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut observed = None;
            for (node, handle) in &handles {
                if *node != leader
                    && let Ok(view) = handle.membership().await
                {
                    observed = Some(view);
                    break;
                }
            }
            if let Some(view) = observed {
                assert_eq!(view.members.len(), 3);
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "reelection timed out"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }

        for shutdown in shutdowns {
            let _ = shutdown.send(());
        }
        drop(handles);
        std::fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_real_rpc_nodes_elect_commit_and_apply() {
        let directory = temp_directory();
        let addresses = [
            available_address(),
            available_address(),
            available_address(),
        ];
        let voters = vec![NodeId(1), NodeId(2), NodeId(3)];
        let endpoints = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| {
                (
                    NodeId(u64::try_from(index + 1).expect("node id")),
                    format!("http://{address}"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut handles = Vec::new();
        let mut shutdowns = Vec::new();

        for (index, address) in addresses.iter().copied().take(2).enumerate() {
            let node_id = NodeId(u64::try_from(index + 1).expect("node id"));
            let mut peer_endpoints = endpoints.clone();
            peer_endpoints.remove(&node_id);
            let running = start_static_node(StaticNodeConfig {
                cluster_id: ClusterId([8; 16]),
                node_id,
                voters: voters.clone(),
                data_file: directory.join(format!("node-{}.sqlite3", node_id.0)),
                request_queue_capacity: 64,
                outbound_queue_capacity: 64,
                election_timeout_min: Duration::from_millis(120),
                election_timeout_max: Duration::from_millis(240),
                heartbeat_interval: Duration::from_millis(30),
                rpc_timeout: Duration::from_millis(100),
                max_clients: 10_000,
                snapshot_threshold_entries: 2,
                peer_endpoints,
                local_peer_endpoint: endpoints[&node_id].clone(),
                staging: false,
                peer_tls: None,
                peer_certificates: BTreeMap::new(),
                metrics: Metrics::new(),
            })
            .expect("start node");
            let (handle, service) = running.spawn_dispatcher();
            let (shutdown, receive_shutdown) = oneshot::channel();
            shutdowns.push(shutdown);
            tokio::spawn(async move {
                serve_peer(address, service, async {
                    let _ = receive_shutdown.await;
                })
                .await
                .expect("peer server");
            });
            handles.push(handle);
        }

        let command = Command::Mutate(MutationCommand {
            client_id: ClientId([5; 16]),
            sequence: 1,
            mutation: Mutation::Create {
                key: "distributed".into(),
                value: b"yes".to_vec(),
            },
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let (revision, leader_index) = loop {
            let mut committed = None;
            for (index, handle) in handles.iter().enumerate() {
                if let Ok(Ok(CommandResult::Mutation(crate::kv::MutationResult::Created {
                    revision,
                }))) = tokio::time::timeout(
                    Duration::from_millis(500),
                    handle.execute_local(command.clone()),
                )
                .await
                {
                    committed = Some((revision, index));
                    break;
                }
            }
            if let Some(committed) = committed {
                break committed;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "cluster did not elect a leader"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(revision >= 2);
        let create_result =
            CommandResult::Mutation(crate::kv::MutationResult::Created { revision });

        let follower = &handles[(leader_index + 1) % handles.len()];
        let read_command = Command::Read {
            key: "distributed".into(),
        };
        let read_result = follower
            .execute(read_command.clone())
            .await
            .expect("follower forwards a logged read");
        assert_eq!(
            read_result,
            CommandResult::Read(crate::kv::ReadResult::Found {
                value: b"yes".to_vec(),
                revision,
            })
        );
        assert!(
            is_linearizable(
                &[
                    Operation {
                        id: 1,
                        invoked_at: 1,
                        completed_at: Some(2),
                        command,
                        result: Some(create_result),
                    },
                    Operation {
                        id: 2,
                        invoked_at: 3,
                        completed_at: Some(4),
                        command: read_command,
                        result: Some(read_result),
                    },
                ],
                CheckOptions::default(),
            )
            .expect("check collected cluster history")
        );

        // Node 3 starts only after the quorum has compacted through the create.
        // Its first successful catch-up must therefore install a snapshot.
        let node_id = NodeId(3);
        let mut peer_endpoints = endpoints.clone();
        peer_endpoints.remove(&node_id);
        let running = start_static_node(StaticNodeConfig {
            cluster_id: ClusterId([8; 16]),
            node_id,
            voters: voters.clone(),
            data_file: directory.join("node-3.sqlite3"),
            request_queue_capacity: 64,
            outbound_queue_capacity: 64,
            election_timeout_min: Duration::from_millis(120),
            election_timeout_max: Duration::from_millis(240),
            heartbeat_interval: Duration::from_millis(30),
            rpc_timeout: Duration::from_millis(100),
            max_clients: 10_000,
            snapshot_threshold_entries: 2,
            peer_endpoints,
            local_peer_endpoint: endpoints[&node_id].clone(),
            staging: false,
            peer_tls: None,
            peer_certificates: BTreeMap::new(),
            metrics: Metrics::new(),
        })
        .expect("start lagging node");
        let (handle, service) = running.spawn_dispatcher();
        let lagging_handle = handle.clone();
        let (shutdown, receive_shutdown) = oneshot::channel();
        shutdowns.push(shutdown);
        tokio::spawn(async move {
            serve_peer(addresses[2], service, async {
                let _ = receive_shutdown.await;
            })
            .await
            .expect("lagging peer server");
        });
        handles.push(handle);

        let catch_up_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let diagnostics = lagging_handle.diagnostics().await.expect("lagging status");
            if diagnostics.applied_index >= revision && diagnostics.snapshot_index == 2 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < catch_up_deadline,
                "lagging follower did not install snapshot"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
        for handle in &handles {
            assert!(
                handle
                    .diagnostics()
                    .await
                    .expect("follower diagnostics")
                    .applied_index
                    >= revision
            );
        }
        for shutdown in shutdowns {
            let _ = shutdown.send(());
        }
    }
}
