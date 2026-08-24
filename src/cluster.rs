//! Multi-process static-cluster runtime and gRPC transport adapters.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tonic::{Request, Response, Status, transport::Channel};
use tracing::warn;

use crate::{
    http::ServiceError,
    kv::{Command, CommandResult, encode_command},
    peer::{
        ClusterId, PeerContext, PeerLimits, SnapshotChunk, encode_append_entries,
        encode_append_entries_response, encode_forward_command, encode_forward_response,
        encode_request_vote, encode_request_vote_response, encode_snapshot_chunk,
        encode_snapshot_response, proto,
    },
    raft::{
        DurableRaft, Effect, EntryPayload, Envelope, Event, Message, NodeId, Role, Term, VoterSet,
    },
    storage::{AppliedEntry, Snapshot, SnapshotStorage, SqliteStorage},
};

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
}

pub struct RunningNode {
    pub handle: StaticNodeHandle,
    pub peer_service: PeerRpcService,
    outbound: tokio_mpsc::Receiver<Outbound>,
    endpoints: BTreeMap<NodeId, String>,
    rpc_timeout: Duration,
}

impl RunningNode {
    pub fn spawn_dispatcher(mut self) -> (StaticNodeHandle, PeerRpcService) {
        let handle = self.handle.clone();
        let response_handle = handle.clone();
        tokio::spawn(async move {
            while let Some(outbound) = self.outbound.recv().await {
                let to = match &outbound {
                    Outbound::Raft(envelope) => envelope.to,
                    Outbound::Snapshot { to, .. } => *to,
                };
                let Some(endpoint) = self.endpoints.get(&to).cloned() else {
                    warn!(peer = to.0, "missing peer endpoint; dropping Raft RPC");
                    continue;
                };
                let response_handle = response_handle.clone();
                let cluster_id = self.peer_service.context.cluster_id;
                let timeout = self.rpc_timeout;
                tokio::spawn(async move {
                    let result = match outbound {
                        Outbound::Raft(envelope) => {
                            dispatch(cluster_id, endpoint, envelope, timeout).await
                        }
                        Outbound::Snapshot {
                            from,
                            to,
                            term,
                            snapshot,
                        } => {
                            dispatch_snapshot(
                                cluster_id, endpoint, from, to, term, snapshot, timeout,
                            )
                            .await
                        }
                    };
                    match result {
                        Ok(response) => response_handle.receive_response(response),
                        Err(error) => warn!(%error, "peer RPC failed"),
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
    endpoints: Arc<BTreeMap<NodeId, String>>,
    rpc_timeout: Duration,
}

impl StaticNodeHandle {
    pub async fn execute(&self, command: Command) -> Result<CommandResult, ServiceError> {
        match self.execute_local(command.clone()).await {
            Err(ServiceError::Unavailable) => self.forward(command).await,
            result => result,
        }
    }

    async fn execute_local(&self, command: Command) -> Result<CommandResult, ServiceError> {
        let (respond, response) = oneshot::channel();
        self.sender
            .try_send(NodeRequest::Client { command, respond })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ServiceError::Overloaded,
                mpsc::TrySendError::Disconnected(_) => ServiceError::Unavailable,
            })?;
        response.await.map_err(|_| ServiceError::Unavailable)?
    }

    async fn status(&self) -> Result<(Option<NodeId>, Term), ServiceError> {
        let (respond, response) = oneshot::channel();
        self.sender
            .try_send(NodeRequest::Status { respond })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ServiceError::Overloaded,
                mpsc::TrySendError::Disconnected(_) => ServiceError::Unavailable,
            })?;
        response.await.map_err(|_| ServiceError::Unavailable)
    }

    async fn forward(&self, command: Command) -> Result<CommandResult, ServiceError> {
        let (leader, term) = self.status().await?;
        let leader = leader
            .filter(|leader| *leader != self.node_id)
            .ok_or(ServiceError::Unavailable)?;
        let endpoint = self
            .endpoints
            .get(&leader)
            .ok_or(ServiceError::Unavailable)?;
        let channel = tokio::time::timeout(
            self.rpc_timeout,
            Channel::from_shared(endpoint.clone())
                .map_err(|_| ServiceError::Internal)?
                .connect(),
        )
        .await
        .map_err(|_| ServiceError::Unavailable)?
        .map_err(|_| ServiceError::Unavailable)?;
        let mut client = proto::raft_peer_client::RaftPeerClient::new(channel);
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
        let _ = self.sender.try_send(NodeRequest::PeerResponse(envelope));
    }

    async fn call(&self, envelope: Envelope) -> Result<Envelope, Status> {
        let (respond, response) = oneshot::channel();
        self.sender
            .try_send(NodeRequest::PeerCall { envelope, respond })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Status::resource_exhausted("peer queue is full"),
                mpsc::TrySendError::Disconnected(_) => Status::unavailable("Raft worker stopped"),
            })?;
        response
            .await
            .map_err(|_| Status::unavailable("Raft worker stopped"))?
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
    let voters = VoterSet::new(config.voters.iter().copied()).map_err(|error| error.to_string())?;
    let raft = DurableRaft::recover_with_snapshot(config.node_id, voters, storage)
        .map_err(|error| error.to_string())?;
    let (sender, receiver) = mpsc::sync_channel(config.request_queue_capacity);
    let (outbound_sender, outbound) = tokio_mpsc::channel(config.outbound_queue_capacity);
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let worker_config = config.clone();
    thread::Builder::new()
        .name(format!("raft-node-{}", config.node_id.0))
        .spawn(move || {
            let mut worker = NodeWorker::new(raft, receiver, outbound_sender, worker_config);
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
        endpoints: Arc::new(config.peer_endpoints.clone()),
        rpc_timeout: config.rpc_timeout,
    };
    Ok(RunningNode {
        peer_service: PeerRpcService {
            context: PeerContext {
                cluster_id: config.cluster_id,
                local_id: config.node_id,
                limits: PeerLimits::default(),
            },
            handle: handle.clone(),
        },
        handle,
        outbound,
        endpoints: config.peer_endpoints,
        rpc_timeout: config.rpc_timeout,
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
    if !config.voters.contains(&config.node_id) {
        return Err("local node is not in the static voter set".to_owned());
    }
    for voter in &config.voters {
        if *voter != config.node_id && !config.peer_endpoints.contains_key(voter) {
            return Err(format!("missing endpoint for voter {}", voter.0));
        }
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
}

struct IncomingSnapshot {
    sender: NodeId,
    term: Term,
    format_version: u32,
    last_included: crate::raft::LogPosition,
    voters: Vec<NodeId>,
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
                let was_leader = self.raft.core().role() == Role::Leader;
                let effects = self
                    .raft
                    .handle(Event::Receive(envelope))
                    .map_err(|error| error.to_string())?;
                self.process(effects, None)?;
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
        }
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
        let encoded = encode_command(&command).map_err(|error| error.to_string())?;
        let (index, effects) = self
            .raft
            .propose(EntryPayload::Command(encoded))
            .map_err(|error| error.to_string())?;
        self.pending_clients.insert(index, respond);
        self.process(effects, None)?;
        Ok(())
    }

    fn on_timer(&mut self) -> Result<(), String> {
        let effects = if self.raft.core().role() == Role::Leader {
            self.heartbeat_deadline = Instant::now() + self.config.heartbeat_interval;
            self.raft.heartbeat().map_err(|error| error.to_string())?
        } else {
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
                    let batch = self
                        .raft
                        .apply_committed(&entries)
                        .map_err(|error| error.to_string())?;
                    self.complete_clients(batch.entries);
                    pending.extend(batch.effects);
                    let compacted = self.raft.core().snapshot_position().index;
                    let applied = self.raft.core().last_applied();
                    if applied.saturating_sub(compacted) >= self.config.snapshot_threshold_entries {
                        self.raft
                            .create_snapshot(self.config.cluster_id.0)
                            .map_err(|error| error.to_string())?;
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
        }
    }

    fn accept_snapshot_chunk(&mut self, chunk: SnapshotChunk) -> Result<SnapshotProgress, Status> {
        let matches_staging = self.incoming_snapshot.as_ref().is_some_and(|staging| {
            staging.sender == chunk.sender
                && staging.term == chunk.term
                && staging.format_version == chunk.format_version
                && staging.last_included == chunk.last_included
                && staging.voters == chunk.voters
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
            payload: staging.bytes,
            digest: staging.snapshot_id,
        };
        self.raft
            .storage()
            .validate_snapshot_for_cluster(self.config.cluster_id.0, &snapshot)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let effects = self
            .raft
            .install_snapshot(
                staging.sender,
                staging.term,
                self.config.cluster_id.0,
                &snapshot,
            )
            .map_err(|error| Status::internal(error.to_string()))?;
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
}

#[tonic::async_trait]
impl proto::raft_peer_server::RaftPeer for PeerRpcService {
    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let chunk = self
            .context
            .decode_snapshot_chunk(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let sender = chunk.sender;
        let (respond, response) = oneshot::channel();
        self.handle
            .sender
            .try_send(NodeRequest::SnapshotChunk { chunk, respond })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Status::resource_exhausted("peer queue is full"),
                mpsc::TrySendError::Disconnected(_) => Status::unavailable("Raft worker stopped"),
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
        let (sender, command) = self
            .context
            .decode_forward_command(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
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
        let envelope = self
            .context
            .decode_request_vote(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = self.handle.call(envelope).await?;
        encode_request_vote_response(self.context.cluster_id, &response)
            .map(Response::new)
            .map_err(|error| Status::internal(error.to_string()))
    }

    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let envelope = self
            .context
            .decode_append_entries(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
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
) -> Result<Envelope, String> {
    let channel = tokio::time::timeout(
        timeout,
        Channel::from_shared(endpoint)
            .map_err(|error| error.to_string())?
            .connect(),
    )
    .await
    .map_err(|_| "peer connection timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    let mut client = proto::raft_peer_client::RaftPeerClient::new(channel);
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
    from: NodeId,
    to: NodeId,
    term: Term,
    snapshot: Snapshot,
    timeout: Duration,
) -> Result<Envelope, String> {
    let channel = tokio::time::timeout(
        timeout,
        Channel::from_shared(endpoint)
            .map_err(|error| error.to_string())?
            .connect(),
    )
    .await
    .map_err(|_| "snapshot peer connection timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    let mut client = proto::raft_peer_client::RaftPeerClient::new(channel);
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

pub fn peer_server(
    service: PeerRpcService,
) -> proto::raft_peer_server::RaftPeerServer<PeerRpcService> {
    proto::raft_peer_server::RaftPeerServer::new(service)
}

pub async fn serve_peer(
    address: SocketAddr,
    service: PeerRpcService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
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
        kv::{ClientId, Mutation, MutationCommand, ValueRecord},
        storage::StateMachineStorage,
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
        })
        .expect("start lagging node");
        let (handle, service) = running.spawn_dispatcher();
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
            let storage =
                SqliteStorage::open(directory.join("node-3.sqlite3")).expect("open lagging state");
            if storage
                .value("distributed")
                .expect("read lagging value")
                .is_some()
            {
                assert_eq!(
                    storage
                        .current_snapshot()
                        .expect("load installed snapshot")
                        .expect("snapshot installed")
                        .last_included
                        .index,
                    2
                );
                break;
            }
            assert!(
                tokio::time::Instant::now() < catch_up_deadline,
                "lagging follower did not install snapshot"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
        for node in 1..=3 {
            let storage = SqliteStorage::open(directory.join(format!("node-{node}.sqlite3")))
                .expect("open follower state");
            assert_eq!(
                storage.value("distributed").expect("read applied value"),
                Some(ValueRecord {
                    value: b"yes".to_vec(),
                    revision,
                })
            );
        }
        for shutdown in shutdowns {
            let _ = shutdown.send(());
        }
    }
}
