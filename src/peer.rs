//! Versioned peer RPC schema and validation boundary.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use crate::{
    cluster::{MembershipError, MembershipView},
    kv::{
        Command, CommandResult, decode_command, decode_command_result, encode_command,
        encode_command_result,
    },
    raft::{
        ClusterConfiguration, EntryPayload, Envelope, LogEntry, LogPosition, Message, NodeId, Term,
        decode_membership, encode_membership,
    },
    storage::{SNAPSHOT_FORMAT_VERSION, Snapshot},
};

pub const PEER_PROTOCOL_VERSION: u32 = 2;
pub const MAX_PEER_MESSAGE_BYTES: usize = 6 * 1024 * 1024;
pub type ForwardMembershipOperation = Option<([u8; 16], ClusterConfiguration)>;

pub mod proto {
    tonic::include_proto!("raft.peer.v1");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerLimits {
    pub max_entries_per_append: usize,
    pub max_command_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub snapshot_chunk_bytes: usize,
}

impl Default for PeerLimits {
    fn default() -> Self {
        Self {
            max_entries_per_append: crate::raft::MAX_ENTRIES_PER_APPEND,
            // The client value limit plus room for the canonical command envelope.
            max_command_bytes: 1024 * 1024 + 1024,
            max_snapshot_bytes: 256 * 1024 * 1024,
            snapshot_chunk_bytes: 256 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerContext {
    pub cluster_id: ClusterId,
    pub local_id: NodeId,
    pub limits: PeerLimits,
}

impl PeerContext {
    pub fn decode_forward_membership(
        &self,
        request: proto::ForwardMembershipRequest,
    ) -> Result<(NodeId, ForwardMembershipOperation), PeerError> {
        let identity = self.validate_identity(request.identity)?;
        if request.hop_count > 1 {
            return Err(PeerError::ForwardingLoop);
        }
        if request.query {
            if !request.operation_id.is_empty() || !request.members.is_empty() {
                return Err(PeerError::InvalidConfiguration);
            }
            return Ok((identity.sender, None));
        }
        let operation_id: [u8; 16] = request
            .operation_id
            .try_into()
            .map_err(|_| PeerError::InvalidConfiguration)?;
        if operation_id == [0; 16] {
            return Err(PeerError::InvalidConfiguration);
        }
        let target = ClusterConfiguration::new(
            request
                .members
                .into_iter()
                .map(|member| (member.node_id, member.peer_endpoint)),
        )
        .map_err(|_| PeerError::InvalidConfiguration)?;
        if target.members().any(|(_, endpoint)| {
            !(endpoint.starts_with("http://") || endpoint.starts_with("https://"))
        }) {
            return Err(PeerError::InvalidConfiguration);
        }
        Ok((identity.sender, Some((operation_id, target))))
    }

    pub fn decode_forward_membership_response(
        &self,
        response: proto::ForwardMembershipResponse,
    ) -> Result<Result<MembershipView, MembershipError>, PeerError> {
        let _ = self.validate_identity(response.identity)?;
        if response.error_code != 0 {
            let error = match response.error_code {
                1 => MembershipError::Unavailable,
                2 => MembershipError::Overloaded,
                3 => MembershipError::Invalid,
                4 => MembershipError::Conflict,
                5 => MembershipError::ChangeInProgress,
                6 => MembershipError::Internal,
                _ => return Err(PeerError::InvalidConfiguration),
            };
            return Ok(Err(error));
        }
        let operation_id = response
            .operation_id
            .try_into()
            .map_err(|_| PeerError::InvalidConfiguration)?;
        let configuration = ClusterConfiguration::new(
            response
                .members
                .into_iter()
                .map(|member| (member.node_id, member.peer_endpoint)),
        )
        .map_err(|_| PeerError::InvalidConfiguration)?;
        Ok(Ok(MembershipView {
            operation_id,
            members: configuration
                .members()
                .map(|(node, endpoint)| (NodeId(node), endpoint.to_owned()))
                .collect(),
            commit_index: response.commit_index,
        }))
    }

    pub fn decode_snapshot_response(
        &self,
        response: proto::InstallSnapshotResponse,
    ) -> Result<([u8; 32], usize, bool, Term), PeerError> {
        let identity = self.validate_identity(response.identity)?;
        let snapshot_id = response
            .snapshot_id
            .try_into()
            .map_err(|_| PeerError::InvalidSnapshotDigest)?;
        let next_offset =
            usize::try_from(response.next_offset).map_err(|_| PeerError::InvalidSnapshotChunk)?;
        if next_offset > self.limits.max_snapshot_bytes {
            return Err(PeerError::InvalidSnapshotChunk);
        }
        Ok((snapshot_id, next_offset, response.installed, identity.term))
    }

    pub fn decode_snapshot_chunk(
        &self,
        request: proto::InstallSnapshotRequest,
    ) -> Result<SnapshotChunk, PeerError> {
        let identity = self.validate_identity(request.identity)?;
        if request.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(PeerError::UnsupportedSnapshotFormat(request.format_version));
        }
        if request.last_included_index == 0 || request.last_included_term == 0 {
            return Err(PeerError::InvalidSnapshotPosition);
        }
        let voters = request
            .voter_ids
            .into_iter()
            .map(valid_node)
            .collect::<Result<Vec<_>, _>>()?;
        if voters.is_empty() || voters.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(PeerError::InvalidSnapshotVoters);
        }
        let membership =
            decode_membership(&request.membership).map_err(|_| PeerError::InvalidConfiguration)?;
        let membership_voters = membership
            .old
            .node_ids()
            .chain(membership.new.node_ids())
            .map(NodeId)
            .collect::<std::collections::BTreeSet<_>>();
        if voters
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            != membership_voters
        {
            return Err(PeerError::InvalidSnapshotVoters);
        }
        let snapshot_id: [u8; 32] = request
            .snapshot_id
            .try_into()
            .map_err(|_| PeerError::InvalidSnapshotDigest)?;
        let digest: [u8; 32] = request
            .sha256
            .try_into()
            .map_err(|_| PeerError::InvalidSnapshotDigest)?;
        if snapshot_id != digest {
            return Err(PeerError::InvalidSnapshotDigest);
        }
        let total_size =
            usize::try_from(request.total_size).map_err(|_| PeerError::SnapshotTooLarge)?;
        let offset =
            usize::try_from(request.offset).map_err(|_| PeerError::InvalidSnapshotChunk)?;
        let end = offset
            .checked_add(request.chunk.len())
            .ok_or(PeerError::InvalidSnapshotChunk)?;
        if total_size == 0 || total_size > self.limits.max_snapshot_bytes {
            return Err(PeerError::SnapshotTooLarge);
        }
        if request.chunk.is_empty()
            || request.chunk.len() > self.limits.snapshot_chunk_bytes
            || end > total_size
            || request.done != (end == total_size)
        {
            return Err(PeerError::InvalidSnapshotChunk);
        }
        Ok(SnapshotChunk {
            sender: identity.sender,
            term: identity.term,
            format_version: request.format_version,
            last_included: LogPosition {
                index: request.last_included_index,
                term: Term(request.last_included_term),
            },
            voters,
            membership,
            snapshot_id,
            offset,
            total_size,
            bytes: request.chunk,
            done: request.done,
        })
    }

    pub fn decode_forward_command(
        &self,
        request: proto::ForwardCommandRequest,
    ) -> Result<(NodeId, Command), PeerError> {
        let identity = self.validate_identity(request.identity)?;
        if request.hop_count > 1 {
            return Err(PeerError::ForwardingLoop);
        }
        if request.command.len() > self.limits.max_command_bytes {
            return Err(PeerError::CommandTooLarge);
        }
        let command = decode_command(&request.command).map_err(|_| PeerError::InvalidCommand)?;
        Ok((identity.sender, command))
    }

    pub fn decode_forward_response(
        &self,
        response: proto::ForwardCommandResponse,
    ) -> Result<CommandResult, PeerError> {
        self.validate_identity(response.identity)?;
        if response.result.len() > self.limits.max_command_bytes {
            return Err(PeerError::CommandTooLarge);
        }
        decode_command_result(&response.result).map_err(|_| PeerError::InvalidCommandResult)
    }

    pub fn decode_request_vote(
        &self,
        request: proto::RequestVoteRequest,
    ) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(request.identity)?;
        let last_log = decode_position(request.last_log)?;
        let candidate = valid_node(request.candidate_id)?;
        if candidate != identity.sender {
            return Err(PeerError::SenderRoleMismatch);
        }
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::RequestVote {
                term: identity.term,
                candidate,
                last_log,
            },
        })
    }

    pub fn decode_request_vote_response(
        &self,
        response: proto::RequestVoteResponse,
    ) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(response.identity)?;
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::RequestVoteResponse {
                term: identity.term,
                vote_granted: response.vote_granted,
            },
        })
    }

    pub fn decode_append_entries(
        &self,
        request: proto::AppendEntriesRequest,
    ) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(request.identity)?;
        let leader = valid_node(request.leader_id)?;
        if leader != identity.sender {
            return Err(PeerError::SenderRoleMismatch);
        }
        if request.entries.len() > self.limits.max_entries_per_append {
            return Err(PeerError::EntryBatchTooLarge);
        }
        let entries = request
            .entries
            .into_iter()
            .map(|entry| decode_entry(entry, self.limits.max_command_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::AppendEntries {
                term: identity.term,
                leader,
                previous: decode_position(request.previous)?,
                entries,
                leader_commit: request.leader_commit,
            },
        })
    }

    pub fn decode_append_entries_response(
        &self,
        response: proto::AppendEntriesResponse,
    ) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(response.identity)?;
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::AppendEntriesResponse {
                term: identity.term,
                accepted: response.accepted,
                match_index: response.match_index,
                request_previous_index: response.request_previous_index,
            },
        })
    }

    fn validate_identity(
        &self,
        identity: Option<proto::RpcIdentity>,
    ) -> Result<Identity, PeerError> {
        let identity = identity.ok_or(PeerError::MissingIdentity)?;
        if identity.protocol_version != PEER_PROTOCOL_VERSION {
            return Err(PeerError::UnsupportedProtocol(identity.protocol_version));
        }
        if identity.cluster_id.as_slice() != self.cluster_id.0 {
            return Err(PeerError::ClusterMismatch);
        }
        let sender = valid_node(identity.sender_id)?;
        if valid_node(identity.recipient_id)? != self.local_id {
            return Err(PeerError::RecipientMismatch);
        }
        if identity.term == 0 {
            return Err(PeerError::ZeroTerm);
        }
        Ok(Identity {
            sender,
            term: Term(identity.term),
        })
    }
}

pub fn encode_forward_membership(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
    operation: Option<([u8; 16], &ClusterConfiguration)>,
) -> Result<proto::ForwardMembershipRequest, PeerError> {
    let (operation_id, members, query) = match operation {
        Some((operation_id, target)) => (
            operation_id.to_vec(),
            target
                .members()
                .map(|(node_id, peer_endpoint)| proto::Member {
                    node_id,
                    peer_endpoint: peer_endpoint.to_owned(),
                })
                .collect(),
            false,
        ),
        None => (Vec::new(), Vec::new(), true),
    };
    Ok(proto::ForwardMembershipRequest {
        identity: Some(encode_rpc_identity(cluster, from, to, term)?),
        operation_id,
        members,
        hop_count: 1,
        query,
    })
}

pub fn encode_forward_membership_response(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
    result: Result<MembershipView, MembershipError>,
) -> Result<proto::ForwardMembershipResponse, PeerError> {
    let (operation_id, members, commit_index, error_code) = match result {
        Ok(view) => (
            view.operation_id.to_vec(),
            view.members
                .into_iter()
                .map(|(node, peer_endpoint)| proto::Member {
                    node_id: node.0,
                    peer_endpoint,
                })
                .collect(),
            view.commit_index,
            0,
        ),
        Err(error) => (
            Vec::new(),
            Vec::new(),
            0,
            match error {
                MembershipError::Unavailable => 1,
                MembershipError::Overloaded => 2,
                MembershipError::Invalid => 3,
                MembershipError::Conflict => 4,
                MembershipError::ChangeInProgress => 5,
                MembershipError::Internal => 6,
            },
        ),
    };
    Ok(proto::ForwardMembershipResponse {
        identity: Some(encode_rpc_identity(cluster, from, to, term)?),
        operation_id,
        members,
        commit_index,
        error_code,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunk {
    pub sender: NodeId,
    pub term: Term,
    pub format_version: u32,
    pub last_included: LogPosition,
    pub voters: Vec<NodeId>,
    pub membership: crate::raft::MembershipEntry,
    pub snapshot_id: [u8; 32],
    pub offset: usize,
    pub total_size: usize,
    pub bytes: Vec<u8>,
    pub done: bool,
}

pub fn encode_snapshot_chunk(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
    snapshot: &Snapshot,
    offset: usize,
    chunk_size: usize,
) -> Result<proto::InstallSnapshotRequest, PeerError> {
    let end = offset
        .checked_add(chunk_size)
        .map(|end| end.min(snapshot.payload.len()))
        .ok_or(PeerError::InvalidSnapshotChunk)?;
    if offset >= end {
        return Err(PeerError::InvalidSnapshotChunk);
    }
    Ok(proto::InstallSnapshotRequest {
        identity: Some(encode_rpc_identity(cluster, from, to, term)?),
        format_version: snapshot.format_version,
        last_included_index: snapshot.last_included.index,
        last_included_term: snapshot.last_included.term.0,
        voter_ids: snapshot.voters.iter().map(|node| node.0).collect(),
        snapshot_id: snapshot.digest.to_vec(),
        offset: u64::try_from(offset).map_err(|_| PeerError::InvalidSnapshotChunk)?,
        total_size: u64::try_from(snapshot.payload.len())
            .map_err(|_| PeerError::SnapshotTooLarge)?,
        chunk: snapshot.payload[offset..end].to_vec(),
        done: end == snapshot.payload.len(),
        sha256: snapshot.digest.to_vec(),
        membership: encode_membership(&snapshot.membership)
            .map_err(|_| PeerError::InvalidConfiguration)?,
    })
}

pub fn encode_snapshot_response(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
    snapshot_id: [u8; 32],
    next_offset: usize,
    installed: bool,
) -> Result<proto::InstallSnapshotResponse, PeerError> {
    Ok(proto::InstallSnapshotResponse {
        identity: Some(encode_rpc_identity(cluster, from, to, term)?),
        snapshot_id: snapshot_id.to_vec(),
        next_offset: u64::try_from(next_offset).map_err(|_| PeerError::InvalidSnapshotChunk)?,
        installed,
    })
}

pub fn encode_forward_command(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
    command: &Command,
) -> Result<proto::ForwardCommandRequest, PeerError> {
    Ok(proto::ForwardCommandRequest {
        identity: Some(encode_rpc_identity(cluster, from, to, term)?),
        command: encode_command(command).map_err(|_| PeerError::InvalidCommand)?,
        hop_count: 1,
    })
}

pub fn encode_forward_response(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
    result: &CommandResult,
) -> Result<proto::ForwardCommandResponse, PeerError> {
    Ok(proto::ForwardCommandResponse {
        identity: Some(encode_rpc_identity(cluster, from, to, term)?),
        result: encode_command_result(result).map_err(|_| PeerError::InvalidCommandResult)?,
    })
}

struct Identity {
    sender: NodeId,
    term: Term,
}

pub fn encode_request_vote(
    cluster: ClusterId,
    envelope: &Envelope,
) -> Result<proto::RequestVoteRequest, PeerError> {
    let Message::RequestVote {
        term,
        candidate,
        last_log,
    } = &envelope.message
    else {
        return Err(PeerError::WrongMessageKind);
    };
    Ok(proto::RequestVoteRequest {
        identity: Some(encode_identity(cluster, envelope, *term)?),
        candidate_id: candidate.0,
        last_log: Some(encode_position(*last_log)),
    })
}

pub fn encode_request_vote_response(
    cluster: ClusterId,
    envelope: &Envelope,
) -> Result<proto::RequestVoteResponse, PeerError> {
    let Message::RequestVoteResponse { term, vote_granted } = &envelope.message else {
        return Err(PeerError::WrongMessageKind);
    };
    Ok(proto::RequestVoteResponse {
        identity: Some(encode_identity(cluster, envelope, *term)?),
        vote_granted: *vote_granted,
    })
}

pub fn encode_append_entries(
    cluster: ClusterId,
    envelope: &Envelope,
) -> Result<proto::AppendEntriesRequest, PeerError> {
    let Message::AppendEntries {
        term,
        leader,
        previous,
        entries,
        leader_commit,
    } = &envelope.message
    else {
        return Err(PeerError::WrongMessageKind);
    };
    Ok(proto::AppendEntriesRequest {
        identity: Some(encode_identity(cluster, envelope, *term)?),
        leader_id: leader.0,
        previous: Some(encode_position(*previous)),
        entries: entries.iter().map(encode_entry).collect(),
        leader_commit: *leader_commit,
    })
}

pub fn encode_append_entries_response(
    cluster: ClusterId,
    envelope: &Envelope,
) -> Result<proto::AppendEntriesResponse, PeerError> {
    let Message::AppendEntriesResponse {
        term,
        accepted,
        match_index,
        request_previous_index,
    } = &envelope.message
    else {
        return Err(PeerError::WrongMessageKind);
    };
    Ok(proto::AppendEntriesResponse {
        identity: Some(encode_identity(cluster, envelope, *term)?),
        accepted: *accepted,
        match_index: *match_index,
        request_previous_index: *request_previous_index,
    })
}

fn encode_identity(
    cluster: ClusterId,
    envelope: &Envelope,
    term: Term,
) -> Result<proto::RpcIdentity, PeerError> {
    encode_rpc_identity(cluster, envelope.from, envelope.to, term)
}

fn encode_rpc_identity(
    cluster: ClusterId,
    from: NodeId,
    to: NodeId,
    term: Term,
) -> Result<proto::RpcIdentity, PeerError> {
    valid_node(from.0)?;
    valid_node(to.0)?;
    if term == Term(0) {
        return Err(PeerError::ZeroTerm);
    }
    Ok(proto::RpcIdentity {
        cluster_id: cluster.0.to_vec(),
        sender_id: from.0,
        recipient_id: to.0,
        protocol_version: PEER_PROTOCOL_VERSION,
        term: term.0,
    })
}

fn encode_position(position: LogPosition) -> proto::LogPosition {
    proto::LogPosition {
        index: position.index,
        term: position.term.0,
    }
}

fn decode_position(position: Option<proto::LogPosition>) -> Result<LogPosition, PeerError> {
    let position = position.ok_or(PeerError::MissingLogPosition)?;
    if (position.index == 0) != (position.term == 0) {
        return Err(PeerError::InvalidLogPosition);
    }
    Ok(LogPosition {
        index: position.index,
        term: Term(position.term),
    })
}

fn encode_entry(entry: &LogEntry) -> proto::LogEntry {
    let payload = match &entry.payload {
        EntryPayload::Noop => proto::log_entry::Payload::Noop(true),
        EntryPayload::Command(command) => proto::log_entry::Payload::Command(command.clone()),
        EntryPayload::Configuration(configuration) => proto::log_entry::Payload::Configuration(
            encode_membership(configuration).expect("validated membership entry"),
        ),
    };
    proto::LogEntry {
        index: entry.index,
        term: entry.term.0,
        payload: Some(payload),
    }
}

fn decode_entry(entry: proto::LogEntry, max_command_bytes: usize) -> Result<LogEntry, PeerError> {
    if entry.index == 0 || entry.term == 0 {
        return Err(PeerError::InvalidLogEntry);
    }
    let payload = match entry.payload.ok_or(PeerError::MissingEntryPayload)? {
        proto::log_entry::Payload::Noop(true) => EntryPayload::Noop,
        proto::log_entry::Payload::Noop(false) => return Err(PeerError::InvalidNoop),
        proto::log_entry::Payload::Command(command) if command.len() <= max_command_bytes => {
            EntryPayload::Command(command)
        }
        proto::log_entry::Payload::Command(_) => return Err(PeerError::CommandTooLarge),
        proto::log_entry::Payload::Configuration(configuration)
            if configuration.len() <= max_command_bytes =>
        {
            EntryPayload::Configuration(
                decode_membership(&configuration).map_err(|_| PeerError::InvalidConfiguration)?,
            )
        }
        proto::log_entry::Payload::Configuration(_) => return Err(PeerError::CommandTooLarge),
    };
    Ok(LogEntry {
        index: entry.index,
        term: Term(entry.term),
        payload,
    })
}

fn valid_node(node: u64) -> Result<NodeId, PeerError> {
    if node == 0 {
        Err(PeerError::ZeroNodeId)
    } else {
        Ok(NodeId(node))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerError {
    MissingIdentity,
    UnsupportedProtocol(u32),
    ClusterMismatch,
    RecipientMismatch,
    ZeroNodeId,
    ZeroTerm,
    SenderRoleMismatch,
    MissingLogPosition,
    InvalidLogPosition,
    EntryBatchTooLarge,
    InvalidLogEntry,
    MissingEntryPayload,
    InvalidNoop,
    CommandTooLarge,
    WrongMessageKind,
    ForwardingLoop,
    InvalidCommand,
    InvalidCommandResult,
    UnsupportedSnapshotFormat(u32),
    InvalidSnapshotPosition,
    InvalidSnapshotVoters,
    InvalidSnapshotDigest,
    SnapshotTooLarge,
    InvalidSnapshotChunk,
    InvalidConfiguration,
}

impl Display for PeerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid peer RPC: {self:?}")
    }
}

impl Error for PeerError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> PeerContext {
        PeerContext {
            cluster_id: ClusterId([4; 16]),
            local_id: NodeId(2),
            limits: PeerLimits::default(),
        }
    }

    #[test]
    fn append_entries_round_trips_without_interpreting_commands() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::AppendEntries {
                term: Term(7),
                leader: NodeId(1),
                previous: LogPosition {
                    index: 2,
                    term: Term(6),
                },
                entries: vec![LogEntry {
                    index: 3,
                    term: Term(7),
                    payload: EntryPayload::Command(vec![0, 255]),
                }],
                leader_commit: 2,
            },
        };
        let encoded = encode_append_entries(ClusterId([4; 16]), &envelope).expect("encode");
        assert_eq!(
            context().decode_append_entries(encoded).expect("decode"),
            envelope
        );
    }

    #[test]
    fn identity_and_resource_mismatches_fail_before_the_core() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::RequestVote {
                term: Term(1),
                candidate: NodeId(1),
                last_log: LogPosition::default(),
            },
        };
        let mut encoded = encode_request_vote(ClusterId([4; 16]), &envelope).expect("encode");
        encoded
            .identity
            .as_mut()
            .expect("identity")
            .protocol_version = 99;
        assert_eq!(
            context().decode_request_vote(encoded),
            Err(PeerError::UnsupportedProtocol(99))
        );
    }

    #[test]
    fn forwarding_round_trips_once_and_rejects_another_hop() {
        let command = Command::Read {
            key: "forwarded".into(),
        };
        let mut request =
            encode_forward_command(ClusterId([4; 16]), NodeId(1), NodeId(2), Term(3), &command)
                .expect("encode request");
        assert_eq!(
            context()
                .decode_forward_command(request.clone())
                .expect("decode request"),
            (NodeId(1), command)
        );
        request.hop_count = 2;
        assert_eq!(
            context().decode_forward_command(request),
            Err(PeerError::ForwardingLoop)
        );

        let result = CommandResult::Read(crate::kv::ReadResult::NotFound);
        let response =
            encode_forward_response(ClusterId([4; 16]), NodeId(1), NodeId(2), Term(3), &result)
                .expect("encode response");
        assert_eq!(
            context()
                .decode_forward_response(response)
                .expect("decode response"),
            result
        );

        let target = ClusterConfiguration::new([
            (1, "http://node-1:9080".to_owned()),
            (2, "http://node-2:9080".to_owned()),
        ])
        .expect("target");
        let membership = encode_forward_membership(
            ClusterId([4; 16]),
            NodeId(1),
            NodeId(2),
            Term(3),
            Some(([5; 16], &target)),
        )
        .expect("encode membership");
        assert_eq!(
            context()
                .decode_forward_membership(membership)
                .expect("decode membership"),
            (NodeId(1), Some(([5; 16], target)))
        );
    }

    #[test]
    fn snapshot_chunks_bind_identity_offsets_and_digest() {
        let snapshot = Snapshot {
            format_version: SNAPSHOT_FORMAT_VERSION,
            cluster_id: [4; 16],
            last_included: LogPosition {
                index: 9,
                term: Term(3),
            },
            voters: vec![NodeId(1), NodeId(2)],
            membership: crate::raft::MembershipEntry::initial(
                crate::raft::ClusterConfiguration::new([
                    (1, "node://1".to_owned()),
                    (2, "node://2".to_owned()),
                ])
                .expect("configuration"),
            ),
            payload: vec![1, 2, 3, 4],
            digest: [8; 32],
        };
        let encoded = encode_snapshot_chunk(
            ClusterId([4; 16]),
            NodeId(1),
            NodeId(2),
            Term(3),
            &snapshot,
            0,
            2,
        )
        .expect("encode chunk");
        let decoded = context()
            .decode_snapshot_chunk(encoded.clone())
            .expect("decode chunk");
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.total_size, 4);
        assert_eq!(decoded.bytes, vec![1, 2]);
        assert!(!decoded.done);

        let mut invalid = encoded;
        invalid.done = true;
        assert_eq!(
            context().decode_snapshot_chunk(invalid),
            Err(PeerError::InvalidSnapshotChunk)
        );
    }
}
