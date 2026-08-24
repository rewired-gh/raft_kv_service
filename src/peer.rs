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
    storage::{SNAPSHOT_FORMAT_VERSION, Snapshot, StorageLimits},
};

pub const PEER_PROTOCOL_VERSION: u32 = 4;
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
    pub replicated_limits: StorageLimits,
}

impl PeerContext {
    pub fn decode_pre_vote(&self, request: proto::PreVoteRequest) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(request.identity)?;
        let prospective_term = valid_term(request.prospective_term)?;
        if identity.term != prospective_term {
            return Err(PeerError::TermMismatch);
        }
        let candidate = valid_node(request.candidate_id)?;
        if candidate != identity.sender {
            return Err(PeerError::SenderRoleMismatch);
        }
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::PreVote {
                prospective_term,
                candidate,
                last_log: decode_position(request.last_log)?,
            },
        })
    }

    pub fn decode_pre_vote_response(
        &self,
        response: proto::PreVoteResponse,
    ) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(response.identity)?;
        let current_term = Term(response.current_term);
        let prospective_term = valid_term(response.prospective_term)?;
        if identity.term != prospective_term {
            return Err(PeerError::TermMismatch);
        }
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::PreVoteResponse {
                current_term,
                prospective_term,
                vote_granted: response.vote_granted,
            },
        })
    }

    pub fn decode_timeout_now(
        &self,
        request: proto::TimeoutNowRequest,
    ) -> Result<Envelope, PeerError> {
        let identity = self.validate_identity(request.identity)?;
        let leader = valid_node(request.leader_id)?;
        if leader != identity.sender || request.fence_index == 0 {
            return Err(if leader != identity.sender {
                PeerError::SenderRoleMismatch
            } else {
                PeerError::InvalidFenceIndex
            });
        }
        Ok(Envelope {
            from: identity.sender,
            to: self.local_id,
            message: Message::TimeoutNow {
                term: identity.term,
                leader,
                fence_index: request.fence_index,
            },
        })
    }

    pub fn decode_timeout_now_response(
        &self,
        response: proto::TimeoutNowResponse,
    ) -> Result<(NodeId, Term, bool), PeerError> {
        let identity = self.validate_identity(response.identity)?;
        Ok((identity.sender, identity.term, response.accepted))
    }

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
        let received_limits = StorageLimits {
            max_clients: identity.max_clients,
            max_key_bytes: identity.max_key_bytes,
            max_value_bytes: identity.max_value_bytes,
        };
        if received_limits != self.replicated_limits {
            return Err(PeerError::IncompatibleReplicatedLimits {
                expected: self.replicated_limits,
                received: received_limits,
            });
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
    limits: StorageLimits,
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
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
        operation_id,
        members,
        hop_count: 1,
        query,
    })
}

pub fn encode_forward_membership_response(
    cluster: ClusterId,
    limits: StorageLimits,
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
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
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
    limits: StorageLimits,
    identity: (NodeId, NodeId, Term),
    snapshot: &Snapshot,
    offset: usize,
    chunk_size: usize,
) -> Result<proto::InstallSnapshotRequest, PeerError> {
    let (from, to, term) = identity;
    let end = offset
        .checked_add(chunk_size)
        .map(|end| end.min(snapshot.payload.len()))
        .ok_or(PeerError::InvalidSnapshotChunk)?;
    if offset >= end {
        return Err(PeerError::InvalidSnapshotChunk);
    }
    Ok(proto::InstallSnapshotRequest {
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
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
    limits: StorageLimits,
    identity: (NodeId, NodeId, Term),
    snapshot_id: [u8; 32],
    next_offset: usize,
    installed: bool,
) -> Result<proto::InstallSnapshotResponse, PeerError> {
    let (from, to, term) = identity;
    Ok(proto::InstallSnapshotResponse {
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
        snapshot_id: snapshot_id.to_vec(),
        next_offset: u64::try_from(next_offset).map_err(|_| PeerError::InvalidSnapshotChunk)?,
        installed,
    })
}

pub fn encode_forward_command(
    cluster: ClusterId,
    limits: StorageLimits,
    from: NodeId,
    to: NodeId,
    term: Term,
    command: &Command,
) -> Result<proto::ForwardCommandRequest, PeerError> {
    Ok(proto::ForwardCommandRequest {
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
        command: encode_command(command).map_err(|_| PeerError::InvalidCommand)?,
        hop_count: 1,
    })
}

pub fn encode_forward_response(
    cluster: ClusterId,
    limits: StorageLimits,
    from: NodeId,
    to: NodeId,
    term: Term,
    result: &CommandResult,
) -> Result<proto::ForwardCommandResponse, PeerError> {
    Ok(proto::ForwardCommandResponse {
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
        result: encode_command_result(result).map_err(|_| PeerError::InvalidCommandResult)?,
    })
}

struct Identity {
    sender: NodeId,
    term: Term,
}

pub fn encode_pre_vote(
    cluster: ClusterId,
    limits: StorageLimits,
    envelope: &Envelope,
) -> Result<proto::PreVoteRequest, PeerError> {
    let Message::PreVote {
        prospective_term,
        candidate,
        last_log,
    } = &envelope.message
    else {
        return Err(PeerError::WrongMessageKind);
    };
    if *candidate != envelope.from {
        return Err(PeerError::SenderRoleMismatch);
    }
    Ok(proto::PreVoteRequest {
        identity: Some(encode_identity(
            cluster,
            limits,
            envelope,
            *prospective_term,
        )?),
        prospective_term: prospective_term.0,
        candidate_id: candidate.0,
        last_log: Some(encode_position(*last_log)),
    })
}

pub fn encode_pre_vote_response(
    cluster: ClusterId,
    limits: StorageLimits,
    envelope: &Envelope,
) -> Result<proto::PreVoteResponse, PeerError> {
    let Message::PreVoteResponse {
        current_term,
        prospective_term,
        vote_granted,
    } = &envelope.message
    else {
        return Err(PeerError::WrongMessageKind);
    };
    Ok(proto::PreVoteResponse {
        // A fresh node may truthfully report current term zero. The non-zero
        // prospective term therefore authenticates the RPC identity, while the
        // explicitly carried current term remains the responder's observation.
        identity: Some(encode_identity(
            cluster,
            limits,
            envelope,
            *prospective_term,
        )?),
        current_term: current_term.0,
        prospective_term: prospective_term.0,
        vote_granted: *vote_granted,
    })
}

pub fn encode_request_vote(
    cluster: ClusterId,
    limits: StorageLimits,
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
        identity: Some(encode_identity(cluster, limits, envelope, *term)?),
        candidate_id: candidate.0,
        last_log: Some(encode_position(*last_log)),
    })
}

pub fn encode_request_vote_response(
    cluster: ClusterId,
    limits: StorageLimits,
    envelope: &Envelope,
) -> Result<proto::RequestVoteResponse, PeerError> {
    let Message::RequestVoteResponse { term, vote_granted } = &envelope.message else {
        return Err(PeerError::WrongMessageKind);
    };
    Ok(proto::RequestVoteResponse {
        identity: Some(encode_identity(cluster, limits, envelope, *term)?),
        vote_granted: *vote_granted,
    })
}

pub fn encode_append_entries(
    cluster: ClusterId,
    limits: StorageLimits,
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
        identity: Some(encode_identity(cluster, limits, envelope, *term)?),
        leader_id: leader.0,
        previous: Some(encode_position(*previous)),
        entries: entries.iter().map(encode_entry).collect(),
        leader_commit: *leader_commit,
    })
}

pub fn encode_append_entries_response(
    cluster: ClusterId,
    limits: StorageLimits,
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
        identity: Some(encode_identity(cluster, limits, envelope, *term)?),
        accepted: *accepted,
        match_index: *match_index,
        request_previous_index: *request_previous_index,
    })
}

pub fn encode_timeout_now(
    cluster: ClusterId,
    limits: StorageLimits,
    envelope: &Envelope,
) -> Result<proto::TimeoutNowRequest, PeerError> {
    let Message::TimeoutNow {
        term,
        leader,
        fence_index,
    } = &envelope.message
    else {
        return Err(PeerError::WrongMessageKind);
    };
    if *leader != envelope.from {
        return Err(PeerError::SenderRoleMismatch);
    }
    if *fence_index == 0 {
        return Err(PeerError::InvalidFenceIndex);
    }
    Ok(proto::TimeoutNowRequest {
        identity: Some(encode_identity(cluster, limits, envelope, *term)?),
        leader_id: leader.0,
        fence_index: *fence_index,
    })
}

pub fn encode_timeout_now_response(
    cluster: ClusterId,
    limits: StorageLimits,
    from: NodeId,
    to: NodeId,
    term: Term,
    accepted: bool,
) -> Result<proto::TimeoutNowResponse, PeerError> {
    Ok(proto::TimeoutNowResponse {
        identity: Some(encode_rpc_identity(cluster, limits, from, to, term)?),
        accepted,
    })
}

fn encode_identity(
    cluster: ClusterId,
    limits: StorageLimits,
    envelope: &Envelope,
    term: Term,
) -> Result<proto::RpcIdentity, PeerError> {
    encode_rpc_identity(cluster, limits, envelope.from, envelope.to, term)
}

fn encode_rpc_identity(
    cluster: ClusterId,
    limits: StorageLimits,
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
        max_clients: limits.max_clients,
        max_key_bytes: limits.max_key_bytes,
        max_value_bytes: limits.max_value_bytes,
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

fn valid_term(term: u64) -> Result<Term, PeerError> {
    if term == 0 {
        Err(PeerError::ZeroTerm)
    } else {
        Ok(Term(term))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerError {
    MissingIdentity,
    UnsupportedProtocol(u32),
    ClusterMismatch,
    IncompatibleReplicatedLimits {
        expected: StorageLimits,
        received: StorageLimits,
    },
    RecipientMismatch,
    ZeroNodeId,
    ZeroTerm,
    TermMismatch,
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
    InvalidFenceIndex,
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

    fn replicated_limits() -> StorageLimits {
        StorageLimits::default()
    }

    fn context() -> PeerContext {
        PeerContext {
            cluster_id: ClusterId([4; 16]),
            local_id: NodeId(2),
            limits: PeerLimits::default(),
            replicated_limits: replicated_limits(),
        }
    }

    #[test]
    fn pre_vote_round_trips_without_advancing_the_durable_term() {
        let request = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::PreVote {
                prospective_term: Term(4),
                candidate: NodeId(1),
                last_log: LogPosition {
                    index: 9,
                    term: Term(3),
                },
            },
        };
        let encoded = encode_pre_vote(ClusterId([4; 16]), replicated_limits(), &request)
            .expect("encode pre-vote");
        assert_eq!(
            context().decode_pre_vote(encoded).expect("decode pre-vote"),
            request
        );

        let response = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::PreVoteResponse {
                current_term: Term(0),
                prospective_term: Term(1),
                vote_granted: true,
            },
        };
        let encoded = encode_pre_vote_response(ClusterId([4; 16]), replicated_limits(), &response)
            .expect("encode pre-vote response");
        assert_eq!(encoded.identity.as_ref().expect("identity").term, 1);
        assert_eq!(
            context()
                .decode_pre_vote_response(encoded)
                .expect("decode pre-vote response"),
            response
        );
    }

    #[test]
    fn pre_vote_rejects_ambiguous_identity_and_term_bindings() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::PreVote {
                prospective_term: Term(4),
                candidate: NodeId(1),
                last_log: LogPosition::default(),
            },
        };
        let valid = encode_pre_vote(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode pre-vote");

        let mut mismatched_term = valid.clone();
        mismatched_term.prospective_term = 5;
        assert_eq!(
            context().decode_pre_vote(mismatched_term),
            Err(PeerError::TermMismatch)
        );
        let mut zero_term = valid.clone();
        zero_term.prospective_term = 0;
        assert_eq!(
            context().decode_pre_vote(zero_term),
            Err(PeerError::ZeroTerm)
        );
        let mut wrong_candidate = valid;
        wrong_candidate.candidate_id = 3;
        assert_eq!(
            context().decode_pre_vote(wrong_candidate),
            Err(PeerError::SenderRoleMismatch)
        );

        let response = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::PreVoteResponse {
                current_term: Term(3),
                prospective_term: Term(4),
                vote_granted: false,
            },
        };
        let mut encoded =
            encode_pre_vote_response(ClusterId([4; 16]), replicated_limits(), &response)
                .expect("encode response");
        encoded.prospective_term = 5;
        assert_eq!(
            context().decode_pre_vote_response(encoded),
            Err(PeerError::TermMismatch)
        );
    }

    #[test]
    fn timeout_now_round_trips_with_a_nonzero_fence() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::TimeoutNow {
                term: Term(7),
                leader: NodeId(1),
                fence_index: 42,
            },
        };
        let encoded = encode_timeout_now(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode timeout-now");
        assert_eq!(
            context()
                .decode_timeout_now(encoded)
                .expect("decode timeout-now"),
            envelope
        );

        let response = encode_timeout_now_response(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(7),
            true,
        )
        .expect("encode timeout-now response");
        assert_eq!(
            context()
                .decode_timeout_now_response(response)
                .expect("decode timeout-now response"),
            (NodeId(1), Term(7), true)
        );
    }

    #[test]
    fn timeout_now_rejects_sender_role_and_zero_fence() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::TimeoutNow {
                term: Term(7),
                leader: NodeId(1),
                fence_index: 42,
            },
        };
        let valid = encode_timeout_now(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode timeout-now");
        let mut wrong_leader = valid.clone();
        wrong_leader.leader_id = 3;
        assert_eq!(
            context().decode_timeout_now(wrong_leader),
            Err(PeerError::SenderRoleMismatch)
        );
        let mut zero_fence = valid;
        zero_fence.fence_index = 0;
        assert_eq!(
            context().decode_timeout_now(zero_fence),
            Err(PeerError::InvalidFenceIndex)
        );
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
        let encoded = encode_append_entries(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode");
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
        let mut encoded = encode_request_vote(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode");
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
    fn replicated_limit_mismatch_fails_before_the_core() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::RequestVote {
                term: Term(1),
                candidate: NodeId(1),
                last_log: LogPosition::default(),
            },
        };
        let mut encoded = encode_request_vote(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode");
        encoded.identity.as_mut().expect("identity").max_value_bytes -= 1;
        assert!(matches!(
            context().decode_request_vote(encoded),
            Err(PeerError::IncompatibleReplicatedLimits { .. })
        ));
    }

    #[test]
    fn identity_validation_rejects_each_ambiguous_peer_boundary() {
        let envelope = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::RequestVote {
                term: Term(1),
                candidate: NodeId(1),
                last_log: LogPosition::default(),
            },
        };
        let valid = encode_request_vote(ClusterId([4; 16]), replicated_limits(), &envelope)
            .expect("encode request");

        let mut missing = valid.clone();
        missing.identity = None;
        assert_eq!(
            context().decode_request_vote(missing),
            Err(PeerError::MissingIdentity)
        );

        let mut wrong_cluster = valid.clone();
        wrong_cluster
            .identity
            .as_mut()
            .expect("identity")
            .cluster_id = vec![9; 16];
        assert_eq!(
            context().decode_request_vote(wrong_cluster),
            Err(PeerError::ClusterMismatch)
        );

        for (sender, recipient, term, expected) in [
            (0, 2, 1, PeerError::ZeroNodeId),
            (1, 0, 1, PeerError::ZeroNodeId),
            (1, 3, 1, PeerError::RecipientMismatch),
            (1, 2, 0, PeerError::ZeroTerm),
        ] {
            let mut request = valid.clone();
            let identity = request.identity.as_mut().expect("identity");
            identity.sender_id = sender;
            identity.recipient_id = recipient;
            identity.term = term;
            assert_eq!(context().decode_request_vote(request), Err(expected));
        }

        let mut mismatched_candidate = valid;
        mismatched_candidate.candidate_id = 3;
        assert_eq!(
            context().decode_request_vote(mismatched_candidate),
            Err(PeerError::SenderRoleMismatch)
        );
    }

    #[test]
    fn forwarding_round_trips_once_and_rejects_another_hop() {
        let command = Command::Read {
            key: "forwarded".into(),
        };
        let mut request = encode_forward_command(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(3),
            &command,
        )
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
        let response = encode_forward_response(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(3),
            &result,
        )
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
            replicated_limits(),
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
    fn forwarding_rejects_resource_encoding_and_membership_ambiguity() {
        let command = Command::Read { key: "key".into() };
        let valid = encode_forward_command(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(1),
            &command,
        )
        .expect("encode command");
        let mut limited = context();
        limited.limits.max_command_bytes = 1;
        assert_eq!(
            limited.decode_forward_command(valid.clone()),
            Err(PeerError::CommandTooLarge)
        );
        let mut malformed = valid;
        malformed.command = vec![0xff];
        assert_eq!(
            context().decode_forward_command(malformed),
            Err(PeerError::InvalidCommand)
        );

        let query = encode_forward_membership(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(1),
            None,
        )
        .expect("encode query");
        let mut looped = query.clone();
        looped.hop_count = 2;
        assert_eq!(
            context().decode_forward_membership(looped),
            Err(PeerError::ForwardingLoop)
        );
        let mut ambiguous_query = query;
        ambiguous_query.operation_id = vec![1; 16];
        assert_eq!(
            context().decode_forward_membership(ambiguous_query),
            Err(PeerError::InvalidConfiguration)
        );

        let target =
            ClusterConfiguration::new([(1, "http://node-1:9080".to_owned())]).expect("target");
        let mutation = encode_forward_membership(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(1),
            Some(([1; 16], &target)),
        )
        .expect("encode mutation");
        let mut zero_operation = mutation.clone();
        zero_operation.operation_id = vec![0; 16];
        assert_eq!(
            context().decode_forward_membership(zero_operation),
            Err(PeerError::InvalidConfiguration)
        );
        let mut invalid_endpoint = mutation;
        invalid_endpoint.members[0].peer_endpoint = "node://1".to_owned();
        assert_eq!(
            context().decode_forward_membership(invalid_endpoint),
            Err(PeerError::InvalidConfiguration)
        );

        let result = CommandResult::Read(crate::kv::ReadResult::NotFound);
        let response = encode_forward_response(
            ClusterId([4; 16]),
            replicated_limits(),
            NodeId(1),
            NodeId(2),
            Term(1),
            &result,
        )
        .expect("encode response");
        let mut oversized_response = response.clone();
        oversized_response.result = vec![0; PeerLimits::default().max_command_bytes + 1];
        assert_eq!(
            context().decode_forward_response(oversized_response),
            Err(PeerError::CommandTooLarge)
        );
        let mut malformed_response = response;
        malformed_response.result = vec![0xff];
        assert_eq!(
            context().decode_forward_response(malformed_response),
            Err(PeerError::InvalidCommandResult)
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
            replicated_limits(),
            (NodeId(1), NodeId(2), Term(3)),
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

    #[test]
    fn snapshot_decoder_rejects_each_framing_and_resource_boundary() {
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
        let valid = encode_snapshot_chunk(
            ClusterId([4; 16]),
            replicated_limits(),
            (NodeId(1), NodeId(2), Term(3)),
            &snapshot,
            0,
            2,
        )
        .expect("encode chunk");

        let mut wrong_format = valid.clone();
        wrong_format.format_version += 1;
        assert!(matches!(
            context().decode_snapshot_chunk(wrong_format),
            Err(PeerError::UnsupportedSnapshotFormat(_))
        ));
        for mutate in [
            |request: &mut proto::InstallSnapshotRequest| request.last_included_index = 0,
            |request: &mut proto::InstallSnapshotRequest| request.last_included_term = 0,
        ] {
            let mut request = valid.clone();
            mutate(&mut request);
            assert_eq!(
                context().decode_snapshot_chunk(request),
                Err(PeerError::InvalidSnapshotPosition)
            );
        }

        for voters in [vec![], vec![2, 1], vec![1]] {
            let mut request = valid.clone();
            request.voter_ids = voters;
            assert_eq!(
                context().decode_snapshot_chunk(request),
                Err(PeerError::InvalidSnapshotVoters)
            );
        }

        let mut wrong_digest = valid.clone();
        wrong_digest.snapshot_id[0] ^= 1;
        assert_eq!(
            context().decode_snapshot_chunk(wrong_digest),
            Err(PeerError::InvalidSnapshotDigest)
        );
        for size in [
            0,
            u64::try_from(PeerLimits::default().max_snapshot_bytes)
                .expect("snapshot limit fits u64")
                + 1,
        ] {
            let mut request = valid.clone();
            request.total_size = size;
            assert_eq!(
                context().decode_snapshot_chunk(request),
                Err(PeerError::SnapshotTooLarge)
            );
        }

        let mut empty = valid.clone();
        empty.chunk.clear();
        assert_eq!(
            context().decode_snapshot_chunk(empty),
            Err(PeerError::InvalidSnapshotChunk)
        );
        let mut past_end = valid.clone();
        past_end.offset = 3;
        assert_eq!(
            context().decode_snapshot_chunk(past_end),
            Err(PeerError::InvalidSnapshotChunk)
        );
        let mut limited = context();
        limited.limits.snapshot_chunk_bytes = 1;
        assert_eq!(
            limited.decode_snapshot_chunk(valid),
            Err(PeerError::InvalidSnapshotChunk)
        );
    }
}
