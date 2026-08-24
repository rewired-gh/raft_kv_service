//! Versioned peer RPC schema and validation boundary.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use crate::{
    kv::{
        Command, CommandResult, decode_command, decode_command_result, encode_command,
        encode_command_result,
    },
    raft::{EntryPayload, Envelope, LogEntry, LogPosition, Message, NodeId, Term},
};

pub const PEER_PROTOCOL_VERSION: u32 = 1;

pub mod proto {
    tonic::include_proto!("raft.peer.v1");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerLimits {
    pub max_entries_per_append: usize,
    pub max_command_bytes: usize,
}

impl Default for PeerLimits {
    fn default() -> Self {
        Self {
            max_entries_per_append: 256,
            // The client value limit plus room for the canonical command envelope.
            max_command_bytes: 1024 * 1024 + 1024,
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
    }
}
