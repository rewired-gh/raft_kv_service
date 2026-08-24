//! Stable, payload-redacted protocol traces and M2 abstraction validation.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
};

use serde::{Deserialize, Serialize};

use super::{Envelope, Message, simulator::TraceEvent};

pub const TRACE_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceAction {
    ElectionTimeout,
    HardStatePersisted,
    LogPersisted,
    CommitAdvanced,
    EntriesApplied,
    MessageSent,
    MessageDelivered,
    MessageDropped,
    ElectionTimerReset,
    NodeCrashed,
    NodeRestarted,
    StalePersistenceDiscarded,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceMessage {
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraceRecord {
    pub sequence: u64,
    pub action: TraceAction,
    pub node: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<TraceMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolTrace {
    pub schema_version: u16,
    pub records: Vec<TraceRecord>,
}

impl ProtocolTrace {
    pub fn from_simulator(events: &[TraceEvent]) -> Result<Self, TraceError> {
        let records = events
            .iter()
            .enumerate()
            .map(|(sequence, event)| {
                let sequence =
                    u64::try_from(sequence).map_err(|_| TraceError::SequenceExhausted)?;
                Ok(record(sequence, event))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            schema_version: TRACE_SCHEMA_VERSION,
            records,
        })
    }

    /// Checks the implementation events against the M1/M2 persistence-order
    /// abstraction used by `formal/RaftElection.tla` and `formal/Raft.tla`.
    pub fn validate_m2_abstraction(&self) -> Result<(), TraceError> {
        if self.schema_version != TRACE_SCHEMA_VERSION {
            return Err(TraceError::UnsupportedSchema(self.schema_version));
        }

        let mut hard_term = BTreeMap::<u64, u64>::new();
        let mut durable_log = BTreeMap::<u64, u64>::new();
        let mut commit = BTreeMap::<u64, u64>::new();
        let mut applied = BTreeMap::<u64, u64>::new();
        for (expected, record) in (0_u64..).zip(&self.records) {
            if record.sequence != expected {
                return Err(TraceError::NonContiguousSequence {
                    expected,
                    found: record.sequence,
                });
            }
            let known_term = hard_term.entry(record.node).or_default();
            let known_log = durable_log.entry(record.node).or_default();
            let known_commit = commit.entry(record.node).or_default();
            let known_applied = applied.entry(record.node).or_default();
            match record.action {
                TraceAction::HardStatePersisted => {
                    let term = required(record.term, record.sequence, "term")?;
                    if term < *known_term {
                        return Err(TraceError::TermRegression(record.sequence));
                    }
                    *known_term = term;
                }
                TraceAction::LogPersisted => {
                    let index = required(record.index, record.sequence, "index")?;
                    if index < *known_commit {
                        return Err(TraceError::DurableLogBelowCommit(record.sequence));
                    }
                    *known_log = index;
                }
                TraceAction::CommitAdvanced => {
                    let index = required(record.index, record.sequence, "index")?;
                    if index < *known_commit {
                        return Err(TraceError::CommitRegression(record.sequence));
                    }
                    if index > *known_log {
                        return Err(TraceError::CommitBeyondDurableLog(record.sequence));
                    }
                    *known_commit = index;
                }
                TraceAction::EntriesApplied => {
                    let index = required(record.index, record.sequence, "index")?;
                    if index <= *known_applied {
                        return Err(TraceError::ApplicationRegression(record.sequence));
                    }
                    if index > *known_commit {
                        return Err(TraceError::ApplicationBeyondCommit(record.sequence));
                    }
                    *known_applied = index;
                }
                TraceAction::MessageSent => validate_send(record, *known_term, *known_log)?,
                TraceAction::ElectionTimeout
                | TraceAction::MessageDelivered
                | TraceAction::MessageDropped
                | TraceAction::ElectionTimerReset
                | TraceAction::NodeCrashed
                | TraceAction::NodeRestarted
                | TraceAction::StalePersistenceDiscarded => {}
            }
        }
        Ok(())
    }
}

fn validate_send(record: &TraceRecord, hard_term: u64, durable_log: u64) -> Result<(), TraceError> {
    let message = record.message.ok_or(TraceError::MissingField {
        sequence: record.sequence,
        field: "message",
    })?;
    let term = required(record.term, record.sequence, "term")?;
    if term > hard_term {
        return Err(TraceError::MessageBeforeHardState(record.sequence));
    }
    match message {
        TraceMessage::AppendEntries => {
            let index = required(record.index, record.sequence, "index")?;
            if index > durable_log {
                return Err(TraceError::AppendBeforeDurableLog(record.sequence));
            }
        }
        TraceMessage::AppendEntriesResponse if record.accepted == Some(true) => {
            let index = required(record.index, record.sequence, "index")?;
            if index > durable_log {
                return Err(TraceError::AcknowledgementBeforeDurableLog(record.sequence));
            }
        }
        TraceMessage::RequestVote
        | TraceMessage::RequestVoteResponse
        | TraceMessage::AppendEntriesResponse => {}
    }
    Ok(())
}

fn required(value: Option<u64>, sequence: u64, field: &'static str) -> Result<u64, TraceError> {
    value.ok_or(TraceError::MissingField { sequence, field })
}

fn record(sequence: u64, event: &TraceEvent) -> TraceRecord {
    let mut record = TraceRecord {
        sequence,
        action: TraceAction::ElectionTimeout,
        node: 0,
        peer: None,
        term: None,
        index: None,
        message: None,
        accepted: None,
    };
    match event {
        TraceEvent::ElectionTimeout(node) => record.node = node.0,
        TraceEvent::HardStatePersisted(node, state) => {
            record.action = TraceAction::HardStatePersisted;
            record.node = node.0;
            record.term = Some(state.current_term.0);
        }
        TraceEvent::LogPersisted(node, mutation) => {
            record.action = TraceAction::LogPersisted;
            record.node = node.0;
            record.index = Some(
                mutation
                    .entries
                    .last()
                    .map_or(mutation.previous.index, |entry| entry.index),
            );
        }
        TraceEvent::CommitIndexAdvanced(node, index) => {
            record.action = TraceAction::CommitAdvanced;
            record.node = node.0;
            record.index = Some(*index);
        }
        TraceEvent::EntriesApplied(node, index) => {
            record.action = TraceAction::EntriesApplied;
            record.node = node.0;
            record.index = Some(*index);
        }
        TraceEvent::MessageSent(envelope) => {
            fill_message(&mut record, TraceAction::MessageSent, envelope)
        }
        TraceEvent::MessageDelivered(envelope) => {
            fill_message(&mut record, TraceAction::MessageDelivered, envelope);
            record.node = envelope.to.0;
            record.peer = Some(envelope.from.0);
        }
        TraceEvent::MessageDropped(envelope) => {
            fill_message(&mut record, TraceAction::MessageDropped, envelope)
        }
        TraceEvent::ElectionTimerReset(node) => {
            record.action = TraceAction::ElectionTimerReset;
            record.node = node.0;
        }
        TraceEvent::NodeCrashed(node) => {
            record.action = TraceAction::NodeCrashed;
            record.node = node.0;
        }
        TraceEvent::NodeRestarted(node) => {
            record.action = TraceAction::NodeRestarted;
            record.node = node.0;
        }
        TraceEvent::StalePersistenceDiscarded(node) => {
            record.action = TraceAction::StalePersistenceDiscarded;
            record.node = node.0;
        }
    }
    record
}

fn fill_message(record: &mut TraceRecord, action: TraceAction, envelope: &Envelope) {
    record.action = action;
    record.node = envelope.from.0;
    record.peer = Some(envelope.to.0);
    let (message, term, index, accepted) = match &envelope.message {
        Message::RequestVote { term, last_log, .. } => (
            TraceMessage::RequestVote,
            term.0,
            Some(last_log.index),
            None,
        ),
        Message::RequestVoteResponse { term, vote_granted } => (
            TraceMessage::RequestVoteResponse,
            term.0,
            None,
            Some(*vote_granted),
        ),
        Message::AppendEntries {
            term,
            previous,
            entries,
            ..
        } => (
            TraceMessage::AppendEntries,
            term.0,
            Some(entries.last().map_or(previous.index, |entry| entry.index)),
            None,
        ),
        Message::AppendEntriesResponse {
            term,
            accepted,
            match_index,
            ..
        } => (
            TraceMessage::AppendEntriesResponse,
            term.0,
            Some(*match_index),
            Some(*accepted),
        ),
    };
    record.message = Some(message);
    record.term = Some(term);
    record.index = index;
    record.accepted = accepted;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraceError {
    UnsupportedSchema(u16),
    SequenceExhausted,
    NonContiguousSequence { expected: u64, found: u64 },
    MissingField { sequence: u64, field: &'static str },
    TermRegression(u64),
    DurableLogBelowCommit(u64),
    CommitRegression(u64),
    CommitBeyondDurableLog(u64),
    ApplicationBeyondCommit(u64),
    ApplicationRegression(u64),
    MessageBeforeHardState(u64),
    AppendBeforeDurableLog(u64),
    AcknowledgementBeforeDurableLog(u64),
}

impl Display for TraceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "protocol trace violates M2 abstraction: {self:?}"
        )
    }
}

impl Error for TraceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeId, simulator::ClusterSimulator};

    #[test]
    fn simulation_trace_round_trips_and_validates() {
        let mut simulation =
            ClusterSimulator::new([NodeId(1), NodeId(2), NodeId(3)]).expect("simulator");
        simulation.timeout(NodeId(1)).expect("timeout");
        simulation.run_until_idle(64).expect("settles");
        let trace = ProtocolTrace::from_simulator(simulation.trace()).expect("trace");
        trace.validate_m2_abstraction().expect("valid abstraction");

        let json = serde_json::to_string(&trace).expect("serialize");
        assert!(!json.contains("payload"));
        let decoded: ProtocolTrace = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, trace);
    }

    #[test]
    fn validator_rejects_success_before_log_persistence() {
        let trace = ProtocolTrace {
            schema_version: TRACE_SCHEMA_VERSION,
            records: vec![TraceRecord {
                sequence: 0,
                action: TraceAction::MessageSent,
                node: 1,
                peer: Some(2),
                term: Some(0),
                index: Some(1),
                message: Some(TraceMessage::AppendEntriesResponse),
                accepted: Some(true),
            }],
        };
        assert_eq!(
            trace.validate_m2_abstraction(),
            Err(TraceError::AcknowledgementBeforeDurableLog(0))
        );
    }
}
