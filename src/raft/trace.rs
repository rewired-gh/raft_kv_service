//! Stable, payload-redacted protocol traces and abstraction validation.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use super::{Envelope, Message, simulator::TraceEvent};

pub const TRACE_SCHEMA_VERSION: u16 = 4;

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
    SnapshotPublished,
    SnapshotTransferStarted,
    SnapshotTransferInterrupted,
    SnapshotInstalled,
    StorageFailed,
    TraceTerminated,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceMessage {
    PreVote,
    PreVoteResponse,
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    TimeoutNow,
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

#[derive(Clone)]
pub struct ProtocolTraceRecorder {
    inner: Arc<Mutex<RecorderInner>>,
}

struct RecorderInner {
    output: BufWriter<File>,
    next_sequence: u64,
    observed: Option<ObservedState>,
}

impl Drop for RecorderInner {
    fn drop(&mut self) {
        let node = self.observed.map_or(0, |state| state.node);
        let _ = self.write(TraceAction::TraceTerminated, node, None, None);
    }
}

#[derive(Clone, Copy)]
struct ObservedState {
    node: u64,
    term: u64,
    last_log: u64,
    commit: u64,
    applied: u64,
    snapshot_index: u64,
    snapshot_term: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TraceFileHeader {
    schema_version: u16,
    format: String,
}

impl ProtocolTraceRecorder {
    pub fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut output = BufWriter::new(file);
        serde_json::to_writer(
            &mut output,
            &TraceFileHeader {
                schema_version: TRACE_SCHEMA_VERSION,
                format: "raft-kv-jsonl".to_owned(),
            },
        )?;
        output.write_all(b"\n")?;
        output.flush()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(RecorderInner {
                output,
                next_sequence: 0,
                observed: None,
            })),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &self,
        node: u64,
        term: u64,
        last_log: u64,
        commit: u64,
        applied: u64,
        snapshot_index: u64,
        snapshot_term: u64,
    ) -> io::Result<()> {
        let next = ObservedState {
            node,
            term,
            last_log,
            commit,
            applied,
            snapshot_index,
            snapshot_term,
        };
        if commit > last_log || applied > commit || snapshot_index > applied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "observed Raft indexes violate durable ordering",
            ));
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("protocol trace recorder poisoned"))?;
        if let Some(previous) = inner.observed
            && (previous.node != node
                || term < previous.term
                || commit < previous.commit
                || applied < previous.applied
                || snapshot_index < previous.snapshot_index
                || (snapshot_index == previous.snapshot_index
                    && snapshot_term != previous.snapshot_term))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "observed Raft state regressed",
            ));
        }
        let previous = inner.observed;
        if previous.is_none_or(|state| term != state.term) && term > 0 {
            inner.write(TraceAction::HardStatePersisted, node, Some(term), None)?;
        }
        if previous.is_none_or(|state| last_log != state.last_log) && last_log > 0 {
            inner.write(TraceAction::LogPersisted, node, None, Some(last_log))?;
        }
        if previous.is_none_or(|state| commit != state.commit) && commit > 0 {
            inner.write(TraceAction::CommitAdvanced, node, None, Some(commit))?;
        }
        if previous.is_none_or(|state| applied != state.applied) && applied > 0 {
            inner.write(TraceAction::EntriesApplied, node, None, Some(applied))?;
        }
        if previous.is_none_or(|state| snapshot_index != state.snapshot_index) && snapshot_index > 0
        {
            inner.write(
                TraceAction::SnapshotPublished,
                node,
                Some(snapshot_term),
                Some(snapshot_index),
            )?;
        }
        inner.observed = Some(next);
        Ok(())
    }
}

impl RecorderInner {
    fn write(
        &mut self,
        action: TraceAction,
        node: u64,
        term: Option<u64>,
        index: Option<u64>,
    ) -> io::Result<()> {
        let record = TraceRecord {
            sequence: self.next_sequence,
            action,
            node,
            peer: None,
            term,
            index,
            message: None,
            accepted: None,
        };
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("protocol trace sequence exhausted"))?;
        serde_json::to_writer(&mut self.output, &record)?;
        self.output.write_all(b"\n")?;
        self.output.flush()
    }
}

impl ProtocolTrace {
    pub fn from_json_lines(path: &Path) -> Result<Self, TraceError> {
        let file =
            File::open(path).map_err(|error| TraceError::InvalidEncoding(error.to_string()))?;
        let mut lines = BufReader::new(file).lines();
        let header = lines
            .next()
            .ok_or(TraceError::Truncated)?
            .map_err(|error| TraceError::InvalidEncoding(error.to_string()))?;
        let header: TraceFileHeader = serde_json::from_str(&header)
            .map_err(|error| TraceError::InvalidEncoding(error.to_string()))?;
        if header.schema_version != TRACE_SCHEMA_VERSION || header.format != "raft-kv-jsonl" {
            return Err(TraceError::UnsupportedSchema(header.schema_version));
        }
        let mut records = Vec::<TraceRecord>::new();
        for line in lines {
            let line = line.map_err(|error| TraceError::InvalidEncoding(error.to_string()))?;
            if line.is_empty() {
                return Err(TraceError::Truncated);
            }
            records.push(
                serde_json::from_str(&line)
                    .map_err(|error| TraceError::InvalidEncoding(error.to_string()))?,
            );
        }
        if records.last().map(|record| record.action) != Some(TraceAction::TraceTerminated) {
            return Err(TraceError::Truncated);
        }
        Ok(Self {
            schema_version: header.schema_version,
            records,
        })
    }

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
                TraceAction::SnapshotInstalled => {
                    let index = required(record.index, record.sequence, "index")?;
                    *known_log = (*known_log).max(index);
                    *known_commit = (*known_commit).max(index);
                    *known_applied = (*known_applied).max(index);
                }
                TraceAction::MessageSent => validate_send(record, *known_term, *known_log)?,
                TraceAction::ElectionTimeout
                | TraceAction::MessageDelivered
                | TraceAction::MessageDropped
                | TraceAction::ElectionTimerReset
                | TraceAction::NodeCrashed
                | TraceAction::NodeRestarted
                | TraceAction::StalePersistenceDiscarded
                | TraceAction::SnapshotPublished
                | TraceAction::SnapshotTransferStarted
                | TraceAction::SnapshotTransferInterrupted
                | TraceAction::StorageFailed
                | TraceAction::TraceTerminated => {}
            }
        }
        Ok(())
    }

    pub fn validate_m4_abstraction(&self) -> Result<(), TraceError> {
        self.validate_m2_abstraction()?;
        let mut applied = BTreeMap::<u64, u64>::new();
        let mut snapshots = BTreeMap::<u64, (u64, u64)>::new();
        let mut transfers = BTreeMap::<u64, (u64, u64, u64)>::new();
        for record in &self.records {
            match record.action {
                TraceAction::EntriesApplied => {
                    applied.insert(
                        record.node,
                        required(record.index, record.sequence, "index")?,
                    );
                }
                TraceAction::SnapshotPublished => {
                    let index = required(record.index, record.sequence, "index")?;
                    let term = required(record.term, record.sequence, "term")?;
                    if index == 0 || term == 0 || index > *applied.entry(record.node).or_default() {
                        return Err(TraceError::InvalidSnapshotPublication(record.sequence));
                    }
                    if snapshots
                        .get(&record.node)
                        .is_some_and(|(previous, _)| index <= *previous)
                    {
                        return Err(TraceError::SnapshotRegression(record.sequence));
                    }
                    snapshots.insert(record.node, (index, term));
                }
                TraceAction::SnapshotTransferStarted => {
                    let peer = required(record.peer, record.sequence, "peer")?;
                    let index = required(record.index, record.sequence, "index")?;
                    let term = required(record.term, record.sequence, "term")?;
                    if snapshots.get(&record.node) != Some(&(index, term)) {
                        return Err(TraceError::UnknownSnapshotTransfer(record.sequence));
                    }
                    transfers.insert(peer, (record.node, index, term));
                }
                TraceAction::SnapshotTransferInterrupted => {
                    let peer = required(record.peer, record.sequence, "peer")?;
                    transfers.remove(&peer);
                }
                TraceAction::SnapshotInstalled => {
                    let source = required(record.peer, record.sequence, "peer")?;
                    let index = required(record.index, record.sequence, "index")?;
                    let term = required(record.term, record.sequence, "term")?;
                    if transfers.get(&record.node) != Some(&(source, index, term)) {
                        return Err(TraceError::InstallWithoutTransfer(record.sequence));
                    }
                    if snapshots
                        .get(&record.node)
                        .is_some_and(|(previous, _)| index < *previous)
                    {
                        return Err(TraceError::SnapshotRegression(record.sequence));
                    }
                    snapshots.insert(record.node, (index, term));
                    applied.insert(record.node, index);
                    transfers.remove(&record.node);
                }
                _ => {}
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
    if term > hard_term
        && !matches!(
            message,
            TraceMessage::PreVote | TraceMessage::PreVoteResponse
        )
    {
        return Err(TraceError::MessageBeforeHardState(record.sequence));
    }
    match message {
        TraceMessage::AppendEntries | TraceMessage::TimeoutNow => {
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
        TraceMessage::PreVote
        | TraceMessage::PreVoteResponse
        | TraceMessage::RequestVote
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
        TraceEvent::SnapshotPublished(node, position) => {
            record.action = TraceAction::SnapshotPublished;
            record.node = node.0;
            record.index = Some(position.index);
            record.term = Some(position.term.0);
        }
        TraceEvent::SnapshotTransferStarted(node, peer, position) => {
            record.action = TraceAction::SnapshotTransferStarted;
            record.node = node.0;
            record.peer = Some(peer.0);
            record.index = Some(position.index);
            record.term = Some(position.term.0);
        }
        TraceEvent::SnapshotTransferInterrupted(node, peer, position) => {
            record.action = TraceAction::SnapshotTransferInterrupted;
            record.node = node.0;
            record.peer = Some(peer.0);
            record.index = Some(position.index);
            record.term = Some(position.term.0);
        }
        TraceEvent::SnapshotInstalled(node, source, position) => {
            record.action = TraceAction::SnapshotInstalled;
            record.node = node.0;
            record.peer = Some(source.0);
            record.index = Some(position.index);
            record.term = Some(position.term.0);
        }
        TraceEvent::StorageFailed(node) => {
            record.action = TraceAction::StorageFailed;
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
        Message::PreVote {
            prospective_term,
            last_log,
            ..
        } => (
            TraceMessage::PreVote,
            prospective_term.0,
            Some(last_log.index),
            None,
        ),
        Message::PreVoteResponse {
            prospective_term,
            vote_granted,
            ..
        } => (
            TraceMessage::PreVoteResponse,
            prospective_term.0,
            None,
            Some(*vote_granted),
        ),
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
        Message::TimeoutNow {
            term, fence_index, ..
        } => (TraceMessage::TimeoutNow, term.0, Some(*fence_index), None),
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
    InvalidEncoding(String),
    Truncated,
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
    InvalidSnapshotPublication(u64),
    SnapshotRegression(u64),
    UnknownSnapshotTransfer(u64),
    InstallWithoutTransfer(u64),
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

    fn temporary_trace(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "raft-kv-{name}-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    fn trace_record(action: TraceAction) -> TraceRecord {
        TraceRecord {
            sequence: 0,
            action,
            node: 1,
            peer: None,
            term: None,
            index: None,
            message: None,
            accepted: None,
        }
    }

    fn trace(mut records: Vec<TraceRecord>) -> ProtocolTrace {
        for (sequence, record) in records.iter_mut().enumerate() {
            record.sequence = u64::try_from(sequence).expect("bounded sequence");
        }
        ProtocolTrace {
            schema_version: TRACE_SCHEMA_VERSION,
            records,
        }
    }

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

    #[test]
    fn m2_validator_rejects_each_persistence_order_violation() {
        let mut unsupported = trace(Vec::new());
        unsupported.schema_version += 1;
        assert!(matches!(
            unsupported.validate_m2_abstraction(),
            Err(TraceError::UnsupportedSchema(_))
        ));

        let mut non_contiguous = trace(vec![trace_record(TraceAction::ElectionTimeout)]);
        non_contiguous.records[0].sequence = 7;
        assert!(matches!(
            non_contiguous.validate_m2_abstraction(),
            Err(TraceError::NonContiguousSequence {
                expected: 0,
                found: 7
            })
        ));

        let missing_term = trace(vec![trace_record(TraceAction::HardStatePersisted)]);
        assert!(matches!(
            missing_term.validate_m2_abstraction(),
            Err(TraceError::MissingField { field: "term", .. })
        ));

        let mut hard_two = trace_record(TraceAction::HardStatePersisted);
        hard_two.term = Some(2);
        let mut hard_one = hard_two.clone();
        hard_one.term = Some(1);
        assert!(matches!(
            trace(vec![hard_two.clone(), hard_one]).validate_m2_abstraction(),
            Err(TraceError::TermRegression(1))
        ));

        let mut log_two = trace_record(TraceAction::LogPersisted);
        log_two.index = Some(2);
        let mut commit_two = trace_record(TraceAction::CommitAdvanced);
        commit_two.index = Some(2);
        let mut log_one = log_two.clone();
        log_one.index = Some(1);
        assert!(matches!(
            trace(vec![log_two.clone(), commit_two.clone(), log_one]).validate_m2_abstraction(),
            Err(TraceError::DurableLogBelowCommit(2))
        ));

        let mut commit_one = commit_two.clone();
        commit_one.index = Some(1);
        assert!(matches!(
            trace(vec![log_two.clone(), commit_two.clone(), commit_one]).validate_m2_abstraction(),
            Err(TraceError::CommitRegression(2))
        ));
        assert!(matches!(
            trace(vec![commit_two.clone()]).validate_m2_abstraction(),
            Err(TraceError::CommitBeyondDurableLog(0))
        ));

        let mut applied_two = trace_record(TraceAction::EntriesApplied);
        applied_two.index = Some(2);
        assert!(matches!(
            trace(vec![applied_two.clone()]).validate_m2_abstraction(),
            Err(TraceError::ApplicationBeyondCommit(0))
        ));
        let mut applied_one = applied_two.clone();
        applied_one.index = Some(1);
        assert!(matches!(
            trace(vec![log_two, commit_two, applied_two, applied_one]).validate_m2_abstraction(),
            Err(TraceError::ApplicationRegression(3))
        ));
    }

    #[test]
    fn m2_validator_binds_every_sent_message_to_durable_state() {
        let sent = trace_record(TraceAction::MessageSent);
        assert!(matches!(
            trace(vec![sent.clone()]).validate_m2_abstraction(),
            Err(TraceError::MissingField {
                field: "message",
                ..
            })
        ));

        let mut vote = sent.clone();
        vote.message = Some(TraceMessage::RequestVote);
        vote.term = Some(1);
        assert!(matches!(
            trace(vec![vote]).validate_m2_abstraction(),
            Err(TraceError::MessageBeforeHardState(0))
        ));

        let mut hard = trace_record(TraceAction::HardStatePersisted);
        hard.term = Some(1);
        let mut append = sent.clone();
        append.message = Some(TraceMessage::AppendEntries);
        append.term = Some(1);
        append.index = Some(1);
        assert!(matches!(
            trace(vec![hard.clone(), append]).validate_m2_abstraction(),
            Err(TraceError::AppendBeforeDurableLog(1))
        ));

        let mut rejected = sent;
        rejected.message = Some(TraceMessage::AppendEntriesResponse);
        rejected.term = Some(1);
        rejected.index = Some(9);
        rejected.accepted = Some(false);
        trace(vec![hard, rejected])
            .validate_m2_abstraction()
            .expect("a rejection does not acknowledge a durable index");
    }

    #[test]
    fn m4_validator_rejects_each_snapshot_causality_violation() {
        let mut applied = trace_record(TraceAction::EntriesApplied);
        applied.index = Some(2);
        let mut durable = trace_record(TraceAction::LogPersisted);
        durable.index = Some(2);
        let mut committed = trace_record(TraceAction::CommitAdvanced);
        committed.index = Some(2);
        let prefix = [durable, committed, applied];

        for (index, term) in [(0, 1), (1, 0), (3, 1)] {
            let mut published = trace_record(TraceAction::SnapshotPublished);
            published.index = Some(index);
            published.term = Some(term);
            assert!(matches!(
                trace(prefix.iter().cloned().chain([published]).collect())
                    .validate_m4_abstraction(),
                Err(TraceError::InvalidSnapshotPublication(3))
            ));
        }

        let mut published_two = trace_record(TraceAction::SnapshotPublished);
        published_two.index = Some(2);
        published_two.term = Some(1);
        let mut published_one = published_two.clone();
        published_one.index = Some(1);
        assert!(matches!(
            trace(
                prefix
                    .iter()
                    .cloned()
                    .chain([published_two.clone(), published_one])
                    .collect()
            )
            .validate_m4_abstraction(),
            Err(TraceError::SnapshotRegression(4))
        ));

        let mut transfer = trace_record(TraceAction::SnapshotTransferStarted);
        transfer.peer = Some(2);
        transfer.index = Some(1);
        transfer.term = Some(1);
        assert!(matches!(
            trace(vec![transfer]).validate_m4_abstraction(),
            Err(TraceError::UnknownSnapshotTransfer(0))
        ));

        let mut install = trace_record(TraceAction::SnapshotInstalled);
        install.node = 2;
        install.peer = Some(1);
        install.index = Some(2);
        install.term = Some(1);
        assert!(matches!(
            trace(vec![install]).validate_m4_abstraction(),
            Err(TraceError::InstallWithoutTransfer(0))
        ));
    }

    #[test]
    fn real_process_json_lines_are_flushed_and_validated() {
        let path = temporary_trace("recorder");
        let recorder = ProtocolTraceRecorder::create(&path).expect("create trace");
        recorder
            .observe(1, 1, 3, 2, 2, 1, 1)
            .expect("observe state");
        recorder
            .observe(1, 1, 4, 3, 3, 2, 1)
            .expect("observe progress");
        drop(recorder);

        let trace = ProtocolTrace::from_json_lines(&path).expect("load trace");
        trace.validate_m4_abstraction().expect("valid trace");
        assert_eq!(trace.records.first().map(|record| record.sequence), Some(0));
        assert!(
            trace
                .records
                .iter()
                .all(|record| record.peer.is_none() && record.message.is_none())
        );
        std::fs::remove_file(path).expect("remove trace");
    }

    #[test]
    fn truncated_json_lines_are_rejected() {
        let path = temporary_trace("truncated");
        std::fs::write(
            &path,
            format!(
                "{{\"schema_version\":{},\"format\":\"raft-kv-jsonl\"}}\n{{",
                TRACE_SCHEMA_VERSION
            ),
        )
        .expect("write malformed trace");
        assert!(matches!(
            ProtocolTrace::from_json_lines(&path),
            Err(TraceError::InvalidEncoding(_))
        ));
        std::fs::remove_file(path).expect("remove trace");
    }

    #[test]
    fn valid_prefix_without_terminal_record_is_rejected_as_truncated() {
        let path = temporary_trace("valid-prefix");
        std::fs::write(
            &path,
            format!(
                "{{\"schema_version\":{},\"format\":\"raft-kv-jsonl\"}}\n\
                 {{\"sequence\":0,\"action\":\"hard_state_persisted\",\"node\":1,\"term\":1}}\n",
                TRACE_SCHEMA_VERSION
            ),
        )
        .expect("write valid prefix");
        assert_eq!(
            ProtocolTrace::from_json_lines(&path),
            Err(TraceError::Truncated)
        );
        std::fs::remove_file(path).expect("remove trace");
    }
}
