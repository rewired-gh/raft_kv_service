use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display, Formatter},
};

use super::LogEntry;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Term(pub u64);

impl Term {
    fn checked_next(self) -> Result<Self, CoreError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(CoreError::TermExhausted)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogPosition {
    pub index: u64,
    pub term: Term,
}

impl LogPosition {
    fn is_at_least_as_up_to_date_as(self, other: Self) -> bool {
        (self.term, self.index) >= (other.term, other.index)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VoterSet(BTreeSet<NodeId>);

impl VoterSet {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Result<Self, CoreError> {
        let mut unique = BTreeSet::new();
        for voter in voters {
            if !unique.insert(voter) {
                return Err(CoreError::DuplicateVoter(voter));
            }
        }
        let voters = unique;
        if voters.is_empty() {
            return Err(CoreError::EmptyVoterSet);
        }
        Ok(Self(voters))
    }

    pub fn contains(&self, node: NodeId) -> bool {
        self.0.contains(&node)
    }

    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.0.iter().copied()
    }

    fn has_quorum(&self, votes: &BTreeSet<NodeId>) -> bool {
        let granted = votes.intersection(&self.0).count();
        granted > self.0.len() / 2
    }

    fn peers(&self, local: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.0.iter().copied().filter(move |node| *node != local)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate: NodeId,
        last_log: LogPosition,
    },
    RequestVoteResponse {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader: NodeId,
        previous: LogPosition,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: Term,
        accepted: bool,
        match_index: u64,
        request_previous_index: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    ElectionTimeout,
    Receive(Envelope),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    PersistHardState(HardState),
    PersistLog(LogMutation),
    Send(Envelope),
    ResetElectionTimer,
    CommitIndexAdvanced(u64),
    ApplyCommitted(Vec<LogEntry>),
    SendSnapshot {
        to: NodeId,
        last_included: LogPosition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogMutation {
    pub previous: LogPosition,
    pub entries: Vec<LogEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoreError {
    EmptyVoterSet,
    DuplicateVoter(NodeId),
    LocalNodeIsNotVoter(NodeId),
    InvalidRecoveredVote(NodeId),
    PersistencePending,
    NoPersistencePending,
    PersistenceAcknowledgementMismatch {
        expected: HardState,
        observed: HardState,
    },
    LogPersistenceAcknowledgementMismatch,
    NoApplicationPending,
    ApplicationAcknowledgementMismatch {
        expected: u64,
        observed: u64,
    },
    InvalidAppendEntries {
        expected: u64,
        found: u64,
    },
    InvalidAppendEntriesTerm {
        index: u64,
    },
    InvalidAppendResponse {
        match_index: u64,
        last_index: u64,
    },
    CommittedLogConflict {
        index: u64,
        commit_index: u64,
    },
    NotLeader,
    LeaderNotReady,
    InvalidRecoveredLog {
        expected: u64,
        found: u64,
    },
    InvalidRecoveredLogTerm {
        index: u64,
    },
    InvalidRecoveredApplied {
        last_applied: u64,
        last_log: u64,
    },
    InvalidSnapshotPosition(LogPosition),
    SnapshotBeyondApplied {
        snapshot_index: u64,
        last_applied: u64,
    },
    StaleSnapshotTerm {
        received: Term,
        current: Term,
    },
    IndexExhausted,
    TermExhausted,
}

impl Display for CoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyVoterSet => formatter.write_str("voter set must not be empty"),
            Self::DuplicateVoter(node) => {
                write!(formatter, "duplicate voter {}", node.0)
            }
            Self::LocalNodeIsNotVoter(node) => {
                write!(formatter, "local node {} is not a voter", node.0)
            }
            Self::InvalidRecoveredVote(node) => {
                write!(formatter, "recovered vote references non-voter {}", node.0)
            }
            Self::PersistencePending => formatter.write_str("durable state persistence is pending"),
            Self::NoPersistencePending => {
                formatter.write_str("no durable state persistence is pending")
            }
            Self::PersistenceAcknowledgementMismatch { expected, observed } => write!(
                formatter,
                "persistence acknowledgement mismatch: expected {expected:?}, observed {observed:?}"
            ),
            Self::LogPersistenceAcknowledgementMismatch => {
                formatter.write_str("log persistence acknowledgement mismatch")
            }
            Self::NoApplicationPending => {
                formatter.write_str("no committed application is pending")
            }
            Self::ApplicationAcknowledgementMismatch { expected, observed } => write!(
                formatter,
                "application acknowledgement mismatch: expected index {expected}, observed {observed}"
            ),
            Self::InvalidAppendEntries { expected, found } => write!(
                formatter,
                "invalid AppendEntries sequence: expected index {expected}, found {found}"
            ),
            Self::InvalidAppendEntriesTerm { index } => {
                write!(formatter, "AppendEntries entry {index} has term zero")
            }
            Self::InvalidAppendResponse {
                match_index,
                last_index,
            } => write!(
                formatter,
                "invalid AppendEntries response match index {match_index}; leader last index is {last_index}"
            ),
            Self::CommittedLogConflict {
                index,
                commit_index,
            } => write!(
                formatter,
                "AppendEntries would replace committed index {index} at commit index {commit_index}"
            ),
            Self::NotLeader => formatter.write_str("node is not the Raft leader"),
            Self::LeaderNotReady => {
                formatter.write_str("leader has not committed its current-term no-op")
            }
            Self::InvalidRecoveredLog { expected, found } => write!(
                formatter,
                "invalid recovered log: expected index {expected}, found {found}"
            ),
            Self::InvalidRecoveredLogTerm { index } => {
                write!(formatter, "recovered log entry {index} has term zero")
            }
            Self::InvalidRecoveredApplied {
                last_applied,
                last_log,
            } => write!(
                formatter,
                "recovered last applied index {last_applied} exceeds log index {last_log}"
            ),
            Self::InvalidSnapshotPosition(position) => write!(
                formatter,
                "invalid snapshot position {} in term {}",
                position.index, position.term.0
            ),
            Self::SnapshotBeyondApplied {
                snapshot_index,
                last_applied,
            } => write!(
                formatter,
                "snapshot index {snapshot_index} exceeds last applied index {last_applied}"
            ),
            Self::StaleSnapshotTerm { received, current } => write!(
                formatter,
                "snapshot term {} is older than current term {}",
                received.0, current.0
            ),
            Self::IndexExhausted => formatter.write_str("Raft log index exhausted"),
            Self::TermExhausted => formatter.write_str("Raft term exhausted"),
        }
    }
}

impl Error for CoreError {}

#[derive(Clone, Debug)]
struct PendingPersistence {
    hard_state: HardState,
    role: Role,
    leader: Option<NodeId>,
    granted_votes: BTreeSet<NodeId>,
    after_persist: Vec<Effect>,
}

#[derive(Clone, Debug)]
struct AppendRequest {
    sender: NodeId,
    term: Term,
    leader: NodeId,
    previous: LogPosition,
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

#[derive(Clone, Debug)]
struct PendingLogPersistence {
    mutation: LogMutation,
    continuation: PendingLogContinuation,
}

#[derive(Clone, Debug)]
enum PendingLogContinuation {
    FollowerAppend(AppendRequest),
    LeaderEntry,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicationProgress {
    pub next_index: u64,
    pub match_index: u64,
}

#[derive(Clone, Debug)]
pub struct RaftCore {
    id: NodeId,
    voters: VoterSet,
    hard_state: HardState,
    role: Role,
    leader: Option<NodeId>,
    snapshot_position: LogPosition,
    log: Vec<LogEntry>,
    granted_votes: BTreeSet<NodeId>,
    pending: Option<PendingPersistence>,
    append_after_hard_state: Option<AppendRequest>,
    pending_log: Option<PendingLogPersistence>,
    commit_index: u64,
    replication: BTreeMap<NodeId, ReplicationProgress>,
    leader_noop_index: Option<u64>,
    last_applied: u64,
    pending_application: Option<Vec<LogEntry>>,
}

impl RaftCore {
    pub fn new(
        id: NodeId,
        voters: VoterSet,
        hard_state: HardState,
        log: Vec<LogEntry>,
    ) -> Result<Self, CoreError> {
        Self::recover(id, voters, hard_state, log, 0)
    }

    pub fn recover(
        id: NodeId,
        voters: VoterSet,
        hard_state: HardState,
        log: Vec<LogEntry>,
        last_applied: u64,
    ) -> Result<Self, CoreError> {
        Self::recover_compacted(
            id,
            voters,
            hard_state,
            LogPosition::default(),
            log,
            last_applied,
        )
    }

    pub fn recover_compacted(
        id: NodeId,
        voters: VoterSet,
        hard_state: HardState,
        snapshot_position: LogPosition,
        log: Vec<LogEntry>,
        last_applied: u64,
    ) -> Result<Self, CoreError> {
        if !voters.contains(id) {
            return Err(CoreError::LocalNodeIsNotVoter(id));
        }
        if let Some(voted_for) = hard_state.voted_for
            && !voters.contains(voted_for)
        {
            return Err(CoreError::InvalidRecoveredVote(voted_for));
        }
        if (snapshot_position.index == 0) != (snapshot_position.term == Term(0)) {
            return Err(CoreError::InvalidSnapshotPosition(snapshot_position));
        }
        validate_recovered_log(snapshot_position.index, &log)?;
        let last_log = log
            .last()
            .map_or(snapshot_position.index, |entry| entry.index);
        if last_applied < snapshot_position.index || last_applied > last_log {
            return Err(CoreError::InvalidRecoveredApplied {
                last_applied,
                last_log,
            });
        }
        Ok(Self {
            id,
            voters,
            hard_state,
            role: Role::Follower,
            leader: None,
            snapshot_position,
            log,
            granted_votes: BTreeSet::new(),
            pending: None,
            append_after_hard_state: None,
            pending_log: None,
            commit_index: last_applied,
            replication: BTreeMap::new(),
            leader_noop_index: None,
            last_applied,
            pending_application: None,
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn voters(&self) -> &VoterSet {
        &self.voters
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn hard_state(&self) -> HardState {
        self.hard_state
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn last_log_position(&self) -> LogPosition {
        self.log
            .last()
            .map_or(self.snapshot_position, |entry| LogPosition {
                index: entry.index,
                term: entry.term,
            })
    }

    pub fn snapshot_position(&self) -> LogPosition {
        self.snapshot_position
    }

    pub fn compact_through(&mut self, position: LogPosition) -> Result<(), CoreError> {
        if position.index < self.snapshot_position.index
            || self.term_at(position.index) != Some(position.term)
        {
            return Err(CoreError::InvalidSnapshotPosition(position));
        }
        if position.index > self.last_applied {
            return Err(CoreError::SnapshotBeyondApplied {
                snapshot_index: position.index,
                last_applied: self.last_applied,
            });
        }
        let remove = usize::try_from(position.index - self.snapshot_position.index)
            .map_err(|_| CoreError::IndexExhausted)?;
        self.log.drain(..remove);
        self.snapshot_position = position;
        Ok(())
    }

    pub fn observe_snapshot_leader(
        &mut self,
        term: Term,
        leader: NodeId,
    ) -> Result<Vec<Effect>, CoreError> {
        if !self.voters.contains(leader) {
            return Err(CoreError::LocalNodeIsNotVoter(leader));
        }
        if term < self.hard_state.current_term {
            return Err(CoreError::StaleSnapshotTerm {
                received: term,
                current: self.hard_state.current_term,
            });
        }
        if term > self.hard_state.current_term {
            return self.begin_persistence(PendingPersistence {
                hard_state: HardState {
                    current_term: term,
                    voted_for: None,
                },
                role: Role::Follower,
                leader: Some(leader),
                granted_votes: BTreeSet::new(),
                after_persist: vec![Effect::ResetElectionTimer],
            });
        }
        self.role = Role::Follower;
        self.leader = Some(leader);
        self.granted_votes.clear();
        Ok(vec![Effect::ResetElectionTimer])
    }

    pub fn restore_snapshot(
        &mut self,
        position: LogPosition,
        retained_suffix: Vec<LogEntry>,
    ) -> Result<(), CoreError> {
        if position.index < self.last_applied || position.term == Term(0) {
            return Err(CoreError::InvalidSnapshotPosition(position));
        }
        validate_recovered_log(position.index, &retained_suffix)?;
        self.snapshot_position = position;
        self.log = retained_suffix;
        self.commit_index = position.index;
        self.last_applied = position.index;
        self.pending_application = None;
        self.pending_log = None;
        Ok(())
    }

    pub fn persistence_pending(&self) -> bool {
        self.pending.is_some() || self.pending_log.is_some()
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    pub fn replication_progress(&self, peer: NodeId) -> Option<ReplicationProgress> {
        self.replication.get(&peer).copied()
    }

    pub fn leader_ready(&self) -> bool {
        self.role == Role::Leader
            && self
                .leader_noop_index
                .is_some_and(|index| self.commit_index >= index)
    }

    pub fn handle(&mut self, event: Event) -> Result<Vec<Effect>, CoreError> {
        if self.persistence_pending() {
            return Err(CoreError::PersistencePending);
        }

        match event {
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::Receive(envelope) if envelope.to == self.id => self.handle_message(envelope),
            Event::Receive(_) => Ok(Vec::new()),
        }
    }

    pub fn propose(
        &mut self,
        payload: super::EntryPayload,
    ) -> Result<(u64, Vec<Effect>), CoreError> {
        if self.persistence_pending() {
            return Err(CoreError::PersistencePending);
        }
        if self.role != Role::Leader {
            return Err(CoreError::NotLeader);
        }
        if !self.leader_ready() {
            return Err(CoreError::LeaderNotReady);
        }

        let previous = self.last_log_position();
        let index = previous
            .index
            .checked_add(1)
            .ok_or(CoreError::IndexExhausted)?;
        let mutation = LogMutation {
            previous,
            entries: vec![LogEntry {
                index,
                term: self.hard_state.current_term,
                payload,
            }],
        };
        self.pending_log = Some(PendingLogPersistence {
            mutation: mutation.clone(),
            continuation: PendingLogContinuation::LeaderEntry,
        });
        Ok((index, vec![Effect::PersistLog(mutation)]))
    }

    pub fn heartbeat(&self) -> Result<Vec<Effect>, CoreError> {
        if self.persistence_pending() {
            return Err(CoreError::PersistencePending);
        }
        if self.role != Role::Leader {
            return Err(CoreError::NotLeader);
        }
        let mut effects = Vec::new();
        for peer in self.voters.peers(self.id) {
            effects.push(self.append_for_peer(peer)?);
        }
        Ok(effects)
    }

    pub fn acknowledge_persisted(&mut self, observed: HardState) -> Result<Vec<Effect>, CoreError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or(CoreError::NoPersistencePending)?;
        if pending.hard_state != observed {
            return Err(CoreError::PersistenceAcknowledgementMismatch {
                expected: pending.hard_state,
                observed,
            });
        }

        let pending = self.pending.take().ok_or(CoreError::NoPersistencePending)?;
        self.hard_state = pending.hard_state;
        self.role = pending.role;
        self.leader = pending.leader;
        self.granted_votes = pending.granted_votes;

        if let Some(request) = self.append_after_hard_state.take() {
            return self.handle_current_term_append(request);
        }

        let mut effects = pending.after_persist;
        self.promote_if_quorum(&mut effects)?;
        Ok(effects)
    }

    pub fn acknowledge_log_persisted(
        &mut self,
        observed: LogMutation,
    ) -> Result<Vec<Effect>, CoreError> {
        let pending = self
            .pending_log
            .as_ref()
            .ok_or(CoreError::NoPersistencePending)?;
        if pending.mutation != observed {
            return Err(CoreError::LogPersistenceAcknowledgementMismatch);
        }
        let pending = self
            .pending_log
            .take()
            .ok_or(CoreError::NoPersistencePending)?;
        reconcile_log(
            self.snapshot_position.index,
            &mut self.log,
            &pending.mutation.entries,
        )?;
        match pending.continuation {
            PendingLogContinuation::FollowerAppend(request) => Ok(self.finish_append(request)),
            PendingLogContinuation::LeaderEntry => self.finish_leader_entry(),
        }
    }

    pub fn acknowledge_applied(&mut self, observed: u64) -> Result<Vec<Effect>, CoreError> {
        let expected = self
            .pending_application
            .as_ref()
            .and_then(|entries| entries.last())
            .map(|entry| entry.index)
            .ok_or(CoreError::NoApplicationPending)?;
        if observed != expected {
            return Err(CoreError::ApplicationAcknowledgementMismatch { expected, observed });
        }
        self.pending_application = None;
        self.last_applied = observed;
        let mut effects = Vec::new();
        self.emit_committed_application(&mut effects);
        Ok(effects)
    }

    fn handle_election_timeout(&mut self) -> Result<Vec<Effect>, CoreError> {
        if self.role == Role::Leader {
            return Ok(Vec::new());
        }

        let next_term = self.hard_state.current_term.checked_next()?;
        let hard_state = HardState {
            current_term: next_term,
            voted_for: Some(self.id),
        };
        let mut granted_votes = BTreeSet::new();
        granted_votes.insert(self.id);

        let mut after_persist = vec![Effect::ResetElectionTimer];
        after_persist.extend(self.voters.peers(self.id).map(|peer| {
            Effect::Send(Envelope {
                from: self.id,
                to: peer,
                message: Message::RequestVote {
                    term: next_term,
                    candidate: self.id,
                    last_log: self.last_log_position(),
                },
            })
        }));

        self.begin_persistence(PendingPersistence {
            hard_state,
            role: Role::Candidate,
            leader: None,
            granted_votes,
            after_persist,
        })
    }

    fn handle_message(&mut self, envelope: Envelope) -> Result<Vec<Effect>, CoreError> {
        if !self.voters.contains(envelope.from) {
            return Ok(Vec::new());
        }
        match envelope.message {
            Message::RequestVote {
                term,
                candidate,
                last_log,
            } => self.handle_request_vote(envelope.from, term, candidate, last_log),
            Message::RequestVoteResponse { term, vote_granted } => {
                self.handle_request_vote_response(envelope.from, term, vote_granted)
            }
            Message::AppendEntries {
                term,
                leader,
                previous,
                entries,
                leader_commit,
            } => self.handle_append_entries(AppendRequest {
                sender: envelope.from,
                term,
                leader,
                previous,
                entries,
                leader_commit,
            }),
            Message::AppendEntriesResponse {
                term,
                accepted,
                match_index,
                request_previous_index,
            } => self.handle_append_response(
                envelope.from,
                term,
                accepted,
                match_index,
                request_previous_index,
            ),
        }
    }

    fn handle_request_vote(
        &mut self,
        sender: NodeId,
        term: Term,
        candidate: NodeId,
        candidate_last_log: LogPosition,
    ) -> Result<Vec<Effect>, CoreError> {
        if sender != candidate || term < self.hard_state.current_term {
            return Ok(vec![self.vote_response(sender, false)]);
        }

        let log_is_eligible =
            candidate_last_log.is_at_least_as_up_to_date_as(self.last_log_position());
        if term > self.hard_state.current_term {
            let vote_granted = log_is_eligible;
            let hard_state = HardState {
                current_term: term,
                voted_for: vote_granted.then_some(candidate),
            };
            let mut after_persist = Vec::with_capacity(2);
            if vote_granted {
                after_persist.push(Effect::ResetElectionTimer);
            }
            after_persist.push(Effect::Send(Envelope {
                from: self.id,
                to: sender,
                message: Message::RequestVoteResponse { term, vote_granted },
            }));
            return self.begin_persistence(PendingPersistence {
                hard_state,
                role: Role::Follower,
                leader: None,
                granted_votes: BTreeSet::new(),
                after_persist,
            });
        }

        let vote_granted = log_is_eligible
            && (self.hard_state.voted_for.is_none()
                || self.hard_state.voted_for == Some(candidate));
        if vote_granted && self.hard_state.voted_for.is_none() {
            let hard_state = HardState {
                current_term: term,
                voted_for: Some(candidate),
            };
            return self.begin_persistence(PendingPersistence {
                hard_state,
                role: Role::Follower,
                leader: None,
                granted_votes: BTreeSet::new(),
                after_persist: vec![
                    Effect::ResetElectionTimer,
                    Effect::Send(Envelope {
                        from: self.id,
                        to: sender,
                        message: Message::RequestVoteResponse {
                            term,
                            vote_granted: true,
                        },
                    }),
                ],
            });
        }

        let mut effects = Vec::with_capacity(2);
        if vote_granted {
            effects.push(Effect::ResetElectionTimer);
        }
        effects.push(self.vote_response(sender, vote_granted));
        Ok(effects)
    }

    fn handle_request_vote_response(
        &mut self,
        voter: NodeId,
        term: Term,
        vote_granted: bool,
    ) -> Result<Vec<Effect>, CoreError> {
        if term > self.hard_state.current_term {
            return self.persist_higher_term(term);
        }
        if term < self.hard_state.current_term || self.role != Role::Candidate || !vote_granted {
            return Ok(Vec::new());
        }

        self.granted_votes.insert(voter);
        let mut effects = Vec::new();
        self.promote_if_quorum(&mut effects)?;
        Ok(effects)
    }

    fn handle_append_entries(&mut self, request: AppendRequest) -> Result<Vec<Effect>, CoreError> {
        if request.sender != request.leader || request.term < self.hard_state.current_term {
            return Ok(vec![self.append_response(
                request.sender,
                false,
                request.previous.index,
            )]);
        }
        validate_append_entries(request.previous.index, &request.entries)?;

        if request.term > self.hard_state.current_term {
            self.append_after_hard_state = Some(request.clone());
            return self.begin_persistence(PendingPersistence {
                hard_state: HardState {
                    current_term: request.term,
                    voted_for: None,
                },
                role: Role::Follower,
                leader: Some(request.leader),
                granted_votes: BTreeSet::new(),
                after_persist: Vec::new(),
            });
        }

        self.handle_current_term_append(request)
    }

    fn handle_current_term_append(
        &mut self,
        request: AppendRequest,
    ) -> Result<Vec<Effect>, CoreError> {
        self.role = Role::Follower;
        self.leader = Some(request.leader);
        self.granted_votes.clear();

        let mut effects = vec![Effect::ResetElectionTimer];
        if self.term_at(request.previous.index) != Some(request.previous.term) {
            effects.push(self.append_response(request.sender, false, request.previous.index));
            return Ok(effects);
        }

        if let Some(conflict_index) =
            first_log_difference(self.snapshot_position.index, &self.log, &request.entries)
            && conflict_index <= self.commit_index
        {
            return Err(CoreError::CommittedLogConflict {
                index: conflict_index,
                commit_index: self.commit_index,
            });
        }

        if log_requires_update(self.snapshot_position.index, &self.log, &request.entries) {
            let mutation = LogMutation {
                previous: request.previous,
                entries: request.entries.clone(),
            };
            self.pending_log = Some(PendingLogPersistence {
                mutation: mutation.clone(),
                continuation: PendingLogContinuation::FollowerAppend(request),
            });
            effects.push(Effect::PersistLog(mutation));
            return Ok(effects);
        }

        effects.extend(self.finish_append(request));
        Ok(effects)
    }

    fn finish_append(&mut self, request: AppendRequest) -> Vec<Effect> {
        let match_index = request
            .entries
            .last()
            .map_or(request.previous.index, |entry| entry.index);
        let mut effects = Vec::with_capacity(2);
        let next_commit = request.leader_commit.min(match_index);
        if next_commit > self.commit_index {
            self.commit_index = next_commit;
            effects.push(Effect::CommitIndexAdvanced(next_commit));
            self.emit_committed_application(&mut effects);
        }
        effects.push(Effect::Send(Envelope {
            from: self.id,
            to: request.sender,
            message: Message::AppendEntriesResponse {
                term: self.hard_state.current_term,
                accepted: true,
                match_index,
                request_previous_index: request.previous.index,
            },
        }));
        effects
    }

    fn handle_append_response(
        &mut self,
        peer: NodeId,
        term: Term,
        accepted: bool,
        match_index: u64,
        request_previous_index: u64,
    ) -> Result<Vec<Effect>, CoreError> {
        if term > self.hard_state.current_term {
            return self.persist_higher_term(term);
        }
        if term < self.hard_state.current_term || self.role != Role::Leader {
            return Ok(Vec::new());
        }

        let last_index = self.last_log_position().index;
        if accepted && (match_index < request_previous_index || match_index > last_index) {
            return Err(CoreError::InvalidAppendResponse {
                match_index,
                last_index,
            });
        }

        let mut retry = false;
        {
            let Some(progress) = self.replication.get_mut(&peer) else {
                return Ok(Vec::new());
            };
            if accepted {
                progress.match_index = progress.match_index.max(match_index);
                progress.next_index = progress.next_index.max(
                    match_index
                        .checked_add(1)
                        .ok_or(CoreError::IndexExhausted)?,
                );
            } else if request_previous_index.checked_add(1) == Some(progress.next_index)
                && progress.match_index < progress.next_index
            {
                progress.next_index = progress
                    .next_index
                    .saturating_sub(1)
                    .max(progress.match_index.saturating_add(1))
                    .max(1);
                retry = true;
            }
        }

        let mut effects = Vec::new();
        let previous_commit = self.commit_index;
        self.advance_leader_commit(&mut effects);
        if retry {
            effects.push(self.append_for_peer(peer)?);
        }
        if self.commit_index > previous_commit {
            let peers = self.voters.peers(self.id).collect::<Vec<_>>();
            for voter in peers {
                effects.push(self.append_for_peer(voter)?);
            }
        }
        Ok(effects)
    }

    fn persist_higher_term(&mut self, term: Term) -> Result<Vec<Effect>, CoreError> {
        self.begin_persistence(PendingPersistence {
            hard_state: HardState {
                current_term: term,
                voted_for: None,
            },
            role: Role::Follower,
            leader: None,
            granted_votes: BTreeSet::new(),
            after_persist: Vec::new(),
        })
    }

    fn begin_persistence(&mut self, pending: PendingPersistence) -> Result<Vec<Effect>, CoreError> {
        if self.persistence_pending() {
            return Err(CoreError::PersistencePending);
        }
        let hard_state = pending.hard_state;
        self.pending = Some(pending);
        Ok(vec![Effect::PersistHardState(hard_state)])
    }

    fn promote_if_quorum(&mut self, effects: &mut Vec<Effect>) -> Result<(), CoreError> {
        if self.role != Role::Candidate || !self.voters.has_quorum(&self.granted_votes) {
            return Ok(());
        }

        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.granted_votes.clear();
        let previous = self.last_log_position();
        let noop_index = previous
            .index
            .checked_add(1)
            .ok_or(CoreError::IndexExhausted)?;
        self.replication = self
            .voters
            .peers(self.id)
            .map(|peer| {
                (
                    peer,
                    ReplicationProgress {
                        next_index: noop_index,
                        match_index: 0,
                    },
                )
            })
            .collect();
        self.leader_noop_index = Some(noop_index);
        let mutation = LogMutation {
            previous,
            entries: vec![LogEntry {
                index: noop_index,
                term: self.hard_state.current_term,
                payload: super::EntryPayload::Noop,
            }],
        };
        self.pending_log = Some(PendingLogPersistence {
            mutation: mutation.clone(),
            continuation: PendingLogContinuation::LeaderEntry,
        });
        effects.push(Effect::PersistLog(mutation));
        Ok(())
    }

    fn finish_leader_entry(&mut self) -> Result<Vec<Effect>, CoreError> {
        let mut effects = Vec::new();
        self.advance_leader_commit(&mut effects);
        let peers = self.voters.peers(self.id).collect::<Vec<_>>();
        for peer in peers {
            effects.push(self.append_for_peer(peer)?);
        }
        Ok(effects)
    }

    fn append_for_peer(&self, peer: NodeId) -> Result<Effect, CoreError> {
        let progress = self
            .replication
            .get(&peer)
            .ok_or(CoreError::LocalNodeIsNotVoter(peer))?;
        if progress.next_index <= self.snapshot_position.index {
            return Ok(Effect::SendSnapshot {
                to: peer,
                last_included: self.snapshot_position,
            });
        }
        let previous_index = progress
            .next_index
            .checked_sub(1)
            .ok_or(CoreError::IndexExhausted)?;
        let previous_term = self
            .term_at(previous_index)
            .ok_or(CoreError::InvalidRecoveredLog {
                expected: previous_index,
                found: self.last_log_position().index,
            })?;
        let start = usize::try_from(
            progress
                .next_index
                .checked_sub(self.snapshot_position.index)
                .and_then(|offset| offset.checked_sub(1))
                .ok_or(CoreError::IndexExhausted)?,
        )
        .map_err(|_| CoreError::IndexExhausted)?;
        let entries = self.log.get(start..).unwrap_or_default().to_vec();
        Ok(Effect::Send(Envelope {
            from: self.id,
            to: peer,
            message: Message::AppendEntries {
                term: self.hard_state.current_term,
                leader: self.id,
                previous: LogPosition {
                    index: previous_index,
                    term: previous_term,
                },
                entries,
                leader_commit: self.commit_index,
            },
        }))
    }

    fn advance_leader_commit(&mut self, effects: &mut Vec<Effect>) {
        let last = self.last_log_position().index;
        let mut candidate = last;
        while candidate > self.commit_index {
            if self.term_at(candidate) == Some(self.hard_state.current_term) {
                let replicated = 1 + self
                    .replication
                    .values()
                    .filter(|progress| progress.match_index >= candidate)
                    .count();
                if replicated > self.voters.0.len() / 2 {
                    self.commit_index = candidate;
                    effects.push(Effect::CommitIndexAdvanced(candidate));
                    self.emit_committed_application(effects);
                    break;
                }
            }
            candidate -= 1;
        }
    }

    fn emit_committed_application(&mut self, effects: &mut Vec<Effect>) {
        if self.pending_application.is_some() || self.last_applied >= self.commit_index {
            return;
        }
        let (Ok(start), Ok(end)) = (
            usize::try_from(self.last_applied - self.snapshot_position.index),
            usize::try_from(self.commit_index - self.snapshot_position.index),
        ) else {
            return;
        };
        let entries = self.log.get(start..end).unwrap_or_default().to_vec();
        if !entries.is_empty() {
            self.pending_application = Some(entries.clone());
            effects.push(Effect::ApplyCommitted(entries));
        }
    }

    fn vote_response(&self, recipient: NodeId, vote_granted: bool) -> Effect {
        Effect::Send(Envelope {
            from: self.id,
            to: recipient,
            message: Message::RequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted,
            },
        })
    }

    fn append_response(
        &self,
        recipient: NodeId,
        accepted: bool,
        request_previous_index: u64,
    ) -> Effect {
        Effect::Send(Envelope {
            from: self.id,
            to: recipient,
            message: Message::AppendEntriesResponse {
                term: self.hard_state.current_term,
                accepted,
                match_index: self.last_log_position().index,
                request_previous_index,
            },
        })
    }

    fn term_at(&self, index: u64) -> Option<Term> {
        if index == 0 {
            return Some(Term(0));
        }
        if index == self.snapshot_position.index {
            return Some(self.snapshot_position.term);
        }
        let position = usize::try_from(
            index
                .checked_sub(self.snapshot_position.index)?
                .checked_sub(1)?,
        )
        .ok()?;
        self.log.get(position).map(|entry| entry.term)
    }
}

fn validate_append_entries(previous: u64, entries: &[LogEntry]) -> Result<(), CoreError> {
    let mut expected = previous.checked_add(1).ok_or(CoreError::IndexExhausted)?;
    for entry in entries {
        if entry.index != expected {
            return Err(CoreError::InvalidAppendEntries {
                expected,
                found: entry.index,
            });
        }
        if entry.term == Term(0) {
            return Err(CoreError::InvalidAppendEntriesTerm { index: entry.index });
        }
        expected = expected.checked_add(1).ok_or(CoreError::IndexExhausted)?;
    }
    Ok(())
}

fn log_requires_update(base_index: u64, log: &[LogEntry], incoming: &[LogEntry]) -> bool {
    first_log_difference(base_index, log, incoming).is_some()
}

fn first_log_difference(base_index: u64, log: &[LogEntry], incoming: &[LogEntry]) -> Option<u64> {
    incoming.iter().find_map(|entry| {
        let Some(offset) = entry
            .index
            .checked_sub(base_index)
            .and_then(|offset| offset.checked_sub(1))
        else {
            return Some(entry.index);
        };
        let Ok(position) = usize::try_from(offset) else {
            return Some(entry.index);
        };
        (log.get(position) != Some(entry)).then_some(entry.index)
    })
}

fn reconcile_log(
    base_index: u64,
    log: &mut Vec<LogEntry>,
    incoming: &[LogEntry],
) -> Result<(), CoreError> {
    for (incoming_position, entry) in incoming.iter().enumerate() {
        let offset = entry
            .index
            .checked_sub(base_index)
            .and_then(|offset| offset.checked_sub(1))
            .ok_or(CoreError::IndexExhausted)?;
        let position = usize::try_from(offset).map_err(|_| CoreError::IndexExhausted)?;
        if log.get(position) == Some(entry) {
            continue;
        }
        log.truncate(position);
        log.extend_from_slice(&incoming[incoming_position..]);
        break;
    }
    Ok(())
}

fn validate_recovered_log(base_index: u64, log: &[LogEntry]) -> Result<(), CoreError> {
    let mut expected = base_index.checked_add(1).ok_or(CoreError::IndexExhausted)?;
    for entry in log {
        if entry.index != expected {
            return Err(CoreError::InvalidRecoveredLog {
                expected,
                found: entry.index,
            });
        }
        if entry.term == Term(0) {
            return Err(CoreError::InvalidRecoveredLogTerm { index: entry.index });
        }
        expected = expected.checked_add(1).ok_or(CoreError::TermExhausted)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::EntryPayload;

    fn voters(ids: &[u64]) -> VoterSet {
        VoterSet::new(ids.iter().copied().map(NodeId)).expect("voter set is valid")
    }

    fn core(id: u64, ids: &[u64]) -> RaftCore {
        RaftCore::new(NodeId(id), voters(ids), HardState::default(), Vec::new())
            .expect("core configuration is valid")
    }

    fn persist_from(effects: &[Effect]) -> HardState {
        match effects {
            [Effect::PersistHardState(hard_state)] => *hard_state,
            other => panic!("expected exactly one persistence effect, got {other:?}"),
        }
    }

    fn receive(from: u64, to: u64, message: Message) -> Event {
        Event::Receive(Envelope {
            from: NodeId(from),
            to: NodeId(to),
            message,
        })
    }

    fn append_request(
        term: u64,
        previous: LogPosition,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Message {
        Message::AppendEntries {
            term: Term(term),
            leader: NodeId(1),
            previous,
            entries,
            leader_commit,
        }
    }

    fn command_entry(index: u64, term: u64, byte: u8) -> LogEntry {
        LogEntry {
            index,
            term: Term(term),
            payload: EntryPayload::Command(vec![byte]),
        }
    }

    #[test]
    fn one_node_election_waits_for_durability_then_wins() {
        let mut node = core(1, &[1]);

        let effects = node
            .handle(Event::ElectionTimeout)
            .expect("timeout is accepted");
        let hard_state = persist_from(&effects);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.hard_state(), HardState::default());
        assert!(node.persistence_pending());

        let effects = node
            .acknowledge_persisted(hard_state)
            .expect("matching persistence is accepted");
        let mutation = match effects.as_slice() {
            [Effect::ResetElectionTimer, Effect::PersistLog(mutation)] => mutation.clone(),
            other => panic!("expected leader no-op persistence, got {other:?}"),
        };
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.leader(), Some(NodeId(1)));
        assert_eq!(node.hard_state().current_term, Term(1));
        assert!(!node.leader_ready());

        let effects = node
            .acknowledge_log_persisted(mutation)
            .expect("leader no-op persists");
        assert_eq!(
            effects,
            vec![
                Effect::CommitIndexAdvanced(1),
                Effect::ApplyCommitted(vec![LogEntry {
                    index: 1,
                    term: Term(1),
                    payload: EntryPayload::Noop,
                }]),
            ]
        );
        assert!(node.leader_ready());
        assert_eq!(node.last_applied(), 0);
        assert_eq!(
            node.acknowledge_applied(1)
                .expect("ordered application is acknowledged"),
            Vec::<Effect>::new()
        );
        assert_eq!(node.last_applied(), 1);
    }

    #[test]
    fn vote_is_not_observable_before_it_is_durable() {
        let mut node = core(2, &[1, 2, 3]);
        let request = Message::RequestVote {
            term: Term(4),
            candidate: NodeId(1),
            last_log: LogPosition::default(),
        };

        let effects = node
            .handle(receive(1, 2, request))
            .expect("request starts persistence");
        let hard_state = persist_from(&effects);
        assert_eq!(hard_state.voted_for, Some(NodeId(1)));
        assert_eq!(node.hard_state(), HardState::default());

        let effects = node
            .acknowledge_persisted(hard_state)
            .expect("persistence acknowledgement succeeds");
        assert_eq!(
            effects,
            vec![
                Effect::ResetElectionTimer,
                Effect::Send(Envelope {
                    from: NodeId(2),
                    to: NodeId(1),
                    message: Message::RequestVoteResponse {
                        term: Term(4),
                        vote_granted: true,
                    },
                }),
            ]
        );
    }

    #[test]
    fn higher_term_with_stale_log_updates_term_but_refuses_vote() {
        let mut node = RaftCore::new(
            NodeId(2),
            voters(&[1, 2, 3]),
            HardState {
                current_term: Term(2),
                voted_for: None,
            },
            vec![LogEntry {
                index: 1,
                term: Term(2),
                payload: EntryPayload::Noop,
            }],
        )
        .expect("core configuration is valid");

        let effects = node
            .handle(receive(
                1,
                2,
                Message::RequestVote {
                    term: Term(3),
                    candidate: NodeId(1),
                    last_log: LogPosition {
                        index: 100,
                        term: Term(1),
                    },
                },
            ))
            .expect("request starts term persistence");
        let hard_state = persist_from(&effects);
        assert_eq!(hard_state.voted_for, None);

        let effects = node
            .acknowledge_persisted(hard_state)
            .expect("persistence acknowledgement succeeds");
        assert_eq!(
            effects,
            vec![Effect::Send(Envelope {
                from: NodeId(2),
                to: NodeId(1),
                message: Message::RequestVoteResponse {
                    term: Term(3),
                    vote_granted: false,
                },
            })]
        );
    }

    #[test]
    fn node_never_votes_for_two_candidates_in_one_term() {
        let mut node = core(3, &[1, 2, 3]);
        let first = Message::RequestVote {
            term: Term(7),
            candidate: NodeId(1),
            last_log: LogPosition::default(),
        };
        let hard_state = persist_from(
            &node
                .handle(receive(1, 3, first))
                .expect("first vote starts persistence"),
        );
        node.acknowledge_persisted(hard_state)
            .expect("first vote persists");

        let second = Message::RequestVote {
            term: Term(7),
            candidate: NodeId(2),
            last_log: LogPosition::default(),
        };
        let effects = node
            .handle(receive(2, 3, second))
            .expect("second request is answered");
        assert_eq!(
            effects,
            vec![Effect::Send(Envelope {
                from: NodeId(3),
                to: NodeId(2),
                message: Message::RequestVoteResponse {
                    term: Term(7),
                    vote_granted: false,
                },
            })]
        );
        assert_eq!(node.hard_state().voted_for, Some(NodeId(1)));
    }

    #[test]
    fn duplicate_votes_do_not_manufacture_a_quorum() {
        let mut node = core(1, &[1, 2, 3, 4, 5]);
        let hard_state = persist_from(
            &node
                .handle(Event::ElectionTimeout)
                .expect("timeout starts persistence"),
        );
        node.acknowledge_persisted(hard_state)
            .expect("candidate state persists");

        let response = Message::RequestVoteResponse {
            term: Term(1),
            vote_granted: true,
        };
        node.handle(receive(2, 1, response.clone()))
            .expect("first response is accepted");
        node.handle(receive(2, 1, response.clone()))
            .expect("duplicate response is accepted");
        assert_eq!(node.role(), Role::Candidate);

        let effects = node
            .handle(receive(3, 1, response))
            .expect("third distinct vote is accepted");
        assert_eq!(node.role(), Role::Leader);
        let mutation = match effects.as_slice() {
            [Effect::PersistLog(mutation)] => mutation.clone(),
            other => panic!("expected leader no-op persistence, got {other:?}"),
        };
        let effects = node
            .acknowledge_log_persisted(mutation)
            .expect("leader no-op persists");
        assert_eq!(effects.len(), 4);
        assert!(effects.iter().all(|effect| matches!(
            effect,
            Effect::Send(Envelope {
                message: Message::AppendEntries { term: Term(1), .. },
                ..
            })
        )));
    }

    #[test]
    fn higher_term_heartbeat_steps_candidate_down_after_persistence() {
        let mut node = core(1, &[1, 2, 3]);
        let candidate_state = persist_from(
            &node
                .handle(Event::ElectionTimeout)
                .expect("timeout starts persistence"),
        );
        node.acknowledge_persisted(candidate_state)
            .expect("candidate state persists");
        assert_eq!(node.role(), Role::Candidate);

        let effects = node
            .handle(receive(
                2,
                1,
                Message::AppendEntries {
                    term: Term(2),
                    leader: NodeId(2),
                    previous: LogPosition::default(),
                    entries: Vec::new(),
                    leader_commit: 0,
                },
            ))
            .expect("heartbeat starts term persistence");
        let follower_state = persist_from(&effects);
        assert_eq!(node.role(), Role::Candidate);

        let effects = node
            .acknowledge_persisted(follower_state)
            .expect("higher term persists");
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.leader(), Some(NodeId(2)));
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::ResetElectionTimer,
                Effect::Send(Envelope {
                    message: Message::AppendEntriesResponse {
                        term: Term(2),
                        accepted: true,
                        match_index: 0,
                        ..
                    },
                    ..
                })
            ]
        ));
    }

    #[test]
    fn mismatched_persistence_acknowledgement_keeps_barrier_closed() {
        let mut node = core(1, &[1]);
        node.handle(Event::ElectionTimeout)
            .expect("timeout starts persistence");

        let error = node
            .acknowledge_persisted(HardState::default())
            .expect_err("mismatched state must be rejected");
        assert!(matches!(
            error,
            CoreError::PersistenceAcknowledgementMismatch { .. }
        ));
        assert!(node.persistence_pending());
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn term_exhaustion_fails_without_changing_state() {
        let hard_state = HardState {
            current_term: Term(u64::MAX),
            voted_for: None,
        };
        let mut node = RaftCore::new(NodeId(1), voters(&[1]), hard_state, Vec::new())
            .expect("core configuration is valid");

        assert_eq!(
            node.handle(Event::ElectionTimeout),
            Err(CoreError::TermExhausted)
        );
        assert_eq!(node.hard_state(), hard_state);
        assert!(!node.persistence_pending());
    }

    #[test]
    fn duplicate_voter_ids_are_rejected() {
        assert_eq!(
            VoterSet::new([NodeId(1), NodeId(1)]),
            Err(CoreError::DuplicateVoter(NodeId(1)))
        );
    }

    #[test]
    fn append_response_waits_for_log_durability() {
        let mut node = RaftCore::new(
            NodeId(2),
            voters(&[1, 2, 3]),
            HardState {
                current_term: Term(4),
                voted_for: None,
            },
            Vec::new(),
        )
        .expect("core configuration is valid");
        let entry = command_entry(1, 4, 9);

        let effects = node
            .handle(receive(
                1,
                2,
                append_request(4, LogPosition::default(), vec![entry.clone()], 1),
            ))
            .expect("append is accepted for persistence");
        let mutation = match effects.as_slice() {
            [Effect::ResetElectionTimer, Effect::PersistLog(mutation)] => mutation.clone(),
            other => panic!("expected timer reset and log persistence, got {other:?}"),
        };
        assert_eq!(node.last_log_position(), LogPosition::default());
        assert_eq!(node.commit_index(), 0);

        let effects = node
            .acknowledge_log_persisted(mutation)
            .expect("matching log persistence is accepted");
        assert_eq!(
            effects,
            vec![
                Effect::CommitIndexAdvanced(1),
                Effect::ApplyCommitted(vec![entry.clone()]),
                Effect::Send(Envelope {
                    from: NodeId(2),
                    to: NodeId(1),
                    message: Message::AppendEntriesResponse {
                        term: Term(4),
                        accepted: true,
                        match_index: 1,
                        request_previous_index: 0,
                    },
                }),
            ]
        );
        assert_eq!(
            node.last_log_position(),
            LogPosition {
                index: 1,
                term: Term(4)
            }
        );
        assert_eq!(node.commit_index(), 1);
    }

    #[test]
    fn higher_term_append_crosses_both_persistence_barriers() {
        let mut node = core(2, &[1, 2, 3]);
        let entry = command_entry(1, 2, 7);

        let hard_effects = node
            .handle(receive(
                1,
                2,
                append_request(2, LogPosition::default(), vec![entry], 0),
            ))
            .expect("higher term starts hard-state persistence");
        let hard_state = persist_from(&hard_effects);
        let log_effects = node
            .acknowledge_persisted(hard_state)
            .expect("hard state persistence is accepted");
        let mutation = match log_effects.as_slice() {
            [Effect::ResetElectionTimer, Effect::PersistLog(mutation)] => mutation.clone(),
            other => panic!("expected log persistence after hard state, got {other:?}"),
        };
        assert_eq!(node.last_log_position(), LogPosition::default());

        let response = node
            .acknowledge_log_persisted(mutation)
            .expect("log persistence is accepted");
        assert!(matches!(
            response.as_slice(),
            [Effect::Send(Envelope {
                message: Message::AppendEntriesResponse {
                    term: Term(2),
                    accepted: true,
                    match_index: 1,
                    ..
                },
                ..
            })]
        ));
    }

    #[test]
    fn append_rejects_a_mismatched_prefix_without_mutation() {
        let mut node = RaftCore::new(
            NodeId(2),
            voters(&[1, 2, 3]),
            HardState {
                current_term: Term(3),
                voted_for: None,
            },
            vec![command_entry(1, 2, 1)],
        )
        .expect("core configuration is valid");

        let effects = node
            .handle(receive(
                1,
                2,
                append_request(
                    3,
                    LogPosition {
                        index: 1,
                        term: Term(1),
                    },
                    vec![command_entry(2, 3, 2)],
                    0,
                ),
            ))
            .expect("prefix mismatch is a protocol response");
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::ResetElectionTimer,
                Effect::Send(Envelope {
                    message: Message::AppendEntriesResponse {
                        accepted: false,
                        ..
                    },
                    ..
                })
            ]
        ));
        assert!(!node.persistence_pending());
        assert_eq!(node.last_log_position().term, Term(2));
    }

    #[test]
    fn stale_failure_cannot_regress_replication_progress() {
        let mut node = core(1, &[1, 2, 3]);
        let hard_state = persist_from(
            &node
                .handle(Event::ElectionTimeout)
                .expect("timeout starts persistence"),
        );
        node.acknowledge_persisted(hard_state)
            .expect("candidate state persists");
        let effects = node
            .handle(receive(
                2,
                1,
                Message::RequestVoteResponse {
                    term: Term(1),
                    vote_granted: true,
                },
            ))
            .expect("quorum elects leader");
        let mutation = match effects.as_slice() {
            [Effect::PersistLog(mutation)] => mutation.clone(),
            other => panic!("expected leader no-op persistence, got {other:?}"),
        };
        node.acknowledge_log_persisted(mutation)
            .expect("leader no-op persists");

        node.handle(receive(
            2,
            1,
            Message::AppendEntriesResponse {
                term: Term(1),
                accepted: true,
                match_index: 1,
                request_previous_index: 0,
            },
        ))
        .expect("successful replication is recorded");
        assert_eq!(
            node.replication_progress(NodeId(2)),
            Some(ReplicationProgress {
                next_index: 2,
                match_index: 1
            })
        );

        node.handle(receive(
            2,
            1,
            Message::AppendEntriesResponse {
                term: Term(1),
                accepted: false,
                match_index: 0,
                request_previous_index: 0,
            },
        ))
        .expect("stale failure is ignored");
        assert_eq!(
            node.replication_progress(NodeId(2)),
            Some(ReplicationProgress {
                next_index: 2,
                match_index: 1
            })
        );
    }

    #[test]
    fn follower_refuses_to_replace_its_committed_prefix() {
        let mut node = RaftCore::new(
            NodeId(2),
            voters(&[1, 2, 3]),
            HardState {
                current_term: Term(5),
                voted_for: None,
            },
            Vec::new(),
        )
        .expect("core configuration is valid");
        let first = command_entry(1, 5, 1);
        let effects = node
            .handle(receive(
                1,
                2,
                append_request(5, LogPosition::default(), vec![first], 1),
            ))
            .expect("first append starts persistence");
        let mutation = match effects.as_slice() {
            [Effect::ResetElectionTimer, Effect::PersistLog(mutation)] => mutation.clone(),
            other => panic!("expected log persistence, got {other:?}"),
        };
        node.acknowledge_log_persisted(mutation)
            .expect("first append commits");
        assert_eq!(node.commit_index(), 1);

        let error = node
            .handle(receive(
                1,
                2,
                append_request(5, LogPosition::default(), vec![command_entry(1, 5, 9)], 1),
            ))
            .expect_err("committed conflict must fail closed");
        assert_eq!(
            error,
            CoreError::CommittedLogConflict {
                index: 1,
                commit_index: 1
            }
        );
        assert_eq!(node.last_log_position().index, 1);
    }
}
