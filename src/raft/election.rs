use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{self, Display, Formatter},
};

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

    fn has_quorum(&self, votes: &BTreeSet<NodeId>) -> bool {
        let granted = votes.intersection(&self.0).count();
        granted > self.0.len() / 2
    }

    fn peers(&self, local: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.0.iter().copied().filter(move |node| *node != local)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    Heartbeat {
        term: Term,
        leader: NodeId,
    },
    HeartbeatResponse {
        term: Term,
        accepted: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event {
    ElectionTimeout,
    Receive(Envelope),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Effect {
    PersistHardState(HardState),
    Send(Envelope),
    ResetElectionTimer,
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
pub struct RaftCore {
    id: NodeId,
    voters: VoterSet,
    hard_state: HardState,
    role: Role,
    leader: Option<NodeId>,
    last_log: LogPosition,
    granted_votes: BTreeSet<NodeId>,
    pending: Option<PendingPersistence>,
}

impl RaftCore {
    pub fn new(
        id: NodeId,
        voters: VoterSet,
        hard_state: HardState,
        last_log: LogPosition,
    ) -> Result<Self, CoreError> {
        if !voters.contains(id) {
            return Err(CoreError::LocalNodeIsNotVoter(id));
        }
        if let Some(voted_for) = hard_state.voted_for
            && !voters.contains(voted_for)
        {
            return Err(CoreError::InvalidRecoveredVote(voted_for));
        }
        Ok(Self {
            id,
            voters,
            hard_state,
            role: Role::Follower,
            leader: None,
            last_log,
            granted_votes: BTreeSet::new(),
            pending: None,
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
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

    pub fn persistence_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn handle(&mut self, event: Event) -> Result<Vec<Effect>, CoreError> {
        if self.pending.is_some() {
            return Err(CoreError::PersistencePending);
        }

        match event {
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::Receive(envelope) if envelope.to == self.id => self.handle_message(envelope),
            Event::Receive(_) => Ok(Vec::new()),
        }
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

        let mut effects = pending.after_persist;
        self.promote_if_quorum(&mut effects);
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
                    last_log: self.last_log,
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
            Message::Heartbeat { term, leader } => {
                self.handle_heartbeat(envelope.from, term, leader)
            }
            Message::HeartbeatResponse { term, .. } => self.observe_response_term(term),
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

        let log_is_eligible = candidate_last_log.is_at_least_as_up_to_date_as(self.last_log);
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
        self.promote_if_quorum(&mut effects);
        Ok(effects)
    }

    fn handle_heartbeat(
        &mut self,
        sender: NodeId,
        term: Term,
        leader: NodeId,
    ) -> Result<Vec<Effect>, CoreError> {
        if sender != leader || term < self.hard_state.current_term {
            return Ok(vec![self.heartbeat_response(sender, false)]);
        }

        let after_persist = vec![
            Effect::ResetElectionTimer,
            Effect::Send(Envelope {
                from: self.id,
                to: sender,
                message: Message::HeartbeatResponse {
                    term,
                    accepted: true,
                },
            }),
        ];
        if term > self.hard_state.current_term {
            return self.begin_persistence(PendingPersistence {
                hard_state: HardState {
                    current_term: term,
                    voted_for: None,
                },
                role: Role::Follower,
                leader: Some(leader),
                granted_votes: BTreeSet::new(),
                after_persist,
            });
        }

        self.role = Role::Follower;
        self.leader = Some(leader);
        self.granted_votes.clear();
        Ok(after_persist)
    }

    fn observe_response_term(&mut self, term: Term) -> Result<Vec<Effect>, CoreError> {
        if term > self.hard_state.current_term {
            self.persist_higher_term(term)
        } else {
            Ok(Vec::new())
        }
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
        if self.pending.is_some() {
            return Err(CoreError::PersistencePending);
        }
        let hard_state = pending.hard_state;
        self.pending = Some(pending);
        Ok(vec![Effect::PersistHardState(hard_state)])
    }

    fn promote_if_quorum(&mut self, effects: &mut Vec<Effect>) {
        if self.role != Role::Candidate || !self.voters.has_quorum(&self.granted_votes) {
            return;
        }

        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.granted_votes.clear();
        effects.extend(self.voters.peers(self.id).map(|peer| {
            Effect::Send(Envelope {
                from: self.id,
                to: peer,
                message: Message::Heartbeat {
                    term: self.hard_state.current_term,
                    leader: self.id,
                },
            })
        }));
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

    fn heartbeat_response(&self, recipient: NodeId, accepted: bool) -> Effect {
        Effect::Send(Envelope {
            from: self.id,
            to: recipient,
            message: Message::HeartbeatResponse {
                term: self.hard_state.current_term,
                accepted,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters(ids: &[u64]) -> VoterSet {
        VoterSet::new(ids.iter().copied().map(NodeId)).expect("voter set is valid")
    }

    fn core(id: u64, ids: &[u64]) -> RaftCore {
        RaftCore::new(
            NodeId(id),
            voters(ids),
            HardState::default(),
            LogPosition::default(),
        )
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
        assert_eq!(effects, vec![Effect::ResetElectionTimer]);
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.leader(), Some(NodeId(1)));
        assert_eq!(node.hard_state().current_term, Term(1));
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
            LogPosition {
                index: 8,
                term: Term(2),
            },
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
        node.handle(receive(2, 1, response))
            .expect("first response is accepted");
        node.handle(receive(2, 1, response))
            .expect("duplicate response is accepted");
        assert_eq!(node.role(), Role::Candidate);

        let effects = node
            .handle(receive(3, 1, response))
            .expect("third distinct vote is accepted");
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(effects.len(), 4);
        assert!(effects.iter().all(|effect| matches!(
            effect,
            Effect::Send(Envelope {
                message: Message::Heartbeat { term: Term(1), .. },
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
                Message::Heartbeat {
                    term: Term(2),
                    leader: NodeId(2),
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
                    message: Message::HeartbeatResponse {
                        term: Term(2),
                        accepted: true
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
        let mut node = RaftCore::new(NodeId(1), voters(&[1]), hard_state, LogPosition::default())
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
}
