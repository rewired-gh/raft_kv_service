//! Deterministic, single-threaded protocol simulator.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt::{self, Display, Formatter},
};

use super::{
    CoreError, Effect, EntryPayload, Envelope, Event, HardState, LogEntry, LogMutation, NodeId,
    RaftCore, Role, Term, VoterSet,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraceEvent {
    ElectionTimeout(NodeId),
    HardStatePersisted(NodeId, HardState),
    LogPersisted(NodeId, LogMutation),
    CommitIndexAdvanced(NodeId, u64),
    EntriesApplied(NodeId, u64),
    MessageSent(Envelope),
    MessageDelivered(Envelope),
    MessageDropped(Envelope),
    ElectionTimerReset(NodeId),
    NodeCrashed(NodeId),
    NodeRestarted(NodeId),
    StalePersistenceDiscarded(NodeId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Scheduled {
    Persist {
        node: NodeId,
        generation: u64,
        hard_state: HardState,
    },
    PersistLog {
        node: NodeId,
        generation: u64,
        mutation: LogMutation,
    },
    Deliver(Envelope),
}

#[derive(Clone, Debug)]
struct SimNode {
    core: Option<RaftCore>,
    durable: HardState,
    log: Vec<LogEntry>,
    generation: u64,
}

#[derive(Clone, Debug)]
pub struct ClusterSimulator {
    voters: VoterSet,
    nodes: BTreeMap<NodeId, SimNode>,
    scheduled: VecDeque<Scheduled>,
    trace: Vec<TraceEvent>,
    now: u64,
    random_state: u64,
    election_timeout_min: u64,
    election_timeout_max: u64,
    election_deadlines: BTreeMap<NodeId, Option<u64>>,
    blocked_links: BTreeSet<(NodeId, NodeId)>,
}

impl ClusterSimulator {
    pub fn new(ids: impl IntoIterator<Item = NodeId>) -> Result<Self, SimError> {
        Self::new_with_seed(ids, 0x7261_6674_2d6b_7621, 10, 20)
    }

    pub fn new_with_seed(
        ids: impl IntoIterator<Item = NodeId>,
        seed: u64,
        election_timeout_min: u64,
        election_timeout_max: u64,
    ) -> Result<Self, SimError> {
        if election_timeout_min == 0 || election_timeout_min > election_timeout_max {
            return Err(SimError::InvalidElectionTimeoutRange {
                min: election_timeout_min,
                max: election_timeout_max,
            });
        }
        election_timeout_max
            .checked_sub(election_timeout_min)
            .and_then(|width| width.checked_add(1))
            .ok_or(SimError::InvalidElectionTimeoutRange {
                min: election_timeout_min,
                max: election_timeout_max,
            })?;

        let ids = ids.into_iter().collect::<Vec<_>>();
        let voters = VoterSet::new(ids.iter().copied())?;
        let mut nodes = BTreeMap::new();
        for id in ids {
            let durable = HardState::default();
            let log = Vec::new();
            let core = RaftCore::new(id, voters.clone(), durable, log.clone())?;
            nodes.insert(
                id,
                SimNode {
                    core: Some(core),
                    durable,
                    log,
                    generation: 0,
                },
            );
        }
        let mut simulator = Self {
            voters,
            nodes,
            scheduled: VecDeque::new(),
            trace: Vec::new(),
            now: 0,
            random_state: seed,
            election_timeout_min,
            election_timeout_max,
            election_deadlines: BTreeMap::new(),
            blocked_links: BTreeSet::new(),
        };
        let node_ids = simulator.nodes.keys().copied().collect::<Vec<_>>();
        for node in node_ids {
            simulator.reset_election_deadline(node)?;
        }
        Ok(simulator)
    }

    pub fn timeout(&mut self, node: NodeId) -> Result<(), SimError> {
        if self.node(node)?.core.is_none() {
            return Err(SimError::NodeOffline(node));
        }
        self.election_deadlines.insert(node, None);
        self.trace.push(TraceEvent::ElectionTimeout(node));
        let effects = self.core_mut(node)?.handle(Event::ElectionTimeout)?;
        self.schedule_effects(node, effects)
    }

    pub fn propose(&mut self, node: NodeId, payload: EntryPayload) -> Result<u64, SimError> {
        let (index, effects) = self.core_mut(node)?.propose(payload)?;
        self.schedule_effects(node, effects)?;
        Ok(index)
    }

    pub fn heartbeat(&mut self, node: NodeId) -> Result<(), SimError> {
        let effects = self.core_mut(node)?.heartbeat()?;
        self.schedule_effects(node, effects)
    }

    pub fn advance_to_next_timeout(&mut self) -> Result<Vec<NodeId>, SimError> {
        let next_deadline = self
            .election_deadlines
            .values()
            .filter_map(|deadline| *deadline)
            .min()
            .ok_or(SimError::NoElectionDeadline)?;
        self.now = next_deadline;
        let due = self
            .election_deadlines
            .iter()
            .filter_map(|(node, deadline)| (*deadline == Some(next_deadline)).then_some(*node))
            .collect::<Vec<_>>();
        for node in &due {
            self.timeout(*node)?;
        }
        Ok(due)
    }

    pub fn step(&mut self) -> Result<bool, SimError> {
        let Some(scheduled) = self.scheduled.pop_front() else {
            return Ok(false);
        };

        match scheduled {
            Scheduled::Persist {
                node,
                generation,
                hard_state,
            } => self.persist(node, generation, hard_state)?,
            Scheduled::PersistLog {
                node,
                generation,
                mutation,
            } => self.persist_log(node, generation, mutation)?,
            Scheduled::Deliver(envelope) => self.deliver(envelope)?,
        }
        Ok(true)
    }

    pub fn run_until_idle(&mut self, max_steps: usize) -> Result<usize, SimError> {
        for completed in 0..max_steps {
            if !self.step()? {
                return Ok(completed);
            }
        }
        if self.scheduled.is_empty() {
            Ok(max_steps)
        } else {
            Err(SimError::StepLimitExceeded(max_steps))
        }
    }

    pub fn crash(&mut self, node: NodeId) -> Result<(), SimError> {
        let sim_node = self.node_mut(node)?;
        if sim_node.core.is_none() {
            return Err(SimError::NodeOffline(node));
        }
        sim_node.core = None;
        self.election_deadlines.insert(node, None);
        self.trace.push(TraceEvent::NodeCrashed(node));
        Ok(())
    }

    pub fn restart(&mut self, node: NodeId) -> Result<(), SimError> {
        let voters = self.voters.clone();
        let sim_node = self.node_mut(node)?;
        if sim_node.core.is_some() {
            return Err(SimError::NodeAlreadyOnline(node));
        }
        sim_node.generation = sim_node
            .generation
            .checked_add(1)
            .ok_or(SimError::GenerationExhausted(node))?;
        sim_node.core = Some(RaftCore::new(
            node,
            voters,
            sim_node.durable,
            sim_node.log.clone(),
        )?);
        self.reset_election_deadline(node)?;
        self.trace.push(TraceEvent::NodeRestarted(node));
        Ok(())
    }

    pub fn drop_messages_between(&mut self, first: NodeId, second: NodeId) -> usize {
        let mut dropped = Vec::new();
        self.scheduled.retain(|scheduled| match scheduled {
            Scheduled::Deliver(envelope)
                if (envelope.from == first && envelope.to == second)
                    || (envelope.from == second && envelope.to == first) =>
            {
                dropped.push(envelope.clone());
                false
            }
            _ => true,
        });
        let count = dropped.len();
        self.trace
            .extend(dropped.into_iter().map(TraceEvent::MessageDropped));
        count
    }

    pub fn partition(&mut self, first: NodeId, second: NodeId) -> Result<(), SimError> {
        self.node(first)?;
        self.node(second)?;
        if first != second {
            self.blocked_links.insert(ordered_link(first, second));
            self.drop_messages_between(first, second);
        }
        Ok(())
    }

    pub fn heal(&mut self, first: NodeId, second: NodeId) -> Result<(), SimError> {
        self.node(first)?;
        self.node(second)?;
        self.blocked_links.remove(&ordered_link(first, second));
        Ok(())
    }

    pub fn drop_all_messages(&mut self) -> usize {
        let mut dropped = Vec::new();
        self.scheduled.retain(|scheduled| match scheduled {
            Scheduled::Deliver(envelope) => {
                dropped.push(envelope.clone());
                false
            }
            Scheduled::Persist { .. } | Scheduled::PersistLog { .. } => true,
        });
        let count = dropped.len();
        self.trace
            .extend(dropped.into_iter().map(TraceEvent::MessageDropped));
        count
    }

    pub fn role(&self, node: NodeId) -> Result<Option<Role>, SimError> {
        Ok(self.node(node)?.core.as_ref().map(RaftCore::role))
    }

    pub fn hard_state(&self, node: NodeId) -> Result<HardState, SimError> {
        Ok(self.node(node)?.durable)
    }

    pub fn last_log_position(&self, node: NodeId) -> Result<crate::raft::LogPosition, SimError> {
        Ok(self
            .node(node)?
            .log
            .last()
            .map_or_else(crate::raft::LogPosition::default, |entry| {
                crate::raft::LogPosition {
                    index: entry.index,
                    term: entry.term,
                }
            }))
    }

    pub fn commit_index(&self, node: NodeId) -> Result<Option<u64>, SimError> {
        Ok(self.node(node)?.core.as_ref().map(RaftCore::commit_index))
    }

    pub fn pending_events(&self) -> usize {
        self.scheduled.len()
    }

    pub fn time(&self) -> u64 {
        self.now
    }

    pub fn election_deadline(&self, node: NodeId) -> Result<Option<u64>, SimError> {
        self.node(node)?;
        Ok(self.election_deadlines.get(&node).copied().flatten())
    }

    pub fn trace(&self) -> &[TraceEvent] {
        &self.trace
    }

    fn persist(
        &mut self,
        node: NodeId,
        generation: u64,
        hard_state: HardState,
    ) -> Result<(), SimError> {
        let sim_node = self.node_mut(node)?;
        if sim_node.generation != generation || sim_node.core.is_none() {
            self.trace.push(TraceEvent::StalePersistenceDiscarded(node));
            return Ok(());
        }
        sim_node.durable = hard_state;
        let effects = sim_node
            .core
            .as_mut()
            .ok_or(SimError::NodeOffline(node))?
            .acknowledge_persisted(hard_state)?;
        self.trace
            .push(TraceEvent::HardStatePersisted(node, hard_state));
        self.schedule_effects(node, effects)
    }

    fn deliver(&mut self, envelope: Envelope) -> Result<(), SimError> {
        let recipient = envelope.to;
        if self.node(recipient)?.core.is_none()
            || self
                .blocked_links
                .contains(&ordered_link(envelope.from, recipient))
        {
            self.trace.push(TraceEvent::MessageDropped(envelope));
            return Ok(());
        }
        self.trace
            .push(TraceEvent::MessageDelivered(envelope.clone()));
        let effects = self.core_mut(recipient)?.handle(Event::Receive(envelope))?;
        self.schedule_effects(recipient, effects)
    }

    fn persist_log(
        &mut self,
        node: NodeId,
        generation: u64,
        mutation: LogMutation,
    ) -> Result<(), SimError> {
        let sim_node = self.node_mut(node)?;
        if sim_node.generation != generation || sim_node.core.is_none() {
            self.trace.push(TraceEvent::StalePersistenceDiscarded(node));
            return Ok(());
        }
        if term_at(&sim_node.log, mutation.previous.index) != Some(mutation.previous.term) {
            return Err(SimError::StorageLogMismatch(node));
        }
        apply_log_mutation(&mut sim_node.log, &mutation.entries)?;
        let effects = sim_node
            .core
            .as_mut()
            .ok_or(SimError::NodeOffline(node))?
            .acknowledge_log_persisted(mutation.clone())?;
        self.trace.push(TraceEvent::LogPersisted(node, mutation));
        self.schedule_effects(node, effects)
    }

    fn schedule_effects(&mut self, node: NodeId, effects: Vec<Effect>) -> Result<(), SimError> {
        for effect in effects {
            match effect {
                Effect::PersistHardState(hard_state) => {
                    let generation = self.node(node)?.generation;
                    self.scheduled.push_front(Scheduled::Persist {
                        node,
                        generation,
                        hard_state,
                    });
                }
                Effect::PersistLog(mutation) => {
                    let generation = self.node(node)?.generation;
                    self.scheduled.push_front(Scheduled::PersistLog {
                        node,
                        generation,
                        mutation,
                    });
                }
                Effect::Send(envelope) => {
                    self.trace.push(TraceEvent::MessageSent(envelope.clone()));
                    if self
                        .blocked_links
                        .contains(&ordered_link(envelope.from, envelope.to))
                    {
                        self.trace.push(TraceEvent::MessageDropped(envelope));
                    } else {
                        self.scheduled.push_back(Scheduled::Deliver(envelope));
                    }
                }
                Effect::ResetElectionTimer => {
                    self.trace.push(TraceEvent::ElectionTimerReset(node));
                    self.reset_election_deadline(node)?;
                }
                Effect::CommitIndexAdvanced(index) => {
                    self.trace
                        .push(TraceEvent::CommitIndexAdvanced(node, index));
                }
                Effect::ApplyCommitted(entries) => {
                    let last_index = entries.last().map_or(0, |entry| entry.index);
                    self.trace
                        .push(TraceEvent::EntriesApplied(node, last_index));
                    let continuation = self.core_mut(node)?.acknowledge_applied(last_index)?;
                    self.schedule_effects(node, continuation)?;
                }
                Effect::SendSnapshot { .. } => {}
            }
        }
        Ok(())
    }

    fn node(&self, node: NodeId) -> Result<&SimNode, SimError> {
        self.nodes.get(&node).ok_or(SimError::UnknownNode(node))
    }

    fn node_mut(&mut self, node: NodeId) -> Result<&mut SimNode, SimError> {
        self.nodes.get_mut(&node).ok_or(SimError::UnknownNode(node))
    }

    fn core_mut(&mut self, node: NodeId) -> Result<&mut RaftCore, SimError> {
        self.node_mut(node)?
            .core
            .as_mut()
            .ok_or(SimError::NodeOffline(node))
    }

    fn reset_election_deadline(&mut self, node: NodeId) -> Result<(), SimError> {
        let width = self
            .election_timeout_max
            .checked_sub(self.election_timeout_min)
            .and_then(|value| value.checked_add(1))
            .ok_or(SimError::InvalidElectionTimeoutRange {
                min: self.election_timeout_min,
                max: self.election_timeout_max,
            })?;
        let timeout = self.election_timeout_min + self.next_random() % width;
        let deadline = self
            .now
            .checked_add(timeout)
            .ok_or(SimError::TimeExhausted)?;
        self.election_deadlines.insert(node, Some(deadline));
        Ok(())
    }

    fn next_random(&mut self) -> u64 {
        self.random_state = self.random_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.random_state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

fn ordered_link(first: NodeId, second: NodeId) -> (NodeId, NodeId) {
    if first <= second {
        (first, second)
    } else {
        (second, first)
    }
}

fn term_at(log: &[LogEntry], index: u64) -> Option<Term> {
    if index == 0 {
        return Some(Term(0));
    }
    let position = usize::try_from(index.checked_sub(1)?).ok()?;
    log.get(position).map(|entry| entry.term)
}

fn apply_log_mutation(log: &mut Vec<LogEntry>, entries: &[LogEntry]) -> Result<(), SimError> {
    for (incoming_position, entry) in entries.iter().enumerate() {
        let offset = entry
            .index
            .checked_sub(1)
            .ok_or(SimError::LogIndexExhausted)?;
        let position = usize::try_from(offset).map_err(|_| SimError::LogIndexExhausted)?;
        if log.get(position) == Some(entry) {
            continue;
        }
        log.truncate(position);
        log.extend_from_slice(&entries[incoming_position..]);
        break;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SimError {
    Core(CoreError),
    UnknownNode(NodeId),
    NodeOffline(NodeId),
    NodeAlreadyOnline(NodeId),
    GenerationExhausted(NodeId),
    StepLimitExceeded(usize),
    StorageLogMismatch(NodeId),
    LogIndexExhausted,
    InvalidElectionTimeoutRange { min: u64, max: u64 },
    NoElectionDeadline,
    TimeExhausted,
}

impl From<CoreError> for SimError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

impl Display for SimError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(error) => Display::fmt(error, formatter),
            Self::UnknownNode(node) => write!(formatter, "unknown simulated node {}", node.0),
            Self::NodeOffline(node) => write!(formatter, "simulated node {} is offline", node.0),
            Self::NodeAlreadyOnline(node) => {
                write!(formatter, "simulated node {} is already online", node.0)
            }
            Self::GenerationExhausted(node) => {
                write!(formatter, "simulated node {} exhausted generations", node.0)
            }
            Self::StepLimitExceeded(limit) => {
                write!(formatter, "simulation exceeded {limit} deterministic steps")
            }
            Self::StorageLogMismatch(node) => {
                write!(
                    formatter,
                    "simulated durable log mismatch on node {}",
                    node.0
                )
            }
            Self::LogIndexExhausted => formatter.write_str("simulated Raft log index exhausted"),
            Self::InvalidElectionTimeoutRange { min, max } => {
                write!(formatter, "invalid election timeout range {min}..={max}")
            }
            Self::NoElectionDeadline => formatter.write_str("no election deadline is armed"),
            Self::TimeExhausted => formatter.write_str("simulated logical time exhausted"),
        }
    }
}

impl Error for SimError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{Message, Term};

    fn three_nodes() -> ClusterSimulator {
        ClusterSimulator::new([NodeId(1), NodeId(2), NodeId(3)])
            .expect("simulator configuration is valid")
    }

    #[test]
    fn three_node_cluster_elects_one_leader_deterministically() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        simulation
            .run_until_idle(32)
            .expect("simulation reaches quiescence");

        assert_eq!(
            simulation.role(NodeId(1)).expect("node exists"),
            Some(Role::Leader)
        );
        assert_eq!(
            simulation.role(NodeId(2)).expect("node exists"),
            Some(Role::Follower)
        );
        assert_eq!(
            simulation.role(NodeId(3)).expect("node exists"),
            Some(Role::Follower)
        );
        for node in [NodeId(1), NodeId(2), NodeId(3)] {
            assert_eq!(
                simulation.last_log_position(node).expect("node exists"),
                crate::raft::LogPosition {
                    index: 1,
                    term: Term(1)
                }
            );
            assert_eq!(simulation.commit_index(node).expect("node exists"), Some(1));
        }
    }

    #[test]
    fn leader_proposal_replicates_and_commits_on_all_nodes() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        simulation
            .run_until_idle(64)
            .expect("election reaches quiescence");

        let index = simulation
            .propose(NodeId(1), EntryPayload::Command(vec![4, 2]))
            .expect("ready leader accepts proposal");
        assert_eq!(index, 2);
        simulation
            .run_until_idle(64)
            .expect("replication reaches quiescence");

        for node in [NodeId(1), NodeId(2), NodeId(3)] {
            assert_eq!(
                simulation
                    .last_log_position(node)
                    .expect("node exists")
                    .index,
                2
            );
            assert_eq!(simulation.commit_index(node).expect("node exists"), Some(2));
        }
    }

    #[test]
    fn restarted_lagging_follower_catches_up_from_heartbeat() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        simulation
            .run_until_idle(64)
            .expect("election reaches quiescence");
        simulation.crash(NodeId(3)).expect("follower crashes");

        simulation
            .propose(NodeId(1), EntryPayload::Command(vec![9]))
            .expect("leader accepts proposal");
        simulation
            .run_until_idle(64)
            .expect("available quorum commits proposal");
        assert_eq!(
            simulation
                .last_log_position(NodeId(3))
                .expect("node exists")
                .index,
            1
        );

        simulation.restart(NodeId(3)).expect("follower restarts");
        simulation
            .heartbeat(NodeId(1))
            .expect("leader emits replication heartbeat");
        simulation.run_until_idle(64).expect("follower catches up");
        assert_eq!(
            simulation
                .last_log_position(NodeId(3))
                .expect("node exists")
                .index,
            2
        );
        assert_eq!(
            simulation.commit_index(NodeId(3)).expect("node exists"),
            Some(2)
        );
    }

    #[test]
    fn majority_replaces_isolated_leaders_uncommitted_suffix() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        simulation.run_until_idle(64).expect("node one is elected");
        simulation
            .partition(NodeId(1), NodeId(2))
            .expect("partition");
        simulation
            .partition(NodeId(1), NodeId(3))
            .expect("partition");

        simulation
            .propose(NodeId(1), EntryPayload::Command(vec![1]))
            .expect("isolated leader durably appends");
        simulation.run_until_idle(16).expect("messages are dropped");
        assert_eq!(simulation.commit_index(NodeId(1)).expect("node"), Some(1));

        simulation
            .timeout(NodeId(2))
            .expect("majority starts election");
        simulation
            .run_until_idle(64)
            .expect("new leader commits no-op");
        assert_eq!(
            simulation.role(NodeId(2)).expect("node"),
            Some(Role::Leader)
        );
        assert_eq!(simulation.commit_index(NodeId(2)).expect("node"), Some(2));

        simulation.heal(NodeId(1), NodeId(2)).expect("heal");
        simulation.heal(NodeId(1), NodeId(3)).expect("heal");
        simulation.heartbeat(NodeId(2)).expect("repair suffix");
        simulation.run_until_idle(64).expect("cluster converges");
        assert_eq!(
            simulation.role(NodeId(1)).expect("node"),
            Some(Role::Follower)
        );
        assert_eq!(simulation.commit_index(NodeId(1)).expect("node"), Some(2));
        assert_eq!(
            simulation.last_log_position(NodeId(1)).expect("node"),
            crate::raft::LogPosition {
                index: 2,
                term: Term(2)
            }
        );
    }

    #[test]
    fn seeded_logical_clock_reproduces_timeout_order() {
        let mut first =
            ClusterSimulator::new_with_seed([NodeId(1), NodeId(2), NodeId(3)], 42, 10, 20)
                .expect("simulator configuration is valid");
        let mut second =
            ClusterSimulator::new_with_seed([NodeId(1), NodeId(2), NodeId(3)], 42, 10, 20)
                .expect("simulator configuration is valid");

        let first_due = first.advance_to_next_timeout().expect("a timeout is armed");
        let second_due = second
            .advance_to_next_timeout()
            .expect("a timeout is armed");
        assert_eq!(first_due, second_due);
        assert_eq!(first.time(), second.time());
        assert_eq!(first.trace(), second.trace());

        first
            .run_until_idle(32)
            .expect("timed election reaches quiescence");
        let elected = first_due[0];
        assert_eq!(
            first.role(elected).expect("node exists"),
            Some(Role::Leader)
        );
    }

    #[test]
    fn crash_before_persist_loses_candidate_transition() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        simulation.crash(NodeId(1)).expect("node can crash");
        simulation.restart(NodeId(1)).expect("node can restart");
        simulation
            .run_until_idle(8)
            .expect("stale persistence is discarded");

        assert_eq!(
            simulation.hard_state(NodeId(1)).expect("node exists"),
            HardState::default()
        );
        assert_eq!(
            simulation.role(NodeId(1)).expect("node exists"),
            Some(Role::Follower)
        );
        assert!(
            simulation
                .trace()
                .contains(&TraceEvent::StalePersistenceDiscarded(NodeId(1)))
        );
    }

    #[test]
    fn dropping_vote_traffic_prevents_false_quorum() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        assert!(simulation.step().expect("candidate state persists"));
        assert_eq!(simulation.drop_all_messages(), 2);
        simulation
            .run_until_idle(8)
            .expect("simulation reaches quiescence");

        assert_eq!(
            simulation.role(NodeId(1)).expect("node exists"),
            Some(Role::Candidate)
        );
    }

    #[test]
    fn delayed_grant_remains_valid_after_voter_advances_term() {
        let mut simulation = three_nodes();
        simulation.timeout(NodeId(1)).expect("timeout is accepted");
        assert!(simulation.step().expect("candidate state persists"));
        assert!(simulation.step().expect("node two receives vote request"));
        assert!(simulation.step().expect("node two persists its vote"));

        simulation
            .timeout(NodeId(2))
            .expect("node two starts a later election");
        assert!(simulation.step().expect("node two persists the newer term"));
        simulation
            .run_until_idle(64)
            .expect("simulation reaches quiescence");

        let advanced = simulation
            .trace()
            .iter()
            .position(|event| {
                matches!(
                    event,
                    TraceEvent::HardStatePersisted(
                        NodeId(2),
                        HardState {
                            current_term: Term(2),
                            ..
                        }
                    )
                )
            })
            .expect("node two advances term");
        let delivered_old_grant = simulation
            .trace()
            .iter()
            .position(|event| {
                matches!(
                    event,
                    TraceEvent::MessageDelivered(Envelope {
                        from: NodeId(2),
                        to: NodeId(1),
                        message: Message::RequestVoteResponse {
                            term: Term(1),
                            vote_granted: true
                        }
                    })
                )
            })
            .expect("old durable grant is eventually delivered");
        assert!(advanced < delivered_old_grant);
    }
}
