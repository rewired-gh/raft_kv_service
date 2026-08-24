//! Fail-closed adapter between the deterministic core and durable storage.

use std::{
    collections::VecDeque,
    error::Error,
    fmt::{self, Display, Formatter},
};

use crate::storage::{AppendOutcome, RaftStorage};

use super::{CoreError, Effect, EntryPayload, Event, LogMutation, NodeId, RaftCore, VoterSet};

#[derive(Debug)]
pub enum RuntimeError<E> {
    Core(CoreError),
    Storage(E),
    StorageDivergence(LogMutation),
    Failed,
}

impl<E: Display> Display for RuntimeError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(error) => write!(formatter, "Raft core rejected the operation: {error}"),
            Self::Storage(error) => write!(formatter, "durable Raft storage failed: {error}"),
            Self::StorageDivergence(mutation) => write!(
                formatter,
                "core and storage logs diverged at preceding index {}",
                mutation.previous.index
            ),
            Self::Failed => formatter.write_str("Raft runtime is permanently failed"),
        }
    }
}

impl<E: Error + 'static> Error for RuntimeError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Core(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::StorageDivergence(_) | Self::Failed => None,
        }
    }
}

/// Owns the only legal path from core persistence effects to durable storage.
///
/// Persistence effects are consumed internally. Callers receive only network,
/// timer, and commit notifications, so they cannot accidentally send a safety-
/// critical response before its write has completed.
pub struct DurableRaft<S: RaftStorage> {
    core: RaftCore,
    storage: S,
    failed: bool,
}

impl<S: RaftStorage> DurableRaft<S> {
    pub fn recover(
        id: NodeId,
        voters: VoterSet,
        storage: S,
    ) -> Result<Self, RuntimeError<S::Error>> {
        let hard_state = storage.load_hard_state().map_err(RuntimeError::Storage)?;
        let log = storage
            .entries_from(1, usize::MAX)
            .map_err(RuntimeError::Storage)?;
        let core = RaftCore::new(id, voters, hard_state, log).map_err(RuntimeError::Core)?;
        Ok(Self {
            core,
            storage,
            failed: false,
        })
    }

    pub fn handle(&mut self, event: Event) -> Result<Vec<Effect>, RuntimeError<S::Error>> {
        self.ensure_active()?;
        let effects = self.core.handle(event).map_err(RuntimeError::Core)?;
        self.execute(effects)
    }

    pub fn propose(
        &mut self,
        payload: EntryPayload,
    ) -> Result<(u64, Vec<Effect>), RuntimeError<S::Error>> {
        self.ensure_active()?;
        let (index, effects) = self.core.propose(payload).map_err(RuntimeError::Core)?;
        Ok((index, self.execute(effects)?))
    }

    pub fn heartbeat(&mut self) -> Result<Vec<Effect>, RuntimeError<S::Error>> {
        self.ensure_active()?;
        let effects = self.core.heartbeat().map_err(RuntimeError::Core)?;
        self.execute(effects)
    }

    pub fn acknowledge_applied(
        &mut self,
        last_index: u64,
    ) -> Result<Vec<Effect>, RuntimeError<S::Error>> {
        self.ensure_active()?;
        let effects = self
            .core
            .acknowledge_applied(last_index)
            .map_err(RuntimeError::Core)?;
        self.execute(effects)
    }

    pub fn core(&self) -> &RaftCore {
        &self.core
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn into_storage(self) -> S {
        self.storage
    }

    fn ensure_active(&self) -> Result<(), RuntimeError<S::Error>> {
        if self.failed {
            Err(RuntimeError::Failed)
        } else {
            Ok(())
        }
    }

    fn execute(&mut self, effects: Vec<Effect>) -> Result<Vec<Effect>, RuntimeError<S::Error>> {
        let mut pending = VecDeque::from(effects);
        let mut external = Vec::new();
        while let Some(effect) = pending.pop_front() {
            let continuation = match effect {
                Effect::PersistHardState(hard_state) => {
                    if let Err(error) = self.storage.persist_hard_state(hard_state) {
                        self.failed = true;
                        return Err(RuntimeError::Storage(error));
                    }
                    match self.core.acknowledge_persisted(hard_state) {
                        Ok(effects) => effects,
                        Err(error) => {
                            self.failed = true;
                            return Err(RuntimeError::Core(error));
                        }
                    }
                }
                Effect::PersistLog(mutation) => {
                    let outcome = self
                        .storage
                        .append_from_leader(mutation.previous, &mutation.entries);
                    match outcome {
                        Ok(AppendOutcome::Appended { .. }) => {}
                        Ok(AppendOutcome::Rejected { .. }) => {
                            self.failed = true;
                            return Err(RuntimeError::StorageDivergence(mutation));
                        }
                        Err(error) => {
                            self.failed = true;
                            return Err(RuntimeError::Storage(error));
                        }
                    }
                    match self.core.acknowledge_log_persisted(mutation) {
                        Ok(effects) => effects,
                        Err(error) => {
                            self.failed = true;
                            return Err(RuntimeError::Core(error));
                        }
                    }
                }
                external_effect => {
                    external.push(external_effect);
                    Vec::new()
                }
            };
            pending.extend(continuation);
        }
        Ok(external)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        convert::Infallible,
    };

    use crate::{
        raft::{Envelope, HardState, LogEntry, LogPosition, Role, Term},
        storage::{AppendOutcome, RaftStorage, SqliteStorage},
    };

    use super::*;

    fn voters() -> VoterSet {
        VoterSet::new([NodeId(1), NodeId(2), NodeId(3)]).expect("valid voters")
    }

    fn messages(effects: Vec<Effect>) -> impl Iterator<Item = Envelope> {
        effects.into_iter().filter_map(|effect| match effect {
            Effect::Send(envelope) => Some(envelope),
            Effect::ResetElectionTimer
            | Effect::CommitIndexAdvanced(_)
            | Effect::ApplyCommitted(_) => None,
            Effect::PersistHardState(_) | Effect::PersistLog(_) => {
                panic!("runtime leaked an internal persistence effect")
            }
        })
    }

    #[test]
    fn three_durable_nodes_elect_replicate_and_recover() {
        let voters = voters();
        let mut nodes = [NodeId(1), NodeId(2), NodeId(3)]
            .into_iter()
            .map(|id| {
                let storage = SqliteStorage::open_in_memory().expect("open storage");
                let node = DurableRaft::recover(id, voters.clone(), storage).expect("recover");
                (id, node)
            })
            .collect::<BTreeMap<_, _>>();

        let effects = nodes
            .get_mut(&NodeId(1))
            .expect("node one")
            .handle(Event::ElectionTimeout)
            .expect("start election");
        let mut queue = messages(effects).collect::<VecDeque<_>>();
        while let Some(envelope) = queue.pop_front() {
            let recipient = envelope.to;
            let effects = nodes
                .get_mut(&recipient)
                .expect("recipient")
                .handle(Event::Receive(envelope))
                .expect("deliver");
            queue.extend(messages(effects));
        }

        let leader = nodes.get(&NodeId(1)).expect("leader");
        assert_eq!(leader.core().role(), Role::Leader);
        assert!(leader.core().leader_ready());
        assert_eq!(leader.core().commit_index(), 1);
        for node in nodes.values() {
            assert_eq!(
                node.storage().entry(1).expect("read log"),
                Some(LogEntry {
                    index: 1,
                    term: Term(1),
                    payload: EntryPayload::Noop,
                })
            );
        }

        let storage = nodes.remove(&NodeId(2)).expect("node two").into_storage();
        let recovered = DurableRaft::recover(NodeId(2), voters, storage).expect("recover node two");
        assert_eq!(recovered.core().hard_state().current_term, Term(1));
        assert_eq!(
            recovered.core().last_log_position(),
            LogPosition {
                index: 1,
                term: Term(1)
            }
        );
    }

    #[derive(Debug)]
    struct FailingStorage;

    impl RaftStorage for FailingStorage {
        type Error = Infallible;

        fn load_hard_state(&self) -> Result<HardState, Self::Error> {
            Ok(HardState::default())
        }
        fn persist_hard_state(&mut self, _: HardState) -> Result<(), Self::Error> {
            Ok(())
        }
        fn last_log_position(&self) -> Result<LogPosition, Self::Error> {
            Ok(LogPosition::default())
        }
        fn entry(&self, _: u64) -> Result<Option<LogEntry>, Self::Error> {
            Ok(None)
        }
        fn entries_from(&self, _: u64, _: usize) -> Result<Vec<LogEntry>, Self::Error> {
            Ok(Vec::new())
        }
        fn append_local(&mut self, _: &LogEntry) -> Result<(), Self::Error> {
            Ok(())
        }
        fn append_from_leader(
            &mut self,
            previous: LogPosition,
            _: &[LogEntry],
        ) -> Result<AppendOutcome, Self::Error> {
            Ok(AppendOutcome::Rejected {
                local_term: (previous.index == 0).then_some(Term(99)),
            })
        }
    }

    #[test]
    fn storage_divergence_permanently_fails_the_runtime() {
        let voters = VoterSet::new([NodeId(1)]).expect("voters");
        let mut runtime = DurableRaft::recover(NodeId(1), voters, FailingStorage).expect("recover");
        let error = runtime
            .handle(Event::ElectionTimeout)
            .expect_err("must fail closed");
        assert!(matches!(error, RuntimeError::StorageDivergence(_)));
        assert!(runtime.is_failed());
        assert!(matches!(
            runtime.handle(Event::ElectionTimeout),
            Err(RuntimeError::Failed)
        ));
    }

    #[test]
    fn persistence_effects_never_escape_the_adapter() {
        let voters = VoterSet::new([NodeId(1)]).expect("voters");
        let storage = SqliteStorage::open_in_memory().expect("storage");
        let mut runtime = DurableRaft::recover(NodeId(1), voters, storage).expect("recover");
        let effects = runtime.handle(Event::ElectionTimeout).expect("elect");
        assert!(
            effects.iter().all(|effect| !matches!(
                effect,
                Effect::PersistHardState(_) | Effect::PersistLog(_)
            ))
        );
        assert_eq!(runtime.core().role(), Role::Leader);
        assert!(runtime.core().leader_ready());
    }
}
