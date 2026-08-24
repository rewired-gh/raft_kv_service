use std::{
    collections::VecDeque,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Mutex,
};

use crate::{
    kv::ValueRecord,
    raft::{HardState, LogEntry, LogPosition, MembershipEntry},
};

use super::{
    AppendOutcome, AppliedEntry, RaftStorage, Snapshot, SnapshotStorage, StateMachineStorage,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    DiskFull,
    PermissionDenied,
    Read,
    Write,
    Transaction,
    Publication,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    LoadHardState,
    PersistHardState,
    LastLogPosition,
    Entry,
    EntriesFrom,
    AppendLocal,
    AppendFromLeader,
    LastApplied,
    ApplyCommitted,
    Value,
    MembershipOperation,
    CurrentSnapshot,
    CreateSnapshot,
    InstallSnapshot,
}

#[derive(Debug)]
pub enum InjectedStorageError<E> {
    Injected { point: FaultPoint, kind: FaultKind },
    SchedulePoisoned,
    Inner(E),
}

impl<E: Display> Display for InjectedStorageError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Injected { point, kind } => {
                write!(formatter, "injected {kind:?} failure at {point:?}")
            }
            Self::SchedulePoisoned => formatter.write_str("storage fault schedule lock poisoned"),
            Self::Inner(error) => Display::fmt(error, formatter),
        }
    }
}

impl<E: Error + 'static> Error for InjectedStorageError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Inner(error) => Some(error),
            Self::Injected { .. } | Self::SchedulePoisoned => None,
        }
    }
}

/// Test adapter that fails exact named storage calls before touching its inner store.
pub struct FaultInjectingStorage<S> {
    inner: S,
    schedule: Mutex<VecDeque<(FaultPoint, FaultKind)>>,
}

impl<S> FaultInjectingStorage<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            schedule: Mutex::new(VecDeque::new()),
        }
    }

    pub fn fail_next(&self, point: FaultPoint, kind: FaultKind) {
        if let Ok(mut schedule) = self.schedule.lock() {
            schedule.push_back((point, kind));
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    fn check<E>(&self, point: FaultPoint) -> Result<(), InjectedStorageError<E>> {
        let mut schedule = self
            .schedule
            .lock()
            .map_err(|_| InjectedStorageError::SchedulePoisoned)?;
        if schedule.front().is_some_and(|(next, _)| *next == point) {
            let (_, kind) = schedule
                .pop_front()
                .ok_or(InjectedStorageError::SchedulePoisoned)?;
            return Err(InjectedStorageError::Injected { point, kind });
        }
        Ok(())
    }
}

impl<S: RaftStorage> RaftStorage for FaultInjectingStorage<S> {
    type Error = InjectedStorageError<S::Error>;

    fn load_hard_state(&self) -> Result<HardState, Self::Error> {
        self.check(FaultPoint::LoadHardState)?;
        self.inner
            .load_hard_state()
            .map_err(InjectedStorageError::Inner)
    }

    fn persist_hard_state(&mut self, state: HardState) -> Result<(), Self::Error> {
        self.check(FaultPoint::PersistHardState)?;
        self.inner
            .persist_hard_state(state)
            .map_err(InjectedStorageError::Inner)
    }

    fn last_log_position(&self) -> Result<LogPosition, Self::Error> {
        self.check(FaultPoint::LastLogPosition)?;
        self.inner
            .last_log_position()
            .map_err(InjectedStorageError::Inner)
    }

    fn entry(&self, index: u64) -> Result<Option<LogEntry>, Self::Error> {
        self.check(FaultPoint::Entry)?;
        self.inner.entry(index).map_err(InjectedStorageError::Inner)
    }

    fn entries_from(&self, start: u64, limit: usize) -> Result<Vec<LogEntry>, Self::Error> {
        self.check(FaultPoint::EntriesFrom)?;
        self.inner
            .entries_from(start, limit)
            .map_err(InjectedStorageError::Inner)
    }

    fn append_local(&mut self, entry: &LogEntry) -> Result<(), Self::Error> {
        self.check(FaultPoint::AppendLocal)?;
        self.inner
            .append_local(entry)
            .map_err(InjectedStorageError::Inner)
    }

    fn append_from_leader(
        &mut self,
        previous: LogPosition,
        entries: &[LogEntry],
    ) -> Result<AppendOutcome, Self::Error> {
        self.check(FaultPoint::AppendFromLeader)?;
        self.inner
            .append_from_leader(previous, entries)
            .map_err(InjectedStorageError::Inner)
    }
}

impl<S: StateMachineStorage> StateMachineStorage for FaultInjectingStorage<S> {
    fn last_applied(&self) -> Result<u64, Self::Error> {
        self.check(FaultPoint::LastApplied)?;
        self.inner
            .last_applied()
            .map_err(InjectedStorageError::Inner)
    }

    fn apply_committed(&mut self, entries: &[LogEntry]) -> Result<Vec<AppliedEntry>, Self::Error> {
        self.check(FaultPoint::ApplyCommitted)?;
        self.inner
            .apply_committed(entries)
            .map_err(InjectedStorageError::Inner)
    }

    fn value(&self, key: &str) -> Result<Option<ValueRecord>, Self::Error> {
        self.check(FaultPoint::Value)?;
        self.inner.value(key).map_err(InjectedStorageError::Inner)
    }

    fn membership_operation(
        &self,
        operation_id: [u8; 16],
    ) -> Result<Option<MembershipEntry>, Self::Error> {
        self.check(FaultPoint::MembershipOperation)?;
        self.inner
            .membership_operation(operation_id)
            .map_err(InjectedStorageError::Inner)
    }
}

impl<S: SnapshotStorage> SnapshotStorage for FaultInjectingStorage<S> {
    fn current_snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        self.check(FaultPoint::CurrentSnapshot)?;
        self.inner
            .current_snapshot()
            .map_err(InjectedStorageError::Inner)
    }

    fn create_snapshot(
        &mut self,
        cluster_id: [u8; 16],
        membership: &MembershipEntry,
    ) -> Result<Snapshot, Self::Error> {
        self.check(FaultPoint::CreateSnapshot)?;
        self.inner
            .create_snapshot(cluster_id, membership)
            .map_err(InjectedStorageError::Inner)
    }

    fn install_snapshot(
        &mut self,
        expected_cluster_id: [u8; 16],
        snapshot: &Snapshot,
    ) -> Result<(), Self::Error> {
        self.check(FaultPoint::InstallSnapshot)?;
        self.inner
            .install_snapshot(expected_cluster_id, snapshot)
            .map_err(InjectedStorageError::Inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteStorage;

    #[test]
    fn scheduled_faults_are_ordered_typed_and_one_shot() {
        let mut storage =
            FaultInjectingStorage::new(SqliteStorage::open_in_memory().expect("in-memory storage"));
        storage.fail_next(FaultPoint::Value, FaultKind::PermissionDenied);
        storage.fail_next(FaultPoint::PersistHardState, FaultKind::Transaction);

        assert_eq!(
            storage.load_hard_state().expect("unmatched read passes"),
            HardState::default()
        );
        assert!(matches!(
            storage.value("key"),
            Err(InjectedStorageError::Injected {
                point: FaultPoint::Value,
                kind: FaultKind::PermissionDenied,
            })
        ));
        assert!(matches!(
            storage.persist_hard_state(HardState::default()),
            Err(InjectedStorageError::Injected {
                point: FaultPoint::PersistHardState,
                kind: FaultKind::Transaction,
            })
        ));
        storage
            .persist_hard_state(HardState::default())
            .expect("fault is consumed exactly once");
    }
}
