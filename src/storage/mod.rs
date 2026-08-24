//! Durable Raft storage contract and implementations.

mod fault;
mod sqlite;

use std::{error::Error, fmt::Debug};

use crate::{
    kv::{CommandResult, ValueRecord},
    raft::{HardState, LogEntry, LogPosition, MembershipEntry, NodeId, Term},
};

pub const SNAPSHOT_FORMAT_VERSION: u32 = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub format_version: u32,
    pub cluster_id: [u8; 16],
    pub last_included: LogPosition,
    pub voters: Vec<NodeId>,
    pub membership: MembershipEntry,
    pub payload: Vec<u8>,
    pub digest: [u8; 32],
}

pub use fault::{FaultInjectingStorage, FaultKind, FaultPoint, InjectedStorageError};
pub use sqlite::{STORAGE_SCHEMA_VERSION, SqliteStorage, StorageLimits};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    Appended { last_index: u64 },
    Rejected { local_term: Option<Term> },
}

pub trait RaftStorage {
    type Error: Error + Send + Sync + 'static;

    fn load_hard_state(&self) -> Result<HardState, Self::Error>;
    fn persist_hard_state(&mut self, hard_state: HardState) -> Result<(), Self::Error>;
    fn last_log_position(&self) -> Result<LogPosition, Self::Error>;
    fn entry(&self, index: u64) -> Result<Option<LogEntry>, Self::Error>;
    fn entries_from(&self, start: u64, limit: usize) -> Result<Vec<LogEntry>, Self::Error>;
    fn append_local(&mut self, entry: &LogEntry) -> Result<(), Self::Error>;
    fn append_from_leader(
        &mut self,
        previous: LogPosition,
        entries: &[LogEntry],
    ) -> Result<AppendOutcome, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedEntry {
    pub index: u64,
    pub result: Option<CommandResult>,
}

pub trait StateMachineStorage: RaftStorage {
    fn last_applied(&self) -> Result<u64, Self::Error>;
    fn apply_committed(&mut self, entries: &[LogEntry]) -> Result<Vec<AppliedEntry>, Self::Error>;
    fn value(&self, key: &str) -> Result<Option<ValueRecord>, Self::Error>;
    fn membership_operation(
        &self,
        operation_id: [u8; 16],
    ) -> Result<Option<MembershipEntry>, Self::Error>;
}

pub trait SnapshotStorage: StateMachineStorage {
    fn current_snapshot(&self) -> Result<Option<Snapshot>, Self::Error>;
    fn create_snapshot(
        &mut self,
        cluster_id: [u8; 16],
        membership: &MembershipEntry,
    ) -> Result<Snapshot, Self::Error>;
    fn install_snapshot(
        &mut self,
        expected_cluster_id: [u8; 16],
        snapshot: &Snapshot,
    ) -> Result<(), Self::Error>;
}
