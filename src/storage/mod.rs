//! Durable Raft storage contract and implementations.

mod sqlite;

use std::{error::Error, fmt::Debug};

use crate::raft::{HardState, LogPosition, Term};

pub use sqlite::SqliteStorage;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntryPayload {
    Noop,
    Command(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEntry {
    pub index: u64,
    pub term: Term,
    pub payload: EntryPayload,
}

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
