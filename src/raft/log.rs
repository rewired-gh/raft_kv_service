//! Raft log domain types shared by the protocol core and storage adapters.

use super::Term;

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
