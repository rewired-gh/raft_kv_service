//! Deterministic Raft protocol state machines.
//!
//! Runtime adapters execute effects from this module. They must not infer or
//! reorder protocol actions. Safety-critical messages are hidden behind an
//! explicit durable-state acknowledgement barrier.

mod election;
mod log;
mod runtime;
pub mod simulator;
mod trace;

pub use election::{
    CoreError, Effect, Envelope, Event, HardState, LogMutation, LogPosition, Message, NodeId,
    RaftCore, ReplicationProgress, Role, Term, VoterSet,
};
pub use log::{EntryPayload, LogEntry};
pub use runtime::{DurableRaft, RuntimeError};
pub use trace::{
    ProtocolTrace, TRACE_SCHEMA_VERSION, TraceAction, TraceError, TraceMessage, TraceRecord,
};
