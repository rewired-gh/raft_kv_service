//! Deterministic Raft protocol state machines.
//!
//! Runtime adapters execute effects from this module. They must not infer or
//! reorder protocol actions. Safety-critical messages are hidden behind an
//! explicit durable-state acknowledgement barrier.

mod election;
pub mod simulator;

pub use election::{
    CoreError, Effect, Envelope, Event, HardState, LogPosition, Message, NodeId, RaftCore, Role,
    Term, VoterSet,
};
