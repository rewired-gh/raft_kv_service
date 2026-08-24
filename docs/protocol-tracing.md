# Protocol trace contract

The M2 protocol trace schema is version `1`. `ProtocolTrace` contains an
ordered list of records with contiguous sequence numbers. Records identify an
abstract Raft action, node, optional peer, term, log index, message kind, and
boolean outcome. Command payloads, key/value data, addresses, and credentials
are intentionally absent.

`ProtocolTrace::from_simulator` converts deterministic executions to this
stable representation. `validate_m2_abstraction` enforces the observable
persistence ordering used by `formal/RaftElection.tla` and `formal/Raft.tla`:

- terms never regress and term-bearing messages do not precede durable term
  state;
- AppendEntries never advertises an entry beyond the sender's durable log;
- a successful AppendEntries response never acknowledges beyond the
  follower's durable log;
- commit indexes are monotonic and never exceed durable log state;
- ordered application acknowledgements are monotonic and never exceed commit.

The validator rejects unknown schema versions, missing required fields, and
non-contiguous traces. It is sampled conformance evidence: it validates the
implementation-to-model action boundary but does not prove that arbitrary Rust
executions satisfy the complete TLA+ next-state relation. M7 extends this seam
with real-process trace capture and full action-by-action validation.
