# Protocol trace contract

The protocol trace schema is version `2`. `ProtocolTrace` contains an
ordered list of records with contiguous sequence numbers. Records identify an
abstract Raft action, node, optional peer, term, log index, message kind, and
boolean outcome. Command payloads, key/value data, addresses, and credentials
are intentionally absent.

M4 adds snapshot publication, transfer start, transfer interruption, and
atomic installation actions. Snapshot records expose only included index and
term, never snapshot contents or their digest.

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

`validate_m4_abstraction` additionally enforces that snapshots are published
only through applied indexes, publication never regresses, transfers reference
the sender's published image, and installation follows a matching transfer.
Atomic installation advances the validator's virtual durable-log, commit, and
application bounds together.

The validator rejects unknown schema versions, missing required fields, and
non-contiguous traces. It is sampled conformance evidence: it validates the
implementation-to-model action boundary but does not prove that arbitrary Rust
executions satisfy the complete TLA+ next-state relation. M7 extends this seam
with continuous real-process trace capture and the full action-by-action model
relation.
