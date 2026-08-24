# Protocol trace contract

The protocol trace schema is version `4`. `ProtocolTrace` contains an
ordered list of records with contiguous sequence numbers. Records identify an
abstract Raft action, node, optional peer, term, log index, message kind, and
boolean outcome. Command payloads, key/value data, addresses, and credentials
are intentionally absent.

Version 4 adds `pre_vote`, `pre_vote_response`, and `timeout_now`. Pre-Vote
records deliberately allow a prospective term one step beyond durable hard
state; RequestVote and TimeoutNow remain bound to durable current term.

M4 adds snapshot publication, transfer start, transfer interruption, and
atomic installation actions. M7 adds a mandatory terminal record to file
traces. Snapshot records expose only included index and term, never snapshot
contents or their digest.

`ProtocolTrace::from_simulator` converts deterministic executions to this
stable representation. A node configured with `raft.trace_file` also creates
an append-only JSON Lines trace. A 20 ms observer samples durable diagnostics,
emits only monotonic state changes, and flushes every record. A graceful close
writes `trace_terminated`; after a deliberate process kill, the harness writes
the same marker only after the child has exited. A valid JSONL prefix without
that marker is therefore rejected as silent truncation. The real-process fuzz
harness validates every node/attempt trace before a campaign can pass;
malformed, silently truncated, or unknown-version files fail closed.

`validate_m2_abstraction` enforces the observable
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
non-contiguous traces. This is sampled conformance evidence, not a claim that
arbitrary Rust executions refine every TLA+ transition: the diagnostics sampler
can coalesce intermediate states and cannot observe an event that occurs wholly
between samples. The deterministic runtime trace covers exact effect ordering;
the real-process trace independently covers durable monotonicity and snapshot
boundaries under faults. Together they make model drift detectable without
overstating trace refinement as a machine-checked refinement proof.
