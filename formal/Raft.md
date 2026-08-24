# Static replication model and abstraction map

`Raft.tla` is the M2 composition model for durable log replication in a
static cluster. Diego Ongaro's canonical TLA+ specification is reference
material; this model is intentionally local because it represents the
implementation's explicit persistence effects.

The checked composition boundary is:

- `Elect` abstracts a completed election from `RaftElection.tla`. Its quorum
  has durably cast at most one vote per term and every voter has performed the
  Raft log-freshness check.
- `BeginLeaderAppend` / `PersistLeaderAppend` map to `Effect::PersistLog` and
  `Core::acknowledge_log_persisted` for a leader-originated entry.
- `AcceptAppend` / `PersistFollowerAppend` map to the same barrier for follower
  suffix replacement. A successful response exists only after persistence.
- `SendAppend`, `RejectAppend`, `HandleResponse`, and `IgnoreResponse` map to
  the corresponding `AppendEntries` transitions in the deterministic core.
- `AdvanceCommit` maps to the leader's current-term quorum commit rule.
- `Crash` discards volatile role, replication progress, commit knowledge, and
  staged writes while preserving durable term, vote, and log.
- `Drop` and set-valued messages model loss, arbitrary delay, reordering, and
  observationally idempotent duplication.

`committed`, `elected`, and `leaderLogHistory` are specification-only history
variables. They make safety claims state predicates and do not correspond to
production storage.

The pull-request configuration is deliberately small and exhaustive. It has
one in-flight message at a time, which still covers loss, arbitrary delay,
retry, crashes, follower replacement, and quorum replication over sequential
deliveries. The extended configuration raises the queue, term, log, and value
bounds. Larger configurations are not evidence for unbounded correctness and
will be complemented by the M7 inductive-invariant checks.
