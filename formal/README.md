# Formal verification workspace

The formal suite evolves with implementation milestones. Rust protocol action
names and trace records must remain mapped to actions in this directory.

Pinned baseline versions are recorded in
[`verification/manifest.toml`](../verification/manifest.toml). Pull-request,
scheduled, and release checks use the immutable formal image defined by
`Dockerfile.formal`.

## Check classes

- TLC exhaustive finite-state matrices for each milestone model.
- Apalache inductive-invariant obligations for initialization, preservation,
  and implication of safety.
- Trace validation from deterministic simulation and sampled real executions.

The pull-request suite checks `RaftElection.cfg`, `Raft.cfg`,
`KvStateMachine.cfg`, `RaftSnapshot.cfg`, `RaftMembership.cfg`, and the fair
liveness scenarios in `RaftLiveness.cfg`. It also runs three Apalache
obligations over `RaftInductive.tla`: initialization establishes the inductive
invariant, every modeled transition preserves it, and it implies the published
core safety conjunction. Their adjacent Markdown files define abstraction maps
and bounds. Extended TLC configurations are reserved for scheduled and release
checks and are enumerated by `scripts/formal-matrix.sh`.

The liveness claim is conditional. Weak fairness is assumed for network healing,
quorum restart, elections, replication, commit, and apply. No progress is
claimed during permanent partition, permanent quorum loss, failed durable
storage, or unfair scheduling.

No passing claim is recorded unless a reproducible command, configuration,
state count, runtime, tool version, and specification hash are retained as an
artifact under `verification/evidence`.
