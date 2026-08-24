# Formal verification workspace

The formal suite evolves with implementation milestones. Rust protocol action
names and trace records must remain mapped to actions in this directory.

Pinned baseline versions are recorded in
[`verification/manifest.toml`](../verification/manifest.toml). CI containers
will use immutable digests before formal checks become release gates.

## Planned check classes

- TLC exhaustive finite-state matrices for each milestone model.
- Apalache inductive-invariant obligations for initialization, preservation,
  and implication of safety.
- Trace validation from deterministic simulation and sampled real executions.

No passing claim is recorded until a reproducible command, configuration,
state count, runtime, tool version, and specification hash are retained as an
artifact.

