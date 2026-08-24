# Delivery Roadmap

This roadmap turns the normative requirements in
[`specification.md`](specification.md) into gated implementation increments.
The specification remains authoritative when this document is less precise.

## Working rules

- Protocol code, its TLA+ model, deterministic tests, and evidence mapping
  advance in the same milestone.
- A milestone is not complete while its required evidence is missing or red.
- Safety-sensitive behavior is implemented first in a deterministic core; I/O
  adapters may execute only explicit effects emitted by that core.
- Every discovered counterexample becomes a retained regression.
- Versions are immutable inside a build. Automated updates are reviewed and
  accepted only after all gates pass.

## Gates

| Milestone | Exit evidence |
| --- | --- |
| M0 Foundation | ADR process, tool policy, requirement and verification manifests, green ordinary CI |
| M1 Election | Pure election core, persistence ordering tests, deterministic scenarios, election TLA+ model |
| M2 Replication | Durable static-cluster log, recovery, replication simulation, core Raft invariants |
| M3 CRUD | Transactional KV state machine, HTTP contract, deduplication, linearizable histories |
| M4 Snapshot | Atomic create/install/recover/compact behavior in code, model, and crash tests |
| M5 Membership | Staging and joint consensus with configuration-safety evidence |
| M6 Hardening | Bounds, diagnostics, TLS, compatibility policy, reproducible release artifacts |
| M7 Closure | Full verification matrix, trace validation, fault fuzzing, coverage and mutation gates |

Progress and evidence identifiers live in
[`verification/requirements.toml`](../verification/requirements.toml) and
[`verification/manifest.toml`](../verification/manifest.toml). Those files are
the machine-readable source for milestone status; this roadmap is explanatory.

