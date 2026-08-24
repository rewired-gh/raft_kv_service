# ADR 0007: Verification closure

- Status: Accepted
- Date: 2026-08-24

## Context

Raft safety, Rust conformance, external linearizability, and operational fault
handling fail in different ways. A single model checker, test suite, or coverage
number cannot establish the North Star claim.

## Decision

M7 uses four mutually reinforcing evidence layers:

1. TLC explores bounded detailed election, replication, CRUD, snapshot,
   membership, and fair-liveness models. Apalache checks initialization,
   preservation, and safety implication for one typed inductive invariant.
2. Deterministic runtime traces check exact durability/effect ordering. Sampled
   JSONL traces from real node processes check monotonic durable state and
   snapshot boundaries during fault campaigns.
3. A multi-process harness executes three- and five-node clusters, concurrent
   CRUD, directional loss/delay/duplication, crashes, restarts, failover, and
   interrupted snapshot recovery. Campaign schema 2 retains the typed workload
   seed and absolute fault/process schedule for exact input replay, and checks
   histories with a bounded exact linearizability oracle. OS scheduling remains
   outside the deterministic replay claim.
4. Stable and nightly-pinned test jobs enforce ordinary tests, line/branch
   coverage, and scheduled sharded mutation analysis. Nightly is verification
   tooling only; the product toolchain remains stable Rust.

The history oracle has a retained exhaustive two-operation corpus evaluated
against a separately written literal CRUD specification. Reusing a prior log
revision is accepted only when the state machine identifies the request as a
durable duplicate.

## Claim boundary

The machine-checked claim applies to the abstract model and its stated bounds.
Trace validation is conformance evidence, not a formal refinement proof of all
Rust executions. Liveness requires eventual synchrony, a durable communicating
quorum, and fair scheduling. The trusted computing base remains the Rust
compiler, dependencies, SQLite, cryptography, OS/filesystem, transport, formal
tools, and hardware.

## Consequences

Pull requests run bounded formal and three-node adversarial checks. Weekly,
manual, and release-tag workflows run the extended model matrix, branch
coverage with three- and five-node campaigns, multiple fuzz seeds, and four
mutation shards. Any counterexample or meaningful surviving mutant becomes a
retained regression before release.
