# RaftInductive proof model

`RaftInductive.tla` is the typed, finite abstraction used for machine-checked
induction. It combines the durable safety state that is split across the more
detailed TLC models: terms and votes, elections, log reconciliation, commit and
apply, snapshots, durable acknowledgements, membership, crashes, restarts, and
network loss.

`scripts/apalache.sh check` checks three independent obligations with Apalache:

1. `Init => InductiveInvariant`;
2. `InductiveInvariant /\ Next => InductiveInvariant'` for every transition;
3. `InductiveInvariant => CoreSafety`.

`CoreSafety` contains election safety, log matching, leader append-only, leader
completeness, state-machine safety, committed-prefix preservation, snapshot
safety, no acknowledgement before durability, and configuration safety.

The model intentionally abstracts payload bytes, RPC encoding, storage-engine
internals, and timing. Its proof therefore establishes the safety conjunction
for the modeled transition system, while Rust trace, simulation, storage, and
process tests provide the implementation-conformance evidence described in
`docs/adr/0007-verification-closure.md`.
