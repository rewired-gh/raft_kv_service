# ADR 0001: Deterministic consensus core

- Status: accepted
- Date: 2026-08-24
- Supersedes: none
- Superseded by: none

## Context

Consensus safety must be testable without sockets, wall clocks, threads, or
real storage. Durable state must be committed before any message whose safety
depends on it becomes observable.

## Decision drivers

- Make transition behavior deterministic and exhaustively testable.
- Keep storage and transport failure handling explicit.
- Align Rust actions, simulator events, and TLA+ actions.
- Prevent accidental send-before-persist behavior.

## Considered options

1. Put Raft logic directly in asynchronous networking tasks.
2. Use a pure core that emits an ordered list of storage and transport effects.
3. Use a pure core with an explicit persistence acknowledgement barrier.

## Decision

Use a deterministic core with an explicit persistence barrier. A transition
that changes safety-critical durable state enters a pending state and exposes
only a persistence request. Messages and timer effects become available only
after the runtime acknowledges that exact durable state as committed.

The core owns protocol decisions. Runtime adapters own I/O execution and feed
outcomes back as explicit events; they do not infer retries or protocol state.

## Consequences

### Positive

- Sending a vote or term-bearing message before persistence is structurally
  difficult rather than merely discouraged.
- Unit tests and deterministic simulation exercise the same transitions used
  by the service.
- Crash points are explicit.

### Negative

- The core has a deliberate pending-persistence state.
- Runtime integration requires correlation and stale-acknowledgement checks.

## Verification

- Unit tests assert that safety-dependent messages are unavailable before the
  matching persistence acknowledgement.
- Simulation crashes nodes before and after every persistence request.
- Formal actions distinguish proposed durable state from committed durable
  state.

