# ADR 0002: SQLite transactional storage

- Status: accepted
- Date: 2026-08-24
- Supersedes: none
- Superseded by: none

## Context

Raft requires atomic, durable updates to term/vote state, log suffixes,
snapshots, applied state, and deduplication results. The project is intended to
study consensus rather than implement a database engine.

## Decision drivers

- Proven transactional and crash-recovery behavior.
- Atomic multi-table commits and consistent read snapshots.
- Ordered log-range access and integrity diagnostics.
- A minimal single-file operational footprint.
- Reproducible local and container builds on supported architectures.

## Considered options

1. SQLite through `rusqlite`.
2. RocksDB with transactions.
3. A pure-Rust embedded transactional KV engine.
4. A custom write-ahead log and state store.

## Decision

Use the latest reviewed stable `rusqlite` release with its bundled SQLite
feature behind an internal `RaftStorage` contract. SQLite is an implementation
detail and no SQL is exposed through service APIs.

File-backed stores use WAL journaling and `synchronous=FULL`. Safety-critical
acknowledgements occur only after the committing call succeeds. Schema and
format versions are explicit; startup validates identity, invariants, and
integrity before serving. Raft unsigned identifiers are stored as fixed-width
big-endian blobs, preserving their complete range and ordered byte comparison.

The runtime gives each Raft instance exclusive ownership of its database
connection on a dedicated blocking execution boundary. Async protocol tasks do
not call SQLite directly.

## Consequences

### Positive

- Raft state, log replacement, state-machine application, and snapshot
  metadata can share one atomic transaction.
- Recovery and corruption detection build on a widely deployed engine.
- The production image needs no external database service.

### Negative

- Bundled SQLite adds native compilation time and binary size.
- A single writer requires deliberate batching and backpressure.
- WAL and sidecar-file lifecycle must be included in backup and diagnostics.
- Encryption at rest remains a deployment-layer responsibility.

## Verification

- A backend-neutral storage conformance suite runs against memory and
  file-backed databases.
- Crash tests terminate before and after every durable operation.
- Startup runs schema checks and bounded integrity validation.
- Tests cover conflicting suffix replacement, monotonic hard state, atomic
  application, disk-full/error mapping, and reopen recovery.

