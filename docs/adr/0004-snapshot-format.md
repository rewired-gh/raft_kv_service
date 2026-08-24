# ADR 0004: Logical transactional snapshots

- Status: Accepted
- Date: 2026-08-24

## Decision

Persist a versioned canonical logical snapshot inside the same SQLite database
as Raft and application state. The payload contains the cluster identity,
included index and term, ordered voter IDs, configured client-session bound,
ordered KV rows, and ordered deduplication records. SHA-256 binds the complete
payload; the digest is also its transfer identity.

Snapshot publication, state restoration, virtual-prefix metadata, and covered
log deletion use one immediate SQLite transaction. Recovery treats the
included index and term as the entry immediately preceding the retained log
suffix. A matching suffix is retained during installation; a conflicting one
is discarded.

Peers transfer bounded chunks over a dedicated gRPC method. Every chunk repeats
the immutable snapshot identity, metadata, total size, offset, and digest. A
receiver keeps one bounded staging transfer, accepts byte-identical duplicate
chunks, resumes from its reported offset, and publishes only after complete
digest and format validation.

## Consequences

- A crash exposes either the prior snapshot/log or the complete replacement.
- Losing an in-progress staging buffer is harmless; the leader restarts at
  offset zero and the transfer remains idempotent.
- The logical format is independent of SQLite page layout and carries an
  explicit format version, but schema evolution requires a tested decoder and
  migration policy.
- SHA-256 and SQLite durability remain in the trusted computing base.
