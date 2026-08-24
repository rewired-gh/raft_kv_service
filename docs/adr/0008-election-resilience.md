# ADR 0008: Resilient elections and controlled leadership transfer

- Status: accepted
- Date: 2026-08-24

## Context

Basic Raft remains safe when an isolated follower repeatedly increments its
term or when an isolated leader continues accepting work it cannot commit, but
the resulting disruption and operational ambiguity are avoidable. Planned
maintenance also needs a way to move leadership without fabricating success or
bypassing the election proof.

## Decision

Use three related, explicitly non-lease mechanisms:

1. Pre-Vote is a volatile `PreCandidate` round for `current_term + 1`. It uses
   the active stable or joint quorum but never persists or resets a recipient's
   election timer. A known leader suppresses grants.
2. CheckQuorum runs once per minimum-election-timeout window. Any valid
   current-term response from an active voter proves recent activity, including
   a rejection. Failure makes the leader step down without changing term.
3. Leadership transfer is allowed only at a quiescent committed/applied
   boundary in a stable configuration. New proposals are fenced while the
   target catches up. A single `TimeoutNow(term, fence_index)` asks the target
   to start the normal durable election; it does not confer leadership.

The authenticated operational endpoint waits until the old leader observes the
target leading a higher term. Timeouts and ambiguous outcomes never return
success. Peer protocol v4 and trace schema v4 make the new message semantics
fail closed across incompatible versions.

## Consequences

- A minority cannot keep serving as leader after one bounded detection window.
- Pre-Vote improves availability but does not alter election safety.
- CheckQuorum is not a lease and cannot justify unlogged reads.
- Transfer can reject busy leaders rather than silently racing outstanding
  client or membership work.
- The TLA+ election model represents Pre-Vote quorum as nondeterministic
  prospective grants, CheckQuorum as leader removal, and TimeoutNow as entry to
  the existing persisted election path. It checks safety, not timed transfer
  completion.
