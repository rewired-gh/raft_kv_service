# ADR 0005: Joint consensus membership operations

- Status: Accepted
- Date: 2026-08-24

## Decision

Represent membership changes as two first-class Raft log entries sharing a
128-bit operation ID: `Joint(C_old, C_new)` followed, only after its commit, by
`Stable(C_new)`. Each component configuration carries stable numeric node IDs
and explicit peer endpoints. Elections and commitment use the newest entry in
the local log; joint state requires independent majorities of both components.

New voters first run in explicit staging mode. The leader replicates its full
current log or snapshot to them without granting a vote or counting them for
commitment. The zero-lag threshold is intentionally conservative: every added
node must acknowledge the leader's current last log index before the joint
entry is admitted.

The operational API names the exact target and operation ID. Stable operations
are recorded transactionally with ordered application, included in snapshots,
and used to make retries idempotent across restart. A conflicting reuse is
rejected. Membership requests and queries are forwarded once to the known
leader and protected by a separate bearer-token boundary.

## Consequences

- Addition, removal, endpoint replacement, and leader removal share one path.
- Only one change can be active; a removed leader steps down at stable commit.
- Snapshot format v2 contains effective membership and completed operation
  records, making compacted recovery independent of startup voter lists.
- Peer endpoint discovery remains operator-supplied; automatic discovery and
  certificate issuance are outside the membership protocol.
- The bearer token is suitable for local experiments. M6 adds TLS/mTLS and
  production secret-loading policy before a release claim.
