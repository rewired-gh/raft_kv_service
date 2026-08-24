# Raft membership model

`RaftMembership.tla` is the M5 feature model for staging, joint consensus,
final stable configuration, leader removal, and durable operation-ID
idempotency.

The abstraction maps `RaftCore::stage_peer` to `Stage`/`CatchUp`, configuration
log persistence and replication acknowledgements to `ProposeJoint`,
`Acknowledge`, and `CommitJoint`, and the second configuration entry to
`ProposeFinal`/`CommitFinal`. `Elect` uses the same separate-majority rule as
the Rust core. Storage and transport mechanics remain in `Raft.tla` and
`RaftSnapshot.tla`; this decomposition avoids claiming that the small feature
model proves the executable.

The fast configuration checks three nodes, an initial two-node configuration,
all non-empty target subsets, leader changes, and one operation identity for
arbitrary execution length within the finite state space. The release matrix
adds larger node and operation domains.
