# RaftElection abstraction map

This bounded M1 model corresponds to `src/raft/election.rs`.

| Rust behavior | TLA+ action |
| --- | --- |
| `handle(ElectionTimeout)` | `BeginPreVote` |
| granted `PreVoteResponse` | `GrantPreVote` |
| Pre-Vote quorum | `BeginElection` |
| `acknowledge_persisted` for candidacy | `PersistCandidate` |
| first eligible `RequestVote` | `BeginGrantVote` |
| persistence acknowledgement for a vote | `PersistVote` |
| repeated or rejected vote request | `RepeatVote` / `RejectVote` |
| granted response handling | `ReceiveGrantedVote` |
| quorum promotion | `WinElection` |
| higher-term heartbeat or RPC response | `BeginHigherTerm` / `PersistHigherTerm` |
| accepted current-term heartbeat | `AcceptCurrentTermLeader` |
| failed `CheckQuorum` round | `CheckQuorumStepDown` |
| accepted `TimeoutNow` | `BeginTransferredElection` |
| network loss | `DropMessage` |

The model deliberately abstracts all logs as equally up to date. The Rust core
already enforces lexicographic last-log term/index ordering; M2 replaces this
abstraction with an explicit log and integrates heartbeats with AppendEntries.
`BeginHigherTerm` over-approximates authenticated higher-term RPC input by
allowing the environment to select any higher bounded term; it cannot create a
leader or vote without the same persistence barrier.
Transport duplication is represented by repeated generation or delivery of
semantically identical set elements; Rust separately retains duplicate-response
regressions.

Pre-Vote transport is deliberately collapsed to nondeterministic
`GrantPreVote` actions. This over-approximates which authenticated, eligible
peer responses arrive while preserving the essential refinement boundary: no
durable term/vote changes and no RequestVote exist until a prospective quorum
has formed. `CheckQuorumStepDown` can only remove a leader, so it cannot create
a second leader. `BeginTransferredElection` bypasses Pre-Vote exactly as
TimeoutNow does, but still crosses `PersistCandidate`, ordinary voting, and
`WinElection`; transfer therefore cannot grant leadership directly. Timing and
eventual transfer completion are outside this safety model.

`MessagesUseDurableTermAndVote` checks the persistence barrier: a granted vote
response can exist only when the sending node's durable vote history records
that term and recipient. Current hard state alone is intentionally insufficient:
an old granted response may remain delayed after its sender advances term.
`ElectionSafety` records every elected leader, including leaders that later
step down.

`RaftElection.cfg` is the fast three-node/one-term gate.
`RaftElection.extended.cfg` raises the term bound for scheduled exploration.
