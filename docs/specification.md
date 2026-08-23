# Raft KV Service Specification

- Status: Draft 0.1
- Last updated: 2026-08-24

## 1. Purpose

This document defines the product boundary, externally observable behavior,
correctness requirements, verification claims, test strategy, and completion
criteria for `raft-kv-service`.

The project is a deliberately small distributed key-value service built to
study Raft deeply. It favors clarity, correctness, reproducibility, and
evidence over feature count or benchmark leadership.

Implementation details that do not affect these requirements belong in
Architecture Decision Records (ADRs), not in this specification.

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to
be interpreted as described by RFC 2119 and RFC 8174 when written in uppercase.

## 2. Vision

The finished system is a distributed, persistent, linearizable key-value
service that:

- implements Raft in this repository rather than embedding a Raft library;
- supports only Create, Read, Update, and Delete operations;
- survives supported process, network, and storage-recovery failures;
- supports snapshots and safe dynamic cluster membership;
- has a formal model of its exact consensus protocol;
- connects the formal model to observed implementation behavior;
- provides layered deterministic, property-based, integration, and
  adversarial testing;
- can be explored locally through a minimal, reproducible container workflow.

This is a toy project in product scope, not in engineering discipline.

## 3. Goals

The system MUST provide:

1. A self-implemented Raft consensus core.
2. A single replicated state machine for one key-value namespace.
3. Linearizable CRUD operations.
4. Durable acknowledged mutations.
5. At-most-once application of retried client mutations.
6. Crash recovery from durable log and snapshot state.
7. Log compaction and snapshot installation.
8. Dynamic membership through Raft joint consensus.
9. Observable, bounded, and diagnosable runtime behavior.
10. A precise and continuously checked formal-verification claim.
11. Reproducible local one-node, three-node, and fault-injection workflows.
12. Fully automated end-to-end fuzz and fault testing with linearizability
    validation.

## 4. Non-goals

The first complete system MUST NOT expand into:

- transactions spanning multiple keys;
- compare-and-swap as a separate user operation;
- range scans, prefix scans, or key listing;
- watches or change streams;
- key expiration or TTLs;
- secondary indexes;
- sharding or Multi-Raft;
- geo-distributed latency optimization;
- stale or eventually consistent reads;
- Byzantine fault tolerance;
- automatic recovery from permanent quorum loss;
- arbitrary disk-corruption repair;
- a general-purpose authentication or authorization service;
- every optimization implemented by production Raft systems.

Throughput and latency are measured to catch regressions, but benchmark
leadership is not a goal.

## 5. System model

### 5.1 Node

A node is one operating-system process with:

- a stable cluster identifier;
- a stable node identifier;
- a durable data directory;
- one client-facing HTTP interface;
- one peer-facing RPC interface;
- one logical operational interface, which MAY share a listener locally;
- at most one active Raft instance for the namespace.

Running two processes concurrently with the same node identifier and data
directory is invalid and MUST be rejected where it can be detected.

### 5.2 Cluster

A cluster is one Raft group replicating one logical key-value namespace.

- Local development MUST default to three voting nodes.
- One-node clusters MUST be supported for development and recovery tests.
- Three-node and five-node clusters MUST be supported.
- A healthy cluster tolerates the unavailability of any minority of its
  current voting configuration.
- Only one membership change MAY be in progress at a time.

### 5.3 Timing and network

Safety MUST NOT depend on bounded message latency or synchronized clocks.

The network MAY:

- delay messages for an arbitrary duration;
- lose messages;
- duplicate messages;
- reorder messages;
- partition any subset of nodes;
- reconnect nodes after arbitrary delay.

Liveness depends on eventual synchrony: a quorum of correct nodes must
eventually communicate within sufficiently stable timing bounds and receive
fair execution time.

### 5.4 Process and storage failures

Nodes MAY stop at any instruction and later restart. Volatile memory is lost
on restart; successfully persisted state remains available.

The system assumes:

- the selected transactional storage engine honors its documented atomicity
  and durability contract;
- durable media does not maliciously fabricate valid data;
- detected corruption causes a node to fail safely rather than continue with
  uncertain state;
- a permanently lost quorum requires operator recovery outside this protocol.

## 6. Data model

### 6.1 Keys

Keys are non-empty UTF-8 strings restricted to the URI-unreserved character
set: ASCII letters, digits, `-`, `.`, `_`, and `~`.

This restriction keeps HTTP resource identifiers canonical and human-usable.
The maximum encoded key length MUST be configurable and MUST have a safe
default documented by the implementation.

### 6.2 Values

Values are opaque byte sequences. The service MUST NOT interpret their
encoding.

The maximum value size MUST be configurable, consistently enforced before log
admission, and bounded by a safe default. Oversized values MUST NOT be partly
replicated or applied.

### 6.3 Revision

Every successful mutation receives a monotonically increasing cluster
revision derived from its committed Raft log position. A key's entity tag
identifies the revision of its latest successful mutation.

Revisions MUST NOT be reused within a cluster, including after restart,
snapshot installation, or leadership change.

## 7. CRUD semantics

All successful operations are linearizable. The linearization point is within
the interval between request invocation and response completion.

### 7.1 Create

Create succeeds only when the key is absent at the command's linearization
point.

- Success creates exactly one value and revision.
- Creating an existing key fails without modifying it.
- A retry with the same client request identity returns the original result.

### 7.2 Read

Read returns the value and revision of the key at its linearization point.

- An absent key returns not-found.
- The initial implementation MUST establish reads through the replicated log.
- A later quorum-confirmed read-only optimization MAY replace logged reads only
  after it is formally modeled and passes the same history checks.
- Lease reads are outside the required scope.

### 7.3 Update

Update succeeds only when the key exists at the command's linearization point.

- An exact revision precondition MAY additionally be supplied.
- A failed existence or revision precondition MUST NOT modify the key.
- Success replaces the complete value and assigns a new revision.

### 7.4 Delete

Delete succeeds only when the key exists at the command's linearization point.

- An exact revision precondition MAY additionally be supplied.
- Success removes the key and assigns a mutation revision.
- Deleting an absent key returns not-found and does not create a revision.

### 7.5 Request deduplication

Every client mutation MUST carry:

- a stable client identifier; and
- a monotonically increasing request sequence number.

A client MUST have at most one mutation in flight. The replicated state
machine stores the greatest applied sequence and its result for each client.

- Repeating the latest sequence returns the stored result without reapplying.
- A sequence lower than the latest sequence is rejected as stale.
- A sequence with a gap is rejected.
- Deduplication state is part of the snapshot and replicated state machine.

The first complete version MAY retain client records indefinitely. A bounded
session-expiration protocol requires its own specification before adoption.

## 8. Client HTTP API

The wire contract will be versioned under `/v1` and described by a committed
OpenAPI 3.1 document.

The initial resource shape is:

| Operation | Request | Required precondition | Success |
| --- | --- | --- | --- |
| Create | `PUT /v1/keys/{key}` | `If-None-Match: *` | `201 Created` |
| Read | `GET /v1/keys/{key}` | none | `200 OK` |
| Update | `PUT /v1/keys/{key}` | `If-Match: *` or exact ETag | `200 OK` |
| Delete | `DELETE /v1/keys/{key}` | `If-Match: *` or exact ETag | `204 No Content` |

Mutation requests without the required condition MUST return `428 Precondition
Required`. Failed conditions MUST return `412 Precondition Failed`.

Values use `application/octet-stream`. Successful reads and writes expose the
current strong `ETag`. Error responses use RFC 9457
`application/problem+json` with stable machine-readable problem types.

Mutations carry the client identifier and request sequence in versioned,
documented request headers. Request correlation and mutation identity MUST be
separate concepts.

Any cluster node MAY accept a client request:

- the leader handles it locally;
- a follower SHOULD forward it to its known leader while preserving the
  request identity and remaining deadline;
- a node without a usable leader returns `503 Service Unavailable` and SHOULD
  include `Retry-After` when meaningful;
- forwarding MUST have a hop limit and MUST NOT loop.

A timeout or disconnected response does not imply that a mutation failed.
Clients retry ambiguous mutations with the same identity.

## 9. Peer RPC API

Peer traffic uses a separately versioned Protobuf schema over gRPC on a
distinct listener and trust boundary.

The required protocol operations are:

- `RequestVote`;
- `AppendEntries`, including empty heartbeats and entry batches;
- `InstallSnapshot`, using bounded chunks or streaming;
- client-request forwarding to the leader;
- operational membership requests that ultimately become replicated log
  entries.

Every peer request MUST identify the cluster, sender, intended recipient,
protocol version, and applicable term. RPCs MUST have explicit deadlines and
bounded message, queue, and concurrency limits.

Generic transport retries MUST NOT hide protocol behavior. Raft owns retry,
backoff, deduplication, batching, and ordering decisions.

Backward-compatible schema evolution MUST preserve field numbers and reserve
removed fields. Incompatible peer protocol versions MUST fail explicitly.

## 10. Raft protocol requirements

### 10.1 Roles and terms

A node is exactly one of follower, candidate, or leader in each term.

- Terms increase monotonically.
- Discovering a higher term causes a node to persist that term and become a
  follower before taking further protocol action.
- At most one vote is granted by a node in a term.
- A candidate's log must be at least as up-to-date as the voter's log.
- Election timeouts are randomized independently.
- Heartbeat intervals are shorter than the minimum election timeout.

### 10.2 Log replication

Log entries contain at least an index, term, and deterministic command.

- Leaders append entries; followers never originate log entries.
- AppendEntries consistency checks use the preceding index and term.
- Conflicting suffixes are truncated before replacement entries are appended.
- A leader advances its commit index only using the Raft quorum rule and the
  current-term commitment rule.
- Followers never expose or apply uncommitted entries.
- Applied entries are processed exactly once and in increasing index order.

### 10.3 Leader initialization

A newly elected leader appends a no-op entry in its current term before relying
on prior-term entries for client-visible progress. It initializes replication
progress for every peer and discovers conflicts without assuming follower log
lengths.

### 10.4 Snapshots and log compaction

A snapshot is a consistent image containing:

- all key-value state;
- client deduplication state;
- the last included log index and term;
- the effective cluster configuration;
- format and integrity metadata.

Snapshot creation MUST NOT block correctness-critical progress for an
unbounded duration. Publication is atomic: after a crash, a node sees either
the prior valid snapshot or the new valid snapshot.

A follower behind the retained log prefix receives and atomically installs a
snapshot. Interrupted or duplicate transfers MUST be safely retryable.

Log entries covered by a durable snapshot MAY be removed only after the node
can recover the state machine and required Raft metadata from that snapshot.

### 10.5 Dynamic membership

Membership changes use joint consensus with overlapping old and new voting
configurations.

- The transition is `C_old -> C_old,new -> C_new`.
- Joint configuration decisions require quorums from both configurations.
- Only one transition may be active.
- A prospective member must catch up sufficiently before becoming a voter.
- A removed node must not continue acting as a member after learning the
  committed removal.
- Removing the current leader must complete safely without special-case safety
  assumptions.
- Membership state is replicated, persisted, snapshotted, and formally
  modeled.

## 11. Persistence contract

The project uses an established embedded transactional storage engine behind
an internal storage abstraction. The engine choice is an ADR; the following
contract is normative.

The storage implementation MUST support atomic write batches, durable commit,
ordered log-range access, consistent snapshots, and corruption detection.

A node MUST durably persist:

- `current_term` and `voted_for` before granting or reporting a vote;
- a newly observed higher term before responding in that term;
- log truncation and replacement entries before acknowledging AppendEntries;
- its own new log entry before counting itself toward a quorum;
- snapshot content and metadata before discarding covered log entries;
- state-machine changes and `last_applied` atomically;
- deduplication results atomically with the mutation they describe;
- committed membership state as part of ordered state-machine application.

Client mutation success is returned only after the command is committed,
durably applied by the leader, and its result is available for deduplicated
retry.

Recovery MUST be deterministic and idempotent. Replaying already applied
entries MUST NOT duplicate state-machine effects.

The implementation MUST provide injectable persistence boundaries so tests can
terminate a node before and after every safety-relevant durable operation.

## 12. Correctness properties

### 12.1 Consensus safety

The formal model and implementation tests MUST cover:

1. **Election safety:** at most one leader is elected in a term.
2. **Log matching:** equal index and term imply identical prefixes.
3. **Leader append-only:** a leader never changes its existing log entries.
4. **Leader completeness:** committed entries appear in every later leader's
   log.
5. **State-machine safety:** no two nodes apply different commands at the same
   index.
6. **Committed-prefix preservation:** committed history is never replaced.
7. **Configuration safety:** every committed membership transition preserves
   quorum intersection required by the protocol.
8. **Monotonic progress:** term, commit index, applied index, and snapshot index
   never regress within their defined durable histories.

### 12.2 Service safety

The complete service MUST additionally satisfy:

1. Every successful CRUD history is linearizable.
2. A mutation request is applied at most once.
3. Acknowledged mutations survive every supported recovery.
4. Applied state is a deterministic prefix of committed commands.
5. Snapshots preserve the exact observable state of their included prefix.
6. Failed preconditions have no state-machine side effects.
7. No stale read is returned as a successful read.

### 12.3 Liveness

Under eventual synchrony, fair scheduling, functioning durable storage, and an
available quorum:

- a leader is eventually elected;
- a submitted command is eventually committed or receives a terminal
  rejection;
- committed commands are eventually applied;
- a sufficiently connected lagging member eventually catches up;
- an interrupted membership change can eventually complete.

Liveness claims MUST always state their assumptions and MUST NOT be presented
as unconditional guarantees.

## 13. Formal verification

### 13.1 Authoritative model

The repository's TLA+ specification is the authoritative abstract protocol
model. It MUST model the behavior actually implemented, including persistence,
crash recovery, snapshots, client deduplication, and joint consensus by the
milestone that introduces each feature.

Names of protocol actions and state SHOULD remain recognizably aligned between
the specification, implementation events, and documentation.

### 13.2 Required evidence

The final formal claim is:

> For the documented failure model and finite parameter domains, the TLA+
> protocol model satisfies the declared Raft safety invariants for arbitrary
> execution length through machine-checked inductive invariants. Larger bounded
> configurations are exhaustively model-checked. Recorded Rust executions are
> accepted behaviors of the model, and externally observed CRUD histories are
> linearizable.

To support that claim:

- TLC MUST exhaustively check a committed matrix of finite configurations.
- The matrix MUST include normal operation, message faults, partitions,
  crashes, restarts, and every implemented protocol extension.
- Apalache MUST check an inductive invariant that implies the core safety
  properties for the declared finite domains and arbitrary trace length.
- If Apalache cannot support a required construct, the limitation and an
  equivalent machine-checked alternative MUST be documented before release.
- Bounded liveness properties MUST be checked under explicit fairness
  assumptions.
- Model parameters, tool versions, state counts, runtimes, and specification
  hashes MUST be published as CI artifacts.

Small verification configurations run on every relevant pull request. Wider
and longer-running matrices run on a schedule and before release.

### 13.3 Trace validation

The implementation MUST emit a stable, machine-readable protocol trace mode.
Trace records MUST contain abstract state changes without leaking key values or
other unnecessary data.

Recorded executions from deterministic simulation and real clusters MUST be
validated against the TLA+ model. Trace validation is sampled conformance
evidence, not exhaustive implementation proof, and documentation MUST describe
it accurately.

### 13.4 Claim boundary

The project MUST NOT claim that the Rust executable is universally proven
correct. The trusted computing base includes at least the Rust compiler,
runtime dependencies, transactional storage engine, operating system,
filesystem, transport stack, formal tools, and hardware.

A TLAPS proof of central safety properties is a stretch objective, not a
condition for the first complete release.

## 14. Test strategy

Tests are evidence layers with different fault-detection roles. No single layer
is a substitute for another.

### 14.1 Pure unit tests

Deterministic functions, transition guards, log comparison, quorum arithmetic,
configuration rules, command semantics, and encoding boundaries MUST have
direct unit tests.

### 14.2 Property and mutation tests

Property tests MUST cover log reconciliation, serialization round trips,
snapshot equivalence, quorum calculations, command determinism, and storage
model behavior.

Consensus and persistence modules SHOULD receive scheduled mutation testing
once their interfaces stabilize. Surviving meaningful mutants require either
a test or a documented reason.

### 14.3 Deterministic protocol simulation

The consensus core MUST be runnable without real sockets, wall clocks, threads,
or disks. A deterministic simulator controls time, message delivery, crashes,
restarts, and storage outcomes.

Every simulation failure MUST be reproducible by seed and event trace.

### 14.4 Storage contract and crash tests

The storage abstraction MUST have a reusable conformance suite. Crash tests
terminate nodes around every persistence boundary and verify safe recovery.

### 14.5 API contract tests

Generated and hand-written tests MUST verify OpenAPI compatibility, HTTP
conditions, stable errors, size limits, deduplication, ambiguous retries, and
leader forwarding.

### 14.6 Process and cluster integration tests

Real-process tests MUST cover one-, three-, and five-node scenarios including
election, failover, restart, follower catch-up, snapshot installation, and
membership changes.

### 14.7 End-to-end fuzz and fault testing

The final harness runs concurrent generated CRUD workloads while a fault
nemesis injects partitions, delay, loss, duplication, process termination,
restart, and snapshot-transfer interruption.

Every run MUST capture:

- random seed and tool versions;
- initial and final cluster configuration;
- complete invocation and response history;
- injected faults and timing decisions;
- node protocol traces, logs, and metrics;
- linearizability result;
- a directly runnable reproducer;
- a minimized workload when shrinking is available.

CI runs a bounded deterministic campaign. Scheduled and pre-release workflows
run longer campaigns across multiple seeds and cluster sizes.

### 14.8 Coverage

Coverage is a guardrail, not the correctness argument.

- Generated code is excluded from coverage targets.
- After the replicated-log milestone, non-generated Rust code MUST maintain at
  least 80% line coverage and 70% branch coverage.
- Consensus state transitions and persistence failure branches target at least
  90% line coverage and 80% branch coverage.
- Coverage MUST NOT fall below the committed baseline without an explicit,
  reviewed rationale.
- Every specified protocol transition MUST have direct or generated test
  evidence regardless of percentage.

## 15. Observability and diagnostics

Every node MUST expose or record:

- cluster ID and node ID;
- role and current term;
- known leader;
- commit, applied, and snapshot indexes;
- per-peer replication progress on the leader;
- current and joint membership configuration;
- election, RPC, replication, apply, snapshot, and storage latency metrics;
- rejected, forwarded, retried, timed-out, and deduplicated client operations;
- bounded queue depth and backpressure metrics;
- build, storage-format, snapshot-format, and peer-protocol versions.

Logs MUST be structured and correlate client requests, peer RPCs, and protocol
events without logging values. Metrics MUST avoid unbounded labels such as key,
client ID, request ID, node-supplied error text, or remote address.

Tracing and metrics SHOULD follow stable OpenTelemetry semantic conventions
where applicable. Sensitive header capture is opt-in and allowlisted.

Readiness means the process can safely serve its configured role. Liveness
means the process event loop is functioning; it does not require cluster
quorum.

## 16. Resource safety and backpressure

All externally influenced resources MUST be bounded, rejected, streamed, or
backpressured. This includes:

- request and response bodies;
- log-entry batches;
- snapshot chunks;
- concurrent RPCs and client requests;
- pending proposals;
- transport queues;
- trace and log buffering;
- retry attempts and retry delay;
- retained snapshots and uncompacted log data.

Overload MUST fail visibly and MUST NOT compromise consensus safety.

## 17. Security boundary

Local development MAY use plaintext listeners on loopback or isolated Compose
networks.

Distributed deployment MUST support:

- TLS for client and operational HTTP;
- mutual TLS for peer identity;
- explicit peer allowlisting from committed membership;
- certificate and key loading from files or secret providers rather than
  checked-in configuration;
- separate bind addresses for client, peer, and operational interfaces;
- non-root containers and read-only runtime filesystems where practical.

Application-level user authentication, authorization, and multitenancy are
outside the first complete release. The service MUST document that it is not
safe to expose to an untrusted network without an external access-control
layer.

## 18. Local and CI operations

The repository MUST provide documented, one-command workflows for:

- starting one development node;
- starting a persistent three-node cluster;
- inspecting cluster and leader state;
- stopping and restarting individual nodes without deleting data;
- resetting explicitly named local experiment data;
- running deterministic simulations;
- running the formal checks;
- running a short end-to-end fuzz campaign;
- reproducing a captured failure;
- running all ordinary quality gates.

Compose is the primary local orchestration mechanism. Fault-injection tooling
MAY use a proxy or controlled network namespace, but it MUST be automated and
seedable.

Production images MUST be reproducible, non-root, health-checked, and built for
supported `linux/amd64` and `linux/arm64` targets. Kubernetes packaging is not
required to complete the project.

Destructive development commands MUST identify their exact target and require
explicit intent. They MUST NOT silently delete persistent cluster data.

## 19. Milestones and gates

Formal modeling, implementation, and testing advance together. A milestone is
complete only when its code, model, documentation, and required evidence agree.

### M0: Charter and engineering foundation

- Normative specification accepted.
- Architecture decision process established.
- Existing Rust, container, configuration, logging, and CI scaffold retained.
- Tooling strategy for TLA+, simulation, coverage, and fuzz artifacts agreed.

### M1: Deterministic protocol kernel and election

- Pure Raft state-transition core.
- Follower, candidate, and leader election behavior.
- Term and vote persistence contract.
- Deterministic clock and message simulation.
- TLA+ election model and election-safety checks.

### M2: Static-cluster replicated log

- AppendEntries, conflict repair, commitment, and ordered application.
- One-node and three-node operation.
- Transactional durable log implementation.
- Core Raft safety invariants in TLA+.
- Partition, leader-loss, and follower-catch-up simulation.

### M3: Linearizable persistent CRUD

- Complete client HTTP contract.
- Deterministic KV state machine.
- Client sequence deduplication and ambiguous retry behavior.
- Crash recovery and storage conformance suite.
- Real-cluster history collection and linearizability checking.

### M4: Snapshots and compaction

- Atomic snapshot creation and recovery.
- Log compaction.
- Chunked or streamed snapshot installation and retry.
- Snapshot-aware model, simulation, crash tests, and trace validation.

### M5: Dynamic membership

- Joint consensus and one-change-at-a-time enforcement.
- Safe catch-up, addition, removal, and leader removal.
- Membership-aware snapshot and recovery.
- Configuration-safety invariants and adversarial membership tests.

### M6: Operational hardening

- Resource bounds and backpressure.
- Metrics, diagnostics, tracing, and stable protocol events.
- TLS and peer mTLS support.
- Upgrade/version rejection rules.
- Reproducible multi-architecture production image.

### M7: Verification and adversarial closure

- Required TLC matrix and inductive invariant checks complete.
- Rust trace validation continuously exercised.
- Automated multi-process fuzz and fault harness complete.
- Linearizability oracle, shrinking, artifact retention, and replay complete.
- Coverage and mutation-testing expectations met.
- Final correctness claims and trusted-computing-base limitations published.

## 20. Definition of complete

The first complete release exists when:

1. Every goal in Section 3 is implemented and documented.
2. Every required safety property has passing formal and implementation
   evidence appropriate to its layer.
3. Required liveness scenarios pass under documented assumptions.
4. The public API is versioned and contract-tested.
5. One-node, three-node, snapshot, recovery, and membership workflows are
   reproducible from a clean checkout.
6. The end-to-end fuzz harness finds no violation in the published release
   campaign, and the exact campaign is reproducible.
7. All CI, coverage, formal, container, and documentation gates pass.
8. Known limitations and the trusted computing base are stated without
   overstating verification.

## 21. Deferred implementation decisions

The following require ADRs before their implementation milestone:

- transactional storage engine selection;
- internal module and task ownership boundaries;
- concrete Protobuf package and compatibility policy;
- snapshot file and chunk formats;
- TLS implementation and certificate reload behavior;
- deterministic simulator implementation strategy;
- linearizability checker selection or implementation;
- fault-injection transport;
- formal-tool container images and extended CI schedule;
- exact default limits and timeouts.

An ADR may choose among implementations, but it MUST NOT weaken this
specification silently. Any necessary requirement change first updates this
document with rationale.

## 22. Reference material

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus
  Algorithm*.
- Diego Ongaro, *Consensus: Bridging Theory and Practice*.
- Diego Ongaro, `raft.tla`, the canonical TLA+ Raft specification.
- RFC 9110, *HTTP Semantics*.
- RFC 9457, *Problem Details for HTTP APIs*.
- TLA+ trace-validation documentation and related implementation-conformance
  work.
- Apalache documentation for bounded and inductive invariant checking.
