# ADR 0003: Versioned gRPC peer transport

- Status: Accepted
- Date: 2026-08-24

## Decision

Use Protobuf over tonic gRPC on a listener distinct from the HTTP client API.
Every RPC carries the exact cluster ID, sender, recipient, protocol version,
and term. Generated code uses a vendored `protoc`; the schema and field numbers
are committed under `proto/raft/peer/v1`.

Wire input is validated and resource-bounded before conversion to a Raft core
event. The transport performs one network attempt per core effect. It does not
apply generic RPC retries: election timers and leader heartbeats cause Raft to
retry according to protocol state.

The node actor owns the core and SQLite connection. Client promises are keyed
by log index and remain pending across asynchronous peer responses; only
transactional ordered application resolves them.

## Consequences

- HTTP and peer traffic have separate versioning and trust boundaries.
- Loss, timeout, connection refusal, and an outbound queue full condition are
  equivalent to a dropped Raft message and cannot fabricate success.
- A new incompatible peer protocol requires an explicit version transition;
  removed Protobuf field numbers must be reserved.
- Client-command forwarding is a separate, one-hop RPC on the same validated
  peer identity envelope; the receiver executes locally and never forwards it.
- Connection pooling, peer mTLS, and snapshot streaming extend this boundary in
  later milestones without changing core transitions.
