# ADR 0006: Operational trust and resource boundaries

- Status: Accepted
- Date: 2026-08-24

## Context

Consensus safety can still be undermined by ambiguous identity, two processes
sharing one database, or unbounded work under loss of quorum. Client,
operational, and peer traffic also have different trust and exposure profiles.

## Decision

Use three independent listeners. Client HTTP supports TLS; operational HTTP
requires a trusted client certificate when TLS is configured and additionally
requires a bearer token; peer gRPC requires mutual TLS. A peer certificate is
bound to one node ID by SHA-256. The authenticated node must also be in
committed membership or in the exact pending target with an unexpired,
operator-configured staging admission. Committed removal revokes transport
authorization without changing quorum calculations.

Bound HTTP bodies and concurrency, gRPC message size and per-connection
concurrency, append batches, snapshot chunks and total size, actor and transport
queues, pending proposals, duplicate membership waiters, durable client
records, and retained snapshots/log prefixes. Hold an OS-level exclusive lock
for the lifetime of a file-backed SQLite owner. Overload is explicit and never
bypasses persistence or quorum barriers.

Peer protocol v3 carries the persisted retained-client, key-size, and
value-size tuple in every authenticated RPC identity. A mismatch is rejected
before protocol processing, preventing voters from applying one committed log
differently. Fixed log-entry encoding bounds remain bound by the immutable peer
protocol version.

Expose bounded-label OpenMetrics and authenticated local diagnostics on the
operational listener. Production images use digest-pinned bases, numeric
non-root identity, liveness health checks, two supported architectures,
embedded SBOM/provenance attestations, and automated advisory, license, secret,
and image-vulnerability gates.

## Consequences

Plaintext remains convenient only for loopback or isolated Compose experiments.
Distributed operators must provision certificate files, node fingerprints,
and expiring admissions before adding nodes. Changing cluster-scoped limits is
a peer-protocol compatibility change. The deployment volume, OS, and key
provisioning mechanism remain inside the trusted computing base.
