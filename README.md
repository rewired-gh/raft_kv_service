# raft-kv-service

A production-shaped Rust microservice foundation. The current implementation
provides HTTP lifecycle, configuration, observability, tests, containers, and
CI. The completed M1 through M6 milestones provide a deterministic Raft election
and static-cluster replication core, explicit persistence and ordered-
application barriers, transactional SQLite storage, a crash/partition-aware
simulator, versioned protocol traces, bounded TLA+ models, transactional CRUD,
deduplicated retries, follower forwarding, external linearizability checks,
atomic logical snapshots with resumable lagging-follower installation, and
dynamic membership through staged joint consensus, bounded resource handling,
authenticated diagnostics and metrics, TLS/mTLS trust boundaries, and a
reproducible supply-chain policy.

The normative project goals, correctness claims, protocol scope, verification
strategy, and milestone gates are defined in
[`docs/specification.md`](docs/specification.md).
Delivery sequencing and evidence status are tracked in
[`docs/roadmap.md`](docs/roadmap.md) and `verification/`.

## Prerequisites

- Rust 1.98.0 through [rustup](https://rustup.rs/) (the repository toolchain file selects it automatically)
- Docker Engine 28+ with Docker Compose v2
- Java 25, `curl`, and OpenSSL for checksum-pinned TLA+ model checking
- GNU or BSD Make (optional command shortcuts)

## Start locally

```sh
cp .env.example .env
make setup
make run
```

The service listens on `http://localhost:8080`:

```sh
curl --fail http://localhost:8080/health/live
curl --fail http://localhost:8080/health/ready

CLIENT_ID=018f47a7-48c1-7d9a-9db6-4d143ba4c273
curl --fail-with-body -X PUT \
  -H 'Content-Type: application/octet-stream' \
  -H 'If-None-Match: *' \
  -H "Raft-KV-Client-ID: ${CLIENT_ID}" \
  -H 'Raft-KV-Request-Sequence: 1' \
  --data-binary 'hello' \
  http://localhost:8080/v1/keys/greeting
curl --fail-with-body http://localhost:8080/v1/keys/greeting

curl --fail-with-body \
  -H 'Authorization: Bearer local-development-only' \
  http://localhost:8088/operational/v1/status
curl --fail-with-body \
  -H 'Authorization: Bearer local-development-only' \
  http://localhost:8088/metrics
```

Run all quality gates, including the fast formal model, with `make check`.
The first `make setup` or `make formal` downloads the stable TLA+ tools JAR and
verifies its pinned SHA-256 digest under the ignored `.tools/` directory.

## Start with Docker

`docker compose up --build` starts the default three-voter experiment. Client
listeners are published on `localhost:8081`, `:8082`, and `:8083`; authenticated
operational listeners use `:8181`, `:8182`, and `:8183`; peer gRPC stays on the
private Compose network. Any node accepts client requests and
forwards them once to its known leader; a node without a usable leader returns
a retryable `503`.

For a single local process, `make run` uses the one-node defaults on
`localhost:8080` and persists state in `data/node-1.sqlite3`.

Membership changes use `PUT /operational/v1/membership` with a stable UUID and
the complete target member list. A joining process starts with
`raft.staging = true`, the current voters/endpoints, a fresh node ID and empty
data directory. The leader admits it only after zero-lag catch-up. Retrying the
same operation ID and target is safe; reusing an ID for another target returns
`409`. See the OpenAPI document for the exact body and response.

Useful experiment commands are:

```sh
docker compose restart node2              # preserve node2 data
curl -H 'Authorization: Bearer local-development-only' \
  http://localhost:8181/operational/v1/status
make reset-local NODE=node2 CONFIRM=yes   # explicit destructive reset
make formal                               # deterministic finite model checks
make fuzz                                 # retained real-process fault campaign
make check                                # ordinary local gate
make supply-chain                         # advisories, licenses, and sources
```

Build the production image with `make image`. Its runtime process uses numeric
UID/GID 10001, has no package-manager-installed runtime tools, and uses the
binary's liveness probe. Deploy it with a read-only root filesystem, a writable
`/var/lib/raft-kv` volume, and read-only secret mounts.

## Configuration

Defaults live in `config/default.toml`. Environment variables override them using the `RAFT_KV__SECTION__FIELD` convention; `.env.example` lists every setting. The application does not load `.env` itself—Compose does, while local shells can use their preferred environment loader.

Use `pretty` logs for development and `json` in deployed environments. Every HTTP request receives an `x-request-id`, and authorization values are redacted from tracing.

`raft.max_clients`, `raft.max_key_bytes`, and `raft.max_value_bytes` are
cluster-scoped replicated-behavior settings. Configure the same values on
every voter before bootstrap or join. They are persisted in SQLite, carried by
snapshots, and negotiated on every peer RPC; changing them in place is rejected
rather than risking divergent state-machine application.

Plaintext is for loopback or isolated Compose experiments only. Distributed
deployments configure client TLS, operational mTLS, and peer mTLS from mounted
certificate/key files. Each peer certificate SHA-256 fingerprint maps to one
node ID. A new node additionally needs a future
`staging_expires_unix_seconds` admission in its node TOML configuration; a
certificate alone never grants membership. The operational bearer token is
defense in depth and must come from secret injection. Do not expose the client
API to an untrusted network without an external authentication and
authorization layer.

Operational status and bounded-label OpenMetrics are available only on the
separate operational listener. The service bounds HTTP/gRPC messages and
concurrency, append and snapshot batches, queues, pending proposals, retained
client sessions, and file ownership. See
[`docs/supply-chain-policy.md`](docs/supply-chain-policy.md) for immutable build,
upgrade, SBOM, provenance, and vulnerability rules.

## Project boundaries

- `src/config.rs`: typed file and environment configuration
- `src/http.rs`: HTTP routes, middleware, and health state
- `src/history.rs`: bounded exact linearizability checker
- `src/kv/`: deterministic CRUD and deduplication state machine
- `src/cluster.rs`: cluster actor, gRPC dispatch, forwarding, and membership control
- `src/raft/`: deterministic protocol core and simulator
- `src/storage/`: transactional Raft persistence contract and SQLite backend
- `src/telemetry.rs`: structured logging initialization
- `src/tls.rs`: TLS material parsing and mutual-authentication configuration
- `src/metrics.rs`: bounded-label OpenMetrics instruments
- `src/lib.rs`: service lifecycle and graceful shutdown
- `src/main.rs`: composition root
- `formal/`: TLA+ models, finite configurations, and abstraction maps
- `verification/`: machine-readable requirement, property, and evidence maps

See [`docs/fuzzing.md`](docs/fuzzing.md) for campaign artifacts, exact input
replay, shrinking, and the boundary between deterministic inputs and OS
scheduling.

No consensus library is used. The trusted computing base and the deliberately
limited formal claim are documented in the specification and verification
manifest; M7 closes the adversarial and formal-evidence gates.
