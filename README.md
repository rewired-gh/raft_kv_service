# raft-kv-service

A production-shaped Rust microservice foundation. The current implementation
provides HTTP lifecycle, configuration, observability, tests, containers, and
CI. The completed M1 through M4 milestones provide a deterministic Raft election
and static-cluster replication core, explicit persistence and ordered-
application barriers, transactional SQLite storage, a crash/partition-aware
simulator, versioned protocol traces, bounded TLA+ models, transactional CRUD,
deduplicated retries, follower forwarding, external linearizability checks,
and atomic logical snapshots with resumable lagging-follower installation.

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
```

Run all quality gates, including the fast formal model, with `make check`.
The first `make setup` or `make formal` downloads the stable TLA+ tools JAR and
verifies its pinned SHA-256 digest under the ignored `.tools/` directory.

## Start with Docker

`docker compose up --build` starts the default three-voter experiment. Client
listeners are published on `localhost:8081`, `:8082`, and `:8083`; peer gRPC
stays on the private Compose network. Any node accepts client requests and
forwards them once to its known leader; a node without a usable leader returns
a retryable `503`.

For a single local process, `make run` uses the one-node defaults on
`localhost:8080` and persists state in `data/node-1.sqlite3`.

Build the production image with `make image`. Its runtime process is non-root and exposes a container health check.

## Configuration

Defaults live in `config/default.toml`. Environment variables override them using the `RAFT_KV__SECTION__FIELD` convention; `.env.example` lists every setting. The application does not load `.env` itself—Compose does, while local shells can use their preferred environment loader.

Use `pretty` logs for development and `json` in deployed environments. Every HTTP request receives an `x-request-id`, and authorization values are redacted from tracing.

## Project boundaries

- `src/config.rs`: typed file and environment configuration
- `src/http.rs`: HTTP routes, middleware, and health state
- `src/history.rs`: bounded exact linearizability checker
- `src/kv/`: deterministic CRUD and deduplication state machine
- `src/cluster.rs`: static-cluster actor, gRPC dispatch, and client forwarding
- `src/raft/`: deterministic protocol core and simulator
- `src/storage/`: transactional Raft persistence contract and SQLite backend
- `src/telemetry.rs`: structured logging initialization
- `src/lib.rs`: service lifecycle and graceful shutdown
- `src/main.rs`: composition root
- `formal/`: TLA+ models, finite configurations, and abstraction maps
- `verification/`: machine-readable requirement, property, and evidence maps

No consensus library is used. Remaining RPC, metrics-backend, and deployment
choices follow the specification and accepted ADRs rather than boilerplate
convention.
