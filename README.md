# raft-kv-service

A production-shaped Rust microservice foundation. The current implementation
provides HTTP lifecycle, configuration, observability, tests, containers, and
CI. The completed M1 and M2 milestones provide a deterministic Raft election
and static-cluster replication core, explicit persistence and ordered-
application barriers, transactional SQLite storage, a crash/partition-aware
simulator, versioned protocol traces, and bounded TLA+ models. The client KV
API is introduced in M3 and is not implemented yet.

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
```

Run all quality gates, including the fast formal model, with `make check`.
The first `make setup` or `make formal` downloads the stable TLA+ tools JAR and
verifies its pinned SHA-256 digest under the ignored `.tools/` directory.

## Start with Docker

`docker compose up --build` starts the default three-voter experiment. Client
listeners are published on `localhost:8081`, `:8082`, and `:8083`; peer gRPC
stays on the private Compose network. Until follower forwarding is enabled,
submit requests to the elected leader (non-leaders return a retryable 503).

For a single local process, `make run` uses the one-node defaults on
`localhost:8080` and persists state in `data/node-1.sqlite3`.

Build the production image with `make image`. Its runtime process is non-root and exposes a container health check.

## Configuration

Defaults live in `config/default.toml`. Environment variables override them using the `RAFT_KV__SECTION__FIELD` convention; `.env.example` lists every setting. The application does not load `.env` itself—Compose does, while local shells can use their preferred environment loader.

Use `pretty` logs for development and `json` in deployed environments. Every HTTP request receives an `x-request-id`, and authorization values are redacted from tracing.

## Project boundaries

- `src/config.rs`: typed file and environment configuration
- `src/http.rs`: HTTP routes, middleware, and health state
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
