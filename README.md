# raft-kv-service

A production-shaped Rust microservice foundation. The current implementation
provides HTTP lifecycle, configuration, observability, tests, containers, and
CI; storage and Raft consensus will arrive through verified vertical
milestones.

The normative project goals, correctness claims, protocol scope, verification
strategy, and milestone gates are defined in
[`docs/specification.md`](docs/specification.md).

## Prerequisites

- Rust 1.98.0 through [rustup](https://rustup.rs/) (the repository toolchain file selects it automatically)
- Docker Engine 28+ with Docker Compose v2
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

Run all quality gates with `make check`.

## Start with Docker

```sh
docker compose up --build
```

Build the production image with `make image`. Its runtime process is non-root and exposes a container health check.

## Configuration

Defaults live in `config/default.toml`. Environment variables override them using the `RAFT_KV__SECTION__FIELD` convention; `.env.example` lists every setting. The application does not load `.env` itself—Compose does, while local shells can use their preferred environment loader.

Use `pretty` logs for development and `json` in deployed environments. Every HTTP request receives an `x-request-id`, and authorization values are redacted from tracing.

## Project boundaries

- `src/config.rs`: typed file and environment configuration
- `src/http.rs`: HTTP routes, middleware, and health state
- `src/telemetry.rs`: structured logging initialization
- `src/lib.rs`: service lifecycle and graceful shutdown
- `src/main.rs`: composition root

No datastore, consensus library, RPC protocol, metrics backend, or deployment platform is assumed yet. Those choices should follow workload, consistency, recovery, and operating-environment requirements rather than boilerplate convention.
