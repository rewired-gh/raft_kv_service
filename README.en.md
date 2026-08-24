# raft-kv-service

English | [简体中文](README.md)

A distributed, persistent KV microservice implemented from scratch in Rust to
make Raft understandable through running code and reproducible evidence. Its
scope is deliberately small—linearizable Create, Read, Update, and Delete—but
its consensus, persistence, recovery, verification, testing, and operational
layers are intentionally complete.

This repository is for developers who want to study a Raft-backed KV service,
and for developers who want to start a real multi-node service quickly and
verify elections, failover, recovery, and linearizability themselves.

> This is a toy project in product scope, not a toy implementation.

## Background

The project was built like an unusually intense hackathon: requirements,
specification, Raft and storage implementation, formal models, fault testing,
containers, and documentation were completed in less than 24 hours.

Nearly the entire process was driven by remotely controlling Codex on a
computer from a mobile phone. The author supplied goals, reviewed trade-offs,
and continuously redirected the work; Codex operated in the real workspace,
wrote code, executed verification, found counterexamples, and refined the
system. It is both a Raft learning project and an experiment in high-bandwidth
human–coding-agent collaboration from a mobile device.

The short build window did not replace correctness work. The repository keeps
its accepted specification, ADRs, layered tests, formal models, and
machine-readable evidence. Finite model checking is not presented as a proof
of every Rust execution.

## What makes it different

- **Raft is implemented here:** no consensus library; Pre-Vote, CheckQuorum,
  controlled leadership transfer, replication, durability barriers, snapshots,
  and joint consensus are repository code.
- **Specification-to-evidence closure:** specification 1.0 and M0–M7 map to
  code, models, tests, and recorded evidence under `verification/`.
- **Rigorous but bounded formal verification:** TLA+/TLC covers election, log,
  CRUD, snapshot, membership, and fair-liveness instances; Apalache checks the
  inductive invariant obligations.
- **A testable model/implementation bridge:** the deterministic core has no
  sockets, disks, threads, or wall clock; traces validate durability ordering,
  message causality, and snapshot boundaries.
- **Real-process adversarial testing:** 3/5-node campaigns combine concurrent
  CRUD with network and process faults, then check exact linearizability.
- **Exact input replay:** campaign schema 2 stores workload inputs and typed
  fault/process schedules while excluding OS scheduling from its claim.
- **Persistence is protocol behavior:** SQLite atomicity, hard-state/log rules,
  application, deduplication, snapshots, and ownership have crash tests.
- **Minimal production shape:** separate listeners, TLS/mTLS, resource bounds,
  OpenMetrics, a non-root image, SBOM/provenance, and supply-chain policy.

## Scope

The service provides one linearizable CRUD namespace with durable client
deduplication, 1/3/5-node deployments, follower forwarding, crash recovery,
logical snapshots, lagging-follower installation, staging, and joint-consensus
membership changes.

Election hardening includes non-disruptive Pre-Vote, quorum-loss leader
stepdown, and a bounded leadership-transfer API that reports success only after
the target is observed as leader.

It intentionally excludes multi-key transactions, scans, watches, TTLs,
sharding/Multi-Raft, Byzantine tolerance, and recovery after permanent quorum
state loss. See [`docs/specification.md`](docs/specification.md).

## Quick start

Prerequisites: Rust 1.98.0, Docker Engine 28+ with Compose v2, Java 25,
`curl`, OpenSSL, and optionally Make.

```sh
cp .env.example .env
make setup
make run
```

```sh
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

Start the default three-voter experiment with `docker compose up --build`.
Client listeners use `localhost:8081`–`:8083`, operational listeners use
`:8181`–`:8183`, and peer gRPC remains private.

## Learn and verify

Read the specification, roadmap, ADRs, `src/raft/`, `formal/`, and finally
`verification/` to follow the path from requirements to evidence.

```sh
make check          # fmt, Clippy, tests, and fast formal checks
make formal-full    # full TLC matrix and Apalache obligations
make fuzz           # retained real-process fault campaign
make mutation       # mutation testing for Raft and storage
make coverage       # line/branch gates plus process campaigns
make supply-chain   # advisories, licenses, and dependency sources
```

Release thresholds are 80% line / 70% branch overall and 90% line / 80%
branch for critical consensus and persistence code. See
[`docs/fuzzing.md`](docs/fuzzing.md),
[`docs/openapi.yaml`](docs/openapi.yaml), and
[`docs/adr/0007-verification-closure.md`](docs/adr/0007-verification-closure.md).

## Deployment

Defaults are in `config/default.toml`; overrides use
`RAFT_KV__SECTION__FIELD`. Plaintext is only for loopback or isolated
experiments. Distributed deployments should enable client TLS, operational
mTLS, peer mTLS, secret injection, and external authorization. The production
image runs as UID/GID 10001 and supports a read-only root filesystem with a
writable `/var/lib/raft-kv` volume. See
[`docs/supply-chain-policy.md`](docs/supply-chain-policy.md).

## Status

M0–M7 of specification 1.0 are complete. The project is suitable for study,
demonstrations, and controlled experiments. Real business deployment still
requires independent review against its threat model, capacity targets,
backup policy, and platform.
