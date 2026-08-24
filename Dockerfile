# syntax=docker/dockerfile:1

FROM rust:1.98.0-trixie AS development
WORKDIR /workspace

FROM development AS builder
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY build.rs ./
COPY proto ./proto
COPY config ./config
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release && \
    cp target/release/raft-kv-service /tmp/raft-kv-service

FROM debian:trixie-slim AS runtime
RUN apt-get update && \
    apt-get install --no-install-recommends --yes ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid 10001 app && \
    useradd --system --uid 10001 --gid app --no-create-home app && \
    mkdir --parents /var/lib/raft-kv && \
    chown 10001:10001 /var/lib/raft-kv

COPY --from=builder /tmp/raft-kv-service /usr/local/bin/raft-kv-service
COPY config/default.toml /app/config/default.toml
WORKDIR /app
USER 10001:10001
EXPOSE 8080 9080
ENV RAFT_KV__TELEMETRY__LOG_FORMAT=json
ENV RAFT_KV__RAFT__DATA_FILE=/var/lib/raft-kv/node.sqlite3
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["curl", "--fail", "--silent", "http://127.0.0.1:8080/health/ready"]
ENTRYPOINT ["/usr/local/bin/raft-kv-service"]
