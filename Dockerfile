# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

FROM rust:1.98.0-trixie@sha256:7f7a53a25a0319dd8284e279d529d45759cb384d59b14cc6806132910f45522e AS development
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

FROM debian:trixie-slim@sha256:3a39a0592364683e6bab97937b72cad5a8fa6dcbbee90edb3bb48c7f8e94f258 AS runtime
RUN mkdir --parents /var/lib/raft-kv && \
    chown 10001:10001 /var/lib/raft-kv

COPY --from=builder /tmp/raft-kv-service /usr/local/bin/raft-kv-service
COPY config/default.toml /app/config/default.toml
WORKDIR /app
USER 10001:10001
EXPOSE 8080 8088 9080
VOLUME ["/var/lib/raft-kv"]
ENV RAFT_KV__TELEMETRY__LOG_FORMAT=json
ENV RAFT_KV__RAFT__DATA_FILE=/var/lib/raft-kv/node.sqlite3
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/raft-kv-service", "healthcheck", "127.0.0.1:8080"]
ENTRYPOINT ["/usr/local/bin/raft-kv-service"]
