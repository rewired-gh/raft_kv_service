.DEFAULT_GOAL := help

.PHONY: help setup run check fmt lint test formal-setup formal formal-full formal-inductive formal-container coverage-setup coverage fuzz mutation supply-chain docker-dev image reset-local clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: formal-setup ## Fetch development dependencies
	cargo fetch --locked

run: ## Run the service locally
	cargo run

check: fmt lint test formal ## Run all local quality gates

fmt: ## Check Rust formatting
	cargo fmt --all -- --check

lint: ## Run Clippy with warnings denied
	cargo clippy --all-targets --all-features -- -D warnings

test: ## Run all tests
	cargo test --all-targets --all-features

formal-setup: ## Fetch checksum-pinned formal tools
	./scripts/tlc.sh setup
	./scripts/apalache.sh setup

formal: ## Model-check the fast formal suite
	./scripts/tlc.sh check formal/RaftElection.cfg
	./scripts/tlc.sh check formal/Raft.cfg
	./scripts/tlc.sh check formal/KvStateMachine.cfg
	./scripts/tlc.sh check formal/RaftSnapshot.cfg
	./scripts/tlc.sh check formal/RaftMembership.cfg
	./scripts/tlc.sh check formal/RaftLiveness.cfg

formal-inductive: ## Prove inductive safety obligations with Apalache
	./scripts/apalache.sh check

formal-full: ## Run the published TLC matrix and inductive proof
	./scripts/formal-matrix.sh
	./scripts/apalache.sh check

formal-container: ## Run full formal verification in the immutable CI image
	docker build --file Dockerfile.formal --tag raft-kv-formal:local .
	mkdir -p target/formal-container
	docker run --rm --user "$$(id -u):$$(id -g)" --volume "$(CURDIR)/target/formal-container:/evidence" raft-kv-formal:local full

coverage-setup: ## Install the pinned nightly coverage toolchain
	./scripts/coverage.sh setup

coverage: ## Run tests and real-process campaigns with coverage gates
	./scripts/coverage.sh check

fuzz: ## Run a reproducible real-process fuzz campaign (SEED/OPS optional)
	cargo build --locked --bins
	./target/debug/raft-kv-fuzz --seed "$${SEED:-424242}" --operations "$${OPS:-12}" --cluster-size "$${NODES:-3}" --artifact-dir "target/fuzz/$${SEED:-424242}-$${NODES:-3}"

mutation: ## Mutation-test consensus and persistence modules
	cargo mutants --workspace --file 'src/raft/*.rs' --file 'src/storage/*.rs' --all-features

supply-chain: ## Audit Rust dependencies, licenses, and sources
	cargo audit
	cargo deny check

docker-dev: ## Start the development service with Compose
	docker compose up --build

image: ## Build the production image
	docker build --target runtime --tag raft-kv-service:local .

reset-local: ## Delete one named Compose node volume: NODE=node1 CONFIRM=yes
	./scripts/reset-local.sh "$(NODE)" "$(CONFIRM)"

clean: ## Remove local Rust build artifacts
	cargo clean
