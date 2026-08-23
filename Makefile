.DEFAULT_GOAL := help

.PHONY: help setup run check fmt lint test docker-dev image clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Fetch Rust dependencies
	cargo fetch --locked

run: ## Run the service locally
	cargo run

check: fmt lint test ## Run all local quality gates

fmt: ## Check Rust formatting
	cargo fmt --all -- --check

lint: ## Run Clippy with warnings denied
	cargo clippy --all-targets --all-features -- -D warnings

test: ## Run all tests
	cargo test --all-targets --all-features

docker-dev: ## Start the development service with Compose
	docker compose up --build

image: ## Build the production image
	docker build --target runtime --tag raft-kv-service:local .

clean: ## Remove local Rust build artifacts
	cargo clean

