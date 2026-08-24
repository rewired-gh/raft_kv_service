#!/bin/sh
set -eu

TOOLCHAIN="nightly-2026-08-24"
CARGO_LLVM_COV_VERSION="0.9.0"
REPORT="${COVERAGE_REPORT:-target/coverage.json}"
CAMPAIGN_ROOT="${COVERAGE_CAMPAIGN_ROOT:-target/coverage-campaigns}"

case "${1:-check}" in
  setup)
    rustup toolchain install "$TOOLCHAIN" --profile minimal --component llvm-tools-preview
    cargo install cargo-llvm-cov --version "$CARGO_LLVM_COV_VERSION" --locked
    exit 0
    ;;
  check)
    ;;
  *)
    echo "usage: $0 [setup|check]" >&2
    exit 64
    ;;
esac

cargo "+$TOOLCHAIN" llvm-cov clean --workspace
# cargo-llvm-cov intentionally emits the exact compiler instrumentation
# environment. shellcheck disable=SC2046
eval "$(cargo "+$TOOLCHAIN" llvm-cov show-env --branch --sh)"

cargo "+$TOOLCHAIN" build --locked --all-features --bins
cargo "+$TOOLCHAIN" test --locked --all-targets --all-features

mkdir -p "$CAMPAIGN_ROOT"
"$CARGO_LLVM_COV_TARGET_DIR/debug/raft-kv-fuzz" \
  --seed 424242 --operations 12 --cluster-size 3 --artifact-dir "$CAMPAIGN_ROOT/424242"
"$CARGO_LLVM_COV_TARGET_DIR/debug/raft-kv-fuzz" \
  --seed 929292 --operations 12 --cluster-size 5 --artifact-dir "$CAMPAIGN_ROOT/929292"

cargo "+$TOOLCHAIN" llvm-cov report --branch --summary-only --json \
  --output-path "$REPORT"
python3 scripts/check-coverage.py "$REPORT"
