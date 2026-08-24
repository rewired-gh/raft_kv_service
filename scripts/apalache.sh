#!/usr/bin/env bash
set -euo pipefail

readonly APALACHE_VERSION="0.62.1"
readonly APALACHE_SHA256="93fef2f381d30465eff906a2d6afa3358cb2c9adf6a8aad3bec0ded4f45432bb"
readonly APALACHE_URL="https://github.com/apalache-mc/apalache/releases/download/v${APALACHE_VERSION}/apalache-${APALACHE_VERSION}.tgz"
readonly TOOL_ROOT=".tools/apalache-${APALACHE_VERSION}"
readonly APALACHE="${TOOL_ROOT}/bin/apalache-mc"
readonly OUTPUT_ROOT="${RAFT_KV_APALACHE_OUTPUT:-target/formal-evidence/apalache}"

checksum() {
  openssl dgst -sha256 "$1" | awk '{print $2}'
}

setup() {
  if [[ -x "${APALACHE}" ]]; then
    return
  fi

  mkdir -p .tools
  local archive temporary extracted
  archive="$(mktemp ".tools/apalache-${APALACHE_VERSION}.XXXXXX.tgz")"
  temporary="$(mktemp -d ".tools/apalache-${APALACHE_VERSION}.XXXXXX")"
  trap 'rm -f "${archive}"; rm -rf "${temporary}"' RETURN
  curl --fail --location --silent --show-error "${APALACHE_URL}" --output "${archive}"
  if [[ "$(checksum "${archive}")" != "${APALACHE_SHA256}" ]]; then
    printf 'Apalache checksum mismatch\n' >&2
    return 1
  fi
  tar -xzf "${archive}" -C "${temporary}"
  extracted="${temporary}/apalache-${APALACHE_VERSION}"
  [[ -x "${extracted}/bin/apalache-mc" ]]
  mv "${extracted}" "${TOOL_ROOT}"
  trap - RETURN
  rm -f "${archive}"
  rm -rf "${temporary}"
}

run_obligation() {
  local name="$1"
  local init="$2"
  local invariant="$3"
  local length="$4"
  mkdir -p "${OUTPUT_ROOT}/${name}"
  "${APALACHE}" \
    --out-dir="${OUTPUT_ROOT}" \
    --run-dir="${name}" \
    check \
    --config=formal/RaftInductive.cfg \
    --init="${init}" \
    --inv="${invariant}" \
    --length="${length}" \
    --no-deadlock \
    formal/RaftInductive.tla
}

case "${1:-}" in
  setup)
    setup
    ;;
  check)
    setup
    run_obligation initialization Init InductiveInvariant 0
    run_obligation preservation InductiveInvariant InductiveInvariant 1
    run_obligation implication InductiveInvariant CoreSafety 0
    ;;
  version)
    setup
    "${APALACHE}" version
    ;;
  *)
    printf 'usage: scripts/apalache.sh setup | check | version\n' >&2
    exit 2
    ;;
esac
