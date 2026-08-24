#!/usr/bin/env bash
set -euo pipefail

readonly TLA_VERSION="1.7.4"
readonly TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
readonly TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
readonly TOOL_DIR=".tools/tla-${TLA_VERSION}"
readonly TLA_JAR="${TOOL_DIR}/tla2tools.jar"

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    openssl dgst -sha256 "$1" | awk '{print $2}'
  fi
}

setup() {
  if [[ -f "${TLA_JAR}" ]] && [[ "$(checksum "${TLA_JAR}")" == "${TLA_SHA256}" ]]; then
    return
  fi

  mkdir -p "${TOOL_DIR}"
  local download_path
  download_path="$(mktemp "${TOOL_DIR}/tla2tools.jar.XXXXXX")"
  trap 'rm -f "${download_path}"' RETURN
  curl --fail --location --silent --show-error "${TLA_URL}" --output "${download_path}"

  local actual_sha256
  actual_sha256="$(checksum "${download_path}")"
  if [[ "${actual_sha256}" != "${TLA_SHA256}" ]]; then
    printf 'TLA+ tools checksum mismatch: expected %s, got %s\n' \
      "${TLA_SHA256}" "${actual_sha256}" >&2
    return 1
  fi
  mv "${download_path}" "${TLA_JAR}"
  trap - RETURN
}

case "${1:-}" in
  setup)
    setup
    ;;
  check)
    readonly MODEL_CONFIG="${2:?usage: scripts/tlc.sh check CONFIG}"
    readonly MODEL_STEM="${MODEL_CONFIG%.cfg}"
    if [[ -f "${MODEL_STEM}.tla" ]]; then
      readonly MODEL_SPEC="${MODEL_STEM}.tla"
    else
      readonly MODEL_SPEC="${MODEL_STEM%.extended}.tla"
    fi
    setup
    readonly MODEL_STATE_ROOT="${RAFT_KV_TLC_STATE_ROOT:-${TOOL_DIR}/states}"
    readonly MODEL_STATE_DIR="${MODEL_STATE_ROOT}/$(basename "${MODEL_CONFIG}" .cfg)"
    mkdir -p "${MODEL_STATE_DIR}"
    exec java -XX:+UseParallelGC -jar "${TLA_JAR}" \
      -config "${MODEL_CONFIG}" \
      -metadir "${MODEL_STATE_DIR}" \
      -workers "${RAFT_KV_TLC_WORKERS:-1}" \
      "${MODEL_SPEC}"
    ;;
  *)
    printf 'usage: scripts/tlc.sh setup | check CONFIG\n' >&2
    exit 2
    ;;
esac
