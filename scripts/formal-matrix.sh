#!/usr/bin/env bash
set -euo pipefail

readonly OUTPUT_ROOT="${RAFT_KV_FORMAL_OUTPUT:-target/formal-evidence/tlc}"
readonly TLA_TOOLS_RELEASE="1.7.4"
readonly TLC_VERSION="2.19"
readonly TLC_REVISION="5a47802"
readonly MATRIX=(
  formal/RaftElection.cfg
  formal/RaftElection.extended.cfg
  formal/Raft.cfg
  formal/Raft.extended.cfg
  formal/KvStateMachine.cfg
  formal/RaftSnapshot.cfg
  formal/RaftMembership.cfg
  formal/RaftLiveness.cfg
)

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    openssl dgst -sha256 "$1" | awk '{print $2}'
  fi
}

mkdir -p "${OUTPUT_ROOT}"
printf '{"schema_version":2,"tla_tools_release":"%s","tlc_version":"%s","tlc_revision":"%s","models":[' \
  "${TLA_TOOLS_RELEASE}" "${TLC_VERSION}" "${TLC_REVISION}" > "${OUTPUT_ROOT}/manifest.json"
separator=""

for configuration in "${MATRIX[@]}"; do
  stem="${configuration%.cfg}"
  if [[ -f "${stem}.tla" ]]; then
    model="${stem}.tla"
  else
    model="${stem%.extended}.tla"
  fi
  name="$(basename "${configuration}" .cfg)"
  log="${OUTPUT_ROOT}/${name}.log"
  started="$(date +%s)"
  RAFT_KV_TLC_WORKERS="${RAFT_KV_TLC_WORKERS:-4}" \
    ./scripts/tlc.sh check "${configuration}" 2>&1 | tee "${log}"
  elapsed="$(( $(date +%s) - started ))"
  generated="$(awk '/states generated, .* distinct states found/{value=$1} END{print value}' "${log}" | tr -d ',')"
  distinct="$(awk '/states generated, .* distinct states found/{value=$4} END{print value}' "${log}" | tr -d ',')"
  generated="${generated:-0}"
  distinct="${distinct:-0}"
  printf '%s{"configuration":"%s","model":"%s","configuration_sha256":"%s","model_sha256":"%s","generated_states":%s,"distinct_states":%s,"runtime_seconds":%s,"result":"pass"}' \
    "${separator}" "${configuration}" "${model}" "$(checksum "${configuration}")" \
    "$(checksum "${model}")" "${generated}" "${distinct}" "${elapsed}" \
    >> "${OUTPUT_ROOT}/manifest.json"
  separator=","
done

printf ']}\n' >> "${OUTPUT_ROOT}/manifest.json"
