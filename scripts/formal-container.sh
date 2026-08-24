#!/usr/bin/env bash
set -euo pipefail

mode="${1:-full}"
export RAFT_KV_TLC_STATE_ROOT="${RAFT_KV_TLC_STATE_ROOT:-/evidence/states}"
export RAFT_KV_APALACHE_OUTPUT="${RAFT_KV_APALACHE_OUTPUT:-/evidence/apalache}"

case "${mode}" in
  fast)
    mkdir -p /evidence/tlc
    for configuration in \
      formal/RaftElection.cfg \
      formal/Raft.cfg \
      formal/KvStateMachine.cfg \
      formal/RaftSnapshot.cfg \
      formal/RaftMembership.cfg \
      formal/RaftLiveness.cfg
    do
      name="$(basename "${configuration}" .cfg)"
      ./scripts/tlc.sh check "${configuration}" 2>&1 | tee "/evidence/tlc/${name}.log"
    done
    ./scripts/apalache.sh check
    ;;
  full)
    RAFT_KV_FORMAL_OUTPUT=/evidence/tlc ./scripts/formal-matrix.sh
    ./scripts/apalache.sh check
    ;;
  *)
    printf 'usage: formal-container.sh [fast|full]\n' >&2
    exit 64
    ;;
esac
