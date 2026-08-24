#!/usr/bin/env sh
set -eu

node=${1:-}
confirm=${2:-}

case "$node" in
  node1|node2|node3) ;;
  *)
    echo "NODE must be exactly node1, node2, or node3" >&2
    exit 2
    ;;
esac

if [ "$confirm" != "yes" ]; then
  echo "refusing destructive reset; rerun with CONFIRM=yes" >&2
  exit 2
fi

docker compose stop "$node"
docker compose rm --force "$node"
docker volume rm "raft-kv-service_${node}-data"
