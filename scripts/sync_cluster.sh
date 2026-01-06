#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

for host in $WORKER_HOSTS; do
  log "Syncing Flink to ${host}"
  maybe_sudo rsync -az "${FLINK_HOME}/" "${SSH_USER}@${host}:${FLINK_HOME}/"

  log "Syncing Nexmark to ${host}"
  maybe_sudo rsync -az "${NEXMARK_HOME}/" "${SSH_USER}@${host}:${NEXMARK_HOME}/"

done
