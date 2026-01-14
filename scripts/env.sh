#!/usr/bin/env bash

# Basic defaults for this repo. Override in your shell if needed.
FLINK_HOME=${FLINK_HOME:-/opt/flink}
NEXMARK_HOME=${NEXMARK_HOME:-/opt/nexmark}
JUSTIN_FLINK_HOME=${JUSTIN_FLINK_HOME:-/opt/justin-custom-flink}
NEXMARK_SRC_BASE=${NEXMARK_SRC_BASE:-/opt/nexmark-src/nexmark-flink}
NEXMARK_SRC_CUSTOM=${NEXMARK_SRC_CUSTOM:-/opt/nexmark-src/nexmark-flink-scaling}
NEXMARK_V2_SRC=${NEXMARK_V2_SRC:-/opt/nexmark-v2/nexmark-flink}
ROCKSDB_OPTIONS_HOME=${ROCKSDB_OPTIONS_HOME:-/opt/rocksdb-options}
WORKER_HOSTS=${WORKER_HOSTS:-""}
SSH_USER=${SSH_USER:-root}

log() {
  echo "$(date -Is) $*"
}

maybe_sudo() {
  if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    sudo "$@"
  else
    "$@"
  fi
}

if [[ -f "${BASH_SOURCE%/*}/env.local.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASH_SOURCE%/*}/env.local.sh"
fi
