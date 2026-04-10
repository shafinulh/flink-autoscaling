#!/usr/bin/env bash

# Basic defaults for this repo. Override in your shell if needed.
AUTOSCALING_ROOT=${AUTOSCALING_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
FLINK_HOME=${FLINK_HOME:-/opt/flink}
NEXMARK_HOME=${NEXMARK_HOME:-/opt/nexmark}
NEXMARK_V2_SRC=${NEXMARK_V2_SRC:-${AUTOSCALING_ROOT}/nexmark-v2/nexmark-flink}
NEXMARK_SEPARATE_UNIQUE_SRC=${NEXMARK_SEPARATE_UNIQUE_SRC:-/opt/nexmark-v2-separate-tables/nexmark-flink}
NEXMARK_SEPARATE_UNIQUE_KAFKA_CONNECTOR_SRC=${NEXMARK_SEPARATE_UNIQUE_KAFKA_CONNECTOR_SRC:-/opt/nexmark-v2-separate-tables/flink-connector-kafka-3.3.0}
ROCKSDB_OPTIONS_HOME=${ROCKSDB_OPTIONS_HOME:-${AUTOSCALING_ROOT}/rocksdb-options}
JUSTIN_FLINK_HOME=${JUSTIN_FLINK_HOME:-${AUTOSCALING_ROOT}/flink-justin}
WORKER_HOSTS=${WORKER_HOSTS:-""}
SSH_USER=${SSH_USER:-root}

log() {
  echo "$(date -Is) $*"
}

maybe_sudo() {
  if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    sudo -E "$@"
  else
    "$@"
  fi
}

if [[ -f "${BASH_SOURCE%/*}/env.local.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASH_SOURCE%/*}/env.local.sh"
fi
