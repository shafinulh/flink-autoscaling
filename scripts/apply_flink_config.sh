#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log "Copying tracked Flink config into ${FLINK_HOME}"
maybe_sudo cp "${REPO_ROOT}/configs/flink/flink-conf.yaml" "${FLINK_HOME}/conf/flink-conf.yaml"
