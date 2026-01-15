#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log "Copying tracked Flink config into ${FLINK_HOME}"
CONFIG_VERSION="${1:-}"
CONFIG_FILE="${REPO_ROOT}/configs/flink/flink-conf.yaml"

case "${CONFIG_VERSION}" in
  "" )
    ;;
  v2 )
    CONFIG_FILE="${REPO_ROOT}/configs/flink/flink-conf-v2.yaml"
    ;;
  * )
    echo "Usage: $(basename "$0") [v2]" >&2
    exit 2
    ;;
esac

maybe_sudo cp "${CONFIG_FILE}" "${FLINK_HOME}/conf/flink-conf.yaml"
