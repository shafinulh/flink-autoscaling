#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log "Copying tracked Flink config into ${FLINK_HOME}"
CONFIG_SUFFIX="${1:-}"
CONFIG_FILE="${REPO_ROOT}/configs/flink/flink-conf.yaml"
if [[ -n "${CONFIG_SUFFIX}" ]]; then
  CONFIG_FILE="${REPO_ROOT}/configs/flink/flink-conf-${CONFIG_SUFFIX}.yaml"
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Unknown Flink config suffix: '${CONFIG_SUFFIX}'" >&2
  echo "Usage: $(basename "$0") [suffix]" >&2
  echo "Available suffixes:" >&2
  (cd "${REPO_ROOT}/configs/flink" && ls -1 flink-conf*.yaml 2>/dev/null \
    | awk '{if ($0=="flink-conf.yaml") {print "(none)"} else {gsub(/^flink-conf-/, "", $0); sub(/\\.yaml$/, "", $0); print $0}}') >&2
  exit 2
fi

maybe_sudo cp "${CONFIG_FILE}" "${FLINK_HOME}/conf/flink-conf.yaml"
