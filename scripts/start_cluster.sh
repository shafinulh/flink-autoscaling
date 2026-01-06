#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log "Starting Flink + Nexmark"
maybe_sudo "${FLINK_HOME}/bin/start-cluster.sh"
maybe_sudo "${NEXMARK_HOME}/bin/setup_cluster.sh"
