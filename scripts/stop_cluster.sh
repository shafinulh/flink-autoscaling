#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log "Stopping Flink + Nexmark"
maybe_sudo "${FLINK_HOME}/bin/stop-cluster.sh" || true
maybe_sudo "${FLINK_HOME}/bin/stop-cluster.sh" || true
maybe_sudo "${NEXMARK_HOME}/bin/shutdown_cluster.sh" || true
