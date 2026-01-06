#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

# Usage: ./build_nexmark.sh [base|custom] [clean]
variant=${1:-base}
clean=${2:-}

if [[ "$variant" == "custom" ]]; then
  nexmark_src="$NEXMARK_SRC_CUSTOM"
else
  nexmark_src="$NEXMARK_SRC_BASE"
fi

if [[ "$clean" == "clean" ]]; then
  log "Removing ${NEXMARK_HOME}"
  maybe_sudo rm -rf "$NEXMARK_HOME"
fi

log "Building Nexmark (${variant}) in ${nexmark_src}"
(
  cd "$nexmark_src" || exit 1
  ./build.sh
)

log "Installing Nexmark into ${NEXMARK_HOME}"
maybe_sudo tar -xzf "${nexmark_src}/nexmark-flink.tgz" -C "$(dirname "$NEXMARK_HOME")"
maybe_sudo mv "$(dirname "$NEXMARK_HOME")/nexmark-flink" "$NEXMARK_HOME"

log "Copying Nexmark jars into Flink + Justin"
maybe_sudo cp "${NEXMARK_HOME}"/lib/*.jar "${FLINK_HOME}/lib/"
maybe_sudo cp "${NEXMARK_HOME}"/lib/*.jar "${JUSTIN_FLINK_HOME}/"
