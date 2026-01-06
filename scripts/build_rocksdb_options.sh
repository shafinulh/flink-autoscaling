#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log "Building rocksdb-options in ${ROCKSDB_OPTIONS_HOME}"
(
  cd "$ROCKSDB_OPTIONS_HOME" || exit 1
  mvn -DskipTests package
)

jar_path="${ROCKSDB_OPTIONS_HOME}/target/rocksdb-options-1.0-SNAPSHOT.jar"
log "Copying jar into Flink + Justin"
maybe_sudo cp "$jar_path" "${FLINK_HOME}/lib/"
maybe_sudo cp "$jar_path" "${JUSTIN_FLINK_HOME}/"
