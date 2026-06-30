#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

clean=""

for arg in "$@"; do
  case "$arg" in
    clean)
      clean="clean"
      ;;
    *)
      echo "Usage: $0 [clean]" >&2
      exit 1
      ;;
  esac
done

nexmark_src="$NEXMARK_V2_SRC"
connector_src="$NEXMARK_KAFKA_CONNECTOR_SRC"
connector_jar="$connector_src/flink-sql-connector-kafka/target/flink-sql-connector-kafka-3.3.0.jar"

if [[ ! -d "$nexmark_src" ]]; then
  echo "Nexmark source directory not found: ${nexmark_src}" >&2
  exit 1
fi

if [[ ! -d "$connector_src" ]]; then
  echo "Kafka connector source directory not found: ${connector_src}" >&2
  exit 1
fi

if [[ "$clean" == "clean" ]]; then
  log "Removing ${NEXMARK_HOME}"
  maybe_sudo rm -rf "$NEXMARK_HOME"
fi

log "Building Kafka SQL connector in ${connector_src}"
(
  cd "$connector_src" || exit 1
  mkdir -p target
  mvn -pl flink-sql-connector-kafka -am -Dflink.version=1.20.0 -DskipTests package
)

log "Building Nexmark in ${nexmark_src}"
(
  cd "$nexmark_src" || exit 1
  ./build.sh
)

log "Installing Nexmark into ${NEXMARK_HOME}"
install_parent="$(dirname "$NEXMARK_HOME")"
extracted_dir="${install_parent}/nexmark-flink"
maybe_sudo rm -rf "$extracted_dir"
maybe_sudo tar -xzf "${nexmark_src}/nexmark-flink.tgz" -C "$install_parent"
maybe_sudo rm -rf "$NEXMARK_HOME"
maybe_sudo mv "$extracted_dir" "$NEXMARK_HOME"

log "Copying Nexmark jars into Flink"
maybe_sudo cp "${NEXMARK_HOME}"/lib/*.jar "${FLINK_HOME}/lib/"

if [[ ! -f "$connector_jar" ]]; then
  echo "Kafka connector jar not found after build: ${connector_jar}" >&2
  exit 1
fi

log "Installing Kafka SQL connector into Flink + Nexmark"
maybe_sudo find "${FLINK_HOME}/lib" -maxdepth 1 -type f -name 'flink-sql-connector-kafka-*.jar.bak-*' -delete
maybe_sudo cp "$connector_jar" "${FLINK_HOME}/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"
maybe_sudo cp "$connector_jar" "${NEXMARK_HOME}/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"
