#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

# Usage: ./build_nexmark.sh [unique|separate-unique] [clean]
variant="unique"
clean=""
connector_src=""
connector_jar=""

for arg in "$@"; do
  case "$arg" in
    unique)
      variant="unique"
      ;;
    separate|separate-unique)
      variant="separate-unique"
      ;;
    clean)
      clean="clean"
      ;;
    *)
      echo "Usage: $0 [unique|separate-unique] [clean]" >&2
      exit 1
      ;;
  esac
done

case "$variant" in
  unique)
    nexmark_src="$NEXMARK_V2_SRC"
    ;;
  separate-unique)
    nexmark_src="$NEXMARK_SEPARATE_UNIQUE_SRC"
    connector_src="$NEXMARK_SEPARATE_UNIQUE_KAFKA_CONNECTOR_SRC"
    connector_jar="$connector_src/flink-sql-connector-kafka/target/flink-sql-connector-kafka-3.3.0.jar"
    ;;
esac

if [[ ! -d "$nexmark_src" ]]; then
  echo "Nexmark source directory not found: ${nexmark_src}" >&2
  exit 1
fi

if [[ -n "$connector_src" && ! -d "$connector_src" ]]; then
  echo "Kafka connector source directory not found: ${connector_src}" >&2
  exit 1
fi

if [[ "$clean" == "clean" ]]; then
  log "Removing ${NEXMARK_HOME}"
  maybe_sudo rm -rf "$NEXMARK_HOME"
fi

if [[ -n "$connector_src" ]]; then
  log "Building Kafka SQL connector in ${connector_src}"
  (
    cd "$connector_src" || exit 1
    mvn -pl flink-sql-connector-kafka -am -Dflink.version=1.20.0 -DskipTests package
  )
fi

log "Building Nexmark (${variant}) in ${nexmark_src}"
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

if [[ -n "$connector_jar" ]]; then
  log "Installing Kafka SQL connector into Flink + Nexmark"
  maybe_sudo find "${FLINK_HOME}/lib" -maxdepth 1 -type f -name 'flink-sql-connector-kafka-*.jar.bak-*' -delete
  maybe_sudo cp "$connector_jar" "${FLINK_HOME}/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"
  maybe_sudo cp "$connector_jar" "${NEXMARK_HOME}/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"
fi
