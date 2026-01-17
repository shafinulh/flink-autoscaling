#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${FLINK_SRC:-/opt/flink_1-20_src}"

if [[ ! -d "${ROOT_DIR}" ]]; then
  echo "ERROR: ${ROOT_DIR} not found. Set FLINK_SRC or use /opt/flink_1-20_src." >&2
  exit 1
fi

cd "${ROOT_DIR}"
PATH="/opt/node-v16.13.2/bin:$PATH" ./mvnw -T1C -DskipTests clean install
