#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${FLINK_SRC:-/opt/flink_1-20_src}"
DIST_ROOT="${ROOT_DIR}/flink-dist/target"
DEST_DIR="/opt/flink"
# NOTE: update "${FLINK_HOME}/conf/workers" with your worker IPs after staging.
# Typical flow: build frocksdb, build nexmark, apply the desired Flink config,
# then sync the cluster.

if [[ ! -d "${DIST_ROOT}" ]]; then
  echo "ERROR: ${DIST_ROOT} not found. Set FLINK_SRC or use /opt/flink_1-20_src." >&2
  exit 1
fi

mapfile -t candidates < <(ls -td "${DIST_ROOT}"/flink-*-bin/flink-* 2>/dev/null || true)
if [[ ${#candidates[@]} -eq 0 ]]; then
  cat >&2 <<'EOF'
ERROR: No built Flink distribution found.
Build it first, for example:
  PATH="/opt/node-v16.13.2/bin:$PATH" ./mvnw -T1C -DskipTests clean install
EOF
  exit 1
fi

SRC_DIR="${candidates[0]}"
BACKUP_DIR=""

if [[ -e "${DEST_DIR}" ]]; then
  echo "ERROR: ${DEST_DIR} already exists. Remove it and retry." >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
cp -a "${SRC_DIR}/." "${DEST_DIR}/"

echo "Staged Flink runtime from ${SRC_DIR} to ${DEST_DIR}"
