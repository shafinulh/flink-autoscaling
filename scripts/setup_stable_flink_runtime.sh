#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="/opt/flink-from-remote-1-20"
DEST_DIR="/opt/flink"

if [[ ! -d "${SRC_DIR}" ]]; then
  echo "ERROR: ${SRC_DIR} not found." >&2
  exit 1
fi

if [[ -e "${DEST_DIR}" ]]; then
  echo "ERROR: ${DEST_DIR} already exists. Remove it and retry." >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
cp -a "${SRC_DIR}/." "${DEST_DIR}/"

echo "Staged Flink runtime from ${SRC_DIR} to ${DEST_DIR}"
