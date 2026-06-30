#!/usr/bin/env bash
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-1.20.3}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"
DEST_DIR="${DEST_DIR:-/opt/flink}"
BASE_URL="${FLINK_BASE_URL:-https://archive.apache.org/dist/flink}"
ARCHIVE_NAME="flink-${FLINK_VERSION}-bin-scala_${SCALA_BINARY_VERSION}.tgz"
DOWNLOAD_URL="${FLINK_DOWNLOAD_URL:-${BASE_URL}/flink-${FLINK_VERSION}/${ARCHIVE_NAME}}"
VERIFY_SHA512="${VERIFY_SHA512:-1}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command '$1' not found." >&2
    exit 1
  fi
}

if [[ -e "${DEST_DIR}" ]]; then
  echo "ERROR: ${DEST_DIR} already exists. Remove it and retry." >&2
  exit 1
fi

require_cmd curl
require_cmd tar
if [[ "${VERIFY_SHA512}" != "0" ]]; then
  require_cmd sha512sum
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

ARCHIVE_PATH="${WORK_DIR}/${ARCHIVE_NAME}"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha512"

echo "Downloading ${DOWNLOAD_URL}"
curl -fL "${DOWNLOAD_URL}" -o "${ARCHIVE_PATH}"

if [[ "${VERIFY_SHA512}" != "0" ]]; then
  echo "Verifying SHA-512 checksum"
  curl -fL "${DOWNLOAD_URL}.sha512" -o "${CHECKSUM_PATH}"
  expected_checksum="$(awk '{print $1; exit}' "${CHECKSUM_PATH}")"
  actual_checksum="$(sha512sum "${ARCHIVE_PATH}" | awk '{print $1}')"
  if [[ "${expected_checksum}" != "${actual_checksum}" ]]; then
    echo "ERROR: SHA-512 mismatch for ${ARCHIVE_NAME}" >&2
    echo "Expected: ${expected_checksum}" >&2
    echo "Actual:   ${actual_checksum}" >&2
    exit 1
  fi
fi

tar -xzf "${ARCHIVE_PATH}" -C "${WORK_DIR}"
SRC_DIR="${WORK_DIR}/flink-${FLINK_VERSION}"

if [[ ! -d "${SRC_DIR}" ]]; then
  echo "ERROR: extracted Flink directory not found at ${SRC_DIR}" >&2
  exit 1
fi

mkdir -p "${DEST_DIR}"
cp -a "${SRC_DIR}/." "${DEST_DIR}/"

echo "Staged Apache Flink ${FLINK_VERSION} runtime from ${DOWNLOAD_URL} to ${DEST_DIR}"
