#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

log() {
  echo "$(date +"%Y-%m-%dT%H:%M:%S%z") $*"
}

fail() {
  echo "$(date +"%Y-%m-%dT%H:%M:%S%z") ERROR: $*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage: prepare_traces.sh [--trace PATH] [--dry-run]

  --trace    Raw RocksDB block-cache trace. Defaults to RAW_TRACE_PATH from env.sh.
  --dry-run  Print resolved commands without running them.
USAGE
}

require_cmd() {
  local cmd=$1
  command -v "${cmd}" >/dev/null 2>&1 || fail "missing required command: ${cmd}"
}

require_file() {
  local path=$1
  [[ -e "${path}" ]] || fail "required file does not exist: ${path}"
}

run_cmd() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    printf 'DRY RUN:'
    printf ' %q' "$@"
    printf '\n'
    return
  fi
  "$@"
}

DRY_RUN=0
TRACE_PATH="${RAW_TRACE_PATH}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trace)
      shift
      [[ $# -gt 0 ]] || fail "--trace requires a value"
      TRACE_PATH="$1"
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
  shift
done

[[ -n "${BLOCK_CACHE_TRACE_ANALYZER}" ]] || fail "BLOCK_CACHE_TRACE_ANALYZER must be set in env.local.sh"
require_cmd awk
require_file "${BLOCK_CACHE_TRACE_ANALYZER}"
require_file "${TRACE_PATH}"

HUMAN_TRACE_PATH="${OUTPUT_DIR}/human.txt"
DATA_ONLY_TRACE_PATH="${OUTPUT_DIR}/data_only_human.txt"
DATA_USER_ONLY_TRACE_PATH="${OUTPUT_DIR}/data_user_only_human.txt"

mkdir -p "${OUTPUT_DIR}"

log "Raw trace: ${TRACE_PATH}"
log "Human-readable trace: ${HUMAN_TRACE_PATH}"
log "Data-only trace: ${DATA_ONLY_TRACE_PATH}"
log "Data-user-only trace: ${DATA_USER_ONLY_TRACE_PATH}"

log "Converting raw trace to human-readable trace"
run_cmd \
  "${BLOCK_CACHE_TRACE_ANALYZER}" \
  "-block_cache_trace_path=${TRACE_PATH}" \
  "-human_readable_trace_file_path=${HUMAN_TRACE_PATH}"

if [[ "${DRY_RUN}" == "1" ]]; then
  printf 'DRY RUN:'
  printf ' %q' awk -F',' '$3 == 9' "${HUMAN_TRACE_PATH}" '>' "${DATA_ONLY_TRACE_PATH}"
  printf '\n'
  printf 'DRY RUN:'
  printf ' %q' awk -F',' '$3 == 9 && ($9 == 1 || $9 == 2 || $9 == 3)' "${HUMAN_TRACE_PATH}" '>' "${DATA_USER_ONLY_TRACE_PATH}"
  printf '\n'
  exit 0
fi

log "Filtering to data-block accesses only"
awk -F',' '$3 == 9' "${HUMAN_TRACE_PATH}" > "${DATA_ONLY_TRACE_PATH}"

log "Filtering to user data-block accesses only (caller 1,2,3)"
awk -F',' '$3 == 9 && ($9 == 1 || $9 == 2 || $9 == 3)' "${HUMAN_TRACE_PATH}" > "${DATA_USER_ONLY_TRACE_PATH}"

log "Done"
