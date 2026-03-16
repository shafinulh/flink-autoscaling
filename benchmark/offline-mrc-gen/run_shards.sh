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
Usage: run_shards.sh --human-trace PATH --ground-truth PATH --run-id NAME [--sampling VALUE] [--dry-run]

  --human-trace   Prepared human-readable trace to convert to Kia.
  --ground-truth  Ground-truth MRC text file to compare against.
  --run-id        Output name for SHARDS bins and plots.
  --sampling      SHARDS sampling value. Defaults to DEFAULT_SHARDS_SAMPLING from env.sh.
  --dry-run       Print resolved commands without running them.
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

ensure_online_mrc_build() {
  local exe="${ONLINE_MRC_BUILD_DIR}/src/run/generate_mrc_exe"

  if [[ -x "${exe}" ]]; then
    printf '%s\n' "${exe}"
    return
  fi

  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "$(date +"%Y-%m-%dT%H:%M:%S%z") generate_mrc_exe is missing; dry-run will assume it will be built at ${exe}" >&2
    printf '%s\n' "${exe}"
    return
  fi

  require_cmd meson
  [[ -d "${ONLINE_MRC_DIR}" ]] || fail "ONLINE_MRC_DIR does not exist: ${ONLINE_MRC_DIR}"

  log "generate_mrc_exe not found; building online_mrc under ${ONLINE_MRC_BUILD_DIR}"
  if [[ ! -d "${ONLINE_MRC_BUILD_DIR}" ]]; then
    meson setup "${ONLINE_MRC_BUILD_DIR}" "${ONLINE_MRC_DIR}"
  fi
  meson compile -C "${ONLINE_MRC_BUILD_DIR}" generate_mrc_exe

  [[ -x "${exe}" ]] || fail "generate_mrc_exe is still missing after build: ${exe}"
  printf '%s\n' "${exe}"
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
HUMAN_TRACE_PATH=""
GROUND_TRUTH_PATH=""
RUN_ID=""
SHARDS_SAMPLING="${DEFAULT_SHARDS_SAMPLING}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --human-trace)
      shift
      [[ $# -gt 0 ]] || fail "--human-trace requires a value"
      HUMAN_TRACE_PATH="$1"
      ;;
    --ground-truth)
      shift
      [[ $# -gt 0 ]] || fail "--ground-truth requires a value"
      GROUND_TRUTH_PATH="$1"
      ;;
    --run-id)
      shift
      [[ $# -gt 0 ]] || fail "--run-id requires a value"
      RUN_ID="$1"
      ;;
    --sampling)
      shift
      [[ $# -gt 0 ]] || fail "--sampling requires a value"
      SHARDS_SAMPLING="$1"
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

[[ -n "${HUMAN_TRACE_PATH}" ]] || fail "--human-trace is required"
[[ -n "${GROUND_TRUTH_PATH}" ]] || fail "--ground-truth is required"
[[ -n "${RUN_ID}" ]] || fail "--run-id is required"

require_cmd python3
require_file "${HUMAN_TRACE_PATH}"
require_file "${GROUND_TRUTH_PATH}"
[[ -d "${ONLINE_MRC_DIR}" ]] || fail "ONLINE_MRC_DIR does not exist: ${ONLINE_MRC_DIR}"

GENERATE_MRC_EXE="$(ensure_online_mrc_build)"

HUMAN_TRACE_STEM="$(basename "${HUMAN_TRACE_PATH}")"
HUMAN_TRACE_STEM="${HUMAN_TRACE_STEM%.txt}"
TRACE_BASE_NAME="${HUMAN_TRACE_STEM%_human}"
KIA_TRACE_PATH="${OUTPUT_DIR}/${TRACE_BASE_NAME}_kia.bin"
SHARDS_MRC_PATH="${OUTPUT_DIR}/${RUN_ID}_mrc.bin"
SHARDS_HIST_PATH="${OUTPUT_DIR}/${RUN_ID}_hist.bin"
MRC_PLOT_PATH="${MRC_PLOTS_DIR}/${RUN_ID}.png"
COMPARISON_PLOT_PATH="${MRC_VS_GROUND_TRUTH_PLOTS_DIR}/${RUN_ID}.png"
MRC_PLOT_TITLE="${RUN_ID} (s=${SHARDS_SAMPLING})"
COMPARISON_PLOT_TITLE="${RUN_ID} (s=${SHARDS_SAMPLING})"

MRC_PLOT_CMD=(
  python3
  "${ONLINE_MRC_DIR}/src/analysis/plot/plot_mrc.py"
  --input "${SHARDS_MRC_PATH}"
  --output "${MRC_PLOT_PATH}"
  --title "${MRC_PLOT_TITLE}"
)

COMPARISON_PLOT_CMD=(
  python3
  "${ONLINE_MRC_DIR}/scripts/plot_shards_vs_groundtruth.py"
  --shards "${SHARDS_MRC_PATH}"
  --truth "${GROUND_TRUTH_PATH}"
  --trace-csv "${HUMAN_TRACE_PATH}"
  --output "${COMPARISON_PLOT_PATH}"
  --title "${COMPARISON_PLOT_TITLE}"
)

mkdir -p "${OUTPUT_DIR}" "${MRC_PLOTS_DIR}" "${MRC_VS_GROUND_TRUTH_PLOTS_DIR}"

log "Human-readable trace: ${HUMAN_TRACE_PATH}"
log "Ground truth: ${GROUND_TRUTH_PATH}"
log "Kia trace: ${KIA_TRACE_PATH}"
log "SHARDS MRC: ${SHARDS_MRC_PATH}"
log "SHARDS histogram: ${SHARDS_HIST_PATH}"
log "SHARDS sampling: ${SHARDS_SAMPLING}"

if [[ ! -e "${KIA_TRACE_PATH}" || "${HUMAN_TRACE_PATH}" -nt "${KIA_TRACE_PATH}" ]]; then
  log "Converting human-readable trace to Kia format"
  run_cmd \
    python3 \
    "${ONLINE_MRC_DIR}/scripts/rocksdb_trace_to_kia.py" \
    --input "${HUMAN_TRACE_PATH}" \
    --output "${KIA_TRACE_PATH}"
else
  log "Reusing existing Kia trace ${KIA_TRACE_PATH}"
fi

log "Running SHARDS"
run_cmd \
  "${GENERATE_MRC_EXE}" \
  -i "${KIA_TRACE_PATH}" \
  -f Kia \
  -r "Fixed-Rate-SHARDS(mrc=${SHARDS_MRC_PATH},hist=${SHARDS_HIST_PATH},sampling=${SHARDS_SAMPLING})"

log "Plotting SHARDS MRC"
run_cmd "${MRC_PLOT_CMD[@]}"

log "Plotting SHARDS versus ground truth"
run_cmd "${COMPARISON_PLOT_CMD[@]}"

log "Done"
log "MRC plot: ${MRC_PLOT_PATH}"
log "Comparison plot: ${COMPARISON_PLOT_PATH}"
