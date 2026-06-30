#!/usr/bin/env bash

set -euo pipefail

RUNNER_SCRIPT="${RUNNER_SCRIPT:-/opt/benchmark/query-runner-v2/run_query_runner_v2_sweep.sh}"
RUN_DATE="$(date +%Y%m%d-%H%M%S)"
RUN_LABEL="${RUN_LABEL:-$(date +%m%d%H)}"
LOG_FILE="${LOG_FILE:-/opt/benchmark/query-runner-v2/paper-q9-watermark-alignment-sweep-${RUN_DATE}.log}"
FLINK_CONF_SUFFIX="${FLINK_CONF_SUFFIX:-v2}"

EXPERIMENT_NAME="paper-draft-3tables"
JOB_NAME_VALUE="q9_unique"
WARMUP_EVENTS="28000000"
EVAL_EVENTS="7000000"
TPS="100000"
MAX_EMIT_SPEED="false"
WATERMARK_ALIGNMENT_UPDATE_INTERVAL="5ms"
WATERMARK_ALIGNMENT_MAX_DRIFTS=("100ms" "1000s")

FIXED_PREFIX_BYTES="22"
BLOOM_FILTER_BITS="10.0"

if [[ $# -gt 0 ]]; then
  TM_SIZES=("$@")
else
  TM_SIZES=("1g")
fi

export FLINK_HOME="${FLINK_HOME:-/opt/flink}"
export LOG_HOSTS_STRING="${LOG_HOSTS_STRING:-c155}"

log() {
  echo "$(date -Is) $*"
}

run_case() {
  local max_drift=$1

  log "========================================================================"
  log "Starting q9 watermark-alignment case: tm=${TM_SIZES[*]}, fp=${FIXED_PREFIX_BYTES}, bloom=${BLOOM_FILTER_BITS}, wm_max_drift=${max_drift}, wm_update=${WATERMARK_ALIGNMENT_UPDATE_INTERVAL}, warmup=${WARMUP_EVENTS}, eval=${EVAL_EVENTS}, tps=${TPS}, max_emit_speed=${MAX_EMIT_SPEED}, run_label=${RUN_LABEL}"
  RUN_DATE_MMDDHH="$RUN_LABEL" JOB_NAME="$JOB_NAME_VALUE" bash "$RUNNER_SCRIPT" \
    -c "$FLINK_CONF_SUFFIX" \
    --rocksdb-fixed-prefix-bytes "$FIXED_PREFIX_BYTES" \
    --rocksdb-bloom-filter-bits "$BLOOM_FILTER_BITS" \
    --wm-alignment-max-drift "$max_drift" \
    --wm-alignment-update-interval "$WATERMARK_ALIGNMENT_UPDATE_INTERVAL" \
    --warmup-events-num "$WARMUP_EVENTS" \
    --events-num "$EVAL_EVENTS" \
    --tps "$TPS" \
    --max-emit-speed "$MAX_EMIT_SPEED" \
    "$EXPERIMENT_NAME" \
    "${TM_SIZES[@]}"
  log "Completed q9 watermark-alignment case: wm_max_drift=${max_drift}"
}

exec > >(tee -a "$LOG_FILE") 2>&1

log "Logging to ${LOG_FILE}"
log "FLINK_HOME: ${FLINK_HOME}"
log "RocksDB log hosts: ${LOG_HOSTS_STRING}"
log "Run label: ${RUN_LABEL}"
log "TaskManager sizes: ${TM_SIZES[*]}"

for max_drift in "${WATERMARK_ALIGNMENT_MAX_DRIFTS[@]}"; do
  run_case "$max_drift"
done

log "Paper Q9 watermark-alignment sweep complete."
