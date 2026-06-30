#!/usr/bin/env bash

set -euo pipefail

RUNNER_SCRIPT="${RUNNER_SCRIPT:-/opt/benchmark/query-runner-v2/run_query_runner_v2_sweep.sh}"
RUN_DATE="$(date +%Y%m%d-%H%M%S)"
RUN_LABEL="${RUN_LABEL:-$(date +%m%d%H)}"
LOG_FILE="${LOG_FILE:-/opt/benchmark/query-runner-v2/paper-q7-sweep-${RUN_DATE}.log}"
FLINK_CONF_SUFFIX="${FLINK_CONF_SUFFIX:-v2}"

WARMUP_EVENTS="10000000"
EVAL_EVENTS="2500000"

NO_FILTER_FIXED_PREFIX_BYTES="0"
NO_FILTER_BLOOM_BITS="0"
FILTER_FIXED_PREFIX_BYTES="0"
FILTER_BLOOM_BITS="10.0"

if [[ $# -gt 0 ]]; then
  TM_SIZES=("$@")
else
  TM_SIZES=("3g" "8g")
fi

export FLINK_HOME="${FLINK_HOME:-/opt/flink}"
export LOG_HOSTS_STRING="${LOG_HOSTS_STRING:-c155}"

log() {
  echo "$(date -Is) $*"
}

run_case() {
  local case_name=$1
  local job_name=$2
  local experiment_name=$3
  local fixed_prefix=$4
  local bloom_bits=$5

  log "========================================================================"
  log "Starting ${case_name}: job=${job_name}, experiment=${experiment_name}, tm=${TM_SIZES[*]}, fp=${fixed_prefix}, bloom=${bloom_bits}, warmup=${WARMUP_EVENTS}, eval=${EVAL_EVENTS}, run_label=${RUN_LABEL}"
  RUN_DATE_MMDDHH="$RUN_LABEL" JOB_NAME="$job_name" bash "$RUNNER_SCRIPT" \
    -c "$FLINK_CONF_SUFFIX" \
    --rocksdb-fixed-prefix-bytes "$fixed_prefix" \
    --rocksdb-bloom-filter-bits "$bloom_bits" \
    --warmup-events-num "$WARMUP_EVENTS" \
    --events-num "$EVAL_EVENTS" \
    "$experiment_name" \
    "${TM_SIZES[@]}"
  log "Completed ${case_name}"
}

exec > >(tee -a "$LOG_FILE") 2>&1

log "Logging to ${LOG_FILE}"
log "FLINK_HOME: ${FLINK_HOME}"
log "RocksDB log hosts: ${LOG_HOSTS_STRING}"
log "Run label: ${RUN_LABEL}"
log "TaskManager sizes: ${TM_SIZES[*]}"

run_case "NM1 baseline" "q7" "paper-draft" "$NO_FILTER_FIXED_PREFIX_BYTES" "$NO_FILTER_BLOOM_BITS"
run_case "NM1 + filters" "q7" "paper-draft" "$FILTER_FIXED_PREFIX_BYTES" "$FILTER_BLOOM_BITS"
run_case "NM2 - filters" "q7_unique" "paper-draft-3tables" "$NO_FILTER_FIXED_PREFIX_BYTES" "$NO_FILTER_BLOOM_BITS"
run_case "NM2" "q7_unique" "paper-draft-3tables" "$FILTER_FIXED_PREFIX_BYTES" "$FILTER_BLOOM_BITS"

log "Paper Q7 sweep complete."
