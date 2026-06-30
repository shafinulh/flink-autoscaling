#!/usr/bin/env bash

set -euo pipefail

RUNNER_SCRIPT="/opt/benchmark/query-runner-v2/run_query_runner_v2_sweep.sh"
RUN_DATE="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="/opt/benchmark/query-runner-v2/filter-matrix-overnight-${RUN_DATE}.log"
EXPERIMENT_NAME="paper-draft-3tables"
TM_SIZES=("3g")

Q20_FILTER_FIXED_PREFIX_BYTES="22"
Q20_FILTER_BLOOM_BITS="10.0"
Q20_WARMUP_EVENTS="15000000"
Q20_EVAL_EVENTS="5000000"
Q20_PERSON_PROPORTION="3"
Q20_AUCTION_PROPORTION="9"
Q20_BID_PROPORTION="38"

# Run the requested drift points in this order.
WATERMARK_ALIGNMENT_MAX_DRIFTS=("100ms" "1000s" "5s" "1s" "100s")

# Optional workload knobs forwarded to run_query_runner_v2_sweep.sh.
# Set OOOGS alone, or set PROB_DELAY/DELAY_MIN/DELAY_MAX together.
OOOGS="${OOOGS:-}"
PROB_DELAY="${PROB_DELAY:-}"
DELAY_MIN="${DELAY_MIN:-}"
DELAY_MAX="${DELAY_MAX:-}"
BASE_OOOGS="$OOOGS"
BASE_PROB_DELAY="$PROB_DELAY"
BASE_DELAY_MIN="$DELAY_MIN"
BASE_DELAY_MAX="$DELAY_MAX"
RUNNER_WORKLOAD_ARGS=()

log() {
  echo "$(date -Is) $*"
}

validate_workload_knobs() {
  local delay_count=0
  [[ -n "$PROB_DELAY" ]] && ((delay_count++)) || true
  [[ -n "$DELAY_MIN" ]] && ((delay_count++)) || true
  [[ -n "$DELAY_MAX" ]] && ((delay_count++)) || true

  if (( delay_count > 0 && delay_count < 3 )); then
    log "ERROR: PROB_DELAY, DELAY_MIN, and DELAY_MAX must all be specified together"
    exit 2
  fi
}

build_runner_workload_args() {
  RUNNER_WORKLOAD_ARGS=()

  if [[ -n "$OOOGS" ]]; then
    RUNNER_WORKLOAD_ARGS+=(--ooogs "$OOOGS")
  fi

  if [[ -n "$PROB_DELAY" ]]; then
    RUNNER_WORKLOAD_ARGS+=(--prob "$PROB_DELAY" --delay-min "$DELAY_MIN" --delay-max "$DELAY_MAX")
  fi
}

set_workload_knobs() {
  local variant_label=$1
  local ooogs=$2
  local prob_delay=$3
  local delay_min=$4
  local delay_max=$5

  OOOGS="$ooogs"
  PROB_DELAY="$prob_delay"
  DELAY_MIN="$delay_min"
  DELAY_MAX="$delay_max"

  validate_workload_knobs
  build_runner_workload_args

  if ((${#RUNNER_WORKLOAD_ARGS[@]} > 0)); then
    log "Workload knobs for ${variant_label}: ${RUNNER_WORKLOAD_ARGS[*]}"
  else
    log "Workload knobs for ${variant_label}: default"
  fi
}

run_case() {
  local job_name=$1
  local experiment_name=$2
  local fixed_prefix=$3
  local bloom_bits=$4
  local max_drift=$5
  local warmup_events=$6
  local eval_events=$7

  log "========================================================================"
  log "Starting case: job=${job_name}, experiment=${experiment_name}, filters=on, tm=${TM_SIZES[*]}, fixed_prefix=${fixed_prefix}, bloom_bits=${bloom_bits}, wm_max_drift=${max_drift}, warmup=${warmup_events}, eval=${eval_events}"
  JOB_NAME="$job_name" bash "$RUNNER_SCRIPT" \
    -c v2 \
    --rocksdb-fixed-prefix-bytes "$fixed_prefix" \
    --rocksdb-bloom-filter-bits "$bloom_bits" \
    --wm-alignment-max-drift "$max_drift" \
    --warmup-events-num "$warmup_events" \
    --events-num "$eval_events" \
    --person-proportion "$Q20_PERSON_PROPORTION" \
    --auction-proportion "$Q20_AUCTION_PROPORTION" \
    --bid-proportion "$Q20_BID_PROPORTION" \
    "${RUNNER_WORKLOAD_ARGS[@]}" \
    "$experiment_name" \
    "${TM_SIZES[@]}"
  log "Completed case: job=${job_name}, experiment=${experiment_name}, filters=on, tm=${TM_SIZES[*]}, wm_max_drift=${max_drift}"
}

run_q20_unique_filter_sweep() {
  local max_drift

  for max_drift in "${WATERMARK_ALIGNMENT_MAX_DRIFTS[@]}"; do
    run_case "q20_unique" "$EXPERIMENT_NAME" \
      "$Q20_FILTER_FIXED_PREFIX_BYTES" \
      "$Q20_FILTER_BLOOM_BITS" \
      "$max_drift" \
      "$Q20_WARMUP_EVENTS" \
      "$Q20_EVAL_EVENTS"
  done
}

exec > >(tee -a "$LOG_FILE") 2>&1

log "Logging to ${LOG_FILE}"

set_workload_knobs "$EXPERIMENT_NAME" "$BASE_OOOGS" "$BASE_PROB_DELAY" "$BASE_DELAY_MIN" "$BASE_DELAY_MAX"

run_q20_unique_filter_sweep
