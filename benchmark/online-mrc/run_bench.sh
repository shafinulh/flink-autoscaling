#!/usr/bin/env bash
# run_bench.sh — drive db_bench with a block cache access workload.
#
# Usage:
#   ./run_bench.sh [--mode MODE] [--run-id NAME] [--sampling RATE]
#                 [--num-workload-ops N] [--dry-run]
#
# Modes:
#   plain                   — Run db_bench and collect RocksDB stats/LOG. No overhead.
#   trace                   — Run db_bench with block cache tracing (Zipfian workload).
#   shards                  — Run db_bench with online SHARDS MRC generation (Zipfian workload).
#                             Set sampling rate via --sampling (default: 1.0).
#   uniform                 — Like shards, but uses uniform key distribution (readrandom).
#   trace_uniform           — Like trace, but uses uniform key distribution.
#   phase_switch            — Like shards, uniform first half then Zipfian second half.
#   reverse_phase_switch    — Like shards, Zipfian first half then uniform second half.
#   trace_phase_switch      — Like trace, but uses readrandom_phase_switch workload.
#   trace_zipfian           — Like trace, but uses readrandom (reads only, Zipfian).
#
# After a trace run, analyze with:
#   block_cache_trace_analyzer \
#     --block_cache_trace_path=results/<run-id>/trace.bin \
#     --block_cache_analysis_result_dir=results/<run-id>/analysis
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
fail() { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; exit 1; }

usage() {
  grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -20
  exit 0
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
MODE="plain"
RUN_ID=""
DRY_RUN=false
SHARDS_SAMPLING="1.0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)             MODE="$2";              shift 2 ;;
    --run-id)           RUN_ID="$2";            shift 2 ;;
    --sampling)         SHARDS_SAMPLING="$2";   shift 2 ;;
    --num-workload-ops) NUM_WORKLOAD_OPS="$2";  shift 2 ;;
    --dry-run)          DRY_RUN=true;           shift ;;
    -h|--help)          usage ;;
    *) fail "Unknown argument: $1" ;;
  esac
done

[[ "$MODE" == "plain" || "$MODE" == "trace" || "$MODE" == "shards" || \
   "$MODE" == "uniform" || "$MODE" == "trace_uniform" || \
   "$MODE" == "phase_switch" || "$MODE" == "reverse_phase_switch" || \
   "$MODE" == "trace_phase_switch" || "$MODE" == "trace_zipfian" ]] \
  || fail "--mode must be one of: plain, trace, shards, uniform, trace_uniform, phase_switch, reverse_phase_switch, trace_phase_switch, trace_zipfian"

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="${MODE}_$(date '+%Y%m%d_%H%M%S')"
fi

# ---------------------------------------------------------------------------
# Validate prerequisites
# ---------------------------------------------------------------------------
[[ -x "$DB_BENCH" ]] \
  || fail "db_bench not found at $DB_BENCH. Build frocksdb first:\n  cd $FROCKSDB_DIR && make -j\$(nproc) DEBUG_LEVEL=0 SNAPPY=1 db_bench"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
DB_DIR="${RUN_DIR}/db"
LOG_FILE="${RUN_DIR}/bench.log"

CACHE_SIZE_BYTES=$(( CACHE_SIZE_MB * 1024 * 1024 ))
TRACE_FILE="${RUN_DIR}/trace.bin"
SHARDS_MRC_FILE="${RUN_DIR}/online_mrc.bin"

log "Run ID      : $RUN_ID"
log "Mode        : $MODE"
if [[ "$MODE" == "shards" || "$MODE" == "uniform" || "$MODE" == "phase_switch" || "$MODE" == "reverse_phase_switch" ]]; then
  log "SHARDS ratio: $SHARDS_SAMPLING"
fi
[[ "$MODE" == "trace_phase_switch" ]] && log "Tracing     : enabled (phase_switch workload)"
[[ "$MODE" == "trace_uniform" ]] && log "Tracing     : enabled (uniform workload)"
log "Keys        : $NUM_KEYS"
log "Fill ops    : $NUM_FILL_OPS"
log "Workload ops: $NUM_WORKLOAD_OPS"
log "Cache       : ${CACHE_SIZE_MB} MB"
log "Alpha       : $ZIPF_ALPHA"
log "RW ratio    : ${READ_WRITE_PERCENT}% reads / $((100 - READ_WRITE_PERCENT))% writes"
log "Threads     : $NUM_THREADS"
log "Seed        : $SEED"
log "Run dir     : $RUN_DIR"

# ---------------------------------------------------------------------------
# db_bench argument lists
# ---------------------------------------------------------------------------
# Common flags shared by both phases
COMMON_ARGS=(
  --key_dist="${KEY_DIST}"
  --zipf_alpha="${ZIPF_ALPHA}"
  --cache_size="${CACHE_SIZE_BYTES}"
  --threads="${NUM_THREADS}"
  --seed="${SEED}"
  --statistics
  --stats_dump_period_sec=60
  --db="${DB_DIR}"
)

if [[ "${CACHE_INDEX_AND_FILTER_BLOCKS}" == "true" ]]; then
  COMMON_ARGS+=(--cache_index_and_filter_blocks=true)
  log "Index/filter blocks : cached in block cache"
fi
if [[ "${USE_DIRECT_READS}" == "true" ]]; then
  COMMON_ARGS+=(--use_direct_reads=true --use_direct_io_for_flush_and_compaction=true)
  log "Direct IO           : enabled (O_DIRECT)"
fi

# Phase 1: fill NUM_FILL_OPS keys into the DB sequentially.
FILL_ARGS=(
  "${COMMON_ARGS[@]}"
  --benchmarks=fillrandom
  --num="${NUM_FILL_OPS}"
)

# Phase 2: NUM_WORKLOAD_OPS read/write ops over the full NUM_KEYS key space.
# --reads controls total op count for readrandomwriterandom (no --duration).
if [[ "$MODE" == "phase_switch" || "$MODE" == "trace_phase_switch" ]]; then
  WORKLOAD_ARGS=(
    "${COMMON_ARGS[@]}"
    --benchmarks=readrandom_phase_switch
    --use_existing_db
    --num="${NUM_KEYS}"
    --reads="${NUM_WORKLOAD_OPS}"
  )
elif [[ "$MODE" == "reverse_phase_switch" ]]; then
  WORKLOAD_ARGS=(
    "${COMMON_ARGS[@]}"
    --benchmarks=readrandom_reverse_phase_switch
    --use_existing_db
    --num="${NUM_KEYS}"
    --reads="${NUM_WORKLOAD_OPS}"
  )
elif [[ "$MODE" == "uniform" || "$MODE" == "trace_uniform" || "$MODE" == "trace_zipfian" ]]; then
  WORKLOAD_ARGS=(
    "${COMMON_ARGS[@]}"
    --benchmarks=readrandom
    --use_existing_db
    --num="${NUM_KEYS}"
    --reads="${NUM_WORKLOAD_OPS}"
  )
else
  WORKLOAD_ARGS=(
    "${COMMON_ARGS[@]}"
    --benchmarks=readrandomwriterandom
    --use_existing_db
    --num="${NUM_KEYS}"
    --reads="${NUM_WORKLOAD_OPS}"
    --readwritepercent="${READ_WRITE_PERCENT}"
  )
fi

if [[ "$MODE" == "trace" || "$MODE" == "trace_phase_switch" || "$MODE" == "trace_uniform" || "$MODE" == "trace_zipfian" ]]; then
  # Only attach the tracer to the workload phase, not the fill phase.
  # db_bench can only hold one active tracer per run; mixing benchmarks in
  # a single invocation with --block_cache_trace_file causes "Resource busy"
  # when the second benchmark tries to re-open the same file.
  WORKLOAD_ARGS+=(
    --block_cache_trace_file="${TRACE_FILE}"
    --block_cache_trace_max_trace_file_size_in_bytes="${MAX_TRACE_SIZE_BYTES}"
  )
fi

if [[ "$MODE" == "shards" || "$MODE" == "uniform" || "$MODE" == "phase_switch" || "$MODE" == "reverse_phase_switch" ]]; then
  # SHARDS is activated via environment variables read by BlockCacheTracer on open.
  # ROCKSDB_SHARDS_OUTPUT    — where to dump the MRC binary at DB close
  # ROCKSDB_SHARDS_RATIO     — sampling rate (1.0 = full sampling, 0.01 = 1%)
  # ROCKSDB_SHARDS_INTERVAL  — snapshot every N total accesses seen (0 = disabled)
  # ROCKSDB_SHARDS_BIN_SIZE  — histogram bin size in blocks
  # ROCKSDB_SHARDS_NUM_BINS  — number of histogram bins
  #
  # BIN_SIZE and NUM_BINS are chosen so that after the 1/s x-axis rescale in
  # plotting scripts, coverage spans 1MB–16GiB.  Formula:
  #   max_bytes = num_bins * bin_size * avg_block_size / s  (avg_bs ~4KB)
  # Per-rate table (targeting 1MB–16GiB coverage):
  #   s=1.0  : bin_size=10, num_bins=421057  → ~16GiB max after rescale
  #   s=0.1  : bin_size=10, num_bins=42106   → ~16GiB max after rescale
  #   s=0.01 : bin_size=1,  num_bins=42106   → ~16GiB max after rescale
  #   s=0.001: bin_size=1,  num_bins=4211    → ~16GiB max after rescale
  export ROCKSDB_SHARDS_OUTPUT="${SHARDS_MRC_FILE}"
  export ROCKSDB_SHARDS_RATIO="${SHARDS_SAMPLING}"
  if [[ -n "${ROCKSDB_SHARDS_INTERVAL:-}" ]]; then
    export ROCKSDB_SHARDS_INTERVAL
  fi
  case "${SHARDS_SAMPLING}" in
    1.0|1)   export ROCKSDB_SHARDS_BIN_SIZE=10;  export ROCKSDB_SHARDS_NUM_BINS=421057 ;;
    0.1)     export ROCKSDB_SHARDS_BIN_SIZE=10;  export ROCKSDB_SHARDS_NUM_BINS=42106  ;;
    0.01)    export ROCKSDB_SHARDS_BIN_SIZE=1;   export ROCKSDB_SHARDS_NUM_BINS=42106  ;;
    0.001)   export ROCKSDB_SHARDS_BIN_SIZE=1;   export ROCKSDB_SHARDS_NUM_BINS=4211   ;;
    0.0001)  export ROCKSDB_SHARDS_BIN_SIZE=1;   export ROCKSDB_SHARDS_NUM_BINS=421    ;;
    0.00001) export ROCKSDB_SHARDS_BIN_SIZE=1;   export ROCKSDB_SHARDS_NUM_BINS=42     ;;
    *)
      # Unknown rate: fall back to env vars if set
      if [[ -n "${ROCKSDB_SHARDS_BIN_SIZE:-}" ]]; then export ROCKSDB_SHARDS_BIN_SIZE; fi
      if [[ -n "${ROCKSDB_SHARDS_NUM_BINS:-}" ]];  then export ROCKSDB_SHARDS_NUM_BINS; fi
      ;;
  esac
fi

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

# db_bench prints "... finished N ops\r" progress lines to stderr.
# Strip them so the log stays readable; real errors still pass through.
filter_progress() { grep -v '^\.\.\..*ops'; }

WORKLOAD_DESC="readrandomwriterandom"
[[ "$MODE" == "uniform"         || "$MODE" == "trace_uniform"       ]] && WORKLOAD_DESC="readrandom (uniform)"
[[ "$MODE" == "trace_zipfian"  ]] && WORKLOAD_DESC="readrandom (zipfian, reads only)"
[[ "$MODE" == "phase_switch"    || "$MODE" == "trace_phase_switch"  ]] && WORKLOAD_DESC="readrandom_phase_switch (uniform→zipf)"
[[ "$MODE" == "reverse_phase_switch" ]] && WORKLOAD_DESC="readrandom_reverse_phase_switch (zipf→uniform)"

if $DRY_RUN; then
  log "DRY RUN — would execute:"
  echo "  mkdir -p ${RUN_DIR}"
  if [[ "$MODE" == "shards" || "$MODE" == "uniform" || "$MODE" == "phase_switch" || "$MODE" == "reverse_phase_switch" ]]; then
    echo "  export ROCKSDB_SHARDS_OUTPUT=${SHARDS_MRC_FILE}"
    echo "  export ROCKSDB_SHARDS_RATIO=${SHARDS_SAMPLING}"
  fi
  if [[ "$MODE" == "trace" || "$MODE" == "trace_phase_switch" || "$MODE" == "trace_uniform" || "$MODE" == "trace_zipfian" ]]; then
    echo "  # block cache tracing -> ${TRACE_FILE}"
  fi
  echo "  # Phase 1: fill ${NUM_FILL_OPS} keys"
  echo "  ${DB_BENCH} ${FILL_ARGS[*]} 2>&1 | filter_progress | tee ${LOG_FILE}"
  echo "  # Phase 2: ${NUM_WORKLOAD_OPS} ops ${WORKLOAD_DESC} over ${NUM_KEYS}-key space"
  echo "  ${DB_BENCH} ${WORKLOAD_ARGS[*]} 2>&1 | filter_progress | tee -a ${LOG_FILE}"
  exit 0
fi

mkdir -p "${RUN_DIR}"

log "Phase 1: filling DB with ${NUM_FILL_OPS} keys..."
"${DB_BENCH}" "${FILL_ARGS[@]}" 2>&1 | filter_progress | tee "${LOG_FILE}"

log "Phase 2: ${NUM_WORKLOAD_OPS} ops ${WORKLOAD_DESC} over ${NUM_KEYS}-key space..."
"${DB_BENCH}" "${WORKLOAD_ARGS[@]}" 2>&1 | filter_progress | tee -a "${LOG_FILE}"

# ---------------------------------------------------------------------------
# Post-run: collect artifacts
# ---------------------------------------------------------------------------
log "Run complete. Collecting artifacts..."

# Copy the RocksDB LOG file if present
ROCKSDB_LOG="${DB_DIR}/LOG"
if [[ -f "$ROCKSDB_LOG" ]]; then
  cp "$ROCKSDB_LOG" "${RUN_DIR}/rocksdb.LOG"
  log "Copied RocksDB LOG -> ${RUN_DIR}/rocksdb.LOG"
fi

if [[ "$MODE" == "trace" || "$MODE" == "trace_phase_switch" || "$MODE" == "trace_uniform" || "$MODE" == "trace_zipfian" ]]; then
  if [[ -f "$TRACE_FILE" ]]; then
    TRACE_SIZE=$(du -sh "$TRACE_FILE" | cut -f1)
    log "Trace: ${TRACE_FILE} (${TRACE_SIZE})"
    log ""
    log "To analyze:"
    log "  block_cache_trace_analyzer \\"
    log "    --block_cache_trace_path=${TRACE_FILE} \\"
    log "    --block_cache_analysis_result_dir=${RUN_DIR}/analysis"
  else
    log "WARNING: trace file not found — block cache tracing may have been disabled at compile time."
  fi
fi

if [[ "$MODE" == "shards" || "$MODE" == "uniform" || "$MODE" == "phase_switch" || "$MODE" == "reverse_phase_switch" ]]; then
  if [[ -f "$SHARDS_MRC_FILE" ]]; then
    log "Online MRC: ${SHARDS_MRC_FILE}"
  else
    log "WARNING: SHARDS MRC file not found — check that frocksdb was built with SHARDS support and ROCKSDB_SHARDS_OUTPUT was set correctly."
  fi
fi

log "All artifacts in: $RUN_DIR"
