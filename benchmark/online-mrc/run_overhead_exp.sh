#!/usr/bin/env bash
# run_overhead_exp.sh — measure wall time and peak RAM overhead of SHARDS.
#
# Runs db_bench in plain mode (no SHARDS) and shards mode at 4 sampling rates.
# For each run, a background monitor polls /proc/PID/status to track peak RSS.
# Wall time is measured with the bash built-in 'time'. Results are written to
# overhead.txt in each run's directory.
#
# Note: ROCKSDB_SHARDS_INTERVAL is intentionally unset so snapshot I/O does not
# inflate the overhead figures — this measures the algorithm cost only.
#
# Usage:
#   RESULTS_DIR=/tom/distr_project \
#   bash run_overhead_exp.sh
#
# Outputs (one per run):
#   results/exp2_overhead_<mode>/overhead.txt
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
fail() { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; exit 1; }

[[ -x "$DB_BENCH" ]] || fail "db_bench not found: $DB_BENCH"

# Unset snapshot interval — measure algorithm overhead only, not snapshot I/O
unset ROCKSDB_SHARDS_INTERVAL

# ---------------------------------------------------------------------------
# Peak RSS monitor — runs as a background process.
#
# Tracks db_bench directly rather than the run_bench.sh wrapper shell.
# run_bench.sh is a bash script (~3MB RSS); db_bench is the actual process
# that allocates the block cache and SHARDS data structures. We find it by
# polling pgrep -x db_bench (exact name match). Since overhead runs are
# sequential there is only one db_bench at a time, so the name match is safe.
# ---------------------------------------------------------------------------
monitor_rss() {
  local PARENT_PID="$1"
  local OUTFILE="$2"
  local MAX_RSS=0
  local RSS TARGET_PID=""

  # Wait until db_bench appears
  while [[ -z "$TARGET_PID" ]] && kill -0 "$PARENT_PID" 2>/dev/null; do
    TARGET_PID=$(pgrep -x db_bench 2>/dev/null || true)
    [[ -z "$TARGET_PID" ]] && sleep 0.5
  done

  # Poll db_bench RSS until the parent shell (run_bench.sh) exits.
  # run_bench.sh launches two sequential db_bench invocations (fill + workload),
  # so TARGET_PID can die mid-run. Re-scan for a new db_bench when that happens.
  while kill -0 "$PARENT_PID" 2>/dev/null; do
    if [[ -n "$TARGET_PID" ]]; then
      RSS=$(awk '/VmRSS/{print $2}' "/proc/${TARGET_PID}/status" 2>/dev/null || true)
      if [[ -z "$RSS" ]]; then
        # Phase 1 db_bench exited; wait for Phase 2 to start
        TARGET_PID=$(pgrep -x db_bench 2>/dev/null || true)
        sleep 0.5
        continue
      fi
      (( RSS > MAX_RSS )) && MAX_RSS=$RSS
    fi
    sleep 1
  done

  echo "peak_rss_kb=${MAX_RSS}" >> "$OUTFILE"
}

# ---------------------------------------------------------------------------
# Run one experiment mode and record overhead
# ---------------------------------------------------------------------------
run_mode() {
  local MODE="$1"
  local SAMPLING="${2:-}"
  local RUN_ID="$3"
  local OVERHEAD_FILE="${RESULTS_DIR}/${RUN_ID}/overhead.txt"

  if [[ -f "$OVERHEAD_FILE" ]]; then
    log "[$RUN_ID] already exists — skipping"
    return
  fi

  log "[$RUN_ID] starting..."

  # Build run_bench.sh args
  local ARGS=(--mode "$MODE" --run-id "$RUN_ID")
  [[ -n "$SAMPLING" ]] && ARGS+=(--sampling "$SAMPLING")

  # Time the run, capture PID for RSS monitoring
  local START END ELAPSED
  START=$(date +%s%N)

  "${SCRIPT_DIR}/run_bench.sh" "${ARGS[@]}" &
  local BENCH_PID=$!

  # Start RSS monitor in background
  monitor_rss "$BENCH_PID" "$OVERHEAD_FILE" &
  local MON_PID=$!

  # Wait for benchmark to finish
  wait "$BENCH_PID"
  local EXIT_CODE=$?

  END=$(date +%s%N)
  wait "$MON_PID" 2>/dev/null || true

  ELAPSED=$(( (END - START) / 1000000 ))  # milliseconds

  # Write timing results
  {
    echo "run_id=${RUN_ID}"
    echo "mode=${MODE}"
    [[ -n "$SAMPLING" ]] && echo "sampling=${SAMPLING}"
    echo "wall_time_ms=${ELAPSED}"
    echo "wall_time_s=$(echo "scale=2; $ELAPSED / 1000" | bc)"
  } >> "$OVERHEAD_FILE"

  log "[$RUN_ID] done — ${ELAPSED}ms, results in $OVERHEAD_FILE"
}

# ---------------------------------------------------------------------------
# Run all 5 modes sequentially (they share the same DB fill, so run serially
# to avoid disk contention; each run recreates the DB from scratch)
# ---------------------------------------------------------------------------
log "Starting overhead experiments..."
log "Results dir: $RESULTS_DIR"
log ""

run_mode plain  ""        exp2_overhead_plain
run_mode shards "1.0"     exp2_overhead_s1
run_mode shards "0.1"     exp2_overhead_s01
run_mode shards "0.01"    exp2_overhead_s001
run_mode shards "0.001"   exp2_overhead_s0001
run_mode shards "0.0001"  exp2_overhead_s00001
run_mode shards "0.00001" exp2_overhead_s000001

log ""
log "All overhead experiments complete."
log "Results:"
for RUN_ID in exp2_overhead_plain exp2_overhead_s1 exp2_overhead_s01 exp2_overhead_s001 exp2_overhead_s0001 exp2_overhead_s00001 exp2_overhead_s000001; do
  OFILE="${RESULTS_DIR}/${RUN_ID}/overhead.txt"
  if [[ -f "$OFILE" ]]; then
    echo "  $RUN_ID:"
    grep -E 'wall_time_s|peak_rss_kb' "$OFILE" | sed 's/^/    /'
  fi
done
