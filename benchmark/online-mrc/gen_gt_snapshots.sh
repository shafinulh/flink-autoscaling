#!/usr/bin/env bash
# gen_gt_snapshots.sh — generate ground-truth MRC at each N*interval-access boundary.
#
# Truncates filtered.csv to the first N*INTERVAL lines and runs the LRU simulator
# on each subset, matching the snapshot boundaries produced by SHARDS runs.
# Jobs are run in parallel (default: 4 at a time).
#
# Usage:
#   ./gen_gt_snapshots.sh \
#     --filtered-csv /path/to/filtered.csv \
#     --output-dir   /path/to/gt_snapshots \
#     [--interval 1000000] [--parallel 4] [--dry-run]
#
# Output per snapshot N:
#   <output-dir>/gt_snapshot_N.txt   (mrc.txt-compatible miss-ratio-curve section)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
fail() { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
FILTERED_CSV=""
OUTPUT_DIR=""
INTERVAL=1000000
MAX_PARALLEL=4
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --filtered-csv) FILTERED_CSV="$2"; shift 2 ;;
    --output-dir)   OUTPUT_DIR="$2";   shift 2 ;;
    --interval)     INTERVAL="$2";     shift 2 ;;
    --parallel)     MAX_PARALLEL="$2"; shift 2 ;;
    --dry-run)      DRY_RUN=true;      shift ;;
    *) fail "Unknown argument: $1" ;;
  esac
done

[[ -n "$FILTERED_CSV" ]] || fail "--filtered-csv is required"
[[ -n "$OUTPUT_DIR" ]]   || fail "--output-dir is required"
[[ -f "$FILTERED_CSV" ]] || fail "filtered.csv not found: $FILTERED_CSV"
[[ -x "$TRACE_ANALYZER" ]] || fail "Analyzer not found: $TRACE_ANALYZER"

TOTAL_LINES=$(wc -l < "$FILTERED_CSV")
MAX_SNAP=$(( TOTAL_LINES / INTERVAL ))

log "filtered.csv  : $FILTERED_CSV  ($TOTAL_LINES lines)"
log "Output dir    : $OUTPUT_DIR"
log "Interval      : $INTERVAL accesses per snapshot"
log "Snapshots     : $MAX_SNAP  (N=1 .. $MAX_SNAP)"
log "Parallelism   : $MAX_PARALLEL"

if $DRY_RUN; then
  log "DRY RUN — would generate snapshots 1..$MAX_SNAP in $OUTPUT_DIR"
  exit 0
fi

mkdir -p "$OUTPUT_DIR"

# Shared cache config (written once, read by all parallel jobs)
CACHE_CFG="${OUTPUT_DIR}/cache_config.txt"
cat > "$CACHE_CFG" <<'EOF'
lru,0,0,4K,8K,16K,32K,64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M,128M,256M,512M,1G,2G,4G
EOF

# ---------------------------------------------------------------------------
# Per-snapshot worker function
# ---------------------------------------------------------------------------
run_snapshot() {
  local N="$1"
  local OUTFILE="${OUTPUT_DIR}/gt_snapshot_${N}.txt"

  if [[ -f "$OUTFILE" ]]; then
    log "  [snap $N] already exists — skipping"
    return
  fi

  local LINES=$(( N * INTERVAL ))
  local TMPDIR
  TMPDIR=$(mktemp -d "${OUTPUT_DIR}/tmp_snap_${N}_XXXXXX")

  # Truncated input CSV
  local TMPCSV="${TMPDIR}/filtered.csv"
  head -n "$LINES" "$FILTERED_CSV" > "$TMPCSV"

  # Run LRU simulator
  "$TRACE_ANALYZER" \
    "-is_block_cache_human_readable_trace=true" \
    "-mrc_only=true" \
    "-block_cache_trace_path=${TMPCSV}" \
    "-block_cache_sim_config_path=${CACHE_CFG}" \
    "-block_cache_analysis_result_dir=${TMPDIR}" \
    "-block_cache_trace_downsample_ratio=1" \
    "-cache_sim_warmup_seconds=0" \
    > "${TMPDIR}/run.log" 2>&1 || true

  # Find the MRC output file
  local MRC_RAW
  MRC_RAW=$(find "$TMPDIR" -maxdepth 1 -name '*_mrc' | sort | head -1)

  if [[ -z "$MRC_RAW" || ! -s "$MRC_RAW" ]]; then
    log "  [snap $N] WARNING: analyzer produced no MRC — skipping"
    rm -rf "$TMPDIR"
    return
  fi

  # Write output in mrc.txt-compatible format (miss ratio curve section only)
  {
    echo "===== miss ratio curve ====="
    echo "# cache_size_bytes,miss_ratio"
    cat "$MRC_RAW"
  } > "$OUTFILE"

  rm -rf "$TMPDIR"
  log "  [snap $N/$MAX_SNAP] done -> $(basename "$OUTFILE")"
}

export -f run_snapshot log
export OUTPUT_DIR INTERVAL MAX_SNAP FILTERED_CSV CACHE_CFG TRACE_ANALYZER

# ---------------------------------------------------------------------------
# Parallel execution — throttle to MAX_PARALLEL concurrent jobs
# ---------------------------------------------------------------------------
log "Starting $MAX_SNAP snapshot jobs (parallel=$MAX_PARALLEL)..."

ACTIVE=0
for N in $(seq 1 $MAX_SNAP); do
  run_snapshot "$N" &
  (( ACTIVE++ )) || true
  if (( ACTIVE >= MAX_PARALLEL )); then
    wait -n 2>/dev/null || wait
    (( ACTIVE-- )) || true
  fi
done

# Wait for all remaining jobs
wait

log "All $MAX_SNAP ground-truth snapshots complete."
log "Output: $OUTPUT_DIR"
