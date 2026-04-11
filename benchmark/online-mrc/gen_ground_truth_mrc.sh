#!/usr/bin/env bash
# gen_ground_truth_mrc.sh — produce the ground-truth MRC from a db_bench trace.
#
# Usage:
#   ./gen_ground_truth_mrc.sh --run-id NAME [--dry-run]
#
# Input:
#   results/<run-id>/trace.bin   (produced by run_bench.sh --mode trace)
#
# Pipeline:
#   1. Convert binary trace → human-readable CSV (via block_cache_trace_analyzer)
#   2. Filter to data-block + user-caller accesses only (awk)
#        block_type == 9            → data blocks only (excludes index/filter/metadata)
#        caller ∈ {1, 2, 3}        → Get, MultiGet, Iterator (excludes compaction etc.)
#   3. Run analyzer on filtered CSV → miss-ratio curve across a range of cache sizes
#
# Outputs (all under results/<run-id>/analysis/):
#   human.csv          full human-readable trace
#   filtered.csv       data-block + user-caller only
#   mrc                raw MRC from the analyzer (cache_size,miss_ratio pairs)
#   mrc.txt            combined config + mrc + analyzer log
#   run.log            raw analyzer stderr/stdout from step 3
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
  grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -25
  exit 0
}

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
RUN_ID=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)  RUN_ID="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage ;;
    *) fail "Unknown argument: $1" ;;
  esac
done

[[ -n "$RUN_ID" ]] || fail "--run-id is required"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
TRACE_BIN="${RUN_DIR}/trace.bin"
ANALYSIS_DIR="${RUN_DIR}/analysis"
HUMAN_CSV="${ANALYSIS_DIR}/human.csv"
FILTERED_CSV="${ANALYSIS_DIR}/filtered.csv"
CACHE_CFG="${ANALYSIS_DIR}/cache_config.txt"
MRC_OUT="${ANALYSIS_DIR}/mrc.txt"
RUN_LOG="${ANALYSIS_DIR}/run.log"

[[ -f "$TRACE_BIN" ]] \
  || fail "Trace not found: $TRACE_BIN  (run: ./run_bench.sh --mode trace --run-id $RUN_ID)"
[[ -x "$TRACE_ANALYZER" ]] \
  || fail "Analyzer not found: $TRACE_ANALYZER  (build: cd \$FROCKSDB_DIR && make -j\$(nproc) DEBUG_LEVEL=0 SNAPPY=1 block_cache_trace_analyzer)"

log "Run ID     : $RUN_ID"
log "Trace      : $TRACE_BIN ($(du -sh "$TRACE_BIN" | cut -f1))"
log "Analysis   : $ANALYSIS_DIR"

if $DRY_RUN; then
  log "DRY RUN — steps:"
  echo "  1. mkdir -p $ANALYSIS_DIR"
  echo "  2. $TRACE_ANALYZER -block_cache_trace_path=$TRACE_BIN -human_readable_trace_file_path=$HUMAN_CSV"
  echo "  3. awk -F, '\$3==9 && (\$9==1||\$9==2||\$9==3)' $HUMAN_CSV > $FILTERED_CSV"
  echo "  4. (write cache_config.txt)"
  echo "  5. $TRACE_ANALYZER -is_block_cache_human_readable_trace=true -mrc_only=true \\"
  echo "       -block_cache_trace_path=$FILTERED_CSV \\"
  echo "       -block_cache_sim_config_path=$CACHE_CFG \\"
  echo "       -block_cache_analysis_result_dir=$ANALYSIS_DIR \\"
  echo "       -block_cache_trace_downsample_ratio=1 \\"
  echo "       -cache_sim_warmup_seconds=0"
  exit 0
fi

mkdir -p "$ANALYSIS_DIR"

# ---------------------------------------------------------------------------
# Step 1: binary trace → human-readable CSV
# ---------------------------------------------------------------------------
log "Step 1/3: converting binary trace to human-readable CSV..."
"$TRACE_ANALYZER" \
  "-block_cache_trace_path=${TRACE_BIN}" \
  "-human_readable_trace_file_path=${HUMAN_CSV}" \
  2>&1 | grep -v '^$' || true

TOTAL_LINES=$(wc -l < "$HUMAN_CSV")
log "  Total accesses: $TOTAL_LINES"

# ---------------------------------------------------------------------------
# Step 2: filter — data blocks (type=9) + user callers (1=Get, 2=MultiGet, 3=Iterator)
# ---------------------------------------------------------------------------
log "Step 2/3: filtering to data-block + user-caller accesses..."
awk -F',' '$3 == 9 && ($9 == 1 || $9 == 2 || $9 == 3)' "$HUMAN_CSV" > "$FILTERED_CSV"

FILTERED_LINES=$(wc -l < "$FILTERED_CSV")
log "  Kept: $FILTERED_LINES / $TOTAL_LINES accesses ($(( FILTERED_LINES * 100 / (TOTAL_LINES + 1) ))%)"

# ---------------------------------------------------------------------------
# Step 3: run analyzer on filtered trace → MRC
# Cache sizes span a practical range (4KB … 4GB) to produce a full MRC curve.
# ---------------------------------------------------------------------------
log "Step 3/3: computing ground-truth MRC..."

cat > "$CACHE_CFG" <<'EOF'
lru,0,0,4K,8K,16K,32K,64K,128K,256K,512K,1M,2M,4M,8M,16M,32M,64M,128M,256M,512M,1G,2G,4G
EOF

"$TRACE_ANALYZER" \
  "-is_block_cache_human_readable_trace=true" \
  "-mrc_only=true" \
  "-block_cache_trace_path=${FILTERED_CSV}" \
  "-block_cache_sim_config_path=${CACHE_CFG}" \
  "-block_cache_analysis_result_dir=${ANALYSIS_DIR}" \
  "-block_cache_trace_downsample_ratio=1" \
  "-cache_sim_warmup_seconds=0" \
  2>&1 | tee "$RUN_LOG"

# The analyzer writes the MRC to a file named after the sim config (e.g. lru_0_mrc).
MRC_RAW=$(find "$ANALYSIS_DIR" -maxdepth 1 -name '*_mrc' | sort | head -1)

if [[ -z "$MRC_RAW" || ! -s "$MRC_RAW" ]]; then
  fail "Analyzer produced no MRC file in $ANALYSIS_DIR"
fi

# Combine into a single readable output file.
{
  echo "===== configuration ====="
  echo "run_id=${RUN_ID}"
  echo "trace=${TRACE_BIN}"
  echo "total_accesses=${TOTAL_LINES}"
  echo "filtered_accesses=${FILTERED_LINES}"
  echo "filter=data_blocks_only(type=9),user_callers_only(Get=1,MultiGet=2,Iterator=3)"
  echo "downsample_ratio=1 (ground truth — no sampling)"
  echo ""
  echo "===== miss ratio curve ====="
  echo "# cache_size_bytes,miss_ratio"
  cat "$MRC_RAW"
  echo ""
  echo "===== analyzer log ====="
  cat "$RUN_LOG"
} > "$MRC_OUT"

rm -f "$MRC_RAW"

log "Done."
log "MRC      : $MRC_OUT"
log "Full log : $RUN_LOG"
