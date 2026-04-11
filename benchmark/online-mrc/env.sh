#!/usr/bin/env bash
# Default environment for online-mrc db_bench runs.
# Override any variable by creating env.local.sh in this directory.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to the frocksdb source tree (db_bench lives at $FROCKSDB_DIR/db_bench
# after building with: make -j$(nproc) DEBUG_LEVEL=0 SNAPPY=1 db_bench)
FROCKSDB_DIR="${FROCKSDB_DIR:-${SCRIPT_DIR}/../../frocksdb}"
DB_BENCH="${DB_BENCH:-${FROCKSDB_DIR}/db_bench}"
TRACE_ANALYZER="${TRACE_ANALYZER:-${FROCKSDB_DIR}/block_cache_trace_analyzer}"

# Workload parameters
NUM_KEYS="${NUM_KEYS:-20000000}"          # key space size (--num for workload phase)
NUM_FILL_OPS="${NUM_FILL_OPS:-20000000}" # ops for fill phase (10M)
NUM_WORKLOAD_OPS="${NUM_WORKLOAD_OPS:-10000000}"  # ops for read/write phase (2M)
CACHE_SIZE_MB="${CACHE_SIZE_MB:-128}"     # 128 MB block cache
KEY_DIST="${KEY_DIST:-zipfian}"           # 'zipfian' or 'uniform'
ZIPF_ALPHA="${ZIPF_ALPHA:-0.6}"           # lower = wider working set; 0.6 for broad locality
READ_WRITE_PERCENT="${READ_WRITE_PERCENT:-75}"   # 75% reads, 25% writes
NUM_THREADS="${NUM_THREADS:-1}"           # single thread for determinism
SEED="${SEED:-42}"                        # RNG seed for reproducibility

# RocksDB options (mimic Flink-like settings when enabled)
CACHE_INDEX_AND_FILTER_BLOCKS="${CACHE_INDEX_AND_FILTER_BLOCKS:-false}"  # true = index+filter in block cache (Flink default)
USE_DIRECT_READS="${USE_DIRECT_READS:-false}"                            # true = O_DIRECT for SST reads (Flink default)

# Block cache trace (mode=trace only)
MAX_TRACE_SIZE_BYTES="${MAX_TRACE_SIZE_BYTES:-$((10 * 1024 * 1024 * 1024))}"  # 10 GB cap

# Results land here (gitignored)
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"

# Source local overrides if present
if [[ -f "${SCRIPT_DIR}/env.local.sh" ]]; then
  # shellcheck source=/dev/null
  source "${SCRIPT_DIR}/env.local.sh"
fi
