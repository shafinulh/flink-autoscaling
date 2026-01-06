#!/usr/bin/env bash

set -euo pipefail

CAPACITIES_MB=(1040)
BLOCK_CACHE_KEY="com.example.rocksdb.manual-block-cache-capacity-bytes"
JOB_WAIT_SECONDS=$((180 * 60))
COOLDOWN_SECONDS=60
DEST_ROOT="/opt/benchmark/mrc-gen/query8"
LOG_HOSTS=(c182 c155 c167)
SSH_USER="${SSH_USER:-root}"

FLINK_BIN="/opt/flink/bin"
NEXMARK_BIN="/opt/nexmark/bin"
JOB_JAR="/opt/flink-justin/benchmarks/target/Query8.jar"
JOB_ARGS=(--auction-srcRate 42000 --person-srcRate 12000 \
  --p-auction-source 2 --p-person-source 2 --disableOperatorChaining true)

mkdir -p "$DEST_ROOT"

cleanup() {
  echo "$(date -Is) Cleaning up cluster before exit..." >&2
  stop_cluster || true
}
trap cleanup EXIT

start_cluster() {
  echo "$(date -Is) Starting Flink + Nexmark cluster..."
  "${FLINK_BIN}/start-cluster.sh"
  "${NEXMARK_BIN}/setup_cluster.sh"
}

stop_cluster() {
  echo "$(date -Is) Stopping Flink + Nexmark cluster..."
  "${FLINK_BIN}/stop-cluster.sh" || true
  "${FLINK_BIN}/stop-cluster.sh" || true
  "${NEXMARK_BIN}/shutdown_cluster.sh" || true
}

submit_job() {
  local capacity_mb=$1
  local capacity_bytes=$((capacity_mb * 1024 * 1024))
  local log_dir="${DEST_ROOT}/exp-${capacity_mb}-bf"
  local log_file="${log_dir}/flink-run.log"
  mkdir -p "$log_dir"

  local cmd=(
    "${FLINK_BIN}/flink" run
    "-D${BLOCK_CACHE_KEY}=${capacity_bytes}"
    -p 1
    "$JOB_JAR"
    --rocksdb-block-cache-capacity-bytes "${capacity_bytes}"
    "${JOB_ARGS[@]}"
  )

  echo "$(date -Is) Submitting Query8 with block cache ${capacity_mb}MB (${capacity_bytes} bytes)..." | tee -a "$log_file"
  nohup "${cmd[@]}" >>"$log_file" 2>&1 &
  echo "$(date -Is) Job submission backgrounded with PID $! (output -> $log_file)" | tee -a "$log_file"
}

collect_logs() {
  local capacity_mb=$1
  local dest_dir="${DEST_ROOT}/exp-${capacity_mb}-bf"
  local log_file="${dest_dir}/log-collection.log"
  mkdir -p "$dest_dir"

  for host in "${LOG_HOSTS[@]}"; do
    echo "$(date -Is) Checking ${host} for RocksDB logs..." | tee -a "$log_file"
    local remote_listing=""
    if ! remote_listing=$(ssh "${SSH_USER}@${host}" 'shopt -s nullglob; for f in /opt/flink/log/data_rocksdb_job*; do echo "$f"; done' 2>/dev/null); then
      remote_listing=""
    fi
    if [[ -z "$remote_listing" ]]; then
      echo "$(date -Is) No data_rocksdb_job files found on ${host}" | tee -a "$log_file"
      continue
    fi
    while IFS= read -r remote_file; do
      [[ -z "$remote_file" ]] && continue
      local base
      base=$(basename "$remote_file")
      local dest_file="${dest_dir}/${base}_${host}"
      echo "$(date -Is) Copying ${remote_file} from ${host} -> ${dest_file}" | tee -a "$log_file"
      scp "${SSH_USER}@${host}:${remote_file}" "$dest_file"
    done <<< "$remote_listing"
  done
}

start_cluster
echo "$(date -Is) Waiting ${COOLDOWN_SECONDS}s before first submission..."
sleep "$COOLDOWN_SECONDS"

last_index=$((${#CAPACITIES_MB[@]} - 1))

for idx in "${!CAPACITIES_MB[@]}"; do
  capacity_mb=${CAPACITIES_MB[$idx]}
  echo "$(date -Is) ===== Sweep ${idx+1}/${#CAPACITIES_MB[@]}: ${capacity_mb}MB ====="

  submit_job "$capacity_mb"
  echo "$(date -Is) Allowing job to run for ${JOB_WAIT_SECONDS}s (~2h)..."
  sleep "$JOB_WAIT_SECONDS"

  echo "$(date -Is) Collecting logs while job is still running..."
  collect_logs "$capacity_mb"

  echo "$(date -Is) Stopping cluster after log collection..."
  stop_cluster
  echo "$(date -Is) Cooling down ${COOLDOWN_SECONDS}s before restart..."
  sleep "$COOLDOWN_SECONDS"

  if [[ $idx -lt $last_index ]]; then
    echo "$(date -Is) Preparing cluster for next sweep..."
    start_cluster
    echo "$(date -Is) Cooling down ${COOLDOWN_SECONDS}s before next submission..."
    sleep "$COOLDOWN_SECONDS"
  fi
done

echo "$(date -Is) All sweeps complete."
