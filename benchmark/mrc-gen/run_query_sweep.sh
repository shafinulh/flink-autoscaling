#!/usr/bin/env bash

set -euo pipefail

JOBS=(q20_unique q9_unique)
BLOCK_CACHE_MB=130
JOB_WAIT_SECONDS=$((120 * 60))
COOLDOWN_SECONDS=60
DEST_ROOT="/opt/benchmark/mrc-gen"
LOG_HOSTS=(c182 c155 c167)
SSH_USER="${SSH_USER:-root}"

FLINK_BIN="/opt/flink/bin"
NEXMARK_BIN="/opt/nexmark/bin"
RUN_QUERY_CMD="${NEXMARK_BIN}/run_query.sh"
SETSID_AVAILABLE=false
if command -v setsid >/dev/null 2>&1; then
  SETSID_AVAILABLE=true
fi

mkdir -p "$DEST_ROOT"

export FLINK_TM_JVM_OPTS="${FLINK_TM_JVM_OPTS:-}"
export FLINK_JOBMANAGER_JVM_OPTS="${FLINK_JOBMANAGER_JVM_OPTS:-}"

cleanup() {
  echo "$(date -Is) Cleaning up cluster before exit..." >&2
  ensure_job_stopped "${current_job_pid:-}" "${current_job_pgid:-}" || true
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
  local job_name=$1
  local log_dir="${DEST_ROOT}/${job_name}/exp-${BLOCK_CACHE_MB}-bf-prefix"
  local log_file="${log_dir}/run_query.log"
  mkdir -p "$log_dir"

  echo "$(date -Is) Submitting ${job_name} with block cache ${BLOCK_CACHE_MB}MB..." | tee -a "$log_file"
  if [[ $SETSID_AVAILABLE == true ]]; then
    nohup setsid "${RUN_QUERY_CMD}" "$job_name" >>"$log_file" 2>&1 &
  else
    nohup "${RUN_QUERY_CMD}" "$job_name" >>"$log_file" 2>&1 &
  fi
  current_job_pid=$!
  if [[ $SETSID_AVAILABLE == true ]]; then
    current_job_pgid=$current_job_pid
  else
    current_job_pgid=$(ps -o pgid= "$current_job_pid" 2>/dev/null | tr -d ' ')
  fi
  echo "$current_job_pid" > "${log_dir}/run_query.pid"
  if [[ -n "$current_job_pgid" ]]; then
    echo "$current_job_pgid" > "${log_dir}/run_query.pgid"
  fi
  echo "$(date -Is) ${job_name} submitted (PID ${current_job_pid})." | tee -a "$log_file"
}

collect_logs() {
  local job_name=$1
  local dest_dir="${DEST_ROOT}/${job_name}/exp-${BLOCK_CACHE_MB}-bf-prefix"
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

ensure_job_stopped() {
  local pid=$1
  local pgid=${2:-}

  if [[ -n "$pgid" ]] && kill -0 "-$pgid" 2>/dev/null; then
    echo "$(date -Is) Terminating run_query process group ${pgid}..."
    kill -- -"$pgid" 2>/dev/null || true
    sleep 1
  fi

  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "$(date -Is) Terminating lingering run_query process ${pid}..."
    kill "$pid" 2>/dev/null || true
  fi
}

current_job_pid=""
current_job_pgid=""

start_cluster
echo "$(date -Is) Waiting ${COOLDOWN_SECONDS}s before first submission..."
sleep "$COOLDOWN_SECONDS"

last_index=$((${#JOBS[@]} - 1))

for idx in "${!JOBS[@]}"; do
  job_name=${JOBS[$idx]}
  echo "$(date -Is) ===== Sweep ${idx+1}/${#JOBS[@]}: ${job_name} ====="

  submit_job "$job_name"
  echo "$(date -Is) Allowing ${job_name} to run for ${JOB_WAIT_SECONDS}s (~2h)..."
  sleep "$JOB_WAIT_SECONDS"

  echo "$(date -Is) Collecting logs while ${job_name} is still running..."
  collect_logs "$job_name"

  echo "$(date -Is) Stopping cluster after log collection..."
  stop_cluster
  ensure_job_stopped "$current_job_pid" "$current_job_pgid"
  current_job_pid=""
  current_job_pgid=""

  if [[ $idx -lt $last_index ]]; then
    echo "$(date -Is) Cooling down ${COOLDOWN_SECONDS}s before restarting cluster..."
    sleep "$COOLDOWN_SECONDS"
    start_cluster
    echo "$(date -Is) Cooling down ${COOLDOWN_SECONDS}s before next submission..."
    sleep "$COOLDOWN_SECONDS"
  fi
done

echo "$(date -Is) Query sweep complete."
