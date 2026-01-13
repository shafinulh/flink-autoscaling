#!/usr/bin/env bash

set -euo pipefail

JOBS=(q20_unique q9_unique q7_unique)
TM_MEMORY_SIZES=(8g 6g 4g 3g)
JOB_WAIT_SECONDS=$((90 * 60))
COOLDOWN_SECONDS=60
DEST_ROOT="/opt/benchmark/managed-mem-sensitivity"
LOG_HOSTS=(c182 c155 c167)
SSH_USER="${SSH_USER:-root}"

FLINK_BIN="/opt/flink/bin"
NEXMARK_BIN="/opt/nexmark/bin"
RUN_QUERY_CMD="${NEXMARK_BIN}/run_query.sh"
FLINK_CONF_FILE="/opt/configs/flink/flink-conf.yaml"
APPLY_CONFIG_CMD="/opt/scripts/apply_flink_config.sh"
SYNC_CLUSTER_CMD="/opt/scripts/sync_cluster.sh"

SETSID_AVAILABLE=false
if command -v setsid >/dev/null 2>&1; then
  SETSID_AVAILABLE=true
fi

mkdir -p "$DEST_ROOT"

export FLINK_TM_JVM_OPTS="${FLINK_TM_JVM_OPTS:-}"
export FLINK_JOBMANAGER_JVM_OPTS="${FLINK_JOBMANAGER_JVM_OPTS:-}"

log() {
  echo "$(date -Is) $*"
}

cleanup() {
  log "Cleaning up cluster before exit..."
  ensure_job_stopped "${current_job_pid:-}" "${current_job_pgid:-}" || true
  stop_cluster || true
}
trap cleanup EXIT

start_cluster() {
  log "Starting Flink + Nexmark cluster..."
  "${FLINK_BIN}/start-cluster.sh"
  "${NEXMARK_BIN}/setup_cluster.sh"
}

stop_cluster() {
  log "Stopping Flink + Nexmark cluster..."
  "${FLINK_BIN}/stop-cluster.sh" || true
  "${FLINK_BIN}/stop-cluster.sh" || true
  "${NEXMARK_BIN}/shutdown_cluster.sh" || true
}

update_flink_conf() {
  local tm_size=$1
  local tmp_file
  tmp_file=$(mktemp)
  awk -v size="$tm_size" '
    BEGIN { updated = 0 }
    $1 == "taskmanager.memory.process.size:" {
      print "taskmanager.memory.process.size: " size
      updated = 1
      next
    }
    { print }
    END {
      if (updated == 0) {
        print "taskmanager.memory.process.size: " size
      }
    }
  ' "$FLINK_CONF_FILE" > "$tmp_file"
  mv "$tmp_file" "$FLINK_CONF_FILE"
}

apply_flink_conf() {
  if [[ -x "$APPLY_CONFIG_CMD" ]]; then
    "$APPLY_CONFIG_CMD"
  else
    cp "$FLINK_CONF_FILE" "/opt/flink/conf/flink-conf.yaml"
  fi
}

sync_cluster() {
  if [[ -x "$SYNC_CLUSTER_CMD" ]]; then
    "$SYNC_CLUSTER_CMD"
  fi
}

submit_job() {
  local job_name=$1
  local tm_size=$2
  local log_dir="${DEST_ROOT}/${job_name}-with_options/exp-${tm_size}-tm-process"
  local log_file="${log_dir}/run_query.log"
  mkdir -p "$log_dir"

  log "Submitting ${job_name} with taskmanager.memory.process.size=${tm_size}..." | tee -a "$log_file"
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
  log "${job_name} submitted (PID ${current_job_pid})." | tee -a "$log_file"
}

collect_logs() {
  local job_name=$1
  local tm_size=$2
  local dest_dir="${DEST_ROOT}/${job_name}-with_options/exp-${tm_size}-tm-process"
  local log_file="${dest_dir}/log-collection.log"
  mkdir -p "$dest_dir"

  for host in "${LOG_HOSTS[@]}"; do
    log "Checking ${host} for RocksDB logs..." | tee -a "$log_file"
    local remote_listing=""
    if ! remote_listing=$(ssh "${SSH_USER}@${host}" 'shopt -s nullglob; for f in /opt/flink/log/data_rocksdb_job*; do echo "$f"; done' 2>/dev/null); then
      remote_listing=""
    fi
    if [[ -z "$remote_listing" ]]; then
      log "No data_rocksdb_job files found on ${host}" | tee -a "$log_file"
      continue
    fi
    while IFS= read -r remote_file; do
      [[ -z "$remote_file" ]] && continue
      local base
      base=$(basename "$remote_file")
      local dest_file="${dest_dir}/${base}_${host}"
      log "Copying ${remote_file} from ${host} -> ${dest_file}" | tee -a "$log_file"
      scp "${SSH_USER}@${host}:${remote_file}" "$dest_file"
    done <<< "$remote_listing"
  done
}

ensure_job_stopped() {
  local pid=$1
  local pgid=${2:-}

  if [[ -n "$pgid" ]] && kill -0 "-$pgid" 2>/dev/null; then
    log "Terminating run_query process group ${pgid}..."
    kill -- -"$pgid" 2>/dev/null || true
    sleep 1
  fi

  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    log "Terminating lingering run_query process ${pid}..."
    kill "$pid" 2>/dev/null || true
  fi
}

current_job_pid=""
current_job_pgid=""
current_tm_size=""

last_tm_index=$((${#TM_MEMORY_SIZES[@]} - 1))
last_job_index=$((${#JOBS[@]} - 1))

for job_idx in "${!JOBS[@]}"; do
  job_name=${JOBS[$job_idx]}

  for tm_idx in "${!TM_MEMORY_SIZES[@]}"; do
    tm_size=${TM_MEMORY_SIZES[$tm_idx]}
    if [[ "$tm_size" != "$current_tm_size" ]]; then
      log "Setting taskmanager.memory.process.size to ${tm_size} in ${FLINK_CONF_FILE}..."
      update_flink_conf "$tm_size"
      apply_flink_conf
      sync_cluster
      current_tm_size=$tm_size
    fi

    log "===== Run: ${job_name} @ ${tm_size} (${job_idx+1}/${#JOBS[@]} job, ${tm_idx+1}/${#TM_MEMORY_SIZES[@]} mem) ====="

    start_cluster
    log "Waiting ${COOLDOWN_SECONDS}s before submission..."
    sleep "$COOLDOWN_SECONDS"

    submit_job "$job_name" "$tm_size"
    log "Allowing ${job_name} to run for ${JOB_WAIT_SECONDS}s (~90m)..."
    sleep "$JOB_WAIT_SECONDS"

    log "Collecting logs while ${job_name} is still running..."
    collect_logs "$job_name" "$tm_size"

    log "Stopping cluster after log collection..."
    stop_cluster
    ensure_job_stopped "$current_job_pid" "$current_job_pgid"
    current_job_pid=""
    current_job_pgid=""

    if [[ $job_idx -lt $last_job_index || $tm_idx -lt $last_tm_index ]]; then
      log "Cooling down ${COOLDOWN_SECONDS}s before next run..."
      sleep "$COOLDOWN_SECONDS"
    fi
  done
done

log "Managed memory sensitivity sweep complete."
