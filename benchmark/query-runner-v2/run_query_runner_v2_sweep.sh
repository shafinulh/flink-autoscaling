#!/usr/bin/env bash

set -euo pipefail

JOB_NAME="${JOB_NAME:-q20_unique}"

ENABLE_WALL_PROFILING=true
PROFILE_DELAY_SECONDS=$((30 * 60))
PROFILE_DURATION_SECONDS=$((5 * 60))
PROFILE_MODE="${PROFILE_MODE:-WALL}"
PROFILE_SCOPE="${PROFILE_SCOPE:-taskmanager}"
PROFILE_TM_ID="${PROFILE_TM_ID:-}"

COOLDOWN_SECONDS=60

DEST_ROOT="/opt/benchmark/query-runner-v2"

LOG_HOSTS=(c182 c155 c167)
SSH_USER="${SSH_USER:-root}"

FLINK_BIN="/opt/flink/bin"
NEXMARK_BIN="/opt/nexmark/bin"
RUN_QUERY_CMD="${NEXMARK_BIN}/run_query.sh"
FLINK_CONF_SUFFIX="${FLINK_CONF_SUFFIX:-v2}"
FLINK_CONF_FILE=""
APPLY_CONFIG_CMD="/opt/scripts/apply_flink_config.sh"
SYNC_CLUSTER_CMD="/opt/scripts/sync_cluster.sh"

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
FLINK_LOG_DIR="${FLINK_LOG_DIR:-/opt/flink/log}"

SETSID_AVAILABLE=false
if command -v setsid >/dev/null 2>&1; then
  SETSID_AVAILABLE=true
fi

export FLINK_TM_JVM_OPTS="${FLINK_TM_JVM_OPTS:-}"
export FLINK_JOBMANAGER_JVM_OPTS="${FLINK_JOBMANAGER_JVM_OPTS:-}"

log() {
  echo "$(date -Is) $*"
}

log_err() {
  echo "$(date -Is) $*" >&2
}

usage() {
  cat <<'USAGE'
Usage: run_query_runner_v2_sweep.sh [--flink-conf-suffix SUFFIX] EXPERIMENT_NAME TM_MEMORY_SIZES...

Examples:
  run_query_runner_v2_sweep.sh --flink-conf-suffix v2 my-exp 3g 6g 8g
  run_query_runner_v2_sweep.sh -c v3 my-exp 8g 6g
USAGE
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
  # Overwrite in place to preserve existing ownership/permissions.
  cat "$tmp_file" > "$FLINK_CONF_FILE"
  rm -f "$tmp_file"
}

apply_flink_conf() {
  if [[ -x "$APPLY_CONFIG_CMD" ]]; then
    if [[ -n "$FLINK_CONF_SUFFIX" ]]; then
      "$APPLY_CONFIG_CMD" "$FLINK_CONF_SUFFIX"
    else
      "$APPLY_CONFIG_CMD"
    fi
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
  local run_dir=$1
  local tm_size=$2
  local log_file="${run_dir}/run_query.log"
  mkdir -p "$run_dir"

  log "Submitting ${JOB_NAME} (tm.process=${tm_size})..." | tee -a "$log_file"
  if [[ $SETSID_AVAILABLE == true ]]; then
    nohup setsid "${RUN_QUERY_CMD}" "oa" "$JOB_NAME" >>"$log_file" 2>&1 &
  else
    nohup "${RUN_QUERY_CMD}" "oa" "$JOB_NAME" >>"$log_file" 2>&1 &
  fi
  current_job_pid=$!
  if [[ $SETSID_AVAILABLE == true ]]; then
    current_job_pgid=$current_job_pid
  else
    current_job_pgid=$(ps -o pgid= "$current_job_pid" 2>/dev/null | tr -d ' ')
  fi
  echo "$current_job_pid" > "${run_dir}/run_query.pid"
  if [[ -n "$current_job_pgid" ]]; then
    echo "$current_job_pgid" > "${run_dir}/run_query.pgid"
  fi
  log "${JOB_NAME} submitted (PID ${current_job_pid})." | tee -a "$log_file"
}

write_metadata() {
  local run_dir=$1
  local tm_size=$2
  local experiment_name=$3
  local experiment_label=$4
  local meta_file="${run_dir}/metadata.txt"
  {
    echo "experiment_label=${experiment_label}"
    echo "experiment_name=${experiment_name}"
    echo "job_name=${JOB_NAME}"
    echo "taskmanager.memory.process.size=${tm_size}"
    echo "taskmanager.numberOfTaskSlots=4"
    echo "flink_conf_suffix=${FLINK_CONF_SUFFIX}"
    echo "profile_mode=${PROFILE_MODE}"
    echo "profile_delay_seconds=${PROFILE_DELAY_SECONDS}"
    echo "profile_duration_seconds=${PROFILE_DURATION_SECONDS}"
    echo "created_at=$(date -Is)"
  } > "$meta_file"
  cp "$FLINK_CONF_FILE" "${run_dir}/flink-conf.yaml"
}

collect_rocksdb_logs() {
  local run_dir=$1
  local stats_dir="${run_dir}/rocksdb_logs"
  local log_file="${stats_dir}/log-collection.log"
  mkdir -p "$stats_dir"

  for host in "${LOG_HOSTS[@]}"; do
    log "Checking ${host} for RocksDB logs..." | tee -a "$log_file"
    local has_logs=""
    if ! has_logs=$(ssh "${SSH_USER}@${host}" 'find /data/rocksdb_native_logs -mindepth 1 -maxdepth 1 -print -quit' 2>&1); then
      log "Log scan failed on ${host}: ${has_logs}" | tee -a "$log_file"
      has_logs=""
    fi
    if [[ -z "$has_logs" ]]; then
      log "No RocksDB logs found in /data/rocksdb_native_logs on ${host}" | tee -a "$log_file"
      log "Recent /data/rocksdb_native_logs entries on ${host} (tail -n 20):" | tee -a "$log_file"
      local remote_ls=""
      if remote_ls=$(ssh "${SSH_USER}@${host}" 'ls -l /data/rocksdb_native_logs 2>&1 | tail -n 20' 2>&1); then
        printf '%s\n' "$remote_ls" | tee -a "$log_file"
      else
        log "Failed to list /data/rocksdb_native_logs on ${host}: ${remote_ls}" | tee -a "$log_file"
      fi
      continue
    fi
    local host_dir="${stats_dir}/${host}"
    mkdir -p "$host_dir"
    log "Copying /data/rocksdb_native_logs from ${host} -> ${host_dir}" | tee -a "$log_file"
    scp -r "${SSH_USER}@${host}:/data/rocksdb_native_logs/." "${host_dir}/"
    log "Removing /data/rocksdb_native_logs contents from ${host} after copy." | tee -a "$log_file"
    ssh "${SSH_USER}@${host}" 'find /data/rocksdb_native_logs -mindepth 1 -maxdepth 1 -exec rm -rf {} +'
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

require_tools() {
  for tool in curl python3; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      log_err "Missing required tool: ${tool}"
      return 1
    fi
  done
}

resolve_flink_conf_file() {
  local suffix=$1
  local conf_root="/opt/configs/flink"
  local candidate="${conf_root}/flink-conf.yaml"
  if [[ -n "$suffix" ]]; then
    candidate="${conf_root}/flink-conf-${suffix}.yaml"
  fi
  if [[ ! -f "$candidate" ]]; then
    log_err "Unknown Flink config suffix: '${suffix}'"
    log_err "Available suffixes:"
    (cd "$conf_root" && ls -1 flink-conf*.yaml 2>/dev/null \
      | awk '{if ($0=="flink-conf.yaml") {print "(none)"} else {gsub(/^flink-conf-/, "", $0); sub(/\\.yaml$/, "", $0); print $0}}') >&2
    exit 2
  fi
  printf '%s' "$candidate"
}

discover_rest_url_from_logs() {
  local log_dir=$1
  local file=""
  local line=""
  local addr=""

  for file in $(ls -t "${log_dir}"/flink-root-standalonesession-*.log* 2>/dev/null); do
    line=$(grep -h "Rest endpoint listening at" "$file" | tail -n 1)
    if [[ -n "$line" ]]; then
      addr=$(printf '%s' "$line" | sed -n 's/.*Rest endpoint listening at \([^ ]*\).*/\1/p')
      if [[ -n "$addr" ]]; then
        echo "http://${addr}"
        return 0
      fi
    fi
  done
  return 1
}

dump_unusable_payload() {
  local label=$1
  local payload
  payload=$(cat)

  local out="${PROFILE_OUTPUT_DIR}/taskmanagers_${label}.txt"
  printf '%s' "$payload" > "$out"
  local snippet
  snippet=$(printf '%s' "$payload" | head -c 200 | tr '\n' ' ')
  log_err "Unparseable taskmanagers response saved to ${out}. First 200 bytes: ${snippet}"
}

extract_taskmanager_id() {
  python3 -c "$(cat <<'PY'
import json
import re
import sys

text = sys.stdin.read()
if not text:
    sys.exit(0)
try:
    data = json.loads(text)
    tms = data.get("taskmanagers", [])
    if tms:
        print(tms[0].get("id", ""))
        sys.exit(0)
except Exception:
    pass
m = re.search(r'"id"\s*:\s*"([^"]+)"', text)
if m:
    print(m.group(1))
PY
)"
}

resolve_taskmanager_id() {
  local rest_root=${1%/}
  local label=$2
  local payload=""
  if ! payload=$(curl -sS -H "Accept: application/json" "${rest_root}/taskmanagers"); then
    return 1
  fi
  if [[ -z "$payload" ]]; then
    return 1
  fi
  local tm_id=""
  tm_id=$(printf '%s' "$payload" | extract_taskmanager_id)
  if [[ -n "$tm_id" ]]; then
    echo "$tm_id"
    return 0
  fi
  printf '%s' "$payload" | dump_unusable_payload "$label"
  return 1
}

resolve_profile_base_url() {
  if [[ "$PROFILE_SCOPE" == "jobmanager" ]]; then
    echo "${FLINK_REST_URL}/jobmanager/profiler"
    return 0
  fi

  if [[ -z "$PROFILE_TM_ID" ]]; then
    local tm_id=""
    local rest_root="${FLINK_REST_URL%/}"
    tm_id=$(resolve_taskmanager_id "$rest_root" "taskmanagers" || true)
    if [[ -z "$tm_id" ]]; then
      tm_id=$(resolve_taskmanager_id "${rest_root}/v1" "v1_taskmanagers" || true)
      if [[ -n "$tm_id" ]]; then
        FLINK_REST_URL="${rest_root}/v1"
      fi
    fi
    if [[ -z "$tm_id" ]]; then
      local discovered=""
      if discovered=$(discover_rest_url_from_logs "$FLINK_LOG_DIR"); then
        rest_root="${discovered%/}"
        tm_id=$(resolve_taskmanager_id "$rest_root" "taskmanagers" || true)
        if [[ -z "$tm_id" ]]; then
          tm_id=$(resolve_taskmanager_id "${rest_root}/v1" "v1_taskmanagers" || true)
          if [[ -n "$tm_id" ]]; then
            FLINK_REST_URL="${rest_root}/v1"
          fi
        else
          FLINK_REST_URL="$rest_root"
        fi
      fi
    fi
    if [[ -z "$tm_id" ]]; then
      log_err "Failed to fetch taskmanagers from ${FLINK_REST_URL}."
      return 1
    fi
    PROFILE_TM_ID="$tm_id"
  fi

  if [[ -z "$PROFILE_TM_ID" ]]; then
    log_err "Unable to resolve TaskManager ID for profiling."
    return 1
  fi
  echo "${FLINK_REST_URL}/taskmanagers/${PROFILE_TM_ID}/profiler"
}

wait_for_profile_finished() {
  local base_url=$1
  local trigger_time=$2
  local timeout_seconds=$3
  local start_ts
  start_ts=$(date +%s)

  while true; do
    local list_json=""
    if ! list_json=$(curl -sf "$base_url"); then
      log_err "Failed to fetch profiling list; retrying..."
      sleep 5
      continue
    fi
    if [[ -z "$list_json" ]]; then
      log_err "Empty profiling list response; retrying..."
      sleep 5
      continue
    fi
    printf '%s' "$list_json" > "${PROFILE_OUTPUT_DIR}/profiling_list.json"

    local status=""
    local output_file=""
    local message=""
    local parsed
    if ! parsed=$(printf '%s' "$list_json" | TRIGGER_TIME="$trigger_time" python3 -c "$(cat <<'PY'
import json
import os
import sys

try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
target = str(os.environ.get("TRIGGER_TIME", ""))
status = ""
output = ""
message = ""
for entry in data.get("profilingList", []):
    if str(entry.get("triggerTime", "")) == target:
        status = str(entry.get("status", ""))
        output = str(entry.get("outputFile", ""))
        message = str(entry.get("message", ""))
        break
print(status)
print(output)
print(message)
PY
)"); then
      log_err "Failed to parse profiling list; retrying..."
      sleep 5
      continue
    fi
    status=$(printf '%s' "$parsed" | sed -n '1p')
    output_file=$(printf '%s' "$parsed" | sed -n '2p')
    message=$(printf '%s' "$parsed" | sed -n '3p')

    if [[ "$status" == "FINISHED" && -n "$output_file" ]]; then
      echo "$output_file"
      return 0
    fi
    if [[ "$status" == "FAILED" ]]; then
      log_err "Profiling failed: ${message}"
      return 1
    fi

    local now_ts
    now_ts=$(date +%s)
    if (( now_ts - start_ts >= timeout_seconds )); then
      log_err "Timed out waiting for profiling to finish."
      return 1
    fi
    sleep 10
  done
}

run_profiling_sequence() {
  if ! require_tools; then
    return 1
  fi
  if [[ -z "${PROFILE_OUTPUT_DIR:-}" ]]; then
    log_err "PROFILE_OUTPUT_DIR is not set."
    return 1
  fi

  mkdir -p "$PROFILE_OUTPUT_DIR"

  local base_url=""
  if ! base_url=$(resolve_profile_base_url); then
    log "Unable to resolve profiling endpoint."
    return 1
  fi

  log "Triggering ${PROFILE_MODE} profiling for ${PROFILE_DURATION_SECONDS}s at ${base_url}..."
  local response=""
  if ! response=$(curl -sf -X POST -H "Content-Type: application/json" \
    -d "{\"mode\":\"${PROFILE_MODE}\",\"duration\":${PROFILE_DURATION_SECONDS}}" \
    "$base_url"); then
    log "Profiling request failed."
    return 1
  fi
  if [[ -z "$response" ]]; then
    log "Profiling request returned empty response."
    return 1
  fi
  printf '%s' "$response" > "${PROFILE_OUTPUT_DIR}/profiling_start.json"

  local trigger_time=""
  if ! trigger_time=$(printf '%s' "$response" | python3 -c "$(cat <<'PY'
import json
import sys

try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
value = data.get("triggerTime")
print("" if value is None else value)
PY
)"); then
    log "Failed to parse profiling response."
    return 1
  fi
  if [[ -z "$trigger_time" ]]; then
    log "Profiling response missing triggerTime."
    return 1
  fi

  local timeout_seconds=$((PROFILE_DURATION_SECONDS + 120))
  local output_file=""
  if ! output_file=$(wait_for_profile_finished "$base_url" "$trigger_time" "$timeout_seconds"); then
    return 1
  fi

  local encoded_output=""
  if ! encoded_output=$(printf '%s' "$output_file" | python3 -c "$(cat <<'PY'
import sys
import urllib.parse

print(urllib.parse.quote(sys.stdin.read().strip(), safe=""))
PY
)"); then
    log "Failed to encode profiling output file."
    return 1
  fi
  if [[ -z "$encoded_output" ]]; then
    log "Failed to encode profiling output file."
    return 1
  fi

  local output_path="${PROFILE_OUTPUT_DIR}/${output_file}"
  log "Downloading profiling result to ${output_path}..."
  if ! curl -sf "${base_url}/${encoded_output}" -o "$output_path"; then
    log "Failed to download profiling result."
    return 1
  fi
  log "Profiling result saved to ${output_path}."
}

schedule_profile() {
  local job_pid=$1
  local output_dir=$2
  local delay_seconds=$3
  local duration_seconds=$4

  (
    sleep "$delay_seconds"
    if ! kill -0 "$job_pid" 2>/dev/null; then
      log "Job finished before profiling window; skipping profiling."
      exit 0
    fi
    PROFILE_OUTPUT_DIR="$output_dir"
    PROFILE_DURATION_SECONDS="$duration_seconds"
    if ! run_profiling_sequence; then
      log "Profiling failed or skipped; continuing run."
    fi
  ) &
  echo $!
}

current_job_pid=""
current_job_pgid=""

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --flink-conf-suffix|-c)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --flink-conf-suffix"
        usage
        exit 2
      fi
      FLINK_CONF_SUFFIX="$1"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      log_err "Unknown option: $1"
      usage
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -lt 2 ]]; then
  usage
  exit 2
fi

FLINK_CONF_FILE=$(resolve_flink_conf_file "$FLINK_CONF_SUFFIX")

EXPERIMENT_NAME=$1
shift

TM_MEMORY_SIZES=("$@")

RUN_DATE_MMDDHH=$(date +%m%d%H)

experiment_label="${EXPERIMENT_NAME}-${JOB_NAME}"
if [[ -n "$FLINK_CONF_SUFFIX" ]]; then
  experiment_label="${experiment_label}-${FLINK_CONF_SUFFIX}"
fi
experiment_label="${RUN_DATE_MMDDHH}-${experiment_label}"
EXPERIMENT_ROOT="${DEST_ROOT}/${experiment_label}"

mkdir -p "$EXPERIMENT_ROOT"

last_tm_index=$((${#TM_MEMORY_SIZES[@]} - 1))

for tm_idx in "${!TM_MEMORY_SIZES[@]}"; do
  tm_size=${TM_MEMORY_SIZES[$tm_idx]}
  run_dir="${EXPERIMENT_ROOT}/exp-${tm_size}-tm-process"
  mkdir -p "$run_dir"

  log "Setting taskmanager.memory.process.size to ${tm_size} in ${FLINK_CONF_FILE}..."
  update_flink_conf "$tm_size"
  apply_flink_conf
  sync_cluster

  log "===== Run: ${JOB_NAME} @ ${tm_size} (${tm_idx+1}/${#TM_MEMORY_SIZES[@]}) ====="
  start_cluster
  log "Waiting ${COOLDOWN_SECONDS}s before submission..."
  sleep "$COOLDOWN_SECONDS"

  write_metadata "$run_dir" "$tm_size" "$EXPERIMENT_NAME" "$experiment_label"
  submit_job "$run_dir" "$tm_size"
  job_start_ts=$(date +%s)
  run_summary_log="${run_dir}/run_summary.log"
  profile_pid=""

  if [[ "$ENABLE_WALL_PROFILING" == true ]]; then
    profile_dir="${run_dir}/profiling/wall_17m_5m"
    profile_pid=$(schedule_profile "$current_job_pid" "$profile_dir" "$PROFILE_DELAY_SECONDS" "$PROFILE_DURATION_SECONDS")
  else
    log "Wall-clock profiling disabled; skipping profiler trigger."
  fi

  log "Waiting for ${JOB_NAME} to complete..."
  if wait "$current_job_pid"; then
    job_status=0
  else
    job_status=$?
  fi
  job_end_ts=$(date +%s)
  run_seconds=$((job_end_ts - job_start_ts))
  run_minutes=$((run_seconds / 60))
  log "Total run time: ${run_seconds}s (~${run_minutes}m)" | tee -a "$run_summary_log"
  log "${JOB_NAME} finished with status ${job_status}."

  if [[ -n "$profile_pid" ]]; then
    log "Waiting for profiling to finish..."
    wait "$profile_pid" || true
  fi

  log "Collecting RocksDB logs after job completion..."
  collect_rocksdb_logs "$run_dir"

  log "Stopping cluster after log collection..."
  stop_cluster
  ensure_job_stopped "$current_job_pid" "$current_job_pgid"
  current_job_pid=""
  current_job_pgid=""

  if [[ $tm_idx -lt $last_tm_index ]]; then
    log "Cooling down ${COOLDOWN_SECONDS}s before next run..."
    sleep "$COOLDOWN_SECONDS"
  fi
done

log "QueryRunnerV2 sweep complete."
