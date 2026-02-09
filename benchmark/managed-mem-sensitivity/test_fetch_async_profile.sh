#!/usr/bin/env bash

set -euo pipefail

JOB_NAME="q20_unique"
TOTAL_RUN_SECONDS=$((10 * 60))
PROFILE_DELAY_SECONDS=$((2 * 60))
PROFILE_DURATION_SECONDS=$((5 * 60))
COOLDOWN_SECONDS=60

EXPERIMENT_NAME="test_fetch_async_profile"
DEST_ROOT="/opt/benchmark/managed-mem-sensitivity/${EXPERIMENT_NAME}"
LOG_HOSTS=(c182 c155 c167)
SSH_USER="${SSH_USER:-root}"

FLINK_BIN="/opt/flink/bin"
NEXMARK_BIN="/opt/nexmark/bin"
RUN_QUERY_CMD="${NEXMARK_BIN}/run_query.sh"

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
FLINK_LOG_DIR="${FLINK_LOG_DIR:-/opt/flink/log}"
PROFILE_MODE="${PROFILE_MODE:-WALL}"
PROFILE_SCOPE="${PROFILE_SCOPE:-taskmanager}"
PROFILE_TM_ID="${PROFILE_TM_ID:-}"

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

log_err() {
  echo "$(date -Is) $*" >&2
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

submit_job() {
  local log_file="${LOG_DIR}/run_query.log"
  mkdir -p "$LOG_DIR"

  log "Submitting ${JOB_NAME}..." | tee -a "$log_file"
  if [[ $SETSID_AVAILABLE == true ]]; then
    nohup setsid "${RUN_QUERY_CMD}" "$JOB_NAME" >>"$log_file" 2>&1 &
  else
    nohup "${RUN_QUERY_CMD}" "$JOB_NAME" >>"$log_file" 2>&1 &
  fi
  current_job_pid=$!
  if [[ $SETSID_AVAILABLE == true ]]; then
    current_job_pgid=$current_job_pid
  else
    current_job_pgid=$(ps -o pgid= "$current_job_pid" 2>/dev/null | tr -d ' ')
  fi
  echo "$current_job_pid" > "${LOG_DIR}/run_query.pid"
  if [[ -n "$current_job_pgid" ]]; then
    echo "$current_job_pgid" > "${LOG_DIR}/run_query.pgid"
  fi
  log "${JOB_NAME} submitted (PID ${current_job_pid})." | tee -a "$log_file"
}

collect_logs() {
  local log_file="${LOG_DIR}/log-collection.log"
  mkdir -p "$LOG_DIR"

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
      local dest_file="${LOG_DIR}/${base}_${host}"
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

require_tools() {
  for tool in curl python3; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      log_err "Missing required tool: ${tool}"
      return 1
    fi
  done
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

current_job_pid=""
current_job_pgid=""
LOG_DIR="${DEST_ROOT}/${JOB_NAME}"
PROFILE_OUTPUT_DIR="${LOG_DIR}/profiling"

log "===== Dummy async profile run: ${JOB_NAME} ====="
start_cluster
log "Waiting ${COOLDOWN_SECONDS}s before submission..."
sleep "$COOLDOWN_SECONDS"

submit_job
job_start_ts=$(date +%s)

log "Allowing ${JOB_NAME} to run for ${PROFILE_DELAY_SECONDS}s before profiling..."
sleep "$PROFILE_DELAY_SECONDS"

if ! run_profiling_sequence; then
  log "Profiling failed or skipped; continuing run."
fi

elapsed=$(( $(date +%s) - job_start_ts ))
remaining=$(( TOTAL_RUN_SECONDS - elapsed ))
if (( remaining > 0 )); then
  log "Allowing ${JOB_NAME} to run for remaining ${remaining}s to reach ${TOTAL_RUN_SECONDS}s total..."
  sleep "$remaining"
fi

log "Collecting RocksDB logs while ${JOB_NAME} is still running..."
collect_logs

log "Stopping cluster after log collection..."
stop_cluster
ensure_job_stopped "$current_job_pid" "$current_job_pgid"
current_job_pid=""
current_job_pgid=""

log "Dummy async profile run complete."
