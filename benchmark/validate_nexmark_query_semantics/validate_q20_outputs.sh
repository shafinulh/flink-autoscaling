#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source /opt/scripts/env.sh

REPO_DIR=${REPO_DIR:-/opt/nexmark-v2/nexmark-flink}
QUERY_DIR="${REPO_DIR}/src/main/resources/queries"
FLINK_REST=${FLINK_REST:-http://localhost:8081}
EVENTS=${EVENTS:-5000}
TPS=${TPS:-100000}
PERSON_PROPORTION=${PERSON_PROPORTION:-1}
AUCTION_PROPORTION=${AUCTION_PROPORTION:-3}
BID_PROPORTION=${BID_PROPORTION:-46}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-180}
BASE_TIME_MILLIS=${BASE_TIME_MILLIS:-1700000000000}
BUILD_AND_DEPLOY=false
KEEP_CLUSTER=false
WORK_DIR=""
STARTED_CLUSTER=false
LEFT_MODE=${LEFT_MODE:-original}
RIGHT_MODE=${RIGHT_MODE:-unique}

usage() {
  cat <<'EOF'
Usage: validate_q20_outputs.sh [options]

Runs two q20 variants against the same generated event stream using a print sink,
then compares the emitted rows as sorted result sets.

Options:
  --build                Build nexmark-flink and copy the jar into /opt/flink/lib
  --events N             Number of source events to generate (default: 5000)
  --tps N                Source TPS for timestamp generation (default: 100000)
  --base-time N          Shared base time in millis (default: 1700000000000)
  --timeout-seconds N    Wait timeout per job (default: 180)
  --keep-cluster         Leave the cluster running if this script started it
  --left-mode MODE       Query mode for the left side: original, unique, unique_modified
                         (default: original)
  --right-mode MODE      Query mode for the right side: original, unique, unique_modified
                         (default: unique)
  --work-dir PATH        Reuse a specific working directory
  --repo-dir PATH        Nexmark flink repo directory (default: /opt/nexmark-v2/nexmark-flink)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      BUILD_AND_DEPLOY=true
      shift
      ;;
    --events)
      EVENTS=$2
      shift 2
      ;;
    --tps)
      TPS=$2
      shift 2
      ;;
    --base-time)
      BASE_TIME_MILLIS=$2
      shift 2
      ;;
    --timeout-seconds)
      TIMEOUT_SECONDS=$2
      shift 2
      ;;
    --keep-cluster)
      KEEP_CLUSTER=true
      shift
      ;;
    --left-mode)
      LEFT_MODE=$2
      shift 2
      ;;
    --right-mode)
      RIGHT_MODE=$2
      shift 2
      ;;
    --work-dir)
      WORK_DIR=$2
      shift 2
      ;;
    --repo-dir)
      REPO_DIR=$2
      QUERY_DIR="${REPO_DIR}/src/main/resources/queries"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${WORK_DIR}" ]]; then
  WORK_DIR="${SCRIPT_DIR}/compare_base_query_to_unique/$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "${WORK_DIR}"

cleanup() {
  if [[ "${KEEP_CLUSTER}" != true ]] && rest_ready; then
    log "Stopping cluster at script exit"
    /opt/scripts/stop_cluster.sh
  fi
}
trap cleanup EXIT

rest_ready() {
  curl -fsS "${FLINK_REST}/overview" >/dev/null 2>&1
}

wait_for_rest() {
  local waited=0
  until rest_ready; do
    if (( waited >= TIMEOUT_SECONDS )); then
      echo "Timed out waiting for Flink REST at ${FLINK_REST}" >&2
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

build_and_deploy() {
  log "Building nexmark-flink from ${REPO_DIR}"
  (
    cd "${REPO_DIR}"
    ./build.sh
  )

  local built_jar="${REPO_DIR}/target/nexmark-flink-0.3-SNAPSHOT.jar"
  if [[ ! -f "${built_jar}" ]]; then
    echo "Built jar not found: ${built_jar}" >&2
    return 1
  fi

  log "Copying ${built_jar} into ${FLINK_HOME}/lib"
  maybe_sudo cp "${built_jar}" "${FLINK_HOME}/lib/nexmark-flink-0.3-SNAPSHOT.jar"

  local workers_file="${FLINK_HOME}/conf/workers"
  if [[ -f "${workers_file}" ]]; then
    while IFS= read -r host; do
      host=${host%%#*}
      host=$(echo "${host}" | xargs)
      if [[ -z "${host}" || "${host}" == "localhost" || "${host}" == "127.0.0.1" ]]; then
        continue
      fi
      log "Syncing nexmark jar to ${host}:${FLINK_HOME}/lib"
      maybe_sudo rsync -az \
        "${FLINK_HOME}/lib/nexmark-flink-0.3-SNAPSHOT.jar" \
        "${SSH_USER}@${host}:${FLINK_HOME}/lib/nexmark-flink-0.3-SNAPSHOT.jar"
    done < "${workers_file}"
  fi
}

ensure_cluster() {
  if rest_ready; then
    log "Flink cluster already running"
    return
  fi

  log "Starting Flink cluster"
  /opt/scripts/start_cluster.sh
  STARTED_CLUSTER=true
  wait_for_rest
}

job_state() {
  local job_id=$1
  curl -fsS "${FLINK_REST}/jobs/${job_id}" | sed -n 's/.*"state":"\([^"]*\)".*/\1/p'
}

wait_for_job_terminal() {
  local job_id=$1
  local waited=0
  while true; do
    local state
    state=$(job_state "${job_id}")
    case "${state}" in
      FINISHED)
        return 0
        ;;
      FAILED|CANCELED|SUSPENDED)
        echo "Job ${job_id} ended in state ${state}" >&2
        return 1
        ;;
      *)
        ;;
    esac

    if (( waited >= TIMEOUT_SECONDS )); then
      echo "Timed out waiting for job ${job_id}, last state=${state}" >&2
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

replace_vars() {
  sed \
    -e "s|\${BASE_TIME_MILLIS}|${BASE_TIME_MILLIS}|g" \
    -e "s|\${TPS}|${TPS}|g" \
    -e "s|\${EVENTS_NUM}|${EVENTS}|g" \
    -e "s|\${PERSON_PROPORTION}|${PERSON_PROPORTION}|g" \
    -e "s|\${AUCTION_PROPORTION}|${AUCTION_PROPORTION}|g" \
    -e "s|\${BID_PROPORTION}|${BID_PROPORTION}|g" \
    -e "s|\${NEXMARK_TABLE}|datagen|g" \
    -e "s|\${KEEP_ALIVE}|false|g" \
    -e "s|\${STOP_AT}|-1|g"
}

render_query_script() {
  local mode=$1
  local output_path=$2
  local sink_identifier=$3

  local ddl_file views_file query_file
  case "${mode}" in
    original)
      ddl_file="${QUERY_DIR}/ddl_gen_v2.sql"
      views_file="${QUERY_DIR}/ddl_views.sql"
      query_file="${QUERY_DIR}/q20.sql"
      ;;
    unique)
      ddl_file="${QUERY_DIR}/ddl_gen_unique_v2.sql"
      views_file="${QUERY_DIR}/ddl_views_unique.sql"
      query_file="${QUERY_DIR}/q20_unique.sql"
      ;;
    unique_modified)
      ddl_file="${QUERY_DIR}/ddl_gen_unique_v2.sql"
      views_file="${QUERY_DIR}/ddl_views_unique.sql"
      query_file="${QUERY_DIR}/q20_unique_modified.sql"
      ;;
    *)
      echo "Unknown mode ${mode}" >&2
      return 1
      ;;
  esac

  {
    printf "SET 'pipeline.name' = 'q20-validate-%s-%s';\n" "${mode}" "${sink_identifier}"
    printf "SET 'state.backend.type' = 'hashmap';\n"
    replace_vars < "${ddl_file}"
    printf "\n"
    replace_vars < "${views_file}"
    printf "\n"
    sed \
      -e "s/'connector' = 'blackhole'/'connector' = 'print',\\
    'print-identifier' = '${sink_identifier}'/" \
      "${query_file}"
  } > "${output_path}"
}

submit_sql_job() {
  local script_path=$1
  local output_path=$2

  "${FLINK_HOME}/bin/sql-client.sh" embedded -Dexecution.attached=false -f "${script_path}" > "${output_path}" 2>&1
  sed -n 's/.*Job ID: \([A-Za-z0-9]\{32\}\).*/\1/p' "${output_path}" | tail -1
}

normalize_rows_for_mode() {
  local mode=$1
  local input_path=$2
  local output_path=$3

  case "${mode}" in
    original|unique)
      cp "${input_path}" "${output_path}"
      ;;
    unique_modified)
      sed -E 's/^([+-][IUD]\[)[^,]+, /\1/' "${input_path}" > "${output_path}"
      ;;
    *)
      echo "Unknown mode ${mode}" >&2
      return 1
      ;;
  esac
}

collect_rows() {
  local sink_identifier=$1
  local output_path=$2
  {
    cat "${FLINK_HOME}"/log/flink-*-taskexecutor-* 2>/dev/null || true

    local workers_file="${FLINK_HOME}/conf/workers"
    if [[ -f "${workers_file}" ]]; then
      while IFS= read -r host; do
        host=${host%%#*}
        host=$(echo "${host}" | xargs)
        if [[ -z "${host}" || "${host}" == "localhost" || "${host}" == "127.0.0.1" ]]; then
          continue
        fi
        maybe_sudo ssh "${SSH_USER}@${host}" "cat '${FLINK_HOME}'/log/flink-*-taskexecutor-* 2>/dev/null || true"
      done < "${workers_file}"
    fi
  } | grep "${sink_identifier}" \
    | sed -n -E 's/^.*> ([+-][IUD]\[.*)$/\1/p' \
    | sort > "${output_path}"
}

if [[ "${BUILD_AND_DEPLOY}" == true ]]; then
  if rest_ready; then
    log "Cluster is running and will be restarted after jar deployment"
    /opt/scripts/stop_cluster.sh
    STARTED_CLUSTER=false
  fi
  build_and_deploy
fi

ensure_cluster

run_id="$(date +%s)"
left_identifier="Q20_LEFT_${run_id}"
right_identifier="Q20_RIGHT_${run_id}"
left_sql="${WORK_DIR}/${LEFT_MODE}.sql"
right_sql="${WORK_DIR}/${RIGHT_MODE}.sql"
left_submit="${WORK_DIR}/${LEFT_MODE}.submit.log"
right_submit="${WORK_DIR}/${RIGHT_MODE}.submit.log"
left_rows_raw="${WORK_DIR}/${LEFT_MODE}.rows.raw"
right_rows_raw="${WORK_DIR}/${RIGHT_MODE}.rows.raw"
left_rows="${WORK_DIR}/${LEFT_MODE}.rows"
right_rows="${WORK_DIR}/${RIGHT_MODE}.rows"
diff_file="${WORK_DIR}/q20.diff"

render_query_script "${LEFT_MODE}" "${left_sql}" "${left_identifier}"
render_query_script "${RIGHT_MODE}" "${right_sql}" "${right_identifier}"

log "Submitting ${LEFT_MODE}"
left_job_id=$(submit_sql_job "${left_sql}" "${left_submit}")
if [[ -z "${left_job_id}" ]]; then
  echo "Could not parse ${LEFT_MODE} job id from ${left_submit}" >&2
  exit 1
fi

log "Submitting ${RIGHT_MODE}"
right_job_id=$(submit_sql_job "${right_sql}" "${right_submit}")
if [[ -z "${right_job_id}" ]]; then
  echo "Could not parse ${RIGHT_MODE} job id from ${right_submit}" >&2
  exit 1
fi

log "Waiting for ${LEFT_MODE} job ${left_job_id}"
wait_for_job_terminal "${left_job_id}"
log "Waiting for ${RIGHT_MODE} job ${right_job_id}"
wait_for_job_terminal "${right_job_id}"

sleep 2

collect_rows "${left_identifier}" "${left_rows_raw}"
collect_rows "${right_identifier}" "${right_rows_raw}"
normalize_rows_for_mode "${LEFT_MODE}" "${left_rows_raw}" "${left_rows}"
normalize_rows_for_mode "${RIGHT_MODE}" "${right_rows_raw}" "${right_rows}"

log "${LEFT_MODE} rows: $(wc -l < "${left_rows}")"
log "${RIGHT_MODE} rows: $(wc -l < "${right_rows}")"

if diff -u "${left_rows}" "${right_rows}" > "${diff_file}"; then
  log "Outputs match"
  echo "MATCH"
else
  log "Outputs differ; see ${diff_file}"
  echo "MISMATCH"
  exit 1
fi

echo "Artifacts:"
echo "  work_dir=${WORK_DIR}"
echo "  left_rows=${left_rows}"
echo "  right_rows=${right_rows}"
echo "  diff=${diff_file}"
