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
FLINK_CONF_PATH="${FLINK_HOME}/conf/flink-conf.yaml"

usage() {
  cat <<'EOF'
Usage: validate_q20_prefix_outputs.sh [options]

Runs q20_unique twice against the same generated event stream:
1. with per-job hashmap state
2. with cluster-default RocksDB from /opt/flink/conf/flink-conf.yaml

This is intended to validate that enabling the custom RocksDB options factory
does not change query output semantics.

Options:
  --build                Build nexmark-flink and copy the jar into /opt/flink/lib
  --events N             Number of source events to generate (default: 5000)
  --tps N                Source TPS for timestamp generation (default: 100000)
  --base-time N          Shared base time in millis (default: 1700000000000)
  --timeout-seconds N    Wait timeout per job (default: 180)
  --keep-cluster         Leave the cluster running after the script completes
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
  WORK_DIR="${SCRIPT_DIR}/compare_unique_query_to_prefix/$(date +%Y%m%d-%H%M%S)"
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

sync_flink_conf() {
  if [[ ! -f "${FLINK_CONF_PATH}" ]]; then
    echo "Missing Flink config: ${FLINK_CONF_PATH}" >&2
    return 1
  fi

  maybe_sudo cp "${FLINK_CONF_PATH}" "${WORK_DIR}/flink-conf.yaml"

  local workers_file="${FLINK_HOME}/conf/workers"
  if [[ -f "${workers_file}" ]]; then
    while IFS= read -r host; do
      host=${host%%#*}
      host=$(echo "${host}" | xargs)
      if [[ -z "${host}" || "${host}" == "localhost" || "${host}" == "127.0.0.1" ]]; then
        continue
      fi
      log "Syncing flink-conf.yaml to ${host}:${FLINK_HOME}/conf"
      maybe_sudo rsync -az \
        "${FLINK_CONF_PATH}" \
        "${SSH_USER}@${host}:${FLINK_HOME}/conf/flink-conf.yaml"
    done < "${workers_file}"
  fi
}

ensure_rocksdb_factory_configured() {
  if ! grep -q '^state.backend.type: rocksdb' "${FLINK_CONF_PATH}"; then
    echo "Expected ${FLINK_CONF_PATH} to set state.backend.type: rocksdb" >&2
    return 1
  fi

  local factory_class
  factory_class=$(sed -n 's/^state\.backend\.rocksdb\.options-factory: //p' "${FLINK_CONF_PATH}" | tail -1)
  if [[ -z "${factory_class}" ]]; then
    echo "Expected ${FLINK_CONF_PATH} to set state.backend.rocksdb.options-factory" >&2
    return 1
  fi

  printf 'state_backend=rocksdb\noptions_factory=%s\n' "${factory_class}" > "${WORK_DIR}/rocksdb_config.txt"
  log "Using RocksDB options factory ${factory_class}"
}

restart_cluster_with_current_conf() {
  if rest_ready; then
    log "Restarting running Flink cluster so flink-conf.yaml is applied consistently"
    /opt/scripts/stop_cluster.sh
  else
    log "Starting Flink cluster from current flink-conf.yaml"
  fi

  sync_flink_conf
  /opt/scripts/start_cluster.sh
  STARTED_CLUSTER=true
  wait_for_rest
}

job_state() {
  local job_id=$1
  curl -fsS "${FLINK_REST}/jobs/${job_id}" | sed -n 's/.*"state":"\([^\"]*\)".*/\1/p'
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
  local backend_mode=$1
  local output_path=$2
  local sink_identifier=$3

  {
    printf "SET 'pipeline.name' = 'q20-prefix-validate-%s-%s';\n" "${backend_mode}" "${sink_identifier}"
    if [[ "${backend_mode}" == "hashmap" ]]; then
      printf "SET 'state.backend.type' = 'hashmap';\n"
    fi
    replace_vars < "${QUERY_DIR}/ddl_gen_unique_v2.sql"
    printf "\n"
    replace_vars < "${QUERY_DIR}/ddl_views_unique.sql"
    printf "\n"
    sed \
      -e "s/'connector' = 'blackhole'/'connector' = 'print',\\
    'print-identifier' = '${sink_identifier}'/" \
      "${QUERY_DIR}/q20_unique.sql"
  } > "${output_path}"
}

submit_sql_job() {
  local script_path=$1
  local output_path=$2

  "${FLINK_HOME}/bin/sql-client.sh" embedded -Dexecution.attached=false -f "${script_path}" > "${output_path}" 2>&1
  sed -n 's/.*Job ID: \([A-Za-z0-9]\{32\}\).*/\1/p' "${output_path}" | tail -1
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
  fi
  build_and_deploy
fi

ensure_rocksdb_factory_configured
restart_cluster_with_current_conf

run_id="$(date +%s)"
hashmap_identifier="Q20_UNIQ_HASH_${run_id}"
rocksdb_identifier="Q20_UNIQ_RDB_${run_id}"
hashmap_sql="${WORK_DIR}/q20_unique_hashmap.sql"
rocksdb_sql="${WORK_DIR}/q20_unique_rocksdb.sql"
hashmap_submit="${WORK_DIR}/q20_unique_hashmap.submit.log"
rocksdb_submit="${WORK_DIR}/q20_unique_rocksdb.submit.log"
hashmap_rows="${WORK_DIR}/q20_unique_hashmap.rows"
rocksdb_rows="${WORK_DIR}/q20_unique_rocksdb.rows"
diff_file="${WORK_DIR}/q20_unique_backend.diff"

render_query_script hashmap "${hashmap_sql}" "${hashmap_identifier}"
render_query_script rocksdb "${rocksdb_sql}" "${rocksdb_identifier}"

log "Submitting q20_unique with per-job hashmap state"
hashmap_job_id=$(submit_sql_job "${hashmap_sql}" "${hashmap_submit}")
if [[ -z "${hashmap_job_id}" ]]; then
  echo "Could not parse hashmap job id from ${hashmap_submit}" >&2
  exit 1
fi

log "Submitting q20_unique with cluster-default RocksDB state"
rocksdb_job_id=$(submit_sql_job "${rocksdb_sql}" "${rocksdb_submit}")
if [[ -z "${rocksdb_job_id}" ]]; then
  echo "Could not parse RocksDB job id from ${rocksdb_submit}" >&2
  exit 1
fi

log "Waiting for hashmap job ${hashmap_job_id}"
wait_for_job_terminal "${hashmap_job_id}"
log "Waiting for RocksDB job ${rocksdb_job_id}"
wait_for_job_terminal "${rocksdb_job_id}"

sleep 2

collect_rows "${hashmap_identifier}" "${hashmap_rows}"
collect_rows "${rocksdb_identifier}" "${rocksdb_rows}"

log "q20_unique hashmap rows: $(wc -l < "${hashmap_rows}")"
log "q20_unique rocksdb rows: $(wc -l < "${rocksdb_rows}")"

if diff -u "${hashmap_rows}" "${rocksdb_rows}" > "${diff_file}"; then
  log "Outputs match"
  echo "MATCH"
else
  log "Outputs differ; see ${diff_file}"
  echo "MISMATCH"
  exit 1
fi

echo "Artifacts:"
echo "  work_dir=${WORK_DIR}"
echo "  hashmap_rows=${hashmap_rows}"
echo "  rocksdb_rows=${rocksdb_rows}"
echo "  diff=${diff_file}"
