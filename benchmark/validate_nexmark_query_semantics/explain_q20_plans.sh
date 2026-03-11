#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source /opt/scripts/env.sh

REPO_DIR=${REPO_DIR:-/opt/nexmark-v2/nexmark-flink}
QUERY_DIR="${REPO_DIR}/src/main/resources/queries"
EVENTS=${EVENTS:-2000}
TPS=${TPS:-100000}
PERSON_PROPORTION=${PERSON_PROPORTION:-1}
AUCTION_PROPORTION=${AUCTION_PROPORTION:-3}
BID_PROPORTION=${BID_PROPORTION:-46}
BASE_TIME_MILLIS=${BASE_TIME_MILLIS:-1700000000000}

usage() {
  cat <<'EOF'
Usage: explain_q20_plans.sh [options]

Renders full SQL scripts for q20 and q20_unique, runs EXPLAIN PLAN FOR through
the Flink SQL Client, and saves both the raw client output and extracted plan text
under the benchmark directory.

Options:
  --events N       Number of source events to substitute into the DDL
  --tps N          Source TPS for timestamp generation
  --base-time N    Shared base time in millis
  --repo-dir PATH  Nexmark flink repo directory
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

mkdir -p "${SCRIPT_DIR}/q20_plan" "${SCRIPT_DIR}/q20_unique_plan"

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

render_explain_script() {
  local mode=$1
  local output_path=$2

  local ddl_file views_file query_file
  case "${mode}" in
    q20)
      ddl_file="${QUERY_DIR}/ddl_gen_v2.sql"
      views_file="${QUERY_DIR}/ddl_views.sql"
      query_file="${QUERY_DIR}/q20.sql"
      ;;
    q20_unique)
      ddl_file="${QUERY_DIR}/ddl_gen_unique_v2.sql"
      views_file="${QUERY_DIR}/ddl_views_unique.sql"
      query_file="${QUERY_DIR}/q20_unique.sql"
      ;;
    *)
      echo "Unknown mode ${mode}" >&2
      return 1
      ;;
  esac

  {
    printf "SET 'pipeline.name' = 'explain-%s';\n" "${mode}"
    replace_vars < "${ddl_file}"
    printf "\n"
    replace_vars < "${views_file}"
    printf "\n"
    sed 's/^INSERT INTO/EXPLAIN PLAN FOR INSERT INTO/' "${query_file}"
  } > "${output_path}"
}

extract_plan() {
  local raw_output=$1
  local plan_output=$2

  awk '
    /^\| == Abstract Syntax Tree ==/ {
      line = $0
      sub(/^\| /, "", line)
      print line
      capture = 1
      next
    }
    capture && /^ \|$/ { exit }
    capture { print }
  ' "${raw_output}" > "${plan_output}"
}

run_explain() {
  local mode=$1
  local output_dir
  case "${mode}" in
    q20)
      output_dir="${SCRIPT_DIR}/q20_plan"
      ;;
    q20_unique)
      output_dir="${SCRIPT_DIR}/q20_unique_plan"
      ;;
    *)
      echo "Unknown mode ${mode}" >&2
      return 1
      ;;
  esac
  local sql_file="${output_dir}/rendered_explain.sql"
  local raw_file="${output_dir}/sql_client_output.txt"
  local plan_file="${output_dir}/explain_plan.txt"

  render_explain_script "${mode}" "${sql_file}"
  "${FLINK_HOME}/bin/sql-client.sh" embedded -f "${sql_file}" > "${raw_file}" 2>&1
  extract_plan "${raw_file}" "${plan_file}"

  if [[ ! -s "${plan_file}" ]]; then
    echo "Failed to extract plan for ${mode}; inspect ${raw_file}" >&2
    return 1
  fi

  log "Saved ${mode} raw output to ${raw_file}"
  log "Saved ${mode} plan to ${plan_file}"
}

run_explain q20
run_explain q20_unique
