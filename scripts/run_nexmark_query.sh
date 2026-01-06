#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

# Examples:
#   ./run_nexmark_query.sh q20_unique_v1
#   ./run_nexmark_query.sh q1,q2,q3
#   ./run_nexmark_query.sh --category oa q1 q2 q3
#   ./run_nexmark_query.sh --no-stop q1,q2

category=""
no_start=false
no_stop=false
queries=()
extra_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --category)
      category=$2
      shift 2
      ;;
    --no-start)
      no_start=true
      shift
      ;;
    --no-stop)
      no_stop=true
      shift
      ;;
    --)
      shift
      extra_args+=("$@")
      break
      ;;
    *)
      queries+=("$1")
      shift
      ;;
  esac
done

if [[ ${#queries[@]} -eq 0 ]]; then
  queries=("all")
fi

queries_csv=$(IFS=,; echo "${queries[*]}")

if [[ "$no_start" == false ]]; then
  "${SCRIPT_DIR}/start_cluster.sh"
fi

log "Running queries: ${queries_csv}"
if [[ -n "$category" ]]; then
  maybe_sudo "${NEXMARK_HOME}/bin/run_query.sh" "$category" "$queries_csv" "${extra_args[@]}"
else
  maybe_sudo "${NEXMARK_HOME}/bin/run_query.sh" "$queries_csv" "${extra_args[@]}"
fi

if [[ "$no_stop" == false ]]; then
  "${SCRIPT_DIR}/stop_cluster.sh"
fi
