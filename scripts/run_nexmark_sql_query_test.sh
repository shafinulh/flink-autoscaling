#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

# Experimental SQL flow for Justin autoscaling.
# Source: /opt/nexmark-src/nexmark-flink-scaling/src/main/java/com/github/nexmark/flink/sql/SqlQueryJob.java

query="q7"
parallelism=1
tps=7500
events=0
duration_seconds=7200
main_class="com.github.nexmark.flink.sql.SqlQueryJob"
jar_path="${FLINK_HOME}/lib/nexmark-flink-0.3-SNAPSHOT.jar"
stop_after=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --query) query=$2; shift 2;;
    --parallelism) parallelism=$2; shift 2;;
    --tps) tps=$2; shift 2;;
    --events) events=$2; shift 2;;
    --duration-seconds) duration_seconds=$2; shift 2;;
    --class) main_class=$2; shift 2;;
    --jar) jar_path=$2; shift 2;;
    --no-stop) stop_after=false; shift;;
    *) query=$1; shift;;
  esac
done

log "Submitting SQL job ${query}"
maybe_sudo "${FLINK_HOME}/bin/flink" run -d \
  -c "$main_class" \
  "$jar_path" \
  --query "$query" \
  --job-name "Nexmark ${query} SQL" \
  --parallelism "$parallelism" \
  --tps "$tps" \
  --events "$events"

log "Waiting ${duration_seconds}s"
sleep "$duration_seconds"

if [[ "$stop_after" == true ]]; then
  "${SCRIPT_DIR}/stop_cluster.sh"
fi
