#!/usr/bin/env bash

set -euo pipefail

JOB_NAME="${JOB_NAME:-q20_unique}"

ENABLE_WALL_PROFILING=true
PROFILE_DELAY_SECONDS=$((10 * 60))
PROFILE_DURATION_SECONDS=$((5 * 60))
PROFILE_MODE="${PROFILE_MODE:-WALL}"
PROFILE_SCOPE="${PROFILE_SCOPE:-taskmanager}"
PROFILE_TM_ID="${PROFILE_TM_ID:-}"

COOLDOWN_SECONDS=30

DEST_ROOT="/opt/benchmark/query-runner-v2"

LOG_HOSTS_STRING="${LOG_HOSTS_STRING:-c155}"
read -r -a LOG_HOSTS <<< "$LOG_HOSTS_STRING"
SSH_USER="${SSH_USER:-}"

FLINK_BIN="/opt/flink/bin"
NEXMARK_BIN="/opt/nexmark/bin"
RUN_QUERY_CMD="${NEXMARK_BIN}/run_query.sh"
FLINK_CONF_SUFFIX="${FLINK_CONF_SUFFIX:-v2}"
FLINK_CONF_FILE=""
APPLY_CONFIG_CMD="/opt/scripts/apply_flink_config.sh"
SYNC_CLUSTER_CMD="/opt/scripts/sync_cluster.sh"

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
FLINK_LOG_DIR="${FLINK_LOG_DIR:-/opt/flink/log}"

NEXMARK_LIVE_CONF="/opt/nexmark/conf/nexmark.yaml"
NEXMARK_UNIQUE_V2_DDL="${NEXMARK_UNIQUE_V2_DDL:-/opt/nexmark/queries/ddl_gen_unique_v2.sql}"
WATERMARK_ALIGNMENT_UPDATE_INTERVAL="5ms"
OOOGS=""
PROB_DELAY=""
DELAY_MIN=""
DELAY_MAX=""
PERSON_PROPORTION=""
AUCTION_PROPORTION=""
BID_PROPORTION=""
NUM_IN_FLIGHT_AUCTIONS=""
KNOB_PREFIX=""
KNOBS_INJECTED=false
VARIANT_NAME=""
CONFIG_OVERRIDE_PREFIX=""

ROCKSDB_FIXED_PREFIX_BYTES=""
ROCKSDB_BLOOM_FILTER_BITS=""
WATERMARK_ALIGNMENT_MAX_DRIFT=""
NEXMARK_EVENTS_NUM_OVERRIDE=""
NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE=""
NEXMARK_TPS_OVERRIDE=""
MAX_EMIT_SPEED_OVERRIDE=""
CONFIG_OVERRIDE_TMPDIR=""
FLINK_CONF_BACKUP=""
NEXMARK_CONF_BACKUP=""
NEXMARK_UNIQUE_V2_DDL_BACKUP=""

BASE_OOOGS=""
BASE_PROB_DELAY=""
BASE_DELAY_MIN=""
BASE_DELAY_MAX=""
BASE_PERSON_PROPORTION=""
BASE_AUCTION_PROPORTION=""
BASE_BID_PROPORTION=""
BASE_NUM_IN_FLIGHT_AUCTIONS=""

VARIANT_SPECS=()

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

remote_ref() {
  local host=$1
  if [[ -n "$SSH_USER" ]]; then
    printf '%s@%s' "$SSH_USER" "$host"
  else
    printf '%s' "$host"
  fi
}

usage() {
  cat <<'USAGE'
Usage: run_query_runner_v2_sweep.sh [OPTIONS] EXPERIMENT_NAME TM_MEMORY_SIZES...

Options:
  --flink-conf-suffix|-c SUFFIX   Flink config variant (default: v2)
  --rocksdb-fixed-prefix-bytes N  Override state.backend.rocksdb.fixed-prefix-bytes.
  --rocksdb-bloom-filter-bits N   Override state.backend.rocksdb.bloom-filter.bits-per-key.
  --wm-alignment-max-drift D      Override scan.watermark.alignment.max-drift in
                                  ddl_gen_unique_v2.sql for unique V2 source tables.
                                  Fractional seconds are normalized to ms
                                  when possible, e.g. 0.1s -> 100ms.
                                  Also fixes alignment update-interval to 5ms
                                  unless --wm-alignment-update-interval is set.
  --wm-alignment-update-interval D
                                  Override scan.watermark.alignment.update-interval
                                  for unique V2 source tables (default: 5ms).
  --tps N                         Override nexmark workload tps.
  --max-emit-speed true|false     Override max-emit-speed in ddl_gen_unique_v2.sql
                                  for unique V2 source tables.
  --events-num N                  Override nexmark workload eval events.num.
  --warmup-events-num N           Override nexmark workload warmup.events.num.
  --ooogs N                       Out-of-order group size (default: 1)
  --prob P --delay-min S --delay-max S
                                  Probabilistic delay: probability P,
                                  min delay S sec, max delay S sec.
                                  All three must be specified together.
  --person-proportion N          Person event proportion for the workload suite.
  --auction-proportion N         Auction event proportion for the workload suite.
  --bid-proportion N             Bid event proportion for the workload suite.
                                 All three proportion flags must be specified together.
  --num-in-flight-auctions N     Approximate live-auction working-set width.
  --variant SPEC                 Run one named overnight variant. Format:
                                 NAME:key=value,key=value
                                 Keys: prob, delay-min, delay-max,
                                 person, auction, bid, ifa, ooogs

Examples:
  run_query_runner_v2_sweep.sh -c v2 my-exp 3g 6g 8g
  run_query_runner_v2_sweep.sh -c v2 --rocksdb-bloom-filter-bits 0 --warmup-events-num 8000000 --events-num 2000000 my-exp 8g
  run_query_runner_v2_sweep.sh -c v2 --wm-alignment-max-drift 100ms --warmup-events-num 30000000 --events-num 10000000 my-exp 3g
  run_query_runner_v2_sweep.sh -c v2 --tps 100000 --max-emit-speed false --wm-alignment-max-drift 100ms --wm-alignment-update-interval 0.005s my-exp 1g
  run_query_runner_v2_sweep.sh -c v2 --ooogs 1000000 my-exp 8g
  run_query_runner_v2_sweep.sh -c v2 --prob 0.1 --delay-min 60 --delay-max 240 my-exp 8g
  run_query_runner_v2_sweep.sh -c v2 --person-proportion 2 --auction-proportion 25 --bid-proportion 73 my-exp 8g
  run_query_runner_v2_sweep.sh -c v2 --num-in-flight-auctions 2000 my-exp 8g
  run_query_runner_v2_sweep.sh -c v2 --person-proportion 2 --auction-proportion 25 --bid-proportion 73 --num-in-flight-auctions 2000 my-exp 8g
  run_query_runner_v2_sweep.sh -c v2 --variant ratio:person=2,auction=25,bid=73 --variant ifa:ifa=2000 my-exp 8g 3g
  run_query_runner_v2_sweep.sh -c v2 --variant prob-0.25-15-60:prob=0.25,delay-min=15,delay-max=60 --variant prob-0.2-60-240:prob=0.2,delay-min=60,delay-max=240 my-exp 8g 3g
USAGE
}

format_number() {
  local n=$1
  if (( n >= 1000000 && n % 1000000 == 0 )); then
    echo "$((n / 1000000))M"
  elif (( n >= 1000 && n % 1000 == 0 )); then
    echo "$((n / 1000))K"
  else
    echo "$n"
  fi
}

label_safe() {
  printf '%s' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//'
}

normalize_duration_for_flink() {
  local raw=$1

  if [[ "$raw" =~ ^[0-9]+[.][0-9]+s$ ]]; then
    local seconds=${raw%s}
    local millis=""
    if ! millis=$(awk -v seconds="$seconds" '
      BEGIN {
        millis = seconds * 1000
        rounded = int(millis + 0.000000001)
        if (millis < 1 || millis - rounded > 0.000001 || rounded - millis > 0.000001) {
          exit 1
        }
        printf "%dms", rounded
      }
    '); then
      log_err "Fractional duration '${raw}' cannot be represented as whole milliseconds. Use an integer duration."
      exit 2
    fi
    printf '%s\n' "$millis"
    return
  fi

  if [[ ! "$raw" =~ ^[0-9]+[[:space:]]*[[:alpha:]]+$ ]]; then
    log_err "Invalid duration '${raw}'. Use an integer Flink duration such as 10ms, 5s, or 1000s."
    exit 2
  fi

  printf '%s\n' "$raw"
}

set_yaml_scalar() {
  local file=$1
  local key=$2
  local value=$3
  local escaped_key

  escaped_key=$(printf '%s' "$key" | sed 's/[][\\/.*^$+?|(){}-]/\\&/g')
  if grep -Eq "^${escaped_key}:" "$file"; then
    sed -i -E "s|^(${escaped_key}):.*$|\\1: ${value}|" "$file"
  else
    printf '%s: %s\n' "$key" "$value" >> "$file"
  fi
}

discover_suite_name() {
  local conf=$1
  sed -n 's/^nexmark\.workload\.suite\.\([^.]*\)\.queries:.*/\1/p' "$conf" | head -1
}

inject_knobs() {
  if [[ "$KNOBS_INJECTED" == true ]]; then
    return
  fi
  local suite
  suite=$(discover_suite_name "$NEXMARK_LIVE_CONF")
  if [[ -z "$suite" ]]; then
    log_err "Cannot discover workload suite name from ${NEXMARK_LIVE_CONF}"
    exit 2
  fi
  local prefix="nexmark.workload.suite.${suite}"
  {
    echo "# BEGIN_SWEEP_KNOBS"
    if [[ -n "$OOOGS" && "$OOOGS" != "1" ]]; then
      echo "${prefix}.out-of-order-group-size: ${OOOGS}"
    fi
    if [[ -n "$PROB_DELAY" ]] && awk "BEGIN{exit(!($PROB_DELAY > 0))}"; then
      echo "${prefix}.prob-delayed-event: ${PROB_DELAY}"
      echo "${prefix}.occasional-delay.min-sec: ${DELAY_MIN}"
      echo "${prefix}.occasional-delay.sec: ${DELAY_MAX}"
    fi
    if [[ -n "$PERSON_PROPORTION" ]]; then
      echo "${prefix}.person.proportion: ${PERSON_PROPORTION}"
      echo "${prefix}.auction.proportion: ${AUCTION_PROPORTION}"
      echo "${prefix}.bid.proportion: ${BID_PROPORTION}"
    fi
    if [[ -n "$NUM_IN_FLIGHT_AUCTIONS" ]]; then
      echo "${prefix}.num-in-flight-auctions: ${NUM_IN_FLIGHT_AUCTIONS}"
    fi
    echo "# END_SWEEP_KNOBS"
  } >> "$NEXMARK_LIVE_CONF"
  KNOBS_INJECTED=true
  log "Injected workload knobs into ${NEXMARK_LIVE_CONF}"
}

remove_knob_injection() {
  if [[ "$KNOBS_INJECTED" != true ]]; then
    return
  fi
  sed -i '/^# BEGIN_SWEEP_KNOBS$/,/^# END_SWEEP_KNOBS$/d' "$NEXMARK_LIVE_CONF"
  KNOBS_INJECTED=false
  log "Removed workload knobs from ${NEXMARK_LIVE_CONF}"
}

validate_ratio_knobs() {
  local ratio_count=0
  [[ -n "$PERSON_PROPORTION" ]] && ((ratio_count++)) || true
  [[ -n "$AUCTION_PROPORTION" ]] && ((ratio_count++)) || true
  [[ -n "$BID_PROPORTION" ]] && ((ratio_count++)) || true
  if (( ratio_count > 0 && ratio_count < 3 )); then
    log_err "--person-proportion, --auction-proportion, and --bid-proportion must all be specified together"
    usage
    exit 2
  fi
}

validate_prob_delay_knobs() {
  local prob_count=0
  [[ -n "$PROB_DELAY" ]] && ((prob_count++)) || true
  [[ -n "$DELAY_MIN" ]] && ((prob_count++)) || true
  [[ -n "$DELAY_MAX" ]] && ((prob_count++)) || true
  if (( prob_count > 0 && prob_count < 3 )); then
    log_err "--prob, --delay-min, and --delay-max must all be specified together"
    usage
    exit 2
  fi
}

compute_knob_prefix() {
  KNOB_PREFIX=""
  if [[ -n "$OOOGS" && "$OOOGS" != "1" ]]; then
    KNOB_PREFIX="$(format_number "$OOOGS")-ooogs"
  fi
  if [[ -n "$PROB_DELAY" ]] && awk "BEGIN{exit(!($PROB_DELAY > 0))}"; then
    local local_tag="prob-delay-${PROB_DELAY}_${DELAY_MIN}-${DELAY_MAX}"
    if [[ -n "$KNOB_PREFIX" ]]; then
      KNOB_PREFIX="${KNOB_PREFIX}-${local_tag}"
    else
      KNOB_PREFIX="${local_tag}"
    fi
  fi
  if [[ -n "$PERSON_PROPORTION" ]]; then
    local local_tag="p${PERSON_PROPORTION}-a${AUCTION_PROPORTION}-b${BID_PROPORTION}"
    if [[ -n "$KNOB_PREFIX" ]]; then
      KNOB_PREFIX="${KNOB_PREFIX}-${local_tag}"
    else
      KNOB_PREFIX="${local_tag}"
    fi
  fi
  if [[ -n "$NUM_IN_FLIGHT_AUCTIONS" ]]; then
    local local_tag="ifa-$(format_number "$NUM_IN_FLIGHT_AUCTIONS")"
    if [[ -n "$KNOB_PREFIX" ]]; then
      KNOB_PREFIX="${KNOB_PREFIX}-${local_tag}"
    else
      KNOB_PREFIX="${local_tag}"
    fi
  fi
}

compute_config_override_prefix() {
  CONFIG_OVERRIDE_PREFIX=""
  if [[ -n "$ROCKSDB_FIXED_PREFIX_BYTES" ]]; then
    CONFIG_OVERRIDE_PREFIX="fp-${ROCKSDB_FIXED_PREFIX_BYTES}"
  fi
  if [[ -n "$ROCKSDB_BLOOM_FILTER_BITS" ]]; then
    local local_tag="bloom-${ROCKSDB_BLOOM_FILTER_BITS}"
    if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
      CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
    else
      CONFIG_OVERRIDE_PREFIX="${local_tag}"
    fi
  fi
  if [[ -n "$WATERMARK_ALIGNMENT_MAX_DRIFT" ]]; then
    local local_tag="wm-drift-$(label_safe "$WATERMARK_ALIGNMENT_MAX_DRIFT")"
    if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
      CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
    else
      CONFIG_OVERRIDE_PREFIX="${local_tag}"
    fi
    local_tag="wm-update-$(label_safe "$WATERMARK_ALIGNMENT_UPDATE_INTERVAL")"
    CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
  fi
  if [[ -n "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE" ]]; then
    local local_tag="warmup-$(format_number "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE")"
    if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
      CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
    else
      CONFIG_OVERRIDE_PREFIX="${local_tag}"
    fi
  fi
  if [[ -n "$NEXMARK_EVENTS_NUM_OVERRIDE" ]]; then
    local local_tag="eval-$(format_number "$NEXMARK_EVENTS_NUM_OVERRIDE")"
    if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
      CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
    else
      CONFIG_OVERRIDE_PREFIX="${local_tag}"
    fi
  fi
  if [[ -n "$NEXMARK_TPS_OVERRIDE" ]]; then
    local local_tag="tps-$(format_number "$NEXMARK_TPS_OVERRIDE")"
    if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
      CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
    else
      CONFIG_OVERRIDE_PREFIX="${local_tag}"
    fi
  fi
  if [[ -n "$MAX_EMIT_SPEED_OVERRIDE" ]]; then
    local local_tag="max-emit-${MAX_EMIT_SPEED_OVERRIDE}"
    if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
      CONFIG_OVERRIDE_PREFIX="${CONFIG_OVERRIDE_PREFIX}-${local_tag}"
    else
      CONFIG_OVERRIDE_PREFIX="${local_tag}"
    fi
  fi
}

apply_variant_spec() {
  local spec=$1
  local name_part=${spec%%:*}
  local settings_part=""
  if [[ "$spec" == *:* ]]; then
    settings_part=${spec#*:}
  fi
  if [[ -z "$name_part" || -z "$settings_part" || "$name_part" == "$spec" ]]; then
    log_err "Invalid --variant '${spec}'. Expected NAME:key=value,key=value"
    exit 2
  fi

  VARIANT_NAME="$name_part"
  OOOGS="$BASE_OOOGS"
  PROB_DELAY="$BASE_PROB_DELAY"
  DELAY_MIN="$BASE_DELAY_MIN"
  DELAY_MAX="$BASE_DELAY_MAX"
  PERSON_PROPORTION="$BASE_PERSON_PROPORTION"
  AUCTION_PROPORTION="$BASE_AUCTION_PROPORTION"
  BID_PROPORTION="$BASE_BID_PROPORTION"
  NUM_IN_FLIGHT_AUCTIONS="$BASE_NUM_IN_FLIGHT_AUCTIONS"

  IFS=',' read -r -a assignments <<< "$settings_part"
  for assignment in "${assignments[@]}"; do
    assignment=${assignment// /}
    local key=${assignment%%=*}
    local value=${assignment#*=}
    if [[ -z "$key" || -z "$value" || "$key" == "$assignment" ]]; then
      log_err "Invalid variant assignment '${assignment}' in '${spec}'"
      exit 2
    fi
    case "$key" in
      prob)
        PROB_DELAY="$value"
        ;;
      delay-min)
        DELAY_MIN="$value"
        ;;
      delay-max)
        DELAY_MAX="$value"
        ;;
      person)
        PERSON_PROPORTION="$value"
        ;;
      auction)
        AUCTION_PROPORTION="$value"
        ;;
      bid)
        BID_PROPORTION="$value"
        ;;
      ifa)
        NUM_IN_FLIGHT_AUCTIONS="$value"
        ;;
      ooogs)
        OOOGS="$value"
        ;;
      *)
        log_err "Unsupported variant key '${key}' in '${spec}'"
        exit 2
        ;;
    esac
  done

  validate_prob_delay_knobs
  validate_ratio_knobs
}

cleanup() {
  log "Cleaning up cluster before exit..."
  ensure_job_stopped "${current_job_pid:-}" "${current_job_pgid:-}" || true
  stop_cluster || true
  remove_knob_injection || true
  restore_config_overrides || true
}

has_config_overrides() {
  [[ -n "$ROCKSDB_FIXED_PREFIX_BYTES" \
    || -n "$ROCKSDB_BLOOM_FILTER_BITS" \
    || -n "$WATERMARK_ALIGNMENT_MAX_DRIFT" \
    || -n "$NEXMARK_EVENTS_NUM_OVERRIDE" \
    || -n "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE" \
    || -n "$NEXMARK_TPS_OVERRIDE" \
    || -n "$MAX_EMIT_SPEED_OVERRIDE" ]]
}

backup_config_overrides() {
  if ! has_config_overrides; then
    return
  fi
  if [[ -n "$CONFIG_OVERRIDE_TMPDIR" ]]; then
    return
  fi
  CONFIG_OVERRIDE_TMPDIR=$(mktemp -d)
  FLINK_CONF_BACKUP="${CONFIG_OVERRIDE_TMPDIR}/$(basename "$FLINK_CONF_FILE").bak"
  NEXMARK_CONF_BACKUP="${CONFIG_OVERRIDE_TMPDIR}/$(basename "$NEXMARK_LIVE_CONF").bak"
  cp "$FLINK_CONF_FILE" "$FLINK_CONF_BACKUP"
  cp "$NEXMARK_LIVE_CONF" "$NEXMARK_CONF_BACKUP"
  if [[ -n "$WATERMARK_ALIGNMENT_MAX_DRIFT" || -n "$MAX_EMIT_SPEED_OVERRIDE" ]]; then
    if [[ ! -f "$NEXMARK_UNIQUE_V2_DDL" ]]; then
      log_err "Cannot find unique V2 DDL: ${NEXMARK_UNIQUE_V2_DDL}"
      exit 2
    fi
    NEXMARK_UNIQUE_V2_DDL_BACKUP="${CONFIG_OVERRIDE_TMPDIR}/$(basename "$NEXMARK_UNIQUE_V2_DDL").bak"
    cp "$NEXMARK_UNIQUE_V2_DDL" "$NEXMARK_UNIQUE_V2_DDL_BACKUP"
  fi
  log "Backed up config overrides into ${CONFIG_OVERRIDE_TMPDIR}"
}

set_unique_v2_watermark_alignment_max_drift() {
  local value=$1
  local update_interval=$2
  local drift_pattern="'scan\\.watermark\\.alignment\\.max-drift'[[:space:]]*=[[:space:]]*'[^']*'"
  local update_pattern="'scan\\.watermark\\.alignment\\.update-interval'[[:space:]]*=[[:space:]]*'[^']*'"
  local drift_count
  local update_count

  if [[ ! -f "$NEXMARK_UNIQUE_V2_DDL" ]]; then
    log_err "Cannot find unique V2 DDL: ${NEXMARK_UNIQUE_V2_DDL}"
    exit 2
  fi

  drift_count=$(grep -Ec "$drift_pattern" "$NEXMARK_UNIQUE_V2_DDL" || true)
  if (( drift_count == 0 )); then
    log_err "No scan.watermark.alignment.max-drift entries found in ${NEXMARK_UNIQUE_V2_DDL}"
    exit 2
  fi
  update_count=$(grep -Ec "$update_pattern" "$NEXMARK_UNIQUE_V2_DDL" || true)
  if (( update_count == 0 )); then
    log_err "No scan.watermark.alignment.update-interval entries found in ${NEXMARK_UNIQUE_V2_DDL}"
    exit 2
  fi

  sed -i -E "s|('scan\\.watermark\\.alignment\\.max-drift'[[:space:]]*=[[:space:]]*)'[^']*'|\\1'${value}'|g" "$NEXMARK_UNIQUE_V2_DDL"
  sed -i -E "s|('scan\\.watermark\\.alignment\\.update-interval'[[:space:]]*=[[:space:]]*)'[^']*'|\\1'${update_interval}'|g" "$NEXMARK_UNIQUE_V2_DDL"
  log "Set scan.watermark.alignment.max-drift=${value} (${drift_count} occurrences) and update-interval=${update_interval} (${update_count} occurrences) in ${NEXMARK_UNIQUE_V2_DDL}."
}

set_unique_v2_max_emit_speed() {
  local value=$1
  local max_emit_pattern="'max-emit-speed'[[:space:]]*=[[:space:]]*'[^']*'"
  local max_emit_count

  case "$value" in
    true|false)
      ;;
    *)
      log_err "Invalid --max-emit-speed value '${value}'. Use true or false."
      exit 2
      ;;
  esac

  if [[ ! -f "$NEXMARK_UNIQUE_V2_DDL" ]]; then
    log_err "Cannot find unique V2 DDL: ${NEXMARK_UNIQUE_V2_DDL}"
    exit 2
  fi

  max_emit_count=$(grep -Ec "$max_emit_pattern" "$NEXMARK_UNIQUE_V2_DDL" || true)
  if (( max_emit_count == 0 )); then
    log_err "No max-emit-speed entries found in ${NEXMARK_UNIQUE_V2_DDL}"
    exit 2
  fi

  sed -i -E "s|('max-emit-speed'[[:space:]]*=[[:space:]]*)'[^']*'|\\1'${value}'|g" "$NEXMARK_UNIQUE_V2_DDL"
  log "Set max-emit-speed=${value} (${max_emit_count} occurrences) in ${NEXMARK_UNIQUE_V2_DDL}."
}

apply_config_overrides() {
  if ! has_config_overrides; then
    return
  fi

  backup_config_overrides

  if [[ -n "$ROCKSDB_FIXED_PREFIX_BYTES" ]]; then
    set_yaml_scalar "$FLINK_CONF_FILE" "state.backend.rocksdb.fixed-prefix-bytes" "$ROCKSDB_FIXED_PREFIX_BYTES"
  fi
  if [[ -n "$ROCKSDB_BLOOM_FILTER_BITS" ]]; then
    set_yaml_scalar "$FLINK_CONF_FILE" "state.backend.rocksdb.bloom-filter.bits-per-key" "$ROCKSDB_BLOOM_FILTER_BITS"
  fi

  local suite=""
  if [[ -n "$NEXMARK_EVENTS_NUM_OVERRIDE" || -n "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE" || -n "$NEXMARK_TPS_OVERRIDE" ]]; then
    suite=$(discover_suite_name "$NEXMARK_LIVE_CONF")
    if [[ -z "$suite" ]]; then
      log_err "Cannot discover workload suite name from ${NEXMARK_LIVE_CONF}"
      exit 2
    fi
  fi
  if [[ -n "$NEXMARK_TPS_OVERRIDE" ]]; then
    set_yaml_scalar "$NEXMARK_LIVE_CONF" "nexmark.workload.suite.${suite}.tps" "$NEXMARK_TPS_OVERRIDE"
  fi
  if [[ -n "$NEXMARK_EVENTS_NUM_OVERRIDE" ]]; then
    set_yaml_scalar "$NEXMARK_LIVE_CONF" "nexmark.workload.suite.${suite}.events.num" "$NEXMARK_EVENTS_NUM_OVERRIDE"
  fi
  if [[ -n "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE" ]]; then
    set_yaml_scalar "$NEXMARK_LIVE_CONF" "nexmark.workload.suite.${suite}.warmup.events.num" "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE"
  fi
  if [[ -n "$WATERMARK_ALIGNMENT_MAX_DRIFT" ]]; then
    set_unique_v2_watermark_alignment_max_drift "$WATERMARK_ALIGNMENT_MAX_DRIFT" "$WATERMARK_ALIGNMENT_UPDATE_INTERVAL"
  fi
  if [[ -n "$MAX_EMIT_SPEED_OVERRIDE" ]]; then
    set_unique_v2_max_emit_speed "$MAX_EMIT_SPEED_OVERRIDE"
  fi
}

restore_config_overrides() {
  if [[ -z "$CONFIG_OVERRIDE_TMPDIR" ]]; then
    return
  fi
  cp "$FLINK_CONF_BACKUP" "$FLINK_CONF_FILE"
  cp "$NEXMARK_CONF_BACKUP" "$NEXMARK_LIVE_CONF"
  if [[ -n "$NEXMARK_UNIQUE_V2_DDL_BACKUP" ]]; then
    cp "$NEXMARK_UNIQUE_V2_DDL_BACKUP" "$NEXMARK_UNIQUE_V2_DDL"
  fi
  rm -rf "$CONFIG_OVERRIDE_TMPDIR"
  CONFIG_OVERRIDE_TMPDIR=""
  FLINK_CONF_BACKUP=""
  NEXMARK_CONF_BACKUP=""
  NEXMARK_UNIQUE_V2_DDL_BACKUP=""
  log "Restored config overrides."
}

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
    [[ -n "$ROCKSDB_FIXED_PREFIX_BYTES" ]] && echo "rocksdb_fixed_prefix_bytes=${ROCKSDB_FIXED_PREFIX_BYTES}"
    [[ -n "$ROCKSDB_BLOOM_FILTER_BITS" ]] && echo "rocksdb_bloom_filter_bits=${ROCKSDB_BLOOM_FILTER_BITS}"
    [[ -n "$WATERMARK_ALIGNMENT_MAX_DRIFT" ]] && echo "watermark_alignment_max_drift=${WATERMARK_ALIGNMENT_MAX_DRIFT}"
    [[ -n "$WATERMARK_ALIGNMENT_MAX_DRIFT" ]] && echo "watermark_alignment_update_interval=${WATERMARK_ALIGNMENT_UPDATE_INTERVAL}"
    [[ -n "$NEXMARK_TPS_OVERRIDE" ]] && echo "tps=${NEXMARK_TPS_OVERRIDE}"
    [[ -n "$MAX_EMIT_SPEED_OVERRIDE" ]] && echo "max_emit_speed=${MAX_EMIT_SPEED_OVERRIDE}"
    [[ -n "$NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE" ]] && echo "warmup_events_num=${NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE}"
    [[ -n "$NEXMARK_EVENTS_NUM_OVERRIDE" ]] && echo "events_num=${NEXMARK_EVENTS_NUM_OVERRIDE}"
    [[ -n "$OOOGS" ]] && echo "out_of_order_group_size=${OOOGS}"
    [[ -n "$PROB_DELAY" ]] && echo "prob_delayed_event=${PROB_DELAY}"
    [[ -n "$DELAY_MIN" ]] && echo "occasional_delay_min_sec=${DELAY_MIN}"
    [[ -n "$DELAY_MAX" ]] && echo "occasional_delay_sec=${DELAY_MAX}"
    [[ -n "$PERSON_PROPORTION" ]] && echo "person_proportion=${PERSON_PROPORTION}"
    [[ -n "$AUCTION_PROPORTION" ]] && echo "auction_proportion=${AUCTION_PROPORTION}"
    [[ -n "$BID_PROPORTION" ]] && echo "bid_proportion=${BID_PROPORTION}"
    [[ -n "$NUM_IN_FLIGHT_AUCTIONS" ]] && echo "num_in_flight_auctions=${NUM_IN_FLIGHT_AUCTIONS}"
    [[ -n "$VARIANT_NAME" ]] && echo "variant_name=${VARIANT_NAME}"
  } > "$meta_file"
  cp "$FLINK_CONF_FILE" "${run_dir}/flink-conf.yaml"
  cp "$NEXMARK_LIVE_CONF" "${run_dir}/nexmark.yaml"
  if [[ -f "$NEXMARK_UNIQUE_V2_DDL" ]]; then
    cp "$NEXMARK_UNIQUE_V2_DDL" "${run_dir}/ddl_gen_unique_v2.sql"
  fi
}

collect_rocksdb_logs() {
  local run_dir=$1
  local stats_dir="${run_dir}/rocksdb_logs"
  local log_file="${stats_dir}/log-collection.log"
  mkdir -p "$stats_dir"

  for host in "${LOG_HOSTS[@]}"; do
    local remote
    remote=$(remote_ref "$host")
    log "Checking ${remote} for RocksDB logs..." | tee -a "$log_file"
    local has_logs=""
    if ! has_logs=$(ssh "$remote" 'find /data/rocksdb_native_logs -mindepth 1 -maxdepth 1 -print -quit' 2>&1); then
      log "Log scan failed on ${remote}: ${has_logs}" | tee -a "$log_file"
      has_logs=""
    fi
    if [[ -z "$has_logs" ]]; then
      log "No RocksDB logs found in /data/rocksdb_native_logs on ${remote}" | tee -a "$log_file"
      log "Recent /data/rocksdb_native_logs entries on ${remote} (tail -n 20):" | tee -a "$log_file"
      local remote_ls=""
      if remote_ls=$(ssh "$remote" 'ls -l /data/rocksdb_native_logs 2>&1 | tail -n 20' 2>&1); then
        printf '%s\n' "$remote_ls" | tee -a "$log_file"
      else
        log "Failed to list /data/rocksdb_native_logs on ${remote}: ${remote_ls}" | tee -a "$log_file"
      fi
      continue
    fi
    local host_dir="${stats_dir}/${host}"
    mkdir -p "$host_dir"
    log "Copying /data/rocksdb_native_logs from ${remote} -> ${host_dir}" | tee -a "$log_file"
    scp -r "${remote}:/data/rocksdb_native_logs/." "${host_dir}/"
    log "Removing /data/rocksdb_native_logs contents from ${remote} after copy." | tee -a "$log_file"
    ssh "$remote" 'find /data/rocksdb_native_logs -mindepth 1 -maxdepth 1 -exec rm -rf {} +'
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
  local scheduler_log="${output_dir}/profile_scheduler.log"

  mkdir -p "$output_dir"

  (
    exec >>"$scheduler_log" 2>&1
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
  scheduled_profile_pid=$!
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
    --rocksdb-fixed-prefix-bytes)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --rocksdb-fixed-prefix-bytes"
        usage
        exit 2
      fi
      ROCKSDB_FIXED_PREFIX_BYTES="$1"
      shift
      ;;
    --rocksdb-bloom-filter-bits)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --rocksdb-bloom-filter-bits"
        usage
        exit 2
      fi
      ROCKSDB_BLOOM_FILTER_BITS="$1"
      shift
      ;;
    --wm-alignment-max-drift)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --wm-alignment-max-drift"
        usage
        exit 2
      fi
      WATERMARK_ALIGNMENT_MAX_DRIFT=$(normalize_duration_for_flink "$1")
      shift
      ;;
    --wm-alignment-update-interval)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --wm-alignment-update-interval"
        usage
        exit 2
      fi
      WATERMARK_ALIGNMENT_UPDATE_INTERVAL=$(normalize_duration_for_flink "$1")
      shift
      ;;
    --tps)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --tps"
        usage
        exit 2
      fi
      NEXMARK_TPS_OVERRIDE="$1"
      shift
      ;;
    --max-emit-speed)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --max-emit-speed"
        usage
        exit 2
      fi
      case "$1" in
        true|false)
          MAX_EMIT_SPEED_OVERRIDE="$1"
          ;;
        *)
          log_err "Invalid --max-emit-speed value '$1'. Use true or false."
          usage
          exit 2
          ;;
      esac
      shift
      ;;
    --events-num)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --events-num"
        usage
        exit 2
      fi
      NEXMARK_EVENTS_NUM_OVERRIDE="$1"
      shift
      ;;
    --warmup-events-num)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --warmup-events-num"
        usage
        exit 2
      fi
      NEXMARK_WARMUP_EVENTS_NUM_OVERRIDE="$1"
      shift
      ;;
    --ooogs)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --ooogs"
        usage
        exit 2
      fi
      OOOGS="$1"
      shift
      ;;
    --prob)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --prob"
        usage
        exit 2
      fi
      PROB_DELAY="$1"
      shift
      ;;
    --delay-min)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --delay-min"
        usage
        exit 2
      fi
      DELAY_MIN="$1"
      shift
      ;;
    --delay-max)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --delay-max"
        usage
        exit 2
      fi
      DELAY_MAX="$1"
      shift
      ;;
    --person-proportion)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --person-proportion"
        usage
        exit 2
      fi
      PERSON_PROPORTION="$1"
      shift
      ;;
    --auction-proportion)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --auction-proportion"
        usage
        exit 2
      fi
      AUCTION_PROPORTION="$1"
      shift
      ;;
    --bid-proportion)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --bid-proportion"
        usage
        exit 2
      fi
      BID_PROPORTION="$1"
      shift
      ;;
    --num-in-flight-auctions)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --num-in-flight-auctions"
        usage
        exit 2
      fi
      NUM_IN_FLIGHT_AUCTIONS="$1"
      shift
      ;;
    --variant)
      shift
      if [[ $# -eq 0 ]]; then
        log_err "Missing value for --variant"
        usage
        exit 2
      fi
      VARIANT_SPECS+=("$1")
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

validate_prob_delay_knobs
validate_ratio_knobs

BASE_OOOGS="$OOOGS"
BASE_PROB_DELAY="$PROB_DELAY"
BASE_DELAY_MIN="$DELAY_MIN"
BASE_DELAY_MAX="$DELAY_MAX"
BASE_PERSON_PROPORTION="$PERSON_PROPORTION"
BASE_AUCTION_PROPORTION="$AUCTION_PROPORTION"
BASE_BID_PROPORTION="$BID_PROPORTION"
BASE_NUM_IN_FLIGHT_AUCTIONS="$NUM_IN_FLIGHT_AUCTIONS"

FLINK_CONF_FILE=$(resolve_flink_conf_file "$FLINK_CONF_SUFFIX")
trap cleanup EXIT
apply_config_overrides

EXPERIMENT_NAME=$1
shift

TM_MEMORY_SIZES=("$@")

RUN_DATE_MMDDHH="${RUN_DATE_MMDDHH:-$(date +%m%d%H)}"

run_current_sweep() {
  compute_knob_prefix
  compute_config_override_prefix

  local experiment_label="${EXPERIMENT_NAME}-${JOB_NAME}"
  if [[ -n "$VARIANT_NAME" ]]; then
    experiment_label="${experiment_label}-${VARIANT_NAME}"
  fi
  if [[ -n "$FLINK_CONF_SUFFIX" ]]; then
    experiment_label="${experiment_label}-${FLINK_CONF_SUFFIX}"
  fi
  if [[ -n "$KNOB_PREFIX" ]]; then
    experiment_label="${experiment_label}-${KNOB_PREFIX}"
  fi
  if [[ -n "$CONFIG_OVERRIDE_PREFIX" ]]; then
    experiment_label="${experiment_label}-${CONFIG_OVERRIDE_PREFIX}"
  fi
  experiment_label="${RUN_DATE_MMDDHH}-${experiment_label}"
  local experiment_root="${DEST_ROOT}/${experiment_label}"

  mkdir -p "$experiment_root"

  if [[ -n "$KNOB_PREFIX" ]]; then
    inject_knobs
  fi

  local last_tm_index=$((${#TM_MEMORY_SIZES[@]} - 1))

  for tm_idx in "${!TM_MEMORY_SIZES[@]}"; do
    tm_size=${TM_MEMORY_SIZES[$tm_idx]}
    run_dir="${experiment_root}/exp-${tm_size}-tm-process"
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
      scheduled_profile_pid=""
      schedule_profile "$current_job_pid" "$profile_dir" "$PROFILE_DELAY_SECONDS" "$PROFILE_DURATION_SECONDS"
      profile_pid="$scheduled_profile_pid"
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

  remove_knob_injection
}

if [[ ${#VARIANT_SPECS[@]} -eq 0 ]]; then
  VARIANT_NAME=""
  OOOGS="$BASE_OOOGS"
  PROB_DELAY="$BASE_PROB_DELAY"
  DELAY_MIN="$BASE_DELAY_MIN"
  DELAY_MAX="$BASE_DELAY_MAX"
  PERSON_PROPORTION="$BASE_PERSON_PROPORTION"
  AUCTION_PROPORTION="$BASE_AUCTION_PROPORTION"
  BID_PROPORTION="$BASE_BID_PROPORTION"
  NUM_IN_FLIGHT_AUCTIONS="$BASE_NUM_IN_FLIGHT_AUCTIONS"
  run_current_sweep
else
  for variant_spec in "${VARIANT_SPECS[@]}"; do
    apply_variant_spec "$variant_spec"
    log "Starting variant '${VARIANT_NAME}'..."
    run_current_sweep
  done
fi

log "QueryRunnerV2 sweep complete."
