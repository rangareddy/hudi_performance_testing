#!/usr/bin/env bash
#
# Run HoodieCompactor (scheduleAndExecute) for a MERGE_ON_READ table.
# Table path must match run_hudi_ingestion.sh / run_hudi_benchmark.sh (same --table-name-suffix).
#
set -euo pipefail

SCRIPT_NAME="$0"
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_NAME}")" && pwd)"

# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/scripts/load_config.sh"

usage() {
  log_info ""
  log_info "Usage:"
  log_info "  bash $SCRIPT_NAME --table-type MERGE_ON_READ --target-hudi-version <ver> [--table-name-suffix <suffix>]"
  log_info ""
  exit 1
}

TARGET_HUDI_VERSION="$HUDI_VERSION"
TABLE_NAME_SUFFIX_ARG=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      [[ -z "${2:-}" ]] && { log_error "❌ Error: --table-type requires a value"; usage; }
      TABLE_TYPE="$2"
      shift 2
      ;;
    --target-hudi-version)
      [[ -z "${2:-}" ]] && { log_error "❌ Error: --target-hudi-version requires a value"; usage; }
      TARGET_HUDI_VERSION="$2"
      shift 2
      ;;
    --table-name-suffix)
      [[ -z "${2:-}" ]] && { log_error "❌ Error: --table-name-suffix requires a value"; usage; }
      TABLE_NAME_SUFFIX_ARG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      log_error "❌ Unknown option: $1"
      usage
      ;;
  esac
done

if [[ -z "${TABLE_TYPE:-}" ]]; then
  log_error "❌ Error: --table-type is required"
  usage
fi

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')
HUDI_VERSION_SUFFIX=$(echo "$TARGET_HUDI_VERSION" | sed 's/-.*//' | cut -d. -f1,2 | tr '.' '_')
IS_LOGICAL_TIMESTAMP_ENABLED=${IS_LOGICAL_TIMESTAMP_ENABLED:-true}

BASE_TABLE_NAME=${BASE_TABLE_NAME:-hudi_regular}

case "$TABLE_TYPE_UPPER" in
  MERGE_ON_READ|MOR)
    TABLE_TYPE="MERGE_ON_READ"
    TABLE_NAME="${BASE_TABLE_NAME}_mor_${HUDI_VERSION_SUFFIX}"
    ;;
  *)
    log_error "❌ Compaction is only supported for MERGE_ON_READ (got: $TABLE_TYPE)"
    exit 1
    ;;
esac

if [[ "$IS_LOGICAL_TIMESTAMP_ENABLED" == true ]]; then
  TABLE_NAME="${TABLE_NAME}_lts"
fi

if [[ -n "$TABLE_NAME_SUFFIX_ARG" ]]; then
  if [[ ! "$TABLE_NAME_SUFFIX_ARG" =~ ^[a-zA-Z0-9_]+$ ]]; then
    log_error "❌ Error: --table-name-suffix must be alphanumeric or underscore only"
    exit 1
  fi
  TABLE_NAME="${TABLE_NAME}_${TABLE_NAME_SUFFIX_ARG}"
fi

TABLE_BASE_PATH="${DATA_PATH}/${TABLE_NAME}"

HUDI_UTILITIES_JAR="${JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
HUDI_SPARK_JAR="${JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
if [[ ! -f "$HUDI_UTILITIES_JAR" ]]; then
  log_error "❌ Hudi Utilities Slim Bundle Jar not found: $HUDI_UTILITIES_JAR"
  exit 1
fi
if [[ ! -f "$HUDI_SPARK_JAR" ]]; then
  log_error "❌ Hudi Spark Bundle Jar not found: $HUDI_SPARK_JAR"
  exit 1
fi
HUDI_JARS="${HUDI_SPARK_JAR},${HUDI_UTILITIES_JAR}"
SPARK_SUBMIT_JARS="$HUDI_JARS"
[[ -n "${AWS_S3_JARS:-}" ]] && SPARK_SUBMIT_JARS="${SPARK_SUBMIT_JARS},${AWS_S3_JARS}"

append_compaction_write_perf() {
  local duration_sec="$1"
  local status="$2"
  [[ -z "${WRITE_PERF_CSV:-}" ]] && return 0
  mkdir -p "$(dirname "$WRITE_PERF_CSV")"
  local header="run_timestamp_utc,table_type,operation,batch_id,hudi_version,execution_time_seconds,status,is_logical_timestamp_enabled"
  if [[ ! -f "$WRITE_PERF_CSV" ]]; then
    echo "$header" > "$WRITE_PERF_CSV"
  fi
  echo "$(date -u +"%Y-%m-%d %H:%M:%S"),${TABLE_TYPE},hudi_compaction,-,${TARGET_HUDI_VERSION},${duration_sec},${status},${IS_LOGICAL_TIMESTAMP_ENABLED:-true}" >> "$WRITE_PERF_CSV"
}

PROPS_ARGS=()
if [[ -n "${PROPS_FILE:-}" ]]; then
  PROPS_ARGS=(--props "$PROPS_FILE")
fi

ZOOKEEPER_HOST=$(hostname)

_log_compact_props=""
if [[ ${#PROPS_ARGS[@]} -gt 0 ]]; then
  _log_compact_props="  --props \"${PROPS_FILE}\" \\
"
fi

log_equal
log_info "Running HoodieCompactor (scheduleAndExecute)"
log_hyphen
log_info "HUDI_VERSION    : $TARGET_HUDI_VERSION"
log_info "TABLE_NAME      : $TABLE_NAME"
log_info "TABLE_BASE_PATH : $TABLE_BASE_PATH"
log_equal

_wp_start=$(date +%s)
_COMPACT_LOG=$(mktemp)
trap 'rm -f "$_COMPACT_LOG"' EXIT

set +e
"${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --deploy-mode client \
  --jars "$SPARK_SUBMIT_JARS" \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --class org.apache.hudi.utilities.HoodieCompactor \
  "$HUDI_UTILITIES_JAR" \
  "${PROPS_ARGS[@]}" \
  --mode scheduleAndExecute \
  --base-path "$TABLE_BASE_PATH" \
  --table-name "$TABLE_NAME" \
  --spark-master yarn \
  --hoodie-conf hoodie.write.concurrency.mode=optimistic_concurrency_control \
  --hoodie-conf hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider \
  --hoodie-conf hoodie.write.lock.zookeeper.url="$ZOOKEEPER_HOST" \
  --hoodie-conf hoodie.write.lock.zookeeper.port=2181 \
  --hoodie-conf hoodie.write.lock.zookeeper.base_path="/hudi/locks" \
  --hoodie-conf hoodie.write.lock.zookeeper.lock_key="$TABLE_NAME" \
  --hoodie-conf hoodie.compact.inline.max.delta.commits=1 \
  2>&1 | tee "$_COMPACT_LOG"
_compact_rc=${PIPESTATUS[0]}
set -e

_wp_end=$(date +%s)
_wp_dur=$((_wp_end - _wp_start))

if [[ "$_compact_rc" -eq 0 ]]; then
  log_info "Compaction finished. Total wall time: ${_wp_dur} seconds"
  append_compaction_write_perf "$_wp_dur" "ok"
  log_success "✅ HoodieCompactor completed successfully in ${_wp_dur} seconds"
elif [[ "${HUDI_COMPACTION_ALLOW_EMPTY:-true}" == "true" ]] && grep -E -q "Couldn't do schedule|No operations are retrieved|Total of 0 compaction operations" "$_COMPACT_LOG"; then
  log_warn "HoodieCompactor had nothing to compact (no MOR delta logs yet, empty path, or already compacted). Set HUDI_COMPACTION_ALLOW_EMPTY=false to fail hard."
  append_compaction_write_perf "$_wp_dur" "no_pending_compaction"
  log_success "✅ HoodieCompactor finished with no pending compaction (${_wp_dur}s)"
else
  append_compaction_write_perf "$_wp_dur" "failure"
  log_error "❌ HoodieCompactor failed after ${_wp_dur} seconds (exit ${_compact_rc})"
  exit 1
fi
log_hyphen