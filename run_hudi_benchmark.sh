#!/usr/bin/env bash
#
# Run Hudi read benchmark for COPY_ON_WRITE or MERGE_ON_READ tables with SOURCE_HUDI_VERSION or TARGET_HUDI_VERSION jars.
# Use with SOURCE_HUDI_VERSION or TARGET_HUDI_VERSION jars to compare read performance (set HUDI_VERSION in common.properties).
#
set -euo pipefail

SCRIPT_NAME="$0"
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_NAME}")" && pwd)"

# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/scripts/load_config.sh"

usage() {
  echo ""
  echo "Usage:"
  echo "  bash $SCRIPT_NAME --table-type <COPY_ON_WRITE|MERGE_ON_READ> --target-hudi-version <SOURCE_HUDI_VERSION|TARGET_HUDI_VERSION> [--batch-id <id> --help]"
  echo ""
  echo "  --batch-id is optional (used by run_benchmark_suite.py for CSV labeling)."
  echo "  --table-name-suffix optional (e.g. baseline / experiment for E2E)."
  echo ""
  echo "Examples:"
  echo "  bash $SCRIPT_NAME --table-type COPY_ON_WRITE --target-hudi-version SOURCE_HUDI_VERSION"
  echo "  bash $SCRIPT_NAME --table-type MERGE_ON_READ --target-hudi-version TARGET_HUDI_VERSION"
  echo ""
  exit 1
}

TABLE_NAME_SUFFIX_ARG=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      if [[ -z "${2:-}" ]]; then
        log_error "❌ Error: --table-type requires a value"
        usage
      fi
      TABLE_TYPE_ARG="$2"
      shift 2
      ;;
    --target-hudi-version)
      if [[ -z "${2:-}" ]]; then
        log_error "❌ Error: --target-hudi-version requires a value"
        usage
      fi
      TARGET_HUDI_VERSION="$2"
      shift 2
    ;;
    --batch-id)
      if [[ -z "${2:-}" ]]; then
        log_error "❌ Error: --batch-id requires a value"
        usage
      fi
      BATCH_ID_ARG="$2"
      shift 2
      ;;
    --table-name-suffix)
      if [[ -z "${2:-}" ]]; then
        log_error "❌ Error: --table-name-suffix requires a value"
        usage
      fi
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

if [[ -z "${TABLE_TYPE_ARG:-}" ]]; then
  log_error "❌ Error: --table-type is required"
  usage
fi

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE_ARG" | tr '[:lower:]' '[:upper:]')
HUDI_VERSION_SUFFIX=$(echo "$TARGET_HUDI_VERSION" | sed 's/-.*//' | cut -d. -f1,2 | tr '.' '_')
IS_LOGICAL_TIMESTAMP_ENABLED=${IS_LOGICAL_TIMESTAMP_ENABLED:-true}
BASE_TABLE_NAME=${BASE_TABLE_NAME:-hudi_regular}

case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW)
    TABLE_TYPE="COPY_ON_WRITE"
    TABLE_NAME="${BASE_TABLE_NAME}_cow_${HUDI_VERSION_SUFFIX}"
    ;;
  MERGE_ON_READ|MOR)
    TABLE_TYPE="MERGE_ON_READ"
    TABLE_NAME="${BASE_TABLE_NAME}_mor_${HUDI_VERSION_SUFFIX}"
    ;;
  *)
    log_error "❌ Invalid TABLE_TYPE: $TABLE_TYPE_ARG"
    log_error "Allowed values: COPY_ON_WRITE (cow) or MERGE_ON_READ (mor)"
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

BENCH_DATA_PATH="${DATA_PATH}/$TABLE_NAME"

export HUDI_SPARK_BUNDLE_JAR="${JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
if [ ! -f "$PY_SCRIPT" ]; then
  log_error "❌ Benchmark script not found: $PY_SCRIPT"
  exit 1
fi
if [ ! -f "$SPARK_DEFAULTS_CONF" ]; then
  log_error "❌ Spark defaults config not found: $SPARK_DEFAULTS_CONF"
  exit 1
fi
if [ ! -f "$HUDI_SPARK_BUNDLE_JAR" ]; then
  log_error "❌ Hudi Spark Bundle Jar not found: $HUDI_SPARK_BUNDLE_JAR"
  exit 1
fi

SPARK_SUBMIT_JARS="$HUDI_SPARK_BUNDLE_JAR"
[[ -n "${AWS_S3_JARS:-}" ]] && SPARK_SUBMIT_JARS="${SPARK_SUBMIT_JARS},${AWS_S3_JARS}"

log_equal
log_info "🚀 Starting Hudi Benchmark"
log_hyphen
log_info "Hudi Version      : $TARGET_HUDI_VERSION"
log_info "Table Type        : $TABLE_TYPE"
log_info "Data Path         : $BENCH_DATA_PATH"
log_info "Spark Home        : $SPARK_HOME"
log_info "Script Path       : $PY_SCRIPT"
log_equal

log_info "Executing spark-submit command: "
log_hyphen
log_info "${SPARK_HOME}"/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --deploy-mode client \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --jars "$SPARK_SUBMIT_JARS" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  "$PY_SCRIPT" \
  "$BENCH_DATA_PATH"
log_hyphen

if "${SPARK_HOME}"/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --deploy-mode client \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --jars "$SPARK_SUBMIT_JARS" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  "$PY_SCRIPT" \
  "$BENCH_DATA_PATH"
then
  log_success "✅ Benchmark job completed successfully"
else
  log_error "❌ Benchmark job failed"
  log_hyphen
  exit 1
fi
log_hyphen