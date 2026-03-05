#!/usr/bin/env bash
#
# Run Hudi read benchmark (distinct query) for COPY_ON_WRITE or MERGE_ON_READ tables.
# Use with 0.14.1 or 0.14.2 jars to compare read performance.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

############################################
# Usage
############################################

SCRIPT_NAME="$0"

usage() {
  echo ""
  echo "Usage:"
  echo "  bash $SCRIPT_NAME --table-type <COPY_ON_WRITE|MERGE_ON_READ>"
  echo ""
  echo "Examples:"
  echo "  bash $SCRIPT_NAME --table-type COPY_ON_WRITE"
  echo "  bash $SCRIPT_NAME --table-type MERGE_ON_READ"
  echo ""
  exit 1
}

############################################
# Parse Arguments
############################################

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      if [[ -z "${2:-}" ]]; then
        echo "❌ Error: --table-type requires a value"
        usage
      fi
      TABLE_TYPE_ARG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "❌ Unknown option: $1"
      usage
      ;;
  esac
done

if [[ -z "${TABLE_TYPE_ARG:-}" ]]; then
  echo "❌ Error: --table-type is required"
  usage
fi

############################################
# Normalize Table Type
############################################

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE_ARG" | tr '[:lower:]' '[:upper:]')

############################################
# Validate Table Type and set DATA_PATH
############################################

case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW)
    TABLE_TYPE="COPY_ON_WRITE"
    DATA_PATH="${BASE_DATA_PATH}/hudi_cow_logical"
    ;;
  MERGE_ON_READ|MOR)
    TABLE_TYPE="MERGE_ON_READ"
    DATA_PATH="${BASE_DATA_PATH}/hudi_mor_logical"
    ;;
  *)
    echo "❌ Invalid TABLE_TYPE: $TABLE_TYPE_ARG"
    echo "Allowed values: COPY_ON_WRITE (cow) or MERGE_ON_READ (mor)"
    exit 1
    ;;
esac

############################################
# Print Configuration
############################################

echo "======================================"
echo "🚀 Starting Hudi Benchmark"
echo "--------------------------------------"
echo "Table Type : $TABLE_TYPE"
echo "Data Path  : $DATA_PATH"
echo "Spark Home : $SPARK_HOME"
echo "Script     : $PY_SCRIPT"
echo "======================================"

############################################
# Run Spark Job
############################################

"$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --jars "$HUDI_JARS" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES}" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=${SPARK_EVENT_LOG_DIR}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}" \
  --conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}" \
  --conf "spark.sql.adaptive.enabled=true" \
  "$PY_SCRIPT" \
  "$DATA_PATH"

echo ""
echo "✅ Benchmark job submitted successfully"
