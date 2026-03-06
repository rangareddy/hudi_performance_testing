#!/usr/bin/env bash
#
# Run Hudi read benchmark (distinct query) for COPY_ON_WRITE or MERGE_ON_READ tables.
# Use with 0.14.1 or 0.14.2 jars to compare read performance (set HUDI_VERSION in common.properties).
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
  echo "  bash $SCRIPT_NAME --table-type <COPY_ON_WRITE|MERGE_ON_READ> --target-hudi-version <0.14.1|0.14.2>"
  echo ""
  echo "Examples:"
  echo "  bash $SCRIPT_NAME --table-type COPY_ON_WRITE --target-hudi-version 0.14.1"
  echo "  bash $SCRIPT_NAME --table-type MERGE_ON_READ --target-hudi-version 0.14.2"
  echo ""
  exit 1
}

############################################
# Parse Arguments
############################################

TARGET_HUDI_VERSION="$HUDI_VERSION"
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
    --target-hudi-version)
      if [[ -z "${2:-}" ]]; then
        echo "❌ Error: --target-hudi-version requires a value"
        usage
      fi
      TARGET_HUDI_VERSION="$2"
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
# Validate Table Type and set DATA_PATH for benchmark
############################################

case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW)
    TABLE_TYPE="COPY_ON_WRITE"
    BENCH_DATA_PATH="${BASE_DATA_PATH}/hudi_cow_logical"
    ;;
  MERGE_ON_READ|MOR)
    TABLE_TYPE="MERGE_ON_READ"
    BENCH_DATA_PATH="${BASE_DATA_PATH}/hudi_mor_logical"
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
echo "Hudi Version  : $TARGET_HUDI_VERSION"
echo "Table Type    : $TABLE_TYPE"
echo "Data Path     : $BENCH_DATA_PATH"
echo "Spark Home    : $SPARK_HOME"
echo "Script Path   : $PY_SCRIPT"
echo "======================================"

HUDI_JARS_BENCH=${HUDI_JARS}
if [ "$TARGET_HUDI_VERSION" != "$HUDI_VERSION" ]; then 
  HUDI_UTILITIES_BENCH_JAR="${JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
  HUDI_SPARK_BENCH_JAR="${JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
  HUDI_JARS_BENCH="${HUDI_SPARK_BENCH_JAR},${HUDI_UTILITIES_BENCH_JAR}"
fi

echo "Hudi Benchmark Jars: $HUDI_JARS_BENCH"

############################################
# Run Spark Job
############################################

"$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --jars "$HUDI_JARS_BENCH" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  --conf "spark.sql.adaptive.enabled=true" \
  "$PY_SCRIPT" \
  "$BENCH_DATA_PATH"

echo ""
echo "✅ Benchmark job submitted successfully"
