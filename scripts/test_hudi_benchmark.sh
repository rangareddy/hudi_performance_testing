#!/usr/bin/env bash
#
# Run Hudi read benchmark (distinct query) for COPY_ON_WRITE or MERGE_ON_READ tables.
# Use with 0.14.1 or 0.14.2 jars to compare read performance.
#
set -euo pipefail

############################################
# Configuration
############################################

SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
# Override HUDI_JARS to point to local jars, e.g. 0.14.1 vs 0.14.2
HUDI_JARS="${HUDI_JARS:-/home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/home/hadoop/hudi-jars/hudi-utilities-slim-spark3.5-bundle_2.12-0.15.0.jar}"
PY_SCRIPT="${PY_SCRIPT:-s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/hudi_benchmark.py}"
BASE_DATA_PATH="${BASE_DATA_PATH:-s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data}"

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
      TABLE_TYPE="$2"
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

if [[ -z "${TABLE_TYPE:-}" ]]; then
  echo "❌ Error: --table-type is required"
  usage
fi

echo "Table Type: $TABLE_TYPE"

############################################
# Normalize Table Type
############################################

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')

############################################
# Validate Table Type
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
    echo "❌ Invalid TABLE_TYPE: $TABLE_TYPE"
    echo "Allowed values: COPY_ON_WRITE (cow) or MERGE_ON_READ (mor)"
    exit 1
    ;;
esac

echo "Data Path: $DATA_PATH"

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
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=9g" \
  --conf "spark.executor.cores=3" \
  --conf "spark.executor.instances=3" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=hdfs:///var/log/spark/apps" \
  --conf "spark.sql.shuffle.partitions=200" \
  --conf "spark.default.parallelism=9" \
  --conf "spark.sql.adaptive.enabled=true" \
  "$PY_SCRIPT" \
  "$DATA_PATH"

echo ""
echo "✅ Benchmark job submitted successfully"
