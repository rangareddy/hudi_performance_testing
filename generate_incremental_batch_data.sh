#!/usr/bin/env bash
#
# Generate incremental batch data using incremental_batch_1.py.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

echo "======================================"
echo "Generating incremental batch data"
echo "--------------------------------------"
echo "SPARK_HOME : $SPARK_HOME"
echo "Script     : $INCREMENTAL_SCRIPT"
echo "======================================"

"${SPARK_HOME}/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY}" \
  --conf spark.executor.cores="${SPARK_EXECUTOR_CORES}" \
  --conf spark.executor.instances="${SPARK_EXECUTOR_INSTANCES}" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="${SPARK_EVENT_LOG_DIR}" \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS}" \
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM}" \
  --conf spark.sql.adaptive.enabled=true \
  "$INCREMENTAL_SCRIPT"

echo "✅ Incremental batch data generation completed"
