#!/usr/bin/env bash
#
# Run initial ingestion using spark-shell and initial_batch.scala.
# Dataset: 500 cols, 10 of which are timestamp columns.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

echo "======================================"
echo "Running initial ingestion"
echo "--------------------------------------"
echo "SPARK_HOME : $SPARK_HOME"
echo "Script     : $INITIAL_BATCH_SCALA"
echo "======================================"

"${SPARK_HOME}/bin/spark-shell" \
  --master yarn \
  --deploy-mode client \
  --conf spark.driver.memory="${SPARK_INITIAL_DRIVER_MEMORY}" \
  --conf spark.executor.memory="${SPARK_INITIAL_EXECUTOR_MEMORY}" \
  --conf spark.executor.cores="${SPARK_EXECUTOR_CORES}" \
  --conf spark.executor.instances="${SPARK_EXECUTOR_INSTANCES}" \
  --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTITIONS}" \
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM}" \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  -i "$INITIAL_BATCH_SCALA"

echo "✅ Initial ingestion completed"
