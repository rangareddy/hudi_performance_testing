#!/usr/bin/env bash
#
# Run initial ingestion using spark-shell and initial_batch.scala.
# Dataset: 500 cols, 10 of which are timestamp columns.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

# So initial_batch.scala can read SOURCE_DFS_ROOT from env
export SOURCE_DFS_ROOT

echo "======================================"
echo "Running initial ingestion"
echo "--------------------------------------"
echo "SPARK_HOME       : $SPARK_HOME"
echo "Script           : $INITIAL_BATCH_SCALA"
echo "SOURCE_DFS_ROOT  : $SOURCE_DFS_ROOT"
echo "======================================"

"${SPARK_HOME}/bin/spark-shell" \
  --master yarn \
  --deploy-mode client \
  --jars $SPARK_HOME/jars/aws-java-sdk-bundle.jar,$SPARK_HOME/jars/hadoop-aws.jar \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  -i "$INITIAL_BATCH_SCALA"

echo "✅ Initial ingestion completed"
