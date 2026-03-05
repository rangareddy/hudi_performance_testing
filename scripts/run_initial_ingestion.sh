#!/usr/bin/env bash
#
# Run initial ingestion using spark-shell and initial_batch.scala.
# Dataset: 500 cols, 10 of which are timestamp columns.
#
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
INITIAL_BATCH_SCALA="${INITIAL_BATCH_SCALA:-/home/hadoop/initial_batch.scala}"

echo "======================================"
echo "Running initial ingestion"
echo "--------------------------------------"
echo "SPARK_HOME : $SPARK_HOME"
echo "Script     : $INITIAL_BATCH_SCALA"
echo "======================================"

"${SPARK_HOME}/bin/spark-shell" \
  --master yarn \
  --deploy-mode client \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=7g \
  --conf spark.executor.cores=3 \
  --conf spark.executor.instances=3 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=9 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  -i "$INITIAL_BATCH_SCALA"

echo "✅ Initial ingestion completed"
