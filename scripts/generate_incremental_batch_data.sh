#!/usr/bin/env bash
#
# Generate incremental batch data using incremental_batch_1.py.
#
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
INCREMENTAL_SCRIPT="${INCREMENTAL_SCRIPT:-/home/hadoop/incremental_batch_1.py}"

echo "======================================"
echo "Generating incremental batch data"
echo "--------------------------------------"
echo "SPARK_HOME : $SPARK_HOME"
echo "Script     : $INCREMENTAL_SCRIPT"
echo "======================================"

"${SPARK_HOME}/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=9g \
  --conf spark.executor.cores=3 \
  --conf spark.executor.instances=3 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs:///var/log/spark/apps \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=9 \
  --conf spark.sql.adaptive.enabled=true \
  "$INCREMENTAL_SCRIPT"

echo "✅ Incremental batch data generation completed"
