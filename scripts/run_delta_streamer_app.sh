#!/usr/bin/env bash
#
# Run HoodieDeltaStreamer for ingestion (initial or incremental).
# Use with 0.14.1 or 0.14.2 jars by setting HUDI_VERSION.
#
set -euo pipefail

BASE_PATH="${BASE_PATH:-s3://performance-benchmark-datasets-us-west-2/hudi-bench/logical_ts_perf}"
JARS_PATH="${JARS_PATH:-${BASE_PATH}/jars}"
HUDI_VERSION="${HUDI_VERSION:-0.14.2-SNAPSHOT}"
SPARK_VERSION="${SPARK_VERSION:-3.4}"

SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
HUDI_JARS="${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar,${JARS_PATH}/hudi-utilities-slim-bundle_2.12-${HUDI_VERSION}.jar"
PROPS_FILE="${PROPS_FILE:-file:///home/hadoop/empty.properties}"

TABLE_TYPE="${TABLE_TYPE:-MERGE_ON_READ}"
TABLE_NAME="${TABLE_NAME:-hudi_mor_logical}"
TABLE_BASE_PATH="${TABLE_BASE_PATH:-${BASE_PATH}/data/hudi_mor_logical}"
SOURCE_DFS_ROOT="${SOURCE_DFS_ROOT:-${BASE_PATH}/data/wide_500cols_10000parts}"
SCHEMA_FILE="${SCHEMA_FILE:-s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc}"

echo "======================================"
echo "Running Delta Streamer"
echo "--------------------------------------"
echo "HUDI_VERSION   : $HUDI_VERSION"
echo "TABLE_TYPE     : $TABLE_TYPE"
echo "TABLE_BASE_PATH: $TABLE_BASE_PATH"
echo "SOURCE_DFS_ROOT: $SOURCE_DFS_ROOT"
echo "======================================"

time "$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
  --driver-memory 4g \
  --executor-memory 6g \
  --executor-cores 3 \
  --num-executors 3 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs:///var/log/spark/apps \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --jars "$HUDI_JARS" \
  --props "$PROPS_FILE" \
  --table-type "$TABLE_TYPE" \
  --op UPSERT \
  --target-base-path "$TABLE_BASE_PATH" \
  --target-table "$TABLE_NAME" \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field col_1 \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root="${SOURCE_DFS_ROOT}" \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file="$SCHEMA_FILE" \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file="$SCHEMA_FILE" \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col

echo "✅ Delta Streamer run completed"
