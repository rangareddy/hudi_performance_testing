#!/usr/bin/env bash
#
# Run HoodieDeltaStreamer for ingestion (initial or incremental).
# Use with 0.14.1 or 0.14.2 jars by setting HUDI_VERSION in common.properties or env.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

HUDI_JARS_DELTA="${JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar,${JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"

# Schema file: use file:// if local path
SCHEMA_FILE_ARG="$SCHEMA_FILE"
if [[ -n "$SCHEMA_FILE_ARG" && "$SCHEMA_FILE_ARG" != file://* && "$SCHEMA_FILE_ARG" != s3:* ]]; then
  SCHEMA_FILE_ARG="file://${SCHEMA_FILE}"
fi

echo "======================================"
echo "Running Delta Streamer"
echo "--------------------------------------"
echo "HUDI_VERSION    : $HUDI_VERSION"
echo "TABLE_TYPE      : $TABLE_TYPE"
echo "TABLE_BASE_PATH : $TABLE_BASE_PATH"
echo "SOURCE_DFS_ROOT : $SOURCE_DFS_ROOT"
echo "======================================"

time "$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --jars "$HUDI_JARS_DELTA" \
  --props "$PROPS_FILE" \
  --table-type "$TABLE_TYPE" \
  --op UPSERT \
  --target-base-path "$TABLE_BASE_PATH" \
  --target-table "$TABLE_NAME" \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field col_1 \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root="${SOURCE_DFS_ROOT}" \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file="$SCHEMA_FILE_ARG" \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file="$SCHEMA_FILE_ARG" \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col

echo "✅ Delta Streamer run completed"
