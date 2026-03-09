#!/usr/bin/env bash
#
# Run HoodieDeltaStreamer for ingestion (initial or incremental).
# Use with 0.14.1 or 0.14.2 jars by setting HUDI_VERSION in common.properties or env.
#
set -euo pipefail

SCRIPT_NAME="$0"  
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_NAME}")" && pwd)"

# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

usage() {
  log_info ""
  log_info "Usage:"
  log_info "  bash $SCRIPT_NAME --table-type <COPY_ON_WRITE|MERGE_ON_READ> --target-hudi-version <0.14.1|0.14.2> [--batch-id <id>]"
  log_info ""
  log_info "Options:"
  log_info "  --batch-id    (optional) Ingest only parquet under SOURCE_DATA/batch_<id>; if omitted, use SOURCE_DATA as root."
  log_info ""
  log_info "Examples:"
  log_info "  bash $SCRIPT_NAME --table-type COPY_ON_WRITE --target-hudi-version 0.14.1"
  log_info "  bash $SCRIPT_NAME --table-type MERGE_ON_READ --target-hudi-version 0.14.2 --batch-id 1"
  log_info ""
  exit 1
}

# Schema file: use file:// if local path
SCHEMA_FILE_ARG="$SCHEMA_FILE"
if [[ -n "$SCHEMA_FILE_ARG" && "$SCHEMA_FILE_ARG" != file://* && "$SCHEMA_FILE_ARG" != s3:* ]]; then
  SCHEMA_FILE_ARG="file://${SCHEMA_FILE}"
fi

TARGET_HUDI_VERSION="$HUDI_VERSION"
BATCH_ID_ARG=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      if [[ -z "$2" ]]; then
        log_error "❌ Error: --table-type requires a value"
        usage
      fi
      TABLE_TYPE="$2"
      shift 2
      ;;
    --target-hudi-version)
      if [[ -z "$2" ]]; then
        log_error "❌ Error: --target-hudi-version requires a value"
        usage
      fi
      TARGET_HUDI_VERSION="$2"
      shift 2
      ;;
    --batch-id)
      if [[ -z "$2" ]]; then
        log_error "❌ Error: --batch-id requires a value"
        usage
      fi
      BATCH_ID_ARG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      log_error "❌ Unknown option: $1"
      usage
      ;;
  esac
done

log_success "✅ Table Type: $TABLE_TYPE"

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')

# Append Hudi version to table name (e.g. 0.14.1 -> 0_14)
HUDI_VERSION_SUFFIX=$(echo "$TARGET_HUDI_VERSION" | sed 's/-.*//' | cut -d. -f1,2 | tr '.' '_')

case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW)
    TABLE_TYPE="COPY_ON_WRITE"
    TABLE_NAME="${BASE_TABLE_NAME}_cow_${HUDI_VERSION_SUFFIX}"
    ;;
  MERGE_ON_READ|MOR)
    TABLE_TYPE="MERGE_ON_READ"
    TABLE_NAME="${BASE_TABLE_NAME}_mor_${HUDI_VERSION_SUFFIX}"
    ;;
  *)
    log_error "❌ Invalid TABLE_TYPE: $TABLE_TYPE"
    log_error "Allowed values: COPY_ON_WRITE (cow) or MERGE_ON_READ (mor)"
    exit 1
    ;;
esac

TABLE_BASE_PATH="${DATA_PATH}/${TABLE_NAME}"

if [[ -n "$BATCH_ID_ARG" ]]; then
  STREAMER_SOURCE_ROOT="${SOURCE_DATA}/batch_${BATCH_ID_ARG}"
else
  STREAMER_SOURCE_ROOT="${SOURCE_DATA}"
fi

HUDI_UTILITIES_JAR="${JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
HUDI_SPARK_JAR="${JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${TARGET_HUDI_VERSION}.jar"
if [ ! -f "$HUDI_UTILITIES_JAR" ]; then
  log_error "❌ Hudi Utilities Slim Bundle Jar not found: $HUDI_UTILITIES_JAR"
  exit 1
fi
if [ ! -f "$HUDI_SPARK_JAR" ]; then
  log_error "❌ Hudi Spark Bundle Jar not found: $HUDI_SPARK_JAR"
  exit 1
fi
HUDI_JARS="${HUDI_SPARK_JAR},${HUDI_UTILITIES_JAR}" 

log_info "============================================================================="
log_info "Running Delta Streamer"
log_info "-----------------------------------------------------------------------------"
log_info "HUDI_VERSION    : $TARGET_HUDI_VERSION"
log_info "TABLE_TYPE      : $TABLE_TYPE"
log_info "TABLE_NAME      : $TABLE_NAME"
log_info "TABLE_BASE_PATH : $TABLE_BASE_PATH"
log_info "SOURCE_DATA     : $SOURCE_DATA"
log_info "Streamer root   : $STREAMER_SOURCE_ROOT"
log_info "HUDI_JARS       : $HUDI_JARS"
log_info "============================================================================="

log_info "Executing spark-submit command: "
log_info "------------------------------------------------------------------------------"
log_info "spark-submit command: $SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars "$HUDI_JARS" \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  "$HUDI_UTILITIES_JAR" \
  --props "$PROPS_FILE" \
  --table-type "$TABLE_TYPE" \
  --op UPSERT \
  --target-base-path "$TABLE_BASE_PATH" \
  --target-table "$TABLE_NAME" \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field col_1 \
  --hoodie-conf hoodie.streamer.source.dfs.root="${STREAMER_SOURCE_ROOT}" \
  --hoodie-conf hoodie.streamer.schemaprovider.source.schema.file="$SCHEMA_FILE_ARG" \
  --hoodie-conf hoodie.streamer.schemaprovider.target.schema.file="$SCHEMA_FILE_ARG" \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col"  
log_info "------------------------------------------------------------------------------"

time "${SPARK_HOME}/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --jars "$HUDI_JARS" \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.fs.s3a.committer.name=directory \
  --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
  --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  "$HUDI_UTILITIES_JAR" \
  --props "$PROPS_FILE" \
  --table-type "$TABLE_TYPE" \
  --op UPSERT \
  --target-base-path "$TABLE_BASE_PATH" \
  --target-table "$TABLE_NAME" \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --source-ordering-field col_1 \
  --hoodie-conf hoodie.streamer.source.dfs.root="${STREAMER_SOURCE_ROOT}" \
  --hoodie-conf hoodie.streamer.schemaprovider.source.schema.file="$SCHEMA_FILE_ARG" \
  --hoodie-conf hoodie.streamer.schemaprovider.target.schema.file="$SCHEMA_FILE_ARG" \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col

if [ $? -eq 0 ]; then
  log_success "✅ Hudi Ingestion job completed successfully"
else
  log_error "❌ Hudi Ingestion job failed"
  log_info "------------------------------------------------------------------------------"
  exit 1
fi
log_info "------------------------------------------------------------------------------"
