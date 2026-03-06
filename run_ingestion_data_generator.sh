#!/usr/bin/env bash
#
# Run initial ingestion using spark-shell and initial_batch.scala.
# Dataset: 500 cols, 10 of which are timestamp columns.
#
set -euo pipefail

SCRIPT_NAME="$0"  
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_NAME}")" && pwd)"

# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

usage() {
  echo ""
  echo "Usage:"
  echo "  bash $SCRIPT_NAME --type <initial|incremental>"
  echo ""
  echo "Examples:"
  echo "  bash $SCRIPT_NAME --type initial"
  echo "  bash $SCRIPT_NAME --type incremental"
  echo ""
  exit 1
}

INGESTION_TYPE="initial"
while [[ $# -gt 0 ]]; do
  case $1 in
    --type)
      if [[ -z "$2" ]]; then
        echo "❌ Error: --type requires a value"
        usage
      fi
      INGESTION_TYPE="$2"
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

if [[ "$INGESTION_TYPE" != "initial" ]] && [[ "$INGESTION_TYPE" != "incremental" ]]; then
  echo "❌ Invalid INGESTION_TYPE: $INGESTION_TYPE"
  echo "Allowed values: initial or incremental"
  exit 1
fi

export SOURCE_DATA

echo "======================================"
echo "Running $INGESTION_TYPE ingestion"
echo "--------------------------------------"
echo "SPARK_HOME        : $SPARK_HOME"
echo "Script            : $INITIAL_BATCH_SCALA"
echo "SOURCE_DATA       : $SOURCE_DATA"
echo "INGESTION_TYPE    : $INGESTION_TYPE"
echo "======================================"

if [[ "$INGESTION_TYPE" == "initial" ]]; then
  echo "Running initial ingestion"
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
else
  echo "Running incremental ingestion"
  "${SPARK_HOME}/bin/spark-submit" \
    --master yarn \
    --deploy-mode client \
    --jars $SPARK_HOME/jars/aws-java-sdk-bundle.jar,$SPARK_HOME/jars/hadoop-aws.jar \
    --properties-file "${SPARK_DEFAULTS_CONF}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.s3a.committer.name=directory \
    "$INCREMENTAL_SCRIPT"
    echo "✅ Incremental ingestion completed"
fi

echo "$SCRIPT_NAME completed successfully."