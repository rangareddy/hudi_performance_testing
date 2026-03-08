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
  echo "  bash $SCRIPT_NAME --type <initial|incremental> --batch-id <id>"
  echo ""
  echo "Options:"
  echo "  --type        initial | incremental"
  echo "  --batch-id    Batch ID (required, non-negative integer)"
  echo ""
  echo "Examples:"
  echo "  bash $SCRIPT_NAME --type initial --batch-id 0"
  echo "  bash $SCRIPT_NAME --type incremental --batch-id 1"
  echo ""
  exit 1
}

INGESTION_TYPE="initial"
BATCH_ID=""
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
    --batch-id)
      if [[ -z "$2" ]]; then
        echo "❌ Error: --batch-id requires a value"
        usage
      fi
      BATCH_ID="$2"
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

if [[ -z "$BATCH_ID" ]]; then
  echo "❌ Error: --batch-id is required"
  usage
fi
if [[ ! "$BATCH_ID" =~ ^[0-9]+$ ]]; then
  echo "❌ Error: --batch-id must be a non-negative integer: $BATCH_ID"
  exit 1
fi

if [[ "$INGESTION_TYPE" != "initial" ]] && [[ "$INGESTION_TYPE" != "incremental" ]]; then
  echo "❌ Invalid INGESTION_TYPE: $INGESTION_TYPE"
  echo "Allowed values: initial or incremental"
  exit 1
fi

# Convert ingestion type to title case means first letter is upper case and remainig lower case
INGESTION_TYPE_TITLE=$(echo "$INGESTION_TYPE" | sed 's/.*/\u&/')
export SOURCE_DATA

echo "======================================"
echo "Running $INGESTION_TYPE_TITLE ingestion"
echo "--------------------------------------"
echo "SPARK_HOME        : $SPARK_HOME"
echo "Script            : $INITIAL_BATCH_SCALA"
echo "SOURCE_DATA       : $SOURCE_DATA"
echo "INGESTION_TYPE    : $INGESTION_TYPE_TITLE"
echo "======================================"

if [ -z "${SOURCE_DATA:-}" ]; then
  echo "❌ SOURCE_DATA not found in s3"
  exit 1
fi

export BATCH_ID
export TARGET_DATA="${SOURCE_DATA}/batch_${BATCH_ID}"
if aws s3 ls "$TARGET_DATA" > /dev/null 2>&1; then
  echo "Already loaded data of $TARGET_DATA exists in s3 for ingestion type: $INGESTION_TYPE_TITLE"
  echo "Skipping ingestion"
  exit 0
fi

EXECTION_STATUS=0

echo "Batch ID : $BATCH_ID"
echo "Target data : $TARGET_DATA"

export NUM_OF_COLUMNS=${NUM_OF_COLUMNS:-500}
export NUM_OF_PARTITIONS=${NUM_OF_PARTITIONS:-10000}

if [[ "$INGESTION_TYPE" == "initial" ]]; then
  echo "Running $INGESTION_TYPE_TITLE ingestion with $NUM_OF_COLUMNS columns and $NUM_OF_PARTITIONS partitions"
  "${SPARK_HOME}/bin/spark-shell" \
    --master yarn \
    --deploy-mode client \
    --jars $AWS_S3_JARS \
    --properties-file "${SPARK_DEFAULTS_CONF}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.s3a.committer.name=directory \
    -i "$INITIAL_BATCH_SCALA"
    EXECTION_STATUS=$?
else
  export NUM_OF_RECORDS_TO_UPDATE=${NUM_OF_RECORDS_TO_UPDATE:-100}
  echo "Running $INGESTION_TYPE_TITLE ingestion with $NUM_OF_RECORDS_TO_UPDATE records to update"
  export SOURCE_DATA="${SOURCE_DATA}/batch_0"
  "${SPARK_HOME}/bin/spark-submit" \
    --master yarn \
    --deploy-mode client \
    --jars $AWS_S3_JARS \
    --properties-file "${SPARK_DEFAULTS_CONF}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.s3a.committer.name=directory \
    "$INCREMENTAL_SCRIPT"
    EXECTION_STATUS=$?
fi

if [ $EXECTION_STATUS -eq 0 ]; then
  echo "✅ Ingestion of $INGESTION_TYPE_TITLE completed (batch-id $BATCH_ID)."
else
  echo "❌ Ingestion of $INGESTION_TYPE_TITLE failed. Batch ID $BATCH_ID."
  exit 1
fi