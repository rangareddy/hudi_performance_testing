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

if [ -z "${SOURCE_DATA:-}" ]; then
  echo "❌ SOURCE_DATA not found in s3"
  exit 1
fi

BATCH_ID_FILE="${SCRIPT_DIR}/incremental_batch_id.txt"
if [[ -f "$BATCH_ID_FILE" ]]; then
  BATCH_ID=$(cat "$BATCH_ID_FILE" | tr -d '[:space:]')
  [[ -z "$BATCH_ID" || ! "$BATCH_ID" =~ ^[0-9]+$ ]] && BATCH_ID=1
else
  BATCH_ID=0
fi
export BATCH_ID

export TARGET_DATA="${SOURCE_DATA}/batch_${BATCH_ID}"
aws s3 ls $TARGET_DATA > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "Already loaded data of $TARGET_DATA exists in s3 for ingestion type: $INGESTION_TYPE"
  echo "Skipping ingestion"
  exit 0
fi
EXECTION_STATUS=0

echo "Incremental batch ID : $BATCH_ID"
echo "Target data : $TARGET_DATA"

if [[ "$INGESTION_TYPE" == "initial" ]]; then
  echo "Running initial ingestion"
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
  echo "Running incremental ingestion"
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
  NEXT_BATCH_ID=$((BATCH_ID + 1))
  echo "$NEXT_BATCH_ID" > "$BATCH_ID_FILE"
  echo "✅ Ingestion of $INGESTION_TYPE completed. Next batch ID will be: $NEXT_BATCH_ID"
else
  echo "❌ Ingestion of $INGESTION_TYPE failed. Batch ID $BATCH_ID not updated."
  exit 1
fi