#!/usr/bin/env bash
#
# Node setup: download Hudi jars and data generation scripts from S3.
# Run this on the cluster node (e.g. EMR master) before running ingestion/benchmarks.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

echo "======================================"
echo "Hudi logical_ts_perf node setup"
echo "--------------------------------------"
echo "BASE_PATH    : ${BASE_PATH}"
echo "JARS_PATH    : ${JARS_PATH}"
echo "HUDI_VERSION : ${HUDI_VERSION}"
echo "SPARK_VERSION: ${SPARK_VERSION}"
echo "DEST_DIR     : ${DEST_DIR}"
echo "======================================"

mkdir -p "$DEST_DIR"
cd "$DEST_DIR"

echo "Downloading Hudi 0.14.1 jars..."
aws s3 cp "${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-0.14.1.jar" .
aws s3 cp "${JARS_PATH}/hudi-utilities-slim-bundle_2.12-0.14.1.jar" .

echo "Downloading Hudi ${HUDI_VERSION} jars..."
aws s3 cp "${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar" .
aws s3 cp "${JARS_PATH}/hudi-utilities-slim-bundle_2.12-${HUDI_VERSION}.jar" .

if [[ ! -f "$DEST_DIR/initial_batch.scala" ]]; then
  echo "Downloading initial batch script..."
  aws s3 cp "${PAVIJARS_S3}/initial_batch.scala" "$DEST_DIR/"
fi

if [[ ! -f "$DEST_DIR/incremental_batch_1.py" ]]; then
  echo "Downloading incremental batch 1 script..."
  aws s3 cp "${PAVIJARS_S3}/incremental_batch_1.py" "$DEST_DIR/"
fi

echo "Creating empty.properties for Delta Streamer..."
touch "$DEST_DIR/empty.properties"

echo "✅ Node setup complete. Jars and scripts are in $DEST_DIR"
