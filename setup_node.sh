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
echo "BASE_PATH         : ${BASE_PATH}"
echo "JARS_PATH         : ${JARS_PATH}"
echo "DEST_SCRIPTS_DIR  : ${DEST_SCRIPTS_DIR}"
echo "HUDI_VERSION      : ${HUDI_VERSION}"
echo "SPARK_VERSION     : ${SPARK_VERSION}"
echo "DEST_DIR          : ${DEST_DIR}"
echo "======================================"

echo "Downloading Hudi 0.14.1 jars..."
aws s3 cp "${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-0.14.1.jar" .
aws s3 cp "${JARS_PATH}/hudi-utilities-slim-bundle_2.12-0.14.1.jar" .

echo "Downloading Hudi ${HUDI_VERSION} jars..."
aws s3 cp "${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar" .
aws s3 cp "${JARS_PATH}/hudi-utilities-slim-bundle_2.12-${HUDI_VERSION}.jar" .

echo "✅ Node setup complete. Jars in $DEST_DIR, scripts in $DEST_SCRIPTS_DIR"
