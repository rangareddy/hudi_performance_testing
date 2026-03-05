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

if [[ ! -d "$SPARK_HOME" ]]; then
  SPARK_TAR_FILE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
  SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR_FILE}"
  if [[ ! -f "$HOME/${SPARK_TAR_FILE}" ]]; then
    wget -O "$HOME/${SPARK_TAR_FILE}" "$SPARK_URL"
  fi
  tar -xzf "$HOME/${SPARK_TAR_FILE}" -C "$HOME"
  rm -f "$HOME/${SPARK_TAR_FILE}"
fi

echo "✅ Node setup complete. Jars in $DEST_DIR, scripts in $DEST_SCRIPTS_DIR, Spark in $SPARK_HOME"
