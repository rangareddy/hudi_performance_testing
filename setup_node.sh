#!/usr/bin/env bash
#
# Node setup: download Hudi jars and data generation scripts from S3.
# Run this on the cluster node (e.g. EMR master) before running ingestion/benchmarks.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Skip Spark home check (Spark not installed yet)
export SKIP_SPARK_HOME_CHECK=1
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

echo "======================================"
echo "Hudi performance testing node setup"
echo "--------------------------------------"
echo "SOURCE_HUDI_VERSION  : ${SOURCE_HUDI_VERSION}"
echo "TARGET_HUDI_VERSION  : ${TARGET_HUDI_VERSION}"
echo "SPARK_VERSION        : ${SPARK_VERSION}"
echo "BASE_PATH            : ${BASE_PATH}"
echo "JARS_PATH            : ${JARS_PATH}"
echo "DATA_PATH            : ${DATA_PATH}"
echo "SCRIPTS_DIR          : ${SCRIPTS_DIR}"
echo "======================================"

if [[ ! -d "$SPARK_HOME" ]]; then
  echo "Installing Spark $SPARK_VERSION"
  SPARK_TAR_FILE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
  SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR_FILE}"
  if [[ ! -f "$HOME/${SPARK_TAR_FILE}" ]]; then
    wget --quiet -O "$HOME/${SPARK_TAR_FILE}" "$SPARK_URL"
  fi
  tar -xzf "$HOME/${SPARK_TAR_FILE}" -C "$HOME"
  rm -f "$HOME/${SPARK_TAR_FILE}"
  SPARK_HOME="$HOME/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
  ln -sf "$SPARK_HOME" "$HOME/spark"
  echo "Spark successfully installed at $SPARK_HOME"
  wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O "$SPARK_HOME/jars/hadoop-aws-3.3.4.jar"
  ln -sf "$SPARK_HOME/jars/hadoop-aws-3.3.4.jar" "$SPARK_HOME/jars/hadoop-aws.jar"
  wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar"
  ln -sf "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar" "$SPARK_HOME/jars/aws-java-sdk-bundle.jar"
fi

echo "✅ Node setup complete."
