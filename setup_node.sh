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
export SPARK_MAJOR_VERSION=$(echo "${SPARK_VERSION}" | cut -d '.' -f 1,2)
export HADOOP_MAJOR_VERSION=$(echo "${HADOOP_VERSION}" | cut -d '.' -f 1,2)

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

download_hudi_jars() {
  local hudi_version="$1"
  if [[ ! -f "$JARS_PATH/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${hudi_version}.jar" ]]; then
    wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${hudi_version}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${hudi_version}.jar \
      -O "$JARS_PATH/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${hudi_version}.jar"
    echo "Hudi Spark Bundle jar downloaded for version $hudi_version successfully"
  fi
  if [[ ! -f "$JARS_PATH/hudi-utilities-slim-bundle_${SCALA_VERSION}-${hudi_version}.jar" ]]; then
    wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-slim-bundle_${SCALA_VERSION}-${hudi_version}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${hudi_version}.jar \
      -O "$JARS_PATH/hudi-utilities-slim-bundle_${SCALA_VERSION}-${hudi_version}.jar"
    echo "Hudi Utilities Slim Bundle jar downloaded for version $hudi_version successfully"
  fi
}

setup_spark() {
  
  if [[ ! -d "$SPARK_HOME" ]]; then
    EXISTING_SPARK_VERSION=$(spark-submit --version 2>&1 | awk '/version/ {split($NF,a,"."); print a[1]"."a[2]}' | head -1)
    if [[ "$EXISTING_SPARK_VERSION" == "$SPARK_MAJOR_VERSION" ]]; then
      echo "Spark $EXISTING_SPARK_VERSION is already installed"
      return 0
    fi

    echo "Installing Spark $SPARK_VERSION"
    SPARK_TAR_FILE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz"
    SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR_FILE}"
    if [[ ! -f "$HOME/${SPARK_TAR_FILE}" ]]; then
      wget --quiet -O "$HOME/${SPARK_TAR_FILE}" "$SPARK_URL"
    fi
    tar -xzf "$HOME/${SPARK_TAR_FILE}" -C "$HOME"
    rm -f "$HOME/${SPARK_TAR_FILE}"
    
    SPARK_HOME="$HOME/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}"
    echo "Spark successfully installed at $SPARK_HOME"

    if [[ ! -f "$SPARK_HOME/jars/hadoop-aws.jar" ]]; then 
      echo "Installing Hadoop AWS $HADOOP_MAJOR_VERSION"
      wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_MAJOR_VERSION/hadoop-aws-$HADOOP_MAJOR_VERSION.jar \
        -O "$SPARK_HOME/jars/hadoop-aws-$HADOOP_MAJOR_VERSION.jar"
      ln -sf "$SPARK_HOME/jars/hadoop-aws-$HADOOP_MAJOR_VERSION.jar" "$SPARK_HOME/jars/hadoop-aws.jar"
      echo "Hadoop AWS $HADOOP_MAJOR_VERSION installed successfully"
    fi
    if [[ ! -f "$SPARK_HOME/jars/aws-java-sdk-bundle.jar" ]]; then
      echo "Installing AWS Java SDK Bundle $AWS_JAVA_SDK_BUNDLE_VERSION"
      wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_JAVA_SDK_BUNDLE_VERSION/aws-java-sdk-bundle-$AWS_JAVA_SDK_BUNDLE_VERSION.jar \
        -O "$SPARK_HOME/jars/aws-java-sdk-bundle-$AWS_JAVA_SDK_BUNDLE_VERSION.jar"
      ln -sf "$SPARK_HOME/jars/aws-java-sdk-bundle-$AWS_JAVA_SDK_BUNDLE_VERSION.jar" "$SPARK_HOME/jars/aws-java-sdk-bundle.jar"
      echo "AWS Java SDK Bundle $AWS_JAVA_SDK_BUNDLE_VERSION installed successfully"
    fi
  fi
}

setup_spark
download_hudi_jars "$SOURCE_HUDI_VERSION"
download_hudi_jars "$TARGET_HUDI_VERSION"

echo "✅ Node setup complete."
