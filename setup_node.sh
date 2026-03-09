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
export HADOOP_MAJOR_VERSION=$(echo "${HADOOP_VERSION}" | cut -d '.' -f 1)

log_equal
log_info "Hudi performance testing node setup"
log_hipen
log_info "SOURCE_HUDI_VERSION  : ${SOURCE_HUDI_VERSION}"
log_info "TARGET_HUDI_VERSION  : ${TARGET_HUDI_VERSION}"
log_info "SPARK_VERSION        : ${SPARK_VERSION}"
log_info "BASE_PATH            : ${BASE_PATH}"
log_info "JARS_PATH            : ${JARS_PATH}"
log_info "DATA_PATH            : ${DATA_PATH}"
log_info "SCRIPTS_DIR          : ${SCRIPTS_DIR}"
log_equal

download_hudi_jars() {
  local hudi_version="$1"
  hudi_version_suffix=$(echo "$hudi_version" | cut -d '.' -f 1,2)
  hudi_spark_bundle_jar="hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${hudi_version}.jar"
  hudi_utilities_slim_bundle_jar="hudi-utilities-slim-bundle_${SCALA_VERSION}-${hudi_version}.jar"
  if [[ ! -f "$JARS_PATH/$hudi_spark_bundle_jar" ]]; then
    aws s3 cp $S3_JARS_PATH/${hudi_version_suffix}/$hudi_spark_bundle_jar $JARS_PATH/$hudi_spark_bundle_jar
    if [ $? -eq 0 ]; then
      log_success "Hudi Spark Bundle $hudi_spark_bundle_jar downloaded successfully"
    else
      log_error "Failed to download Hudi Spark Bundle $hudi_spark_bundle_jar"
      exit 1
    fi
  fi
  if [[ ! -f "$JARS_PATH/$hudi_utilities_slim_bundle_jar" ]]; then
    aws s3 cp $S3_JARS_PATH/${hudi_version_suffix}/$hudi_utilities_slim_bundle_jar $JARS_PATH/$hudi_utilities_slim_bundle_jar
    if [ $? -eq 0 ]; then
      log_success "Hudi Utilities Slim Bundle $hudi_utilities_slim_bundle_jar downloaded successfully"
    else
      log_error "Failed to download Hudi Utilities Slim Bundle $hudi_utilities_slim_bundle_jar"
      exit 1
    fi
  fi
}

setup_spark() {
  log_info "Setting the Spark $SPARK_VERSION with Hadoop $HADOOP_MAJOR_VERSION"
  log_hipen
  if [[ ! -d "$SPARK_HOME" ]]; then
    log_info "Downloading Spark $SPARK_VERSION"
    SPARK_TAR_FILE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz"
    SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR_FILE}"
    if [[ ! -f "$HOME/${SPARK_TAR_FILE}" ]]; then
      wget --quiet -O "$HOME/${SPARK_TAR_FILE}" "$SPARK_URL"
    fi
    tar -xzf "$HOME/${SPARK_TAR_FILE}" -C "$HOME"
    rm -f "$HOME/${SPARK_TAR_FILE}"
    SPARK_HOME="$HOME/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}"
    log_info "Spark $SPARK_VERSION successfully installed in $SPARK_HOME"

    hadoop_aws_jar="hadoop-aws-$HADOOP_VERSION.jar"
    if [[ ! -f "$SPARK_HOME/jars/$hadoop_aws_jar" ]]; then 
      log_info "Installing Hadoop AWS $HADOOP_VERSION for Spark $SPARK_VERSION"
      wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/$hadoop_aws_jar \
        -O "$SPARK_HOME/jars/$hadoop_aws_jar"
    fi
    ln -sf "$SPARK_HOME/jars/$hadoop_aws_jar" "$SPARK_HOME/jars/hadoop-aws.jar"
    log_info "Hadoop AWS $HADOOP_VERSION for Spark $SPARK_VERSION installed successfully"
    aws_java_sdk_bundle_jar="aws-java-sdk-bundle-$AWS_JAVA_SDK_BUNDLE_VERSION.jar"  
    if [[ ! -f "$SPARK_HOME/jars/$aws_java_sdk_bundle_jar" ]]; then
      log_info "Installing AWS Java SDK Bundle $AWS_JAVA_SDK_BUNDLE_VERSION for Spark $SPARK_VERSION"
      wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_JAVA_SDK_BUNDLE_VERSION/$aws_java_sdk_bundle_jar \
        -O "$SPARK_HOME/jars/$aws_java_sdk_bundle_jar"
    fi
    ln -sf "$SPARK_HOME/jars/$aws_java_sdk_bundle_jar" "$SPARK_HOME/jars/aws-java-sdk-bundle.jar"
    log_info "AWS Java SDK Bundle $AWS_JAVA_SDK_BUNDLE_VERSION for Spark $SPARK_VERSION installed successfully"
  fi
}

setup_spark
download_hudi_jars "$SOURCE_HUDI_VERSION"
download_hudi_jars "$TARGET_HUDI_VERSION"

log_success "Node setup completed successfully."
