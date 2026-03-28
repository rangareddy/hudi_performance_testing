#!/usr/bin/env bash
#
# Load common.properties into environment variables.
# Source this from other scripts: source "$(dirname "$0")/load_config.sh"
# CONFIG_FILE can override the path to the properties file.
#

log_time() {
  date "+%Y-%m-%d %H:%M:%S"
}

log_equal() {
  echo "============================================================================="
}

log_hipen() {
  echo "-----------------------------------------------------------------------------"
}

log_info() {
  local msg="$(log_time) [INFO ] $*"
  echo "$msg"
}

log_success() {
  local msg="$(log_time) [SUCCESS] $*"
  echo "$msg"
}

log_warn() {
  local msg="$(log_time) [WARN ] $*"
  echo "$msg"
}

log_error() {
  local msg="$(log_time) [ERROR] $*"
  echo "$msg" >&2
}

load_config() {
  local script_dir
  # Directory containing this loader (and common.properties)
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local config_file="${CONFIG_FILE:-${script_dir}/common.properties}"

  if [[ ! -f "$config_file" ]]; then
    log_warn "Warning: config file not found: $config_file" >&2
    return 1
  fi

  while IFS= read -r line; do
    line="${line%%#*}"           # strip comment
    line="${line#"${line%%[![:space:]]*}"}"  # strip leading space
    line="${line%"${line##*[![:space:]]}"}"  # strip trailing space
    [[ -z "$line" ]] && continue
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      local key="${BASH_REMATCH[1]}"
      local val="${BASH_REMATCH[2]}"
      # Expand $VAR and ${VAR} in value (so JARS_PATH=$BASE_PATH/jars works)
      val="$(eval echo "$val")"
      export "${key}=${val}"
    fi
  done < "$config_file"

  if [[ "${IS_USE_INSTALLED_SPARK:-false}" == "true" ]] && [[ -n "${TEMP_SPARK_HOME:-}" ]] && [[ -d "$TEMP_SPARK_HOME" ]]; then
    export SPARK_HOME="$TEMP_SPARK_HOME"
  fi

  if [[ "${SKIP_SPARK_HOME_CHECK:-0}" != "1" ]]; then
    if [[ ! -d "${SPARK_HOME:-}" ]]; then
      log_error "❌ Spark home not found: ${SPARK_HOME:-}"
      log_error "Set IS_USE_INSTALLED_SPARK=true and TEMP_SPARK_HOME (e.g. /usr/lib/spark on EMR), or run setup_node.sh"
      exit 1
    fi
  fi

  export SPARK_MAJOR_VERSION=$(echo "${SPARK_VERSION}" | cut -d '.' -f 1,2)

  # Derived paths (only if not already set)
  [[ -z "${JARS_PATH:-}" && -n "${BASE_PATH:-}" ]] && export JARS_PATH="${BASE_PATH}/jars"
  [[ -z "${DATA_PATH:-}" && -n "${BASE_PATH:-}" ]] && export DATA_PATH="${BASE_PATH}/data"

  if [[ -z "${SOURCE_DATA:-}" ]]; then
    if [[ "${IS_LOGICAL_TIMESTAMP_ENABLED:-true}" == "true" ]]; then
      export SOURCE_DATA="${DATA_PATH}/wide_${NUM_OF_COLUMNS}cols_${NUM_OF_PARTITIONS}parts_lts"
    else
      export SOURCE_DATA="${DATA_PATH}/wide_${NUM_OF_COLUMNS}cols_${NUM_OF_PARTITIONS}parts"
    fi
  fi

  # Delta Streamer --props must be file:// URL when using local path
  if [[ -n "${PROPS_FILE:-}" && "$PROPS_FILE" != file://* && "$PROPS_FILE" != s3:* ]]; then
    export PROPS_FILE="file://${PROPS_FILE}"
  fi

  # Resolve SPARK_DEFAULTS_CONF to absolute path so spark-submit finds it from any cwd
  if [[ -n "${SPARK_DEFAULTS_CONF:-}" && "$SPARK_DEFAULTS_CONF" != /* ]]; then
    local config_dir
    config_dir="$(dirname "$config_file")"
    if [[ -f "$config_dir/$SPARK_DEFAULTS_CONF" ]]; then
      export SPARK_DEFAULTS_CONF="$(cd "$config_dir" && cd "$(dirname "$SPARK_DEFAULTS_CONF")" && pwd)/$(basename "$SPARK_DEFAULTS_CONF")"
    fi
  fi

  HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-''}
  if [ -z "$HADOOP_CONF_DIR" ]; then
    export HADOOP_CONF_DIR=/etc/hadoop/conf
  fi

  if [[ "${SKIP_SPARK_HOME_CHECK:-0}" != "1" ]]; then
    if [[ -z "${AWS_S3_JARS:-}" ]]; then
      if [[ "${IS_USE_INSTALLED_SPARK:-false}" == "true" ]]; then
        _emr_sdk=$(ls /usr/share/aws/aws-java-sdk/aws-java-sdk-bundle*.jar | head -1)
        _emr_ha=$(ls /usr/lib/hadoop/hadoop-aws*.jar | head -1)
        if [[ ! -f "$_emr_sdk" ]]; then
          log_error "❌ EMR AWS SDK jar not found: $_emr_sdk"
          log_error "Set EMR_AWS_SDK_JAR or AWS_S3_JARS in common.properties."
          exit 1
        fi
        if [[ ! -f "$_emr_ha" ]]; then
          log_error "❌ EMR hadoop-aws jar not found: $_emr_ha"
          log_error "Set EMR_HADOOP_AWS_JAR or AWS_S3_JARS in common.properties."
          exit 1
        fi
        export AWS_S3_JARS="${_emr_sdk},${_emr_ha}"
        log_info "Using EMR S3 jars: $AWS_S3_JARS"
      else
        _spark_jars="${SPARK_HOME}/jars"
        aws_java_sdk_bundle_jar="aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION:-}.jar"
        hadoop_aws_jar="hadoop-aws-${HADOOP_VERSION:-}.jar"
        _aws_path="${_spark_jars}/${aws_java_sdk_bundle_jar}"
        _ha_path="${_spark_jars}/${hadoop_aws_jar}"
        if [[ ! -f "$_aws_path" ]]; then
          shopt -s nullglob
          _cand=("${_spark_jars}"/aws-java-sdk-bundle-*.jar)
          shopt -u nullglob
          [[ ${#_cand[@]} -gt 0 ]] && _aws_path="${_cand[0]}"
        fi
        if [[ ! -f "$_ha_path" ]]; then
          shopt -s nullglob
          _cand=("${_spark_jars}"/hadoop-aws-*.jar)
          shopt -u nullglob
          [[ ${#_cand[@]} -gt 0 ]] && _ha_path="${_cand[0]}"
        fi
        if [[ ! -f "$_aws_path" ]]; then
          log_error "❌ AWS Java SDK bundle jar not found under ${_spark_jars}"
          exit 1
        fi
        if [[ ! -f "$_ha_path" ]]; then
          log_error "❌ hadoop-aws jar not found under ${_spark_jars}"
          exit 1
        fi
        export AWS_S3_JARS="${_aws_path},${_ha_path}"
      fi
    fi
  fi
  return 0
}

load_config