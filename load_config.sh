#!/usr/bin/env bash
#
# Load common.properties into environment variables.
# Source this from other scripts: source "$(dirname "$0")/load_config.sh"
# CONFIG_FILE can override the path to the properties file.
#
load_config() {
  local script_dir
  # Directory containing this loader (and common.properties)
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local config_file="${CONFIG_FILE:-${script_dir}/common.properties}"

  if [[ ! -f "$config_file" ]]; then
    echo "Warning: config file not found: $config_file" >&2
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

  if [ ! -d "$SPARK_HOME" ]; then
    echo "❌ Spark home not found: $SPARK_HOME"
    echo "Please run setup_node.sh to install Spark"
    if [[ "${SKIP_SPARK_HOME_CHECK:-0}" != "1" && "${SKIP_SPARK_HOME_CHECK:-0}" != "true" ]]; then
      exit 1
    fi
  fi

  export SPARK_MAJOR_VERSION=$(echo "${SPARK_VERSION}" | cut -d '.' -f 1,2)

  # Derived paths (only if not already set)
  [[ -z "${JARS_PATH:-}" && -n "${BASE_PATH:-}" ]] && export JARS_PATH="${BASE_PATH}/jars"
  [[ -z "${DATA_PATH:-}" && -n "${BASE_PATH:-}" ]] && export DATA_PATH="${BASE_PATH}/data"
  [[ -z "${TABLE_BASE_PATH:-}" && -n "${DATA_PATH:-}" && -n "${TABLE_NAME:-}" ]] && export TABLE_BASE_PATH="${DATA_PATH}/hudi_logical"
  [[ -z "${SOURCE_DATA:-}" && -n "${DATA_PATH:-}" ]] && export SOURCE_DATA="${DATA_PATH}/wide_500cols_10000parts"
  [[ -z "${BASE_DATA_PATH:-}" && -n "${DATA_PATH:-}" ]] && export BASE_DATA_PATH="${DATA_PATH}"

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

  if [ -z "${AWS_S3_JARS:-}" ]; then
    export AWS_S3_JARS="${SPARK_HOME}/jars/aws-java-sdk-bundle.jar,${SPARK_HOME}/jars/hadoop-aws.jar"
  fi
  return 0
}

load_config
