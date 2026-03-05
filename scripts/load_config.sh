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
      export "${BASH_REMATCH[1]}=${BASH_REMATCH[2]}"
    fi
  done < "$config_file"

  # Derived paths (only if not already set)
  [[ -z "${JARS_PATH:-}" && -n "${BASE_PATH:-}" ]] && export JARS_PATH="${BASE_PATH}/jars"
  [[ -z "${TABLE_BASE_PATH:-}" && -n "${BASE_PATH:-}" ]] && export TABLE_BASE_PATH="${BASE_PATH}/data/hudi_mor_logical"
  [[ -z "${SOURCE_DFS_ROOT:-}" && -n "${BASE_PATH:-}" ]] && export SOURCE_DFS_ROOT="${BASE_PATH}/data/wide_500cols_10000parts"

  return 0
}

load_config
