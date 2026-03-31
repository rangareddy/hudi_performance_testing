#!/usr/bin/env bash
#
# Run initial ingestion using spark-shell and initial_batch.scala.
# Dataset: 500 cols, 10 of which are timestamp columns.
#
set -euo pipefail

SCRIPT_NAME="$0"  
SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_NAME}")" && pwd)"

# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/scripts/load_config.sh"

usage() {
  log_info ""
  log_info "Usage:"
  log_info "  bash $SCRIPT_NAME --type <initial|incremental> --batch-id <id>"
  log_info ""
  log_info "Options:"
  log_info "  --type        initial | incremental"
  log_info "  --batch-id    Batch ID (required, non-negative integer)"
  log_info ""
  log_info "Examples:"
  log_info "  bash $SCRIPT_NAME --type initial --batch-id 0"
  log_info "  bash $SCRIPT_NAME --type incremental --batch-id 1"
  log_info ""
  exit 1
}

INGESTION_TYPE="initial"
BATCH_ID=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --type)
      if [[ -z "$2" ]]; then
        log_error "❌ Error: --type requires a value"
        usage
      fi
      INGESTION_TYPE="$2"
      shift 2
      ;;
    --batch-id)
      if [[ -z "$2" ]]; then
        log_error "❌ Error: --batch-id requires a value"
        usage
      fi
      BATCH_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      log_error "❌ Unknown option: $1"
      usage
      ;;
  esac
done

if [[ -z "$BATCH_ID" ]]; then
  log_error "❌ Error: --batch-id is required"
  usage
fi
if [[ ! "$BATCH_ID" =~ ^[0-9]+$ ]]; then
  log_error "❌ Error: --batch-id must be a non-negative integer: $BATCH_ID"
  exit 1
fi

# E2E batch id for reporting (incremental job may export BATCH_ID=0 for IS_REPEAT_SAME_BATCH)
REQUESTED_BATCH_ID="$BATCH_ID"

if [[ "$INGESTION_TYPE" != "initial" ]] && [[ "$INGESTION_TYPE" != "incremental" ]]; then
  log_error "❌ Invalid Ingestion Type: $INGESTION_TYPE"
  echo "Allowed values: initial or incremental"
  exit 1
fi

# Convert ingestion type to title case means first letter is upper case and remainig lower case
INGESTION_TYPE_TITLE=$(echo "$INGESTION_TYPE" | sed 's/.*/\u&/')
export SOURCE_DATA

if [ -z "${SOURCE_DATA:-}" ]; then
  log_error "❌ Source Data is not set"
  exit 1
fi

append_parquet_write_perf() {
  local duration_sec="$1"
  local status="$2"
  [[ -z "${WRITE_PERF_CSV:-}" ]] && return 0
  mkdir -p "$(dirname "$WRITE_PERF_CSV")"
  local header="run_timestamp_utc,table_type,operation,batch_id,hudi_version,execution_time_seconds,status,is_logical_timestamp_enabled"
  if [[ ! -f "$WRITE_PERF_CSV" ]]; then
    echo "$header" > "$WRITE_PERF_CSV"
  fi
  echo "$(date -u +"%Y-%m-%d %H:%M:%S"),${TABLE_TYPE:-},parquet_${INGESTION_TYPE},${REQUESTED_BATCH_ID},,${duration_sec},${status},${IS_LOGICAL_TIMESTAMP_ENABLED:-true}" >> "$WRITE_PERF_CSV"
}

export TARGET_DATA="${SOURCE_DATA}/batch_${REQUESTED_BATCH_ID}"
_target_exists=false
if [[ "$TARGET_DATA" == s3://* ]]; then
  if aws s3 ls "$TARGET_DATA" >/dev/null 2>&1; then
    _target_exists=true
  fi
else
  # Local/FS path: consider it present if directory exists and has at least one file.
  if [[ -d "$TARGET_DATA" ]]; then
    shopt -s nullglob dotglob
    _files=("$TARGET_DATA"/*)
    shopt -u nullglob dotglob
    if ((${#_files[@]})); then
      _target_exists=true
    fi
  fi
fi

if [[ "$_target_exists" == true ]]; then
  log_success "✅ Target data already exists at ${TARGET_DATA}. Skipping ${INGESTION_TYPE_TITLE} ingestion for batch ${REQUESTED_BATCH_ID}."
  append_parquet_write_perf 0 skipped_existing
  exit 0
fi
unset _files

export BATCH_ID="$REQUESTED_BATCH_ID"

EXECUTION_SCRIPT=$INCREMENTAL_SCRIPT
if [[ "$INGESTION_TYPE" == "initial" ]]; then
  EXECUTION_SCRIPT=$INITIAL_BATCH_SCALA
fi
if [[ ! -f "$EXECUTION_SCRIPT" ]]; then
  log_error "❌ Execution script not found: $EXECUTION_SCRIPT"
  exit 1
fi
if [[ ! -f "$SPARK_DEFAULTS_CONF" ]]; then
  log_error "❌ Spark defaults config not found: $SPARK_DEFAULTS_CONF"
  exit 1
fi

# Optional --jars: do not use "${arr[@]}" when empty — bash 3.2 + set -u treats it as unbound.
_PARQUET_JARS_ECHO=""
if [[ -n "${AWS_S3_JARS:-}" ]]; then
  _PARQUET_JARS_ECHO=" --jars ${AWS_S3_JARS}"
fi

log_equal
log_info "Starting ${INGESTION_TYPE_TITLE} ingestion job"
log_hipen
log_info "Ingestion Type        : $INGESTION_TYPE_TITLE"
log_info "Execution Script      : $EXECUTION_SCRIPT"
log_info "Source Data Path      : $SOURCE_DATA"
log_info "Target Data Path      : $TARGET_DATA"
log_info "Batch ID              : $BATCH_ID"
log_info "Is Logical Timestamp  : $IS_LOGICAL_TIMESTAMP_ENABLED"
log_equal

EXECUTION_STATUS_CODE=0
export NUM_OF_COLUMNS=${NUM_OF_COLUMNS:-500}
export NUM_OF_PARTITIONS=${NUM_OF_PARTITIONS:-10000}
export NUM_OF_RECORDS=${NUM_OF_RECORDS:-$NUM_OF_PARTITIONS}

log_info "Executing $INGESTION_TYPE_TITLE ingestion"
log_info "Dataset configuration: columns=${NUM_OF_COLUMNS}, partitions=${NUM_OF_PARTITIONS}, records=${NUM_OF_RECORDS}"

if [[ "$INGESTION_TYPE" == "initial" ]]; then
  _wp_start=$(date +%s)
  log_info "Executing Command:"
  echo "${SPARK_HOME}/bin/spark-shell --master ${SPARK_MASTER} --deploy-mode client${_PARQUET_JARS_ECHO} --properties-file ${SPARK_DEFAULTS_CONF} -i $EXECUTION_SCRIPT"
  if [[ -n "${AWS_S3_JARS:-}" ]]; then
    _parquet_shell_status=0
    "${SPARK_HOME}/bin/spark-shell" \
      --master "${SPARK_MASTER}" \
      --deploy-mode client \
      --jars "$AWS_S3_JARS" \
      --properties-file "${SPARK_DEFAULTS_CONF}" \
      -i "$EXECUTION_SCRIPT" || _parquet_shell_status=$?
  else
    _parquet_shell_status=0
    "${SPARK_HOME}/bin/spark-shell" \
      --master "${SPARK_MASTER}" \
      --deploy-mode client \
      --properties-file "${SPARK_DEFAULTS_CONF}" \
      -i "$EXECUTION_SCRIPT" || _parquet_shell_status=$?
  fi
  if [[ "${_parquet_shell_status:-0}" -eq 0 ]]; then
    EXECUTION_STATUS_CODE=0
    _wp_end=$(date +%s)
    _wp_dur=$((_wp_end - _wp_start))
    log_info "Write Execution Complete. parquet_initial batch ${REQUESTED_BATCH_ID}. Total execution time: ${_wp_dur} seconds"
    append_parquet_write_perf "$_wp_dur" "ok"
  else
    EXECUTION_STATUS_CODE="${_parquet_shell_status}"
    _wp_end=$(date +%s)
    _wp_dur=$((_wp_end - _wp_start))
    append_parquet_write_perf "$_wp_dur" "failure"
  fi
  unset _parquet_shell_status
else
  export IS_REPEAT_SAME_BATCH=${IS_REPEAT_SAME_BATCH:-true}
  if [[ "$IS_REPEAT_SAME_BATCH" == true ]]; then
    export BATCH_ID=0
  else
    export BATCH_ID=$BATCH_ID
  fi
  export NUM_OF_RECORDS_TO_UPDATE=${NUM_OF_RECORDS_TO_UPDATE:-100}
  export SOURCE_DATA="${SOURCE_DATA}/batch_0"
  log_info "Records to update per batch: ${NUM_OF_RECORDS_TO_UPDATE}"
  _wp_start=$(date +%s)
  log_info "Executing Command:"
  echo "${SPARK_HOME}/bin/spark-submit --master ${SPARK_MASTER} --deploy-mode client${_PARQUET_JARS_ECHO} --properties-file ${SPARK_DEFAULTS_CONF} $EXECUTION_SCRIPT"
  if [[ -n "${AWS_S3_JARS:-}" ]]; then
    _parquet_submit_status=0
    "${SPARK_HOME}/bin/spark-submit" \
      --master "${SPARK_MASTER}" \
      --deploy-mode client \
      --jars "$AWS_S3_JARS" \
      --properties-file "${SPARK_DEFAULTS_CONF}" \
      "$EXECUTION_SCRIPT" || _parquet_submit_status=$?
  else
    _parquet_submit_status=0
    "${SPARK_HOME}/bin/spark-submit" \
      --master "${SPARK_MASTER}" \
      --deploy-mode client \
      --properties-file "${SPARK_DEFAULTS_CONF}" \
      "$EXECUTION_SCRIPT" || _parquet_submit_status=$?
  fi
  if [[ "${_parquet_submit_status:-0}" -eq 0 ]]; then
    EXECUTION_STATUS_CODE=0
    _wp_end=$(date +%s)
    _wp_dur=$((_wp_end - _wp_start))
    log_info "Write Execution Complete. parquet_incremental batch ${REQUESTED_BATCH_ID}. Total execution time: ${_wp_dur} seconds"
    append_parquet_write_perf "$_wp_dur" "ok"
  else
    EXECUTION_STATUS_CODE="${_parquet_submit_status}"
    _wp_end=$(date +%s)
    _wp_dur=$((_wp_end - _wp_start))
    append_parquet_write_perf "$_wp_dur" "failure"
  fi
  unset _parquet_submit_status
fi

log_basic_info() {
  log_hipen
  log_info "Ingestion Type : ${INGESTION_TYPE_TITLE}"
  log_info "Batch ID       : ${REQUESTED_BATCH_ID}"
  log_info "Target Path    : ${TARGET_DATA}"
  log_hipen
}

if [ $EXECUTION_STATUS_CODE -eq 0 ]; then
  log_success "Parquet Ingestion completed successfully"
  log_basic_info  
else
  log_error "Parquet Ingestion failed"
  log_basic_info
  exit 1
fi