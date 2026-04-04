#!/usr/bin/env bash
#
# End-to-end Hudi performance test:
#   1. One initial parquet ingestion
#   2. Hudi ingestion (initial load)
#   3. Run benchmark
#   4. Two incremental cycles: (generate incremental data → Hudi ingestion → benchmark) × 2
#
# Usage:
#   bash run_e2e_performance_test.sh --table-type COPY_ON_WRITE
#   bash run_e2e_performance_test.sh --table-type MERGE_ON_READ
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/scripts/load_config.sh"

usage() {
  log_info ""
  log_info "Usage:"
  log_info "  bash $0 --table-type <COPY_ON_WRITE|MERGE_ON_READ> [--dry-run]"
  log_info ""
  log_info "Options:"
  log_info "  --table-type      COPY_ON_WRITE or MERGE_ON_READ (for Hudi table and benchmark)."
  log_info "  --dry-run         Print the plan only, do not run any step."
  log_info "  --force           Ignore saved state and run all steps (default: skip steps that already succeeded)."
  log_info ""
  exit 1
}

TABLE_TYPE=""
HUDI_VERSIONS=${HUDI_VERSIONS:-${SOURCE_HUDI_VERSION},${TARGET_HUDI_VERSION}}
DRY_RUN=false
FORCE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      [[ -z "${2:-}" ]] && { log_error "❌ Error: --table-type requires a value"; usage; }
      TABLE_TYPE="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --force)
      FORCE=true
      shift
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

if [[ -z "$TABLE_TYPE" ]]; then
  log_error "❌ Error: --table-type is required"
  usage
fi

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')
case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW) TABLE_TYPE="COPY_ON_WRITE" ;;
  MERGE_ON_READ|MOR) TABLE_TYPE="MERGE_ON_READ" ;;
  *)
    log_error "❌ Invalid --table-type: $TABLE_TYPE"
    usage
    ;;
esac

TARGET_VERSION=$(echo "${TARGET_HUDI_VERSION}" | cut -d '.' -f 1,2 | tr -d '.')
TABLE_TYPE_LOWER=$(echo "$TABLE_TYPE" | tr '[:upper:]' '[:lower:]')
E2E_STATE_DIR="${SCRIPT_DIR}/.e2e_state"
E2E_STATE_FILE="${E2E_STATE_DIR}/state_${TABLE_TYPE_LOWER}_${IS_LOGICAL_TIMESTAMP_ENABLED}_v${TARGET_VERSION}.txt"
S3_STATE_FILE="${BASE_PATH}/e2e_state/state_${TABLE_TYPE_LOWER}_${IS_LOGICAL_TIMESTAMP_ENABLED}_v${TARGET_VERSION}.txt"
S3_LOGS_DIR="${BASE_PATH}/logs"
REPORTS_DIR="${SCRIPT_DIR}/reports"

mkdir -p "$E2E_STATE_DIR" "$REPORTS_DIR"

# Benchmark CSV path: reports/hudi_benchmark_results_<cow|mor>_<0_14|0_15|...>.csv
if [[ "$TABLE_TYPE" == "COPY_ON_WRITE" ]]; then
  BENCHMARK_TABLE_SUFFIX="cow"
else
  BENCHMARK_TABLE_SUFFIX="mor"
fi

BENCHMARK_VERSION_SUFFIX=""
for v in $(echo "$HUDI_VERSIONS" | tr ',' ' '); do
  v_clean="${v%%-*}"
  major_minor=$(echo "$v_clean" | cut -d. -f1,2 | tr '.' '_')
  case " ${BENCHMARK_VERSION_SUFFIX} " in
    *" ${major_minor} "*) ;;
    *) BENCHMARK_VERSION_SUFFIX="${BENCHMARK_VERSION_SUFFIX} ${major_minor}" ;;
  esac
done
BENCHMARK_VERSION_SUFFIX=$(echo $BENCHMARK_VERSION_SUFFIX | tr ' ' '_')
[[ -z "$BENCHMARK_VERSION_SUFFIX" ]] && BENCHMARK_VERSION_SUFFIX="0_14"

# Report filenames include table (cow|mor), IS_LOGICAL_TIMESTAMP_ENABLED, and Hudi version suffix
BENCHMARK_CSV_PATH="${REPORTS_DIR}/hudi_benchmark_results_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}.csv"
WRITE_PERF_CSV="${REPORTS_DIR}/hudi_write_performance_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}.csv"
export WRITE_PERF_CSV TABLE_TYPE IS_LOGICAL_TIMESTAMP_ENABLED

# Log file: logs/<YYYYMMDD>/e2e_<table>_v<ver>_<lts>.log
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_RUN_ID="$(date +%Y%m%d)"
LOG_SUBDIR="${LOG_DIR}/${LOG_RUN_ID}"
mkdir -p "$LOG_SUBDIR"
LOG_FILE="${LOG_SUBDIR}/e2e_${TABLE_TYPE_LOWER}_v${TARGET_VERSION}_${IS_LOGICAL_TIMESTAMP_ENABLED}.log"

# Create log file and write all output to it (and console) via helper
: > "$LOG_FILE"
log_echo() { printf '%s\n' "$*" | tee -a "$LOG_FILE"; }
log_run() { "$@" 2>&1 | tee -a "$LOG_FILE"; return "${PIPESTATUS[0]}"; }

echo "Log file: $LOG_FILE"

# If state file exists on S3, download to local so we resume from last run
if aws s3 ls "$S3_STATE_FILE" &>/dev/null; then
  echo "Downloading existing state from S3: $S3_STATE_FILE"
  aws s3 cp "$S3_STATE_FILE" "$E2E_STATE_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
fi

log_equal
log_info "  E2E Hudi Performance Test"
log_info "  Table type          : $TABLE_TYPE"
log_info "  Source Hudi version : $SOURCE_HUDI_VERSION"
log_info "  Target Hudi version : $TARGET_HUDI_VERSION"
log_equal

get_step_status() {
  local step_id="$1"
  if [[ -f "$E2E_STATE_FILE" ]]; then
    local line
    line=$(grep -F "${step_id}=" "$E2E_STATE_FILE" 2>/dev/null || true)
    if [[ -n "$line" ]]; then
      echo "${line#*=}" | tr -d '[:space:]'
    else
      echo ""
    fi
  else
    echo ""
  fi
}

set_step_status() {
  local step_id="$1"
  local status="$2"
  local tmp_file="${E2E_STATE_FILE}.tmp"
  if [[ -f "$E2E_STATE_FILE" ]]; then
    grep -v -F "${step_id}=" "$E2E_STATE_FILE" 2>/dev/null > "$tmp_file" || true
  else
    : > "$tmp_file"
  fi
  echo "${step_id}=${status}" >> "$tmp_file"
  mv "$tmp_file" "$E2E_STATE_FILE"
  if [[ -f "$E2E_STATE_FILE" ]] && [[ "$DRY_RUN" != true ]]; then
    aws s3 cp "$E2E_STATE_FILE" "$S3_STATE_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
  fi
}

upload_benchmark_csv_to_s3() {
  if [[ "$DRY_RUN" == true ]]; then
    return 0
  fi
  local f
  for f in "${REPORTS_DIR}"/hudi_benchmark_results*.csv; do
    if [[ -f "$f" ]]; then
      echo "Uploading $(basename "$f") to S3: ${BASE_PATH}/reports/$(basename "$f")"
      aws s3 cp "$f" "${BASE_PATH}/reports/$(basename "$f")" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
    fi
  done
}

upload_write_perf_csv_to_s3() {
  if [[ "$DRY_RUN" == true ]]; then
    return 0
  fi
  local f
  for f in "${REPORTS_DIR}"/hudi_write_performance*.csv; do
    if [[ -f "$f" ]]; then
      echo "Uploading write performance $(basename "$f") to S3: ${BASE_PATH}/reports/$(basename "$f")"
      aws s3 cp "$f" "${BASE_PATH}/reports/$(basename "$f")" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
    fi
  done
}

run_step() {
  local step_id="$1"
  local step_name="$2"
  shift 2
  local status
  status=$(get_step_status "$step_id")
  if [[ "$FORCE" != true ]] && [[ "$status" == "success" ]]; then
    log_hyphen
    log_echo ">>> $step_name [SKIPPED - already succeeded]"
    log_hyphen
    return 0
  fi

  if [[ "$status" == "failure" ]]; then
    log_echo ">>> $step_name [RETRY - previous run failed]"
    log_hyphen
  elif [[ "$FORCE" == true ]]; then
    log_echo ""
    log_echo ">>> $step_name [FORCE - running regardless of state]"
  else
    log_echo ""
    log_echo ">>> $step_name"
  fi
  log_echo "    $*"
  if [[ "$DRY_RUN" == true ]]; then
    return 0
  fi
  if log_run "$@"; then
    set_step_status "$step_id" "success"
    log_echo "✅ Success (state saved)"
  else
    set_step_status "$step_id" "failure"
    log_echo "❌ Failed (state saved; will retry this step next run)"
    return 1
  fi
}

if [[ "$DRY_RUN" == true ]]; then
  log_echo ""
  log_echo "[DRY RUN] Would execute the following steps:"
fi

step_num=0
TOTAL_BATCHES=4
TOTAL_STEPS=$((TOTAL_BATCHES * 3))

for ((BATCH_ID=0; BATCH_ID<TOTAL_BATCHES; BATCH_ID++)); do
    start_time=$(date +%s)
    log_info "$(log_hyphen)"
    log_echo "Processing batch $BATCH_ID..."

    job_type="incremental"
    if [[ "$BATCH_ID" == 0 ]]; then
      job_type="initial"
      run_hudi_version="$SOURCE_HUDI_VERSION"
    elif [[ "$BATCH_ID" == 1 ]]; then
      run_hudi_version="$SOURCE_HUDI_VERSION"
    else
      run_hudi_version="$TARGET_HUDI_VERSION"
    fi

    step_num=$((step_num + 1))
    run_step "step${step_num}_${job_type}_${BATCH_ID}_parquet" "Step ${step_num}/${TOTAL_STEPS}: ${job_type} batch $BATCH_ID - generate parquet data" \
    bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" \
      --type $job_type \
      --batch-id $BATCH_ID

    step_num=$((step_num + 1))
    run_step "step${step_num}_${job_type}_${BATCH_ID}_hudi" "Step ${step_num}/${TOTAL_STEPS}: Hudi ${job_type} ingestion" \
    bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" \
      --table-type "$TABLE_TYPE" \
      --target-hudi-version "$run_hudi_version" \
      --batch-id $BATCH_ID

    step_num=$((step_num + 1))
    run_step "step${step_num}_${job_type}_${BATCH_ID}_benchmark" "Step ${step_num}/${TOTAL_STEPS}: Benchmark - after ${job_type} batch $BATCH_ID" \
    python3 "${SCRIPT_DIR}/run_benchmark_suite.py" \
      --table-type "$TABLE_TYPE" \
      --hudi-versions "$HUDI_VERSIONS" \
      --batch-id $BATCH_ID \
      --output "$BENCHMARK_CSV_PATH"

    upload_benchmark_csv_to_s3
    upload_write_perf_csv_to_s3

    end_time=$(date +%s)
    duration=$((end_time - start_time))
    if [[ "$OSTYPE" == "darwin"* ]]; then
      duration_formatted=$(date -u -r "$duration" +%H:%M:%S)
    else
      duration_formatted=$(date -u -d "@$duration" +%H:%M:%S)
    fi
    
    log_info "Batch $BATCH_ID processing completed in $duration_formatted ..."
    log_info "$(log_hyphen)"
done

# Upload results and log to S3 (state and CSV already uploaded after each step / each benchmark)
if [[ "$DRY_RUN" != true ]]; then
  upload_benchmark_csv_to_s3
  upload_write_perf_csv_to_s3
  if [[ -f "$LOG_FILE" ]]; then
    S3_LOG_FILE="${S3_LOGS_DIR}/${LOG_RUN_ID}/$(basename "$LOG_FILE")"
    echo "Uploading log to S3: $S3_LOG_FILE"
    aws s3 cp "$LOG_FILE" "$S3_LOG_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
  fi
  echo "E2E state synced to S3 after each step: $S3_STATE_FILE"
fi

log_equal
echo "E2E performance test completed"
echo "Report (read)  : $BENCHMARK_CSV_PATH"
echo "Report (write) : $WRITE_PERF_CSV"
echo "Log file       : $LOG_FILE"
log_equal
