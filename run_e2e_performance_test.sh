#!/usr/bin/env bash
#
# End-to-end Hudi performance test (two phases, then comparison):
#   Baseline: 5 batches — Hudi ingestion always uses SOURCE_HUDI_VERSION.
#   Experiment: 5 batches — batches 0–2 use SOURCE_HUDI_VERSION, batches 3–4 use TARGET_HUDI_VERSION.
#   Each batch: parquet → Hudi ingestion → read benchmark.
#   Baseline and experiment use separate Hudi table paths (--table-name-suffix baseline|experiment).
#   MERGE_ON_READ: after 5 batches, run compaction then read benchmark again (post-compaction CSV).
#   After both phases: compare write + read performance (reports + S3).
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
S3_LOGS_DIR="${BASE_PATH}/logs"
REPORTS_DIR="${SCRIPT_DIR}/reports"
# Per-phase state/sync (set in run_e2e_phase)
E2E_STATE_FILE=""
S3_STATE_FILE=""

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

# Report stems (phase suffix: _baseline | _experiment added in run_e2e_phase)
BENCHMARK_REPORT_STEM="${REPORTS_DIR}/hudi_benchmark_results_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}"
WRITE_REPORT_STEM="${REPORTS_DIR}/hudi_write_performance_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}"
COMPARISON_CSV="${REPORTS_DIR}/e2e_baseline_vs_experiment_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}.csv"
BENCHMARK_CSV_PATH=""
WRITE_PERF_CSV=""
export TABLE_TYPE IS_LOGICAL_TIMESTAMP_ENABLED

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

log_equal
log_info "  E2E Hudi Performance Test"
log_info "  Table type          : $TABLE_TYPE"
log_info "  Source Hudi version : $SOURCE_HUDI_VERSION"
log_info "  Target Hudi version : $TARGET_HUDI_VERSION"
log_info "  Phases              : baseline (5 batches, all SOURCE) then experiment (5 batches, 0-2 SOURCE, 3-4 TARGET)"
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

upload_comparison_csv_to_s3() {
  if [[ "$DRY_RUN" == true ]]; then
    return 0
  fi
  local f
  for f in "${REPORTS_DIR}"/e2e_baseline_vs_experiment*.csv; do
    if [[ -f "$f" ]]; then
      echo "Uploading comparison $(basename "$f") to S3: ${BASE_PATH}/reports/$(basename "$f")"
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
    log_hipen
    log_echo ">>> $step_name [SKIPPED - already succeeded]"
    log_hipen
    return 0
  fi

  if [[ "$status" == "failure" ]]; then
    log_echo ">>> $step_name [RETRY - previous run failed]"
    log_hipen
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

run_e2e_phase() {
  local phase="$1"
  local phase_upper
  phase_upper=$(echo "$phase" | tr '[:lower:]' '[:upper:]')
  E2E_STATE_FILE="${E2E_STATE_DIR}/state_${phase}_${TABLE_TYPE_LOWER}_${IS_LOGICAL_TIMESTAMP_ENABLED}_v${TARGET_VERSION}.txt"
  S3_STATE_FILE="${BASE_PATH}/e2e_state/state_${phase}_${TABLE_TYPE_LOWER}_${IS_LOGICAL_TIMESTAMP_ENABLED}_v${TARGET_VERSION}.txt"
  BENCHMARK_CSV_PATH="${BENCHMARK_REPORT_STEM}_${phase}.csv"
  BENCHMARK_POST_COMPACT_CSV="${BENCHMARK_REPORT_STEM}_${phase}_post_compact.csv"
  WRITE_PERF_CSV="${WRITE_REPORT_STEM}_${phase}.csv"
  export WRITE_PERF_CSV

  log_equal
  log_info "  Phase: ${phase_upper}"
  if [[ "$phase" == "baseline" ]]; then
    log_info "  Hudi ingestion: SOURCE_HUDI_VERSION only (${SOURCE_HUDI_VERSION}) for all 5 batches"
  else
    log_info "  Hudi ingestion: SOURCE (${SOURCE_HUDI_VERSION}) for batches 0–2; TARGET (${TARGET_HUDI_VERSION}) for batches 3–4"
  fi
  log_equal

  if aws s3 ls "$S3_STATE_FILE" &>/dev/null; then
    echo "Downloading ${phase} state from S3: $S3_STATE_FILE"
    aws s3 cp "$S3_STATE_FILE" "$E2E_STATE_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
  fi

  local step_num=0
  local TOTAL_BATCHES=5
  local TOTAL_BATCH_STEPS=$((TOTAL_BATCHES * 3))
  local PHASE_TOTAL_STEPS=$TOTAL_BATCH_STEPS
  if [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]; then
    PHASE_TOTAL_STEPS=$((TOTAL_BATCH_STEPS + 2))
  fi
  local BATCH_ID
  local job_type
  local run_hudi_version

  for ((BATCH_ID=0; BATCH_ID<TOTAL_BATCHES; BATCH_ID++)); do
    start_time=$(date +%s)
    log_info "$(log_hipen)"
    log_echo "[${phase_upper}] Processing batch $BATCH_ID..."

    job_type="incremental"
    if [[ "$BATCH_ID" == 0 ]]; then
      job_type="initial"
    fi

    if [[ "$phase" == "baseline" ]]; then
      run_hudi_version="$SOURCE_HUDI_VERSION"
    else
      if [[ "$BATCH_ID" -le 2 ]]; then
        run_hudi_version="$SOURCE_HUDI_VERSION"
      else
        run_hudi_version="$TARGET_HUDI_VERSION"
      fi
    fi

    step_num=$((step_num + 1))
    run_step "step${step_num}_${job_type}_${BATCH_ID}_parquet" "[${phase}] Step ${step_num}/${TOTAL_BATCH_STEPS}: ${job_type} batch $BATCH_ID - parquet" \
    bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" \
      --type $job_type \
      --batch-id $BATCH_ID

    step_num=$((step_num + 1))
    run_step "step${step_num}_${job_type}_${BATCH_ID}_hudi" "[${phase}] Step ${step_num}/${TOTAL_BATCH_STEPS}: Hudi ${job_type} (${run_hudi_version}) batch $BATCH_ID" \
    bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" \
      --table-type "$TABLE_TYPE" \
      --target-hudi-version "$run_hudi_version" \
      --batch-id $BATCH_ID \
      --table-name-suffix "$phase"

    step_num=$((step_num + 1))
    run_step "step${step_num}_${job_type}_${BATCH_ID}_benchmark" "[${phase}] Step ${step_num}/${TOTAL_BATCH_STEPS}: Benchmark batch $BATCH_ID" \
    python3 "${SCRIPT_DIR}/run_benchmark_suite.py" \
      --table-type "$TABLE_TYPE" \
      --hudi-versions "$HUDI_VERSIONS" \
      --batch-id $BATCH_ID \
      --table-name-suffix "$phase" \
      --output "$BENCHMARK_CSV_PATH"

    upload_benchmark_csv_to_s3
    upload_write_perf_csv_to_s3

    end_time=$(date +%s)
    duration=$((end_time - start_time))
    if [[ "$OSTYPE" == "darwin"* ]]; then
      duration_formatted=$(date -u -r "$duration" +%H:%M:%S 2>/dev/null || echo "${duration}s")
    else
      duration_formatted=$(date -u -d "@$duration" +%H:%M:%S 2>/dev/null || echo "${duration}s")
    fi

    log_info "[${phase}] Batch $BATCH_ID completed in $duration_formatted"
    log_info "$(log_hipen)"
  done

  if [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]; then
    local compact_version
    if [[ "$phase" == "baseline" ]]; then
      compact_version="$SOURCE_HUDI_VERSION"
    else
      compact_version="$TARGET_HUDI_VERSION"
    fi
    step_num=$((step_num + 1))
    run_step "mor_${phase}_compaction" "[${phase}] Step ${step_num}/${PHASE_TOTAL_STEPS}: MOR compaction (${compact_version})" \
      bash "${SCRIPT_DIR}/run_hudi_compaction.sh" \
        --table-type "$TABLE_TYPE" \
        --target-hudi-version "$compact_version" \
        --table-name-suffix "$phase"
    upload_write_perf_csv_to_s3

    step_num=$((step_num + 1))
    run_step "mor_${phase}_benchmark_post_compact" "[${phase}] Step ${step_num}/${PHASE_TOTAL_STEPS}: MOR read benchmark after compaction" \
      python3 "${SCRIPT_DIR}/run_benchmark_suite.py" \
        --table-type "$TABLE_TYPE" \
        --hudi-versions "$HUDI_VERSIONS" \
        --batch-id 5 \
        --table-name-suffix "$phase" \
        --output "$BENCHMARK_POST_COMPACT_CSV"
    upload_benchmark_csv_to_s3
  fi
}

if [[ "$DRY_RUN" == true ]]; then
  log_echo ""
  log_echo "[DRY RUN] Would execute: baseline (5 batches), experiment (5 batches), then comparison report"
fi

run_e2e_phase baseline
run_e2e_phase experiment

if [[ "$DRY_RUN" != true ]]; then
  log_equal
  log_info "  Baseline vs experiment comparison (read + write totals)"
  log_equal
  _cmp_args=(
    python3 "${SCRIPT_DIR}/scripts/compare_e2e_phases.py"
    --baseline-read "${BENCHMARK_REPORT_STEM}_baseline.csv"
    --experiment-read "${BENCHMARK_REPORT_STEM}_experiment.csv"
    --baseline-write "${WRITE_REPORT_STEM}_baseline.csv"
    --experiment-write "${WRITE_REPORT_STEM}_experiment.csv"
    --output "$COMPARISON_CSV"
  )
  if [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]; then
    _cmp_args+=(
      --baseline-read-post "${BENCHMARK_REPORT_STEM}_baseline_post_compact.csv"
      --experiment-read-post "${BENCHMARK_REPORT_STEM}_experiment_post_compact.csv"
    )
  fi
  "${_cmp_args[@]}" 2>&1 | tee -a "$LOG_FILE"
  _cmp_st="${PIPESTATUS[0]}"
  if [[ "$_cmp_st" -ne 0 ]]; then
    log_echo "⚠ Comparison step failed (exit $_cmp_st); check phase CSVs under ${REPORTS_DIR}"
  fi
fi

# Upload results and log to S3 (state and CSV already uploaded after each step / each benchmark)
if [[ "$DRY_RUN" != true ]]; then
  upload_benchmark_csv_to_s3
  upload_write_perf_csv_to_s3
  upload_comparison_csv_to_s3
  if [[ -f "$LOG_FILE" ]]; then
    S3_LOG_FILE="${S3_LOGS_DIR}/${LOG_RUN_ID}/$(basename "$LOG_FILE")"
    echo "Uploading log to S3: $S3_LOG_FILE"
    aws s3 cp "$LOG_FILE" "$S3_LOG_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
  fi
  echo "E2E state synced to S3 per phase after each step (baseline + experiment state files under ${BASE_PATH}/e2e_state/)"
fi

log_equal
echo "E2E performance test completed"
echo "Read benchmarks:  ${BENCHMARK_REPORT_STEM}_{baseline,experiment}.csv"
if [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]; then
  echo "Read (post-compact): ${BENCHMARK_REPORT_STEM}_{baseline,experiment}_post_compact.csv"
fi
echo "Write performance: ${WRITE_REPORT_STEM}_{baseline,experiment}.csv"
echo "Comparison report: $COMPARISON_CSV"
echo "Log file         : $LOG_FILE"
log_equal
