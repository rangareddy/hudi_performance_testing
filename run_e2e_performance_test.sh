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

if [[ "${ENABLE_WRITE_OPERATIONS:-true}" == false ]] && [[ "${ENABLE_READ_OPERATIONS:-true}" == false ]]; then
  log_error "❌ ENABLE_WRITE_OPERATIONS and ENABLE_READ_OPERATIONS cannot both be false"
  exit 1
fi

TARGET_VERSION=$(echo "${TARGET_HUDI_VERSION}" | cut -d '.' -f 1,2 | tr -d '.')
TABLE_TYPE_LOWER=$(echo "$TABLE_TYPE" | tr '[:upper:]' '[:lower:]')
# DATA_SHAPE_TAG from load_config: ${NUM_OF_COLUMNS}_${NUM_OF_PARTITIONS}_${NUM_OF_RECORDS_PER_PARTITION}
DATA_SHAPE_TAG="${DATA_SHAPE_TAG:-${NUM_OF_COLUMNS}_${NUM_OF_PARTITIONS}_${NUM_OF_RECORDS_PER_PARTITION:-1}}"
E2E_STATE_DIR="${SCRIPT_DIR}/.e2e_state/${DATA_SHAPE_TAG}"
S3_LOGS_DIR="${BASE_PATH}/logs"
REPORTS_DIR="${SCRIPT_DIR}/reports"
REPORTS_SHAPE_ROOT="${REPORTS_DIR}/${DATA_SHAPE_TAG}"
REPORTS_READ_DIR="${REPORTS_SHAPE_ROOT}/read"
REPORTS_WRITE_DIR="${REPORTS_SHAPE_ROOT}/write"
# Per-phase state/sync (set in run_e2e_phase)
E2E_STATE_FILE=""
S3_STATE_FILE=""

mkdir -p "$E2E_STATE_DIR" "$REPORTS_READ_DIR" "$REPORTS_WRITE_DIR"

# Read benchmark CSV: reports/<cols>_<parts>_<rpp>/read/hudi_benchmark_results_<cow|mor>_...
# Write performance CSV: reports/<shape>/write/hudi_write_performance_<cow|mor>_...
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
BENCHMARK_REPORT_STEM="${REPORTS_READ_DIR}/hudi_benchmark_results_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}_${DATA_SHAPE_TAG}"
WRITE_REPORT_STEM="${REPORTS_WRITE_DIR}/hudi_write_performance_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}_${DATA_SHAPE_TAG}"
COMPARISON_CSV="${REPORTS_SHAPE_ROOT}/e2e_baseline_vs_experiment_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}_${DATA_SHAPE_TAG}.csv"
BENCHMARK_CSV_PATH=""
WRITE_PERF_CSV=""
export TABLE_TYPE IS_LOGICAL_TIMESTAMP_ENABLED

# Read benchmarks: iterations per suite step (common.properties READ_PERFORMANCE_ITERATIONS; min 1).
READ_PERF_ITERATIONS="${READ_PERFORMANCE_ITERATIONS:-${read_performance_iterations:-1}}"
if ! [[ "${READ_PERF_ITERATIONS}" =~ ^[0-9]+$ ]] || [[ "${READ_PERF_ITERATIONS}" -lt 1 ]]; then
  READ_PERF_ITERATIONS=1
fi
export READ_PERFORMANCE_ITERATIONS="${READ_PERF_ITERATIONS}"
export read_performance_iterations="${READ_PERF_ITERATIONS}"

# Log file: logs/<YYYYMMDD>/<cols>_<parts>_<rpp>/e2e_<table>_v<ver>_<lts>_....log
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_RUN_ID="$(date +%Y%m%d)"
LOG_SUBDIR="${LOG_DIR}/${LOG_RUN_ID}/${DATA_SHAPE_TAG}"
mkdir -p "$LOG_SUBDIR"
LOG_FILE="${LOG_SUBDIR}/e2e_${TABLE_TYPE_LOWER}_v${TARGET_VERSION}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${DATA_SHAPE_TAG}.log"
STEP_TIMINGS_CSV="${LOG_SUBDIR}/e2e_${TABLE_TYPE_LOWER}_v${TARGET_VERSION}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${DATA_SHAPE_TAG}_step_timings.csv"

# Create log file and write all output to it (and console) via helper
: > "$LOG_FILE"
log_echo() { printf '%s\n' "$*" | tee -a "$LOG_FILE"; }
log_run() { "$@" 2>&1 | tee -a "$LOG_FILE"; return "${PIPESTATUS[0]}"; }

# Per-step timing log (CSV): written even if the main run fails mid-way.
# start_time_utc / end_time_utc are human-readable UTC timestamps (not epoch).
_init_step_timings_csv() {
  if [[ -f "$STEP_TIMINGS_CSV" ]]; then
    return 0
  fi
  printf '%s\n' "run_id,table_type,is_logical_timestamp,phase,step_id,step_name,status,start_time_utc,end_time_utc,duration_seconds,command" > "$STEP_TIMINGS_CSV"
}

_format_step_time_utc() {
  local epoch="$1"
  if [[ "${OSTYPE:-}" == darwin* ]]; then
    date -u -r "$epoch" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || printf '%s' "$epoch"
  else
    date -u -d "@$epoch" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || printf '%s' "$epoch"
  fi
}

_csv_escape() {
  local s="${1:-}"
  s="${s//\"/\"\"}"
  printf '"%s"' "$s"
}

_log_step_timing() {
  local phase="$1"
  local step_id="$2"
  local step_name="$3"
  local st="$4"
  local start_epoch="$5"
  local end_epoch="$6"
  local dur="$7"
  shift 7
  local cmd_str="$*"
  local start_h end_h
  start_h="$(_format_step_time_utc "$start_epoch")"
  end_h="$(_format_step_time_utc "$end_epoch")"
  _init_step_timings_csv
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$LOG_RUN_ID" \
    "$TABLE_TYPE" \
    "$IS_LOGICAL_TIMESTAMP_ENABLED" \
    "$phase" \
    "$step_id" \
    "$(_csv_escape "$step_name")" \
    "$st" \
    "$(_csv_escape "$start_h")" \
    "$(_csv_escape "$end_h")" \
    "$dur" \
    "$(_csv_escape "$cmd_str")" \
    >> "$STEP_TIMINGS_CSV"
}

echo "Log file: $LOG_FILE"
echo "Step timings: $STEP_TIMINGS_CSV"
E2E_START_TS=$(date +%s)
trap 'E2E_SCRIPT_EXIT_CODE=$?; _e2e_exit_finalize' EXIT

log_equal
log_info "  E2E Hudi Performance Test"
log_info "  Table type          : $TABLE_TYPE"
log_info "  Source Hudi version : $SOURCE_HUDI_VERSION"
log_info "  Target Hudi version : $TARGET_HUDI_VERSION"
log_info "  ENABLE_WRITE_OPS    : ${ENABLE_WRITE_OPERATIONS} (parquet + Hudi ingest; MOR compaction when write on)"
log_info "  ENABLE_READ_OPS     : ${ENABLE_READ_OPERATIONS} (read benchmarks; MOR post-compact read only if write+read on)"
log_info "  Phases              : baseline then experiment (steps depend on flags above)"
log_info "  Read iterations     : ${READ_PERF_ITERATIONS} when reads enabled (READ_PERFORMANCE_ITERATIONS in common.properties)"
log_info "  Data shape tag      : ${DATA_SHAPE_TAG} (NUM_OF_COLUMNS_NUM_OF_PARTITIONS_NUM_OF_RECORDS_PER_PARTITION)"
log_info "  DATA_PATH / SOURCE  : ${DATA_PATH} / ${SOURCE_DATA}"
log_info "  Reports / state     : ${REPORTS_SHAPE_ROOT} / ${E2E_STATE_DIR}"
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
  if [[ -f "$E2E_STATE_FILE" ]] && [[ "$DRY_RUN" != true ]] && [[ "${IS_LOCAL_RUN:-false}" != "true" ]]; then
    aws s3 cp "$E2E_STATE_FILE" "$S3_STATE_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
  fi
}

upload_benchmark_csv_to_s3() {
  if [[ "$DRY_RUN" == true ]] || [[ "${IS_LOCAL_RUN:-false}" == "true" ]]; then
    return 0
  fi
  local f
  for f in "${REPORTS_READ_DIR}"/hudi_benchmark_results*.csv; do
    if [[ -f "$f" ]]; then
      echo "Uploading $(basename "$f") to S3: ${BASE_PATH}/reports/${DATA_SHAPE_TAG}/read/$(basename "$f")"
      aws s3 cp "$f" "${BASE_PATH}/reports/${DATA_SHAPE_TAG}/read/$(basename "$f")" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
    fi
  done
}

upload_write_perf_csv_to_s3() {
  if [[ "$DRY_RUN" == true ]] || [[ "${IS_LOCAL_RUN:-false}" == "true" ]]; then
    return 0
  fi
  local f
  for f in "${REPORTS_WRITE_DIR}"/hudi_write_performance*.csv; do
    if [[ -f "$f" ]]; then
      echo "Uploading write performance $(basename "$f") to S3: ${BASE_PATH}/reports/${DATA_SHAPE_TAG}/write/$(basename "$f")"
      aws s3 cp "$f" "${BASE_PATH}/reports/${DATA_SHAPE_TAG}/write/$(basename "$f")" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
    fi
  done
}

upload_comparison_csv_to_s3() {
  if [[ "$DRY_RUN" == true ]] || [[ "${IS_LOCAL_RUN:-false}" == "true" ]]; then
    return 0
  fi
  local f
  for f in "${REPORTS_SHAPE_ROOT}"/e2e_baseline_vs_experiment*.csv; do
    if [[ -f "$f" ]]; then
      echo "Uploading comparison $(basename "$f") to S3: ${BASE_PATH}/reports/${DATA_SHAPE_TAG}/$(basename "$f")"
      aws s3 cp "$f" "${BASE_PATH}/reports/${DATA_SHAPE_TAG}/$(basename "$f")" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
    fi
  done
}

upload_e2e_log_to_s3() {
  if [[ "$DRY_RUN" == true ]] || [[ "${IS_LOCAL_RUN:-false}" == "true" ]]; then
    return 0
  fi
  if [[ ! -f "${LOG_FILE:-}" ]]; then
    return 0
  fi
  local s3_log_file="${S3_LOGS_DIR}/${LOG_RUN_ID}/${DATA_SHAPE_TAG}/$(basename "$LOG_FILE")"
  echo "Uploading log to S3: $s3_log_file"
  aws s3 cp "$LOG_FILE" "$s3_log_file" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true

  if [[ -f "${STEP_TIMINGS_CSV:-}" ]]; then
    local s3_timings="${S3_LOGS_DIR}/${LOG_RUN_ID}/${DATA_SHAPE_TAG}/$(basename "$STEP_TIMINGS_CSV")"
    echo "Uploading step timings to S3: $s3_timings"
    aws s3 cp "$STEP_TIMINGS_CSV" "$s3_timings" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
  fi
}

_format_e2e_elapsed() {
  local d=$1
  if [[ "${OSTYPE:-}" == darwin* ]]; then
    date -u -r "$d" +%H:%M:%S 2>/dev/null || printf '%ss\n' "$d"
  else
    date -u -d "@$d" +%H:%M:%S 2>/dev/null || printf '%ss\n' "$d"
  fi
}

# Runs on every exit (success or failure) so S3 uploads and timing still happen if set -e stops the script mid-run.
_e2e_exit_finalize() {
  if [[ -z "${LOG_FILE:-}" || ! -f "$LOG_FILE" ]]; then
    return 0
  fi
  local end_ts dur fmt
  end_ts=$(date +%s)
  if [[ -n "${E2E_START_TS:-}" ]]; then
    dur=$((end_ts - E2E_START_TS))
  else
    dur=0
  fi
  fmt=$(_format_e2e_elapsed "$dur")
  log_echo "Total wall time (run_e2e_performance_test.sh): ${fmt} (${dur}s)"
  if [[ "${E2E_SCRIPT_EXIT_CODE:-0}" -ne 0 ]]; then
    log_echo "Exiting with status ${E2E_SCRIPT_EXIT_CODE} (uploading artifacts to S3 where applicable)"
  fi

  if [[ "$DRY_RUN" == true ]]; then
    return 0
  fi
  if [[ "${IS_LOCAL_RUN:-false}" == "true" ]]; then
    log_info "IS_LOCAL_RUN=true: skipping benchmark/write/comparison CSV and log uploads to S3 (local log: $LOG_FILE)"
    echo "IS_LOCAL_RUN=true: E2E state and reports kept locally under ${SCRIPT_DIR}/.e2e_state/${DATA_SHAPE_TAG}/ and ${REPORTS_SHAPE_ROOT}/"
    return 0
  fi

  upload_benchmark_csv_to_s3
  upload_write_perf_csv_to_s3
  upload_comparison_csv_to_s3
  upload_e2e_log_to_s3
  echo "E2E state synced to S3 per phase after each step (under ${BASE_PATH}/e2e_state/${DATA_SHAPE_TAG}/)"
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
    _log_step_timing "${phase:-}" "$step_id" "$step_name" "skipped" "$(date -u +%s)" "$(date -u +%s)" "0" "$@"
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
    _log_step_timing "${phase:-}" "$step_id" "$step_name" "dry_run" "$(date -u +%s)" "$(date -u +%s)" "0" "$@"
    return 0
  fi
  local _st _et _dur
  _st=$(date -u +%s)
  if log_run "$@"; then
    set_step_status "$step_id" "success"
    log_echo "✅ Step $step_id: Success"
    _et=$(date -u +%s)
    _dur=$((_et - _st))
    _log_step_timing "${phase:-}" "$step_id" "$step_name" "success" "$_st" "$_et" "$_dur" "$@"
  else
    set_step_status "$step_id" "failure"
    log_echo "❌ Step $step_id: Failed (state saved; will retry this step next run)"
    _et=$(date -u +%s)
    _dur=$((_et - _st))
    _log_step_timing "${phase:-}" "$step_id" "$step_name" "failure" "$_st" "$_et" "$_dur" "$@"
    return 1
  fi
}

run_e2e_phase() {
  local phase="$1"
  local phase_upper
  phase_upper=$(echo "$phase" | tr '[:lower:]' '[:upper:]')
  E2E_STATE_FILE="${E2E_STATE_DIR}/state_${phase}_${TABLE_TYPE_LOWER}_${IS_LOGICAL_TIMESTAMP_ENABLED}_v${TARGET_VERSION}.txt"
  S3_STATE_FILE="${BASE_PATH}/e2e_state/${DATA_SHAPE_TAG}/state_${phase}_${TABLE_TYPE_LOWER}_${IS_LOGICAL_TIMESTAMP_ENABLED}_v${TARGET_VERSION}.txt"
  BENCHMARK_CSV_PATH="${BENCHMARK_REPORT_STEM}_${phase}.csv"
  WRITE_PERF_CSV="${WRITE_REPORT_STEM}_${phase}.csv"
  export WRITE_PERF_CSV

  local BENCH_HUDI_VERSIONS
  if [[ "$phase" == "baseline" ]]; then
    BENCH_HUDI_VERSIONS="$SOURCE_HUDI_VERSION"
  else
    BENCH_HUDI_VERSIONS="$TARGET_HUDI_VERSION"
  fi

  log_equal
  log_info "  Phase: ${phase_upper}"
  if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
    if [[ "$phase" == "baseline" ]]; then
      log_info "  Hudi ingestion: SOURCE_HUDI_VERSION only (${SOURCE_HUDI_VERSION}) for all 5 batches"
    else
      log_info "  Hudi ingestion: SOURCE (${SOURCE_HUDI_VERSION}) for batches 0–2; TARGET (${TARGET_HUDI_VERSION}) for batches 3–4"
    fi
  else
    log_info "  Write path: skipped (ENABLE_WRITE_OPERATIONS=false)"
  fi
  if [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
    if [[ "$TABLE_TYPE" == "COPY_ON_WRITE" ]]; then
      log_info "  Read benchmark: ${READ_PERF_ITERATIONS} full-table run(s) (${BENCH_HUDI_VERSIONS})"
    else
      if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
        log_info "  Read benchmarks: ${READ_PERF_ITERATIONS} before compaction + ${READ_PERF_ITERATIONS} after (${BENCH_HUDI_VERSIONS})"
      else
        log_info "  Read benchmarks: ${READ_PERF_ITERATIONS} before compaction only (no compaction without write path)"
      fi
    fi
  else
    log_info "  Read path: skipped (ENABLE_READ_OPERATIONS=false)"
  fi
  log_equal

  if [[ "${IS_LOCAL_RUN:-false}" != "true" ]]; then
    if aws s3 ls "$S3_STATE_FILE" &>/dev/null; then
      echo "Downloading ${phase} state from S3: $S3_STATE_FILE"
      aws s3 cp "$S3_STATE_FILE" "$E2E_STATE_FILE" --only-show-errors 2>&1 | tee -a "$LOG_FILE" || true
    fi
  fi

  # Phase-level gate: skip the whole phase when previously completed (batch/stage keys still in file for audit).
  if [[ "$DRY_RUN" != true ]] && [[ "$FORCE" != true ]]; then
    local _phase_done
    _phase_done=$(get_step_status "phase_completeness")
    if [[ "$_phase_done" == "success" ]]; then
      log_equal
      log_echo "Phase ${phase_upper}: phase_completeness=success — skipping all steps for this phase (use --force to rerun)."
      log_equal
      return 0
    fi
  fi

  local step_num=0
  local TOTAL_BATCHES=5
  local INGEST_STEPS=$((TOTAL_BATCHES * 2))
  local READ_STEP_COUNT="${READ_PERF_ITERATIONS}"
  local PHASE_TOTAL_STEPS=0
  if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
    PHASE_TOTAL_STEPS=$((PHASE_TOTAL_STEPS + INGEST_STEPS))
  fi
  if [[ "$TABLE_TYPE" == "COPY_ON_WRITE" ]]; then
    [[ "$ENABLE_READ_OPERATIONS" == true ]] && PHASE_TOTAL_STEPS=$((PHASE_TOTAL_STEPS + READ_STEP_COUNT))
  else
    if [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
      PHASE_TOTAL_STEPS=$((PHASE_TOTAL_STEPS + READ_STEP_COUNT))
    fi
    if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
      PHASE_TOTAL_STEPS=$((PHASE_TOTAL_STEPS + 1))
    fi
    if [[ "$ENABLE_READ_OPERATIONS" == true ]] && [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
      PHASE_TOTAL_STEPS=$((PHASE_TOTAL_STEPS + READ_STEP_COUNT))
    fi
  fi

  # One run_sequence for all iterations in a logical read block; one python invocation per iteration (versions in one call).
  _e2e_read_benchmark_iterations() {
    local _label="$1" _batch="$2" _out="$3" _suffix="$4" _versions="$5" _id="$6"
    local _seq
    if [[ "$DRY_RUN" == true ]]; then
      _seq=0
    else
      _seq=$(python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --allocate-run-sequence-only) || return 1
    fi
    local _it
    for ((_it = 1; _it <= READ_PERF_ITERATIONS; _it++)); do
      step_num=$((step_num + 1))
      run_step "step${step_num}_${_id}_i${_it}" "[${phase}] Step ${step_num}/${PHASE_TOTAL_STEPS}: ${_label} iteration ${_it}/${READ_PERF_ITERATIONS} (${_versions})" \
        python3 "${SCRIPT_DIR}/run_benchmark_suite.py" \
          --table-type "$TABLE_TYPE" \
          --hudi-versions "$_versions" \
          --batch-id "$_batch" \
          --iteration "$_it" \
          --run-sequence "$_seq" \
          --read-performance-iterations "${READ_PERF_ITERATIONS}" \
          --table-name-suffix "$_suffix" \
          --output "$_out"
    done
  }
  local BATCH_ID
  local job_type
  local run_hudi_version

  if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
    for ((BATCH_ID=0; BATCH_ID<TOTAL_BATCHES; BATCH_ID++)); do
      start_time=$(date +%s)
      log_info "$(log_hyphen)"
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
      run_step "step${step_num}_${job_type}_${BATCH_ID}_parquet" "[${phase}] Step ${step_num}/${PHASE_TOTAL_STEPS}: ${job_type} batch $BATCH_ID - parquet" \
      bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" \
        --type $job_type \
        --batch-id $BATCH_ID

      step_num=$((step_num + 1))
      run_step "step${step_num}_${job_type}_${BATCH_ID}_hudi" "[${phase}] Step ${step_num}/${PHASE_TOTAL_STEPS}: Hudi ${job_type} (${run_hudi_version}) batch $BATCH_ID" \
      bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" \
        --table-type "$TABLE_TYPE" \
        --target-hudi-version "$run_hudi_version" \
        --batch-id $BATCH_ID \
        --table-name-suffix "$phase"

      upload_write_perf_csv_to_s3

      end_time=$(date +%s)
      duration=$((end_time - start_time))
      if [[ "$OSTYPE" == "darwin"* ]]; then
        duration_formatted=$(date -u -r "$duration" +%H:%M:%S 2>/dev/null || echo "${duration}s")
      else
        duration_formatted=$(date -u -d "@$duration" +%H:%M:%S 2>/dev/null || echo "${duration}s")
      fi

      log_info "[${phase}] Batch $BATCH_ID ingest (parquet + Hudi) completed in $duration_formatted"
      log_info "$(log_hyphen)"
    done
  fi

  if [[ "$TABLE_TYPE" == "COPY_ON_WRITE" ]]; then
    _e2e_read_benchmark_iterations "Full-table read benchmark" 0 "$BENCHMARK_CSV_PATH" "$phase" "$BENCH_HUDI_VERSIONS" "post_ingest_read" || return 1
  elif [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]] && [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
    _e2e_read_benchmark_iterations "MOR read before compaction" 0 "$BENCHMARK_CSV_PATH" "$phase" "$BENCH_HUDI_VERSIONS" "read_before_compact" || return 1
  fi
  if [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
    upload_benchmark_csv_to_s3
  fi
  if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
    upload_write_perf_csv_to_s3
  fi

  if [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]] && [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
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

    if [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
      _e2e_read_benchmark_iterations "MOR read after compaction" 5 "$BENCHMARK_CSV_PATH" "$phase" "$BENCH_HUDI_VERSIONS" "benchmark_post_compact" || return 1
      upload_benchmark_csv_to_s3
    fi
  fi

  # Phase-level: mark complete only after every step above succeeded (set -e would have exited otherwise).
  if [[ "$DRY_RUN" != true ]]; then
    set_step_status "phase_completeness" "success"
    log_echo "Phase ${phase_upper}: phase_completeness=success"
  fi
}

if [[ "$DRY_RUN" == true ]]; then
  log_echo ""
  log_echo "[DRY RUN] write=${ENABLE_WRITE_OPERATIONS} read=${ENABLE_READ_OPERATIONS} — baseline + experiment phases; comparison only if both true"
fi

run_e2e_phase baseline
run_e2e_phase experiment

if [[ "$DRY_RUN" != true ]]; then
  if [[ "$ENABLE_WRITE_OPERATIONS" == true ]] && [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
    log_equal
    log_info "  Baseline vs experiment comparison (read + write totals)"
    log_equal
    _cmp_args=(
      python3 "${SCRIPT_DIR}/scripts/compare_e2e_phases.py"
      --baseline-read "${BENCHMARK_REPORT_STEM}_baseline.csv"
      --experiment-read "${BENCHMARK_REPORT_STEM}_experiment.csv"
      --baseline-write "${WRITE_REPORT_STEM}_baseline.csv"
      --experiment-write "${WRITE_REPORT_STEM}_experiment.csv"
      --baseline-hudi-version "${SOURCE_HUDI_VERSION}"
      --experiment-hudi-version "${TARGET_HUDI_VERSION}"
      --output "$COMPARISON_CSV"
    )
    # MOR post-compaction read rows (batch_id 5) live in the same benchmark CSV as pre-compact reads.
    "${_cmp_args[@]}" 2>&1 | tee -a "$LOG_FILE"
    _cmp_st="${PIPESTATUS[0]}"
    if [[ "$_cmp_st" -ne 0 ]]; then
      log_echo "⚠ Comparison step failed (exit $_cmp_st); check phase CSVs under ${REPORTS_SHAPE_ROOT}"
    fi
  else
    log_equal
    log_info "  Skipping baseline vs experiment comparison (requires ENABLE_WRITE_OPERATIONS=true and ENABLE_READ_OPERATIONS=true)"
    log_equal
  fi
fi

# Final S3 uploads, log upload, and wall-clock duration run in EXIT trap (_e2e_exit_finalize) so they still run after errors.

log_info "$(log_equal)"
echo "E2E performance test completed"
if [[ "$ENABLE_READ_OPERATIONS" == true ]]; then
  if [[ "$TABLE_TYPE" == "MERGE_ON_READ" ]]; then
    echo "Read benchmarks:  ${BENCHMARK_REPORT_STEM}_{baseline,experiment}.csv (MOR may include batch_id=5 post-compaction when write+read enabled)"
  else
    echo "Read benchmarks:  ${BENCHMARK_REPORT_STEM}_{baseline,experiment}.csv"
  fi
else
  echo "Read benchmarks:  (skipped — ENABLE_READ_OPERATIONS=false)"
fi
if [[ "$ENABLE_WRITE_OPERATIONS" == true ]]; then
  echo "Write performance: ${WRITE_REPORT_STEM}_{baseline,experiment}.csv"
else
  echo "Write performance: (skipped — ENABLE_WRITE_OPERATIONS=false)"
fi
# compare_e2e_phases.py appends _<baseline_hudi>_vs_<experiment_hudi> before .csv
shopt -s nullglob
_cmp_prefix="${REPORTS_SHAPE_ROOT}/e2e_baseline_vs_experiment_${BENCHMARK_TABLE_SUFFIX}_${IS_LOGICAL_TIMESTAMP_ENABLED}_${BENCHMARK_VERSION_SUFFIX}_${DATA_SHAPE_TAG}"
_cmp_written=("${_cmp_prefix}"*.csv)
if ((${#_cmp_written[@]})); then
  for _cf in "${_cmp_written[@]}"; do
    echo "Comparison report: ${_cf}"
  done
else
  echo "Comparison report: (expected ${_cmp_prefix}_<ver>_vs_<ver>.csv and _per_batch_tables.csv)"
fi
shopt -u nullglob
echo "Log file         : $LOG_FILE"
log_info "$(log_equal)"
