#!/usr/bin/env bash
#
# End-to-end Hudi performance test:
#   1. One initial parquet ingestion
#   2. Hudi ingestion (initial load)
#   3. Run benchmark
#   4. Two incremental cycles: (generate incremental data → Hudi ingestion → benchmark) × 2
#
# Usage:
#   bash run_e2e_performance_test.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
#   bash run_e2e_performance_test.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2 --hudi-versions 0.14.1,0.14.2
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

usage() {
  echo ""
  echo "Usage:"
  echo "  bash $0 --table-type <COPY_ON_WRITE|MERGE_ON_READ> --target-hudi-version <0.14.1|0.14.2> [--hudi-versions 0.14.1,0.14.2] [--dry-run]"
  echo ""
  echo "Options:"
  echo "  --table-type           COPY_ON_WRITE or MERGE_ON_READ (for Hudi table and benchmark)."
  echo "  --target-hudi-version  Hudi version used for ingestion (e.g. 0.14.1 or 0.14.2)."
  echo "  --hudi-versions        Comma-separated versions for benchmark runs (default: SOURCE_HUDI_VERSION,TARGET_HUDI_VERSION from common.properties)."
  echo "  --dry-run              Print the plan only, do not run any step."
  echo "  --force               Ignore saved state and run all steps (default: skip steps that already succeeded)."
  echo ""
  echo "Flow (9 steps):"
  echo "  1. Initial parquet ingestion"
  echo "  2. Hudi ingestion (initial load)"
  echo "  3. Benchmark"
  echo "  4. Incremental batch 1: generate parquet data"
  echo "  5. Incremental batch 1: Hudi ingestion"
  echo "  6. Benchmark"
  echo "  7. Incremental batch 2: generate parquet data"
  echo "  8. Incremental batch 2: Hudi ingestion"
  echo "  9. Benchmark"
  echo ""
  exit 1
}

TABLE_TYPE=""
# Save config defaults before overwriting (for default --hudi-versions)
CONFIG_SOURCE_HUDI="${SOURCE_HUDI_VERSION}"
CONFIG_TARGET_HUDI="${TARGET_HUDI_VERSION}"
TARGET_HUDI_VERSION=""
HUDI_VERSIONS="${CONFIG_SOURCE_HUDI},${CONFIG_TARGET_HUDI}"
DRY_RUN=false
FORCE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      [[ -z "${2:-}" ]] && { echo "❌ Error: --table-type requires a value"; usage; }
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
      echo "❌ Unknown option: $1"
      usage
      ;;
  esac
done

if [[ -z "$TABLE_TYPE" ]]; then
  echo "❌ Error: --table-type is required"
  usage
fi

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')
case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW) TABLE_TYPE="COPY_ON_WRITE" ;;
  MERGE_ON_READ|MOR) TABLE_TYPE="MERGE_ON_READ" ;;
  *)
    echo "❌ Invalid --table-type: $TABLE_TYPE"
    usage
    ;;
esac

# State file per table type (avoid duplicate runs; retry only on failure)
TARGET_VERSION=$(echo "${TARGET_HUDI_VERSION}" | cut -d '.' -f 1,2 | tr -d '.')
TABLE_TYPE_LOWER=$(echo "$TABLE_TYPE" | tr '[:upper:]' '[:lower:]')
E2E_STATE_DIR="${SCRIPT_DIR}/.e2e_state"
E2E_STATE_FILE="${E2E_STATE_DIR}/state_${TABLE_TYPE_LOWER}_v${TARGET_VERSION}.txt"
S3_STATE_FILE="${BASE_PATH}/e2e_state/state_${TABLE_TYPE_LOWER}_v${TARGET_VERSION}.txt"
S3_CSV_FILE="${BASE_PATH}/hudi_benchmark_results.csv"
mkdir -p "$E2E_STATE_DIR"

# Log file: all output from here goes to log and console
LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/e2e_${TABLE_TYPE_LOWER}_v${TARGET_VERSION}_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee "$LOG_FILE") 2>&1
echo "Log file: $LOG_FILE"

# If state file exists on S3, download to local so we resume from last run
if aws s3 ls "$S3_STATE_FILE" &>/dev/null; then
  echo "Downloading existing state from S3: $S3_STATE_FILE"
  aws s3 cp "$S3_STATE_FILE" "$E2E_STATE_FILE" || true
fi

echo "=============================================="
echo "  E2E Hudi Performance Test"
echo "=============================================="
echo "  Table type          : $TABLE_TYPE"
echo "  Target Hudi version : $TARGET_HUDI_VERSION"
echo "  Benchmark versions  : $HUDI_VERSIONS"
echo "  Dry run             : $DRY_RUN"
echo "  State dir           : $E2E_STATE_DIR"
echo "=============================================="

get_step_status() {
  local step_id="$1"
  if [[ -f "$E2E_STATE_FILE" ]]; then
    local line
    line=$(grep -E "^${step_id}=" "$E2E_STATE_FILE" 2>/dev/null || true)
    [[ -n "$line" ]] && echo "${line#*=}" || echo ""
  else
    echo ""
  fi
}

set_step_status() {
  local step_id="$1"
  local status="$2"
  local tmp_file="${E2E_STATE_FILE}.tmp"
  if [[ -f "$E2E_STATE_FILE" ]]; then
    grep -v -E "^${step_id}=" "$E2E_STATE_FILE" 2>/dev/null > "$tmp_file" || true
  else
    : > "$tmp_file"
  fi
  echo "${step_id}=${status}" >> "$tmp_file"
  mv "$tmp_file" "$E2E_STATE_FILE"
}

run_step() {
  local step_id="$1"
  local step_name="$2"
  shift 2
  local status
  status=$(get_step_status "$step_id")
  if [[ "$status" == "success" ]]; then
    echo ""
    echo "--------------------------------------"
    echo ">>> $step_name [SKIPPED - already succeeded]"
    echo "--------------------------------------"
    return 0
  fi
  if [[ "$status" == "failure" ]]; then
    echo ""
    echo ">>> $step_name [RETRY - previous run failed]"
  else
    echo ""
    echo ">>> $step_name"
  fi
  echo "    $*"
  if [[ "$DRY_RUN" == true ]]; then
    return 0
  fi
  if "$@"; then
    set_step_status "$step_id" "success"
    echo "    ✅ Success (state saved)"
  else
    set_step_status "$step_id" "failure"
    echo "    ❌ Failed (state saved; will retry this step next run)"
    return 1
  fi
}

if [[ "$DRY_RUN" == true ]]; then
  echo ""
  echo "[DRY RUN] Would execute the following steps:"
fi

# ---------------------------------------------------------------------------
# 1. Initial parquet ingestion
# ---------------------------------------------------------------------------
run_step "step1_initial_parquet" "Step 1/9: Initial parquet ingestion" \
  bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" --type initial

# ---------------------------------------------------------------------------
# 2. Hudi ingestion (initial load)
# ---------------------------------------------------------------------------
run_step "step2_initial_hudi" "Step 2/9: Hudi ingestion - initial load" \
  bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" --table-type "$TABLE_TYPE" --target-hudi-version "$SOURCE_HUDI_VERSION"

# ---------------------------------------------------------------------------
# 3. Benchmark (after initial load)
# ---------------------------------------------------------------------------
run_step "step3_benchmark_initial" "Step 3/9: Benchmark - after initial load" \
  python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --table-type "$TABLE_TYPE" --hudi-versions "$HUDI_VERSIONS"

# ---------------------------------------------------------------------------
# 4–5. First incremental cycle: generate data → Hudi ingestion → benchmark
# ---------------------------------------------------------------------------
run_step "step4_incr1_parquet" "Step 4/9: Incremental batch 1 - generate parquet data" \
  bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" --type incremental

run_step "step5_incr1_hudi" "Step 5/9: Incremental batch 1 - Hudi ingestion" \
  bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" --table-type "$TABLE_TYPE" --target-hudi-version "$TARGET_HUDI_VERSION"

run_step "step6_benchmark_incr1" "Step 6/9: Benchmark - after incremental batch 1" \
  python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --table-type "$TABLE_TYPE" --hudi-versions "$HUDI_VERSIONS"

# ---------------------------------------------------------------------------
# 6–7. Second incremental cycle: generate data → Hudi ingestion → benchmark
# ---------------------------------------------------------------------------
run_step "step7_incr2_parquet" "Step 7/9: Incremental batch 2 - generate parquet data" \
  bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" --type incremental

run_step "step8_incr2_hudi" "Step 8/9: Incremental batch 2 - Hudi ingestion" \
  bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" --table-type "$TABLE_TYPE" --target-hudi-version "$TARGET_HUDI_VERSION"

run_step "step9_benchmark_incr2" "Step 9/9: Benchmark - after incremental batch 2" \
  python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --table-type "$TABLE_TYPE" --hudi-versions "$HUDI_VERSIONS"

# Upload results and state to S3 (if files exist)
if [[ "$DRY_RUN" != true ]]; then
  if [[ -f "${SCRIPT_DIR}/hudi_benchmark_results.csv" ]]; then
    echo ""
    echo "Uploading hudi_benchmark_results.csv to S3: $S3_CSV_FILE"
    aws s3 cp "${SCRIPT_DIR}/hudi_benchmark_results.csv" "$S3_CSV_FILE" || true
  fi
  if [[ -f "$E2E_STATE_FILE" ]]; then
    echo "Uploading E2E state to S3: $S3_STATE_FILE"
    aws s3 cp "$E2E_STATE_FILE" "$S3_STATE_FILE" || true
  fi
fi

echo ""
echo "=============================================="
echo "  ✅ E2E performance test completed"
echo "  Report  : ${SCRIPT_DIR}/hudi_benchmark_results.csv"
echo "  Log file: $LOG_FILE"
echo "=============================================="
