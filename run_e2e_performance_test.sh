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

echo "=============================================="
echo "  E2E Hudi Performance Test"
echo "=============================================="
echo "  Table type          : $TABLE_TYPE"
echo "  Target Hudi version : $TARGET_HUDI_VERSION"
echo "  Benchmark versions  : $HUDI_VERSIONS"
echo "  Dry run             : $DRY_RUN"
echo "==============================================""

run_step() {
  local step_name="$1"
  shift
  echo ""
  echo ">>> $step_name"
  echo "    $*"
  if [[ "$DRY_RUN" != true ]]; then
    "$@"
  fi
}

if [[ "$DRY_RUN" == true ]]; then
echo ""
echo "[DRY RUN] Would execute the following steps:"
fi

# ---------------------------------------------------------------------------
# 1. Initial parquet ingestion
# ---------------------------------------------------------------------------
run_step "Step 1/9: Initial parquet ingestion" \
  bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" --type initial

# ---------------------------------------------------------------------------
# 2. Hudi ingestion (initial load)
# ---------------------------------------------------------------------------
run_step "Step 2/9: Hudi ingestion (initial load)" \
  bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" --table-type "$TABLE_TYPE" --target-hudi-version "$SOURCE_HUDI_VERSION"

# ---------------------------------------------------------------------------
# 3. Benchmark (after initial load)
# ---------------------------------------------------------------------------
run_step "Step 3/9: Benchmark (after initial load)" \
  python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --table-type "$TABLE_TYPE" --hudi-versions "$HUDI_VERSIONS"

# ---------------------------------------------------------------------------
# 4–5. First incremental cycle: generate data → Hudi ingestion → benchmark
# ---------------------------------------------------------------------------
run_step "Step 4/9: Incremental batch 1 - generate parquet data" \
  bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" --type incremental

run_step "Step 5/9: Incremental batch 1 - Hudi ingestion" \
  bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" --table-type "$TABLE_TYPE" --target-hudi-version "$TARGET_HUDI_VERSION"

run_step "Step 6/9: Benchmark (after incremental batch 1)" \
  python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --table-type "$TABLE_TYPE" --hudi-versions "$HUDI_VERSIONS"

# ---------------------------------------------------------------------------
# 6–7. Second incremental cycle: generate data → Hudi ingestion → benchmark
# ---------------------------------------------------------------------------
run_step "Step 7/9: Incremental batch 2 - generate parquet data" \
  bash "${SCRIPT_DIR}/run_parquet_ingestion.sh" --type incremental

run_step "Step 8/9: Incremental batch 2 - Hudi ingestion" \
  bash "${SCRIPT_DIR}/run_hudi_ingestion.sh" --table-type "$TABLE_TYPE" --target-hudi-version "$TARGET_HUDI_VERSION"

run_step "Step 9/9: Benchmark (after incremental batch 2)" \
  python3 "${SCRIPT_DIR}/run_benchmark_suite.py" --table-type "$TABLE_TYPE" --hudi-versions "$HUDI_VERSIONS"

echo ""
echo "=============================================="
echo "  ✅ E2E performance test completed"
echo "Report available at: ${SCRIPT_DIR}/hudi_benchmark_results.csv"
echo "=============================================="
