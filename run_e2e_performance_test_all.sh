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
START_TS=$(date +%s)

_format_elapsed() {
  local d=$1
  if [[ "${OSTYPE:-}" == darwin* ]]; then
    date -u -r "$d" +%H:%M:%S 2>/dev/null || printf '%ss\n' "$d"
  else
    date -u -d "@$d" +%H:%M:%S 2>/dev/null || printf '%ss\n' "$d"
  fi
}

_print_total_time() {
  local end_ts dur
  end_ts=$(date +%s)
  dur=$((end_ts - START_TS))
  echo "Total wall time (run_e2e_performance_test_all.sh): $(_format_elapsed "$dur") (${dur}s)"
}

trap _print_total_time EXIT

echo "Running end to end performance test for COPY_ON_WRITE table type"
bash $SCRIPT_DIR/run_e2e_performance_test.sh --table-type COPY_ON_WRITE
if [[ $? -ne 0 ]]; then
  echo "Error: run_e2e_performance_test.sh --table-type COPY_ON_WRITE failed"
else
  echo "Success: run_e2e_performance_test.sh --table-type COPY_ON_WRITE succeeded"
fi

echo "Running end to end performance test for MERGE_ON_READ table type"
bash $SCRIPT_DIR/run_e2e_performance_test.sh --table-type MERGE_ON_READ
if [[ $? -ne 0 ]]; then
  echo "Error: run_e2e_performance_test.sh --table-type MERGE_ON_READ failed"
else
  echo "Success: run_e2e_performance_test.sh --table-type MERGE_ON_READ succeeded"
fi