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
  local end_ts dur fmt
  end_ts=$(date +%s)
  dur=$((end_ts - START_TS))
  fmt=$(_format_elapsed "$dur")
  echo "=============================================================================" >&2
  echo "Total wall time (run_e2e_performance_test_all.sh): ${fmt} (${dur}s)" >&2
  echo "=============================================================================" >&2
}

trap _print_total_time EXIT

ALL_EXIT=0

echo "Running end to end performance test for COPY_ON_WRITE table type"
if bash "$SCRIPT_DIR/run_e2e_performance_test.sh" --table-type COPY_ON_WRITE; then
  echo "Success: run_e2e_performance_test.sh --table-type COPY_ON_WRITE succeeded"
else
  echo "Error: run_e2e_performance_test.sh --table-type COPY_ON_WRITE failed" >&2
  ALL_EXIT=1
fi

echo "Running end to end performance test for MERGE_ON_READ table type"
if bash "$SCRIPT_DIR/run_e2e_performance_test.sh" --table-type MERGE_ON_READ; then
  echo "Success: run_e2e_performance_test.sh --table-type MERGE_ON_READ succeeded"
else
  echo "Error: run_e2e_performance_test.sh --table-type MERGE_ON_READ failed" >&2
  ALL_EXIT=1
fi

exit "$ALL_EXIT"
