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