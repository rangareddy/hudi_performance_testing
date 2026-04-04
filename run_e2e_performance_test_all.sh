#!/usr/bin/env bash
#
# Run end-to-end Hudi performance test for both COPY_ON_WRITE and MERGE_ON_READ table types.
# Delegates to run_e2e_performance_test.sh for each table type sequentially.
#
# Usage:
#   bash run_e2e_performance_test_all.sh
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