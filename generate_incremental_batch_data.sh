#!/usr/bin/env bash
#
# Generate incremental batch data using incremental_batch.py.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=load_config.sh
source "${SCRIPT_DIR}/load_config.sh"

# So incremental_batch.py can read SOURCE_DFS_ROOT from env
export SOURCE_DFS_ROOT

echo "======================================"
echo "Generating incremental batch data"
echo "--------------------------------------"
echo "SPARK_HOME      : $SPARK_HOME"
echo "Script          : $INCREMENTAL_SCRIPT"
echo "SOURCE_DFS_ROOT : $SOURCE_DFS_ROOT"
echo "======================================"

"${SPARK_HOME}/bin/spark-submit" \
  --master yarn \
  --deploy-mode client \
  --properties-file "${SPARK_DEFAULTS_CONF}" \
  --conf spark.sql.adaptive.enabled=true \
  "$INCREMENTAL_SCRIPT"

echo "✅ Incremental batch data generation completed"
