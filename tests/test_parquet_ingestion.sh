#!/usr/bin/env bash
#
# Tests for run_parquet_ingestion.sh (CLI validation, Avro/NUM_OF_COLUMNS checks, skip-existing, missing deps).
# Does not run Spark jobs.
#
# Usage:
#   bash tests/test_parquet_ingestion.sh
#

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PARQUET_SCRIPT="$ROOT/run_parquet_ingestion.sh"
TMP_BASE="$(mktemp -d "${TMPDIR:-/tmp}/hudi_parquet_ingest_test.XXXXXX")"

cleanup() {
  rm -rf "$TMP_BASE"
}
trap cleanup EXIT

PASS=0
FAIL=0

assert_eq() {
  local test_name="$1"
  local expected="$2"
  local actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $test_name"
    echo "      expected: '$expected'"
    echo "      actual  : '$actual'"
    FAIL=$((FAIL + 1))
  fi
}

assert_contains() {
  local test_name="$1"
  local substring="$2"
  local actual="$3"
  if [[ "$actual" == *"$substring"* ]]; then
    echo "PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $test_name"
    echo "      expected to contain: '$substring'"
    echo "      actual             : '$actual'"
    FAIL=$((FAIL + 1))
  fi
}

# Minimal properties: absolute paths so CONFIG_FILE in /tmp still resolves Spark/script paths.
# $1 = config path, $2 = SOURCE_DATA directory (created if missing)
write_minimal_config() {
  local cfg="$1"
  local src="$2"
  mkdir -p "$src" "$TMP_BASE/fake_spark"
  cat > "$cfg" << EOF
SOURCE_HUDI_VERSION=0.14.1
TARGET_HUDI_VERSION=0.14.2-SNAPSHOT
SPARK_VERSION=3.4.4
SCALA_VERSION=2.12
HADOOP_VERSION=3.3.4
AWS_JAVA_SDK_BUNDLE_VERSION=1.12.262
IS_LOGICAL_TIMESTAMP_ENABLED=true
IS_LOCAL_RUN=true
SKIP_DATA_PATH_SHAPE_SUFFIX=1
DEST_DIR=$ROOT
SPARK_HOME=$TMP_BASE/fake_spark
BASE_PATH=$TMP_BASE/base
DATA_PATH=$TMP_BASE/data
SOURCE_DATA=$src
INITIAL_BATCH_SCALA=$ROOT/scripts/initial_batch.scala
INCREMENTAL_SCRIPT=$ROOT/scripts/incremental_batch.py
PY_SCRIPT=$ROOT/scripts/hudi_benchmark.py
PROPS_FILE=$ROOT/scripts/hoodie.properties
SPARK_DEFAULTS_CONF=$ROOT/scripts/spark-defaults.conf
NUM_OF_COLUMNS=500
NUM_OF_PARTITIONS=100
NUM_OF_RECORDS_PER_PARTITION=10
NUM_OF_RECORDS_TO_UPDATE=100
BASE_TABLE_NAME=hudi_test
EOF
}

run_parquet() {
  env CONFIG_FILE="$CONFIG_FILE" \
    SKIP_SPARK_HOME_CHECK=1 \
    IS_EMR_CLUSTER=false \
    IS_LOCAL_RUN=true \
    bash "$PARQUET_SCRIPT" "$@" 2>&1
}

echo ""
echo "=== run_parquet_ingestion.sh tests ==="

# --- missing --batch-id (after load_config) ---
CFG="$TMP_BASE/cfg1.properties"
write_minimal_config "$CFG" "$TMP_BASE/src1"
export CONFIG_FILE="$CFG"
set +e
out=$(run_parquet --type initial)
rc=$?
set -e
assert_eq "exit 1 when --batch-id missing" "1" "$rc"
assert_contains "stderr mentions batch-id required" "--batch-id is required" "$out"

# --- invalid --batch-id ---
set +e
out=$(run_parquet --type initial --batch-id xyz)
rc=$?
set -e
assert_eq "exit 1 for non-numeric batch-id" "1" "$rc"
assert_contains "stderr mentions non-negative integer" "non-negative integer" "$out"

# --- invalid ingestion type ---
set +e
out=$(run_parquet --type bulk --batch-id 0)
rc=$?
set -e
assert_eq "exit 1 for invalid --type" "1" "$rc"
assert_contains "stderr mentions Invalid Ingestion Type" "Invalid Ingestion Type" "$out"

# --- unknown option ---
set +e
out=$(run_parquet --type initial --batch-id 0 --not-an-option)
rc=$?
set -e
assert_eq "exit 1 for unknown option" "1" "$rc"
assert_contains "stderr mentions Unknown option" "Unknown option" "$out"

# (SOURCE_DATA empty in properties is re-filled by load_config.sh from DATA_PATH, so the parquet
# script's "Source Data is not set" guard is not reachable via CONFIG_FILE alone.)

# --- NUM_OF_COLUMNS mismatch vs Avro schema ---
CFG="$TMP_BASE/cfg_bad_cols.properties"
write_minimal_config "$CFG" "$TMP_BASE/src_cols"
{
  grep -v '^NUM_OF_COLUMNS=' "$CFG" > "$CFG.tmp" || true
  echo "NUM_OF_COLUMNS=1" >> "$CFG.tmp"
  mv "$CFG.tmp" "$CFG"
}
export CONFIG_FILE="$CFG"
set +e
out=$(run_parquet --type initial --batch-id 0)
rc=$?
set -e
assert_eq "exit 1 when NUM_OF_COLUMNS mismatches schema" "1" "$rc"
assert_contains "stderr mentions Avro schema validation failed" "Avro schema validation failed" "$out"

# --- missing Avro schema file ---
CFG="$TMP_BASE/cfg_bad_schema.properties"
write_minimal_config "$CFG" "$TMP_BASE/src_schema"
{
  grep -v '^NUM_OF_COLUMNS=' "$CFG" > "$CFG.tmp"
  echo "NUM_OF_COLUMNS=500" >> "$CFG.tmp"
  echo "SCHEMA_FILE=$TMP_BASE/does_not_exist_schema.avsc" >> "$CFG.tmp"
  mv "$CFG.tmp" "$CFG"
}
export CONFIG_FILE="$CFG"
set +e
out=$(run_parquet --type initial --batch-id 0)
rc=$?
set -e
assert_eq "exit 1 when SCHEMA_FILE missing" "1" "$rc"
assert_contains "stderr mentions Avro schema file not found" "Avro schema file not found" "$out"

# --- skip when target batch already has data ---
CFG="$TMP_BASE/cfg_skip.properties"
SRC_SKIP="$TMP_BASE/src_skip"
write_minimal_config "$CFG" "$SRC_SKIP"
mkdir -p "$SRC_SKIP/batch_0"
touch "$SRC_SKIP/batch_0/.nonempty_marker"
export CONFIG_FILE="$CFG"
set +e
out=$(run_parquet --type initial --batch-id 0)
rc=$?
set -e
assert_eq "exit 0 when target parquet batch already exists" "0" "$rc"
assert_contains "stdout/skip message" "Target data already exists" "$out"

# --- missing initial_batch.scala path ---
CFG="$TMP_BASE/cfg_no_scala.properties"
write_minimal_config "$CFG" "$TMP_BASE/src_noscala"
{
  grep -v '^INITIAL_BATCH_SCALA=' "$CFG" > "$CFG.tmp"
  echo "INITIAL_BATCH_SCALA=$TMP_BASE/nonexistent_initial_batch.scala" >> "$CFG.tmp"
  mv "$CFG.tmp" "$CFG"
}
export CONFIG_FILE="$CFG"
set +e
out=$(run_parquet --type initial --batch-id 0)
rc=$?
set -e
assert_eq "exit 1 when execution script missing" "1" "$rc"
assert_contains "stderr mentions Execution script not found" "Execution script not found" "$out"

# --- Avro validation runs before Spark (log line; Spark may succeed or fail per environment) ---
CFG="$TMP_BASE/cfg_spark.properties"
write_minimal_config "$CFG" "$TMP_BASE/src_spark"
export CONFIG_FILE="$CFG"
set +e
out=$(run_parquet --type initial --batch-id 0)
set -e
assert_contains "schema validation log present" "Validated NUM_OF_COLUMNS=500 matches" "$out"
# Ensure we did not fail at Avro check (would say validation failed)
if [[ "$out" == *"Avro schema validation failed"* ]]; then
  echo "FAIL: unexpected Avro validation failure in happy-path config"
  FAIL=$((FAIL + 1))
else
  echo "PASS: Avro validation succeeded for default NUM_OF_COLUMNS + lts_schema"
  PASS=$((PASS + 1))
fi

echo ""
echo "============================="
echo "Results: $PASS passed, $FAIL failed"
echo "============================="
[[ "$FAIL" -eq 0 ]]
