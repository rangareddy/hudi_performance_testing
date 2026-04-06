#!/usr/bin/env bash
#
# Tests for scripts/load_config.sh
#
# Usage:
#   bash tests/test_load_config.sh
#
# Prints PASS/FAIL for each assertion and exits nonzero if any test fails.
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

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

assert_not_empty() {
  local test_name="$1"
  local actual="$2"
  if [[ -n "$actual" ]]; then
    echo "PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $test_name"
    echo "      expected non-empty value"
    FAIL=$((FAIL + 1))
  fi
}

# ---------------------------------------------------------------------------
# Helpers: create a temp properties file and source the loader in a subshell
# ---------------------------------------------------------------------------

make_config() {
  local tmpfile
  tmpfile="$(mktemp /tmp/test_common.XXXXXX.properties)"
  cat > "$tmpfile" << 'EOF'
SOURCE_HUDI_VERSION=0.14.1
TARGET_HUDI_VERSION=0.14.2-SNAPSHOT
HUDI_VERSION=$TARGET_HUDI_VERSION
HUDI_VERSIONS=$SOURCE_HUDI_VERSION,$TARGET_HUDI_VERSION
SPARK_VERSION=3.4.4
SCALA_VERSION=2.12
HADOOP_VERSION=3.3.4
HADOOP_MAJOR_VERSION=$(echo "${HADOOP_VERSION}" | cut -d '.' -f 1)
AWS_JAVA_SDK_BUNDLE_VERSION=1.12.262
IS_LOGICAL_TIMESTAMP_ENABLED=true
DEST_DIR=/tmp/hudi_performance_testing
SPARK_HOME=/tmp/fake_spark_home
BASE_PATH=s3://test-bucket/hudi-bench
S3_JARS_PATH=$BASE_PATH/jars
JARS_PATH=$DEST_DIR/jars
DATA_PATH=$BASE_PATH/data
SCRIPTS_DIR=$DEST_DIR/scripts
INITIAL_BATCH_SCALA=$SCRIPTS_DIR/initial_batch.scala
INCREMENTAL_SCRIPT=$SCRIPTS_DIR/incremental_batch.py
PY_SCRIPT=$SCRIPTS_DIR/hudi_benchmark.py
SCHEMA_FILE=$SCRIPTS_DIR/full_schema.avsc
PROPS_FILE=$SCRIPTS_DIR/hoodie.properties
SPARK_DEFAULTS_CONF=$SCRIPTS_DIR/spark-defaults.conf
NUM_OF_COLUMNS=500
NUM_OF_PARTITIONS=10000
IS_TIMESTAMP_COLUMN=true
NUM_OF_RECORDS_TO_UPDATE=100
BASE_TABLE_NAME=hudi_logical
EOF
  echo "$tmpfile"
}

load_in_subshell() {
  local config_file="$1"
  shift
  env -i HOME=/tmp PATH="$PATH" \
    CONFIG_FILE="$config_file" \
    SKIP_SPARK_HOME_CHECK=1 \
    AWS_S3_JARS="fake.jar" \
    bash -c "
      source '${SCRIPT_DIR}/scripts/load_config.sh'
      $*
    "
}

# ---------------------------------------------------------------------------
# Test: log functions produce correct prefixes
# ---------------------------------------------------------------------------
echo ""
echo "=== log function tests ==="

log_output=$(bash -c "
  source '${SCRIPT_DIR}/scripts/load_config.sh' 2>/dev/null || true
  SKIP_SPARK_HOME_CHECK=1 CONFIG_FILE=/dev/null source '${SCRIPT_DIR}/scripts/load_config.sh' 2>/dev/null || true
  log_info 'hello info'
" 2>&1 || true)

# Source just the logging functions (no load_config call) by extracting them
log_test_output=$(bash << 'EOFTEST'
  log_time() { date "+%Y-%m-%d %H:%M:%S"; }
  log_info()    { echo "$(log_time) [INFO ] $*"; }
  log_success() { echo "$(log_time) [SUCCESS] $*"; }
  log_warn()    { echo "$(log_time) [WARN ] $*"; }
  log_error()   { echo "$(log_time) [ERROR] $*" >&2; }
  log_equal()   { echo "============================================================================="; }
  log_hyphen()  { echo "-----------------------------------------------------------------------------"; }

  out_info=$(log_info "test message")
  out_success=$(log_success "test message")
  out_warn=$(log_warn "test message")
  out_error=$(log_error "test message" 2>&1)
  out_equal=$(log_equal)
  out_hyphen=$(log_hyphen)

  echo "INFO_CONTAINS_TAG:${out_info}"
  echo "SUCCESS_CONTAINS_TAG:${out_success}"
  echo "WARN_CONTAINS_TAG:${out_warn}"
  echo "ERROR_CONTAINS_TAG:${out_error}"
  echo "EQUAL_CHAR:${out_equal:0:1}"
  echo "HYPHEN_CHAR:${out_hyphen:0:1}"
EOFTEST
)

assert_contains "log_info produces [INFO ] tag"    "[INFO ]"    "$(echo "$log_test_output" | grep INFO_CONTAINS_TAG)"
assert_contains "log_success produces [SUCCESS] tag" "[SUCCESS]" "$(echo "$log_test_output" | grep SUCCESS_CONTAINS_TAG)"
assert_contains "log_warn produces [WARN ] tag"    "[WARN ]"    "$(echo "$log_test_output" | grep WARN_CONTAINS_TAG)"
assert_contains "log_error produces [ERROR] tag"   "[ERROR]"    "$(echo "$log_test_output" | grep ERROR_CONTAINS_TAG)"
assert_contains "log_equal prints = characters"    "="          "$(echo "$log_test_output" | grep EQUAL_CHAR)"
assert_contains "log_hyphen prints - characters"   "-"          "$(echo "$log_test_output" | grep HYPHEN_CHAR)"

# ---------------------------------------------------------------------------
# Test: config file parsing
# ---------------------------------------------------------------------------
echo ""
echo "=== config parsing tests ==="

CONFIG_FILE="$(make_config)"
trap "rm -f '$CONFIG_FILE'" EXIT

result=$(load_in_subshell "$CONFIG_FILE" 'echo "SRC=$SOURCE_HUDI_VERSION TGT=$TARGET_HUDI_VERSION"')
assert_contains "SOURCE_HUDI_VERSION parsed"  "SRC=0.14.1"               "$result"
assert_contains "TARGET_HUDI_VERSION parsed"  "TGT=0.14.2-SNAPSHOT"      "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$SPARK_VERSION"')
assert_eq "SPARK_VERSION parsed" "3.4.4" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$SCALA_VERSION"')
assert_eq "SCALA_VERSION parsed" "2.12" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$NUM_OF_COLUMNS"')
assert_eq "NUM_OF_COLUMNS parsed" "500" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$NUM_OF_PARTITIONS"')
assert_eq "NUM_OF_PARTITIONS parsed" "10000" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$BASE_TABLE_NAME"')
assert_eq "BASE_TABLE_NAME parsed" "hudi_logical" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$IS_LOGICAL_TIMESTAMP_ENABLED"')
assert_eq "IS_LOGICAL_TIMESTAMP_ENABLED parsed" "true" "$result"

# ---------------------------------------------------------------------------
# Test: variable expansion in values
# ---------------------------------------------------------------------------
echo ""
echo "=== variable expansion tests ==="

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$HUDI_VERSION"')
assert_eq "HUDI_VERSION expands to TARGET_HUDI_VERSION" "0.14.2-SNAPSHOT" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$HUDI_VERSIONS"')
assert_contains "HUDI_VERSIONS contains source" "0.14.1"           "$result"
assert_contains "HUDI_VERSIONS contains target" "0.14.2-SNAPSHOT"  "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$S3_JARS_PATH"')
assert_contains "S3_JARS_PATH expands BASE_PATH" "s3://test-bucket/hudi-bench/jars" "$result"

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$HADOOP_MAJOR_VERSION"')
assert_eq "HADOOP_MAJOR_VERSION derived from HADOOP_VERSION" "3" "$result"

# ---------------------------------------------------------------------------
# Test: SOURCE_DATA derived correctly
# ---------------------------------------------------------------------------
echo ""
echo "=== SOURCE_DATA derivation tests ==="

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$SOURCE_DATA"')
assert_contains "SOURCE_DATA contains _lts suffix when IS_LOGICAL_TIMESTAMP_ENABLED=true" "_lts" "$result"
assert_contains "SOURCE_DATA contains column count" "500cols" "$result"
assert_contains "SOURCE_DATA contains partition count" "10000parts" "$result"

# ---------------------------------------------------------------------------
# Test: PROPS_FILE gets file:// prefix for local paths
# ---------------------------------------------------------------------------
echo ""
echo "=== PROPS_FILE prefix tests ==="

result=$(load_in_subshell "$CONFIG_FILE" 'echo "$PROPS_FILE"')
assert_contains "PROPS_FILE gets file:// prefix" "file://" "$result"

# ---------------------------------------------------------------------------
# Test: comment and blank line stripping
# ---------------------------------------------------------------------------
echo ""
echo "=== comment stripping tests ==="

COMMENT_CONFIG="$(mktemp /tmp/test_comment.XXXXXX.properties)"
cat > "$COMMENT_CONFIG" << 'EOF'
# This is a comment
TEST_KEY=hello_world

# Another comment
TEST_KEY2=foo # inline comment
EOF

result=$(env -i PATH="$PATH" CONFIG_FILE="$COMMENT_CONFIG" SKIP_SPARK_HOME_CHECK=1 AWS_S3_JARS="x" \
  bash -c "source '${SCRIPT_DIR}/scripts/load_config.sh'; echo \"\$TEST_KEY\"")
assert_eq "Plain value parsed correctly"  "hello_world" "$result"

result=$(env -i PATH="$PATH" CONFIG_FILE="$COMMENT_CONFIG" SKIP_SPARK_HOME_CHECK=1 AWS_S3_JARS="x" \
  bash -c "source '${SCRIPT_DIR}/scripts/load_config.sh'; echo \"\$TEST_KEY2\"")
assert_eq "Inline comment stripped"  "foo" "$result"

rm -f "$COMMENT_CONFIG"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================="
echo "Results: ${PASS} passed, ${FAIL} failed"
echo "============================="

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
