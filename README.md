# Hudi Performance Testing

A repeatable benchmark suite for comparing Apache Hudi read/write performance across versions on Spark (YARN). The suite generates wide parquet datasets, ingests them into COPY_ON_WRITE and MERGE_ON_READ Hudi tables, and records execution times in CSV reports.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [Step-by-Step Usage](#step-by-step-usage)
  - [1. Node Setup](#1-node-setup)
  - [2. Data Generation](#2-data-generation)
  - [3. Hudi Ingestion](#3-hudi-ingestion)
  - [4. Benchmarking](#4-benchmarking)
  - [5. End-to-End Test](#5-end-to-end-test)
- [Output Artifacts](#output-artifacts)
- [Script Reference](#script-reference)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## Overview

The benchmark flow compares a `SOURCE_HUDI_VERSION` (stable) against a `TARGET_HUDI_VERSION` (candidate) using 4 batches:

| Batch | Data Generation | Hudi Ingestion Version | Benchmark |
|-------|-----------------|------------------------|-----------|
| 0 | Initial (Scala) | `SOURCE_HUDI_VERSION` | Both versions |
| 1 | Incremental (Python) | `SOURCE_HUDI_VERSION` | Both versions |
| 2 | Incremental (Python) | `TARGET_HUDI_VERSION` | Both versions |
| 3 | Incremental (Python) | `TARGET_HUDI_VERSION` | Both versions |

State is persisted to S3 after every step, so interrupted runs resume safely.

---

## Project Structure

```
.
├── setup_node.sh                    # One-time node setup (Spark + Hudi jars)
├── run_parquet_ingestion.sh         # Generate initial or incremental parquet data
├── run_hudi_ingestion.sh            # Ingest parquet into a Hudi table via DeltaStreamer
├── run_hudi_benchmark.sh            # Run a single Spark read benchmark
├── run_benchmark_suite.py           # Benchmark multiple Hudi versions and write CSV
├── run_e2e_performance_test.sh      # Full 4-batch E2E orchestrator (single table type)
├── run_e2e_performance_test_all.sh  # Run E2E for both COW and MOR sequentially
├── scripts/
│   ├── load_config.sh               # Config loader + shared logging functions
│   ├── common.properties            # Central configuration (edit before running)
│   ├── spark-defaults.conf          # Spark memory and executor settings
│   ├── hoodie.properties            # HoodieStreamer properties
│   ├── full_schema.avsc             # 500-column Avro schema for the dataset
│   ├── initial_batch.scala          # Spark Shell script for initial data generation
│   ├── incremental_batch.py         # PySpark script for incremental data generation
│   └── hudi_benchmark.py            # PySpark read benchmark script
├── tests/
│   ├── test_benchmark_suite.py      # Unit tests for run_benchmark_suite.py
│   ├── test_incremental_batch.py    # Unit tests for scripts/incremental_batch.py
│   ├── test_hudi_benchmark.py       # Unit tests for scripts/hudi_benchmark.py
│   └── test_load_config.sh          # Bash tests for scripts/load_config.sh
└── README.md
```

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| AWS EMR or EC2 node | YARN must be running; AWS credentials configured |
| `aws` CLI | Used for S3 reads/writes and jar downloads |
| `wget` | Used by `setup_node.sh` to download Spark and Maven jars |
| `tmux` | Recommended for long-running sessions |
| Python 3 | Required for `run_benchmark_suite.py` and incremental batch |
| `pytest` (optional) | Required only to run the unit test suite |

Hudi jars (`hudi-spark*-bundle` and `hudi-utilities-slim-bundle`) must be available either in S3 at `S3_JARS_PATH` or downloaded by `setup_node.sh`.

---

## Configuration

All runtime settings are in `scripts/common.properties`. Edit this file **before** running any other script.

| Property | Description | Default |
|----------|-------------|---------|
| `SOURCE_HUDI_VERSION` | Baseline Hudi version | `0.14.1` |
| `TARGET_HUDI_VERSION` | Candidate Hudi version | `0.14.2-SNAPSHOT` |
| `SPARK_VERSION` | Spark version to install/use | `3.4.4` |
| `SCALA_VERSION` | Scala binary version for jar names | `2.12` |
| `HADOOP_VERSION` | Hadoop version for AWS jars | `3.3.4` |
| `BASE_PATH` | S3 root for all data, jars, logs, and state | `s3://…/logical_ts_perf` |
| `NUM_OF_COLUMNS` | Number of columns in the generated dataset | `500` |
| `NUM_OF_PARTITIONS` | Number of partitions in the generated dataset | `10000` |
| `NUM_OF_RECORDS_TO_UPDATE` | Records updated per incremental batch | `100` |
| `IS_LOGICAL_TIMESTAMP_ENABLED` | Enable logical timestamp columns | `true` |
| `BASE_TABLE_NAME` | Prefix for all Hudi table names | `hudi_logical` |

Table names are derived automatically, e.g.:

- `hudi_logical_cow_0_14_lts` — COPY_ON_WRITE, Hudi 0.14.x, logical timestamps enabled
- `hudi_logical_mor_0_14` — MERGE_ON_READ, Hudi 0.14.x, logical timestamps disabled

---

## Quick Start

```sh
sudo yum install tmux git -y
tmux
# tmux attach (reconnect)
```

To begin your performance testing, you can obtain the source code either via GitHub or directly from the S3 benchmark bucket.

**Option 1: Using Git (Recommended)**

Clone the repository directly from GitHub to ensure you have the latest version:

```sh
git clone https://github.com/rangareddy/hudi_performance_testing.git
cd hudi_performance_testing
```

**Option 2: Using AWS CLI**

If you are working directly on an EMR cluster or an EC2 instance, you can pull the source code from S3:

```sh
# Copy the source code to your local directory
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/performance/logical_ts_perf/source_code/ . --recursive

# Extract and enter the project directory
unzip hudi_performance_testing.zip
cd hudi_performance_testing
```

---

## Step-by-Step Usage

### 1. Node Setup

Run once per cluster node. Downloads Spark, Hudi jars, and AWS SDK jars.

```sh
bash setup_node.sh
```

`setup_node.sh` checks whether the configured Spark version is already installed before downloading. It downloads both `SOURCE_HUDI_VERSION` and `TARGET_HUDI_VERSION` jars from `S3_JARS_PATH`.

---

### 2. Data Generation

**Initial batch** (uses `scripts/initial_batch.scala` via `spark-shell`):

```sh
bash run_parquet_ingestion.sh --type initial --batch-id 0
```

**Incremental batches** (uses `scripts/incremental_batch.py` via `spark-submit`):

```sh
bash run_parquet_ingestion.sh --type incremental --batch-id 1
bash run_parquet_ingestion.sh --type incremental --batch-id 2
bash run_parquet_ingestion.sh --type incremental --batch-id 3
```

The script skips a batch if the target S3 path already exists.

| Option | Values | Required |
|--------|--------|----------|
| `--type` | `initial` or `incremental` | Yes |
| `--batch-id` | Non-negative integer | Yes |

---

### 3. Hudi Ingestion

Runs HoodieStreamer (DeltaStreamer) to upsert a batch of parquet data into a Hudi table.

```sh
# Ingest batch 0 with the source version into a COW table
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1 --batch-id 0

# Ingest batch 2 with the target version into a MOR table
bash run_hudi_ingestion.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2-SNAPSHOT --batch-id 2
```

| Option | Values | Required |
|--------|--------|----------|
| `--table-type` | `COPY_ON_WRITE` (or `COW`) / `MERGE_ON_READ` (or `MOR`) | Yes |
| `--target-hudi-version` | Any version present in `JARS_PATH` | Yes |
| `--batch-id` | Non-negative integer | No (defaults to full `SOURCE_DATA`) |

---

### 4. Benchmarking

**Single benchmark run:**

```sh
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash run_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2-SNAPSHOT
```

**Benchmark suite** (runs all configured Hudi versions and appends results to CSV):

```sh
python3 run_benchmark_suite.py \
  --table-type COPY_ON_WRITE \
  --hudi-versions 0.14.1,0.14.2-SNAPSHOT \
  --batch-id 2 \
  --output reports/my_results.csv
```

The suite increments a persistent run sequence number in `benchmark_run_sequence.txt` and appends one row per version to the CSV. Use `--dry-run` to preview the next sequence number without modifying files.

| Option | Default | Description |
|--------|---------|-------------|
| `--table-type` | `COPY_ON_WRITE` | Table type to benchmark |
| `--hudi-versions` | From `common.properties` | Comma-separated list of versions |
| `--batch-id` | `0` | Recorded in the CSV for traceability |
| `--output` | `hudi_benchmark_results.csv` | Output CSV path (created or appended) |

---

### 5. End-to-End Test

Runs all 4 batches (12 steps total) for a single table type with S3-backed state and resumption:

```sh
bash run_e2e_performance_test.sh --table-type COPY_ON_WRITE
bash run_e2e_performance_test.sh --table-type MERGE_ON_READ
```

Or run both table types sequentially:

```sh
bash run_e2e_performance_test_all.sh
```

**Options:**

| Option | Description |
|--------|-------------|
| `--table-type` | `COPY_ON_WRITE` or `MERGE_ON_READ` (required) |
| `--dry-run` | Print the plan only; do not execute any step |
| `--force` | Ignore saved state; rerun all steps from scratch |

**Resumption:** If a run is interrupted, re-running the same command will skip steps that previously succeeded and retry the first failed step. Use `--force` to rerun everything.

---

## Output Artifacts

| Artifact | Location | Description |
|----------|----------|-------------|
| Read benchmark CSV | `reports/hudi_benchmark_results_<cow\|mor>_<lts>_<versions>.csv` | Execution times and record counts per run |
| Write performance CSV | `reports/hudi_write_performance_<cow\|mor>_<lts>_<versions>.csv` | Parquet and Hudi ingestion durations |
| E2E state file | `.e2e_state/state_<table>_<lts>_v<ver>.txt` | Step success/failure for resumption |
| Log file | `logs/<YYYYMMDD>/e2e_<table>_v<ver>_<lts>.log` | Full stdout/stderr for the E2E run |
| S3 copies | `${BASE_PATH}/reports/`, `${BASE_PATH}/logs/`, `${BASE_PATH}/e2e_state/` | Uploaded after each batch |

**Read benchmark CSV columns:**

| Column | Description |
|--------|-------------|
| `run_sequence` | Monotonically increasing run counter |
| `table_type` | `COPY_ON_WRITE` or `MERGE_ON_READ` |
| `hudi_version` | Hudi version used for the benchmark |
| `batch_id` | Which batch was in the table at benchmark time |
| `execution_time_seconds` | Elapsed time for the Spark read + count |
| `count` | Distinct record count returned |
| `run_timestamp_utc` | UTC timestamp when the row was recorded |
| `start_time` / `end_time` | Wall-clock start and end of the benchmark run |
| `status` | `ok`, `parse_failed`, `exit_code_N`, or `timeout` |

---

## Script Reference

| Script | Type | Description |
|--------|------|-------------|
| `setup_node.sh` | Bash | One-time node setup: Spark + Hudi jars |
| `run_parquet_ingestion.sh` | Bash | Generate initial or incremental parquet batches |
| `run_hudi_ingestion.sh` | Bash | Run HoodieStreamer upsert for a given table type and version |
| `run_hudi_benchmark.sh` | Bash | Run a single Spark read benchmark against a Hudi table |
| `run_benchmark_suite.py` | Python | Benchmark multiple Hudi versions; append results to CSV |
| `run_e2e_performance_test.sh` | Bash | Full 4-batch E2E flow with state, logs, and S3 upload |
| `run_e2e_performance_test_all.sh` | Bash | Run E2E for both COW and MOR table types sequentially |
| `scripts/load_config.sh` | Bash | Load `common.properties`, validate runtime deps, provide `log_*` functions |
| `scripts/initial_batch.scala` | Scala | Generate the initial 500-column parquet dataset |
| `scripts/incremental_batch.py` | Python | Filter and write an incremental parquet batch from batch_0 |
| `scripts/hudi_benchmark.py` | Python | Load a Hudi table via Spark, count distinct records, print elapsed time |

Use `-h` or `--help` on any script for the full CLI usage.

---

## Testing

Run the unit tests (no Spark or AWS required):

```sh
# Python unit tests
python3 -m pytest tests/ -v

# Bash tests for config loading
bash tests/test_load_config.sh
```

| Test file | What it covers |
|-----------|---------------|
| `tests/test_benchmark_suite.py` | Sequence tracking, output parsing, `run_benchmark` status codes, CSV output correctness |
| `tests/test_incremental_batch.py` | Range calculation, value generation, env var handling, Spark mock integration |
| `tests/test_hudi_benchmark.py` | Output format patterns matched by the benchmark suite parser |
| `tests/test_load_config.sh` | Config parsing, variable expansion, comment stripping, `log_*` function output, `PROPS_FILE` prefix, `SOURCE_DATA` derivation |

---

## Troubleshooting

**`Spark home not found`**
Run `bash setup_node.sh` first, or verify `SPARK_HOME` in `common.properties` matches the installed path.

**`Hudi Spark Bundle Jar not found`**
Run `bash setup_node.sh` to download jars, or manually copy them to `JARS_PATH`. Jar names follow the pattern `hudi-spark<X.Y>-bundle_<scala>-<hudi_version>.jar`.

**`AWS Java SDK Bundle Jar not found`**
`setup_node.sh` downloads this from Maven Central into `$SPARK_HOME/jars/`. Run it again or check your internet access.

**Benchmark shows `parse_failed`**
The Spark job completed but the output did not contain the expected lines. Check the log file under `logs/` for Spark errors or stack traces.

**E2E step keeps failing and retrying**
Use `--force` to reset all step state and start from scratch, or delete `.e2e_state/state_*.txt` and the corresponding S3 state file.

**Incremental batch produces 0 records**
The filter uses `value_<i>_<batch_id>` keys. Verify the initial batch (`batch_0`) was written with the matching key format by checking the parquet files at `SOURCE_DATA/batch_0`.