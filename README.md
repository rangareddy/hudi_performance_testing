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
- [Baseline vs experiment comparison](#baseline-vs-experiment-comparison)
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
├── run_hudi_compaction.sh           # MOR compaction (E2E / manual)
├── run_benchmark_suite.py           # Benchmark multiple Hudi versions; write CSV (+ E2E iterations)
├── run_e2e_performance_test.sh      # Full 4-batch E2E orchestrator (single table type)
├── run_e2e_performance_test_all.sh  # Run E2E for both COW and MOR sequentially
├── scripts/
│   ├── load_config.sh               # Config loader + shared logging functions
│   ├── common.properties            # Default config on EMR (`IS_EMR_CLUSTER=true`)
│   ├── common_local.properties      # Default config when running locally (see below)
│   ├── compare_e2e_phases.py        # Merge read/write CSVs into baseline vs experiment reports
│   ├── spark-defaults.conf          # Spark memory and executor settings
│   ├── hoodie.properties            # HoodieStreamer properties
│   ├── lts_schema.avsc              # Avro schema when logical timestamps are enabled
│   ├── non_lts_schema.avsc          # Avro schema when logical timestamps are disabled
│   ├── initial_batch.scala          # Spark Shell script for initial data generation
│   ├── incremental_batch.py         # PySpark script for incremental data generation
│   └── hudi_benchmark.py            # PySpark read benchmark script
├── tests/
│   ├── data/                        # Golden read/write CSVs + expected E2E comparison outputs
│   ├── test_benchmark_suite.py      # Unit tests for run_benchmark_suite.py
│   ├── test_compare_e2e_fixtures.py # Golden-file tests for compare_e2e_phases.py
│   ├── test_incremental_batch.py    # Unit tests for scripts/incremental_batch.py
│   ├── test_hudi_benchmark.py       # Unit tests for scripts/hudi_benchmark.py
│   ├── test_load_config.sh          # Bash tests for scripts/load_config.sh
│   └── test_parquet_ingestion.sh    # Bash tests for run_parquet_ingestion.sh
└── README.md
```

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| AWS EMR or EC2 (typical) | YARN running; AWS credentials for S3 |
| Local workstation (optional) | Set `IS_EMR_CLUSTER=false`, `IS_LOCAL_RUN=true`, and paths in `common_local.properties`; local Spark master (e.g. `local[4]`) |
| `aws` CLI | S3 reads/writes and jar sync when using S3 paths |
| `wget` | Used by `setup_node.sh` to download Spark and Maven jars |
| `tmux` | Recommended for long-running sessions on servers |
| Python 3 | `run_benchmark_suite.py`, `incremental_batch.py`, `compare_e2e_phases.py`, and tests |
| `pytest` (optional) | Run `python3 -m pytest tests/ -v` |

Hudi jars (`hudi-spark*-bundle` and `hudi-utilities-slim-bundle`) must be on the node at `JARS_PATH` / `S3_JARS_PATH`, or installed via `setup_node.sh`.

---

## Configuration

- **EMR / default cluster:** `scripts/common.properties` is loaded when `IS_EMR_CLUSTER` is not `false`.
- **Local / laptop:** export `IS_EMR_CLUSTER=false` so `scripts/common_local.properties` is loaded instead (see [Running locally](#running-locally)).
- **Override:** set `CONFIG_FILE=/path/to/my.properties` before running any script that sources `load_config.sh`.

| Property | Description | Example |
|----------|-------------|---------|
| `SOURCE_HUDI_VERSION` | Baseline Hudi version | `0.14.1` |
| `TARGET_HUDI_VERSION` | Candidate Hudi version | `0.14.2-SNAPSHOT` |
| `SPARK_VERSION` | Spark version to install/use | `3.4.4` |
| `SCALA_VERSION` | Scala binary version for jar names | `2.12` |
| `HADOOP_VERSION` | Hadoop version for AWS jars | `3.3.4` |
| `BASE_PATH` | Root for data, jars, logs, and state (often S3 on EMR) | `s3://…/perf_test` |
| `NUM_OF_COLUMNS` | Wide table **data** column count (`col_1`…`col_N`); must match the chosen Avro schema (see Data Generation) | `500` |
| `NUM_OF_PARTITIONS` | Partition count for generated data | `2000`, `10000` |
| `NUM_OF_RECORDS_PER_PARTITION` | Records per partition (also used in path tags) | `1`, `100` |
| `NUM_OF_RECORDS_TO_UPDATE` | Records touched per incremental batch | `100` |
| `READ_PERFORMANCE_ITERATIONS` | Read benchmark repeats per E2E step; `>1` adds per-iteration rows and an `avg` summary in the read CSV | `1` |
| `IS_LOGICAL_TIMESTAMP_ENABLED` | Use LTS column layout and `lts_schema.avsc` | `true` / `false` |
| `BASE_TABLE_NAME` | Prefix for Hudi table names | `hudi_regular` |
| `ENABLE_WRITE_OPERATIONS` / `ENABLE_READ_OPERATIONS` | E2E gates for write path and read benchmarks | `true` / `false` |

Table names are derived automatically, e.g.:

- `hudi_logical_cow_0_14_lts` — COPY_ON_WRITE, Hudi 0.14.x, logical timestamps enabled
- `hudi_logical_mor_0_14` — MERGE_ON_READ, Hudi 0.14.x, logical timestamps disabled

### Running locally

On a laptop or any host that is **not** AWS EMR, export this **before** sourcing scripts or running `run_parquet_ingestion.sh`, `run_hudi_ingestion.sh`, `run_e2e_performance_test.sh`, and other entry points that call `scripts/load_config.sh`:

```sh
export IS_EMR_CLUSTER=false
```

With `IS_EMR_CLUSTER=false`, `load_config.sh` loads `scripts/common_local.properties` instead of `scripts/common.properties`. Edit `common_local.properties` for your machine: `SPARK_HOME`, `BASE_PATH` / `DATA_PATH`, `IS_LOCAL_RUN=true` for a local Spark master, and `SKIP_SPARK_HOME_CHECK=1` only if you intentionally bypass the Spark directory check in tests or minimal setups.

---

## Quick Start

### On AWS EMR / Linux server

```sh
sudo yum install tmux git -y
tmux new -s hudi_bench
# Reattach later: tmux attach -t hudi_bench
```

### Clone the repository

```sh
git clone https://github.com/rangareddy/hudi_performance_testing.git
cd hudi_performance_testing
```

### Local workstation (before any benchmark script)

```sh
export IS_EMR_CLUSTER=false
# Optional: point at a custom properties file
# export CONFIG_FILE="$PWD/scripts/common_local.properties"
```

Then edit `scripts/common_local.properties` (Spark path, local `BASE_PATH` or `DATA_PATH`, `IS_LOCAL_RUN=true` as needed) and continue with [Node Setup](#1-node-setup) if you use `setup_node.sh`, or run individual scripts once Spark and jars are available.

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

**Avro schema vs `NUM_OF_COLUMNS`:** Before running Spark, `run_parquet_ingestion.sh` checks that `NUM_OF_COLUMNS` matches the **data** field count in the Avro record (all `fields` except a trailing `partition_col`). The schema file is `scripts/lts_schema.avsc` when `IS_LOGICAL_TIMESTAMP_ENABLED=true`, else `scripts/non_lts_schema.avsc`, unless you set `SCHEMA_FILE` / `SCHEMA_FILE_NO_LTS_STRING_TS` (same variables as `run_hudi_ingestion.sh`).

**Skip if output exists:** If the target batch directory already exists and contains files (local path or S3 listing), the script exits successfully without re-running Spark and records `skipped_existing` in the write perf CSV when configured.

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

The suite increments a persistent run sequence in `benchmark_run_sequence.txt` (unless you pass `--run-sequence`, as the E2E script does) and appends one row per Hudi version. With `READ_PERFORMANCE_ITERATIONS` > 1 (or `--read-performance-iterations N`), each invocation writes an `iteration` column; after the last iteration, rows with `read_aggregate=avg` record mean read time per version.

| Option | Default | Description |
|--------|---------|-------------|
| `--table-type` | `COPY_ON_WRITE` | Table type to benchmark |
| `--hudi-versions` | From defaults in the script | Comma-separated versions |
| `--batch-id` | `0` | Recorded in the CSV |
| `--iteration` | `1` | Iteration index when repeating reads (E2E) |
| `--run-sequence` | (allocate from file) | Fixed sequence for a multi-iteration block |
| `--read-performance-iterations` | env `READ_PERFORMANCE_ITERATIONS` or `1` | Total iterations for averaging |
| `--table-name-suffix` | (none) | Passed through to `run_hudi_benchmark.sh` (e.g. `baseline` / `experiment`) |
| `--output` | `hudi_benchmark_results.csv` | Output CSV path (created or appended) |
| `--allocate-run-sequence-only` | off | Print next `run_sequence` to stdout only (used by E2E bash) |

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

E2E and manual runs typically write under `reports/<NUM_OF_COLUMNS>_<NUM_OF_PARTITIONS>_<NUM_OF_RECORDS_PER_PARTITION>/` with `read/` and `write/` subdirectories for that data shape.

| Artifact | Location (pattern) | Description |
|----------|--------------------|-------------|
| Read benchmark CSV | `reports/.../read/hudi_benchmark_results_<cow\|mor>_<lts>_<versions>_<shape>_{baseline,experiment}.csv` | Read times and counts; may include `iteration` and `read_aggregate=avg` |
| Write performance CSV | `reports/.../write/hudi_write_performance_*.csv` | Parquet and Hudi streamer timings |
| E2E comparison CSV | `reports/.../e2e_baseline_vs_experiment_<ver>_vs_<ver>.csv` | Rolled-up baseline vs experiment metrics (from `compare_e2e_phases.py`) |
| Per-batch comparison | `..._per_batch_tables.csv` | Side-by-side write/read seconds per batch |
| E2E state | `.e2e_state/<shape>/` | Step resumption markers |
| Log file | `logs/<YYYYMMDD>/<shape>/e2e_*.log` | Captured E2E output |
| S3 mirrors | Under `${BASE_PATH}/` as configured | Reports, logs, state sync when enabled |

**Read benchmark CSV columns (main fields):**

| Column | Description |
|--------|-------------|
| `run_sequence` | Run block id (shared across iterations in E2E) |
| `table_type` | `COPY_ON_WRITE` or `MERGE_ON_READ` |
| `hudi_version` | Hudi version used for the benchmark |
| `batch_id` | Table batch at benchmark time |
| `iteration` | Read repeat index when `READ_PERFORMANCE_ITERATIONS` > 1 |
| `read_aggregate` | Empty for raw runs; `avg` for mean-time summary rows |
| `execution_time_seconds` | Elapsed time for the Spark read + count |
| `count` | Distinct record count |
| `run_timestamp_utc` | UTC timestamp for the row |
| `start_time` / `end_time` | Wall-clock span of the subprocess |
| `status` | `ok`, `parse_failed`, `exit_code_N`, or `timeout` |

---

## Baseline vs experiment comparison

After E2E produces `read/` and `write/` CSVs under a report bundle directory, generate the summary and per-batch tables:

```sh
python3 scripts/compare_e2e_phases.py --report-dir reports/<shape_tag_directory>
```

Use `--report-root <dir>` to process every immediate subdirectory that contains both `read/` and `write/`. Output filenames include the inferred Hudi version tag, e.g. `e2e_baseline_vs_experiment_0.14.1_vs_0.14.2-SNAPSHOT.csv`.

To refresh **golden** comparison files used by tests after editing fixture CSVs under `tests/data/read` and `tests/data/write`:

```sh
python3 scripts/compare_e2e_phases.py --report-dir tests/data \
  --initial-batch-size 200000 \
  --incremental-batch-size 100
```

(`200000` = `NUM_OF_PARTITIONS` × `NUM_OF_RECORDS_PER_PARTITION` for the `500_2000_100` fixture shape.)

---

## Script Reference

| Script | Type | Description |
|--------|------|-------------|
| `setup_node.sh` | Bash | One-time node setup: Spark + Hudi jars |
| `run_parquet_ingestion.sh` | Bash | Generate initial or incremental parquet batches |
| `run_hudi_ingestion.sh` | Bash | Run HoodieStreamer upsert for a given table type and version |
| `run_hudi_benchmark.sh` | Bash | Run a single Spark read benchmark against a Hudi table |
| `run_hudi_compaction.sh` | Bash | Run MOR compaction for a given Hudi version (E2E or manual) |
| `run_benchmark_suite.py` | Python | Benchmark multiple Hudi versions; CSV output, E2E iteration/`avg` support |
| `run_e2e_performance_test.sh` | Bash | Full 4-batch E2E flow with state, logs, and S3 upload |
| `run_e2e_performance_test_all.sh` | Bash | Run E2E for both COW and MOR sequentially |
| `scripts/load_config.sh` | Bash | Load properties (`common.properties` or `common_local.properties`), validate deps, `log_*` |
| `scripts/compare_e2e_phases.py` | Python | Build baseline vs experiment CSVs from `read/` + `write/` bundles |
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

# Bash tests for run_parquet_ingestion.sh (CLI, Avro/NUM_OF_COLUMNS, skip-existing; no Spark required for most assertions)
bash tests/test_parquet_ingestion.sh
```

| Test file | What it covers |
|-----------|---------------|
| `tests/test_benchmark_suite.py` | Sequence tracking, output parsing, `run_benchmark` status codes, CSV output, multi-iteration `avg` rows |
| `tests/test_compare_e2e_fixtures.py` | `compare_e2e_phases.process_report_bundle` vs golden CSVs in `tests/data/`; `load_read_by_batch` prefers `read_aggregate=avg` |
| `tests/test_incremental_batch.py` | Range calculation, value generation, env var handling, Spark mock integration |
| `tests/test_hudi_benchmark.py` | Output format patterns matched by the benchmark suite parser |
| `tests/test_load_config.sh` | Config parsing, variable expansion, comment stripping, `log_*` output, `PROPS_FILE`, `SOURCE_DATA` derivation |
| `tests/test_parquet_ingestion.sh` | Parquet CLI validation, Avro/`NUM_OF_COLUMNS`, missing schema, skip-existing, missing Scala script |

Fixture CSVs for the compare tests live in `tests/data/read/`, `tests/data/write/`, and golden E2E outputs at `tests/data/e2e_baseline_vs_experiment_*.csv` (not inside `read/` or `write/`).

---

## Troubleshooting

**`Spark home not found`**
Run `bash setup_node.sh` first, or set `SPARK_HOME` in `common.properties` / `common_local.properties`. For tests only, `SKIP_SPARK_HOME_CHECK=1` skips the directory check.

**`NUM_OF_COLUMNS` / Avro schema mismatch**
`run_parquet_ingestion.sh` exits if `NUM_OF_COLUMNS` does not match the data-column count in the active `.avsc` file. Regenerate schemas or set `NUM_OF_COLUMNS` to match; optional `SCHEMA_FILE` / `SCHEMA_FILE_NO_LTS_STRING_TS` must stay consistent with `IS_LOGICAL_TIMESTAMP_ENABLED`.

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