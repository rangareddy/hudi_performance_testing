# Hudi Performance Testing

## Overview

This repo runs a repeatable Hudi logical timestamp benchmark flow on Spark:

1. Generate parquet source data.
2. Ingest that data into Hudi tables with `SOURCE_HUDI_VERSION` and `TARGET_HUDI_VERSION`.
3. Run read benchmarks for the configured Hudi versions.
4. Persist E2E state, logs, and benchmark results to S3 so runs can resume safely.

## Configuration

All runtime settings live in `common.properties`. Every shell entrypoint sources `load_config.sh`, which loads that file and expands simple variable references safely.

Important properties:

- `SOURCE_HUDI_VERSION`, `TARGET_HUDI_VERSION`, `HUDI_VERSION`, `HUDI_VERSIONS`
- `SPARK_VERSION`, `SCALA_VERSION`, `HADOOP_VERSION`, `HADOOP_MAJOR_VERSION`
- `SPARK_HOME`, `BASE_PATH`, `S3_JARS_PATH`, `JARS_PATH`, `DATA_PATH`
- `SCRIPTS_DIR`, `INITIAL_BATCH_SCALA`, `INCREMENTAL_SCRIPT`, `PY_SCRIPT`
- `SCHEMA_FILE`, `PROPS_FILE`, `SPARK_DEFAULTS_CONF`
- `NUM_OF_COLUMNS`, `NUM_OF_PARTITIONS`, `NUM_OF_RECORDS_TO_UPDATE`
- `SOURCE_DATA`, `BASE_TABLE_NAME`

Table names are derived from `BASE_TABLE_NAME`, table type, and the Hudi version major/minor. For example:

- `hudi_logical_cow_0_14`
- `hudi_logical_mor_0_14`

## Setup

```sh
sudo yum install tmux -y
tmux
# tmux attach (reconnect)
```

Run once on the cluster node:

```sh
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/performance/logical_ts_perf/source_code/ . --recursive
unzip hudi_performace_testing.zip && cd hudi_performace_testing
```

Before running `setup_node.sh`, update the `scripts/common.properties` file according to your requirements. For example, update the `SOURCE_HUDI_VERSION` and `TARGET_HUDI_VERSION`.

```sh
bash setup_node.sh
```

## Data Generation

Initial parquet generation uses `scripts/initial_batch.scala` and reads configuration from environment variables exported by `run_parquet_ingestion.sh`:

- `BATCH_ID`
- `NUM_OF_COLUMNS`
- `NUM_OF_PARTITIONS`
- `IS_LOGICAL_TIMESTAMP_ENABLED`
- `TARGET_DATA`

Incremental parquet generation uses `scripts/incremental_batch.py`:

- Reads from `SOURCE_DATA/batch_0`
- Writes to `SOURCE_DATA/batch_<BATCH_ID>`
- Uses record keys that match the initial dataset format, so batches `1+` update real rows instead of filtering empty ranges

Examples:

```sh
bash run_parquet_ingestion.sh --type initial --batch-id 0
bash run_parquet_ingestion.sh --type incremental --batch-id 1
```

## Hudi Ingestion

Run Delta Streamer for a specific table type and Hudi version:

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1 --batch-id 0
bash run_hudi_ingestion.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2-SNAPSHOT --batch-id 2
```

Notes:

- `--batch-id` is optional, but in the E2E flow it is always provided.
- If `--batch-id` is set, ingestion reads only `SOURCE_DATA/batch_<id>`.
- The script validates `hoodie.properties`, schema file, Spark defaults, and required Hudi jars before starting.

## Benchmarking

Run a single benchmark:

```sh
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

Run the benchmark suite:

```sh
python3 run_benchmark_suite.py --table-type COPY_ON_WRITE --hudi-versions 0.14.1,0.14.2-SNAPSHOT --batch-id 2
```

Behavior:

- Defaults for `--hudi-versions` come from `common.properties`.
- `--dry-run` shows the next sequence number but does not increment `benchmark_run_sequence.txt`.
- The suite now exits nonzero if any benchmark run fails or cannot be parsed, so E2E correctly marks the step as failed.

## E2E Performance Test

Run the end-to-end flow:

```sh
bash run_e2e_performance_test.sh --table-type COPY_ON_WRITE
bash run_e2e_performance_test.sh --table-type MERGE_ON_READ
```

(or)

```sh
bash run_e2e_performance_test_all.sh
```

The E2E flow runs **two phases**, each with **5 batches** (15 steps per phase: parquet → Hudi → benchmark):

1. **Baseline** — Hudi ingestion uses **`SOURCE_HUDI_VERSION` only** for batches `0–4` (batch `0` = initial, `1–4` = incremental).
2. **Experiment** — batches `0–2` use **`SOURCE_HUDI_VERSION`**; batches `3–4` use **`TARGET_HUDI_VERSION`**.

For **MERGE_ON_READ**, E2E still runs offline **`HoodieCompactor`** then a post-compaction read benchmark. **Hudi 0.15+ Delta Streamer requires `hoodie.compact.inline=true`**, so ingestion does not force `inline=false` (that fails in `StreamSync`). Ingestion may already compact inline, so **`HoodieCompactor` often has nothing to schedule**; **`run_hudi_compaction.sh`** then exits successfully with write-perf status **`no_pending_compaction`** unless **`HUDI_COMPACTION_ALLOW_EMPTY=false`**.

After both phases, **`scripts/compare_e2e_phases.py`** writes **`reports/e2e_baseline_vs_experiment_*.csv`** with aggregated read and write times (baseline vs experiment, delta %).

Options:

- `--table-type` required: `COPY_ON_WRITE` or `MERGE_ON_READ`
- `--dry-run`: print the plan only
- `--force`: ignore saved state and rerun all steps

State and artifacts (per phase: `_baseline` / `_experiment` in filenames where noted):

- Local state: `.e2e_state/state_{baseline|experiment}_<table>_<IS_LOGICAL_TIMESTAMP_ENABLED>_v<ver>.txt` — **two levels**: (1) `phase_completeness=success` skips the whole phase on rerun unless `--force`; (2) `step…=success|failure` per batch stage (parquet, Hudi, benchmark, and MOR compaction/post-benchmark when applicable).
- S3 state: `${BASE_PATH}/e2e_state/state_{baseline|experiment}_...`
- Local log: `logs/<YYYYMMDD>/e2e_<table>_v<ver>_<IS_LOGICAL_TIMESTAMP_ENABLED>.log`
- Read benchmarks: `reports/read/hudi_benchmark_results_<cow|mor>_<lts>_<versions>_{baseline|experiment}.csv`
- Write performance: `reports/write/hudi_write_performance_<cow|mor>_<lts>_<versions>_{baseline|experiment}.csv`
- Comparison: `reports/e2e_baseline_vs_experiment_<cow|mor>_<lts>_<versions>.csv`
- S3 uploads: `${BASE_PATH}/reports/read/`, `${BASE_PATH}/reports/write/`, comparison CSVs under `${BASE_PATH}/reports/`; logs under `${BASE_PATH}/logs/`

The E2E script uploads state to S3 after each step **within the current phase**, so reruns can resume baseline or experiment independently.

## Script Reference

| Script | Purpose |
|--------|--------|
| `setup_node.sh` | One-time setup for Spark, jars, and runtime directories. |
| `load_config.sh` | Loads `common.properties`, expands simple variable references, and validates core runtime dependencies. |
| `run_parquet_ingestion.sh` | Generates parquet data. Requires `--type` and `--batch-id`. |
| `run_hudi_ingestion.sh` | Runs Hudi Delta Streamer for a table type and Hudi version; validates required files before running. |
| `run_hudi_benchmark.sh` | Runs a single Spark benchmark against a Hudi table. |
| `run_benchmark_suite.py` | Runs benchmarks for multiple Hudi versions and writes CSV rows with run sequence and batch id. |
| `run_e2e_performance_test.sh` | Baseline (5 batches, all SOURCE) + experiment (5 batches, mixed SOURCE/TARGET), comparison report, S3 uploads. |
| `scripts/compare_e2e_phases.py` | Aggregates read/write CSVs from baseline vs experiment into one comparison CSV. |

Use `-h` or `--help` on any script for the latest CLI usage.
