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

The current E2E flow runs **4 batches** and **12 steps** total:

- Batch `0`: initial parquet, Hudi ingestion with `SOURCE_HUDI_VERSION`, benchmark
- Batch `1`: incremental parquet, Hudi ingestion with `SOURCE_HUDI_VERSION`, benchmark
- Batch `2`: incremental parquet, Hudi ingestion with `TARGET_HUDI_VERSION`, benchmark
- Batch `3`: incremental parquet, Hudi ingestion with `TARGET_HUDI_VERSION`, benchmark

Options:

- `--table-type` required: `COPY_ON_WRITE` or `MERGE_ON_READ`
- `--dry-run`: print the plan only
- `--force`: ignore saved state and rerun all steps

State and artifacts:

- Local state file: `.e2e_state/state_<table>_v<ver>.txt`
- S3 state file: `${BASE_PATH}/e2e_state/state_<table>_v<ver>.txt`
- Local log file: `logs/e2e_<table>_v<ver>_<timestamp>.log`
- S3 log upload: `${BASE_PATH}/logs/<log file>`
- Benchmark CSV upload: `${BASE_PATH}/hudi_benchmark_results.csv`

The E2E script uploads state to S3 after each step, so reruns skip successful steps and retry failed ones.

## Script Reference

| Script | Purpose |
|--------|--------|
| `setup_node.sh` | One-time setup for Spark, jars, and runtime directories. |
| `load_config.sh` | Loads `common.properties`, expands simple variable references, and validates core runtime dependencies. |
| `run_parquet_ingestion.sh` | Generates parquet data. Requires `--type` and `--batch-id`. |
| `run_hudi_ingestion.sh` | Runs Hudi Delta Streamer for a table type and Hudi version; validates required files before running. |
| `run_hudi_benchmark.sh` | Runs a single Spark benchmark against a Hudi table. |
| `run_benchmark_suite.py` | Runs benchmarks for multiple Hudi versions and writes CSV rows with run sequence and batch id. |
| `run_e2e_performance_test.sh` | Full 4-batch E2E flow with S3-backed state, log upload, and CSV upload. |

Use `-h` or `--help` on any script for the latest CLI usage.
