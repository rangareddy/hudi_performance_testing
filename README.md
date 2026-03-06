# Hudi Performance Testing

## Hudi Logical Timestamp Testing

**Process**

1. Initial batch + 1 incremental batch with 0.14.1.
2. Get read benchmark numbers using both jars 0.14.1 and 0.14.2.
3. Run incremental batches using 0.14.2 jar.
4. Get read benchmark numbers using both jars 0.14.1 and 0.14.2.

---

## Configuration

All paths and versions are defined in **`common.properties`** at the project root. Edit this file to change:

- `SPARK_HOME`, `BASE_PATH`, `JARS_PATH`, `DATA_PATH`, `SOURCE_DATA`
- `DEST_DIR`, `SCRIPTS_DIR`
- `SOURCE_HUDI_VERSION`, `TARGET_HUDI_VERSION`, `HUDI_VERSION` (defaults to `TARGET_HUDI_VERSION`)
- `SPARK_VERSION`, `SCALA_VERSION`, `HADOOP_VERSION`
- Script paths: `INITIAL_BATCH_SCALA`, `INCREMENTAL_SCRIPT`, `PY_SCRIPT`, `SCHEMA_FILE`, `PROPS_FILE`, `SPARK_DEFAULTS_CONF`

Scripts source `load_config.sh`, which reads `common.properties` and exports these variables.

---

## Setup

Run once on the cluster node (e.g. EMR master) to install Spark (if missing), AWS/S3 jars, and prepare the environment:

```sh
sudo yum install tmux
tmux
bash setup_node.sh
```

---

## COW Table

### 1. Generate initial parquet data

```sh
bash run_parquet_ingestion.sh --type initial
```

### 2. Create initial Hudi COW table (0.14.1)

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

### 3. Read benchmarks with 0.14.1 and 0.14.2

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 4. Generate incremental parquet data

```sh
bash run_parquet_ingestion.sh --type incremental
```

### 5. Apply incremental ingestion with 0.14.1

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

### 6. Read benchmarks again (both versions)

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 7. Generate more incremental data

```sh
bash run_parquet_ingestion.sh --type incremental
```

### 8. Apply incremental ingestion with 0.14.2

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 9. Generate another incremental batch (optional)

```sh
bash run_parquet_ingestion.sh --type incremental
```

### 10. Apply again with 0.14.2 (optional)

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 11. Final read benchmarks (both versions)

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

---

## MOR Table

Use the same flow as COW, but pass **`--table-type MERGE_ON_READ`** and use the MOR table name/paths (see `common.properties` and `load_config.sh`).

**Create initial MOR table (0.14.1):**

```sh
bash run_hudi_ingestion.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1
```

**Create/update with 0.14.2:**

```sh
bash run_hudi_ingestion.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```

**Read benchmarks (both versions):**

```sh
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```

Data generation is shared: run **`run_ingestion_data_generator.sh --type initial`** once, then **`--type incremental`** as needed, before running Delta Streamer for either COW or MOR.

---

## E2E performance test (single script)

To run the full flow in one go: **1 initial ingestion → Hudi ingestion → benchmark → 2 incremental cycles** (each cycle: generate data → Hudi ingestion → benchmark):

```sh
bash run_e2e_performance_test.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

For MERGE_ON_READ:

```sh
bash run_e2e_performance_test.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```

Optional: `--hudi-versions 0.14.1,0.14.2` (versions to use in benchmark runs), `--dry-run` (print plan only).

---

## Script reference

| Script | Purpose |
|--------|--------|
| `setup_node.sh` | One-time setup: Spark, AWS jars, env (no S3 jar download). |
| `run_parquet_ingestion.sh` | Generate parquet data: `--type initial` or `--type incremental`. |
| `run_hudi_ingestion.sh` | Run Hudi Streamer: `--table-type COPY_ON_WRITE \| MERGE_ON_READ` and `--target-hudi-version 0.14.1 \| 0.14.2`. |
| `run_hudi_benchmark.sh` | Single read benchmark: `--table-type` and `--target-hudi-version`. |
| `run_benchmark_suite.py` | Run benchmarks for multiple Hudi versions, append results to CSV with run sequence. |
| `run_e2e_performance_test.sh` | **E2E:** 1 initial + 2 incremental cycles (parquet → Hudi ingestion → benchmark each). |

Use `-h` or `--help` on any script for usage.

### Benchmark suite (Python)

Run all combinations and write to CSV with an incrementing run sequence:

```sh
python run_benchmark_suite.py --table-types COPY_ON_WRITE --hudi-versions 0.14.1,0.14.2 --output $PWD/results.csv
```

Options:

- `--table-types COPY_ON_WRITE,MERGE_ON_READ` (default: both)
- `--hudi-versions 0.14.1,0.14.2` (default: both)
- `--output hudi_benchmark_results.csv` (default CSV name)
- `--project-dir /path/to/project` (default: auto-detect)
- `--dry-run` — print planned runs only

The first run creates `hudi_benchmark_results.csv` and `benchmark_run_sequence.txt` (set to 1). Each later run increments the sequence and appends one row per (table_type, hudi_version) to the CSV. CSV columns: `run_sequence`, `table_type`, `hudi_version`, `execution_time_seconds`, `count`, `run_timestamp_utc`, `status`.
