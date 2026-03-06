# Hudi Performance Testing

## Hudi Logical Timestamp Testing

**Process**

1. Initial batch + 1 incremental batch with 0.14.1.
2. Get read benchmark numbers using both jars 0.14.1 and 0.14.2.
3. Run incremental batches using 0.14.2 jar.
4. Get read benchmark numbers using both jars 0.14.1 and 0.14.2.

---

## Configuration

All paths and versions are defined in **`common.properties`** at the project root. Scripts source **`load_config.sh`**, which reads `common.properties` and exports variables (with `$BASE_PATH` etc. expanded).

Key settings:

- **Versions:** `SOURCE_HUDI_VERSION`, `TARGET_HUDI_VERSION`, `HUDI_VERSION`, `HUDI_VERSIONS`, `SPARK_VERSION`, `SCALA_VERSION`, `HADOOP_VERSION`
- **Paths:** `SPARK_HOME`, `BASE_PATH`, `JARS_PATH`, `DATA_PATH`, `SOURCE_DATA`, `DEST_DIR`, `SCRIPTS_DIR`
- **Script/config paths:** `INITIAL_BATCH_SCALA`, `INCREMENTAL_SCRIPT`, `PY_SCRIPT`, `SCHEMA_FILE`, `PROPS_FILE`, `SPARK_DEFAULTS_CONF`
- **Table:** `BASE_TABLE_NAME`. The actual table name used by ingestion and benchmark scripts appends table type and Hudi version (e.g. `hudi_logical_cow_0_14` for COW with 0.14.x, `hudi_logical_mor_0_14` for MOR). Version is derived as major_minor (e.g. 0.14.1 → `0_14`).

---

## Setup

Run once on the cluster node (e.g. EMR master) to install Spark (if missing), AWS/S3 jars, and prepare the environment:

```sh
sudo yum install tmux -y
tmux
bash setup_node.sh
# tmux attach (reconnect)
```

---

## COW Table

### 1. Generate initial parquet data

```sh
bash run_parquet_ingestion.sh --type initial --batch-id 0
```

### 2. Create initial Hudi COW table (batch 0)

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1 --batch-id 0
```

### 3. Read benchmarks with 0.14.1 and 0.14.2

```sh
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 4. Generate incremental parquet data (batch 1)

```sh
bash run_parquet_ingestion.sh --type incremental --batch-id 1
```

### 5. Apply incremental ingestion (batch 1)

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1 --batch-id 1
```

### 6. Read benchmarks again (both versions)

```sh
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 7. Generate more incremental data (batch 2)

```sh
bash run_parquet_ingestion.sh --type incremental --batch-id 2
```

### 8. Apply incremental ingestion with 0.14.2 (batch 2)

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2 --batch-id 2
```

### 9. Generate another incremental batch (optional, batch 3)

```sh
bash run_parquet_ingestion.sh --type incremental --batch-id 3
```

### 10. Apply again with 0.14.2 (optional)

```sh
bash run_hudi_ingestion.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2 --batch-id 3
```

### 11. Final read benchmarks (both versions)

```sh
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash run_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

---

## MOR Table

Use the same flow as COW, but pass **`--table-type MERGE_ON_READ`** and use the MOR table name/paths (see `common.properties` and `load_config.sh`).

**Create initial MOR table (batch 0):**

```sh
bash run_hudi_ingestion.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1 --batch-id 0
```

**Create/update with 0.14.2 (e.g. batch 1 or 2):**

```sh
bash run_hudi_ingestion.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2 --batch-id 1
```

**Read benchmarks (both versions):**

```sh
bash run_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1
bash run_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```

Parquet data is shared: use **`run_parquet_ingestion.sh --type initial --batch-id 0`** once, then **`--type incremental --batch-id N`** for each incremental batch, before running Delta Streamer for either COW or MOR.

---

## E2E performance test (single script)

Runs the full flow in one go: **3 batches** (batch 0 = initial, batches 1–2 = incremental). For each batch: **generate parquet → Hudi ingestion → benchmark** (9 steps total). Versions come from **`common.properties`** (`SOURCE_HUDI_VERSION`, `TARGET_HUDI_VERSION`).

```sh
bash run_e2e_performance_test.sh --table-type COPY_ON_WRITE
```

For MERGE_ON_READ:

```sh
bash run_e2e_performance_test.sh --table-type MERGE_ON_READ
```

**Options:**

- **`--table-type`** — Required. `COPY_ON_WRITE` or `MERGE_ON_READ`.
- **`--dry-run`** — Print the plan only; do not run any step.
- **`--force`** — Ignore saved state and run all steps (default: skip steps that already succeeded, retry on failure).

**State and resume:**

- State is stored in **`.e2e_state/state_<table>_v<ver>.txt`** and synced to **S3** (`${BASE_PATH}/e2e_state/...`) after each step. If you re-run or move to another host, the script downloads state from S3 and skips steps that already succeeded; failed steps are retried.
- Logs go to **`logs/e2e_<table>_v<ver>_<timestamp>.log`**.
- At the end, **`hudi_benchmark_results.csv`** is uploaded to **`${BASE_PATH}/hudi_benchmark_results.csv`**.

---

## Script reference

| Script | Purpose |
|--------|--------|
| `setup_node.sh` | One-time setup: Spark, AWS jars, env (no S3 jar download). |
| `run_parquet_ingestion.sh` | Generate parquet data. **Required:** `--type initial \| incremental`, **`--batch-id <id>`**. |
| `run_hudi_ingestion.sh` | Run Hudi Delta Streamer. **Required:** `--table-type COPY_ON_WRITE \| MERGE_ON_READ`, `--target-hudi-version`. **Optional:** `--batch-id <id>` (ingest only `SOURCE_DATA/batch_<id>`). |
| `run_hudi_benchmark.sh` | Single read benchmark. **Required:** `--table-type`, `--target-hudi-version`. **Optional:** `--batch-id` (for CSV labeling when called by suite). |
| `run_benchmark_suite.py` | Run benchmarks for multiple Hudi versions, append results to CSV with run sequence. |
| `run_e2e_performance_test.sh` | **E2E:** 3 batches × (parquet → Hudi ingestion → benchmark). State synced to S3 after each step. |

Use **`-h`** or **`--help`** on any script for usage.

### Benchmark suite (Python)

Runs `run_hudi_benchmark.sh` for each Hudi version and appends one row per (table_type, hudi_version) to a CSV with an incrementing run sequence.

```sh
python3 run_benchmark_suite.py --table-type COPY_ON_WRITE --hudi-versions 0.14.1,0.14.2 --output hudi_benchmark_results.csv
```

**Options:**

- **`--table-type`** — Table type, e.g. `COPY_ON_WRITE` (default: `COPY_ON_WRITE`).
- **`--hudi-versions`** — Comma-separated versions (default from `common.properties`).
- **`--batch-id`** — Batch ID for CSV labeling (default: 0).
- **`--output`** — Output CSV path (default: project dir `hudi_benchmark_results.csv`).
- **`--dry-run`** — Print what would be run and exit.

The first run creates **`hudi_benchmark_results.csv`** and **`benchmark_run_sequence.txt`** (starts at 1). Each run increments the sequence and appends rows. CSV columns: **`run_sequence`**, **`table_type`**, **`hudi_version`**, **`batch_id`**, **`execution_time_seconds`**, **`count`**, **`run_timestamp_utc`**, **`start_time`**, **`end_time`**, **`status`**.
