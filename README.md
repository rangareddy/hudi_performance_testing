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
