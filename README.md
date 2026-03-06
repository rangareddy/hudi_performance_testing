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

- `SPARK_HOME`, `BASE_PATH`, `JARS_PATH`, `DATA_PATH`, `SOURCE_DFS_ROOT`
- `DEST_DIR`, `DEST_SCRIPTS_DIR`
- `HUDI_VERSION`, `SPARK_VERSION`, `SCALA_VERSION`, `HADOOP_VERSION`
- Script paths: `INITIAL_BATCH_SCALA`, `INCREMENTAL_SCRIPT`, `PY_SCRIPT`, `SCHEMA_FILE`, `PROPS_FILE`, `SPARK_DEFAULTS_CONF`

Scripts source `load_config.sh`, which reads `common.properties` and exports these variables.

---

## Setup

Run once on the cluster node (e.g. EMR master) to install Spark (if missing), AWS/S3 jars, and prepare the environment:

```sh
bash setup_node.sh
```

---

## COW Table

### 1. Generate initial parquet data

```sh
bash run_ingestion_data_generator.sh --type initial
```

### 2. Create initial Hudi COW table (0.14.1)

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

### 3. Read benchmarks with 0.14.1 and 0.14.2

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 4. Generate incremental parquet data

```sh
bash run_ingestion_data_generator.sh --type incremental
```

### 5. Apply incremental ingestion with 0.14.1

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

### 6. Read benchmarks again (both versions)

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 7. Generate more incremental data

```sh
bash run_ingestion_data_generator.sh --type incremental
```

### 8. Apply incremental ingestion with 0.14.2

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

### 9. Generate another incremental batch (optional)

```sh
bash run_ingestion_data_generator.sh --type incremental
```

### 10. Apply again with 0.14.2 (optional)

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
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
bash run_delta_streamer_app.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1
```

**Create/update with 0.14.2:**

```sh
bash run_delta_streamer_app.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```

**Read benchmarks (both versions):**

```sh
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```

Data generation is shared: run **`run_ingestion_data_generator.sh --type initial`** once, then **`--type incremental`** as needed, before running Delta Streamer for either COW or MOR.

---

## Script reference

| Script | Purpose |
|--------|--------|
| `setup_node.sh` | One-time setup: Spark, AWS jars, env (no S3 jar download). |
| `run_ingestion_data_generator.sh` | Generate parquet data: `--type initial` or `--type incremental`. |
| `run_delta_streamer_app.sh` | Run Hudi Streamer: `--table-type COPY_ON_WRITE \| MERGE_ON_READ` and `--target-hudi-version 0.14.1 \| 0.14.2`. |
| `test_hudi_benchmark.sh` | Read benchmark: `--table-type COPY_ON_WRITE \| MERGE_ON_READ` and `--target-hudi-version 0.14.1 \| 0.14.2`. |

Use `-h` or `--help` on any script for usage.
