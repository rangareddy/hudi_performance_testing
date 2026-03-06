# Hudi Performace Testing

## Hudi Logical Timestamp Testsing

**Test Cases**

Use 0.14.2 branch and cherrypick

Benchmark 1 - All 500 fields will not contain timestamp columns

Benchmark 2 - use initial_batch.scala

	* initial_batch.scala -- dataset with 500 cols out which 10 cols are ts. 
	* incremental_batch - incremental batches

**Process**

1. Initial batch + 1 incremental batch with 0.14.1
1. Get read bechmarks numbers using both jars 0.14.1 and 0.14.2
1. Incremental batches using new 0.14.2 jar
1. Get read bechmarks numbers using both jars 0.14.1 and 0.14.2

**Setup Used:**

```sh
bash setup_node.sh
```

## COW Table

**Generating Initial Data for Ingestion:**

```sh
bash run_ingestion_data_generator.sh --type initial
```

**Initial Hudi table generator using 0.14.1**

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

**Read the performance using both 0.14.1 and 0.14.2**

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

**Run the incremenatal hudi table data generator**

```sh
bash run_ingestion_data_generator.sh --type incremental
```

**Incremental Hudi table generator using 0.14.1**

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
```

**Read the performance using both 0.14.1 and 0.14.2**

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

**Run the incremenatal hudi table data generator**

```sh
bash run_ingestion_data_generator.sh --type incremental
```

**Incremental Hudi table generator using 0.14.2**

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

**Run the incremenatal hudi table data generator**

```sh
bash run_ingestion_data_generator.sh --type incremental
```

**Incremental Hudi table generator using 0.14.2**

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

**Read the performance using both 0.14.1 and 0.14.2**

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

## MOR Table

```sh
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.1
bash run_delta_streamer_app.sh --table-type COPY_ON_WRITE --target-hudi-version 0.14.2
```

```sh
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.1
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ --target-hudi-version 0.14.2
```