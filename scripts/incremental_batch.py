#!/usr/bin/env python3
"""
Incremental Batch - Filter and append specific records
Usage: spark-submit incremental_batch.py
Environment:
  SOURCE_DATA - parquet source path
  TARGET_DATA - parquet output path
  BATCH_ID
  NUM_OF_RECORDS_TO_UPDATE
  NUM_OF_RECORDS_PER_PARTITION - when > 1, scope to first N partition_col values, then at most
      M rows per partition (ordered by row index parsed from col_1) before col_1 filter / validate / write.
  NUM_OF_PARTITIONS - used to cap how many distinct partition_col values exist (default 2000)
  INCREMENTAL_PARTITION_SCAN_LIMIT - max partition keys to include when NUM_OF_RECORDS_PER_PARTITION > 1 (default 100)
  INCREMENTAL_ROWS_PER_PARTITION_CAP - max rows per partition in that scope (default 100)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col, regexp_extract, row_number
from pyspark.sql.window import Window


def create_spark(app_name: str) -> SparkSession:
    """Create Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def get_env_int(name: str, default: int) -> int:
    """Read integer environment variable safely; invalid or empty values use default."""
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def calculate_range(batch_id: int, num_records: int):
    """Calculate col_1 row-index range for the batch (inclusive)."""
    if batch_id == 0:
        return 1, num_records
    return (batch_id - 1) * num_records + 1, batch_id * num_records


def generate_values(start: int, end: int, batch_id: int):
    """Generate filter values."""
    batch_id = 1 if batch_id == 0 else batch_id
    return [f"value_{i}_{batch_id}" for i in range(start, end + 1)]


def run_incremental_batch(spark: SparkSession, batch_id: int):
    """Main processing logic."""

    source_data_path = os.environ.get("SOURCE_DATA")
    target_data_path = os.environ.get("TARGET_DATA")

    if not source_data_path:
        print("❌ SOURCE_DATA not found in environment")
        sys.exit(1)

    if not target_data_path:
        print("❌ TARGET_DATA not found in environment")
        sys.exit(1)

    print(f"🚀 Starting incremental batch {batch_id}")
    is_incremental_data_processed = False
    total_records_to_process = None  # set when building from col_1 filter; None = skip count check (chained read)
    if batch_id > 1:
        parent = os.path.dirname(source_data_path)
        source_data_path = os.path.join(parent, f"batch_{batch_id - 1}")
        final_bench_df = None
        try:
            final_bench_df = spark.read.parquet(source_data_path)
            is_incremental_data_processed = True
        except Exception as e:
            print(f"❌ Error reading source data path {source_data_path}: {e}")
        
    if not is_incremental_data_processed:
        num_partitions = get_env_int("NUM_OF_PARTITIONS", 2000)
        num_records_per_partition = get_env_int("NUM_OF_RECORDS_PER_PARTITION", 1)
        num_of_records_to_update = get_env_int("NUM_OF_RECORDS_TO_UPDATE", 100)
        num_of_file_groups_to_touch = get_env_int("NUM_OF_FILE_GROUPS_TO_TOUCH", 100)

        total_records_available = num_partitions * num_records_per_partition
        total_records_to_update = num_of_records_to_update * num_of_file_groups_to_touch
        total_records_per_partition_to_update = num_of_records_to_update if num_records_per_partition > num_of_records_to_update else num_records_per_partition
        total_records_to_process = num_of_file_groups_to_touch * total_records_per_partition_to_update

        start, end = calculate_range(batch_id, num_of_records_to_update)
        values = generate_values(start, end, batch_id)

        print(f"📍 Reading from: {source_data_path}")
        df = spark.read.parquet(source_data_path).persist(StorageLevel.MEMORY_AND_DISK)
        if total_records_per_partition_to_update > 1:
            top_n_partitions = (df.select("partition_col").distinct().orderBy("partition_col").limit(num_of_file_groups_to_touch))
            allowed_partitions = {r[0] for r in top_n_partitions.collect()}
            required_for_batch = {
                f"partition_{(i % num_partitions):05d}" for i in range(start, end + 1)
            }
            allowed_partitions |= required_for_batch
            if not allowed_partitions:
                print("❌ No partition_col values found in source")
                sys.exit(1)
            
            top_n_partitions_df = df.join(top_n_partitions, on="partition_col", how="inner")
            window_spec = Window.partitionBy("partition_col").orderBy("col_1")
            final_bench_df = (top_n_partitions_df.withColumn("row_num", row_number().over(window_spec))
                                .filter(col("row_num") <= total_records_per_partition_to_update)
                                .drop("row_num"))
        else:
            final_bench_df = df.filter(col("col_1").isin(values))
        df.unpersist()

    final_bench_df = final_bench_df.persist(StorageLevel.MEMORY_AND_DISK)
    record_count = final_bench_df.count()
    print(f"✅ Filtered {record_count} records")
    if total_records_to_process is not None and record_count != total_records_to_process:
        final_bench_df.unpersist()
        print(f"❌ Expected exactly {total_records_to_process} records, got {record_count} (batch_id={batch_id}, total_records_to_process={total_records_to_process})")
        sys.exit(1)

    print(f"📍 Writing to: {target_data_path}")
    final_bench_df.repartition(1).write.mode("append").parquet(target_data_path)
    print(f"📍 Data written to: {target_data_path} for batch {batch_id}")
    final_bench_df.unpersist()
    print(f"✅ Incremental batch {batch_id} completed successfully")


def main():
    batch_id = get_env_int("BATCH_ID", 1)
    REQUESTED_BATCH_ID = get_env_int("REQUESTED_BATCH_ID", batch_id)   
    print(f"🚀 Starting incremental batch {REQUESTED_BATCH_ID}")
    spark = None
    try:
        spark = create_spark(f"IncrementalBatch_{REQUESTED_BATCH_ID}")
        run_incremental_batch(spark, batch_id)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
