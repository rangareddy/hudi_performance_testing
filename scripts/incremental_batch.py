#!/usr/bin/env python3
"""
Incremental Batch - Filter and append specific records
Usage: spark-submit incremental_batch.py
Environment:
  SOURCE_DATA - parquet source path
  TARGET_DATA - parquet output path
  BATCH_ID
  NUM_OF_RECORDS_TO_UPDATE
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_env_int(name: str, default: int) -> int:
    """Read integer environment variable safely."""
    return int(os.environ.get(name, str(default)))


def create_spark(app_name: str) -> SparkSession:
    """Create Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def calculate_range(batch_id: int, num_records: int):
    """Calculate record range for the batch."""
    start = (batch_id - 1) * num_records + 1
    end = batch_id * num_records + 1
    return start, end


def generate_values(start: int, end: int, batch_id: int):
    """Generate filter values."""
    return [f"value_{i}_{batch_id}" for i in range(start, end)]


def run_incremental_batch(spark: SparkSession, batch_id: int):
    """Main processing logic."""

    num_records = get_env_int("NUM_OF_RECORDS_TO_UPDATE", 100)
    source_data_path = os.environ.get("SOURCE_DATA")
    target_data_path = os.environ.get("TARGET_DATA")

    if not source_data_path:
        print("❌ SOURCE_DATA not found in environment")
        sys.exit(1)

    if not target_data_path:
        print("❌ TARGET_DATA not found in environment")
        sys.exit(1)

    print(f"🚀 Starting incremental batch {batch_id}")
    
    start, end = calculate_range(batch_id, num_records)
    values = generate_values(start, end, batch_id)

    print(f"📍 Reading from: {source_data_path}")
    df = spark.read.parquet(source_data_path)

    print(f"📍 Filtering records from {start} to {end}")
    filtered_df = df.filter(col("col_1").isin(values))
    record_count = filtered_df.count()
    print(f"✅ Filtered {record_count} records")

    print(f"📍 Writing to: {target_data_path}")
    filtered_df.write.mode("append").parquet(target_data_path)

    print("✅ Incremental batch completed successfully")
    print(f"📍 Data written to: {target_data_path}")


def main():
    batch_id = get_env_int("BATCH_ID", 1)

    spark = create_spark(f"IncrementalBatch_{batch_id}")

    try:
        run_incremental_batch(spark, batch_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
