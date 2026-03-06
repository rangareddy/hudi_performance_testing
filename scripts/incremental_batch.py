#!/usr/bin/env python3
"""
Incremental Batch - Filter and append specific records
Usage: spark-submit incremental_batch.py
Environment: SOURCE_DATA (parquet data path). Set by run_ingestion_data_generator.sh from common.properties.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

batch_id = os.environ.get("BATCH_ID", "1")

# Create Spark session
spark = SparkSession.builder \
    .appName(f"IncrementalBatch_{batch_id}") \
    .getOrCreate()

# Data path from env (set by shell from common.properties)
target_data_path = os.environ.get("TARGET_DATA")
source_data_path = os.environ.get("SOURCE_DATA")
if source_data_path is None:
    print("❌ SOURCE_DATA not found in environment")
    exit(1)

print(f"🚀 Starting incremental batch {batch_id}...")
print(f"📍 Reading from: {source_data_path}")
print(f"📍 Writing to: {target_data_path}")

# Read the existing parquet data
df = spark.read.parquet(source_data_path)

# Filter specific rows (first 100 values)
start = (batch_id - 1) * 100 + 1
end = batch_id * 100 + 1

values = [f"value_{i}_{batch_id}" for i in range(start, end)]
filtered_df = df.filter(col("col_1").isin(values))

print(f"📊 Filtered {filtered_df.count()} records")
print(f"💾 Appending filtered data back to: {target_data_path}")

# Append the filtered data back
filtered_df.write.mode("append").parquet(target_data_path)
print("✅ Incremental batch completed successfully")
print(f"📍 Data written to: {target_data_path}")
# Stop the spark session
spark.stop()
System.exit(0)
