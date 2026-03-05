#!/usr/bin/env python3
"""
Incremental Batch - Filter and append specific records
Usage: spark-submit incremental_batch.py
Environment: SOURCE_DFS_ROOT (parquet data path). Set by generate_incremental_batch_data.sh from common.properties.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session (for EMR, no need to specify master)
spark = SparkSession.builder \
    .appName("WideTimestampExample-IncrementalBatch1") \
    .getOrCreate()

# Data path from env (set by shell from common.properties)
data_path = os.environ.get(
    "SOURCE_DFS_ROOT",
    "s3://performance-benchmark-datasets-us-west-2/hudi-bench/performance/logical_ts_perf/data/wide_500cols_10000parts"
)

print(f"🚀 Starting incremental batch...")
print(f"📍 Reading from: {data_path}")

# Read the existing parquet data
df = spark.read.parquet(data_path)

# Filter specific rows (first 100 values)
values = [f"value_{i}_1" for i in range(1, 101)]
filtered_df = df.filter(col("col_1").isin(values))

print(f"📊 Filtered {filtered_df.count()} records")
print(f"💾 Appending filtered data back to: {data_path}")

# Append the filtered data back
filtered_df.write.mode("append").parquet(data_path)

print("✅ Incremental batch completed successfully")

# Stop the spark session
spark.stop()
