import sys
import time
from pyspark.sql import SparkSession

def main():
    # Check if the path argument was provided
    if len(sys.argv) < 2:
        print("Error: Please provide the Hudi path as a parameter.")
        sys.exit(1)

    # Capture the first argument after the script name
    hudi_path = sys.argv[1]

    spark = SparkSession.builder \
        .appName(f"Hudi Benchmark: {hudi_path.split('/')[-1]}") \
        .getOrCreate()

    spark.sql("SET hoodie.metadata.enable=false")
    spark.conf.set("hoodie.enable.data.skipping", "false")

    print(f"Target Hudi Path: {hudi_path}")
    
    start_time = time.time()

    df = spark.read.format("hudi").load(hudi_path)
    count = df.distinct().count()

    end_time = time.time()
    
    print(f"Execution Complete. Count: {count}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    spark.stop()

if __name__ == "__main__":
    main()