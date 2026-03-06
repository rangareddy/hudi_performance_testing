// Initial Batch - Wide Timestamp Example
// Usage: spark-shell -i initial_batch.scala --conf spark.driver.memory=4g --conf spark.executor.memory=8g

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.sql.Timestamp

// Get batch ID from JVM property (for spark-shell -i compatibility)
val batchId: Int = sys.props.get("BATCH_ID").map(_.toInt).getOrElse(1)

// Create Spark session (for EMR, no need to specify master)
val spark = SparkSession.builder()
  .appName("TimestampExample-InitialBatch")
  .getOrCreate()

import spark.implicits._

// ----------------------------------------------------------------------
// Parameters
// ----------------------------------------------------------------------
val numCols = 500
val numPartitions = 10000
val baseTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)
val zone = java.time.ZoneId.systemDefault()

// S3 output path (set by run_initial_ingestion.sh from common.properties)
val outputPath = sys.env.getOrElse("TARGET_DATA", "s3://performance-benchmark-datasets-us-west-2/hudi-bench/performance/logical_ts_perf/data/wide_500cols_10000parts")

// Helper functions
def toTimestamp(local: LocalDateTime): Timestamp = Timestamp.valueOf(local)
def toMillis(local: LocalDateTime): Long = local.atZone(zone).toInstant.toEpochMilli

// ----------------------------------------------------------------------
// Define schema dynamically
// ----------------------------------------------------------------------
val fields = (1 to numCols).map { i =>
  if (i % 50 == 0) {
    // Logical timestamp columns
    if ((i / 50) % 2 == 0)
      StructField(s"ts_millis_$i", LongType, true)
    else
      StructField(s"ts_micros_$i", TimestampType, true)
  } else {
    // Regular string columns
    StructField(s"col_$i", StringType, true)
  }
}.toBuffer

// Add partition column
fields += StructField("partition_col", StringType, true)

val schema = StructType(fields)

// ----------------------------------------------------------------------
// Generate data
// ----------------------------------------------------------------------
println(s"🚀 Starting data generation with ${numCols} columns and ${numPartitions} partitions...")

val data = spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map { i =>
  val localTs = baseTime.plusSeconds(i)

  val values: Seq[Any] = (1 to numCols).map { colIdx =>
    if (colIdx % 50 == 0) {
      // alternating between ts_millis and ts_micros
      if ((colIdx / 50) % 2 == 0)
        toMillis(localTs.plusNanos(colIdx * 1000L))
      else
        toTimestamp(localTs.plusNanos(colIdx * 2000L))
    } else {
      s"value_${i}_${colIdx}"
    }
  }

  Row.fromSeq(values :+ f"partition_${i}%05d")
}

// ----------------------------------------------------------------------
// Create DataFrame and write to S3
// ----------------------------------------------------------------------
val df = spark.createDataFrame(data, schema)

println(s"📝 DataFrame created. Writing to S3: ${outputPath}")

df.repartition(numPartitions, $"partition_col")
  .write
  .mode("overwrite")
  .parquet(outputPath)

println(s"✅ Successfully generated DataFrame with ${numCols} columns and ${numPartitions} partitions.")
println(s"📍 Data written to: ${outputPath}")
df.printSchema()
df.show(5, false)

// Stop the spark session
spark.stop()

System.exit(0)