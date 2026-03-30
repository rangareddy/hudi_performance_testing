import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.sql.{Date, Timestamp}
import java.math.BigDecimal

// Create Spark session
val spark = SparkSession.builder()
  .appName("TimestampExample-InitialBatch")
  .getOrCreate()

import spark.implicits._

val batchId: Int = sys.env.get("BATCH_ID").map(_.toInt).getOrElse(0)
val numCols = sys.env.get("NUM_OF_COLUMNS").map(_.toInt).getOrElse(500)
val numPartitions = sys.env.get("NUM_OF_PARTITIONS").map(_.toInt).getOrElse(10000)

val DEFAULT_TARGET="s3://performance-benchmark-datasets-us-west-2/hudi-bench/performance/logical_ts_perf/data/wide_500cols_10000parts"
val outputPath = sys.env.getOrElse("TARGET_DATA", DEFAULT_TARGET)
val enableLogicalTs: Boolean = sys.env.get("IS_LOGICAL_TIMESTAMP_ENABLED").map(_.toBoolean).getOrElse(true)

val baseTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)
val zone = java.time.ZoneId.systemDefault()

def toTimestamp(local: LocalDateTime): Timestamp = Timestamp.valueOf(local)
def toMillis(local: LocalDateTime): Long = local.atZone(zone).toInstant.toEpochMilli

// ----------------------------------------------------------------------
// First 10 datatypes
// ----------------------------------------------------------------------
val firstTenTypes = Seq(
  StringType,
  ShortType,
  IntegerType,
  LongType,
  FloatType,
  DoubleType,
  DecimalType(18,2),
  DateType,
  TimestampType,
  StringType
)

// ----------------------------------------------------------------------
// Define schema dynamically
// ----------------------------------------------------------------------
val fields = (1 to numCols).map { i =>
  if (i <= 10) {
    StructField(s"col_$i", firstTenTypes(i - 1), true)
  } else if (enableLogicalTs && i % 50 == 0) {
    if ((i / 50) % 2 == 0)
      StructField(s"ts_millis_$i", LongType, true)
    else
      StructField(s"ts_micros_$i", TimestampType, true)
  } else {
    StructField(s"col_$i", StringType, true)
  }
}.toBuffer

// Add partition column
fields += StructField("partition_col", StringType, true)

val schema = StructType(fields)

// ----------------------------------------------------------------------
// Generate data
// ----------------------------------------------------------------------
println(s"🚀 Starting data generation")
println(s"Batch ID: $batchId")
println(s"Columns: $numCols  Partitions: $numPartitions")
println(s"Logical Timestamp Enabled: $enableLogicalTs")

val data = spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map { i =>
  val localTs = baseTime.plusSeconds(i)
  val values = (1 to numCols).map { colIdx =>
    if (colIdx <= 10) {
      firstTenTypes(colIdx - 1) match {
        case StringType =>
          s"value_${i}_${colIdx}"
        case ShortType =>
          (i + colIdx).toShort
        case IntegerType =>
          i + colIdx
        case LongType =>
          i.toLong * 1000 + colIdx
        case FloatType =>
          i.toFloat + colIdx * 0.1f
        case DoubleType =>
          i.toDouble + colIdx * 0.01
        case DecimalType() =>
          new BigDecimal(i * colIdx * 0.1)
        case DateType =>
          Date.valueOf(baseTime.toLocalDate.plusDays(i))
        case TimestampType =>
          toTimestamp(localTs)
      }
    } else if (enableLogicalTs && colIdx % 50 == 0) {
      if ((colIdx / 50) % 2 == 0)
        toMillis(localTs.plusNanos(colIdx * 1000))
      else
        toTimestamp(localTs.plusNanos(colIdx * 2000))
    } else {
      s"value_${i}_${colIdx}"
    }
  }

  Row.fromSeq(values :+ f"partition_${i}%05d")
}

// ----------------------------------------------------------------------
// Create DataFrame
// ----------------------------------------------------------------------
val df = spark.createDataFrame(data, schema)
println(s"✅ Successfully generated DataFrame with ${numCols} columns and ${numPartitions} partitions.")

println(s"📝 Writing data to $outputPath")
df.repartition($"partition_col")
  .write
  .mode("overwrite")
  .parquet(outputPath)

println(s"📍 Data written to: ${outputPath}")
df.printSchema()
df.show(5, false)

// Stop the spark session
spark.stop()

System.exit(0)