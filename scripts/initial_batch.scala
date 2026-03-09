import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.sql.Timestamp

// Create Spark session
val spark = SparkSession.builder()
  .appName("TimestampExample-InitialBatch")
  .getOrCreate()

import spark.implicits._

val batchId: Int = sys.props.get("BATCH_ID").map(_.toInt).getOrElse(1)
val numCols = sys.props.get("NUM_OF_COLUMNS").map(_.toInt).getOrElse(500)
val numPartitions = sys.props.get("NUM_OF_PARTITIONS").map(_.toInt).getOrElse(10000) 
val DEFAULT_TARGET="s3://performance-benchmark-datasets-us-west-2/hudi-bench/performance/logical_ts_perf/data/wide_500cols_10000parts"
val outputPath = sys.env.getOrElse("TARGET_DATA", DEFAULT_TARGET)
val enableLogicalTs: Boolean = sys.props.get("ENABLE_LOGICAL_TIMESTAMP").map(_.toBoolean).getOrElse(true)

val baseTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)
val zone = java.time.ZoneId.systemDefault()

def toTimestamp(local: LocalDateTime): Timestamp = Timestamp.valueOf(local)
def toMillis(local: LocalDateTime): Long = local.atZone(zone).toInstant.toEpochMilli

// ----------------------------------------------------------------------
// Supported datatypes rotation
// ----------------------------------------------------------------------
val primitiveTypes = Seq(
  StringType,
  IntegerType,
  LongType,
  DoubleType,
  BooleanType
)

// ----------------------------------------------------------------------
// Define schema dynamically
// ----------------------------------------------------------------------
val fields = (1 to numCols).map { i =>

  if (enableLogicalTs && i % 50 == 0) {
    if ((i / 50) % 2 == 0)
      StructField(s"ts_millis_$i", LongType, true)
    else
      StructField(s"ts_micros_$i", TimestampType, true)
  } else {
    val dtype = primitiveTypes(i % primitiveTypes.size)

    dtype match {
      case StringType  => StructField(s"col_$i", StringType, true)
      case IntegerType => StructField(s"col_$i", IntegerType, true)
      case LongType    => StructField(s"col_$i", LongType, true)
      case DoubleType  => StructField(s"col_$i", DoubleType, true)
      case BooleanType => StructField(s"col_$i", BooleanType, true)
    }
  }

}.toBuffer

// Add partition column
fields += StructField("partition_col", StringType, true)

val schema = StructType(fields)

// ----------------------------------------------------------------------
// Generate data
// ----------------------------------------------------------------------
println(s"🚀 Starting data generation")
println(s"Columns        : $numCols")
println(s"Partitions     : $numPartitions")
println(s"Logical TS     : $enableLogicalTs")

val data = spark.sparkContext.parallelize(1 to numPartitions, numPartitions).map { i =>
  val localTs = baseTime.plusSeconds(i)
  val values: Seq[Any] = (1 to numCols).map { colIdx =>
    if (enableLogicalTs && colIdx % 50 == 0) {
        if ((colIdx / 50) % 2 == 0)
          toMillis(localTs.plusNanos(colIdx * 1000L))
        else
          toTimestamp(localTs.plusNanos(colIdx * 2000L))
    } else {
      primitiveTypes(colIdx % primitiveTypes.size) match {
        case StringType =>
          s"value_${i}_${colIdx}"
        case IntegerType =>
          i + colIdx
        case LongType =>
          (i.toLong * 1000) + colIdx
        case DoubleType =>
          i.toDouble + colIdx * 0.1
        case BooleanType =>
          colIdx % 2 == 0
      }
    }
  }
  Row.fromSeq(values :+ f"partition_${i}%05d")
}

// ----------------------------------------------------------------------
// Create DataFrame and write to S3
// ----------------------------------------------------------------------
val df = spark.createDataFrame(data, schema)
println(s"✅ Successfully generated DataFrame with ${numCols} columns and ${numPartitions} partitions.")

println(s"📝 Writing data to $outputPath")

df.repartition(numPartitions, $"partition_col")
  .write
  .mode("overwrite")
  .parquet(outputPath)

println(s"📍 Data written to: ${outputPath}")
df.printSchema()
df.show(5, false)

// Stop the spark session
spark.stop()

System.exit(0)