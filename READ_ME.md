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

* Terraform workspace used: 
* S3 URI for jars and data generation script:
	s3://performance-benchmark-datasets-us-west-2/hudi-bench/logical_ts_perf/data/

**Node setup:**

```sh
BASE_PATH="s3://performance-benchmark-datasets-us-west-2/hudi-bench/logical_ts_perf"
JARS_PATH=${BASE_PATH}/jars
HUDI_VERSION="0.14.2-SNAPSHOT"
SPARK_VERSION=3.4

aws s3 cp ${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-0.14.1.jar .
aws s3 cp ${JARS_PATH}/hudi-utilities-slim-bundle_2.12-0.14.1.jar .
aws s3 cp ${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar .
aws s3 cp ${JARS_PATH}/hudi-utilities-slim-bundle_2.12-${HUDI_VERSION}.jar .

aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/initial_batch.scala \
    /home/hadoop/

aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/incremental_batch_1.py \
    /home/hadoop/

SPARK_HOME="/home/hadoop/spark-3.4.4-bin-hadoop3"
```

**Initial ingestion:**

`vi run_initial_ingestion.sh`

```sh
SPARK_HOME=/home/hadoop/spark-3.5.0-bin-hadoop3

${SPARK_HOME}/bin/spark-shell \
	--master yarn \
	--deploy-mode client \
	--conf spark.driver.memory=2g \
	--conf spark.executor.memory=7g \
	--conf spark.executor.cores=3 \
	--conf spark.executor.instances=3 \
	--conf spark.sql.shuffle.partitions=200 \
	--conf spark.default.parallelism=9 \
	--conf spark.sql.adaptive.enabled=true \
	--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.s3a.committer.name=directory \
  	-i /home/hadoop/initial_batch.scala
```

```sh
touch /home/hadoop/empty.properties
```

**Ingestion with 0.14.1:**

**Ingestion with 0.14.2:**

`vi run_delta_streamer_app.sh`

```sh
SPARK_HOME=/home/hadoop/spark-3.5.0-bin-hadoop3

HUDI_JARS=${JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar \
        ${JARS_PATH}/hudi-utilities-slim-bundle_2.12-${HUDI_VERSION}.jar

TABLE_TYPE=MERGE_ON_READ
TABLE_NAME=hudi_mor_logical
TABLE_BASE_PATH=${BASE_PATH}/data/hudi_mor_logical
SOURCE_DFS_ROOT=${BASE_PATH}/data/wide_500cols_10000parts
SCHEMA_FILE=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc

time $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
    --driver-memory 4g \
    --executor-memory 6g \
    --executor-cores 3 \
    --num-executors 3 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///var/log/spark/apps \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.hadoop.fs.s3a.committer.name=directory \
    --conf spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol \
    --conf spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --jars $HUDI_JARS \
    --props file:///home/hadoop/empty.properties \
    --table-type $TABLE_TYPE \
    --op UPSERT \
    --target-base-path $TABLE_BASE_PATH \
    --target-table $TABLE_NAME \
    --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --source-ordering-field col_1 \
    --hoodie-conf hoodie.deltastreamer.source.dfs.root=${SOURCE_DFS_ROOT} \
    --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file=$SCHEMA_FILE \
    --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file=$SCHEMA_FILE \
    --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
    --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
    --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col
```

**Read benchmark for distint query:**

`vi generate_incremental_batch_data.sh`

```sh
#!/usr/bin/env bash

set -euo pipefail

SPARK_HOME=/home/hadoop/spark-3.5.0-bin-hadoop3

${SPARK_HOME}/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=9g \
  --conf spark.executor.cores=3 \
  --conf spark.executor.instances=3 \
  --conf spark.eventLog.enabled=true  \
  --conf spark.eventLog.dir=hdfs:///var/log/spark/apps \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=9 \
  --conf spark.sql.adaptive.enabled=true \
  /home/hadoop/incremental_batch_1.py
```

```sh
bash generate_incremental_batch_data.sh
```

`vi test_hudi_benchmark.sh`

```sh
#!/usr/bin/env bash

set -euo pipefail

############################################
# Configuration
############################################

SPARK_HOME=/home/hadoop/spark-3.5.0-bin-hadoop3
HUDI_JARS=/home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/home/hadoop/hudi-jars/hudi-utilities-slim-spark3.5-bundle_2.12-0.15.0.jar
TABLE_TYPE="COPY_ON_WRITE"
PY_SCRIPT=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/hudi_benchmark.py
BASE_DATA_PATH=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/

############################################
# Usage
############################################

SCRIPT_NAME=$0

usage() {
  echo ""
  echo "Usage:"
  echo "  bash $SCRIPT_NAME --table-type <COPY_ON_WRITE|MERGE_ON_READ>"
  echo ""
  echo "Examples:"
  echo "  bash $SCRIPT_NAME --table-type COPY_ON_WRITE"
  echo "  bash $SCRIPT_NAME --table-type MERGE_ON_READ"
  echo ""
  exit 1
}

############################################
# Parse Arguments
############################################

while [[ $# -gt 0 ]]; do
  case $1 in
    --table-type)
      if [[ -z "$2" ]]; then
        echo "❌ Error: --table-type requires a value"
        usage
      fi
      TABLE_TYPE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "❌ Unknown option: $1"
      usage
      ;;
  esac
done

echo "Table Type: $TABLE_TYPE"

############################################
# Normalize Table Type
############################################

TABLE_TYPE_UPPER=$(echo "$TABLE_TYPE" | tr '[:lower:]' '[:upper:]')

############################################
# Validate Table Type
############################################

case "$TABLE_TYPE_UPPER" in
  COPY_ON_WRITE|COW)
    TABLE_TYPE="COPY_ON_WRITE"
    DATA_PATH="${BASE_DATA_PATH}/hudi_cow_logical"
    ;;
  MERGE_ON_READ|MOR)
    TABLE_TYPE="MERGE_ON_READ"
    DATA_PATH="${BASE_DATA_PATH}/hudi_mor_logical"
    ;;
  *)
    echo "❌ Invalid TABLE_TYPE: $TABLE_TYPE"
    echo "Allowed values: COPY_ON_WRITE (cow) or MERGE_ON_READ (mor)"
    exit 1
    ;;
esac

echo "Data Path: $DATA_PATH"

############################################
# Print Configuration
############################################

echo "======================================"
echo "🚀 Starting Hudi Benchmark"
echo "--------------------------------------"
echo "Table Type : $TABLE_TYPE"
echo "Data Path  : $DATA_PATH"
echo "Spark Home : $SPARK_HOME"
echo "Script     : $PY_SCRIPT"
echo "======================================"

############################################
# Run Spark Job
############################################

"$SPARK_HOME/bin/spark-submit" \
    --master yarn \
    --deploy-mode client \
    --jars "$HUDI_JARS" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
    --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
    --conf "spark.driver.memory=4g" \
    --conf "spark.executor.memory=9g" \
    --conf "spark.executor.cores=3" \
    --conf "spark.executor.instances=3" \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.eventLog.dir=hdfs:///var/log/spark/apps" \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.default.parallelism=9" \
    --conf "spark.sql.adaptive.enabled=true" \
    "$PY_SCRIPT" \
    "$DATA_PATH"

echo ""
echo "✅ Benchmark job submitted successfully"
```

```sh
bash test_hudi_benchmark.sh --table-type COPY_ON_WRITE
bash test_hudi_benchmark.sh --table-type MERGE_ON_READ
```