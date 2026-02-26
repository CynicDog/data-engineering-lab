from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DateType, TimestampType

spark = SparkSession.builder \
    .appName("mta_multi_stage_pipeline") \
    .config("spark.sql.warehouse.dir", "/app/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS mta_db")

mta_schema = StructType([
    StructField('actual_track', StringType(), True),
    StructField('arrival_time', StringType(), True),
    StructField('departure_time', StringType(), True),
    StructField('direction', StringType(), True),
    StructField('ingestion_ts', StringType(), True),
    StructField('is_assigned', BooleanType(), True),
    StructField('route_id', StringType(), True),
    StructField('scheduled_track', StringType(), True),
    StructField('stop_id', StringType(), True),
    StructField('train_id', StringType(), True),
    StructField('trip_id', StringType(), True),
    StructField('date', DateType(), True),
    StructField('hour', IntegerType(), True)
])

input_path = "/app/pipeline-storage/"
checkpoint_path = "/app/metadata/checkpoints/mta_multi_stage"

raw_stream = (spark.readStream
              .format("json")
              .schema(mta_schema)
              .load(input_path))

cleaned_stream = (raw_stream
                  .withColumn("route_id", F.upper(F.col("route_id")))
                  .withColumn("event_time", F.to_timestamp(F.col("arrival_time")))
                  .withWatermark("event_time", "10 minutes"))

# Multi-Table Logic (for the pipeline representation in OpenMetadata)
def process_multi_stage_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.write \
        .format("parquet") \
        .mode("append") \
        .saveAsTable("mta_db.bronze_mta_raw")

    bronze_df = spark.table("mta_db.bronze_mta_raw")
    silver_df = bronze_df.filter(F.col("route_id").isNotNull())

    silver_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("mta_db.silver_mta_filtered")

    gold_df = (silver_df
    .groupBy(
        F.window(F.col("event_time"), "10 minutes", "5 minutes"),
        F.col("route_id")
    )
    .count()
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "route_id",
        "count"
    ))

    gold_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("mta_db.gold_mta_windowed_counts")


query = (cleaned_stream.writeStream
         .foreachBatch(process_multi_stage_batch)
         .option("checkpointLocation", checkpoint_path)
         .start())

print("MTA Multi-Stage Pipeline is active. Check OpenMetadata for Bronze -> Silver -> Gold lineage.")
query.awaitTermination()