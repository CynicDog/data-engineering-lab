import time
import datetime
from pyspark.sql import SparkSession, Row, functions as F

from fetcher import fetch_feed
from parser import parse_feed
from config import MTA_FEED_URL, HEADERS, POLL_INTERVAL_SECONDS

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, IntegerType

mta_comprehensive_schema = StructType([
    # Header Data
    StructField("ingestion_ts", TimestampType(), True),
    StructField("feed_timestamp", TimestampType(), True),
    StructField("nyct_subway_version", StringType(), True),
    StructField("route_replacement_until", TimestampType(), True),

    # Trip Descriptor Fields
    StructField("trip_id", StringType(), True),
    StructField("affected_trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("direction_id", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("schedule_relationship", StringType(), True),

    # NYCT Trip Extensions
    StructField("nyct_train_id", StringType(), True),
    StructField("nyct_is_assigned", BooleanType(), True),
    StructField("nyct_direction", StringType(), True),

    # Experimental TripMod Fields
    StructField("mod_shape_id", StringType(), True),

    # StopTimeUpdate Fields
    StructField("stop_id", StringType(), True),
    StructField("stop_sequence", IntegerType(), True),
    StructField("arrival_time", TimestampType(), True),
    StructField("departure_time", TimestampType(), True),
    StructField("stu_relationship", StringType(), True),

    # NYCT Stop Extensions
    StructField("scheduled_track", StringType(), True),
    StructField("actual_track", StringType(), True),

    # Vehicle Data
    StructField("vehicle_id", StringType(), True),
    StructField("vehicle_label", StringType(), True),
    StructField("vehicle_license", StringType(), True),
    StructField("vehicle_last_movement_ts", TimestampType(), True),
    StructField("vehicle_current_status", StringType(), True),
    StructField("occupancy_status", StringType(), True),

    # Alert Data
    StructField("is_delayed", BooleanType(), True),
    StructField("alert_text", StringType(), True),
    StructField("alert_cause", StringType(), True),
    StructField("alert_effect", StringType(), True)
])

spark = (SparkSession.builder.master("local")
    .appName("MTA_Ingestion_Job")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openmetadata.transport.hostPort", "http://openmetadata-server:8585")
    .config("spark.openmetadata.transport.type", "openmetadata")
    .config("spark.openmetadata.transport.pipelineServiceName", "mta_service")
    .config("spark.openmetadata.transport.pipelineName", "mta_raw_to_hive")
    .config("spark.openmetadata.transport.databaseServiceNames", "hive_service")
    .getOrCreate())


def run_ingestion_cycle():
    ingestion_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    try:
        feed = fetch_feed(MTA_FEED_URL, HEADERS)
        rows = parse_feed(feed, ingestion_ts)
        if rows:
            df = spark.createDataFrame([Row(**r) for r in rows], schema=mta_comprehensive_schema)
            df = df.withColumn("processing_ts", F.current_timestamp())
            df.write.mode("append").format("orc").saveAsTable("default.mta_subway_status")
            print(f"[INFO] Ingested {len(rows)} rows.")
    except Exception as e:
        import traceback
        print(f"[ERROR] Cycle failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    while True:
        run_ingestion_cycle()
        time.sleep(POLL_INTERVAL_SECONDS)