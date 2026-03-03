import time
import datetime
from pyspark.sql import SparkSession, Row, functions as F

from fetcher import fetch_feed
from parser import parse_feed
from config import MTA_FEED_URL, HEADERS, POLL_INTERVAL_SECONDS

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
            df = spark.createDataFrame([Row(**r) for r in rows])
            df = df.withColumn("processing_ts", F.current_timestamp())
            df.write.mode("append").format("orc").saveAsTable("default.mta_subway_status")
            print(f"[INFO] Ingested {len(rows)} rows.")
    except Exception as e:
        print(f"[ERROR] Cycle failed: {e}")

if __name__ == "__main__":
    while True:
        run_ingestion_cycle()
        time.sleep(POLL_INTERVAL_SECONDS)