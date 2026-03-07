# Reference
#   - https://docs.databricks.com/aws/en/ldp/

## https://docs.databricks.com/aws/en/ldp/flows
from pyspark import pipelines as dp

@dp.table()
def customers_silver():
    """sql
    CREATE OR REFRESH STREAMING TABLE customer_silver
    AS SELECT * FROM STREAM(customers_bronze)
    """
    return spark.readStream.table("customers_bronze")

## https://docs.databricks.com/aws/en/ldp/flows?language=Python#using-multiple-flows-to-write-to-a-single-target
"""sql
CREATE OR REFRESH STREAM customer_silver; 
"""
dp.create_streaming_table("customer_silver")

@dp.append_flow(target="customer_silver")
def customer_silver():
    """sql
    CREATE FLOW customers_silver
    AS INSERT INTO customers_silver BY NAME
    SELECT * FROM STREAM(customers_bronze)
    """
    return spark.readStream.table("customers_bronze")

## https://docs.databricks.com/aws/en/ldp/flow-examples?language=Python#example-write-to-a-streaming-table-from-multiple-kafka-topics
"""sql
CREATE OR REFRESH STREAMING TABLE kafka_target; 
"""
dp.create_streaming_table("kafka_target")

@dp.append_flow(target="kafka_target")
def topic1():
    """sql
    CREATE FLOW topic1
    AS INSERT INTO kafka_target BY NAME
    SELECT *
    FROM read_kafka(
        bootstrapServers => 'host1:port1,...',
        subscribe => 'topic1'
    );
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "host1:port1,...")
        .option("subscribe", "topic1")
        .load()
    )

@dp.append_flow(target="kafka_target")
def topic2():
    """sql
    CREATE FLOW topic2
    AS INSERT INTO kafka_target BY NAME
    SELECT *
    FROM read_kafka(
        bootstrapServers => 'host1:port2,...',
        subscribe => 'topic1'
    );
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "host2:port2,...")
        .option("subscribe", "topic2")
        .load()
    )

## https://docs.databricks.com/aws/en/ldp/flow-examples?language=Python#example-run-a-one-time-data-backfill
@dp.table()
def csv_target():
    """sql
    CREATE OR REFRESH STREAMING TABLE csv_target
    AS SELECT * FROM
        read_files(
            "path/to/sourceDir", "csv"
        )
    """
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .load("path/to/sourceDir")

@dp.append_flow(
    target="csv_target",
    once=True
)
def backfill():
    """SQL
    CREATE FLOW backfill
    AS INSERT INTO ONCE csv_target BY NAME
    SELECT * FROM
        read_files(
            "path/to/backfillDir", "csv"
        )
    """
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .load("path/to/backfillDir")
