# Reference
#   - https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture

catalog = "catalog"
schema = "schema"
employees_cdf_table = "employees_cdf_table"
employees_table = "employees_table"

def upsert_to_delta(micro_batch_df, batch_id):
    micro_batch_df = micro_batch_df \
            .groupBy("id") \
            .agg(max_by(struct("*"), "sequenceNum").alias("row")) \
            .select("row.*").createOrReplaceTempView("updates")

    micro_batch_df.sparkSession.sql(f"""
        MERGE INTO {catalog}.{schema}.{employees_table} t
        USING updates s 
        ON s.id = t.id 
        WHEN MATCHED AND s.operation = 'DELETE' THEN UPDATE SET DELETED_AT=now() 
        WHEN MATCHED THEN UPDATE SET 
            name    = CASE WHEN s.sequenceNum > t.sequenceNum THEN s.name    ELSE t.name     END
            age     = CASE WHEN s.sequenceNum > t.sequenceNum THEN s.age     ELSE t.age      END
            country = CASE WHEN s.sequenceNum > t.sequenceNum THEN s.country ELSE t.country  END     
    """)

def create_table():
    spark.sql(f"""DROP TABLE IF EXISTS {catalog}.{schema}.{employees_table}""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{employees_table}
        (id, INT, name STRING, age INT, country STRING)
    """)

create_table()

cdcData = spark.readStream.table(f"{catalog}.{schema}.{employees_cdf_table}")

cdcData.writeStream \
    .foreachBatch(upsert_to_delta) \
    .outputMode("append") \
    .start()

# ```sql
# CREATE VIEW employees_v AS
# SELECT * FROM employees_table
# WHERE DELETED_AT = NULL
# ```
#
# ```sql
# DELETE FROM employees_table
# WHERE DELETED_AT < now() INTERVAL 1 DAT
# ```