# Reference
#   - https://docs.databricks.com/aws/en/delta/merge

from delta.tables import *

deltaTablePeople = DeltaTable.forName(spark, "people10m")

deltaTablePeopleUpdates = DelatTable.forName(spark, "people10mUpdates")
dfUpdates = deltaTablePeopleUpdates.toDF()

deltaTablePeople.alias('people') \
    .merge(dfUpdates.alias('updates'), 'people.id = updates.id') \
    .whenMatchedUpdate(set = {
          "people.id"           : "updates.id"
        , "people.firstName"    : "updates.firstName"
        , "people.middleName"   : "updates.middleName"
        , "people.lastName"     : "updates.lastName"
        , "people.gender"       : "updates.gender"
        , "people.birthDate"    : "updates.birthDate"
        , "people.ssn"          : "updates.ssn"
        , "people.salary"       : "updates.salary"
    }) \
    .whenNotMatchedInsert(values = {
          "people.id"           : "updates.id"
        , "people.firstName"    : "updates.firstName"
        , "people.middleName"   : "updates.middleName"
        , "people.lastName"     : "updates.lastName"
        , "people.gender"       : "updates.gender"
        , "people.birthDate"    : "updates.birthDate"
        , "people.ssn"          : "updates.ssn"
        , "people.salary"       : "updates.salary"
    }) \
    .execute()

# https://docs.databricks.com/aws/en/delta/merge#merge-operation-semantics
targetDF \
    .merge(sourceDF, "source.key = target.key") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .whenNotMatchedBySourceDelete() \
    .execute()

targetDF \
    .merge(sourceDF, "source.key = target.key") \
    .whenMatchedUpdate(set = {
    # `whenMatched` clause are executed when a source row matches a target table row based on the
    # match condition. The clause can have at most one `update` and one `delete` action. If there
    # are multiple `whenMatched` clauses, then they are evaluated in the order they are specified.
        "target.lastSeen" : "source.timestamp"
    }) \
    .whenNotMatchedInsert(values = {
    # `whenNotMatched` clauses are executed when a source row does not match any target row based on
    # the match condition. The clause can have `insert` action.
        "target.key": "source.key",
        "target.lastSeen": "source.timestamp",
        "target.status": "'active'"
    }) \
    .whenNotMatchedBySourceUpdate(
    # `whenNotMatchedBySourceUpdate` clauses are executed when a target row does not match any
    # source row based on the merge condition. The clause can specify `delete` and `updates` actions.
        condition="target.lastSeen >= (current_date() - INTERVAL '5' DAY)",
        set = {"target.status": "'inactive'"}
    ) \
    .execute()

# https://docs.databricks.com/aws/en/delta/merge#data-deduplication-when-writing-into-delta-tables
from pyspark.sql import functions as F

# Read the stream
raw_stream_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/path/to/source/logs"))

# Define Intra-batch deduplication
deduped_stream_df = (raw_stream_df
    .withWatermark("eventTimestamp", "10 minutes")
    .dropDuplicates(["uniqueId", "eventTimestamp"]))

from delta.tables import DeltaTable

def merge_to_destination(micro_batch_df, batch_id):
    newDedupedLogs = micro_batch_df
    target_table = DeltaTable.forPath(spark, "/path/to/target/delta_table")

    # Inter-table deduplication
    (target_table.alias("logs")
     .merge(
        newDedupedLogs.alias("newDedupedLogs"),
        "logs.uniqueId = newDedupedLogs.uniqueId"
    )
     .whenNotMatchedInsertAll()
     .execute())

# Start the write stream using the foreachBatch sink
query = (deduped_stream_df.writeStream
    .foreachBatch(merge_to_destination)
    .outputMode("update")
    .option("checkpointLocation", "/path/to/checkpoints/dedup_logs")
    .start())

query.awaitTermination()
