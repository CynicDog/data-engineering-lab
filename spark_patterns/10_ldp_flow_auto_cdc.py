# Reference
#   - https://docs.databricks.com/aws/en/ldp/cdc

## https://docs.databricks.com/aws/en/ldp/cdc#process-scd-type-1-updates
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def users():
    return spark.readStream.table("cdc_data.users")

dp.create_streaming_table("target")

dp.create_auto_cdc_flow(
    target              = "target",
    source              = "users",
    keys                = ["userId"],
    sequence_by         = col("sequenceNum"),
    apply_as_deletes    = expr("operation = 'DELETE'"),
    apply_as_truncates  = expr("operation = 'TRUNCATE'"),
    except_column_list  = ["operation", "sequenceNum"],
    stored_as_scd_type  = 1
)
"""sql
CREATE FLOW my_flow AS AUTO CDC INTO    
    target 
FROM 
    stream(cdc_data.users)
KEYS
    (userId)
APPLY AS DELETE WHEN 
    operation = "DELETE"
APPLY AS TRUNCATE WHEN 
    operation = "TRUNCATE"
SEQUENCE BY 
    sequenceNum
COLUMNS * EXCEPT 
    (operation, sequenceNum)
STORED AS 
    SCD TYPE 1
"""

## https://docs.databricks.com/aws/en/ldp/cdc?language=SQL#example-periodic-snapshot-processing
@dp.view(name="source")
def source():
    return spark.read.table("mycatalog.myschema.mytable")

dp.create_streaming_table("target")

dp.create_auto_cdc_from_snapshot_flow(
    traget              = "target",
    source              = "source",
    keys                = ['key'],
    stored_as_scd_type  = 2
)

## https://docs.databricks.com/aws/en/ldp/cdc?language=SQL#example-historical-snapshot-processing
def exist(file_name):
    return True # Storage system-dependent function

def next_snapshot_and_version(latest_snapshot_version) -> (DataFrame, int):
    latest_snapshot_version = latest_snapshot_version or 0
    next_version = latest_snapshot_version + 1
    file_name = "dir_path/fileName_" + next_version + ".csv"
    if exist(file_name):
        return spark.read.load(file_name), next_version
    else:
        return None

dp.create_streaming_table("target")

dp.create_auto_cdc_from_snapshot_flow(
    target = "target",
    source = next_snapshot_and_version,
    keys = ['key'],
    stored_as_scd_type = 2,
    track_history_column_list = ["TrackingCol"]
)