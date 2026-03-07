# Reference
#   - https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture

catalog = "catalog"
schema = "schema"
employees_cdf_table = "employees_cdf_table"

def write_employees_cdf_to_delta():
    data = [
        (1, "Alex", "chef", "FR", "INSERT", 1),
        (2, "Jessica", "owner", "US", "INSERT", 2),
        (3, "Mikhail", "security", "UK", "INSERT", 3),
        (4, "Gary", "cleaner", "UK", "INSERT", 4),
        (5, "Chris", "owner", "NL", "INSERT", 6),
        (5, "Chris", "manager", "NL", "UPDATE", 5), # out of order update
        (6, "Pat", "mechanic", "NL", "DELETE", 8),
        (6, "Pat", "mechanic", "NL", "INSERT", 7)
    ]
    columns = ["id", "name", "role", "country", "operation", "sequenceNum"]
    df = spark.createDataframe(data, columns)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{employees_cdf_table}")

write_employees_cdf_to_delta()

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, lit, when
from pyspark.sql.types import StringType, ArrayType

employees_table_current = "employees_table_current"
employees_table_historical = "employees_table_historical"

@dp.temporary_view
def employees_cdf():
    return spark.readStream.format("delta").table(f"{catalog}.{schema}.{employees_cdf_table}")

dp.create_target_table(f"{catalog}.{schema}.{employees_table_historical}")

dp.create_auto_cdc_flow(
    target=f"{catalog}.{schema}.{employees_table_historical}",
    source=employees_cdf_table,
    keys=["id"],
    sequence_by=col("sequenceNum"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequenceNum"],
    sorted_as_scd_type=1
)
