# Reference
#   - https://docs.databricks.com/aws/en/udf/python

from pyspark.sql.types import LongType

## register a function as a UDF
def squared(s):
    return s * s

spark.udf.register("squaredWithPython", squared, LongType())

## call the UDF in Spark SQL
spark.range(1, 20).createOrReplaceTempView("test")
"""sql 
SELECT id, squaredWithPython(id) AS id_squared 
FROM test 
"""

## use UDF with DataFrames
from pyspark.sql.functions import udf
squared_udf = udf(squared, LongType())

df = spark.table("test")
df.select("id", squared_udf("id").alias("id_squared"))

## alternatively, you can declare the same UDF using annotation syntax
@udf("long")
def squared_udf(s):
    return s * s

df = spark.table("test")
df.select("id", squared_udf("id").alias("id_squared"))


