# Reference
#   - https://www.databricks.com/blog/simplify-pyspark-testing-dataframe-equality-functions

# Using DataFrame equality test functions
from pyspark.testing import assertDataFrameEqual

df_expected = spark.createDataFrame(
    data=[
        ("Alfred", 1500),
        ("Alfred", 2500),
        ("Anna", 500),
        ("Anna", 3000)
    ],
    schema=["name", "amount"]
)

df_actual = spark.createDataFrame(
    data=[
        ("Alfred", 1200),
        ("Alfred", 2500),
        ("Anna", 500),
        ("Anna", 3000)
    ],
    schema=["name", "amount"]
)

assertDataFrameEqual(df_expected, df_actual)

from pyspark.testing import assertSchemaEqual

data_expected = [["Alfred", 1500], ["Alfred", 2500], ["Anna", 500], ["Anna", 3000]]
data_actual = [["Alfred", 1500.0], ["Alfred", 2500.0], ["Anna", 500.0], ["Anna", 3000.0]]

schema_actual = "name STRING, amount DOUBLE"

df_expected = spark.createDataFrame(data = data_expected)
df_actual = spark.createDataFrame(data = data_actual, schema = schema_actual)

assertSchemaEqual(df_actual.schema, df_expected.schema)

# Structured output for debugging differences in PySpark DataFrames
from pyspark.errors import PySparkAssertionError
try:
    assertDataFrameEqual(df_expected, df_actual, includeDiffRows=True)
except PySparkAssertionError as e:
    spark.createDataFrame(e.data, schema = ["Actual", "Expected"]).show()


# Pandas API on Spark equality test functions
import pandas as pd

df_pandas = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

assert_frame_equal(df1, df_pandas) # Comparing Pandas API on Spark DataFrame with the Pandas DataFrame
assert_series_equal(df1.a, df_pandas.a) # Comparing Pandas API on Spark Series with the Pandas Series
assert_index_equal(df1.index, df_pandas.index) # Comparing Pandas API on Spark Index with the Pandas Index