# Reference
#   - https://docs.databricks.com/aws/en/udf/python-udtf

from pyspark.sql.functions import lit, udtf

# define a UDTF
@udtf(returnType="sum: int, diff: int")
class GetSumDiff:
    def eval(self, x:int , y:int) -> int:
        yield x + y, x - y

## register a UDTF on a session
spark.udtf.register("get_sum_diff", GetSumDiff)

## call a registered UDTF
spark.sql("SELECT * FROM get_sum_diff(1, 2);").show()

## register a UDTF to Unity Catalog
# CREATE OR REPLACE FUNCTIUON get_sum_diff(x INT, y INT)
# RETURN TABLE (sum INT, diff INT)
# LANGUAGE PYTHON
# HANDLER 'GetSumDiff'
# AS $$
# class GetSumDiff:
#     def eval(self, x:int , y:int) -> int:
#         yield x + y, x - y
# $$;
# SELECT * FROM get_sum_diff(10, 3)