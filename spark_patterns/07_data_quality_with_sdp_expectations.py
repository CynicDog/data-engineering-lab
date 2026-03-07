# Reference
#   - https://docs.databricks.com/aws/en/ldp/expectations

# Simple constraint
@dp.expect("non_negative_price", "price >= 0")
# ```sql
# CONSTRAINT non_negative_price EXPECT (price >= 0)
# ```

# SQL functions
@dp.expect("valid_date", "year(transaction_date) >= 2020")
# ```sql
# CONSTRAINT valid_date EXPECT (year(transaction_date) >= 2020)
# ```

# CASE statements
@dp.expect_or_drop("valid_order_status", """
    CASE 
        WHEN type = 'ORDER' THEN status IN ('PENDING', 'CANCELED', 'CANCELED')
        WHEN type = 'REFUND' THEN status IN ('PENDING', 'APPROVED', 'REJECTED')
        ELSE false
    END 
""")
# ```sql
# CONSTRAINT valid_order_status EXPECT (
#   CASE
#     WHEN type = 'ORDER' THEN status IN ('PENDING', 'COMPLETED', 'CANCELLED')
#     WHEN type = 'REFUND' THEN status IN ('PENDING', 'APPROVED', 'REJECTED')
#     ELSE false
#   END
# ) ON VALIDATION DROP ROW

# Complex boolean logic
@dp.expect_or_fail("valid_order_state", """
   (status = 'ACTIVE' AND balance > 0)
   OR (status = 'PENDING' AND created_date > current_date() - INTERVAL 7 DAYS)
""")
# ```sql
# CONSTRAINT valid_order_state EXPECT (
#     (status = 'ACTIVE' AND balance > 0)
#     OR (status = 'PENDING' AND created_date > current_date() - INTERVAL 7 DAYS)
# ) ON VALIDATION FAIL UPDATE
# ```
def prepared_data():
    return spark.read.table("transactions")