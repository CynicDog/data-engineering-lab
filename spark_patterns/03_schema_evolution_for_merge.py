# ```sql
# MERGE WITH SCHEMA EVOLUTION INTO target
# USING source
# ON source.key = target.key
# WHEN MATCHED THEN
#     UPDATE SET *
# WHEN NOT MATCHED THEN
#     INSERT *
# WHEN NOT MATCHED BY SOURCE THEN
#     DELETE
# ```

targetTable \
    .merge(sourceDF, "source.key = target.key") \
    .withSchemaEvolution() \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .whenNotMatchedBySourceDelete() \
    .execute()