from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.master("local")
    .appName("OpenMetadataLineage")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openmetadata.transport.hostPort", "http://openmetadata-server:8585")
    .config("spark.openmetadata.transport.type", "openmetadata")
    .config("spark.openmetadata.transport.pipelineServiceName", "spark_lineage_service")
    .config("spark.openmetadata.transport.pipelineName", "employee_transfer_job")
    .config("spark.openmetadata.transport.databaseServiceNames", "hive_service")
    .getOrCreate()
)

employee_df = spark.table("default.employee")
employee_df.write.mode("overwrite").saveAsTable("default.employee_new")

spark.stop()