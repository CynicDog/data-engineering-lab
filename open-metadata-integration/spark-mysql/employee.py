from pyspark.sql import SparkSession

# Note: Paths are internal to the Spark container
spark = (
    SparkSession.builder.master("local")
    .appName("OpenMetadataLineageTest")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openmetadata.transport.hostPort", "http://openmetadata-server:8585")
    .config("spark.openmetadata.transport.type", "openmetadata")
    .config("spark.openmetadata.transport.pipelineServiceName", "spark_lineage_service")
    .config("spark.openmetadata.transport.pipelineName", "employee_transfer_job")
    .config("spark.openmetadata.transport.databaseServiceNames", "local_mysql")
    .getOrCreate()
)

# Read from MySQL
employee_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:mysql://openmetadata_mysql:3306/openmetadata_db")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "openmetadata_db.employee")
    .option("user", "openmetadata_user")
    .option("password", "openmetadata_password")
    .load()
)

# Write to MySQL
(
    employee_df.write.format("jdbc")
    .option("url", "jdbc:mysql://openmetadata_mysql:3306/openmetadata_db")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "openmetadata_db.employee_new")
    .option("user", "openmetadata_user")
    .option("password", "openmetadata_password")
    .mode("overwrite")
    .save()
)

spark.stop()