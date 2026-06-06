"""SparkSession factory with Delta Lake and optional S3A (MinIO/ADLS) support.

Key teaching point — compute cold start:
    In Databricks, creating a SparkSession on a Job Cluster means waiting
    6–7 minutes for Azure VMs to provision. Here, the session starts in
    seconds because the JVM is warm inside the Airflow container.

    The architecture lesson: keep a warm compute layer (All-Purpose Cluster /
    SQL Warehouse in Databricks, always-on container here) and never spin up
    per-job VMs for latency-sensitive or interactive workloads.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lakehouse.config.settings import Settings


def get_spark(settings: "Settings | None" = None, app_name: str = "lakehouse"):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # Sane default for local
        .config("spark.ui.enabled", "false")
    )

    # hadoop-aws must go through extra_packages, not spark.jars.packages on the
    # builder directly — configure_spark_with_delta_pip overwrites that config key.
    extra_packages: list[str] = []
    if settings and settings.s3_endpoint:
        extra_packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
        for key, value in settings.spark_s3a_conf.items():
            builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
