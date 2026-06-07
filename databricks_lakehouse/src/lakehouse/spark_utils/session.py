"""SparkSession factory with Delta Lake and optional S3A (MinIO/ADLS) support.

Key teaching point — compute cold start:
    In Databricks, creating a SparkSession on a Job Cluster means waiting
    6–7 minutes for Azure VMs to provision. Here, the session starts in
    seconds because the JVM is warm inside the Airflow container.

    The architecture lesson: keep a warm compute layer (All-Purpose Cluster /
    SQL Warehouse in Databricks, always-on container here) and never spin up
    per-job VMs for latency-sensitive or interactive workloads.

Embedded-Spark-in-Airflow constraints (local dev only):
    Airflow's LocalExecutor runs a pool of long-lived worker processes.  Each
    worker can execute several tasks in sequence inside the *same* Python
    process, meaning they share a single Py4J JVM gateway and thus a single
    SparkContext (getOrCreate() returns the same instance).

    Rules that follow from this:
      1. Never call spark.stop() inside a task — it corrupts the shared JVM
         state and causes the next task's BlockManager registration to fail
         with a NullPointerException.
      2. Use local[2] (not local[*]) so the JVM never saturates all CPUs and
         leaves the Python process unable to send Airflow API heartbeats.
      3. Keep driver memory modest (512 m) so four concurrent worker processes
         don't collectively exhaust the Docker VM's RAM.
      4. Set heartbeat.maxFailures high — the local-mode heartbeat NPE is a
         known Spark 3.5.x quirk; we tolerate it rather than let it kill the
         executor thread.
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
        # local[2]: 2 executor threads keep the JVM from consuming all host
        # CPUs and starving the Python process that talks to the Airflow API.
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # 512 m is sufficient for the small synthetic datasets in this lab.
        # Four concurrent worker processes × 2 g would OOM the Docker VM.
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # Bind to loopback so RPC never tries to resolve the random container hostname.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Heartbeat tolerance: the local-mode BlockManagerMasterEndpoint NPE
        # increments the failure counter but does not actually affect task
        # correctness.  Set the threshold high so the executor thread never
        # calls System.exit() mid-job.
        .config("spark.executor.heartbeat.maxFailures", "9999")
        # Longer heartbeat interval → fewer NPE log lines, less RPC churn.
        .config("spark.executor.heartbeatInterval", "30s")
        # Give S3A / Delta commits enough time on a slow Docker network.
        .config("spark.network.timeout", "300s")
        .config("spark.rpc.askTimeout", "120s")
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

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
