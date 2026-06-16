"""Cluster-aware SparkSession factory for Spark-on-Kubernetes jobs.

Why this exists (and why we do NOT reuse `lakehouse.spark_utils.session.get_spark`):

    get_spark() is built for the EMBEDDED-in-Airflow model. It hardcodes
    `.master("local[2]")`, binds the driver to 127.0.0.1, disables the Spark UI,
    and sets heartbeat-failure tolerances that only make sense for local mode.
    None of that is correct when the Spark Operator launches a real driver pod
    with its own executors.

In cluster mode the Spark Operator / spark-submit injects everything that
get_spark() used to hardcode:

    * `spark.master`        -> k8s://https://kubernetes.default.svc  (by the Operator)
    * Delta extensions      -> from the SparkApplication spec.sparkConf
    * S3A / MinIO config     -> from the SparkApplication spec.sparkConf
    * Delta + hadoop-aws jars -> baked into the Spark image ($SPARK_HOME/jars),
                                so there is NO Maven/Ivy download at runtime
                                (repeatable, airgap-friendly).

So the job entrypoints only need a bare builder. The medallion functions
(`ingest_table_with_audit`, `transform_table`, `build_mart`) take a `spark`
handle plus `settings` and are reused completely unchanged — they never touch
the session factory or any Spark config.
"""

from __future__ import annotations


def get_cluster_spark(app_name: str):
    """Return a SparkSession configured entirely by the SparkApplication spec.

    No `.master()` and no Delta/S3A config here on purpose — those are supplied
    by the Operator and the CRD's sparkConf. Setting them again would either be
    redundant or fight the cluster-mode values.
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()
