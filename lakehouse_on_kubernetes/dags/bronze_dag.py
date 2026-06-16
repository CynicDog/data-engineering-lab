"""Bronze ingestion DAG — submits one SparkApplication per (channel, table).

This is the Kubernetes-native counterpart of databricks_lakehouse/dags/bronze_dag.py.
The topology is identical — one independently-retryable task per source table,
each emitting an Airflow Asset on success — but the EXECUTION model changed:

    Before: the task called get_spark() and ran Spark IN-PROCESS (local[2]).
    Now:    the task submits a SparkApplication CRD; the Spark Operator launches
            a dedicated driver + executor pods that run jobs/bronze_job.py.

Tasks are still generated dynamically from config/table_registry.yaml — adding a
table is one YAML entry, zero DAG code changes.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk.definitions.asset import Asset

# Airflow only guarantees the dags/ folder on sys.path during DAG parsing. Task
# execution under KubernetesExecutor re-imports this module in a fresh worker pod
# where that guarantee does not hold, so sibling imports must put dags/ on the
# path themselves.
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from _spark_app import NAMESPACE, render_bronze
from lakehouse.config.registry import load_registry

registry = load_registry()

BRONZE_ASSETS = {
    (ch.name, t.name): Asset(f"s3://lakehouse/bronze/{ch.name}/{t.name}")
    for ch in registry.channels
    for t in ch.tables
}


def _dag():
    from airflow.sdk import dag

    @dag(
        dag_id="ingest_bronze",
        schedule="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        # Serialize the per-table tasks: every bronze ingest appends to the SAME
        # control/ingestion_log Delta table, and concurrent Delta commits on S3A
        # (no lock coordinator) conflict — the losers fail with a
        # concurrent-append/version conflict. Running one task at a time makes the
        # audit appends sequential. (Matches the source project's bronze_dag.)
        # Silver/Gold write per-table paths with no shared table, so they stay parallel.
        max_active_tasks=1,
        tags=["bronze", "ingest", "delta", "spark-on-k8s"],
        doc_md="""
## Bronze Ingestion (Spark-on-Kubernetes)

One `SparkKubernetesOperator` per (channel, table). Each submits a
`SparkApplication` to the Spark Operator, which runs `jobs/bronze_job.py` in a
dedicated driver pod with 2 executor pods, reading Parquet from the MinIO
landing zone and writing partitioned Delta to bronze.

**Watch a run**: `kubectl get sparkapplications -n lakehouse -w`

**Asset outlets**: each successful task emits a bronze Asset that triggers
`transform_silver` — no flag files, no polling.
""",
    )
    def ingest_bronze():
        for ch in registry.channels:
            for spec in ch.tables:
                SparkKubernetesOperator(
                    task_id=f"ingest_{ch.name}_{spec.name}",
                    namespace=NAMESPACE,
                    application_file=render_bronze(ch.name, spec.name),
                    # The worker pod runs in-cluster under a service account that
                    # already carries Kubernetes API credentials, so the operator
                    # talks to the API directly. conn_id=None selects in-cluster
                    # config; no Airflow connection is involved.
                    kubernetes_conn_id=None,
                    in_cluster=True,
                    get_logs=True,
                    do_xcom_push=False,
                    outlets=[BRONZE_ASSETS[(ch.name, spec.name)]],
                    retries=2,
                )

    return ingest_bronze()


dag = _dag()
