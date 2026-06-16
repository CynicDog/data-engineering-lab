"""Silver transform DAG — Asset-triggered, one SparkApplication per table.

Counterpart of databricks_lakehouse/dags/silver_dag.py. The original ran ALL
tables in a single in-process task; here each table is its own SparkApplication
(jobs/silver_job.py) so failures are isolated and independently retryable.

Trigger semantics unchanged: scheduled on the bronze Assets (OR — any bronze
update starts silver). Each table task emits its own silver Asset, which the
gold DAG depends on.
"""

from __future__ import annotations

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk.definitions.asset import Asset

# Task execution under KubernetesExecutor re-imports this module without dags/ on
# sys.path, so the sibling import must put it there itself. See bronze_dag.
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from _spark_app import NAMESPACE, render_silver
from lakehouse.config.registry import load_registry

registry = load_registry()

BRONZE_ASSETS = [
    Asset(f"s3://lakehouse/bronze/{ch.name}/{t.name}")
    for ch in registry.channels
    for t in ch.tables
]
SILVER_ASSETS = {
    (ch.name, t.name): Asset(f"s3://lakehouse/silver/{ch.name}/{t.name}")
    for ch in registry.channels
    for t in ch.tables
}


def _dag():
    from airflow.sdk import dag

    @dag(
        dag_id="transform_silver",
        schedule=BRONZE_ASSETS,
        catchup=False,
        max_active_runs=1,
        tags=["silver", "transform", "delta", "spark-on-k8s"],
        doc_md="""
## Silver Transforms (Spark-on-Kubernetes)

One `SparkKubernetesOperator` per (channel, table), each running
`jobs/silver_job.py`: cast -> dedup (latest by `_ingest_ts`) -> PII mask ->
derived columns, all driven by `config/table_registry.yaml`.

**Trigger**: bronze Asset events from `ingest_bronze`.
**Outlets**: per-table silver Assets that trigger `build_gold`.
""",
    )
    def transform_silver():
        for ch in registry.channels:
            for spec in ch.tables:
                SparkKubernetesOperator(
                    task_id=f"silver_{ch.name}_{spec.name}",
                    namespace=NAMESPACE,
                    application_file=render_silver(ch.name, spec.name),
                    # In-cluster service-account credentials; no Airflow
                    # connection is involved. See bronze_dag.
                    kubernetes_conn_id=None,
                    in_cluster=True,
                    get_logs=True,
                    do_xcom_push=False,
                    outlets=[SILVER_ASSETS[(ch.name, spec.name)]],
                    retries=1,
                )

    return transform_silver()


dag = _dag()
