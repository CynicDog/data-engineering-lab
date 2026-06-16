"""Gold mart DAG — Asset-triggered, one SparkApplication per mart.

Counterpart of databricks_lakehouse/dags/gold_dag.py. Each mart from the
registry (voc_daily, policy_summary, claims_analysis) runs as its own
SparkApplication (jobs/gold_job.py), independently retryable.

Completes the Bronze -> Silver -> Gold Asset chain:
    ingest_bronze --[bronze assets]--> transform_silver --[silver assets]--> build_gold
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

from _spark_app import NAMESPACE, render_gold
from lakehouse.config.registry import load_registry

registry = load_registry()

# Gold depends on every silver table being refreshed.
SILVER_ASSETS = [
    Asset(f"s3://lakehouse/silver/{ch.name}/{t.name}")
    for ch in registry.channels
    for t in ch.tables
]
GOLD_ASSETS = {mart: Asset(f"s3://lakehouse/gold/{mart}") for mart in registry.marts}


def _dag():
    from airflow.sdk import dag

    @dag(
        dag_id="build_gold",
        schedule=SILVER_ASSETS,
        catchup=False,
        max_active_runs=1,
        tags=["gold", "mart", "delta", "spark-on-k8s"],
        doc_md="""
## Gold Marts (Spark-on-Kubernetes)

One `SparkKubernetesOperator` per mart, each running `jobs/gold_job.py`:

- `voc_daily` — daily VOC counts, resolution rate, avg resolution time
- `policy_summary` — active policy counts + premium by product/channel
- `claims_analysis` — settlement rates and processing time by product

**Trigger**: silver Asset events from `transform_silver`.
""",
    )
    def build_gold():
        for mart in registry.marts:
            SparkKubernetesOperator(
                task_id=f"gold_{mart}",
                namespace=NAMESPACE,
                application_file=render_gold(mart),
                # In-cluster service-account credentials; no Airflow
                # connection is involved. See bronze_dag.
                kubernetes_conn_id=None,
                in_cluster=True,
                get_logs=True,
                do_xcom_push=False,
                outlets=[GOLD_ASSETS[mart]],
                retries=1,
            )

    return build_gold()


dag = _dag()
