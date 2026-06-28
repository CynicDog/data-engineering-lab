"""Gold mart DAGs — one DAG per mart, each scheduled on its exact silver dependencies.

This replaces the original single build_gold DAG that depended on ALL silver assets
(OR semantics: any silver update triggered all three marts). The problem: if only
chan1/policy silver updates, voc_daily would run against stale chan2/voc silver.

The fix: split into three DAGs, each with AssetAll(...) on the specific silver tables
that mart actually reads. AssetAll provides AND semantics — the mart only runs when
ALL of its upstream silver tables have been refreshed.

    transform_silver [chan2/voc, chan1/customer] → build_gold_voc_daily
    transform_silver [chan1/policy, chan1/customer] → build_gold_policy_summary
    transform_silver [chan1/claims, chan1/policy] → build_gold_claims_analysis

Industry pattern: Airbnb, Netflix, and Shopify all map each consumer job to exactly
the upstream assets it reads — never to ALL upstream assets as a blanket dependency.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk.definitions.asset import Asset, AssetAll

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from _spark_app import NAMESPACE, render_gold

# Explicit per-mart silver dependency map.
# Each key is a mart name; the value is the complete list of silver tables
# that mart's Spark job reads. AssetAll ensures the mart only runs when
# EVERY one of its silver inputs has been refreshed in the current cycle.
MART_SILVER_DEPS: dict[str, list[Asset]] = {
    "voc_daily": [
        Asset("s3://lakehouse/silver/chan2/voc"),
        Asset("s3://lakehouse/silver/chan1/customer"),
    ],
    "policy_summary": [
        Asset("s3://lakehouse/silver/chan1/policy"),
        Asset("s3://lakehouse/silver/chan1/customer"),
    ],
    "claims_analysis": [
        Asset("s3://lakehouse/silver/chan1/claims"),
        Asset("s3://lakehouse/silver/chan1/policy"),
    ],
}

GOLD_ASSETS = {mart: Asset(f"s3://lakehouse/gold/{mart}") for mart in MART_SILVER_DEPS}


def _make_gold_dag(mart: str, silver_deps: list[Asset]):
    from airflow.sdk import dag

    @dag(
        dag_id=f"build_gold_{mart}",
        # AssetAll: the DAG only triggers when ALL listed silver assets have
        # been updated — not just any one of them. This prevents a mart from
        # running against a mix of fresh and stale silver tables.
        schedule=AssetAll(*silver_deps),
        catchup=False,
        max_active_runs=1,
        tags=["gold", "mart", "delta", "spark-on-k8s"],
        doc_md=f"""
## Gold Mart: {mart} (Spark-on-Kubernetes)

Triggered only when ALL required silver tables are refreshed:
{chr(10).join(f"- `{a.name}`" for a in silver_deps)}

Runs `jobs/gold_job.py --mart {mart}`.
""",
    )
    def _dag_fn():
        SparkKubernetesOperator(
            task_id=f"gold_{mart}",
            namespace=NAMESPACE,
            application_file=render_gold(mart, profile="small"),
            kubernetes_conn_id=None,
            in_cluster=True,
            get_logs=True,
            do_xcom_push=False,
            outlets=[GOLD_ASSETS[mart]],
            retries=1,
            execution_timeout=timedelta(minutes=30),
        )

    return _dag_fn()


# Expose each per-mart DAG as a module-level name so Airflow's DAG bag
# discovers all three. Using globals() is the standard pattern for
# programmatically creating multiple DAGs in one file.
for _mart, _deps in MART_SILVER_DEPS.items():
    globals()[f"dag_gold_{_mart}"] = _make_gold_dag(_mart, _deps)
