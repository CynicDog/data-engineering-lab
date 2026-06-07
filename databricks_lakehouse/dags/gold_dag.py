"""Gold mart DAG — triggered by Silver Asset events.

Mirrors: Databricks Workflow that runs mart queries after silver completes,
         previously triggered by a 'gold_trigger.flag' artifact file.

This demonstrates the complete Bronze → Silver → Gold Asset chain:
    ingest_bronze  --[BRONZE_ASSETS]--> transform_silver --[SILVER_ASSET]--> build_gold

No files are generated as signals. The DAG dependency graph in Airflow is
the single source of truth for pipeline ordering.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

from lakehouse.config.registry import load_registry

registry = load_registry()

SILVER_ASSET = Asset("s3://lakehouse/silver")
GOLD_ASSET = Asset("s3://lakehouse/gold")


@dag(
    dag_id="build_gold",
    schedule=[SILVER_ASSET],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["gold", "mart", "delta"],
    doc_md="""
## Gold Marts

Builds denormalized, business-ready mart tables from Silver.

**Trigger**: Airflow Asset event from `transform_silver`.

**Available marts** (from `config/table_registry.yaml`):
- `voc_daily` — daily VOC counts, resolution rate, avg resolution time
- `policy_summary` — active policy counts + premium by product/channel
- `claims_analysis` — settlement rates and processing time by product

These marts are the equivalent of Databricks Gold notebooks that were
full of custom SQL and ad-hoc joins — now testable, versionable Python.
""",
)
def build_gold():
    @task(outlets=[GOLD_ASSET], retries=1)
    def build_all() -> dict:
        from lakehouse.config.registry import load_registry
        from lakehouse.config.settings import load_settings
        from lakehouse.gold.mart import build_mart
        from lakehouse.spark_utils.session import get_spark

        settings = load_settings()
        registry = load_registry()
        spark = get_spark(settings, app_name="gold_build")

        results = {}
        for mart in registry.marts:
            rows = build_mart(spark, settings, mart)
            results[mart] = rows
        return results

    build_all()


build_gold()
