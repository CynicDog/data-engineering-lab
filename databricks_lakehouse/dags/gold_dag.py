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

SILVER_ASSET = Asset("s3://lakehouse/silver")
GOLD_ASSET = Asset("s3://lakehouse/gold")

MARTS = ["voc_daily", "policy_summary", "claims_analysis"]


@dag(
    dag_id="build_gold",
    schedule=[SILVER_ASSET],
    catchup=False,
    max_active_runs=1,
    tags=["gold", "mart", "delta"],
    doc_md="""
## Gold Marts

Builds denormalized, business-ready mart tables from Silver.

**Trigger**: Airflow Asset event from `transform_silver`.

**Available marts**:
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
        from lakehouse.config.settings import load_settings
        from lakehouse.gold.mart import MARTS, build_mart
        from lakehouse.spark_utils.session import get_spark

        settings = load_settings()
        spark = get_spark(settings, app_name="gold_build")

        results = {}
        try:
            for mart in MARTS:
                rows = build_mart(spark, settings, mart)
                results[mart] = rows
        finally:
            spark.stop()

        return results

    build_all()


build_gold()
