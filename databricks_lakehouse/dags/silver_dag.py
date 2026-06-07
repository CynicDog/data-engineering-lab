"""Silver transform DAG — triggered by Bronze Asset events.

Mirrors: Databricks Workflow with a dependency on the bronze pipeline,
         previously coordinated via a 'check file' written to ADLS.

Key patterns demonstrated:
    Anti-pattern: Bronze notebook writes a check file to
                  s3://lakehouse/control/bronze_done_{table}.flag
                  Silver scans for all four flag files, proceeds only when
                  all four exist. Files are never cleaned up → state rot.

    This pattern: Airflow Asset graph. Bronze tasks declare `outlets`, Silver
                  declares `schedule=list(BRONZE_ASSETS.values())`. Airflow
                  tracks the dependency graph natively — no flag files,
                  no polling loops, no state rot.

    The trigger semantics here are OR: any one Bronze Asset event starts
    Silver. To require ALL tables first, use AssetAlias or a sensor.
    The right choice depends on your SLA — document it here.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

from lakehouse.config.registry import load_registry

registry = load_registry()

BRONZE_ASSETS = [
    Asset(f"s3://lakehouse/bronze/{ch.name}/{t.name}")
    for ch in registry.channels
    for t in ch.tables
]
SILVER_ASSET = Asset("s3://lakehouse/silver")


@dag(
    dag_id="transform_silver",
    schedule=BRONZE_ASSETS,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["silver", "transform", "delta"],
    doc_md="""
## Silver Transforms

Reads each Bronze Delta table, applies dedup + type casting + PII masking,
and writes clean Silver Delta tables — one per (channel, table) pair.

**Trigger**: Airflow Asset events from `ingest_bronze`. No flag files.

**Silver contract**: Output tables are deduplicated, typed, and PII-safe.
No business logic is applied here — that belongs in Gold.

**Config-driven**: all transform logic lives in `config/table_registry.yaml`.
""",
)
def transform_silver():
    @task(outlets=[SILVER_ASSET], retries=1)
    def transform_all() -> dict:
        from lakehouse.config.registry import load_registry
        from lakehouse.config.settings import load_settings
        from lakehouse.silver.transform import transform_table
        from lakehouse.spark_utils.session import get_spark

        settings = load_settings()
        registry = load_registry()
        spark = get_spark(settings, app_name="silver_transform")

        results = {}
        for spec in registry.all_tables():
            rows = transform_table(spark, settings, spec)
            results[f"{spec.channel}.{spec.name}"] = rows
        return results

    transform_all()


transform_silver()
