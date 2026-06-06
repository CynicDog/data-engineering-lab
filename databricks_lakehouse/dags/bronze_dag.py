"""Bronze ingestion DAG — Parquet landing zone → Delta tables.

Mirrors: Databricks Workflow that reads ADF-produced Parquets from ADLS
         and converts them to Delta tables in Unity Catalog bronze schema.

Key patterns demonstrated here vs the current anti-pattern:
    Anti-pattern: Foreach loop in a Databricks notebook that iterates a
                  status.txt file, processes parquets, deletes the file.
                  Problem: daily and hourly schedules share the same folder,
                  causing glob conflicts and silent overwrites.

    This pattern: One Airflow task per source table. Each task is independently
                  retryable, observable, and emits an Airflow Asset on success.
                  The audit log records every run with status + row count.
                  Recovery = query the audit log, not grep the storage.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

TABLES = ["customer", "policy", "claims", "voc"]

BRONZE_ASSETS = {t: Asset(f"s3://lakehouse/bronze/{t}") for t in TABLES}


@dag(
    dag_id="ingest_bronze",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingest", "delta"],
    doc_md="""
## Bronze Ingestion

Reads Parquet files written to the MinIO landing zone (ADLS equivalent)
and converts them to partitioned Delta tables.

**Asset outlets**: each successful task emits an Airflow Asset that
triggers `transform_silver` automatically — no check files, no polling.

**Audit trail**: every run is recorded in `s3://lakehouse/control/ingestion_log`
(a Delta table). Query it to diagnose failures instead of crawling storage.
""",
)
def ingest_bronze():
    for table in TABLES:

        @task(
            task_id=f"ingest_{table}",
            outlets=[BRONZE_ASSETS[table]],
            retries=2,
        )
        def _ingest(table: str = table, data_interval_end: datetime | None = None) -> dict:
            from lakehouse.bronze.ingest import ingest_table_with_audit
            from lakehouse.config.settings import load_settings
            from lakehouse.spark_utils.session import get_spark

            settings = load_settings()
            spark = get_spark(settings, app_name=f"bronze_ingest_{table}")

            dt = (data_interval_end or datetime.now(timezone.utc)).date().isoformat()

            try:
                result = ingest_table_with_audit(
                    spark, settings, table, dt, schedule_type="daily"
                )
                return result
            finally:
                spark.stop()

        _ingest()


ingest_bronze()
