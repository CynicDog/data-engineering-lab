"""Bronze ingest — reads source files from /opt/airflow/source_data, lands
them as Hive-partitioned Parquet under s3://lakehouse/bronze/{table}/dt=...

Idempotent over (table, dt): re-running for the same date rewrites that
partition. That's what makes backfills boring instead of scary.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

from medallion.ingest import TABLES, ingest_table


BRONZE_ASSETS = {t: Asset(f"s3://lakehouse/bronze/{t}") for t in TABLES}


@dag(
    dag_id="ingest_bronze",
    description="Land source files as Hive-partitioned Bronze Parquet.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingest"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def ingest_bronze():
    # One task per table — they're independent, so the scheduler can run them
    # in parallel and a single bad file doesn't block the others.
    for table in TABLES:
        @task(task_id=f"ingest_{table}", outlets=[BRONZE_ASSETS[table]])
        def _ingest(table: str = table, data_interval_end: datetime = None) -> dict:
            dt = data_interval_end.date().isoformat()
            return ingest_table(table, dt=dt)

        _ingest()


dag = ingest_bronze()
