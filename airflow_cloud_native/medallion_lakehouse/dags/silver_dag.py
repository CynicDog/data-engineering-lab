"""Silver — runs `dbt build` for all models tagged `silver`.

`dbt build` runs each model AND its tests in dependency order, so a failed
test fails this task. That's the contract: broken upstream data fails the
DAG before reaching Gold.

Scheduled on Bronze asset updates — fires whenever the ingest_bronze DAG
publishes a new partition for any source table.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.sdk.definitions.asset import Asset

from medallion.ingest import TABLES


DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

BRONZE_ASSETS = [Asset(f"s3://lakehouse/bronze/{t}") for t in TABLES]
SILVER_ASSET = Asset("s3://lakehouse/silver")


@dag(
    dag_id="silver_models",
    description="dbt build (run + test) on Silver layer models.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=BRONZE_ASSETS,
    catchup=False,
    max_active_runs=1,
    tags=["silver", "dbt"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def silver_models():
    BashOperator(
        task_id="build",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt deps --profiles-dir {DBT_PROJECT_PATH} && "
            # --no-partial-parse: don't reuse target/partial_parse.msgpack from a
            # previous run; config-file edits otherwise get masked by stale cache.
            f"dbt build --select tag:silver --profiles-dir {DBT_PROJECT_PATH} --no-partial-parse"
        ),
        outlets=[SILVER_ASSET],
    )


dag = silver_models()
