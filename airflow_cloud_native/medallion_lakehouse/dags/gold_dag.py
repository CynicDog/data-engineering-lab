"""Gold — runs `dbt build` for all models tagged `gold`.

Fires on the Silver asset, so Gold always runs against the latest Silver
that passed its tests.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.sdk.definitions.asset import Asset


DBT_PROJECT_PATH = "/opt/airflow/dbt_project"

SILVER_ASSET = Asset("s3://lakehouse/silver")
GOLD_ASSET = Asset("s3://lakehouse/gold")


@dag(
    dag_id="gold_models",
    description="dbt build (run + test) on Gold layer marts.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=[SILVER_ASSET],
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def gold_models():
    BashOperator(
        task_id="build",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"dbt deps --profiles-dir {DBT_PROJECT_PATH} && "
            # See silver_dag.py for the rationale.
            f"dbt build --select tag:gold --profiles-dir {DBT_PROJECT_PATH} --no-partial-parse"
        ),
        outlets=[GOLD_ASSET],
    )


dag = gold_models()
