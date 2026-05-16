"""Standalone data-quality sweep — runs `dbt test` plus a `dbt source freshness`
check across every layer. Decoupled from the transform DAGs so DQ can be
scheduled independently (e.g. hourly while transforms run nightly).

Failing tests page the platform team; warning-severity tests just log.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


DBT_PROJECT_PATH = "/opt/airflow/dbt_project"
DBT_BIN = "dbt"


@dag(
    dag_id="data_quality",
    description="dbt source freshness + dbt test across all layers.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=["dq", "dbt"],
    default_args={"retries": 0},
)
def data_quality():
    # Source freshness: did Bronze partitions land within SLA?
    freshness = BashOperator(
        task_id="bronze_freshness",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"{DBT_BIN} source freshness --profiles-dir {DBT_PROJECT_PATH}"
        ),
    )

    # Full test sweep across Silver + Gold. `--store-failures` keeps a copy of
    # rows that violated tests in a DuckDB table for inspection.
    test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            f"cd {DBT_PROJECT_PATH} && "
            f"{DBT_BIN} test --profiles-dir {DBT_PROJECT_PATH} --store-failures"
        ),
    )

    freshness >> test_all


dag = data_quality()
