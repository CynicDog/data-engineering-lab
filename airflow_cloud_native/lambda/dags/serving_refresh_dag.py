"""Serving DAG — triggered by either layer; rebuilds the union view that
queries actually hit.

This is the place where Lambda's "two writes, one read" reconciliation
shows up: batch is truth for closed hours, speed fills the open hour.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

from lambda_pipeline.serving import rebuild_serving_view


# Define one asset per upstream layer; the DAG fires whenever either updates.
# Airflow's postgres provider requires `postgres://<host>/<db>/<schema>/<table>`.
BATCH_ASSET = Asset("postgres://postgres/warehouse/public/batch_metrics")
SPEED_ASSET = Asset("postgres://postgres/warehouse/public/speed_metrics")


@dag(
    dag_id="serving_refresh",
    description="Rebuild the unified serving_metrics view (batch ∪ speed[h > batch_hw])",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    # In Airflow 3 you can schedule on asset updates instead of cron.
    # If you want a clock-based fallback as well, pair this with a cron entry.
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["lambda", "serving"],
)
def serving_refresh():
    @task(outlets=[Asset("postgres://postgres/warehouse/public/serving_metrics")])
    def rebuild() -> None:
        rebuild_serving_view()

    rebuild()


dag = serving_refresh()
