"""Stream pipeline DAG — bounded ~50s tick, scheduled every minute.

This is the *one* pipeline in Kappa. There is no batch counterpart; if you
need older data, see `replay_pipeline`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

from kappa_pipeline.stream import run_stream_tick


EVENTS_ROLLUP = Asset("postgres://postgres/warehouse/public/events_rollup")


@dag(
    dag_id="stream_pipeline",
    description="Kappa stream pipeline — Kafka -> Polars -> Delta -> Postgres events_rollup",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["kappa", "stream"],
    default_args={"retries": 0},
)
def stream_pipeline():
    @task(outlets=[EVENTS_ROLLUP])
    def tick() -> dict:
        return run_stream_tick(max_seconds=50, group_id="kappa-stream")

    tick()


dag = stream_pipeline()
