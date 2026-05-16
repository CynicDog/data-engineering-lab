"""Batch layer DAG — minutely tick, upsert-add into batch_metrics.

Drains the previous closed minute from Kafka, lands raw events in Bronze
parquet, aggregates into a Silver Delta table on MinIO, then upserts into
`batch_metrics` in Postgres with `count += new_count` semantics.

Retries are disabled because the upsert is *not* idempotent — re-running the
same minute would double-count that minute's events.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from lambda_pipeline.batch import run_batch


@dag(
    dag_id="batch_layer",
    description="Lambda batch layer — Kafka window -> Bronze -> Silver Delta -> Postgres batch_metrics",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["lambda", "batch"],
    default_args={"retries": 0},
)
def batch_layer():
    @task
    def process(data_interval_end: datetime) -> dict:
        return run_batch(window_end=data_interval_end, window=timedelta(minutes=1))

    process()


dag = batch_layer()
