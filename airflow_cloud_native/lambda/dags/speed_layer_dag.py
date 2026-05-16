"""Speed layer DAG — short, frequent ticks; each tick consumes for ~4 minutes
then commits offsets. Running on a 5-minute schedule keeps the consumer "always
on" without using a long-running service. Group id is fixed so offsets resume
across ticks.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from lambda_pipeline.speed import run_speed


@dag(
    dag_id="speed_layer",
    description="Lambda speed layer — bounded Kafka consumer tick that upserts speed_metrics",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    tags=["lambda", "speed"],
    default_args={"retries": 0},
)
def speed_layer():
    @task
    def tick() -> dict:
        return run_speed(max_seconds=240, group_id="lambda-speed")

    tick()


dag = speed_layer()
