"""Replay DAG — manually triggered with config:

    {"since": "2026-01-01T00:00:00Z", "drain_seconds": 120}

Rewinds offsets to `since`, rebuilds the Silver table at a new versioned path,
and atomically promotes it via `silver_registry`.

This is *the* knob in Kappa for backfills, late-arriving data fixes, or schema
changes: you don't write a separate job; you replay the same code over older
events.
"""
from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset

from kappa_pipeline.replay import run_replay


SILVER_REGISTRY = Asset("postgres://postgres/warehouse/public/silver_registry")


@dag(
    dag_id="replay_pipeline",
    description="Kappa replay — rebuild events_rollup from a chosen log timestamp",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["kappa", "replay"],
    params={
        "since": "2026-01-01T00:00:00Z",
        "drain_seconds": 120,
    },
)
def replay_pipeline():
    @task(outlets=[SILVER_REGISTRY])
    def replay(params: dict) -> dict:
        since = datetime.fromisoformat(params["since"].replace("Z", "+00:00"))
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        return run_replay(since=since, drain_seconds=int(params["drain_seconds"]))

    replay()


dag = replay_pipeline()
