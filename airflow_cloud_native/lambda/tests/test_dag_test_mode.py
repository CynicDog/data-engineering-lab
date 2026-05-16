"""End-to-end DAG runs via `dag.test()` with the heavy IO stubbed.

Exercises DAG plumbing — task wiring, params, outlet emission — in one
in-process call. No scheduler, no celery, no waiting for the next tick.

Needs Airflow installed → run inside a container:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag_test_mode.py
"""
from __future__ import annotations

import pytest

pytest.importorskip("airflow")


def test_batch_layer_runs_end_to_end(monkeypatch):
    import lambda_pipeline.batch as batch_mod
    from datetime import timedelta

    calls: list[dict] = []

    def fake_run_batch(window_end=None, window: timedelta = timedelta(minutes=1)) -> dict:
        calls.append({"window_end": window_end, "window": window})
        return {"raw_rows": 0, "agg_rows": 0}

    monkeypatch.setattr(batch_mod, "run_batch", fake_run_batch)

    import batch_layer_dag
    monkeypatch.setattr(batch_layer_dag, "run_batch", fake_run_batch)

    batch_layer_dag.dag.test()

    assert len(calls) == 1
    assert calls[0]["window"] == timedelta(minutes=1)


def test_speed_layer_runs_end_to_end(monkeypatch):
    import lambda_pipeline.speed as speed_mod

    calls: list[dict] = []

    def fake_run_speed(max_seconds: int = 300, group_id: str = "lambda-speed") -> dict:
        calls.append({"max_seconds": max_seconds, "group_id": group_id})
        return {"processed": 0}

    monkeypatch.setattr(speed_mod, "run_speed", fake_run_speed)

    import speed_layer_dag
    monkeypatch.setattr(speed_layer_dag, "run_speed", fake_run_speed)

    speed_layer_dag.dag.test()

    assert calls == [{"max_seconds": 240, "group_id": "lambda-speed"}]


def test_serving_refresh_runs_end_to_end(monkeypatch):
    import lambda_pipeline.serving as serving_mod

    calls: list[int] = []

    def fake_rebuild() -> None:
        calls.append(1)

    monkeypatch.setattr(serving_mod, "rebuild_serving_view", fake_rebuild)

    import serving_refresh_dag
    monkeypatch.setattr(serving_refresh_dag, "rebuild_serving_view", fake_rebuild)

    serving_refresh_dag.dag.test()

    assert calls == [1]
