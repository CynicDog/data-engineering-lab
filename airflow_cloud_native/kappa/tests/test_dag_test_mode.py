"""End-to-end DAG run via `dag.test()` with the heavy IO stubbed.

Exercises DAG plumbing — task wiring, params, retries, outlet emission — in
one in-process call. No scheduler, no celery, no minute-tick wait. The
pipeline body itself is covered by test_pipeline_unit.py.

Needs Airflow installed → run inside a container:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag_test_mode.py
"""
from __future__ import annotations

import pytest

pytest.importorskip("airflow")


def test_stream_pipeline_runs_end_to_end(monkeypatch):
    import kappa_pipeline.stream as stream_mod

    calls: list[dict] = []

    def fake_tick(max_seconds: int = 50, group_id: str = "kappa-stream") -> dict:
        calls.append({"max_seconds": max_seconds, "group_id": group_id})
        return {"raw_rows": 0, "agg_rows": 0}

    monkeypatch.setattr(stream_mod, "run_stream_tick", fake_tick)

    import stream_pipeline_dag
    monkeypatch.setattr(stream_pipeline_dag, "run_stream_tick", fake_tick)

    stream_pipeline_dag.dag.test()

    assert calls == [{"max_seconds": 50, "group_id": "kappa-stream"}]


def test_replay_pipeline_runs_end_to_end(monkeypatch):
    import kappa_pipeline.replay as replay_mod

    captured: dict = {}

    def fake_replay(since, drain_seconds: int = 120) -> dict:
        captured["since"] = since
        captured["drain_seconds"] = drain_seconds
        return {"version": "replay-test", "raw_rows": 0, "agg_rows": 0, "path": "s3://x"}

    monkeypatch.setattr(replay_mod, "run_replay", fake_replay)

    import replay_dag
    monkeypatch.setattr(replay_dag, "run_replay", fake_replay)

    replay_dag.dag.test(
        run_conf={"since": "2026-01-01T00:00:00Z", "drain_seconds": 30},
    )

    assert captured["drain_seconds"] == 30
    assert captured["since"].isoformat() == "2026-01-01T00:00:00+00:00"
