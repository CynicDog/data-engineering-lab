"""DagBag parse tests — fastest pre-deploy check.

Catches anything that breaks DAG *import*: missing modules, bad Asset URIs,
schedule typos, decorator misuse. Today's `postgres://warehouse/events_rollup`
bug would have surfaced here as an import error before the file ever reached
the scheduler.

Needs Airflow installed → run inside a container:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models.dagbag import DagBag  # noqa: E402

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent / "dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_present(dagbag):
    assert "stream_pipeline" in dagbag.dags
    assert "replay_pipeline" in dagbag.dags


def test_stream_pipeline_is_minute_scheduled_singleton(dagbag):
    dag = dagbag.dags["stream_pipeline"]
    assert dag.schedule == timedelta(minutes=1)
    assert dag.max_active_runs == 1
    assert {t.task_id for t in dag.tasks} == {"tick"}


def test_replay_pipeline_is_manual_only(dagbag):
    dag = dagbag.dags["replay_pipeline"]
    assert dag.schedule is None
    assert {t.task_id for t in dag.tasks} == {"replay"}
