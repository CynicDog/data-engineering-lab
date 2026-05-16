"""DagBag parse tests — fastest pre-deploy check.

Catches anything that breaks DAG *import*: missing modules, bad Asset URIs,
schedule typos, decorator misuse.

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
    assert "batch_layer" in dagbag.dags
    assert "speed_layer" in dagbag.dags
    assert "serving_refresh" in dagbag.dags


def test_batch_layer_runs_every_minute(dagbag):
    dag = dagbag.dags["batch_layer"]
    assert dag.schedule == timedelta(minutes=1)
    assert dag.max_active_runs == 1
    assert {t.task_id for t in dag.tasks} == {"process"}


def test_speed_layer_runs_every_5_minutes(dagbag):
    dag = dagbag.dags["speed_layer"]
    assert dag.schedule == timedelta(minutes=5)
    assert dag.max_active_runs == 1
    assert {t.task_id for t in dag.tasks} == {"tick"}


def test_serving_refresh_structure(dagbag):
    dag = dagbag.dags["serving_refresh"]
    assert dag.schedule == timedelta(minutes=5)
    assert {t.task_id for t in dag.tasks} == {"rebuild"}
