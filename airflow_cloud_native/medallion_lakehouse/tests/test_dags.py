"""DagBag parse tests — fastest pre-deploy check.

Catches anything that breaks DAG *import*: missing modules, bad Asset URIs,
schedule typos.

Needs Airflow installed → run inside a container:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py
"""
from __future__ import annotations

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
    assert {"ingest_bronze", "silver_models", "gold_models", "data_quality"} <= set(dagbag.dags)


def test_ingest_bronze_has_one_task_per_source_table(dagbag):
    from medallion.ingest import TABLES

    dag = dagbag.dags["ingest_bronze"]
    expected = {f"ingest_{t}" for t in TABLES}
    assert {t.task_id for t in dag.tasks} == expected


def test_silver_has_a_single_build_task(dagbag):
    dag = dagbag.dags["silver_models"]
    assert {t.task_id for t in dag.tasks} == {"build"}


def test_gold_has_a_single_build_task(dagbag):
    dag = dagbag.dags["gold_models"]
    assert {t.task_id for t in dag.tasks} == {"build"}


def test_data_quality_runs_freshness_then_tests(dagbag):
    dag = dagbag.dags["data_quality"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "bronze_freshness" in task_ids
    assert "dbt_test_all" in task_ids
    [freshness] = [t for t in dag.tasks if t.task_id == "bronze_freshness"]
    downstream_ids = {t.task_id for t in freshness.downstream_list}
    assert "dbt_test_all" in downstream_ids
