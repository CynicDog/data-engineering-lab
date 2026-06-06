"""DAG parse tests — run inside the airflow-scheduler container.

These verify that all DAGs load without import errors and have the
expected structure. They do not execute any tasks.

Run with:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dags.py -v
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_present(dagbag):
    expected = {"ingest_bronze", "transform_silver", "build_gold"}
    assert expected.issubset(set(dagbag.dag_ids)), (
        f"Missing DAGs: {expected - set(dagbag.dag_ids)}"
    )


class TestIngestBronze:
    def test_has_one_task_per_channel_table(self, dagbag):
        dag = dagbag.get_dag("ingest_bronze")
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "ingest_chan1_customer",
            "ingest_chan1_policy",
            "ingest_chan1_claims",
            "ingest_chan2_voc",
        }

    def test_schedule_is_daily(self, dagbag):
        dag = dagbag.get_dag("ingest_bronze")
        assert dag.schedule_interval == "@daily"


class TestTransformSilver:
    def test_has_transform_all_task(self, dagbag):
        dag = dagbag.get_dag("transform_silver")
        task_ids = {t.task_id for t in dag.tasks}
        assert "transform_all" in task_ids

    def test_triggered_by_assets(self, dagbag):
        dag = dagbag.get_dag("transform_silver")
        assert dag.schedule_interval is not None


class TestBuildGold:
    def test_has_build_all_task(self, dagbag):
        dag = dagbag.get_dag("build_gold")
        task_ids = {t.task_id for t in dag.tasks}
        assert "build_all" in task_ids
