"""Pure-Python unit tests on the Bronze ingest surface.

Silver and Gold transformations live in dbt SQL and are tested by
`dbt test` (run via the data_quality DAG and the silver/gold DAGs themselves
via Cosmos' AFTER_EACH test behavior). This file covers the Python-side
ingest helpers only.

Run locally with `uv run pytest tests/test_pipeline_unit.py`.
"""
from __future__ import annotations

import pytest

from medallion.ingest import TABLES, ingest_table


def test_tables_list_is_stable():
    # Bronze sources contract — any change here must be matched by a dbt
    # source declaration in dbt_project/models/bronze/sources.yml.
    assert TABLES == ["orders", "customers", "products", "web_events"]


def test_bronze_path_is_hive_partitioned(fake_settings):
    path = fake_settings.bronze_path("orders", "2026-05-16")
    assert path == "s3://test-bucket/bronze/orders/dt=2026-05-16/part.parquet"


def test_bronze_path_changes_per_partition(fake_settings):
    a = fake_settings.bronze_path("orders", "2026-05-16")
    b = fake_settings.bronze_path("orders", "2026-05-17")
    assert a != b
    assert a.endswith("dt=2026-05-16/part.parquet")
    assert b.endswith("dt=2026-05-17/part.parquet")


def test_storage_options_targets_minio(fake_settings):
    opts = fake_settings.storage_options
    assert opts["AWS_ENDPOINT_URL"].startswith("http://")
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert "AWS_ACCESS_KEY_ID" in opts


def test_ingest_missing_source_is_a_warning_not_a_crash(fake_settings, monkeypatch):
    # `ingest_table` calls `load_settings()` internally, which reads the live
    # process environment. Patch it to return our fixture instead.
    import medallion.ingest as ingest_mod

    monkeypatch.setattr(ingest_mod, "load_settings", lambda: fake_settings)

    # source_dir is a tmp dir with no orders.csv inside — ingest must gracefully
    # return zero rows rather than blow up, so a missing upstream file doesn't
    # take down the whole DAG.
    result = ingest_table("orders", dt="2026-05-16")
    assert result == {"rows": 0}
