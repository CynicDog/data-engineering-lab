"""Pure-Python unit tests on the pipeline core — no Airflow, no Kafka, no S3.

Run locally with `uv run pytest tests/test_pipeline_unit.py`.
"""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from kappa_pipeline.stream import _to_rollup


def _evt(user: str, action: str, ts: datetime) -> dict:
    return {"event_id": "x", "user_id": user, "action": action, "_kafka_ts": ts}


def test_to_rollup_counts_events_in_same_hour():
    t = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            _evt("u1", "click", t),
            _evt("u1", "click", t.replace(minute=30)),
            _evt("u1", "click", t.replace(minute=59)),
        ]
    )
    out = _to_rollup(df)
    assert out.height == 1
    [row] = out.to_dicts()
    assert row["event_count"] == 3
    assert row["event_hour"] == t


def test_to_rollup_splits_across_hour_boundary():
    t = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            _evt("u1", "click", t),
            _evt("u1", "click", t.replace(hour=11)),
        ]
    )
    out = _to_rollup(df).sort("event_hour")
    assert out.height == 2
    assert out["event_count"].to_list() == [1, 1]


def test_to_rollup_groups_by_user_and_action():
    t = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            _evt("u1", "click", t),
            _evt("u2", "click", t),
            _evt("u1", "view", t),
        ]
    )
    out = _to_rollup(df)
    assert out.height == 3
    assert set(out["event_count"].to_list()) == {1}


def test_silver_uri_default_is_current(fake_settings):
    assert fake_settings.silver_uri() == "s3://test-bucket/silver/events_rollup/current"


def test_silver_uri_versioned(fake_settings):
    assert (
        fake_settings.silver_uri(version="replay-x")
        == "s3://test-bucket/silver/events_rollup/replay-x"
    )


def test_storage_options_opts_out_of_lock_client(fake_settings):
    # This is the flag that unblocked Delta writes against MinIO — protect it.
    assert fake_settings.storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
