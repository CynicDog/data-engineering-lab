"""Replay: rebuild a brand-new Silver table from a chosen point in the log,
then atomically swap the `current` pointer once the rebuild is complete.

This is the Kappa equivalent of a "backfill" — there is no separate batch job,
just the same `_drain + _to_rollup` code re-run with an earlier starting offset.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import polars as pl
import psycopg
from confluent_kafka import Consumer, TopicPartition
from deltalake import write_deltalake

from .common import load_settings
from .stream import _to_rollup

log = logging.getLogger(__name__)


def _drain_from(timestamp: datetime, max_seconds: int, group_id: str) -> pl.DataFrame:
    s = load_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    metadata = consumer.list_topics(s.topic, timeout=10)
    parts = [
        TopicPartition(s.topic, p, int(timestamp.timestamp() * 1000))
        for p in metadata.topics[s.topic].partitions
    ]
    consumer.assign(consumer.offsets_for_times(parts, timeout=10))

    import json

    rows: list[dict] = []
    stop_at = datetime.now(tz=timezone.utc).timestamp() + max_seconds
    while datetime.now(tz=timezone.utc).timestamp() < stop_at:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            break
        if msg.error():
            log.warning("kafka error: %s", msg.error())
            continue
        payload = json.loads(msg.value())
        _, ts_ms = msg.timestamp()
        payload["_kafka_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        rows.append(payload)
    consumer.close()
    if not rows:
        return pl.DataFrame(
            schema={
                "event_id": pl.Utf8,
                "user_id": pl.Utf8,
                "action": pl.Utf8,
                "_kafka_ts": pl.Datetime("us", "UTC"),
            }
        )
    return pl.DataFrame(rows)


def run_replay(since: datetime, drain_seconds: int = 120) -> dict[str, str | int]:
    """Build a versioned Silver table from `since`, write it under a uuid,
    and atomically point `current` at the new path via a Postgres metadata row."""
    s = load_settings()
    run_id = uuid.uuid4().hex[:8]
    version = f"replay-{since:%Y%m%dT%H%M%S}-{run_id}"

    df = _drain_from(since, max_seconds=drain_seconds, group_id=f"kappa-replay-{run_id}")
    if df.is_empty():
        log.warning("nothing to replay from %s", since)
        return {"version": version, "raw_rows": 0, "agg_rows": 0}

    agg = _to_rollup(df)
    target = s.silver_uri(version=version)
    write_deltalake(target, agg.to_arrow(), mode="overwrite", storage_options=s.storage_options)

    # Atomic pointer swap: keep a tiny registry in Postgres that records which
    # Silver path is canonical. Consumers read this row, then read the path.
    with psycopg.connect(s.warehouse_dsn, autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_registry (
                table_name TEXT PRIMARY KEY,
                version TEXT NOT NULL,
                path TEXT NOT NULL,
                promoted_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            INSERT INTO silver_registry (table_name, version, path)
            VALUES ('events_rollup', %s, %s)
            ON CONFLICT (table_name) DO UPDATE
              SET version = EXCLUDED.version,
                  path = EXCLUDED.path,
                  promoted_at = now()
            """,
            (version, target),
        )

    return {"version": version, "raw_rows": df.height, "agg_rows": agg.height, "path": target}
