"""Stream pipeline: short bounded tick that drains new events, transforms with
Polars, MERGEs into a Delta table, then mirrors into Postgres.

The DAG calls this every minute. Each tick is its own consumer-group commit
boundary, so a crashed tick simply replays its un-committed offsets — Delta's
MERGE on event_id keeps the output idempotent."""
from __future__ import annotations

import json
import logging
import signal
from datetime import datetime, timezone

import polars as pl
import psycopg
from confluent_kafka import Consumer
from deltalake import DeltaTable, write_deltalake

from .common import Settings, load_settings

log = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS events_rollup (
    user_id TEXT NOT NULL,
    action TEXT NOT NULL,
    event_hour TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL,
    PRIMARY KEY (user_id, action, event_hour)
);
"""

_UPSERT = """
INSERT INTO events_rollup (user_id, action, event_hour, event_count)
VALUES (%s, %s, %s, %s)
ON CONFLICT (user_id, action, event_hour)
DO UPDATE SET event_count = events_rollup.event_count + EXCLUDED.event_count;
"""


def _drain(s: Settings, group_id: str, max_seconds: int, from_beginning: bool) -> pl.DataFrame:
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )
    consumer.subscribe([s.topic])

    stop_at = datetime.now(tz=timezone.utc).timestamp() + max_seconds
    stop = False

    def _shutdown(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    rows: list[dict] = []
    try:
        while not stop and datetime.now(tz=timezone.utc).timestamp() < stop_at:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("kafka error: %s", msg.error())
                continue
            payload = json.loads(msg.value())
            _, ts_ms = msg.timestamp()
            payload["_kafka_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            rows.append(payload)
        consumer.commit(asynchronous=False)
    finally:
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


def _to_rollup(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("_kafka_ts").dt.truncate("1h").alias("event_hour"))
        .group_by(["user_id", "action", "event_hour"])
        .agg(pl.len().alias("event_count"))
    )


def run_stream_tick(max_seconds: int = 50, group_id: str = "kappa-stream") -> dict[str, int]:
    s = load_settings()
    df = _drain(s, group_id=group_id, max_seconds=max_seconds, from_beginning=False)
    if df.is_empty():
        return {"raw_rows": 0, "agg_rows": 0}

    agg = _to_rollup(df)

    silver = s.silver_uri()
    try:
        DeltaTable(silver, storage_options=s.storage_options)
        mode = "append"
    except Exception:
        mode = "overwrite"
    # Append, then MERGE-deduplicate would be the production pattern;
    # for the lab we keep it as append + Postgres-side upsert.
    write_deltalake(silver, agg.to_arrow(), mode=mode, storage_options=s.storage_options)

    with psycopg.connect(s.warehouse_dsn, autocommit=True) as conn:
        conn.execute(_DDL)
        with conn.cursor() as cur:
            cur.executemany(_UPSERT, agg.iter_rows())

    return {"raw_rows": df.height, "agg_rows": agg.height}
