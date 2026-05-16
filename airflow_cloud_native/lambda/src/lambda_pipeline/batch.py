"""Batch layer: drain a window of Kafka events into Bronze parquet,
aggregate into a Silver Delta table, then load into Postgres `batch_metrics`."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import polars as pl
import psycopg
from confluent_kafka import Consumer, TopicPartition
from deltalake import DeltaTable, write_deltalake

from .common import Settings, load_settings

log = logging.getLogger(__name__)


def _to_rollup(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("_kafka_ts").dt.truncate("1h").alias("event_hour"))
        .group_by(["user_id", "action", "event_hour"])
        .agg(pl.len().alias("event_count"))
    )


def _consume_window(
    settings: Settings,
    window_start: datetime,
    window_end: datetime,
    max_seconds: int = 120,
) -> pl.DataFrame:
    """Read a closed [window_start, window_end) slice of the topic by timestamp seek.

    Bounded by `max_seconds` of wall-clock time so a topic that keeps receiving
    events past `window_end` (e.g. a still-running producer) doesn't trap the
    consumer in an infinite skip loop.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"lambda-batch-{window_start.isoformat()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(settings.topic, timeout=10)
        partitions = [
            TopicPartition(settings.topic, p, int(window_start.timestamp() * 1000))
            for p in metadata.topics[settings.topic].partitions
        ]
        offsets = consumer.offsets_for_times(partitions, timeout=10)
        consumer.assign(offsets)

        end_ms = int(window_end.timestamp() * 1000)
        deadline = datetime.now(tz=timezone.utc).timestamp() + max_seconds
        rows: list[dict] = []
        while datetime.now(tz=timezone.utc).timestamp() < deadline:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                log.warning("kafka error: %s", msg.error())
                continue
            ts_type, ts_ms = msg.timestamp()
            if ts_ms >= end_ms:
                # Past the window, but later partitions may still have in-window
                # events — keep skipping until poll() returns None or we hit the
                # wall-clock deadline.
                continue
            payload = json.loads(msg.value())
            payload["_kafka_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            rows.append(payload)
    finally:
        consumer.close()

    if not rows:
        return pl.DataFrame(schema={"event_id": pl.Utf8, "user_id": pl.Utf8, "action": pl.Utf8, "_kafka_ts": pl.Datetime("us", "UTC")})
    return pl.DataFrame(rows)


def run_batch(
    window_end: datetime | None = None,
    window: timedelta = timedelta(minutes=1),
) -> dict[str, int]:
    """Run one batch tick. Returns counts for logging.

    `window_end` defaults to the previous closed minute boundary so we operate
    on a closed past window and never race with in-flight events.
    """
    s = load_settings()
    if window_end is None:
        window_end = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    window_start = window_end - window

    log.info("batch window: %s -> %s", window_start, window_end)

    df = _consume_window(s, window_start, window_end)
    if df.is_empty():
        log.info("no events in window")
        return {"raw_rows": 0, "agg_rows": 0}

    # Bronze: one parquet per minute under date/hour partition
    bronze_path = (
        f"{s.bronze_uri}"
        f"/dt={window_start.date()}"
        f"/hour={window_start.hour:02d}"
        f"/minute={window_start.minute:02d}.parquet"
    )
    df.write_parquet(bronze_path, storage_options=s.storage_options)

    # Silver: aggregate counts per (user, action, hour) — hour granularity is
    # preserved across minutely runs by the upsert-add below.
    agg = _to_rollup(df)

    try:
        DeltaTable(s.silver_uri, storage_options=s.storage_options)
        mode = "append"
    except Exception:
        mode = "overwrite"
    write_deltalake(
        s.silver_uri,
        agg.to_arrow(),
        mode=mode,
        storage_options=s.storage_options,
    )

    with psycopg.connect(s.warehouse_dsn, autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS batch_metrics (
                user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                event_hour TIMESTAMPTZ NOT NULL,
                event_count BIGINT NOT NULL,
                PRIMARY KEY (user_id, action, event_hour)
            )
            """
        )
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO batch_metrics (user_id, action, event_hour, event_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, action, event_hour) DO UPDATE
                  SET event_count = batch_metrics.event_count + EXCLUDED.event_count
                """,
                agg.iter_rows(),
            )

    return {"raw_rows": df.height, "agg_rows": agg.height}
