"""Speed layer: a long-running Kafka consumer that maintains an
approximate rolling rollup in Postgres `speed_metrics`. The serving view
trusts batch for hours that have closed and falls back to speed for the
current (open) hour."""
from __future__ import annotations

import json
import logging
import signal
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer

from .common import load_settings

log = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS speed_metrics (
    user_id TEXT NOT NULL,
    action TEXT NOT NULL,
    event_hour TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, action, event_hour)
);
"""

_UPSERT = """
INSERT INTO speed_metrics (user_id, action, event_hour, event_count, updated_at)
VALUES (%s, %s, %s, 1, now())
ON CONFLICT (user_id, action, event_hour)
DO UPDATE SET event_count = speed_metrics.event_count + 1, updated_at = now();
"""


def run_speed(max_seconds: int = 300, group_id: str = "lambda-speed") -> dict[str, int]:
    """Consume events for up to `max_seconds` and upsert per-hour counts.

    This is intended to be triggered by Airflow on a short schedule. Each run is
    a bounded slice — the next run picks up at the committed offset. State lives
    in Postgres so the consumer itself is stateless.
    """
    s = load_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
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

    processed = 0
    with psycopg.connect(s.warehouse_dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()

        while not stop and datetime.now(tz=timezone.utc).timestamp() < stop_at:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("kafka error: %s", msg.error())
                continue

            payload = json.loads(msg.value())
            _, ts_ms = msg.timestamp()
            event_hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )

            with conn.cursor() as cur:
                cur.execute(_UPSERT, (payload["user_id"], payload["action"], event_hour))
            processed += 1

            if processed % 200 == 0:
                conn.commit()
                consumer.commit(asynchronous=False)

        conn.commit()
        consumer.commit(asynchronous=False)

    consumer.close()
    return {"processed": processed}
