"""Serving layer: rebuild the unified view that the application queries.

`batch_metrics` is treated as truth for every closed past hour.
`speed_metrics` fills the open current hour.
"""
from __future__ import annotations

import logging

import psycopg

from .common import load_settings

log = logging.getLogger(__name__)

# Mirror the schemas batch.py / speed.py create lazily, so the view can be
# rebuilt before either pipeline has ever run a tick (otherwise the first
# scheduled serving_refresh fails with UndefinedTable).
_BATCH_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS batch_metrics (
    user_id TEXT NOT NULL,
    action TEXT NOT NULL,
    event_hour TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL,
    PRIMARY KEY (user_id, action, event_hour)
)
"""

_SPEED_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS speed_metrics (
    user_id TEXT NOT NULL,
    action TEXT NOT NULL,
    event_hour TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, action, event_hour)
)
"""

_DROP_VIEW = "DROP VIEW IF EXISTS serving_metrics"

_CREATE_VIEW = """
CREATE VIEW serving_metrics AS
WITH batch_high_water AS (
    SELECT COALESCE(MAX(event_hour), TIMESTAMPTZ '1970-01-01') AS hw
    FROM batch_metrics
)
SELECT user_id, action, event_hour, event_count, 'batch' AS source
FROM batch_metrics
UNION ALL
SELECT s.user_id, s.action, s.event_hour, s.event_count, 'speed' AS source
FROM speed_metrics s, batch_high_water b
WHERE s.event_hour > b.hw
"""


def rebuild_serving_view() -> None:
    # psycopg3's prepared-statement protocol rejects multi-statement SQL,
    # so each DDL goes in its own execute() call.
    s = load_settings()
    with psycopg.connect(s.warehouse_dsn, autocommit=True) as conn:
        conn.execute(_BATCH_METRICS_DDL)
        conn.execute(_SPEED_METRICS_DDL)
        conn.execute(_DROP_VIEW)
        conn.execute(_CREATE_VIEW)
    log.info("serving_metrics view rebuilt")
