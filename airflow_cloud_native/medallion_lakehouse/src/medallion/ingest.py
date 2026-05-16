"""Bronze ingest: copy source files (CSV/JSON) from /opt/airflow/source_data
into Hive-partitioned Parquet on MinIO.

Bronze is **append-only** and partitioned by ingest date. The Silver layer
deduplicates — that lets us re-ingest the same source file safely if upstream
sends late retries.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from .common import load_settings

log = logging.getLogger(__name__)

TABLES = ["orders", "customers", "products", "web_events"]


def ingest_table(table: str, dt: str | None = None) -> dict[str, int]:
    """Read source_data/{table}.csv → write to s3://.../bronze/{table}/dt=.../part.parquet.

    `dt` defaults to today (UTC). Pass an older date to backfill — the path is
    fully deterministic so re-runs overwrite the same partition.
    """
    s = load_settings()
    dt = dt or datetime.now(tz=timezone.utc).date().isoformat()

    src = Path(s.source_dir) / f"{table}.csv"
    if not src.exists():
        log.warning("source file %s does not exist — skipping", src)
        return {"rows": 0}

    df = pl.read_csv(src, try_parse_dates=True).with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("ingest_ts")
    )

    out = s.bronze_path(table, dt)
    df.write_parquet(out, storage_options=s.storage_options)
    log.info("wrote %s rows to %s", df.height, out)
    return {"rows": df.height, "path": out}


def ingest_all(dt: str | None = None) -> dict[str, dict]:
    return {t: ingest_table(t, dt) for t in TABLES}
