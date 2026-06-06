"""Ingestion audit log — the replacement for the status.txt anti-pattern.

Anti-pattern (current state):
    ADF writes a status text file to ADLS.
    Databricks reads it, parses line-by-line, processes parquets, deletes the file.

    Problems:
    - Daily schedule file overlaps with hourly schedule file (same folder/extension).
    - File-based IPC: if the file isn't created, there's no way to know what ran.
    - Recovery requires grep-ing storage to find files.
    - No history: the file is deleted after processing.

Better pattern (this module):
    Write every ingestion event as a row in a Delta table (_control/ingestion_log).
    Query the table to know what ran, when, how many rows, and with what status.
    Recovery is a SQL query, not a storage crawl.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from lakehouse.config.settings import Settings
    from pyspark.sql import SparkSession

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("landing_path", StringType(), nullable=False),
        StructField("schedule_type", StringType(), nullable=False),  # daily | hourly
        StructField("dt", StringType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("row_count", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),  # success | error
        StructField("error_msg", StringType(), nullable=True),
    ]
)


def write_audit(
    spark: "SparkSession",
    audit_path: str,
    record: dict,
) -> None:
    """Append a single audit record to the ingestion_log Delta table.

    If the table doesn't exist yet, it's created automatically.
    This is idempotent by run_id: re-running the same task writes a new row
    (we keep history) rather than silently overwriting.
    """
    row = spark.createDataFrame([record], schema=AUDIT_SCHEMA)
    (
        row.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(audit_path)
    )


def get_successful_tables(
    spark: "SparkSession",
    audit_path: str,
    schedule_type: str,
    dt: str,
) -> list[str]:
    """Return tables that completed successfully for a given schedule_type + dt.

    Replaces: checking whether a 'check file' exists in a folder.
    Now: query the Delta table — no storage crawl, full history retained.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, audit_path):
        return []

    return [
        row["source_table"]
        for row in (
            spark.read.format("delta")
            .load(audit_path)
            .filter(
                (F.col("schedule_type") == schedule_type)
                & (F.col("dt") == dt)
                & (F.col("status") == "success")
            )
            .select("source_table")
            .distinct()
            .collect()
        )
    ]


def show_audit_log(spark: "SparkSession", audit_path: str, limit: int = 20) -> None:
    """Print recent audit log entries — useful for debugging pipeline failures."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, audit_path):
        print("Audit log not yet created.")
        return

    (
        spark.read.format("delta")
        .load(audit_path)
        .orderBy(F.col("ingested_at").desc())
        .limit(limit)
        .show(truncate=False)
    )
