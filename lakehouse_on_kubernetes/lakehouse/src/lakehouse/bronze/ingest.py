"""Bronze ingestion: Parquet landing zone → Delta table.

This is the Databricks pattern equivalent of:
    ADF writes Parquet to ADLS landing zone
    → Databricks reads it and writes a Delta table.

The ingestion is idempotent per (channel, table, dt): re-running for the same date
overwrites that partition only, leaving other partitions intact.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from lakehouse.config.settings import Settings
    from pyspark.sql import SparkSession


def ingest_table(
    spark: "SparkSession",
    settings: "Settings",
    channel: str,
    table: str,
    dt: str,
) -> int:
    """Read Parquet from the landing zone and write to a Delta bronze table.

    Returns the number of rows written.

    Landing path: s3a://lakehouse/landing/{channel}/{table}/dt={dt}/*.parquet
    Bronze path:  s3a://lakehouse/bronze/{channel}/{table}   (partitioned by _dt)

    Why partition by _dt?
        Enables partition pruning on date filters and makes replaceWhere
        overwrites cheap — only the target date partition is rewritten.
    """
    landing_path = f"{settings.landing_path(channel, table, dt)}/*.parquet"
    bronze_path = settings.bronze_path(channel, table)

    df = (
        spark.read.parquet(landing_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_dt", F.lit(dt))
    )

    row_count = df.count()

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_dt = '{dt}'")
        .partitionBy("_dt")
        .save(bronze_path)
    )

    return row_count


def ingest_table_with_audit(
    spark: "SparkSession",
    settings: "Settings",
    channel: str,
    table: str,
    dt: str,
    schedule_type: str,
    run_id: str | None = None,
) -> dict:
    """Ingest a table and write the result to the audit log.

    This is the full production-grade version combining ingest + audit.
    Returns a summary dict suitable for use as an Airflow task's XCom return value.
    """
    from lakehouse.bronze.audit import write_audit

    run_id = run_id or str(uuid.uuid4())
    audit_path = settings.control_path("ingestion_log")
    landing_path = settings.landing_path(channel, table, dt)

    try:
        row_count = ingest_table(spark, settings, channel, table, dt)
        write_audit(
            spark,
            audit_path,
            {
                "run_id": run_id,
                "source_table": f"{channel}.{table}",
                "landing_path": landing_path,
                "schedule_type": schedule_type,
                "dt": dt,
                "ingested_at": datetime.now(timezone.utc),
                "row_count": row_count,
                "status": "success",
                "error_msg": None,
            },
        )
        return {"channel": channel, "table": table, "dt": dt, "rows": row_count, "status": "success"}

    except Exception as exc:
        write_audit(
            spark,
            audit_path,
            {
                "run_id": run_id,
                "source_table": f"{channel}.{table}",
                "landing_path": landing_path,
                "schedule_type": schedule_type,
                "dt": dt,
                "ingested_at": datetime.now(timezone.utc),
                "row_count": 0,
                "status": "error",
                "error_msg": str(exc)[:2000],
            },
        )
        raise
