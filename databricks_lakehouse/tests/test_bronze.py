"""Tests for bronze/audit.py and bronze/ingest.py.

These are pure unit tests: no Airflow, no MinIO, no network.
Delta tables are written to tmp_path (local filesystem).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


def _make_audit_record(**overrides) -> dict:
    base = {
        "run_id": "test-run-001",
        "source_table": "customer",
        "landing_path": "/tmp/landing/customer/dt=2024-01-15",
        "schedule_type": "daily",
        "dt": "2024-01-15",
        "ingested_at": datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        "row_count": 500,
        "status": "success",
        "error_msg": None,
    }
    return {**base, **overrides}


class TestWriteAudit:
    def test_creates_delta_table_on_first_write(self, spark, local_paths):
        from lakehouse.bronze.audit import write_audit

        audit_path = local_paths["audit"]
        write_audit(spark, audit_path, _make_audit_record())

        df = spark.read.format("delta").load(audit_path)
        assert df.count() == 1

    def test_appends_multiple_records(self, spark, local_paths):
        from lakehouse.bronze.audit import write_audit

        audit_path = local_paths["audit"]
        for i in range(3):
            write_audit(spark, audit_path, _make_audit_record(run_id=f"run-{i}"))

        df = spark.read.format("delta").load(audit_path)
        assert df.count() == 3

    def test_records_error_status(self, spark, local_paths):
        from lakehouse.bronze.audit import write_audit

        audit_path = local_paths["audit"]
        write_audit(
            spark,
            audit_path,
            _make_audit_record(status="error", error_msg="FileNotFoundError: no parquet"),
        )

        row = spark.read.format("delta").load(audit_path).first()
        assert row["status"] == "error"
        assert "FileNotFoundError" in row["error_msg"]


class TestGetSuccessfulTables:
    def test_returns_empty_when_table_not_exists(self, spark, local_paths):
        from lakehouse.bronze.audit import get_successful_tables

        result = get_successful_tables(spark, local_paths["audit"], "daily", "2024-01-15")
        assert result == []

    def test_returns_only_successful_tables_for_dt(self, spark, local_paths):
        from lakehouse.bronze.audit import get_successful_tables, write_audit

        audit_path = local_paths["audit"]
        write_audit(spark, audit_path, _make_audit_record(source_table="customer", status="success"))
        write_audit(spark, audit_path, _make_audit_record(source_table="policy", status="error"))
        write_audit(spark, audit_path, _make_audit_record(source_table="voc", status="success"))

        result = get_successful_tables(spark, audit_path, "daily", "2024-01-15")
        assert sorted(result) == ["customer", "voc"]

    def test_filters_by_schedule_type(self, spark, local_paths):
        from lakehouse.bronze.audit import get_successful_tables, write_audit

        audit_path = local_paths["audit"]
        write_audit(spark, audit_path, _make_audit_record(source_table="customer", schedule_type="daily"))
        write_audit(spark, audit_path, _make_audit_record(source_table="customer", schedule_type="hourly"))

        daily = get_successful_tables(spark, audit_path, "daily", "2024-01-15")
        hourly = get_successful_tables(spark, audit_path, "hourly", "2024-01-15")

        assert daily == ["customer"]
        assert hourly == ["customer"]
