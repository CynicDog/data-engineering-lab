"""Tests for silver/transform.py.

Focus on the key invariants: dedup, PII masking, type casting, derived columns.
No Airflow, no network — pure PySpark + Delta on tmp_path.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class TestDedupByLatest:
    def test_keeps_latest_by_ingest_ts(self, spark):
        from lakehouse.silver.transform import dedup_by_latest

        rows = [
            ("C001", "Kim Cheol-su", datetime(2024, 1, 1, 9, 0)),
            ("C001", "Kim Cheol-su (updated)", datetime(2024, 1, 1, 10, 0)),
            ("C002", "Lee Young-hee", datetime(2024, 1, 1, 9, 0)),
        ]
        df = spark.createDataFrame(rows, ["customer_id", "name", "_ingest_ts"])
        result = dedup_by_latest(df, pk="customer_id")

        assert result.count() == 2
        c001 = result.filter("customer_id = 'C001'").first()
        assert c001["name"] == "Kim Cheol-su (updated)"

    def test_single_row_unchanged(self, spark):
        from lakehouse.silver.transform import dedup_by_latest

        df = spark.createDataFrame(
            [("P001", datetime(2024, 1, 1))], ["policy_id", "_ingest_ts"]
        )
        assert dedup_by_latest(df, pk="policy_id").count() == 1


class TestMaskRrn:
    def test_masks_back_seven_digits(self, spark):
        from lakehouse.silver.transform import mask_rrn

        df = spark.createDataFrame([("900101-1234567",)], ["rrn_masked"])
        result = mask_rrn(df).first()["rrn_masked"]
        assert result == "900101-*******"

    def test_already_masked_is_idempotent(self, spark):
        from lakehouse.silver.transform import mask_rrn

        df = spark.createDataFrame([("900101-*******",)], ["rrn_masked"])
        result = mask_rrn(df).first()["rrn_masked"]
        assert result == "900101-*******"


class TestMaskPhone:
    def test_masks_middle_digits(self, spark):
        from lakehouse.silver.transform import mask_phone

        df = spark.createDataFrame([("010-1234-5678",)], ["phone_masked"])
        result = mask_phone(df).first()["phone_masked"]
        assert result == "010-****-5678"


class TestTransformFromSpec:
    def _make_customer_spec(self):
        from lakehouse.config.registry import TableSpec

        return TableSpec(
            name="customer",
            channel="chan1",
            pk="customer_id",
            dedup_ts_col="_ingest_ts",
            schedule_type="daily",
            cast_types={"birth_date": "date", "created_at": "timestamp"},
            pii={"rrn_masked": "mask_rrn", "phone_masked": "mask_phone"},
            derived_columns={},
        )

    def test_pii_masking_applied(self, spark):
        from lakehouse.silver.transform import transform_from_spec

        spec = self._make_customer_spec()
        schema = StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("birth_date", StringType()),
            StructField("rrn_masked", StringType()),
            StructField("phone_masked", StringType()),
            StructField("created_at", TimestampType()),
            StructField("_ingest_ts", TimestampType()),
            StructField("_dt", StringType()),
        ])
        rows = [Row(
            customer_id="C001", name="Kim",
            birth_date="1990-01-01",
            rrn_masked="900101-1234567",
            phone_masked="010-1234-5678",
            created_at=datetime(2024, 1, 1),
            _ingest_ts=datetime(2024, 1, 15, 10, 0),
            _dt="2024-01-15",
        )]
        df = spark.createDataFrame(rows, schema=schema)
        result = transform_from_spec(df, spec)

        row = result.first()
        assert row["rrn_masked"] == "900101-*******"
        assert row["phone_masked"] == "010-****-5678"

    def test_dedup_keeps_latest(self, spark):
        from lakehouse.silver.transform import transform_from_spec

        spec = self._make_customer_spec()
        rows = [
            ("C001", "Old Name", "900101-*******", "010-****-5678",
             datetime(2024, 1, 1), datetime(2024, 1, 1, 9, 0), "2024-01-15"),
            ("C001", "New Name", "900101-*******", "010-****-5678",
             datetime(2024, 1, 1), datetime(2024, 1, 1, 10, 0), "2024-01-15"),
        ]
        df = spark.createDataFrame(
            rows,
            ["customer_id", "name", "rrn_masked", "phone_masked",
             "created_at", "_ingest_ts", "_dt"],
        )
        result = transform_from_spec(df, spec)
        assert result.count() == 1
        assert result.first()["name"] == "New Name"

    def test_derived_column_policy_age(self, spark):
        from lakehouse.config.registry import TableSpec
        from lakehouse.silver.transform import transform_from_spec

        spec = TableSpec(
            name="policy",
            channel="chan1",
            pk="policy_id",
            dedup_ts_col="_ingest_ts",
            schedule_type="daily",
            cast_types={"start_date": "date", "end_date": "date"},
            pii={},
            derived_columns={"policy_age_days": "datediff(coalesce(end_date, current_date()), start_date)"},
        )
        from datetime import date
        rows = [Row(
            policy_id="P001",
            start_date=date(2020, 1, 1),
            end_date=date(2021, 1, 1),
            _ingest_ts=datetime(2024, 1, 15),
            _dt="2024-01-15",
        )]
        schema = StructType([
            StructField("policy_id", StringType()),
            StructField("start_date", DateType()),
            StructField("end_date", DateType()),
            StructField("_ingest_ts", TimestampType()),
            StructField("_dt", StringType()),
        ])
        df = spark.createDataFrame(rows, schema=schema)
        result = transform_from_spec(df, spec)
        assert result.first()["policy_age_days"] == 366

    def test_derived_column_processing_days(self, spark):
        from lakehouse.config.registry import TableSpec
        from lakehouse.silver.transform import transform_from_spec

        spec = TableSpec(
            name="claims",
            channel="chan1",
            pk="claim_id",
            dedup_ts_col="_ingest_ts",
            schedule_type="daily",
            cast_types={"claim_date": "date", "processed_at": "timestamp"},
            pii={},
            derived_columns={
                "processing_days": "CASE WHEN processed_at IS NOT NULL THEN datediff(cast(processed_at AS date), claim_date) END"
            },
        )
        from datetime import date
        rows = [Row(
            claim_id="CL001",
            claim_date=date(2024, 1, 1),
            processed_at=datetime(2024, 1, 6),
            _ingest_ts=datetime(2024, 1, 15),
            _dt="2024-01-15",
        )]
        schema = StructType([
            StructField("claim_id", StringType()),
            StructField("claim_date", DateType()),
            StructField("processed_at", TimestampType()),
            StructField("_ingest_ts", TimestampType()),
            StructField("_dt", StringType()),
        ])
        df = spark.createDataFrame(rows, schema=schema)
        result = transform_from_spec(df, spec)
        assert result.first()["processing_days"] == 5
