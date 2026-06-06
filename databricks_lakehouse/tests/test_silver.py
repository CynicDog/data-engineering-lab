"""Tests for silver/transform.py.

Focus on the key invariants: dedup, PII masking, type casting.
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


class TestTransformCustomer:
    def test_produces_required_columns(self, spark):
        from lakehouse.silver.transform import transform_customer
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

        schema = StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("birth_date", StringType()),
            StructField("gender", StringType()),
            StructField("channel", StringType()),
            StructField("rrn_masked", StringType()),
            StructField("phone_masked", StringType()),
            StructField("email", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType()),
            StructField("_ingest_ts", TimestampType()),
            StructField("_dt", StringType()),
        ])
        rows = [Row(
            customer_id="C001", name="Kim", birth_date="1990-01-01",
            gender="M", channel="온라인", rrn_masked="900101-1234567",
            phone_masked="010-1234-5678", email="kim@test.com",
            created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1),
            _ingest_ts=datetime(2024, 1, 15, 10, 0), _dt="2024-01-15",
        )]
        df = spark.createDataFrame(rows, schema=schema)
        result = transform_customer(df)

        # RRN must be masked
        assert result.first()["rrn_masked"] == "900101-*******"
        # Phone must be masked
        assert result.first()["phone_masked"] == "010-****-5678"


class TestTransformClaims:
    def test_computes_processing_days(self, spark):
        from lakehouse.silver.transform import transform_claims

        schema = StructType([
            StructField("claim_id", StringType()),
            StructField("policy_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("claim_date", DateType()),
            StructField("claim_amount", DoubleType()),
            StructField("settled_amount", DoubleType()),
            StructField("status", StringType()),
            StructField("claim_type", StringType()),
            StructField("processed_at", TimestampType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType()),
            StructField("_ingest_ts", TimestampType()),
            StructField("_dt", StringType()),
        ])
        from datetime import date
        rows = [Row(
            claim_id="CL001", policy_id="P001", customer_id="C001",
            claim_date=date(2024, 1, 1),
            claim_amount=1000000.0, settled_amount=900000.0,
            status="지급완료", claim_type="입원",
            processed_at=datetime(2024, 1, 6),
            created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 6),
            _ingest_ts=datetime(2024, 1, 15, 10, 0), _dt="2024-01-15",
        )]
        df = spark.createDataFrame(rows, schema=schema)
        result = transform_claims(df)

        assert result.first()["processing_days"] == 5
