"""Pipeline integration tests — Bronze → Silver → Gold on local filesystem.

These tests run the actual ingest / transform / mart functions end-to-end
with real Parquet writes, real Delta Lake tables, and real PySpark processing.

No Airflow, no MinIO — all I/O goes to tmp_path via the `local_settings` fixture.
The key is Settings._storage_root: when s3_endpoint is empty, lake_bucket is
treated as a local path and all path methods return local filesystem paths.

What these tests cover:
  - Bronze: Parquet landing zone → Delta (audit columns, partitioning, idempotency)
  - Silver: generic transform_from_spec (cast types, dedup, PII masking, derived cols)
  - Gold: cross-table aggregation marts
  - Full chain: landing → bronze → silver → gold end-to-end
"""

from __future__ import annotations

import io
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest


DT = "2024-01-15"

_BASE_DATE = date(2024, 1, 15)
_TS = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


def _customers() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "customer_id": "C001",
            "name": "Kim Cheol-su",
            "rrn_masked": "900101-1234567",
            "birth_date": "1990-01-01",
            "gender": "M",
            "channel": "온라인",
            "phone_masked": "010-1234-5678",
            "email": "kim@test.com",
            "created_at": _TS,
            "updated_at": _TS,
        },
        {
            "customer_id": "C002",
            "name": "Lee Young-hee",
            "rrn_masked": "850615-2345678",
            "birth_date": "1985-06-15",
            "gender": "F",
            "channel": "설계사",
            "phone_masked": "010-9876-5432",
            "email": "lee@test.com",
            "created_at": _TS,
            "updated_at": _TS,
        },
        {
            "customer_id": "C001",
            "name": "Kim Cheol-su (updated)",
            "rrn_masked": "900101-1234567",
            "birth_date": "1990-01-01",
            "gender": "M",
            "channel": "온라인",
            "phone_masked": "010-1234-5678",
            "email": "kim_new@test.com",
            "created_at": _TS,
            "updated_at": datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        },
    ])


def _policies() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "policy_id": "P001",
            "customer_id": "C001",
            "product_code": "LIFE",
            "product_name": "종신보험",
            "start_date": "2020-01-01",
            "end_date": "2040-01-01",
            "premium": 500000.0,
            "status": "active",
            "created_at": _TS,
            "updated_at": _TS,
        },
        {
            "policy_id": "P002",
            "customer_id": "C002",
            "product_code": "HEALTH",
            "product_name": "건강보험",
            "start_date": "2021-06-01",
            "end_date": "2031-06-01",
            "premium": 300000.0,
            "status": "active",
            "created_at": _TS,
            "updated_at": _TS,
        },
    ])


def _claims() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "claim_id": "CL001",
            "policy_id": "P001",
            "customer_id": "C001",
            "claim_date": date(2024, 1, 1),
            "claim_amount": 1_000_000.0,
            "settled_amount": 900_000.0,
            "status": "지급완료",
            "claim_type": "입원",
            "processed_at": datetime(2024, 1, 6, 12, 0, 0, tzinfo=timezone.utc),
            "created_at": _TS,
            "updated_at": _TS,
        },
    ])


def _voc() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "voc_id": "V001",
            "customer_id": "C001",
            "complaint_type": "보험금",
            "channel": "전화",
            "description": "보험금 처리가 너무 느립니다.",
            "priority": "high",
            "status": "resolved",
            "created_at": datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc),
            "resolution_note": "처리 완료.",
        },
        {
            "voc_id": "V002",
            "customer_id": "C002",
            "complaint_type": "서비스",
            "channel": "온라인",
            "description": "앱 오류 발생.",
            "priority": "normal",
            "status": "open",
            "created_at": datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            "resolved_at": None,
            "resolution_note": None,
        },
    ])


def _write_parquet(df: pd.DataFrame, dir_path: str) -> None:
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    df.to_parquet(f"{dir_path}/part.parquet", index=False)


class TestBronzeIngestion:
    def test_ingest_creates_delta_with_system_columns(self, spark, local_settings):
        _write_parquet(_customers(), local_settings.landing_path("chan1", "customer", DT))

        from lakehouse.bronze.ingest import ingest_table

        rows = ingest_table(spark, local_settings, "chan1", "customer", DT)

        assert rows == 3  # 3 rows from _customers() including the duplicate

        bronze_df = spark.read.format("delta").load(
            local_settings.bronze_path("chan1", "customer")
        )
        assert bronze_df.count() == 3
        assert "_ingest_ts" in bronze_df.columns
        assert "_dt" in bronze_df.columns
        assert bronze_df.filter("_dt = '2024-01-15'").count() == 3

    def test_ingest_is_idempotent(self, spark, local_settings):
        _write_parquet(_customers(), local_settings.landing_path("chan1", "customer", DT))

        from lakehouse.bronze.ingest import ingest_table

        ingest_table(spark, local_settings, "chan1", "customer", DT)
        ingest_table(spark, local_settings, "chan1", "customer", DT)

        bronze_df = spark.read.format("delta").load(
            local_settings.bronze_path("chan1", "customer")
        )
        assert bronze_df.count() == 3  # same 3 rows, not 6

    def test_ingest_with_audit_records_success(self, spark, local_settings):
        _write_parquet(_customers(), local_settings.landing_path("chan1", "customer", DT))

        from lakehouse.bronze.ingest import ingest_table_with_audit

        result = ingest_table_with_audit(
            spark, local_settings, "chan1", "customer", DT, schedule_type="daily"
        )
        assert result["status"] == "success"
        assert result["rows"] == 3

        audit_df = spark.read.format("delta").load(local_settings.control_path("ingestion_log"))
        row = audit_df.filter("status = 'success'").first()
        assert row["source_table"] == "chan1.customer"
        assert row["row_count"] == 3

    def test_ingest_multi_channel_no_path_conflict(self, spark, local_settings):
        _write_parquet(_customers(), local_settings.landing_path("chan1", "customer", DT))
        _write_parquet(_voc(), local_settings.landing_path("chan2", "voc", DT))

        from lakehouse.bronze.ingest import ingest_table

        cu_rows = ingest_table(spark, local_settings, "chan1", "customer", DT)
        voc_rows = ingest_table(spark, local_settings, "chan2", "voc", DT)

        assert cu_rows == 3
        assert voc_rows == 2

        assert local_settings.bronze_path("chan1", "customer") != \
               local_settings.bronze_path("chan2", "voc")


class TestSilverTransform:
    def _ingest_all(self, spark, settings):
        from lakehouse.bronze.ingest import ingest_table

        _write_parquet(_customers(), settings.landing_path("chan1", "customer", DT))
        _write_parquet(_policies(), settings.landing_path("chan1", "policy", DT))
        _write_parquet(_claims(), settings.landing_path("chan1", "claims", DT))
        _write_parquet(_voc(), settings.landing_path("chan2", "voc", DT))

        for ch, tbl in [("chan1", "customer"), ("chan1", "policy"),
                        ("chan1", "claims"), ("chan2", "voc")]:
            ingest_table(spark, settings, ch, tbl, DT)

    def test_customer_dedup_and_pii_masking(self, spark, local_settings):
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        _write_parquet(_customers(), local_settings.landing_path("chan1", "customer", DT))
        ingest_table(spark, local_settings, "chan1", "customer", DT)

        spec = load_registry().table_spec("chan1", "customer")
        transform_table(spark, local_settings, spec)

        silver_df = spark.read.format("delta").load(
            local_settings.silver_path("chan1", "customer")
        )

        # 3 bronze rows → 2 silver rows (C001 deduplicated regardless of which wins)
        assert silver_df.count() == 2

        # PII must be masked on every row
        for row in silver_df.collect():
            assert "****" in row["phone_masked"], f"phone not masked: {row['phone_masked']}"
            assert "*******" in row["rrn_masked"], f"rrn not masked: {row['rrn_masked']}"

    def test_dedup_across_batches_keeps_latest_ingest(self, spark, local_settings):
        """Row ingested in a later Bronze run wins dedup over the same PK from earlier run.

        Within a single Parquet file all rows share the same _ingest_ts so tie-breaking
        is non-deterministic. Across separate ingestion dates the timestamps differ and
        dedup is deterministic.
        """
        import time
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        dt_old = "2024-01-14"
        dt_new = "2024-01-15"

        old_batch = pd.DataFrame([{
            "customer_id": "C001", "name": "Kim (old)", "rrn_masked": "900101-*******",
            "birth_date": "1990-01-01", "gender": "M", "channel": "온라인",
            "phone_masked": "010-1234-5678", "email": "old@test.com",
            "created_at": _TS, "updated_at": _TS,
        }])
        _write_parquet(old_batch, local_settings.landing_path("chan1", "customer", dt_old))
        ingest_table(spark, local_settings, "chan1", "customer", dt_old)

        time.sleep(1)  # ensure the two current_timestamp() calls differ

        new_batch = pd.DataFrame([{
            "customer_id": "C001", "name": "Kim (new)", "rrn_masked": "900101-*******",
            "birth_date": "1990-01-01", "gender": "M", "channel": "온라인",
            "phone_masked": "010-1234-5678", "email": "new@test.com",
            "created_at": _TS, "updated_at": _TS,
        }])
        _write_parquet(new_batch, local_settings.landing_path("chan1", "customer", dt_new))
        ingest_table(spark, local_settings, "chan1", "customer", dt_new)

        spec = load_registry().table_spec("chan1", "customer")
        transform_table(spark, local_settings, spec)

        silver_df = spark.read.format("delta").load(
            local_settings.silver_path("chan1", "customer")
        )
        assert silver_df.count() == 1
        assert silver_df.first()["name"] == "Kim (new)"

    def test_policy_derived_column(self, spark, local_settings):
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        _write_parquet(_policies(), local_settings.landing_path("chan1", "policy", DT))
        ingest_table(spark, local_settings, "chan1", "policy", DT)

        spec = load_registry().table_spec("chan1", "policy")
        transform_table(spark, local_settings, spec)

        silver_df = spark.read.format("delta").load(
            local_settings.silver_path("chan1", "policy")
        )
        p001 = silver_df.filter("policy_id = 'P001'").first()
        # 2020-01-01 to 2040-01-01 = 7305 days
        assert p001["policy_age_days"] == 7305

    def test_claims_processing_days_derived_column(self, spark, local_settings):
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        _write_parquet(_claims(), local_settings.landing_path("chan1", "claims", DT))
        ingest_table(spark, local_settings, "chan1", "claims", DT)

        spec = load_registry().table_spec("chan1", "claims")
        transform_table(spark, local_settings, spec)

        silver_df = spark.read.format("delta").load(
            local_settings.silver_path("chan1", "claims")
        )
        cl001 = silver_df.filter("claim_id = 'CL001'").first()
        # 2024-01-01 to 2024-01-06 = 5 days
        assert cl001["processing_days"] == 5

    def test_voc_resolution_hours_derived_column(self, spark, local_settings):
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        _write_parquet(_voc(), local_settings.landing_path("chan2", "voc", DT))
        ingest_table(spark, local_settings, "chan2", "voc", DT)

        spec = load_registry().table_spec("chan2", "voc")
        transform_table(spark, local_settings, spec)

        silver_df = spark.read.format("delta").load(
            local_settings.silver_path("chan2", "voc")
        )
        v001 = silver_df.filter("voc_id = 'V001'").first()
        # 09:00 → 11:00 = 2.0 hours
        assert v001["resolution_hours"] == pytest.approx(2.0, abs=0.01)

        # V002 is unresolved — resolution_hours must be NULL
        v002 = silver_df.filter("voc_id = 'V002'").first()
        assert v002["resolution_hours"] is None

    def test_transform_is_idempotent(self, spark, local_settings):
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        _write_parquet(_customers(), local_settings.landing_path("chan1", "customer", DT))
        ingest_table(spark, local_settings, "chan1", "customer", DT)

        spec = load_registry().table_spec("chan1", "customer")
        transform_table(spark, local_settings, spec)
        rows_second_run = transform_table(spark, local_settings, spec)

        silver_df = spark.read.format("delta").load(
            local_settings.silver_path("chan1", "customer")
        )
        assert silver_df.count() == rows_second_run == 2


class TestGoldMartsIntegration:
    def _setup_silver(self, spark, settings):
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        tables = [
            ("chan1", "customer", _customers()),
            ("chan1", "policy", _policies()),
            ("chan1", "claims", _claims()),
            ("chan2", "voc", _voc()),
        ]
        registry = load_registry()
        for ch, tbl, df in tables:
            _write_parquet(df, settings.landing_path(ch, tbl, DT))
            ingest_table(spark, settings, ch, tbl, DT)
            transform_table(spark, settings, registry.table_spec(ch, tbl))

    def test_voc_daily_mart_columns_and_rows(self, spark, local_settings):
        from lakehouse.gold.mart import build_voc_daily

        self._setup_silver(spark, local_settings)
        rows = build_voc_daily(spark, local_settings)
        assert rows > 0

        mart_df = spark.read.format("delta").load(local_settings.gold_path("voc_daily"))
        expected_cols = {"date", "complaint_type", "total_voc", "resolved_count",
                         "avg_resolution_hours", "resolution_rate"}
        assert expected_cols.issubset(set(mart_df.columns))

    def test_policy_summary_only_active(self, spark, local_settings):
        from lakehouse.gold.mart import build_policy_summary

        self._setup_silver(spark, local_settings)
        build_policy_summary(spark, local_settings)

        mart_df = spark.read.format("delta").load(local_settings.gold_path("policy_summary"))
        # Both P001 and P002 are active — should appear in mart
        assert mart_df.agg({"active_policies": "sum"}).first()[0] == 2

    def test_claims_analysis_settlement_ratio(self, spark, local_settings):
        from lakehouse.gold.mart import build_claims_analysis

        self._setup_silver(spark, local_settings)
        build_claims_analysis(spark, local_settings)

        mart_df = spark.read.format("delta").load(local_settings.gold_path("claims_analysis"))
        row = mart_df.filter("status = '지급완료'").first()
        assert row is not None
        # settled_amount 900_000 / claim_amount 1_000_000 = 0.9
        assert row["settlement_ratio"] == pytest.approx(0.9, abs=0.001)


class TestEndToEndPipeline:
    def test_full_bronze_silver_gold_chain(self, spark, local_settings):
        """Data written at landing emerges correctly shaped in Gold."""
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.gold.mart import build_voc_daily
        from lakehouse.silver.transform import transform_table

        registry = load_registry()

        tables = [
            ("chan1", "customer", _customers()),
            ("chan1", "policy", _policies()),
            ("chan1", "claims", _claims()),
            ("chan2", "voc", _voc()),
        ]

        for ch, tbl, df in tables:
            _write_parquet(df, local_settings.landing_path(ch, tbl, DT))

        bronze_counts = {}
        for ch, tbl, _ in tables:
            bronze_counts[(ch, tbl)] = ingest_table(spark, local_settings, ch, tbl, DT)

        assert bronze_counts[("chan1", "customer")] == 3  # 2 unique + 1 duplicate
        assert bronze_counts[("chan2", "voc")] == 2

        silver_counts = {}
        for ch, tbl, _ in tables:
            spec = registry.table_spec(ch, tbl)
            silver_counts[(ch, tbl)] = transform_table(spark, local_settings, spec)

        assert silver_counts[("chan1", "customer")] == 2  # dedup removed C001 duplicate

        gold_rows = build_voc_daily(spark, local_settings)
        assert gold_rows > 0

        gold_df = spark.read.format("delta").load(local_settings.gold_path("voc_daily"))
        assert "resolution_rate" in gold_df.columns

    def test_registry_drives_all_layers(self, spark, local_settings):
        """All tables in the registry can be ingested and transformed without errors."""
        from lakehouse.bronze.ingest import ingest_table
        from lakehouse.config.registry import load_registry
        from lakehouse.silver.transform import transform_table

        registry = load_registry()

        source_data = {
            ("chan1", "customer"): _customers(),
            ("chan1", "policy"): _policies(),
            ("chan1", "claims"): _claims(),
            ("chan2", "voc"): _voc(),
        }

        for spec in registry.all_tables():
            df = source_data[(spec.channel, spec.name)]
            _write_parquet(df, local_settings.landing_path(spec.channel, spec.name, DT))
            ingest_table(spark, local_settings, spec.channel, spec.name, DT)
            rows = transform_table(spark, local_settings, spec)
            assert rows > 0, f"Silver empty for {spec.channel}.{spec.name}"
