from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession in local[1] mode with Delta Lake enabled.

    scope=session means one SparkSession for all tests — JVM startup happens
    once. This is the same reason we want an always-on cluster in production.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("lakehouse-tests")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def fake_settings(tmp_path):
    """Settings pointing at tmp_path (local filesystem, no S3)."""
    from lakehouse.config.settings import Settings

    base = str(tmp_path)
    return Settings(
        env="test",
        catalog="TEST",
        lake_bucket="test-bucket",
        s3_endpoint="",
        s3_access_key="",
        s3_secret_key="",
        ods_url="",
    ), base  # Return both settings and the base path for overriding paths


@pytest.fixture
def local_paths(tmp_path):
    """Concrete local paths for Delta tables (no S3 in unit tests)."""
    return {
        "bronze": str(tmp_path / "bronze"),
        "silver": str(tmp_path / "silver"),
        "gold": str(tmp_path / "gold"),
        "audit": str(tmp_path / "control" / "ingestion_log"),
    }
