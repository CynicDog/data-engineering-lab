"""Make `kappa_pipeline` and the DAG modules importable from tests without
requiring an editable install."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "dags"))


@pytest.fixture
def fake_settings():
    from kappa_pipeline.common import Settings

    return Settings(
        kafka_bootstrap="localhost:0",
        schema_registry="http://localhost:0",
        topic="test-events",
        s3_endpoint="http://localhost:0",
        s3_access_key="x",
        s3_secret_key="x",
        bucket="test-bucket",
        warehouse_dsn="postgresql://x:x@localhost:0/x",
    )
