"""Make `medallion` and DAG modules importable from tests without an editable install."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "dags"))


@pytest.fixture
def fake_settings(tmp_path):
    from medallion.common import Settings

    return Settings(
        bucket="test-bucket",
        s3_endpoint="http://localhost:0",
        s3_access_key="x",
        s3_secret_key="x",
        source_dir=str(tmp_path),
    )
