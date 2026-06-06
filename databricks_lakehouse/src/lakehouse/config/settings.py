"""Profile-based settings — the answer to VOCP/VOCD catalog chaos.

Instead of branching on environment inside source code:
    if env == "prod":
        catalog = "VOCP"
    else:
        catalog = "VOCD"

Inject the catalog name (and everything else) via a profile config file.
All code references `settings.catalog`, never a hardcoded string.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parents[3]  # .../databricks_lakehouse/


@dataclass(frozen=True)
class Settings:
    env: str
    catalog: str  # "VOCD" in dev, "VOCP" in prod — injected, never hardcoded
    lake_bucket: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    ods_url: str

    def landing_path(self, table: str, dt: str) -> str:
        return f"s3a://{self.lake_bucket}/landing/{table}/dt={dt}"

    def bronze_path(self, table: str) -> str:
        return f"s3a://{self.lake_bucket}/bronze/{table}"

    def silver_path(self, table: str) -> str:
        return f"s3a://{self.lake_bucket}/silver/{table}"

    def gold_path(self, mart: str) -> str:
        return f"s3a://{self.lake_bucket}/gold/{mart}"

    def control_path(self, name: str) -> str:
        """Control plane paths: audit log, pipeline flags, etc."""
        return f"s3a://{self.lake_bucket}/control/{name}"

    @property
    def spark_s3a_conf(self) -> dict[str, str]:
        """Spark Hadoop config keys for S3A (MinIO / ADLS) access."""
        return {
            "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }


def load_settings(profile: str | None = None) -> Settings:
    """Load settings from config/{profile}.yaml, with env-var overrides.

    Resolution order (highest wins):
    1. Environment variables (LAKE_BUCKET, LAKE_S3_ENDPOINT, …)
    2. config/{profile}.yaml
    3. Defaults

    The active profile is determined by:
    - `profile` argument  →  LAKEHOUSE_PROFILE env var  →  "dev"
    """
    profile = profile or os.environ.get("LAKEHOUSE_PROFILE", "dev")
    config_path = _REPO_ROOT / "config" / f"{profile}.yaml"

    with open(config_path) as f:
        data: dict = yaml.safe_load(f)

    return Settings(
        env=data["env"],
        catalog=data["catalog"],
        lake_bucket=os.environ.get("LAKE_BUCKET", data["lake_bucket"]),
        s3_endpoint=os.environ.get("LAKE_S3_ENDPOINT", data["s3_endpoint"]),
        s3_access_key=os.environ.get("LAKE_S3_ACCESS_KEY", data["s3_access_key"]),
        s3_secret_key=os.environ.get("LAKE_S3_SECRET_KEY", data["s3_secret_key"]),
        ods_url=os.environ.get("ODS_URL", data["ods_url"]),
    )
