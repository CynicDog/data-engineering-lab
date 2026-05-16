from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bucket: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    source_dir: str

    def bronze_path(self, table: str, dt: str) -> str:
        return f"s3://{self.bucket}/bronze/{table}/dt={dt}/part.parquet"

    @property
    def storage_options(self) -> dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": self.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": self.s3_secret_key,
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
        }


def load_settings() -> Settings:
    return Settings(
        bucket=os.environ.get("LAKE_BUCKET", "lakehouse"),
        s3_endpoint=os.environ.get("LAKE_S3_ENDPOINT", "http://localhost:9000"),
        s3_access_key=os.environ.get("LAKE_S3_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.environ.get("LAKE_S3_SECRET_KEY", "minioadmin"),
        source_dir=os.environ.get("LAKE_SOURCE_DIR", "/opt/airflow/source_data"),
    )


settings = load_settings()
