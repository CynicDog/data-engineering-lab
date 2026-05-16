from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    schema_registry: str
    topic: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    bucket: str
    warehouse_dsn: str

    def silver_uri(self, table: str = "events_rollup", version: str = "current") -> str:
        return f"s3://{self.bucket}/silver/{table}/{version}"

    @property
    def storage_options(self) -> dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": self.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": self.s3_secret_key,
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            # Safe here: stream DAG is max_active_runs=1 and replay writes to a
            # fresh versioned path before swapping — there is never a concurrent
            # writer on the same Delta location, so the LockClient is unneeded.
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap=os.environ.get("KAPPA_KAFKA_BOOTSTRAP", "localhost:29092"),
        schema_registry=os.environ.get("KAPPA_SCHEMA_REGISTRY", "http://localhost:28081"),
        topic=os.environ.get("KAPPA_TOPIC", "events"),
        s3_endpoint=os.environ.get("KAPPA_S3_ENDPOINT", "http://localhost:9010"),
        s3_access_key=os.environ.get("KAPPA_S3_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.environ.get("KAPPA_S3_SECRET_KEY", "minioadmin"),
        bucket=os.environ.get("KAPPA_BUCKET", "kappa-lake"),
        warehouse_dsn=os.environ.get(
            "KAPPA_WAREHOUSE_DSN",
            "postgresql://warehouse:warehouse@localhost:5442/warehouse",
        ),
    )


settings = load_settings()
