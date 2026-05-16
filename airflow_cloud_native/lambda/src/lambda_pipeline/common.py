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

    @property
    def bronze_uri(self) -> str:
        return f"s3://{self.bucket}/bronze/{self.topic}"

    @property
    def silver_uri(self) -> str:
        return f"s3://{self.bucket}/silver/{self.topic}_hourly"

    @property
    def storage_options(self) -> dict[str, str]:
        # delta-rs / pyarrow / s3fs all consume this shape
        return {
            "AWS_ACCESS_KEY_ID": self.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": self.s3_secret_key,
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            # batch_layer is max_active_runs=1 and writes the only Silver path —
            # no concurrent writers, so the LockClient (DynamoDB) requirement
            # doesn't apply. Without this flag delta-rs refuses S3 writes.
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap=os.environ.get("LAMBDA_KAFKA_BOOTSTRAP", "localhost:19092"),
        schema_registry=os.environ.get("LAMBDA_SCHEMA_REGISTRY", "http://localhost:18081"),
        topic=os.environ.get("LAMBDA_TOPIC", "events"),
        s3_endpoint=os.environ.get("LAMBDA_S3_ENDPOINT", "http://localhost:9000"),
        s3_access_key=os.environ.get("LAMBDA_S3_ACCESS_KEY", "minioadmin"),
        s3_secret_key=os.environ.get("LAMBDA_S3_SECRET_KEY", "minioadmin"),
        bucket=os.environ.get("LAMBDA_BUCKET", "lambda-lake"),
        warehouse_dsn=os.environ.get(
            "LAMBDA_WAREHOUSE_DSN",
            "postgresql://warehouse:warehouse@localhost:5432/warehouse",
        ),
    )


settings = load_settings()
