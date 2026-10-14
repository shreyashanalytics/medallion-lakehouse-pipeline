"""Central, environment-driven configuration for the lakehouse pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime settings; every value can be overridden through env variables."""

    s3_endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    bucket: str
    api_base_url: str
    synthetic_rows: int
    lookback_days: int
    spark_driver_memory: str

    @property
    def s3a_root(self) -> str:
        """Root URI used by Spark (s3a scheme)."""
        return f"s3a://{self.bucket}"


def get_settings() -> Settings:
    """Build :class:`Settings` from the current environment."""
    return Settings(
        s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localstack:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        bucket=os.getenv("LAKEHOUSE_BUCKET", "lakehouse"),
        api_base_url=os.getenv("API_BASE_URL", "https://dummyjson.com"),
        synthetic_rows=int(os.getenv("SYNTHETIC_ROWS", "50000")),
        lookback_days=int(os.getenv("SYNTHETIC_LOOKBACK_DAYS", "30")),
        spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
    )


def bronze_prefix(entity: str, ds: str) -> str:
    """Hive-style Bronze prefix, e.g. ``bronze/users/ingestion_date=2025-01-01``."""
    return f"bronze/{entity}/ingestion_date={ds}"
