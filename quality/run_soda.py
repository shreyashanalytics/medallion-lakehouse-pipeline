"""Run Soda Core contract checks against the Silver Parquet files in S3.

DuckDB reads the Parquet directly (httpfs), exposes it as views, and Soda scans
those views. Exit code 1 => at least one check failed => Airflow task fails.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import duckdb
from soda.scan import Scan

CHECKS_FILE = Path(__file__).with_name("checks.yml")
SILVER_VIEWS = {
    "silver_transactions": "transactions/**/*.parquet",
    "silver_customers": "customers/*.parquet",
    "silver_products": "products/*.parquet",
}


def build_connection() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with S3 (LocalStack) access and Silver views."""
    endpoint = urlparse(os.getenv("S3_ENDPOINT_URL", "http://localstack:4566"))
    bucket = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")

    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    settings = {
        "s3_endpoint": endpoint.netloc,
        "s3_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "s3_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        "s3_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "s3_url_style": "path",
        "s3_use_ssl": str(endpoint.scheme == "https").lower(),
    }
    for key, value in settings.items():
        con.execute(f"SET {key}='{value}'")

    for view, pattern in SILVER_VIEWS.items():
        con.execute(
            f"CREATE OR REPLACE VIEW {view} AS "
            f"SELECT * FROM read_parquet('s3://{bucket}/silver/{pattern}', "
            "hive_partitioning=true)"
        )
    return con


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    scan = Scan()
    scan.set_scan_definition_name("silver_data_contract")
    scan.set_data_source_name("duckdb")
    scan.add_duckdb_connection(build_connection())
    scan.add_sodacl_yaml_file(str(CHECKS_FILE))
    scan.execute()
    print(scan.get_logs_text())

    try:
        scan.assert_no_error_logs()
        scan.assert_no_checks_fail()
    except AssertionError as exc:
        print(f"DATA CONTRACT VIOLATION: {exc}", file=sys.stderr)
        return 1
    print("Data contract passed: Silver layer is safe to promote to Gold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
