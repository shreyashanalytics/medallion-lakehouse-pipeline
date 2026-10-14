"""Bronze ingestion: public API -> raw JSON Lines in an S3 (LocalStack) bucket.

* ``products`` and ``users`` come from the free DummyJSON API (no key needed).
* ``transactions`` are generated deterministically from those real entities,
  with deliberate data-quality problems (duplicates, nulls, messy casing) so the
  downstream Silver/quality layers have real work to do.

Swap :func:`generate_transactions` for a Kaggle CSV reader if you prefer; the
rest of the pipeline only depends on the Bronze JSONL contract.

Run standalone:  ``python -m lakehouse.ingestion --ds 2025-01-31``
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lakehouse.config import Settings, bronze_prefix, get_settings

logger = logging.getLogger(__name__)

STATUSES = ["completed", "pending", "cancelled", "refunded"]
STATUS_WEIGHTS = [0.80, 0.08, 0.07, 0.05]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "paypal", "wallet"]
DISCOUNTS = [0, 0, 0, 5, 10, 15, 20]

Record = dict[str, Any]


# --------------------------------------------------------------------------- #
# API extraction
# --------------------------------------------------------------------------- #
def build_http_session() -> requests.Session:
    """HTTP session with exponential-backoff retries for transient failures."""
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_collection(
    session: requests.Session, base_url: str, endpoint: str, timeout: int = 30
) -> list[Record]:
    """Fetch a full DummyJSON collection (``limit=0`` returns every record)."""
    response = session.get(f"{base_url}/{endpoint}", params={"limit": 0}, timeout=timeout)
    response.raise_for_status()
    records = response.json()[endpoint]
    logger.info("Fetched %d %s from API", len(records), endpoint)
    return records


# --------------------------------------------------------------------------- #
# Synthetic transaction generator
# --------------------------------------------------------------------------- #
def _messy_case(value: str, rng: random.Random) -> str:
    """Randomly corrupt casing/whitespace (5% of rows) to simulate dirty data."""
    roll = rng.random()
    if roll < 0.02:
        return value.upper()
    if roll < 0.04:
        return f"  {value} "
    if roll < 0.05:
        return value.title()
    return value


def generate_transactions(
    products: list[Record],
    users: list[Record],
    ds: str,
    n_rows: int,
    lookback_days: int,
    ingested_at: str,
    duplicate_rate: float = 0.02,
) -> list[Record]:
    """Create deterministic raw transactions for the batch date ``ds``.

    Deliberate defects: ~2% duplicate transaction IDs (re-delivered, later
    ``ingested_at``), ~0.5% null customer, ~0.2% negative quantity, and messy
    status casing / whitespace.
    """
    rng = random.Random(f"{ds}:{n_rows}")
    window_end = datetime.fromisoformat(ds).replace(tzinfo=UTC) + timedelta(days=1)
    window_seconds = max(lookback_days, 1) * 86_400

    rows: list[Record] = []
    for i in range(n_rows):
        product = rng.choice(products)
        user = rng.choice(users)
        event_time = window_end - timedelta(seconds=rng.randrange(window_seconds))
        status = rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
        digest = hashlib.sha1(f"{ds}-{i}".encode()).hexdigest()[:12].upper()

        rows.append(
            {
                "transaction_id": f"TXN-{digest}",
                "user_id": None if rng.random() < 0.005 else user["id"],
                "product_id": product["id"],
                "quantity": -1 if rng.random() < 0.002 else rng.randint(1, 5),
                "unit_price": product["price"],
                "discount_pct": rng.choice(DISCOUNTS),
                "status": _messy_case(status, rng),
                "payment_method": rng.choice(PAYMENT_METHODS),
                "transaction_ts": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ingested_at": ingested_at,
            }
        )

    # Re-delivered duplicates: same business key, later ingestion timestamp.
    redelivery_time = (
        datetime.fromisoformat(ingested_at.replace("Z", "+00:00")) + timedelta(minutes=5)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    n_duplicates = int(n_rows * duplicate_rate)
    for original in rng.sample(rows, k=min(n_duplicates, len(rows))):
        rows.append({**original, "ingested_at": redelivery_time})

    logger.info("Generated %d raw transactions (incl. duplicates)", len(rows))
    return rows


# --------------------------------------------------------------------------- #
# S3 helpers
# --------------------------------------------------------------------------- #
def build_s3_client(settings: Settings):
    """boto3 S3 client pointed at LocalStack (or real S3 if endpoint is unset)."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
    )


def ensure_bucket(s3_client, bucket: str) -> None:
    """Create the bucket if it does not exist (idempotent)."""
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        logger.info("Creating bucket %s", bucket)
        s3_client.create_bucket(Bucket=bucket)


def write_jsonl(s3_client, bucket: str, key: str, records: list[Record]) -> None:
    """Upload records as newline-delimited JSON (overwrites the key => idempotent)."""
    body = "\n".join(json.dumps(r, separators=(",", ":")) for r in records).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    logger.info("Wrote s3://%s/%s (%d records, %d bytes)", bucket, key, len(records), len(body))


# --------------------------------------------------------------------------- #
# Orchestration entrypoint
# --------------------------------------------------------------------------- #
def run_ingestion(ds: str | None = None, settings: Settings | None = None) -> dict[str, int]:
    """Ingest one batch into Bronze and return record counts per entity."""
    settings = settings or get_settings()
    now = datetime.now(UTC)
    ds = ds or now.strftime("%Y-%m-%d")
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    session = build_http_session()
    products = fetch_collection(session, settings.api_base_url, "products")
    users = fetch_collection(session, settings.api_base_url, "users")
    for record in (*products, *users):
        record["ingested_at"] = ingested_at

    transactions = generate_transactions(
        products, users, ds, settings.synthetic_rows, settings.lookback_days, ingested_at
    )

    s3_client = build_s3_client(settings)
    ensure_bucket(s3_client, settings.bucket)

    payloads = {"products": products, "users": users, "transactions": transactions}
    for entity, records in payloads.items():
        key = f"{bronze_prefix(entity, ds)}/{entity}.jsonl"
        write_jsonl(s3_client, settings.bucket, key, records)

    return {entity: len(records) for entity, records in payloads.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw data into the Bronze layer")
    parser.add_argument("--ds", help="Batch date (YYYY-MM-DD); defaults to today (UTC)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_ingestion(args.ds)))


if __name__ == "__main__":
    main()
