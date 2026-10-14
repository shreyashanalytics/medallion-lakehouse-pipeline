"""PySpark job: Bronze (raw JSONL) -> Silver (clean, deduplicated Parquet).

Per entity it will:
  1. read raw JSON with an *explicit* schema (no inference surprises),
  2. standardise names / types / casing,
  3. quarantine invalid rows instead of silently dropping them,
  4. deduplicate on the business key (latest ``ingested_at`` wins),
  5. write Parquet to S3 (transactions partitioned by ``transaction_date``).

The job rebuilds Silver from all of Bronze, so re-runs are idempotent.

Run:  ``python -m lakehouse.bronze_to_silver``
"""

from __future__ import annotations

import json
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import Settings, get_settings

logger = logging.getLogger(__name__)

TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("user_id", T.LongType()),
        T.StructField("product_id", T.LongType()),
        T.StructField("quantity", T.LongType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("discount_pct", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("payment_method", T.StringType()),
        T.StructField("transaction_ts", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
    ]
)

USER_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("firstName", T.StringType()),
        T.StructField("lastName", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("gender", T.StringType()),
        T.StructField("age", T.LongType()),
        T.StructField("phone", T.StringType()),
        T.StructField(
            "address",
            T.StructType(
                [
                    T.StructField("city", T.StringType()),
                    T.StructField("state", T.StringType()),
                    T.StructField("country", T.StringType()),
                ]
            ),
        ),
        T.StructField("ingested_at", T.StringType()),
    ]
)

PRODUCT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("title", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("rating", T.DoubleType()),
        T.StructField("stock", T.LongType()),
        T.StructField("sku", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
    ]
)


def build_spark_session(settings: Settings) -> SparkSession:
    """Local-mode Spark session configured for S3A against LocalStack."""
    return (
        SparkSession.builder.appName("bronze_to_silver")
        .master("local[*]")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def _latest_per_key(df: DataFrame, key: str) -> DataFrame:
    """Keep one row per ``key``: the one with the latest ``ingested_at``."""
    window = Window.partitionBy(key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


# --------------------------------------------------------------------------- #
# Entity processors
# --------------------------------------------------------------------------- #
def process_transactions(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Clean, validate, deduplicate and partition transactions."""
    root = settings.s3a_root
    raw = spark.read.schema(TRANSACTION_SCHEMA).json(f"{root}/bronze/transactions/")

    cleaned = raw.select(
        F.upper(F.trim("transaction_id")).alias("transaction_id"),
        F.col("user_id").alias("customer_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.coalesce(F.col("discount_pct"), F.lit(0.0)).alias("discount_pct"),
        F.lower(F.trim("status")).alias("status"),
        F.lower(F.trim("payment_method")).alias("payment_method"),
        F.to_timestamp("transaction_ts").alias("transaction_ts"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
    )

    reject_reason = (
        F.when(
            F.col("transaction_id").isNull() | (F.col("transaction_id") == ""),
            "missing_transaction_id",
        )
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("transaction_ts").isNull(), "invalid_timestamp")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "invalid_price")
    )
    flagged = cleaned.withColumn("reject_reason", reject_reason).cache()

    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    silver = (
        _latest_per_key(valid, "transaction_id")
        .withColumn(
            "line_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2),
        )
        .withColumn("transaction_date", F.to_date("transaction_ts"))
    )

    rejected.write.mode("overwrite").parquet(f"{root}/quarantine/transactions/")
    silver.write.mode("overwrite").partitionBy("transaction_date").parquet(
        f"{root}/silver/transactions/"
    )

    metrics = {
        "bronze_rows": flagged.count(),
        "quarantined_rows": rejected.count(),
        "silver_rows": spark.read.parquet(f"{root}/silver/transactions/").count(),
    }
    flagged.unpersist()
    return metrics


def process_customers(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Flatten and deduplicate customers (from the raw ``users`` feed)."""
    root = settings.s3a_root
    raw = spark.read.schema(USER_SCHEMA).json(f"{root}/bronze/users/")
    customers = raw.select(
        F.col("id").alias("customer_id"),
        F.initcap(F.trim("firstName")).alias("first_name"),
        F.initcap(F.trim("lastName")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.lower(F.trim("gender")).alias("gender"),
        F.col("age"),
        F.col("phone"),
        F.col("address.city").alias("city"),
        F.col("address.state").alias("state"),
        F.col("address.country").alias("country"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
    ).filter(F.col("customer_id").isNotNull())

    out = _latest_per_key(customers, "customer_id")
    out.coalesce(1).write.mode("overwrite").parquet(f"{root}/silver/customers/")
    return {"bronze_rows": raw.count(), "silver_rows": out.count()}


def process_products(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Standardise and deduplicate products."""
    root = settings.s3a_root
    raw = spark.read.schema(PRODUCT_SCHEMA).json(f"{root}/bronze/products/")
    products = raw.select(
        F.col("id").alias("product_id"),
        F.trim("title").alias("title"),
        F.lower(F.trim("category")).alias("category"),
        F.initcap(F.coalesce(F.trim("brand"), F.lit("unknown"))).alias("brand"),
        F.col("price"),
        F.col("rating"),
        F.col("stock"),
        F.col("sku"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
    ).filter(F.col("product_id").isNotNull())

    out = _latest_per_key(products, "product_id")
    out.coalesce(1).write.mode("overwrite").parquet(f"{root}/silver/products/")
    return {"bronze_rows": raw.count(), "silver_rows": out.count()}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    spark = build_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = {
            "transactions": process_transactions(spark, settings),
            "customers": process_customers(spark, settings),
            "products": process_products(spark, settings),
        }
        logger.info("Bronze -> Silver complete: %s", json.dumps(report))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
