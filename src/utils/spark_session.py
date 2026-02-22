"""
Spark session factory — pre-configured for LocalStack S3A access.
"""

import os

from loguru import logger
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "DataPipeline",
    master: str | None = None,
    log_level: str = "WARN",
) -> SparkSession:
    """
    Create (or reuse) a SparkSession configured for LocalStack S3A.

    Args:
        app_name:  Spark application name shown in the UI.
        master:    Spark master URL. Falls back to env var SPARK_MASTER_URL
                   or local[*].
        log_level: Spark log level (DEBUG, INFO, WARN, ERROR).

    Returns:
        A fully configured SparkSession.
    """
    aws_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    master_url = master or os.getenv("SPARK_MASTER_URL", "local[*]")

    logger.info(f"Creating SparkSession: app={app_name!r}, master={master_url!r}")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        # ── S3A / LocalStack ──────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", aws_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Performance ───────────────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # ── Shuffle ───────────────────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(log_level)
    logger.success(f"SparkSession ready — Spark {spark.version}")
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    spark.stop()
    logger.info("SparkSession stopped.")
