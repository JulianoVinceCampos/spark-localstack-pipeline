"""
Ingestion Job — Bronze Layer
Reads raw CSV files from S3, validates schema, adds metadata columns,
and writes Parquet to the Bronze layer.
"""
from datetime import datetime

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Raw CSV schema ─────────────────────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("order_id",        StringType(),    nullable=False),
    StructField("customer_id",     StringType(),    nullable=False),
    StructField("customer_name",   StringType(),    nullable=True),
    StructField("customer_email",  StringType(),    nullable=True),
    StructField("region",          StringType(),    nullable=True),
    StructField("city",            StringType(),    nullable=True),
    StructField("state",           StringType(),    nullable=True),
    StructField("product_id",      StringType(),    nullable=False),
    StructField("product_name",    StringType(),    nullable=True),
    StructField("category",        StringType(),    nullable=True),
    StructField("quantity",        IntegerType(),   nullable=True),
    StructField("unit_price",      DoubleType(),    nullable=True),
    StructField("discount_pct",    DoubleType(),    nullable=True),
    StructField("discount_amount", DoubleType(),    nullable=True),
    StructField("gross_revenue",   DoubleType(),    nullable=True),
    StructField("net_revenue",     DoubleType(),    nullable=True),
    StructField("payment_method",  StringType(),    nullable=True),
    StructField("status",          StringType(),    nullable=True),
    StructField("order_date",      StringType(),    nullable=True),
    StructField("order_year",      IntegerType(),   nullable=True),
    StructField("order_month",     IntegerType(),   nullable=True),
    StructField("order_day",       IntegerType(),   nullable=True),
    StructField("created_at",      StringType(),    nullable=True),
])

REQUIRED_COLUMNS = ["order_id", "customer_id", "product_id"]


def read_raw_csv(spark: SparkSession, source_path: str) -> DataFrame:
    """Read raw CSV files from S3 with schema enforcement."""
    logger.info(f"Reading raw CSV from: {source_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_SCHEMA)
        .csv(source_path)
    )
    count = df.count()
    logger.info(f"Read {count:,} raw records")
    return df


def validate_schema(df: DataFrame) -> DataFrame:
    """
    Validate required columns and separate corrupt records.
    Returns a DataFrame with only valid records and a `_is_valid` flag.
    """
    logger.info("Validating schema and required fields...")

    # Flag records missing required keys
    valid_condition = F.lit(True)
    for col in REQUIRED_COLUMNS:
        valid_condition = valid_condition & F.col(col).isNotNull()

    df = df.withColumn("_is_valid", valid_condition)

    invalid_count = df.filter(~F.col("_is_valid")).count()
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count:,} invalid records (will be quarantined)")

    return df.filter(F.col("_is_valid")).drop("_is_valid")


def add_ingestion_metadata(df: DataFrame, source_path: str) -> DataFrame:
    """Enrich records with pipeline metadata columns."""
    ingestion_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return (
        df
        .withColumn("_ingested_at",    F.lit(ingestion_ts).cast(TimestampType()))
        .withColumn("_source_path",    F.lit(source_path))
        .withColumn("_pipeline_run_id", F.lit(ingestion_ts.replace(" ", "T").replace(":", "")))
    )


def write_bronze(df: DataFrame, bronze_path: str) -> int:
    """Write validated data as partitioned Parquet to bronze layer."""
    logger.info(f"Writing Bronze Parquet to: {bronze_path}")
    (
        df.write
        .mode("append")
        .partitionBy("order_year", "order_month")
        .parquet(bronze_path)
    )
    count = df.count()
    logger.success(f"Bronze layer written: {count:,} records → {bronze_path}")
    return count


def run_ingestion(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
) -> int:
    """End-to-end ingestion: CSV → validated → Bronze Parquet."""
    logger.info("=== INGESTION JOB START ===")
    df_raw = read_raw_csv(spark, source_path)
    df_valid = validate_schema(df_raw)
    df_enriched = add_ingestion_metadata(df_valid, source_path)
    count = write_bronze(df_enriched, bronze_path)
    logger.success(f"=== INGESTION JOB DONE — {count:,} records ===")
    return count
