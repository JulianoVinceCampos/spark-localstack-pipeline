"""
Transformation Job — Silver Layer
Reads Bronze Parquet, applies business rules, deduplicates, cleans data,
and writes enriched Parquet to the Silver layer.
"""

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_STATUSES = {"completed", "pending", "cancelled", "refunded"}
VALID_CATEGORIES = {
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports",
    "Food & Beverages",
}


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Read Bronze Parquet files."""
    logger.info(f"Reading Bronze layer from: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    logger.info(f"Bronze records loaded: {df.count():,}")
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate orders keeping the most recently ingested record.
    Uses a window function on order_id ordered by _ingested_at desc.
    """
    logger.info("Deduplicating by order_id...")
    window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    df_deduped = (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    removed = df.count() - df_deduped.count()
    logger.info(f"Removed {removed:,} duplicate records")
    return df_deduped


def clean_and_standardize(df: DataFrame) -> DataFrame:
    """
    Apply business cleaning rules:
    - Standardize status / category casing
    - Cap quantities at a business maximum
    - Nullify clearly invalid prices
    - Parse order_date string to proper timestamp
    - Trim string fields
    """
    logger.info("Cleaning and standardizing columns...")
    return (
        df
        # String normalization
        .withColumn("status", F.trim(F.lower(F.col("status"))))
        .withColumn("category", F.trim(F.initcap(F.col("category"))))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("payment_method", F.trim(F.lower(F.col("payment_method"))))
        .withColumn("customer_email", F.trim(F.lower(F.col("customer_email"))))
        # Invalid status → null
        .withColumn(
            "status",
            F.when(F.col("status").isin(list(VALID_STATUSES)), F.col("status")).otherwise(
                F.lit(None)
            ),
        )
        # Negative prices → null
        .withColumn(
            "unit_price",
            F.when(F.col("unit_price") > 0, F.col("unit_price")).otherwise(F.lit(None)),
        )
        # Clamp quantity [1, 100]
        .withColumn(
            "quantity",
            F.when(F.col("quantity").between(1, 100), F.col("quantity")).otherwise(F.lit(None)),
        )
        # Parse timestamps
        .withColumn("order_date", F.to_timestamp(F.col("order_date"), "yyyy-MM-dd HH:mm:ss"))
        # Recalculate derived financials to ensure consistency
        .withColumn("gross_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn(
            "discount_amount", F.round(F.col("gross_revenue") * F.col("discount_pct") / 100, 2)
        )
        .withColumn("net_revenue", F.round(F.col("gross_revenue") - F.col("discount_amount"), 2))
    )


def add_silver_columns(df: DataFrame) -> DataFrame:
    """Add derived / enrichment columns for the Silver layer."""
    return (
        df.withColumn(
            "revenue_bucket",
            F.when(F.col("net_revenue") < 100, F.lit("low"))
            .when(F.col("net_revenue") < 500, F.lit("medium"))
            .when(F.col("net_revenue") < 2000, F.lit("high"))
            .otherwise(F.lit("premium")),
        )
        .withColumn("is_discounted", F.col("discount_pct") > 0)
        .withColumn("order_quarter", F.quarter(F.col("order_date")))
        .withColumn("order_week", F.weekofyear(F.col("order_date")))
        .withColumn("_transformed_at", F.current_timestamp())
    )


def write_silver(df: DataFrame, silver_path: str) -> int:
    """Write Silver Parquet, partitioned by year/month/category."""
    logger.info(f"Writing Silver Parquet to: {silver_path}")
    df.write.mode("overwrite").partitionBy("order_year", "order_month", "category").parquet(
        silver_path
    )
    count = df.count()
    logger.success(f"Silver layer written: {count:,} records → {silver_path}")
    return count


def run_transformation(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> int:
    """End-to-end transformation: Bronze → Silver."""
    logger.info("=== TRANSFORMATION JOB START ===")
    df_bronze = read_bronze(spark, bronze_path)
    df_deduped = deduplicate(df_bronze)
    df_clean = clean_and_standardize(df_deduped)
    df_silver = add_silver_columns(df_clean)
    count = write_silver(df_silver, silver_path)
    logger.success(f"=== TRANSFORMATION JOB DONE — {count:,} records ===")
    return count
