"""
Load Job — Gold Layer  (Data Marts)
Reads Silver Parquet and produces three Gold aggregations:
  1. sales_by_region    – revenue KPIs by region & month
  2. product_performance – top products by revenue
  3. customer_segments  – RFM-style segmentation
"""

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def read_silver(spark: SparkSession, silver_path: str) -> DataFrame:
    """Read Silver Parquet (only completed orders for Gold marts)."""
    logger.info(f"Reading Silver layer from: {silver_path}")
    df = spark.read.parquet(silver_path).filter(F.col("status") == "completed")
    logger.info(f"Silver completed records: {df.count():,}")
    return df


# ── Mart 1: Sales by Region ────────────────────────────────────────────────────


def build_sales_by_region(df: DataFrame) -> DataFrame:
    """Monthly revenue KPIs per region."""
    logger.info("Building mart: sales_by_region")
    return (
        df.groupBy("region", "order_year", "order_month")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("net_revenue").alias("total_revenue"),
            F.avg("net_revenue").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_discounted"), F.col("net_revenue")).otherwise(0)).alias(
                "discounted_revenue"
            ),
        )
        .withColumn("avg_order_value", F.round("avg_order_value", 2))
        .withColumn(
            "revenue_rank",
            F.rank().over(
                Window.partitionBy("order_year", "order_month").orderBy(
                    F.col("total_revenue").desc()
                )
            ),
        )
        .withColumn("_created_at", F.current_timestamp())
        .orderBy("order_year", "order_month", "revenue_rank")
    )


# ── Mart 2: Product Performance ────────────────────────────────────────────────


def build_product_performance(df: DataFrame) -> DataFrame:
    """Top products by net revenue with category context."""
    logger.info("Building mart: product_performance")
    agg = df.groupBy("product_id", "product_name", "category").agg(
        F.sum("net_revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.count("order_id").alias("total_orders"),
        F.avg("unit_price").alias("avg_price"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.countDistinct("customer_id").alias("unique_buyers"),
    )

    window_cat = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())
    return (
        agg.withColumn("avg_price", F.round("avg_price", 2))
        .withColumn("avg_discount_pct", F.round("avg_discount_pct", 2))
        .withColumn("rank_in_category", F.rank().over(window_cat))
        .withColumn("_created_at", F.current_timestamp())
    )


# ── Mart 3: Customer Segments ──────────────────────────────────────────────────


def build_customer_segments(df: DataFrame) -> DataFrame:
    """
    Simplified RFM segmentation:
    - Recency  : days since last order
    - Frequency: number of orders
    - Monetary : total net revenue
    """
    logger.info("Building mart: customer_segments")

    latest_date = df.agg(F.max("order_date")).collect()[0][0]

    rfm = (
        df.groupBy("customer_id", "customer_name", "region")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.count("order_id").alias("frequency"),
            F.sum("net_revenue").alias("monetary"),
        )
        .withColumn("recency_days", F.datediff(F.lit(latest_date), F.col("last_order_date")))
    )

    tile = 5
    r_window = Window.orderBy(F.col("recency_days").asc())
    f_window = Window.orderBy(F.col("frequency").desc())
    m_window = Window.orderBy(F.col("monetary").desc())

    rfm = (
        rfm.withColumn("r_score", F.ntile(tile).over(r_window))
        .withColumn("f_score", F.ntile(tile).over(f_window))
        .withColumn("m_score", F.ntile(tile).over(m_window))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
        .withColumn(
            "segment",
            F.when(F.col("rfm_score") >= 13, "Champions")
            .when(F.col("rfm_score") >= 10, "Loyal Customers")
            .when(F.col("rfm_score") >= 7, "Potential Loyalists")
            .when(F.col("rfm_score") >= 5, "At Risk")
            .otherwise("Lost"),
        )
        .withColumn("monetary", F.round("monetary", 2))
        .withColumn("_created_at", F.current_timestamp())
    )

    rfm.groupBy("segment").count().orderBy("segment").show(truncate=False)
    return rfm


def write_gold_mart(df: DataFrame, gold_path: str, mart_name: str) -> int:
    """Write a Gold data mart as Parquet."""
    output = f"{gold_path}/{mart_name}"
    logger.info(f"Writing Gold mart '{mart_name}' → {output}")
    df.write.mode("overwrite").parquet(output)
    count = df.count()
    logger.success(f"Gold mart '{mart_name}': {count:,} records")
    return count


def run_load(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
) -> dict:
    """End-to-end load: Silver → Gold marts."""
    logger.info("=== LOAD JOB START ===")
    df_silver = read_silver(spark, silver_path)

    counts = {}
    counts["sales_by_region"] = write_gold_mart(
        build_sales_by_region(df_silver), gold_path, "sales_by_region"
    )
    counts["product_performance"] = write_gold_mart(
        build_product_performance(df_silver), gold_path, "product_performance"
    )
    counts["customer_segments"] = write_gold_mart(
        build_customer_segments(df_silver), gold_path, "customer_segments"
    )

    logger.success(f"=== LOAD JOB DONE === {counts}")
    return counts
