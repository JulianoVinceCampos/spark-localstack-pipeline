"""
Unit tests for the load job (Gold layer — data marts).
"""
import pytest
from pyspark.sql import functions as F

from src.jobs.ingestion import add_ingestion_metadata, validate_schema
from src.jobs.load import (
    build_customer_segments,
    build_product_performance,
    build_sales_by_region,
)
from src.jobs.transformation import (
    add_silver_columns,
    clean_and_standardize,
    deduplicate,
)


@pytest.fixture()
def silver_df(sample_orders_df):
    """Full pipeline up to Silver, filtering completed orders."""
    df = validate_schema(sample_orders_df)
    df = add_ingestion_metadata(df, "s3a://test/bronze")
    df = deduplicate(df)
    df = clean_and_standardize(df)
    df = add_silver_columns(df)
    return df.filter(F.col("status") == "completed")


class TestSalesByRegion:
    def test_has_expected_columns(self, silver_df):
        mart = build_sales_by_region(silver_df)
        required = {"region", "order_year", "order_month", "total_orders",
                    "total_revenue", "avg_order_value", "unique_customers"}
        assert required.issubset(set(mart.columns))

    def test_total_revenue_positive(self, silver_df):
        mart = build_sales_by_region(silver_df)
        negatives = mart.filter(F.col("total_revenue") < 0).count()
        assert negatives == 0

    def test_revenue_rank_starts_at_one(self, silver_df):
        mart = build_sales_by_region(silver_df)
        min_rank = mart.agg(F.min("revenue_rank")).collect()[0][0]
        assert min_rank == 1

    def test_one_row_per_region_month(self, silver_df):
        mart = build_sales_by_region(silver_df)
        total = mart.count()
        distinct = mart.select("region", "order_year", "order_month").distinct().count()
        assert total == distinct


class TestProductPerformance:
    def test_has_expected_columns(self, silver_df):
        mart = build_product_performance(silver_df)
        required = {"product_id", "product_name", "category", "total_revenue",
                    "total_units", "total_orders", "rank_in_category"}
        assert required.issubset(set(mart.columns))

    def test_rank_in_category_positive(self, silver_df):
        mart = build_product_performance(silver_df)
        bad_rank = mart.filter(F.col("rank_in_category") < 1).count()
        assert bad_rank == 0

    def test_one_row_per_product(self, silver_df):
        mart = build_product_performance(silver_df)
        assert mart.count() == mart.select("product_id").distinct().count()


class TestCustomerSegments:
    def test_has_expected_columns(self, silver_df):
        mart = build_customer_segments(silver_df)
        required = {"customer_id", "frequency", "monetary", "recency_days",
                    "r_score", "f_score", "m_score", "rfm_score", "segment"}
        assert required.issubset(set(mart.columns))

    def test_segment_values_valid(self, silver_df):
        mart = build_customer_segments(silver_df)
        valid_segs = {"Champions", "Loyal Customers", "Potential Loyalists", "At Risk", "Lost"}
        segments = {r.segment for r in mart.select("segment").collect() if r.segment}
        assert segments.issubset(valid_segs)

    def test_frequency_at_least_one(self, silver_df):
        mart = build_customer_segments(silver_df)
        bad = mart.filter(F.col("frequency") < 1).count()
        assert bad == 0

    def test_monetary_non_negative(self, silver_df):
        mart = build_customer_segments(silver_df)
        neg = mart.filter(F.col("monetary") < 0).count()
        assert neg == 0
