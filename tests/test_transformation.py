"""
Unit tests for the transformation job (Silver layer).
"""
import pytest
from pyspark.sql import functions as F

from src.jobs.ingestion import add_ingestion_metadata, validate_schema
from src.jobs.transformation import (
    add_silver_columns,
    clean_and_standardize,
    deduplicate,
)


@pytest.fixture()
def bronze_df(sample_orders_df):
    """Simulate a DataFrame that has already passed ingestion."""
    df_valid = validate_schema(sample_orders_df)
    return add_ingestion_metadata(df_valid, "s3a://test/bronze")


class TestDeduplicate:
    def test_removes_duplicates(self, bronze_df):
        original = bronze_df.count()
        deduped = deduplicate(bronze_df)
        # ORD-00000005 appears twice → one must be removed
        assert deduped.count() <= original

    def test_unique_order_ids(self, bronze_df):
        deduped = deduplicate(bronze_df)
        total = deduped.count()
        distinct = deduped.select("order_id").distinct().count()
        assert total == distinct

    def test_no_extra_records_removed(self, bronze_df):
        """Unique records should all survive."""
        deduped = deduplicate(bronze_df)
        # We had 1 genuine duplicate; result should have exactly (original - 1)
        assert deduped.count() == bronze_df.count() - 1


class TestCleanAndStandardize:
    def test_status_lowercased(self, bronze_df):
        deduped = deduplicate(bronze_df)
        cleaned = clean_and_standardize(deduped)
        statuses = [r.status for r in cleaned.select("status").collect() if r.status]
        assert all(s == s.lower() for s in statuses)

    def test_negative_price_nullified(self, spark, bronze_df):
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType
        # Inject a record with negative price
        bad_data = [("ORD-BAD", "CUST-X", "Test", "t@t.com", "Sul", "RS", "RS",
                     "PROD-X", "Item", "Books", 1, -50.0, 0.0, 0.0, -50.0, -50.0,
                     "pix", "completed", "2024-01-01 00:00:00", 2024, 1, 1, "2024-01-01")]
        cols = [f.name for f in bronze_df.schema.fields
                if f.name not in ("_ingested_at", "_source_path", "_pipeline_run_id")]
        bad_df = add_ingestion_metadata(
            spark.createDataFrame(bad_data, cols), "s3a://test/bronze"
        )
        combined = bronze_df.union(bad_df)
        cleaned = clean_and_standardize(combined)
        bad_row = cleaned.filter(F.col("order_id") == "ORD-BAD").first()
        assert bad_row["unit_price"] is None

    def test_email_lowercased(self, bronze_df):
        cleaned = clean_and_standardize(deduplicate(bronze_df))
        emails = [r.customer_email for r in cleaned.select("customer_email").collect() if r.customer_email]
        assert all(e == e.lower() for e in emails)

    def test_order_date_is_timestamp(self, bronze_df):
        cleaned = clean_and_standardize(deduplicate(bronze_df))
        dtype = dict(cleaned.dtypes)["order_date"]
        assert dtype == "timestamp"


class TestAddSilverColumns:
    def test_has_revenue_bucket(self, bronze_df):
        deduped = deduplicate(bronze_df)
        cleaned = clean_and_standardize(deduped)
        silver = add_silver_columns(cleaned)
        assert "revenue_bucket" in silver.columns

    def test_revenue_bucket_values(self, bronze_df):
        deduped = deduplicate(bronze_df)
        cleaned = clean_and_standardize(deduped)
        silver = add_silver_columns(cleaned)
        valid_buckets = {"low", "medium", "high", "premium"}
        buckets = {r.revenue_bucket for r in silver.select("revenue_bucket").collect()
                   if r.revenue_bucket}
        assert buckets.issubset(valid_buckets)

    def test_has_is_discounted(self, bronze_df):
        deduped = deduplicate(bronze_df)
        cleaned = clean_and_standardize(deduped)
        silver = add_silver_columns(cleaned)
        assert "is_discounted" in silver.columns

    def test_has_order_quarter(self, bronze_df):
        deduped = deduplicate(bronze_df)
        cleaned = clean_and_standardize(deduped)
        silver = add_silver_columns(cleaned)
        assert "order_quarter" in silver.columns

    def test_transformed_at_not_null(self, bronze_df):
        deduped = deduplicate(bronze_df)
        cleaned = clean_and_standardize(deduped)
        silver = add_silver_columns(cleaned)
        null_count = silver.filter(F.col("_transformed_at").isNull()).count()
        assert null_count == 0
