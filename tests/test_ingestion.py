"""
Unit tests for the ingestion job (Bronze layer).
"""
from pyspark.sql import DataFrame, functions as F

from src.jobs.ingestion import add_ingestion_metadata, validate_schema


class TestValidateSchema:
    def test_removes_records_with_null_order_id(self, sample_orders_df):
        result = validate_schema(sample_orders_df)
        assert result.filter(F.col("order_id").isNull()).count() == 0

    def test_valid_records_are_kept(self, sample_orders_df):
        result = validate_schema(sample_orders_df)
        assert result.count() >= 5

    def test_returns_dataframe(self, sample_orders_df):
        result = validate_schema(sample_orders_df)
        assert isinstance(result, DataFrame)


class TestAddIngestionMetadata:
    def test_adds_ingested_at_column(self, sample_orders_df):
        df_valid = validate_schema(sample_orders_df)
        result = add_ingestion_metadata(df_valid, "s3a://test/bronze")
        assert "_ingested_at" in result.columns

    def test_adds_source_path_column(self, sample_orders_df):
        source = "s3a://test/bronze/sales"
        df_valid = validate_schema(sample_orders_df)
        result = add_ingestion_metadata(df_valid, source)
        assert "_source_path" in result.columns
        assert result.select("_source_path").first()[0] == source

    def test_adds_pipeline_run_id(self, sample_orders_df):
        df_valid = validate_schema(sample_orders_df)
        result = add_ingestion_metadata(df_valid, "s3a://test/bronze")
        assert "_pipeline_run_id" in result.columns
        assert result.select("_pipeline_run_id").first()[0] is not None

    def test_record_count_unchanged(self, sample_orders_df):
        df_valid = validate_schema(sample_orders_df)
        original_count = df_valid.count()
        result = add_ingestion_metadata(df_valid, "s3a://test/bronze")
        assert result.count() == original_count
