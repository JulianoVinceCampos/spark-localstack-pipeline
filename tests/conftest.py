"""
Test configuration and shared fixtures.
Uses moto to mock AWS S3 and a local SparkSession (no cluster needed).
"""
import os

import boto3
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession

# ── Point tests at moto (no real LocalStack needed) ───────────────────────────
os.environ.setdefault("AWS_ACCESS_KEY_ID",      "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY",  "test")
os.environ.setdefault("AWS_DEFAULT_REGION",     "us-east-1")
os.environ.setdefault("AWS_ENDPOINT_URL",       "http://localhost:4566")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped local SparkSession for unit tests."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pipeline")
        .config("spark.sql.shuffle.partitions",           "2")
        .config("spark.default.parallelism",              "2")
        .config("spark.sql.adaptive.enabled",             "false")
        .config("spark.ui.enabled",                       "false")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture()
def aws_credentials():
    """Mocked AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"]     = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"]    = "us-east-1"


@pytest.fixture()
def s3_bucket(aws_credentials):
    """Create a real in-memory S3 bucket via moto."""
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-pipeline-data")
        yield conn


# ─── Sample DataFrames ─────────────────────────────────────────────────────────

@pytest.fixture()
def sample_orders_df(spark):
    """10-row synthetic orders DataFrame for unit tests."""
    data = [
        ("ORD-00000001", "CUST-001", "Alice",  "alice@email.com", "Sudeste", "SP", "SP",
         "PROD-001", "Laptop Pro",    "Electronics", 2, 3999.99, 10.0, 800.0, 7999.98, 7199.98,
         "credit_card", "completed",  "2024-03-15 10:00:00", 2024, 3, 15, "2024-03-15 10:00:00"),
        ("ORD-00000002", "CUST-002", "Bob",    "bob@email.com",   "Sul",    "RS", "RS",
         "PROD-002", "Running Shoes", "Sports",      1,  299.99,  0.0,   0.0,  299.99,  299.99,
         "pix",         "completed",  "2024-04-20 14:30:00", 2024, 4, 20, "2024-04-20 14:30:00"),
        ("ORD-00000003", "CUST-001", "Alice",  "alice@email.com", "Sudeste", "SP", "SP",
         "PROD-003", "Python Book",  "Books",        3,   89.90,  5.0,  13.49,  269.70,  256.21,
         "boleto",      "completed",  "2024-05-01 09:00:00", 2024, 5,  1, "2024-05-01 09:00:00"),
        ("ORD-00000004", "CUST-003", "Carol",  "carol@email.com", "Norte",  "AM", "AM",
         "PROD-001", "Laptop Pro",   "Electronics", 1, 3999.99, 15.0, 599.99, 3999.99, 3399.99,
         "credit_card", "cancelled",  "2024-05-10 16:45:00", 2024, 5, 10, "2024-05-10 16:45:00"),
        ("ORD-00000005", "CUST-004", "Dave",   "dave@email.com",  "Nordeste","BA", "BA",
         "PROD-004", "Coffee Maker", "Home & Garden",2,  499.00,  0.0,   0.0,  998.00,  998.00,
         "debit_card",  "pending",    "2024-06-05 11:00:00", 2024, 6,  5, "2024-06-05 11:00:00"),
        ("ORD-00000005", "CUST-004", "Dave",   "dave@email.com",  "Nordeste","BA", "BA",
         "PROD-004", "Coffee Maker", "Home & Garden",2,  499.00,  0.0,   0.0,  998.00,  998.00,
         "debit_card",  "pending",    "2024-06-05 11:00:00", 2024, 6,  5, "2024-06-05 11:05:00"),  # duplicate
        (None, "CUST-005", "Eve", "eve@email.com", "Sul", "PR", "PR",
         "PROD-005", "Yoga Mat",     "Sports",       1,   79.90,  0.0,   0.0,   79.90,   79.90,
         "pix",         "completed",  "2024-06-10 08:00:00", 2024, 6, 10, "2024-06-10 08:00:00"),  # null order_id
    ]
    columns = [
        "order_id", "customer_id", "customer_name", "customer_email",
        "region", "city", "state", "product_id", "product_name", "category",
        "quantity", "unit_price", "discount_pct", "discount_amount",
        "gross_revenue", "net_revenue", "payment_method", "status",
        "order_date", "order_year", "order_month", "order_day", "created_at",
    ]
    return spark.createDataFrame(data, columns)
