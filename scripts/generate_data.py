#!/usr/bin/env python3
"""
Generate synthetic e-commerce sales data and upload to S3 Bronze layer.
Usage:
    python scripts/generate_data.py --rows 10000 --upload
"""
import csv
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import click
from faker import Faker
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

fake = Faker("pt_BR")

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Food & Beverages"]
STATUSES = ["completed", "pending", "cancelled", "refunded"]
PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "boleto", "paypal"]
REGIONS = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]


def generate_order(order_id: int) -> dict:
    """Generate a single synthetic order record."""
    order_date = fake.date_time_between(start_date="-1y", end_date="now")
    category = random.choice(CATEGORIES)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(9.99, 999.99), 2)
    discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])

    gross_revenue = round(quantity * unit_price, 2)
    discount_amount = round(gross_revenue * discount_pct / 100, 2)
    net_revenue = round(gross_revenue - discount_amount, 2)

    return {
        "order_id": f"ORD-{order_id:08d}",
        "customer_id": f"CUST-{fake.numerify(text='######')}",
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "region": random.choice(REGIONS),
        "city": fake.city(),
        "state": fake.estado_sigla(),
        "product_id": f"PROD-{fake.numerify(text='####')}",
        "product_name": fake.catch_phrase(),
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": discount_pct,
        "discount_amount": discount_amount,
        "gross_revenue": gross_revenue,
        "net_revenue": net_revenue,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
        "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "order_year": order_date.year,
        "order_month": order_date.month,
        "order_day": order_date.day,
        "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


def write_csv(rows: int, output_path: Path) -> Path:
    """Write generated data to CSV file."""
    logger.info(f"Generating {rows:,} synthetic orders...")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records = [generate_order(i + 1) for i in range(rows)]
    fieldnames = list(records[0].keys())

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    size_kb = output_path.stat().st_size / 1024
    logger.success(f"Written {rows:,} rows to {output_path} ({size_kb:.1f} KB)")
    return output_path


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def upload_to_s3(local_path: Path, bucket: str, s3_key: str, endpoint_url: str) -> None:
    """Upload file to S3 (LocalStack)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    logger.info(f"Uploading {local_path.name} → s3://{bucket}/{s3_key}")
    s3.upload_file(str(local_path), bucket, s3_key)
    logger.success(f"Upload complete: s3://{bucket}/{s3_key}")


@click.command()
@click.option("--rows", default=50000, show_default=True, help="Number of orders to generate")
@click.option("--output", default="data/sample/sales_raw.csv", show_default=True, help="Local output path")
@click.option("--upload/--no-upload", default=True, show_default=True, help="Upload to S3 bronze layer")
@click.option("--bucket", default="pipeline-data", show_default=True, help="S3 bucket name")
@click.option("--endpoint", default="http://localhost:4566", show_default=True, help="S3 endpoint URL")
def main(rows: int, output: str, upload: bool, bucket: str, endpoint: str):
    """Generate synthetic e-commerce data and load into the Bronze layer."""
    output_path = Path(output)
    csv_path = write_csv(rows, output_path)

    if upload:
        today = datetime.utcnow().strftime("%Y/%m/%d")
        s3_key = f"bronze/sales/{today}/sales_raw.csv"
        upload_to_s3(csv_path, bucket, s3_key, endpoint)


if __name__ == "__main__":
    main()
