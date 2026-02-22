"""
AWS / LocalStack helper utilities.
"""
import os

import boto3
from botocore.exceptions import ClientError
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


def get_s3_client(endpoint_url: str | None = None):
    """Return a boto3 S3 client pointing at LocalStack (or real AWS)."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def bucket_exists(bucket: str, endpoint_url: str | None = None) -> bool:
    """Check whether an S3 bucket exists."""
    s3 = get_s3_client(endpoint_url)
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=10))
def ensure_bucket(bucket: str, region: str = "us-east-1", endpoint_url: str | None = None) -> None:
    """Create an S3 bucket if it does not already exist."""
    s3 = get_s3_client(endpoint_url)
    if not bucket_exists(bucket, endpoint_url):
        logger.info(f"Creating bucket: {bucket}")
        s3.create_bucket(Bucket=bucket)
        logger.success(f"Bucket created: s3://{bucket}")
    else:
        logger.debug(f"Bucket already exists: s3://{bucket}")


def list_objects(bucket: str, prefix: str = "", endpoint_url: str | None = None) -> list[str]:
    """List all object keys under a prefix."""
    s3 = get_s3_client(endpoint_url)
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def get_s3a_path(bucket: str, prefix: str) -> str:
    """Return an s3a:// URI suitable for Spark."""
    return f"s3a://{bucket}/{prefix.lstrip('/')}"
