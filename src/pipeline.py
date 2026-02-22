"""
Pipeline Orchestrator
Runs the full ETL pipeline: Ingestion → Transformation → Load

Usage:
    python -m src.pipeline [--steps ingestion,transformation,load]
    python -m src.pipeline --steps ingestion
    python -m src.pipeline --steps transformation,load
"""
import os
import sys
import time

import click
from loguru import logger

from src.jobs.ingestion import run_ingestion
from src.jobs.load import run_load
from src.jobs.transformation import run_transformation
from src.utils.aws_helpers import ensure_bucket
from src.utils.spark_session import get_spark_session, stop_spark

# ── Defaults from environment ──────────────────────────────────────────────────
BUCKET       = os.getenv("S3_BUCKET",    "pipeline-data")
BRONZE_PATH  = os.getenv("BRONZE_PATH",  f"s3a://{BUCKET}/bronze/sales")
SILVER_PATH  = os.getenv("SILVER_PATH",  f"s3a://{BUCKET}/silver/sales")
GOLD_PATH    = os.getenv("GOLD_PATH",    f"s3a://{BUCKET}/gold")
SOURCE_PATH  = os.getenv("SOURCE_PATH",  f"s3a://{BUCKET}/bronze/sales/*/*/sales_raw.csv")

ALL_STEPS = ["ingestion", "transformation", "load"]


def setup_infrastructure() -> None:
    """Ensure S3 buckets and prefixes exist before running the pipeline."""
    logger.info("Setting up AWS infrastructure...")
    ensure_bucket(BUCKET)
    ensure_bucket("pipeline-logs")
    logger.success("Infrastructure ready.")


def run_pipeline(steps: list[str]) -> None:
    """Execute the requested pipeline steps in order."""
    logger.info(f"Pipeline starting. Steps: {steps}")
    start_time = time.time()

    setup_infrastructure()

    spark = get_spark_session(
        app_name="LocalStackSparkPipeline",
        master=os.getenv("SPARK_MASTER_URL", "local[*]"),
    )

    results: dict = {}

    try:
        if "ingestion" in steps:
            t0 = time.time()
            count = run_ingestion(spark, SOURCE_PATH, BRONZE_PATH)
            results["ingestion"] = {"records": count, "elapsed_s": round(time.time() - t0, 2)}

        if "transformation" in steps:
            t0 = time.time()
            count = run_transformation(spark, BRONZE_PATH, SILVER_PATH)
            results["transformation"] = {"records": count, "elapsed_s": round(time.time() - t0, 2)}

        if "load" in steps:
            t0 = time.time()
            mart_counts = run_load(spark, SILVER_PATH, GOLD_PATH)
            results["load"] = {"marts": mart_counts, "elapsed_s": round(time.time() - t0, 2)}

    finally:
        stop_spark(spark)

    elapsed = round(time.time() - start_time, 2)
    logger.success("=" * 60)
    logger.success(f"  PIPELINE COMPLETE in {elapsed}s")
    for step, info in results.items():
        logger.success(f"  {step.upper()}: {info}")
    logger.success("=" * 60)


@click.command()
@click.option(
    "--steps",
    default="ingestion,transformation,load",
    show_default=True,
    help="Comma-separated list of pipeline steps to run.",
)
def main(steps: str):
    """End-to-end data pipeline: CSV → Bronze → Silver → Gold (LocalStack S3)."""
    step_list = [s.strip() for s in steps.split(",")]
    invalid = [s for s in step_list if s not in ALL_STEPS]
    if invalid:
        logger.error(f"Unknown steps: {invalid}. Valid: {ALL_STEPS}")
        sys.exit(1)

    run_pipeline(step_list)


if __name__ == "__main__":
    main()
