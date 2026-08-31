# Databricks notebook source
"""Run all Bronze ingestions sequentially in Databricks."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from pyspark.sql.types import StructType

try:
    from src.bronze.ingestion import ingest_bronze_entity
    from src.bronze.schemas import (
        CUSTOMERS_SCHEMA,
        ORDERS_SCHEMA,
        PRODUCTS_SCHEMA,
    )
    from src.common.config import (
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
        CUSTOMERS_CSV_PATH,
        ORDERS_CSV_PATH,
        PRODUCTS_CSV_PATH,
        ensure_unity_storage,
    )
except ImportError:
    from bronze.ingestion import ingest_bronze_entity
    from bronze.schemas import CUSTOMERS_SCHEMA, ORDERS_SCHEMA, PRODUCTS_SCHEMA
    from common.config import (
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
        CUSTOMERS_CSV_PATH,
        ORDERS_CSV_PATH,
        PRODUCTS_CSV_PATH,
        ensure_unity_storage,
    )

LOGGER = logging.getLogger("bronze.ingest_all")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


@dataclass(frozen=True)
class IngestJob:
    """Configuration for one Bronze entity ingestion."""

    entity_name: str
    source_path: str
    source_file_name: str
    table_name: str
    schema: StructType


@dataclass(frozen=True)
class IngestResult:
    """Result displayed in the final orchestration summary."""

    table: str
    rows_ingested: int
    duration_s: float
    status: str
    error: str | None = None


INGEST_JOBS = (
    IngestJob(
        "customers",
        CUSTOMERS_CSV_PATH,
        "customers.csv",
        BRONZE_CUSTOMERS_TABLE,
        CUSTOMERS_SCHEMA,
    ),
    IngestJob(
        "orders",
        ORDERS_CSV_PATH,
        "orders.csv",
        BRONZE_ORDERS_TABLE,
        ORDERS_SCHEMA,
    ),
    IngestJob(
        "products",
        PRODUCTS_CSV_PATH,
        "products.csv",
        BRONZE_PRODUCTS_TABLE,
        PRODUCTS_SCHEMA,
    ),
)


def run_ingest_job(job: IngestJob) -> IngestResult:
    """Run one Bronze ingestion and convert errors into a result.

    Args:
        job: Entity-specific source, target, and schema configuration.

    Returns:
        Successful or failed ingest result. Exceptions are logged so the
        orchestrator can continue to the next entity.
    """
    started = time.perf_counter()
    try:
        df = ingest_bronze_entity(
            spark,
            dbutils,
            entity_name=job.entity_name,
            source_path=job.source_path,
            source_file_name=job.source_file_name,
            table_name=job.table_name,
            schema=job.schema,
        )
        rows = int(df.count())
        duration_s = time.perf_counter() - started
        return IngestResult(
            table=job.entity_name,
            rows_ingested=rows,
            duration_s=duration_s,
            status="SUCCESS",
        )
    except Exception as exc:
        duration_s = time.perf_counter() - started
        LOGGER.exception(
            "%s Bronze ingest failed after %.3f seconds",
            job.entity_name,
            duration_s,
        )
        return IngestResult(
            table=job.entity_name,
            rows_ingested=0,
            duration_s=duration_s,
            status="FAILED",
            error=str(exc),
        )


def get_overall_status(results: list[IngestResult]) -> str:
    """Calculate the overall status from entity results.

    Args:
        results: Results from all configured ingest jobs.

    Returns:
        ``SUCCESS`` when all pass, ``FAILED`` when none pass, otherwise
        ``PARTIAL``.
    """
    successes = sum(result.status == "SUCCESS" for result in results)
    if successes == len(results):
        return "SUCCESS"
    if successes == 0:
        return "FAILED"
    return "PARTIAL"


def print_summary(
    results: list[IngestResult],
    overall_status: str,
    total_duration_s: float,
) -> None:
    """Print the requested per-table Bronze ingestion summary.

    Args:
        results: Per-entity ingestion results.
        overall_status: SUCCESS, PARTIAL, or FAILED.
        total_duration_s: End-to-end elapsed seconds.
    """
    print("\n========== Bronze ingest summary ==========")
    print(
        f"| {'Table':<10} | {'Rows Ingested':>13} | "
        f"{'Duration (s)':>12} | {'Status':<7} |"
    )
    print(f"|{'-' * 12}|{'-' * 15}|{'-' * 14}|{'-' * 9}|")
    for result in results:
        print(
            f"| {result.table:<10} | {result.rows_ingested:>13,} | "
            f"{result.duration_s:>12.1f} | {result.status:<7} |"
        )
    print(f"Overall status: {overall_status}")
    print(f"Total duration (s): {total_duration_s:.1f}")
    print("==========================================\n")


def ingest_all() -> str:
    """Run customers, orders, and products Bronze ingestion in sequence.

    Returns:
        Overall status: SUCCESS, PARTIAL, or FAILED.
    """
    started = time.perf_counter()
    LOGGER.info("Bronze orchestration started")

    try:
        ensure_unity_storage(spark)
    except Exception:
        LOGGER.exception("Unity Catalog setup failed; attempting each job")

    results = [run_ingest_job(job) for job in INGEST_JOBS]
    duration_s = time.perf_counter() - started
    overall_status = get_overall_status(results)
    print_summary(results, overall_status, duration_s)
    LOGGER.info(
        "Bronze orchestration completed in %.3f seconds with status %s",
        duration_s,
        overall_status,
    )
    return overall_status


ingest_all()
