# Databricks notebook source
"""Orchestrate Bronze ingest for customers, orders, and products."""

from __future__ import annotations

import builtins
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. Get spark & dbutils (works on Databricks and locally)
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
    dbutils  # noqa: F821
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

# Make spark & dbutils available to dynamically imported scripts
builtins.spark = spark
builtins.dbutils = dbutils

# ---------------------------------------------------------------------------
# 2. Ensure src/ is on sys.path
# ---------------------------------------------------------------------------
try:
    _SRC_DIR = str(Path(__file__).resolve().parent.parent)
except NameError:
    _SRC_DIR = (
        "/Workspace/Users/tushar.katyal@tothenew.com/"
        "databricks-medallion-pipeline/src"
    )
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

# ---------------------------------------------------------------------------
# 3. Logging
# ---------------------------------------------------------------------------
LOGGER = logging.getLogger("bronze.ingest_all")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

# ---------------------------------------------------------------------------
# 4. Now import the ingest functions directly
# ---------------------------------------------------------------------------
from common.config import (  # noqa: E402
    BRONZE_CUSTOMERS_TABLE,
    BRONZE_ORDERS_TABLE,
    BRONZE_PRODUCTS_TABLE,
    ensure_unity_storage,
)

# Files start with digits, so use importlib
import importlib  # noqa: E402

_cust_mod = importlib.import_module("bronze.01_ingest_customers")
_orders_mod = importlib.import_module("bronze.02_ingest_orders")
_products_mod = importlib.import_module("bronze.03_ingest_products")

INGEST_JOBS = (
    ("customers", _cust_mod.ingest_customers),
    ("orders", _orders_mod.ingest_orders),
    ("products", _products_mod.ingest_products),
)


# ---------------------------------------------------------------------------
# 5. Simple dataclass + runner
# ---------------------------------------------------------------------------
@dataclass
class IngestResult:
    table: str
    rows_ingested: int
    duration_s: float
    status: str
    error: str | None = None


def _run_one(table: str, ingest_fn) -> IngestResult:
    started = time.perf_counter()
    try:
        LOGGER.info("Starting Bronze ingest for %s", table)
        df = ingest_fn()
        rows = int(df.count())
        dur = time.perf_counter() - started
        LOGGER.info("Finished %s: %s rows in %.1fs", table, rows, dur)
        return IngestResult(table, rows, dur, "SUCCESS")
    except Exception as exc:
        dur = time.perf_counter() - started
        LOGGER.exception("Failed %s after %.1fs: %s", table, dur, exc)
        return IngestResult(table, 0, dur, "FAILED", str(exc))


def ingest_all() -> str:
    wall_start = time.perf_counter()
    LOGGER.info("Bronze ingest_all started")

    ensure_unity_storage(spark)

    results = [_run_one(table, fn) for table, fn in INGEST_JOBS]

    total = time.perf_counter() - wall_start
    ok = sum(1 for r in results if r.status == "SUCCESS")
    overall = "SUCCESS" if ok == len(results) else ("FAILED" if ok == 0 else "PARTIAL")

    # Print summary
    print("\n========== Bronze ingest summary ==========")
    print(f"| {'Table':<10} | {'Rows':>13} | {'Duration':>10} | {'Status':<7} |")
    print(f"|{'-'*12}|{'-'*15}|{'-'*12}|{'-'*9}|")
    for r in results:
        print(f"| {r.table:<10} | {r.rows_ingested:>13,} | {r.duration_s:>9.1f}s | {r.status:<7} |")
    print(f"Overall: {overall} | Total: {total:.1f}s")
    print("==========================================\n")

    return overall


# Auto-run when executed via %run in Databricks
ingest_all()