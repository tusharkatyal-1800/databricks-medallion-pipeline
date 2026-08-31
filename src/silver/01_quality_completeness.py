# Databricks notebook source
"""Detect NULLs in critical Bronze fields and flag completeness."""

from __future__ import annotations

import logging

try:
    from src.silver.completeness import run_completeness_checks
except ImportError:
    from silver.completeness import run_completeness_checks

LOGGER = logging.getLogger("silver.quality_completeness")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_quality_completeness():
    """Flag completeness on Bronze customers and orders.

    Returns:
        Tuple of (customers_df, orders_df) with ``completeness_check``.
        Rows are marked, never dropped.

    Raises:
        Exception: If Bronze tables cannot be read or checks fail.
    """
    LOGGER.info("Silver completeness notebook started")
    customers_df, orders_df = run_completeness_checks(spark)
    LOGGER.info(
        "Returned flagged DataFrames: customers columns=%s, orders columns=%s",
        customers_df.columns,
        orders_df.columns,
    )
    return customers_df, orders_df


customers_completeness_df, orders_completeness_df = run_quality_completeness()
