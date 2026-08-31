# Databricks notebook source
"""Detect duplicate Bronze primary keys and flag uniqueness."""

from __future__ import annotations

import logging

try:
    from src.silver.uniqueness import run_uniqueness_checks
except ImportError:
    from silver.uniqueness import run_uniqueness_checks

LOGGER = logging.getLogger("silver.quality_uniqueness")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_quality_uniqueness():
    """Flag uniqueness on Bronze customers and orders.

    Returns:
        Tuple of (customers_df, orders_df) with ``uniqueness_check``.
        Rows are marked, never dropped.

    Raises:
        Exception: If Bronze tables cannot be read or checks fail.
    """
    LOGGER.info("Silver uniqueness notebook started")
    customers_df, orders_df = run_uniqueness_checks(spark)
    LOGGER.info(
        "Returned flagged DataFrames: customers columns=%s, orders columns=%s",
        customers_df.columns,
        orders_df.columns,
    )
    return customers_df, orders_df


customers_uniqueness_df, orders_uniqueness_df = run_quality_uniqueness()
