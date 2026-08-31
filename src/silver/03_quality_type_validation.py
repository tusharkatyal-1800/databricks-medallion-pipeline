# Databricks notebook source
"""Validate Bronze types and business rules; flag rows, never drop them."""

from __future__ import annotations

import logging

try:
    from src.silver.type_validation import run_type_validation_checks
except ImportError:
    from silver.type_validation import run_type_validation_checks

LOGGER = logging.getLogger("silver.quality_type_validation")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_quality_type_validation():
    """Flag type and business-rule issues on Bronze tables.

    Returns:
        Tuple of (customers_df, orders_df, products_df) with
        ``type_validation_check``. Rows are marked, never dropped.

    Raises:
        Exception: If Bronze tables cannot be read or checks fail.
    """
    LOGGER.info("Silver type-validation notebook started")
    customers_df, orders_df, products_df = run_type_validation_checks(spark)
    LOGGER.info(
        "Returned flagged DataFrames: customers=%s, orders=%s, products=%s",
        customers_df.columns,
        orders_df.columns,
        products_df.columns,
    )
    return customers_df, orders_df, products_df


(
    customers_type_df,
    orders_type_df,
    products_type_df,
) = run_quality_type_validation()
