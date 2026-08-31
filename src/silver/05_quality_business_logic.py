# Databricks notebook source
"""Flag extra e-commerce consistency rules; never drop rows."""

from __future__ import annotations

import logging

try:
    from src.silver.business_logic import run_business_logic_checks
except ImportError:
    from silver.business_logic import run_business_logic_checks

LOGGER = logging.getLogger("silver.quality_business_logic")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_quality_business_logic():
    """Flag extra consistency rules on Bronze tables.

    Returns:
        Tuple of (customers_df, orders_df, products_df) with
        ``business_logic_check``. Rows are marked, never dropped.
        Failures map to ``TYPE_VALIDATION_FAIL`` when Silver is assembled.

    Raises:
        Exception: If Bronze tables cannot be read or checks fail.
    """
    LOGGER.info("Silver business-logic notebook started")
    customers_df, orders_df, products_df = run_business_logic_checks(spark)
    LOGGER.info(
        "Returned flagged DataFrames: customers=%s, orders=%s, products=%s",
        customers_df.columns,
        orders_df.columns,
        products_df.columns,
    )
    return customers_df, orders_df, products_df


(
    customers_business_df,
    orders_business_df,
    products_business_df,
) = run_quality_business_logic()
