# Databricks notebook source
"""Detect orphan order foreign keys; flag rows, never drop them."""

from __future__ import annotations

import logging

try:
    from src.silver.referential import run_referential_checks
except ImportError:
    from silver.referential import run_referential_checks

LOGGER = logging.getLogger("silver.quality_referential")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_quality_referential_integrity():
    """Flag referential integrity on Bronze orders.

    Returns:
        Orders DataFrame with ``referential_integrity_check``.
        Null FKs stay PASS; only non-null orphans fail.

    Raises:
        Exception: If Bronze tables cannot be read or checks fail.
    """
    LOGGER.info("Silver referential-integrity notebook started")
    orders_df = run_referential_checks(spark)
    LOGGER.info(
        "Returned flagged orders DataFrame columns=%s",
        orders_df.columns,
    )
    return orders_df


orders_referential_df = run_quality_referential_integrity()
