# Databricks notebook source
"""Ingest orders CSV from the Volume into managed Bronze Delta."""

from __future__ import annotations

import logging

try:
    from src.bronze.ingestion import ingest_bronze_entity
    from src.bronze.schemas import ORDERS_SCHEMA
    from src.common.config import (
        BRONZE_ORDERS_TABLE,
        ORDERS_CSV_PATH,
        ensure_unity_storage,
    )
except ImportError:
    from bronze.ingestion import ingest_bronze_entity
    from bronze.schemas import ORDERS_SCHEMA
    from common.config import (
        BRONZE_ORDERS_TABLE,
        ORDERS_CSV_PATH,
        ensure_unity_storage,
    )

LOGGER = logging.getLogger("bronze.ingest_orders")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def ingest_orders():
    """Ingest orders into ``ecommerce.medallion.bronze_orders``.

    Returns:
        DataFrame written to the managed Bronze table.

    Raises:
        Exception: If storage setup, source read, or table write fails.
    """
    ensure_unity_storage(spark)
    return ingest_bronze_entity(
        spark,
        dbutils,
        entity_name="orders",
        source_path=ORDERS_CSV_PATH,
        source_file_name="orders.csv",
        table_name=BRONZE_ORDERS_TABLE,
        schema=ORDERS_SCHEMA,
    )


if __name__ == "__main__":
    ingest_orders()
