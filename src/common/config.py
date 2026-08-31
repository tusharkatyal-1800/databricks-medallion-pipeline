"""Unity Catalog raw-file paths and managed table names.

Raw source storage::

    /Volumes/ecommerce/medallion/data/raw/...

Curated Bronze, Silver, and Gold datasets are managed Delta tables named
``ecommerce.medallion.<table>``. A table cannot use a directory inside a
Unity Catalog Volume as its registered table location.
"""

from __future__ import annotations

import logging

LOGGER = logging.getLogger(__name__)

UC_CATALOG = "ecommerce"
UC_SCHEMA = "medallion"
UC_VOLUME = "data"

VOLUME_ROOT = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}"

RAW_DIR = f"{VOLUME_ROOT}/raw"

CUSTOMERS_CSV_PATH = f"{RAW_DIR}/customers.csv"
ORDERS_CSV_PATH = f"{RAW_DIR}/orders.csv"
PRODUCTS_CSV_PATH = f"{RAW_DIR}/products.csv"

BRONZE_CUSTOMERS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.bronze_customers"
BRONZE_ORDERS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.bronze_orders"
BRONZE_PRODUCTS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.bronze_products"
SILVER_CUSTOMERS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.customers_silver"
SILVER_ORDERS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.orders_silver"
SILVER_PRODUCTS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.products_silver"
QUALITY_METRICS_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.quality_metrics"
GOLD_SALES_BY_PRODUCT_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.sales_by_product"
GOLD_REVENUE_BY_CUSTOMER_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.revenue_by_customer"
GOLD_CUSTOMER_SEGMENTATION_TABLE = (
    f"{UC_CATALOG}.{UC_SCHEMA}.customer_segmentation"
)


def volume_file(*parts: str) -> str:
    """Join path segments under the pipeline Volume root.

    Args:
        *parts: Relative segments (for example ``raw``, ``customers.csv``).

    Returns:
        Absolute ``/Volumes/...`` path.
    """
    return "/".join((VOLUME_ROOT, *parts))


def ensure_unity_storage(spark) -> None:
    """Create catalog, schema, and Volume if the caller is allowed to.

    Args:
        spark: Databricks notebook SparkSession.

    Raises:
        Exception: Re-raised if schema/volume creation fails after logging.
    """
    logger = LOGGER
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
    except Exception:
        logger.warning(
            "Could not CREATE CATALOG %s; it must already exist",
            UC_CATALOG,
        )
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}"
        )
        logger.info(
            "Ensured UC schema %s.%s and volume %s",
            UC_CATALOG,
            UC_SCHEMA,
            UC_VOLUME,
        )
    except Exception:
        logger.exception(
            "Failed to ensure Unity Catalog schema/volume %s.%s.%s",
            UC_CATALOG,
            UC_SCHEMA,
            UC_VOLUME,
        )
        raise
