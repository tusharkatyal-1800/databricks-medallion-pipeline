# Databricks notebook source
"""Explicit PySpark schemas for Bronze CSV ingestion.

These contracts are passed to ``spark.read.csv(..., schema=..., header=True)``
with ``inferSchema=False`` (the default when a schema is supplied).

Why an explicit schema (not inference):
    * Production-like data contract that does not drift with sample rows.
    * Unexpected extra/missing columns or unparsable values fail fast or
      become null instead of silently changing types between runs.
    * Documents the expected types for reviewers and downstream Silver.

Malformed CSV values that cannot cast to the declared type become null.
Nullable fields (emails, FKs, payment_date) must stay nullable so planted
completeness issues survive Bronze.

Ingestion metadata (``ingestion_timestamp``, ``source_file_name``) is added
after read and is not part of these source schemas.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

MONEY_TYPE = DecimalType(10, 2)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_value", MONEY_TYPE, True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", MONEY_TYPE, True),
        StructField("total_amount", MONEY_TYPE, True),
        StructField("order_status", StringType(), True),
        StructField("payment_date", DateType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", MONEY_TYPE, True),
        StructField("cost", MONEY_TYPE, True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
    ]
)

SCHEMA_BY_ENTITY = {
    "customers": CUSTOMERS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
}


def get_bronze_schema(entity_name: str) -> StructType:
    """Return the explicit Bronze source schema for an entity.

    Args:
        entity_name: One of ``customers``, ``orders``, or ``products``.

    Returns:
        The matching ``StructType`` contract.

    Raises:
        KeyError: If ``entity_name`` is not a known Bronze entity.
    """
    try:
        return SCHEMA_BY_ENTITY[entity_name]
    except KeyError as exc:
        known = ", ".join(sorted(SCHEMA_BY_ENTITY))
        raise KeyError(
            f"Unknown Bronze entity {entity_name!r}. Expected one of: {known}."
        ) from exc
