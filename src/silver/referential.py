"""Referential integrity checks for Bronze order foreign keys.

Orphan non-null FKs are found with LEFT ANTI JOIN. Null FKs are not
scored here; completeness owns them. Rows are never dropped.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from src.common.config import (
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
    )
except ImportError:
    from common.config import (
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
    )

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReferentialMetric:
    """Orphan counts for one foreign key."""

    table_name: str
    field_checked: str
    total_rows: int
    applicable_rows: int
    orphan_count: int
    integrity_pct: float


def validate_columns(df: DataFrame, required_fields: tuple[str, ...]) -> None:
    """Raise if a required column is missing.

    Args:
        df: Input Spark DataFrame.
        required_fields: Column names that must exist.

    Raises:
        ValueError: If any required column is absent.
    """
    missing = [name for name in required_fields if name not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for referential check: {missing}")


def orphan_foreign_keys(
    child_df: DataFrame,
    parent_df: DataFrame,
    key_column: str,
) -> DataFrame:
    """Return distinct non-null FK values missing from the parent table.

    Uses LEFT ANTI JOIN so only keys with no parent match are returned.
    Duplicate parent keys still count as present.

    Args:
        child_df: Child (orders) DataFrame.
        parent_df: Parent DataFrame (customers or products).
        key_column: Shared key name (``customer_id`` or ``product_id``).

    Returns:
        DataFrame with ``key_column`` and ``is_orphan`` = True.

    Raises:
        Exception: Re-raised after logging if the join fails.
    """
    try:
        parents = (
            parent_df.select(F.col(key_column).alias("parent_id"))
            .where(F.col("parent_id").isNotNull())
            .distinct()
        )
        orphans = (
            child_df.select(key_column)
            .where(F.col(key_column).isNotNull())
            .join(
                parents,
                child_df[key_column] == parents["parent_id"],
                "left_anti",
            )
            .select(F.col(key_column))
            .distinct()
            .withColumn("is_orphan", F.lit(True))
        )
        LOGGER.info("Computed orphan %s keys via left_anti join", key_column)
        return orphans
    except Exception:
        LOGGER.exception("Failed left_anti join for orphan %s keys", key_column)
        raise


def add_referential_integrity_check(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Add ``referential_integrity_check`` for order foreign keys.

    Null ``customer_id`` / ``product_id`` stay PASS. Non-null FKs missing
    from the parent table are ``FAIL_ORPHAN_{field}`` (pipe-joined if both).

    Args:
        orders_df: Bronze orders.
        customers_df: Bronze customers (parent).
        products_df: Bronze products (parent).

    Returns:
        Orders DataFrame with ``referential_integrity_check``.
        Row count is unchanged.

    Raises:
        ValueError: If required columns are missing.
    """
    validate_columns(orders_df, ("customer_id", "product_id"))
    validate_columns(customers_df, ("customer_id",))
    validate_columns(products_df, ("product_id",))

    orphan_customers = orphan_foreign_keys(
        orders_df,
        customers_df,
        "customer_id",
    )
    orphan_products = orphan_foreign_keys(
        orders_df,
        products_df,
        "product_id",
    )

    flagged = (
        orders_df.join(orphan_customers, on="customer_id", how="left")
        .withColumnRenamed("is_orphan", "orphan_customer")
        .join(orphan_products, on="product_id", how="left")
        .withColumnRenamed("is_orphan", "orphan_product")
    )
    fail_tokens = [
        F.when(
            F.col("orphan_customer").isNotNull(),
            F.lit("FAIL_ORPHAN_customer_id"),
        ),
        F.when(
            F.col("orphan_product").isNotNull(),
            F.lit("FAIL_ORPHAN_product_id"),
        ),
    ]
    combined_flags = F.concat_ws("|", *fail_tokens)
    return (
        flagged.withColumn(
            "referential_integrity_check",
            F.when(combined_flags == "", F.lit("PASS")).otherwise(combined_flags),
        )
        .drop("orphan_customer", "orphan_product")
    )


def compute_referential_metrics(
    df: DataFrame,
) -> list[ReferentialMetric]:
    """Compute orphan counts and integrity percent per foreign key.

    Applicable rows are non-null FKs only. Null FKs are excluded from
    the integrity denominator.

    Args:
        df: Orders DataFrame with ``referential_integrity_check``.

    Returns:
        Metrics for ``customer_id`` and ``product_id``.

    Raises:
        Exception: Re-raised after logging if aggregation fails.
    """
    try:
        total_rows = int(df.count())
        specs = (
            ("customer_id", "FAIL_ORPHAN_customer_id"),
            ("product_id", "FAIL_ORPHAN_product_id"),
        )
        metrics: list[ReferentialMetric] = []
        for field, fail_token in specs:
            applicable_rows = int(df.filter(F.col(field).isNotNull()).count())
            orphan_count = int(
                df.filter(
                    F.col("referential_integrity_check").contains(fail_token)
                ).count()
            )
            valid_count = applicable_rows - orphan_count
            integrity_pct = (
                (valid_count / applicable_rows) * 100.0 if applicable_rows else 0.0
            )
            metrics.append(
                ReferentialMetric(
                    table_name="orders",
                    field_checked=field,
                    total_rows=total_rows,
                    applicable_rows=applicable_rows,
                    orphan_count=orphan_count,
                    integrity_pct=integrity_pct,
                )
            )
        return metrics
    except Exception:
        LOGGER.exception("Failed to compute referential integrity metrics")
        raise


def print_referential_metrics(metrics: list[ReferentialMetric]) -> None:
    """Print referential integrity metrics as a formatted table.

    Args:
        metrics: Rows to display.
    """
    print("\n========== Referential integrity metrics ==========")
    print(
        f"| {'Table':<10} | {'Field':<14} | {'Total':>10} | "
        f"{'Non-null FK':>12} | {'Orphans':>8} | {'Integrity %':>14} |"
    )
    print(
        f"|{'-' * 12}|{'-' * 16}|{'-' * 12}|{'-' * 14}|{'-' * 10}|{'-' * 16}|"
    )
    for row in metrics:
        print(
            f"| {row.table_name:<10} | {row.field_checked:<14} | "
            f"{row.total_rows:>10,} | {row.applicable_rows:>12,} | "
            f"{row.orphan_count:>8,} | {row.integrity_pct:>13.2f}% |"
        )
    print("====================================================\n")


def load_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Load a managed Bronze Delta table.

    Args:
        spark: Databricks notebook SparkSession.
        table_name: Three-level Unity Catalog table name.

    Returns:
        Bronze DataFrame.

    Raises:
        Exception: Re-raised after logging if the table cannot be read.
    """
    try:
        df = spark.table(table_name)
        LOGGER.info("Loaded Bronze table %s", table_name)
        return df
    except Exception:
        LOGGER.exception("Failed to load Bronze table %s", table_name)
        raise


def run_referential_checks(spark: SparkSession) -> DataFrame:
    """Flag orphan order FKs against Bronze customers and products.

    Args:
        spark: Databricks notebook SparkSession.

    Returns:
        Orders DataFrame with ``referential_integrity_check``.
        No rows are dropped.

    Raises:
        Exception: Re-raised after logging if a table read or check fails.
    """
    LOGGER.info("Starting referential integrity checks on Bronze orders")
    orders_df = load_bronze_table(spark, BRONZE_ORDERS_TABLE)
    customers_df = load_bronze_table(spark, BRONZE_CUSTOMERS_TABLE)
    products_df = load_bronze_table(spark, BRONZE_PRODUCTS_TABLE)
    flagged_df = add_referential_integrity_check(
        orders_df,
        customers_df,
        products_df,
    )
    metrics = compute_referential_metrics(flagged_df)
    print_referential_metrics(metrics)
    fail_rows = int(
        flagged_df.filter(F.col("referential_integrity_check") != "PASS").count()
    )
    LOGGER.info(
        "Referential integrity: %s orphan rows out of %s (null FKs not scored)",
        fail_rows,
        metrics[0].total_rows if metrics else 0,
    )
    return flagged_df
