"""Uniqueness checks for Bronze primary keys.

Duplicate keys are flagged with a window ``row_number``. Rows are never
dropped. The first row per key is PASS; later rows are FAIL.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from src.common.config import BRONZE_CUSTOMERS_TABLE, BRONZE_ORDERS_TABLE
except ImportError:
    from common.config import BRONZE_CUSTOMERS_TABLE, BRONZE_ORDERS_TABLE

LOGGER = logging.getLogger(__name__)

CUSTOMERS_PRIMARY_KEY = "customer_id"
ORDERS_PRIMARY_KEY = "order_id"
INGESTION_TIMESTAMP_COL = "_ingestion_timestamp"


@dataclass(frozen=True)
class UniquenessMetric:
    """One uniqueness metric row for a primary key."""

    table_name: str
    field_checked: str
    total_rows: int
    unique_rows: int
    duplicate_rows: int
    uniqueness_pct: float


def validate_uniqueness_columns(df: DataFrame, key_column: str) -> None:
    """Raise if the primary key or ingest timestamp is missing.

    Args:
        df: Input Spark DataFrame.
        key_column: Primary-key column name.

    Raises:
        ValueError: If a required column is absent.
    """
    required = (key_column, INGESTION_TIMESTAMP_COL)
    missing = [name for name in required if name not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for uniqueness check: {missing}")


def add_uniqueness_check(df: DataFrame, key_column: str) -> DataFrame:
    """Add ``uniqueness_check`` using ``row_number`` over the primary key.

    The window is ``PARTITION BY key ORDER BY _ingestion_timestamp``.
    ``row_num == 1`` is PASS. ``row_num > 1`` is ``FAIL_DUPLICATE_{key}``.
    Null keys are PASS so completeness owns missing keys. All rows are kept.

    Args:
        df: Input Spark DataFrame.
        key_column: Primary-key column to partition on.

    Returns:
        DataFrame with ``uniqueness_check``. Row count is unchanged.

    Raises:
        ValueError: If required columns are missing.
    """
    validate_uniqueness_columns(df, key_column)
    key_window = Window.partitionBy(F.col(key_column)).orderBy(
        F.col(INGESTION_TIMESTAMP_COL).asc()
    )
    fail_token = f"FAIL_DUPLICATE_{key_column}"
    return (
        df.withColumn("_row_num", F.row_number().over(key_window))
        .withColumn(
            "uniqueness_check",
            F.when(F.col(key_column).isNull(), F.lit("PASS"))
            .when(F.col("_row_num") == 1, F.lit("PASS"))
            .otherwise(F.lit(fail_token)),
        )
        .drop("_row_num")
    )


def compute_uniqueness_metrics(
    df: DataFrame,
    table_name: str,
    key_column: str,
) -> UniquenessMetric:
    """Compute unique vs duplicate row counts and uniqueness percent.

    Args:
        df: DataFrame after uniqueness flags (row count unchanged).
        table_name: Label printed in the metrics table.
        key_column: Primary-key field name.

    Returns:
        Uniqueness metrics for one table.

    Raises:
        Exception: Re-raised after logging if aggregation fails.
    """
    try:
        total_rows = int(df.count())
        duplicate_rows = int(
            df.filter(F.col("uniqueness_check") != "PASS").count()
        )
        unique_rows = total_rows - duplicate_rows
        uniqueness_pct = (
            (unique_rows / total_rows) * 100.0 if total_rows else 0.0
        )
        return UniquenessMetric(
            table_name=table_name,
            field_checked=key_column,
            total_rows=total_rows,
            unique_rows=unique_rows,
            duplicate_rows=duplicate_rows,
            uniqueness_pct=uniqueness_pct,
        )
    except Exception:
        LOGGER.exception("Failed to compute uniqueness metrics for %s", table_name)
        raise


def print_uniqueness_metrics(metrics: list[UniquenessMetric]) -> None:
    """Print uniqueness metrics as a formatted table.

    Args:
        metrics: Rows to display.
    """
    print("\n========== Uniqueness check metrics ==========")
    print(
        f"| {'Table':<12} | {'Field':<14} | {'Total Rows':>10} | "
        f"{'Unique Rows':>11} | {'Duplicates':>10} | {'Uniqueness %':>14} |"
    )
    print(
        f"|{'-' * 14}|{'-' * 16}|{'-' * 12}|{'-' * 13}|{'-' * 12}|{'-' * 16}|"
    )
    for row in metrics:
        print(
            f"| {row.table_name:<12} | {row.field_checked:<14} | "
            f"{row.total_rows:>10,} | {row.unique_rows:>11,} | "
            f"{row.duplicate_rows:>10,} | {row.uniqueness_pct:>13.2f}% |"
        )
    print("==============================================\n")


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


def apply_uniqueness_to_table(
    spark: SparkSession,
    table_name: str,
    display_name: str,
    key_column: str,
) -> tuple[DataFrame, UniquenessMetric]:
    """Load one Bronze table, flag duplicate keys, and compute metrics.

    Args:
        spark: Databricks notebook SparkSession.
        table_name: Three-level Bronze table name.
        display_name: Short name used in the metrics table.
        key_column: Primary-key column.

    Returns:
        Flagged DataFrame and uniqueness metrics. Row count is unchanged.
    """
    bronze_df = load_bronze_table(spark, table_name)
    flagged_df = add_uniqueness_check(bronze_df, key_column)
    metrics = compute_uniqueness_metrics(flagged_df, display_name, key_column)
    LOGGER.info(
        "%s uniqueness: %s duplicate rows out of %s (rows retained)",
        display_name,
        metrics.duplicate_rows,
        metrics.total_rows,
    )
    return flagged_df, metrics


def run_uniqueness_checks(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Run uniqueness checks on customers and orders Bronze tables.

    Args:
        spark: Databricks notebook SparkSession.

    Returns:
        Tuple of (customers_df, orders_df) with ``uniqueness_check``.
        No rows are dropped.

    Raises:
        Exception: Re-raised after logging if a table read or check fails.
    """
    LOGGER.info("Starting uniqueness checks on Bronze tables")
    customers_df, customer_metrics = apply_uniqueness_to_table(
        spark,
        BRONZE_CUSTOMERS_TABLE,
        "customers",
        CUSTOMERS_PRIMARY_KEY,
    )
    orders_df, order_metrics = apply_uniqueness_to_table(
        spark,
        BRONZE_ORDERS_TABLE,
        "orders",
        ORDERS_PRIMARY_KEY,
    )
    print_uniqueness_metrics([customer_metrics, order_metrics])
    LOGGER.info("Uniqueness checks completed; all Bronze rows retained")
    return customers_df, orders_df
