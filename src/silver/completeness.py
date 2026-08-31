"""Completeness checks for Bronze tables.

Nulls in required fields are flagged. Rows are never dropped or filtered.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from src.common.config import BRONZE_CUSTOMERS_TABLE, BRONZE_ORDERS_TABLE
except ImportError:
    from common.config import BRONZE_CUSTOMERS_TABLE, BRONZE_ORDERS_TABLE

LOGGER = logging.getLogger(__name__)

CUSTOMERS_REQUIRED_FIELDS = ("email",)
ORDERS_REQUIRED_FIELDS = ("customer_id", "product_id")


@dataclass(frozen=True)
class CompletenessMetric:
    """One completeness metric row for a single field."""

    table_name: str
    field_checked: str
    total_rows: int
    null_count: int
    not_null_count: int
    completeness_pct: float


def validate_required_columns(df: DataFrame, required_fields: tuple[str, ...]) -> None:
    """Raise if a required completeness column is missing.

    Args:
        df: Input Spark DataFrame.
        required_fields: Column names that must exist.

    Raises:
        ValueError: If any required column is absent.
    """
    missing = [name for name in required_fields if name not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for completeness: {missing}")


def add_completeness_check(
    df: DataFrame,
    required_fields: tuple[str, ...],
) -> DataFrame:
    """Add ``completeness_check`` using NULL flags on required fields.

    Args:
        df: Input Spark DataFrame. All rows are retained.
        required_fields: Fields that must be non-null to PASS.

    Returns:
        DataFrame with ``completeness_check``:
        ``PASS``, ``FAIL_NULL_<field>``, or pipe-joined fail tokens.

    Raises:
        ValueError: If a required column is missing.
    """
    validate_required_columns(df, required_fields)
    fail_tokens = [
        F.when(F.col(field).isNull(), F.lit(f"FAIL_NULL_{field}"))
        for field in required_fields
    ]
    combined_flags = F.concat_ws("|", *fail_tokens)
    return df.withColumn(
        "completeness_check",
        F.when(combined_flags == "", F.lit("PASS")).otherwise(combined_flags),
    )


def compute_field_metrics(
    df: DataFrame,
    table_name: str,
    required_fields: tuple[str, ...],
) -> list[CompletenessMetric]:
    """Compute per-field NULL counts and completeness percent.

    Args:
        df: DataFrame after completeness flags (row count unchanged).
        table_name: Label printed in the metrics table.
        required_fields: Fields included in the summary.

    Returns:
        One ``CompletenessMetric`` per required field.

    Raises:
        Exception: Re-raised after logging if aggregation fails.
    """
    try:
        total_rows = int(df.count())
        aggregations = [
            F.sum(F.col(field).isNull().cast("int")).alias(field)
            for field in required_fields
        ]
        counts = df.agg(*aggregations).collect()[0]
        metrics: list[CompletenessMetric] = []
        for field in required_fields:
            null_count = int(counts[field] or 0)
            not_null_count = total_rows - null_count
            completeness_pct = (
                (not_null_count / total_rows) * 100.0 if total_rows else 0.0
            )
            metrics.append(
                CompletenessMetric(
                    table_name=table_name,
                    field_checked=field,
                    total_rows=total_rows,
                    null_count=null_count,
                    not_null_count=not_null_count,
                    completeness_pct=completeness_pct,
                )
            )
        return metrics
    except Exception:
        LOGGER.exception("Failed to compute completeness metrics for %s", table_name)
        raise


def print_completeness_metrics(metrics: list[CompletenessMetric]) -> None:
    """Print completeness metrics as a formatted table.

    Args:
        metrics: Rows to display.
    """
    print("\n========== Completeness check metrics ==========")
    print(
        f"| {'Table':<12} | {'Field':<14} | {'Total Rows':>10} | "
        f"{'NULL Count':>10} | {'Not-Null':>10} | {'Completeness %':>15} |"
    )
    print(
        f"|{'-' * 14}|{'-' * 16}|{'-' * 12}|{'-' * 12}|{'-' * 12}|{'-' * 17}|"
    )
    for row in metrics:
        print(
            f"| {row.table_name:<12} | {row.field_checked:<14} | "
            f"{row.total_rows:>10,} | {row.null_count:>10,} | "
            f"{row.not_null_count:>10,} | {row.completeness_pct:>14.2f}% |"
        )
    print("================================================\n")


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
        LOGGER.info("Loaded Bronze table %s (%s rows)", table_name, df.count())
        return df
    except Exception:
        LOGGER.exception("Failed to load Bronze table %s", table_name)
        raise


def apply_completeness_to_table(
    spark: SparkSession,
    table_name: str,
    display_name: str,
    required_fields: tuple[str, ...],
) -> tuple[DataFrame, list[CompletenessMetric]]:
    """Load one Bronze table, flag nulls, and compute field metrics.

    Args:
        spark: Databricks notebook SparkSession.
        table_name: Three-level Bronze table name.
        display_name: Short name used in the metrics table.
        required_fields: Critical columns to check.

    Returns:
        Flagged DataFrame and per-field metrics. Row count is unchanged.
    """
    bronze_df = load_bronze_table(spark, table_name)
    flagged_df = add_completeness_check(bronze_df, required_fields)
    metrics = compute_field_metrics(flagged_df, display_name, required_fields)
    fail_count = int(
        flagged_df.filter(F.col("completeness_check") != "PASS").count()
    )
    LOGGER.info(
        "%s completeness: %s fail rows out of %s (rows retained)",
        display_name,
        fail_count,
        metrics[0].total_rows if metrics else 0,
    )
    return flagged_df, metrics


def run_completeness_checks(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Run completeness checks on customers and orders Bronze tables.

    Args:
        spark: Databricks notebook SparkSession.

    Returns:
        Tuple of (customers_df, orders_df) with ``completeness_check``.
        No rows are dropped.

    Raises:
        Exception: Re-raised after logging if a table read or check fails.
    """
    LOGGER.info("Starting completeness checks on Bronze tables")
    customers_df, customer_metrics = apply_completeness_to_table(
        spark,
        BRONZE_CUSTOMERS_TABLE,
        "customers",
        CUSTOMERS_REQUIRED_FIELDS,
    )
    orders_df, order_metrics = apply_completeness_to_table(
        spark,
        BRONZE_ORDERS_TABLE,
        "orders",
        ORDERS_REQUIRED_FIELDS,
    )
    print_completeness_metrics(customer_metrics + order_metrics)
    LOGGER.info("Completeness checks completed; all Bronze rows retained")
    return customers_df, orders_df
