"""Type and business-rule validation for Bronze tables.

Present values are scored against date, numeric, string, and business
rules. Nulls are left to completeness except where a rule requires a
value (Completed/Pending payment_date). Rows are never dropped.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
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

EMAIL_REGEX = r".*@.*\..*"
CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
ORDER_STATUSES = ("Pending", "Completed", "Cancelled")
SIGNUP_DATE_MIN = "2020-01-01"
ORDER_DATE_MIN = "2023-01-01"
AMOUNT_TOLERANCE = 0.01


@dataclass(frozen=True)
class TypeCheck:
    """One type or business-rule check."""

    check_name: str
    check_type: str
    fail_condition: Column


@dataclass(frozen=True)
class TypeValidationMetric:
    """Fail counts for one named check."""

    table_name: str
    check_type: str
    check_name: str
    total_rows: int
    fail_count: int
    pass_count: int
    pass_pct: float


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
        raise ValueError(f"Missing columns for type validation: {missing}")


def _present_and(condition: Column, *fields: str) -> Column:
    """AND a rule with non-null guards so completeness owns missing values.

    Args:
        condition: Domain/business fail condition.
        *fields: Columns that must be non-null for the rule to apply.

    Returns:
        Fail condition that is false when any listed field is null.
    """
    present = F.lit(True)
    for field in fields:
        present = present & F.col(field).isNotNull()
    return present & condition


def customer_type_checks() -> tuple[TypeCheck, ...]:
    """Return customer date and string checks.

    Returns:
        Customer type-check definitions.
    """
    today = F.current_date()
    signup_min = F.to_date(F.lit(SIGNUP_DATE_MIN))
    invalid_signup = (F.col("signup_date") < signup_min) | (
        F.col("signup_date") > today
    )
    invalid_email = ~F.col("email").rlike(EMAIL_REGEX)
    invalid_segment = ~F.col("customer_segment").isin(list(CUSTOMER_SEGMENTS))
    return (
        TypeCheck(
            "signup_date",
            "Date",
            _present_and(invalid_signup, "signup_date"),
        ),
        TypeCheck(
            "email",
            "String/Regex",
            _present_and(invalid_email, "email"),
        ),
        TypeCheck(
            "customer_segment",
            "String/Regex",
            _present_and(invalid_segment, "customer_segment"),
        ),
    )


def order_type_checks() -> tuple[TypeCheck, ...]:
    """Return order date, numeric, string, and business-rule checks.

    Returns:
        Order type-check definitions.
    """
    today = F.current_date()
    order_min = F.to_date(F.lit(ORDER_DATE_MIN))
    invalid_order_date = (F.col("order_date") < order_min) | (
        F.col("order_date") > today
    )
    invalid_payment_order = F.col("payment_date") <= F.col("order_date")
    expected_amount = F.col("quantity") * F.col("unit_price")
    amount_delta = F.abs(F.col("total_amount") - expected_amount)
    amount_tolerance = F.abs(expected_amount) * F.lit(AMOUNT_TOLERANCE)
    invalid_amount_formula = amount_delta > amount_tolerance
    completed_missing_payment = (F.col("order_status") == "Completed") & F.col(
        "payment_date"
    ).isNull()
    pending_has_payment = (F.col("order_status") == "Pending") & F.col(
        "payment_date"
    ).isNotNull()
    invalid_status = ~F.col("order_status").isin(list(ORDER_STATUSES))
    return (
        TypeCheck(
            "order_date",
            "Date",
            _present_and(invalid_order_date, "order_date"),
        ),
        TypeCheck(
            "payment_date",
            "Date",
            _present_and(invalid_payment_order, "payment_date", "order_date"),
        ),
        TypeCheck(
            "quantity",
            "Numeric",
            _present_and(~(F.col("quantity") > 0), "quantity"),
        ),
        TypeCheck(
            "unit_price",
            "Numeric",
            _present_and(~(F.col("unit_price") > 0), "unit_price"),
        ),
        TypeCheck(
            "total_amount",
            "Numeric",
            _present_and(~(F.col("total_amount") > 0), "total_amount"),
        ),
        TypeCheck(
            "order_status",
            "String/Regex",
            _present_and(invalid_status, "order_status"),
        ),
        TypeCheck(
            "amount_formula",
            "Business",
            _present_and(
                invalid_amount_formula,
                "quantity",
                "unit_price",
                "total_amount",
            ),
        ),
        TypeCheck(
            "completed_payment_date",
            "Business",
            completed_missing_payment,
        ),
        TypeCheck(
            "pending_payment_date",
            "Business",
            pending_has_payment,
        ),
    )


def product_type_checks() -> tuple[TypeCheck, ...]:
    """Return product numeric checks.

    Returns:
        Product type-check definitions.
    """
    invalid_cost = ~(
        (F.col("cost") > 0) & (F.col("cost") < F.col("price"))
    )
    return (
        TypeCheck(
            "price",
            "Numeric",
            _present_and(~(F.col("price") > 0), "price"),
        ),
        TypeCheck(
            "cost",
            "Numeric",
            _present_and(invalid_cost, "cost", "price"),
        ),
        TypeCheck(
            "stock_quantity",
            "Numeric",
            _present_and(F.col("stock_quantity") < 0, "stock_quantity"),
        ),
    )


def add_type_validation_check(
    df: DataFrame,
    checks: tuple[TypeCheck, ...],
) -> DataFrame:
    """Add ``type_validation_check`` from named fail conditions.

    Args:
        df: Input Spark DataFrame. All rows are retained.
        checks: Ordered checks whose fail tokens are pipe-joined.

    Returns:
        DataFrame with ``type_validation_check``:
        ``PASS`` or ``FAIL_INVALID_<check_name>`` tokens joined by ``|``.
    """
    fail_tokens = [
        F.when(check.fail_condition, F.lit(f"FAIL_INVALID_{check.check_name}"))
        for check in checks
    ]
    combined_flags = F.concat_ws("|", *fail_tokens)
    return df.withColumn(
        "type_validation_check",
        F.when(combined_flags == "", F.lit("PASS")).otherwise(combined_flags),
    )


def compute_type_metrics(
    df: DataFrame,
    table_name: str,
    checks: tuple[TypeCheck, ...],
) -> list[TypeValidationMetric]:
    """Compute pass/fail counts for each named check.

    Args:
        df: DataFrame used for the check conditions.
        table_name: Label printed in the metrics table.
        checks: Checks to score.

    Returns:
        One metric row per check.

    Raises:
        Exception: Re-raised after logging if aggregation fails.
    """
    try:
        total_rows = int(df.count())
        aggregations = [
            F.sum(check.fail_condition.cast("int")).alias(check.check_name)
            for check in checks
        ]
        counts = df.agg(*aggregations).collect()[0]
        metrics: list[TypeValidationMetric] = []
        for check in checks:
            fail_count = int(counts[check.check_name] or 0)
            pass_count = total_rows - fail_count
            pass_pct = (pass_count / total_rows) * 100.0 if total_rows else 0.0
            metrics.append(
                TypeValidationMetric(
                    table_name=table_name,
                    check_type=check.check_type,
                    check_name=check.check_name,
                    total_rows=total_rows,
                    fail_count=fail_count,
                    pass_count=pass_count,
                    pass_pct=pass_pct,
                )
            )
        return metrics
    except Exception:
        LOGGER.exception("Failed to compute type-validation metrics for %s", table_name)
        raise


def print_type_metrics(metrics: list[TypeValidationMetric]) -> None:
    """Print type-validation metrics as a formatted table.

    Args:
        metrics: Rows to display.
    """
    print("\n========== Type validation metrics ==========")
    print(
        f"| {'Table':<12} | {'Type':<13} | {'Check':<24} | "
        f"{'Total':>10} | {'Fails':>8} | {'Pass %':>10} |"
    )
    print(
        f"|{'-' * 14}|{'-' * 15}|{'-' * 26}|{'-' * 12}|{'-' * 10}|{'-' * 12}|"
    )
    for row in metrics:
        print(
            f"| {row.table_name:<12} | {row.check_type:<13} | "
            f"{row.check_name:<24} | {row.total_rows:>10,} | "
            f"{row.fail_count:>8,} | {row.pass_pct:>9.2f}% |"
        )
    print("============================================\n")


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


def apply_type_validation(
    spark: SparkSession,
    table_name: str,
    display_name: str,
    required_fields: tuple[str, ...],
    checks: tuple[TypeCheck, ...],
) -> tuple[DataFrame, list[TypeValidationMetric]]:
    """Load one Bronze table, apply type checks, and compute metrics.

    Args:
        spark: Databricks notebook SparkSession.
        table_name: Three-level Bronze table name.
        display_name: Short name used in the metrics table.
        required_fields: Columns required by the checks.
        checks: Type and business-rule checks.

    Returns:
        Flagged DataFrame and per-check metrics. Row count is unchanged.
    """
    bronze_df = load_bronze_table(spark, table_name)
    validate_columns(bronze_df, required_fields)
    flagged_df = add_type_validation_check(bronze_df, checks)
    metrics = compute_type_metrics(flagged_df, display_name, checks)
    fail_rows = int(
        flagged_df.filter(F.col("type_validation_check") != "PASS").count()
    )
    LOGGER.info(
        "%s type validation: %s fail rows out of %s (rows retained)",
        display_name,
        fail_rows,
        metrics[0].total_rows if metrics else 0,
    )
    return flagged_df, metrics


def run_type_validation_checks(
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Run type validation on customers, orders, and products Bronze tables.

    Args:
        spark: Databricks notebook SparkSession.

    Returns:
        Tuple of (customers_df, orders_df, products_df) with
        ``type_validation_check``. No rows are dropped.

    Raises:
        Exception: Re-raised after logging if a table read or check fails.
    """
    LOGGER.info("Starting type validation on Bronze tables")
    customers_df, customer_metrics = apply_type_validation(
        spark,
        BRONZE_CUSTOMERS_TABLE,
        "customers",
        ("signup_date", "email", "customer_segment"),
        customer_type_checks(),
    )
    orders_df, order_metrics = apply_type_validation(
        spark,
        BRONZE_ORDERS_TABLE,
        "orders",
        (
            "order_date",
            "payment_date",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
        ),
        order_type_checks(),
    )
    products_df, product_metrics = apply_type_validation(
        spark,
        BRONZE_PRODUCTS_TABLE,
        "products",
        ("price", "cost", "stock_quantity"),
        product_type_checks(),
    )
    print_type_metrics(customer_metrics + order_metrics + product_metrics)
    LOGGER.info("Type validation completed; all Bronze rows retained")
    return customers_df, orders_df, products_df
