"""Extra e-commerce consistency rules for Bronze tables.

These rules complement type validation. Failures map to
``TYPE_VALIDATION_FAIL`` when Silver results are assembled. Rows are
never dropped. Null FKs and missing parents are not scored on join rules.
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

PRICE_TOLERANCE = 0.01
PARENT_SIGNUP_COL = "_parent_signup_date"
CATALOG_PRICE_COL = "_catalog_price"


@dataclass(frozen=True)
class BusinessCheck:
    """One extra consistency rule."""

    check_name: str
    fail_condition: Column


@dataclass(frozen=True)
class BusinessLogicMetric:
    """Fail counts for one named consistency rule."""

    table_name: str
    check_name: str
    total_rows: int
    fail_count: int
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
        raise ValueError(f"Missing columns for business logic: {missing}")


def _present_and(condition: Column, *fields: str) -> Column:
    """AND a rule with non-null guards so completeness owns missing values.

    Args:
        condition: Fail condition.
        *fields: Columns that must be non-null for the rule to apply.

    Returns:
        Fail condition that is false when any listed field is null.
    """
    present = F.lit(True)
    for field in fields:
        present = present & F.col(field).isNotNull()
    return present & condition


def customer_business_checks() -> tuple[BusinessCheck, ...]:
    """Return intra-row customer consistency checks.

    Returns:
        Customer check definitions.
    """
    invalid_ltv = F.col("lifetime_value") < 0
    return (
        BusinessCheck(
            "lifetime_value",
            _present_and(invalid_ltv, "lifetime_value"),
        ),
    )


def product_business_checks() -> tuple[BusinessCheck, ...]:
    """Return intra-row product consistency checks.

    Returns:
        Product check definitions.
    """
    invalid_reorder = F.col("reorder_level") < 0
    return (
        BusinessCheck(
            "reorder_level",
            _present_and(invalid_reorder, "reorder_level"),
        ),
    )


def order_business_checks() -> tuple[BusinessCheck, ...]:
    """Return order intra-row and join-based consistency checks.

    Join helper columns ``_parent_signup_date`` and ``_catalog_price``
    must already exist on the DataFrame.

    Returns:
        Order check definitions.
    """
    cancelled_has_payment = (F.col("order_status") == "Cancelled") & F.col(
        "payment_date"
    ).isNotNull()
    future_payment = F.col("payment_date") > F.current_date()
    order_before_signup = F.col("order_date") < F.col(PARENT_SIGNUP_COL)
    price_delta = F.abs(F.col("unit_price") - F.col(CATALOG_PRICE_COL))
    price_tolerance = F.abs(F.col(CATALOG_PRICE_COL)) * F.lit(PRICE_TOLERANCE)
    catalog_mismatch = price_delta > price_tolerance
    return (
        BusinessCheck("cancelled_payment_date", cancelled_has_payment),
        BusinessCheck(
            "future_payment_date",
            _present_and(future_payment, "payment_date"),
        ),
        BusinessCheck(
            "order_before_signup",
            _present_and(
                order_before_signup,
                "customer_id",
                "order_date",
                PARENT_SIGNUP_COL,
            ),
        ),
        BusinessCheck(
            "unit_price_catalog",
            _present_and(
                catalog_mismatch,
                "product_id",
                "unit_price",
                CATALOG_PRICE_COL,
            ),
        ),
    )


def add_business_logic_check(
    df: DataFrame,
    checks: tuple[BusinessCheck, ...],
) -> DataFrame:
    """Add ``business_logic_check`` from named fail conditions.

    Args:
        df: Input Spark DataFrame. All rows are retained.
        checks: Ordered checks whose fail tokens are pipe-joined.

    Returns:
        DataFrame with ``business_logic_check``:
        ``PASS`` or ``FAIL_INVALID_<check_name>`` tokens joined by ``|``.
    """
    fail_tokens = [
        F.when(check.fail_condition, F.lit(f"FAIL_INVALID_{check.check_name}"))
        for check in checks
    ]
    combined_flags = F.concat_ws("|", *fail_tokens)
    return df.withColumn(
        "business_logic_check",
        F.when(combined_flags == "", F.lit("PASS")).otherwise(combined_flags),
    )


def compute_business_metrics(
    df: DataFrame,
    table_name: str,
    checks: tuple[BusinessCheck, ...],
) -> list[BusinessLogicMetric]:
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
        metrics: list[BusinessLogicMetric] = []
        for check in checks:
            fail_count = int(counts[check.check_name] or 0)
            pass_count = total_rows - fail_count
            pass_pct = (pass_count / total_rows) * 100.0 if total_rows else 0.0
            metrics.append(
                BusinessLogicMetric(
                    table_name=table_name,
                    check_name=check.check_name,
                    total_rows=total_rows,
                    fail_count=fail_count,
                    pass_pct=pass_pct,
                )
            )
        return metrics
    except Exception:
        LOGGER.exception(
            "Failed to compute business-logic metrics for %s", table_name
        )
        raise


def print_business_metrics(metrics: list[BusinessLogicMetric]) -> None:
    """Print business-logic metrics as a formatted table.

    Args:
        metrics: Rows to display.
    """
    print("\n========== Business logic metrics ==========")
    print(
        f"| {'Table':<12} | {'Check':<26} | {'Total':>10} | "
        f"{'Fails':>8} | {'Pass %':>10} |"
    )
    print(f"|{'-' * 14}|{'-' * 28}|{'-' * 12}|{'-' * 10}|{'-' * 12}|")
    for row in metrics:
        print(
            f"| {row.table_name:<12} | {row.check_name:<26} | "
            f"{row.total_rows:>10,} | {row.fail_count:>8,} | "
            f"{row.pass_pct:>9.2f}% |"
        )
    print("=============================================\n")


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


def distinct_parents(
    parent_df: DataFrame,
    key_column: str,
    value_column: str,
    alias: str,
) -> DataFrame:
    """Return one parent row per key so joins do not explode child rows.

    Args:
        parent_df: Parent Bronze DataFrame.
        key_column: Join key.
        value_column: Attribute needed by the rule.
        alias: Output name for the attribute.

    Returns:
        Distinct key/attribute DataFrame.
    """
    return (
        parent_df.where(F.col(key_column).isNotNull())
        .select(key_column, F.col(value_column).alias(alias))
        .dropDuplicates([key_column])
    )


def enrich_orders_for_joins(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Left-join parent signup date and catalog price onto orders.

    Duplicate parent keys are collapsed so order grain is preserved.
    Null FKs and orphans leave helper columns null and therefore PASS.

    Args:
        orders_df: Bronze orders.
        customers_df: Bronze customers.
        products_df: Bronze products.

    Returns:
        Orders plus ``_parent_signup_date`` and ``_catalog_price``.
    """
    customer_parents = distinct_parents(
        customers_df,
        "customer_id",
        "signup_date",
        PARENT_SIGNUP_COL,
    )
    product_parents = distinct_parents(
        products_df,
        "product_id",
        "price",
        CATALOG_PRICE_COL,
    )
    return orders_df.join(customer_parents, on="customer_id", how="left").join(
        product_parents,
        on="product_id",
        how="left",
    )


def apply_customer_business_logic(
    customers_df: DataFrame,
) -> tuple[DataFrame, list[BusinessLogicMetric]]:
    """Flag customer lifetime-value consistency.

    Args:
        customers_df: Bronze customers.

    Returns:
        Flagged DataFrame and metrics. Row count is unchanged.
    """
    validate_columns(customers_df, ("lifetime_value",))
    checks = customer_business_checks()
    flagged_df = add_business_logic_check(customers_df, checks)
    metrics = compute_business_metrics(flagged_df, "customers", checks)
    return flagged_df, metrics


def apply_product_business_logic(
    products_df: DataFrame,
) -> tuple[DataFrame, list[BusinessLogicMetric]]:
    """Flag product reorder-level consistency.

    Args:
        products_df: Bronze products.

    Returns:
        Flagged DataFrame and metrics. Row count is unchanged.
    """
    validate_columns(products_df, ("reorder_level",))
    checks = product_business_checks()
    flagged_df = add_business_logic_check(products_df, checks)
    metrics = compute_business_metrics(flagged_df, "products", checks)
    return flagged_df, metrics


def apply_order_business_logic(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> tuple[DataFrame, list[BusinessLogicMetric]]:
    """Flag order payment and cross-table consistency rules.

    Args:
        orders_df: Bronze orders.
        customers_df: Bronze customers used for signup comparison.
        products_df: Bronze products used for catalog price comparison.

    Returns:
        Flagged orders without join helper columns. Row count is unchanged.
    """
    validate_columns(
        orders_df,
        ("customer_id", "product_id", "order_date", "unit_price",
         "order_status", "payment_date"),
    )
    validate_columns(customers_df, ("customer_id", "signup_date"))
    validate_columns(products_df, ("product_id", "price"))
    enriched_df = enrich_orders_for_joins(orders_df, customers_df, products_df)
    checks = order_business_checks()
    flagged_df = add_business_logic_check(enriched_df, checks)
    metrics = compute_business_metrics(flagged_df, "orders", checks)
    return flagged_df.drop(PARENT_SIGNUP_COL, CATALOG_PRICE_COL), metrics


def run_business_logic_checks(
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Run extra consistency checks on all three Bronze tables.

    Args:
        spark: Databricks notebook SparkSession.

    Returns:
        Tuple of (customers_df, orders_df, products_df) with
        ``business_logic_check``. No rows are dropped.

    Raises:
        Exception: Re-raised after logging if a table read or check fails.
    """
    LOGGER.info("Starting extra business-logic checks on Bronze tables")
    customers_df = load_bronze_table(spark, BRONZE_CUSTOMERS_TABLE)
    orders_df = load_bronze_table(spark, BRONZE_ORDERS_TABLE)
    products_df = load_bronze_table(spark, BRONZE_PRODUCTS_TABLE)

    customers_flagged, customer_metrics = apply_customer_business_logic(
        customers_df
    )
    products_flagged, product_metrics = apply_product_business_logic(products_df)
    orders_flagged, order_metrics = apply_order_business_logic(
        orders_df,
        customers_df,
        products_df,
    )
    print_business_metrics(customer_metrics + order_metrics + product_metrics)
    LOGGER.info(
        "Business-logic checks completed; all Bronze rows retained"
    )
    return customers_flagged, orders_flagged, products_flagged
