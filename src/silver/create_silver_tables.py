# Databricks notebook source
"""Build managed Silver tables and the consolidated quality metrics report."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from src.common.config import (
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
        QUALITY_METRICS_TABLE,
        SILVER_CUSTOMERS_TABLE,
        SILVER_ORDERS_TABLE,
        SILVER_PRODUCTS_TABLE,
        ensure_unity_storage,
    )
    from src.silver.business_logic import (
        CATALOG_PRICE_COL,
        PARENT_SIGNUP_COL,
        add_business_logic_check,
        customer_business_checks,
        enrich_orders_for_joins,
        order_business_checks,
        product_business_checks,
    )
    from src.silver.completeness import add_completeness_check
    from src.silver.referential import add_referential_integrity_check
    from src.silver.type_validation import (
        add_type_validation_check,
        customer_type_checks,
        order_type_checks,
        product_type_checks,
    )
    from src.silver.uniqueness import add_uniqueness_check
except ImportError:
    from common.config import (
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
        QUALITY_METRICS_TABLE,
        SILVER_CUSTOMERS_TABLE,
        SILVER_ORDERS_TABLE,
        SILVER_PRODUCTS_TABLE,
        ensure_unity_storage,
    )
    from silver.business_logic import (
        CATALOG_PRICE_COL,
        PARENT_SIGNUP_COL,
        add_business_logic_check,
        customer_business_checks,
        enrich_orders_for_joins,
        order_business_checks,
        product_business_checks,
    )
    from silver.completeness import add_completeness_check
    from silver.referential import add_referential_integrity_check
    from silver.type_validation import (
        add_type_validation_check,
        customer_type_checks,
        order_type_checks,
        product_type_checks,
    )
    from silver.uniqueness import add_uniqueness_check

LOGGER = logging.getLogger("silver.create_silver_tables")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

CUSTOMERS_REQUIRED_FIELDS = ("email",)
ORDERS_REQUIRED_FIELDS = ("customer_id", "product_id")

QUALITY_METRICS_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("field_checked", StringType(), False),
        StructField("total_rows", IntegerType(), False),
        StructField("applicable_rows", IntegerType(), False),
        StructField("passed", IntegerType(), False),
        StructField("failed", IntegerType(), False),
        StructField("pass_rate_pct", DoubleType(), False),
        StructField("threshold", DoubleType(), False),
        StructField("threshold_met", BooleanType(), False),
        StructField("batch_timestamp", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class MetricSpec:
    """Configuration for one consolidated quality metric."""

    check_name: str
    field_checked: str
    fail_condition: Column
    threshold: float
    applicable_condition: Column | None = None
    strict_threshold: bool = False


def load_managed_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Load a managed Unity Catalog table.

    Args:
        spark: Databricks runtime SparkSession.
        table_name: Three-level Unity Catalog table name.

    Returns:
        Loaded Spark DataFrame.

    Raises:
        Exception: Re-raised after logging if the read fails.
    """
    try:
        df = spark.table(table_name)
        LOGGER.info("Loaded managed table %s", table_name)
        return df
    except Exception:
        LOGGER.exception("Failed to load managed table %s", table_name)
        raise


def add_quality_check_result(df: DataFrame) -> DataFrame:
    """Assemble ordered category-level quality failure tokens.

    Business-logic failures map to ``TYPE_VALIDATION_FAIL`` rather than
    creating a fifth quality-check category.

    Args:
        df: DataFrame containing all detailed check columns.

    Returns:
        DataFrame with ``quality_check_result``.

    Raises:
        ValueError: If any required check column is absent.
    """
    required = (
        "completeness_check",
        "uniqueness_check",
        "type_validation_check",
        "business_logic_check",
        "referential_integrity_check",
    )
    missing = [name for name in required if name not in df.columns]
    if missing:
        raise ValueError(f"Cannot assemble quality result; missing: {missing}")

    fail_tokens = [
        F.when(
            F.col("completeness_check") != "PASS",
            F.lit("COMPLETENESS_FAIL"),
        ),
        F.when(
            F.col("uniqueness_check") != "PASS",
            F.lit("UNIQUENESS_FAIL"),
        ),
        F.when(
            (F.col("type_validation_check") != "PASS")
            | (F.col("business_logic_check") != "PASS"),
            F.lit("TYPE_VALIDATION_FAIL"),
        ),
        F.when(
            ~F.col("referential_integrity_check").isin("PASS", "N/A"),
            F.lit("REFERENTIAL_INTEGRITY_FAIL"),
        ),
    ]
    combined = F.concat_ws("|", *fail_tokens)
    return df.withColumn(
        "quality_check_result",
        F.when(combined == "", F.lit("PASS")).otherwise(combined),
    )


def build_customers_silver(customers_df: DataFrame) -> DataFrame:
    """Apply all applicable checks to Bronze customers.

    Args:
        customers_df: Bronze customers DataFrame.

    Returns:
        Customers Silver DataFrame with every Bronze row retained.
    """
    result = add_completeness_check(customers_df, CUSTOMERS_REQUIRED_FIELDS)
    result = add_uniqueness_check(result, "customer_id")
    result = add_type_validation_check(result, customer_type_checks())
    result = add_business_logic_check(result, customer_business_checks())
    result = result.withColumn("referential_integrity_check", F.lit("N/A"))
    return add_quality_check_result(result)


def build_products_silver(products_df: DataFrame) -> DataFrame:
    """Apply all applicable checks to Bronze products.

    Args:
        products_df: Bronze products DataFrame.

    Returns:
        Products Silver DataFrame with every Bronze row retained.
    """
    result = products_df.withColumn("completeness_check", F.lit("PASS"))
    result = add_uniqueness_check(result, "product_id")
    result = add_type_validation_check(result, product_type_checks())
    result = add_business_logic_check(result, product_business_checks())
    result = result.withColumn("referential_integrity_check", F.lit("N/A"))
    return add_quality_check_result(result)


def build_orders_silver(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Apply all checks to Bronze orders.

    Args:
        orders_df: Bronze orders DataFrame.
        customers_df: Bronze customers used as the customer parent.
        products_df: Bronze products used as the product parent.

    Returns:
        Tuple containing the final Orders Silver DataFrame and a metric-source
        DataFrame that retains temporary parent attributes.
    """
    result = add_completeness_check(orders_df, ORDERS_REQUIRED_FIELDS)
    result = add_uniqueness_check(result, "order_id")
    result = add_type_validation_check(result, order_type_checks())
    result = enrich_orders_for_joins(result, customers_df, products_df)
    result = add_business_logic_check(result, order_business_checks())
    result = add_referential_integrity_check(
        result,
        customers_df,
        products_df,
    )
    result = add_quality_check_result(result)
    final_df = result.drop(PARENT_SIGNUP_COL, CATALOG_PRICE_COL)
    return final_df, result


def collect_metric(
    df: DataFrame,
    table_name: str,
    spec: MetricSpec,
    batch_timestamp: datetime,
) -> tuple[Any, ...]:
    """Calculate one quality metric without removing source rows.

    Args:
        df: Checked DataFrame.
        table_name: Short table label.
        spec: Metric definition.
        batch_timestamp: Shared report timestamp.

    Returns:
        Tuple matching ``QUALITY_METRICS_SCHEMA``.
    """
    total_rows = int(df.count())
    applicable_condition = (
        spec.applicable_condition
        if spec.applicable_condition is not None
        else F.lit(True)
    )
    applicable_rows = int(df.filter(applicable_condition).count())
    failed = int(
        df.filter(applicable_condition & spec.fail_condition).count()
    )
    passed = applicable_rows - failed
    pass_rate_pct = (
        (passed / applicable_rows) * 100.0 if applicable_rows else 100.0
    )
    threshold_met = (
        pass_rate_pct > spec.threshold
        if spec.strict_threshold
        else pass_rate_pct >= spec.threshold
    )
    return (
        table_name,
        spec.check_name,
        spec.field_checked,
        total_rows,
        applicable_rows,
        passed,
        failed,
        pass_rate_pct,
        spec.threshold,
        threshold_met,
        batch_timestamp,
    )


def completeness_metric_specs(
    fields: tuple[str, ...],
) -> list[MetricSpec]:
    """Create per-field completeness metric definitions.

    Args:
        fields: Required fields represented by detailed completeness tokens.

    Returns:
        Completeness metric definitions.
    """
    return [
        MetricSpec(
            check_name="Completeness",
            field_checked=field,
            fail_condition=F.col("completeness_check").contains(
                f"FAIL_NULL_{field}"
            ),
            threshold=99.0,
            strict_threshold=True,
        )
        for field in fields
    ]


def uniqueness_metric_spec(key_column: str) -> MetricSpec:
    """Create a primary-key uniqueness metric definition.

    Args:
        key_column: Primary-key column.

    Returns:
        Uniqueness metric definition.
    """
    return MetricSpec(
        check_name="Uniqueness",
        field_checked=key_column,
        fail_condition=F.col("uniqueness_check").contains(
            f"FAIL_DUPLICATE_{key_column}"
        ),
        applicable_condition=F.col(key_column).isNotNull(),
        threshold=100.0,
    )


def detailed_type_metric_specs() -> dict[str, list[MetricSpec]]:
    """Create metrics for every type and extra business rule.

    Returns:
        Metric definitions keyed by table label.
    """
    type_checks = {
        "customers": customer_type_checks(),
        "orders": order_type_checks(),
        "products": product_type_checks(),
    }
    business_checks = {
        "customers": customer_business_checks(),
        "orders": order_business_checks(),
        "products": product_business_checks(),
    }
    result: dict[str, list[MetricSpec]] = {}
    for table_name, checks in type_checks.items():
        result[table_name] = [
            MetricSpec(
                check_name=f"Type validation ({check.check_type})",
                field_checked=check.check_name,
                fail_condition=check.fail_condition,
                threshold=100.0,
            )
            for check in checks
        ]
    for table_name, checks in business_checks.items():
        result[table_name].extend(
            MetricSpec(
                check_name="Type validation (business)",
                field_checked=check.check_name,
                fail_condition=check.fail_condition,
                threshold=100.0,
            )
            for check in checks
        )
    return result


def referential_metric_specs() -> list[MetricSpec]:
    """Create non-null-FK referential metric definitions.

    Returns:
        Customer and product FK metric definitions.
    """
    return [
        MetricSpec(
            check_name="Referential integrity",
            field_checked=field,
            fail_condition=F.col("referential_integrity_check").contains(token),
            applicable_condition=F.col(field).isNotNull(),
            threshold=99.9,
            strict_threshold=True,
        )
        for field, token in (
            ("customer_id", "FAIL_ORPHAN_customer_id"),
            ("product_id", "FAIL_ORPHAN_product_id"),
        )
    ]


def overall_metric_spec() -> MetricSpec:
    """Create the overall clean-row metric definition.

    Returns:
        Overall quality metric definition.
    """
    return MetricSpec(
        check_name="Overall quality",
        field_checked="all_checks",
        fail_condition=F.col("quality_check_result") != "PASS",
        threshold=100.0,
    )


def build_quality_metrics(
    spark: SparkSession,
    customers_df: DataFrame,
    orders_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Build the consolidated quality report.

    Args:
        spark: Databricks runtime SparkSession.
        customers_df: Checked customers Silver DataFrame.
        orders_df: Checked orders DataFrame retaining business helper columns.
        products_df: Checked products Silver DataFrame.

    Returns:
        Quality metrics DataFrame.
    """
    batch_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
    type_specs = detailed_type_metric_specs()
    specs = {
        "customers": (
            completeness_metric_specs(CUSTOMERS_REQUIRED_FIELDS)
            + [uniqueness_metric_spec("customer_id")]
            + type_specs["customers"]
            + [overall_metric_spec()]
        ),
        "orders": (
            completeness_metric_specs(ORDERS_REQUIRED_FIELDS)
            + [uniqueness_metric_spec("order_id")]
            + type_specs["orders"]
            + referential_metric_specs()
            + [overall_metric_spec()]
        ),
        "products": (
            [uniqueness_metric_spec("product_id")]
            + type_specs["products"]
            + [overall_metric_spec()]
        ),
    }
    frames = {
        "customers": customers_df,
        "orders": orders_df,
        "products": products_df,
    }
    rows = [
        collect_metric(
            frames[table_name],
            table_name,
            metric_spec,
            batch_timestamp,
        )
        for table_name, table_specs in specs.items()
        for metric_spec in table_specs
    ]
    return spark.createDataFrame(rows, schema=QUALITY_METRICS_SCHEMA)


def write_managed_delta_table(
    df: DataFrame,
    table_name: str,
    expected_rows: int,
) -> None:
    """Overwrite a managed Delta table and validate its row count.

    Args:
        df: DataFrame to persist.
        table_name: Three-level managed table name.
        expected_rows: Required row count after writing.

    Raises:
        ValueError: If the persisted count differs from ``expected_rows``.
        Exception: Re-raised after logging if the write fails.
    """
    try:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        actual_rows = int(df.sparkSession.table(table_name).count())
        if actual_rows != expected_rows:
            raise ValueError(
                f"Row-count mismatch for {table_name}: "
                f"expected={expected_rows}, actual={actual_rows}"
            )
        LOGGER.info("Wrote managed table %s (%s rows)", table_name, actual_rows)
    except Exception:
        LOGGER.exception("Failed to write managed table %s", table_name)
        raise


def print_quality_report(metrics_df: DataFrame) -> None:
    """Print per-check and overall clean-row quality summaries.

    Args:
        metrics_df: Consolidated quality metrics DataFrame.
    """
    print("\n=== DATA QUALITY REPORT ===")
    (
        metrics_df.orderBy("table_name", "check_name", "field_checked")
        .select(
            "table_name",
            "check_name",
            "field_checked",
            "total_rows",
            "applicable_rows",
            "passed",
            "failed",
            F.round("pass_rate_pct", 2).alias("pass_rate_pct"),
            "threshold_met",
        )
        .show(100, truncate=False)
    )
    print("=== OVERALL CLEAN ROWS ===")
    (
        metrics_df.filter(F.col("check_name") == "Overall quality")
        .select(
            "table_name",
            F.col("total_rows").alias("total"),
            F.col("passed").alias("clean_pass"),
            F.col("failed").alias("flagged"),
            F.round("pass_rate_pct", 2).alias("clean_rate_pct"),
        )
        .orderBy("table_name")
        .show(truncate=False)
    )


def create_silver_tables(spark: SparkSession) -> str:
    """Build Silver tables and the managed quality metrics table.

    Args:
        spark: Databricks runtime SparkSession.

    Returns:
        ``SUCCESS`` after all tables and metrics are validated and written.

    Raises:
        Exception: If a read, quality check, validation, or write fails.
    """
    LOGGER.info("Silver orchestration started")
    ensure_unity_storage(spark)

    customers_bronze = load_managed_table(spark, BRONZE_CUSTOMERS_TABLE)
    orders_bronze = load_managed_table(spark, BRONZE_ORDERS_TABLE)
    products_bronze = load_managed_table(spark, BRONZE_PRODUCTS_TABLE)
    bronze_counts = {
        "customers": int(customers_bronze.count()),
        "orders": int(orders_bronze.count()),
        "products": int(products_bronze.count()),
    }

    customers_silver = build_customers_silver(customers_bronze)
    orders_silver, orders_metric_source = build_orders_silver(
        orders_bronze,
        customers_bronze,
        products_bronze,
    )
    products_silver = build_products_silver(products_bronze)

    silver_frames = {
        "customers": customers_silver,
        "orders": orders_silver,
        "products": products_silver,
    }
    for name, silver_df in silver_frames.items():
        actual_rows = int(silver_df.count())
        if actual_rows != bronze_counts[name]:
            raise ValueError(
                f"{name} retention failed before write: "
                f"bronze={bronze_counts[name]}, silver={actual_rows}"
            )

    metrics_df = build_quality_metrics(
        spark,
        customers_silver,
        orders_metric_source,
        products_silver,
    )
    print_quality_report(metrics_df)

    write_managed_delta_table(
        customers_silver,
        SILVER_CUSTOMERS_TABLE,
        bronze_counts["customers"],
    )
    write_managed_delta_table(
        orders_silver,
        SILVER_ORDERS_TABLE,
        bronze_counts["orders"],
    )
    write_managed_delta_table(
        products_silver,
        SILVER_PRODUCTS_TABLE,
        bronze_counts["products"],
    )
    metrics_count = int(metrics_df.count())
    write_managed_delta_table(
        metrics_df,
        QUALITY_METRICS_TABLE,
        metrics_count,
    )

    LOGGER.info(
        "Silver orchestration completed successfully; all Bronze rows retained"
    )
    return "SUCCESS"


create_silver_tables(spark)
