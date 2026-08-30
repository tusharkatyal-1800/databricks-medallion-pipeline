"""Validate generated e-commerce CSVs against planted quality-issue counts.

Loads ``./data/customers.csv``, ``./data/orders.csv``, and
``./data/products.csv``, then reports row counts, nulls, duplicate keys,
orphans, and an expected-vs-actual comparison table.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

DATA_DIR = Path("./data")
CUSTOMER_PATH = DATA_DIR / "customers.csv"
ORDER_PATH = DATA_DIR / "orders.csv"
PRODUCT_PATH = DATA_DIR / "products.csv"

EXPECTED_ROW_COUNTS = {
    "customers": 10_000,
    "orders": 100_000,
    "products": 500,
}

# Extra planted duplicate *rows* (keep='first'), matching the generator spec.
EXPECTED_ISSUES = [
    ("customers", "NULL emails", 50),
    ("customers", "duplicate customer_ids (extra rows)", 10),
    ("orders", "NULL customer_ids", 100),
    ("orders", "NULL product_ids", 200),
    ("orders", "orphan customer_ids (planted 90001-99000)", 50),
    ("orders", "orphan product_ids (planted 9001-9500)", 30),
    ("orders", "duplicate order_ids (extra rows)", 20),
]

LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Configure a simple stdout logger for the validator."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_csv(path: Path) -> pd.DataFrame:
    """Load a generated CSV, keeping empty fields as missing values.

    Args:
        path: CSV location under ``./data``.

    Returns:
        DataFrame with empty strings treated as NA.

    Raises:
        FileNotFoundError: If the file does not exist.
        OSError: If the file cannot be read.
    """
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    try:
        df = pd.read_csv(path, keep_default_na=True)
        LOGGER.info("Loaded %s rows from %s", len(df), path)
        return df
    except OSError:
        LOGGER.exception("Failed to read CSV from %s", path)
        raise


def extra_duplicate_rows(series: pd.Series) -> int:
    """Count extra rows that reuse a non-null primary key.

    The first occurrence of each key is treated as valid; later copies are
    the planted duplicates (10 customers, 20 orders).

    Args:
        series: Primary-key column.

    Returns:
        Count of duplicate extra rows (``duplicated(keep='first')``).
    """
    return int(series.dropna().duplicated(keep="first").sum())


def duplicate_group_rows(series: pd.Series) -> int:
    """Count all rows that belong to a duplicate-key group.

    Args:
        series: Primary-key column.

    Returns:
        Rows whose key appears more than once (excluding NA).
    """
    valid = series.dropna()
    return int(valid.duplicated(keep=False).sum())


def count_orphans(
    child: pd.Series,
    parent_ids: pd.Series,
) -> int:
    """Count non-null foreign keys that are missing from the parent table.

    Args:
        child: Foreign-key column on the child table.
        parent_ids: Parent primary-key column.

    Returns:
        Number of orphan keys.
    """
    parent_set = set(parent_ids.dropna().astype("int64"))
    child_valid = child.dropna().astype("int64")
    return int((~child_valid.isin(parent_set)).sum())


def count_ids_in_range(series: pd.Series, low: int, high: int) -> int:
    """Count non-null integer values inside an inclusive range.

    Args:
        series: Foreign-key column.
        low: Inclusive lower bound.
        high: Inclusive upper bound.

    Returns:
        Number of values in ``[low, high]``.
    """
    valid = series.dropna().astype("int64")
    return int(((valid >= low) & (valid <= high)).sum())


def format_null_table(name: str, df: pd.DataFrame) -> str:
    """Build a per-column null-count listing for one table.

    Args:
        name: Logical table name.
        df: Loaded DataFrame.

    Returns:
        Multi-line string of column null counts.
    """
    lines = [f"NULL counts ({name}):"]
    nulls = df.isna().sum()
    for column, count in nulls.items():
        lines.append(f"  {column:20} {int(count)}")
    return "\n".join(lines)


def print_comparison_table(actual: dict[tuple[str, str], int]) -> bool:
    """Print expected vs actual planted-issue counts.

    Args:
        actual: Mapping of (table, issue) to measured count.

    Returns:
        True if every expected count matches actual.
    """
    header = (
        f"{'Table':<12} {'Issue':<40} {'Expected':>10} "
        f"{'Actual':>10} {'Match':>8}"
    )
    divider = "-" * len(header)
    lines = [
        "",
        "========== Expected vs actual quality issues ==========",
        header,
        divider,
    ]
    all_ok = True
    for table, issue, expected in EXPECTED_ISSUES:
        value = actual[(table, issue)]
        match = value == expected
        all_ok = all_ok and match
        lines.append(
            f"{table:<12} {issue:<40} {expected:>10} "
            f"{value:>10} {'OK' if match else 'FAIL':>8}"
        )
    lines.extend([divider, ""])
    print("\n".join(lines))
    return all_ok


def main() -> int:
    """Validate generated CSVs and print a quality-issue comparison.

    Returns:
        0 if all expected issue counts and row counts match; 1 otherwise.
    """
    _configure_logging()
    try:
        customers = load_csv(CUSTOMER_PATH)
        orders = load_csv(ORDER_PATH)
        products = load_csv(PRODUCT_PATH)
    except (OSError, FileNotFoundError):
        LOGGER.exception("Unable to load generated CSV files from %s", DATA_DIR)
        raise

    row_ok = True
    print("")
    print("========== Row counts ==========")
    for name, df, expected in (
        ("customers", customers, EXPECTED_ROW_COUNTS["customers"]),
        ("orders", orders, EXPECTED_ROW_COUNTS["orders"]),
        ("products", products, EXPECTED_ROW_COUNTS["products"]),
    ):
        actual_rows = len(df)
        match = actual_rows == expected
        row_ok = row_ok and match
        print(
            f"  {name:<12} expected={expected:<8} actual={actual_rows:<8} "
            f"{'OK' if match else 'FAIL'}"
        )
    print("")
    print(format_null_table("customers", customers))
    print(format_null_table("orders", orders))
    print(format_null_table("products", products))
    print("")

    cust_dup_extra = extra_duplicate_rows(customers["customer_id"])
    order_dup_extra = extra_duplicate_rows(orders["order_id"])
    print("========== Primary-key duplicates ==========")
    print(
        f"  customers.customer_id extra rows: {cust_dup_extra} "
        f"(group rows={duplicate_group_rows(customers['customer_id'])})"
    )
    print(
        f"  orders.order_id extra rows:         {order_dup_extra} "
        f"(group rows={duplicate_group_rows(orders['order_id'])})"
    )
    print(
        f"  products.product_id extra rows:   "
        f"{extra_duplicate_rows(products['product_id'])}"
    )
    print("")

    orphan_customers_all = count_orphans(
        orders["customer_id"],
        customers["customer_id"],
    )
    orphan_products_all = count_orphans(
        orders["product_id"],
        products["product_id"],
    )
    planted_orphan_customers = count_ids_in_range(
        orders["customer_id"],
        90001,
        99000,
    )
    planted_orphan_products = count_ids_in_range(
        orders["product_id"],
        9001,
        9500,
    )
    print("========== Orphan foreign keys ==========")
    print(
        f"  planted orphan customer_id (90001-99000): {planted_orphan_customers}"
    )
    print(
        f"  planted orphan product_id (9001-9500):    {planted_orphan_products}"
    )
    print(
        f"  total customer_id not in customers:         {orphan_customers_all}"
    )
    print(
        f"  total product_id not in products:          {orphan_products_all}"
    )
    print(
        "  Note: extra customer orphans come from rows 9941-9950 reusing "
        "customer_id 1-10, so ids 9941-9950 no longer exist as parents."
    )

    actual = {
        ("customers", "NULL emails"): int(customers["email"].isna().sum()),
        ("customers", "duplicate customer_ids (extra rows)"): cust_dup_extra,
        ("orders", "NULL customer_ids"): int(orders["customer_id"].isna().sum()),
        ("orders", "NULL product_ids"): int(orders["product_id"].isna().sum()),
        ("orders", "orphan customer_ids (planted 90001-99000)"): (
            planted_orphan_customers
        ),
        ("orders", "orphan product_ids (planted 9001-9500)"): (
            planted_orphan_products
        ),
        ("orders", "duplicate order_ids (extra rows)"): order_dup_extra,
    }
    issues_ok = print_comparison_table(actual)

    if row_ok and issues_ok:
        print("Validation PASSED: row counts and planted issues match.")
        LOGGER.info("Validation passed")
        return 0

    LOGGER.error("Validation FAILED: see expected vs actual table above")
    print("Validation FAILED.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
