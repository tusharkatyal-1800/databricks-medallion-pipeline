"""Generate three e-commerce CSV extracts with planted data-quality issues.

Outputs:
    ./data/customers.csv
    ./data/orders.csv
    ./data/products.csv

The random seed is fixed at 42 so re-runs produce the same files.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

RANDOM_SEED = 42
OUTPUT_DIR = Path("./data")

CUSTOMER_COUNT = 10_000
ORDER_COUNT = 100_000
PRODUCT_COUNT = 500

COUNTRY_CHOICES = [
    "USA",
    "UK",
    "Germany",
    "India",
    "Canada",
    "Australia",
    "France",
    "Japan",
    "Brazil",
    "Mexico",
    "Spain",
    "Italy",
    "Netherlands",
    "Singapore",
    "UAE",
]
COUNTRY_WEIGHTS = np.array(
    [
        0.40,
        0.15,
        0.10,
        0.10,
        0.04,
        0.03,
        0.03,
        0.03,
        0.02,
        0.02,
        0.02,
        0.02,
        0.02,
        0.02,
        0.02,
    ],
    dtype=float,
)
COUNTRY_WEIGHTS = COUNTRY_WEIGHTS / COUNTRY_WEIGHTS.sum()

EMAIL_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "protonmail.com",
]

SEGMENTS = ["Premium", "Standard", "Basic"]
SEGMENT_WEIGHTS = np.array([0.20, 0.50, 0.30], dtype=float)
SEGMENT_WEIGHTS = SEGMENT_WEIGHTS / SEGMENT_WEIGHTS.sum()
SEGMENT_LTV_RANGES = {
    "Premium": (500.00, 5000.00),
    "Standard": (100.00, 500.00),
    "Basic": (10.00, 100.00),
}

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Books",
    "Sports",
    "Beauty",
    "Toys",
    "Food & Beverage",
]
CATEGORY_PRICE_RANGES = {
    "Electronics": (49.99, 2000.00),
    "Clothing": (15.00, 250.00),
    "Home & Garden": (12.00, 800.00),
    "Books": (5.00, 80.00),
    "Sports": (10.00, 600.00),
    "Beauty": (8.00, 180.00),
    "Toys": (8.00, 150.00),
    "Food & Beverage": (5.00, 75.00),
}
PRODUCT_NAME_TEMPLATES = {
    "Electronics": [
        "Wireless Bluetooth Headphones",
        "4K Streaming Media Player",
        "USB-C Laptop Charger",
        "Noise-Cancelling Earbuds",
        "Portable Power Bank",
        "Smart Home Speaker",
        "Wireless Mouse and Keyboard Kit",
        "LED Monitor Stand",
    ],
    "Clothing": [
        "Classic Cotton T-Shirt",
        "Slim Fit Denim Jeans",
        "Waterproof Hiking Jacket",
        "Merino Wool Sweater",
        "Running Shorts",
        "Linen Button-Down Shirt",
        "Knit Beanie",
        "Casual Canvas Sneakers",
    ],
    "Home & Garden": [
        "Ceramic Table Lamp",
        "Stainless Steel Cookware Set",
        "Indoor Herb Garden Kit",
        "Memory Foam Pillow",
        "Bamboo Cutting Board",
        "Outdoor String Lights",
        "Cotton Throw Blanket",
        "Cast Iron Skillet",
    ],
    "Books": [
        "Hardcover Mystery Novel",
        "Beginner Python Workbook",
        "Travel Photography Guide",
        "Paperback Historical Fiction",
        "Children's Picture Book",
        "Personal Finance Handbook",
        "Science Fiction Omnibus",
        "World Atlas",
    ],
    "Sports": [
        "Yoga Mat with Carry Strap",
        "Adjustable Dumbbell Pair",
        "Insulated Water Bottle",
        "Trail Running Shoes",
        "Resistance Band Set",
        "Soccer Training Ball",
        "Cycling Gloves",
        "Tennis Racket",
    ],
    "Beauty": [
        "Vitamin C Face Serum",
        "Mineral Sunscreen SPF 50",
        "Hydrating Lip Balm Set",
        "Gentle Foaming Cleanser",
        "Hair Repair Oil",
        "Makeup Brush Kit",
        "Aloe After-Sun Gel",
        "Nourishing Night Cream",
    ],
    "Toys": [
        "Wooden Building Block Set",
        "Remote Control Race Car",
        "Plush Stuffed Animal",
        "Strategy Board Game",
        "STEM Robot Kit",
        "Puzzle 1000 Pieces",
        "Bubble Machine",
        "Play Kitchen Accessories",
    ],
    "Food & Beverage": [
        "Organic Coffee Beans",
        "Dark Chocolate Gift Box",
        "Sparkling Mineral Water Pack",
        "Gourmet Pasta Sauce",
        "Mixed Nuts Variety Pack",
        "Herbal Tea Sampler",
        "Extra Virgin Olive Oil",
        "Honey and Granola Bundle",
    ],
}

ORDER_STATUSES = ["Completed", "Pending", "Cancelled"]
ORDER_STATUS_WEIGHTS = np.array([0.60, 0.25, 0.15], dtype=float)
ORDER_STATUS_WEIGHTS = ORDER_STATUS_WEIGHTS / ORDER_STATUS_WEIGHTS.sum()

SIGNUP_YEAR_CHOICES = [2020, 2021, 2022, 2023, 2024, 2025]
SIGNUP_YEAR_WEIGHTS = np.array([0.08, 0.10, 0.14, 0.18, 0.28, 0.22], dtype=float)
SIGNUP_YEAR_WEIGHTS = SIGNUP_YEAR_WEIGHTS / SIGNUP_YEAR_WEIGHTS.sum()

LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Configure a simple stdout logger for the generator."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _slug_name_part(value: str) -> str:
    """Return a lowercase alphabetic token suitable for an email local-part.

    Args:
        value: Raw first or last name from Faker.

    Returns:
        Letters-only lowercase string, or an empty string if none remain.
    """
    return re.sub(r"[^a-z]", "", value.lower())


def _random_date_in_year(
    rng: np.random.Generator,
    year: int,
    start_bound: date | None = None,
    end_bound: date | None = None,
) -> date:
    """Pick a uniform calendar date inside a year, clipped to optional bounds.

    Args:
        rng: NumPy random generator.
        year: Calendar year.
        start_bound: Inclusive earliest allowed date.
        end_bound: Inclusive latest allowed date.

    Returns:
        A date within the clipped year range.

    Raises:
        ValueError: If the clipped range is empty.
    """
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    if start_bound is not None:
        year_start = max(year_start, start_bound)
    if end_bound is not None:
        year_end = min(year_end, end_bound)
    if year_start > year_end:
        raise ValueError(f"Empty date range for year {year}")
    offset = int(rng.integers(0, (year_end - year_start).days + 1))
    return year_start + timedelta(days=offset)


def generate_customers(fake: Faker, rng: np.random.Generator) -> pd.DataFrame:
    """Build 10,000 customers with realistic names, emails, and segments.

    Args:
        fake: Seeded Faker instance.
        rng: Seeded NumPy generator.

    Returns:
        Customer DataFrame before intentional quality issues.
    """
    countries = rng.choice(COUNTRY_CHOICES, size=CUSTOMER_COUNT, p=COUNTRY_WEIGHTS)
    segments = rng.choice(SEGMENTS, size=CUSTOMER_COUNT, p=SEGMENT_WEIGHTS)
    signup_years = rng.choice(
        SIGNUP_YEAR_CHOICES,
        size=CUSTOMER_COUNT,
        p=SIGNUP_YEAR_WEIGHTS,
    )

    records = []
    used_emails: set[str] = set()
    for idx in range(CUSTOMER_COUNT):
        customer_id = idx + 1
        first_name = fake.first_name()
        last_name = fake.last_name()
        customer_name = f"{first_name} {last_name}"
        local_first = _slug_name_part(first_name) or f"user{customer_id}"
        local_last = _slug_name_part(last_name) or f"customer{customer_id}"
        domain = EMAIL_DOMAINS[int(rng.integers(0, len(EMAIL_DOMAINS)))]
        email = f"{local_first}.{local_last}@{domain}"
        suffix = 1
        while email in used_emails:
            email = f"{local_first}.{local_last}{suffix}@{domain}"
            suffix += 1
        used_emails.add(email)

        segment = str(segments[idx])
        low, high = SEGMENT_LTV_RANGES[segment]
        lifetime_value = round(float(rng.uniform(low, high)), 2)
        signup_date = _random_date_in_year(
            rng,
            int(signup_years[idx]),
            start_bound=date(2020, 1, 1),
            end_bound=date(2025, 12, 31),
        )
        records.append(
            {
                "customer_id": customer_id,
                "customer_name": customer_name,
                "email": email,
                "country": str(countries[idx]),
                "signup_date": signup_date.isoformat(),
                "customer_segment": segment,
                "lifetime_value": lifetime_value,
            }
        )
    return pd.DataFrame.from_records(records)


def apply_customer_quality_issues(customers: pd.DataFrame) -> pd.DataFrame:
    """Plant documented completeness and uniqueness issues on customers.

    Args:
        customers: Clean generated customer frame (10,000 rows).

    Returns:
        Copy of the frame with planted defects.
    """
    df = customers.copy()

    # Quality issue: uniqueness — rows 9941-9950 reuse customer_id values
    # from rows 1-10 so 10 keys appear twice. Other attributes stay as generated.
    duplicate_source_ids = list(range(1, 11))
    df.loc[9940:9949, "customer_id"] = duplicate_source_ids

    # Quality issue: completeness — rows 9951-10000 have NULL email (50 rows).
    df.loc[9950:9999, "email"] = pd.NA

    return df


def generate_products(rng: np.random.Generator) -> pd.DataFrame:
    """Build 500 products with category-correlated prices and costs.

    Args:
        rng: Seeded NumPy generator.

    Returns:
        Product DataFrame.
    """
    records = []
    for product_id in range(1, PRODUCT_COUNT + 1):
        category = CATEGORIES[(product_id - 1) % len(CATEGORIES)]
        templates = PRODUCT_NAME_TEMPLATES[category]
        base_name = templates[(product_id - 1) // len(CATEGORIES) % len(templates)]
        product_name = f"{base_name} {product_id}"
        low, high = CATEGORY_PRICE_RANGES[category]
        price = round(float(rng.uniform(low, high)), 2)
        cost = round(price * float(rng.uniform(0.3, 0.7)), 2)
        stock_quantity = int(rng.integers(0, 5001))
        reorder_level = int(rng.integers(10, 201))
        records.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "price": price,
                "cost": cost,
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
            }
        )
    return pd.DataFrame.from_records(records)


def generate_orders(
    rng: np.random.Generator,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """Build 100,000 orders with prices copied from the product catalog.

    Args:
        rng: Seeded NumPy generator.
        products: Product catalog used for unit_price lookup.

    Returns:
        Order DataFrame before intentional quality issues.
    """
    price_lookup = products.set_index("product_id")["price"]
    customer_ids = rng.integers(1, CUSTOMER_COUNT + 1, size=ORDER_COUNT)
    product_ids = rng.integers(1, PRODUCT_COUNT + 1, size=ORDER_COUNT)
    quantities = rng.integers(1, 11, size=ORDER_COUNT)
    statuses = rng.choice(ORDER_STATUSES, size=ORDER_COUNT, p=ORDER_STATUS_WEIGHTS)
    start = date(2023, 1, 1)
    end = date(2025, 12, 31)
    span_days = (end - start).days
    order_offsets = rng.integers(0, span_days + 1, size=ORDER_COUNT)
    payment_lags = rng.integers(1, 6, size=ORDER_COUNT)

    order_dates = pd.to_datetime(start) + pd.to_timedelta(order_offsets, unit="D")
    unit_prices = price_lookup.loc[product_ids].to_numpy()
    total_amounts = np.round(quantities * unit_prices, 2)
    completed_mask = statuses == "Completed"
    payment_dates = pd.Series(pd.NA, index=range(ORDER_COUNT), dtype="object")
    completed_payments = (
        order_dates[completed_mask]
        + pd.to_timedelta(payment_lags[completed_mask], unit="D")
    )
    payment_dates.loc[completed_mask] = pd.Index(completed_payments).strftime(
        "%Y-%m-%d"
    )

    return pd.DataFrame(
        {
            "order_id": np.arange(1, ORDER_COUNT + 1),
            "customer_id": customer_ids,
            "order_date": pd.Series(order_dates).dt.strftime("%Y-%m-%d"),
            "product_id": product_ids,
            "quantity": quantities,
            "unit_price": unit_prices,
            "total_amount": total_amounts,
            "order_status": statuses,
            "payment_date": payment_dates,
        }
    )


def apply_order_quality_issues(
    orders: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Plant documented completeness, uniqueness, and orphan issues on orders.

    Args:
        orders: Clean generated order frame (100,000 rows).
        rng: Seeded NumPy generator for orphan keys.

    Returns:
        Copy of the frame with planted defects.
    """
    df = orders.copy()
    df["customer_id"] = df["customer_id"].astype("Int64")
    df["product_id"] = df["product_id"].astype("Int64")

    # Quality issue: uniqueness — rows 99601-99620 reuse order_id values
    # from rows 1-20 (20 duplicate keys).
    df.loc[99600:99619, "order_id"] = list(range(1, 21))

    # Quality issue: referential integrity — rows 99621-99650 get product_id
    # values in 9001-9500, which do not exist in products.csv (30 orphans).
    df.loc[99620:99649, "product_id"] = rng.integers(9001, 9501, size=30)

    # Quality issue: referential integrity — rows 99651-99700 get customer_id
    # values in 90001-99000, which do not exist in customers.csv (50 orphans).
    df.loc[99650:99699, "customer_id"] = rng.integers(90001, 99001, size=50)

    # Quality issue: completeness — rows 99701-99800 have NULL customer_id
    # (100 rows). These are missing FKs, not orphans.
    df.loc[99700:99799, "customer_id"] = pd.NA

    # Quality issue: completeness — rows 99801-100000 have NULL product_id
    # (200 rows).
    df.loc[99800:99999, "product_id"] = pd.NA

    return df


def _count_nulls(series: pd.Series) -> int:
    """Return the number of missing values in a series.

    Args:
        series: Column to inspect.

    Returns:
        Missing-value count.
    """
    return int(series.isna().sum())


def _count_duplicate_key_rows(series: pd.Series) -> int:
    """Count rows whose key appears more than once (excluding NA keys).

    Args:
        series: Natural-key column.

    Returns:
        Number of rows that participate in a duplicate-key group.
    """
    valid = series.dropna()
    duplicated = valid.duplicated(keep=False)
    return int(duplicated.sum())


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame as CSV, using empty fields for NULL values.

    Args:
        df: Frame to persist.
        path: Destination CSV path.

    Raises:
        OSError: If the file cannot be written.
    """
    try:
        df.to_csv(path, index=False, na_rep="")
        LOGGER.info("Wrote %s rows to %s", len(df), path)
    except OSError:
        LOGGER.exception("Failed to write CSV to %s", path)
        raise


def print_generation_summary(
    customers: pd.DataFrame,
    orders: pd.DataFrame,
    products: pd.DataFrame,
) -> None:
    """Print row counts and planted quality-issue counts.

    Args:
        customers: Final customers frame.
        orders: Final orders frame.
        products: Final products frame.
    """
    customer_null_emails = _count_nulls(customers["email"])
    customer_dup_rows = _count_duplicate_key_rows(customers["customer_id"])
    unique_dup_customer_ids = int(
        customers.loc[
            customers["customer_id"].duplicated(keep=False),
            "customer_id",
        ].nunique()
    )
    order_null_customers = _count_nulls(orders["customer_id"])
    order_null_products = _count_nulls(orders["product_id"])
    orphan_customers = int(
        orders["customer_id"]
        .dropna()
        .astype(int)
        .gt(CUSTOMER_COUNT)
        .sum()
    )
    orphan_products = int(
        orders["product_id"].dropna().astype(int).gt(PRODUCT_COUNT).sum()
    )
    order_dup_rows = _count_duplicate_key_rows(orders["order_id"])
    extra_dup_order_rows = 20
    extra_dup_customer_rows = 10
    total_listed_issues = (
        customer_null_emails
        + extra_dup_customer_rows
        + order_null_customers
        + order_null_products
        + orphan_customers
        + orphan_products
        + extra_dup_order_rows
    )

    lines = [
        "",
        "========== Sample data generation summary ==========",
        f"customers.csv rows: {len(customers)}",
        f"orders.csv rows:     {len(orders)}",
        f"products.csv rows:   {len(products)}",
        "",
        "Quality issues (customers):",
        f"  NULL emails:              {customer_null_emails}",
        f"  Duplicate customer_id rows (extra planted): {extra_dup_customer_rows}",
        f"  Rows in duplicate key groups: {customer_dup_rows} "
        f"({unique_dup_customer_ids} distinct ids)",
        "",
        "Quality issues (orders):",
        f"  NULL customer_id:        {order_null_customers}",
        f"  NULL product_id:          {order_null_products}",
        f"  Orphan customer_id:       {orphan_customers}",
        f"  Orphan product_id:         {orphan_products}",
        f"  Duplicate order_id rows (extra planted): {extra_dup_order_rows}",
        f"  Rows in duplicate key groups: {order_dup_rows}",
        "",
        f"Total listed issue instances: {total_listed_issues}",
        "===================================================",
        "",
    ]
    summary = "\n".join(lines)
    print(summary)


def main() -> int:
    """Generate customers, products, and orders CSVs under ./data.

    Returns:
        Process exit code (0 on success).
    """
    _configure_logging()
    fake = Faker()
    fake.seed_instance(RANDOM_SEED)
    rng = np.random.default_rng(RANDOM_SEED)

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Generating products (%s rows)", PRODUCT_COUNT)
        products = generate_products(rng)
        LOGGER.info("Generating customers (%s rows)", CUSTOMER_COUNT)
        customers = apply_customer_quality_issues(generate_customers(fake, rng))
        LOGGER.info("Generating orders (%s rows)", ORDER_COUNT)
        orders = apply_order_quality_issues(generate_orders(rng, products), rng)

        write_csv(customers, OUTPUT_DIR / "customers.csv")
        write_csv(products, OUTPUT_DIR / "products.csv")
        write_csv(orders, OUTPUT_DIR / "orders.csv")
        print_generation_summary(customers, orders, products)
    except Exception:
        LOGGER.exception("Sample data generation failed")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
