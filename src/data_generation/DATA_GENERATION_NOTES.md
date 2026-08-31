# Data Generation Notes

This note explains `src/data_generation/generate_sample_data.py`: what it builds, how it plants defects, and what Silver will see that the generator **does not** plant.

The script is **local Python** (pandas, numpy, Faker). It does not use Spark. Output CSVs are later uploaded to `/Volumes/ecommerce/medallion/data/raw/` for Bronze.

Companion check: `src/data_generation/validate_generated_data.py` (expected vs actual planted counts).

---

## Purpose

Produce three stable e-commerce extracts so the Medallion pipeline can demonstrate quality checks:

| File | Rows | Grain |
| --- | --- | --- |
| `data/customers.csv` | 10,000 | one customer per row |
| `data/products.csv` | 500 | one product per row |
| `data/orders.csv` | 100,000 | one order line per row |

Defects are **written into existing rows**. The files do **not** grow to 10,010 / 100,020. Uniqueness plants **reuse** keys instead of appending extra records.

---

## How to run

From the repo root (venv with `requirements.txt`):

```text
python src/data_generation/generate_sample_data.py
python src/data_generation/validate_generated_data.py
```

`RANDOM_SEED = 42` is applied to both Faker and NumPy (`default_rng`). Re-runs overwrite `./data/*.csv` with the same content.

`main()` order: **products → customers (then plant) → orders (then plant)**. Orders need the product price catalog before `unit_price` is set.

NULLs are written as empty CSV fields (`na_rep=""`).

---

## Shared design

- **Logging** for writes; exceptions are logged and re-raised.
- **Dates** are ISO `YYYY-MM-DD` strings (Bronze parses them as `DATE`).
- **pandas `.loc` is 0-based.** Comments talk about CSV data rows 9941–9950; that is `df.loc[9940:9949]`.

---

## Customers (`generate_customers` + `apply_customer_quality_issues`)

Clean generation (ids 1–10,000):

| Column | How it is filled |
| --- | --- |
| `customer_id` | Sequential 1 … 10,000 |
| `customer_name` | Faker first + last name |
| `email` | `{slug(first)}.{slug(last)}@{domain}` with a uniqueness suffix if needed |
| `country` | Weighted choice: 40% USA, 15% UK, 10% Germany, 10% India, rest split across other listed countries |
| `signup_date` | 2020-01-01 … 2025-12-31; year weights skew **recent** (2024/2025 heaviest) |
| `customer_segment` | 20% Premium, 50% Standard, 30% Basic |
| `lifetime_value` | Uniform in a **segment band** (Premium 500–5000, Standard 100–500, Basic 10–100), rounded to 2 decimals |

Planted issues (after the clean frame exists):

1. **Uniqueness (10 keys reused)** — data rows 9941–9950 get `customer_id` 1–10. Names/emails on those rows stay as generated. Result: 10 ids appear twice (20 rows in duplicate groups). **Ids 9941–9950 no longer exist as customer keys.**
2. **Completeness (50 null emails)** — data rows 9951–10000: `email` = NULL.

Silver uniqueness is keep-first (`row_number` by `_ingestion_timestamp`): 10 later customer rows fail, not all 20 group members.

### Side effect: extra customer orphans

Orders were sampled against ids 1–10,000 **before** uniqueness reuse. After ids 9941–9950 disappear, any order still pointing at those ids is an orphan **in addition to** the 50 planted high ids (90001–99000). A full FK scan therefore sees **~157** missing customer ids, not 50. The validator scores the planted 50 separately and notes the extra orphans.

---

## Products (`generate_products`)

No planted completeness or uniqueness defects.

| Column | How it is filled |
| --- | --- |
| `product_id` | Sequential 1 … 500 |
| `category` | Round-robin over 8 categories |
| `product_name` | Template for that category + id (e.g. `Wireless Bluetooth Headphones 1`) |
| `price` | Uniform inside a **category band** (Books cheap, Electronics expensive) |
| `cost` | `price * uniform(0.3, 0.7)` (wholesale) |
| `stock_quantity` | Integer **0–5000** (zero is valid; Silver type uses `>= 0`) |
| `reorder_level` | Integer 10–200 |

Silver still runs uniqueness and type on products. Completeness is forced `PASS` (no required product fields planted).

---

## Orders (`generate_orders` + `apply_order_quality_issues`)

Clean generation (ids 1–100,000):

| Column | How it is filled |
| --- | --- |
| `order_id` | Sequential 1 … 100,000 |
| `customer_id` | Uniform 1–10,000 |
| `product_id` | Uniform 1–500 |
| `order_date` | Uniform 2023-01-01 … 2025-12-31 |
| `quantity` | Integer 1–10 |
| `unit_price` | **Copied from** `products.price` for that `product_id` |
| `total_amount` | `round(quantity * unit_price, 2)` (identity holds on clean rows) |
| `order_status` | 60% Completed, 25% Pending, 15% Cancelled |
| `payment_date` | Completed: `order_date` + 1–5 days; Pending and Cancelled: NULL |

Planted issues (same 100,000 rows; later plants can overwrite earlier ones on overlapping ranges — ranges below do **not** overlap):

| Data rows | Plant | Silver check |
| --- | --- | --- |
| 99601–99620 | `order_id` reused as 1–20 (20 extras) | Uniqueness (`FAIL_DUPLICATE_order_id` on later rows) |
| 99621–99650 | `product_id` in 9001–9500 | Referential (30 product orphans) |
| 99651–99700 | `customer_id` in 90001–99000 | Referential (50 planted customer orphans) |
| 99701–99800 | `customer_id` NULL | Completeness only (not orphans) |
| 99801–100000 | `product_id` NULL | Completeness only |

Null FKs are **not** scored as referential fails.

Listed planted instances: **50 + 10 + 100 + 200 + 50 + 30 + 20 = 460**.

---

## What this script does **not** generate

- **No extra ~240 type/domain plants** (bad emails, negative qty, broken amounts). Clean rows have valid segments, statuses, qty 1–10, and `total_amount = qty × catalog price`.
- **Signup date and order date are independent.** Silver extra rule `order_before_signup` therefore fails a large share of otherwise clean orders (`TYPE_VALIDATION_FAIL`). That is why orders overall PASS can sit near **64%**. It is not a planted CSV defect.
- CSV `lifetime_value` is **not** tied to later Gold `lifetime_value_actual` (Gold uses completed PASS revenue).

---

## Pipeline mapping

```text
generate_sample_data.py
        → data/{customers,orders,products}.csv
        → Volume raw/
        → Bronze (typed land, same row counts)
        → Silver (flag 460-style plants + extra orphans + unplanted date rule)
        → Gold (PASS + Completed)
```

If you change plants, re-run the generator **and** the validator, then re-upload CSVs and overwrite Bronze/Silver/Gold.
