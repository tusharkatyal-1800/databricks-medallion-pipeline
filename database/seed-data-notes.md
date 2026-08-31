# Seed data notes

How the three CSV extracts are produced locally and loaded onto the Unity Catalog Volume so Bronze can ingest them. Script internals: `src/data_generation/DATA_GENERATION_NOTES.md`. DDL after upload: `database/setup-notes.md`.

---

## What is seeded

Committed in `data/` (also regenerated with a fixed seed):

| Local file | Rows | Volume path |
| --- | --- | --- |
| `data/customers.csv` | 10,000 | `/Volumes/ecommerce/medallion/data/raw/customers.csv` |
| `data/orders.csv` | 100,000 | `/Volumes/ecommerce/medallion/data/raw/orders.csv` |
| `data/products.csv` | 500 | `/Volumes/ecommerce/medallion/data/raw/products.csv` |

These are **files on a Volume**, not Delta tables. Bronze reads them and writes managed tables `ecommerce.medallion.bronze_*`.

---

## How the CSVs are generated

Local Python only (pandas, numpy, Faker). Not Spark.

```text
python -m pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
python src/data_generation/validate_generated_data.py
```

- **`RANDOM_SEED = 42`** on Faker and NumPy. Same command → same files.
- Writes `./data/*.csv` (overwrite). Empty fields are SQL/CSV nulls (`na_rep=""`).
- Build order: products → customers (then plant) → orders (then plant). Order `unit_price` copies `products.price`.
- Uniqueness defects **reuse keys in place**. Row counts stay 10,000 / 100,000 / 500 (not extra appended rows).

---

## Planted quality issues (listed = 460)

| Issue | Table | Count | How |
| --- | --- | --- | --- |
| NULL `email` | customers | 50 | Data rows 9951–10000 |
| Duplicate `customer_id` | customers | 10 extra | Rows 9941–9950 reuse ids 1–10 |
| NULL `customer_id` | orders | 100 | Rows 99701–99800 |
| NULL `product_id` | orders | 200 | Rows 99801–100000 |
| Orphan `customer_id` (90001–99000) | orders | 50 | Rows 99651–99700 |
| Orphan `product_id` (9001–9500) | orders | 30 | Rows 99621–99650 |
| Duplicate `order_id` | orders | 20 extra | Rows 99601–99620 reuse ids 1–20 |

Products have **no** planted completeness or uniqueness defects.

**Not planted:** extra ~240 type/domain defects (bad emails, negative qty, broken amounts). Clean rows keep valid amounts (`qty × catalog price`) and allowed statuses/segments.

**Side effects Silver will see:**

- Reusing customer ids 1–10 **removes** parent ids 9941–9950 → extra customer orphans (validator ~157 total, not 50).
- Signup dates and order dates are independent → extra rule `order_before_signup` fails many orders (`TYPE_VALIDATION_FAIL`). Not a CSV plant.

---

## Volume upload

1. Create catalog/schema/volume if needed (`database/schema.sql` or the Bronze job).
2. In the workspace, open volume **`ecommerce.medallion.data`**.
3. Folder **`raw/`** (create if missing).
4. Upload the three CSVs. Names must be `customers.csv`, `orders.csv`, `products.csv`.
5. Run `src/bronze/ingest_all.py` on an **all-purpose cluster**.

Expected Bronze counts: **10,000 / 100,000 / 500**. Re-upload + overwrite ingest is safe; do not append.

Do **not** register tables with `LOCATION` under `/Volumes/...`. Only raw files live on the Volume.

---

## After seed

Silver flags planted issues (and the side effects above). Gold uses `quality_check_result = 'PASS'` and `order_status = 'Completed'` for product/customer KPIs.
