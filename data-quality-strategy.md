# Data Quality Strategy

Silver applies **four independent checks** on every row. Failures are **flagged, never deleted**. Per-check columns use `PASS` or detailed fail tokens (`FAIL_NULL_*`, `FAIL_DUPLICATE_*`, `FAIL_INVALID_*`, `FAIL_ORPHAN_*`). Referential may be `N/A` on customers/products. `quality_check_result` is **`PASS`** when nothing failed, otherwise a **pipe-delimited list of the four roll-up tokens** (not a bitmask or JSON), for example `COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL`. Extra business rules fold into `TYPE_VALIDATION_FAIL` — there is no fifth token.

Checks run per table in one Spark plan. Uniqueness uses `row_number()` (keep-first). Referential uses an anti-join on orders after parents exist.

**As built:** Completeness/uniqueness/referential plants are in-place (10K/100K/500 rows). Type/domain extras (~240) were **not** planted. `order_before_signup` is not planted but fails many orders. Customer orphans can exceed 50 because uniqueness reuse of ids 1–10 drops parent ids 9941–9950.

---

## Quality Checks Overview

### 1. Completeness Check

**What:** Required fields must be non-null and non-blank (after trim). Blank `""` is treated as missing.

**Columns in scope (planted + scored):**

| Table | Columns checked | Intentional nulls |
| --- | --- | --- |
| `customers` | `email` | 50 |
| `orders` | `customer_id`, `product_id` | 100 + 200 |
| `products` | none in the planted set | 0 |

**Total intentional completeness errors: 50 + 100 + 200 = 350 NULL rows.**

(`payment_date` is nullable by design and is **not** a completeness failure.)

**How NULLs are found and marked**

1. Read Bronze (typed columns from `schemas.py`).
2. A field is missing if `col IS NULL` (blank strings are not planted on these INT/email fields).
3. Mark the row:
   - `completeness_check = FAIL_NULL_{field}` (pipe-joined if several).
   - `completeness_check = PASS` otherwise.
   - Products: always `PASS`.
4. If FAIL, include token `COMPLETENESS_FAIL` when building `quality_check_result`.
5. **Do not** `filter`/`drop` the row.

Spark SQL equivalent:

```sql
CASE
  WHEN email IS NULL THEN 'FAIL_NULL_email'
  ELSE 'PASS'
END
```

```sql
concat_ws(
  '|',
  CASE WHEN customer_id IS NULL THEN 'FAIL_NULL_customer_id' END,
  CASE WHEN product_id IS NULL THEN 'FAIL_NULL_product_id' END
)
```

**Threshold:** **>99% complete**  
`completeness_rate = pass_count / rows_total`  
`threshold_met` iff `completeness_rate > 0.99` (equivalently fail rate &lt; 1%).

On this sample the bar is **expected to miss** on orders (~300 planted FK nulls on 100,000 rows is still >99% complete). Customers: 50/10,000 = 99.5% — **meets** a >99% bar. The job still succeeds; the metrics report records `threshold_met` per field.

---

### 2. Uniqueness Check

**What:** Natural keys must appear once. Duplicate **keys** fail, even if other columns differ. This is not “entire row identical.”

**Columns in scope:**

| Table | Key | Intentional extra duplicate rows |
| --- | --- | --- |
| `customers` | `customer_id` | 10 |
| `orders` | `order_id` | 20 |
| `products` | `product_id` | 0 (still checked) |

**Total intentional uniqueness extras: 10 + 20 = 30 rows that reuse an existing key.** **As built:** `row_number() OVER (PARTITION BY key ORDER BY _ingestion_timestamp)`. `row_num = 1` → `PASS`. `row_num > 1` → `FAIL_DUPLICATE_{key}`. Fail **row** count is **10 customers + 20 orders**, not every member of the group.

**How duplicates are found**

**Window `row_number` (chosen), not “flag the whole group.”**

```sql
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingestion_timestamp) AS row_num
-- uniqueness_check = CASE WHEN customer_id IS NOT NULL AND row_num > 1
--   THEN 'FAIL_DUPLICATE_customer_id' ELSE 'PASS' END
```

Null keys: completeness already fails them. Uniqueness: null keys are **PASS** so completeness owns missing keys.

**Threshold:** **100% unique keys** on later-row scoring.  
`uniqueness_rate = 1 - (later_duplicate_rows / total_rows)`  
`threshold_met` iff uniqueness_rate = 1.0.

Silver **does not deduplicate**. Gold uses `WHERE quality_check_result = 'PASS'`, so only the first ingest-timestamp copy can enter KPIs if it otherwise PASSes. The uniqueness metrics row is **expected to fail** the 100% threshold because of the 30 reused keys.

---

### 3. Type Validation Check

**What:** When a value is present, it must match the domain. Completeness (nulls) is a different check; a null email is not a type failure.

**Email regex (Spark `rlike`)**

```text
.*@.*\..*
```

Loose presence of `@` and a dot after it. Null email → completeness FAIL, type not scored as email-invalid.

**Invalid date**

Bronze already stores `DATE`. Type rules score **range / sequencing**, not CSV parse:

| Case | Example |
| --- | --- |
| Signup/order before documented min or after `current_date()` | Out-of-range |
| `payment_date` ≤ `order_date` | Sequence fail |
| Completed with null `payment_date` | Type fail |
| Pending with non-null `payment_date` | Type fail |

**Numeric constraints**

| Field | Rule |
| --- | --- |
| `quantity` | Integer **and** `quantity > 0` |
| `unit_price` | Numeric **and** `unit_price > 0` |
| `total_amount` | Numeric **and** `total_amount > 0` |
| `price`, `cost` (products) | Numeric **and** `> 0` when present |
| `lifetime_value` | Numeric **and** `>= 0` |
| `stock_quantity` | Integer **and** `>= 0` |
| Line identity | `abs(total_amount - quantity * unit_price) > abs(qty×price) * 0.01` (**1% relative**) |

**Extra business rules** (`src/silver/business_logic.py`): catalog price vs `unit_price`, reorder vs stock, `order_before_signup`. Failures are `FAIL_INVALID_{name}` and roll up to **`TYPE_VALIDATION_FAIL` only**.

**How it is marked:** `type_check` is `PASS` or pipe-joined `FAIL_INVALID_*`. If any fail, include token `TYPE_VALIDATION_FAIL` in `quality_check_result`.

**Threshold:** informational. **Do not expect ~240 planted type fails** — the generator did not add them. Observed type fails are mostly `order_before_signup` plus genuine domain misses.

---

### 4. Referential Integrity Check

**What:**

- `orders.customer_id` must exist in `customers.customer_id`
- `orders.product_id` must exist in `products.product_id`

Parent tables: `referential_check = 'N/A'` (no FKs).

**How orphan rows are detected (`LEFT ANTI JOIN`)**

Only **non-null, non-blank** FKs can be orphans. Null FKs are completeness, not referential.

```sql
-- Orphan customers: order FK not in customers
SELECT o.*
FROM ecommerce.medallion.bronze_orders o
LEFT ANTI JOIN (
  SELECT DISTINCT customer_id
  FROM ecommerce.medallion.bronze_customers
  WHERE customer_id IS NOT NULL AND trim(customer_id) <> ''
) c
  ON o.customer_id = c.customer_id
WHERE o.customer_id IS NOT NULL AND trim(o.customer_id) <> '';

-- Orphan products: same pattern on product_id vs bronze_products
```

Stamp two booleans, then:

```text
referential_check = FAIL if orphan_customer OR orphan_product else PASS
-- FAIL contributes token REFERENTIAL_INTEGRITY_FAIL
```

Parent match uses `DISTINCT` parent keys so a **duplicate customer_id** still satisfies “exists” (uniqueness is a separate fail on the customer table).

**Expected intentional errors: 50 orphan customer_ids + 30 orphan product_ids = 80 planted.**  
These are in addition to the 100 + 200 null FKs.

**As built:** product orphans stay **30**. Customer orphans were **157** in the successful run because reusing customer ids 1–10 on rows 9941–9950 removes those parent keys while orders still reference them.

**Threshold:** **>99.9% valid** referential on applicable order rows  
`referential_rate = pass_count / applicable_non_null_fk_rows`  
`threshold_met` iff rate > 0.999.

Use applicable rows only (exclude null-FK rows from the referential denominator). Do not require `failed = 80` after the uniqueness id-reuse side effect.

---

## How `quality_check_result` is populated

Tokens (fixed order, only those that failed):

| Check | Fail token |
| --- | --- |
| Completeness | `COMPLETENESS_FAIL` |
| Uniqueness | `UNIQUENESS_FAIL` |
| Type validation | `TYPE_VALIDATION_FAIL` |
| Referential integrity | `REFERENTIAL_INTEGRITY_FAIL` |

**Rules**

1. Evaluate all four checks independently (do not stop at the first failure).
2. Collect fail tokens in the order above (stable, review-friendly).
3. If the token list is empty → `quality_check_result = 'PASS'`.
4. If one or more failed → join with `|` (no spaces).

```sql
-- Illustrative assembly (per-check flags already computed)
CASE
  WHEN completeness_check = 'PASS'
   AND uniqueness_check = 'PASS'
   AND type_check = 'PASS'
   AND (referential_check IN ('PASS', 'N/A'))
  THEN 'PASS'
  ELSE concat_ws(
    '|',
    CASE WHEN completeness_check <> 'PASS' THEN 'COMPLETENESS_FAIL' END,
    CASE WHEN uniqueness_check <> 'PASS' THEN 'UNIQUENESS_FAIL' END,
    CASE WHEN type_check <> 'PASS' THEN 'TYPE_VALIDATION_FAIL' END,
    CASE WHEN referential_check <> 'PASS' AND referential_check <> 'N/A'
         THEN 'REFERENTIAL_INTEGRITY_FAIL' END
  )
END AS quality_check_result
```

**Examples**

| Situation | `quality_check_result` |
| --- | --- |
| All applicable checks pass | `PASS` |
| Null `orders.customer_id` only | `COMPLETENESS_FAIL` |
| Null FK **and** orphan product on the same row is impossible for one FK; null `customer_id` + orphan `product_id` | `COMPLETENESS_FAIL\|REFERENTIAL_INTEGRITY_FAIL` |
| Duplicate `order_id` with negative `quantity` | `UNIQUENESS_FAIL\|TYPE_VALIDATION_FAIL` |
| All four fail | `COMPLETENESS_FAIL\|UNIQUENESS_FAIL\|TYPE_VALIDATION_FAIL\|REFERENTIAL_INTEGRITY_FAIL` |

Gold / clean KPI filter:

```sql
WHERE quality_check_result = 'PASS'
```

Inspect one check:

```sql
WHERE quality_check_result LIKE '%COMPLETENESS_FAIL%'
```

`N/A` referential on customers/products does **not** add a token and does not block `PASS`.

---

## Rows that fail multiple checks

- **Keep one Silver row.** Never split, delete, or quarantine. Never overwrite earlier flags.
- Each of `completeness_check`, `uniqueness_check`, `type_check`, `referential_check` is set on its own. A completeness fail does **not** skip uniqueness, type, or referential (except: null FKs are not scored as orphans).
- `quality_check_result` lists **every** fail token, pipe-separated, so a row can be `COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL`.
- **Metrics count the row in each failed check.** If one order has a null `customer_id` and a negative `quantity`, it increments completeness `failed` **and** type `failed`. `passed + failed` per check still equals `total_rows` (or applicable rows for referential).
- `~700` issue instances was a **design target**, not the measured Silver outcome. Type plants were not generated.
- Gold treats any non-`PASS` roll-up as excluded from KPIs (one fail is enough).

---

## Quality Metrics Report

Delta table `ecommerce.medallion.quality_metrics`. Grain: `(batch_timestamp, table_name, check_name, field_checked)`. Several rows per check (one per field/rule).

**Report structure:**

| Column | Type | Description |
| --- | --- | --- |
| `table_name` | `STRING` | `customers`, `orders`, `products` |
| `check_name` | `STRING` | `completeness`, `uniqueness`, `type_validation`, `referential_integrity`, `overall` |
| `field_checked` | `STRING` | Column or rule (`email`, `order_id`, `amount_formula`, `_all`, …) |
| `total_rows` | `INT` | Silver row count |
| `applicable_rows` | `INT` | Rows scored for that field |
| `passed` | `INT` | Applicable rows that passed |
| `failed` | `INT` | Applicable rows that failed |
| `pass_rate_pct` | `DOUBLE` | `100.0 * passed / applicable_rows` |
| `threshold` | `DOUBLE` | Documented bar |
| `threshold_met` | `BOOLEAN` | Whether the bar was met |
| `batch_timestamp` | `TIMESTAMP` | Run time |

Example shape after a run (orders; figures **illustrative**, not the warehouse snapshot):

| check_name | field_checked | total_rows | failed |
| --- | --- | --- | --- |
| completeness | customer_id | 100000 | 100 |
| completeness | product_id | 100000 | 200 |
| uniqueness | order_id | 100000 | 20 |
| referential_integrity | customer_id | 100000 | 157 (as-built, not 50) |
| referential_integrity | product_id | 100000 | 30 |
| type_validation | order_before_signup | 100000 | large (unplanted date clash) |

| Check | Threshold | Expected on this sample |
| --- | --- | --- |
| Completeness (customers email) | `pass_rate_pct` > 99 | **Met** (50/10000 = 99.5%) |
| Completeness (order FKs) | `pass_rate_pct` > 99 | **Met** (~0.3% fail) |
| Uniqueness | `pass_rate_pct` = 100 | **Not met** (10 + 20 later duplicates) |
| Type validation | informational | Dominated by `order_before_signup`, not ~240 plants |
| Referential | `pass_rate_pct` > 99.9 | Product 30 orphans; customer orphans inflated by id reuse |

`threshold_met = false` is a **successful demonstration** of the checks, not a pipeline crash. Gold still uses `quality_check_result = 'PASS'`.

---

## Sample Data Quality Issues

| # | Issue | Table | Check | Planted count |
| --- | --- | --- | --- | --- |
| 1 | NULL `email` | customers | Completeness | 50 |
| 2 | NULL `customer_id` | orders | Completeness | 100 |
| 3 | NULL `product_id` | orders | Completeness | 200 |
| 4 | Duplicate `customer_id` | customers | Uniqueness | 10 reused keys (later rows FAIL) |
| 5 | Duplicate `order_id` | orders | Uniqueness | 20 reused keys (later rows FAIL) |
| 6 | `customer_id` not in customers | orders | Referential | 50 planted; **as-built ~157** after id reuse |
| 7 | `product_id` not in products | orders | Referential | 30 |
| 8 | Extra type/domain plants | customers, orders, products | Type | **Not planted (~240 skipped)** |
| 9 | `order_before_signup` | orders | Type (business) | Unplanted; high fail rate from independent dates |

**Null vs orphan:** a null `orders.customer_id` is completeness only. An orphan is a **non-null** id that fails the anti-join.

**Duplicates vs Gold:** later `row_number` rows have `UNIQUENESS_FAIL`; they never enter Gold. The first copy can PASS uniqueness and may enter Gold if other checks pass.
