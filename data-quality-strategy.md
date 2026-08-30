# Data Quality Strategy

Silver applies **four independent checks** on every row. Failures are **flagged, never deleted**. Per-check columns stay `PASS` / `FAIL` (referential may be `N/A` on customers/products). `quality_check_result` is **`PASS`** when nothing failed, otherwise a **pipe-delimited list of fail tokens** (not a bitmask or JSON), for example `COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL`.

Checks run per table in one Spark plan. Uniqueness uses a window; referential uses `LEFT ANTI JOIN` on orders after parents exist.

Planted issue instances (this sample): **350 completeness + 30 uniqueness + 80 referential = 460**, plus type/domain plants to reach **~700** total instances. One row can fail more than one check (see below).

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

1. Read Bronze STRING columns.
2. A field is missing if `col IS NULL` OR `trim(col) = ''`.
3. Mark the row:
   - `completeness_check = 'FAIL'` if **any** in-scope column is missing.
   - `completeness_check = 'PASS'` otherwise.
4. If FAIL, include token `COMPLETENESS_FAIL` when building `quality_check_result` (see below).
5. **Do not** `filter`/`drop` the row.

Spark SQL equivalent:

```sql
CASE
  WHEN email IS NULL OR trim(email) = '' THEN 'FAIL'   -- customers
  ELSE 'PASS'
END
```

```sql
CASE
  WHEN customer_id IS NULL OR trim(customer_id) = ''
    OR product_id IS NULL OR trim(product_id) = ''
  THEN 'FAIL'   -- orders
  ELSE 'PASS'
END
```

**Threshold:** **>99% complete**  
`completeness_rate = pass_count / rows_total`  
`threshold_met` iff `completeness_rate > 0.99` (equivalently fail rate &lt; 1%).

On this sample the bar is **expected to miss** (350 planted nulls on ~10K customers and ~100K orders). The job still succeeds; the metrics report records the miss.

---

### 2. Uniqueness Check

**What:** Natural keys must appear once. Duplicate **keys** fail, even if other columns differ. This is not “entire row identical.”

**Columns in scope:**

| Table | Key | Intentional extra duplicate rows |
| --- | --- | --- |
| `customers` | `customer_id` | 10 |
| `orders` | `order_id` | 20 |
| `products` | `product_id` | 0 (still checked) |

**Total intentional uniqueness errors: 10 + 20 = 30 duplicate rows** (extra records sharing a key). Every row that participates in a duplicate set is flagged, so fail **row** count is **≥ 30** (each extra row implies at least two rows with that key). Metrics should report fail_count = all rows whose key count &gt; 1, and a separate `duplicate_key_groups` if useful.

**How duplicates are found**

**Window function (chosen), not `groupBy` as the only step.**

- `groupBy(key).count()` finds keys with count &gt; 1 but **drops row context**. Flagging in place would need a join back.
- `COUNT(*) OVER (PARTITION BY key)` keeps every Bronze row and stamps the count on each.

```sql
COUNT(*) OVER (PARTITION BY customer_id) AS key_cnt  -- customers
-- uniqueness_check = CASE WHEN customer_id is usable AND key_cnt > 1 THEN 'FAIL' ELSE 'PASS' END
-- uniqueness FAIL contributes token UNIQUENESS_FAIL
```

Null keys: completeness already fails them. Uniqueness: null/blank keys are **not** treated as one duplicate group (they do not `PARTITION BY` together as a real id). They stay uniqueness `PASS` or `N/A`; completeness carries the fail. Documented rule: uniqueness applies only when the key is non-null.

**Threshold:** **100% unique keys** on the stored Silver table (no silent dedupe).  
`uniqueness_rate = rows_with_key_cnt_1 / rows_with_non_null_key`  
`threshold_met` iff uniqueness_rate = 1.0.

Silver **does not deduplicate**. “100% after deduplicating” is the **Gold** story: `WHERE quality_check_result = 'PASS'` so KPI grains are unique. The Silver metrics row for uniqueness is **expected to fail** the 100% threshold because of the 30 planted extras.

---

### 3. Type Validation Check

**What:** When a value is present, it must match the domain. Completeness (nulls) is a different check; a null email is not a type failure.

**Email regex (Spark `rlike`)**

```text
^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$
```

- Local part: letters, digits, `. _ % + -`
- Domain: labels and dots, TLD at least 2 letters
- Rejects: missing `@`, spaces, `user@localhost` (no TLD), `user@.com`
- Intentionally simpler than full RFC 5322 (readable in a notebook review)

Null email → completeness FAIL, type not scored as email-invalid.

**Invalid date**

Parse with `to_date(col, 'yyyy-MM-dd')` only (calendar date, **no timezone conversion**).

A date is **invalid** if any of:

| Case | Example |
| --- | --- |
| Unparseable / wrong format | `31-01-2024`, `2024/01/31`, `Jan 31 2024` |
| Impossible calendar day | `2024-02-30`, `2024-13-01` (`to_date` → null) |
| Trailing/embedded time or offset (this pipeline) | `2024-01-15T10:00:00Z`, `2024-01-15+05:30` |
| Empty string | `''` (also completeness if the column is required) |

`order_date` / `signup_date` must parse when present. `payment_date`: if present, must parse; if `order_status = 'Completed'`, it must be present **and** valid (type FAIL if completed and payment_date missing or invalid).

**Numeric constraints**

| Field | Rule |
| --- | --- |
| `quantity` | Integer **and** `quantity > 0` (negative or zero → FAIL) |
| `unit_price` | Numeric **and** `unit_price > 0` |
| `total_amount` | Numeric **and** `total_amount > 0` |
| `price`, `cost` (products) | Numeric **and** `> 0` when present |
| `lifetime_value` | Numeric **and** `>= 0` (zero LTV allowed for new customers) |
| Line identity | `abs(total_amount - quantity * unit_price) <= 0.01` after successful parse (`~` allows 1 cent rounding) |

Non-numeric text (`"ten"`, `N/A`) → type FAIL.

**How it is marked:** `type_check = 'FAIL'` if any applicable rule fails; else `PASS`. If FAIL, include token `TYPE_VALIDATION_FAIL` in `quality_check_result`.

**Threshold:** no SLI in the brief; report fail_count. Remaining plants after 460 listed issues (~240) are type/domain (bad emails, invalid dates, negative/zero qty or price, broken qty × price).

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
FROM ecommerce.orders_bronze o
LEFT ANTI JOIN (
  SELECT DISTINCT customer_id
  FROM ecommerce.customers_bronze
  WHERE customer_id IS NOT NULL AND trim(customer_id) <> ''
) c
  ON o.customer_id = c.customer_id
WHERE o.customer_id IS NOT NULL AND trim(o.customer_id) <> '';

-- Orphan products: same pattern on product_id vs products_bronze
```

Stamp two booleans, then:

```text
referential_check = FAIL if orphan_customer OR orphan_product else PASS
-- FAIL contributes token REFERENTIAL_INTEGRITY_FAIL
```

Parent match uses `DISTINCT` parent keys so a **duplicate customer_id** still satisfies “exists” (uniqueness is a separate fail on the customer table).

**Expected intentional errors: 50 orphan customer_ids + 30 orphan product_ids = 80.**  
These are in addition to the 100 + 200 null FKs.

**Threshold:** **>99.9% valid** referential on applicable order rows  
`referential_rate = pass_count / (pass_count + fail_count)`  
`threshold_met` iff rate > 0.999.

Expected to **miss** on this sample (~80 orphans on ~100K orders is ~0.08% fail, which may still sit near 99.9%; 80/100000 = 0.08%, so 99.92% valid — **just under or around 99.9%** depending on denominator). Use applicable rows only (exclude null-FK rows from the referential denominator so 80 orphans are not diluted by 300 completeness nulls). Denominator ≈ 100K − 100 − 200 + overlap handling; **fail_count must equal 80** planted orphans (plus any type-planted bad FKs if introduced).

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
    CASE WHEN completeness_check = 'FAIL' THEN 'COMPLETENESS_FAIL' END,
    CASE WHEN uniqueness_check = 'FAIL' THEN 'UNIQUENESS_FAIL' END,
    CASE WHEN type_check = 'FAIL' THEN 'TYPE_VALIDATION_FAIL' END,
    CASE WHEN referential_check = 'FAIL' THEN 'REFERENTIAL_INTEGRITY_FAIL' END
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
- `~700` issue instances can exceed distinct bad-row counts for this reason.
- Gold treats any non-`PASS` roll-up as excluded from KPIs (one fail is enough).

---

## Quality Metrics Report

Delta table `ecommerce.quality_metrics`. Grain: `(batch_timestamp, table_name, check_name)`. One row per check per Silver table per run.

**Report structure (required columns):**

| Column | Type | Description |
| --- | --- | --- |
| `table_name` | `STRING` | `customers_silver`, `orders_silver`, `products_silver` |
| `check_name` | `STRING` | `completeness`, `uniqueness`, `type_validation`, `referential_integrity` |
| `total_rows` | `BIGINT` | Silver row count for that table (referential: applicable non-null FK rows only) |
| `passed` | `BIGINT` | Rows with that check = `PASS` |
| `failed` | `BIGINT` | Rows with that check = `FAIL` |
| `pass_rate_%` | `DOUBLE` | `round(100.0 * passed / total_rows, 4)` when `total_rows > 0` |

Also stored (not required on the printed report): `batch_timestamp`, `threshold`, `threshold_met`.

Example shape after a run (orders, figures illustrative):

| check_name | total_rows | passed | failed | pass_rate_% |
| --- | --- | --- | --- | --- |
| completeness | 100020 | 99620 | 400 | 99.6001 |
| uniqueness | 100020 | 99980 | 40 | 99.9600 |
| type_validation | 100020 | 99780 | 240 | 99.7600 |
| referential_integrity | 99720 | 99640 | 80 | 99.9198 |

(`completeness` failed ≈ 100 + 200 minus overlap if both FKs null on one row; uniqueness `failed` is all rows in duplicate key groups, not only the 20 extras.)

| Check | Threshold | Expected on this sample |
| --- | --- | --- |
| Completeness | `pass_rate_%` > 99 | **Not met** (~350 nulls) |
| Uniqueness | `pass_rate_%` = 100 | **Not met** (30 extra duplicate rows) |
| Type validation | informational | `failed` ≈ remaining plants (~240) |
| Referential | `pass_rate_%` > 99.9 | **Not met or borderline**; `failed` = **80** |

`threshold_met = false` is a **successful demonstration** of the checks, not a pipeline crash. Gold still uses `quality_check_result = 'PASS'`.

---

## Sample Data Quality Issues

| # | Issue | Table | Check | Planted count |
| --- | --- | --- | --- | --- |
| 1 | NULL `email` | customers | Completeness | 50 |
| 2 | NULL `customer_id` | orders | Completeness | 100 |
| 3 | NULL `product_id` | orders | Completeness | 200 |
| 4 | Duplicate `customer_id` | customers | Uniqueness | 10 extra rows |
| 5 | Duplicate `order_id` | orders | Uniqueness | 20 extra rows |
| 6 | `customer_id` not in customers | orders | Referential (`LEFT ANTI JOIN`) | 50 |
| 7 | `product_id` not in products | orders | Referential (`LEFT ANTI JOIN`) | 30 |
| 8 | Bad email / invalid date / qty ≤ 0 / price ≤ 0 / amount ≠ qty × price | customers, orders, products | Type | ~240 |

**Null vs orphan:** a null `orders.customer_id` is completeness only. An orphan is a **non-null** id that fails the anti-join.

**Duplicates vs Gold:** all rows with `key_cnt > 1` have `UNIQUENESS_FAIL` in `quality_check_result` (not `PASS`); they never enter Gold aggregations.
