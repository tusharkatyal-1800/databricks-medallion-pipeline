# Design Notes

## Architecture Overview

```
customers.csv / orders.csv / products.csv
        │  (upload)
        ▼
/Volumes/ecommerce/medallion/data/raw/*.csv
        │  Bronze: explicit schema + ingest metadata, no cleansing
        ▼
Delta  ecommerce.medallion.bronze_*
        │  Silver: four quality checks, flag in place, metrics table
        ▼
Delta  ecommerce.medallion.*_silver  +  ecommerce.medallion.quality_metrics
        │  Gold: PASS + Completed only, full overwrite aggregations
        ▼
Delta  ecommerce.medallion.sales_by_product
       ecommerce.medallion.revenue_by_customer
       ecommerce.medallion.customer_segmentation
        │
        ▼
Databricks SQL Dashboard (3+ visuals on Gold only)
```

**Platform:** Databricks Unity Catalog. Catalog `ecommerce`, schema `medallion`, Volume `data`. Paths under `/Volumes/ecommerce/medallion/data/`.

### Separation of concerns

| Layer | Owns | Must not do |
| --- | --- | --- |
| **Raw (Volume)** | Immutable CSV extracts | Parsing, KPI logic |
| **Bronze** | Faithful land: 1:1 with file rows, STRING columns, ingest lineage | Dedupe, type repair, FK repair, quality flags |
| **Silver** | Audit: same grain as Bronze, four checks, `quality_check_result`, metrics | Delete/quarantine rows, compute dashboard KPIs |
| **Gold** | Business grain: product / customer / segment metrics from **clean** orders | Re-ingest CSVs, hide Silver failures |
| **Dashboard** | Presentation: charts over Gold | New transformations or Bronze scans |

Each layer is a full **overwrite** so a second ingest of the same files cannot double-count.

---

## Data Model & Schema

Canonical column lists, Spark types, and nullability live in [`data-model.md`](data-model.md). This note only records design choices that drive those schemas.

---

## Bronze Layer Design

### Path naming convention

Raw root: `/Volumes/ecommerce/medallion/data/raw/`. Curated layers use managed
Unity Catalog tables because registered tables cannot use locations inside a
Volume.

| Role | Storage/access | UC table |
| --- | --- | --- |
| Raw CSV | `/Volumes/ecommerce/medallion/data/raw/customers.csv` (and `orders.csv`, `products.csv`) | — |
| Bronze Delta | Managed table | `ecommerce.medallion.bronze_customers`, `bronze_orders`, `bronze_products` |
| Silver Delta | Managed table | `ecommerce.medallion.customers_silver`, `orders_silver`, `products_silver` |
| Metrics | Managed table | `ecommerce.medallion.quality_metrics` |
| Gold Delta | Managed table | `ecommerce.medallion.sales_by_product`, `revenue_by_customer`, `customer_segmentation` |

### Schema inference vs schema definition

**Choice: explicit schema, all source columns as `STRING`. Do not use `inferSchema=True`.**

Reasons:

1. Planted defects (bad dates, negative numbers as text, malformed emails) must survive Bronze. Inference often turns garbage into `null` and **erases the evidence** Silver is supposed to flag.
2. Inference is order-dependent and can change types between runs (idempotency / review risk).
3. Bronze is “no transformation”: landing as strings is the honest representation of the CSV. Casts belong in Silver **type validation**, not in the reader.

Empty or missing files fail the job (logged, raised) before any Gold overwrite.

### Metadata columns

Append only; never overwrite source fields:

| Column | Type | Value |
| --- | --- | --- |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` at write (timezone-naive; cluster TZ documented as session default, dates in the payload stay calendar dates) |
| `source_file_name` | `STRING` | e.g. `customers.csv` or the Volume path |

### Delta table properties

- Format: Delta; `mode("overwrite")` + `option("overwriteSchema", "true")`.
- `CREATE SCHEMA IF NOT EXISTS ecommerce.medallion` and
  `CREATE VOLUME IF NOT EXISTS ecommerce.medallion.data`.
- Use `saveAsTable("ecommerce.medallion.<table>")`; do not register a table
  with a location inside the Volume.
- Comments on tables (`ecommerce medallion bronze customers extract`).
- No partitioning (10K / 100K / 500 is small; partitioning `customer_id` would hurt).

---

## Silver Layer Design

### Structure of quality checks

**Per table, one Spark job each; checks are columns in a single plan, not Python row loops.**

```
products_silver  ← completeness + uniqueness + type  (referential = N/A)
customers_silver ← completeness + uniqueness + type  (referential = N/A)
orders_silver    ← completeness + uniqueness + type + referential
                   (referential joins Bronze/Silver parent *keys*, including duplicate parents)
        ↓
quality_metrics  ← aggregate flags (small table)
```

| Check | How it runs | Dependency |
| --- | --- | --- |
| Completeness | Per-row `IS NOT NULL` / non-blank on required STRING fields | None |
| Type / domain | Per-row try-cast + allowed-value + `quantity > 0` + `total_amount = quantity * unit_price` when both parse | None |
| Uniqueness | Window `COUNT(*) OVER (PARTITION BY key)`; **all** rows with count > 1 fail | Full table |
| Referential | Left anti / `IS IN` against parent `customer_id` / `product_id`. Null FK → completeness only, **not** orphan | Parents landed |

Spark executes these in one DAG per table (parallel stages). **Do not** split PASS and FAIL into separate writes that could drop rows.

Dimension tables: `referential_check = 'N/A'` so they are not scored as 100% referential “pass” in a misleading way. Metrics omit `N/A` or report them separately.

### `quality_check_result` column design

**Pipe-delimited fail tokens** on `quality_check_result` (not a bitmask, not JSON). Per-check `PASS`/`FAIL` columns still exist for metrics.

| `quality_check_result` | Meaning |
| --- | --- |
| `PASS` | Every applicable check passed |
| `COMPLETENESS_FAIL` | Completeness only |
| `COMPLETENESS_FAIL\|REFERENTIAL_INTEGRITY_FAIL` | Two checks failed (example) |
| `COMPLETENESS_FAIL\|UNIQUENESS_FAIL\|TYPE_VALIDATION_FAIL\|REFERENTIAL_INTEGRITY_FAIL` | All four failed |

Token order is always: completeness, uniqueness, type, referential. Multi-fail rows stay as **one row**; tokens are concatenated. Gold: `WHERE quality_check_result = 'PASS'`.

Full assembly rules: `data-quality-strategy.md`.

### One table vs multiple (clean vs flagged)

**One Silver table per entity.** Clean and flagged rows live together. No `_quarantine` table.

Rationale: FR-S1 (100% of Bronze rows), evaluators can `SELECT * WHERE quality_check_result <> 'PASS'`, and Gold is a **query filter**, not a delete.

### Quality metrics report format

Delta table `ecommerce.medallion.quality_metrics`, grain `(batch_timestamp, table_name, check_name)`.

| Column | Meaning |
| --- | --- |
| `check_name` | `completeness` / `uniqueness` / `type_validation` / `referential_integrity` |
| `total_rows` | Rows scored |
| `passed` | Check = `PASS` |
| `failed` | Check = `FAIL` |
| `pass_rate_%` | `100.0 * passed / total_rows` |

Also: `table_name`, `batch_timestamp`, `threshold`, `threshold_met`. Multi-fail rows increment `failed` on **each** failed check. Details: `data-quality-strategy.md`.

---

## Gold Layer Design

### Which rows go into aggregations

**Clean rows only for KPIs:**

- Orders: `quality_check_result = 'PASS'` **and** `order_status = 'Completed'`.
- Joins: customer/product attributes from Silver rows that **PASS** (if a customer fails uniqueness, they never PASS, so they will not appear as a Gold customer dimension — duplicate keys cannot inflate revenue).
- Failed rows remain in Silver only.

This is a filter, not a Silver delete.

### Update strategy

**Overwrite** (`mode("overwrite")` / `CREATE OR REPLACE TABLE`), not MERGE.

Daily extracts are full snapshots. MERGE is for incremental keys and would add complexity without benefit on CE. Overwrite is the idempotent story for “same file ingested twice.”

### Aggregation schemas (summary)

Full nullability and comments: [`data-model.md`](data-model.md).

1. **`sales_by_product`** — grain `product_id`. `units_sold`, `order_count`, `gross_revenue`, `total_cogs` (`quantity * cost`), `gross_margin`, `avg_unit_price`.
2. **`revenue_by_customer`** — grain `customer_id`. `completed_order_count`, `gross_revenue`, `avg_order_value`, `lifetime_value` (CSV), `country`, `customer_segment`.
3. **`customer_segmentation`** — grain `customer_segment`. `customer_count`, `total_revenue`, `avg_lifetime_value`, `avg_order_revenue`, `pct_of_revenue`.

---

## Dashboard Design

Visuals query **Gold only**. At least three charts.

| Visualization | Gold table | Intent |
| --- | --- | --- |
| 1. Top products by revenue (bar) | `sales_by_product` | Rank / mix |
| 2. Revenue by customer segment (pie or bar) | `customer_segmentation` | Segment mix |
| 3. Revenue by country (bar) **or** top customers (table/bar) | `revenue_by_customer` | Geo or concentration |
| 4. (Optional extra) Margin by category | `sales_by_product` | `SUM(gross_margin) GROUP BY category` |

### SQL queries (ANSI Spark SQL)

```sql
-- Viz 1: top products
SELECT product_name, category, units_sold, gross_revenue, gross_margin
FROM ecommerce.medallion.sales_by_product
ORDER BY gross_revenue DESC
LIMIT 15;

-- Viz 2: segment mix
SELECT customer_segment, customer_count, total_revenue, pct_of_revenue
FROM ecommerce.medallion.customer_segmentation
ORDER BY total_revenue DESC;

-- Viz 3: country (from customer grain)
SELECT country,
       COUNT(*) AS customer_count,
       SUM(gross_revenue) AS gross_revenue
FROM ecommerce.medallion.revenue_by_customer
GROUP BY country
ORDER BY gross_revenue DESC;

-- Optional: top customers
SELECT customer_id, customer_name, customer_segment, gross_revenue, lifetime_value
FROM ecommerce.medallion.revenue_by_customer
ORDER BY gross_revenue DESC
LIMIT 20;
```

If a SQL warehouse is unavailable, the same queries run in a notebook (`display`) against Unity Catalog tables — same three questions.

---

## Data Quality Validation Strategy

Silver implements the four checks and the metrics table as specified above. Thresholds (completeness >99%, uniqueness 100%, referential >99.9%) are **reported**, not enforced by deleting rows. Planted defects are supposed to miss those bars. Operational detail for how each check is coded can also live in `data-quality-strategy.md`.

---

## Debugging Approach

- Log row counts at Bronze read, Bronze write, Silver write, and Gold write; they must match Bronze→Silver 1:1.
- `WHERE quality_check_result LIKE '%COMPLETENESS_FAIL%'` (and `UNIQUENESS_FAIL` / `TYPE_VALIDATION_FAIL` / `REFERENTIAL_INTEGRITY_FAIL`) to inspect planted issues.
- Compare `quality_metrics.fail_count` to the planted list (50 null emails, 10 duplicate customers, 100/200 null FKs, 50/30 orphans, 20 duplicate orders, plus type plants).
- Re-run the same notebook: counts must not grow (overwrite).
- Do not `collect()` orders to the driver; use `count()` and `display` of samples only.
