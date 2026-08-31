# Design Notes

**Implementation snapshot (2026-08-31):** Matches the running Databricks job. Bronze is **typed** (`src/bronze/schemas.py`), not all-STRING. Metadata columns are `_ingestion_timestamp`, `_source_file`, `_batch_id`. Uniqueness is keep-first. Gold includes daily/weekly trend tables. Dashboard is queries 1–9 on Gold + `quality_metrics`.

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
       ecommerce.medallion.sales_daily_trends
       ecommerce.medallion.sales_weekly_trends
        │
        ▼
Databricks SQL Dashboard (queries 1–9 on Gold + quality_metrics)
```

**Platform:** Databricks Unity Catalog. Catalog `ecommerce`, schema `medallion`, Volume `data`. Paths under `/Volumes/ecommerce/medallion/data/`.

### Separation of concerns

| Layer | Owns | Must not do |
| --- | --- | --- |
| **Raw (Volume)** | Immutable CSV extracts | Parsing, KPI logic |
| **Bronze** | Faithful land: 1:1 with file rows, explicit typed schema, ingest lineage | Dedupe, FK repair, quality flags, business cleansing |
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
| Gold Delta | Managed table | `sales_by_product`, `revenue_by_customer`, `customer_segmentation`, `sales_daily_trends`, `sales_weekly_trends` |

### Schema inference vs schema definition

**Choice: explicit typed schema from `src/bronze/schemas.py`. Do not use `inferSchema=True`.**

Reasons:

1. Production-like contract (INT / DATE / DECIMAL(10,2) / STRING). Unparsable CSV values become null; Silver type checks score remaining domain rules.
2. Inference is order-dependent and can change types between runs.
3. Bronze still does **no business cleansing** (no drop, no dedupe, no FK repair). Typing is persist-only.

Empty or missing files fail the job (logged, raised) before any Gold overwrite.

### Metadata columns

Append only; never overwrite source fields:

| Column | Type | Value |
| --- | --- | --- |
| `_ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` at write |
| `_source_file` | `STRING` | Source file name (e.g. `customers.csv`) |
| `_batch_id` | `STRING` | Run/batch identifier |

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
| Completeness | Customers: `email`. Orders: `customer_id`, `product_id`. Products: always `PASS`. Per-field tokens `FAIL_NULL_{field}` | None |
| Type / domain | Dates in range, allowed segments/statuses, email `.*@.*\..*`, qty/price > 0, `total_amount` within **1% relative** of qty × price, extra business rules (`order_before_signup`, catalog price, LTV, reorder). Fail tokens `FAIL_INVALID_{name}`; roll-up token `TYPE_VALIDATION_FAIL` | Optional parent join helpers |
| Uniqueness | `row_number()` `PARTITION BY key ORDER BY _ingestion_timestamp`; **first row PASS**, later `FAIL_DUPLICATE_{key}` | Full table |
| Referential | Left anti / `IS IN` against parent keys. Null FK → completeness only. Tokens `FAIL_ORPHAN_{field}` | Parents landed |

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

Delta table `ecommerce.medallion.quality_metrics`. Grain is **per table, check, and `field_checked`** (not one row per check). Built in `src/silver/create_silver_tables.py`.

| Column | Meaning |
| --- | --- |
| `table_name` | `customers` / `orders` / `products` |
| `check_name` | `completeness` / `uniqueness` / `type_validation` / `referential_integrity` / `overall` |
| `field_checked` | Column or rule name (`email`, `order_id`, `amount_formula`, `_all`, …) |
| `total_rows` | Table row count |
| `applicable_rows` | Rows scored for that field |
| `passed` / `failed` | Counts on applicable rows |
| `pass_rate_pct` | `100.0 * passed / applicable_rows` |
| `threshold` / `threshold_met` | Documented bar vs observed rate |
| `batch_timestamp` | Run time |

Multi-fail rows increment `failed` on **each** failed field/check. Details: `data-quality-strategy.md`.

---

## Gold Layer Design

### Which rows go into aggregations

**Clean rows only for KPIs:**

- Product/customer Gold: `quality_check_result = 'PASS'` **and** `order_status = 'Completed'`.
- Trend Gold: all PASS orders; Completed for revenue/AOV/items; Cancelled counted separately.
- Joins use PASS customers/products. Keep-first uniqueness means the first copy of a reused key can still PASS uniqueness.
- Failed rows remain in Silver only.

This is a filter, not a Silver delete.

### Update strategy

**Overwrite** (`mode("overwrite")` / `CREATE OR REPLACE TABLE`), not MERGE.

Daily extracts are full snapshots. MERGE is unnecessary. Overwrite is the idempotent story for “same file ingested twice.”

### Aggregation schemas (summary)

Full nullability and comments: [`data-model.md`](data-model.md). SQL: `src/gold/01`–`04`.

1. **`sales_by_product`** — grain `product_id` (INT). `total_orders`, `total_revenue`, `avg_order_value`, `total_quantity_sold`, `profit_margin` (%).
2. **`revenue_by_customer`** — grain `customer_id`. `total_orders`, `total_revenue`, `avg_order_value`, first/last order, `customer_tenure_days`, `lifetime_value_actual`. No `country`.
3. **`customer_segmentation`** — grain `segment_type` (value buckets, not CSV Premium/Standard/Basic).
4. **`sales_daily_trends` / `sales_weekly_trends`** — PASS orders; Completed for revenue/AOV/items; Cancelled counts separate.

---

## Dashboard Design

Canonical SQL: `src/dashboard/dashboard_queries.sql`. Lakeview export: `src/dashboard/E-commerce Medallion.lvdash.json`. Bind datasets in **Databricks SQL** (warehouse), not the all-purpose cluster.

| Query | Source | Intent |
| --- | --- | --- |
| 1 Top 10 products | `sales_by_product` | Revenue rank |
| 2 Revenue buckets | `revenue_by_customer` | Distribution |
| 3 Value segments | `customer_segmentation` | High-Value / Repeat / One-Time / Inactive / Other |
| 4 Quality table | `quality_metrics` | Per-field pass rates |
| 5 KPI tiles | Gold + Silver | Headline metrics |
| 6–7 Daily / weekly trends | `sales_daily_trends` / `sales_weekly_trends` | Time series |
| 8 Category mix | `sales_by_product` | Category revenue |
| 9 Top customers | `revenue_by_customer` | Concentration |

There is **no country chart** (Gold customer grain has no `country`). Screenshots: `src/dashboard/screenshots/`.

---

## Data Quality Validation Strategy

Silver implements the four checks and the metrics table as specified above. Thresholds (completeness >99%, uniqueness 100%, referential >99.9%) are **reported**, not enforced by deleting rows. Planted defects are supposed to miss those bars. Operational detail for how each check is coded can also live in `data-quality-strategy.md`.

---

## Debugging Approach

- Log row counts at Bronze read, Bronze write, Silver write, and Gold write; they must match Bronze→Silver 1:1.
- `WHERE quality_check_result LIKE '%COMPLETENESS_FAIL%'` (and `UNIQUENESS_FAIL` / `TYPE_VALIDATION_FAIL` / `REFERENTIAL_INTEGRITY_FAIL`) to inspect planted issues.
- Compare `quality_metrics.failed` to planted lists (50 null emails, 10 later duplicate customers, 100/200 null FKs, ~30 product orphans, extra customer orphans from reused ids, 20 later duplicate orders). Do not expect ~240 planted type fails.
- Re-run the same notebook: counts must not grow (overwrite).
- Do not `collect()` orders to the driver; use `count()` and `display` of samples only.
