# Requirement Analysis

**Implementation snapshot (2026-08-31):** Pipeline is built and ran on Databricks. Bronze `bronze_*` tables are 10,000 / 100,000 / 500. Silver keeps those counts and flags rows. Gold uses `PASS` + `Completed`. Dashboard is Databricks SQL with queries 1–9. Details below that disagree with this snapshot are superseded by **As built**.

## Problem Statement

The business already has daily extracts of customers, products, and orders, but those files cannot be trusted as-is for reporting. Keys collide, emails and foreign keys go missing, and some orders point at customers or products that do not exist. If those files are loaded straight into a dashboard, revenue, segment mix, and product rank will be wrong, and nobody will be able to explain *which* rows caused the distortion.

This project is a Unity Catalog Medallion pipeline that makes that feed usable without hiding the dirt. Bronze is a faithful landing zone: the three CSVs become Delta tables with no business cleansing. Silver keeps every row, runs four quality checks, stamps `quality_check_result`, and publishes a metrics report so failures stay auditable. Gold then builds three decision tables (sales by product, revenue by customer, customer segmentation) that analysts can chart. The dashboard is the proof that Gold is queryable, not a second transformation layer.

The engineering constraint is as important as the business one: Databricks Unity Catalog with a Volume at `/Volumes/ecommerce/medallion/data/`. Jobs must be re-runnable, logged, and written in PEP 8 PySpark plus ANSI Spark SQL.

## Functional Requirements

### Shared (all layers)

- FR-1: Keep raw CSVs under `/Volumes/ecommerce/medallion/data/raw/` and
  persist every curated dataset as a managed Unity Catalog Delta table.
- FR-2: Use Unity Catalog three-level names (for example
  `ecommerce.medallion.bronze_orders`) for curated layers.
- FR-3: Generate synthetic source CSVs at the stated volumes, with about 700 planted quality defects (see Assumptions for how the listed defects map to that total).
- FR-4: Every notebook/script is idempotent: a second run on the same input replaces the same Delta tables rather than appending duplicates.

### Bronze — raw ingestion

- FR-B1: Read `customers.csv`, `orders.csv`, and `products.csv` from `/Volumes/ecommerce/medallion/data/raw/`.
- FR-B2: Write one managed Delta table per source (`bronze_customers`, `bronze_orders`, `bronze_products`) with source column names unchanged.
- FR-B3: Apply no business transformations (no dropping nulls, no dedupe, no FK repair). Typed CSV schema is used only to persist (`INT`/`DATE`/`DECIMAL`); unparsable values become null. Ingest metadata: `_ingestion_timestamp`, `_source_file`, `_batch_id`.
- FR-B4: Bronze row counts must match the CSV line counts (header excluded), including defective rows.

### Silver — quality, flag, report

- FR-S1: Read Bronze Delta; emit Silver Delta tables that contain **100% of Bronze rows** (never delete or filter out failures).
- FR-S2: Apply exactly four checks, recorded as per-check flags plus a roll-up `quality_check_result` (`PASS` if all applicable checks pass; otherwise pipe-delimited tokens such as `COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL`):
  1. **Completeness** — planted required fields: customers `email`; orders `customer_id` and `product_id`. Products have no planted completeness fields (`completeness_check = PASS`). `payment_date` is not a completeness failure.
  2. **Uniqueness** — `customer_id`, `order_id`, `product_id`. **As built:** `row_number()` over the key ordered by `_ingestion_timestamp`; first row PASS, later rows `FAIL_DUPLICATE_{key}` (10 customer extras, 20 order extras).
  3. **Type / domain validation** — dates in range, amounts > 0, segments/statuses, email `.*@.*\..*` when present, `total_amount` within 1% of qty × price, payment vs status. Extra consistency (LTV, reorder, catalog price, `order_before_signup`) maps to `TYPE_VALIDATION_FAIL`, not a fifth token.
  4. **Referential integrity** — non-null order FKs exist on parents. Null FKs are completeness only. Duplicate parent keys still count as “exists.” Uniqueness reuse of ids 1–10 removes ids 9941–9950 from customers, so orphan **customer** counts can exceed the planted 50.
- FR-S3: Produce a **quality metrics report** (Delta table and/or queryable view) with at least: table name, check name, fail count, pass count, fail rate, and comparison to documented thresholds (completeness >99%, uniqueness 100% unique keys, referential >99.9% valid).
- FR-S4: Planted defects must be visible in Silver flags and in the metrics report (not “fixed” in Silver).

### Gold — aggregations

- FR-G1: Gold Delta tables sourced from Silver PASS + Completed (except cancelled counts on trend tables, which use PASS Cancelled):
  1. **Sales by product** — `total_orders`, `total_revenue`, `avg_order_value`, `total_quantity_sold`, `profit_margin` %.
  2. **Revenue by customer** — `total_orders`, `total_revenue`, AOV, first/last order, tenure, `lifetime_value_actual`. No `country` column in the built table.
  3. **Customer segmentation** — **value** segments High-Value / Repeat / One-Time / Inactive / Other (not CSV Premium/Standard/Basic; those remain on `revenue_by_customer.customer_segment`).
  4. **Sales daily / weekly trends** — extra Gold tables for the dashboard time series.
- FR-G2: Document and apply a consistent grain and filter: metrics use rows with `quality_check_result = 'PASS'` unless a report specifically includes failed rows. Do not delete Silver data to achieve this.
- FR-G3: Define which `order_status` values count as revenue (default assumption: `Completed` only; Pending/Cancelled excluded from revenue, still countable if needed as operational metrics).

### Dashboard

- FR-D1: Databricks SQL dashboard **E-commerce Medallion** with queries 1–9 (top products, revenue buckets, value-segment pie, quality table, KPIs, daily/weekly trends, category mix, top customers). Export: `src/dashboard/E-commerce Medallion.lvdash.json`.
- FR-D2: Visuals must be driven by Gold tables, not by re-aggregating Bronze CSVs.

## Non-Functional Requirements

### Performance

- NFR-P1: Target data volumes (10K / 100K / 500 plus planted extras from duplicates) must complete Bronze → Silver → Gold on a Databricks cluster in one interactive session.
- NFR-P2: Prefer Spark-native operations (DataFrame / Spark SQL). Do not collect 100K-row datasets to the driver except for small metric summaries.
- NFR-P3: Partitioning is optional at this scale; if used, partition Gold by low-cardinality keys only (for example `order_date` month or `category`), never by `customer_id` at 10K+ cardinality without justification.

### Idempotency

- NFR-I1: Writes use `mode("overwrite")` with `overwriteSchema` where schema can change, `CREATE OR REPLACE TABLE`, or Delta `MERGE` on a stable key. No `append` to Bronze/Silver/Gold for full daily rebuilds.
- NFR-I2: `CREATE SCHEMA IF NOT EXISTS` / `CREATE VOLUME IF NOT EXISTS` / `dbutils.fs.mkdirs` so a first run and a tenth run both succeed.
- NFR-I3: Re-running the generator must recreate the same planted-issue *counts* (not necessarily the same random names if a seed is unset; see Assumptions).

### Logging and operability

- NFR-L1: Use Python `logging` (not `print` as the operational channel). Log start/end, row counts in/out, fail counts per check, and write paths.
- NFR-L2: try/except around I/O and writes; log with `logger.exception` and re-raise. Do not swallow errors.
- NFR-L3: Validate expected columns exist before writing Silver/Gold.

### Quality of code (evaluation constraints)

- NFR-C1: PEP 8 Python; Google-style docstrings on every function.
- NFR-C2: ANSI-compatible Spark SQL.
- NFR-C3: No secrets, tokens, or hardcoded workspace credentials.

## Assumptions

1. **Platform:** Databricks Unity Catalog, `spark` / `dbutils` in notebooks. Raw CSVs: `/Volumes/ecommerce/medallion/data/raw/`. Bronze/Silver/Gold are **managed** tables (`saveAsTable` / `CREATE OR REPLACE TABLE`), not Volume `LOCATION`s.
2. **Daily batch, full refresh:** “Daily sales data” is modeled as a full overwrite of the three extracts, not incremental CDC.
3. **Nullable `payment_date`:** Null is valid for `Pending` (and possibly `Cancelled`). It is a type/domain issue only if `Completed` has a null or unparseable `payment_date` (clarification below).
4. **Flag ≠ drop:** Silver never removes rows. Gold filters to `PASS` for financial KPIs so dashboards are not inflated by duplicates/orphans.
5. **~700 defects:** Completeness / uniqueness / referential plants are in the generator **in place** (row counts stay 10,000 / 100,000 / 500). The extra ~240 type/domain plants were **not** added. Extra rule `order_before_signup` fails many orders because signup and order dates are independent. “~700” is not a measured Gold/Silver instance count.
6. **Orphans vs nulls:** 50 orphan `customer_id`s and 30 orphan `product_id`s are non-null values absent from the parent table. They are in addition to the 100/200 null FKs.
7. **Duplicates:** 10 customer rows and 20 order rows **reuse** keys (ids 1–10 and 1–20). Uniqueness uses `row_number()` ordered by `_ingestion_timestamp`: first row PASS, later rows `FAIL_DUPLICATE_{key}` (10 + 20 fail rows, not every member of the group). Reusing customer ids 1–10 on rows 9941–9950 **removes** those later parent ids, so orphan **customer** counts can exceed the planted 50.
8. **Revenue:** Sum of `total_amount` for `Completed` + `PASS` orders, unless clarified otherwise. `lifetime_value` on customers may not equal computed revenue; Gold can expose both.
9. **Products:** No planted completeness/uniqueness defects. Completeness is forced `PASS`. Uniqueness and type still run. Referential is `N/A`.
10. **Generator seed:** A fixed random seed is used so re-runs of data generation are stable for the evaluation.
11. **Catalog/schema:** `ecommerce.medallion`. Tables: `bronze_*`, `*_silver`, `quality_metrics`, `sales_by_product`, `revenue_by_customer`, `customer_segmentation`, `sales_daily_trends`, `sales_weekly_trends`.

## Edge Cases

| Scenario | Handling |
| --- | --- |
| Row fails multiple checks | Set each per-check flag independently; keep one row; `quality_check_result` lists every fail token (e.g. `COMPLETENESS_FAIL\|REFERENTIAL_INTEGRITY_FAIL`). Count the row in each failed check’s metrics. |
| Null `customer_id` on an order | Completeness FAIL; do **not** also count as an orphan. |
| Orphan non-null FK | Completeness PASS (if other required fields present); referential FAIL. |
| Duplicate `order_id` with different amounts | First ingest-timestamp row uniqueness PASS; later rows `FAIL_DUPLICATE_order_id`. Gold is PASS + Completed, so only the first copy can enter KPIs if it otherwise PASSes. |
| Duplicate `customer_id` | Same keep-first rule. Reused keys also change which parent ids exist for referential checks. |
| `payment_date` null | Allowed for non-completed orders; does not fail completeness. |
| `total_amount` ≠ `quantity * unit_price` | Type/domain FAIL (inconsistent numeric business rule). |
| Invalid `customer_segment` or `order_status` | Type/domain FAIL. |
| Email null vs email malformed | Null → completeness; malformed non-null → type validation. |
| Order references a duplicated customer key | Referential check treats parent as “exists if any parent row has that id”; uniqueness of the parent is a separate customer-table failure. |
| Missing CSV (path does not exist) | Fail Bronze immediately: log the path, raise; do not write Silver/Gold from a previous run’s leftovers without an explicit, logged overwrite of empty (we do not silently keep stale Gold). |
| Empty CSV (0-byte file, or header only / 0 data rows) | Fail Bronze with a logged error if row count is 0 for a required extract (`customers` / `orders` / `products`). Do not create an empty Gold dashboard feed. Header-only is treated as empty, not as a successful ingest. |
| Same file ingested twice (re-run / duplicate job) | Idempotent overwrite: Bronze, Silver, and Gold row counts and KPI totals must match the first successful run. Never `append`. Duplicate ingest must not double 10K/100K/500 rows or planted defects. |
| Timezone differences in dates (`signup_date`, `order_date`, `payment_date`) | CSVs carry calendar dates (and naive timestamps if present), not an explicit timezone. Store as Spark `DATE` or timezone-naive timestamp; do **not** shift days via session `spark.sql.session.timeZone` or `from_utc_timestamp` unless a source TZ is provided. Compare dates as calendar dates. If a value includes an offset (`Z`, `+05:30`), parse it and persist the UTC instant **and** the calendar date in the source TZ only after documenting that rule; otherwise type/domain FAIL the unparseable/ambiguous value. Dashboard “by day” uses the stored calendar date, not the cluster’s local zone. |
| Negative `quantity` (or negative `unit_price` / `price` / `cost` / `stock_quantity` where the field is a count or money amount) | Type/domain FAIL; keep the row in Silver with flags. Do not abs() or drop it. Gold PASS-only KPIs exclude it so revenue/units are not reduced by bad signs. `quantity` must be a positive integer for PASS. |
| Extra/missing CSV columns | Fail fast after schema validation; do not infer-and-continue with wrong types. |
| Unicode in `customer_name` / `product_name` | Allowed; store as string. |
| Cluster restart mid-job | Re-run from Bronze; overwrite makes partial Gold from a previous success replace cleanly. |
| Dashboard warehouse vs all-purpose cluster | Queries must work on Unity Catalog tables (`ecommerce.medallion.*`). |

## Acceptance Criteria per Layer

### Bronze

- [x] Three managed Delta tables exist as `ecommerce.medallion.bronze_*`.
- [x] Column names match the CSV headers listed in the business context.
- [x] Row counts equal CSV records, including planted defects (10,000 / 100,000 / 500).
- [x] Re-run does not increase row counts (overwrite, not append).
- [x] No quality flags and no dropped/repaired values (typed persist only).

### Silver

- [x] Silver row counts equal Bronze for each entity.
- [x] Columns include per-check flags and `quality_check_result`.
- [x] Completeness detects 50 null emails, 100 null order `customer_id`s, 200 null `product_id`s.
- [x] Uniqueness flags 10 later duplicate `customer_id`s and 20 later duplicate `order_id`s (keep-first).
- [x] Referential checks detect planted product orphans (30) and customer orphans (planted 50 plus extra from reused ids).
- [ ] Type validation does **not** see ~240 extra planted domain issues (generator never added them). Extra `order_before_signup` drives most type fails.
- [x] Quality metrics report (`field_checked`, `pass_rate_pct`, thresholds) is written each run.
- [x] No Silver `DELETE`/`filter` that removes bad rows from the stored table.

### Gold

- [x] Five Delta aggregation tables exist (`sales_by_product`, `revenue_by_customer`, `customer_segmentation`, `sales_daily_trends`, `sales_weekly_trends`).
- [x] Aggregations are reproducible: same Silver input → same Gold totals on re-run.
- [x] KPI totals use PASS + Completed (trends also count PASS Cancelled for cancelled volume).
- [x] `customer_segmentation` is value-based (`High-Value` / `Repeat` / `One-Time` / `Inactive` / `Other`). CSV Premium/Standard/Basic stays on `revenue_by_customer.customer_segment`.

### Dashboard

- [x] Databricks SQL dashboard **E-commerce Medallion** with queries 1–9 (more than three visuals).
- [x] A viewer can answer: top products, revenue buckets, value segments, daily/weekly trends, category mix, top customers, and Silver quality rates.

### Cross-cutting

- [x] Functions have docstrings; Python is PEP 8; SQL is Spark ANSI-style.
- [x] Logs show counts and failures; missing-path errors are logged and raised.
- [x] Entire pipeline re-runnable using Unity Catalog + the raw Volume.
