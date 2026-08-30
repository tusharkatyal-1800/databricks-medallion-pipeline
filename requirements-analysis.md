# Requirement Analysis

## Problem Statement

The business already has daily extracts of customers, products, and orders, but those files cannot be trusted as-is for reporting. Keys collide, emails and foreign keys go missing, and some orders point at customers or products that do not exist. If those files are loaded straight into a dashboard, revenue, segment mix, and product rank will be wrong, and nobody will be able to explain *which* rows caused the distortion.

This project is a Community Edition Medallion pipeline that makes that feed usable without hiding the dirt. Bronze is a faithful landing zone: the three CSVs become Delta tables with no business cleansing. Silver keeps every row, runs four quality checks, stamps `quality_check_result`, and publishes a metrics report so failures stay auditable. Gold then builds three decision tables (sales by product, revenue by customer, customer segmentation) that analysts can chart. The dashboard is the proof that Gold is queryable, not a second transformation layer.

The engineering constraint is as important as the business one: Databricks Community Edition (Hive metastore, DBFS `FileStore`, no Unity Catalog, no Volumes). Jobs must be re-runnable, logged, and written in PEP 8 PySpark plus ANSI Spark SQL.

## Functional Requirements

### Shared (all layers)

- FR-1: Persist every curated dataset as Delta under `dbfs:/FileStore/ecommerce/`.
- FR-2: Use Hive two-level names (for example `ecommerce.orders_bronze`), never Unity Catalog three-level names or `/Volumes/` paths.
- FR-3: Generate synthetic source CSVs at the stated volumes, with about 700 planted quality defects (see Assumptions for how the listed defects map to that total).
- FR-4: Every notebook/script is idempotent: a second run on the same input replaces the same Delta tables rather than appending duplicates.

### Bronze — raw ingestion

- FR-B1: Read `customers.csv`, `orders.csv`, and `products.csv` from `dbfs:/FileStore/ecommerce/raw/`.
- FR-B2: Write one Delta table per source (`customers_bronze`, `orders_bronze`, `products_bronze`) with source column names unchanged.
- FR-B3: Apply no business transformations (no type coercion for cleansing, no dropping nulls, no dedupe, no FK repair). Optional ingest metadata only (`ingest_timestamp`, `source_file_name`) if it does not rewrite source fields.
- FR-B4: Bronze row counts must match the CSV line counts (header excluded), including defective rows.

### Silver — quality, flag, report

- FR-S1: Read Bronze Delta; emit Silver Delta tables that contain **100% of Bronze rows** (never delete or filter out failures).
- FR-S2: Apply exactly four checks, recorded as per-check flags plus a roll-up `quality_check_result` (`PASS` if all applicable checks pass; otherwise pipe-delimited tokens such as `COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL`):
  1. **Completeness** — required fields non-null. At minimum: `customer_id` / `customer_name` on customers; `email` is required for a completeness pass even though the sample plants nulls; `order_id`, `customer_id`, `product_id` on orders; `product_id` / `product_name` on products. `payment_date` is **not** a completeness failure (nullable by design).
  2. **Uniqueness** — `customer_id` unique in customers; `order_id` unique in orders; `product_id` unique in products. Duplicate key rows are all flagged, not silently collapsed.
  3. **Type / domain validation** — parseable types and allowed values (dates, numeric amounts ≥ 0, `customer_segment` in `{Premium, Standard, Basic}`, `order_status` in `{Pending, Completed, Cancelled}`, email shape when present, `quantity` integer > 0, `price`/`cost` numeric).
  4. **Referential integrity** — `orders.customer_id` exists in `customers.customer_id`; `orders.product_id` exists in `products.product_id`. Null FKs are completeness failures, not orphans. Orphans are non-null FKs with no parent.
- FR-S3: Produce a **quality metrics report** (Delta table and/or queryable view) with at least: table name, check name, fail count, pass count, fail rate, and comparison to documented thresholds (completeness >99%, uniqueness 100% unique keys, referential >99.9% valid).
- FR-S4: Planted defects must be visible in Silver flags and in the metrics report (not “fixed” in Silver).

### Gold — aggregations

- FR-G1: Build three Delta tables sourced from Silver:
  1. **Sales by product** — units and revenue by `product_id` / `product_name` / `category` (and cost/margin if price and cost are available).
  2. **Revenue by customer** — order count and revenue by `customer_id` (and name/country/segment where the join succeeds).
  3. **Customer segmentation** — counts, revenue, and average lifetime or computed revenue by `customer_segment` (and optionally country).
- FR-G2: Document and apply a consistent grain and filter: metrics use rows with `quality_check_result = 'PASS'` unless a report specifically includes failed rows. Do not delete Silver data to achieve this.
- FR-G3: Define which `order_status` values count as revenue (default assumption: `Completed` only; Pending/Cancelled excluded from revenue, still countable if needed as operational metrics).

### Dashboard

- FR-D1: Databricks SQL dashboard with **at least three** visualizations against Gold (for example: top products by revenue, revenue by segment, orders/revenue over time or by country).
- FR-D2: Visuals must be driven by Gold tables, not by re-aggregating Bronze CSVs.

## Non-Functional Requirements

### Performance

- NFR-P1: Target data volumes (10K / 100K / 500 plus planted extras from duplicates) must complete Bronze → Silver → Gold on a Community Edition cluster in one interactive session without job-orchestration features that CE does not provide.
- NFR-P2: Prefer Spark-native operations (DataFrame / Spark SQL). Do not collect 100K-row datasets to the driver except for small metric summaries.
- NFR-P3: Partitioning is optional at this scale; if used, partition Gold by low-cardinality keys only (for example `order_date` month or `category`), never by `customer_id` at 10K+ cardinality without justification.

### Idempotency

- NFR-I1: Writes use `mode("overwrite")` with `overwriteSchema` where schema can change, `CREATE OR REPLACE TABLE`, or Delta `MERGE` on a stable key. No `append` to Bronze/Silver/Gold for full daily rebuilds.
- NFR-I2: `CREATE DATABASE IF NOT EXISTS` / `dbutils.fs.mkdirs` so a first run and a tenth run both succeed.
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

1. **Platform:** Databricks Community Edition, Hive metastore, `spark` / `dbutils` in notebooks, data on `dbfs:/FileStore/ecommerce/{raw,bronze,silver,gold}/`.
2. **Daily batch, full refresh:** “Daily sales data” is modeled as a full overwrite of the three extracts, not incremental CDC.
3. **Nullable `payment_date`:** Null is valid for `Pending` (and possibly `Cancelled`). It is a type/domain issue only if `Completed` has a null or unparseable `payment_date` (clarification below).
4. **Flag ≠ drop:** Silver never removes rows. Gold filters to `PASS` for financial KPIs so dashboards are not inflated by duplicates/orphans.
5. **~700 defects:** The brief lists 460 explicit defects (50+10+100+200+50+30+20). The remaining ~240 are assumed to be **type/domain** issues (malformed emails, invalid status/segment, negative quantity/price, unparseable dates, `Completed` without `payment_date`, etc.) so the generator can hit ~700 flagged *issue instances*. One row may fail multiple checks; “~700” is counted as issue instances, not distinct rows.
6. **Orphans vs nulls:** 50 orphan `customer_id`s and 30 orphan `product_id`s are non-null values absent from the parent table. They are in addition to the 100/200 null FKs.
7. **Duplicates:** 10 extra customer rows and 20 extra order rows (same key, possibly different attributes). Uniqueness flags **all** rows that share a duplicated key.
8. **Revenue:** Sum of `total_amount` for `Completed` + `PASS` orders, unless clarified otherwise. `lifetime_value` on customers may not equal computed revenue; Gold can expose both.
9. **Products:** No planted completeness/uniqueness issues required beyond what type checks introduce; still run all four checks on products.
10. **Generator seed:** A fixed random seed is used so re-runs of data generation are stable for the evaluation.
11. **Database name:** `ecommerce` (Hive). Table names: `{entity}_{bronze|silver}` and gold names `sales_by_product`, `revenue_by_customer`, `customer_segmentation`.

## Edge Cases

| Scenario | Handling |
| --- | --- |
| Row fails multiple checks | Set each per-check flag independently; keep one row; `quality_check_result` lists every fail token (e.g. `COMPLETENESS_FAIL\|REFERENTIAL_INTEGRITY_FAIL`). Count the row in each failed check’s metrics. |
| Null `customer_id` on an order | Completeness FAIL; do **not** also count as an orphan. |
| Orphan non-null FK | Completeness PASS (if other required fields present); referential FAIL. |
| Duplicate `order_id` with different amounts | Both rows unique-check FAIL; if they otherwise PASS other checks they still must not both inflate Gold — Gold uses PASS-only, so both drop out of KPI unless we pick a survivor (see Clarifications). |
| Duplicate `customer_id` | Same as orders: uniqueness FAIL on all copies; Gold customer grain must not double-count. |
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
| Community Edition cluster restart mid-job | Re-run from Bronze; overwrite makes partial Gold from a previous success replace cleanly. |
| Dashboard warehouse vs all-purpose cluster | Queries must work on tables registered in the Hive metastore from the notebook cluster. |

## Acceptance Criteria per Layer

### Bronze

- [ ] Three Delta tables exist at `dbfs:/FileStore/ecommerce/bronze/` (or registered `ecommerce.*_bronze`).
- [ ] Column names match the CSV headers listed in the business context.
- [ ] Row counts equal CSV records, including planted defects.
- [ ] Re-run does not increase row counts (overwrite, not append).
- [ ] No quality flags and no dropped/repaired values.

### Silver

- [ ] Silver row counts equal Bronze for each entity.
- [ ] Columns include per-check flags and `quality_check_result`.
- [ ] Completeness detects 50 null emails, 100 null order `customer_id`s, 200 null `product_id`s (plus any other planted nulls).
- [ ] Uniqueness detects 10 duplicate `customer_id`s and 20 duplicate `order_id`s (all members of each duplicate set flagged).
- [ ] Referential checks detect 50 orphan customers and 30 orphan products on orders.
- [ ] Type validation detects the additional planted domain issues used to reach ~700 issue instances.
- [ ] Quality metrics report shows fail counts/rates per table and check; uniqueness is not 100%; completeness and referential rates miss the stated thresholds because of planted data.
- [ ] No Silver `DELETE`/`filter` that removes bad rows from the stored table.

### Gold

- [ ] Three Delta aggregation tables exist and are documented (grain, filters, `order_status` rule).
- [ ] Aggregations are reproducible: same Silver input → same Gold totals on re-run.
- [ ] KPI totals are not inflated by known duplicate keys or null/orphan FKs (PASS-only, or an explicit survivor rule if duplicates can still PASS other checks — they should not PASS uniqueness).
- [ ] Customer segmentation uses `Premium` / `Standard` / `Basic` (failed-segment rows excluded from that chart’s PASS path).

### Dashboard

- [ ] At least three visualizations bound to Gold.
- [ ] A viewer can answer: top products, revenue by segment (or by customer), and at least one more view (time, country, or order volume).

### Cross-cutting

- [ ] Functions have docstrings; Python is PEP 8; SQL is Spark ANSI-style.
- [ ] Logs show counts and failures; a forced missing-path error is logged and raised.
- [ ] Entire pipeline re-runnable on Community Edition without Unity Catalog.
