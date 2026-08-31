# Database setup notes

`database/schema.sql` is the Databricks SQL DDL for catalog, schema, volume, and empty managed Delta tables. Pipeline jobs (`ingest_all.py`, `create_silver_tables.py`, `create_gold_tables.py`) still **overwrite** those tables with data. You do not have to run this script first if the jobs already call `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS`, but it is the documented contract for reviewers.

Dashboard queries live in `database/dashboard_queries.sql` (same SQL as `src/dashboard/dashboard_queries.sql`). They read Gold after the jobs have loaded data.

---

## How to run `schema.sql`

1. Sync this repo into the Databricks workspace.
2. Open `database/schema.sql`.
3. Attach a **SQL warehouse** (or an all-purpose cluster that can run SQL). A warehouse is enough; this file has no Python.
4. Run all statements top to bottom.
5. Confirm objects:

```sql
SHOW SCHEMAS IN ecommerce;
SHOW TABLES IN ecommerce.medallion;
SHOW VOLUMES IN ecommerce.medallion;
DESCRIBE TABLE EXTENDED ecommerce.medallion.bronze_customers;
```

`Type` / `Provider` should be Delta. `Location` is the catalog’s **managed** storage, not `/Volumes/ecommerce/medallion/data/...`.

If `CREATE CATALOG ecommerce` is denied, ask an admin to create catalog `ecommerce` and grant `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `READ VOLUME`, and `WRITE VOLUME` on `ecommerce.medallion` and volume `data`. Then re-run from `CREATE DATABASE IF NOT EXISTS ecommerce.medallion`.

`CREATE DATABASE IF NOT EXISTS medallion_ecommerce` may create an extra empty schema in the current catalog. Jobs never write there. It is only the Hive-style name from the evaluation brief.

After DDL, run the pipeline on an **all-purpose cluster**:

1. Upload `data/*.csv` to `/Volumes/ecommerce/medallion/data/raw/`.
2. `src/bronze/ingest_all.py`
3. `src/silver/create_silver_tables.py`
4. `src/gold/create_gold_tables.py`

Re-running `schema.sql` is safe (`IF NOT EXISTS`). It does **not** empty tables that already have data.

---

## Naming conventions

| Brief / Hive wording | This workspace |
| --- | --- |
| `CREATE DATABASE medallion_ecommerce` | Catalog `ecommerce` + schema `medallion` |
| Two-level `medallion_ecommerce.orders` | Three-level `ecommerce.medallion.<table>` |

Unity Catalog: `catalog.schema.table`.

| Object | Name |
| --- | --- |
| Catalog | `ecommerce` |
| Schema | `medallion` |
| Volume (files only) | `data` |
| Bronze | `bronze_customers`, `bronze_orders`, `bronze_products` |
| Silver entities | `customers_silver`, `orders_silver`, `products_silver` |
| Silver report | `quality_metrics` |
| Gold | `sales_by_product`, `revenue_by_customer`, `customer_segmentation`, `sales_daily_trends`, `sales_weekly_trends` |

Silver flag columns include `quality_check_result` plus per-check columns (`completeness_check`, `uniqueness_check`, `type_validation_check`, `business_logic_check`, `referential_integrity_check`). Business-rule fails still roll up to `TYPE_VALIDATION_FAIL` on `quality_check_result` (no fifth token).

---

## Delta table storage locations

**Raw CSVs (files, not tables)**

`/Volumes/ecommerce/medallion/data/raw/{customers,orders,products}.csv`

The Volume is for **files**. Do not `CREATE TABLE ... LOCATION '/Volumes/ecommerce/medallion/data/bronze/...'`. Unity Catalog rejects a Volume path as a registered table location.

**Bronze / Silver / Gold / quality_metrics**

Managed Delta tables:

```text
ecommerce.medallion.<table>
```

Created with `USING DELTA` and no `LOCATION` clause. Spark jobs use `saveAsTable` / `CREATE OR REPLACE TABLE ... USING DELTA AS`. Storage is the catalog’s managed root (workspace default), not the Volume.

| Layer | Storage |
| --- | --- |
| Raw | Volume files under `/Volumes/ecommerce/medallion/data/raw/` |
| Curated | Managed UC Delta (`ecommerce.medallion.*`) |

Overwrite each run (`mode("overwrite")` / `CREATE OR REPLACE`). Do not `APPEND` Bronze, Silver, or Gold.
