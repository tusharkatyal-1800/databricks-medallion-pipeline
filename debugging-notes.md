# Debugging Notes

Issues found while building and running the e-commerce Medallion pipeline on Databricks Unity Catalog. Fixes are in the repo; this file is the evaluation trail. Detailed Cursor prompts for the Bronze import work live in `ai-prompts/debugging.md`.

## How we debug

1. Read the Databricks error (cluster vs warehouse, import vs `%run`, Unity Catalog).
2. Confirm paths: raw CSVs on `/Volumes/ecommerce/medallion/data/raw/`; curated data as managed tables `ecommerce.medallion.<table>`.
3. Compare row counts Bronze → Silver (must match). Compare Silver flags to planted defects.
4. Re-run the same job (overwrite). Counts must not grow.
5. Do not `collect()` the orders table; use `count()` and small `display` samples.

---

## Issue 1 — Python notebook on a SQL warehouse

**Symptom:** `ingest_all.py` would not run. Compute attached was a SQL warehouse.

**Cause:** SQL warehouses execute SQL. They do not run PySpark notebooks.

**Fix:** Attach an **all-purpose / personal cluster** for every `src/**/*.py` job (Bronze, Silver, Gold orchestrator). Use a SQL warehouse only for `.sql` files and the dashboard.

**Lesson:** Cluster type is the first check, before changing code.

---

## Issue 2 — `ModuleNotFoundError: No module named 'common'`

**Symptom:** Databricks `%run` / notebook could not import `common.config`.

**Cause:** Workspace layout is not a local package install. `sys.path` did not include `src`.

**Fix:** Entry notebooks add the repo `src` directory to `sys.path` (no hardcoded `/Workspace/Users/...` in the final ingest). Shared names live in `src/common/config.py`.

**Lesson:** Databricks does not automatically treat the Git folder as `PYTHONPATH`.

---

## Issue 3 — `NotebookImportException` (notebook vs module)

**Symptom:** After path fixes: *Unable to import module. The file appears to be a notebook.*

**Cause:** Any `.py` that starts with `# Databricks notebook source` is a notebook. Notebooks cannot be `import`ed; they can only be `%run`.

**Fix:** Split files:

| Role | Header | Examples |
| --- | --- | --- |
| Job entry | `# Databricks notebook source` | `ingest_all.py`, `01_ingest_*.py`, `create_silver_tables.py`, `create_gold_tables.py` |
| Library | **No** header | `ingestion.py`, `schemas.py`, `config.py`, `completeness.py`, `uniqueness.py`, `type_validation.py`, `business_logic.py`, `referential.py` |

**Lesson:** One thin notebook + importable helpers. Mixing both in one file fails.

---

## Issue 4 — `NameError: name 'spark' is not defined`

**Symptom:** Imports worked; ingest helpers crashed on `spark`.

**Cause:** Databricks injects `spark` / `dbutils` into notebooks, not into imported modules.

**Fix:** Pass `spark` into library functions (and `dbutils` where needed). Do not call `SparkSession.builder` in pipeline jobs.

**Lesson:** Treat `spark` as a runtime argument, not a global in library code.

---

## Issue 5 — Cannot register a table with `LOCATION` inside a Volume

**Symptom:** `CREATE TABLE ... LOCATION '/Volumes/ecommerce/medallion/data/...'` rejected by Unity Catalog.

**Cause:** Managed Volume paths are for **files** (raw CSVs). They are not legal table locations.

**Fix:** Raw = Volume files. Bronze / Silver / Gold / `quality_metrics` = **managed** Delta via `saveAsTable("ecommerce.medallion.<name>")` or `CREATE OR REPLACE TABLE ... USING DELTA AS`. No Volume `LOCATION` on curated tables.

**Lesson:** Volume ≠ table storage for this catalog setup.

---

## Issue 6 — Duplicate keys planted in place (extra orphans)

**Symptom:** Local validator expected 50 orphan `customer_id`s; a full FK scan saw **157**.

**Cause:** Duplicate plants **reuse** ids 1–10 on customer rows 9941–9950. Those later ids disappear as parents. Orders that still point at 9941–9950 become extra orphans. Row counts stay 10,000 / 100,000 / 500 (not +10 / +20 files).

**Fix:** Document planted vs side-effect orphans. Uniqueness uses `row_number()` keep-first (`FAIL_DUPLICATE_*` on later rows only). Referential still treats “parent id exists” independently of uniqueness.

**Lesson:** In-place key reuse changes the parent set. Do not treat 50 as a hard Silver fail count.

---

## Issue 7 — `order_before_signup` crushed Gold PASS volume

**Symptom:** Orders overall `quality_check_result = PASS` about **64%**. Gold looked “too small” vs 100K orders.

**Cause:** Extra business rule `order_before_signup` is **not** planted. Signup dates and order dates are generated independently, so many valid-looking orders fail type/business and roll up to `TYPE_VALIDATION_FAIL`. The generator also never planted the extra ~240 type/domain defects from the original brief.

**Fix:** Keep the rule (it is a real consistency check). Document it in README / data-quality-strategy. Gold correctly uses `PASS` + `Completed` only. Do not drop Silver rows to “fix” the rate.

**Lesson:** Extra join rules need a matching generator, or they dominate metrics.

---

## Issue 8 — Product `stock_quantity = 0`

**Symptom:** Type check failed in-stock products with zero stock.

**Cause:** Rule used `> 0` instead of `>= 0`. Zero on-hand is valid; negative is not.

**Fix:** `stock_quantity >= 0` in `src/silver/type_validation.py`.

---

## Issue 9 — Git push to `main` failed

**Symptom:** `git push -u origin main` rejected / no such branch.

**Cause:** This repo’s default branch is **`master`**.

**Fix:** `git push origin master` (or `git push` while on `master`). Do not assume GitHub `main`.

---

## Issue 10 — Dashboard widgets empty / queries missing on canvas

**Symptom:** SQL Editor queries saved, but Lakeview dashboard showed no data or no datasets.

**Cause:** Lakeview often needs SQL **pasted as dashboard datasets**. A warehouse must run those queries. The all-purpose cluster that built Gold is the wrong compute for Databricks SQL.

**Fix:** SQL warehouse + queries 1–9 from `src/dashboard/dashboard_queries.sql` as datasets. Export: `src/dashboard/E-commerce Medallion.lvdash.json`. Screenshots under `src/dashboard/screenshots/`.

---

## Issue 11 — `.gitignore` hid sample CSVs

**Symptom:** Generated `data/*.csv` would not be committed.

**Cause:** First `.gitignore` ignored `data/*.csv` so the venv stay clean.

**Fix:** Stop ignoring those CSVs so the evaluation repo includes seed-42 extracts. Venv / `__pycache__` / `.env` stay ignored.

---

## Runtime checks that passed

| Layer | What we checked | Result |
| --- | --- | --- |
| Data gen | `validate_generated_data.py` | Planted 50/10/100/200/50/30/20 |
| Bronze | `ingest_all.py` on cluster | 10,000 / 100,000 / 500 SUCCESS |
| Silver | `create_silver_tables.py` | Same counts as Bronze; flags only |
| Gold | `create_gold_tables.py` | 500 / 8,782 / 1,096 / 158 / 5 SUCCESS |
| Re-run | Overwrite | Counts did not grow |

---

## Useful inspections

```sql
-- Flagged Silver orders
SELECT *
FROM ecommerce.medallion.orders_silver
WHERE quality_check_result <> 'PASS'
LIMIT 50;

-- Metrics vs planted issues
SELECT table_name, check_name, field_checked, failed, pass_rate_pct, threshold_met
FROM ecommerce.medallion.quality_metrics
ORDER BY table_name, check_name, field_checked;
```
