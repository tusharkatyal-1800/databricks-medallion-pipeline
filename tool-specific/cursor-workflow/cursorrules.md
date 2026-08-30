# Databricks Medallion Pipeline — Cursor Rules

You are generating production-style PySpark / Spark SQL for an e-commerce
Medallion pipeline (Bronze → Silver → Gold → Dashboard) on Databricks
with Unity Catalog and Volumes. Follow every rule below. Prefer the smallest
change that satisfies the request.

## Project context

- Platform: Databricks with Unity Catalog. Files live on a managed Volume.
- Languages: Python 3, PySpark, ANSI Spark SQL.
- Dataset (CSV): customers (~10K rows), orders (~100K rows), products (~500 rows).
- Bronze: raw ingestion only — no business transformations.
- Silver: four data-quality checks (completeness, uniqueness, data type
  verification, referential integrity).
- Gold: three aggregation tables (sales by product, revenue by customer,
  customer segmentation).
- Dashboard: Databricks SQL Dashboard with at least three visualizations.

## 1. Unity Catalog and Volumes

- Use three-level names: `ecommerce.medallion.<table>`.
- Store all pipeline files on the Volume
  `/Volumes/ecommerce/medallion/data/` (catalog `ecommerce`, schema
  `medallion`, volume `data`).
- Use the canonical Volume root for every pipeline file.
- Use `spark` / `dbutils` from the Databricks notebook runtime. Do not
  assume a local `SparkSession.builder` unless the user asked for local tests.
- Create catalog/schema/volume if missing (`CREATE CATALOG/SCHEMA/VOLUME IF NOT
  EXISTS`) when the workspace allows it.

```python
# BAD
df.write.saveAsTable("ecommerce.orders_bronze")
spark.read.csv("/mnt/legacy/ecommerce/raw/orders.csv")

# GOOD
df.write.format("delta").mode("overwrite").saveAsTable(
    "ecommerce.medallion.bronze_orders"
)
spark.read.csv("/Volumes/ecommerce/medallion/data/raw/orders.csv", header=True)
```

## 2. Delta Lake for all tables

- Persist Bronze, Silver, and Gold as Delta (`format("delta")` or `USING DELTA`).
- Do not leave curated layers as CSV/JSON/Parquet-only tables.
- Register tables with a Volume location under
  `/Volumes/ecommerce/medallion/data/...`.
- Use Delta options that support re-runs (`overwriteSchema` when schema can change).

```python
# BAD
df.write.mode("overwrite").parquet("/Volumes/ecommerce/medallion/data/silver/orders")

# GOOD
(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/Volumes/ecommerce/medallion/data/silver/orders")
)
```

## 3. PEP 8 for all Python

- snake_case for functions and variables; UPPER_SNAKE for constants.
- Imports: stdlib, then third-party, then local; unused imports forbidden.
- Max line length 88–100; break Spark chains with parentheses, not backslashes.
- Two blank lines between top-level functions; spaces around `=` in kwargs.
- No wildcard imports (`from pyspark.sql.functions import *`).

```python
# BAD
from pyspark.sql.functions import *
def LoadBronze(Path):
    return spark.read.csv(Path,header=True)

# GOOD
from pyspark.sql import functions as F

def load_bronze(path: str):
    return spark.read.csv(path, header=True, inferSchema=False)
```

## 4. ANSI-compatible Spark SQL

- Write Spark SQL that is ANSI-friendly: explicit `JOIN ... ON`, `COALESCE`,
  `CAST`, `CASE WHEN`, standard date functions (`DATE_TRUNC`, `TO_DATE`).
- Avoid T-SQL / PL/SQL / BigQuery-only syntax (`TOP n`, `IFNULL` as default,
  `QUALIFY` unless necessary and documented, `SELECT INTO`, variables).
- Quote identifiers with backticks only when required; prefer unquoted
  snake_case names.

```sql
-- BAD
SELECT TOP 10 * FROM orders WHERE order_date = GETDATE()

-- GOOD
SELECT *
FROM ecommerce.medallion.orders_silver
WHERE CAST(order_date AS DATE) = CURRENT_DATE
LIMIT 10
```

## 5. Docstrings on every function

- Every public function (and notebook helper) needs a docstring: one-line
  summary, Args, Returns, and Raises when applicable.
- Use Google-style docstrings.

```python
def apply_completeness_check(df, required_columns):
    """Flag rows that have nulls in any required column.

    Args:
        df: Input Spark DataFrame.
        required_columns: Column names that must be non-null.

    Returns:
        DataFrame with completeness flags added (rows never dropped).
    """
```

## 6. Unity Catalog Volume paths only

- All pipeline files live under
  `/Volumes/ecommerce/medallion/data/...`. Do not use mounts, direct cloud
  object-store URLs, or local `C:\\` paths in pipeline code.
- Canonical layout:
  - Raw CSV: `/Volumes/ecommerce/medallion/data/raw/`
  - Bronze: `/Volumes/ecommerce/medallion/data/bronze/`
  - Silver: `/Volumes/ecommerce/medallion/data/silver/`
  - Gold: `/Volumes/ecommerce/medallion/data/gold/`
- Create parent objects if missing (`CREATE SCHEMA IF NOT EXISTS`,
  `CREATE VOLUME IF NOT EXISTS`, `dbutils.fs.mkdirs`).

## 7. Error handling and logging

- Every script/notebook uses `logging` (not `print` for operational messages).
- Wrap I/O, table writes, and SQL execution in try/except; log the error
  with context; do not swallow exceptions.
- Validate inputs (paths exist, required columns present) before writes.

```python
import logging

logger = logging.getLogger(__name__)

def write_delta(df, path: str) -> None:
    """Write a DataFrame as Delta, overwriting the location."""
    try:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        logger.info("Wrote Delta table to %s", path)
    except Exception:
        logger.exception("Failed to write Delta table to %s", path)
        raise
```

## 8. Data quality: flag, never delete

- Silver applies exactly four checks: completeness, uniqueness, data type
  verification, referential integrity.
- Add a `quality_check_result` column (and per-check flag columns if useful).
- Bad rows are FLAGED and retained. Never `filter` them out, `drop`,
  `DELETE`, or overwrite-exclude them in Silver.
- Gold aggregations may *exclude* failed rows from metrics via
  `WHERE quality_check_result = 'PASS'` (or equivalent). That is a
  downstream filter, not a delete of source data.

```python
# BAD — drops bad rows
silver_df = df.filter(F.col("customer_id").isNotNull())

# GOOD — keeps all rows, records the outcome
completeness_ok = F.col("customer_id").isNotNull()
df = df.withColumn("completeness_check", F.when(completeness_ok, "PASS").otherwise("FAIL"))
```

## 9. Idempotence (safe to re-run)

- Every job must produce the same curated result when run twice on the
  same input.
- Use `mode("overwrite")`, `CREATE OR REPLACE TABLE`, or Delta `MERGE`
  with a stable key. Do not `append` to Bronze/Silver/Gold on each run.
- Use `CREATE SCHEMA IF NOT EXISTS`, `CREATE VOLUME IF NOT EXISTS`, and
  `CREATE TABLE IF NOT EXISTS` (or replace) — never fail because the object
  already exists.
- Do not depend on widgets or timestamps for primary table contents unless
  they are overwritten each run.
- If using checkpoints, document and reset them for batch re-runs.

```python
# BAD
df.write.format("delta").mode("append").saveAsTable("ecommerce.medallion.bronze_orders")

# GOOD
(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecommerce.medallion.bronze_orders")
)
```

## Layer-specific constraints

### Bronze
- Read CSV from `/Volumes/ecommerce/medallion/data/raw/`.
- Land as Delta with original columns; no cleansing, no type coercion
  beyond what is required to persist (prefer strings if types are uncertain).
- Optional metadata only if it does not rewrite source fields
  (`ingest_timestamp`, `source_file_name`).

### Silver
- Read Bronze Delta; run the four checks; add `quality_check_result`.
- Keep 100% of Bronze rows.

### Gold
- Build exactly: sales by product, revenue by customer, customer segmentation.
- Source from Silver; document whether metrics use PASS-only rows.

### Dashboard
- Databricks SQL queries against Gold Delta tables; at least three charts
  (for example: revenue by product, revenue by customer segment, order volume).

## What not to generate

- Secrets, tokens, or hardcoded workspace URLs with credentials.
- `eval`, dynamic SQL built from unsanitized strings.
- Deletion of “bad” data, sample-row truncation of Silver, or non-Delta sinks
  for curated tables.

## End-of-response documentation summary

Always finish every reply with a short **Documentation summary** the user can
paste into evaluation notes (`ai-prompts/`, `tool-workflow.md`, etc.).

- Place it last, after the full answer.
- 3–6 bullets or 4–8 sentences.
- State: what was done, which files changed, key decisions, and any follow-up.
- No secrets. Keep it copy-paste ready (plain language, no internal jargon).
