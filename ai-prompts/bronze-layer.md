# AI Prompts — Bronze Layer (Phase 3) - Raw ingestion

## Prompt 1: Schema definition file for all the 3 csv files

**PROMPT SENT:**
Create a schema definition file `src/bronze/schemas.py` for my Bronze layer ingestion.
I need PySpark StructType schemas for:
1. customers: customer_id (IntegerType), customer_name (StringType), email (StringType), 
   country (StringType), signup_date (DateType), customer_segment (StringType), 
   lifetime_value (DecimalType(10,2))
2. orders: order_id (IntegerType), customer_id (IntegerType), order_date (DateType), 
   product_id (IntegerType), quantity (IntegerType), unit_price (DecimalType(10,2)), 
   total_amount (DecimalType(10,2)), order_status (StringType), payment_date (DateType)
3. products: product_id (IntegerType), product_name (StringType), category (StringType), 
   price (DecimalType(10,2)), cost (DecimalType(10,2)), stock_quantity (IntegerType), 
   reorder_level (IntegerType)
Use explicit schema definition (NOT inferSchema) 
Each schema should be a module-level constant (CUSTOMERS_SCHEMA, ORDERS_SCHEMA, PRODUCTS_SCHEMA).


**AI RESPONSE SUMMARY:**
Created src/bronze/schemas.py with explicit, nullable PySpark contracts (no inferSchema).

Constant	Types
CUSTOMERS_SCHEMA : 
IntegerType id, strings, DateType signup, DecimalType(10,2)

ORDERS_SCHEMA : 
int ids/qty, dates, money decimals, string status

PRODUCTS_SCHEMA :
int id/stock/reorder, strings, DecimalType(10,2) price/cost

All fields are nullable so planted null emails and FKs, plus empty payment_date, still land. get_bronze_schema("customers") is a small lookup helper.

Example ingest (for the next notebook):

from src.bronze.schemas import CUSTOMERS_SCHEMA
df = spark.read.csv(
    "/Volumes/ecommerce/medallion/data/raw/customers.csv",
    header=True,
    schema=CUSTOMERS_SCHEMA,
    mode="PERMISSIVE",
    dateFormat="yyyy-MM-dd",
)
Values that cannot cast to the declared type become null (Spark’s usual behaviour). That is expected with a typed Bronze contract.

Documentation summary

Added src/bronze/schemas.py with CUSTOMERS_SCHEMA, ORDERS_SCHEMA, and PRODUCTS_SCHEMA.
Used explicit StructType (no schema inference) as the CSV data contract.
Columns are nullable; money fields use DecimalType(10,2).

**YOUR EVALUATION:**
This schema definition file is important so that if anyday our project got csv files of different schemas then our pipeline should not crash.

Explicit schema is better to use here because :
- It's more production-like and prevents schema drift
- It fails fast on unexpected data formats
- It documents the expected data contract.

## Prompt 2: config.py file creation which will be used by all scripts

**PROMPT SENT:**
Create a shared Databricks config module `src/common/config.py` (and `src/common/__init__.py` if needed) for my Medallion pipeline.
Use Unity Catalog Volumes as the final storage convention.
Unity Catalog objects:
- Catalog: ecommerce
- Schema: medallion
- Volume: data
Canonical Volume root:
/Volumes/ecommerce/medallion/data/
Path layout:
- Raw CSV: /Volumes/ecommerce/medallion/data/raw/{customers,orders,products}.csv
- Bronze Delta: /Volumes/ecommerce/medallion/data/bronze/{customers,orders,products}
- Silver Delta: /Volumes/ecommerce/medallion/data/silver/{customers,orders,products}
- Quality metrics: /Volumes/ecommerce/medallion/data/silver/quality_metrics
- Gold Delta: /Volumes/ecommerce/medallion/data/gold/{sales_by_product,revenue_by_customer,customer_segmentation}
Three-level table names:
- ecommerce.medallion.bronze_customers
- ecommerce.medallion.bronze_orders
- ecommerce.medallion.bronze_products
- ecommerce.medallion.customers_silver
- ecommerce.medallion.orders_silver
- ecommerce.medallion.products_silver
- ecommerce.medallion.quality_metrics
- ecommerce.medallion.sales_by_product
- ecommerce.medallion.revenue_by_customer
- ecommerce.medallion.customer_segmentation
Requirements:
- Module-level constants for catalog, schema, volume, every path, and every table name
- Build paths from UC_CATALOG / UC_SCHEMA / UC_VOLUME so names are not duplicated
- Helper volume_file(*parts) that joins segments under the Volume root
- Helper ensure_unity_storage(spark) that runs CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS
  - If CREATE CATALOG fails, log a warning and continue (catalog may already exist / no privilege)
  - If CREATE SCHEMA or CREATE VOLUME fails, log and re-raise
- Use logging (not print), Google-style docstrings, PEP 8
- Compatible with Databricks notebooks (spark passed in; no SparkSession.builder)
- Use only `/Volumes/ecommerce/medallion/data/` for pipeline storage

**AI RESPONSE SUMMARY:**
Created src/common/config.py as the single source of Volume paths and Unity Catalog table names.

- VOLUME_ROOT = /Volumes/ecommerce/medallion/data
- RAW/BRONZE/SILVER/GOLD dirs plus CSV, Bronze, Silver, Gold, and quality_metrics paths
- Three-level tables ecommerce.medallion.bronze_* / *_silver / gold tables
- volume_file(*parts) builds /Volumes/... paths
- ensure_unity_storage(spark) creates schema and volume; catalog create is best-effort

Ingest notebooks should import these constants instead of hardcoding paths or table names.

Documentation summary
- Added src/common/config.py with Volume paths and ecommerce.medallion.* tables.
- Added ensure_unity_storage() for idempotent UC object creation.

**YOUR EVALUATION:**
_TODO_


## Prompt 3: Customer.csv ingestion script

**PROMPT SENT:**
Write `src/bronze/01_ingest_customers.py` — a Python script for Databricks notebook that will ingest `customers.csv` into a Bronze Delta table.

Requirements:
- Source: /Volumes/ecommerce/medallion/data/raw/customers.csv
- Destination: Delta table at /Volumes/ecommerce/medallion/data/bronze/customers
- Explicit schema from schemas.py, don't use `inferSchema`
- Metadata columns:
  - `_ingestion_timestamp`: `current_timestamp()`
  - `_source_file`: `lit("customers.csv")`
  - `_batch_id`: `lit(generated UUID or timestamp)`
- Write mode: overwrite (idempotent)
- Delta format: `df.write.format("delta").mode("overwrite").save(path)`
- Register a temp view or a table: `spark.sql("CREATE TABLE IF NOT EXISTS bronze_customers ...")`
- Logging: row counts before and after, schema
- Error handling: `try/except` on read and write operations
- Compatible with Databricks Unity Catalog (use `dbutils.fs` for Volume file checking)

Note: This script will be executed as a cell of a Databricks notebook, therefore:
- No `if __name__ == "__main__"` block
- Use `spark` (already initialized SparkSession)
- Use  `dbutils` for file operations


**AI RESPONSE SUMMARY:**
Created src/bronze/01_ingest_customers.py as a Databricks notebook-style ingest (no __main__, uses spark / dbutils).

Behavior

Reads /Volumes/ecommerce/medallion/data/raw/customers.csv with CUSTOMERS_SCHEMA (no inferSchema)
Adds _ingestion_timestamp, _source_file (customers.csv), _batch_id (UUID)
Overwrites Delta at /Volumes/ecommerce/medallion/data/bronze/customers
Registers Unity Catalog table ecommerce.medallion.bronze_customers (CREATE TABLE IF NOT EXISTS … USING DELTA LOCATION)
Logs bytes, schema, and row counts before/after; fails if the source is missing, empty, or counts differ
try/except around dbutils.fs.ls, CSV read, Delta write, and SQL register
On Databricks with Unity Catalog

Upload customers.csv to /Volumes/ecommerce/medallion/data/raw/
Put schemas.py on the Python path (Repo import) or run %run ./schemas first
Run this file as a notebook
A second run overwrites the same path (same row count, new _batch_id).

Documentation summary

Added src/bronze/01_ingest_customers.py for CE Bronze ingest.
Explicit schema, metadata columns, Delta overwrite, Unity Catalog table ecommerce.medallion.bronze_customers.
Logging and error handling on file check, read, write, and table registration.

**YOUR EVALUATION:**
The scripts looks good in first view as per our requirements.

The scripts follows the idempotency too.

Lets run the script in Databricks to validate


## Iteration 1: Add Databricks source notebook comment at the very first line

**PROMPT SENT:**
Add databricks source notebook at the very first line of ingest_customer.py file and remember to add for all future py scripts 

**AI RESPONSE SUMMARY:**
That is the Databricks marker so the file opens and runs as a notebook in the workspace.

I also added this to .cursorrules and tool-specific/cursor-workflow/cursorrules.md, so later job scripts (Bronze/Silver/Gold/dashboard) will get the same first line. Shared modules (schemas.py, config.py) and local generators under src/data_generation/ stay without it, because those are imported or run on your laptop, not as Databricks notebooks.

Documentation summary

Added # Databricks notebook source as line 1 of src/bronze/01_ingest_customers.py.
Recorded the same header as a Cursor rule for future Databricks job .py files.
Left library modules and local data-generation scripts without the notebook header.


## Prompt 4: Order.csv ingestion script

**PROMPT SENT:**
Now create `src/bronze/02_ingest_orders.py` following the exact same pattern as 
01_ingest_customers.py but for the orders.csv file.
Use the ORDERS_SCHEMA from schemas.py.
Source: dbfs:/FileStore/medallion/raw/orders.csv
Target: dbfs:/FileStore/medallion/bronze/orders
Same metadata columns, same error handling, same logging pattern.

**AI RESPONSE SUMMARY:**
Created src/bronze/02_ingest_orders.py on the same pattern as customers ingest.

Header
# Databricks notebook source

Schema
ORDERS_SCHEMA (no inferSchema)

Source
/Volumes/ecommerce/medallion/data/raw/orders.csv

Target
/Volumes/ecommerce/medallion/data/bronze/orders

Table
ecommerce.medallion.bronze_orders

Metadata
_ingestion_timestamp, _source_file (orders.csv), _batch_id

Write
Delta overwrite + overwriteSchema

Ops
dbutils.fs check, logging, try/except on read/write/register

The prompt still used FileStore paths. This file uses the Volume paths from config.py, same as customers, so the repo stays on Unity Catalog.

Documentation summary

Added src/bronze/02_ingest_orders.py mirroring customers ingest.

Uses ORDERS_SCHEMA, Volume raw/bronze paths, metadata columns, overwrite, logging, and error handling.

Did not use FileStore; storage stays /Volumes/ecommerce/medallion/data/.

**YOUR EVALUATION:**
_TODO_


## Prompt 5: product.csv ingestion script

**PROMPT SENT:**
Create `src/bronze/03_ingest_products.py` — same pattern for products.csv.
Use PRODUCTS_SCHEMA from schemas.py.
Source: dbfs:/FileStore/medallion/raw/products.csv
Target: dbfs:/FileStore/medallion/bronze/products

**AI RESPONSE SUMMARY:**
Created src/bronze/03_ingest_products.py on the same pattern as the customers and orders ingest jobs.

Header
# Databricks notebook source
Schema
PRODUCTS_SCHEMA

Source
/Volumes/ecommerce/medallion/data/raw/products.csv

Target
/Volumes/ecommerce/medallion/data/bronze/products

Table
ecommerce.medallion.bronze_products

Metadata
_ingestion_timestamp, _source_file, _batch_id

Write
Delta overwrite, logging, try/except

FileStore paths in the prompt were mapped to the project Volume convention.

Documentation summary

Added src/bronze/03_ingest_products.py using PRODUCTS_SCHEMA.
Same ingest pattern: Volume paths, metadata, overwrite, logging, error handling.
Registered table is ecommerce.medallion.bronze_products.

**YOUR EVALUATION:**
_TODO_


## Prompt 6: ingest_all.py creation

**PROMPT SENT:**
Create `src/bronze/ingest_all.py` — an orchestrator script that runs all 3 ingestion 
scripts in sequence.
Requirements:
- Import and call the ingestion logic from each script
- Add overall timing (start time, end time, duration)
- Print a summary table at the end:
  | Table      | Rows Ingested | Duration (s) | Status  |
  |------------|---------------|---------------|---------|
  | customers  | 10,000        | 2.3           | SUCCESS |
  | orders     | 100,000       | 8.1           | SUCCESS |
  | products   | 500           | 1.2           | SUCCESS |
- If any ingestion fails, log the error but continue with the others
- Return an overall status (SUCCESS if all succeeded, PARTIAL if some failed, FAILED if all failed)

**AI RESPONSE SUMMARY:**
Created src/bronze/ingest_all.py to run customers → orders → products in sequence.

Behavior

Loads each ingest script by file path (names like 01_ingest_*.py are not valid Python modules)
Times the full run and each table
On failure: logs the error and continues
Prints a summary table (rows, duration, SUCCESS/FAILED)
Returns SUCCESS / PARTIAL / FAILED
The three ingest files now use if __name__ == "__main__" so importing them from the orchestrator does not start ingest immediately. Running each notebook by itself still works.

Run ingest_all.py as a Databricks notebook after the CSVs are on the Volume.

Documentation summary

Added src/bronze/ingest_all.py with per-job timing, a summary table, and SUCCESS/PARTIAL/FAILED.
One job failing is logged; the other two still run.
Guarded the three ingest entrypoints so the orchestrator can import them without auto-running.

**YOUR EVALUATION:**
_TODO_


## Iteration 1: Update in ingest_all.py

**PROMPT SENT:**
"BRONZE_DIR = Path(__file__).resolve().parent" in ingest_all.py
databricks is unable to resolve this line.Please check 

**AI RESPONSE SUMMARY:**
Databricks notebooks do not define __file__, so Path(__file__).resolve().parent fails there.

ingest_all.py now finds src/bronze in this order:

__file__ when it exists (local Python)
Databricks notebook folder: /Workspace + dbutils notebook path
Search from the current working directory for 01_ingest_customers.py
Re-run ingest_all.py on the cluster. The log should show a line like Bronze scripts directory (Databricks notebook): /Workspace/Repos/.../src/bronze.

Documentation summary

Databricks notebooks have no __file__; that was breaking BRONZE_DIR.
ingest_all.py now resolves the bronze folder via notebook context, then cwd.
Re-run the orchestrator notebook after this change.

**YOUR EVALUATION:**
_TODO_

## Iteration 2 : Refactor `src/bronze/ingest_all.py` for Databricks compatibility.

**Prompt Sent:**

I ran into a critical Databricks architecture issue. Files with 
`# Databricks notebook source` header are treated as notebooks and CANNOT 
be imported via `import` or `importlib`. Only the entry-point orchestrator 
should be a notebook. All other files are regular Python modules.
CURRENT ARCHITECTURE (broken):
- ingest_all.py uses `importlib.util.spec_from_file_location()` to load 
  01_ingest_customers.py, 02_ingest_orders.py, 03_ingest_products.py
- This fails because Databricks intercepts file loading for workspace files
REQUIRED ARCHITECTURE:
- ingest_all.py is the ONLY file with `# Databricks notebook source`
- All 3 ingest scripts are regular Python modules (no notebook header)
- Databricks doesn't inject `spark` and `dbutils` into imported modules, 
  so we must make them globally available using `builtins`
REQUIREMENTS:
1. Use `builtins.spark = spark` and `builtins.dbutils = dbutils` at the top 
   to inject Databricks globals before any imports
2. Add src/ to sys.path for Databricks (fallback to hardcoded path: 
   "/Workspace/Users/tushar.katyal@tothenew.com/databricks-medallion-pipeline/src")
3. Import ingest functions using `importlib.import_module()` since filenames 
   start with digits (invalid Python identifiers)
4. Remove the entire resolve_bronze_dir() function and all the complex 
   path resolution logic — sys.path handles it now
5. Keep the IngestResult dataclass, timing, summary table, and overall 
   status logic (SUCCESS/PARTIAL/FAILED)
6. Call ingest_all() at the bottom (outside __main__ guard) so it auto-runs 
   when executed via %run in Databricks
7. Each ingest function (ingest_customers, ingest_orders, ingest_products) 
   takes no arguments and returns a DataFrame
Keep all existing docstrings. Keep the summary table format unchanged.
Remove unused imports (importlib.util, types.ModuleType).

**AI RESPONSE SUMMARY:**
Cursor refactored ingest_all.py to:
- Use `builtins.spark` injection instead of relying on Databricks notebook globals
- Replace `importlib.util.spec_from_file_location` with `importlib.import_module`
- Remove 60+ lines of path resolution logic (resolve_bronze_dir, etc.)
- Add auto-execution at module level for %run compatibility

**YOUR EVALUATION:**
Accepted: builtins approach — clean, one-line fix for global spark access

importlib.import_module — simpler, leverages sys.path correctly

Removed path resolution — was over-engineered for the problem