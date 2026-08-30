# AI Prompts — Documentation

## Prompt 1: setup .cursorrules file for the project

**PROMPT SENT:**
I am developing Databricks Medallion Architecture pipeline for an e-commerce dataset 
for my engineering evaluation. Help me develop the .cursorrules file in tool-specific\cursor-workflow directory for the project.
The project context:
- The platform used is Databricks Community Edition
- Languages used are Python, PySpark, SQL
- Architecture follows Medallion Architecture (Bronze -> Silver -> Gold -> Dashboard)
- Data is 3 CSV files - customers (10K rows), orders (100K rows), products (500 rows)
- Bronze – raw ingestion, no transformations
- Silver – 4 data quality checks (completeness, uniqueness, data type verification, referential integrity)
- Gold – 3 aggregations tables (sales by product, revenue by customer, customer segmentation)
- Dashboard - Databricks SQL Dashboard with 3+ visualizations
Rules for the .cursorrules file:
1. The PySpark code should be compatible with Databricks Community Edition (no Unity Catalog, no Volumes)
2. All tables should be of Delta Lake format
3. All the Python code should adhere to PEP 8 rules
4. All the SQL should be ANSI-compatible Spark SQL
5. All functions should have the docstring
6. Use DBFS paths to store data (dbfs:/FileStore/...)
7. All scripts should include error handling and logging
8. The data quality checks should FLAG bad rows (quality_check_result column added), but never delete them
9. Idempotence (can be re-run) of all scripts

**AI RESPONSE SUMMARY:**
The .cursorrules file is in tool-specific/cursor-workflow/.cursorrules. A matching copy is at the repo root (.cursorrules) so Cursor actually loads it. Rationale is in tool-specific/cursor-workflow/cursor-rules-or-instructions.md.

Each constraint is written as an instruction plus a BAD vs GOOD snippet so generated notebooks stay on Community Edition.

Rule	What Cursor is told to do
Community Edition
Hive two-level names only; no Unity Catalog, no /Volumes/
Delta Lake
Bronze/Silver/Gold as format("delta"), not Parquet/CSV sinks
PEP 8
snake_case, no from pyspark.sql.functions import *
ANSI Spark SQL
Standard JOIN/CAST/CASE; no TOP / GETDATE()
Docstrings
Google-style Args/Returns on every function
DBFS
dbfs:/FileStore/ecommerce/{raw,bronze,silver,gold}/
Logging
logging + try/except that re-raises after logger.exception
DQ flag, never delete
quality_check_result (and per-check flags); Silver keeps 100% of rows. Gold may filter PASS for metrics only
Idempotence
mode("overwrite") / CREATE OR REPLACE; never append on re-run

**YOUR EVALUATION:**
These rules are perfect to give a initial start to the project, lator if required we can update these rules

## Prompt 2: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**


## Prompt 3: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 4: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 5: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 6: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 7: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 8: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 9: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 10: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 11: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 12: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 13: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**
