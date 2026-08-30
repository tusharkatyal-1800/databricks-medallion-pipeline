# AI Prompts — Documentation (Phase 1)

## Prompt 1: setup .cursorrules file for the project

**PROMPT SENT:**
I am developing Databricks Medallion Architecture pipeline for an e-commerce dataset 
for my engineering evaluation. Help me develop the .cursorrules file in tool-specific\cursor-workflow directory for the project.
The project context:
- The platform uses Databricks Unity Catalog and Volumes
- Languages used are Python, PySpark, SQL
- Architecture follows Medallion Architecture (Bronze -> Silver -> Gold -> Dashboard)
- Data is 3 CSV files - customers (10K rows), orders (100K rows), products (500 rows)
- Bronze – raw ingestion, no transformations
- Silver – 4 data quality checks (completeness, uniqueness, data type verification, referential integrity)
- Gold – 3 aggregations tables (sales by product, revenue by customer, customer segmentation)
- Dashboard - Databricks SQL Dashboard with 3+ visualizations
Rules for the .cursorrules file:
1. The PySpark code should use Databricks Unity Catalog and Volumes
2. All tables should be of Delta Lake format
3. All the Python code should adhere to PEP 8 rules
4. All the SQL should be ANSI-compatible Spark SQL
5. All functions should have the docstring
6. Use Volume paths under `/Volumes/ecommerce/medallion/data/`
7. All scripts should include error handling and logging
8. The data quality checks should FLAG bad rows (quality_check_result column added), but never delete them
9. Idempotence (can be re-run) of all scripts

**AI RESPONSE SUMMARY:**
The .cursorrules file is in tool-specific/cursor-workflow/.cursorrules. A matching copy is at the repo root (.cursorrules) so Cursor actually loads it. Rationale is in tool-specific/cursor-workflow/cursor-rules-or-instructions.md.

Each constraint is written as an instruction plus a BAD vs GOOD snippet so generated notebooks stay aligned with Unity Catalog.

Rule	What Cursor is told to do
Unity Catalog
Three-level names and `/Volumes/ecommerce/medallion/data/`
Delta Lake
Bronze/Silver/Gold as format("delta"), not Parquet/CSV sinks
PEP 8
snake_case, no from pyspark.sql.functions import *
ANSI Spark SQL
Standard JOIN/CAST/CASE; no TOP / GETDATE()
Docstrings
Google-style Args/Returns on every function
Volumes
/Volumes/ecommerce/medallion/data/{raw,bronze,silver,gold}/
Logging
logging + try/except that re-raises after logger.exception
DQ flag, never delete
quality_check_result (and per-check flags); Silver keeps 100% of rows. Gold may filter PASS for metrics only
Idempotence
mode("overwrite") / CREATE OR REPLACE; never append on re-run

**YOUR EVALUATION:**
These rules are perfect to give a initial start to the project, lator if required we can update these rules

## Prompt 2: Building Requirements for the project

**PROMPT SENT:**
I'm building a Databricks Medallion Architecture pipeline for an e-commerce company. 
Here is the business context:
An e-commerce company ingests daily sales data from 3 sources:
- customers.csv (10,000 rows): customer_id, customer_name, email, country, signup_date, customer_segment (Premium/Standard/Basic), lifetime_value
- orders.csv (100,000 rows): order_id, customer_id, order_date, product_id, quantity, unit_price, total_amount, order_status (Pending/Completed/Cancelled), payment_date (nullable)
- products.csv (500 rows): product_id, product_name, category, price, cost, stock_quantity, reorder_level
The pipeline must:
- Bronze: Ingest raw CSVs into Delta tables (no transformation)
- Silver: Apply 4 quality checks (completeness, uniqueness, type validation, referential integrity), flag bad rows, generate quality metrics report
- Gold: 3 aggregations (Sales by Product, Revenue by Customer, Customer Segmentation)
- Dashboard: 3+ visualizations
The sample data must include ~700 intentional quality issues:
- customers: 50 NULL emails, 10 duplicate customer_ids
- orders: 100 NULL customer_ids, 200 NULL product_ids, 50 orphan customer_ids, 30 orphan product_ids, 20 duplicate order_ids
Help me create a requirements-analysis.md that covers:
1. Problem statement in my own words (don't just repeat the spec)
2. Functional requirements (broken down by layer)
3. Non-functional requirements (performance, idempotency, logging)
4. Assumptions I'm making
5. Edge cases to handle
6. Acceptance criteria per layer

**AI RESPONSE SUMMARY:**
Documentation summary

Wrote requirements-analysis.md with all seven requested sections.
Problem framed as: keep bad rows auditable, then publish trustworthy Gold on Databricks Unity Catalog.
Functional reqs split by Bronze / Silver / Gold / Dashboard; NFRs cover performance, overwrite idempotency, and logging.
Called out 460 listed defects vs ~700 total; assumed remaining issues are type/domain

**YOUR EVALUATION:**
This document is well defined and all the important points are listing clearly in this document, this will help us to focus on only our requirements throughout the project.
Given new prompt to add certain extra edge cases 


## Iteration 1: Adding extra edge cases 

**PROMPT SENT:**
Please mention these egde case also in the docuement
What if a CSV is empty?
What if the same file is ingested twice?
How to handle timezone differences in dates?
What if quantity is negative?

**AI RESPONSE SUMMARY:**
Documentation summary

Updated the Edge Cases section in requirements-analysis.md.
Added explicit handling for empty CSV, duplicate ingest, date timezones, and negative quantity.
Empty file vs missing path are separate; re-ingest is overwrite idempotency; dates stay timezone-naive unless an offset is present; negative quantity is flagged, not dropped or abs’d.

**YOUR EVALUATION:**
Perfectly added the extra edge cases i have mentioned.  


## Prompt 3: Design Notes & Data Model creation

**PROMPT SENT:**
Based on the analysis of the requirements above, please assist me in creating a design-notes.md file with:
1. Architecture Overview
   - Dataflow: CSV -> Volume Upload -> Bronze (Delta) -> Silver (Delta) -> Gold (Delta) -> Dashboard
   - Describe the separation of concerns between layers
2. Bronze Layer Design
   - Path naming convention (e.g., `/Volumes/ecommerce/medallion/data/bronze/{table_name}`)
   - Schema inference vs. schema definition - which to choose and why?
   - Metadata columns to add: ingestion_timestamp, source_file_name
   - Delta table properties
3. Silver Layer Design
   - Structure of quality checks (sequential/parallel/per table)
   - quality_check_result column design (value types, bitmasking/JSON/pipe-separated string/etc.)
   - If rows are clean and flagged put into one table or multiple tables
   - Quality metrics report format
4. Gold Layer Design
   - Structure of aggregations (only clean rows or all)
   - Update strategy (overwrite/merge?)
   - Schema for 3 tables with aggregations
5. Dashboard Design
   - SQL queries needed
   - What Gold tables correspond to what visualizations
Please also create data-model.md that describes the schema of all tables in all layers (Bronze, Silver, Gold) including data types, nullable flag, and description.

**AI RESPONSE SUMMARY:**
Created design-notes.md (architecture, Bronze/Silver/Gold/dashboard) and data-model.md (all schemas).
Bronze: explicit schema + ingest metadata; paths under `/Volumes/ecommerce/medallion/data/`.
Silver: per-table Spark checks; PASS/FAIL columns + pipe details; one table keeps all rows; metrics Delta table.
Gold: PASS + Completed, full overwrite; three aggregation schemas; dashboard SQL mapped to those tables.

**YOUR EVALUATION:**
The data model is perfectly designed using the requirements-analysis.md file, all the table structure are correct.
Silver layer will be responsible for quality checks and validations using completeness, uniqueness and referential integrity and will generate the quality metrics report.

The design notes contains the metadata columns and the delta table properties which looks good to me and for the quality check result column, it will use basis string to highlight pass and fail for all the checks
Note : Gold will be a query filter from silver , not complete delete from silver

The design notes document also has debugging approach which will help us to fast debugg our project.

In short these two documents are very importment for our project going ahead from now.
If required, i will update any of these if i face any issues.


## Prompt 4: Data quality strategy defined 

**PROMPT SENT:**
Develop data-quality-strategy.md for Medallion pipeline. 
I need to define the following for each of 4 quality checks:
1. **Completeness Check**:
   - What columns are checked per table (customers: email; orders: customer_id, product_id)?
   - How are NULLs found and marked?
   - Expected threshold: more than 99% of completeness.
   - How many intentional errors are there? 50 + 100 + 200 = 350 NULL rows.
2. **Uniqueness Check**:
   - What columns are checked per table (customers: customer_id; orders: order_id)?
   - How are duplicates found (window functions? groupBy?)?
   - Expected threshold: 100% of uniqueness after deduplicating.
   - How many intentional errors are there? 10 + 20 = 30 duplicate rows.
3. **Type Validation Check**:
   - Validate types of the values: dates are valid dates, numbers are positive and not zero, emails are valid according to pattern.
   - What is a regex pattern for validating emails?
   - What would you consider an invalid date?
   - What numeric constraints should be taken into account? (quantity > 0, unit_price > 0, total_amount ~ quantity × unit_price)
4. **Referential Integrity Check**:
   - orders.customer_id should be present in customers.customer_id.
   - orders.product_id should be present in products.product_id.
   - How do orphan rows get detected (LEFT ANTI JOIN).
   - Expected errors: 50 orphans customer_ids + 30 orphans product_ids = 80.

**AI RESPONSE SUMMARY:**

Filled data-quality-strategy.md for completeness, uniqueness, type, and referential checks.
Completeness: email / order FKs, 350 nulls, >99% complete.
Uniqueness: window COUNT over keys (not groupBy-only), 30 extra dupes; no Silver delete.
Type: email regex, yyyy-MM-dd dates, qty/price > 0, amount ≈ qty × price (±0.01).
Referential: LEFT ANTI JOIN, 80 orphans, >99.9% valid; null FKs are completeness, not orphans.
**YOUR EVALUATION:**

AI has perfectly understood data quality checks i mentioned in the prompt and provided me good strategy to tackle them
Need to add some points to get clarification on them.

## Iteration 1: Adding some points in quality strategy

**PROMPT SENT:** 
Also include:
- How the quality_check_result column is populated  "COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL")
- Quality metrics report structure (table showing check_name, total_rows, passed, failed, pass_rate_%)
- How rows that fail multiple checks are handled?

**AI RESPONSE SUMMARY:**
Updated data-quality-strategy.md: quality_check_result is PASS or pipe-delimited fail tokens (e.g. COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL).
Multi-fail: one row, all checks evaluated, each fail counted in metrics.
Metrics columns: check_name, total_rows, passed, failed, pass_rate_%.
Synced design-notes.md, data-model.md, and requirements-analysis.md.

**YOUR EVALUATION:**
Now all strategy looks complete to handle the check result and we have the strategy to handle multiple -fail also.

The multiple fail section is debatable, but i feel this is good , each fail should be counted differently.

I am satisfied with this Quality strategy now


## Prompt 5: Task breakdown with a defined format

**PROMPT SENT:**
Using your design, write a task-breakdown.md file containing all the coding tasks 
that I need to do, classified according to pipeline layers. Each task must be:
- Doable within a single session of Cursor (30-60 minutes)
- Have inputs and outputs clearly defined
- Have acceptance criteria
Format:
## Task [N]: [Title]
- **Layer:** Bronze/Silver/Gold/Dashboard
- **File(s):** src/bronze/01_ingest_customers.py
- **Input:** CSV file located at `/Volumes/ecommerce/medallion/data/raw/customers.csv`
- **Output:** Delta table at `/Volumes/ecommerce/medallion/data/bronze/customers`
- **Acceptance Criteria:** 
  - Table contains 10,000 records
  - Schema conforms to data model
  - inclusion of ingestion_timestamp
- **Dependencies:** None / Task N

**AI RESPONSE SUMMARY:**
Created tool-specific/cursor-workflow/task-breakdown.md with 19 dependency-ordered tasks covering:

Shared utilities and sample-data generation
Raw Volume upload and three Bronze ingestions
Four Silver quality checks, metrics, and validation
Three Gold aggregations and reconciliation
Dashboard SQL, setup, and validation
Each task includes layer, files, inputs, outputs, acceptance criteria, and dependencies, scoped to 30–60 minutes.

Documentation summary

Created a 19-task implementation roadmap.
Tasks follow Bronze → Silver → Gold → Dashboard.
Included quality flags, metrics reporting, idempotency, and reconciliation.
Used the project’s `/Volumes/ecommerce/medallion/data/` convention.

**YOUR EVALUATION:**
The task breakdown structure looks good to me, nothing to add in this from my end , the model has provided the 19 tasks as per my given format.

Now I can start the actual implementation of my project, the required documents are ready now.

## Prompt 6: 

**PROMPT SENT:**

**AI RESPONSE SUMMARY:**

**YOUR EVALUATION:**

## Prompt 7: 

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
