# AI Prompts — Silver Layer (Phase 4)

## Prompt 1: Generating quality_completeness logic

**PROMPT SENT:**
Make `src/silver/01_quality_completeness.py` — a PySpark script for detecting NULL values
in important fields of all Bronze tables.
Requirements:
- Load data from Bronze Delta tables:
  - bronze_customers: detect NULLs in 'email'
  - bronze_orders: detect NULLs in 'customer_id' and 'product_id'   
- Add new field `completeness_check` to each DataFrame with value:
  - "PASS" in case if all critical fields are not null
  - "FAIL_NULL_{field_name}" in case if some field is NULL (e.g. "FAIL_NULL_email")
  - In case several fields are NULL: "FAIL_NULL_customer_id|FAIL_NULL_product_id"
- Compute metrics summary:
  - Table name, field checked, total rows, NULL count, not-null count, completeness %
- Print metrics as a formatted table
- Return DataFrames with new column completeness_check
- DO NOT drop or filter any rows (just mark them)
Expected output:
- customers: there should be 50 rows marked as FAIL_NULL_email (50 out of 10,000, 0.5%)
- orders: there should be 100 rows marked as FAIL_NULL_customer_id + 200 FAIL_NULL_product_id
Use PySpark functions: col().isNull(), when(), concat_ws() for flags combination.

**AI RESPONSE SUMMARY:**
Added completeness check that reads Bronze customers/orders, adds completeness_check (PASS or FAIL_NULL_{field} via concat_ws), prints a metrics table, and keeps every row. Shared logic is in completeness.py; 01_quality_completeness.py is the Databricks notebook.

Documentation summary

Added Silver completeness: flag NULLs on customer email and order customer_id/product_id.
Files: src/silver/01_quality_completeness.py, src/silver/completeness.py, src/silver/__init__.py.
Rows are marked, not deleted; no Silver table write yet.
Run the notebook on an all-purpose cluster after Bronze SUCCESS.

**YOUR EVALUATION:**
Two files on purpose: notebook entry point vs importable completeness logic.

Same Databricks pattern as Bronze ingestion.py vs ingest notebooks.
---

## Prompt 2: Generating quality_uniqueness logic

**PROMPT SENT:**
Write `src/silver/02_quality_uniqueness.py`, a PySpark script that will validate the presence of duplicate
primary keys.
To validate:
- customers: duplicate customer_id
- orders: duplicate order_id
Approach to solve:
- Utilize Window functions: row_number() OVER (PARTITION BY primary_key ORDER BY _ingestion_timestamp)
- Consider row_num greater than one as duplicates
- Insert new column `uniqueness_check`:
  - "PASS" for first row (row_num == 1)
  - "FAIL_DUPLICATE_{key}" for duplicates (row_num > 1)
- Output metrics: table, field, total number of rows, unique rows, duplicates, uniqueness % 
Expected results:
- customers: 10 duplicate customer_ids
- orders: 20 duplicate order_ids
NOTE: Use Window functions, not groupBy().count() > 1, since groupBy() does not retain all rows.

**AI RESPONSE SUMMARY:**
Added uniqueness via Window row_number on customer_id / order_id ordered by _ingestion_timestamp. First row per key is PASS; later rows are FAIL_DUPLICATE_{key}. Metrics print total / unique / duplicates / uniqueness %. Shared logic in uniqueness.py; 02_quality_uniqueness.py is the Databricks notebook.

Documentation summary

Added Silver uniqueness: Window row_number flags extra duplicate keys; first row PASS.
Files: src/silver/02_quality_uniqueness.py, src/silver/uniqueness.py.
Expected fails: 10 customers, 20 orders; all Bronze rows retained; no Silver write.

**YOUR EVALUATION:**
uniqueness quality is perfectly checkedc by this script

---

## Prompt 3: Generating type validation logic

**PROMPT SENT:**
Create the file `src/silver/03_quality_type_validation.py` – a PySpark code snippet to validate data types and business rules.
Tests:
1. **Date validation:**
   - customers.signup_date: between 2020-01-01 and today()
   - orders.order_date: between 2023-01-01 and today()
   - orders.payment_date: greater than order_date (if payment_date is not NULL)
2. **Numeric validation:**
   - orders.quantity: greater than 0
   - orders.unit_price: greater than 0
   - orders.total_amount: greater than 0
   - products.price: greater than 0
   - products.cost: greater than 0 and less than price (cost must be lower than price)
   - products.stock_quantity: greater than 0
3. **String/Regex validation:**
   - customers.email: matches basic email regex pattern (.*@.*\..*)
   - customers.customer_segment: in set ('Premium', 'Standard', 'Basic')
   - orders.order_status: in set ('Pending', 'Completed', 'Cancelled')
4. **Business rules validation:**
   - orders.total_amount approximately equal to quantity * unit_price
     (with allowance of 1% tolerance for rounding)
   - Completed orders have a non-null payment_date
   - Pending orders have a null payment_date
Add column `type_validation_check`:  
- "PASS" if all tests passed
- "FAIL_INVALID_{check_name}" in case of failure; separated by pipes if several errors
Compute metrics per check type.

**AI RESPONSE SUMMARY:**
Added type/business-rule flags on Bronze customers, orders, and products. Fail tokens are FAIL_INVALID_{check_name} joined with |. Metrics are printed per check. Shared logic in type_validation.py; 03_quality_type_validation.py is the Databricks notebook.

Documentation summary

Added Silver type validation for dates, numerics, regex/sets, and order payment/amount rules.
Files: src/silver/03_quality_type_validation.py, src/silver/type_validation.py.
Products are checked here; zero stock can fail. Rows are flagged, not dropped; no Silver write.

---

## Prompt 4:  Generating Refrential integrity logic 

**PROMPT SENT:**
Create file `src/silver/04_quality_referential_integrity.py` – PySpark code for validating 
referential integrity relationships.
Verification:
1. orders.customer_id should be present in customers.customer_id
2. orders.product_id should be present in products.product_id
How:
- Use LEFT ANTI JOIN to get orphan rows:
  ```python
  orphan_customers = orders_df.join(customers_df,
        orders_df.customer_id == customers_df.customer_id, 
        "left_anti")
Add referential_integrity_check column:
"PASS" if all foreign keys correct
"FAIL_ORPHAN_customer_id" if customer_id is wrong
"FAIL_ORPHAN_product_id" if product_id is wrong
Both when appropriate
Expected output:

50 wrong customer_ids
30 wrong product_ids
Special case: NULL customer_id and NULL product_id should NOT raise referential integrity issues – they are handled by completeness checks. Referential integrity check must validate non-NULL foreign keys only.

Calculate statistics: orphan rows count for each foreign key, referential integrity percentage.

**AI RESPONSE SUMMARY:**
Added referential check using LEFT ANTI JOIN of non-null order FKs to distinct parent keys. Flags are FAIL_ORPHAN_customer_id / FAIL_ORPHAN_product_id. Null FKs are not scored. Shared logic in referential.py; 04_quality_referential_integrity.py is the Databricks notebook.

Documentation summary

Added Silver referential integrity for order customer_id and product_id via LEFT ANTI JOIN.
Files: src/silver/04_quality_referential_integrity.py, src/silver/referential.py.
Expected: 50 + 30 orphans; null FKs PASS; all order rows retained.

---

## Prompt 5: Generating remaining buisness logic

**PROMPT SENT:**
Create `src/silver/05_quality_business_logic.py` as a Databricks notebook, plus
an importable module `src/silver/business_logic.py` (no notebook header).

This is NOT a fifth quality-check family. It adds extra e-commerce consistency
rules that the first four Silver scripts do not cover. Failures must later map
to TYPE_VALIDATION_FAIL when quality_check_result is assembled. Do not invent
a new token such as BUSINESS_LOGIC_FAIL.

Read managed Bronze tables:
- ecommerce.medallion.bronze_customers
- ecommerce.medallion.bronze_orders
- ecommerce.medallion.bronze_products

Add column `business_logic_check`:
- "PASS" if all applicable rules pass
- "FAIL_INVALID_{check_name}" on failure
- Join multiple failures with "|"
- Never drop, filter, or deduplicate rows
- Null FKs must not fail join-based rules; completeness already owns those

Checks:

Customers (intra-row):
- lifetime_value: present value must be >= 0
  token: FAIL_INVALID_lifetime_value

Products (intra-row):
- reorder_level: present value must be >= 0
  token: FAIL_INVALID_reorder_level

Orders (intra-row):
- Cancelled orders must have a null payment_date
  token: FAIL_INVALID_cancelled_payment_date
- payment_date, when present, must be <= current_date()
  token: FAIL_INVALID_future_payment_date

Orders joined to customers (non-null customer_id only):
- order_date >= customers.signup_date
  token: FAIL_INVALID_order_before_signup
- Use a left join. If the customer parent is missing (orphan), do not fail
  this rule; referential integrity already owns orphans.

Orders joined to products (non-null product_id only):
- orders.unit_price must match products.price within 1% relative tolerance
  token: FAIL_INVALID_unit_price_catalog
- If the product parent is missing (orphan), do not fail this rule.

Metrics:
Print a formatted table with table, check_name, total rows, fail count,
and pass %. Expected result on this sample: most checks 0 fails, because these
rules were not planted in generate_sample_data.py. That is acceptable.


**AI RESPONSE SUMMARY:**
Added extra consistency flags (lifetime_value, reorder_level, cancelled/future payment, order vs signup, unit price vs catalog). Null FKs and orphans are not scored. Shared logic in business_logic.py; 05_quality_business_logic.py is the Databricks notebook. Fails map later to TYPE_VALIDATION_FAIL.

Documentation summary

Added extra e-commerce consistency checks as Silver notebook 05, not a new quality token.
Files: src/silver/05_quality_business_logic.py, src/silver/business_logic.py.
Join rules skip null FKs and orphans; sample data is expected to show ~0 fails. No Silver tables written.

**YOUR EVALUATION:**
the remaining buisness logics are good to be applied in our project for better results

---

## Prompt 6: Generating create_silver_tables.py

**PROMPT SENT:**
Create `src/silver/create_silver_tables.py` as a Databricks notebook that
builds the final Silver layer.

The exact first line must be:
Databricks notebook source

Read these managed Unity Catalog Bronze tables:
- ecommerce.medallion.bronze_customers
- ecommerce.medallion.bronze_orders
- ecommerce.medallion.bronze_products

Import reusable functions from the regular Python modules under src/silver.
Do not import or execute numbered Databricks notebook files.

Apply the four quality-check categories while retaining every Bronze row:

1. Completeness
2. Uniqueness
3. Type validation, including the extra business-logic rules
4. Referential integrity

Requirements by table:

Customers:
- completeness: email
- uniqueness: customer_id
- type validation: signup_date, email, customer_segment
- business rules: lifetime_value >= 0
- referential_integrity_check = "N/A"

Orders:
- completeness: customer_id, product_id
- uniqueness: order_id
- type and business validations from type_validation.py and business_logic.py
- referential integrity: customer_id and product_id

Products:
- completeness_check = "PASS" when no required product fields are configured
- uniqueness: product_id
- type validation: price, cost, stock_quantity
- business rules: reorder_level >= 0
- referential_integrity_check = "N/A"

Keep detailed per-check columns, including:
- completeness_check
- uniqueness_check
- type_validation_check
- business_logic_check
- referential_integrity_check

Add `quality_check_result` using only these ordered category tokens:
- COMPLETENESS_FAIL
- UNIQUENESS_FAIL
- TYPE_VALIDATION_FAIL
- REFERENTIAL_INTEGRITY_FAIL

Rules:
- Use "PASS" when every applicable check passes.
- Join failed category tokens with "|".
- A failed business_logic_check contributes TYPE_VALIDATION_FAIL.
- Include each category token only once.
- Do not put field-level tokens such as FAIL_NULL_email directly into
  quality_check_result; keep them in their individual check columns.
- Never filter, drop, or deduplicate bad rows.

Write idempotently using:
.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
.saveAsTable(...)

Managed Silver tables:
- ecommerce.medallion.customers_silver
- ecommerce.medallion.orders_silver
- ecommerce.medallion.products_silver

Validate before and after every write that Silver row count equals Bronze row
count. Raise an exception on a mismatch.

Generate a quality metrics DataFrame with:
- table_name
- check_name
- field_checked
- total_rows
- applicable_rows
- passed
- failed
- pass_rate_pct
- threshold
- threshold_met
- batch_timestamp

For referential-integrity metrics, exclude NULL foreign keys from
applicable_rows because completeness handles them.

Write the report idempotently as the managed Delta table:
- ecommerce.medallion.quality_metrics

Print:
1. Per-table/per-check metrics
2. Overall clean rows per table:
   total_rows, clean_rows, flagged_rows, clean_rate_pct

Calculate all counts from the resulting DataFrames. Do not hardcode expected
clean counts such as 9,940 or approximately 99,600 because checks can overlap
and type/business rules may contribute additional failures.

Use Databricks runtime spark, logging, try/except around table reads and
writes, Google-style docstrings, PEP 8, and no SparkSession.builder.
Call the orchestration function at the bottom so Databricks Run all executes it.

**AI RESPONSE SUMMARY:**
Added src/silver/create_silver_tables.py.
Applies all checks while retaining every Bronze row.
Business failures map to TYPE_VALIDATION_FAIL.
Writes managed Delta tables:
ecommerce.medallion.customers_silver
ecommerce.medallion.orders_silver
ecommerce.medallion.products_silver
ecommerce.medallion.quality_metrics
Validates Bronze/Silver row-count equality.
Prints detailed metrics and overall clean-row results.
Uses no DBFS/FileStore paths.
Validation passed: Python syntax check and IDE lint reported no errors. Runtime verification still requires running create_silver_tables.py on the Databricks all-purpose cluster after syncing the updated files.

Documentation summary

Modified src/silver/type_validation.py to accept zero stock.
Added src/silver/create_silver_tables.py for Silver assembly, managed writes, retention validation, and metrics.
Quality results use the four standard category tokens.
Databricks runtime execution remains the final verification step.

**YOUR EVALUATION:**
This script will run all the required checks and store the tables and data quality reports in databricks.

---

## Prompt 7: Update Readme file for silver phase

**PROMPT SENT:**
Update readme.md for silver phase 

**AI RESPONSE SUMMARY:**
README.md now includes a Silver runbook after Bronze.

It covers:

Run src/silver/create_silver_tables.py on an all-purpose cluster after Bronze SUCCESS
Managed tables: customers_silver, orders_silver, products_silver, quality_metrics
Flag-only design and quality_check_result tokens
01–05 are optional and do not write tables
Expected planted fails vs threshold_met = false
Why orders clean rate can be ~64% (order_before_signup)
SQL verification and overwrite re-runs
File list for src/silver/
Documentation summary

Extended README.md with Silver steps, tables, expected quality results, and troubleshooting.
Main entry point documented as src/silver/create_silver_tables.py.
Gold is still marked as not in this guide.
