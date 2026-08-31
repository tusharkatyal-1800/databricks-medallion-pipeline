# Task Breakdown for Cursor

Each task is scoped for one 30–60 minute Cursor session. The implementation
uses Databricks Unity Catalog (`ecommerce.medallion`), and Volume
paths under `/Volumes/ecommerce/medallion/data/`. Tasks are ordered by dependency.

## Task 1: Create Shared Databricks Configuration and Utilities
- **Layer:** Bronze
- **File(s):** `src/common/config.py`, `src/common/io_utils.py`
- **Input:** Path and table conventions from `design-notes.md` and schemas from
  `data-model.md`
- **Output:** Reusable constants, Unity Catalog setup, input validation, logging,
  and idempotent Delta-write helpers
- **Acceptance Criteria:**
  - Defines raw, Bronze, Silver, and Gold paths under
    `/Volumes/ecommerce/medallion/data/`
  - Defines three-level Unity Catalog table names under `ecommerce.medallion`
  - Creates required Volume folders and schema/volume if missing
  - Validates that required files, rows, and columns exist
  - Delta helper uses overwrite and `overwriteSchema`
  - I/O errors are logged with context and re-raised
  - Every function has a Google-style docstring and follows PEP 8
- **Dependencies:** None

## Task 2: Generate Reproducible Source CSV Data
- **Layer:** Bronze
- **File(s):** `src/bronze/00_generate_source_data.py`
- **Input:** E-commerce schemas and intentional-error counts from
  `requirements-analysis.md` and `data-quality-strategy.md`
- **Output:** `customers.csv`, `orders.csv`, and `products.csv` ready for upload
  to `/Volumes/ecommerce/medallion/data/raw/`
- **Acceptance Criteria:**
  - Uses a fixed random seed so repeated generation is reproducible
  - Produces approximately 10,000 base customers, 100,000 base orders, and
    exactly 500 products
  - Adds 50 null emails, 100 null order customer IDs, 200 null order product
    IDs, 10 duplicate customer rows, 20 duplicate order rows, 50 orphan
    customer IDs, and 30 orphan product IDs
  - Adds documented type/domain issues to bring total issue instances to
    approximately 700
  - Duplicate rows follow the documented appended-row convention, resulting
    in approximately 10,010 customer and 100,020 order records
  - Logs generated row and intentional-error counts
- **Dependencies:** Task 1

## Task 3: Upload and Validate Raw CSV Files
- **Layer:** Bronze
- **File(s):** `src/bronze/01_upload_raw_files.py`
- **Input:** Locally generated `customers.csv`, `orders.csv`, and `products.csv`
- **Output:** Files at `/Volumes/ecommerce/medallion/data/raw/{file_name}.csv`
- **Acceptance Criteria:**
  - All three files exist under the canonical raw Volume path
  - A missing, zero-byte, or header-only required CSV causes a logged error
  - CSV headers match the source schemas in `data-model.md`
  - Re-uploading the same source replaces it rather than creating a duplicate
  - No credentials or local machine paths are hardcoded in reusable pipeline
    code
- **Dependencies:** Tasks 1 and 2

## Task 4: Ingest Customers into Bronze
- **Layer:** Bronze
- **File(s):** `src/bronze/02_ingest_customers.py`
- **Input:** `/Volumes/ecommerce/medallion/data/raw/customers.csv`
- **Output:** Managed Delta table `ecommerce.medallion.bronze_customers`
- **Acceptance Criteria:**
  - Uses an explicit all-STRING source schema; schema inference is disabled
  - Preserves every CSV record and source value without cleansing or dedupe
  - Adds non-null `ingestion_timestamp` and `source_file_name`
  - Row count equals the CSV data-row count (approximately 10,010 with
    appended duplicates)
  - Output schema conforms to `data-model.md`
  - A second run produces the same row count
- **Dependencies:** Tasks 1 and 3

## Task 5: Ingest Products into Bronze
- **Layer:** Bronze
- **File(s):** `src/bronze/03_ingest_products.py`
- **Input:** `/Volumes/ecommerce/medallion/data/raw/products.csv`
- **Output:** Managed Delta table `ecommerce.medallion.bronze_products`
- **Acceptance Criteria:**
  - Uses the explicit all-STRING products schema
  - Preserves all 500 source rows without cleansing
  - Adds non-null `ingestion_timestamp` and `source_file_name`
  - Output schema conforms to `data-model.md`
  - Re-running overwrites rather than appends
- **Dependencies:** Tasks 1 and 3

## Task 6: Ingest Orders into Bronze
- **Layer:** Bronze
- **File(s):** `src/bronze/04_ingest_orders.py`
- **Input:** `/Volumes/ecommerce/medallion/data/raw/orders.csv`
- **Output:** Managed Delta table `ecommerce.medallion.bronze_orders`
- **Acceptance Criteria:**
  - Uses an explicit all-STRING orders schema
  - Preserves nulls, orphans, negative values, malformed dates, and duplicates
  - Adds non-null `ingestion_timestamp` and `source_file_name`
  - Row count equals the CSV data-row count (approximately 100,020 with
    appended duplicates)
  - Output schema conforms to `data-model.md`
  - A second run does not increase the row count
- **Dependencies:** Tasks 1 and 3

## Task 7: Validate the Bronze Layer
- **Layer:** Bronze
- **File(s):** `src/bronze/05_validate_bronze.py`
- **Input:** The three `ecommerce.*_bronze` tables
- **Output:** Logged Bronze validation results
- **Acceptance Criteria:**
  - Verifies all expected tables, columns, and Delta locations exist
  - Compares each Bronze count with its raw CSV count
  - Confirms all source columns are STRING and metadata columns are present
  - Confirms the intentional null and duplicate records still exist
  - Fails with a logged exception when any validation fails
- **Dependencies:** Tasks 4, 5, and 6

## Task 8: Implement Reusable Silver Quality-Check Helpers
- **Layer:** Silver
- **File(s):** `src/silver/quality_checks.py`
- **Input:** Spark DataFrames and per-table check configuration
- **Output:** Reusable completeness, uniqueness, type-validation, referential,
  and result-assembly functions
- **Acceptance Criteria:**
  - Completeness detects null and trimmed-empty values
  - Uniqueness uses a window count and flags all members of duplicate groups
  - Type checks validate email regex, `yyyy-MM-dd` dates, allowed domains,
    positive numeric values, and the amount tolerance of 0.01
  - Referential helper supports left anti-join detection on non-null FKs
  - `quality_check_result` is `PASS` or ordered pipe-delimited fail tokens
  - Rows failing multiple checks include every token exactly once
  - Helpers never filter or delete rows and have docstrings
- **Dependencies:** Tasks 1 and 7

## Task 9: Build the Customers Silver Table
- **Layer:** Silver
- **File(s):** `src/silver/01_process_customers.py`
- **Input:** `ecommerce.medallion.bronze_customers`
- **Output:** Managed Delta table `ecommerce.medallion.customers_silver`
- **Acceptance Criteria:**
  - Runs completeness on `email`, uniqueness on `customer_id`, and documented
    email/date/segment/LTV type checks
  - Sets `referential_check = 'N/A'`
  - Detects the 50 null emails and duplicate customer-key groups
  - Keeps exactly the same number of rows as customers Bronze
  - Uses `PASS`, `COMPLETENESS_FAIL`, `UNIQUENESS_FAIL`, and
    `TYPE_VALIDATION_FAIL` tokens as applicable
  - Writes Delta idempotently and conforms to `data-model.md`
- **Dependencies:** Task 8

## Task 10: Build the Products Silver Table
- **Layer:** Silver
- **File(s):** `src/silver/02_process_products.py`
- **Input:** `ecommerce.medallion.bronze_products`
- **Output:** Managed Delta table `ecommerce.medallion.products_silver`
- **Acceptance Criteria:**
  - Runs completeness and uniqueness on product fields
  - Validates positive price/cost and non-negative integer stock/reorder values
  - Sets `referential_check = 'N/A'`
  - Keeps exactly 500 rows
  - Writes Delta idempotently and conforms to `data-model.md`
- **Dependencies:** Task 8

## Task 11: Build the Orders Silver Table
- **Layer:** Silver
- **File(s):** `src/silver/03_process_orders.py`
- **Input:** `ecommerce.medallion.bronze_orders`,
  `ecommerce.medallion.bronze_customers`, and `ecommerce.medallion.bronze_products`
- **Output:** Managed Delta table `ecommerce.medallion.orders_silver`
- **Acceptance Criteria:**
  - Detects 100 null customer IDs and 200 null product IDs, accounting for any
    deliberate overlap in row-level counts
  - Flags all duplicate `order_id` groups using a window function
  - Validates dates, statuses, positive quantities/prices, and
    `total_amount ≈ quantity * unit_price` within 0.01
  - Detects 50 customer and 30 product orphans using left anti joins
  - Null FKs fail completeness but are not counted as orphans
  - Multi-fail rows contain ordered pipe-delimited tokens such as
    `COMPLETENESS_FAIL|REFERENTIAL_INTEGRITY_FAIL`
  - Keeps exactly the same number of rows as orders Bronze
- **Dependencies:** Tasks 8, 9, and 10

## Task 12: Generate the Silver Quality Metrics Report
- **Layer:** Silver
- **File(s):** `src/silver/04_build_quality_metrics.py`
- **Input:** `ecommerce.medallion.customers_silver`,
  `ecommerce.medallion.orders_silver`, and `ecommerce.medallion.products_silver`
- **Output:** Managed Delta table `ecommerce.medallion.quality_metrics`
- **Acceptance Criteria:**
  - Produces one row per table/check/run
  - Includes `check_name`, `total_rows`, `passed`, `failed`, and
    `pass_rate_%`
  - Also records `table_name`, `batch_timestamp`, threshold, and
    `threshold_met`
  - Referential denominators exclude non-applicable/null-FK rows
  - A multi-fail source row increments `failed` for every failed check
  - Metrics reconcile with intentional issues, including 80 orphans
  - Re-running replaces the report rather than appending duplicate run rows
- **Dependencies:** Tasks 9, 10, and 11

## Task 13: Validate Silver Retention and Quality Results
- **Layer:** Silver
- **File(s):** `src/silver/05_validate_silver.py`
- **Input:** All Bronze/Silver tables and `ecommerce.medallion.quality_metrics`
- **Output:** Logged Silver reconciliation and quality-test results
- **Acceptance Criteria:**
  - Confirms Bronze and Silver row counts match per entity
  - Confirms no bad row was dropped
  - Confirms clean rows equal `quality_check_result = 'PASS'`
  - Confirms all non-PASS values contain only documented, correctly ordered
    tokens
  - Confirms `passed + failed = total_rows` for each applicable metric
  - Confirms expected threshold misses are reported, not raised as pipeline
    failures
- **Dependencies:** Task 12

## Task 14: Build Sales by Product
- **Layer:** Gold
- **File(s):** `src/gold/01_sales_by_product.py`
- **Input:** `ecommerce.medallion.orders_silver` and
  `ecommerce.medallion.products_silver`
- **Output:** Managed Delta table `ecommerce.medallion.sales_by_product`
- **Acceptance Criteria:**
  - Uses only `PASS` and `Completed` orders joined to PASS products
  - Produces one row per `product_id`
  - Includes order count, units sold, gross revenue, COGS, gross margin, and
    average unit price
  - Numeric types conform to `data-model.md`
  - Re-running with unchanged Silver data produces identical business totals
- **Dependencies:** Task 13

## Task 15: Build Revenue by Customer
- **Layer:** Gold
- **File(s):** `src/gold/02_revenue_by_customer.py`
- **Input:** `ecommerce.medallion.orders_silver` and
  `ecommerce.medallion.customers_silver`
- **Output:** Managed Delta table `ecommerce.medallion.revenue_by_customer`
- **Acceptance Criteria:**
  - Uses only `PASS` and `Completed` orders joined to PASS customers
  - Produces one row per `customer_id`
  - Includes customer attributes, completed order count, units, gross revenue,
    average order value, and source lifetime value
  - Duplicate or failed keys cannot inflate revenue
  - Re-running overwrites and preserves identical totals
- **Dependencies:** Task 13

## Task 16: Build Customer Segmentation
- **Layer:** Gold
- **File(s):** `src/gold/03_customer_segmentation.py`
- **Input:** `ecommerce.medallion.revenue_by_customer`
- **Output:** Managed Delta table
  `ecommerce.medallion.customer_segmentation`
- **Acceptance Criteria:**
  - Produces at most one row for each Premium, Standard, and Basic segment
  - Includes customer count, completed order count, total revenue, average
    lifetime value, average computed revenue, and revenue percentage
  - Revenue percentages total approximately 100%, allowing rounding tolerance
  - Invalid/failed segment values are absent because Gold uses PASS rows
  - Re-running overwrites without changing totals
- **Dependencies:** Task 15

## Task 17: Reconcile and Validate Gold Aggregations
- **Layer:** Gold
- **File(s):** `src/gold/04_validate_gold.py`
- **Input:** All three Gold tables and relevant Silver PASS rows
- **Output:** Logged Gold reconciliation and idempotency results
- **Acceptance Criteria:**
  - Sum of product revenue equals sum of customer revenue within 0.01
  - Sum of segment revenue equals sum of customer revenue within 0.01
  - Gold contains no duplicate product/customer/segment grains
  - Gold revenue traces only to PASS, Completed orders
  - Running Gold twice does not change row counts or totals
  - Any reconciliation failure is logged and raised
- **Dependencies:** Tasks 14, 15, and 16

## Task 18: Create Dashboard SQL Queries
- **Layer:** Dashboard
- **File(s):** `src/dashboard/dashboard_queries.sql`
- **Input:** `ecommerce.medallion.sales_by_product`,
  `ecommerce.medallion.revenue_by_customer`, and
  `ecommerce.medallion.customer_segmentation`
- **Output:** ANSI-compatible Spark SQL queries for dashboard visualizations
- **Acceptance Criteria:**
  - Includes top products by revenue query
  - Includes revenue and customer count by segment query
  - Includes revenue by country or top customers query
  - Includes an optional margin-by-category query
  - Queries use Gold tables only and use Unity Catalog three-level names
  - Results have clear chart labels, deterministic ordering, and sensible
    limits
- **Dependencies:** Task 17

## Task 19: Assemble and Validate the Dashboard
- **Layer:** Dashboard
- **File(s):** `src/dashboard/dashboard_setup.md`,
  `src/dashboard/dashboard_validation.sql`
- **Input:** Queries from Task 18 and the three Gold Delta tables
- **Output:** Databricks SQL Dashboard (or notebook visualization fallback)
  notebook visualization fallback) with at least three visualizations
- **Acceptance Criteria:**
  - Creates at least three visuals: top products, revenue by segment, and
    revenue by country/top customers
  - Each visualization maps to a query and Gold source documented in
    `dashboard_setup.md`
  - Dashboard displays non-empty results and labels currency/count fields
    clearly
  - Validation SQL reconciles displayed totals with Gold
  - No dashboard query reads Raw, Bronze, or Silver tables
- **Dependencies:** Task 18

