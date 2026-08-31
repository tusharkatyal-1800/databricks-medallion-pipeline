-- Databricks notebook source
-- DDL for the e-commerce Medallion pipeline (Unity Catalog).
-- Idempotent: CREATE IF NOT EXISTS only. Jobs still overwrite table *data*.
-- Do not attach LOCATION '/Volumes/...' to these tables.

-- -----------------------------------------------------------------------------
-- Catalog / schema / volume
-- The evaluation brief uses Hive-style:
--   CREATE DATABASE IF NOT EXISTS medallion_ecommerce;
-- On Databricks Unity Catalog, DATABASE is SCHEMA. Pipeline tables are
-- three-level names: ecommerce.medallion.<table> (not medallion_ecommerce.t).
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS medallion_ecommerce
COMMENT 'Brief Hive-style database name. Unused by jobs. Canonical schema is ecommerce.medallion.';

CREATE CATALOG IF NOT EXISTS ecommerce
COMMENT 'Unity Catalog for the e-commerce Medallion pipeline.';

CREATE DATABASE IF NOT EXISTS ecommerce.medallion
COMMENT 'Medallion schema: Bronze ingest, Silver quality flags, Gold aggregations, quality_metrics.';

CREATE VOLUME IF NOT EXISTS ecommerce.medallion.data
COMMENT 'File volume for raw CSVs only (/Volumes/ecommerce/medallion/data/raw/). Not a table LOCATION.';

-- =============================================================================
-- Bronze — raw land, typed persist, ingest metadata only
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce.medallion.bronze_customers (
    customer_id INT COMMENT 'Natural key from CSV; duplicates possible',
    customer_name STRING COMMENT 'Display name',
    email STRING COMMENT 'Contact email; planted nulls allowed',
    country STRING COMMENT 'Country as delivered',
    signup_date DATE COMMENT 'Calendar signup date yyyy-MM-dd',
    customer_segment STRING COMMENT 'Expected Premium / Standard / Basic',
    lifetime_value DECIMAL(10, 2) COMMENT 'Source lifetime value',
    _ingestion_timestamp TIMESTAMP COMMENT 'Bronze write time',
    _source_file STRING COMMENT 'Source file name',
    _batch_id STRING COMMENT 'Ingest batch identifier'
)
USING DELTA
COMMENT 'Bronze customers: 1:1 with customers.csv. No cleansing. Managed Delta.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.bronze_orders (
    order_id INT COMMENT 'Natural key; planted duplicate ids possible',
    customer_id INT COMMENT 'FK to customers; nulls and orphans possible',
    order_date DATE COMMENT 'Order calendar date yyyy-MM-dd',
    product_id INT COMMENT 'FK to products; nulls and orphans possible',
    quantity INT COMMENT 'Units ordered',
    unit_price DECIMAL(10, 2) COMMENT 'Unit price from catalog at generate time',
    total_amount DECIMAL(10, 2) COMMENT 'Line total',
    order_status STRING COMMENT 'Expected Pending / Completed / Cancelled',
    payment_date DATE COMMENT 'Nullable; required for Completed in Silver type checks',
    _ingestion_timestamp TIMESTAMP COMMENT 'Bronze write time',
    _source_file STRING COMMENT 'Source file name',
    _batch_id STRING COMMENT 'Ingest batch identifier'
)
USING DELTA
COMMENT 'Bronze orders: 1:1 with orders.csv. No cleansing. Managed Delta.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.bronze_products (
    product_id INT COMMENT 'Natural key',
    product_name STRING COMMENT 'Product display name',
    category STRING COMMENT 'Merchandise category',
    price DECIMAL(10, 2) COMMENT 'List price',
    cost DECIMAL(10, 2) COMMENT 'Unit cost',
    stock_quantity INT COMMENT 'On-hand units; zero allowed',
    reorder_level INT COMMENT 'Reorder threshold',
    _ingestion_timestamp TIMESTAMP COMMENT 'Bronze write time',
    _source_file STRING COMMENT 'Source file name',
    _batch_id STRING COMMENT 'Ingest batch identifier'
)
USING DELTA
COMMENT 'Bronze products: 1:1 with products.csv. No cleansing. Managed Delta.';

-- =============================================================================
-- Silver — all Bronze rows plus quality flags (never dropped)
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce.medallion.customers_silver (
    customer_id INT COMMENT 'Natural key from Bronze',
    customer_name STRING COMMENT 'Display name',
    email STRING COMMENT 'Contact email',
    country STRING COMMENT 'Country as delivered',
    signup_date DATE COMMENT 'Calendar signup date',
    customer_segment STRING COMMENT 'Expected Premium / Standard / Basic',
    lifetime_value DECIMAL(10, 2) COMMENT 'Source lifetime value',
    _ingestion_timestamp TIMESTAMP COMMENT 'Bronze ingest time',
    _source_file STRING COMMENT 'Source file name',
    _batch_id STRING COMMENT 'Ingest batch identifier',
    completeness_check STRING COMMENT 'PASS or FAIL_NULL_email',
    uniqueness_check STRING COMMENT 'PASS or FAIL_DUPLICATE_customer_id (keep-first)',
    type_validation_check STRING COMMENT 'PASS or FAIL_INVALID_* tokens',
    business_logic_check STRING COMMENT 'PASS or FAIL_INVALID_*; rolls into TYPE_VALIDATION_FAIL',
    referential_integrity_check STRING COMMENT 'N/A on customers',
    quality_check_result STRING COMMENT 'PASS or pipe-delimited COMPLETENESS_FAIL|UNIQUENESS_FAIL|TYPE_VALIDATION_FAIL'
)
USING DELTA
COMMENT 'Silver customers: Bronze grain plus four quality checks. Bad rows flagged, not deleted.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.orders_silver (
    order_id INT COMMENT 'Natural key from Bronze',
    customer_id INT COMMENT 'FK to customers',
    order_date DATE COMMENT 'Order calendar date',
    product_id INT COMMENT 'FK to products',
    quantity INT COMMENT 'Units ordered',
    unit_price DECIMAL(10, 2) COMMENT 'Unit price',
    total_amount DECIMAL(10, 2) COMMENT 'Line total',
    order_status STRING COMMENT 'Pending / Completed / Cancelled',
    payment_date DATE COMMENT 'Nullable by design',
    _ingestion_timestamp TIMESTAMP COMMENT 'Bronze ingest time',
    _source_file STRING COMMENT 'Source file name',
    _batch_id STRING COMMENT 'Ingest batch identifier',
    completeness_check STRING COMMENT 'PASS or FAIL_NULL_customer_id|FAIL_NULL_product_id',
    uniqueness_check STRING COMMENT 'PASS or FAIL_DUPLICATE_order_id (keep-first)',
    type_validation_check STRING COMMENT 'PASS or FAIL_INVALID_* tokens',
    business_logic_check STRING COMMENT 'PASS or FAIL_INVALID_*; includes order_before_signup',
    referential_integrity_check STRING COMMENT 'PASS or FAIL_ORPHAN_*',
    quality_check_result STRING COMMENT 'PASS or pipe-delimited fail tokens including REFERENTIAL_INTEGRITY_FAIL'
)
USING DELTA
COMMENT 'Silver orders: Bronze grain plus four quality checks. Gold reads PASS (+ Completed for KPIs).';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.products_silver (
    product_id INT COMMENT 'Natural key from Bronze',
    product_name STRING COMMENT 'Product display name',
    category STRING COMMENT 'Merchandise category',
    price DECIMAL(10, 2) COMMENT 'List price',
    cost DECIMAL(10, 2) COMMENT 'Unit cost',
    stock_quantity INT COMMENT 'On-hand units',
    reorder_level INT COMMENT 'Reorder threshold',
    _ingestion_timestamp TIMESTAMP COMMENT 'Bronze ingest time',
    _source_file STRING COMMENT 'Source file name',
    _batch_id STRING COMMENT 'Ingest batch identifier',
    completeness_check STRING COMMENT 'Always PASS (no planted product completeness fields)',
    uniqueness_check STRING COMMENT 'PASS or FAIL_DUPLICATE_product_id',
    type_validation_check STRING COMMENT 'PASS or FAIL_INVALID_* tokens',
    business_logic_check STRING COMMENT 'PASS or FAIL_INVALID_reorder_level',
    referential_integrity_check STRING COMMENT 'N/A on products',
    quality_check_result STRING COMMENT 'PASS or pipe-delimited fail tokens'
)
USING DELTA
COMMENT 'Silver products: Bronze grain plus quality checks. Completeness is PASS.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.quality_metrics (
    table_name STRING COMMENT 'customers, orders, or products',
    check_name STRING COMMENT 'completeness, uniqueness, type_validation, referential_integrity, or overall',
    field_checked STRING COMMENT 'Column or rule name',
    total_rows INT COMMENT 'Silver row count for the table',
    applicable_rows INT COMMENT 'Rows scored for this field',
    passed INT COMMENT 'Applicable rows that passed',
    failed INT COMMENT 'Applicable rows that failed',
    pass_rate_pct DOUBLE COMMENT '100.0 * passed / applicable_rows',
    threshold DOUBLE COMMENT 'Documented pass-rate bar',
    threshold_met BOOLEAN COMMENT 'Whether pass_rate_pct meets the bar',
    batch_timestamp TIMESTAMP COMMENT 'Silver run timestamp'
)
USING DELTA
COMMENT 'Per-field Silver quality report. threshold_met false is expected for planted defects, not a job crash.';

-- =============================================================================
-- Gold — aggregations from Silver PASS (Completed for product/customer KPIs)
-- =============================================================================

CREATE TABLE IF NOT EXISTS ecommerce.medallion.sales_by_product (
    product_id INT COMMENT 'Product grain',
    product_name STRING COMMENT 'From Silver products',
    category STRING COMMENT 'From Silver products',
    total_orders BIGINT COMMENT 'Completed PASS orders',
    total_revenue DECIMAL(18, 2) COMMENT 'Sum of total_amount',
    avg_order_value DECIMAL(18, 2) COMMENT 'Average total_amount',
    total_quantity_sold BIGINT COMMENT 'Sum of quantity',
    profit_margin DECIMAL(18, 2) COMMENT '(revenue - qty*cost) / revenue * 100'
)
USING DELTA
COMMENT 'Gold sales by product from PASS Completed orders joined to PASS products.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.revenue_by_customer (
    customer_id INT COMMENT 'Customer grain',
    customer_name STRING COMMENT 'From Silver customers',
    customer_segment STRING COMMENT 'CSV Premium / Standard / Basic',
    total_orders BIGINT COMMENT 'Distinct completed PASS orders',
    total_revenue DECIMAL(18, 2) COMMENT 'Sum of total_amount',
    avg_order_value DECIMAL(18, 2) COMMENT 'total_revenue / total_orders',
    first_order_date DATE COMMENT 'Min completed order_date',
    last_order_date DATE COMMENT 'Max completed order_date',
    customer_tenure_days INT COMMENT 'DATEDIFF(last, first)',
    lifetime_value_actual DECIMAL(18, 2) COMMENT 'Computed revenue, not CSV lifetime_value'
)
USING DELTA
COMMENT 'Gold revenue by customer from PASS Completed orders joined to PASS customers.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.customer_segmentation (
    segment_type STRING COMMENT 'Inactive, High-Value, Repeat, One-Time, or Other',
    customer_count BIGINT COMMENT 'Customers in the value bucket',
    avg_revenue DECIMAL(18, 2) COMMENT 'Average total_revenue',
    total_revenue DECIMAL(18, 2) COMMENT 'Sum of total_revenue',
    avg_orders DECIMAL(18, 2) COMMENT 'Average total_orders'
)
USING DELTA
COMMENT 'Gold value segments from revenue_by_customer (not CSV Premium/Standard/Basic).';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.sales_daily_trends (
    order_date DATE COMMENT 'Calendar day grain',
    year_week STRING COMMENT 'yyyy-Wnn from WEEKOFYEAR',
    total_orders BIGINT COMMENT 'Distinct Completed PASS orders that day',
    total_revenue DECIMAL(18, 2) COMMENT 'Completed PASS revenue',
    avg_order_value DECIMAL(18, 2) COMMENT 'Average Completed total_amount',
    total_items_sold BIGINT COMMENT 'Sum of Completed quantity',
    completed_orders_cnt BIGINT COMMENT 'Distinct Completed PASS orders',
    cancelled_orders_cnt BIGINT COMMENT 'Distinct Cancelled PASS orders',
    revenue_growth_pct DECIMAL(18, 2) COMMENT 'Day-over-day revenue growth via LAG'
)
USING DELTA
COMMENT 'Gold daily trends: PASS orders. Revenue from Completed; cancelled counts separate.';

CREATE TABLE IF NOT EXISTS ecommerce.medallion.sales_weekly_trends (
    week_start_date DATE COMMENT 'DATE_TRUNC WEEK of order_date',
    year_week STRING COMMENT 'yyyy-Wnn',
    total_orders BIGINT COMMENT 'Distinct Completed PASS orders that week',
    total_revenue DECIMAL(18, 2) COMMENT 'Completed PASS revenue',
    avg_order_value DECIMAL(18, 2) COMMENT 'Average Completed total_amount',
    total_items_sold BIGINT COMMENT 'Sum of Completed quantity',
    completed_orders_cnt BIGINT COMMENT 'Distinct Completed PASS orders',
    cancelled_orders_cnt BIGINT COMMENT 'Distinct Cancelled PASS orders',
    revenue_growth_pct DECIMAL(18, 2) COMMENT 'Week-over-week revenue growth via LAG'
)
USING DELTA
COMMENT 'Gold weekly trends: PASS orders. Revenue from Completed; cancelled counts separate.';
