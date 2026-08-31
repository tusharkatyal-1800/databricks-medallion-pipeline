-- Databricks SQL Dashboard queries
-- Each block is a separate Query in Databricks SQL.
-- Do not run this file as one notebook. Gold and quality_metrics must already exist.
-- Catalog.schema: ecommerce.medallion (not gold_* / silver_* aliases).

-- =============================================================================
-- QUERY 1 — Top 10 products by revenue (bar chart)
-- Visualization: Bar. X = product_name, Y = total_revenue
-- Optional dashboard filter: category
-- =============================================================================
SELECT
    product_name,
    category,
    total_revenue,
    total_orders,
    profit_margin
FROM ecommerce.medallion.sales_by_product
ORDER BY total_revenue DESC
LIMIT 10
;

-- =============================================================================
-- QUERY 2 — Customer revenue distribution (histogram / bar)
-- Visualization: Bar. X = revenue_bucket, Y = customer_count
-- =============================================================================
SELECT
    CASE
        WHEN total_revenue < 100 THEN '1. $0-100'
        WHEN total_revenue < 500 THEN '2. $100-500'
        WHEN total_revenue < 1000 THEN '3. $500-1000'
        WHEN total_revenue < 5000 THEN '4. $1000-5000'
        ELSE '5. $5000+'
    END AS revenue_bucket,
    COUNT(*) AS customer_count,
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS bucket_revenue
FROM ecommerce.medallion.revenue_by_customer
GROUP BY
    CASE
        WHEN total_revenue < 100 THEN '1. $0-100'
        WHEN total_revenue < 500 THEN '2. $100-500'
        WHEN total_revenue < 1000 THEN '3. $500-1000'
        WHEN total_revenue < 5000 THEN '4. $1000-5000'
        ELSE '5. $5000+'
    END
ORDER BY revenue_bucket
;

-- =============================================================================
-- QUERY 3 — Customer value segmentation (pie chart)
-- Visualization: Pie. Slice = segment_type, value = customer_count or total_revenue
-- Optional dashboard filter: segment_type
-- =============================================================================
SELECT
    segment_type,
    customer_count,
    total_revenue,
    avg_revenue,
    avg_orders
FROM ecommerce.medallion.customer_segmentation
ORDER BY total_revenue DESC
;

-- =============================================================================
-- QUERY 4 — Data quality summary (table / scoreboard)
-- Visualization: Table. Also use Overall quality rows as KPI counters.
-- =============================================================================
SELECT
    table_name,
    check_name,
    field_checked,
    total_rows,
    applicable_rows,
    passed,
    failed,
    CAST(pass_rate_pct AS DECIMAL(5, 2)) AS pass_rate,
    threshold_met
FROM ecommerce.medallion.quality_metrics
ORDER BY table_name, check_name, field_checked
;

-- =============================================================================
-- QUERY 5 — KPI: completed Gold revenue and orders
-- Visualization: Counter / scoreboard (single-row)
-- =============================================================================
SELECT
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS completed_revenue,
    CAST(SUM(total_orders) AS BIGINT) AS completed_orders,
    CAST(
        SUM(total_revenue) / NULLIF(SUM(total_orders), 0)
        AS DECIMAL(18, 2)
    ) AS avg_order_value,
    CAST(SUM(total_items_sold) AS BIGINT) AS items_sold
FROM ecommerce.medallion.sales_daily_trends
;

-- =============================================================================
-- QUERY 6 — Daily revenue trend (line chart)
-- Visualization: Line. X = order_date, Y = total_revenue
-- Dashboard filter: order_date date range
-- =============================================================================
SELECT
    order_date,
    year_week,
    total_revenue,
    total_orders,
    avg_order_value,
    total_items_sold,
    cancelled_orders_cnt,
    revenue_growth_pct
FROM ecommerce.medallion.sales_daily_trends
ORDER BY order_date
;

-- =============================================================================
-- QUERY 7 — Weekly revenue and week-over-week growth (combo / line)
-- Visualization: Line (revenue) + optional second axis (revenue_growth_pct)
-- =============================================================================
SELECT
    week_start_date,
    year_week,
    total_revenue,
    total_orders,
    avg_order_value,
    revenue_growth_pct,
    completed_orders_cnt,
    cancelled_orders_cnt
FROM ecommerce.medallion.sales_weekly_trends
ORDER BY week_start_date
;

-- =============================================================================
-- QUERY 8 — Revenue by product category (bar)
-- Visualization: Bar. X = category, Y = total_revenue
-- Dashboard filter: category
-- =============================================================================
SELECT
    category,
    COUNT(*) AS product_count,
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue,
    CAST(SUM(total_orders) AS BIGINT) AS total_orders,
    CAST(AVG(profit_margin) AS DECIMAL(18, 2)) AS avg_profit_margin
FROM ecommerce.medallion.sales_by_product
GROUP BY category
ORDER BY total_revenue DESC
;

-- =============================================================================
-- QUERY 9 — Top 10 customers by revenue (bar or table)
-- Visualization: Bar or table
-- Optional filter: customer_segment (Premium / Standard / Basic)
-- =============================================================================
SELECT
    customer_name,
    customer_segment,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    customer_tenure_days
FROM ecommerce.medallion.revenue_by_customer
ORDER BY total_revenue DESC
LIMIT 10
;
