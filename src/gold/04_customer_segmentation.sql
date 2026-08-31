-- Databricks notebook source
-- Gold: value-based customer segments from revenue_by_customer.
-- Writes managed Delta table ecommerce.medallion.customer_segmentation (overwrite).
-- Requires 02_revenue_by_customer.sql to have been run.
-- Inactive is evaluated first, using MAX(last_order_date) as the dataset as-of date.
-- Unmatched customers (for example two orders) are labeled Other.
-- Does not use Volume LOCATION or dbfs:/FileStore paths.

CREATE OR REPLACE TABLE ecommerce.medallion.customer_segmentation
USING DELTA AS
SELECT
    segment_type,
    COUNT(*) AS customer_count,
    CAST(AVG(total_revenue) AS DECIMAL(18, 2)) AS avg_revenue,
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue,
    CAST(AVG(total_orders) AS DECIMAL(18, 2)) AS avg_orders
FROM (
    SELECT
        r.total_revenue,
        r.total_orders,
        CASE
            WHEN r.last_order_date < ADD_MONTHS(m.dataset_max_order_date, -6)
                THEN 'Inactive'
            WHEN r.total_revenue > 1000 AND r.total_orders >= 5
                THEN 'High-Value'
            WHEN r.total_orders >= 3 AND r.total_revenue <= 1000
                THEN 'Repeat'
            WHEN r.total_orders = 1
                THEN 'One-Time'
            ELSE 'Other'
        END AS segment_type
    FROM ecommerce.medallion.revenue_by_customer AS r
    CROSS JOIN (
        SELECT MAX(last_order_date) AS dataset_max_order_date
        FROM ecommerce.medallion.revenue_by_customer
    ) AS m
) AS segmented_customers
GROUP BY segment_type
;
