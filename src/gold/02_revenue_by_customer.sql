-- Databricks notebook source
-- Gold: revenue by customer from Silver PASS + Completed orders.
-- Writes managed Delta table ecommerce.medallion.revenue_by_customer (overwrite).
-- Does not use Volume LOCATION or dbfs:/FileStore paths.

CREATE OR REPLACE TABLE ecommerce.medallion.revenue_by_customer
USING DELTA AS
SELECT
    customer_id,
    customer_name,
    customer_segment,
    total_orders,
    total_revenue,
    CAST(
        total_revenue / NULLIF(total_orders, 0)
        AS DECIMAL(18, 2)
    ) AS avg_order_value,
    first_order_date,
    last_order_date,
    DATEDIFF(last_order_date, first_order_date) AS customer_tenure_days,
    total_revenue AS lifetime_value_actual
FROM (
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        COUNT(DISTINCT o.order_id) AS total_orders,
        CAST(SUM(o.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date
    FROM ecommerce.medallion.orders_silver AS o
    INNER JOIN ecommerce.medallion.customers_silver AS c
        ON o.customer_id = c.customer_id
    WHERE o.quality_check_result = 'PASS'
        AND c.quality_check_result = 'PASS'
        AND o.order_status = 'Completed'
    GROUP BY
        c.customer_id,
        c.customer_name,
        c.customer_segment
) AS customer_revenue
;
