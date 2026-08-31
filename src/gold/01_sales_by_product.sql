-- Databricks notebook source
-- Gold: sales by product from Silver PASS + Completed orders.
-- Writes managed Delta table ecommerce.medallion.sales_by_product (overwrite).
-- Does not use Volume LOCATION or dbfs:/FileStore paths.

CREATE OR REPLACE TABLE ecommerce.medallion.sales_by_product
USING DELTA AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(o.order_id) AS total_orders,
    CAST(SUM(o.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
    CAST(AVG(o.total_amount) AS DECIMAL(18, 2)) AS avg_order_value,
    CAST(SUM(o.quantity) AS BIGINT) AS total_quantity_sold,
    CAST(
        (
            SUM(o.total_amount) - SUM(o.quantity * p.cost)
        ) / NULLIF(SUM(o.total_amount), 0) * 100
        AS DECIMAL(18, 2)
    ) AS profit_margin
FROM ecommerce.medallion.orders_silver AS o
INNER JOIN ecommerce.medallion.products_silver AS p
    ON o.product_id = p.product_id
WHERE o.quality_check_result = 'PASS'
    AND p.quality_check_result = 'PASS'
    AND o.order_status = 'Completed'
GROUP BY
    p.product_id,
    p.product_name,
    p.category
;
