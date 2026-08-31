-- Databricks notebook source
-- Gold: daily and weekly sales trends from Silver PASS orders.
-- Revenue, AOV, and items use Completed orders only.
-- Cancelled counts use PASS Cancelled orders on the same day/week.
-- Writes managed Delta tables (overwrite). No Volume LOCATION or FileStore.

CREATE OR REPLACE TABLE ecommerce.medallion.sales_daily_trends
USING DELTA AS
SELECT
    order_date,
    year_week,
    total_orders,
    total_revenue,
    avg_order_value,
    total_items_sold,
    completed_orders_cnt,
    cancelled_orders_cnt,
    CAST(
        (
            total_revenue - LAG(total_revenue) OVER (ORDER BY order_date)
        ) / NULLIF(
            LAG(total_revenue) OVER (ORDER BY order_date),
            0
        ) * 100
        AS DECIMAL(18, 2)
    ) AS revenue_growth_pct
FROM (
    SELECT
        CAST(order_date AS DATE) AS order_date,
        CONCAT(
            DATE_FORMAT(CAST(order_date AS DATE), 'yyyy'),
            '-W',
            LPAD(CAST(WEEKOFYEAR(CAST(order_date AS DATE)) AS STRING), 2, '0')
        ) AS year_week,
        COUNT(
            DISTINCT CASE
                WHEN order_status = 'Completed' THEN order_id
            END
        ) AS total_orders,
        CAST(
            SUM(
                CASE
                    WHEN order_status = 'Completed' THEN total_amount
                    ELSE 0
                END
            ) AS DECIMAL(18, 2)
        ) AS total_revenue,
        CAST(
            AVG(
                CASE
                    WHEN order_status = 'Completed' THEN total_amount
                END
            ) AS DECIMAL(18, 2)
        ) AS avg_order_value,
        CAST(
            SUM(
                CASE
                    WHEN order_status = 'Completed' THEN quantity
                    ELSE 0
                END
            ) AS BIGINT
        ) AS total_items_sold,
        COUNT(
            DISTINCT CASE
                WHEN order_status = 'Completed' THEN order_id
            END
        ) AS completed_orders_cnt,
        COUNT(
            DISTINCT CASE
                WHEN order_status = 'Cancelled' THEN order_id
            END
        ) AS cancelled_orders_cnt
    FROM ecommerce.medallion.orders_silver
    WHERE quality_check_result = 'PASS'
    GROUP BY CAST(order_date AS DATE)
) AS daily_metrics
;

-- COMMAND ----------

CREATE OR REPLACE TABLE ecommerce.medallion.sales_weekly_trends
USING DELTA AS
SELECT
    week_start_date,
    CONCAT(
        DATE_FORMAT(week_start_date, 'yyyy'),
        '-W',
        LPAD(CAST(WEEKOFYEAR(week_start_date) AS STRING), 2, '0')
    ) AS year_week,
    total_orders,
    total_revenue,
    avg_order_value,
    total_items_sold,
    completed_orders_cnt,
    cancelled_orders_cnt,
    CAST(
        (
            total_revenue - LAG(total_revenue) OVER (ORDER BY week_start_date)
        ) / NULLIF(
            LAG(total_revenue) OVER (ORDER BY week_start_date),
            0
        ) * 100
        AS DECIMAL(18, 2)
    ) AS revenue_growth_pct
FROM (
    SELECT
        CAST(DATE_TRUNC('WEEK', CAST(order_date AS DATE)) AS DATE)
            AS week_start_date,
        COUNT(
            DISTINCT CASE
                WHEN order_status = 'Completed' THEN order_id
            END
        ) AS total_orders,
        CAST(
            SUM(
                CASE
                    WHEN order_status = 'Completed' THEN total_amount
                    ELSE 0
                END
            ) AS DECIMAL(18, 2)
        ) AS total_revenue,
        CAST(
            AVG(
                CASE
                    WHEN order_status = 'Completed' THEN total_amount
                END
            ) AS DECIMAL(18, 2)
        ) AS avg_order_value,
        CAST(
            SUM(
                CASE
                    WHEN order_status = 'Completed' THEN quantity
                    ELSE 0
                END
            ) AS BIGINT
        ) AS total_items_sold,
        COUNT(
            DISTINCT CASE
                WHEN order_status = 'Completed' THEN order_id
            END
        ) AS completed_orders_cnt,
        COUNT(
            DISTINCT CASE
                WHEN order_status = 'Cancelled' THEN order_id
            END
        ) AS cancelled_orders_cnt
    FROM ecommerce.medallion.orders_silver
    WHERE quality_check_result = 'PASS'
    GROUP BY CAST(DATE_TRUNC('WEEK', CAST(order_date AS DATE)) AS DATE)
) AS weekly_metrics
;
