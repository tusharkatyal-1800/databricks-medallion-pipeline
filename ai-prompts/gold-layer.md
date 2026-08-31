# AI Prompts — Gold Layer (Phase 5)

## Prompt 1: Generating sales by product logic

**PROMPT SENT:**
Create src/gold/01_sales_by_product.sql — a Spark SQL script that creates the Gold-layer Sales by Product aggregation.

Read from Silver tables (only clean rows where quality_check_result = 'PASS').

Join silver_orders with silver_products to produce:

product_id
product_name
category
total_orders: COUNT(order_id)
total_revenue: SUM(total_amount)
avg_order_value: AVG(total_amount)
total_quantity_sold: SUM(quantity)
profit_margin: (SUM(total_amount) - SUM(quantity * cost)) / SUM(total_amount) * 100
Only include Completed orders (order_status = 'Completed').

save as it should be stored in databricks

**AI RESPONSE SUMMARY:**
src/gold/01_sales_by_product.sql is a Databricks SQL notebook that overwrites the managed table ecommerce.medallion.sales_by_product. It does not use Volume LOCATION or dbfs:/FileStore.

Filters: quality_check_result = 'PASS' on orders and products, and order_status = 'Completed'.

Grain: product_id with total_orders, total_revenue, avg_order_value, total_quantity_sold, and profit_margin as a percent. NULLIF avoids divide-by-zero.

Run (Silver must already exist):

Open src/gold/01_sales_by_product.sql in Databricks.
Attach a SQL warehouse or an all-purpose cluster (this file is SQL-only, so a warehouse is valid).
Run all.

---

## Prompt 2: Genrating revenue by customer logic

**PROMPT SENT:**
Create src/gold/02_revenue_by_customer.sql — Gold-layer Revenue by Customer.

Read from clean Silver rows only.

Join silver_orders with silver_customers:

customer_id
customer_name
customer_segment
total_orders: COUNT(DISTINCT order_id)
total_revenue: SUM(total_amount) for Completed orders
avg_order_value: total_revenue / total_orders
first_order_date: MIN(order_date)
last_order_date: MAX(order_date)
customer_tenure_days: DATEDIFF(last_order_date, first_order_date)
lifetime_value_actual: total_revenue (rename for clarity)

**AI RESPONSE SUMMARY:**
src/gold/02_revenue_by_customer.sql overwrites managed table ecommerce.medallion.revenue_by_customer.

It joins PASS Silver orders to PASS Silver customers and keeps order_status = 'Completed' only, so total_revenue and total_orders are both completed-order metrics.

Column	Logic
total_orders
COUNT(DISTINCT order_id)
total_revenue
SUM(total_amount)
avg_order_value
total_revenue / total_orders (NULLIF if zero orders)
first_order_date / last_order_date
MIN / MAX of order_date
customer_tenure_days
DATEDIFF(last_order_date, first_order_date)
lifetime_value_actual
same as total_revenue
Run: after Silver (and optionally after 01_sales_by_product.sql). Open the file, attach a SQL warehouse or all-purpose cluster, Run all.

---

## Prompt 3: Generating daily_weekly_trends logic

**PROMPT SENT:**
Create a Spark SQL script `src/gold/03_daily_weekly_trends.sql` (or PySpark equivalent in `src/gold/03_daily_weekly_trends.py`) for the Gold layer.
Business Context & Objectives:
Calculate daily and weekly sales trends from clean Silver data to help business stakeholders track revenue growth, order volume, and order velocity over time.
Requirements:
1. Read ONLY clean rows from Silver orders (`ecommerce.medallion.orders_silver` where `quality_check_result = 'PASS'`)
2. Only include completed orders (`order_status = 'Completed'`)
3. Calculate metrics at both Daily and Weekly levels:
   - `order_date`: The date of transactions
   - `year_week`: Year and week format (e.g., '2024-W12' or year/week integer) for weekly rollup
   - `total_orders`: COUNT(DISTINCT order_id)
   - `total_revenue`: SUM(total_amount)
   - `avg_order_value`: ROUND(AVG(total_amount), 2)
   - `total_items_sold`: SUM(quantity)
   - `completed_orders_cnt`: Count of completed orders
   - `cancelled_orders_cnt`: Count of cancelled orders (from Silver orders)
   - `revenue_growth_pct`: Day-over-day (DoD) or week-over-week (WoW) revenue growth using LAG() window functions

**AI RESPONSE SUMMARY:**
src/gold/03_daily_weekly_trends.sql writes two managed Gold tables:

ecommerce.medallion.sales_daily_trends
ecommerce.medallion.sales_weekly_trends
Rules

Base filter: quality_check_result = 'PASS'
Revenue, AOV, items, total_orders, completed_orders_cnt: Completed only
cancelled_orders_cnt: PASS Cancelled orders on the same day/week (so it is not always zero)
year_week: yyyy-W12
revenue_growth_pct: LAG DoD / WoW vs prior period (NULLIF if prior revenue is 0)
Weekly grain: DATE_TRUNC('WEEK', order_date) (Spark weeks start Monday)

Added 03_daily_weekly_trends.sql writing managed daily/weekly trend tables. Completed orders drive revenue metrics; cancelled counts come from PASS cancelled rows. Growth uses LAG.

Documentation summary

Added src/gold/03_daily_weekly_trends.sql and Gold table names in src/common/config.py.
Daily and weekly managed Delta overwrites; PASS Silver only; Completed vs Cancelled split with window growth %.
Run after Silver; customer segmentation Gold table is still pending.

---

## Prompt 4: Generating Customer_segments logic

**PROMPT SENT:**
Create src/gold/04_customer_segmentation.sql — Gold-layer Customer Segmentation.

Using the Revenue by Customer aggregation, segment customers:

Segmentation rules:

High-Value: total_revenue > $1000 AND total_orders >= 5
Repeat: total_orders >= 3 AND total_revenue <= $1000
One-Time: total_orders = 1
Inactive: No orders in the last 6 months (relative to MAX(order_date) in dataset)
Produce:

segment_type (High-Value/Repeat/One-Time/Inactive)
customer_count: COUNT
avg_revenue: AVG(total_revenue)
total_revenue: SUM(total_revenue)
avg_orders: AVG(total_orders)

**AI RESPONSE SUMMARY:**
src/gold/04_customer_segmentation.sql overwrites managed ecommerce.medallion.customer_segmentation from revenue_by_customer (so run 02 first).

Rule order (first match wins):

Inactive — last_order_date older than 6 months before MAX(last_order_date) in that Gold table
High-Value — total_revenue > 1000 and total_orders >= 5
Repeat — total_orders >= 3 and total_revenue <= 1000
One-Time — total_orders = 1
Other — leftover (for example exactly 2 completed orders)
Output: segment_type, customer_count, avg_revenue, total_revenue, avg_orders.

This is value/behavior segmentation, not CSV Premium/Standard/Basic (those stay on revenue_by_customer.customer_segment).

Added 04_customer_segmentation.sql rolling revenue_by_customer into High-Value / Repeat / One-Time / Inactive (plus Other). Inactive uses dataset max order date minus 6 months. Managed table overwrite.

Documentation summary

Added src/gold/04_customer_segmentation.sql.
Segments Gold customers from revenue_by_customer; Inactive is checked first.
Run after 02_revenue_by_customer.sql. CSV Premium/Standard/Basic is unchanged on the customer revenue table.

---

## Prompt 5: Single create gold table script with summary

**PROMPT SENT:**
Create src/gold/create_gold_tables.py to:

Read all .sql files from the gold directory and execute them
Log row counts for each Gold table
Add timing for each aggregation
Print summary:
Gold Table	Rows	Duration (s)


**AI RESPONSE SUMMARY:**
src/gold/create_gold_tables.py is the Gold orchestrator. It finds src/gold/*.sql, runs them in name order (01 → 04), times each statement, counts rows, and prints a summary.

It splits -- COMMAND ---------- so 03 creates both daily and weekly tables. If one file fails, later files stop (04 needs 02).

The SQL files must sit next to the notebook under src/gold/.

Documentation summary

Added src/gold/create_gold_tables.py to run all Gold SQL files, time them, and log row counts.
Execution order is 01–04; stops after a failed file.
Run on an all-purpose cluster after Silver; summary includes daily/weekly trend tables as well as the three named aggregations.
