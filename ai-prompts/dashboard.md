# AI Prompts — Dashboard

## Prompt 1: Dashboard creation

**PROMPT SENT:**
Create src/dashboard/dashboard_queries.sql with 4 SQL queries for Databricks SQL Dashboard:

Top 10 Products by Revenue (Bar Chart): SELECT product_name, total_revenue FROM gold_sales_by_product ORDER BY total_revenue DESC LIMIT 10

Customer Revenue Distribution (Histogram): SELECT CASE WHEN total_revenue < 100 THEN '$0-100' WHEN total_revenue < 500 THEN '$100-500' WHEN total_revenue < 1000 THEN '$500-1000' WHEN total_revenue < 5000 THEN '$1000-5000' ELSE '$5000+' END as revenue_bucket, COUNT(*) as customer_count FROM gold_revenue_by_customer GROUP BY 1 ORDER BY 1

Customer Segmentation (Pie Chart): SELECT segment_type, customer_count, total_revenue FROM gold_customer_segmentation

Data Quality Summary (Table/Scoreboard): SELECT check_name, table_name, pass_rate FROM silver_quality_report

You can add more queries for dashboard so that we can get a dertailed view from the dashboard 

Also create src/dashboard/DASHBOARD_GUIDE.md with:

Step-by-step instructions to create the dashboard in Databricks Community Edition
Screenshots placeholder descriptions
Filter configuration (by category, by date range, by segment)

**AI RESPONSE SUMMARY:**
Dashboard SQL and a create-guide are in place. Queries use ecommerce.medallion.* (not gold_sales_by_product). Quality comes from quality_metrics (pass_rate_pct aliased as pass_rate).

Files

src/dashboard/dashboard_queries.sql — 9 separate Databricks SQL queries
src/dashboard/DASHBOARD_GUIDE.md — SQL warehouse steps, layout, filters, screenshot placeholders, CE limits
Required four

Chart	Source
Top 10 products (bar)
sales_by_product
Revenue buckets (bar)
revenue_by_customer
Value segments (pie)
customer_segmentation (segment_type)
Quality table
quality_metrics
Also included: KPI counters, daily/weekly lines, category mix, top customers (queries 5–9).

How to build: SQL Editor → warehouse (not the all-purpose cluster) → save one query per block → New Dashboard → add widgets. Filters: category, order_date / week, segment_type, customer_segment. Date filters apply to trend queries only.

This workspace is Unity Catalog, not Community Edition. CE cannot see ecommerce.medallion. The guide uses Path A for your real workspace and Path B only to explain CE limits.

Documentation summary

Added src/dashboard/dashboard_queries.sql (queries 1–9) against managed Gold/quality tables.
Added src/dashboard/DASHBOARD_GUIDE.md with Databricks SQL steps, filters, screenshot placeholders, and CE vs UC notes.
Next: paste queries into SQL Editor and assemble the dashboard; capture screenshots for evaluation.

**YOUR EVALUATION:**
Accepted Queries 1 - 9 for dashboard and discarded rest of the queries.
