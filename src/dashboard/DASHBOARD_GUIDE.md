# Databricks SQL Dashboard Guide

This guide builds an e-commerce dashboard on **Gold** tables (and one Silver quality table). Run Bronze, Silver, and Gold first so these objects exist:

- `ecommerce.medallion.sales_by_product`
- `ecommerce.medallion.revenue_by_customer`
- `ecommerce.medallion.customer_segmentation`
- `ecommerce.medallion.sales_daily_trends`
- `ecommerce.medallion.sales_weekly_trends`
- `ecommerce.medallion.quality_metrics`

SQL text lives in `src/dashboard/dashboard_queries.sql`. Each `-- QUERY N` block is a **separate** Databricks SQL query. Do not run the whole file as one notebook.

This project uses **Unity Catalog**. That is a full Databricks workspace with SQL, not classic Community Edition (CE). CE has no Unity Catalog and no `ecommerce.medallion` tables. Follow **Path A** for this repo. Path B notes CE limits if an evaluator only has CE.

---

## Path A — Databricks SQL (this project)

### 1. Confirm compute and data

1. Open **SQL Editor** (left nav: SQL, or **New** → **Query**).
2. Attach a **SQL warehouse** (Serverless or Pro). Dashboards do not use the all-purpose cluster.
3. Run a smoke test:

```sql
SELECT COUNT(*) FROM ecommerce.medallion.sales_by_product;
```

**Screenshot placeholder:** SQL Editor with warehouse selected and a non-zero count.

If this fails, re-run `src/gold/create_gold_tables.py` on the all-purpose cluster, then come back here.

### 2. Save the dashboard queries

For each block in `dashboard_queries.sql` (queries **1–9** only):

1. **New** → **Query**.
2. Paste one `SELECT` only (no `-- QUERY` header required).
3. **Run**.
4. **Save** with the query title, for example `Dash - Top 10 products`.
5. Optionally set the default visualization (bar, pie, line, counter, table) using **Add visualization**.

**Screenshot placeholder:** Query 1 results as a horizontal bar chart of product names vs `total_revenue`.

| Query | Save name | Chart type | X / slice | Y / value |
|---|---|---|---|---|
| 1 | Top 10 products by revenue | Bar | `product_name` | `total_revenue` |
| 2 | Customer revenue buckets | Bar | `revenue_bucket` | `customer_count` |
| 3 | Value segmentation | Pie | `segment_type` | `customer_count` (or `total_revenue`) |
| 4 | Data quality summary | Table | — | all columns |
| 5 | KPI completed revenue | Counter | — | `completed_revenue` (add extra counters for orders / AOV) |
| 6 | Daily revenue | Line | `order_date` | `total_revenue` |
| 7 | Weekly revenue | Line | `week_start_date` | `total_revenue` |
| 8 | Revenue by category | Bar | `category` | `total_revenue` |
| 9 | Top 10 customers | Bar or table | `customer_name` | `total_revenue` |

**Screenshot placeholder:** Visualization editor showing chart type, X column, and Y column for Query 1.

### 3. Create the dashboard

**Legacy SQL Dashboard**

1. **New** → **Dashboard** (SQL Dashboards).
2. Name it `E-commerce Medallion — Gold`.
3. **Add** → pick each saved query / visualization.
4. Arrange widgets:
   - Row 1: KPI counters (Query 5)
   - Row 2: top products (1) + category mix (8)
   - Row 3: daily line (6) + weekly line (7)
   - Row 4: value pie (3) + revenue buckets (2)
   - Row 5: top customers (9) + quality table (4)

**Lakeview / AI/BI Dashboard** (newer workspaces)

1. **New** → **Dashboard**.
2. **Add data** → existing SQL queries, or paste SQL as datasets.
3. Add widgets from those datasets.
4. Publish and share.

**Screenshot placeholder:** Full dashboard canvas with KPIs on top, charts in a grid, quality table at the bottom.

### 4. Filters

Add dashboard-level filters and map them to queries that have the column.

| Filter | Type | Widget / column | Queries it should apply to |
|---|---|---|---|
| Category | Dropdown, multi-select | `sales_by_product.category` | 1, 8, 12 |
| Order date | Date range | `sales_daily_trends.order_date` | 6 (and 5 if you filter daily then aggregate) |
| Week start | Date range | `sales_weekly_trends.week_start_date` | 7 |
| Value segment | Dropdown | `customer_segmentation.segment_type` | 3 |
| Marketing segment | Dropdown | `revenue_by_customer.customer_segment` | 9 |

**How to add (legacy SQL Dashboard)**

1. Open the dashboard → **Add** → **Filter**.
2. Choose **Query based dropdown** or **Date range picker**.
3. Query-based category example:

```sql
SELECT DISTINCT category
FROM ecommerce.medallion.sales_by_product
ORDER BY category
```

4. On each chart, open **Widget settings** → **Parameters / filters** and bind `category` (or `order_date`) to the filter.

**How to add (Lakeview)**

1. **Add filter** → **Single value** or **Date range**.
2. Connect the filter field to the matching dataset column (`category`, `order_date`, `week_start_date`, `segment_type`, `customer_segment`).

Date-range filters only work on queries that **select a date column**. Product and customer Gold tables are snapshots; they have no order-date grain. Use Query 6/7 for time filters. Do not expect a category filter to change the quality table.

**Screenshot placeholder:** Filter bar with Category, Date range, and Segment dropdowns above the charts; one bar chart updating after a category is selected.

### 5. Refresh and share

1. Warehouse must be **running** when viewers open the dashboard.
2. Optional: schedule the warehouse + dashboard refresh after Gold jobs.
3. Share the dashboard with the evaluator account (**Can view**).

**Screenshot placeholder:** Share dialog and a scheduled refresh (if enabled).

---

## Path B — Databricks Community Edition (limited)

Classic Community Edition **cannot** host this project as-is:

| Feature this repo uses | Community Edition |
|---|---|
| Unity Catalog `ecommerce.medallion.*` | Not available |
| SQL warehouse + SQL Dashboard | Not available (notebooks only) |
| Volumes `/Volumes/ecommerce/...` | Not available |

If you only have CE, you can still **demo charts in a notebook**:

1. Attach a CE cluster.
2. Paste one Gold `SELECT` per cell (you would first need local/Hive tables, not this UC layout).
3. `display(spark.sql("""..."""))` and choose bar/pie/line.

That is **not** a SQL Dashboard. For the evaluation, use the workspace where Bronze/Silver/Gold already ran (Path A).

---

## Suggested layout (what to capture)

1. **Cover / KPI row** — completed revenue, completed orders, AOV, items sold.
2. **Product performance** — top 10 bar + category bar.
3. **Time** — daily revenue line + weekly growth.
4. **Customers** — value-segment pie, revenue buckets, top 10 customers.
5. **Quality** — `quality_metrics` table (Query 4).

Minimum for the project brief is **three visualizations**. This dashboard uses **queries 1–9** only.

---

## Troubleshooting

- Empty charts: Gold job not run, or warehouse cannot see catalog `ecommerce`.
- `TABLE_OR_VIEW_NOT_FOUND`: wrong catalog/schema; use `ecommerce.medallion.<table>`.
- Filter does nothing: widget is not bound, or the query has no matching column.
- Quality `pass_rate` is `pass_rate_pct` in the table; the dashboard query aliases it as `pass_rate`.
- Histogram buckets are labeled `1. $0-100` so sort order is numeric, not alphabetical.
