# Data Model

Unity Catalog: catalog `ecommerce`, schema `medallion`.  
Bronze uses explicit typed schemas from `src/bronze/schemas.py`. Silver retains
those source types and adds quality flags. Gold uses business aggregation types.  
Metadata columns: `_ingestion_timestamp`, `_source_file`, `_batch_id` (not in the CSVs).

**As built (2026-08-31):** Bronze/Silver row counts are **10,000 / 100,000 / 500**. Duplicate keys were planted by **reusing ids in place**, not by appending extra CSV rows.

---

## Bronze Layer Tables

Storage: managed Unity Catalog Delta tables (no Volume table locations).

### `ecommerce.medallion.bronze_customers`

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `customer_id` | `INT` | YES | Natural key from CSV; duplicates possible. |
| `customer_name` | `STRING` | YES | Display name. |
| `email` | `STRING` | YES | Contact email; ~50 planted nulls. |
| `country` | `STRING` | YES | Country name or code as delivered. |
| `signup_date` | `DATE` | YES | Calendar signup date parsed with `yyyy-MM-dd`. |
| `customer_segment` | `STRING` | YES | Expected `Premium` / `Standard` / `Basic`; invalid values possible. |
| `lifetime_value` | `DECIMAL(10,2)` | YES | Source lifetime value. |
| `_ingestion_timestamp` | `TIMESTAMP` | NO | Time the Bronze write ran. |
| `_source_file` | `STRING` | NO | Source file name of `customers.csv`. |
| `_batch_id` | `STRING` | NO | Ingest batch identifier. |

Grain: one row per CSV record (not necessarily unique on `customer_id`). **10,000 rows.** Ten rows reuse `customer_id` 1–10 (ids 9941–9950 are overwritten in the extract).

### `ecommerce.medallion.bronze_orders`

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `order_id` | `INT` | YES | Natural key; ~20 planted duplicate ids. |
| `customer_id` | `INT` | YES | FK to customers; ~100 nulls + planted orphans. |
| `order_date` | `DATE` | YES | Order calendar date parsed with `yyyy-MM-dd`. |
| `product_id` | `INT` | YES | FK to products; ~200 nulls + ~30 orphans. |
| `quantity` | `INT` | YES | Units; negative/zero values can be quality failures. |
| `unit_price` | `DECIMAL(10,2)` | YES | Unit price. |
| `total_amount` | `DECIMAL(10,2)` | YES | Line total; may not equal qty × price. |
| `order_status` | `STRING` | YES | Expected `Pending` / `Completed` / `Cancelled`. |
| `payment_date` | `DATE` | YES | Nullable by design. |
| `_ingestion_timestamp` | `TIMESTAMP` | NO | Time the Bronze write ran. |
| `_source_file` | `STRING` | NO | Source file name of `orders.csv`. |
| `_batch_id` | `STRING` | NO | Ingest batch identifier. |

Grain: one row per CSV record. **100,000 rows.** Twenty rows reuse `order_id` 1–20.

### `ecommerce.medallion.bronze_products`

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `product_id` | `INT` | YES | Natural key. |
| `product_name` | `STRING` | YES | Product display name. |
| `category` | `STRING` | YES | Merchandise category. |
| `price` | `DECIMAL(10,2)` | YES | List price. |
| `cost` | `DECIMAL(10,2)` | YES | Unit cost. |
| `stock_quantity` | `INT` | YES | On-hand units. |
| `reorder_level` | `INT` | YES | Reorder threshold. |
| `_ingestion_timestamp` | `TIMESTAMP` | NO | Time the Bronze write ran. |
| `_source_file` | `STRING` | NO | Source file name of `products.csv`. |
| `_batch_id` | `STRING` | NO | Ingest batch identifier. |

Grain: one row per CSV record (**500 rows**).

---

## Silver Layer Tables

Storage: managed Unity Catalog Delta tables.  
Each entity table = **all Bronze rows** (same count) + quality columns.

Shared quality columns (all three entity tables):

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `completeness_check` | `STRING` | NO | `PASS` or `FAIL_NULL_{field}` (pipe-joined if several). Products: always `PASS`. |
| `uniqueness_check` | `STRING` | NO | `PASS` or `FAIL_DUPLICATE_{key}`. First `_ingestion_timestamp` row per key PASSes. |
| `type_check` | `STRING` | NO | `PASS` or `FAIL_INVALID_{rule}` (pipe-joined). Extra business rules use the same tokens. |
| `referential_check` | `STRING` | NO | `PASS` / `FAIL_ORPHAN_{field}` on orders; `N/A` on customers and products. |
| `quality_check_result` | `STRING` | NO | `PASS` if all applicable checks pass; else pipe-delimited `COMPLETENESS_FAIL\|UNIQUENESS_FAIL\|TYPE_VALIDATION_FAIL\|REFERENTIAL_INTEGRITY_FAIL`. |

### `ecommerce.medallion.customers_silver`

All `bronze_customers` columns **plus** the shared quality columns.

**Completeness required:** `email` only.  
**Uniqueness key:** `customer_id` (keep-first).  
**Type / domain (when present):** email `.*@.*\..*`; `signup_date` in range; `customer_segment` in `Premium|Standard|Basic`; `lifetime_value` ≥ 0.

### `ecommerce.medallion.orders_silver`

All `bronze_orders` columns **plus** the shared quality columns.

**Completeness required:** `customer_id`, `product_id`. `payment_date` is **not** required.  
**Uniqueness key:** `order_id` (keep-first).  
**Type / domain:** dates in range; `quantity` integer **> 0**; prices/amounts **> 0**; `total_amount` within **1% relative** of qty × price; `order_status` in `Pending|Completed|Cancelled`; Completed must have `payment_date`; extra join rule `order_before_signup` (maps to `TYPE_VALIDATION_FAIL`).  
**Referential:** non-null `customer_id` exists on at least one `customers` row; non-null `product_id` exists on at least one `products` row.

### `ecommerce.medallion.products_silver`

All `bronze_products` columns **plus** the shared quality columns.

**Completeness required:** none planted; column is always `PASS`.  
**Uniqueness key:** `product_id`.  
**Type / domain:** `price` / `cost` numeric > 0 when present; `stock_quantity` integer ≥ 0; `reorder_level` ≥ 0.

### `ecommerce.medallion.quality_metrics`

Storage: managed Unity Catalog Delta table.  
Grain: one row per `(batch_timestamp, table_name, check_name, field_checked)`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `table_name` | `STRING` | NO | `customers`, `orders`, or `products`. |
| `check_name` | `STRING` | NO | `completeness`, `uniqueness`, `type_validation`, `referential_integrity`, or `overall`. |
| `field_checked` | `STRING` | NO | Field or rule name (`email`, `customer_id`, `amount_formula`, `_all`, …). |
| `total_rows` | `INT` | NO | Silver row count for that table. |
| `applicable_rows` | `INT` | NO | Rows scored for that field. |
| `passed` | `INT` | NO | Applicable rows that passed. |
| `failed` | `INT` | NO | Applicable rows that failed. |
| `pass_rate_pct` | `DOUBLE` | NO | `100.0 * passed / applicable_rows`. |
| `threshold` | `DOUBLE` | NO | Documented pass-rate bar. |
| `threshold_met` | `BOOLEAN` | NO | Whether `pass_rate_pct` meets the bar. |
| `batch_timestamp` | `TIMESTAMP` | NO | Pipeline run timestamp. |

---

## Gold Layer Tables

Storage: managed Unity Catalog Delta tables.  
Population: Silver **PASS** orders with `order_status = 'Completed'` for product/customer KPIs (trend tables also count PASS Cancelled). Full **overwrite** each run. Actual SUCCESS counts: `sales_by_product` 500, `revenue_by_customer` 8,782, `sales_daily_trends` 1,096, `sales_weekly_trends` 158, `customer_segmentation` 5.

### `ecommerce.medallion.sales_by_product`

Grain: `product_id`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `product_id` | `INT` | NO | Product natural key. |
| `product_name` | `STRING` | YES | From Silver products. |
| `category` | `STRING` | YES | From Silver products. |
| `total_orders` | `BIGINT` | NO | Completed PASS orders for the product. |
| `total_revenue` | `DECIMAL(18,2)` | NO | Sum of `total_amount`. |
| `avg_order_value` | `DECIMAL(18,2)` | YES | Average `total_amount`. |
| `total_quantity_sold` | `BIGINT` | NO | Sum of `quantity`. |
| `profit_margin` | `DECIMAL(18,2)` | YES | `(revenue − qty×cost) / revenue * 100`. |

### `ecommerce.medallion.revenue_by_customer`

Grain: `customer_id`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `customer_id` | `INT` | NO | Customer natural key. |
| `customer_name` | `STRING` | YES | From Silver customers. |
| `customer_segment` | `STRING` | YES | CSV `Premium` / `Standard` / `Basic`. |
| `total_orders` | `BIGINT` | NO | Distinct completed PASS orders. |
| `total_revenue` | `DECIMAL(18,2)` | NO | Sum of `total_amount`. |
| `avg_order_value` | `DECIMAL(18,2)` | YES | `total_revenue / total_orders`. |
| `first_order_date` | `DATE` | YES | Min completed `order_date`. |
| `last_order_date` | `DATE` | YES | Max completed `order_date`. |
| `customer_tenure_days` | `INT` | YES | `DATEDIFF(last, first)`. |
| `lifetime_value_actual` | `DECIMAL(18,2)` | YES | Same as `total_revenue` (computed, not CSV LTV). |

No `country` column.

### `ecommerce.medallion.customer_segmentation`

Grain: `segment_type` (value buckets from `revenue_by_customer`).

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `segment_type` | `STRING` | NO | `Inactive` if last order &lt; as-of − 6 months; else `High-Value` (revenue &gt; 1000 and orders ≥ 5); `Repeat` (orders ≥ 3 and revenue ≤ 1000); `One-Time` (orders = 1); else `Other`. |
| `customer_count` | `BIGINT` | NO | Customers in the bucket. |
| `avg_revenue` | `DECIMAL(18,2)` | YES | Average `total_revenue`. |
| `total_revenue` | `DECIMAL(18,2)` | NO | Sum of `total_revenue`. |
| `avg_orders` | `DECIMAL(18,2)` | YES | Average `total_orders`. |

### `ecommerce.medallion.sales_daily_trends`

Grain: `order_date`. PASS orders only. Revenue/AOV/items from Completed; `cancelled_orders_cnt` from Cancelled. Includes `year_week`, `revenue_growth_pct`.

### `ecommerce.medallion.sales_weekly_trends`

Grain: `week_start_date` (`DATE_TRUNC('WEEK', order_date)`). Same metrics as daily.

### Gold type mapping

| Source field | Gold / metric type |
| --- | --- |
| `product_id` / `customer_id` | `INT` |
| `quantity` | summed as `BIGINT` |
| `unit_price`, `total_amount`, `price`, `cost` | `DECIMAL(18,2)` |
| `order_date` | `DATE` on trend tables and customer first/last |
