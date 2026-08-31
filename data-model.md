# Data Model

Unity Catalog: catalog `ecommerce`, schema `medallion`.  
Bronze uses explicit typed schemas from `src/bronze/schemas.py`. Silver retains
those source types and adds quality flags. Gold uses business aggregation types.  
`ingestion_timestamp` / `source_file_name` are pipeline metadata (not in the CSVs).

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
| `ingestion_timestamp` | `TIMESTAMP` | NO | Time the Bronze write ran. |
| `source_file_name` | `STRING` | NO | Source file name or Volume path of `customers.csv`. |

Grain: one row per CSV record (not necessarily unique on `customer_id`). Expected ~10,010 rows if 10 duplicate-key extra rows are appended.

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
| `ingestion_timestamp` | `TIMESTAMP` | NO | Time the Bronze write ran. |
| `source_file_name` | `STRING` | NO | Source file name or Volume path of `orders.csv`. |

Grain: one row per CSV record. Expected ~100,020 rows with 20 extra duplicate-key rows.

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
| `ingestion_timestamp` | `TIMESTAMP` | NO | Time the Bronze write ran. |
| `source_file_name` | `STRING` | NO | Source file name or Volume path of `products.csv`. |

Grain: one row per CSV record (~500 rows).

---

## Silver Layer Tables

Storage: managed Unity Catalog Delta tables.  
Each entity table = **all Bronze rows** (same count) + quality columns.

Shared quality columns (all three entity tables):

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `completeness_check` | `STRING` | NO | `PASS` or `FAIL`. |
| `uniqueness_check` | `STRING` | NO | `PASS` or `FAIL`. All members of a duplicate key fail. |
| `type_check` | `STRING` | NO | `PASS` or `FAIL` (parse, domain, negative qty, amount identity, etc.). |
| `referential_check` | `STRING` | NO | `PASS` / `FAIL` on orders; `N/A` on customers and products. |
| `quality_check_result` | `STRING` | NO | `PASS` if all applicable checks pass; else pipe-delimited fail tokens, e.g. `COMPLETENESS_FAIL\|REFERENTIAL_INTEGRITY_FAIL`. |

### `ecommerce.medallion.customers_silver`

All `customers_bronze` columns **plus** the shared quality columns.

**Completeness required (non-null / non-blank):** `customer_id`, `customer_name`, `email`.  
**Uniqueness key:** `customer_id`.  
**Type / domain (when present):** email pattern; `signup_date` parses as date; `customer_segment` in `Premium|Standard|Basic`; `lifetime_value` numeric and ≥ 0.

### `ecommerce.medallion.orders_silver`

All `orders_bronze` columns **plus** the shared quality columns.

**Completeness required:** `order_id`, `customer_id`, `product_id`, `order_date`, `quantity`, `unit_price`, `total_amount`, `order_status`. `payment_date` is **not** required.  
**Uniqueness key:** `order_id`.  
**Type / domain:** dates parse (calendar, no TZ shift); `quantity` integer **> 0**; `unit_price` / `total_amount` numeric ≥ 0; `total_amount = quantity * unit_price` after successful parse; `order_status` in `Pending|Completed|Cancelled`; if status is `Completed`, `payment_date` must be present and parseable.  
**Referential:** non-null `customer_id` exists on at least one `customers` row; non-null `product_id` exists on at least one `products` row.

### `ecommerce.medallion.products_silver`

All `products_bronze` columns **plus** the shared quality columns.

**Completeness required:** `product_id`, `product_name`.  
**Uniqueness key:** `product_id`.  
**Type / domain:** `price` / `cost` numeric ≥ 0; `stock_quantity` / `reorder_level` integer ≥ 0.

### `ecommerce.medallion.quality_metrics`

Storage: managed Unity Catalog Delta table.  
Grain: one row per `(batch_timestamp, table_name, check_name)`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `batch_timestamp` | `TIMESTAMP` | NO | Pipeline run timestamp. |
| `table_name` | `STRING` | NO | `customers_silver`, `orders_silver`, or `products_silver`. |
| `check_name` | `STRING` | NO | `completeness`, `uniqueness`, `type_validation`, or `referential_integrity`. |
| `total_rows` | `BIGINT` | NO | Rows scored for that check. |
| `passed` | `BIGINT` | NO | Rows with that check = `PASS`. |
| `failed` | `BIGINT` | NO | Rows with that check = `FAIL`. |
| `pass_rate_%` | `DOUBLE` | YES | `100.0 * passed / total_rows`. |
| `not_applicable_count` | `BIGINT` | NO | Rows with `N/A` (referential on dimensions). |
| `threshold` | `DOUBLE` | YES | Max allowed fail rate (0.01 completeness, 0 uniqueness, 0.001 referential; null for type). |
| `threshold_met` | `BOOLEAN` | YES | Whether `pass_rate_%` meets the documented bar. |

---

## Gold Layer Tables

Storage: managed Unity Catalog Delta tables.  
Population: Silver **PASS** orders with `order_status = 'Completed'`, joined to PASS customers/products. Full **overwrite** each run.

### `ecommerce.medallion.sales_by_product`

Grain: `product_id`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `product_id` | `STRING` | NO | Product natural key. |
| `product_name` | `STRING` | YES | From Silver products. |
| `category` | `STRING` | YES | From Silver products. |
| `order_count` | `BIGINT` | NO | Distinct completed PASS orders for the product. |
| `units_sold` | `BIGINT` | NO | Sum of `quantity`. |
| `gross_revenue` | `DECIMAL(18,2)` | NO | Sum of `total_amount`. |
| `total_cogs` | `DECIMAL(18,2)` | NO | Sum of `quantity * cost`. |
| `gross_margin` | `DECIMAL(18,2)` | NO | `gross_revenue - total_cogs`. |
| `avg_unit_price` | `DECIMAL(18,2)` | YES | `gross_revenue / units_sold` when units > 0. |
| `ingestion_timestamp` | `TIMESTAMP` | NO | Gold build time. |

### `ecommerce.medallion.revenue_by_customer`

Grain: `customer_id`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `customer_id` | `STRING` | NO | Customer natural key. |
| `customer_name` | `STRING` | YES | From Silver customers. |
| `country` | `STRING` | YES | From Silver customers. |
| `customer_segment` | `STRING` | YES | `Premium` / `Standard` / `Basic`. |
| `completed_order_count` | `BIGINT` | NO | Count of completed PASS orders. |
| `units_sold` | `BIGINT` | NO | Sum of `quantity`. |
| `gross_revenue` | `DECIMAL(18,2)` | NO | Sum of `total_amount`. |
| `avg_order_value` | `DECIMAL(18,2)` | YES | `gross_revenue / completed_order_count` when count > 0. |
| `lifetime_value` | `DECIMAL(18,2)` | YES | CSV LTV (may differ from `gross_revenue`). |
| `ingestion_timestamp` | `TIMESTAMP` | NO | Gold build time. |

### `ecommerce.medallion.customer_segmentation`

Grain: `customer_segment`.

| Column | Data type | Nullable | Description |
| --- | --- | --- | --- |
| `customer_segment` | `STRING` | NO | `Premium`, `Standard`, or `Basic`. |
| `customer_count` | `BIGINT` | NO | Distinct customers in `revenue_by_customer` for the segment. |
| `completed_order_count` | `BIGINT` | NO | Sum of completed orders in the segment. |
| `total_revenue` | `DECIMAL(18,2)` | NO | Sum of `gross_revenue`. |
| `avg_lifetime_value` | `DECIMAL(18,2)` | YES | Average CSV LTV. |
| `avg_computed_revenue` | `DECIMAL(18,2)` | YES | Average `gross_revenue` per customer. |
| `pct_of_revenue` | `DOUBLE` | YES | Segment revenue / all-segment revenue. |
| `ingestion_timestamp` | `TIMESTAMP` | NO | Gold build time. |

### Gold type mapping (from Silver STRING)

| Source field | Gold / metric type |
| --- | --- |
| `quantity` | `INT` / summed as `BIGINT` |
| `unit_price`, `total_amount`, `price`, `cost`, `lifetime_value` | `DECIMAL(18,2)` |
| `order_date`, `signup_date`, `payment_date` | Not stored on Gold grains above; parsed in Silver type check as `DATE` (calendar, no timezone conversion) |
