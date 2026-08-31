# Candidate Information

**Name:** Tushar Katyal  
**Role:** SE  
**Primary Technology Stack:** PySpark, SQL, Databricks  
**Primary AI Tool Used:** Cursor  
**Project Option Selected:** Data Pipeline (Medallion Architecture)  
**Assessment Start Date:** 2026-08-17  
**Submission Date:** 2026-08-31  

## Tools & Environment

- Databricks workspace with Unity Catalog (catalog `ecommerce`, schema `medallion`, volume `data`)
- Raw CSVs on `/Volumes/ecommerce/medallion/data/raw/`
- Curated Bronze, Silver, and Gold as **managed** Unity Catalog Delta tables (`ecommerce.medallion.<table>`), not Volume `LOCATION` and not DBFS/FileStore
- Languages: Python 3, PySpark, ANSI Spark SQL
- Local sample generation: pandas, numpy, faker (`requirements.txt`)
- Compute: all-purpose cluster for notebook jobs; SQL warehouse for Gold SQL checks and the dashboard
- AI: Cursor (prompts in `ai-prompts/`)
- Repo: https://github.com/tusharkatyal-1800/databricks-medallion-pipeline

## Setup Summary

1. **Local data** — Generated `data/customers.csv` (~10K), `orders.csv` (~100K), `products.csv` (500) with planted quality issues (seed 42). Validated with `src/data_generation/validate_generated_data.py`.
2. **Databricks** — Synced the Git repo into the workspace. Uploaded the three CSVs to the Volume `raw/` folder. Used Personal / all-purpose compute (not a SQL warehouse) for Python notebooks.
3. **Bronze** — Ran `src/bronze/ingest_all.py`. Result: `bronze_customers` 10,000, `bronze_orders` 100,000, `bronze_products` 500, status SUCCESS.
4. **Silver** — Ran `src/silver/create_silver_tables.py`. Flags completeness, uniqueness, type/business rules, and referential integrity; never drops rows. Wrote `customers_silver`, `orders_silver`, `products_silver`, `quality_metrics`. Bronze and Silver counts match. Extra `order_before_signup` rule lowers orders overall PASS rate (~64%); planted nulls/duplicates/orphans match the quality report.
5. **Gold** — Ran `src/gold/create_gold_tables.py` (executes `01`–`04` SQL). PASS + Completed only. Typical counts: `sales_by_product` 500, `revenue_by_customer` 8,782, `sales_daily_trends` 1,096, `sales_weekly_trends` 158, `customer_segmentation` 5. Status SUCCESS.
6. **Dashboard** — Databricks SQL dashboard **E-commerce Medallion** (queries 1–9). Export: `src/dashboard/E-commerce Medallion.lvdash.json`. Screenshots: `src/dashboard/screenshots/`. How to run: `README.md`.

End-to-end path: CSV → Volume → Bronze Delta → Silver Delta (flagged) → Gold Delta → SQL dashboard.
