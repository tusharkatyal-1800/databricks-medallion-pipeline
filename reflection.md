# Reflection

## What I Built

An e-commerce Medallion pipeline on Databricks Unity Catalog: raw CSVs on volume `ecommerce.medallion.data`, then managed Delta Bronze → Silver (four quality checks, rows flagged not deleted) → Gold aggregations → a Databricks SQL dashboard (**E-commerce Medallion**, queries 1–9).

Bronze lands 10,000 customers, 100,000 orders, and 500 products. Silver keeps those counts and writes `quality_metrics`. Gold is PASS + Completed (trend tables also count PASS Cancelled). Typical Gold sizes: `sales_by_product` 500, `revenue_by_customer` 8,782, `sales_daily_trends` 1,096, `sales_weekly_trends` 158, `customer_segmentation` 5 value buckets.

## How I Used AI (Across the Lifecycle)

Cursor was the only coding assistant. Work went layer by layer: `.cursorrules` and requirements docs, then data generation, Bronze, Silver, Gold, dashboard, README, and evaluation write-ups. Each step started with a short prompt saved under `ai-prompts/`. I reviewed diffs against Unity Catalog rules (managed tables, overwrite, flag-not-delete, PEP 8, ANSI Spark SQL) before running on Databricks. Debugging prompts for notebook vs module imports are in `ai-prompts/debugging.md`; the runbook is `debugging-notes.md`.

## What AI Helped With Most

- Boilerplate that must stay consistent: schemas, `config.py`, Google-style docstrings, logging + re-raise.
- Silver windows and anti-joins (`row_number` uniqueness, LEFT ANTI referential) without dropping rows.
- Gold Spark SQL (`CREATE OR REPLACE TABLE`, `NULLIF`, `LAG`, `DATE_TRUNC`).
- Turning Databricks errors into a clear split: notebook header vs importable library.

## What AI Got Wrong

- First drafts used Volume `LOCATION` for Delta tables. Unity Catalog rejected that; curated layers had to be **managed** `saveAsTable`.
- Early Bronze mixed `# Databricks notebook source` with `import`, which Databricks cannot do.
- Design docs lagged the code (all-STRING Bronze, extra CSV rows, Premium/Standard/Basic Gold segmentation, “flag every duplicate”). I had to align docs to keep-first uniqueness, typed Bronze, and value-based Gold segments.
- Extra rule `order_before_signup` was generated without a matching plant in the CSVs, so orders PASS rate dropped to about 64%. I kept the rule and documented it instead of deleting Silver rows.
- First `.gitignore` ignored `data/*.csv`, which we need for submission.
- `git push origin main` does not apply here; the branch is `master`.

## How I Validated AI Output

- Local: `validate_generated_data.py` (planted 460 listed issues; extra customer orphans explained by in-place id reuse).
- Databricks logs: Bronze SUCCESS 10K/100K/500; Silver same counts; Gold SUCCESS with the five table sizes above.
- SQL: `quality_check_result` tokens, `quality_metrics.threshold_met`, Gold filters PASS + Completed.
- Dashboard: warehouse queries 1–9 and screenshots, not a single static render.
- Re-run overwrite: counts did not increase.

## What I Would Improve Next

- Plant type/domain defects in the generator if the brief still wants ~700 issue instances, or drop `order_before_signup` from scoring if dates stay independent.
- Avoid uniqueness plants that reuse ids and silently delete parent keys (or remap orders when ids are overwritten).
- Add a tiny local PySpark test harness so uniqueness/completeness can be checked without a cluster.
- Keep design docs in the same PR as code so they cannot drift.

## Reusable Workflow

1. Lock platform rules in `.cursorrules` (UC names, Volume = raw only, managed Delta, flag not delete, overwrite).
2. Write a short prompt; paste it into `ai-prompts/` with evaluation notes.
3. Generate the smallest files; split Databricks **jobs** (notebook header) from **libraries** (no header).
4. Review against the rules; reject Volume table locations and `SparkSession.builder` in jobs.
5. Run: cluster for `.py`, SQL warehouse for `.sql` and the dashboard.
6. Record SUCCESS counts and any `threshold_met = false` as expected SLI misses, then update docs.
