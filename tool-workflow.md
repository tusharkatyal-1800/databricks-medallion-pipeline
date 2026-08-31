# AI Workflow Foundation (Part A)

## Primary AI Tool

Cursor (this repo). Prompts and evaluations live in `ai-prompts/`. Rules live in `.cursorrules` (copy under `tool-specific/cursor-workflow/`).

## Project Context Setup

Asked Cursor for `.cursorrules` first: Unity Catalog, managed Delta tables, Volume for raw CSVs only, PEP 8, ANSI Spark SQL, flag-not-delete, overwrite re-runs. Kept that file loaded for the rest of the work.

## AI for Requirement Analysis

Prompts in `ai-prompts/documentation.md` produced `requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`, and `tool-specific/cursor-workflow/task-breakdown.md`. Reviewed and kept UC three-level names plus PASS+Completed for Gold.

## AI for Pipeline Design

Bronze = ingest only. Silver = four checks plus optional extra business rules folded into type validation. Gold = aggregations from Silver PASS. Dashboard = Databricks SQL on Gold. Rejected DBFS/FileStore and tables with `LOCATION` inside `/Volumes`.

## AI for Code Generation

Layer-by-layer prompts: data gen → Bronze notebooks → Silver helpers + `create_silver_tables.py` → Gold `.sql` + `create_gold_tables.py` → `dashboard_queries.sql`. Databricks jobs start with `# Databricks notebook source`; importable modules do not.

## Validating AI-Generated Code

Read diffs against `.cursorrules`. Ran `py_compile` locally where useful. Confirmed Databricks logs: Bronze 10K/100K/500 SUCCESS; Silver same row counts; Gold SUCCESS (500 / 8,782 / 1,096 / 158 / 5).

## AI for Testing & Validation

Used `validate_generated_data.py` on planted issues. Compared Silver metrics to planted nulls/duplicates/orphans. Explained `threshold_met = false` as expected SLI misses, not job failure. `order_before_signup` is extra and cuts Gold PASS volume.

## AI for Debugging

Bronze first runs failed (SQL warehouse, notebook vs module import, UC table vs Volume). Captured in `ai-prompts/debugging.md`. Fix: all-purpose cluster, shared `ingestion.py` / quality modules, `saveAsTable` managed tables.

## AI for Data Quality Checks

Completeness, uniqueness (window `row_number`), type/business, referential (LEFT ANTI JOIN). `quality_check_result` = `PASS` or ordered `COMPLETENESS_FAIL|UNIQUENESS_FAIL|TYPE_VALIDATION_FAIL|REFERENTIAL_INTEGRITY_FAIL`. Rows never dropped.

## Security & Responsible AI

No secrets, tokens, or workspace URLs with credentials in code. No `eval` or string-built SQL from user input. Did not log passwords. GitHub remote uses account auth, not pasted tokens in the repo.

## Reusable Workflow

1. Update/confirm `.cursorrules`  
2. Short prompt + paste into `ai-prompts/`  
3. Generate smallest files  
4. Review against rules  
5. Run on Databricks (cluster for `.py`, warehouse for SQL/dashboard)  
6. Paste SUCCESS log + evaluation notes  

## Lessons Learned

- Databricks notebooks cannot `import` files that start with `# Databricks notebook source`.  
- SQL warehouses cannot run Python notebooks.  
- UC tables cannot use a Volume path as `LOCATION`.  
- Extra join rules (signup vs order date) can dominate Gold if the generator does not plant them.  
- Lakeview dashboards often need SQL pasted as datasets; saved SQL Editor queries may not show on the canvas.
