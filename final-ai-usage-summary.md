# Final AI Usage Summary

Primary tool: **Cursor**. Prompt logs: `ai-prompts/`. Workflow: `tool-workflow.md`. Accuracy is how often the first useful draft matched Databricks Unity Catalog after my review (not “perfect on first token”).

| Phase | # Prompts | Key Decisions Made | AI Accuracy | My Modifications |
| --- | --- | --- | --- | --- |
| Data Gen | 4 | Faker + pandas, seed 42; plant nulls/dupes/orphans **in place** (10K/100K/500 rows); keep CSVs in git for submission | High for generator and validator; Medium on gitignore (ignored CSVs at first) | Restored `data/*.csv` tracking; documented extra orphans (157) from reused customer ids 1–10 |
| Bronze | 9 | Explicit typed schemas; shared `ingestion.py`; managed `bronze_*` via `saveAsTable`; notebook header only on jobs | Medium until architecture split | Removed Volume `LOCATION`; notebook vs library split; pass `spark` into modules; all-purpose cluster not SQL warehouse |
| Silver | 7 | Four checks only; flag never drop; keep-first `row_number`; business rules → `TYPE_VALIDATION_FAIL`; `quality_metrics` per field | High on check mechanics | `stock_quantity >= 0`; products completeness always PASS; kept `order_before_signup` and documented ~64% orders PASS |
| Gold | 5 | PASS + Completed KPIs; extra daily/weekly tables; value segments (High-Value / Repeat / One-Time / Inactive / Other) | High on SQL; Medium vs original “3 tables only” brief | Orchestrator `create_gold_tables.py`; Cancelled counts on trends; `Other` bucket for unmatched customers |
| Dashboard | 2 | Databricks SQL warehouse; queries 1–9; Lakeview datasets from pasted SQL | Medium (wrong table aliases in first prompt; CE vs UC) | Forced `ecommerce.medallion.*`; dropped extra queries; warehouse + screenshots + `.lvdash.json` export |
| Testing | 3 | Local CSV validator; Databricks row-count equality; metrics vs planted list | High | Explained planted 50 orphans vs observed customer orphans; `threshold_met = false` is not a job fail |
| Debugging | 6 | Cluster vs warehouse; import path; notebook import ban; managed tables vs Volume | High once errors were pasted | See `debugging-notes.md` and `ai-prompts/debugging.md`; push to `master` not `main` |
| Docs | 8 | Requirements, design, data model, DQ strategy, README, candidate/tool files | Medium (docs lagged as-built code) | Rewrote docs for typed Bronze, keep-first uniqueness, real Gold columns, dashboard 1–9 |

**Prompt counts** follow headings in `ai-prompts/` (including documented iterations), plus later review/debug chats for dashboard screenshots and design-doc alignment.

**Totals (approx.):** ~40 recorded prompts/iterations. I accepted structure and Spark patterns from Cursor, then changed storage (managed UC), Databricks file roles, uniqueness keep-first, Gold grains, and anything that failed on the workspace.

**What I did not let AI do:** put secrets in the repo, delete Silver “bad” rows, append on re-run, or use `eval` / dynamic SQL from unsanitized strings.
