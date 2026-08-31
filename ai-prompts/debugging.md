# AI Prompts — Debugging

## Debugging: Databricks Notebook Import Architecture

### Problem
Bronze orchestrator (`ingest_all.py`) failed to run on Databricks with multiple cascading errors.

### Prompt 1: Initial Execution Failure (for bronze)
**PROMPT SENT:**
"I'm running my Bronze ingest_all.py via `%run` on Databricks but getting 
`ModuleNotFoundError: No module named 'common'`. The script uses importlib 
to dynamically load ingest scripts which import from common.config. 
How should I structure Python imports for Databricks notebooks?"

**AI RESPONSE SUMMARY:**
Suggested adding `sys.path.insert(0, "/Workspace/.../src")` to make project 
modules discoverable.

**EVALUATION:**
Accepted — sys.path fix resolved the import path issue
But revealed a deeper problem...

---
### Iteration 1: Notebook vs Module Conflict
**PROMPT SENT:**
"After fixing sys.path, I now get: `NotebookImportException: Unable to import 
module. The file appears to be a notebook. Importing notebooks directly is not 
supported.` My .py files have `# Databricks notebook source` header. How do I 
structure files that need to be both runnable and importable?"

**AI RESPONSE SUMMARY:**
Explained that Databricks treats any file with `# Databricks notebook source` 
as a notebook, and notebooks cannot be imported via `import` — only executed 
via `%run`. Recommended separating files into two categories:
- **Entry points** (keep header): files you `%run` directly
- **Library modules** (no header): files that get imported

**EVALUATION:**
Accepted — This is a fundamental Databricks architecture pattern I hadn't 
considered. The distinction between "notebook" and "library" is critical.

**MY DECISION:**
- Only `ingest_all.py` (orchestrator) keeps the notebook header
- All individual ingest scripts + config + schemas become regular Python modules
- This mirrors production patterns where you have thin entry-point notebooks 
  that import from a shared library package

---

### Iteration 2: Spark Session Injection
**PROMPT SENT:**
"After removing notebook headers, imports work but ingest functions fail with 
`NameError: name 'spark' is not defined`. Databricks auto-injects `spark` into 
notebooks but not into imported modules. How to make spark available globally?"

**AI RESPONSE SUMMARY:**
Two approaches: 
1. `SparkSession.builder.getOrCreate()` in each module
2. `builtins.spark = spark` in the entry point to make it globally available

**EVALUATION:**
Accepted option 2 — `builtins.spark` is cleaner because it sets spark 
once in the orchestrator rather than repeating in every file. `getOrCreate()` 
is redundant since Databricks already has a session.

---

### Key Learning
Databricks has a strict separation between **notebooks** (interactive, `%run`-able) 
and **modules** (importable libraries). Mixing the two causes import failures. 
The production pattern is:
- **One thin notebook** as entry point (has `# Databricks notebook source`)
- **All business logic in regular Python modules** (no header, importable)
- **Use `builtins`** to inject Databricks globals (`spark`, `dbutils`) into 
  the module scope

---

## Prompt 2: Bronze ingest fix

**PROMPT SENT:**
Fix Bronze ingest for Databricks Unity Catalog.
Raw CSVs stay on the Volume. Bronze must be managed Delta tables via saveAsTable, not LOCATION inside /Volumes.
Use a shared ingestion module. Keep 01/02/03 notebooks and ingest_all as thin wrappers.
Use spark/dbutils from the runtime. No SparkSession.builder, no hardcoded workspace paths, no __file__.
If one ingest fails, log it and continue. Print a SUCCESS/PARTIAL/FAILED summary.

**AI RESPONSE SUMMARY:**
Refactored Bronze for Databricks UC:
- Volume = raw CSVs only (/Volumes/ecommerce/medallion/data/raw/)
- Bronze = managed tables: ecommerce.medallion.bronze_customers / bronze_orders / bronze_products
- Shared logic in src/bronze/ingestion.py
- 01/02/03 + ingest_all.py are notebook wrappers (Databricks notebook source header)
- ingest_all times each job, continues on failure, prints a summary table
- Removed SparkSession.builder, hardcoded /Workspace/Users/... path, and CREATE TABLE ... LOCATION '/Volumes/...'

**YOUR EVALUATION:**
We have changes the architeture for bronze layer jobs with a common ingestion.py scripts to be used for all 3 ingestion jobs.
