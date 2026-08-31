# Databricks Medallion Pipeline

E-commerce data pipeline using Bronze → Silver → Gold on Databricks with Unity Catalog.

| Layer | What it does |
|---|---|
| **Bronze** | Land raw CSVs as managed Delta tables (no cleansing) |
| **Silver** | Four quality checks; flag bad rows, never delete them |
| **Gold** | Aggregations for dashboards |

This README covers **how to run the Bronze layer** in a Databricks workspace.

---

## What Bronze produces

| Source on Volume | Managed table | Expected rows |
|---|---|---|
| `/Volumes/ecommerce/medallion/data/raw/customers.csv` | `ecommerce.medallion.bronze_customers` | 10,000 |
| `/Volumes/ecommerce/medallion/data/raw/orders.csv` | `ecommerce.medallion.bronze_orders` | 100,000 |
| `/Volumes/ecommerce/medallion/data/raw/products.csv` | `ecommerce.medallion.bronze_products` | 500 |

Raw files stay on the Volume. Bronze tables are **managed Unity Catalog Delta tables** (`saveAsTable`). Do not register tables with `LOCATION` inside `/Volumes/...` (UC tables and Volumes cannot overlap).

---

## Prerequisites

You need:

1. A Databricks workspace with **Unity Catalog**.
2. Permission to use catalog `ecommerce` (the job runs `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` if you are allowed).
3. An **all-purpose cluster** (Personal Compute is fine).  
   **Do not** attach a SQL warehouse to Python notebooks. Warehouses only execute SQL cells and will fail with:  
   `Unsupported cell during execution. SQL warehouses only support executing SQL cells.`
4. The three CSVs from this repo: `data/customers.csv`, `data/orders.csv`, `data/products.csv`.

---

## 1. Get the code into Databricks

Keep the repo folder structure intact (`src/bronze/`, `src/common/`). Imports expect that layout.

**Option A — Git folder (recommended)**

1. In Databricks: **Workspace** → **Create** → **Git folder** (or Repos).
2. Clone this GitHub repository.
3. Confirm you can see `src/bronze/ingest_all.py`.

Typical path:

`/Workspace/Users/<your-email>/databricks-medallion-pipeline/`

**Option B — Upload files**

Upload the whole project folder into your user workspace so `src/` is not flattened or renamed.

---

## 2. Create Unity Catalog objects (if they do not exist)

The notebook also creates these if your user can. You can create them once in a SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS ecommerce.medallion;
CREATE VOLUME IF NOT EXISTS ecommerce.medallion.data;
```

If `CREATE CATALOG` is blocked, ask an admin to create `ecommerce` and grant you `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `READ VOLUME`, and `WRITE VOLUME` on `ecommerce.medallion` / volume `data`.

---

## 3. Upload raw CSVs to the Volume

Catalog Explorer:

1. **Catalog** → `ecommerce` → `medallion` → volume **`data`**.
2. Open (or create) folder **`raw`**.
3. Upload from this repo:
   - `data/customers.csv`
   - `data/orders.csv`
   - `data/products.csv`

Target paths (must match exactly):

```text
/Volumes/ecommerce/medallion/data/raw/customers.csv
/Volumes/ecommerce/medallion/data/raw/orders.csv
/Volumes/ecommerce/medallion/data/raw/products.csv
```

If you previously created Bronze tables with `LOCATION '/Volumes/...'`, drop them before the first managed-table ingest:

```sql
DROP TABLE IF EXISTS ecommerce.medallion.bronze_customers;
DROP TABLE IF EXISTS ecommerce.medallion.bronze_orders;
DROP TABLE IF EXISTS ecommerce.medallion.bronze_products;
```

---

## 4. Attach the right compute

1. Open `src/bronze/ingest_all.py` in the workspace (Databricks treats it as a notebook because the first line is `# Databricks notebook source`).
2. In the compute dropdown at the top, choose an **All-purpose / Personal compute** cluster.
3. Wait until the cluster is **Running**.

Do **not** select a SQL warehouse.

---

## 5. Run Bronze ingest

1. With the cluster attached, click **Run all**.
2. The last line of `ingest_all.py` calls `ingest_all()`, so you do not need `%run` from another notebook.
3. The job:
   - ensures catalog / schema / volume
   - ingests customers, then orders, then products
   - continues if one entity fails
   - prints a summary table

You can also run the entity notebooks one at a time (`01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`). `ingest_all.py` is the usual entry point.

### Expected success log

Row counts and times will vary slightly; status should be `SUCCESS`:

```text
========== Bronze ingest summary ==========
| Table      | Rows Ingested | Duration (s) | Status  |
|------------|---------------|--------------|---------|
| customers  |        10,000 |         19.1 | SUCCESS |
| orders     |       100,000 |          6.7 | SUCCESS |
| products   |           500 |          5.8 | SUCCESS |
Overall status: SUCCESS
```

Each table keeps source columns plus `_ingestion_timestamp`, `_source_file`, and `_batch_id`. No business cleansing in Bronze.

---

## 6. Verify in SQL

You can run this in a notebook SQL cell **or** in a SQL warehouse after the Python job has finished:

```sql
SELECT 'customers' AS table_name, COUNT(*) AS n
FROM ecommerce.medallion.bronze_customers
UNION ALL
SELECT 'orders', COUNT(*)
FROM ecommerce.medallion.bronze_orders
UNION ALL
SELECT 'products', COUNT(*)
FROM ecommerce.medallion.bronze_products;
```

Expected: `10000`, `100000`, `500`.

Spot-check metadata:

```sql
SELECT * FROM ecommerce.medallion.bronze_customers LIMIT 5;
```

---

## 7. Re-run (idempotent)

Run **Run all** on `ingest_all.py` again. Bronze uses overwrite, so counts must stay 10,000 / 100,000 / 500 — not double.

---

## Troubleshooting

### `Unsupported cell during execution. SQL warehouses only support executing SQL cells.`

The notebook is attached to a **SQL warehouse**. Attach an **all-purpose cluster** and run again.

### `ModuleNotFoundError: No module named 'src'` (or `'bronze'` / `'common'`)

Databricks did not put the repo on `sys.path`. Add this as the **first** cell (use your real workspace path), then **Run all**:

```python
import sys

REPO = "/Workspace/Users/<your-email>/databricks-medallion-pipeline"
sys.path.insert(0, REPO)
sys.path.insert(0, f"{REPO}/src")
```

If the project lives under Repos, use that path instead, for example `/Workspace/Repos/<your-email>/databricks-medallion-pipeline`.

### Cannot access `/Volumes/ecommerce/medallion/data/raw/customers.csv`

- Confirm the three files were uploaded under volume `data` → `raw`.
- Confirm names are exactly `customers.csv`, `orders.csv`, `products.csv`.
- Confirm `READ VOLUME` on `ecommerce.medallion.data`.

### `NotebookImportException: ... appears to be a notebook`

Only entry-point files (`ingest_all.py`, `01_` / `02_` / `03_`) have `# Databricks notebook source`. Shared modules (`ingestion.py`, `schemas.py`, `config.py`) must **not** have that header. Do not `%run` library modules; import them.

### Zero rows or empty file errors

Re-upload the CSVs from `data/` in this repo. Files must include a header row.

---

## Optional: regenerate sample CSVs locally

The repo already includes `data/*.csv`. To regenerate on your laptop:

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
python src/data_generation/validate_generated_data.py
```

Then upload the new files to the Volume `raw/` folder again.

---

## Bronze files

| File | Role |
|---|---|
| `src/bronze/ingest_all.py` | Orchestrator notebook — run this |
| `src/bronze/01_ingest_customers.py` | Customers only |
| `src/bronze/02_ingest_orders.py` | Orders only |
| `src/bronze/03_ingest_products.py` | Products only |
| `src/bronze/ingestion.py` | Shared ingest logic (not a notebook) |
| `src/bronze/schemas.py` | Explicit CSV schemas |
| `src/common/config.py` | Volume paths and table names |
| `data/*.csv` | Sample source files |

Silver and Gold notebooks are not required for this Bronze run.
