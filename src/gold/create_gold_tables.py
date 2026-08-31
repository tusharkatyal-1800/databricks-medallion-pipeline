# Databricks notebook source
"""Run all Gold SQL aggregations and write managed Unity Catalog tables."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path

try:
    from src.common.config import ensure_unity_storage
except ImportError:
    from common.config import ensure_unity_storage

LOGGER = logging.getLogger("gold.create_gold_tables")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

COMMAND_SPLIT = re.compile(r"^\s*--\s*COMMAND\s+-+", re.MULTILINE)
CREATE_TABLE_NAME = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+TABLE\s+([`\w.]+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GoldResult:
    """One Gold table result for the orchestration summary."""

    table: str
    rows: int
    duration_s: float
    status: str
    error: str | None = None


def resolve_gold_sql_dir() -> Path:
    """Locate ``src/gold`` so ``*.sql`` files can be read on Databricks.

    Returns:
        Directory that contains ``01_sales_by_product.sql``.

    Raises:
        FileNotFoundError: If no candidate directory contains the SQL files.
    """
    candidates: list[Path] = []
    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        candidates.append(
            (Path("/Workspace") / notebook_path.lstrip("/")).parent
        )
    except Exception:
        LOGGER.info("Notebook path is unavailable; trying local gold folders")

    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parent)

    cwd = Path.cwd()
    candidates.extend(
        [
            cwd / "src" / "gold",
            cwd,
            cwd / "gold",
        ]
    )

    for folder in candidates:
        if (folder / "01_sales_by_product.sql").is_file():
            LOGGER.info("Gold SQL directory: %s", folder)
            return folder

    raise FileNotFoundError(
        "Could not find Gold SQL files. Sync src/gold/*.sql next to "
        "create_gold_tables.py or run from the repo root."
    )


def list_gold_sql_files(gold_dir: Path) -> list[Path]:
    """Return Gold SQL scripts in numeric file-name order.

    Args:
        gold_dir: Directory that holds ``01_*.sql`` through ``04_*.sql``.

    Returns:
        Sorted SQL file paths (excludes this Python orchestrator).
    """
    files = sorted(
        path
        for path in gold_dir.glob("*.sql")
        if path.is_file()
    )
    if not files:
        raise FileNotFoundError(f"No .sql files found in {gold_dir}")
    LOGGER.info("Found %s Gold SQL scripts", len(files))
    return files


def split_sql_statements(sql_text: str) -> list[str]:
    """Split a Databricks SQL notebook into executable statements.

    Args:
        sql_text: Raw contents of a ``.sql`` notebook file.

    Returns:
        Non-empty SQL statements with notebook cell markers removed.
    """
    statements: list[str] = []
    for part in COMMAND_SPLIT.split(sql_text):
        lines = []
        for line in part.splitlines():
            stripped = line.strip()
            if stripped.startswith("-- Databricks notebook source"):
                continue
            lines.append(line)
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def created_table_name(sql: str) -> str | None:
    """Return the three-level table name from a CREATE OR REPLACE statement.

    Args:
        sql: One SQL statement.

    Returns:
        Table name, or None if the statement does not create a table.
    """
    match = CREATE_TABLE_NAME.search(sql)
    if not match:
        return None
    return match.group(1).replace("`", "")


def execute_sql_file(sql_path: Path) -> list[GoldResult]:
    """Execute every statement in one Gold SQL file and count written rows.

    Args:
        sql_path: Path to a Gold ``.sql`` notebook.

    Returns:
        One result per created table (or one FAILED row if the file errors).
    """
    started = time.perf_counter()
    LOGGER.info("Executing Gold SQL %s", sql_path.name)
    try:
        statements = split_sql_statements(sql_path.read_text(encoding="utf-8"))
        if not statements:
            raise ValueError(f"{sql_path.name} contains no SQL statements")

        results: list[GoldResult] = []
        for statement in statements:
            stmt_started = time.perf_counter()
            table_name = created_table_name(statement) or sql_path.stem
            try:
                spark.sql(statement)
                duration_s = time.perf_counter() - stmt_started
                rows = 0
                if created_table_name(statement):
                    rows = int(spark.table(table_name).count())
                LOGGER.info(
                    "Wrote %s (%s rows) in %.3f seconds",
                    table_name,
                    rows,
                    duration_s,
                )
                results.append(
                    GoldResult(table_name, rows, duration_s, "SUCCESS")
                )
            except Exception as exc:
                duration_s = time.perf_counter() - stmt_started
                LOGGER.exception("Failed Gold statement in %s", sql_path.name)
                results.append(
                    GoldResult(
                        table_name,
                        0,
                        duration_s,
                        "FAILED",
                        str(exc),
                    )
                )
                raise
        return results
    except Exception as exc:
        duration_s = time.perf_counter() - started
        if "results" in locals() and results and results[-1].status == "FAILED":
            return results
        LOGGER.exception("Failed Gold SQL file %s", sql_path.name)
        return [
            GoldResult(sql_path.stem, 0, duration_s, "FAILED", str(exc))
        ]


def get_overall_status(results: list[GoldResult]) -> str:
    """Classify the Gold run from per-table statuses.

    Args:
        results: Table-level results.

    Returns:
        SUCCESS, PARTIAL, or FAILED.
    """
    statuses = {row.status for row in results}
    if statuses == {"SUCCESS"}:
        return "SUCCESS"
    if "SUCCESS" in statuses:
        return "PARTIAL"
    return "FAILED"


def print_summary(
    results: list[GoldResult],
    overall_status: str,
    total_duration_s: float,
) -> None:
    """Print the Gold orchestration summary table.

    Args:
        results: Per-table results.
        overall_status: Rollup status.
        total_duration_s: Wall time for all SQL files.
    """
    print("\n========== Gold aggregation summary ==========")
    print(f"| {'Gold Table':<28} | {'Rows':>10} | {'Duration (s)':>12} | {'Status':<7} |")
    print(f"|{'-' * 30}|{'-' * 12}|{'-' * 14}|{'-' * 9}|")
    for row in results:
        short_name = row.table.split(".")[-1]
        print(
            f"| {short_name:<28} | {row.rows:>10,} | "
            f"{row.duration_s:>12.1f} | {row.status:<7} |"
        )
    print(f"Overall status: {overall_status}")
    print(f"Total duration (s): {total_duration_s:.1f}")
    print("==============================================\n")


def create_gold_tables() -> str:
    """Run Gold SQL scripts in order and print row counts and timings.

    Returns:
        Overall status: SUCCESS, PARTIAL, or FAILED.

    Raises:
        FileNotFoundError: If Gold SQL files cannot be located.
    """
    started = time.perf_counter()
    LOGGER.info("Gold orchestration started")
    try:
        ensure_unity_storage(spark)
    except Exception:
        LOGGER.exception("Unity Catalog setup failed; attempting Gold SQL")

    gold_dir = resolve_gold_sql_dir()
    results: list[GoldResult] = []
    for sql_path in list_gold_sql_files(gold_dir):
        file_results = execute_sql_file(sql_path)
        results.extend(file_results)
        if any(item.status == "FAILED" for item in file_results):
            LOGGER.error("Stopping Gold run after failure in %s", sql_path.name)
            break

    duration_s = time.perf_counter() - started
    overall_status = get_overall_status(results)
    print_summary(results, overall_status, duration_s)
    LOGGER.info(
        "Gold orchestration completed in %.3f seconds with status %s",
        duration_s,
        overall_status,
    )
    return overall_status


create_gold_tables()
