# Databricks notebook source
"""Orchestrate Bronze ingest for customers, orders, and products.

Runs the three ingest jobs in sequence, times each one, and prints a
summary table. A failure in one job is logged; remaining jobs still run.

Overall status:
    SUCCESS — all three succeeded
    PARTIAL — at least one succeeded and at least one failed
    FAILED  — all three failed
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

LOGGER = logging.getLogger("bronze.ingest_all")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

BRONZE_DIR = Path(__file__).resolve().parent
INGEST_JOBS = (
    ("customers", "01_ingest_customers.py", "ingest_customers"),
    ("orders", "02_ingest_orders.py", "ingest_orders"),
    ("products", "03_ingest_products.py", "ingest_products"),
)


@dataclass
class IngestResult:
    """Outcome of one Bronze ingest job.

    Attributes:
        table: Entity name (customers, orders, products).
        rows_ingested: Rows written, or 0 when the job failed.
        duration_s: Wall-clock seconds for the job.
        status: ``SUCCESS`` or ``FAILED``.
        error: Exception message when failed, else None.
    """

    table: str
    rows_ingested: int
    duration_s: float
    status: str
    error: str | None = None


def _load_ingest_module(module_name: str, filename: str) -> ModuleType:
    """Load an ingest script by file path (names start with digits).

    Args:
        module_name: Unique loader name (valid Python identifier).
        filename: File under ``src/bronze/``.

    Returns:
        Loaded module containing the ingest function.

    Raises:
        FileNotFoundError: If the script is missing.
        ImportError: If the module cannot be executed.
    """
    path = BRONZE_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Ingest script not found: {path}")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load ingest script: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _run_one_ingest(table: str, filename: str, func_name: str) -> IngestResult:
    """Run one ingest function and capture rows, duration, and status.

    Args:
        table: Entity label for the summary table.
        filename: Ingest script filename.
        func_name: Function to call on the loaded module.

    Returns:
        ``IngestResult`` for this job (never raises to the caller).
    """
    started = time.perf_counter()
    try:
        module = _load_ingest_module(f"bronze_{table}_ingest", filename)
        ingest_fn = getattr(module, func_name)
        LOGGER.info("Starting Bronze ingest for %s", table)
        df = ingest_fn()
        rows = int(df.count())
        duration_s = time.perf_counter() - started
        LOGGER.info(
            "Finished Bronze ingest for %s: %s rows in %.3fs",
            table,
            rows,
            duration_s,
        )
        return IngestResult(
            table=table,
            rows_ingested=rows,
            duration_s=duration_s,
            status="SUCCESS",
        )
    except Exception as exc:
        duration_s = time.perf_counter() - started
        LOGGER.exception(
            "Bronze ingest failed for %s after %.3fs: %s",
            table,
            duration_s,
            exc,
        )
        return IngestResult(
            table=table,
            rows_ingested=0,
            duration_s=duration_s,
            status="FAILED",
            error=str(exc),
        )


def _overall_status(results: list[IngestResult]) -> str:
    """Map per-job results to SUCCESS, PARTIAL, or FAILED.

    Args:
        results: One result per ingest job.

    Returns:
        Overall status string.
    """
    success_count = sum(1 for item in results if item.status == "SUCCESS")
    if success_count == len(results):
        return "SUCCESS"
    if success_count == 0:
        return "FAILED"
    return "PARTIAL"


def _print_summary(
    results: list[IngestResult],
    overall: str,
    total_duration_s: float,
) -> None:
    """Print the ingest summary table requested for the evaluation.

    Args:
        results: Per-table ingest outcomes.
        overall: Roll-up status.
        total_duration_s: End-to-end wall-clock seconds.
    """
    header = (
        f"| {'Table':<10} | {'Rows Ingested':>13} | "
        f"{'Duration (s)':>12} | {'Status':<7} |"
    )
    divider = (
        f"|{'-' * 12}|{'-' * 15}|{'-' * 14}|{'-' * 9}|"
    )
    lines = [
        "",
        "========== Bronze ingest summary ==========",
        header,
        divider,
    ]
    for item in results:
        lines.append(
            f"| {item.table:<10} | {item.rows_ingested:>13,} | "
            f"{item.duration_s:>12.1f} | {item.status:<7} |"
        )
    lines.extend(
        [
            divider,
            f"Overall status: {overall}",
            f"Total duration (s): {total_duration_s:.1f}",
            "==========================================",
            "",
        ]
    )
    print("\n".join(lines))


def ingest_all() -> str:
    """Run customers, orders, then products Bronze ingest.

    Returns:
        Overall status: ``SUCCESS``, ``PARTIAL``, or ``FAILED``.
    """
    wall_start = time.perf_counter()
    start_ts = time.strftime("%Y-%m-%d %H:%M:%S")
    LOGGER.info("Bronze ingest_all started at %s", start_ts)

    results: list[IngestResult] = []
    for table, filename, func_name in INGEST_JOBS:
        results.append(_run_one_ingest(table, filename, func_name))

    total_duration_s = time.perf_counter() - wall_start
    end_ts = time.strftime("%Y-%m-%d %H:%M:%S")
    overall = _overall_status(results)
    LOGGER.info(
        "Bronze ingest_all ended at %s (duration=%.3fs, status=%s)",
        end_ts,
        total_duration_s,
        overall,
    )
    _print_summary(results, overall, total_duration_s)
    return overall


if __name__ == "__main__":
    ingest_all()
