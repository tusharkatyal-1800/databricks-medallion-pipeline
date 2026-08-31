"""Bronze ingest: orders.csv -> Delta on a Unity Catalog Volume.

Run this file as a Databricks notebook (or paste into a notebook cell).
Uses the notebook ``spark`` session and ``dbutils``; do not build a local
SparkSession.

Source: ``/Volumes/ecommerce/medallion/data/raw/orders.csv``
Target: ``/Volumes/ecommerce/medallion/data/bronze/orders``
Table: ``ecommerce.medallion.bronze_orders``
"""

from __future__ import annotations

import logging
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

try:
    from src.common.config import (
        BRONZE_DIR,
        BRONZE_ORDERS_PATH,
        BRONZE_ORDERS_TABLE,
        ORDERS_CSV_PATH,
        ensure_unity_storage,
    )
except ImportError:
    from common.config import (  # type: ignore
        BRONZE_DIR,
        BRONZE_ORDERS_PATH,
        BRONZE_ORDERS_TABLE,
        ORDERS_CSV_PATH,
        ensure_unity_storage,
    )

SOURCE_PATH = ORDERS_CSV_PATH
BRONZE_PATH = BRONZE_ORDERS_PATH
BRONZE_TABLE = BRONZE_ORDERS_TABLE
SOURCE_FILE_NAME = "orders.csv"

LOGGER = logging.getLogger("bronze.ingest_orders")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _load_orders_schema() -> StructType:
    """Import the explicit orders StructType (never inferSchema).

    Returns:
        ``ORDERS_SCHEMA`` from ``schemas.py``.

    Raises:
        ImportError: If the schema module is not on the Python path.
    """
    try:
        from src.bronze.schemas import ORDERS_SCHEMA
    except ImportError:
        try:
            from schemas import ORDERS_SCHEMA
        except ImportError as exc:
            raise ImportError(
                "Could not import ORDERS_SCHEMA. Add the repo to "
                "sys.path or run %run ./schemas in a prior notebook cell."
            ) from exc
    return ORDERS_SCHEMA


def source_csv_exists(path: str) -> bool:
    """Return True if the Volume CSV exists and is non-empty.

    Args:
        path: Full ``/Volumes/...`` path to the source file.

    Returns:
        True when ``dbutils.fs.ls`` finds a file with size > 0.

    Raises:
        FileNotFoundError: If the path is missing or size is 0.
    """
    try:
        listing = dbutils.fs.ls(path)
    except Exception as exc:
        LOGGER.exception("dbutils.fs.ls failed for %s", path)
        raise FileNotFoundError(f"Source CSV not found: {path}") from exc
    if not listing:
        raise FileNotFoundError(f"Source CSV not found: {path}")
    size = int(listing[0].size)
    if size <= 0:
        raise FileNotFoundError(f"Source CSV is empty: {path}")
    LOGGER.info("Found source file %s (%s bytes)", path, size)
    return True


def read_orders_csv(source_path: str, schema: StructType) -> DataFrame:
    """Read orders CSV with the explicit Bronze schema.

    Args:
        source_path: Volume path to ``orders.csv``.
        schema: Explicit ``ORDERS_SCHEMA`` (no inference).

    Returns:
        Spark DataFrame of source columns only.

    Raises:
        Exception: Re-raised after logging if the read fails.
    """
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(schema)
            .load(source_path)
        )
        LOGGER.info("Read orders CSV from %s", source_path)
        LOGGER.info("Source schema:\n%s", df.schema.simpleString())
        return df
    except Exception:
        LOGGER.exception("Failed to read orders CSV from %s", source_path)
        raise


def add_ingest_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Append lineage columns without rewriting source fields.

    Args:
        df: Raw orders DataFrame.
        batch_id: UUID for this overwrite run.

    Returns:
        DataFrame plus ``_ingestion_timestamp``, ``_source_file``,
        ``_batch_id``.
    """
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(SOURCE_FILE_NAME))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def write_orders_bronze(df: DataFrame, bronze_path: str) -> None:
    """Overwrite the orders Bronze Delta location on the Volume.

    Args:
        df: Orders DataFrame including metadata columns.
        bronze_path: Delta directory under ``/Volumes/.../bronze``.

    Raises:
        Exception: Re-raised after logging if the write fails.
    """
    try:
        dbutils.fs.mkdirs(BRONZE_DIR)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )
        LOGGER.info("Wrote Delta table to %s", bronze_path)
    except Exception:
        LOGGER.exception("Failed to write Delta table to %s", bronze_path)
        raise


def register_bronze_orders_table(bronze_path: str, table_name: str) -> None:
    """Register a Unity Catalog table over the Bronze Delta Volume path.

    Args:
        bronze_path: Volume location of the Delta files.
        table_name: Three-level name ``catalog.schema.table``.

    Raises:
        Exception: Re-raised after logging if SQL registration fails.
    """
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        f"USING DELTA LOCATION '{bronze_path}'"
    )
    try:
        spark.sql(ddl)
        LOGGER.info("Ensured UC table %s at %s", table_name, bronze_path)
    except Exception:
        LOGGER.exception("Failed to register table %s", table_name)
        raise


def ingest_orders() -> DataFrame:
    """Run the full orders Bronze ingest (idempotent overwrite).

    Returns:
        The DataFrame that was written to Delta.

    Raises:
        FileNotFoundError: If the source CSV is missing or empty.
        Exception: On read, write, or table-registration failure.
    """
    ensure_unity_storage(spark)
    schema = _load_orders_schema()
    source_csv_exists(SOURCE_PATH)
    batch_id = str(uuid.uuid4())
    LOGGER.info("Starting orders Bronze ingest batch_id=%s", batch_id)

    source_df = read_orders_csv(SOURCE_PATH, schema)
    source_count = source_df.count()
    LOGGER.info("Row count after read: %s", source_count)
    if source_count == 0:
        raise ValueError(f"Source CSV has 0 data rows: {SOURCE_PATH}")

    bronze_df = add_ingest_metadata(source_df, batch_id)
    LOGGER.info("Bronze schema:\n%s", bronze_df.schema.simpleString())

    write_orders_bronze(bronze_df, BRONZE_PATH)
    register_bronze_orders_table(BRONZE_PATH, BRONZE_TABLE)

    written = spark.read.format("delta").load(BRONZE_PATH)
    written_count = written.count()
    LOGGER.info("Row count after write: %s", written_count)
    if written_count != source_count:
        raise ValueError(
            f"Bronze row count {written_count} != source count {source_count}"
        )
    LOGGER.info("Orders Bronze ingest complete (table %s)", BRONZE_TABLE)
    return bronze_df


# Databricks notebook / local driver entrypoint.
# Guarded so ingest_all.py can import without immediately running ingest.
if __name__ == "__main__":
    ingest_orders()
