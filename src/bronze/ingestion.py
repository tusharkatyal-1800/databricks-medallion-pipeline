"""Reusable Bronze ingestion logic for Databricks notebooks.

Raw CSV files are read from a Unity Catalog Volume. Curated Bronze datasets
are written as managed Unity Catalog Delta tables using three-level names.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

LOGGER = logging.getLogger(__name__)


def validate_source_file(dbutils: Any, source_path: str) -> None:
    """Validate that a source CSV exists and contains bytes.

    Args:
        dbutils: Databricks notebook utilities.
        source_path: Absolute path under ``/Volumes/...``.

    Raises:
        FileNotFoundError: If the source path does not exist.
        ValueError: If the source file is empty.
        RuntimeError: If the Volume cannot be inspected.
    """
    try:
        files = dbutils.fs.ls(source_path)
    except Exception as exc:
        LOGGER.exception("Unable to inspect source file %s", source_path)
        raise RuntimeError(
            f"Cannot access source file {source_path}. Verify the path and "
            "READ VOLUME privilege."
        ) from exc

    if not files:
        raise FileNotFoundError(f"Source CSV not found: {source_path}")
    if int(files[0].size) <= 0:
        raise ValueError(f"Source CSV is empty: {source_path}")
    LOGGER.info("Validated source file %s (%s bytes)", source_path, files[0].size)


def read_csv(
    spark: SparkSession,
    source_path: str,
    schema: StructType,
    entity_name: str,
) -> DataFrame:
    """Read a CSV from a Volume using an explicit schema.

    Args:
        spark: Databricks runtime SparkSession.
        source_path: Absolute source CSV path.
        schema: Explicit source data contract.
        entity_name: Entity label used in logs.

    Returns:
        Source DataFrame.

    Raises:
        Exception: Re-raised after logging when the read fails.
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
        LOGGER.info("Read %s CSV from %s", entity_name, source_path)
        LOGGER.info("%s source schema: %s", entity_name, df.schema.simpleString())
        return df
    except Exception:
        LOGGER.exception("Failed to read %s CSV from %s", entity_name, source_path)
        raise


def add_ingestion_metadata(
    df: DataFrame,
    source_file_name: str,
    batch_id: str,
) -> DataFrame:
    """Add standard Bronze lineage columns.

    Args:
        df: Source DataFrame.
        source_file_name: Base source filename.
        batch_id: Stable identifier for this ingestion run.

    Returns:
        DataFrame with ingestion metadata appended.
    """
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file_name))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def write_managed_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    entity_name: str,
) -> int:
    """Overwrite a managed Unity Catalog Delta table and return its row count.

    Args:
        spark: Databricks runtime SparkSession.
        df: Bronze DataFrame to persist.
        table_name: Three-level Unity Catalog table name.
        entity_name: Entity label used in logs.

    Returns:
        Number of rows read back from the managed table.

    Raises:
        Exception: Re-raised after logging when write or verification fails.
    """
    try:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        written_count = int(spark.table(table_name).count())
        LOGGER.info(
            "Wrote managed Delta table %s (%s rows)",
            table_name,
            written_count,
        )
        return written_count
    except Exception:
        LOGGER.exception(
            "Failed to write managed Delta table %s for %s",
            table_name,
            entity_name,
        )
        raise


def ingest_bronze_entity(
    spark: SparkSession,
    dbutils: Any,
    *,
    entity_name: str,
    source_path: str,
    source_file_name: str,
    table_name: str,
    schema: StructType,
) -> DataFrame:
    """Ingest one Volume CSV into a managed Bronze Delta table.

    Args:
        spark: Databricks runtime SparkSession.
        dbutils: Databricks notebook utilities.
        entity_name: Entity label used in logs.
        source_path: Source CSV path under the Volume.
        source_file_name: Base source filename for lineage.
        table_name: Managed three-level Unity Catalog table name.
        schema: Explicit source schema.

    Returns:
        Bronze DataFrame written to the table.

    Raises:
        ValueError: If the source has zero data rows or counts do not reconcile.
        Exception: For inaccessible files, invalid schemas, or failed writes.
    """
    batch_id = str(uuid.uuid4())
    LOGGER.info(
        "Starting %s Bronze ingest (batch_id=%s, source=%s, table=%s)",
        entity_name,
        batch_id,
        source_path,
        table_name,
    )

    validate_source_file(dbutils, source_path)
    source_df = read_csv(spark, source_path, schema, entity_name)
    source_count = int(source_df.count())
    LOGGER.info("%s rows read: %s", entity_name, source_count)
    if source_count == 0:
        raise ValueError(f"Source CSV has zero data rows: {source_path}")

    bronze_df = add_ingestion_metadata(source_df, source_file_name, batch_id)
    LOGGER.info("%s Bronze schema: %s", entity_name, bronze_df.schema.simpleString())
    written_count = write_managed_delta_table(
        spark,
        bronze_df,
        table_name,
        entity_name,
    )
    if written_count != source_count:
        raise ValueError(
            f"{entity_name} row-count mismatch: source={source_count}, "
            f"managed_table={written_count}"
        )

    LOGGER.info(
        "%s Bronze ingest completed successfully (%s rows)",
        entity_name,
        written_count,
    )
    return bronze_df
