"""Configuration Module

Centralized configuration for LAB 4 data pipeline.
Defines Unity Catalog table names for bronze and silver layers,
organized by Slowly Changing Dimension (SCD) type.

Table Structure:
    Bronze Layer: Raw streaming events (append-only)
    Silver Layer: Cleaned, deduplicated events (SCD I or II)

Configuration:
    Catalog and schema are passed explicitly to get_tables() at runtime.
    Falls back to environment variables or project defaults when not provided.

Usage:
    >>> from config import get_tables
    >>> tables = get_tables(catalog="my_catalog", schema="my_schema")
    >>> bronze_table = tables["I"]["bronze"]
    >>> silver_table = tables["II"]["silver"]

Author: Paweł Nowak
Date: 2026-08-10
"""

import os
from typing import Final

DEFAULT_CATALOG: Final[str] = os.getenv("CATALOG_NAME", "dbr_dev")
DEFAULT_SCHEMA: Final[str] = os.getenv("SCHEMA_NAME", "pawelnowak2004pri219")

TBL_NAME_SCD_I: Final[str] = "streaming_events_SCD_I"
TBL_NAME_SCD_II: Final[str] = "streaming_events_SCD_II"


def get_tables(catalog: str = DEFAULT_CATALOG, schema: str = DEFAULT_SCHEMA) -> dict:
    """Build and return the table name mapping for the given catalog and schema.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema (database) name within the catalog.

    Returns:
        dict with keys "I" and "II", each containing "bronze" and "silver" table names.
    """
    return {
        "I": {
            "bronze": f"{catalog}.{schema}.bronze_{TBL_NAME_SCD_I}",
            "silver": f"{catalog}.{schema}.silver_{TBL_NAME_SCD_I}",
        },
        "II": {
            "bronze": f"{catalog}.{schema}.bronze_{TBL_NAME_SCD_II}",
            "silver": f"{catalog}.{schema}.silver_{TBL_NAME_SCD_II}",
        },
    }


def create_catalog_and_schema(catalog: str = DEFAULT_CATALOG, schema: str = DEFAULT_SCHEMA) -> None:
    """Create the Unity Catalog catalog and schema if they don't exist.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema (database) name within the catalog.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")