"""Configuration Module

Centralized configuration for LAB 4 data pipeline.
Defines Unity Catalog table names for bronze and silver layers,
organized by Slowly Changing Dimension (SCD) type.

Table Structure:
    Bronze Layer: Raw streaming events (append-only)
    Silver Layer: Cleaned, deduplicated events (SCD I or II)

Configuration:
    Uses notebook query parameters (widgets) when available,
    falls back to environment variables or defaults.

Usage:
    >>> from config import tables
    >>> bronze_table = tables["I"]["bronze"]
    >>> silver_table = tables["II"]["silver"]

Author: Paweł Nowak
Date: 2026-08-10
"""

import os
from typing import Final

# I wanted so badly to have a parametrizable Python script (like widgers parameters in the notebook) :D
_dbutils = None
try:
    from IPython import get_ipython
    ipython = get_ipython()
    if ipython is not None:
        _dbutils = ipython.user_ns.get('dbutils', None)
    
    if _dbutils is None:
        from databricks.sdk.runtime import dbutils as _dbutils
except Exception:
    _dbutils = None

if _dbutils is not None:
    _dbutils.widgets.text("catalog", "workspace", "Catalog Name")
    _dbutils.widgets.text("schema", "lab_4_schema", "Schema Name")
    
    catalog_name: str = _dbutils.widgets.get("catalog")
    schema_name: str = _dbutils.widgets.get("schema")
else:
    catalog_name: str = os.getenv("CATALOG_NAME", "workspace")
    schema_name: str = os.getenv("SCHEMA_NAME", "lab_4_schema")

def create_catalog_and_schema(catalog: str, schema: str):
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

TBL_NAME_SCD_I: Final[str] = "streaming_events_SCD_I"
TBL_NAME_SCD_II: Final[str] = "streaming_events_SCD_II"

target_bronze_table_SCD_I = f"{catalog_name}.{schema_name}.bronze_{TBL_NAME_SCD_I}"
target_silver_table_SCD_I = f"{catalog_name}.{schema_name}.silver_{TBL_NAME_SCD_I}"

target_bronze_table_SCD_II = f"{catalog_name}.{schema_name}.bronze_{TBL_NAME_SCD_II}"
target_silver_table_SCD_II = f"{catalog_name}.{schema_name}.silver_{TBL_NAME_SCD_II}"

tables = {
    "I": {
        "bronze": target_bronze_table_SCD_I,
        "silver": target_silver_table_SCD_I
    },
    "II": {
        "bronze": target_bronze_table_SCD_II,
        "silver": target_silver_table_SCD_II
    }
}