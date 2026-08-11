"""
music_pipeline_setup.py
-----------------------
Shared configuration and Unity Catalog setup utilities for the music analytics pipeline.

Defines all path constants, Unity Catalog table names, and idempotent setup
helper functions used across project notebooks.

Public exports:
    catalog_name, music_schema, volume_name,
    volume_path, json_landing_path,
    music_metadata_dir, music_metadata_file,
    bronze_music_metadata_table, bronze_music_stats_table,
    spark, w, yt_api_key,
    setup_catalog, setup_schema_and_volume,
    setup_music_metadata_and_json_dirs, setup_bronze_music_metadata_table
"""

import os
from pathlib import Path
from typing import Final

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient


def _require_env(var: str) -> str:
    """Return the value of a required environment variable.

    Args:
        var: Environment variable name.

    Returns:
        The variable's string value.

    Raises:
        EnvironmentError: If the variable is not set or is empty.
    """
    value = os.environ.get(var)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set. "
            "Add it to .env (see .env.example) or set it as a cluster environment variable."
        )
    return value


def _load_env_file(env_path: Path) -> None:
    """Load key=value pairs from a .env file into os.environ.

    Skips blank lines and comments. Does not overwrite variables that are
    already set (cluster environment variables take precedence over .env).
    Silently no-ops if the file does not exist.

    Args:
        env_path: Absolute path to the .env file to load.
    """
    if not env_path.exists():
        return
    with env_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, sep, value = line.partition("=")
            if sep:
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


# Load .env from the repo root (two directories above this file).
# Cluster environment variables always take precedence (setdefault).
_load_env_file(Path(__file__).resolve().parents[2] / ".env")

w = WorkspaceClient()
spark = SparkSession.getActiveSession()

# Unity Catalog identifiers
catalog_name: str = "dbr_dev"
music_schema: str = "music_analytics"
volume_name: str = "raw_landing_zone"

# Derived volume paths and fully-qualified table names
volume_path: str = f"{music_schema}.{volume_name}"
json_landing_path: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/yt_snapshots"

music_metadata_dir: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/music_metadata"
music_metadata_file: str = f"{music_metadata_dir}/music_discography.csv"
silver_music_metadata_table: str = f"{catalog_name}.{music_schema}.silver_music_metadata"

music_stats_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_stats",
    "silver": f"{catalog_name}.{music_schema}.silver_music_stats",
    "gold": f"{catalog_name}.{music_schema}.gold_music_stats",
}
bronze_checkpoint_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/checkpoints/bronze_music_stats"
bronze_schema_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/schemas/bronze_music_stats"

# YouTube Data API v3 statistics endpoint
yt_video_url: str = "https://www.googleapis.com/youtube/v3/videos"


# Secret scope configuration — read from .env or cluster environment variables.
# The actual API key value is never stored in code or .env; it lives exclusively
# in the Databricks secret scope.
_SECRET_SCOPE: Final[str] = _require_env("DBRICKS_SECRET_SCOPE")
_SECRET_KEY: Final[str] = _require_env("DBRICKS_SECRET_KEY")

# YouTube Data API v3 key — retrieved from Databricks secrets at import time.
yt_api_key: str = w.dbutils.secrets.get(scope=_SECRET_SCOPE, key=_SECRET_KEY)

def setup_catalog() -> None:
    """
    Create the target catalog if it does not exist
    """
    spark.sql(f"USE CATALOG {catalog_name}")

def setup_schema() -> None:
    """
    Create the target schema if it does not exist
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {music_schema}")

def setup_volume() -> None:
    """
    Create the raw landing volume if it does not exist
    """
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")

def setup_music_metadata_dir():
    """
    Create the music metadata directory if it does not exist
    """
    w.dbutils.fs.mkdirs(music_metadata_dir)

def set_up_jsons_landing_dir():
    """
    Create the directory for JSON snapshots if it does not exist
    """
    w.dbutils.fs.mkdirs(json_landing_path)


def setup_silver_music_metadata_table() -> None:
    """Create the silver music-metadata Delta table if it does not exist.

    Creates an empty ``silver_music_metadata_table`` using
    ``CREATE TABLE IF NOT EXISTS``. Schema is inferred on the first write.

    Note: the raw metadata records are already clean, deduplicated so we skip the
    usual deduplication step and immediately write the metadata into silver layer.
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_music_metadata_table}")

# Setup the catalog, schema, and volume
# These are idempotent operations.

if __name__ == "__main__":
    setup_catalog()
    setup_schema()
    setup_volume()
    setup_music_metadata_dir()
    set_up_jsons_landing_dir()
    setup_silver_music_metadata_table()

# NOTE: bronze/silver/gold music_stats tables are intentionally NOT pre-created here.
# Their schemas are inferred on first write by the streaming writer (stream_yt_stats_to_bronze).
# Pre-creating them with CREATE TABLE IF NOT EXISTS (no columns) causes schema mismatch
# when writeStream tries to write because Delta rejects writes to a column-less table.

    