"""Shared configuration, schemas and bootstrap for the LAB 5 music pipeline.
This file is DLT-import safe. It delays SparkSession and dbutils initialization.
"""
import argparse
import os
from pyspark.sql.types import DateType, StringType, StructField, StructType

# ------------------------------------------------------------------------------
# 1. SCHEMAS AND CONSTANTS
# ------------------------------------------------------------------------------
metadata_music_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("album", StringType(), True),
    StructField("album_release_date", DateType(), True),
    StructField("author", StringType(), True),
])

yt_video_url = "https://www.googleapis.com/youtube/v3/videos"


# ------------------------------------------------------------------------------
# 2. PATH RESOLVER
# ------------------------------------------------------------------------------
def get_pipeline_paths(catalog: str, schema: str, volume: str) -> dict:
    """Returns a dictionary of paths and table names for the given Unity Catalog entities."""
    base_volume = f"/Volumes/{catalog}/{schema}/{volume}"
    return {
        "volume_path": f"{catalog}.{schema}.{volume}",
        "json_landing_path": f"{base_volume}/yt_snapshots",
        "music_metadata_dir": f"{base_volume}/music_metadata",
        "music_metadata_file": f"{base_volume}/music_metadata/music_discography.csv",
        "bronze_schema_path": f"{base_volume}/music_schema",
        "music_metadata_tables": {
            "bronze": f"{catalog}.{schema}.bronze_music_metadata",
            "silver": f"{catalog}.{schema}.silver_music_metadata",
            "silver_history": f"{catalog}.{schema}.silver_music_metadata_history",
            "gold": f"{catalog}.{schema}.gold_music_metadata",
        },
        "music_stats_tables": {
            "bronze": f"{catalog}.{schema}.bronze_music_stats",
            "silver": f"{catalog}.{schema}.silver_music_stats",
            "gold": f"{catalog}.{schema}.gold_music_stats",
        },
    }


# ------------------------------------------------------------------------------
# 3. SAFE ARGUMENT PARSING (For Global Variables & SDP Imports)
# ------------------------------------------------------------------------------
# When the SDP runtime imports this module there are no CLI args — parse_known_args
# silently ignores unknown args rather than raising SystemExit.
_parser = argparse.ArgumentParser(description="Music Pipeline Configuration Parser")
_parser.add_argument("--catalog", default=os.environ.get("DBRICKS_CATALOG"))
_parser.add_argument("--schema", default=os.environ.get("DBRICKS_SCHEMA"))
_parser.add_argument("--volume", default=os.environ.get("DBRICKS_VOLUME"))

_args, _ = _parser.parse_known_args()

catalog_name = _args.catalog
music_schema = _args.schema
volume_name = _args.volume

# DLT/Lakeflow Pipeline fallback: Try extracting config from active Spark session
if not all([catalog_name, music_schema, volume_name]):
    try:
        from pyspark.sql import SparkSession
        _spark = SparkSession.getActiveSession()
        if _spark:
            catalog_name = catalog_name or _spark.conf.get("music.catalog", None)
            music_schema = music_schema or _spark.conf.get("music.schema", None)
            volume_name = volume_name or _spark.conf.get("music.volume", None)
    except Exception:
        pass

if catalog_name and music_schema and volume_name:
    _paths = get_pipeline_paths(catalog_name, music_schema, volume_name)
    volume_path = _paths["volume_path"]
    json_landing_path = _paths["json_landing_path"]
    music_metadata_dir = _paths["music_metadata_dir"]
    music_metadata_file = _paths["music_metadata_file"]
    bronze_schema_path = _paths["bronze_schema_path"]
    music_metadata_tables = _paths["music_metadata_tables"]
    music_stats_tables = _paths["music_stats_tables"]
else:
    volume_path = None
    json_landing_path = None
    music_metadata_dir = None
    music_metadata_file = None
    bronze_schema_path = None
    music_metadata_tables = {}
    music_stats_tables = {}


# ------------------------------------------------------------------------------
# 4. BOOTSTRAP LOGIC (Executed only when run directly as a script)
# ------------------------------------------------------------------------------
def bootstrap_infrastructure() -> None:
    """Creates the catalog, schema, volume, and required directory structures."""
    if not all([catalog_name, music_schema, volume_name]):
        raise ValueError(
            "Setup error: Missing required parameters (catalog, schema, volume). "
            "Ensure databricks.yml passes CLI parameters to this task."
        )

    # Deferred imports prevent SparkSession creation at module-import time (SDP context).
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    print(f"[BOOTSTRAP] Setting up infrastructure for: {catalog_name}.{music_schema}.{volume_name}")

    catalogs = [row[0] for row in spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'").collect()]
    if not catalogs:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        except Exception as exc:
            raise RuntimeError(f"Permission denied when creating catalog '{catalog_name}'.") from exc

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{music_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{music_schema}.{volume_name}")

    paths = get_pipeline_paths(catalog_name, music_schema, volume_name)
    
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        w.dbutils.fs.mkdirs(paths["json_landing_path"])
        w.dbutils.fs.mkdirs(paths["music_metadata_dir"])
    except Exception:
        # Fallback if WorkspaceClient is not available
        dbutils.fs.mkdirs(paths["json_landing_path"])
        dbutils.fs.mkdirs(paths["music_metadata_dir"])

    print("[BOOTSTRAP] Infrastructure successfully configured.")


# ------------------------------------------------------------------------------
# 5. ENTRY POINT FOR TASK 1 (CLI Execution)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    bootstrap_infrastructure()