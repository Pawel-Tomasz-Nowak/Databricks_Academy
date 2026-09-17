"""Define shared configuration and bootstrap helpers for the lab_7 pipeline.

This module is imported by both Lakeflow Spark Declarative Pipeline files and
job-style Python tasks. It keeps Spark-dependent work delayed until runtime so
imports remain safe in pipeline compilation contexts.
"""
import argparse
import os
import sys
import glob
import shutil

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
    """Return the resolved volume paths and table names for a target environment.

    Args:
        catalog: Unity Catalog catalog used by the project.
        schema: Unity Catalog schema that stores the pipeline tables.
        volume: Unity Catalog volume used for landing files and metadata files.

    Returns:
        Dictionary with derived volume paths plus bronze, silver, quarantine,
        fact, and gold table names used across the project.
    """
    base_volume = f"/Volumes/{catalog}/{schema}/{volume}"
    return {
        "volume_path": f"{catalog}.{schema}.{volume}",
        "json_landing_path": f"{base_volume}/yt_snapshots",
        "music_metadata_dir": f"{base_volume}/music_metadata",
        "music_metadata_file": f"{base_volume}/music_metadata/music_discography*.csv",
        "bronze_schema_path": f"{base_volume}/music_schema",
        "music_metadata_tables": {
            "bronze": f"{catalog}.{schema}.bronze_music_metadata",
            "silver": f"{catalog}.{schema}.silver_music_metadata",
            "silver_history": f"{catalog}.{schema}.silver_music_metadata_history",
            "silver_quarantine": f"{catalog}.{schema}.silver_music_quarantine",
            "gold": f"{catalog}.{schema}.dim_music_metadata",
        },
        "music_stats_tables": {
            "bronze": f"{catalog}.{schema}.bronze_music_stats",
            "silver": f"{catalog}.{schema}.silver_music_stats",
            "silver_quarantine": f"{catalog}.{schema}.silver_music_stats_quarantine",
            "fact": f"{catalog}.{schema}.fact_music_stats",  # Video-level fact snapshot.
            "gold_album": f"{catalog}.{schema}.gold_album_music_stats",  # Album-level aggregate.
            "gold_author": f"{catalog}.{schema}.gold_author_music_stats"  # Artist-level aggregate.
        }  # Grain narrows from video snapshot to album rollup to artist rollup.
    }


# ------------------------------------------------------------------------------
# 3. SAFE ARGUMENT PARSING (For Global Variables & SDP Imports)
# ------------------------------------------------------------------------------
# When the pipeline runtime imports this module there are no CLI arguments.
# parse_known_args prevents unexpected Databricks-injected arguments from
# terminating the import with SystemExit.
_parser = argparse.ArgumentParser(description="Music pipeline configuration parser")
_parser.add_argument("--catalog", default=os.environ.get("DBRICKS_CATALOG"))
_parser.add_argument("--schema", default=os.environ.get("DBRICKS_SCHEMA"))
_parser.add_argument("--volume", default=os.environ.get("DBRICKS_VOLUME"))

_args, _ = _parser.parse_known_args()

catalog_name = _args.catalog
music_schema = _args.schema
volume_name = _args.volume

# Fall back to spark.conf when the module is imported from a pipeline file rather
# than executed as a task with explicit CLI parameters.
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
    """Create the catalog, schema, volume, and landing directories.

    The task is designed to run before the ingestion and pipeline tasks so the
    landing zone and metadata folder always exist before new files are written.
    """
    if not all([catalog_name, music_schema, volume_name]):
        raise ValueError(
            "Setup error: Missing required parameters (catalog, schema, volume). "
            "Ensure databricks.yml passes CLI parameters to this task."
        )

    # Deferred imports prevent SparkSession creation during module import.
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
        # w.dbutils.fs.mkdirs(paths["music_metadata_dir"])
    except Exception:
        # Fallback for contexts where WorkspaceClient is unavailable.
        dbutils.fs.mkdirs(paths["json_landing_path"])
        # dbutils.fs.mkdirs(paths["music_metadata_dir"])

    print("[BOOTSTRAP] Infrastructure successfully configured.")

# ------------------------------------------------------------------------------
# 5. PREPARING METADATA LANDING ZONE
# ------------------------------------------------------------------------------
def get_bundle_root() -> str:
    candidate_path = None
    if len(sys.argv) > 0 and sys.argv[0] and sys.argv[0].endswith(".py"):
        candidate_path = os.path.abspath(sys.argv[0])
    elif "__file__" in globals():
        candidate_path = os.path.abspath(__file__)

    if candidate_path and "/files" in candidate_path:
        return candidate_path.split("/files")[0] + "/files"

    cwd = os.getcwd()
    if "/files" in cwd:
        return cwd.split("/files")[0] + "/files"
        
    return os.path.abspath(os.path.join(cwd, "..", ".."))

# Bundle path resolving
bundle_root = get_bundle_root()
seed_dir = os.path.join(bundle_root, "data", "seed")

# Make sure the metadata directory exists
os.makedirs(music_metadata_dir, exist_ok=True)

seed_files_pattern = os.path.join(seed_dir, music_metadata_file)
found_seed_files = glob.glob(seed_files_pattern)

if not found_seed_files:
    print(f"Warning: Did not found any files matching {seed_files_pattern}")
else:
    for source_path in found_seed_files:
        file_name = os.path.basename(source_path)
        target_path = os.path.join(music_metadata_dir, file_name)
        
        # Copy only missing values to ensure idempotency
        if not os.path.exists(target_path):
            shutil.copyfile(source_path, target_path)
            print(f"Copied seed: {file_name} -> {target_path}")

# ------------------------------------------------------------------------------
# 6. ENTRY POINT FOR TASK 1 (CLI Execution)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    bootstrap_infrastructure()