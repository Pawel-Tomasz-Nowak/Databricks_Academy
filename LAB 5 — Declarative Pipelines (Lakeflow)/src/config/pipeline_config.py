"""Pipeline configuration constants for DLT pipeline execution.

This module provides configuration that does NOT use __file__ or sys.path,
making it compatible with Databricks DLT pipeline runtime.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType


# Get active Spark session (available in pipeline context)
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _spark_conf_or_default(key: str, default: str) -> str:
    """Safely get Spark config value or return default."""
    try:
        return spark.conf.get(key)
    except Exception:
        return default


# ============================================================================
# CONFIGURATION - Read from Spark conf (set by pipeline configuration)
# ============================================================================

catalog_name: str = _spark_conf_or_default("music_project.catalog_name", "dbr_dev")
music_schema: str = _spark_conf_or_default("music_project.schema_name", "music_analytics")
volume_name: str = _spark_conf_or_default("music_project.volume_name", "landing_zone")

# ============================================================================
# PATHS - Derived from configuration
# ============================================================================

volume_path: str = f"{music_schema}.{volume_name}"
json_landing_path: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/yt_snapshots"
music_metadata_dir: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/music_metadata"
music_metadata_file: str = f"{music_metadata_dir}/music_discography.csv"

bronze_checkpoint_path = (
    f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/checkpoints/bronze_music_stats"
)
bronze_schema_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/schemas/bronze_music_stats"

# ============================================================================
# TABLE NAMES - Fully qualified Unity Catalog table references
# ============================================================================

music_metadata_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_metadata",
    "silver": f"{catalog_name}.{music_schema}.silver_music_metadata",
    "silver_history": f"{catalog_name}.{music_schema}.silver_music_metadata_history",
    "gold": f"{catalog_name}.{music_schema}.gold_music_metadata",
}

music_stats_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_stats",
    "silver": f"{catalog_name}.{music_schema}.silver_music_stats",
    "gold": f"{catalog_name}.{music_schema}.gold_music_stats",
}

# ============================================================================
# SCHEMAS - PySpark StructType definitions
# ============================================================================

metadata_music_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("album", StringType(), True),
    StructField("album_release_date", DateType(), True),
    StructField("author", StringType(), True),
])
