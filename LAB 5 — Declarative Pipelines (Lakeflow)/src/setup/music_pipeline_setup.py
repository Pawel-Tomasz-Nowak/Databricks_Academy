"""Shared configuration and bootstrap for the LAB 5 music pipeline."""

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType


metadata_music_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("album", StringType(), True),
    StructField("album_release_date", DateType(), True),
    StructField("author", StringType(), True),
])



spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
w = WorkspaceClient()


def _spark_conf_or_default(key: str, default: str) -> str:
    try:
        return spark.conf.get(key)
    except Exception:
        return default


catalog_name: str = _spark_conf_or_default("music_project.catalog_name", "dbr_dev")
music_schema: str = _spark_conf_or_default("music_project.schema_name", "music_analytics")
volume_name: str = _spark_conf_or_default("music_project.volume_name", "landing_zone")

volume_path: str = f"{music_schema}.{volume_name}"
json_landing_path: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/yt_snapshots"
music_metadata_dir: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/music_metadata"
music_metadata_file: str = f"{music_metadata_dir}/music_discography.csv"

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

bronze_checkpoint_path = (
    f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/checkpoints/bronze_music_stats"
)
bronze_schema_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/schemas/bronze_music_stats"

yt_video_url: str = "https://www.googleapis.com/youtube/v3/videos"


def get_yt_api_key() -> str:
    """Resolve YouTube API key from Databricks secret reference."""
 
    scope = os.environ.get("DBRICKS_SECRET_SCOPE")
    secret_key = os.environ.get("DBRICKS_SECRET_KEY")
    if scope and secret_key:
        return w.dbutils.secrets.get(scope=scope, key=secret_key)

    raise EnvironmentError(
        "YouTube API key is missing. Set YT_API_KEY or DBRICKS_SECRET_SCOPE/DBRICKS_SECRET_KEY."
    )


def _catalog_exists(name: str) -> bool:
    """Return True when Unity Catalog already contains the given catalog."""
    return spark.sql(f"SHOW CATALOGS LIKE '{name}'").count() > 0


def _bootstrap() -> None:
    """Create UC catalog/schema/volume and required landing directories."""
    global _BOOTSTRAP_DONE

    if _BOOTSTRAP_DONE:
        return

    if not _catalog_exists(catalog_name):
        try:
            spark.sql(f"CREATE CATALOG {catalog_name}")
        except Exception as exc:
            raise RuntimeError(
                "Unable to create catalog "
                f"'{catalog_name}'. The metastore default storage is not ready. "
                "Create the catalog in the UI with managed storage or switch "
                "music_project.catalog_name to an existing catalog (for example 'main')."
            ) from exc

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{music_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{music_schema}.{volume_name}")

    w.dbutils.fs.mkdirs(json_landing_path)
    w.dbutils.fs.mkdirs(music_metadata_dir)

    _BOOTSTRAP_DONE = True


def bootstrap_infrastructure() -> None:
    """Public entrypoint used by job tasks to initialize UC infrastructure."""
    _bootstrap()


_BOOTSTRAP_DONE = False

if __name__ == "__main__":
    bootstrap_infrastructure()


try:
    yt_api_key = get_yt_api_key()
except EnvironmentError:
    yt_api_key = None
