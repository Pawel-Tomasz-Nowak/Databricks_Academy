"""Shared configuration and bootstrap for the LAB 5 music pipeline."""

import os
import sys
from pyspark.sql.types import DateType, StringType, StructField, StructType


metadata_music_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("album", StringType(), True),
    StructField("album_release_date", DateType(), True),
    StructField("author", StringType(), True),
])


from pyspark.dbutils import DBUtils

dbutils = DBUtils(spark)

catalog_name = spark.conf.get("music_project.catalog_name")
music_schema = spark.conf.get("music_project.schema_name")
volume_name = spark.conf.get("music_project.volume_name")

scope = spark.conf.get("secret_scope_val")
secret_key = spark.conf.get("secret_key_val")
bundle_root = spark.conf.get("bundle_root")


if bundle_root and bundle_root not in sys.path:
    sys.path.append(bundle_root)

# Bezpieczne pobranie właściwego sekretu z Azure Key Vault / Databricks Secrets
yt_api_key = dbutils.secrets.get(scope=scope, key=secret_key)





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

