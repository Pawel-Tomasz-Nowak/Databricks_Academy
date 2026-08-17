"""Bronze layer ingestion for music analytics pipeline.

Reads YouTube video statistics and music metadata from cloud storage.
The file is intentionally self-contained because DLT executes modules through
exec() and does not expose __file__ or repo-local import paths.
"""

import dlt
from databricks.sdk.runtime import spark
from pyspark.sql.types import DateType, StringType, StructField, StructType

catalog_name = spark.conf.get("music_project.catalog_name", "dbr_dev")
music_schema = spark.conf.get("music_project.schema_name", "music_analytics")
volume_name = spark.conf.get("music_project.volume_name", "landing_zone")

json_landing_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/yt_snapshots"
music_metadata_file = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/music_metadata/music_discography.csv"
bronze_schema_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/schemas/bronze_music_stats"

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

metadata_music_schema = StructType([
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("album", StringType(), True),
    StructField("album_release_date", DateType(), True),
    StructField("author", StringType(), True),
])


# Streaming source (autoloader for JSON files from YouTube)
@dlt.table(
    name=music_stats_tables["bronze"],
    comment="Bronze streaming table reading music popularity snapshots from YouTube",
    table_properties={"quality": "bronze"},
)
def bronze_youtube_stats():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", bronze_schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("multiline", "true")
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .load(json_landing_path)
    )


# Static batch source (CSV for music metadata)
@dlt.table(
    name=music_metadata_tables["bronze"],
    comment="Bronze static table loading author and metadata dictionary from CSV",
    table_properties={"quality": "bronze"},
)
def bronze_music_metadata():
    return (
        spark.read
        .format("csv")
        .option("delimiter", ";")
        .option("header", "true")
        .schema(metadata_music_schema)
        .load(music_metadata_file)
    )