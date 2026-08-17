"""Bronze layer ingestion for music analytics pipeline.

Reads YouTube video statistics and music metadata from cloud storage.
The file is intentionally self-contained because DLT executes modules through
exec() and does not expose __file__ or repo-local import paths.
"""


import dlt
import os
import sys


bundle_root_dir = os.getcwd() # it'l return: "/Workspace/Users/pawel.nowak@twoja_firma.com/.bundle/music_project/dev/files"

if bundle_root_dir not in sys.path:
    sys.path.insert(0, bundle_root_dir)


from src.setup.music_pipeline_setup import (
    json_landing_path,
    music_metadata_file,
    bronze_schema_path,
    music_metadata_tables,
    music_stats_tables,
    metadata_music_schema
)

from databricks.sdk.runtime import spark

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