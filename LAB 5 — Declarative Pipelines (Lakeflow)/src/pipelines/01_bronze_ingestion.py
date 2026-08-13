# Project root setup for absolute imports
import os
import sys
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.setup.music_pipeline_setup import (
    bronze_schema_path,
    json_landing_path,
    metadata_music_schema,
    music_metadata_file,
    music_stats_tables,
    music_metadata_tables
)

import dlt




# # For later use, we'll switch to these parameters
# LANDING_ZONE_PATH = spark.conf.get("music_project.landing_zone_path")
# SCHEMA_PATH = spark.conf.get("music_project.checkpoint_path")


# Streaming source (autoloader for JSON files from YouTube)
@dlt.table(
    name=music_stats_tables["bronze"],
    comment="Bronze streaming table reading music popularity snapshots from YouTube",
    table_properties = {
        "quality":"bronze"
    }
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
        .option("cloudFiles.maxFilesPerTrigger","1000")
        .load(json_landing_path)
    )

# Static batch source (CSV for music metadata)
@dlt.table(
    name=music_metadata_tables["bronze"],
    comment="Bronze static table loading author and metadata dictionary from CSV",
    table_properties = {
        "quality":"bronze"
    }
)
def bronze_music_metadata():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(metadata_music_schema)
        .load(music_metadata_file)
    )