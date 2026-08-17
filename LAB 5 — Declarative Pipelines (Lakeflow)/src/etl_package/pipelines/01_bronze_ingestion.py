from etl_package.setup.music_pipeline_setup import (
    bronze_schema_path,
    json_landing_path,
    metadata_music_schema,
    music_metadata_file,
    music_metadata_tables,
    music_stats_tables,
)

import dlt
from databricks.sdk.runtime import spark


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
