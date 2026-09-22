"""Bronze layer ingestion for the music analytics pipeline.

Reads YouTube video statistics snapshots and music metadata from cloud storage.
The import block below resolves the 'src' package inside the Lakeflow Spark
Declarative Pipeline (SDP) runtime, where __file__ is unavailable.
"""
import sys
import os
from pyspark.sql import SparkSession, functions as F

# The SDP runtime evaluates pipeline files dynamically — __file__ is not set.
# bundle.root is injected via spark.conf by the pipeline cluster configuration.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
bundle_root = spark.conf.get("bundle.root", "")

# Walk several candidate roots because the working directory varies by DBR context.
possible_roots = [
    bundle_root, 
    os.getcwd(), 
    os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
]

for path in possible_roots:
    if path and os.path.isdir(os.path.join(path, "src")):
        if path not in sys.path:
            sys.path.insert(0, path)
        break

import pyspark.pipelines as dp
from src.setup.music_pipeline_setup import (
    json_landing_path,
    music_metadata_file,
    bronze_schema_path,
    music_metadata_tables,
    music_stats_tables,
    metadata_music_schema
)

@dp.table(
    name=music_stats_tables["bronze"],
    comment="Bronze streaming table reading music popularity snapshots from YouTube",
    table_properties={"quality": "bronze"},
)
def bronze_youtube_stats() -> None:
    """Stream YouTube video-statistics snapshots from the landing volume into the bronze table.

    Reads multiline JSON files deposited by the producer task under
    ``json_landing_path`` using Auto Loader (``cloudFiles`` format). Schema is
    inferred on first load and stored at ``bronze_schema_path``; subsequent runs
    use schema evolution in rescue mode so unexpected fields are preserved in
    a ``_rescued_data`` column rather than causing failures.

    Returns:
        A streaming DataFrame of raw YouTube statistics records.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", bronze_schema_path+"_snapshots")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("multiline", "true")
        .option("cloudFiles.maxFilesPerTrigger", "50")
        .load(json_landing_path)
        .withColumns({
            "_source_file_path": F.col("_metadata.file_path"),
            "_ingested_at": F.col("_metadata.file_modification_time").cast("timestamp"),
        })
        .drop("_metadata")
    )

@dp.table(
    name=music_metadata_tables["bronze"],
    comment="Bronze streaming table loading author and metadata dictionary from CSV",
    table_properties={"quality": "bronze"},
)
def bronze_music_metadata() -> None:
    """Stream the music metadata dictionary from CSV files into the bronze table.

    Reads ``music_discography.csv`` from the landing volume as a streaming read using
    the predefined ``metadata_music_schema``. The file uses a semicolon delimiter,
    includes a header row, and applies schema evolution in rescue mode so unexpected
    fields are preserved in a ``_rescued_data`` column rather than causing failures.
    Maximum files per batch is 50.

    Returns:
        A streaming DataFrame of music metadata records (url, title, album,
        album_release_date, author).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", bronze_schema_path+"_metadata")
        .option("cloudFiles.maxFilesPerTrigger", "50")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("delimiter", ";")
        .option("header", "true")
        .option("dateFormat", "dd.MM.yyyy")
        .schema(metadata_music_schema)
        .load(music_metadata_file)
        .withColumns({
            "_source_file_path": F.col("_metadata.file_path"),
            "_ingested_at": F.col("_metadata.file_modification_time").cast("timestamp"),
        })
        .drop("_metadata")
    )