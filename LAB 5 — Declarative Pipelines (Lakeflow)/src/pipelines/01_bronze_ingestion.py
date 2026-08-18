"""Bronze layer ingestion for music analytics pipeline.

Reads YouTube video statistics and music metadata from cloud storage.
Includes a robust import mechanism to resolve the 'src' package
within the Databricks Delta Live Tables (DLT) runtime environment.
"""
import sys
import os
from pyspark.sql import SparkSession

# 1. BULLETPROOF DLT IMPORT MECHANISM
# DLT evaluates code dynamically, so __file__ is not available.
# We retrieve the bundle root defined in databricks.yml via spark.conf.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
bundle_root = spark.conf.get("bundle.root", "")

# We check multiple possible root directories depending on the DBR execution context
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

# 2. LDP TABLE DEFINITIONS

@dp.table(
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

@dp.table(
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