"""
stream_yt_stats_to_bronze.py
----------------------------
Streaming ingestion pipeline: reads YouTube-statistics JSON snapshots from a
Unity Catalog Volume via Auto Loader and appends enriched records to the
bronze Delta table.

Execution context: run via ``%run`` or as a Databricks job task on a cluster
with an active SparkSession.

Side effects:
    - Starts a Structured Streaming query (trigger=availableNow) that appends
      rows to ``bronze_music_stats_table``.
    - Checkpoint state is persisted at ``checkpoint_path``.
    - Schema inference state is persisted at ``schema_path``.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col

from ..setup.music_pipeline_setup import (
    music_stats_tables,
    catalog_name,
    json_landing_path,
    music_schema,
    volume_name,
    bronze_checkpoint_path,
    bronze_schema_path
)

bronze_music_stats_table = music_stats_tables["bronze"]
# ---------------------------------------------------------------------------
# Streaming read — Auto Loader
# ---------------------------------------------------------------------------
df_stream: DataFrame = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", bronze_schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # rescue unexpected columns for future schema evolution
    .option("multiline", "true")
    .load(json_landing_path)
)

# ---------------------------------------------------------------------------
# Enrichment — metadata columns and type casts
# ---------------------------------------------------------------------------
# Source file path surfaced from Spark's hidden _metadata struct.

df_stream_enriched: DataFrame = (
    df_stream
    .withColumn("_source_file", col("_metadata.file_path"))
)

# # # ---------------------------------------------------------------------------
# # # Streaming write — bronze Delta table
# # # ---------------------------------------------------------------------------
query = (
    df_stream_enriched
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint_path)
    .trigger(availableNow=True)
    .table(bronze_music_stats_table)
)
query.awaitTermination()