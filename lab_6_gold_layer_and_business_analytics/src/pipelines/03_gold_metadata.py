"""Create the gold music metadata dimension for business-facing analytics.

This file exposes the current metadata snapshot as a dimension table used by the
dashboard, alerting logic, and row/column security SQL assets in the BI folder.
"""
import os
import sys

from pyspark.sql import SparkSession
import pyspark.pipelines as dp
import pyspark.sql.functions as F

# Lakeflow Spark Declarative Pipelines evaluates files dynamically, so __file__
# is not available. The bundle root is injected through spark.conf instead.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
bundle_root = spark.conf.get("bundle.root", "")
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

from src.setup.music_pipeline_setup import (
    music_metadata_tables
)


@dp.materialized_view(
    name=music_metadata_tables["gold"],
    comment="Gold dimension table for music metadata; keeps the current video attributes without the raw source URL.",
    table_properties={"quality": "gold", "table_type": "dimension"},
    cluster_by=["author", "album"],
)
def golden_music_metadata():
    """Return the current metadata dimension used by gold analytics assets.

    The upstream silver metadata table is a materialized view that already holds
    the latest known record per ``video_id``. This gold step removes the source
    URL so downstream BI assets work with business-facing descriptive columns.
    """
    silver_tbl = spark.read.table(music_metadata_tables["silver"])
    return silver_tbl.drop(F.col("url"))
