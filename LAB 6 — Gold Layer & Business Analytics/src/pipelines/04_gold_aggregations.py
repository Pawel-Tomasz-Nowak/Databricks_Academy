"""Gold layer aggregations for music analytics pipeline.

The value of the gold layer is not the raw row count; it is the business-ready
view over time. Minute-level aggregation makes the trend readable without
loading the full streaming history back into a reporting tool.
Implemented using Databricks Lakeflow (pyspark.pipelines).
"""
import os
import sys

from pyspark.sql import SparkSession

# The SDP runtime evaluates pipeline files dynamically — __file__ is not set.
# bundle.root is injected via spark.conf by the pipeline cluster configuration.
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



from pyspark import pipelines as dp

from src.setup.music_pipeline_setup import music_stats_tables, music_metadata_tables, _TIMESTAMP_GRANULARITY
from src.transformations.aggregate_stats import aggregate_stats

@dp.table(
    name = music_stats_tables["gold_author"],
    comment="Business view summarising total engagement metrics for authors across minute-level snapshots.",
    table_properties={"quality": "gold", "table_type":"fact"}
)
def gold_author_stats_by_minute():
    """Reads silver data and computes minute-level stats for authors."""
    facts_df = spark.read.table(music_stats_tables["fact"])
    dim_df = spark.read.table(music_metadata_tables["gold"]).select("video_id", "author")


    ext_facts_df = facts_df.join(dim_df, on ="video_id" ,how = "left")

    return aggregate_stats(ext_facts_df, timestamp_granularity=_TIMESTAMP_GRANULARITY)


@dp.table(
    name = music_stats_tables["gold_album"],
    comment=f"Business view summarising total engagement metrics for albums across {_TIMESTAMP_GRANULARITY}-level snapshots.",
    table_properties={"quality": "gold", "table_type":"fact"}
)
def gold_album_stats_by_minute():
    """Reads silver data and computes minute-level stats for authors."""
    facts_df = spark.read.table(music_stats_tables["fact"])
    dim_df = spark.read.table(music_metadata_tables["gold"]).select("video_id", "album")


    ext_facts_df = facts_df.join(dim_df, on ="video_id" ,how = "left")

    return aggregate_stats(ext_facts_df, by = "album", timestamp_granularity = _TIMESTAMP_GRANULARITY)