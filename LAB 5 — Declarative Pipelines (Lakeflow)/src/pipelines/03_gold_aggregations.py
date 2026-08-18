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

from src.setup.music_pipeline_setup import music_stats_tables
from src.transformations.aggregate_author_stats_by_minute import aggregate_author_stats_by_minute
from src.transformations.aggregate_video_stats_by_minute import aggregate_video_stats_by_minute


@dp.table(
    name=music_stats_tables["gold"] + "_by_author",
    comment="Business view summarising total engagement metrics for authors across minute-level snapshots.",
    table_properties={"quality": "gold"},
)
def gold_author_stats_by_minute():
    """Reads silver data and computes minute-level stats for authors."""
    facts_df = spark.read.table(music_stats_tables["silver"])
    return aggregate_author_stats_by_minute(facts_df)


@dp.table(
    name=music_stats_tables["gold"] + "_by_video",
    comment="Business view showing the total views, likes, and comments for each video by minute.",
    table_properties={"quality": "gold"},
)
def gold_video_stats_by_minute():
    """Reads silver data and computes minute-level stats for videos."""
    facts_df = spark.read.table(music_stats_tables["silver"])
    return aggregate_video_stats_by_minute(facts_df)