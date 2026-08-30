"""Create the video-grain fact table used by gold business aggregates.

The fact layer preserves one metric snapshot per ``video_id`` and ingestion
timestamp, which downstream album- and artist-level rollups can reuse without
re-reading the silver transformation logic.
"""
import os
import sys

from pyspark.sql import SparkSession

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

from pyspark import pipelines as dp

from src.setup.music_pipeline_setup import music_stats_tables
from src.transformations.aggregate_video_stats import aggregate_video_stats

# Lab 7 adds a strict gate on the fact table so invalid snapshots stop the
# pipeline before downstream dashboards and scorecards consume them.
fact_expectations = {
    "video_id_valid": "video_id IS NOT NULL AND length(trim(video_id)) > 0",
    "ingested_at_valid": "_ingested_at IS NOT NULL AND to_date(_ingested_at) IS NOT NULL AND to_date(_ingested_at) < current_date()",
    "total_views_nonnegative": "total_views IS NULL OR total_views >= 0",
    "total_likes_nonnegative": "total_likes IS NULL OR total_likes >= 0",
    "total_comment_nonnegative": "total_comments IS NULL OR total_comments >= 0"
}


@dp.materialized_view(
    name=music_stats_tables["fact"],
    comment="Fact table with one video-level snapshot of views, likes, and comments per ingestion timestamp.",
    table_properties={
        "table_type": "fact",
        "grain": "video",
    },
)
@dp.expect_all_or_fail(fact_expectations)
def fact_video_stats():
    """Return the business-facing fact table derived from silver stats.

    ``silver_music_stats`` is materialized, so the function reads it as a static
    table snapshot and then delegates the final column selection to
    ``aggregate_video_stats``.
    """
    facts_df = spark.read.table(music_stats_tables["silver"])
    return aggregate_video_stats(facts_df)