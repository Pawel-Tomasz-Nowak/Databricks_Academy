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
from src.transformations.aggregate_video_stats import aggregate_video_stats

fact_expectations = {
    "video_id_valid": "video_id IS NOT NULL AND length(trim(video_id)) > 0",
    "ingested_at_valid": "_ingested_at IS NOT NULL AND to_date(_ingested_at) IS NOT NULL AND to_date(_ingested_at) < current_date()",
    "total_views_nonnegative": "total_views IS NULL OR total_views >= 0",
    "total_likes_nonnegative": "total_likes IS NULL OR total_likes >= 0",
    "total_comment_nonnegative": "total_comments IS NULL OR total_comments >= 0"
}


@dp.materialized_view(
    name=music_stats_tables["fact"],
    comment="Fact table showing the total views, likes, and comments for each video.",
    table_properties={
        "table_type": "fact",
        "grain": "video",
    },
)
@dp.expect_all_or_fail(fact_expectations)
def fact_video_stats():
    """Reads silver materialized view as a static snapshot and aggregates to fact-level.

    silver_music_stats is a materialized view (full snapshot with LAG deltas),
    so it cannot be read with readStream — spark.read.table is required.
    """
    facts_df = spark.read.table(music_stats_tables["silver"])
    return aggregate_video_stats(facts_df)