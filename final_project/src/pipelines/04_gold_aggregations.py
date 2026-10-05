"""Create gold aggregate tables for artist- and album-level reporting.

Lab 7 keeps the business aggregates from lab 6 and adds explicit expectations so
missing dimension joins fail early instead of silently skewing scorecards and
reconciliation checks.
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

from src.setup.music_pipeline_setup import music_stats_tables, music_metadata_tables
from src.transformations.aggregate_stats import aggregate_stats


@dp.materialized_view(
    name=music_stats_tables["gold_author"],
    comment="Artist-level gold aggregate with descriptive statistics across video snapshots.",
    table_properties={"quality": "gold", "table_type": "fact"},
)
@dp.expect_or_fail("author is not null", "author IS NOT NULL AND len(trim(author)) > 0")
def gold_author_stats_by_minute():
    """Join the fact table with the dimension table and aggregate by author.

    Both sources are materialized views, so the function uses ``spark.read`` for
    each input and then hands the joined frame to ``aggregate_stats``.
    """
    facts_df = spark.read.table(music_stats_tables["fact"])
    dim_df = spark.read.table(music_metadata_tables["gold"]).select("video_id", "author")

    ext_facts_df = facts_df.join(dim_df, on="video_id", how="left")
    return aggregate_stats(ext_facts_df)


@dp.materialized_view(
    name=music_stats_tables["gold_album"],
    comment="Album-level gold aggregate with descriptive statistics across video snapshots.",
    table_properties={"quality": "gold", "table_type": "fact"},
)
@dp.expect_or_fail("album is not null", "album IS NOT NULL AND len(trim(album)) > 0")
def gold_album_stats_by_minute():
    """Join the fact table with the dimension table and aggregate by album."""
    facts_df = spark.read.table(music_stats_tables["fact"])
    dim_df = spark.read.table(music_metadata_tables["gold"]).select("video_id", "album")

    ext_facts_df = facts_df.join(dim_df, on="video_id", how="left")

    return aggregate_stats(ext_facts_df, by="album")