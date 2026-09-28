"""Build artist- and album-level gold aggregates from the video fact table.

These views turn per-video metric snapshots into business-facing rollups that
are easier to consume in dashboards and alerting logic.
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
    comment="Business view summarising engagement metrics per artist across fact table snapshots.",
    table_properties={"quality": "gold", "table_type": "fact"},
)
def gold_author_stats_by_minute():
    """Join fact and dimension data, then aggregate metrics at the artist grain.

    Both source objects are materialized views, so the function reads them with
    ``spark.read.table`` and then enriches each fact row with the artist name
    before applying the shared aggregation helper.
    """
    facts_df = spark.read.table(music_stats_tables["fact"])
    dim_df = spark.read.table(music_metadata_tables["gold"]).select("video_id", "author")

    ext_facts_df = facts_df.join(dim_df, on="video_id", how="left")
    return aggregate_stats(ext_facts_df)


@dp.materialized_view(
    name=music_stats_tables["gold_album"],
    comment="Business view summarising engagement metrics per album across fact table snapshots.",
    table_properties={"quality": "gold", "table_type": "fact"},
)
def gold_album_stats_by_minute():
    """Join fact and dimension data, then aggregate metrics at the album grain."""
    facts_df = spark.read.table(music_stats_tables["fact"])
    dim_df = spark.read.table(music_metadata_tables["gold"]).select("video_id", "album")

    ext_facts_df = facts_df.join(dim_df, on="video_id", how="left")
    return aggregate_stats(ext_facts_df, by="album")