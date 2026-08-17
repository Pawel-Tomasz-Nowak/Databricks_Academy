"""Gold layer aggregations for music analytics pipeline.

The value of the gold layer is not the raw row count; it is the business-ready
view over time. Minute-level aggregation makes the trend readable without
loading the full streaming history back into a reporting tool.
"""

import dlt
from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame, functions as F

catalog_name = spark.conf.get("music_project.catalog_name", "dbr_dev")
music_schema = spark.conf.get("music_project.schema_name", "music_analytics")
volume_name = spark.conf.get("music_project.volume_name", "landing_zone")

music_stats_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_stats",
    "silver": f"{catalog_name}.{music_schema}.silver_music_stats",
    "gold": f"{catalog_name}.{music_schema}.gold_music_stats",
}


def aggregate_author_stats_by_minute(silver_df: DataFrame) -> DataFrame:
    """Aggregate total engagement metrics for each author and minute bucket.

    Keeping this at minute granularity is intentional: it gives enough temporal
    resolution for trend analysis without producing an unreadable time series of
    every individual event.
    """
    silver_df_minutes = silver_df.withColumn("_ingested_at_minutes", F.date_trunc("minute", F.col("_ingested_at")))
    return (
        silver_df_minutes.groupBy("author", "_ingested_at_minutes")
        .agg(
            F.countDistinct("video_id").alias("total_videos"),
            F.sum("view_count").alias("total_views"),
            F.sum("like_count").alias("total_likes"),
            F.sum("comment_count").alias("total_comments"),
        )
        .select("author", "_ingested_at_minutes", "total_videos", "total_views", "total_likes", "total_comments")
    )


def aggregate_video_stats_by_minute(silver_df: DataFrame) -> DataFrame:
    """Aggregate total engagement for each song and minute bucket.

    This keeps the per-track signal easy to follow and makes the downstream gold
    table useful for artist-level and content-level trend monitoring.
    """
    silver_df_minutes = silver_df.withColumn("_ingested_at_minutes", F.date_trunc("minute", F.col("_ingested_at")))
    return (
        silver_df_minutes.groupBy("author", "song_title", "_ingested_at_minutes")
        .agg(
            F.sum("view_count").alias("total_views"),
            F.sum("like_count").alias("total_likes"),
            F.sum("comment_count").alias("total_comments"),
        )
        .select("author", "song_title", "_ingested_at_minutes", "total_views", "total_likes", "total_comments")
    )


@dlt.table(
    name=music_stats_tables["gold"] + "_by_author",
    comment="Business view summarising total engagement metrics for authors across minute-level snapshots.",
    table_properties={"quality": "gold"},
)
def gold_author_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_author_stats_by_minute(facts_df)


@dlt.table(
    name=music_stats_tables["gold"] + "_by_video",
    comment="Business view showing the total views, likes, and comments for each video by minute.",
    table_properties={"quality": "gold"},
)
def gold_video_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_video_stats_by_minute(facts_df)


