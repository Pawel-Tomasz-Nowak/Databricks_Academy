"""Aggregate per-grouper engagement stats from the silver music table.

Groups by a custom ``by`` field (e.g., author or album) at minute-level granularity
and computes a full statistical profile (total, min, max, mean, coefficient of variation)
for views, likes, and comments across the grouper's catalogue.
"""

from typing import Final

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def aggregate_stats(silver_df: DataFrame, by:str = "author") -> DataFrame:
    """Aggregate a full engagement statistics profile per custom grouper per ingestion-minute.

    Truncates the ``_ingested_at`` timestamp to minute precision and groups
    the silver table by the specified grouper (e.g., author or album) and truncated timestamp,
    producing total, min, max, mean, and coefficient-of-variation metrics for views, likes, and
    comments.

    Args:
        silver_df: Silver-layer DataFrame containing columns for the specified ``by`` grouper,
            ``video_id``, ``total_views``, ``total_likes``, ``total_comments``,
            and ``ingested_at_minutes``.
        by: Field to group by (e.g., "author" or "album").

    Returns:
        DataFrame with columns for the ``by`` grouper, ``ingested_at_minutes``,
        ``total_videos``, ``total/max/min/mean_views``, ``cv_views_pct``,
        and the corresponding columns for likes and comments.
    """

    agg_df = silver_df.groupBy(by, "_ingested_at").agg(
        F.countDistinct("video_id").alias("total_videos"),

        F.sum("total_views").alias("total_views"),
        F.min("total_views").alias("min_views"),
        F.max("total_views").alias("max_views"),
        F.mean("total_views").alias("mean_views"),
        F.stddev("total_views").alias("stddev_views"),

        F.sum("total_likes").alias("total_likes"),
        F.min("total_likes").alias("min_likes"),
        F.max("total_likes").alias("max_likes"),
        F.mean("total_likes").alias("mean_likes"),
        F.stddev("total_likes").alias("stddev_likes"),

        F.sum("total_comments").alias("total_comments"),
        F.min("total_comments").alias("min_comments"),
        F.max("total_comments").alias("max_comments"),
        F.mean("total_comments").alias("mean_comments"),
        F.stddev("total_comments").alias("stddev_comments"),
    )

    # One could wisely ask: how about zero division?
    # Well, theoretically it's possible but most of the videos have at least 1 views.
    # Moreover, we're only consider most popular bands with hundreds of thousands/milions of views, 
    # so it's extremely impossible to get 0 views.
    # For interested readers, one can prove that if any of the video has positive number of views while the rest 
    # of the views are 0, the mean is s till positive (it's video_1_views/number_of_views :D which is positive since number_of_views can't be infinity! DOUBLE BAM! NERD TIME!)

    agg_df = agg_df.withColumn(
        "cv_views_pct",
        (F.col("stddev_views") / F.col("mean_views")) * 100 
    ).withColumn(
        "cv_likes_pct",
        (F.col("stddev_likes") / F.col("mean_likes")) * 100
    ).withColumn(
        "cv_comments_pct",
        (F.col("stddev_comments") / F.col("mean_comments")) * 100
    ).select(
        by, "_ingested_at", "total_videos",
        "total_views", "min_views", "max_views", "mean_views","stddev_views","cv_views_pct",
        "total_likes", "min_likes", "max_likes", "mean_likes", "stddev_likes","cv_likes_pct",
        "total_comments", "min_comments", "max_comments", "mean_comments", "stddev_comments","cv_comments_pct"
    )

    return agg_df