"""Aggregate per-author engagement stats from the silver music table.

Groups by author at minute-level granularity and computes a full statistical
profile (total, min, max, mean, coefficient of variation) for views, likes,
and comments across the author's catalogue.
"""

from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    date_trunc,
    sum,
)

_TIMESTAMP_GRANULARITY: Final[str] = "minute"

def aggregate_author_stats_by_minute(silver_df: DataFrame) -> DataFrame:
    """Aggregate a full engagement statistics profile per author per ingestion-minute.

    Truncates the ``_ingested_at`` timestamp to minute precision and groups
    the silver table by author and truncated timestamp, producing total, min,
    max, mean, and coefficient-of-variation metrics for views, likes, and
    comments.

    Args:
        silver_df: Silver-layer DataFrame containing columns ``author``,
            ``video_id``, ``total_views``, ``total_likes``, ``total_comments``,
            and ``ingested_at_minutes``.

    Returns:
        DataFrame with columns ``author``, ``ingested_at_minutes``,
        ``total_videos``, ``total/max/min/mean_views``, ``cv_views_pct``,
        and the corresponding columns for likes and comments.
    """

    from pyspark.sql.functions import min, max, mean, stddev

    agg_df = silver_df.groupBy("author", "ingested_at_minutes").agg(
        countDistinct("video_id").alias("total_videos"),

        sum("view_count").alias("total_views"),
        min("view_count").alias("min_views"),
        max("view_count").alias("max_views"),
        mean("view_count").alias("mean_views"),
        stddev("view_count").alias("stddev_views"),

        sum("like_count").alias("total_likes"),
        min("like_count").alias("min_likes"),
        max("like_count").alias("max_likes"),
        mean("like_count").alias("mean_likes"),
        stddev("like_count").alias("stddev_likes"),

        sum("comment_count").alias("total_comments"),
        min("comment_count").alias("min_comments"),
        max("comment_count").alias("max_comments"),
        mean("comment_count").alias("mean_comments"),
        stddev("comment_count").alias("stddev_comments"),
    )

    # One could wisely ask: how about zero division?
    # Well, theoretically it's possible but most of the videos have at least 1 views.
    # Moreover, we're only consider most popular bands with hundreds of thousands/milions of views, 
    # so it's extremely impossible to get 0 views.
    # For interested readers, one can prove that if any of the video has positive number of views while the rest 
    # of the views are 0, the mean is s till positive (it's video_1_views/number_of_views :D which is positive since number_of_views can't be infinity! DOUBLE BAM! NERD TIME!)

    agg_df = agg_df.withColumn(
        "cv_views_pct",
        (col("stddev_views") / col("mean_views")) * 100 
    ).withColumn(
        "cv_likes_pct",
        (col("stddev_likes") / col("mean_likes")) * 100
    ).withColumn(
        "cv_comments_pct",
        (col("stddev_comments") / col("mean_comments")) * 100
    ).select(
        "author", "ingested_at_minutes", "total_videos",
        "total_views", "min_views", "max_views", "mean_views", "cv_views_pct",
        "total_likes", "min_likes", "max_likes", "mean_likes", "cv_likes_pct",
        "total_comments", "min_comments", "max_comments", "mean_comments", "cv_comments_pct"
    )

    return agg_df