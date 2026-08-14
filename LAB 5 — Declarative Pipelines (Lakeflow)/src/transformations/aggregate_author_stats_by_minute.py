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
    max,
    mean,
    min,
    round,
    std,
    sum,
)

_TIMESTAMP_GRANULARITY: Final[str] = "minute"
_MEAN_ROUND_PRECISION: Final[int] = 1
_CV_ROUND_PRECISION: Final[int] = 2
_CV_SCALE: Final[int] = 100


def aggregate_author_stats_by_minute(silver_df: DataFrame) -> DataFrame:
    """Aggregate a full engagement statistics profile per author per ingestion-minute.

    Truncates the ``_ingested_at`` timestamp to minute precision and groups
    the silver table by author and truncated timestamp, producing total, min,
    max, mean, and coefficient-of-variation metrics for views, likes, and
    comments.

    Args:
        silver_df: Silver-layer DataFrame containing columns ``author``,
            ``video_id``, ``view_count``, ``like_count``, ``comment_count``,
            and ``_ingested_at``.

    Returns:
        DataFrame with columns ``author``, ``_ingested_at_minutes``,
        ``total_videos``, ``total/max/min/mean_views``, ``cv_views_pct``,
        and the corresponding columns for likes and comments.
    """
    silver_df_minutes = silver_df.withColumns({
        "_ingested_at_minutes": date_trunc(_TIMESTAMP_GRANULARITY, col("_ingested_at")),
    })

    return silver_df_minutes.groupBy("author", "_ingested_at_minutes").agg(
        countDistinct("video_id").alias("total_videos"),
        sum("view_count").alias("total_views"),
        sum("like_count").alias("total_likes"),
        sum("comment_count").alias("total_comments")
        # )
        # max("view_count").alias("max_views"),
        # max("like_count").alias("max_likes"),
        # max("comment_count").alias("max_comments"),
        # min("view_count").alias("min_views"),
        # min("like_count").alias("min_likes"),
        # min("comment_count").alias("min_comments"),
        # round(mean("view_count"), _MEAN_ROUND_PRECISION).alias("mean_views"),
        # round(mean("like_count"), _MEAN_ROUND_PRECISION).alias("mean_likes"),
        # round(mean("comment_count"), _MEAN_ROUND_PRECISION).alias("mean_comments"),
        # round(_CV_SCALE * std("view_count") / mean("view_count"), _CV_ROUND_PRECISION).alias("cv_views_pct"),
        # round(_CV_SCALE * std("like_count") / mean("like_count"), _CV_ROUND_PRECISION).alias("cv_likes_pct"),
        # round(_CV_SCALE * std("comment_count") / mean("comment_count"), _CV_ROUND_PRECISION).alias("cv_comments_pct"),
    ).select("author", "_ingested_at_minutes", "total_videos", "total_views", "total_likes", "total_comments")