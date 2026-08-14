"""Aggregate per-video (song) engagement stats from the silver music table.

Groups by author and song title at minute-level granularity, computing total
views, likes, and comments per snapshot window.
"""

from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_trunc, sum

_TIMESTAMP_GRANULARITY: Final[str] = "minute"


def aggregate_video_stats_by_minute(silver_df: DataFrame) -> DataFrame:
    """Aggregate total engagement metrics per video per ingestion-minute.

    Truncates the ``_ingested_at`` timestamp to minute precision and groups
    the silver table by author, song title, and truncated timestamp.

    Args:
        silver_df: Silver-layer DataFrame containing columns ``author``,
            ``song_title``, ``view_count``, ``like_count``, ``comment_count``,
            and ``_ingested_at``.

    Returns:
        DataFrame with columns ``author``, ``song_title``,
        ``_ingested_at_minutes``, ``total_views``, ``total_likes``, and
        ``total_comments`` only.
    """
    silver_df_minutes = silver_df.withColumns({
        "_ingested_at_minutes": date_trunc(_TIMESTAMP_GRANULARITY, col("_ingested_at")),
    })

    return silver_df_minutes.groupBy("author", "song_title", "_ingested_at_minutes").agg(
        sum("view_count").alias("total_views"),
        sum("like_count").alias("total_likes"),
        sum("comment_count").alias("total_comments"),
    ).select("author", "song_title", "_ingested_at_minutes", "total_views", "total_likes", "total_comments")