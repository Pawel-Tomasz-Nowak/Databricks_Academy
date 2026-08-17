"""Aggregate per-author engagement stats from the silver music table."""

from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, date_trunc, sum

_TIMESTAMP_GRANULARITY: Final[str] = "minute"


def aggregate_author_stats_by_minute(silver_df: DataFrame) -> DataFrame:
    """Aggregate total engagement metrics per author per ingestion-minute."""
    silver_df_minutes = silver_df.withColumns({
        "_ingested_at_minutes": date_trunc(_TIMESTAMP_GRANULARITY, col("_ingested_at")),
    })

    return silver_df_minutes.groupBy("author", "_ingested_at_minutes").agg(
        countDistinct("video_id").alias("total_videos"),
        sum("view_count").alias("total_views"),
        sum("like_count").alias("total_likes"),
        sum("comment_count").alias("total_comments"),
    ).select("author", "_ingested_at_minutes", "total_videos", "total_views", "total_likes", "total_comments")
