"""Prepare video-grain metric snapshots for the fact table.

The fact layer keeps one row per ``video_id`` and ingestion timestamp. This file
intentionally avoids extra time bucketing because summing multiple observations
from the same minute would inflate the latest metric values.
"""


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql import Window


# A previous version truncated timestamps and summed rows inside the same minute.
# That could overstate metrics when multiple snapshots for one video landed in a
# short interval. The current implementation preserves the original timestamp and
# only standardizes the metric column names expected by downstream gold tables.
def aggregate_video_stats(silver_df: DataFrame) -> DataFrame:
    """Return video-level metric snapshots with fact-table column names.

    Args:
        silver_df: Silver-layer DataFrame containing ``video_id``,
            ``_ingested_at``, ``view_count``, ``like_count``, and
            ``comment_count``.

    Returns:
        DataFrame with one record per incoming silver snapshot and the renamed
        metric columns ``total_views``, ``total_likes``, and ``total_comments``.
    """
    cols_to_rename = {
        "view_count": "total_views",
        "like_count": "total_likes",
        "comment_count": "total_comments"
    }

    silver_df = silver_df.withColumnsRenamed(cols_to_rename)

    return silver_df.select(
        "video_id",
        "_ingested_at",
        "total_views",
        "total_likes",
        "total_comments",
    )