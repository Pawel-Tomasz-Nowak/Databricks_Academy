"""Project silver snapshots into the gold fact-table schema.

Lab 7 intentionally keeps the full ingestion timestamp instead of truncating it.
That preserves the exact observation points needed by reconciliation checks and
prevents accidental over-aggregation when multiple snapshots arrive in the same
minute.
"""

from pyspark.sql import DataFrame


def aggregate_video_stats(silver_df: DataFrame) -> DataFrame:
    """Rename silver metrics into the fact-table column convention.

    Args:
        silver_df: Silver-layer DataFrame containing ``video_id``,
            ``_ingested_at``, and the raw metric columns ``view_count``,
            ``like_count``, and ``comment_count``.

    Returns:
        DataFrame with one row per original silver snapshot and the renamed
        metric columns ``total_views``, ``total_likes``, and ``total_comments``.
    """

    # The full snapshot timestamp must be preserved. If two records land within
    # the same minute, truncation would merge them and inflate totals. Taking the
    # maximum is not safe either because YouTube counters can legitimately fall
    # after invalid traffic is removed.
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