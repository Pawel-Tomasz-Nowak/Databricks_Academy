"""Aggregate engagement statistics for a chosen business dimension.

The same helper is reused by the author- and album-level gold views. It keeps
all group-level metrics aligned on the original ingestion timestamp stored in
``fact_music_stats``.
"""

from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
)


def aggregate_stats(silver_df: DataFrame, by: str = "author") -> DataFrame:
    """Return descriptive engagement metrics grouped by a business dimension.

    Args:
        silver_df: DataFrame that already contains ``video_id``, ``_ingested_at``,
            and the ``total_*`` metric columns, plus the grouping column selected
            by ``by``.
        by: Column name that defines the business grain of the aggregation, such
            as ``author`` or ``album``.

    Returns:
        DataFrame with counts, totals, min/max values, means, standard
        deviations, and coefficient-of-variation percentages for views, likes,
        and comments.
    """
    from pyspark.sql.functions import min, max, mean, stddev

    agg_df = silver_df.groupBy(by, "_ingested_at").agg(
        countDistinct("video_id").alias("total_videos"),
        sum("total_views").alias("total_views"),
        min("total_views").alias("min_views"),
        max("total_views").alias("max_views"),
        mean("total_views").alias("mean_views"),
        stddev("total_views").alias("stddev_views"),
        sum("total_likes").alias("total_likes"),
        min("total_likes").alias("min_likes"),
        max("total_likes").alias("max_likes"),
        mean("total_likes").alias("mean_likes"),
        stddev("total_likes").alias("stddev_likes"),
        sum("total_comments").alias("total_comments"),
        min("total_comments").alias("min_comments"),
        max("total_comments").alias("max_comments"),
        mean("total_comments").alias("mean_comments"),
        stddev("total_comments").alias("stddev_comments"),
    )

    # The coefficient of variation is left as a direct Spark expression. If a
    # mean is null or zero, Spark propagates null rather than changing the logic
    # with custom guards.
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
        by, "_ingested_at", "total_videos",
        "total_views", "min_views", "max_views", "mean_views", "stddev_views", "cv_views_pct",
        "total_likes", "min_likes", "max_likes", "mean_likes", "stddev_likes", "cv_likes_pct",
        "total_comments", "min_comments", "max_comments", "mean_comments", "stddev_comments", "cv_comments_pct"
    )

    return agg_df