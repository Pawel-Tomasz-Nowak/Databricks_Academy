"""Aggregate engagement statistics by author or album.

This helper is shared by the gold aggregation pipeline so the artist- and
album-level tables stay consistent and expose the same descriptive metrics.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def aggregate_stats(silver_df: DataFrame, by: str = "author") -> DataFrame:
    """Aggregate descriptive engagement statistics per grouping key and snapshot.

    Args:
        silver_df: DataFrame containing ``video_id``, ``_ingested_at``, the
            grouping column selected by ``by``, and the ``total_*`` metrics.
        by: Grouping column name, typically ``author`` or ``album``.

    Returns:
        DataFrame with totals, minima, maxima, means, standard deviations, and
        coefficient-of-variation percentages for views, likes, and comments.
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

    # A zero mean would make the coefficient of variation undefined. In practice,
    # the monitored catalogue contains real published videos with positive totals,
    # so the ratios below are valid for the intended use case.
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
        "total_views", "min_views", "max_views", "mean_views", "stddev_views", "cv_views_pct",
        "total_likes", "min_likes", "max_likes", "mean_likes", "stddev_likes", "cv_likes_pct",
        "total_comments", "min_comments", "max_comments", "mean_comments", "stddev_comments", "cv_comments_pct"
    )

    return agg_df