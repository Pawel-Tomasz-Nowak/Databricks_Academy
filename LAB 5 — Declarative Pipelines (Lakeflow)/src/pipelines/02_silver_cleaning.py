"""Silver layer processing for music analytics pipeline.

This stage keeps the bronze data trustworthy enough for downstream reporting.
The expensive part is not formatting itself; it is making each row comparable
across snapshots so the per-hour delta logic can be meaningful.
"""

import dlt
import os, sys

from pyspark.sql import SparkSession


try:
    spark = SparkSession.getActiveSession()
    if spark:
        bundle_root = spark.conf.get("bundle_root", None)
        if bundle_root and bundle_root not in sys.path:
            sys.path.append(bundle_root)
except Exception:
    pass

from src.transformations.delta_per_hour_metrics import compute_per_hour_deltas
from pyspark.sql import functions as F


from src.setup.music_pipeline_setup import (
    music_metadata_tables,
    music_stats_tables
)


@dlt.table(
    name=music_stats_tables["silver"],
    comment="Cleaned YouTube stats. Invalid records are dropped or cause pipeline failure.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_video_id", "video_id IS NOT NULL")
@dlt.expect_or_fail("valid_view_count", "view_count IS NULL OR view_count >= 0")
@dlt.expect_or_fail("valid_like_count", "like_count IS NULL OR like_count >= 0")
@dlt.expect_or_fail("valid_comment_count", "comment_count IS NULL OR comment_count >= 0")
def silver_youtube_stats():
    df_raw = dlt.read(music_stats_tables["bronze"]).withColumn("_ingested_at", F.current_timestamp())

    columns_to_cast = {
        "_ingested_at": F.col("_ingested_at").cast("timestamp"),
        "published_at": F.col("published_at").cast("timestamp"),
        "video_id": F.col("video_id").cast("string"),
        "view_count": F.col("view_count").cast("double"),
        "like_count": F.col("like_count").cast("double"),
        "comment_count": F.col("comment_count").cast("int"),
        "author": F.col("author").cast("string"),
        "song_title": F.col("song_title").cast("string"),
    }

    df_clean = df_raw.withColumns(columns_to_cast).dropDuplicates(["video_id", "_ingested_at"])
    return compute_per_hour_deltas(df_clean)


@dlt.table(
    name=music_metadata_tables["silver"],
    comment="Current music metadata snapshot (materialized view style). The table is recomputed from bronze CSV metadata on each pipeline update.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_metadata_id", "video_id IS NOT NULL AND video_id <> ''")
def silver_music_metadata_current():
    df_raw = dlt.read(music_metadata_tables["bronze"])

    return (
        df_raw
        .withColumn("video_id", F.regexp_extract(F.col("url"), r"v=([a-zA-Z0-9_-]{11})", 1))
        .withColumn("_ingested_at", F.current_timestamp())
        .dropDuplicates(["video_id"])
    )


dlt.create_streaming_table(
    name=music_metadata_tables["silver_history"],
    comment="SCD Type 2 history table for music metadata. Tracks attribute changes for each video_id over time.",
    table_properties={"quality": "silver"},
)

if hasattr(dlt, "create_auto_cdc_from_snapshot_flow"):
    dlt.create_auto_cdc_from_snapshot_flow(
        target=music_metadata_tables["silver_history"],
        source=music_metadata_tables["silver"],
        keys=["video_id"],
        stored_as_scd_type=2,
        track_history_column_list=["url", "author", "title", "album", "album_release_date"],
    )
else:
    dlt.apply_changes_from_snapshot(
        target=music_metadata_tables["silver_history"],
        source=music_metadata_tables["silver"],
        keys=["video_id"],
        stored_as_scd_type=2,
        track_history_column_list=["url", "author", "title", "album", "album_release_date"],
    )