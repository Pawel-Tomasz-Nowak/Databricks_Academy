"""Silver layer processing for music analytics pipeline.

Cleaning, validation, and delta calculations for streaming music statistics.
The file is intentionally self-contained because DLT does not expose __file__.
"""

import dlt
from databricks.sdk.runtime import spark
from pyspark.sql import Window, functions as F
from pyspark.sql.functions import abs, col, current_timestamp, lag, round, unix_timestamp, when

catalog_name = spark.conf.get("music_project.catalog_name", "dbr_dev")
music_schema = spark.conf.get("music_project.schema_name", "music_analytics")
volume_name = spark.conf.get("music_project.volume_name", "landing_zone")

music_metadata_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_metadata",
    "silver": f"{catalog_name}.{music_schema}.silver_music_metadata",
    "silver_history": f"{catalog_name}.{music_schema}.silver_music_metadata_history",
    "gold": f"{catalog_name}.{music_schema}.gold_music_metadata",
}

music_stats_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_stats",
    "silver": f"{catalog_name}.{music_schema}.silver_music_stats",
    "gold": f"{catalog_name}.{music_schema}.gold_music_stats",
}


def compute_per_hour_deltas(df):
    seconds_per_hour = 3600.0
    near_zero_threshold = 1e-8

    df = df.withColumn("_ingested_at_hours", unix_timestamp(col("_ingested_at")) / seconds_per_hour)
    video_window = Window.partitionBy("video_id").orderBy("_ingested_at_hours")

    df = df.withColumn(
        "hour_delta",
        when(col("_ingested_at_hours").isNull() | lag("_ingested_at_hours", 1).over(video_window).isNull(), None)
        .otherwise(col("_ingested_at_hours") - lag("_ingested_at_hours", 1).over(video_window)),
    )
    df = df.withColumn(
        "view_delta",
        when(col("view_count").isNull() | lag("view_count", 1).over(video_window).isNull(), None)
        .otherwise(col("view_count") - lag("view_count", 1).over(video_window)),
    )
    df = df.withColumn(
        "like_delta",
        when(col("like_count").isNull() | lag("like_count", 1).over(video_window).isNull(), None)
        .otherwise(col("like_count") - lag("like_count", 1).over(video_window)),
    )
    df = df.withColumn(
        "comment_delta",
        when(col("comment_count").isNull() | lag("comment_count", 1).over(video_window).isNull(), None)
        .otherwise(col("comment_count") - lag("comment_count", 1).over(video_window)),
    )

    df = df.withColumn(
        "view_delta_per_hour",
        when(col("view_delta").isNull() | col("hour_delta").isNull() | (abs(col("hour_delta")) < near_zero_threshold), None)
        .otherwise(round(col("view_delta") / col("hour_delta"), 1)),
    )
    df = df.withColumn(
        "like_delta_per_hour",
        when(col("like_delta").isNull() | col("hour_delta").isNull() | (abs(col("hour_delta")) < near_zero_threshold), None)
        .otherwise(round(col("like_delta") / col("hour_delta"), 1)),
    )
    df = df.withColumn(
        "comment_delta_per_hour",
        when(col("comment_delta").isNull() | col("hour_delta").isNull() | (abs(col("hour_delta")) < near_zero_threshold), None)
        .otherwise(round(col("comment_delta") / col("hour_delta"), 1)),
    )

    return df.drop("hour_delta", "view_delta", "like_delta", "comment_delta")


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
    df_raw = dlt.read(music_stats_tables["bronze"]).withColumn("_ingested_at", current_timestamp())

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
        .withColumn("_ingested_at", current_timestamp())
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