"""Build the silver layer and lab_7 data-quality controls.

Compared with lab 6, this file adds stronger expectations, a quarantine table
for rejected metadata rows, and an SCD Type 2 history flow for metadata changes.
"""

import os
import sys

from pyspark.sql import SparkSession, Window

# Lakeflow Spark Declarative Pipelines evaluates files dynamically, so __file__
# is not available. The bundle root is injected through spark.conf instead.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
bundle_root = spark.conf.get("bundle.root", "")
possible_roots = [
    bundle_root,
    os.getcwd(),
    os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
]

for path in possible_roots:
    if path and os.path.isdir(os.path.join(path, "src")):
        if path not in sys.path:
            sys.path.insert(0, path)
        break

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from src.transformations.delta_per_hour_metrics import compute_per_hour_deltas
from src.setup.music_pipeline_setup import (
    music_metadata_tables,
    music_stats_tables
)

# These expectations enforce the minimum quality needed for downstream velocity
# metrics and business aggregates to remain interpretable.
silver_youtube_stats_expectations = {
    "valid_author": "author IS NOT NULL AND LEN(trim(author)) > 0",
    "valid_video_id": "video_id IS NOT NULL AND LEN(trim(video_id)) > 0",
    "valid_video_title": "video_title IS NOT NULL AND LEN(trim(video_title)) > 0",
    "comment_count_positive_or_null": "comment_count IS NULL OR comment_count > 0",
    "like_count_positive_or_null": "like_count IS NULL OR like_count > 0",
    "view_count_positive_or_null": "view_count IS NULL OR view_count > 0",
    "valid_published_at": "published_at IS NOT NULL AND published_at <= current_date()"
}



@dp.materialized_view(
    name=music_stats_tables["silver"],
    comment="Cleaned YouTube stats with typed columns, deduplicated snapshots, and per-hour delta metrics.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(silver_youtube_stats_expectations)
def silver_youtube_stats():
    """Cast bronze stats to stable types and compute rate-of-change metrics."""
    df_raw = spark.read.table(music_stats_tables["bronze"])

    columns_to_cast = {
        "_ingested_at": F.col("_ingested_at").cast("timestamp"),
        "published_at": F.col("published_at").cast("timestamp"),
        "video_id": F.col("video_id").cast("string"),
        "view_count": F.col("view_count").cast("double"),
        "like_count": F.col("like_count").cast("double"),
        "comment_count": F.col("comment_count").cast("int"),
        "author": F.col("author").cast("string"),
        "video_title": F.col("video_title").cast("string"),
    }

    df_clean = df_raw.withColumns(columns_to_cast).dropDuplicates(["video_id", "_ingested_at"])
    return compute_per_hour_deltas(df_clean)


stats_reasons_array = F.array([
    F.when(F.expr(f"NOT ({cond})"), F.lit(rule_name))
    for rule_name, cond in silver_youtube_stats_expectations.items()
])
stats_expect_negation = " OR ".join([f"NOT ({cond})" for cond in silver_youtube_stats_expectations.values()])

@dp.table(
    name=music_stats_tables["silver_quarantine"],
    comment="Video stats quarantine table with records rejected from the clean silver snapshot and their rule violations.",
    table_properties={"quality": "silver"}
)
def silver_music_stats_quarantine():
    """Capture rejected video_stats rows together with the violated rule names."""
    df_raw = spark.read.table(music_stats_tables["bronze"])

    df_filtered = df_raw.where(stats_expect_negation)

    quarantine_df = df_filtered.withColumn(
        "_quarantine_reason",
        F.concat_ws(", ", F.array_compact(stats_reasons_array))
    )

    return quarantine_df













music_metadata_expectations = {
    "valid_url": "url IS NOT NULL AND LEN(trim(url)) > 0",
    "valid_title": "title IS NOT NULL AND LEN(trim(title)) > 0",
    "valid_album": "album IS NOT NULL AND LEN(trim(album)) > 0",
    "valid_video_id": "video_id IS NOT NULL AND LEN(trim(video_id)) > 0",
    "valid_album_release_date": "album_release_date IS NOT NULL AND album_release_date <= current_date()",
    "valid_author": "author IS NOT NULL AND LEN(trim(author)) > 0",
}


@dp.materialized_view(
    name=music_metadata_tables["silver"],
    comment="Latest clean metadata snapshot per video_id, keeping only the most recent valid landed record.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(music_metadata_expectations)
def silver_music_metadata_current():
    """Build the latest valid metadata snapshot per ``video_id``.

    The bronze metadata table is append-only, so the silver snapshot uses a
    window ordered by ingestion time and source file path to keep the newest row.
    """
    df_raw = spark.read.table(music_metadata_tables["bronze"])
    latest_row_window = Window.partitionBy("video_id").orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file_path").desc(),
    )

    return (
        df_raw
        .withColumn("video_id", F.regexp_extract(F.col("url"), r"v=([a-zA-Z0-9_-]{11})", 1))
        .filter(F.col("video_id") != "")
        .withColumn("_row_num", F.row_number().over(latest_row_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# Reuse the same expectation rules to capture why a metadata row was excluded
# from the clean silver snapshot.
reasons_array = F.array([
    F.when(F.expr(f"NOT ({cond})"), F.lit(rule_name))
    for rule_name, cond in music_metadata_expectations.items()
])
expect_negation = " OR ".join([f"NOT ({cond})" for cond in music_metadata_expectations.values()])


@dp.table(
    name=music_metadata_tables["silver_quarantine"],
    comment="Metadata quarantine table with records rejected from the clean silver snapshot and their rule violations.",
    table_properties={"quality": "silver"}
)
def silver_music_metadata_quarantine():
    """Capture rejected metadata rows together with the violated rule names."""
    df_raw = spark.read.table(music_metadata_tables["bronze"])

    df_filtered = df_raw.withColumn(
        "video_id",
        F.regexp_extract(F.col("url"), r"v=([a-zA-Z0-9_-]{11})", 1)
    ).where(expect_negation)

    quarantine_df = df_filtered.withColumn(
        "_quarantine_reason",
        F.concat_ws(", ", F.array_compact(reasons_array))
    )

    return quarantine_df


@dp.temporary_view(name="silver_music_metadata_cdc_source")
@dp.expect_all_or_drop(music_metadata_expectations)
def silver_music_metadata_cdc_source():
    """Expose ordered metadata changes as the source for SCD Type 2 history."""
    return (
        spark.readStream.table(music_metadata_tables["bronze"])
        .withColumn("video_id", F.regexp_extract(F.col("url"), r"v=([a-zA-Z0-9_-]{11})", 1))
        .filter(F.col("video_id") != "")
    )


dp.create_streaming_table(
    name=music_metadata_tables["silver_history"],
    comment="SCD Type 2 history table for music metadata. Tracks attribute changes for each video_id over time.",
    table_properties={"quality": "silver"},
)

# A deterministic secondary ordering column is required because multiple CSV
# files can land with the same _ingested_at for a single video_id.
dp.create_auto_cdc_flow(
    target=music_metadata_tables["silver_history"],
    source="silver_music_metadata_cdc_source",
    keys=["video_id"],
    sequence_by=F.struct(F.col("_ingested_at"), F.col("_source_file_path")),
    stored_as_scd_type=2,
    track_history_column_list=["url", "author", "title", "album", "album_release_date"],
    name="silver_music_metadata_history_flow",
)