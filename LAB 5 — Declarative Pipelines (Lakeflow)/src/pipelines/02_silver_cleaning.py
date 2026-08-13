import dlt
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from ..setup.music_pipeline_setup import (
    bronze_schema_path,
    json_landing_path,
    metadata_music_schema,
    music_metadata_file,
    music_stats_tables,
    bronze_music_metadata_table
)


import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from databricks.sdk.runtime import spark
from delta_per_hour_metrics import compute_per_hour_deltas

@dlt.table(
    name=music_stats_tables["silver"],
    comment="Cleaned YouTube stats. Invalid records are dropped or cause pipeline failure.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_video_id", "video_id IS NOT NULL")
@dlt.expect_or_fail("valid_view_count", "viewCount >= 0")
@dlt.expect_or_fail("valid_like_count", "likeCount >= 0")
@dlt.expect_or_fail("valid_comment_count", "commentCount >= 0")
def silver_youtube_stats():
    df_raw = dlt.read(music_stats_tables["bronze"]).withColumn("_ingested_at", current_timestamp())
    

    columns_to_cast = {
        "_ingested_at": F.col("_ingested_at").cast("timestamp"),
        "published_at": F.col("published_at").cast("timestamp"),
        "video_id": F.col("video_id").cast("string"),
        "album": F.col("album").cast("string"),
        "video_title": F.col("video_title").cast("string"),
        "view_count": F.col("view_count").cast("double"),
        "like_count": F.col("like_count").cast("double"),
        "comment_count": F.col("comment_count").cast("int"),
        "author": F.col("author").cast("string"),
        "song_title": F.col("song_title").cast("string"),
    }

    df_clean = df_raw.withColumns(columns_to_cast)

    return compute_per_hour_deltas(df_clean)





dlt.create_streaming_table(
    name=music_metadata_tables["silver"],
    comment="Oczyszczony słownik metadanych ze śledzeniem zmian w czasie (SCD Type 2). Idealne do analizy np. zmian nazw zespołów (np. Burn the Priest -> Lamb of God).",
    table_properties={"quality": "silver"}
)

@dlt.view(
    name="silver_music_metadata_stg"
)
@dlt.expect_or_drop("valid_metatada_id", "video_id IS NOT NULL")
def silver_music_metadata_stg():
    return dlt.read_stream(music_metadata_tables["bronze"])

dlt.apply_changes(
    target = music_metadata_tables["silver"],
    source = "silver_music_metadata_stg",
    keys=["video_id"],
    track_history_column_list = ["author", "title", "album"]
)