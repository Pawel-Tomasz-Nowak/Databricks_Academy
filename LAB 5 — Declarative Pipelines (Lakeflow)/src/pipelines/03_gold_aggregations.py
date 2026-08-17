"""Gold layer aggregations for music analytics pipeline.

Business-ready aggregated views for author and video engagement metrics.
The file is intentionally self-contained because DLT does not expose __file__.
"""

import dlt
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F

catalog_name = spark.conf.get("music_project.catalog_name", "dbr_dev")
music_schema = spark.conf.get("music_project.schema_name", "music_analytics")
volume_name = spark.conf.get("music_project.volume_name", "landing_zone")

music_stats_tables = {
    "bronze": f"{catalog_name}.{music_schema}.bronze_music_stats",
    "silver": f"{catalog_name}.{music_schema}.silver_music_stats",
    "gold": f"{catalog_name}.{music_schema}.gold_music_stats",
}


def aggregate_author_stats_by_minute(silver_df):
    silver_df_minutes = silver_df.withColumn("_ingested_at_minutes", F.date_trunc("minute", F.col("_ingested_at")))
    return (
        silver_df_minutes.groupBy("author", "_ingested_at_minutes")
        .agg(
            F.countDistinct("video_id").alias("total_videos"),
            F.sum("view_count").alias("total_views"),
            F.sum("like_count").alias("total_likes"),
            F.sum("comment_count").alias("total_comments"),
        )
        .select("author", "_ingested_at_minutes", "total_videos", "total_views", "total_likes", "total_comments")
    )


def aggregate_video_stats_by_minute(silver_df):
    silver_df_minutes = silver_df.withColumn("_ingested_at_minutes", F.date_trunc("minute", F.col("_ingested_at")))
    return (
        silver_df_minutes.groupBy("author", "song_title", "_ingested_at_minutes")
        .agg(
            F.sum("view_count").alias("total_views"),
            F.sum("like_count").alias("total_likes"),
            F.sum("comment_count").alias("total_comments"),
        )
        .select("author", "song_title", "_ingested_at_minutes", "total_views", "total_likes", "total_comments")
    )


@dlt.table(
    name=music_stats_tables["gold"] + "_by_author",
    comment="Tabela biznesowa pokazująca łączna liczbę różnych miar (polubienia liczby koemntarzy liczby wyświetleń)",
    table_properties={"quality": "gold"},
)
def gold_author_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_author_stats_by_minute(facts_df)


@dlt.table(
    name=music_stats_tables["gold"] + "_by_video",
    comment="Tabela biznesowa pokazująca liczbe wyświetleń/polubień/komentarzy danego video",
    table_properties={"quality": "gold"},
)
def gold_video_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_video_stats_by_minute(facts_df)


