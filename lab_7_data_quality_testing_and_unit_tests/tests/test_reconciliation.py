from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pytest

from path_resolver import _init_bundle_path
_init_bundle_path()


catalog = "dbr_dev"
schema = "music_analytics"

# Keep the test table mapping aligned with the runtime setup module so the
# reconciliation tests validate the same physical assets as the job task.
music_metadata_tables = {
    "bronze": f"{catalog}.{schema}.bronze_music_metadata",
    "silver": f"{catalog}.{schema}.silver_music_metadata",
    "silver_history": f"{catalog}.{schema}.silver_music_metadata_history",
    "silver_quarantine": f"{catalog}.{schema}.silver_music_quarantine",
    "gold": f"{catalog}.{schema}.dim_music_metadata",
}

music_stats_table = {
    "bronze": f"{catalog}.{schema}.bronze_music_stats",
    "silver": f"{catalog}.{schema}.silver_music_stats",
    "fact": f"{catalog}.{schema}.fact_music_stats",  # Video-level fact snapshot.
    "gold_album": f"{catalog}.{schema}.gold_album_music_stats",  # Album-level rollup.
    "gold_author": f"{catalog}.{schema}.gold_author_music_stats"  # Artist-level rollup.
}


def test_reconciliation_bronze_to_silver_music_metadata(spark: SparkSession):
    try:
        bronze_df = spark.read.table(music_metadata_tables["bronze"])
        silver_clean_df = spark.read.table(music_metadata_tables["silver"])
        silver_quarantine = spark.read.table(music_metadata_tables["silver_quarantine"])
    except:
        pytest.fail("Couldn't read some of the tables!")


    bronze_count = bronze_df.select("url").distinct().count()
    silver_clean_count = silver_clean_df.count()
    silver_quarantine_count = silver_quarantine.count()


    assert bronze_count == (silver_clean_count + silver_quarantine_count), (
        f"Silent data loss detected!\n"
        f"Bronze ({bronze_count}) != Silver ({silver_clean_count}) + Quarantine ({silver_quarantine_count})"
    )


def test_reconciliation_total_views_album_vs_author_songs(spark: SparkSession):
    try:
        video_df = spark.read.table(music_stats_table["fact"])
        author_df = spark.read.table(music_stats_table["gold_author"])
        album_df = spark.read.table(music_stats_table["gold_album"])
    except Exception as exc:
        pytest.fail(f"Could not read one of the Gold tables: {exc}")


  
    video_views = video_df.agg(F.sum("total_views")).first()[0] or 0
    author_views = author_df.agg(F.sum("total_views")).first()[0] or 0
    album_views = album_df.agg(F.sum("total_views")).first()[0] or 0


    assert video_views == author_views == album_views, (
        f"Silent data loss detected across Gold aggregates!\n"
        f"Fact Table Views:   {video_views}\n"
        f"Author Views:       {author_views}\n"
        f"Album Views:        {album_views}"
    )
