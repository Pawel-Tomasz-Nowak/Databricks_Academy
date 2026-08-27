from pyspark.sql import SparkSession
from pyspark.sql.functions import count_distinct
import pytest

from path_resolver import _init_bundle_path
_init_bundle_path()


catalog = "dbr_dev"
schema = "music_analytics"

music_metadata_tables= {
            "bronze": f"{catalog}.{schema}.bronze_music_metadata",
            "silver": f"{catalog}.{schema}.silver_music_metadata",
            "silver_history": f"{catalog}.{schema}.silver_music_metadata_history",
            "silver_quarantine": f"{catalog}.{schema}.silver_music_quarantine",
            "gold": f"{catalog}.{schema}.dim_music_metadata",
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