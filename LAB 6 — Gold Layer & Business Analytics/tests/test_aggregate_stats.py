from datetime import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from path_resolver import _init_bundle_path
_init_bundle_path()

from src.transformations.aggregate_stats import aggregate_stats

@pytest.fixture
def sample_silver_df(spark: SparkSession):
    schema = StructType([
        StructField("author", StringType(), False),
        StructField("album", StringType(), False),
        StructField("video_id", StringType(), False),
        StructField("total_views", IntegerType(), False),
        StructField("total_likes", IntegerType(), False),
        StructField("total_comments", IntegerType(), False),
        StructField("_ingested_at", TimestampType(), False),
    ])

    data = [
        # Metallica - Album "Black Album" - 2 wideo w tej samej minucie
        (
            "Metallica",
            "Black Album",
            "vid_1",
            100,
            10,
            2,
            datetime(2026, 8, 26, 16, 0, 0),
        ),
        (
            "Metallica",
            "Black Album",
            "vid_2",
            300,
            30,
            6,
            datetime(2026, 8, 26, 16, 0, 0),
        ),
        # Megadeth - Album "Rust in Peace" - 1 wideo (pojedyncza próba)
        (
            "Megadeth",
            "Rust in Peace",
            "vid_3",
            500,
            50,
            5,
            datetime(2026, 8, 26, 16, 0, 0),
        ),
    ]
    return spark.createDataFrame(data, schema)


def test_aggregate_stats_by_author(spark: SparkSession, sample_silver_df):
    result_df = aggregate_stats(sample_silver_df, by="author")

    assert "author" in result_df.columns
    assert "album" not in result_df.columns

    rows = result_df.orderBy("author").collect()
    assert len(rows) == 2

    # Megadeth (1 wideo) -> stddev jest None, cv jest None
    megadeth = rows[0]
    assert megadeth["author"] == "Megadeth"
    assert megadeth["total_videos"] == 1
    assert megadeth["total_views"] == 500
    assert megadeth["mean_views"] == 500.0
    assert megadeth["stddev_views"] is None
    assert megadeth["cv_views_pct"] is None

    # Metallica (2 wideo: 100 i 300)
    # Mean: 200.0, Sample StdDev: sqrt(((100-200)^2 + (300-200)^2) / 1) ≈ 141.421356
    # CV%: (141.421356 / 200.0) * 100 ≈ 70.71%
    metallica = rows[1]
    assert metallica["author"] == "Metallica"
    assert metallica["total_videos"] == 2
    assert metallica["total_views"] == 400
    assert metallica["min_views"] == 100
    assert metallica["max_views"] == 300
    assert metallica["mean_views"] == 200.0
    assert pytest.approx(metallica["stddev_views"], rel=1e-3) == 141.421
    assert pytest.approx(metallica["cv_views_pct"], rel=1e-3) == 70.710


def test_aggregate_stats_by_album(spark: SparkSession, sample_silver_df):
    result_df = aggregate_stats(sample_silver_df, by="album")

    assert "album" in result_df.columns
    assert "author" not in result_df.columns

    rows = result_df.orderBy("album").collect()
    assert len(rows) == 2
    assert rows[0]["album"] == "Black Album"
    assert rows[0]["total_videos"] == 2
    assert rows[1]["album"] == "Rust in Peace"
    assert rows[1]["total_videos"] == 1