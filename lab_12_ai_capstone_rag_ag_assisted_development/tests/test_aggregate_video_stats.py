from datetime import datetime
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

from src.transformations.aggregate_video_stats import aggregate_video_stats


def test_aggregate_video_stats(spark: SparkSession):
    # 1. Input schema modeled after the silver layer.
    schema = StructType([
        StructField("video_id", StringType(), False),
        StructField("author", StringType(), False),
        StructField("song_title", StringType(), False),
        StructField("view_count", IntegerType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("comment_count", IntegerType(), True),
        StructField("_ingested_at", TimestampType(), False),
    ])

    data = [
        ("vid_1", "Metallica", "One", 10000, 500, 50, datetime(2026, 8, 26, 15, 20, 5)),
        ("vid_1", "Metallica", "One", 10500, 520, 52, datetime(2026, 8, 26, 15, 20, 10)),
        ("vid_2", "Megadeth", "Holy Wars", 8000, 400, 40, datetime(2026, 8, 26, 15, 20, 5)),
    ]

    input_df = spark.createDataFrame(data, schema)

    # 2. Execute the transformation.
    result_df = aggregate_video_stats(input_df)

    # 3. Verify the output schema and confirm no extra attributes remain.
    expected_columns = [
        "video_id",
        "_ingested_at",
        "total_views",
        "total_likes",
        "total_comments",
    ]
    assert result_df.columns == expected_columns

    # 4. Verify that rows are preserved without accidental aggregation or truncation.
    rows = result_df.orderBy("video_id", "_ingested_at").collect()
    assert len(rows) == 3

    # Row 0: vid_1 at 15:20:05.
    assert rows[0]["video_id"] == "vid_1"
    assert rows[0]["_ingested_at"] == datetime(2026, 8, 26, 15, 20, 5)
    assert rows[0]["total_views"] == 10000
    assert rows[0]["total_likes"] == 500
    assert rows[0]["total_comments"] == 50

    # Row 1: vid_1 at 15:20:10, preserving the exact snapshot timestamp.
    assert rows[1]["video_id"] == "vid_1"
    assert rows[1]["_ingested_at"] == datetime(2026, 8, 26, 15, 20, 10)
    assert rows[1]["total_views"] == 10500

    # Row 2: vid_2.
    assert rows[2]["video_id"] == "vid_2"
    assert rows[2]["total_views"] == 8000