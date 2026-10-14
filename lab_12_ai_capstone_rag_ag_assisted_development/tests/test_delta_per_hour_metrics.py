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

from src.transformations.delta_per_hour_metrics import compute_per_hour_deltas


def test_compute_per_hour_deltas_edge_cases(spark: SparkSession):
    # 1. Define an explicit silver-layer input schema.
    schema = StructType([
        StructField("video_id", StringType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("view_count", IntegerType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("comment_count", IntegerType(), True),
    ])

    # 2. Create synthetic input covering five edge cases.
    data = [
        # --- VIDEO 1 ---
        # Case 1: first snapshot -> no history, so deltas stay null.
        ("vid_1", datetime(2026, 8, 26, 10, 0, 0), 1000, 100, 10),
        # Case 2: normal growth over 2 hours.
        ("vid_1", datetime(2026, 8, 26, 12, 0, 0), 2000, 150, 20),
        # Case 3: zero time delta -> safe division returns null.
        ("vid_1", datetime(2026, 8, 26, 12, 0, 0), 2500, 160, 25),
        # Case 4: input null in like_count -> like delta stays null.
        ("vid_1", datetime(2026, 8, 26, 14, 0, 0), 3000, None, 30),
        # --- VIDEO 2 ---
        # Case 5: partition isolation for a new video_id.
        ("vid_2", datetime(2026, 8, 26, 15, 0, 0), 500, 50, 5),
    ]

    input_df = spark.createDataFrame(data, schema)

    # 3. Execute the transformation.
    result_df = compute_per_hour_deltas(input_df)

    # 4. Collect deterministically ordered results.
    rows = (
        result_df.orderBy("video_id", "_ingested_at_hours", "view_count")
        .collect()
    )

    # --- Assertions ---

    # Row 0 (vid_1, 10:00): first snapshot.
    assert rows[0]["video_id"] == "vid_1"
    assert rows[0]["view_delta_per_hour"] is None
    assert rows[0]["like_delta_per_hour"] is None

    # Row 1 (vid_1, 12:00): normal growth, 1000 views / 2h = 500.0 and 50 likes / 2h = 25.0.
    assert rows[1]["view_delta_per_hour"] == 500.0
    assert rows[1]["like_delta_per_hour"] == 25.0
    assert rows[1]["comment_delta_per_hour"] == 5.0

    # Row 2 (vid_1, 12:00): zero time delta remains null and avoids ZeroDivisionError.
    assert rows[2]["view_delta_per_hour"] is None

    # Row 3 (vid_1, 14:00): null like_count propagates to a null hourly delta.
    assert rows[3]["view_delta_per_hour"] == 250.0  # (3000 - 2500) / 2h
    assert rows[3]["like_delta_per_hour"] is None

    # Row 4 (vid_2, 15:00): a new video_id starts a fresh partition.
    assert rows[4]["video_id"] == "vid_2"
    assert rows[4]["view_delta_per_hour"] is None
    assert rows[4]["like_delta_per_hour"] is None