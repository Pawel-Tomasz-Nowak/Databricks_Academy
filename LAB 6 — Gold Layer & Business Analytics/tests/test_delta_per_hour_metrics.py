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
    # 1. Definiujemy jawny schemat wejściowy Silver
    schema = StructType([
        StructField("video_id", StringType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("view_count", IntegerType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("comment_count", IntegerType(), True),
    ])

    # 2. Tworzymy syntetyczny mock pokrywający 5 przypadków brzegowych
    data = [
        # --- VIDEO 1 ---
        # Przypadek 1: Pierwszy snapshot -> Brak historii (delt = None)
        ("vid_1", datetime(2026, 8, 26, 10, 0, 0), 1000, 100, 10),
        # Przypadek 2: Happy Path -> Delta 2h, +1000 views (500/h), +50 likes (25/h)
        ("vid_1", datetime(2026, 8, 26, 12, 0, 0), 2000, 150, 20),
        # Przypadek 3: Zero Time Delta (dt = 0) -> Bezpieczne dzielenie (None)
        ("vid_1", datetime(2026, 8, 26, 12, 0, 0), 2500, 160, 25),
        # Przypadek 4: Wejściowy NULL w metryce (like_count=None) -> like_delta = None
        ("vid_1", datetime(2026, 8, 26, 14, 0, 0), 3000, None, 30),
        # --- VIDEO 2 ---
        # Przypadek 5: Izolacja partycji (Nowe video) -> Brak historii (delt = None)
        ("vid_2", datetime(2026, 8, 26, 15, 0, 0), 500, 50, 5),
    ]

    input_df = spark.createDataFrame(data, schema)

    # 3. Wywołanie transformacji
    result_df = compute_per_hour_deltas(input_df)

    # 4. Pobranie wyników posortowanych deterministycznie
    rows = (
        result_df.orderBy("video_id", "_ingested_at_hours", "view_count")
        .collect()
    )

    # --- Asercje (Weryfikacja wyników) ---

    # Wiersz 0 (vid_1, 10:00) - Pierwszy snapshot
    assert rows[0]["video_id"] == "vid_1"
    assert rows[0]["view_delta_per_hour"] is None
    assert rows[0]["like_delta_per_hour"] is None

    # Wiersz 1 (vid_1, 12:00) - Przyrost normalny (1000 views / 2h = 500.0, 50 likes / 2h = 25.0)
    assert rows[1]["view_delta_per_hour"] == 500.0
    assert rows[1]["like_delta_per_hour"] == 25.0
    assert rows[1]["comment_delta_per_hour"] == 5.0

    # Wiersz 2 (vid_1, 12:00) - Zero Time Delta (dt < 1e-8 -> wynik None, brak ZeroDivisionError)
    assert rows[2]["view_delta_per_hour"] is None

    # Wiersz 3 (vid_1, 14:00) - Obsługa NULL (like_count is None -> like_delta_per_hour is None)
    assert rows[3]["view_delta_per_hour"] == 250.0  # (3000 - 2500) / 2h
    assert rows[3]["like_delta_per_hour"] is None

    # Wiersz 4 (vid_2, 15:00) - Nowa partycja video_id (musi być None, nie łączy z vid_1)
    assert rows[4]["video_id"] == "vid_2"
    assert rows[4]["view_delta_per_hour"] is None
    assert rows[4]["like_delta_per_hour"] is None