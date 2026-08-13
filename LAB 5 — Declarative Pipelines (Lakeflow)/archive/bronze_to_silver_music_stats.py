"""Transform bronze music-stats snapshots into the silver layer.

Reads the bronze music-stats Delta table, applies column casting and
deduplication, enriches each snapshot with per-hour engagement delta
metrics, then appends the result to the silver music-stats table.

Typical usage:
    Run directly as a script or via ``%run`` from a controlling notebook.
    Requires an active SparkSession (``spark``) and the project config
    directory on ``sys.path``.
"""

from typing import Final
import sys
import os

# Add project root to path for absolute imports
project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.setup.music_pipeline_setup import music_stats_tables, spark
from archive.preprocess_bronze_stats import cast_and_deduplicate
from src.transformations.delta_per_hour_metrics import compute_per_hour_deltas

_SILVER_COLUMNS: Final[list[str]] = [
    "author",
    "video_id",
    "album",
    "song_title",
    "_ingested_at",
    "_ingested_at_hours",
    "view_count",
    "like_count",
    "comment_count",
    "view_delta_per_hour",
    "like_delta_per_hour",
    "comment_delta_per_hour",
]

# bronze_music_tbl_path: str = music_stats_tables["bronze"]
# silver_music_tbl_path: str = music_stats_tables["silver"]

# bronze_df = spark.read.table(bronze_music_tbl_path)
# preprocessed_df = cast_and_deduplicate(bronze_df)
# silver_df = compute_per_hour_deltas(preprocessed_df).select(*_SILVER_COLUMNS)

# silver_df.write.format("delta").mode("append").saveAsTable(silver_music_tbl_path)