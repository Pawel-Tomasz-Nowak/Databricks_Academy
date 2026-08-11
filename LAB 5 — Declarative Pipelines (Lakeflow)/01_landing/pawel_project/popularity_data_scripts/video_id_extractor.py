"""
video_id_extractor.py
---------------------
Utility module for extracting YouTube video IDs from a music-metadata Spark
DataFrame.

The YouTube video ID is parsed from the ``url`` column using a regex that
captures the 11-character ``v=<id>`` query parameter.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract


def find_video_ids(table: DataFrame) -> tuple[DataFrame, list[str]]:
    """Extract YouTube video IDs from the ``url`` column of a metadata table.

    Parses the 11-character ``v=<id>`` query parameter from each URL using
    a regular expression, enriches the DataFrame with a ``video_id`` column,
    and returns both the enriched DataFrame and a deduplicated list of IDs.

    Args:
        table: Spark DataFrame containing at least a ``url`` column with
            YouTube watch URLs (e.g. ``https://www.youtube.com/watch?v=...``).

    Returns:
        A 2-tuple of:

        - The input DataFrame enriched with a ``video_id`` column.
        - A deduplicated list of non-null video ID strings.
    """
    # Parse the YouTube video ID from the URL ``v=`` query parameter.
    table_widh_ids: DataFrame = table.withColumn(
        "video_id", 
        regexp_extract(col("url"), r"v=([a-zA-Z0-9_-]{11})", 1)
    )

    video_id_rows = table_widh_ids.select("video_id").distinct().collect()
    video_ids_list: list[str] = [row.video_id for row in video_id_rows if row.video_id]

    return table_widh_ids, video_ids_list