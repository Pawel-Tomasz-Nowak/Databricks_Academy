"""
fetch_yt_video_stats.py
-----------------------
YouTube Data API v3 client for fetching video statistics in batches.

Reads the music metadata table from the bronze layer, extracts video IDs,
calls the YouTube Data API v3 in configurable-sized batches, and returns
a list of enriched video-statistics records ready for downstream persistence.

Execution context: imported as a module by the snapshot runner or run
directly as a Databricks job task. Requires an active SparkSession.
"""

import requests
from ..setup.music_pipeline_setup import music_metadata_tables, spark, yt_api_key, yt_video_url
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql import DataFrame
from datetime import datetime

silver_music_metadata_table = music_metadata_tables["silver"]

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

    video_id_rows = table_widh_ids.select("video_id").collect()
    
    video_ids_list: list[str] = [row.video_id for row in video_id_rows if row.video_id]

    return table_widh_ids, video_ids_list




def read_data_from_api(batch_size: int = 50) -> list[dict]:
    """Fetch video statistics for all tracks in the music metadata table.

    Reads ``silver_music_metadata_table``, extracts YouTube video IDs,
    queries the YouTube Data API v3 in batches of up to ``batch_size``
    items, and returns a list of enriched statistics records.

    Args:
        batch_size: Maximum number of video IDs per API request. The
            YouTube API enforces a hard cap of 50. Defaults to 50.

    Returns:
        List of dicts, one per video, with keys: ``_ingested_at``,
        ``video_id``, ``album``, ``published_at``, ``video_title``,
        ``view_count``, ``like_count``, ``comment_count``, ``author``,
        ``song_title``. Non-numeric counts are coerced to ``None``.
    """
    metadata_table = spark.read.table(silver_music_metadata_table)
    metadata_table, video_ids_list = find_video_ids(metadata_table)

    # Precompute video_id -> [album, title, author] mapping for fast lookup
    video_id_attr_map = {
        row.video_id: [row.album, row.title, row.author]
        for row in metadata_table.select("video_id", "album", "title", "author")
        .filter(col("video_id").isNotNull())
        .toLocalIterator()
    }

    # Split the videos into predefined-sized batches
    id_batches: list[list[str]] = [
        video_ids_list[i : i + batch_size]
        for i in range(0, len(video_ids_list) + 1, batch_size)
    ]

    all_video_responses: list[dict] = []

    for batch in id_batches:
        batch_ids_str = ",".join(batch)
        
        params = {
            "part": "snippet, statistics",
            "id": batch_ids_str,
            "key": yt_api_key
        }
        
        response = requests.get(yt_video_url, params=params)
        
        if response.status_code == 200:
            items = response.json().get("items", [])

            for item in items:
                item_snippet = item.get("snippet", {})
                item_statistics = item.get("statistics", {})

                video_id = item.get("id")
                published_at = item_snippet.get("publishedAt")
                title:str = item_snippet.get("title")

                viewCount = item_statistics.get("viewCount")
                likeCount = item_statistics.get("likeCount")
                commentCount = item_statistics.get("commentCount")

                album, song_title, author = video_id_attr_map.get(video_id, [None, None, None])

                video_response = {
                    "video_id": video_id,
                    "album": album,
                    "published_at": published_at,
                    "video_title": title,
                    "view_count": viewCount,
                    "like_count": likeCount,
                    "comment_count": commentCount,
                    "author": author,
                    "song_title": song_title,
                }
                all_video_responses.append(video_response)

        else:
            print("Error ")

    return all_video_responses