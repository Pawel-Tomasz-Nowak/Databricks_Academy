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

import sys

sys.path.insert(0, "../../../00_setup/pawel_project")


from video_id_extractor import find_video_ids
import requests
from music_pipeline_setup import silver_music_metadata_table, spark, yt_api_key, yt_video_url
from pyspark.sql.functions import col
from datetime import datetime



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

    def safe_int(val: object) -> int | None:
        try:
            return int(val) if val is not None and str(val).isdigit() else None
        except Exception:
            return None

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

                viewCount = safe_int(item_statistics.get("viewCount"))
                likeCount = safe_int(item_statistics.get("likeCount"))
                commentCount = safe_int(item_statistics.get("commentCount"))

                album, song_title, author = video_id_attr_map.get(video_id, [None, None, None])

                video_response = {
                    "_ingested_at": datetime.now().isoformat(timespec='seconds'),
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