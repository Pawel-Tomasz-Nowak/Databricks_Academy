"""
fetch_yt_video_stats.py
-----------------------
YouTube Data API v3 client for fetching video statistics in batches.

Reads the music metadata table from the bronze layer, extracts video IDs,
calls the YouTube Data API v3 in configurable-sized batches, and returns
a list of enriched video-statistics records ready for downstream persistence.
"""

from datetime import datetime

import requests
from pyspark.sql.functions import col, regexp_extract

from etl_package.setup.music_pipeline_setup import (
    metadata_music_schema,
    music_metadata_file,
    spark,
    yt_api_key,
    yt_video_url,
)


def find_video_ids(csv_file_path: str) -> list[str]:
    """Extract YouTube video IDs from the url column of a music metadata file."""
    tbl = spark.read.csv(csv_file_path, header=True, schema=metadata_music_schema).select("url")
    tbl_with_ids = tbl.withColumn(
        "video_id",
        regexp_extract(col("url"), r"v=([a-zA-Z0-9_-]{11})", 1),
    )
    video_id_rows = tbl_with_ids.select("video_id").distinct().collect()
    return [row["video_id"] for row in video_id_rows if row["video_id"]]


def read_data_from_api(batch_size: int = 50) -> list[dict]:
    """Fetch video statistics for all tracks in the music metadata table."""
    if not yt_api_key:
        raise EnvironmentError(
            "YT_API_KEY is not configured. Set it as a cluster env var or provide DBRICKS_SECRET_SCOPE/DBRICKS_SECRET_KEY."
        )

    video_ids_list = find_video_ids(music_metadata_file)
    if not video_ids_list:
        print("No VALID videos found")
        return []

    id_batches: list[list[str]] = [
        video_ids_list[i : i + batch_size]
        for i in range(0, len(video_ids_list), batch_size)
    ]

    all_video_responses: list[dict] = []

    for batch in id_batches:
        batch_ids_str = ",".join(batch)
        params = {
            "part": "snippet, statistics",
            "id": batch_ids_str,
            "key": yt_api_key,
        }

        response = requests.get(yt_video_url, params=params)

        if response.status_code == 200:
            items = response.json().get("items", [])

            for item in items:
                item_snippet = item.get("snippet", {})
                item_statistics = item.get("statistics", {})
                video_id = item.get("id")
                published_at = item_snippet.get("publishedAt", None)
                author = item_snippet.get("channelTitle", None)
                video_title: str = item_snippet.get("title", None)

                video_response = {
                    "video_id": video_id,
                    "published_at": published_at,
                    "author": author,
                    "song_title": video_title,
                    "view_count": item_statistics.get("viewCount"),
                    "like_count": item_statistics.get("likeCount"),
                    "comment_count": item_statistics.get("commentCount"),
                    "_ingested_at": datetime.utcnow().isoformat(),
                }
                all_video_responses.append(video_response)
        else:
            print(f"Youtube API error: Status {response.status_code} - {response.text}")

    return all_video_responses
