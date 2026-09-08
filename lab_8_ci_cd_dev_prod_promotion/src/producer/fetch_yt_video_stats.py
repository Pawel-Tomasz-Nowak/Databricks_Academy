"""
fetch_yt_video_stats.py
-----------------------
YouTube Data API v3 client for fetching video statistics in batches.

Reads the music metadata table from the bronze layer, extracts video IDs,
calls the YouTube Data API v3 in configurable-sized batches, and returns
a list of enriched video-statistics records ready for downstream persistence.
"""
import argparse
import os
import requests
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col, regexp_extract

from src.setup.music_pipeline_setup import (
    metadata_music_schema,
    music_metadata_file,
    yt_video_url,
)

def get_yt_api_key() -> str:
    """Fetches the YouTube API key from Databricks Secrets dynamically.
    
    This ensures that secrets are only fetched when this specific script is executed
    by the orchestrator preventing unauthorized access errors when other parts of 
    the project import the setup module.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret_scope", default=os.environ.get("DBRICKS_SECRET_SCOPE"))
    parser.add_argument("--secret_key", default=os.environ.get("DBRICKS_SECRET_KEY"))
    
    # parse_known_args allows ignoring other flags passed by the Job Definition
    args, _ = parser.parse_known_args()
    
    if not args.secret_scope or not args.secret_key:
        raise ValueError("Missing --secret_scope or --secret_key arguments in Task parameters!")

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    
    return dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)


def find_video_ids(csv_file_path: str) -> list[str]:
    """Extract YouTube video IDs from the ``url`` column of a metadata table.

    Parses the 11-character ``v=<id>`` query parameter from each URL using
    a regular expression, enriches the DataFrame with a ``video_id`` column,
    and returns a deduplicated list of IDs.
    """
    spark = SparkSession.builder.getOrCreate()
    print(f"[API CLIENT] Reading metadata file from: {csv_file_path}")
    
    # Parse the YouTube video ID from the URL query parameter
    tbl = spark.read.csv(csv_file_path, header=True, schema=metadata_music_schema).select("url")

    tbl_with_ids = tbl.withColumn(
        "video_id", 
        regexp_extract(col("url"), r"v=([a-zA-Z0-9_-]{11})", 1)
    )
  
    video_id_rows = tbl_with_ids.select("video_id").distinct().collect()
    video_ids_cleaned = [row["video_id"] for row in video_id_rows if row["video_id"]]

    print(f"[API CLIENT] Found {len(video_ids_cleaned)} unique video IDs to fetch.")
    return video_ids_cleaned


def read_data_from_api(batch_size: int = 50) -> list[dict]:
    """Fetch video statistics for all tracks in the music metadata table.

    Reads metadata, extracts YouTube video IDs, queries the YouTube Data API v3 
    in batches of up to ``batch_size`` items, and returns a list of enriched 
    statistics records.
    """
    yt_api_key = get_yt_api_key()
    
    video_ids_list = find_video_ids(music_metadata_file)

    if not video_ids_list:
        print("[API CLIENT] No VALID videos found in metadata source.")
        return []

    id_batches: list[list[str]] = [
        video_ids_list[i : i + batch_size]
        for i in range(0, len(video_ids_list), batch_size)
    ]

    all_video_responses: list[dict] = []
    print(f"[API CLIENT] Starting API batch requests (Batch size: {batch_size})...")

    for batch in id_batches:
        batch_ids_str = ",".join(batch)
        
        params = {
            "part": "snippet,statistics",
            "id": batch_ids_str,
            "key": yt_api_key
        }
        
        response = requests.get(yt_video_url, params=params)
        
        if response.status_code == 200:
            items = response.json().get("items", [])

            for item in items:
                item_snippet = item.get("snippet", {})
                item_statistics = item.get("statistics", {})

                video_response = {
                    "video_id": item.get("id"),
                    "published_at": item_snippet.get("publishedAt"),
                    "author": item_snippet.get("channelTitle"),
                    "video_title": item_snippet.get("title"),
                    "view_count": item_statistics.get("viewCount"),
                    "like_count": item_statistics.get("likeCount"),
                    "comment_count": item_statistics.get("commentCount"),
                    "_ingested_at": datetime.utcnow().isoformat(),
                }
                all_video_responses.append(video_response)
        else:
            print(f"[API CLIENT] Youtube API error: Status {response.status_code} - {response.text}")

    print(f"[API CLIENT] Successfully fetched {len(all_video_responses)} records from YouTube API.")
    return all_video_responses