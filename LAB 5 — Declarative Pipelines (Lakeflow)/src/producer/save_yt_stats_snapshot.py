"""
save_yt_stats_snapshot.py
-------------------------
Point-in-time snapshot runner: fetches the latest YouTube video statistics
via the API and persists them as a timestamped JSON file to the landing Volume.

Execution context: run via ``%run`` or as a Databricks job task.
Requires ``dbutils`` (provided by the cluster).

Side effects:
    - Calls the YouTube Data API v3 (network I/O).
    - Creates ``json_landing_path`` (the logic is in the music_pipeline_setup) if the directory is absent.
    - Writes a single JSON file to ``json_landing_path`` with a compact
      timestamp suffix (e.g. ``yt_stats_20260728_143000.json``).
"""

import json
from datetime import datetime

from src.setup.music_pipeline_setup import json_landing_path
from src.producer.fetch_yt_video_stats import read_data_from_api

# ---------------------------------------------------------------------------
# Fetch and persist
# ---------------------------------------------------------------------------
all_video_responses: list[dict] = read_data_from_api()

timestamp_str: str = datetime.now().strftime("%Y%m%d_%H%M%S")  # compact ISO-8601 suffix
json_file_path: str = f"{json_landing_path}/yt_stats_{timestamp_str}.json"

with open(json_file_path, "w", encoding="utf-8") as f:
    json.dump(all_video_responses, f, ensure_ascii=False, indent=4)