"""Persist the fetched YouTube snapshot to the landing volume.

This task is intentionally small and side-effect-heavy: its real value is to
materialize the API payload into a stable landing file that the DLT bronze layer
can pick up reliably. Keeping the write step separate helps the pipeline stay
idempotent and makes the source of truth for the raw feed explicit.
"""
import sys, os

def _init_bundle_path():
    root = None
    cwd = os.getcwd()
    if "/files" in cwd:
        root = cwd.split("/files")[0] + "/files"
    elif sys.argv and "/files" in sys.argv[0]:
        root = os.path.abspath(sys.argv[0]).split("/files")[0] + "/files"
    
    if root and root not in sys.path:
        sys.path.insert(0, root)

_init_bundle_path()


import json
from datetime import datetime

from src.setup.music_pipeline_setup import json_landing_path
from src.producer.fetch_yt_video_stats import read_data_from_api

# ---------------------------------------------------------------------------
# Fetch and persist
# ---------------------------------------------------------------------------
all_video_responses: list[dict] = read_data_from_api()

timestamp_str: str = datetime.now().strftime("%Y%m%d_%H%M%S")
# The timestamp keeps each landing file unique and makes retries observable.
json_file_path: str = f"{json_landing_path}/yt_stats_{timestamp_str}.json"

with open(json_file_path, "w", encoding="utf-8") as f:
    json.dump(all_video_responses, f, ensure_ascii=False, indent=4)