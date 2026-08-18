"""Persist the fetched YouTube snapshot to the landing volume.

This task is intentionally small and side-effect-heavy: its real value is to
materialize the API payload into a stable landing file that the bronze layer
can pick up reliably. Keeping the write step separate helps the pipeline stay
idempotent and makes the source of truth for the raw feed explicit.
"""
import sys
import os

# BULLETPROOF PATH RESOLUTION FOR STANDARD JOBS
# Databricks asset bundles (DABs) deploy code into a /files/ directory structure.
# We check multiple environmental markers to safely resolve the project root.
def _init_bundle_path() -> None:
    possible_roots = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else "",
    ]
    
    if sys.argv and sys.argv[0]:
        possible_roots.append(os.path.dirname(os.path.abspath(sys.argv[0])))
        
    for base_path in possible_roots:
        if not base_path:
            continue
            
        # Handle standard DABs structure by anchoring to the "/files" root
        if "/files" in base_path:
            root = base_path.split("/files")[0] + "/files"
            if root not in sys.path:
                sys.path.insert(0, root)
            return
            
        # Fallback: look for the 'src' directory explicitly if /files is missing
        if os.path.isdir(os.path.join(base_path, "src")):
            if base_path not in sys.path:
                sys.path.insert(0, base_path)
            return

_init_bundle_path()

import json
from datetime import datetime

from src.setup.music_pipeline_setup import json_landing_path
from src.producer.fetch_yt_video_stats import read_data_from_api


def main() -> None:
    """Executes the API fetch and saves the result to the Databricks Volume."""
    print("[SNAPSHOT RUNNER] Fetching raw YouTube signal batches...")
    
    # Call the imported function to fetch data from the YouTube API
    all_video_responses: list[dict] = read_data_from_api()

    if not all_video_responses:
        print("[SNAPSHOT RUNNER] WARNING: API returned empty response list. Skipping write.")
        return

    # The timestamp keeps each landing file unique and makes retries observable.
    timestamp_str: str = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file_path: str = f"{json_landing_path}/yt_stats_{timestamp_str}.json"

    print(f"[SNAPSHOT RUNNER] Materializing API payload into path: {json_file_path}")
    
    # Ensure the parent directory exists on the driver before writing
    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

    # Write the payload to the Volume as a JSON file
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(all_video_responses, f, ensure_ascii=False, indent=4)
        
    print("[SNAPSHOT RUNNER] Task completed successfully.")


if __name__ == "__main__":
    main()