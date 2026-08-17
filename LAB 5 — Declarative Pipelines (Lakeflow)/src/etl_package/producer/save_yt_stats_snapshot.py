"""Persist the latest YouTube stats snapshot to a landing volume."""

import json
from datetime import datetime

from etl_package.setup.music_pipeline_setup import json_landing_path
from etl_package.producer.fetch_yt_video_stats import read_data_from_api


all_video_responses = read_data_from_api()

timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
json_file_path = f"{json_landing_path}/yt_stats_{timestamp_str}.json"

with open(json_file_path, "w", encoding="utf-8") as f:
    json.dump(all_video_responses, f, ensure_ascii=False, indent=4)
