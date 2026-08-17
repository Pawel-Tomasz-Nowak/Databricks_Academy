"""Gold layer aggregations for music analytics pipeline.

The value of the gold layer is not the raw row count; it is the business-ready
view over time. Minute-level aggregation makes the trend readable without
loading the full streaming history back into a reporting tool.
"""

import dlt
import os
import sys


bundle_root_dir = os.getcwd() 

if bundle_root_dir not in sys.path:
    sys.path.insert(0, bundle_root_dir)


from src.setup.music_pipeline_setup import (
    music_stats_tables,
)
from src.transformations import aggregate_author_stats_by_minute, aggregate_video_stats_by_minute



@dlt.table(
    name=music_stats_tables["gold"] + "_by_author",
    comment="Business view summarising total engagement metrics for authors across minute-level snapshots.",
    table_properties={"quality": "gold"},
)
def gold_author_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_author_stats_by_minute(facts_df)


@dlt.table(
    name=music_stats_tables["gold"] + "_by_video",
    comment="Business view showing the total views, likes, and comments for each video by minute.",
    table_properties={"quality": "gold"},
)
def gold_video_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_video_stats_by_minute(facts_df)


