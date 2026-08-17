import dlt

from etl_package.setup.music_pipeline_setup import music_stats_tables
from etl_package.transformations.aggregate_author_stats_by_minute import aggregate_author_stats_by_minute
from etl_package.transformations.aggregate_video_stats_by_minute import aggregate_video_stats_by_minute


@dlt.table(
    name=music_stats_tables["gold"] + "_by_author",
    comment="Business table showing total engagement metrics by author per minute.",
    table_properties={"quality": "gold"},
)
def gold_author_stats_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_author_stats_by_minute(facts_df)


@dlt.table(
    name=music_stats_tables["gold"] + "_by_video",
    comment="Business table showing views/likes/comments per video per minute.",
    table_properties={"quality": "gold"},
)
def gold_video_stats_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])
    return aggregate_video_stats_by_minute(facts_df)
