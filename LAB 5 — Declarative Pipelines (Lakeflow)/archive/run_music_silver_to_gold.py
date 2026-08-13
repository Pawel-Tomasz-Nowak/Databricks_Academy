"""Orchestrate the silver-to-gold transformation for music statistics.

Reads the silver music table, applies per-video and per-author aggregations,
and appends the results to their respective gold Delta tables.
"""

from ..setup.music_pipeline_setup import music_stats_tables, spark

from .aggregate_video_stats_by_minute import aggregate_video_stats_by_minute
from .aggregate_author_stats_by_minute import aggregate_author_stats_by_minute

silver_music_tbl_path: str = music_stats_tables["silver"]
gold_music_tbl_path: str = music_stats_tables["gold"]

silver_table = spark.read.table(silver_music_tbl_path)

gold_table_by_video = aggregate_video_stats_by_minute(silver_table)
gold_table_by_author = aggregate_author_stats_by_minute(silver_table)

gold_tbl_path_by_video: str = gold_music_tbl_path + "_by_video"
gold_tbl_path_by_author: str = gold_music_tbl_path + "_by_author"

gold_table_by_video.write.format("delta").mode("append").saveAsTable(gold_tbl_path_by_video)
gold_table_by_author.write.format("delta").mode("append").saveAsTable(gold_tbl_path_by_author)