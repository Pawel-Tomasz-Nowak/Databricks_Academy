from src.setup.music_pipeline_setup import (
    music_stats_tables

)
from src.transformations.aggregate_author_stats_by_minute import aggregate_author_stats_by_minute
from src.transformations.aggregate_video_stats_by_minute import aggregate_video_stats_by_minute

import dlt

@dlt.table(
    name=music_stats_tables["gold"]+"_by_author",
    comment="Tabela biznesowa pokazująca łączna liczbę różnych miar (polubienia liczby koemntarzy liczby wyświetleń)",
    table_properties={"quality": "gold"}
)
def gold_author_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])

    return aggregate_author_stats_by_minute(facts_df)


@dlt.table(
    name=music_stats_tables["gold"]+"_by_video",
    comment="Tabela biznesowa pokazująca liczbe wyświetleń/polubień/komentarzy danego video",
    table_properties={"quality": "gold"}
)
def gold_video_stast_by_minute():
    facts_df = dlt.read(music_stats_tables["silver"])

    return aggregate_video_stats_by_minute(facts_df)


