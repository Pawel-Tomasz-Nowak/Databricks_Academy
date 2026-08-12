from pyspark.sql import Column, DataFrame
from pyspark.sql.types import LongType, IntegerType, TimestampType, StringType
from pyspark.sql.functions import col


def cast_and_deduplicate(bronze_df: DataFrame) -> DataFrame:
    """Cast bronze music-stats columns and remove duplicate snapshots.

    Selects only the columns relevant to the silver layer, casts each
    to its target type, then drops rows that are redundant within the
    same ingestion timestamp (same video snapshot captured more than once).

    Args:
        bronze_df: Raw bronze DataFrame produced by the Auto Loader
            streaming ingestion. All metric columns arrive as strings.

    Returns:
        A deduplicated DataFrame with correctly typed columns, ready
        to be written to the silver music-stats table.
    """
    columns_to_cast: dict[str, Column] = {
        "_ingested_at": col("_ingested_at").cast(TimestampType()),
        "video_id": col("video_id").cast(StringType()),
        "album": col("album").cast(StringType()),
        "video_title": col("video_title").cast(StringType()),
        "view_count": col("view_count").cast(LongType()),
        "like_count": col("like_count").cast(LongType()),
        "comment_count": col("comment_count").cast(IntegerType()),
        "author": col("author").cast(StringType()),
        "song_title": col("song_title").cast(StringType()),
    }

    silver_df = (
        bronze_df
        .select(*columns_to_cast.keys())
        .withColumns(columns_to_cast)
    )

    # Deduplication key is (video_id, _ingested_at): one video may appear across
    # many snapshots at different ingestion times, but never twice in the same one.
    return silver_df.dropDuplicates(["video_id", "_ingested_at"])



