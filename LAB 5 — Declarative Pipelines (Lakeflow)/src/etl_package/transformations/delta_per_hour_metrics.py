from collections.abc import Sequence
from typing import Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.window import WindowSpec
from pyspark.sql.functions import abs, col, lag, round, unix_timestamp, when

_SECONDS_PER_HOUR: Final[float] = 3600.0
_NEAR_ZERO_THRESHOLD: Final[float] = 1e-8


def compute_per_hour_deltas(df: DataFrame) -> DataFrame:
    """Enrich a music-stats DataFrame with per-hour engagement delta columns."""

    def compute_lag_differences(
        df: DataFrame,
        window: WindowSpec,
        col_pairs: Sequence[tuple[str, str]],
    ) -> DataFrame:
        exprs: dict[str, Column] = {
            new_col: when(
                col(base_col).isNull() | lag(base_col, 1).over(window).isNull(),
                None,
            ).otherwise(col(base_col) - lag(base_col, 1).over(window))
            for new_col, base_col in col_pairs
        }
        return df.withColumns(exprs)

    def compute_safe_quotients(
        df: DataFrame,
        triplets: Sequence[tuple[str, str, str]],
    ) -> DataFrame:
        exprs: dict[str, Column] = {
            new_col: when(
                col(num_col).isNull()
                | col(denom_col).isNull()
                | (abs(col(denom_col)) < _NEAR_ZERO_THRESHOLD),
                None,
            ).otherwise(round(col(num_col) / col(denom_col), 1))
            for new_col, num_col, denom_col in triplets
        }
        return df.withColumns(exprs)

    df = df.withColumns({
        "_ingested_at_hours": unix_timestamp(col("_ingested_at")) / _SECONDS_PER_HOUR,
    })

    video_window = Window.partitionBy("video_id").orderBy("_ingested_at_hours")

    df = compute_lag_differences(df, video_window, [
        ("hour_delta", "_ingested_at_hours"),
        ("view_delta", "view_count"),
        ("like_delta", "like_count"),
        ("comment_delta", "comment_count"),
    ])

    df = compute_safe_quotients(df, [
        ("view_delta_per_hour", "view_delta", "hour_delta"),
        ("like_delta_per_hour", "like_delta", "hour_delta"),
        ("comment_delta_per_hour", "comment_delta", "hour_delta"),
    ])

    return df.drop("hour_delta", "view_delta", "like_delta", "comment_delta")
