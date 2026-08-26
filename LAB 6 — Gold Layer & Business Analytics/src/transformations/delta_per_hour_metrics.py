from collections.abc import Sequence
from typing import Final

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.window import WindowSpec
import pyspark.sql.functions as F

_SECONDS_PER_HOUR: Final[float] = 3600.0
_NEAR_ZERO_THRESHOLD: Final[float] = 1e-8

def compute_per_hour_deltas(df: DataFrame) -> DataFrame:
    """Enrich a music-stats DataFrame with per-hour engagement delta columns.

    For each video, computes the change in view, like, and comment counts
    between consecutive ingestion snapshots, then normalises each delta by
    the elapsed time in hours between those snapshots.

    Args:
        df: Silver-layer DataFrame containing ``video_id``,
            ``_ingested_at`` (TimestampType), ``view_count``,
            ``like_count``, and ``comment_count``.

    Returns:
        Input DataFrame augmented with ``_ingested_at_hours``,
        ``view_delta_per_hour``, ``like_delta_per_hour``, and
        ``comment_delta_per_hour``. Intermediate raw-delta columns are
        dropped before returning.
    """

    def compute_lag_differences(
        df: DataFrame,
        window: WindowSpec,
        col_pairs: Sequence[tuple[str, str]],
    ) -> DataFrame:
        """Add null-safe lag-difference columns to a DataFrame.

        Args:
            df: Input DataFrame.
            window: Window spec defining partition and ordering for the lag.
            col_pairs: Pairs of ``(new_col, base_col)`` — the output column
                name and the source column to diff against its previous row.

        Returns:
            DataFrame with one additional difference column per pair;
            ``None`` when either the current or previous value is null.
        """
        exprs: dict[str, Column] = {
            new_col: F.when(
                F.col(base_col).isNull() | F.lag(base_col, 1).over(window).isNull(),
                None,
            ).otherwise(F.col(base_col) - F.lag(base_col, 1).over(window))
            for new_col, base_col in col_pairs
        }
        return df.withColumns(exprs)

    def compute_safe_quotients(
        df: DataFrame,
        triplets: Sequence[tuple[str, str, str]],
    ) -> DataFrame:
        """Add null- and zero-safe quotient columns to a DataFrame.

        Args:
            df: Input DataFrame.
            triplets: Tuples of ``(new_col, num_col, denom_col)`` — the
                output column name, numerator column, and denominator column.

        Returns:
            DataFrame with one additional ratio column per triplet, rounded
            to one decimal place; ``None`` when either operand is null or
            the denominator is below ``_NEAR_ZERO_THRESHOLD``.
        """
        exprs: dict[str, Column] = {
            new_col: F.when(
                F.col(num_col).isNull()
                | F.col(denom_col).isNull()
                | (F.abs(F.col(denom_col)) < _NEAR_ZERO_THRESHOLD),
                None,
            ).otherwise(F.round(F.col(num_col) / F.col(denom_col), 1))
            for new_col, num_col, denom_col in triplets
        }
        return df.withColumns(exprs)

    df = df.withColumns({
        "_ingested_at_hours": F.unix_timestamp(F.col("_ingested_at")) / _SECONDS_PER_HOUR,
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