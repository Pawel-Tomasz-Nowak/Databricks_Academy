"""Data Generator Module

Synthetic streaming event data generator for LAB 4 demonstrations.
Produces realistic music streaming events with configurable batch sizes
and intentional data quality issues for testing data cleaning pipelines.

Generated Data Features:
    - Event IDs with controlled overlap between batches (via batch_offset)
    - Random user, track, and genre assignments
    - Intentional data quality issues:
        * 3% negative play_time_seconds values
        * Null genres (~14% probability)
        * Duplicate event_ids with different timestamps
    - Metadata columns (_ingested_at, _source_file)
    - Time-distributed events (30-day window)

Typical Usage:
    >>> from data_generators import generate_bronze_data
    >>> # Initial batch
    >>> batch1 = generate_bronze_data(NUM_ROWS=1_000_000, batch_offset=0)
    >>> 
    >>> # Overlapping batch (25% overlap with batch1)
    >>> batch2 = generate_bronze_data(NUM_ROWS=1_000_000, batch_offset=750_000)

Batch Overlap Mechanism:
    The batch_offset creates overlapping event_ids between consecutive batches,
    simulating late-arriving events and enabling SCD merge testing.
    
    Example with NUM_ROWS=1000, batch_offset=750:
    - Batch 1: event_id ranges evt_0 to evt_949
    - Batch 2: event_id ranges evt_750 to evt_1699 (overlaps evt_750-949)

Author: Paweł Nowak
Date: 2026-08-10
"""

from typing import Final

from pyspark.sql import DataFrame, SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

OVERLAP_FACTOR: Final[float] = 0.95
NEGATIVE_VALUE_PROBABILITY: Final[float] = 0.03
NEGATIVE_PLAY_TIME: Final[int] = -100
SKIP_PROBABILITY: Final[float] = 0.2

def generate_bronze_data(NUM_ROWS: int = 1_000_000, batch_offset: int = 0) -> DataFrame:
    """Generate synthetic bronze-layer streaming event data.
    
    Creates a PySpark DataFrame with music streaming events containing
    intentional data quality issues for testing cleaning and SCD merge logic.
    
    Args:
        NUM_ROWS (int, optional): Number of events to generate. Defaults to 1,000,000.
        batch_offset (int, optional): Offset for event_id calculation to create
                                     overlapping batches. Set to (NUM_ROWS * 0.75)
                                     for 25% overlap. Defaults to 0.
    
    Returns:
        DataFrame: PySpark DataFrame with schema:
            - event_id (string): Format "evt_N", with modulo for overlap
            - user_id (string): Format "usr_N", random from 100k users
            - track_id (string): Format "trk_N", random from 5k tracks
            - genre (string): Metal/Rock/Pop/Hip-Hop/Indie/Electronic/null
            - play_time_seconds (int): 10-310s, or -100 (3% negative for testing)
            - royalty_rate (decimal(10,4)): Random 0.0-0.05
            - is_skipped (boolean): True ~20% of time
            - country_code (string): PL/US/DE/UK/FR/JP/BR
            - device_type (string): mobile/desktop/smart_tv/web_player
            - event_timestamp (timestamp): Distributed over past 30 days
            - _ingested_at (timestamp): Current timestamp
            - _source_file (string): Static "raw_stream_landing/events_batch_01.json"
    
    Example:
        >>> # Generate first batch
        >>> df = generate_bronze_data(NUM_ROWS=1000, batch_offset=0)
        >>> df.select("event_id").show(5)
        +--------+
        |event_id|
        +--------+
        |   evt_1|
        |   evt_2|
        |   evt_3|
        |   evt_4|
        |   evt_5|
        +--------+
        
        >>> # Generate overlapping batch
        >>> df2 = generate_bronze_data(NUM_ROWS=1000, batch_offset=750)
        >>> # Will have event_ids like evt_750, evt_751, ..., evt_1699
        >>> # Overlaps with first batch on evt_750 - evt_949
    
    Notes:
        - Modulo threshold = 0.95 * NUM_ROWS + batch_offset ensures 5% overlap
        - Negative play_time_seconds (3% of records) tests data quality cleaning
        - Null genres test missing value handling
        - Duplicate event_ids with different timestamps test deduplication
        - Event timestamps span 30 days to simulate historical data
    """
    df_raw = spark.range(1, NUM_ROWS+1)

    genres = ["Metal", "Rock", "Pop", "Hip-Hop", "Indie", "Electronic", None]
    countries = ["PL", "US", "DE", "UK", "FR", "JP", "BR"]
    device_types = ["mobile", "desktop", "smart_tv", "web_player"]

    modulo_threshold: int = int(OVERLAP_FACTOR * NUM_ROWS) + batch_offset
    
    df_bronze = df_raw.select(
        F.concat(F.lit("evt_"), ((F.col("id") + F.lit(batch_offset)) % modulo_threshold)).alias("event_id"),
        
        F.concat(F.lit("usr_"), (F.rand() * 100000).cast("int")).alias("user_id"),
        F.concat(F.lit("trk_"), (F.rand() * 5000).cast("int")).alias("track_id"),
        
        F.element_at(F.array(*[F.lit(g) for g in genres]), (F.rand() * 7 + 1).cast("int")).alias("genre"),
        

        F.when(F.rand() < 0.03, -100)
        .otherwise((F.rand() * 300 + 10).cast("int")).alias("play_time_seconds"),
        
        (F.rand() * 0.05).cast("decimal(10,4)").alias("royalty_rate"),
        (F.rand() < 0.2).alias("is_skipped"),
        
        F.element_at(F.array(*[F.lit(c) for c in countries]), (F.rand() * 7 + 1).cast("int")).alias("country_code"),
        F.element_at(F.array(*[F.lit(d) for d in device_types]), (F.rand() * 4 + 1).cast("int")).alias("device_type"),
        

        # We're making sure the concrete value of event_id has multiple UNIQUE timestamps
        (F.current_timestamp() - (30 - (F.col("id") / 35000)).cast("int") * F.expr("INTERVAL 1 DAY")).alias("event_timestamp"),
        
        F.current_timestamp().alias("_ingested_at"),
        F.lit("raw_stream_landing/events_batch_01.json").alias("_source_file")
    )

      
    return df_bronze


