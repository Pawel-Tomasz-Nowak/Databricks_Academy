"""Data Cleaner Module

Transforms raw bronze-layer streaming event data into clean silver-layer tables.
Handles data quality issues including negative values, missing data, and duplicates.

Cleaning Pipeline:
    1. Fix negative play_time_seconds values (replace with 0)
    2. Handle missing values (filter nulls in event_id, replace null genres with "Unknown")
    3. Deduplicate by event_id (keep most recent event_timestamp)
    4. Add processing timestamp
    5. For SCD Type II: Add validity tracking columns (valid_from, valid_to, is_current)

Typical Usage:
    >>> from data_cleaner import clean_bronze_data
    >>> bronze_df = spark.read.table("bronze_events")
    >>> clean_df = clean_bronze_data(bronze_df, scd_type="II")
    >>> clean_df.write.format("delta").saveAsTable("silver_events")

Author: Paweł Nowak
Date: 2026-08-10
"""

import pyspark.sql.functions as F

def clean_bronze_data(bronze_table, scd_type:str = "I"):
    """Clean and transform bronze streaming event data for silver layer.
    
    Applies data quality transformations to prepare raw event data for analytics:
    - Corrects negative play time values (data quality violation)
    - Handles missing values per business rules
    - Deduplicates events by keeping most recent timestamp
    - Adds processing metadata
    - Optionally adds SCD Type II tracking columns
    
    Args:
        bronze_table (DataFrame): Raw PySpark DataFrame from bronze layer.
                                  Expected schema includes: event_id, play_time_seconds,
                                  genre, event_timestamp, and other event attributes.
        scd_type (str, optional): Target SCD strategy. "I" for simple updates,
                                 "II" to add historical tracking columns. Defaults to "I".
    
    Returns:
        DataFrame: Cleaned silver-layer DataFrame ready for merge/insert.
                  Contains all original columns plus:
                  - _processed_at: current timestamp
                  - (SCD II only) valid_from, valid_to, is_current
    
    Raises:
        AnalysisException: If bronze_table has unexpected schema
    
    Example:
        >>> bronze = spark.read.table("workspace.lab_4.bronze_events")
        >>> silver = clean_bronze_data(bronze, scd_type="II")
        >>> print(f"Cleaned {silver.count()} records")
        Cleaned 950000 records
    
    Notes:
        - Empty input returns empty output (no-op)
        - Deduplication uses window function partitioned by event_id
        - Negative play_time_seconds values (3% of synthetic data) set to 0
        - Reports row count change to stdout for monitoring
    """
    if bronze_table.count()==0:
        return bronze_table
    
    # part1 - handling negative values in non-negative columns

    # There's a great chance our synthetized batch has negative value in the `play_time_seconds` column
    play_col_name = "play_time_seconds"
    min_play_time_before: float = bronze_table.agg(F.min(F.col(play_col_name))).collect()[0][0]

    if min_play_time_before < 0:
        play_time_seconds_fixed = F.when(F.col(play_col_name) <0, 0).otherwise(F.col(play_col_name))
        df_silver1 = bronze_table.withColumn(play_col_name, play_time_seconds_fixed)
    else:
        df_silver1 = bronze_table


    min_play_time_after: float = df_silver1.agg(F.min(F.col(play_col_name))).collect()[0][0]
    print(f"The minimum play time is {min_play_time_after} seconds")


    # part 2 - checking missing values
    missing_counts = df_silver1.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_silver1.columns
    ])

    # There are no missing values in any columns, particularly in 'event_id' which serves as a primary key and cannot be Null, but genre whose missing values are fgoing ot be transformed to Unknown for clearer interpretation

    df_silver2 = (
        df_silver1
        .filter(F.col("event_id").isNotNull()) # Just for sanity, even though there shouldn't be nulls 
        .withColumn(
            "genre",
            F.when(F.col("genre").isNull(), "Unknown").otherwise(F.col("genre"))
        )
    )


    # Part 2 - deduplicating by event_id
    event_count = df_silver2.groupBy(F.col("event_id")).agg(F.count(F.col("event_id")).alias("count")).sort(F.desc(F.col("count")))

    # Some events appear multiple times with different timestamp.
    # To eliminate duplicates with respect to event_id, we'll use the timestamp column to determine which event is the most recent

    event_count_with_timestamp = df_silver1.groupBy(F.col("event_id"), F.col("event_timestamp")).agg(F.count(F.col("event_id")).alias("count")).sort(F.desc(F.col("count")))


    from pyspark.sql import Window

    event_window = Window.partitionBy("event_id").orderBy(F.desc("event_timestamp"))

    df_silver3 = (df_silver2.withColumn("row_number", F.row_number().over(event_window))
                .filter(F.col("row_number") == 1)
                .drop("row_number"))


    df_silver4 = df_silver3 \
        .withColumn("_processed_at", F.current_timestamp())

    bronze_count = bronze_table.count()
    silver_count = df_silver4.count()

    print(f"Number of records in bronze table: {bronze_count}")
    print(f"Number of records in silver table: {silver_count}")
    records_change_pct = round(100*(silver_count-bronze_count)/bronze_count, 1)
    print(f"Percentage change in number of records: {records_change_pct}%")


    # Part 3 (reserved for SCD type II only) - adding `is_current`, `valid_from` and `valid_to` columns
    if scd_type == "II":
        df_silver4 = df_silver4 \
            .withColumn("valid_from", F.col("_processed_at")) \
            .withColumn("valid_to", F.lit(None).cast("timestamp"))\
            .withColumn("is_current", F.lit(True)) 

    return df_silver4