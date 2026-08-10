"""Preprocessing Orchestration Module

Orchestrates the end-to-end data pipeline for streaming events:
1. Generate synthetic bronze data (raw events)
2. Write/append to bronze Delta table
3. Clean and transform data
4. Write/merge into silver Delta table using SCD strategy

This module serves as the entry point for batch processing in the medallion
architecture (bronze → silver transformation).

Typical Usage:
    >>> from preprocessing import preprocess_new_batch
    >>> # Initial load - 1M records, SCD Type I
    >>> preprocess_new_batch()
    >>> 
    >>> # Incremental batch with overlap, SCD Type II
    >>> preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=750_000, scd_type="II")

Table Naming:
    Configured via config.py:
    - Bronze: workspace.lab_4_schema.bronze_streaming_events_SCD_{I,II}
    - Silver: workspace.lab_4_schema.silver_streaming_events_SCD_{I,II}

Author: Paweł Nowak
Date: 2026-08-10
"""

from config import tables
from data_cleaner import clean_bronze_data
from data_generators import generate_bronze_data
from data_mergers import merge_new_batch
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def preprocess_new_batch(NUM_ROWS: int = 1_000_000, batch_offset: int = 0, scd_type: str = "I") -> None:
    """Generate and process a new batch of streaming events through the data pipeline.
    
    Orchestrates the complete bronze-to-silver transformation:
    1. Generates synthetic streaming event data (or receives real stream)
    2. Writes to bronze table (overwrite if first run, append otherwise)
    3. Cleans data (fixes negatives, handles nulls, deduplicates)
    4. Writes to silver table (overwrite if first run, merge otherwise using SCD strategy)
    
    The batch_offset parameter enables testing incremental loads with overlapping
    event_ids between consecutive batches, simulating real-world late-arriving data.
    
    Args:
        NUM_ROWS (int, optional): Number of events to generate. Defaults to 1,000,000.
        batch_offset (int, optional): Offset for event_id generation. Use non-zero
                                     values to create overlapping batches for testing
                                     merge behavior. Defaults to 0 (initial load).
        scd_type (str, optional): Slowly Changing Dimension strategy - "I" or "II".
                                 Determines which bronze/silver table pair to use
                                 and merge logic. Defaults to "I".
    
    Side Effects:
        - Creates/updates bronze Delta table
        - Creates/updates silver Delta table
        - Prints cleaning statistics to stdout
    
    Raises:
        KeyError: If scd_type is not "I" or "II" (config.py keys)
        AnalysisException: If table creation/write fails
    
    Example:
        >>> # Initial load: 1M records, SCD Type I
        >>> preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=0, scd_type="I")
        The minimum play time is 0 seconds
        Number of records in bronze table: 1000000
        Number of records in silver table: 950000
        Percentage change in number of records: -5.0%
        
        >>> # Incremental batch with 25% overlap, SCD Type II
        >>> preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=750_000, scd_type="II")
        ...
    
    Notes:
        - First run creates tables with overwriteSchema=true
        - Subsequent runs append to bronze, merge to silver
        - Type I merge: simple upsert by event_id
        - Type II merge: historical tracking with validity periods
        - Batch overlap (offset < NUM_ROWS) simulates late-arriving events
    """    
    bronze_batch = generate_bronze_data(NUM_ROWS=NUM_ROWS, batch_offset=batch_offset)

    bronze_table: str = tables[scd_type]["bronze"]
    silver_table: str = tables[scd_type]["silver"]

    if not spark.catalog.tableExists(bronze_table):
        bronze_batch.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
    else:
        bronze_batch.write.format("delta").mode("append").saveAsTable(bronze_table)


    silver_batch = clean_bronze_data(bronze_batch, scd_type)


    if not spark.catalog.tableExists(silver_table):
        silver_batch.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').saveAsTable(silver_table)
    else:
        merge_new_batch(silver_batch, silver_table, scd_type = scd_type)