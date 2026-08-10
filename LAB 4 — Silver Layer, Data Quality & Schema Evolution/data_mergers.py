"""Data Mergers Module

This module handles merging new data batches into existing Delta tables using
Slowly Changing Dimension (SCD) strategies. Supports both Type I and Type II SCDs.

SCD Type I:
    - Overwrites existing records with updated values
    - No historical tracking
    - Updates only when new event has later timestamp

SCD Type II:
    - Maintains full history of changes
    - Tracks validity periods (valid_from, valid_to)
    - Marks current/expired records (is_current flag)
    - Handles brand new records and versioned updates

Typical Usage:
    >>> from data_mergers import merge_new_batch
    >>> # Prepare cleaned data batch
    >>> merge_new_batch(cleaned_batch, "catalog.schema.silver_table", scd_type="II")

Author: Paweł Nowak
Date: 2026-08-10
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def merge_new_batch(batch, table_name:str, scd_type: str = "I"):
    """Merge a new data batch into an existing Delta table using SCD strategy.
    
    Implements Slowly Changing Dimension patterns to handle updates, inserts, and
    historical tracking. The merge strategy depends on the scd_type parameter.
    
    SCD Type I Behavior:
        - Matches on event_id
        - Updates target record if new event_timestamp is later
        - No history preservation - overwrites old values
    
    SCD Type II Behavior:
        - Matches on event_id
        - Identifies three categories of records:
            1. Brand new events (no match in target) → insert with is_current=True
            2. Changed events (dimensions differ) → insert new version, expire old
            3. Unchanged events (identical) → no action
        - Uses staging merge pattern with merge_id for precise control
        - Expires old versions by setting is_current=False and valid_to timestamp
    
    Args:
        batch (DataFrame): Cleaned PySpark DataFrame containing new records to merge.
                          Must have schema matching the target table.
        table_name (str): Fully qualified Delta table name (catalog.schema.table)
        scd_type (str, optional): SCD strategy - "I" or "II". Defaults to "I".
    
    Raises:
        AnalysisException: If table_name doesn't exist or isn't a Delta table
        ValueError: If scd_type is not "I" or "II"
    
    Example:
        >>> # Type I merge - simple upsert
        >>> merge_new_batch(new_data, "workspace.lab_4_schema.silver_events", scd_type="I")
        >>> 
        >>> # Type II merge - historical tracking
        >>> merge_new_batch(new_data, "workspace.lab_4_schema.silver_events_history", scd_type="II")
    
    Notes:
        - Type II requires batch to have valid_from, valid_to, is_current columns
        - Type II staging uses merge_id trick: None for inserts, event_id for updates
        - Commented code at bottom shows alternative Type II implementation
    """
    delta_silver_table = DeltaTable.forName(spark, table_name)

    if scd_type == "I":
        col_updates = {
            "target.user_id": F.col("source.user_id"),
            "target.track_id": F.col("source.track_id"),
            "target.genre": F.col("source.genre"),
            "target.play_time_seconds": F.col("source.play_time_seconds"),
            "target.royalty_rate": F.col("source.royalty_rate"),
            "target.is_skipped": F.col("source.is_skipped"),
            "target.country_code": F.col("source.country_code"),
            "target.device_type": F.col("source.device_type"),
            "target.event_timestamp": F.col("source.event_timestamp"),
            "target._ingested_at": F.col("source._ingested_at"),
            "target._source_file": F.col("source._source_file"),
            "target._processed_at": F.current_timestamp()
        }
         
        delta_silver_table.alias("target").merge(
            source = batch.alias("source"),
            condition = F.col("target.event_id") == F.col("source.event_id")
        ).whenMatchedUpdate(
            condition = F.col("source.event_timestamp") > F.col("target.event_timestamp"),
            set = col_updates
        ).execute()

    elif scd_type == "II":
        something_has_changed_condition = (
                (F.col("tgt.user_id") != F.col("src.user_id")) |
                (F.col("tgt.track_id") != F.col("src.track_id")) |
                (F.col("tgt.genre") != F.col("src.genre")) |
                (F.col("tgt.play_time_seconds") != F.col("src.play_time_seconds")) |
                (F.col("tgt.royalty_rate") != F.col("src.royalty_rate")) |
                (F.col("tgt.is_skipped") != F.col("src.is_skipped")) |
                (F.col("tgt.country_code") != F.col("src.country_code")) |
                (F.col("tgt.device_type") != F.col("src.device_type")) |
                (F.col("tgt.event_timestamp") != F.col("src.event_timestamp")) |
                (F.col("tgt._ingested_at") != F.col("src._ingested_at")) |
                (F.col("tgt._source_file") != F.col("src._source_file"))
        )
            
        # First, convert DeltaTable to pyspark dataframe and retrieve active (is_current = True) records only
        target_df = delta_silver_table.toDF().alias("tgt").filter(F.col("is_current")==True)
        batch = batch.alias("src")


        # Then, find brand new records (those that cannot have their match in the `batch`)
        brand_new_records_to_insert = batch.join(target_df, on = "event_id", how="leftanti").withColumn("merge_id", F.lit(None).cast("string"))

        # Afterwards, find records having their match in `batch`
        records_to_update = batch.join(target_df, on = "event_id")

        # In the above dataframe, there are three types of records (each of them have their match in the target df).
        # 1st Type: literally identicial records - nothing to do, just leave them

        # 2nd type: new (i.e. with bigger event_timestamp!!) record with some dimension changed - 
            # we insert this record 
            # and update the old one by setting is_current=False and tgt.valid_to = src.event_timestamp
        valid_records = records_to_update.filter(something_has_changed_condition & (F.col("src.valid_from") > F.col("tgt.valid_from"))\
            )
        
        valid_records_to_insert = valid_records.select("src.*").withColumn("merge_id", F.lit(None).cast("string"))
        valid_records_to_update = valid_records.select("tgt.*").withColumn("merge_id", F.col("tgt.event_id"))

        staging_df = brand_new_records_to_insert.unionByName(valid_records_to_insert).unionByName(valid_records_to_update)
  
        
         # Finally, merge the silver_target with staging_df

        # 3rd type: old record (possibly with some dimensions changed but that's not necessary) - we insert into another extra for out-of-order data
        # old_records_to_insert = records_to_update.filter(F.col("src.event_timestamp") <= F.col("tgt.event_timestamp"))
        
  
        delta_silver_table.alias("tgt") \
        .merge(
            staging_df.alias("src"), 
            "tgt.event_id = merge_id AND tgt.is_current = true"
        ) \
        .whenMatchedUpdate(set={
            "tgt.is_current": F.lit(False),
            "tgt.valid_to": F.col("src.event_timestamp") 
        }) \
        .whenNotMatchedInsert(values={
            "event_id": F.col("src.event_id"),
            "user_id": F.col("src.user_id"),
            "track_id": F.col("src.track_id"),
            "genre": F.col("src.genre"),
            "play_time_seconds": F.col("src.play_time_seconds"),
            "royalty_rate": F.col("src.royalty_rate"),
            "is_skipped": F.col("src.is_skipped"),
            "country_code": F.col("src.country_code"),
            "device_type": F.col("src.device_type"),
            "event_timestamp": F.col("src.event_timestamp"),
            "_ingested_at": F.col("src._ingested_at"),
            "_source_file": F.col("src._source_file"),
            "valid_from": F.col("src.event_timestamp"),
            "valid_to": F.lit(None).cast("timestamp"),
            "is_current": F.lit(True)
        }) \
        .execute()


    



        # filter_condition = (
        #     (
        #         (F.col("tgt.user_id") != F.col("src.user_id")) |
        #         (F.col("tgt.track_id") != F.col("src.track_id")) |
        #         (F.col("tgt.genre") != F.col("src.genre")) |
        #         (F.col("tgt.play_time_seconds") != F.col("src.play_time_seconds")) |
        #         (F.col("tgt.royalty_rate") != F.col("src.royalty_rate")) |
        #         (F.col("tgt.is_skipped") != F.col("src.is_skipped")) |
        #         (F.col("tgt.country_code") != F.col("src.country_code")) |
        #         (F.col("tgt.device_type") != F.col("src.device_type")) |
        #         (F.col("tgt.event_timestamp") != F.col("src.event_timestamp")) |
        #         (F.col("tgt._ingested_at") != F.col("src._ingested_at")) |
        #         (F.col("tgt._source_file") != F.col("src._source_file"))
        #     ) &
        #     (F.col("tgt.is_current") == True) &
        #     (F.col("src.event_timestamp") > F.col("tgt.event_timestamp"))
        # )

        # records_to_update = batch.alias("src")\
        #     .join(delta_silver_table.toDF().alias("tgt"), on = "event_id")\
        #     .filter(filter_condition)\
        #     .select("src.*")

        # records_to_expire = records_to_update.withColumn("merge_id", F.col("event_id"))

        # records_to_insert_new_versions = records_to_update.withColumn("merge_id", F.lit(None).cast("string"))

        # # Completely new event_ids
        # records_to_insert_brand_new = batch.alias("src")\
        #     .join(delta_silver_table.toDF().alias("tgt"), on="event_id", how="leftanti")\
        #     .withColumn("merge_id", F.lit(None).cast("string"))

        # staging_df = records_to_expire.union(records_to_insert_new_versions).union(records_to_insert_brand_new)
                

        # delta_silver_table.alias("tgt") \
        #     .merge(
        #         staging_df.alias("src"), 
        #         # Match only on the active target row using our custom merge_id
        #         """tgt.event_id = src.merge_id AND tgt.is_current = true 
        #         AND src.event_timestamp > tgt.event_timestamp"""
        #     ) \
        #     .whenMatchedUpdate(set={
        #         # Action 1: Expire the old record because a match was found
        #         "tgt.is_current": F.lit(False),
        #         "tgt.valid_to": F.col("src.event_timestamp") 
        #     }) \
        #     .whenNotMatchedInsert(values={
        #         "event_id": F.col("src.event_id"),
        #         "user_id": F.col("src.user_id"),
        #         "track_id": F.col("src.track_id"),
        #         "genre": F.col("src.genre"),
        #         "play_time_seconds": F.col("src.play_time_seconds"),
        #         "royalty_rate": F.col("src.royalty_rate"),
        #         "is_skipped": F.col("src.is_skipped"),
        #         "country_code": F.col("src.country_code"),
        #         "device_type": F.col("src.device_type"),
        #         "event_timestamp": F.col("src.event_timestamp"),
        #         "_ingested_at": F.col("src._ingested_at"),
        #         "_source_file": F.col("src._source_file"),
        #         "valid_from": F.col("src.event_timestamp"),
        #         "valid_to": F.lit(None).cast("timestamp"),
        #         "is_current": F.lit(True)
        #     }) \
        #     .execute()