# Databricks notebook source
# MAGIC %md
# MAGIC # Setting up environment names

# COMMAND ----------

dbutils.widgets.text(name = "catalog_name", defaultValue = "dbr_dev")
dbutils.widgets.dropdown(name = "schema_name", defaultValue="music_analytics",
                                       choices = ['music_analytics', 'music_analytics_prod'])

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

zerobus_raw_tbl_name = "zerobus_music_events"
zerobus_target_tbl_name = "target_music_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating source and target tables

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{zerobus_raw_tbl_name} (
    event_id STRING NOT NULL,
    video_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    view_count BIGINT,
    like_count BIGINT,
    comment_count INT,
    _ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{zerobus_target_tbl_name} (
    event_id STRING NOT NULL,
    video_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    view_count BIGINT,
    like_count BIGINT,
    comment_count INT,
    _ingested_at TIMESTAMP,
    _processed_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Running the zerobus producer

# COMMAND ----------

from producer.producer import main

main()

# COMMAND ----------

# MAGIC %md
# MAGIC # Assuring the raw and duplicated records have arrived at the source table

# COMMAND ----------

sanity_test_df = spark.sql(f"""SELECT 
    COUNT(*) AS total_raw_record_count,
    COUNT(DISTINCT event_id) AS total_unique_record_count,
    COUNT(*) - COUNT(DISTINCT event_id) AS total_duplicated_record_count
    FROM {catalog_name}.{schema_name}.{zerobus_raw_tbl_name}
    """)

display(sanity_test_df) # There are duplicated records as expected!!!

# COMMAND ----------

# MAGIC %md
# MAGIC # Idemponent merging

# COMMAND ----------

spark.sql(f"""MERGE INTO {catalog_name}.{schema_name}.{zerobus_target_tbl_name} AS target
USING (
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT 
        event_id,
        video_id,
        event_timestamp,
        view_count,
        like_count,
        comment_count,
        _ingested_at,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC, _ingested_at DESC) AS row_num
        FROM {catalog_name}.{schema_name}.{zerobus_raw_tbl_name}
    )
    WHERE row_num==1
) AS source
ON TARGET.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET 
    target.video_id = source.video_id,
    target.event_timestamp = source.event_timestamp,
    target.view_count = source.view_count,
    target.like_count = source.like_count,
    target.comment_count = source.comment_count,
    target._ingested_at = source._ingested_at,
    target._processed_at = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    event_id,
    video_id,
    event_timestamp,
    view_count,
    like_count,
    comment_count,
    _ingested_at,
    _processed_at
  )
  VALUES (
    source.event_id,
    source.video_id,
    source.event_timestamp,
    source.view_count,
    source.like_count,
    source.comment_count,
    source._ingested_at,
    current_timestamp()
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # One final sanity test - are there any duplicates in the target zerobus table?

# COMMAND ----------

merged_result = spark.sql(f"""SELECT 
    COUNT(*) AS target_total_records,
    COUNT(DISTINCT event_id) AS target_unique_events
FROM {catalog_name}.{schema_name}.{zerobus_target_tbl_name}""")
display(merged_result) # No duplicates - every single record is unique