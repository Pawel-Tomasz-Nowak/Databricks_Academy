# Databricks notebook source
# MAGIC %md
# MAGIC # Creating a delta table out of the external federal table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbr_dev.music_analytics.target_artists_cdc (
# MAGIC     artist_id INT,
# MAGIC     artist_name STRING,
# MAGIC     record_label STRING,
# MAGIC     country STRING,
# MAGIC     contract_signed_year INT,
# MAGIC     updated_at TIMESTAMP,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     _synced_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT OVERWRITE dbr_dev.music_analytics.target_artists_cdc
# MAGIC SELECT 
# MAGIC     artist_id,
# MAGIC     artist_name,
# MAGIC     record_label,
# MAGIC     country,
# MAGIC     contract_signed_year,
# MAGIC     updated_at,
# MAGIC     is_deleted,
# MAGIC     current_timestamp() AS _synced_at
# MAGIC FROM neon_catalog.public.artists_record_labels;
# MAGIC
# MAGIC SELECT * FROM dbr_dev.music_analytics.target_artists_cdc;

# COMMAND ----------

# MAGIC %md
# MAGIC # Merging the source data changes into the target delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dbr_dev.music_analytics.target_artists_cdc AS target
# MAGIC USING neon_catalog.public.artists_record_labels AS source
# MAGIC ON target.artist_id = source.artist_id
# MAGIC
# MAGIC WHEN MATCHED AND source.is_deleted = TRUE THEN
# MAGIC   UPDATE SET 
# MAGIC     target.is_deleted = TRUE,
# MAGIC     target.updated_at = source.updated_at,
# MAGIC     target._synced_at = current_timestamp()
# MAGIC
# MAGIC WHEN MATCHED AND source.updated_at > target.updated_at THEN
# MAGIC   UPDATE SET 
# MAGIC     target.artist_name = source.artist_name,
# MAGIC     target.record_label = source.record_label,
# MAGIC     target.country = source.country,
# MAGIC     target.contract_signed_year = source.contract_signed_year,
# MAGIC     target.updated_at = source.updated_at,
# MAGIC     target.is_deleted = source.is_deleted,
# MAGIC     target._synced_at = current_timestamp()
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED AND source.is_deleted = FALSE THEN
# MAGIC   INSERT (
# MAGIC     artist_id,
# MAGIC     artist_name,
# MAGIC     record_label,
# MAGIC     country,
# MAGIC     contract_signed_year,
# MAGIC     updated_at,
# MAGIC     is_deleted,
# MAGIC     _synced_at
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.artist_id,
# MAGIC     source.artist_name,
# MAGIC     source.record_label,
# MAGIC     source.country,
# MAGIC     source.contract_signed_year,
# MAGIC     source.updated_at,
# MAGIC     source.is_deleted,
# MAGIC     current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC # Making sure the updates are reflected

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     artist_id, 
# MAGIC     artist_name, 
# MAGIC     record_label, 
# MAGIC     is_deleted, 
# MAGIC     updated_at, 
# MAGIC     _synced_at
# MAGIC FROM dbr_dev.music_analytics.target_artists_cdc
# MAGIC ORDER BY artist_id;