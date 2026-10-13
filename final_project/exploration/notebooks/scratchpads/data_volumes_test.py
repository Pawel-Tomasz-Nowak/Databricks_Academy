# Databricks notebook source
bronze_name = "dbr_dev.music_analytics.bronze_music_stats"
silver_name = "dbr_dev.music_analytics.bronze_music_stats"

# COMMAND ----------

bronze_df = spark.read.table(bronze_name)

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

silver_df.count()