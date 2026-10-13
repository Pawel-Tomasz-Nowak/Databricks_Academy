# Databricks notebook source
# MAGIC %sql
# MAGIC WITH dq_metrics AS (
# MAGIC     SELECT 
# MAGIC         (SELECT COUNT(*) FROM dbr_dev.music_analytics.silver_music_metadata) AS clean_count,
# MAGIC         (SELECT COUNT(*) FROM dbr_dev.music_analytics.silver_music_quarantine) AS quarantine_count
# MAGIC )
# MAGIC SELECT 
# MAGIC     clean_count,
# MAGIC     quarantine_count,
# MAGIC     (clean_count + quarantine_count) AS total_processed,
# MAGIC     ROUND((clean_count / (clean_count + quarantine_count)) * 100, 2) AS pass_rate_pct,
# MAGIC     CASE 
# MAGIC         WHEN quarantine_count = 0 THEN 'HEALTHY (100%)'
# MAGIC         WHEN (clean_count / (clean_count + quarantine_count)) >= 0.95 THEN 'WARNING (>95%)'
# MAGIC         ELSE 'CRITICAL (<95%)'
# MAGIC     END AS pipeline_status
# MAGIC FROM dq_metrics;