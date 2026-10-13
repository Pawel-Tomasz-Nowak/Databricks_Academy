# Databricks notebook source
# MAGIC %md
# MAGIC # At-Least-Once vs. Exactly-Once Semantics
# MAGIC
# MAGIC When a streaming pipeline crashes mid-process, the way your data is handled depends on the processing guarantee:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## At-Least-Once
# MAGIC
# MAGIC - **Guarantee:** No data is lost. Every message is processed at least once.
# MAGIC - **Caveat:** Some records may be processed and written twice, resulting in duplicate rows if a failure occurs before Spark records its progress.
# MAGIC - **Example Scenario:** If the cluster crashes after writing a batch to storage but before saving the read position, Spark will re-read and re-write that batch when restarted.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exactly-Once
# MAGIC
# MAGIC - **Guarantee:** Each record is processed exactly one time—no missing data, no duplicates.
# MAGIC - **How Spark + Delta Lake Achieves This:**
# MAGIC   1. **Replayable Source:** The stream source (like Azure Event Hubs or Kafka) allows Spark to rewind to a specific message position (offset).
# MAGIC   2. **Checkpointing:** Spark saves stream progress and metadata to cloud storage.
# MAGIC   3. **Idempotent Sink:** Delta Lake uses a transaction log. If Spark tries to write the same micro-batch twice, Delta recognizes the duplicate and skips the extra commit.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Checkpointing and Fault Tolerance
# MAGIC
# MAGIC ## Checkpointing
# MAGIC
# MAGIC Checkpointing acts as a permanent bookmark on durable cloud storage (such as ADLS or Unity Catalog Volumes). After each successful micro-batch, Spark writes two tracking records to the checkpoint directory:
# MAGIC
# MAGIC - **offsets:** Tracks how far Spark has read from the queue (e.g., "read up to offset 4500").
# MAGIC - **commits:** Confirms that the micro-batch was successfully committed to the target table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fault Tolerance
# MAGIC
# MAGIC Checkpoints make the pipeline resilient:
# MAGIC
# MAGIC 1. If a cluster crashes or terminates after finishing a job with `trigger(availableNow=True)`, a new cluster can start up.
# MAGIC 2. Spark checks the checkpoint directory.
# MAGIC 3. It reads the last committed offset and resumes processing from where it stopped, without scanning the entire dataset again.