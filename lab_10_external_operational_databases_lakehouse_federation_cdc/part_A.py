# Databricks notebook source
# DBTITLE 1,Cell 1
password = dbutils.secrets.get('pawelnowak2004pri219_scope', 'pawel-neon-db-password')
spark.sql(f"""
CREATE CONNECTION IF NOT EXISTS neon_postgres_conn
TYPE POSTGRESQL
OPTIONS (
  host 'ep-xyz.aws.neon.tech',
  port '5432',
  user 'neondb_owner',
  password '{password}'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS neon_catalog
# MAGIC USING CONNECTION neon_postgres_conn
# MAGIC OPTIONS (database 'neondb');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM neon_catalog.public.artists_record_labels;

# COMMAND ----------

# MAGIC %md
# MAGIC # Hybrid join: external vs delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ext.artist_name,
# MAGIC     ext.record_label,
# MAGIC     ext.country,
# MAGIC     ext.contract_signed_year,
# MAGIC     loc.title AS track_title,
# MAGIC     loc.album
# MAGIC FROM neon_catalog.public.artists_record_labels ext
# MAGIC INNER JOIN dbr_dev.music_analytics.dim_music_metadata loc
# MAGIC     ON lower(trim(ext.artist_name)) = lower(trim(loc.author))
# MAGIC ORDER BY ext.contract_signed_year ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Comparing the performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Federal query plan inspection (Predicate / Projection Pushdown to PostgreSQL)
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT artist_name, record_label 
# MAGIC FROM neon_catalog.public.artists_record_labels
# MAGIC WHERE contract_signed_year > 1985;

# COMMAND ----------

# MAGIC %md
# MAGIC ## delta table queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Local delta plan inspection (Parquet Metadata Pruning)
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT author, album 
# MAGIC FROM dbr_dev.music_analytics.dim_music_metadata
# MAGIC WHERE author = 'Metallica';

# COMMAND ----------

# MAGIC %md
# MAGIC # Discussion
# MAGIC # Lab 10 Notes: Lakehouse Federation & External Operational Databases
# MAGIC
# MAGIC ## Overview
# MAGIC In this lab, we connected Databricks directly to an external PostgreSQL database (hosted on Neon) using Lakehouse Federation. Instead of ingesting and copying raw files into cloud storage (our usual Bronze/Silver workflow), we queried the external transactional system in place and in real time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Step-by-Step Implementation
# MAGIC
# MAGIC 1. **External Source Setup (Neon PostgreSQL):**
# MAGIC    - Spun up a serverless PostgreSQL instance on Neon.
# MAGIC    - Created a lookup reference table `artists_record_labels` and populated it with initial test records matching our YouTube project artists.
# MAGIC
# MAGIC 2. **Unity Catalog Connection Object:**
# MAGIC    - Registered a secure `CONNECTION` in Databricks SQL pointing to the Neon endpoint (host, port 5432, username, and password).
# MAGIC    - This object abstracts credentials securely—passwords remain hidden and are not hardcoded inside SQL queries.
# MAGIC
# MAGIC 3. **Foreign Catalog Creation:**
# MAGIC    - Created a `FOREIGN CATALOG` named `neon_catalog` referencing the connection object and the `neondb` database.
# MAGIC    - Unity Catalog automatically discovered remote schemas and mapped the PostgreSQL tables into the workspace catalog tree.
# MAGIC
# MAGIC 4. **Querying & Hybrid JOIN:**
# MAGIC    - Verified direct reads with a simple `SELECT` against `neon_catalog.public.artists_record_labels`.
# MAGIC    - Executed a hybrid `JOIN` combining the external PostgreSQL reference data with our local Delta table (`dim_music_metadata`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Federation vs. Ingestion: Architectural Trade-Offs
# MAGIC
# MAGIC The core engineering decision comes down to **querying in place (Federation)** vs. **copying data into the lakehouse (Ingestion into Delta Lake)**.
# MAGIC
# MAGIC | Dimension | Lakehouse Federation (Query in Place) | Ingestion (Delta Lake / Medallion) |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **Data Movement** | None. Data stays in the external database. | Full copy. Data is loaded and written to Parquet/Delta files. |
# MAGIC | **Data Freshness** | Immediate (zero latency). Always returns the live OLTP state. | Depends on pipeline frequency (e.g., hourly batch or near-real-time streaming). |
# MAGIC | **Analytical Speed** | Constrained by network bandwidth and the remote database CPU/RAM. | Extremely fast. Leverages Spark parallelism, file partitioning, and data skipping. |
# MAGIC | **Cost & Impact** | Low storage footprint, but generates outbound network traffic and adds load to the operational database. | Storage and compute costs for the pipeline, but queries never impact production OLTP systems. |
# MAGIC | **Best Used For** | Small lookup tables, reference data, ad-hoc audits, low-frequency queries. | High-volume fact tables, telemetry, core BI dashboards, ML model training. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Under the Hood: Query Execution Plans
# MAGIC
# MAGIC Looking at the `EXPLAIN` query plans highlights the structural differences:
# MAGIC - **Federated Queries:** Databricks relies on *Predicate & Projection Pushdown*. Filters (`WHERE`) and column selections (`SELECT`) are translated directly into native PostgreSQL queries and executed on the remote database. Only the necessary filtered rows travel over the network.
# MAGIC - **Delta Lake Queries:** Spark reads local Delta transaction logs and applies file-level *Data Skipping* (min/max statistics) directly on cloud storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Security & Network Considerations
# MAGIC
# MAGIC - **Centralized Governance:** From an access management perspective, the external database behaves like any other Unity Catalog entity. We can grant table permissions (`GRANT SELECT`) to workspace users without provisioning accounts for them inside PostgreSQL.
# MAGIC - **Service Account Principle:** The `CONNECTION` object relies on a technical database user. This account should follow the principle of least privilege (read-only permissions) to avoid accidental writes to the operational database.
# MAGIC - **Operational Database Protection:** High query volume through federation risks connection pool exhaustion or CPU spikes on the source database. For high-concurrency dashboards, ingesting data into Bronze/Silver Delta tables remains the safer pattern.