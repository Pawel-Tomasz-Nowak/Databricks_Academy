# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup: Add src/ to Python path
import sys
import os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)  # .../LAB 4.../notebooks
lab_root = os.path.dirname(notebook_dir)  # .../LAB 4...
lab_root_path = f"/Workspace{lab_root}"

if lab_root_path not in sys.path:
    sys.path.insert(0, lab_root_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # General tests
# MAGIC * Specifically - we're testing whether the rows of the consecutive batches overlap (overlapping is determined by *event_id* column)

# COMMAND ----------

from src.config import tables, create_catalog_and_schema, catalog_name, schema_name

create_catalog_and_schema(catalog_name, schema_name)

bronze_I_tbl = tables['I']['bronze']
silver_I_tbl = tables['I']['silver']

bronze_II_tbl = tables['II']['bronze']
silver_II_tbl = tables['II']['silver']

# COMMAND ----------

# DBTITLE 1,Quick Test: Distinct Event IDs per Batch
from src.data_generators import generate_bronze_data
# Quick test: Generate 2 small batches with different offsets
print("Batch 1 (offset=0) - first 5 event_ids:")
batch1 = generate_bronze_data(NUM_ROWS=10, batch_offset=0)
display(batch1.select("event_id").show())

print("\nBatch 2 (offset=5) - first 5 event_ids:")
batch2 = generate_bronze_data(NUM_ROWS=10, batch_offset=5)
display(batch2.select("event_id").show())

# There are some event_id which occurs in both tables - we're good!!! (and the second table has new records)

# COMMAND ----------

# MAGIC %md
# MAGIC # Slowly Changing Dimensions - I type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initializing the bronze and silver tables (*the very first batches*)

# COMMAND ----------

# Use only if needed and with cautious :D
for tbl in [bronze_I_tbl, silver_I_tbl, bronze_II_tbl, silver_II_tbl]:
    if spark.catalog.tableExists(tbl):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")

# COMMAND ----------

from src.preprocessing import preprocess_new_batch

preprocess_new_batch()

# COMMAND ----------

row_count_bronze_I = spark.sql(f"SELECT COUNT(*) FROM {bronze_I_tbl}").collect()[0][0]
print(f"Number of rows in bronze table of SCD type I: {row_count_bronze_I:,}") # Size is as expeceted - 1 million
display(spark.sql(f"SELECT * FROM {bronze_I_tbl} LIMIT 5"))

# COMMAND ----------

row_count_silver_I = spark.sql(f"SELECT COUNT(*) FROM {silver_I_tbl}").collect()[0][0]
print(f"Number of rows in silver table of SCD type I: {row_count_silver_I:,}") # 50,000 records less than bronze table
display(spark.sql(f"SELECT * FROM {silver_I_tbl} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populating the silver table with new batch of data (*UPSERTING*)

# COMMAND ----------

preprocess_new_batch(batch_offset = 750_000)

# COMMAND ----------

row_count_bronze_I_after = spark.sql(f"SELECT COUNT(*) FROM {bronze_I_tbl}").collect()[0][0]
print(f"Number of rows in bronze table of SCD type I after ingesting new batch: {row_count_bronze_I_after:,}") # the size is as expected - 2 mln
display(spark.sql(f"SELECT * FROM {bronze_I_tbl} LIMIT 5"))

# COMMAND ----------

row_count_silver_I_after = spark.sql(f"SELECT COUNT(*) FROM {silver_I_tbl}").collect()[0][0]
print(f"Number of rows in silver table of SCD type I: {row_count_silver_I_after:,}") # 1.7 mln records - some records from the previous version of the silver table were updated with the new batch and therefore the size isn't 2 mln
display(spark.sql(f"SELECT * FROM {silver_I_tbl} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Slowly Changing Dimensions - II type

# COMMAND ----------

# Use only if needed and with cautious :D
for tbl in [bronze_II_tbl, silver_II_tbl]:
    if spark.catalog.tableExists(tbl):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")

# COMMAND ----------

preprocess_new_batch(scd_type="II")

# COMMAND ----------

row_count_bronze_II = spark.sql(f"SELECT COUNT(*) FROM {bronze_II_tbl}").collect()[0][0]
print(f"Number of rows in bronze table of SCD type II: {row_count_bronze_II:,}") # Size is as expeceted - 1 million
display(spark.sql(f"SELECT * FROM {bronze_II_tbl} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC * We deliberately skip showing the basic stats for silver table SCD II because nothing new and interesting has happened there - it's the same as in the SCD I approach.

# COMMAND ----------

preprocess_new_batch(batch_offset = 750_000, scd_type="II")

# COMMAND ----------

# MAGIC %md
# MAGIC * Again, for the sake of your prrecious time, we deliberately skip showing the basic stats for bronze table SCD II after the second batch ingestion because nothing new and interesting has happened there - it's the same as in the SCD I approach.

# COMMAND ----------

row_count_silver_II_after = spark.sql(f"SELECT COUNT(*) FROM {silver_II_tbl}").collect()[0][0]
print(f"Number of rows in silver table of SCD type II: {row_count_silver_II_after:,}") 
display(spark.sql(f"SELECT * FROM {silver_II_tbl} LIMIT 5"))

# COMMAND ----------

display(spark.sql(f"SELECT `is_current` AS `current_state`, COUNT(*) FROM {silver_II_tbl} GROUP BY `is_current`")) # there are both active and inactive records

# COMMAND ----------

# MAGIC %md
# MAGIC * One final test for SCD II - we're going to ingest another, third batch and see what happens to the records of the same id. Specifically, will the new record update the oldest, inactive records? Just making sure the data are correct and not distorted - the merging condition was updated to update only active records, but this caveat is something one should be aware of - do not "update" oldest records

# COMMAND ----------

preprocess_new_batch(batch_offset = 850_000, scd_type="II")

# COMMAND ----------

from pyspark.sql.functions import col

filtering_query = f"""
SELECT event_id
FROM {silver_II_tbl}
GROUP BY event_id
HAVING COUNT(*) >= 3
"""

df = spark.sql(f"""
SELECT event_id, event_timestamp, valid_from, valid_to, is_current, device_type, genre
FROM {silver_II_tbl}
WHERE event_id IN (
    {filtering_query}
)
""")
df_sort = df.sort(col("event_id"))
display(df_sort)

# COMMAND ----------

# MAGIC %md
# MAGIC * As can be seen - the SCD works as expected.

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Enforcemenet

# COMMAND ----------

# MAGIC %md
# MAGIC * The test: we're generating another batch of bronze data (as previously) and we take two approaches to test the *schema enforcemenet* mechanism:
# MAGIC - First: we manually add a new column
# MAGIC - Second: we stay with same columns but cast a numeric column to a string, then add some non-numeric values to each record.
# MAGIC
# MAGIC Either case in the end we attempt to write the new batch to the existing bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extra column mismatch

# COMMAND ----------

SE_batch1 = generate_bronze_data(NUM_ROWS = 5_000)
display(SE_batch1.show(5))

# COMMAND ----------

from pyspark.sql.functions import lit

SE_batch1_ext = SE_batch1.withColumn("country_extended", lit("Poland"))
display(SE_batch1_ext.show(5))

# COMMAND ----------

# MAGIC %skip
# MAGIC SE_batch1_ext.write.format("delta").mode("append").saveAsTable(bronze_I_tbl)
# MAGIC # [DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table. SQLSTATE: 42KDG
# MAGIC # This is the error we wanted and expected ;D

# COMMAND ----------

# MAGIC %md
# MAGIC ![DELTA_METADATA_MISMATCH - Extra Column Error](../images/error_delta_metadata_mismatch_extra_column.png "Error when attempting to write DataFrame with extra column to existing Delta table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mismatched datatype

# COMMAND ----------

SE_batch2 = generate_bronze_data(NUM_ROWS = 5_000)

# COMMAND ----------

from pyspark.sql.functions import concat

SE_batch2_ext = SE_batch2.withColumn("play_time_seconds", concat(col("play_time_seconds"), lit("_xD")))
display(SE_batch2_ext.show(5))

# COMMAND ----------

SE_batch2_ext.printSchema()

# COMMAND ----------

# MAGIC %skip
# MAGIC SE_batch2_ext.write.format("delta").mode("append").saveAsTable(bronze_I_tbl)
# MAGIC # [CAST_INVALID_INPUT]  -another error we wanted and expected :DDD

# COMMAND ----------

# MAGIC %md
# MAGIC ![CAST_INVALID_INPUT - String to Int Error](../images/error_cast_invalid_input_string_to_int.png "Error when attempting to cast string column with non-numeric values to integer type")

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC * Now let's relax Schema Enforcement mechanism and attempd to ingest the new batch with extra column into the bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra columns

# COMMAND ----------

SEV_batch1 = generate_bronze_data(NUM_ROWS = 5_000)
SEV_batch1_ext = SEV_batch1.withColumn("country_extended",  lit("USA"))


# COMMAND ----------

SEV_batch1_ext.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(bronze_I_tbl) # Worked nice and clean

# COMMAND ----------

bronze_I_table = spark.read.table(bronze_I_tbl)

null_count = bronze_I_table.filter(col("country_extended").isNull()).count()
total_count = bronze_I_table.count()

print(f"Null count: {null_count} ({round(null_count / total_count * 100, 2)}%)") # Nearly all rows have missing values in the extra column since this field hadn't existed before the insertion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data mismatch

# COMMAND ----------

SEV_batch2 = generate_bronze_data(NUM_ROWS = 5_000)

# COMMAND ----------

SEV_batch2_ext = SEV_batch2.withColumn("play_time_seconds", concat(col("play_time_seconds"), lit("_xD")))
SEV_batch2_ext2 = SEV_batch2.withColumn("play_time_seconds", col("play_time_seconds").cast("string"))


# COMMAND ----------

# MAGIC %skip
# MAGIC SEV_batch2_ext.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(bronze_I_tbl) # Didn't work. 
# MAGIC # Databricks cannot cast the column to string which is the common type of Integer and String

# COMMAND ----------

# MAGIC %md
# MAGIC ![Schema Merge Incompatible Types Error](../images/error_schema_merge_incompatible_types.png "Error when mergeSchema=true cannot find common type between integer and string with non-numeric values")

# COMMAND ----------

SEV_batch2_ext2.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(bronze_I_tbl) # This time writing string column worked because `play_time_seconds` is a numeric string column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renamed column error?
# MAGIC * I'm really curious if only renaming a column can trigger Schema Mismatch Error. Let's find out

# COMMAND ----------

renamed_col_batch = generate_bronze_data(NUM_ROWS = 5_000)
renamed_col_batch = renamed_col_batch.withColumnRenamed("event_timestamp", "event_time")

# COMMAND ----------

# MAGIC %skip
# MAGIC renamed_col_batch.write.format("delta").mode("append").saveAsTable(bronze_I_tbl) # Even a simple column renaming can cause schema mismatch

# COMMAND ----------

# MAGIC %md
# MAGIC ![Schema Mismatch - Renamed Column Error](../images/error_schema_mismatch_renamed_column.png "Error when attempting to write DataFrame with renamed column to existing Delta table")

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {bronze_I_tbl}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type Widening
# MAGIC To test type widening, we'll do the simple workflow:
# MAGIC 1. Generate a batch
# MAGIC 2. Cast an INTEGER column to BigInt column and scale up the values so that the BigInt is actually need to store them
# MAGIC 3. Write the batch to bronze table

# COMMAND ----------

TW_batch = generate_bronze_data(NUM_ROWS = 5_000)

# COMMAND ----------

from pyspark.sql.types import LongType

TW_batch_widened = TW_batch.withColumn("play_time_seconds", col("play_time_seconds").cast(LongType()))

# COMMAND ----------

TW_batch_widened.printSchema()

# COMMAND ----------

multiplier = 2147483647
TW_batch_widened = TW_batch_widened.withColumn("play_time_seconds", multiplier*col("play_time_seconds"))

TW_batch = TW_batch.withColumn("play_time_seconds_overflowed", multiplier*col("play_time_seconds")) # Just for curiosity we want to see how pyspark reacts when it encounters number overflow

# COMMAND ----------

# MAGIC %skip
# MAGIC TW_batch.select('play_time_seconds_overflowed').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ![Integer Overflow Error](../images/error_integer_overflow.png "Error showing integer overflow when multiplying by 2147483647")

# COMMAND ----------

TW_batch_widened.select("play_time_seconds").show(5) # overflown contained :)

# COMMAND ----------

# MAGIC %skip
# MAGIC TW_batch_widened.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(bronze_I_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC Well, it's strange. It seems type widening doesn't work. I'd suggest calling the below reaction as "type narowing"
# MAGIC ![Type Widening/Narrowing Error](../images/error_type_widening_narrowing.png "Error when attempting type widening from int to long - Delta requires explicit type widening enablement")

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {bronze_I_tbl}")) # the MinWriterVersion is 7 so type widening should be enabled by default, but not?
# we'll change the metadata manually

# COMMAND ----------

# MAGIC %md
# MAGIC * Changing the table properties manually didn't solve the problem.

# COMMAND ----------

spark.sql(f"ALTER TABLE {bronze_I_tbl} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")

# COMMAND ----------

TW_batch_widened.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(bronze_I_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC # Column mapping: renaming and dropping

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {silver_I_tbl} SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
)
""")

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {silver_I_tbl} RENAME COLUMN event_timestamp TO event_time
""") # Renaming works as expected

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {silver_I_tbl}"))

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {silver_I_tbl} DROP COLUMN royalty_rate
""")

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {silver_I_tbl}")) # 'royalty_rate' column is gone

# COMMAND ----------

# MAGIC %md
# MAGIC Just for curiosity we'll see what happens if we don't set 'delta.columnMapping.mode' = 'name' but leave the default value (which is None) and try renaming a column

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test_table (
# MAGIC   id INT,
# MAGIC   ugly_column STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO test_table VALUES (1, 'Test');

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC ALTER TABLE test_table RENAME COLUMN ugly_column TO nice_column; -- We got an expected error, nice :D

# COMMAND ----------

# MAGIC %md
# MAGIC ![Column Mapping Mode Required Error](../images/error_column_mapping_mode_required.png "Error when attempting to rename column without enabling delta.columnMapping.mode")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS test_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Controlled Evolution & Data Contracts: The "Tap & Sewage Filter" Analogy
# MAGIC
# MAGIC ## Why Silent Schema Evolution Is Risky
# MAGIC
# MAGIC - **Silent schema evolution (`mergeSchema = true`) in production:**
# MAGIC   - Upstream teams may introduce breaking changes:
# MAGIC     - Renaming fields (e.g., `user_id` → `user_identification`)
# MAGIC     - Dropping critical fields without notice
# MAGIC   - **Result:** Silent evolution blindly accepts these mutations
# MAGIC     - Corrupted Silver tables
# MAGIC     - Blank dashboards for stakeholders
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Solution: Data Contracts & Quarantine Pattern
# MAGIC
# MAGIC ### 1. Data Contracts
# MAGIC
# MAGIC - **Explicit schema definitions**
# MAGIC - **Data quality rules**
# MAGIC - **Ingestion Gate (Contract Check):**
# MAGIC   - Validates incoming records against contracts
# MAGIC
# MAGIC ### 2. Quarantine (Dead Letter Queue) Pattern
# MAGIC
# MAGIC - **Valid records:**
# MAGIC   - Proceed to Silver layer
# MAGIC   - No breaking downstream dependencies
# MAGIC - **Malformed/contract-breaking records:**
# MAGIC   - Diverted into quarantine table (`silver_quarantine`)
# MAGIC
# MAGIC ### 3. Collaborative Remediation
# MAGIC
# MAGIC - **Alert triggers:**
# MAGIC   - Notifies data engineers
# MAGIC   - Collaboration with upstream source owners
# MAGIC   - Investigate root causes (e.g., missing keys, unexpected drops)
# MAGIC   - Update contract via Git PR
# MAGIC   - Reprocess quarantined records
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Analogy: Tap & Sewage Filter
# MAGIC
# MAGIC - **Washing dishes at a sink:**
# MAGIC   - If untreated sewage flows from the tap:
# MAGIC     - Shut off the valve to protect clean dishes
# MAGIC     - Install a bypass filter:
# MAGIC       - Clean water flows to dishes
# MAGIC       - Sewage diverted to holding tank
# MAGIC       - Filtered/processed water safely rejoins system
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > (Prosze mi wierzyć, że te analogię z szambem i zlewem wymyśliłem sam - to nie było AI :). AI jedynie sformatowało tekst, żeby przyjemnie się czytało.

# COMMAND ----------

# MAGIC %md
# MAGIC # Reliability & table maintenance 

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE, so compressing multiple, small parquet files into bigger ones

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {silver_I_tbl}"))

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {silver_II_tbl}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM - deleting older (smaller) files

# COMMAND ----------

display(spark.sql(f"VACUUM {silver_I_tbl} RETAIN 168 HOURS")) # Nothing was, I suppose, expected to happen because the table is relatively fresh (it's been here for a few minutes).

# COMMAND ----------

display(spark.sql(f"VACUUM {silver_II_tbl} RETAIN 168 HOURS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARTITION BY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test_partitioned (
# MAGIC   id INT,
# MAGIC   event_date DATE,
# MAGIC   amount DOUBLE
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (event_date); -- The PARTTION BY command is now shadowed which should tell you it's now recommended. Nice detail

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO test_partitioned VALUES (1, '2022-01-01', 100.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED test_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE test_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ZORDER BY

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test_zorder (
# MAGIC   user_id STRING,
# MAGIC   device_type STRING,
# MAGIC   click_count INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO test_zorder VALUES 
# MAGIC   ('usr_1', 'mobile', 5), 
# MAGIC   ('usr_2', 'desktop', 10), 
# MAGIC   ('usr_1', 'tablet', 2),
# MAGIC   ('usr_3', 'mobile', 8);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE test_zorder ZORDER BY (user_id, device_type);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE test_zorder;

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIQUID CLUSTERING

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE liquid_test_table (
# MAGIC   id STRING,
# MAGIC   event_time TIMESTAMP,
# MAGIC   temperature DOUBLE,
# MAGIC   device_id STRING
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (device_id, event_time);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED liquid_test_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE liquid_test_table;

# COMMAND ----------

spark.sql(f"ALTER TABLE {silver_I_tbl} CLUSTER BY (genre)")
spark.sql(f"OPTIMIZE {silver_I_tbl} FULL")

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {silver_I_tbl}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Rules
# MAGIC We'll apply the following data quality constrains:
# MAGIC - event_id musn't be NULL and (with event_time (formerly: event_timestamp)) has to be unique
# MAGIC - play_time_seconds has to be non-negative
# MAGIC

# COMMAND ----------

spark.sql(f"ALTER TABLE {silver_I_tbl} DROP CONSTRAINT IF EXISTS unique_event_id_event_time")
spark.sql(f"ALTER TABLE {silver_I_tbl} DROP CONSTRAINT IF EXISTS play_time_non_negative")
spark.sql(f"ALTER TABLE {silver_I_tbl} ALTER COLUMN event_time DROP NOT NULL")

# COMMAND ----------

# DBTITLE 1,Add remaining data quality constraints
spark.sql(f"ALTER TABLE {silver_I_tbl} ALTER COLUMN event_time SET NOT NULL")

spark.sql(f"""
    ALTER TABLE {silver_I_tbl} 
    ADD CONSTRAINT play_time_non_negative 
    CHECK (play_time_seconds >= 0)
""")

spark.sql(f"""
    ALTER TABLE {silver_I_tbl}
    ADD CONSTRAINT unique_event_id_event_time
    UNIQUE (event_id, event_time)
""")

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {silver_I_tbl}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Optional task

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuration as Code (YAML)
# MAGIC A data contract is implemented as a physical configuration file, typically YAML, stored directly within the project repository (e.g., using Databricks Asset Bundles). Instead of relying on passive text documentation, this file defines strict rules, such as expected columns, data types, and non-null constraints. The Databricks pipeline parses this file at runtime, creating an automated validation gate that checks all incoming data against these rules before ingestion.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Managing Upstream Changes via Pull Requests
# MAGIC Schema changes are no longer deployed without prior notice. If an upstream application team needs to add or modify a column, they are required to update the YAML contract and submit a Pull Request (PR). This workflow ensures that Data Engineers review, discuss, and approve the modifications before they reach the main branch, preventing unexpected pipeline failures in the production environment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Dead Letter Queue (Quarantine) Recovery
# MAGIC Records that fail the contract validation are diverted to a quarantine table (Dead Letter Queue). We never delete this data, as it holds business value. Instead, we apply recovery logic: missing values (such as a missing primary key) can be backfilled using joins with reference tables, and valid structural changes (like a column rename) can be resolved by updating the metadata using Delta Column Mapping. Once the data is corrected and meets the contract requirements, it is reprocessed and loaded into the Silver layer.