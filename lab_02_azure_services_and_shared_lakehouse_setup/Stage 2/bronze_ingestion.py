# Databricks notebook source
# MAGIC %md
# MAGIC # Setting up login, tables names and file paths

# COMMAND ----------

dbutils.widgets.text("login", "", "Enter your login")
dbutils.widgets.text("catalog", "", "Enter your catalog")

# COMMAND ----------

login = dbutils.widgets.get("login")
catalog = dbutils.widgets.get("catalog")

source_data_name = "alabama_sold_real_estate"
source_schema:str = login
target_schema = f"{login}_bronze"


source_table_name = f"{catalog}.{login}.{source_data_name}_intelligence_2026"
target_table_name = f"{catalog}.{login}_bronze.{source_data_name}_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC # Proper workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the metadata from the source table

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import current_timestamp, col

# Read source table and add metadata columns
raw_df = spark.read.table(source_table_name)
updates_raw_df = (
    raw_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_filename", col("_metadata.file_path"))
)

# Check if target table exists
target_table_exists = spark.catalog.tableExists(target_table_name)

if not target_table_exists:
    # If target table doesn't exist, write updates_raw_df as new table
    updates_raw_df.write.mode("overwrite").saveAsTable(target_table_name)
else:
    from delta.tables import DeltaTable

    # Read target table and check for missing metadata columns
    bronze_table = DeltaTable.forName(spark, target_table_name)
    bronze_df = bronze_table.toDF()
    missing_cols = []
    if "ingestion_timestamp" not in bronze_df.columns:
        missing_cols.append(("ingestion_timestamp", current_timestamp()))
    if "source_filename" not in bronze_df.columns:
        missing_cols.append(("source_filename", col("_metadata.file_path")))

    # Add missing columns if necessary
    if missing_cols:
        for col_name, col_expr in missing_cols:
            bronze_df = bronze_df.withColumn(col_name, col_expr)
        bronze_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table_name)

    # Merge updates_raw_df into target table
    (
        bronze_table.alias("target")
        .merge(
            updates_raw_df.alias("source"),
            "target.Index = source.Index"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# target_table_exists = spark.catalog.tableExists(target_table_name)

# if target_table_exists:
#     from delta.tables import DeltaTable
#     print(f"Target table already exists")

#     bronze_table = DeltaTable.forName(spark, target_table_name)

#     # Add new columns to the target table if they don't exist
#     bronze_df = bronze_table.toDF()
#     if "ingestion_timestamp" not in bronze_df.columns or "source_filename" not in bronze_df.columns:
#         bronze_df = bronze_df.withColumn("ingestion_timestamp", current_timestamp()) \
#                              .withColumn("source_filename", col("_metadata.file_path"))
#         bronze_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table_name)

#     (bronze_table.alias("target")
#      .merge(
#          updates_raw_df.alias("source"),
#          "target.Index = source.Index"
#      )
#      .whenMatchedUpdateAll()
#      .whenNotMatchedInsertAll()
#      .execute()
#     )
# else:
#     updates_raw_df.write.mode("overwrite").saveAsTable(target_table_name)