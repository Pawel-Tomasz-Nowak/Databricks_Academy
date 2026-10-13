# Databricks notebook source
# MAGIC %md
# MAGIC # Setting up login, tables names and file paths

# COMMAND ----------

dbutils.widgets.text("login", "", "Enter your login")
dbutils.widgets.text("catalog", "", "Enter your catalog")

# COMMAND ----------

login = dbutils.widgets.get("login")
catalog = dbutils.widgets.get("catalog")

data_name = "alabama_sold_real_estate"

source_schema = f"{login}_bronze"
target_schema = f"{login}_silver"

source_table_name = f"{catalog}.{login}.{data_name}_bronze"
target_table_name = f"{catalog}.{target_schema}.{data_name}_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC # Proper workflow

# COMMAND ----------

# DBTITLE 1,Cell 5
from delta.tables import DeltaTable

# Columns to clean (drop nulls)
clean_cols = [
    "type",
    "listPrice",
    "lastSoldPrice",
    "soldOn",
    "sqft",
    "stories",
    "beds",
    "baths",
    "baths_full",
    "baths_full_calc",
    "garage",
    "year_built",
    "postal_code",
    "property_age",
]

# Read bronze table and clean records
bronze_df = spark.read.table(source_table_name)
cleaned_bronze_df = bronze_df.dropna(subset=clean_cols)

# Check if silver table exists
silver_table_exists = spark.catalog.tableExists(target_table_name)


if not silver_table_exists:
    # Write cleaned bronze records to silver table
    cleaned_bronze_df.write.mode("overwrite").saveAsTable(target_table_name)
else:
    # Merge cleaned bronze records into existing silver table
    silver_table = DeltaTable.forName(spark, target_table_name)
    (
        silver_table.alias("target")
        .merge(
            cleaned_bronze_df.alias("source"),
            "target.Index = source.Index"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )