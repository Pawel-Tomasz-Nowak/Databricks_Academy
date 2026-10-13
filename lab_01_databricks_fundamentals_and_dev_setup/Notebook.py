# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Session, schema configuration and csv data loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verifying the CWD

# COMMAND ----------

# DBTITLE 1,Print Working Directory
import os
print(os.getcwd())

# COMMAND ----------

# MAGIC %md
# MAGIC ~~Defining an explicit schema for the data~~ (no need anymore since the data is in the unity catalog with correct schema defined)
# MAGIC
# MAGIC I'm using [this dataset](https://www.kaggle.com/datasets/uradkr/alabama-sold-real-estate-intelligence-2026) for my analysis

# COMMAND ----------

# DBTITLE 1,Cell 7
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, DateType

# alabama_real_estate_schema = StructType([
#     StructField("url", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("sub_type", StringType(), True),
#     StructField("listPrice", FloatType(), True),
#     StructField("lastSoldPrice", FloatType(), True),
#     StructField("soldOn", StringType(), True), 
#     StructField("sqft", FloatType(), True),
#     StructField("stories", FloatType(), True),
#     StructField("beds", FloatType(), True),
#     StructField("baths", FloatType(), True),
#     StructField("baths_full", FloatType(), True),
#     StructField("baths_full_calc", FloatType(), True),
#     StructField("garage", FloatType(), True),
#     StructField("year_built", FloatType(), True),
#     StructField("postal_code", StringType(), True),
#     StructField("is_valid_al_zip", BooleanType(), True),
#     StructField("sold_year", FloatType(), True),
#     StructField("sold_month", FloatType(), True),
#     StructField("sold_quarter", FloatType(), True),
#     StructField("property_age", FloatType(), True),
#     StructField("sold_to_list_ratio", FloatType(), True),
#     StructField("price_premium_pct", FloatType(), True),
#     StructField("negotiation_outcome", StringType(), True),
#     StructField("ratio_outlier_flag", BooleanType(), True),
#     StructField("price_per_sqft", FloatType(), True),
#     StructField("text_clean", StringType(), True),
#     StructField("pii_redacted_flag", BooleanType(), True)
# ])

# columns: list[str] = [f.name for f in alabama_real_estate_schema.fields if f.name != "url"]  # exclude url
# print(columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the data

# COMMAND ----------

# Create a widget for the login
dbutils.widgets.text("Login", defaultValue="", 
                     label="Login")

# COMMAND ----------

login = dbutils.widgets.get("Login")

# COMMAND ----------

absolute_data_path = f"""dbr_dev.{login}.alabama_sold_real_estate_intelligence_2026"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Making sure the provided absolute path does exist

# COMMAND ----------

# DBTITLE 1,Cell 14
# We're checking if the data path provided by the user exists or not - defensive programming
table_exists = spark.catalog.tableExists(absolute_data_path)

if table_exists:
    print(f"Loading data from {absolute_data_path}")
    alabama_df = spark.read.table(absolute_data_path)

    alabama_df.printSchema()
else:
    print(f"Table {absolute_data_path} does not exist.")
    raise Exception(f"Table {absolute_data_path} does not exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the schema

# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = f"{catalog}.{login}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Specifying the path of the sourced table

# COMMAND ----------

table_name = "alabama_sold_real_estate"
full_path = f"{schema}.{table_name}"

# COMMAND ----------

# DBTITLE 1,Cell 18
# (alabama_df.write.
#  format("delta").mode("overwrite").option("overwriteSchema", "true").
#  saveAsTable(full_path)
#  )

# COMMAND ----------

# MAGIC %md
# MAGIC # Simple spark transformations (grouping, sorting, filtering etc.)

# COMMAND ----------

alabama_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the columns for clarity

# COMMAND ----------

# Rename columns to shorter, relevant names
short_names = {
    "property_id": "pid",
    "address": "addr",
    "city": "cty",
    "state": "st",
    "zip_code": "zip",
    "listing_price": "price",
    "bedrooms": "beds",
    "bathrooms": "baths",
    "square_feet": "sqft",
    "listing_date": "list_date",
    "year_built": "year_built",
    "property_type": "prop_type",
    "lot_size": "lotsz",
    "hoa_fee": "hoa",
    "description": "desc",
    "agent_name": "agent",
    "agent_phone": "agent_phone",
    "status": "stat"
}


alabama_df = alabama_df.withColumnsRenamed(short_names) # Much faster than selecting each column separately
display(alabama_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Counting missing values across each column

# COMMAND ----------

from pyspark.sql.functions import when, isnull, count, col

null_count = alabama_df.select(*[count(when(isnull(c), c)).alias(c) for c in alabama_df.columns])
null_count.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping and sorting

# COMMAND ----------

from pyspark.sql.functions import avg, min, max, count, round

# Let's only select interesting columns
df_subset = alabama_df.select("sub_type", "baths", "beds")

# Group by subtypes and take min, max and average of number of baths and beds
grouped_df = df_subset.groupby("sub_type").agg(
    count("*").alias("count"),
    min("baths").alias("min_baths"),
    max("baths").alias("max_baths"),
    round(avg("baths"), 1).alias("avg_baths"),
    min("beds").alias("min_beds"),
    max("beds").alias("max_beds"),
    round(avg("beds"), 1).alias("avg_beds")
)

display(grouped_df) # Why some houses don't have any baths in there? 

# COMMAND ----------

no_bath_props = alabama_df.filter(col("baths")==0).select("type", "sub_type", "baths")
no_bath_prop_type_count = no_bath_props.groupby("sub_type").agg(count("*").alias("count"))

# COMMAND ----------

display(no_bath_prop_type_count) # Some houses don't have baths or maybe the issue is data quality.

# COMMAND ----------

from pyspark.sql.functions import desc, col

grouped_sorted_df = grouped_df.sort(desc(col("count")))
display(grouped_sorted_df) # Single family houses are the most common property type in Alabama followed up by condos

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading data from external API

# COMMAND ----------

# MAGIC %md
# MAGIC * Just a sample of the data to get the idea of what the json structure is like in this case
# MAGIC ![image_1783362421351.png](./image_1783362421351.png "image_1783362421351.png")

# COMMAND ----------

# DBTITLE 1,Cell 30
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType


# Downloading the data from external API to the driver node
url = "https://api.nbp.pl/api/exchangerates/tables/A/?format=json"  # Exchange rates by national poland bank (NBP)
response = requests.get(url)

if response.status_code == 200: # 200 is okay, 404 is not found
    raw_data = response.json()

    # Define an explicit schema for the data (it's a list of dictionaries)   
    schema = StructType([        StructField("rates", ArrayType(StructType([
            StructField("currency", StringType(), True),
            StructField("code", StringType(), True),
            StructField("mid", DoubleType(), True)
        ])), True),

        StructField("table", StringType(), True),
        StructField("no", StringType(), True),
        StructField("effectiveDate", StringType(), True)
    ])
    df_raw = spark.createDataFrame(raw_data, schema=schema) # Read the data

    # Explode the rates column and select relevant renamed columns
    df_silver = df_raw.withColumn("rates_flat", explode(col("rates"))) \
                      .select(
                          col("effectiveDate").alias("Date"),
                          col("rates_flat.code").alias("Currency_Code"),
                          col("rates_flat.mid").alias("Exchange_Rate")
                      )

    print(df_silver.show(5))
    print(df_silver.printSchema())
else:
    print(f"Error while downloading the data from API. Status code {response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Optional question:
# MAGIC Optional additions: explain Delta Lake benefits (ACID, time travel, schema enforcement)

# COMMAND ----------

# MAGIC %md
# MAGIC **Delta Lake: Core Concepts**
# MAGIC Delta Lake adds a management layer on top of object storage to fix common issues with distributed data processing.
# MAGIC
# MAGIC **ACID Transactions**
# MAGIC Traditional storage (like CSV) can be corrupted if a write operation is interrupted (e.g., a node fails). Delta Lake solves this with a Transaction Log.
# MAGIC
# MAGIC How it works: All changes are recorded in a log (_delta_log/). A write is only considered complete if it is successfully recorded in this log. This ensures "All-or-Nothing" operations, keeping your data consistent even if tasks fail.
# MAGIC
# MAGIC
# MAGIC **Time Travel**
# MAGIC Delta Lake tracks the version history of your data. When you overwrite a table, Delta does not immediately delete the old files.
# MAGIC
# MAGIC How it works: Because the transaction log stores the state of the table at each commit, you can query specific versions by timestamp or version ID.
# MAGIC
# MAGIC Maintenance: Over time, old files accumulate in storage. Use the VACUUM command periodically to delete files that are no longer needed by the log, which helps control storage costs.
# MAGIC
# MAGIC
# MAGIC **Schema Enforcement & Evolution**
# MAGIC Delta Lake protects data quality by acting as a guardrail for your table structure.
# MAGIC
# MAGIC Schema Enforcement: By default, Delta blocks any write that doesn't match the existing table schema (e.g., trying to write a String into an Integer column). This prevents "silent" data corruption.
# MAGIC
# MAGIC Schema Evolution: If you need to update the schema (e.g., adding new columns from an API), you can enable it explicitly:
# MAGIC * df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Extra exercise - moving from bronze layer to gold one. (Thank you Artem for the inspiration)!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC * The dataset, alabama_df, is already raw (it's in the bronze layer), so there's no need to modify it.

# COMMAND ----------

(alabama_df.write.
 format("delta").mode("overwrite").option("overwriteSchema", "true").
 saveAsTable(full_path+"_bronze")
 )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver layer
# MAGIC Select only the most important coluns and get rid of the records with missing values them

# COMMAND ----------

relevant_columns: list[str] =[
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

# COMMAND ----------

silver_alabama_df = alabama_df.select(relevant_columns)
display(silver_alabama_df)

# COMMAND ----------

no_records_before = silver_alabama_df.count()

# COMMAND ----------

silver_alabama_df = silver_alabama_df.na.drop()

# COMMAND ----------

no_records_after = silver_alabama_df.count()
print(f"Number of records before dropping nulls: {no_records_before}")
print(f"Number of records after dropping nulls: {no_records_after}")
row_count_change_pct = (no_records_after - no_records_before) / no_records_before * 100
print(f"Percentage change in number of records: {row_count_change_pct:.2f}%")


# COMMAND ----------

# Save the silver DataFrame as a Delta table
full_path = "alabama_sold_real_estate_silver"
schema = f"dbr_dev.{login}"

silver_alabama_df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{full_path}_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold layer
# MAGIC Group by the sold date and property type and aggregate basic properties

# COMMAND ----------

golden_alabama_df = silver_alabama_df.groupBy(col("type").alias('property_type'), col("soldOn").alias('sale_date')).agg(
    avg("listPrice").alias("average_listed_price"),
    avg("lastSoldPrice").alias("average_sold_price"),
    avg("sqft").alias("average_sqft"),
    avg("stories").alias("average_stories"),
    avg("beds").alias("average_beds"),
    avg("baths").alias("average_baths"),
)

# COMMAND ----------

golden_alabama_df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}_gold.alabama_sold_real_estate_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC