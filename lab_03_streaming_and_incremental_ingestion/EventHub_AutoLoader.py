# Databricks notebook source
# MAGIC %md
# MAGIC ## Testing YouTube API connection and music data retreival

# COMMAND ----------

# MAGIC %pip install azure-eventhub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("Youtube_API_Key", "")
YOUTUBE_API_KEY = dbutils.widgets.get("Youtube_API_Key")

# COMMAND ----------

# DBTITLE 1,Cell 5
import requests


def retrieve_music_stat_data(video_url: str) -> None:
    """Fetch and print headline YouTube statistics for a single video URL."""
    video_id = video_url.split("v=")[1]

    # YouTube Data API v3 endpoint for video metadata and engagement metrics.
    api_url = (
        f"https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    )

    response = requests.get(api_url)
    result = response.json()

    if "items" in result and result["items"]:
        video = result["items"][0]
        info = video["snippet"]
        stats = video["statistics"]

        print("Title:", info["title"])
        print("Views:", stats.get("viewCount"))
        print("Likes:", stats.get("likeCount"))
        print("Comments:", stats.get("commentCount"))
    else:
        print("Could not fetch video details.")

# COMMAND ----------

print(retrieve_music_stat_data("https://www.youtube.com/watch?v=BQ6WtkEbXiM"))

# COMMAND ----------

print(retrieve_music_stat_data("https://www.youtube.com/watch?v=dtyEENLOmbI"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Connecting to Azure Hubs

# COMMAND ----------

connection_string = dbutils.secrets.get("pawelnowak2004pri219_scope", "pawelnowak-eventhub-cs")
event_hub_name = dbutils.secrets.get("pawelnowak2004pri219_scope","pawelnowak-eventhub-key-name")

# COMMAND ----------

# DBTITLE 1,Cell 10
import json
import time
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient


def fetch_youtube_stats(video_url: str) -> dict | None:
    """Fetch structured YouTube statistics for one video and prepare an event payload."""
    video_id = video_url.split("v=")[1]
    api_url = (
        f"https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    )

    response = requests.get(api_url)
    data = response.json()

    if "items" not in data or not data["items"]:
        return None

    item = data["items"][0]
    snippet = item["snippet"]
    stats = item["statistics"]

    return {
        "video_id": video_id,
        "title": snippet.get("title"),
        "channel_title": snippet.get("channelTitle"),
        "view_count": int(stats.get("viewCount", 0)),
        "like_count": int(stats.get("likeCount", 0)),
        "comment_count": int(stats.get("commentCount", 0)),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

# COMMAND ----------

# DBTITLE 1,Cell 11
def run_producer(
    video_url: str = "https://www.youtube.com/watch?v=BQ6WtkEbXiM",
    n_events: int = 3,
) -> None:
    """Send YouTube statistics to Azure Event Hubs as JSON events."""
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=event_hub_name,
    )

    print("Starting Event Hub producer...")

    try:
        with producer:
            for i in range(n_events):
                event_payload = fetch_youtube_stats(video_url)

                if event_payload:
                    event_data_batch = producer.create_batch()
                    json_data = json.dumps(event_payload)

                    # Each event contains a fresh snapshot of the selected video's stats.
                    event_data_batch.add(EventData(json_data))
                    producer.send_batch(event_data_batch)

                    print(f"[{i + 1}/{n_events}] Sent event at: {event_payload['fetched_at']}")

                if i < n_events - 1:
                    time.sleep(10)
    except Exception as e:
        print(f"Error sending to Event Hub: {e}")


# COMMAND ----------

run_producer()

# COMMAND ----------

# MAGIC %md
# MAGIC # Defining the final continuous pipeline

# COMMAND ----------

# DBTITLE 1,Cell 14

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

host_domain = connection_string.split("sb://")[1].split("/")[0]
BOOTSTRAP_SERVERS = f"{host_domain}:9093"

# JAAS config for connecting to Azure Event Hubs through the Kafka interface.
eh_sasl = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="$ConnectionString" password="{connection_string}";'
)

df_raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", event_hub_name)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", eh_sasl)
    .option("startingOffsets", "latest")
    .load()
)

youtube_event_schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("view_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("comment_count", LongType(), True),
    StructField("fetched_at", StringType(), True),
])

# COMMAND ----------

# DBTITLE 1,Cell 15
# Parse the Kafka payload and add ingestion metadata for the bronze table.
df_parsed_stream = (
    df_raw_stream
    .select(from_json(col("value").cast("string"), youtube_event_schema).alias("data"), col("timestamp"))
    .select(
        col("data.video_id"),
        col("data.title"),
        col("data.view_count"),
        col("data.like_count"),
        col("data.comment_count"),
        col("data.fetched_at").cast("timestamp").alias("event_timestamp"),
        col("timestamp").alias("event_hub_enqueued_time"),
        current_timestamp().alias("ingestion_time"),
    )
)

# COMMAND ----------

# Define catalog name, user_name
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("user_name","")

catalog_name = dbutils.widgets.get("catalog_name")
user_name = dbutils.widgets.get("user_name")


# COMMAND ----------

# Define the volume UC-path
volume_name = "Youtube_data"
volume_path = f"{catalog_name}.{user_name}.{volume_name}"

# Define the directory-like volume path
volume_dir_path = f"/Volumes/{catalog_name}/{user_name}/{volume_name}/landing_zone/"

# COMMAND ----------

# Create the volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")

# COMMAND ----------

# Create a landing directory
dbutils.fs.mkdirs(volume_dir_path)

# COMMAND ----------

# Define the path to the bronze table and checkpoint
target_bronze_table = f"{catalog_name}.{user_name}_bronze.youtube_video_stats_bronze"

checkpoint_loc = f"{volume_dir_path}/checkpoint"

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {target_bronze_table}")
dbutils.fs.mkdirs(checkpoint_loc)

# COMMAND ----------

query = (
    df_parsed_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_loc)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .table(target_bronze_table)
)

query.awaitTermination()