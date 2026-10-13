# Databricks notebook source
# MAGIC %md
# MAGIC ## Testing YouTube API connection and music data retreival

# COMMAND ----------

# MAGIC %pip install azure-eventhub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("Youtube API Key", "")
YOUTUBE_API_KEY = dbutils.widgets.get("Youtube API Key")

# COMMAND ----------

import requests

def retrieve_music_stat_data(video_url: str):
    VIDEO_ID = video_url.split("v=")[1]

    # YouTube Data API v3 endpoint for video stats
    api_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={VIDEO_ID}&key={YOUTUBE_API_KEY}"

    resp = requests.get(api_url)
    result = resp.json()

    if "items" in result and result["items"]:
        vid = result["items"][0]
        info = vid["snippet"]
        stats = vid["statistics"]
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

import json
import time
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

def fetch_youtube_stats(video_url: str):
    VIDEO_ID = video_url.split("v=")[1]
    
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={VIDEO_ID}&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    data = response.json()
    
    if "items" in data and len(data["items"]) > 0:
        item = data["items"][0]
        stats = item["statistics"]
        snippet = item["snippet"]
        payload = {
            "video_id": VIDEO_ID,
            "title": snippet.get("title"),
            "channel_title": snippet.get("channelTitle"),
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
        return payload
    return None

# COMMAND ----------

def run_producer(video_url: str = "https://www.youtube.com/watch?v=BQ6WtkEbXiM"):
    """Creates an Event Hub client and sends messages."""
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=event_hub_name
    )
    
    n_events: int = 10
    print("Starting Event Hub producer...")
    try:
        with producer:
            for i in range(n_events):
                event_payload = fetch_youtube_stats(video_url)
                if event_payload:
                    event_data_batch = producer.create_batch()
                    json_data = json.dumps(event_payload)
                    
                    event_data_batch.add(EventData(json_data))
                    producer.send_batch(event_data_batch)
                    
                    print(f"[{i+1}/{n_events}] Sent event at: {event_payload['fetched_at']}")
                
                time.sleep(2)
    except Exception as e:
        print(f"Error sending to Event Hub: {e}")


# COMMAND ----------

run_producer()

# COMMAND ----------

# MAGIC %md
# MAGIC # Defining the final continuous pipeline

# COMMAND ----------


from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

ehConf = {
    'eventhubs.connectionString': connection_string,
    'eventhubs.startingPosition': json.dumps({
        "offset": "@latest",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True
    })
}

df_raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

youtube_event_schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("view_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("comment_count", LongType(), True),
    StructField("fetched_at", StringType(), True)
])

# COMMAND ----------

df_parsed_stream = (
    df_raw_stream
    .select(from_json(col("body").cast("string"), youtube_event_schema).alias("data"), col("enqueuedTime"))
    .select(
        col("data.video_id"),
        col("data.title"),
        col("data.view_count"),
        col("data.like_count"),
        col("data.fetched_at").cast("timestamp").alias("event_timestamp"),
        col("enqueuedTime").alias("event_hub_enqueued_time"),
        current_timestamp().alias("ingestion_time")
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
# schema_loc = f"{volume_dir_path}/schema"

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {target_bronze_table}")
dbutils.fs.mkdirs(checkpoint_loc)

# COMMAND ----------

query = (
    df_parsed_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_loc)
    .outputMode("append")
    .trigger(availableNow=True)
    .table(target_bronze_table)
)

query.awaitTermination()