# Lab 03 — Streaming and Incremental Ingestion

This lab compares file-based and event-driven ingestion patterns in Databricks: Auto Loader reads files arriving in a Volume, while Azure Event Hubs carries music events into a streaming pipeline.

## What is demonstrated

- Generating partitioned input data and discovering new files with CloudFiles Auto Loader.
- Configuring checkpointing and incremental processing so already-seen files are not treated as new input.
- Connecting to Azure Event Hubs with the `azure-eventhub` package.
- Retrieving music data from the YouTube API and publishing events to Event Hubs.
- Building a continuous streaming pipeline from Event Hubs to a Databricks target.
- Comparing at-least-once and exactly-once processing semantics, including the role of checkpoints and idempotent sinks.

`EventHub_AutoLoader.ipynb` and `metallica_event_hubs.ipynb` contain closely related Event Hubs exercises; the latter focuses on the Metallica example. `optional_addiitions.ipynb` contains the conceptual follow-up material.

## Directory structure

```text
lab_03_streaming_and_incremental_ingestion/
├── CloudFiles_AutoLoader.ipynb       # Partitioned file generation and Auto Loader stream
├── EventHub_AutoLoader.ipynb         # Event Hubs connection and continuous pipeline
├── metallica_event_hubs.ipynb        # Music/Metallica Event Hubs variant
├── optional_addiitions.ipynb         # Delivery guarantees and streaming notes
└── README.md                         # This documentation
```

## Running the lab

1. Attach the notebooks to a Databricks cluster with access to a Unity Catalog Volume.
2. For `CloudFiles_AutoLoader.ipynb`, set the catalog, schema, and volume widgets, run the data-generation cells, and then start the streaming query with a persistent checkpoint path.
3. For the Event Hubs notebooks, install `azure-eventhub` when prompted and configure the Event Hubs connection through a secret scope or notebook parameters.
4. Configure the YouTube API credentials required by the selected example.
5. Run the producer and streaming cells in order. Inspect the checkpoint and target table to verify that subsequent batches process only newly available data.

Connection strings, API keys, and checkpoint locations are environment-specific and must not be committed to the repository.
