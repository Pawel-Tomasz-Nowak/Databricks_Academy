# 01_landing / pawel_project — Data Ingestion

Handles all data acquisition for Pawel's asset within the [gdelt-financial-anomalies](../../README.md)
pipeline. Two independent ingestion pipelines land data into Unity Catalog:

* **Music metadata** — a one-time CSV read that seeds the silver metadata table used for video ID lookup.
* **YouTube engagement snapshots** — a periodic API fetch that produces timestamped JSON files,
  followed by an Auto Loader streaming write to the bronze Delta table.

---

## Pipelines

### 1. Music Metadata Ingestion

**Notebook:** `music_metadata_ingestion`  
**Run frequency:** Once (or when the source CSV changes)  
**Source:** `/Volumes/dbr_dev/music_analytics/raw_landing_zone/music_metadata/music_discography.csv`  
**Target:** `dbr_dev.music_analytics.silver_music_metadata`

Steps:

1. Read `music_discography.csv` with an explicit schema (`;` delimiter, `dd.MM.yyyy` date format, UTF-8 encoding).
2. Enrich with bronze audit columns: `ingest_timestamp` (`current_timestamp()`) and `ingest_file` (`_metadata.file_path`).
3. Cast `album_release_date` from `StringType` to `DateType`.
4. Overwrite `silver_music_metadata` (full refresh; `overwriteSchema=true` to allow evolution).

> **Note** The metadata CSV is treated as already clean — no deduplication or NULL handling is applied.
> The table is written to the silver layer directly despite the landing-layer location.

---

### 2. YouTube Stats Snapshot + Bronze Streaming Ingestion

A two-stage pipeline. Stage A writes a JSON snapshot to the Volume; Stage B reads all new snapshots into the bronze Delta table via Auto Loader.

#### Stage A — Fetch & Persist snapshot

**Entry point:** `popularity_data_scripts/save_yt_stats_snapshot.py`  
**Run frequency:** Periodic (e.g. hourly via Databricks job)  
**Target:** `/Volumes/dbr_dev/music_analytics/raw_landing_zone/yt_snapshots/yt_stats_<YYYYMMDD_HHMMSS>.json`

Call chain:

```
save_yt_stats_snapshot.py
  └── fetch_yt_video_stats.py  →  read_data_from_api(batch_size=50)
        └── video_id_extractor.py  →  find_video_ids(table)
```

1. `find_video_ids()` reads `silver_music_metadata`, parses the 11-character `v=<id>` query
   parameter from each `url` column via regex, and returns a deduplicated ID list.
2. `read_data_from_api()` splits IDs into batches of up to 50 (YouTube API hard cap),
   calls the **YouTube Data API v3** `/videos` endpoint (`part=snippet,statistics`),
   and assembles one record per video with the fields below.
3. `save_yt_stats_snapshot.py` serialises the record list to a timestamped JSON file
   in the landing Volume.

Record schema written to JSON:

| Field | Type | Source |
| --- | --- | --- |
| `_ingested_at` | ISO-8601 string | `datetime.now()` at fetch time |
| `video_id` | string | YouTube item `id` |
| `album` | string | Looked up from metadata table |
| `published_at` | string | `snippet.publishedAt` |
| `video_title` | string | `snippet.title` |
| `view_count` | int \| None | `statistics.viewCount` (coerced) |
| `like_count` | int \| None | `statistics.likeCount` (coerced) |
| `comment_count` | int \| None | `statistics.commentCount` (coerced) |
| `author` | string | Looked up from metadata table |
| `song_title` | string | Looked up from metadata table |

#### Stage B — Stream JSON snapshots to bronze

**Entry point:** `popularity_data_scripts/stream_yt_stats_to_bronze.py`  
**Run frequency:** After each Stage A run (or on the same job schedule)  
**Source:** `/Volumes/dbr_dev/music_analytics/raw_landing_zone/yt_snapshots/` (Auto Loader)  
**Target:** `dbr_dev.music_analytics.bronze_music_stats`

1. Auto Loader (`cloudFiles.format=json`, `multiline=true`) reads all new JSON files since the last
   checkpoint. Schema is inferred and cached at `bronze_schema_path`; unexpected columns are rescued
   into `_rescued_data`.
2. Adds `_source_file` from `_metadata.file_path`.
3. Writes with `trigger(availableNow=True)` + `awaitTermination()` — processes all pending files
   then stops. Checkpoint is persisted at `bronze_checkpoint_path`.

> **Warning** If `bronze_music_stats` was pre-created without columns (e.g. by an earlier
> `music_pipeline_setup.py` run), Delta will reject the write with a schema mismatch.
> Drop the table before running: `spark.sql("DROP TABLE IF EXISTS dbr_dev.music_analytics.bronze_music_stats")`
> and clear the checkpoint: `dbutils.fs.rm(bronze_checkpoint_path, recurse=True)`.

---

## Directory Structure

```text
01_landing/pawel_project/
├── music_metadata_ingestion       # Notebook: CSV → silver_music_metadata (run once)
├── popularity_data_scripts/
│   ├── video_id_extractor.py          # find_video_ids() — regex-parses v= IDs from URLs
│   ├── fetch_yt_video_stats.py        # read_data_from_api() — YT Data API v3 batch client
│   ├── save_yt_stats_snapshot.py      # Stage A entry point: fetches + writes JSON to Volume
│   └── stream_yt_stats_to_bronze.py   # Stage B entry point: Auto Loader → bronze_music_stats
└── archive/                           # Deprecated notebooks — do not run
    ├── 01c_yt_stats_snapshot          # Superseded by save_yt_stats_snapshot.py
    └── 01c_music_metadata_ingestion   # Superseded by music_metadata_ingestion notebook
```

---

## Prerequisites

| Requirement | Detail |
| --- | --- |
| Setup complete | `00_setup/pawel_project/music_pipeline_setup.py` run at least once |
| Silver metadata table | `music_metadata_ingestion` notebook executed at least once (Stage A depends on it) |
| YouTube API key | Stored in Databricks secret scope `pawelnowak2004pri219_scope`, key `pawelnowak-youtube-api` |
| Shared compute | USER_ISOLATION data security mode (Spark Connect) |

---

## Running in Isolation

```bash
# One-time: seed the silver metadata table
# Run music_metadata_ingestion notebook from the Databricks UI

# Periodic Stage A: fetch a fresh YouTube snapshot and write to Volume
%run 01_landing/pawel_project/popularity_data_scripts/save_yt_stats_snapshot.py

# Periodic Stage B: stream all new JSON files to the bronze Delta table
%run 01_landing/pawel_project/popularity_data_scripts/stream_yt_stats_to_bronze.py
```
