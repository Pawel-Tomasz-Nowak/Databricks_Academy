# 00_setup / pawel_project — Shared Configuration & One-Time Setup

Centralises all Unity Catalog identifiers, Volume path constants, and idempotent setup helpers
used across every stage of Pawel's pipeline in [gdelt-financial-anomalies](../../README.md).
Run the setup notebook **once** before executing any other pipeline step.

---

## Local Logic

### `music_pipeline_setup.py` — Configuration Module

Import this module in any pipeline script to access shared constants and setup helpers.
When executed directly (not imported), it also runs all setup functions to initialise
the Unity Catalog environment.

**Exported constants:**

| Constant | Value | Description |
| --- | --- | --- |
| `catalog_name` | `dbr_dev` | Unity Catalog target catalog |
| `music_schema` | `music_analytics` | Target schema |
| `volume_name` | `raw_landing_zone` | Landing Volume name |
| `json_landing_path` | `/Volumes/dbr_dev/music_analytics/raw_landing_zone/yt_snapshots` | JSON snapshot drop location |
| `music_metadata_file` | `/Volumes/.../music_metadata/music_discography.csv` | Source metadata CSV |
| `music_stats_tables` | `{"bronze": "dbr_dev.music_analytics.bronze_music_stats", ...}` | Fully-qualified Delta table names per layer |
| `bronze_checkpoint_path` | `/Volumes/.../checkpoints/bronze_music_stats` | Auto Loader checkpoint directory |
| `bronze_schema_path` | `/Volumes/.../schemas/bronze_music_stats` | Auto Loader schema inference cache |
| `silver_music_metadata_table` | `dbr_dev.music_analytics.silver_music_metadata` | Silver metadata table |
| `yt_api_key` | *(from secret scope)* | YouTube Data API v3 key, fetched at import time |
| `yt_video_url` | `https://www.googleapis.com/youtube/v3/videos` | YT API endpoint |

**Setup helpers** (idempotent — safe to call multiple times):

| Function | SQL equivalent |
| --- | --- |
| `setup_catalog()` | `USE CATALOG dbr_dev` |
| `setup_schema()` | `CREATE SCHEMA IF NOT EXISTS music_analytics` |
| `setup_volume()` | `CREATE VOLUME IF NOT EXISTS music_analytics.raw_landing_zone` |
| `setup_music_metadata_dir()` | Creates `/Volumes/.../music_metadata/` directory |
| `set_up_jsons_landing_dir()` | Creates `/Volumes/.../yt_snapshots/` directory |
| `setup_silver_music_metadata_table()` | `CREATE TABLE IF NOT EXISTS silver_music_metadata` |
| `setup_music_stats_table(type)` | `CREATE TABLE IF NOT EXISTS {bronze/silver/gold}_music_stats` |

> **Warning** `setup_music_stats_table()` creates Delta tables **without columns**.
> Do **not** call it for `bronze_music_stats`, `silver_music_stats`, or `gold_music_stats`
> before the streaming writer runs — doing so causes a schema mismatch error.
> These tables are intentionally excluded from the module-level auto-run.

**Import behaviour:**

```python
# In pipeline scripts — constants only, no setup side-effects:
from music_pipeline_setup import json_landing_path, music_stats_tables

# Run directly as __main__ — executes all setup functions once:
%run 00_setup/pawel_project/music_pipeline_setup.py
```

---

## Directory Structure

```text
00_setup/pawel_project/
├── music_pipeline_setup.py   # Central config module — import this everywhere
├── setup                     # Notebook: one-time environment setup (run before all else)
```

---

## Prerequisites

| Requirement | Detail |
| --- | --- |
| Unity Catalog enabled | Workspace must have UC enabled on `dbr_dev` catalog |
| Secret scope | A Databricks secret scope containing the YouTube Data API v3 key must exist. Set `DBRICKS_SECRET_SCOPE` and `DBRICKS_SECRET_KEY` in `.env` or as cluster environment variables. |
| Shared compute | USER_ISOLATION data security mode (Spark Connect) |

---

## Running the Setup

```bash
# Run once before any other pipeline step.
# Creates catalog context, schema, volume, directories, and the silver metadata table.
%run 00_setup/pawel_project/music_pipeline_setup.py
```

Alternatively, open and run the `setup` notebook from the Databricks UI — it calls
`music_pipeline_setup.py` and confirms the environment is ready.
