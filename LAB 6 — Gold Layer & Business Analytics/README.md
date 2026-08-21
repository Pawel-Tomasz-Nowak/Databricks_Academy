# LAB 5 — Music Popularity Spike Detection

A Declarative Automation Bundle (DABs) project that ingests YouTube engagement
signals, normalises them through a medallion pipeline (bronze → silver → gold),
and exposes minute-level aggregate tables for trend analysis. Orchestration is
handled by a three-task Lakeflow Job; the transformation pipeline runs as a
Lakeflow Spark Declarative Pipeline (SDP) using the `pyspark.pipelines` API.

---

## Architecture

```text
YouTube Data API v3
        │
        ▼  (Task 2: fetch_youtube_data)
save_yt_stats_snapshot.py
        │  writes timestamped JSON files
        ▼
/Volumes/{catalog}/{schema}/landing_zone/yt_snapshots/
        │
        │  Auto Loader (cloudFiles/json, schema evolution)
        ▼
bronze_music_stats          ◄── music_discography.csv
        │                             (static CSV, semicolon-delimited)
        │  cast · deduplicate · DQ expectations
        │  per-hour delta enrichment (window lag over video_id)
        ▼
silver_music_stats
silver_music_metadata       ◄── current snapshot by video_id
silver_music_metadata_history    (SCD Type 2 via apply_changes_from_snapshot)
        │
        │  minute-level groupBy aggregations
        ▼
gold_music_stats_by_author
gold_music_stats_by_video
```

---

## Project Structure

```text
LAB 5 — Declarative Pipelines (Lakeflow)/
├── databricks.yml                              # DABs bundle: variables, pipeline, job, dev/prod targets
├── README.md                                   # This file
├── src/
│   ├── __init__.py
│   ├── setup/
│   │   ├── __init__.py
│   │   └── music_pipeline_setup.py             # Shared config, path resolver, UC bootstrap
│   ├── producer/
│   │   ├── __init__.py
│   │   ├── fetch_yt_video_stats.py             # YouTube Data API v3 client (batch fetch, secret lookup)
│   │   └── save_yt_stats_snapshot.py           # Job entry point: fetch → write JSON to volume
│   ├── pipelines/
│   │   ├── 01_bronze_ingestion.py              # @dp.table: streaming Auto Loader + static CSV
│   │   ├── 02_silver_cleaning.py               # @dp.table + expectations + SCD2 CDC flow
│   │   └── 03_gold_aggregations.py             # @dp.table: author and video minute aggregations
│   └── transformations/
│       ├── __init__.py
│       ├── delta_per_hour_metrics.py            # compute_per_hour_deltas() — lag window function
│       ├── aggregate_author_stats_by_minute.py  # aggregate_author_stats_by_minute()
│       └── aggregate_video_stats_by_minute.py   # aggregate_video_stats_by_minute()
└── tests/
    └── test_tester.py                          # Placeholder; folder tracked by Git
```

---

## File Descriptions

### `databricks.yml` — Bundle Configuration

Defines the full project as a Declarative Automation Bundle.

| Section | Contents |
| --- | --- |
| `variables` | `catalog_name`, `schema_name`, `landing_zone_name`, `existing_cluster_id`, `secret_scope_val`, `secret_key_val` |
| `resources.pipelines.music_dlt_pipeline` | SDP pipeline (ADVANCED edition, CURRENT channel). Libraries: the three `src/pipelines/*.py` files. Cluster: `Standard_D4ds_v5`, 1 worker. Spark config injects all variables via `spark.conf`. |
| `resources.jobs.music_etl_orchestrator` | Three-task Lakeflow Job (see Execution Order below). |
| `targets.dev` | Default target. `catalog_name=dbr_dev`, `pipelines_development=true`, triggers paused. |
| `targets.prod` | `catalog_name=dbr_prod`, `pipelines_development=false`, triggers unpaused. |

Variables are passed to pipeline files via `spark.conf` keys (`music.catalog`,
`music.schema`, `music.volume`, `music.secret_scope`, `music.secret_key`,
`bundle.root`). Job tasks receive the same values as CLI `--parameter` flags.

---

### `src/setup/music_pipeline_setup.py` — Shared Configuration

Import-safe module used by both pipeline files and job tasks.

- `metadata_music_schema` — `StructType` for the CSV metadata file.
- `get_pipeline_paths(catalog, schema, volume)` — returns a dict of all Volume
  paths and fully-qualified table names for the given Unity Catalog entities.
- Safe argument parsing with `parse_known_args` — reads `--catalog`, `--schema`,
  `--volume` from CLI args when available, falls back to `spark.conf` when
  imported by SDP pipeline files (where no CLI args are present).
- Module-level globals (`json_landing_path`, `bronze_schema_path`,
  `music_metadata_tables`, `music_stats_tables`, …) are populated once at
  import time from the resolved values.
- `bootstrap_infrastructure()` — creates catalog, schema, volume, and landing
  directories. Called only when the file is the job task entry point
  (`__name__ == "__main__"`).

---

### `src/producer/fetch_yt_video_stats.py` — YouTube API Client

- `get_yt_api_key()` — reads the API key from Databricks Secrets using
  `--secret_scope` and `--secret_key` CLI args (or env vars as fallback).
- `find_video_ids(csv_file_path)` — reads the metadata CSV, extracts 11-char
  video IDs with `regexp_extract`, and returns a deduplicated list.
- `read_data_from_api(batch_size=50)` — splits video IDs into batches of up to
  50, calls the YouTube Data API v3 `videos` endpoint (`snippet,statistics`
  parts), and returns a flat list of enriched record dicts.

### `src/producer/save_yt_stats_snapshot.py` — Job Entry Point (Task 2)

- `_init_bundle_path()` — prepends the project root to `sys.path` before any
  `src.*` import. Handles both DABs `/files` deployment paths and plain
  workspace execution contexts.
- `main()` — calls `read_data_from_api()`, timestamps the result, and writes
  the payload as a JSON file to the landing volume path.

---

### `src/pipelines/01_bronze_ingestion.py` — Bronze Layer

Two `@dp.table` definitions registered with the SDP runtime.

- `bronze_youtube_stats` — streaming table. Reads multiline JSON snapshots from
  the landing volume via Auto Loader (`cloudFiles.format=json`). Schema is
  inferred on first run and stored at `bronze_schema_path`; schema evolution
  mode is `rescue` (unexpected fields go to `_rescued_data`).
- `bronze_music_metadata` — batch table. Reads `music_discography.csv` with
  the predefined `metadata_music_schema` (semicolon-delimited, with header).

The "bulletproof import" block at the top of every pipeline file reads
`bundle.root` from `spark.conf`, then walks `possible_roots` until a directory
containing `src/` is found and prepended to `sys.path`. This is the only
reliable way to resolve the `src` package inside the SDP runtime, where
`__file__` is unavailable and the working directory is not guaranteed.

---

### `src/pipelines/02_silver_cleaning.py` — Silver Layer

- `silver_youtube_stats` — reads `bronze_music_stats`, casts all columns to
  their target types, drops duplicate `(video_id, _ingested_at)` pairs, and
  calls `compute_per_hour_deltas`. Data quality expectations:
  - `@dp.expect_or_drop("valid_video_id")` — drops rows with null `video_id`.
  - `@dp.expect_or_fail("valid_view_count/like_count/comment_count")` — fails
    the pipeline if any count is negative (nulls are permitted).
- `silver_music_metadata_current` — reads `bronze_music_metadata`, extracts
  `video_id` from the `url` column via regex, adds `_ingested_at`, and drops
  duplicates on `video_id`.
- `silver_music_metadata_history` — SCD Type 2 history table. Created via
  `dp.create_streaming_table`, then wired with
  `dp.create_auto_cdc_from_snapshot_flow` (or `dp.apply_changes_from_snapshot`
  as a fallback) tracking changes in `url`, `author`, `title`, `album`, and
  `album_release_date` keyed on `video_id`.

---

### `src/pipelines/03_gold_aggregations.py` — Gold Layer

- `gold_author_stats_by_minute` — reads `silver_music_stats`, delegates to
  `aggregate_author_stats_by_minute`. Produces `author`,
  `_ingested_at_minutes`, `total_videos`, `total_views`, `total_likes`,
  `total_comments`.
- `gold_video_stats_by_minute` — reads `silver_music_stats`, delegates to
  `aggregate_video_stats_by_minute`. Produces `author`, `video_title`,
  `_ingested_at_minutes`, `total_views`, `total_likes`, `total_comments`.

---

### `src/transformations/delta_per_hour_metrics.py` — Per-Hour Delta Enrichment

`compute_per_hour_deltas(df)` — enriches the silver DataFrame with velocity
columns for views, likes, and comments.

1. Converts `_ingested_at` to fractional hours (`unix_timestamp / 3600`).
2. Partitions by `video_id`, orders by `_ingested_at_hours`.
3. Computes raw lag differences (`view_delta`, `like_delta`, `comment_delta`,
   `hour_delta`) using `compute_lag_differences`.
4. Divides each delta by `hour_delta` using `compute_safe_quotients` (null- and
   near-zero-safe, rounded to 1 decimal place).
5. Drops intermediate columns before returning.

### `src/transformations/aggregate_author_stats_by_minute.py`

`aggregate_author_stats_by_minute(silver_df)` — truncates `_ingested_at` to
minute precision, groups by `(author, _ingested_at_minutes)`, and aggregates
`total_videos` (countDistinct), `total_views`, `total_likes`, `total_comments`.

### `src/transformations/aggregate_video_stats_by_minute.py`

`aggregate_video_stats_by_minute(silver_df)` — same truncation strategy,
groups by `(author, video_title, _ingested_at_minutes)`, and sums
`total_views`, `total_likes`, `total_comments`.

---

## Runtime Import Model

The project uses a single `src/` package accessible by both job tasks and SDP
pipeline files. Path resolution is handled differently per execution context:

| Context | Mechanism |
| --- | --- |
| **SDP pipeline files** | `spark.conf.get("bundle.root")` → walk `possible_roots` until `src/` found → `sys.path.insert` |
| **Job tasks** | `_init_bundle_path()` anchors to the `/files` DABs deployment root, or falls back to a `src/` directory scan |
| **`music_pipeline_setup.py`** | `parse_known_args` + `SparkSession.conf` fallback — works in both contexts without raising an error |

This approach avoids the two common failure modes in Databricks: (1) SDP files
that assume `__file__` is available, and (2) job tasks that assume a wheel is
installed.

---

## Prerequisites

| Requirement | Value |
| --- | --- |
| Databricks Runtime | 17.3+ (required for `pyspark.pipelines` API) |
| Data security mode | USER_ISOLATION |
| Unity Catalog | Enabled |
| Secret scope | `pawelnowak2004pri219_scope` |
| Secret key | `pawelnowak-youtube-api` (YouTube Data API v3 key) |
| Pipeline cluster | `Standard_D4ds_v5`, 1 worker |
| Job cluster | Existing cluster `0702-132442-toro5spu` |
| Dev catalog | `dbr_dev` |
| Prod catalog | `dbr_prod` |

---

## Execution Order

The `music_etl_orchestrator` job runs three tasks in sequence:

1. **`setup_environment`** — runs `src/setup/music_pipeline_setup.py` as a
   `spark_python_task`. Creates the catalog, schema, volume, and landing
   directories if they do not exist.
2. **`fetch_youtube_data`** (depends on `setup_environment`) — runs
   `src/producer/save_yt_stats_snapshot.py`. Calls the YouTube Data API and
   writes a timestamped JSON snapshot to the landing volume.
3. **`run_dlt_pipeline`** (depends on `fetch_youtube_data`) — triggers the
   `music_dlt_pipeline` SDP pipeline, which runs the full bronze → silver →
   gold flow.

---

## Bundle Commands

```bash
# Validate bundle syntax and paths (always run before deploy)
databricks bundle validate --strict --target dev

# Deploy all resources to the dev workspace
databricks bundle deploy --target dev

# Run the full ETL job (end-to-end)
databricks bundle run music_etl_orchestrator --target dev

# Run only the SDP pipeline (skips setup and fetch tasks)
databricks bundle run music_dlt_pipeline --target dev

# Deploy and run against production
databricks bundle deploy --target prod
databricks bundle run music_etl_orchestrator --target prod
```

---

## Tables Created

| Layer | Table | Type | Description |
| --- | --- | --- | --- |
| Bronze | `bronze_music_stats` | Streaming | Raw YouTube stat snapshots from Auto Loader |
| Bronze | `bronze_music_metadata` | Batch | Music metadata dictionary from CSV |
| Silver | `silver_music_stats` | Materialized view | Cleaned, typed, deduplicated stats + per-hour deltas |
| Silver | `silver_music_metadata` | Materialized view | Current metadata snapshot with extracted video IDs |
| Silver | `silver_music_metadata_history` | Streaming (SCD2) | Full history of metadata attribute changes |
| Gold | `gold_music_stats_by_author` | Materialized view | Minute-level engagement totals per author |
| Gold | `gold_music_stats_by_video` | Materialized view | Minute-level engagement totals per video |
