# lab_6_gold_layer_and_business_analytics

This lab documents the gold layer and business analytics assets built on top of the music medallion pipeline. It keeps the bronze and silver ingestion flow, adds business-facing fact and dimension tables, and connects them to dashboard, alerting, and security assets used in Databricks SQL.

---

## Architecture

```text
YouTube Data API v3                     music_discography*.csv
        │                                        │
        ▼                                        ▼
save_yt_stats_snapshot.py              bronze_music_metadata
        │
        ▼
bronze_music_stats
        │
        ▼
silver_music_stats ───────────────► fact_music_stats
silver_music_metadata ────────────► dim_music_metadata
silver_music_metadata_history
                                        │
                                        ├──► gold_author_music_stats
                                        ├──► gold_album_music_stats
                                        ├──► Music Artist Pulse dashboard
                                        ├──► volume_drop_alert
                                        └──► BI/RLS and BI/CLS SQL views
```

---

## Project Structure

```text
lab_6_gold_layer_and_business_analytics/
├── databricks.yml                              # Bundle definition for the Lakeflow pipeline and orchestration job
├── README.md                                   # Lab-level documentation
├── Daily Standups.txt                          # Sprint notes captured during the assignment
├── BI/
│   ├── Music Artist Pulse.lvdash.json          # Dashboard draft stored in the workspace folder
│   ├── volume_drop_alert.dbalert.json          # SQL alert query for latest-batch volume checks
│   ├── CLS/
│   │   └── cls_config.sql                      # Column masking view over dim_music_metadata
│   └── RLS/
│       ├── priviliges_granting.sql             # Row filter function, secured view, and grants
│       └── RLS_test.sql                        # Small validation query for the row filter
├── screenshots/
│   └── full_run_pipeline_graph.png             # Pipeline graph screenshot after a full run
├── src/
│   ├── __init__.py
│   ├── archive/
│   │   └── aggregate_author_stats_by_minute.py # Older helper retained for reference
│   ├── notebooks/
│   │   ├── New Notebook 2026-08-21 21_47_59    # Empty exploratory notebook placeholder
│   │   └── scratchpads/
│   │       └── data_volumes_test.ipynb         # Ad hoc table and volume validation notebook
│   ├── pipelines/
│   │   ├── 01_bronze_ingestion.py              # Bronze ingestion from JSON snapshots and metadata CSV
│   │   ├── 02_silver_cleaning.py               # Data quality rules, typing, and SCD-ready silver logic
│   │   ├── 03_fact_video_metrics.py            # Video-grain fact table for downstream analytics
│   │   ├── 03_gold_metadata.py                 # Gold dimension table used by BI assets
│   │   └── 04_gold_aggregations.py             # Album- and artist-level gold rollups
│   ├── producer/
│   │   ├── __init__.py
│   │   ├── fetch_yt_video_stats.py             # YouTube Data API reader
│   │   └── save_yt_stats_snapshot.py           # Snapshot writer for the landing volume
│   ├── setup/
│   │   ├── __init__.py
│   │   └── music_pipeline_setup.py             # Shared paths, table names, and bootstrap logic
│   └── transformations/
│       ├── __init__.py
│       ├── aggregate_stats.py                  # Shared gold aggregation helper for author and album views
│       ├── aggregate_video_stats.py            # Column standardization for fact_music_stats
│       └── delta_per_hour_metrics.py           # Velocity metrics derived from snapshot deltas
└── tests/
    └── test_tester.py                          # Placeholder test module
```

---

## File Descriptions

### `databricks.yml` — Bundle Configuration

Defines the full project as a Declarative Automation Bundle.

| Section | Contents |
| --- | --- |
| `variables` | `catalog_name`, `schema_name`, `landing_zone_name`, `existing_cluster_id`, `secret_scope_val`, `secret_key_val` |
| `resources.pipelines.music_dlt_pipeline` | Lakeflow Spark Declarative Pipeline (ADVANCED edition, CURRENT channel). Libraries: `01_bronze_ingestion.py`, `02_silver_cleaning.py`, `03_fact_video_metrics.py`, `03_gold_metadata.py`, and `04_gold_aggregations.py`. Cluster: `Standard_D4ds_v5`, 1 worker. Spark config injects all variables via `spark.conf`. |
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

### `src/pipelines/03_fact_video_metrics.py` — Fact Layer

- Builds `fact_music_stats` from `silver_music_stats`.
- Keeps one record per `video_id` and `_ingested_at` snapshot.
- Standardises metric names to `total_views`, `total_likes`, and
  `total_comments` so downstream gold aggregations share one schema.

### `src/pipelines/03_gold_metadata.py` — Dimension Layer

- Builds `dim_music_metadata` from the current silver metadata snapshot.
- Keeps descriptive columns such as `author`, `title`, `album`, and
  `album_release_date`.
- Drops the raw `url` column before exposing the table to BI assets.

### `src/pipelines/04_gold_aggregations.py` — Business Rollups

- Builds `gold_author_music_stats` by joining `fact_music_stats` with
  `dim_music_metadata` and aggregating by `author`.
- Builds `gold_album_music_stats` with the same pattern at the `album` grain.
- Reuses the shared `aggregate_stats()` helper so both outputs expose the same
  statistical columns.

---

### `src/transformations/delta_per_hour_metrics.py` — Per-Hour Delta Enrichment

`compute_per_hour_deltas(df)` enriches the silver stats stream with velocity
columns for views, likes, and comments.

1. Converts `_ingested_at` to fractional hours (`unix_timestamp / 3600`).
2. Partitions by `video_id`, orders by `_ingested_at_hours`.
3. Computes lag-based differences for metrics and time.
4. Divides each metric delta by `hour_delta` with near-zero-safe logic.
5. Drops helper columns before returning the enriched DataFrame.

### `src/transformations/aggregate_stats.py`

`aggregate_stats(silver_df, by="author")` groups fact-style metrics by a chosen
business dimension and returns totals, min/max values, means, standard
deviations, and coefficient-of-variation percentages for views, likes, and
comments.

### `src/transformations/aggregate_video_stats.py`

`aggregate_video_stats(silver_df)` prepares the video-level fact output. It does
not bucket timestamps because collapsing multiple snapshots from the same minute
would overstate the latest metric values.

---

## BI assets

| Asset | Purpose |
| --- | --- |
| `fact_music_stats` | Video-grain fact table used as the main reporting source for snapshots of views, likes, and comments. |
| `dim_music_metadata` | Gold dimension table with the descriptive metadata needed to label fact rows in BI queries. |
| `gold_author_music_stats` | Artist-level rollup with totals and descriptive statistics across each ingestion snapshot. |
| `gold_album_music_stats` | Album-level rollup with the same statistical profile used for artist analytics. |
| `Music Artist Pulse` | Lakeview dashboard that joins `fact_music_stats` with `dim_music_metadata` to filter by artist, album, and time. |
| `volume_drop_alert` | SQL alert that compares the row count in the latest `fact_music_stats` batch with the metadata row count in `dim_music_metadata`. |
| `BI/RLS/priviliges_granting.sql` | Defines the `author_row_filter` function, builds the `rls_dim_music_metadata` view, and grants access to artist-specific groups. |
| `BI/RLS/RLS_test.sql` | Simple validation query for checking the row filter behaviour. |
| `BI/CLS/cls_config.sql` | Defines `mask_video_id()` and builds `cls_dim_music_metadata`, which masks `video_id` for non-admin users. |
| `screenshots/full_run_pipeline_graph.png` | Screenshot of the full pipeline graph after a successful run. |
| `src/notebooks/scratchpads/data_volumes_test.ipynb` | Small scratch notebook used to inspect table access during development. |
| `src/notebooks/New Notebook 2026-08-21 21_47_59` | Empty notebook placeholder currently stored with the lab assets. |

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
   `music_dlt_pipeline` Lakeflow Spark Declarative Pipeline, which runs the full
   bronze → silver → fact/dimension → gold flow.

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

## Core Tables Created

| Layer | Table | Type | Description |
| --- | --- | --- | --- |
| Bronze | `bronze_music_stats` | Streaming | Raw YouTube stat snapshots loaded from the landing volume with Auto Loader. |
| Bronze | `bronze_music_metadata` | Batch | Music metadata dictionary loaded from `music_discography*.csv`. |
| Silver | `silver_music_stats` | Materialized view | Cleaned, typed, deduplicated stats enriched with per-hour deltas. |
| Silver | `silver_music_metadata` | Materialized view | Current metadata snapshot with extracted `video_id` values. |
| Silver | `silver_music_metadata_history` | Streaming (SCD2) | Historical metadata changes tracked with snapshot-based CDC. |
| Gold | `fact_music_stats` | Materialized view | Video-level fact table used as the primary BI snapshot source. |
| Gold | `dim_music_metadata` | Materialized view | Video metadata dimension used to label fact rows and secured views. |
| Gold | `gold_author_music_stats` | Materialized view | Artist-level rollup with totals and descriptive statistics per snapshot. |
| Gold | `gold_album_music_stats` | Materialized view | Album-level rollup with totals and descriptive statistics per snapshot. |
