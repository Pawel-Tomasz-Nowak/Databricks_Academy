# LAB 7 — Data Quality Testing and Unit Tests

Builds on the medallion pipeline introduced in lab 6 and adds three layers of
quality assurance: `@dp.expect_*` rules on every pipeline stage, a quarantine
table for rejected metadata rows, a post-pipeline reconciliation task that
detects silent data loss, and a pytest suite that validates each transformation
in isolation against a shared Databricks Connect session.

---

## Architecture

```text
YouTube Data API v3
        │
        ▼  (Task 1: setup_environment)
src/setup/music_pipeline_setup.py
        │  creates catalog, schema, volume, and landing directories
        ▼  (Task 2: fetch_youtube_data)
src/producer/save_yt_stats_snapshot.py
        │  writes timestamped JSON files
        ▼
/Volumes/{catalog}/{schema}/landing_zone/yt_snapshots/
        │
        │  Auto Loader (cloudFiles/json, schema evolution)
        ▼
bronze_music_stats          ◄── music_discography.csv
        │                             (static CSV, semicolon-delimited)
        │  cast · deduplicate · @dp.expect_all_or_fail
        │  per-hour delta enrichment (window lag over video_id)
        ▼
silver_music_stats           (MV, @dp.expect_all_or_fail for 7 rules)
silver_music_metadata        ◄── current snapshot, @dp.expect_all_or_drop
silver_music_quarantine      ←── rejected metadata rows + failure reasons
silver_music_metadata_history     (SCD Type 2 via dp.create_auto_cdc_flow)
        │
        │  join with dim + groupBy aggregations
        ▼
fact_music_stats             (@dp.expect_all_or_fail for 5 rules)
dim_music_metadata           (gold dimension, url excluded, clustered by author/album)
gold_author_music_stats      (@dp.expect_or_fail: author not null)
gold_album_music_stats       (@dp.expect_or_fail: album not null)
        │
        ▼  (Task 4: reconciliation_gate)
src/quality/reconciliation_gate.py
        │  bronze count == clean + quarantine; fact views == author views == album views
```

---

## Project Structure

```text
lab_7_data_quality_testing_and_unit_tests/
├── databricks.yml                              # DABs bundle: variables, pipeline, 4-task job, dev/prod targets
├── README.md                                   # This file
├── src/
│   ├── __init__.py
│   ├── setup/
│   │   └── music_pipeline_setup.py             # Shared config, path resolver, UC bootstrap
│   ├── producer/
│   │   ├── fetch_yt_video_stats.py             # YouTube Data API v3 client (batch fetch, secret lookup)
│   │   └── save_yt_stats_snapshot.py           # Task 2 entry point: fetch → write JSON to volume
│   ├── pipelines/
│   │   ├── 01_bronze_ingestion.py              # Streaming Auto Loader + static CSV
│   │   ├── 02_silver_cleaning.py               # Typed stats MV + metadata MV + quarantine + SCD2 history
│   │   ├── 03_fact_video_metrics.py            # fact_music_stats MV (@dp.expect_all_or_fail)
│   │   ├── 03_gold_metadata.py                 # dim_music_metadata MV (url excluded, clustered by author/album)
│   │   └── 04_gold_aggregations.py             # gold_author_music_stats + gold_album_music_stats MVs
│   ├── quality/
│   │   ├── reconciliation_gate.py              # Post-pipeline job task: metadata and views reconciliation
│   │   ├── no_delta_constraints_justification.md  # Design note explaining why Delta constraints were not used
│   │   └── scorecards.ipynb                    # Exploratory notebook for DQ scorecard inspection
│   └── transformations/
│       ├── __init__.py
│       ├── delta_per_hour_metrics.py            # compute_per_hour_deltas() — lag window function
│       ├── aggregate_stats.py                  # aggregate_stats(df, by) — author and album rollup
│       └── aggregate_video_stats.py            # aggregate_video_stats(df) — video-level fact projection
├── tests/
│   ├── conftest.py                             # pytest session fixture: Databricks Connect SparkSession
│   ├── path_resolver.py                        # _init_bundle_path() — shared sys.path helper for tests
│   ├── test_aggregate_stats.py                 # Unit tests for aggregate_stats()
│   ├── test_aggregate_video_stats.py           # Unit tests for aggregate_video_stats()
│   ├── test_delta_per_hour_metrics.py          # Unit tests for compute_per_hour_deltas() edge cases
│   └── test_reconciliation.py                 # Integration tests: bronze→silver and gold views consistency
├── BI/
│   ├── Music Artist Pulse.lvdash.json          # Artist-focused engagement explorer dashboard
│   ├── Music Data Quality & Medallion Scorecard.lvdash.json  # DQ KPIs and quarantine inspection dashboard
│   ├── volume_drop_alert.dbalert.json          # Alert: fires when a batch has fewer records than expected
│   ├── RLS/
│   │   ├── priviliges_granting.sql             # author_row_filter function + view + GRANT statements
│   │   └── RLS_test.sql                        # Manual smoke test for the row filter function
│   └── CLS/
│       └── cls_config.sql                      # mask_video_id function + CLS view combining RLS and CLS
└── screenshots/
    └── full_run_pipeline_graph.png             # Screenshot of a completed SDP pipeline run
```

---

## File Descriptions

### `databricks.yml` — Bundle Configuration

| Section | Contents |
| --- | --- |
| `variables` | `catalog_name`, `schema_name`, `landing_zone_name`, `existing_cluster_id`, `secret_scope_val`, `secret_key_val` |
| `resources.pipelines.music_dlt_pipeline` | SDP pipeline (ADVANCED, CURRENT channel). Libraries: all five `src/pipelines/*.py` files. Cluster: `Standard_D4ds_v5`, 1 worker. Spark conf injects variables. |
| `resources.jobs.music_etl_orchestrator` | Four-task Lakeflow Job (see Execution Order). Task 4 sends an email on failure. |
| `targets.dev` | Default target. `catalog_name=dbr_dev`, `pipelines_development=true`, triggers paused. |
| `targets.prod` | `catalog_name=dbr_prod`, `pipelines_development=false`, triggers unpaused. |

---

### `src/setup/music_pipeline_setup.py` — Shared Configuration

Import-safe module used by both pipeline files and job tasks.

- `metadata_music_schema` — `StructType` for the CSV metadata file.
- `get_pipeline_paths(catalog, schema, volume)` — returns a dict of all volume
  paths and fully-qualified table names.
- Safe argument parsing with `parse_known_args` plus `spark.conf` fallback so
  the module works in both CLI (job) and pipeline import contexts.
- `bootstrap_infrastructure()` — creates catalog, schema, volume, and landing
  directories. Called only as `__main__` (Task 1).

---

### `src/pipelines/01_bronze_ingestion.py` — Bronze Layer

- `bronze_music_stats` — streaming table. Reads multiline JSON snapshots from
  the landing volume via Auto Loader. Schema stored in the volume; evolution
  mode is `rescue`.
- `bronze_music_metadata` — batch table. Reads `music_discography*.csv` files
  with the predefined `metadata_music_schema` (semicolon-delimited).

---

### `src/pipelines/02_silver_cleaning.py` — Silver Layer

- `silver_music_stats` (MV) — casts bronze stats columns, drops duplicates on
  `(video_id, _ingested_at)`, computes per-hour deltas.
  `@dp.expect_all_or_fail` enforces seven rules: non-null author, video ID,
  video title, positive-or-null counts, and a valid `published_at` date.
- `silver_music_metadata` (MV) — latest-row snapshot per `video_id` extracted
  from the bronze CSV feed. `@dp.expect_all_or_drop` silently drops rows that
  fail the six metadata rules.
- `silver_music_quarantine` (streaming table) — rows rejected from
  `silver_music_metadata`, enriched with a `_quarantine_reason` column that
  lists each violated rule name.
- `silver_music_metadata_cdc_source` (temporary view) — streams from bronze
  metadata to feed the SCD Type 2 flow.
- `silver_music_metadata_history` (streaming table) — SCD Type 2 history keyed
  on `video_id`, ordered by `(_ingested_at, _source_file_path)` to handle
  batches with the same timestamp.

---

### `src/pipelines/03_fact_video_metrics.py` — Fact Table

- `fact_music_stats` (MV) — video-level snapshot of `total_views`,
  `total_likes`, and `total_comments` per `video_id` and `_ingested_at`.
  `@dp.expect_all_or_fail` enforces five rules (non-null `video_id`,
  valid `_ingested_at`, non-negative metric values).

---

### `src/pipelines/03_gold_metadata.py` — Gold Dimension

- `dim_music_metadata` (MV) — reads `silver_music_metadata` and drops the
  raw `url` column. Clustered by `author` and `album`.

---

### `src/pipelines/04_gold_aggregations.py` — Gold Aggregates

- `gold_author_music_stats` (MV) — joins `fact_music_stats` with
  `dim_music_metadata` on `video_id` and aggregates by `author` via
  `aggregate_stats`. `@dp.expect_or_fail` ensures `author` is not null.
- `gold_album_music_stats` (MV) — same join pattern aggregated by `album`.
  `@dp.expect_or_fail` ensures `album` is not null.

---

### `src/quality/reconciliation_gate.py` — Post-Pipeline Quality Gate

Job Task 4. Runs after the SDP pipeline completes and fails the job (and sends
an email) when either check fails.

- `run_reconciliation_metadata()` — verifies that distinct bronze URL count
  equals `silver_music_metadata` count plus `silver_music_quarantine` count.
  Exits with a `SILENT DATA LOSS DETECTED` message on mismatch.
- `run_reconciliation_gold_aggregates()` — verifies that
  `SUM(total_views)` is identical across `fact_music_stats`,
  `gold_author_music_stats`, and `gold_album_music_stats`.

---

### `src/quality/no_delta_constraints_justification.md`

Design note explaining the trade-offs considered when choosing
`@dp.expect_*` decorators over Delta table constraints (`ALTER TABLE ... ADD CONSTRAINT`).

---

### `src/transformations/delta_per_hour_metrics.py`

`compute_per_hour_deltas(df)` — enriches the silver stats DataFrame with
hourly rate-of-change columns for views, likes, and comments using a LAG
window partitioned by `video_id`.

### `src/transformations/aggregate_stats.py`

`aggregate_stats(silver_df, by)` — groups a joined fact-and-dimension DataFrame
by the `by` column and `_ingested_at`, computing totals, min, max, mean,
standard deviation, and coefficient-of-variation for views, likes, and comments.

### `src/transformations/aggregate_video_stats.py`

`aggregate_video_stats(silver_df)` — renames raw metric columns to the
`total_*` convention and selects the video-grain columns needed by the fact table.
The full `_ingested_at` timestamp is preserved to avoid over-aggregation when
multiple snapshots arrive within the same minute.

---

### `tests/` — Unit and Integration Tests

All tests require a Databricks Connect session pointing at an all-purpose cluster.

| File | What it tests |
| --- | --- |
| `conftest.py` | Session-scoped `spark` fixture backed by `DatabricksSession` |
| `path_resolver.py` | `_init_bundle_path()` — locates the project root and adds it to `sys.path` before any `src.*` import |
| `test_aggregate_stats.py` | `aggregate_stats()` by author and by album, including single-video null stddev and CV |
| `test_aggregate_video_stats.py` | Row count preservation and exact timestamp retention by `aggregate_video_stats()` |
| `test_delta_per_hour_metrics.py` | Five edge cases for `compute_per_hour_deltas()`: first snapshot, normal growth, zero time delta, null input metric, partition isolation |
| `test_reconciliation.py` | Integration checks against live `dbr_dev` tables: bronze→silver metadata count and gold view sum equality |

Run the suite with:

```bash
cd tests
pytest -v
```

---

### `BI/` — Business Intelligence Assets

#### Dashboards

- **Music Artist Pulse** — artist engagement explorer. One dataset joining
  `fact_music_stats` and `dim_music_metadata`. Widgets: artist filter, date
  range filter, album counter, album view share pie, views-over-time line,
  album likes bar, and album snapshot table.
- **Music Data Quality & Medallion Scorecard** — data quality dashboard with
  three datasets: `quality_summary` (clean vs. quarantine counts, pass rate,
  pipeline status), `medallion_flow` (record counts per layer), and
  `quarantine_inspection` (rejected records ordered by ingestion time).

#### Alert

- **`volume_drop_alert`** — compares the number of records in the latest
  `fact_music_stats` batch against the expected count from `dim_music_metadata`.
  Returns `is_volume_dropped = 1` when the batch is smaller than expected.

#### Row-Level Security (`BI/RLS/`)

- `priviliges_granting.sql` — creates the `author_row_filter` UC function, an
  RLS view (`rls_dim_music_metadata`), and grants SELECT to `linkin_park_fans`
  and `metallica_fans` groups.
- `RLS_test.sql` — smoke test for manual verification of the filter function.

#### Column-Level Security (`BI/CLS/`)

- `cls_config.sql` — creates the `mask_video_id` function (admins see the real
  ID; others see `VID-***-<last 3 chars>`) and a combined RLS+CLS view
  (`cls_dim_music_metadata`).

---

## Runtime Import Model

| Context | Mechanism |
| --- | --- |
| **SDP pipeline files** | `spark.conf.get("bundle.root")` → walk `possible_roots` until `src/` found → `sys.path.insert` |
| **Job tasks** | `_init_bundle_path()` in `path_resolver.py` or `save_yt_stats_snapshot.py` anchors to the `/files` DABs deployment root |
| **`music_pipeline_setup.py`** | `parse_known_args` + `SparkSession.conf` fallback — works in both contexts without raising |
| **Tests** | `_init_bundle_path()` from `tests/path_resolver.py` resolves the same root before each test file imports `src.*` |

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

The `music_etl_orchestrator` job runs four tasks in sequence:

1. **`setup_environment`** — runs `src/setup/music_pipeline_setup.py`.
   Creates catalog, schema, volume, and landing directories.
2. **`fetch_youtube_data`** (depends on `setup_environment`) — runs
   `src/producer/save_yt_stats_snapshot.py`. Writes a JSON snapshot to the
   landing volume.
3. **`run_dlt_pipeline`** (depends on `fetch_youtube_data`) — triggers the SDP
   pipeline (bronze → silver → gold).
4. **`reconciliation_gate`** (depends on `run_dlt_pipeline`) — runs
   `src/quality/reconciliation_gate.py`. Fails the job and sends an email
   notification if data loss is detected.

---

## Bundle Commands

```bash
# Validate bundle syntax and paths
databricks bundle validate --strict --target dev

# Deploy all resources to dev
databricks bundle deploy --target dev

# Run the full ETL job end-to-end
databricks bundle run music_etl_orchestrator --target dev

# Run only the SDP pipeline (skips setup, fetch, and reconciliation)
databricks bundle run music_dlt_pipeline --target dev

# Deploy and run in production
databricks bundle deploy --target prod
databricks bundle run music_etl_orchestrator --target prod
```

---

## Tables Created

| Layer | Table | Type | Description |
| --- | --- | --- | --- |
| Bronze | `bronze_music_stats` | Streaming | Raw YouTube stat snapshots from Auto Loader |
| Bronze | `bronze_music_metadata` | Streaming | Music metadata records from CSV files |
| Silver | `silver_music_stats` | Materialized view | Typed, deduplicated stats with per-hour deltas. Fails on invalid rows. |
| Silver | `silver_music_metadata` | Materialized view | Latest clean metadata snapshot per video. Drops invalid rows. |
| Silver | `silver_music_quarantine` | Streaming table | Rejected metadata rows with `_quarantine_reason` column. |
| Silver | `silver_music_metadata_history` | Streaming (SCD2) | Full attribute-change history keyed on `video_id`. |
| Gold | `fact_music_stats` | Materialized view | Video-level snapshot of views, likes, and comments. Fails on invalid rows. |
| Gold | `dim_music_metadata` | Materialized view | Current video metadata without source URL, clustered by author/album. |
| Gold | `gold_author_music_stats` | Materialized view | Descriptive engagement statistics aggregated by author. |
| Gold | `gold_album_music_stats` | Materialized view | Descriptive engagement statistics aggregated by album. |
