# 02_bronze / pawel_project — Bronze → Silver Music Stats

Transforms raw YouTube engagement snapshots written to `dbr_dev.music_analytics.bronze_music_stats`
by `01_landing` into a clean, typed, deduplicated silver table enriched with per-hour engagement deltas.
This module is Pawel's data asset within the shared [gdelt-financial-anomalies](../../README.md) pipeline.

---

## Local Logic

The transformation runs as a single batch read-transform-write (no streaming):

1. Read all rows from `dbr_dev.music_analytics.bronze_music_stats` (all columns arrive as `StringType` from Auto Loader).
2. Select the 9 relevant columns and cast each to its target type:

   | Column | Target type |
   | --- | --- |
   | `_ingested_at` | `TimestampType` |
   | `view_count`, `like_count` | `LongType` |
   | `comment_count` | `IntegerType` |
   | `video_id`, `album`, `video_title`, `author`, `song_title` | `StringType` |

3. Drop duplicate rows sharing the same `(video_id, _ingested_at)` key — same video may appear in multiple snapshots at different times, but never twice in the same one.
4. Convert `_ingested_at` to float hours since epoch (`_ingested_at_hours`) for window ordering.
5. For each `video_id`, compute lag differences between consecutive snapshots: `view_delta`, `like_delta`, `comment_delta`, `hour_delta`.
6. Normalise each count delta by `hour_delta` → `view_delta_per_hour`, `like_delta_per_hour`, `comment_delta_per_hour` (null-safe, zero-division-safe, rounded to 1 d.p.).
7. Drop intermediate delta columns and select the final silver column set.
8. Append to `dbr_dev.music_analytics.silver_music_stats`.

---

## Directory Structure

```text
02_bronze/pawel_project/
├── bronze_to_silver_music_stats.py  # Pipeline entry point — orchestrates steps 1–8
├── preprocess_bronze_stats.py       # cast_and_deduplicate() — steps 2–3
└── delta_per_hour_metrics.py        # compute_per_hour_deltas() — steps 4–7
```

---

## Prerequisites

| Requirement | Detail |
| --- | --- |
| Setup script run | `00_setup/pawel_project/music_pipeline_setup.py` executed at least once |
| Bronze table populated | `01_landing/pawel_project/stream_yt_stats_to_bronze.py` completed at least one trigger |
| Unity Catalog | `dbr_dev.music_analytics` schema and `silver_music_stats` table must not exist with a conflicting empty schema — see note below |

> **Warning** If `silver_music_stats` was pre-created without columns (e.g. by an earlier run of
> `music_pipeline_setup.py`), Delta will reject the write with a schema mismatch. Drop the table
> before running: `spark.sql("DROP TABLE IF EXISTS dbr_dev.music_analytics.silver_music_stats")`.

---

## Running in Isolation

```bash
# From any notebook in the same repo, run the full transformation:
%run ../../02_bronze/pawel_project/bronze_to_silver_music_stats.py
```

Or execute the file directly from the Databricks file editor using the **Run** button.
No arguments are required — all paths are resolved from `music_pipeline_setup.py`.

---

## Output

Table: `dbr_dev.music_analytics.silver_music_stats`

| Column | Type | Description |
| --- | --- | --- |
| `author` | String | Artist name |
| `album` | String | Album name |
| `song_title` | String | Track title |
| `_ingested_at` | Timestamp | Snapshot ingestion time |
| `_ingested_at_hours` | Double | `_ingested_at` as float hours since epoch |
| `view_count` | Long | Total views at snapshot time |
| `like_count` | Long | Total likes at snapshot time |
| `comment_count` | Integer | Total comments at snapshot time |
| `view_delta_per_hour` | Double | View count change per hour since previous snapshot |
| `like_delta_per_hour` | Double | Like count change per hour since previous snapshot |
| `comment_delta_per_hour` | Double | Comment count change per hour since previous snapshot |
