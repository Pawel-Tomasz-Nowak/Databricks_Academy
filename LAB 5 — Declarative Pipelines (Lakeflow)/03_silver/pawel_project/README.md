# 03_silver/pawel_project — Silver → Gold Aggregation (Music Stats)

Transforms the cleaned silver music stats table into two gold Delta tables by
applying parallel minute-level aggregations: one grouped per video, one grouped
per author. This sub-module sits downstream of `02_bronze/pawel_project` and
upstream of any gold-layer analysis or reporting.

Table paths are resolved at runtime from `00_setup/pawel_project/music_pipeline_setup.py`.

---

## Local Logic

1. Read `silver_music_stats` Delta table via `music_stats_tables["silver"]`.
2. **Per-video aggregation** — truncate `_ingested_at` to minute, group by
   `author + song_title + _ingested_at_minutes`, compute:
   `total_views`, `total_likes`, `total_comments`.
   Result written to `{gold_path}_by_video`.
3. **Per-author aggregation** — truncate `_ingested_at` to minute, group by
   `author + _ingested_at_minutes`, compute a full statistical profile across
   the author's catalogue:
   `total_videos`, `total/max/min/mean_{views,likes,comments}`,
   `cv_{views,likes,comments}_pct` (coefficient of variation %).
   Result written to `{gold_path}_by_author`.
4. Both results are appended to their respective gold Delta tables.

---

## Directory Structure

```text
03_silver/pawel_project/
├── run_music_silver_to_gold.py           # Pipeline entry point — orchestrates steps 1–4
├── aggregate_video_stats_by_minute.py    # aggregate_video_stats_by_minute() — step 2
├── aggregate_author_stats_by_minute.py   # aggregate_author_stats_by_minute() — step 3
├── silver_to_gold_music_stats.ipynb      # Interactive notebook version of the same pipeline
└── README.md
```

---

## Prerequisites

| Requirement | Detail |
| --- | --- |
| Upstream table | `silver_music_stats` must be populated (run `02_bronze/pawel_project` first) |
| Setup | `00_setup/pawel_project/music_pipeline_setup.py` must have been executed once to register table paths |
| Compute | Databricks Runtime 17.3+, Unity Catalog enabled |

---

## Running in Isolation

**Script (non-interactive):**
```bash
%run 03_silver/pawel_project/run_music_silver_to_gold.py
```

**Notebook (interactive):**
Open `silver_to_gold_music_stats.ipynb` and run all cells.

---

## Output Tables

| Table | Key columns | Granularity |
| --- | --- | --- |
| `{gold_path}_by_video` | `author`, `song_title`, `_ingested_at_minutes`, `total_views/likes/comments` | Per video per minute |
| `{gold_path}_by_author` | `author`, `_ingested_at_minutes`, `total_videos`, `total/max/min/mean/cv_*` | Per author per minute |
