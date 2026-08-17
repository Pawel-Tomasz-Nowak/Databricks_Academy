# Music Popularity Spike Detection

This module implements a Databricks Lakeflow + Asset Bundles pipeline that ingests YouTube activity signals, normalises them in bronze and silver layers, and exposes gold-level aggregate views for trend analysis. The project is designed around the actual Databricks execution model: Lakeflow files are intentionally self-contained, while job tasks use the packaged wheel under `src/etl_package`.

## Why this structure exists

The most important design constraint in this project is runtime separation.

- DLT files are executed by Databricks through `exec()`. In that context, `__file__` is not available and repository-relative imports are not reliable.
- Job tasks run inside the installed wheel package, where package imports such as `from etl_package.setup...` are valid.
- The result is a deliberate split between:
  - self-contained DLT pipeline entrypoints in `src/pipelines`
  - wheel-based bootstrap and producer code in `src/etl_package` and `src/producer`

This separation avoids the classic import failures caused by mixing DLT runtime assumptions with job-runtime package assumptions.

## Architecture

```text
YouTube API
    │
    ▼
Landing Volume (/Volumes/.../yt_snapshots)
    │
    ├── bronze_youtube_stats
    │       └── Auto Loader + schema evolution
    │
    ├── bronze_music_metadata
    │       └── static metadata CSV
    │
    ▼
silver_youtube_stats
    ├── cast and validate fields
    ├── drop duplicates
    └── compute per-hour deltas
    │
    ├── silver_music_metadata_current
    │       └── current snapshot by video_id
    │
    └── silver_music_metadata_history
            └── SCD Type 2 history table
    │
    ▼
Gold aggregates
    ├── author aggregation by minute
    └── video aggregation by minute
```

## Active project layout

```text
LAB 5 — Declarative Pipelines (Lakeflow)/
├── README.md                              # This file: project-level guide for the Lakeflow module
├── databricks.yml                         # Asset bundle definition and job/pipeline orchestration
├── requirements.txt                       # Python dependencies for bundle packaging
├── setup.py                               # Packaging entrypoint for the wheel
├── archive/                               # Legacy scripts kept for reference; not part of the active runtime path
│   ├── bronze_to_silver_music_stats.py
│   ├── preprocess_bronze_stats.py
│   ├── run_music_silver_to_gold.py
│   ├── stream_yt_stats_to_bronze.py
│   └── video_id_extractor.py
├── notebooks/                             # Lab notebooks and exploratory code
├── resources/                             # Bundle resource definitions and YAML fragments
├── src/
│   ├── config/
│   │   └── pipeline_config.py             # Compatibility configuration module; not required by active DLT files
│   ├── etl_package/
│   │   ├── producer/
│   │   │   ├── fetch_yt_video_stats.py
│   │   │   └── save_yt_stats_snapshot.py
│   │   ├── setup/
│   │   │   └── music_pipeline_setup.py
│   │   ├── pipelines/
│   │   │   ├── 01_bronze_ingestion.py
│   │   │   ├── 02_silver_cleaning.py
│   │   │   └── 03_gold_aggregations.py
│   │   └── transformations/
│   │       ├── aggregate_author_stats_by_minute.py
│   │       ├── aggregate_video_stats_by_minute.py
│   │       └── delta_per_hour_metrics.py
│   ├── pipelines/
│   │   ├── 01_bronze_ingestion.py         # Active DLT bronze layer entrypoint
│   │   ├── 02_silver_cleaning.py          # Active DLT silver layer entrypoint
│   │   └── 03_gold_aggregations.py        # Active DLT gold layer entrypoint
│   ├── producer/
│   │   ├── fetch_yt_video_stats.py        # Job-side data fetch helper
│   │   └── save_yt_stats_snapshot.py      # Persists the fetched snapshot into the landing volume
│   ├── setup/
│   │   └── run_setup.py                   # Job bootstrap entrypoint for UC setup
│   ├── transformations/
│   │   ├── aggregate_author_stats_by_minute.py
│   │   ├── aggregate_video_stats_by_minute.py
│   │   ├── delta_per_hour_metrics.py
│   │   └── __init__.py
│   └── __init__.py
├── tests/
│   └── test_tester.py
└── .github/skills/                        # internal documentation guidance for this repo
```

## Runtime model

This project intentionally uses two different Python execution contexts.

### DLT pipeline files

Files in `src/pipelines` must be self-contained. They read values from Spark configuration via `spark.conf.get(...)` instead of importing repo-local modules. This is required because DLT executes the file in a stripped-down runtime where the standard module import assumptions do not hold.

### Job and wheel tasks

The job path relies on `etl_package` and the installed wheel. The bootstrap and producer tasks resolve configuration and create Unity Catalog objects using the actual package path. This is where `from etl_package...` imports are valid and expected.

## Prerequisites

- Databricks Runtime: 14.3.x or newer for the cluster used in the bundle
- Unity Catalog enabled in the target workspace
- A valid Databricks secret or environment variable for the YouTube API key
- A target catalog and schema already available or creatable by the bootstrap task
- `music_project.catalog_name`, `music_project.schema_name`, and `music_project.volume_name` configured in `databricks.yml`

## Local bundle validation

Run the following from the project root:

```bash
databricks bundle validate
```

This checks that the bundle definition and all referenced paths are syntactically valid before deployment.

## Deployment

```bash
databricks bundle deploy
```

The deploy step packages the wheel and publishes the bundle resources to the configured workspace.

## Execution order

The job orchestration in `databricks.yml` follows this sequence:

1. `setup_infrastructure` creates or validates the catalog, schema, and volume.
2. `fetch_youtube_data` writes a snapshot of current YouTube statistics into the landing volume.
3. `run_dlt_pipeline` starts the Lakeflow pipeline that reads bronze, cleans silver, and builds gold aggregates.

## Operational note

The project intentionally avoids `__file__`-based path resolution and `sys.path` patches. That pattern is not portable in Databricks DLT and is one of the most common causes of pipeline import failures. The current structure keeps the runtime contract explicit and predictable.

## Key implementation files

- `src/pipelines/01_bronze_ingestion.py` — bronze ingestion from JSON snapshots and metadata CSV
- `src/pipelines/02_silver_cleaning.py` — validation, deduplication, and per-hour delta enrichment
- `src/pipelines/03_gold_aggregations.py` — author and song aggregation by minute
- `src/etl_package/setup/music_pipeline_setup.py` — Unity Catalog bootstrap and shared configuration
- `src/etl_package/producer/fetch_yt_video_stats.py` — YouTube API fetch logic for job task execution
- `src/producer/save_yt_stats_snapshot.py` — writes the API payload to the landing volume
- `databricks.yml` — bundle definition, job orchestration, and DLT resource configuration

## Testing and validation

The project is validated primarily through:

- Databricks bundle validation
- runtime checks inside the Databricks job cluster
- DLT ownership of the bronze/silver/gold layer flow
- spark configuration-based path resolution instead of repo-local imports

This is the least fragile pattern for a Databricks-managed pipeline because it matches the platform execution model instead of fighting it.
