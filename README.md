# Databricks Academy by SoftServe

This repository collects the lab work for the Databricks Academy by SoftServe. It is organized by assignment so each lab stays in its own folder with the notebooks, datasets, screenshots, and other files that belong to that part of the course.

## Repository layout

```text
README.md
LICENSE
.gitignore
.git/
.github/
agent_skills/
LAB 1 – Databricks Fundamentals & DEV Setup/
	Notebook.ipynb
	alabama_sold_real_estate_intelligence_2026.csv
	image_1783362421351.png
	Dashboard - week 1.lvdash.json
LAB 2 – Azure Services & Shared Lakehouse Setup/
	Stage 1/
		key_vault.png
		storage_account.png
		workspace.png
	Stage 2/
		Service Principal connection.ipynb
		bronze_ingestion.ipynb
		creating schemas.sql
		optional task.ipynb
		silver_ingestion.ipynb
LAB 3 — Streaming & Incremental Ingestion/
	CloudFiles_AutoLoader.ipynb
	EventHub_AutoLoader.ipynb
	metallica_event_hubs.ipynb
	optional_additions.ipynb
LAB 4 — Silver Layer, Data Quality & Schema Evolution/
	notebooks/
		Lab 4.ipynb
		workflow_for_job.ipynb
	src/
		__init__.py
		config.py
		data_generators.py
		data_cleaner.py
		data_mergers.py
		preprocessing.py
	images/
		cast_invalid_input_string_to_int.png
		column_mapping_mode_required.png
		delta_metadata_mismatch_extra_column.png
		integer_overflow.png
		schema_merge_incompatible_types.png
		schema_mismatch_renamed_column.png
		type_widening_narrowing.png
	setup_imports.py
	README.md
LAB 5 — Declarative Pipelines (Lakeflow)/
	databricks.yml
	README.md
	src/
		__init__.py
		setup/
			__init__.py
			music_pipeline_setup.py
		producer/
			__init__.py
			fetch_yt_video_stats.py
			save_yt_stats_snapshot.py
		pipelines/
			01_bronze_ingestion.py
			02_silver_cleaning.py
			03_gold_aggregations.py
		transformations/
			__init__.py
			delta_per_hour_metrics.py
			aggregate_author_stats_by_minute.py
			aggregate_video_stats_by_minute.py
	tests/
		test_tester.py
```

## What is in the repository

The repository is split into lab folders rather than one large notebook collection. That keeps the material for each topic together and makes it easier to follow the course week by week.

### LAB 1 – Databricks Fundamentals & DEV Setup

This lab covers the basics of the Databricks environment and the development setup. The folder currently contains:

- `Notebook.ipynb`, which holds the main lab work
- `alabama_sold_real_estate_intelligence_2026.csv`, the dataset used in the exercises
- `Dashboard - week 1.lvdash.json`, a dashboard export for the first week
- `image_1783362421351.png`, a supporting image file

### LAB 2 – Azure Services & Shared Lakehouse Setup

This lab is divided into stages.

#### Stage 1

Stage 1 contains screenshots that document the Azure setup steps:

- `key_vault.png`
- `storage_account.png`
- `workspace.png`

#### Stage 2

Stage 2 contains the notebooks for the lakehouse and ingestion tasks:

- `bronze_ingestion.ipynb`
- `creating schemas.dbquery.ipynb`
- `optional task.ipynb`
- `Service Principal connection.ipynb`
- `silver_ingestion.ipynb`

### LAB 3 — Streaming & Incremental Ingestion

This lab focuses on streaming and incremental ingestion patterns using Databricks AutoLoader, Event Hubs, and related streaming sources. The folder contains:

- `CloudFiles_AutoLoader.ipynb` — demonstrates file-based streaming ingestion with AutoLoader
- `EventHub_AutoLoader.ipynb` — shows ingesting events from Azure Event Hubs
- `metallica_event_hubs.ipynb` — an example notebook used in the Event Hubs exercises
- `optional_additions.ipynb` — additional streaming patterns and examples

### LAB 4 — Silver Layer, Data Quality & Schema Evolution ✅

This lab demonstrates building a production-grade Silver Layer pipeline with data quality enforcement, schema evolution, and Slowly Changing Dimensions (SCD) Type I and Type II patterns. The lab showcases modular architecture and clean code practices.

**Key Features:**
* Modular Python architecture with reusable modules organized in `src/` package
* Dynamic configuration supporting widget-based and environment-based parameters
* Comprehensive data quality transformations (negative value handling, deduplication, missing data)
* Delta Lake MERGE operations for both SCD Type I and Type II
* Synthetic data generation with intentional overlaps to simulate late-arriving data
* Complete documentation following clean code principles

**Directory Structure:**
* `notebooks/` — Interactive demonstration notebooks
  * `Lab 4.ipynb` — main interactive demonstration of SCD Type I and Type II workflows
  * `workflow_for_job.ipynb` — job orchestration workflow for production deployment
* `src/` — Python source modules (reusable package)
  * `__init__.py` — package initialization
  * `config.py` — dynamic configuration module with widget/environment parameter support
  * `data_generators.py` — synthetic streaming event data generation with controlled overlaps
  * `data_cleaner.py` — data quality transformations (validation, deduplication, SCD columns)
  * `data_mergers.py` — Delta Lake merge logic for SCD Type I and Type II patterns
  * `preprocessing.py` — end-to-end ETL orchestration pipeline
* `images/` — documentation screenshots showing schema evolution errors and solutions
* `setup_imports.py` — import path helper for module loading
* `README.md` — comprehensive documentation with usage examples and completion status

**Status:** ✅ Completed with code review, documentation, and clean code improvements.

### LAB 5 — Declarative Pipelines (Lakeflow) ✅

This lab builds an end-to-end music analytics pipeline using Databricks Lakeflow Spark Declarative Pipelines and Declarative Automation Bundles (DABs). The pipeline ingests YouTube video engagement snapshots via the YouTube Data API v3, processes them through a bronze–silver–gold medallion architecture, and exposes minute-level aggregate tables for trend analysis.

**Key Features:**
* Full Declarative Automation Bundle setup with `databricks.yml`: two targets (`dev` and `prod`), six configurable variables, and a three-task Lakeflow Job connecting infrastructure setup, API fetch, and pipeline execution in sequence
* Lakeflow Spark Declarative Pipeline using the `pyspark.pipelines` API (`@dp.table`, `@dp.expect_or_drop`, `@dp.expect_or_fail`, `dp.create_streaming_table`, `dp.apply_changes_from_snapshot`)
* Auto Loader streaming ingestion from a Unity Catalog Volume landing zone with schema evolution in rescue mode
* SCD Type 2 history tracking for music metadata via `apply_changes_from_snapshot`
* Per-hour engagement velocity metrics computed using a lag window function partitioned by `video_id`
* Bulletproof `sys.path` resolution for `src/` package imports inside the SDP runtime (where `__file__` is unavailable)
* YouTube Data API v3 client with Databricks Secrets integration and configurable batch fetching
* Shared configuration module (`music_pipeline_setup.py`) that works in both job and SDP contexts via `parse_known_args` + `spark.conf` fallback

**Directory Structure:**
* `databricks.yml` — bundle definition: pipeline, job, variables, `dev`/`prod` targets
* `src/setup/` — Unity Catalog bootstrap and shared path/table-name configuration
* `src/producer/` — YouTube Data API client and job entry point for snapshot writes
* `src/pipelines/` — three SDP pipeline files (bronze, silver, gold)
* `src/transformations/` — pure PySpark transformation functions (per-hour deltas, minute aggregations)
* `tests/` — placeholder for pytest tests
* `README.md` — full architecture diagram, per-file descriptions, prerequisites, and bundle commands

**Tables Created (in `dbr_dev.music_analytics` / `dbr_prod.music_analytics`):**
* `bronze_music_stats`, `bronze_music_metadata`
* `silver_music_stats`, `silver_music_metadata`, `silver_music_metadata_history`
* `gold_music_stats_by_author`, `gold_music_stats_by_video`

**Status:** ✅ Completed with full documentation, inline docstrings, and clean code.

## Notes

The repository structure is expected to grow as more labs are added. New assignments should follow the same pattern: create a folder for the lab, keep related notebooks and assets together, and avoid mixing files between weeks.

## License

This repository is shared under the terms of the included license.
