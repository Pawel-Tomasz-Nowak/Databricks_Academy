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

## Notes

The repository structure is expected to grow as more labs are added. New assignments should follow the same pattern: create a folder for the lab, keep related notebooks and assets together, and avoid mixing files between weeks.

## License

This repository is shared under the terms of the included license.
