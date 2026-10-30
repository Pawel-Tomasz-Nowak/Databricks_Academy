# Databricks Academy

This repository contains the practical work for a Databricks Academy track: twelve progressively larger labs and a final project built around an Azure Databricks music-analytics lakehouse.

## Learning path

The labs move from basic Spark work to a governed, tested, and AI-enabled lakehouse:

| Lab | Focus | Main result |
| --- | --- | --- |
| 01 | Databricks fundamentals | Spark exploration of Alabama real-estate data, a bronze–silver–gold exercise, and a first dashboard |
| 02 | Azure and shared lakehouse setup | ADLS, Key Vault, secret-scope authentication, and parameterised bronze/silver schemas |
| 03 | Streaming ingestion | CloudFiles Auto Loader and Azure Event Hubs ingestion patterns |
| 04 | Silver layer | Data quality cleaning, schema evolution, and SCD Type I/II processing |
| 05 | Lakeflow declarative pipelines | DAB-managed music popularity pipeline and spike-oriented aggregations |
| 06 | Gold layer and analytics | Business-facing facts/dimensions, dashboards, alerts, and governance assets |
| 07 | Testing and quality | Expectations, quarantine, reconciliation gates, and Pytest coverage |
| 08 | CI/CD promotion | Dev/prod Asset Bundle targets and deployable pipeline resources |
| 09 | REST API automation | Programmatic job and compute control with the Databricks SDK |
| 10 | External operational databases | Lakehouse Federation with PostgreSQL and CDC patterns |
| 11 | Zerobus-style ingestion | Brokerless push ingestion with source idempotency and Delta reconciliation |
| 12 | AI capstone | RAG with Vector Search, inference audit logging, and governance tests |

Labs 04–07 and 09–11 already contain detailed local documentation; their README files are the authoritative guides for those modules.

## Shared lakehouse architecture

Most of the later labs build on the same music analytics flow:

```text
YouTube API / seed files / Event Hubs / PostgreSQL
                    │
                    ▼
        Bronze raw snapshots and landing tables
                    │
                    ▼
       Silver cleaned, typed, deduplicated data
                    │
                    ▼
 Gold facts, dimensions, hourly aggregates, and spikes
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
 Databricks SQL BI        RAG / governance / automation
 dashboards and alerts    Vector Search and API workflows
```

The first three labs use the Alabama real-estate dataset to teach the platform and ingestion basics. From lab 04 onward, the main example is music metadata and YouTube engagement data.

## Repository structure

```text
Databricks_Academy/
├── lab_01_databricks_fundamentals_and_dev_setup/       # Spark and first dashboard
├── lab_02_azure_services_and_shared_lakehouse_setup/   # Azure resources and schemas
├── lab_03_streaming_and_incremental_ingestion/         # Auto Loader and Event Hubs
├── lab_04_silver_layer_data_quality_and_schema_evolution/
├── lab_05_declarative_pipelines_lakeflow/
├── lab_06_gold_layer_and_business_analytics/
├── lab_07_data_quality_testing_and_unit_tests/
├── lab_08_ci_cd_dev_prod_promotion/                    # Bundle targets and tests
├── lab_09_databricks_rest_api_automation/               # SDK-based orchestration
├── lab_10_external_operational_databases_lakehouse_federation_cdc/
├── lab_11_zero_bus_streaming_with_zerobus/
├── lab_12_ai_capstone_rag_ag_assisted_development/
├── final_project/                                      # Consolidated end-to-end project
├── .github/                                            # Repository automation and project skills
├── .ai-dev-kit/, .claude/, .cursor/                    # Local assistant configuration
├── LICENSE                                             # Repository license
└── README.md                                           # This overview
```

Typical project assets inside the labs include:

- `src/` Python pipeline, producer, transformation, setup, quality, and RAG code.
- `resources/jobs_dlt.yml` and `databricks.yml` Asset Bundle definitions.
- `tests/` Pytest suites for transformations, reconciliation, and inference.
- `BI/` Lakeview dashboards, alerts, column masking, and row-level security queries.
- `.ipynb` notebooks for interactive Databricks execution and explanations.

Generated caches such as `__pycache__`, `.pytest_cache`, and `.databricks/bundle` state are not part of the learning flow.

## Prerequisites

The exact requirements vary by lab, but the later projects assume:

- An Azure Databricks workspace with Unity Catalog.
- A Databricks Runtime with PySpark; streaming labs additionally need access to Azure Event Hubs.
- Python 3.10+ for the local test and automation tooling.
- Databricks CLI and, where required, the `databricks-sdk` Python package.
- Azure storage, Key Vault, and a Databricks secret scope for labs 02–03 and the music pipeline.
- A SQL warehouse for SQL dashboards and API execution examples.
- YouTube API access for the music ingestion labs.
- PostgreSQL/Neon access for lab 10 and Vector Search/model access for lab 12.

Never commit tokens, client secrets, Event Hubs connection strings, or model endpoint credentials. Use Databricks secret scopes, CLI profiles, or environment variables.

## Getting started

For the introductory labs, open the notebook in the relevant folder and run it on a Databricks cluster after making the dataset available. Each local README contains the parameters and external services needed by that module.

For a bundle-based music pipeline:

```bash
cd lab_08_ci_cd_dev_prod_promotion
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev music_etl_orchestrator
pytest -q tests
```

The same workflow applies to the later bundle-based labs after reviewing their local `databricks.yml` and resource configuration. Deploy to `prod` only after checking catalog, schema, Volume, cluster, and secret values for the target workspace.

## How to use the repository

Read the labs in numerical order when following the course. Use the README inside a lab for its detailed execution steps and assumptions; use this file to understand how the modules fit together. The `final_project/` directory is the consolidated version of the lakehouse and supporting assets for an end-to-end review.
