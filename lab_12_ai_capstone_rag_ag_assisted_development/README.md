# Lab 12 — AI Capstone: RAG and Agent-Assisted Development

This capstone extends the music analytics lakehouse with a retrieval-augmented generation (RAG) workflow, inference logging, governance checks, and automated tests for the resulting Python components.

## What is demonstrated

The RAG notebook in `src/rag/part_A_RAG_on_databricks.ipynb`:

1. Splits text documents into chunks.
2. Stores the chunks in a Delta table.
3. Applies constraints and sanity checks to the prepared data.
4. Defines a Databricks Vector Search endpoint for Power BI documentation.
5. Compares a RAG-assisted LLM response with a base-model response.
6. Records prompt and inference information for auditability.

The capstone also carries the production pipeline assets from the previous labs: DLT/Lakeflow source files, producer and setup scripts, quality reconciliation, dashboards, and `databricks.yml`. The SQL notebooks in `BI/CLS` and `BI/RLS` define column masking and author-based row filtering, while `automation/` contains scripts for platform execution and short-lived compute.

## Local directory structure

```text
lab_12_ai_capstone_rag_ag_assisted_development/
├── src/rag/
│   ├── part_A_RAG_on_databricks.ipynb # Chunking, Vector Search, RAG comparison, and audit notes
│   └── inference.py                   # Reusable RAG inference implementation
├── automation/                        # Platform connection and run helpers
├── src/pipelines/                     # Bronze-to-gold Lakeflow pipeline
├── src/quality/                        # Reconciliation gate and quality scorecard
├── BI/CLS/                             # Column-level masking function
├── BI/RLS/                             # Row-level security function and tests
├── tests/                              # Transformation, reconciliation, and RAG tests
├── resources/jobs_dlt.yml              # Pipeline and job resources
├── databricks.yml                      # Asset Bundle configuration
└── README.md                           # This documentation
```

## Running the capstone

1. Authenticate the Databricks CLI and configure the catalog, schema, Volume, Vector Search endpoint, and model endpoint for the workspace.
2. Run the lakehouse setup and ingestion workflow defined by `resources/jobs_dlt.yml`, or use the existing tables required by the RAG notebook.
3. Open `src/rag/part_A_RAG_on_databricks.ipynb`, configure the source document location, and run the cells in order.
4. Run the SQL notebooks in `BI/CLS` and `BI/RLS` with an administrator and a restricted test user to verify masking and row filtering.
5. Run the local checks with:

```bash
pytest -q tests
```

The RAG notebook requires Databricks Vector Search and model access. Keep workspace tokens, API keys, document contents, and endpoint identifiers in secret scopes or environment configuration.
