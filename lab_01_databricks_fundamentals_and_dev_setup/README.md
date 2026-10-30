# Lab 01 — Databricks Fundamentals and Development Setup

This lab introduces the Databricks workspace and Spark workflow by loading a real-estate CSV dataset, exploring it with PySpark, and building a small bronze–silver–gold analysis.

## What is demonstrated

- Starting a Spark session and checking the notebook working directory.
- Reading `alabama_sold_real_estate_intelligence_2026.csv` with an explicit schema.
- Applying common DataFrame operations: renaming columns, checking missing values, filtering, grouping, and sorting.
- Calling an external API and inspecting a JSON response.
- Explaining Delta Lake capabilities such as ACID transactions, time travel, and schema enforcement.
- Converting the raw dataset into a simple analytical result:
  - bronze: the source table as received,
  - silver: selected columns with incomplete records removed,
  - gold: daily and property-type aggregates.

The supplied dashboard presents the first analysis in Databricks SQL/Lakeview.

## Directory structure

```text
lab_01_databricks_fundamentals_and_dev_setup/
├── Notebook.ipynb                              # Main Spark exploration and transformation notebook
├── alabama_sold_real_estate_intelligence_2026.csv # Source real-estate dataset
├── Dashboard - week 1.lvdash.json               # Dashboard definition for the analysis
├── image_1783362421351.png                      # Example API response used in the notebook
└── README.md                                    # This documentation
```

## Running the lab

1. Upload the folder or its files to a Databricks workspace with Unity Catalog enabled.
2. Make the CSV available to the notebook, or change the input path in `Notebook.ipynb`.
3. Open `Notebook.ipynb` and attach a cluster with PySpark support.
4. Run the cells from top to bottom. The notebook creates the intermediate DataFrames and finishes with the gold-level aggregation.
5. Import `Dashboard - week 1.lvdash.json` into the workspace if you want to reproduce the dashboard.

The notebook contains workspace-specific paths. Update those paths rather than hardcoding credentials or tokens.
