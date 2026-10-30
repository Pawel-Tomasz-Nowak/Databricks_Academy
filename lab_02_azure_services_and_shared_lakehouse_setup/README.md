# Lab 02 — Azure Services and Shared Lakehouse Setup

This lab provisions the Azure and Databricks building blocks needed by the later lakehouse labs, then moves the real-estate dataset through bronze and silver schemas using parameterised notebooks.

## What is demonstrated

The work is split into two stages:

1. **Azure resource preparation** — screenshots document the workspace, storage account, and Key Vault configuration.
2. **Lakehouse implementation** — notebooks create schemas and connect Databricks to Azure Data Lake Storage using a service principal whose values are read from a Databricks secret scope.

The ingestion flow is:

```text
source table
    │
    ├── bronze_ingestion.ipynb ──► <login>_bronze
    │
    └── silver_ingestion.ipynb ──► <login>_silver
```

`creating schemas.dbquery.ipynb` creates the storage-backed schemas with dynamic SQL parameters. The optional notebook explains the medallion architecture, compares managed identity with service principals, and shows how to install external Python libraries from a Unity Catalog Volume.

## Directory structure

```text
lab_02_azure_services_and_shared_lakehouse_setup/
├── Stage 1/
│   ├── workspace.png                         # Azure Databricks workspace evidence
│   ├── storage_account.png                   # ADLS storage configuration evidence
│   └── key_vault.png                         # Key Vault configuration evidence
├── Stage 2/
│   ├── creating schemas.dbquery.ipynb         # Parameterised bronze/silver schema setup
│   ├── Service Principal connection.ipynb     # Secret-scope based ADLS authentication
│   ├── bronze_ingestion.ipynb                # Source table to bronze schema
│   ├── silver_ingestion.ipynb                # Bronze to silver schema workflow
│   └── optional task.ipynb                   # Medallion, identity, and library notes
└── README.md                                  # This documentation
```

## Running the lab

1. Create or obtain access to an Azure Databricks workspace, ADLS storage account, and Azure Key Vault.
2. Store the service-principal application ID, tenant ID, and secret in the Databricks secret scope expected by the notebooks.
3. Open `Stage 2/creating schemas.dbquery.ipynb` and provide the catalog, storage account, and container parameters.
4. Run `Service Principal connection.ipynb` to configure the authenticated storage access.
5. Run `bronze_ingestion.ipynb`, then `silver_ingestion.ipynb`, supplying the `login` and `catalog` widgets in each notebook.

Do not place client secrets in notebook cells. The notebooks are intended to retrieve them with `dbutils.secrets.get`.
