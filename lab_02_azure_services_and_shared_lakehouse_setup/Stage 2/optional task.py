# Databricks notebook source
# MAGIC %md
# MAGIC # The medallion architecture
# MAGIC This is the modern industry-standard design pattern for organizing data within a Lakehouse. It structures files into three distinct logical layers:
# MAGIC
# MAGIC - Bronze: Ingests raw data directly from your source systems (like files or APIs) with no transformations. We keep this as our "immutable source of truth".
# MAGIC - Silver: Filters out corrupt files, removes duplicate records, enforces schemas, and normalizes values.
# MAGIC - Gold: Stores high-level, aggregate data optimized for dashboards or reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC # Managed Identity vs. Service Principal
# MAGIC
# MAGIC When connecting to cloud storage, organizations typically use either a Service Principal or Managed Identity for authentication.
# MAGIC
# MAGIC Service Principal:
# MAGIC
# MAGIC - Credential Handling: Requires manual setup of Client IDs, Tenant IDs, and Client Secrets.
# MAGIC - Maintenance Effort: Teams must store credentials securely and monitor their expiration to avoid disruptions.
# MAGIC - Governance Granularity: Access permissions are generally set at the resource or container level.
# MAGIC
# MAGIC Managed Identity:
# MAGIC
# MAGIC - Credential Handling: Azure automatically manages authentication, eliminating the need for manual credentials.
# MAGIC - Maintenance Effort: No passwords or secrets to manage, reducing the risk of credential leaks or expiration.
# MAGIC - Governance Granularity: Permissions can be assigned directly to specific assets, offering more precise control and integration with Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # External libraries
# MAGIC If you need extra Python tools or drivers for your PySpark jobs, here’s how you can add them:
# MAGIC
# MAGIC - Wheels: Build your own Python packages, upload them to a Volume, and install with %pip install /Volumes/....
# MAGIC - PIP (%pip): Use this to grab standard Python packages from PyPI (like scikit-learn) for your notebook.
# MAGIC - Maven: For Scala or Java libraries—such as JDBC drivers for databases like PostgreSQL or MongoDB—attach them at the cluster level.