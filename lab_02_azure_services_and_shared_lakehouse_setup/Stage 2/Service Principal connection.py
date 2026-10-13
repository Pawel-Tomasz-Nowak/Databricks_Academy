# Databricks notebook source
dbutils.widgets.text("login", "", "User Login")
login = dbutils.widgets.get("login")

# COMMAND ----------

# DBTITLE 1,Set secret scope
secret_scope = f"{login}_scope"

# COMMAND ----------

# DBTITLE 1,Retrieve credentials from Azure Key Vault
# Retrieve credentials from Azure Key Vault
client_id = dbutils.secrets.get(scope=secret_scope, key="sp-databricks-adls-appid")
tenant_id = dbutils.secrets.get(scope=secret_scope, key="tenant-id")
client_secret = dbutils.secrets.get(scope=secret_scope, key="sp-databricks-adls-appkey")

# COMMAND ----------

storage_account = "dlspl21databricks"
container_name = login
mount_point = f"/mnt/{container_name}_legacy"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

# DBTITLE 1,Configure direct ABFSS access (no mount needed)
for key, value in configs.items():
    spark.conf.set(key, value)

abfss_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
print(f"Configured direct access to {abfss_path}")