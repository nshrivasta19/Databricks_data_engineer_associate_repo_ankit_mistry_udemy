# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting ADLS to DBFS

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Steps to mount the ADLS to DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Registration Details
# MAGIC 1. Create an app in app registration
# MAGIC 2. create a client secret
# MAGIC

# COMMAND ----------

application_id = "c3cd2864-ea08-4963-8c4d-48ae02117f1a"
tenant_id = "93f33571-550f-43cf-b09f-cd331338d086"
secret = "wrc8Q~WJ02EH5K2RUQlSt85NZSbNsH8NB-cXWc-z"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Service App Permissions
# MAGIC 1. Grant access on storage account in IAM

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting ADLS to DBFS

# COMMAND ----------

# Command to unmount any mountpoint
dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "c3cd2864-ea08-4963-8c4d-48ae02117f1a",
          "fs.azure.account.oauth2.client.secret": "wrc8Q~WJ02EH5K2RUQlSt85NZSbNsH8NB-cXWc-z",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/93f33571-550f-43cf-b09f-cd331338d086/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@dataengineerstorage19.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

bank_data = spark.read.csv("/mnt/bronze/bank_data.csv", header=True)

# COMMAND ----------

bank_data.display()

# COMMAND ----------

bank_data = spark.read.json("/mnt/bronze/sales_orders.json")

# COMMAND ----------

bank_data.display()

# COMMAND ----------

