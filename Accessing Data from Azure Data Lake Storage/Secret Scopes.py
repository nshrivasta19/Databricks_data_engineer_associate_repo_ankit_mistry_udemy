# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Mount Storage container using f string

# COMMAND ----------

application_id = "c3cd2864-ea08-4963-8c4d-48ae02117f1a"
tenant_id = "93f33571-550f-43cf-b09f-cd331338d086"
secret = "wrc8Q~WJ02EH5K2RUQlSt85NZSbNsH8NB-cXWc-z"

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze/")

# COMMAND ----------

container_name = "bronze"
account_name = "dataengineerstorage19"
mount_point = "/mnt/bronze"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Secret scopes / azure key vault

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step -1. Creating a key vault
# MAGIC
# MAGIC - create a key vault for your storage account
# MAGIC - Create a secret scope using databricks url by adding #/secrets/createScope
# MAGIC
# MAGIC example - https://adb-908463096322310.10.azuredatabricks.net/?o=908463096322310#/secrets/createScope
# MAGIC
# MAGIC - to create resource id for key vault, simply change the required names from below line
# MAGIC
# MAGIC /subscriptions/25f91caa-f779-4929-b0a8-21631c42f6b5/resourcegroups/dataengineer/providers/Microsoft.KeyVault/vaults/nikita-db-secret

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step - 2. Accessing the secret values

# COMMAND ----------

# command to list secret scopes
scopes = dbutils.secrets.listScopes()
for scope in scopes:
    print(scope)

# COMMAND ----------

# MAGIC %md
# MAGIC #### To resolve below error while using to get the secret keys from keyvault --
# MAGIC Operation failed: This request is not authorized to perform this operation using this permission. , 403, GET, https://dataengineersa.dfs.core.windows.net/bronze?upn=false&resource=filesystem&maxResults=5000&timeout=90&recursive=false, AuthorizationPermissionMismatch, This request is not authorized to perform this operation using this permission. RequestId:9d995707-801f-0005-095d-9ac721000000 Time:2024-04-29T17:46:31.0056002Z
# MAGIC
# MAGIC You need to perform below steps to fix the code---
# MAGIC
# MAGIC 1. Go to the Azure portal.
# MAGIC 2. Navigate to the specified KeyVault (https://nikita-db-secret.vault.azure.net/).
# MAGIC 3. 4. Click on "Access control (IAM)" on the left-side menu.
# MAGIC 5. Click on the "+ Add a role assignment" button.
# MAGIC 6. In the "Add role assignment" panel, specify the details:
# MAGIC Role: Select a role that has the required permissions, such as "Key Vault Secrets Officer" or "Key Vault Secrets User".
# MAGIC 7. Assign access to: Select "Azure AD user, group, or service principal".
# MAGIC 8. Select: Search and select "AzureDatabricks".
# MAGIC 9.Click on the "Save" button to save the role assignment.
# MAGIC
# MAGIC After granting the necessary permissions, you should be able to run the code successfully without getting the "PERMISSION_DENIED" error.
# MAGIC

# COMMAND ----------

# redacted means output will not be visibel
dbutils.secrets.get(scope="nikita-db-secrets", key="tenant-id")

# COMMAND ----------

# But one can view the secret value by using loop such as
for i in dbutils.secrets.get(scope="nikita-db-secrets", key="tenant-id"):
    print(i)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step - 3. Mount Storage Container

# COMMAND ----------

application_id = "c3cd2864-ea08-4963-8c4d-48ae02117f1a"
tenant_id = "93f33571-550f-43cf-b09f-cd331338d086"
secret = "wrc8Q~WJ02EH5K2RUQlSt85NZSbNsH8NB-cXWc-z"

# COMMAND ----------

application_id = dbutils.secrets.get(scope="nikita-db-secrets", key="application-id")
tenant_id = dbutils.secrets.get(scope="nikita-db-secrets", key="tenant-id")
secret = dbutils.secrets.get(scope="nikita-db-secrets", key="secret")

# COMMAND ----------

container_name = "bronze"
account_name = "dataengineerstorage19"
mount_point = "/mnt/bronze"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

bank_data = spark.read.csv("/mnt/bronze/bank_data.csv", header = True)

# COMMAND ----------

bank_data.display()

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------


