# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting container

# COMMAND ----------

application_id = dbutils.secrets.get(scope="nikita-db-secrets", key="application-id")
tenant_id = dbutils.secrets.get(scope="nikita-db-secrets", key="tenant-id")
secret = dbutils.secrets.get(scope="nikita-db-secrets", key="secret")

# COMMAND ----------

container_name = "retail-org"
account_name = "dataengineerstorage19"
mount_point = "/mnt/retail-org"

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

# Reading data from new mount points

# COMMAND ----------

customers = spark.read.csv("/mnt/retail-org/customers/customers.csv", header=True)

# COMMAND ----------

customers.display()

# COMMAND ----------

sales_orders = spark.read.json("/mnt/retail-org/sales_orders/sales_orders.json")

# COMMAND ----------

sales_orders.display()

# COMMAND ----------


