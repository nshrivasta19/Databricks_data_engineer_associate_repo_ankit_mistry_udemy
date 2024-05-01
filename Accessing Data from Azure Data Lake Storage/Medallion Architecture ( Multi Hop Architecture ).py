# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture
# MAGIC ## Layers in Medallion Architecture - Bronze, Silver and Gold
# MAGIC - Bronze -> Raw ingested data is stored here.
# MAGIC - Silver -> After applying some minor transformation in bronze layer, it is stored in silver.
# MAGIC - Gold -> After applying some more transformations it is stored in gold and used for Power BI reporting, ML and analytics etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting 3 containers

# COMMAND ----------

application_id = dbutils.secrets.get(scope="nikita-db-secrets", key="application-id")
tenant_id = dbutils.secrets.get(scope="nikita-db-secrets", key="tenant-id")
secret = dbutils.secrets.get(scope="nikita-db-secrets", key="secret")

# COMMAND ----------

container_name = "bronze"
account_name = "dataengineerstorage19"
mount_point = "/mnt/bronze"

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

container_name = "silver"
account_name = "dataengineerstorage19"
mount_point = "/mnt/silver"

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

container_name = "gold"
account_name = "dataengineerstorage19"
mount_point = "/mnt/gold"

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

# MAGIC %md
# MAGIC ### Reading the file from the bronze container

# COMMAND ----------

# created a variable bank_data_path and then will do processing after reading the file
bank_data_path = "/mnt/bronze/bank_data.csv"

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
bank_data_schema = StructType([
                    StructField("CustomerId", IntegerType()),
                    StructField("Surname", StringType()),
                    StructField("CreditScore", IntegerType()),
                    StructField("Geography", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Age", IntegerType()),
                    StructField("Tenure", IntegerType()),
                    StructField("Balance", DoubleType()),
                    StructField("NumOfProducts", IntegerType()),
                    StructField("HasCrCard", IntegerType()),
                    StructField("IsActiveMember", IntegerType()),
                    StructField("EstimatedSalary", DoubleType()),
                    StructField("Exited", IntegerType())
                    ]
                    )
 
bank_data=spark.read.csv(path=bank_data_path, header=True, schema=bank_data_schema)

# COMMAND ----------

bank_data.display()

# COMMAND ----------

bank_data = bank_data.drop("Surname","Geography","Gender")

# COMMAND ----------

bank_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data to silver

# COMMAND ----------

bank_data.write.parquet("/mnt/silver/bank_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data to gold

# COMMAND ----------

bank_data = spark.read.parquet("/mnt/silver/bank_data")

# COMMAND ----------

bank_data.display()

# COMMAND ----------

# Removing data whch have 0 balance 
bank_data = bank_data[bank_data['Balance'] !=0]

# COMMAND ----------

bank_data.display()

# COMMAND ----------

bank_data.write.parquet("/mnt/gold/bank_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmounting the containers

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")
dbutils.fs.unmount("/mnt/silver")
dbutils.fs.unmount("/mnt/gold")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


