# Databricks notebook source
# MAGIC %md
# MAGIC ### Link ko the documentation
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-the-account-key

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dataengineerstorage19.dfs.core.windows.net",
    "mVp7qXuuQndOpm+eEH6leis+uZ9ohXSSq7XclrpzwwX1rGQ4VUBIUoCQwaPn5iwAp9X/MN3b4ubN+AStJD15iw==")

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver
# MAGIC

# COMMAND ----------

abfs[s]://bronze@dataengineerstorage19.dfs.core.windows.net/bank_data.csv

# COMMAND ----------

## reading the file data by using spark command and storing it in a variable bank_data
bank_data = spark.read.csv("abfss://bronze@dataengineerstorage19.dfs.core.windows.net/bank_data.csv", header=True)

# COMMAND ----------

# Displaying the data 
bank_data.display()

# COMMAND ----------

