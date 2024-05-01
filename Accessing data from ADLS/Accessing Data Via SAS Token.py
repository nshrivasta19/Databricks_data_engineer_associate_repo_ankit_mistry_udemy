# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Resources:
# MAGIC
# MAGIC 1) Access Azure Data Lake Storage Gen2 or Blob Storage using a SAS token - https://learn.microsoft.com/en-us/azure/databricks/external-data/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token
# MAGIC
# MAGIC 2) Use the Azure Data Lake Storage Gen2 URI - https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri

# COMMAND ----------

sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-05-01T00:56:22Z&st=2024-04-28T16:56:22Z&spr=https&sig=%2FcNarNVwk7uZvAPdcMwWGS2fxn6zhyl%2B%2Be%2FFA%2Fcp2Zo%3D

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dataengineerstorage19.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dataengineerstorage19.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dataengineerstorage19.dfs.core.windows.net","sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-05-01T00:56:22Z&st=2024-04-28T16:56:22Z&spr=https&sig=%2FcNarNVwk7uZvAPdcMwWGS2fxn6zhyl%2B%2Be%2FFA%2Fcp2Zo%3D")

# COMMAND ----------

## reading the file data by using spark command and storing it in a variable bank_data
bank_data = spark.read.csv("abfss://bronze@dataengineerstorage19.dfs.core.windows.net/bank_data.csv", header=True)

# COMMAND ----------

# Displaying the data 
bank_data.display()

# COMMAND ----------

