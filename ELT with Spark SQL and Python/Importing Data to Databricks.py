# Databricks notebook source
# below command will list all the files in dbfs

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/"

# COMMAND ----------

import zipfile

# define the path to the zip file 
zip_path = "/dbfs/FileStore/employee_details.zip"

# extract the files from the zip file
with zipfile.ZipFile(zip_path,"r") as zip_ref:
    zip_ref.extractall("/dbfs/FileStore/extracted_files/")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/"

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/extracted_files/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/extracted_files/employee_details/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/extracted_files/employee_details/csv_files/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/extracted_files/employee_details/json_files/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/extracted_files/employee_details/parquet_files/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/extracted_files/employee_details/parquet_files_new/
