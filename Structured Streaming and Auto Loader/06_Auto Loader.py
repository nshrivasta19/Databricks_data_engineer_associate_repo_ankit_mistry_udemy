# Databricks notebook source
# importing datatypes from pyspark library 
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# define the path of the file
bankdata_streaming_path  = "/mnt/streaming-demo/streaming_dataset/bankdata_streaming.csv"

# define schema of the file
bankdata_schema = StructType([
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



# COMMAND ----------

# Now we have to create a streaming data frame from above mentioned path
# created a streaming dataframe to read the streaming data bankdata_sdf and readStream will read the data from the streaming file
bankdata_sdf = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").schema(bankdata_schema).load(bankdata_streaming_path,header=True)

# COMMAND ----------

#creating a view from above streaming dataframe
bankdata_sdf.createTempView("bank_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_view

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating table from streaming dataframe and storing it in some particular location

# COMMAND ----------

bankdata_sdf.writeStream.option("checkpointLocation", "/mnt/streaming-demo/streaming_dataset/table/_checkpointLocation").table("bankdata_t")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bankdata_t

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended bankdata_t

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()
