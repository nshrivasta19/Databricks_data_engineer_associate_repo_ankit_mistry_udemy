# Databricks notebook source
# MAGIC %md
# MAGIC ## Writing streaming data as a stream in some other location

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Writing a datastream

# COMMAND ----------

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
bankdata_sdf = spark.readStream.csv(bankdata_streaming_path, bankdata_schema, header=True)

# COMMAND ----------

bankdata_sdf.display()

# COMMAND ----------

# write this data as a stream.. this will dump the synced data from bankdata_streaming.csv to another file
# checkpointLocation contains all those data related to meta stuff and the reason is we want to keep track of how many records have been read and how many more records need to write

streamQuery = bankdata_sdf.writeStream.format("delta").\
option("checkpointLocation", "/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink/_checkpointLocation").\
start("/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink")

# COMMAND ----------

# command to verify if streamQuery is still running or not
streamQuery.isActive

# COMMAND ----------

streamQuery.recentProgress

# COMMAND ----------

# command to read delta from stream sink folder
spark.read.format('delta').load('/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink').display()

# COMMAND ----------

bankdata_sdf.isStreaming

# COMMAND ----------

spark.read.format('delta').load('/mnt/streaming-demo/streaming_dataset/bankdata_stream_sink').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Write data stream into a table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a table to write the streaming data into table
# MAGIC create database streaming_db

# COMMAND ----------

# command to write data into table instead of storage data or (dbfs)
streamQuery = bankdata_sdf.writeStream.format("delta").\
option("checkpointLocation", "/mnt/streaming-demo/streaming_dataset/streaming_db/managed/_checkpointLocation").\
toTable("streaming_db.bankdata_m")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming_db.bankdata_m

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming_db.bankdata_m

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history streaming_db.bankdata_m

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended streaming_db.bankdata_m

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table streaming_db.bankdata_m

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


